package io.activedata.xnifi2.processors.enrich;

import com.github.benmanes.caffeine.cache.Cache;
import io.activedata.xnifi2.core.batch.Input;
import io.activedata.xnifi2.sql2o.Sql2oHelper;
import io.activedata.xnifi2.support.cache.CacheBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"lookup", "cache", "enrich", "join", "rdbms", "database", "reloadable", "key", "value", "record"})
@CapabilityDescription("数据库检索服务")
public class SqlLookupService extends AbstractControllerService implements LookupService {

    public static final PropertyDescriptor PROP_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("sql-lookup-service")
            .displayName("数据库连接池")
            .description("数据库连接池服务")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor PROP_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("sql-lookup-cache-size")
            .displayName("缓存大小")
            .description("检索结果缓存大小，key为搜索条件，value为返回的记录。")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .required(true)
            .build();

    public static final PropertyDescriptor PROP_CLEAR_CACHE_ON_ENABLED = new PropertyDescriptor.Builder()
            .name("sql-lookup-clear-cache-on-enabled")
            .displayName("是否在启动时清空缓存")
            .description("是否在启动时清空缓存。")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor PROP_CACHE_EXPIRATION = new PropertyDescriptor.Builder()
            .name("sql-lookup-cache-expiration")
            .displayName("缓存失效时间")
            .description("缓存失效时间，如果为0则永不失效.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROP_LOOKUP_SQL = new PropertyDescriptor.Builder()
            .name("sql-lookup-sql")
            .displayName("检索SQL")
            .description("检索SQL，可用':param1引用记录字段或FlowFile属性。优先引用记录字段'")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private volatile Sql2o sql2o;
    private volatile String lookupSql;
    private volatile Cache<String, Record> cache;
    private volatile DBCPService dbcpService;
    private RecordSchema schema;
    protected List<PropertyDescriptor> properties;

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PROP_DBCP_SERVICE);
        properties.add(PROP_LOOKUP_SQL);
        properties.add(PROP_CACHE_SIZE);
        properties.add(PROP_CLEAR_CACHE_ON_ENABLED);
        properties.add(PROP_CACHE_EXPIRATION);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final int cacheSize = context.getProperty(PROP_CACHE_SIZE).evaluateAttributeExpressions().asInteger();
        final boolean clearCache = context.getProperty(PROP_CLEAR_CACHE_ON_ENABLED).asBoolean();
        final long durationNanos = context.getProperty(PROP_CACHE_EXPIRATION).isSet() ? context.getProperty(PROP_CACHE_EXPIRATION).evaluateAttributeExpressions().asTimePeriod(TimeUnit.NANOSECONDS) : 0L;
        this.lookupSql = context.getProperty(PROP_LOOKUP_SQL).getValue();
        this.dbcpService = context.getProperty(PROP_DBCP_SERVICE).asControllerService(DBCPService.class);
        this.sql2o = Sql2oHelper.create(dbcpService);
        this.schema = Sql2oHelper.schema(sql2o, lookupSql);

        getLogger().error(schema.toString());

        if (this.cache == null || (cacheSize > 0 && clearCache)) {
            if (durationNanos > 0) {
                this.cache = CacheBuilder.build(cacheSize, durationNanos);
            } else {
                this.cache = CacheBuilder.build(cacheSize);
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        try {
            this.lookupSql = context.getProperty(PROP_LOOKUP_SQL).getValue();
            this.dbcpService = context.getProperty(PROP_DBCP_SERVICE).asControllerService(DBCPService.class);
            this.sql2o = Sql2oHelper.create(dbcpService);
            this.schema = Sql2oHelper.schema(sql2o, lookupSql);
        }catch (Exception e){
            ValidationResult validationResult = new ValidationResult.Builder()
                    .input("检索SQL")
                    .subject("检索服务无法启用。")
                    .valid(false)
                    .explanation(ExceptionUtils.getMessage(e))
                    .build();
            return Arrays.asList(validationResult);
        }

        return Collections.EMPTY_LIST;
    }

    @Override
    public Optional<Record> lookup(Input input, Map<String, String> attributes) {
        try (Connection conn = sql2o.open()) {
            Query query = conn.createQuery(lookupSql);
            Map<String, List<Integer>> paramIdxMap = query.getParamNameToIdxMap();
            String key = Sql2oHelper.cacheKey(paramIdxMap, input, attributes);
            Record foundRecord = cache.getIfPresent(key);

            if (foundRecord == null) {
                foundRecord = new MapRecord(this.schema, Sql2oHelper.fetchFirstResult(sql2o, lookupSql, input, attributes));
                if (foundRecord != null) {
                    cache.put(key, foundRecord);
                    getLogger().debug("读取[" + key + "]对应的记录并放入到缓存中。");
                } else {
                    getLogger().debug("没有找到[" + key + "]对应的记录。");
                }
            }

            return Optional.ofNullable(foundRecord);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}
