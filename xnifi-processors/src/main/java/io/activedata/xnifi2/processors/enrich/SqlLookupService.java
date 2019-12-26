package io.activedata.xnifi2.processors.enrich;

import com.github.benmanes.caffeine.cache.Cache;
import io.activedata.xnifi2.core.batch.Input;
import io.activedata.xnifi2.sql2o.Sql2oHelper;
import io.activedata.xnifi2.support.cache.CacheBuilder;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"lookup", "cache", "enrich", "join", "rdbms", "database", "reloadable", "key", "value", "record"})
@CapabilityDescription("数据库检索服务")
public class SqlLookupService extends AbstractControllerService implements LookupService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlLookupService.class);

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("sql-lookup-service")
            .displayName("数据库连接池")
            .description("数据库连接池服务")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("sql-lookup-cache-size")
            .displayName("缓存大小")
            .description("检索结果缓存大小，key为搜索条件，value为返回的记录。")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .required(true)
            .build();

    public static final PropertyDescriptor CLEAR_CACHE_ON_ENABLED = new PropertyDescriptor.Builder()
            .name("sql-lookup-clear-cache-on-enabled")
            .displayName("是否在启动时清空缓存")
            .description("是否在启动时清空缓存。")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor CACHE_EXPIRATION = new PropertyDescriptor.Builder()
            .name("sql-lookup-cache-expiration")
            .description("缓存失效时间，如果为0则永不失效.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor LOOKUP_SQL = new PropertyDescriptor.Builder()
            .name("sql-lookup-sql")
            .displayName("检索SQL")
            .description("检索SQL。")
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
        properties.add(DBCP_SERVICE);
        properties.add(LOOKUP_SQL);
        properties.add(CACHE_SIZE);
        properties.add(CLEAR_CACHE_ON_ENABLED);
        properties.add(CACHE_EXPIRATION);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.lookupSql = context.getProperty(LOOKUP_SQL).getValue();
        if (this.sql2o == null) {
            this.dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
            this.sql2o = Sql2oHelper.create(dbcpService);
            this.schema = Sql2oHelper.schema(sql2o, this.lookupSql);
        }

        final int cacheSize = context.getProperty(CACHE_SIZE).evaluateAttributeExpressions().asInteger();
        final boolean clearCache = context.getProperty(CLEAR_CACHE_ON_ENABLED).asBoolean();
        final long durationNanos = context.getProperty(CACHE_EXPIRATION).isSet() ? context.getProperty(CACHE_EXPIRATION).evaluateAttributeExpressions().asTimePeriod(TimeUnit.NANOSECONDS) : 0L;
        if (this.cache == null || (cacheSize > 0 && clearCache)) {
            if (durationNanos > 0) {
                this.cache = CacheBuilder.build(cacheSize, durationNanos);
            } else {
                this.cache = CacheBuilder.build(cacheSize);
            }
        }
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
                    LOGGER.debug("读取[{}]对应的记录并放入到缓存中。", key);
                }else {
                    LOGGER.debug("没有找到[{}]对应的记录。", key);
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
