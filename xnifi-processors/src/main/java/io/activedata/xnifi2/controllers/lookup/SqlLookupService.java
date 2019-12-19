package io.activedata.xnifi2.controllers.lookup;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.activedata.xnifi2.sql2o.Sql2oHelper;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.Tuple;
import org.sql2o.Query;
import org.sql2o.Sql2o;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"lookup", "cache", "enrich", "join", "rdbms", "database", "reloadable", "key", "value", "record"})
@CapabilityDescription("A relational-database-based lookup service. When the lookup key is found in the database, "
        + "the specified columns (or all if Lookup Value Columns are not specified) are returned as a Record. Only one row "
        + "will be returned for each lookup, duplicate database entries are ignored.")
public class SqlLookupService extends AbstractControllerService implements RecordLookupService {
    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("sql-lookup-service")
            .displayName("数据库连接池")
            .description("数据库连接池服务")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("sql-lookup-cache-size")
            .displayName("缓存大小")
            .description("Specifies how many lookup values/records should be cached. The cache is shared for all tables and keeps a map of lookup values to records. "
                    + "Setting this property to zero means no caching will be done and the table will be queried for each lookup value in each record. If the lookup "
                    + "table changes often or the most recent data must be retrieved, do not use the cache.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .required(true)
            .build();

    static final PropertyDescriptor CLEAR_CACHE_ON_ENABLED = new PropertyDescriptor.Builder()
            .name("sql-lookup-clear-cache-on-enabled")
            .displayName("是否在启动时清空缓存")
            .description("Whether to clear the cache when this service is enabled. If the Cache Size is zero then this property is ignored. Clearing the cache when the "
                    + "service is enabled ensures that the service will first go to the database to get the most recent data.")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .required(true)
            .build();

    static final PropertyDescriptor CACHE_EXPIRATION = new PropertyDescriptor.Builder()
            .name("Cache Expiration")
            .description("缓存失效时间，如果为0则永不失效.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private volatile Sql2o sql2o;
    private volatile String lookupSql;
    private volatile Cache<Tuple<String, Object>, Record> cache;
    private volatile DBCPService dbcpService;
    protected List<PropertyDescriptor> properties;

    static final PropertyDescriptor LOOKUP_SQL = new PropertyDescriptor.Builder()
            .name("sql-lookup-sql")
            .displayName("检索SQL")
            .description("")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();


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
        if (this.sql2o == null) {
            this.dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
            this.sql2o = Sql2oHelper.create(dbcpService);
        }

        this.lookupSql = context.getProperty(LOOKUP_SQL).getValue();

        final int cacheSize = context.getProperty(CACHE_SIZE).evaluateAttributeExpressions().asInteger();
        final boolean clearCache = context.getProperty(CLEAR_CACHE_ON_ENABLED).asBoolean();
        final long durationNanos = context.getProperty(CACHE_EXPIRATION).isSet() ? context.getProperty(CACHE_EXPIRATION).evaluateAttributeExpressions().asTimePeriod(TimeUnit.NANOSECONDS) : 0L;
        if (this.cache == null || (cacheSize > 0 && clearCache)) {
            if (durationNanos > 0) {
                this.cache = Caffeine.newBuilder()
                        .maximumSize(cacheSize)
                        .expireAfter(new Expiry<Tuple<String, Object>, Record>() {
                            @Override
                            public long expireAfterCreate(Tuple<String, Object> stringObjectTuple, Record record, long currentTime) {
                                return durationNanos;
                            }

                            @Override
                            public long expireAfterUpdate(Tuple<String, Object> stringObjectTuple, Record record, long currentTime, long currentDuration) {
                                return currentDuration;
                            }

                            @Override
                            public long expireAfterRead(Tuple<String, Object> stringObjectTuple, Record record, long currentTime, long currentDuration) {
                                return currentDuration;
                            }
                        })
                        .build();
            } else {
                this.cache = Caffeine.newBuilder()
                        .maximumSize(cacheSize)
                        .build();
            }
        }
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        return lookup(coordinates, null);
    }

    @Override
    public Optional<Record> lookup(final Map<String, Object> coordinates, Map<String, String> context) throws LookupFailureException {
        try (org.sql2o.Connection con = sql2o.open()) {
            Query query = con.createQuery(lookupSql);
            Map<String, List<Integer>> paramIdxMap = query.getParamNameToIdxMap();
            String key = Sql2oHelper.cacheKey(paramIdxMap, context, coordinates);
            Tuple<String, Object> cacheLookupKey = new Tuple<>("LookupCache", key);
            // Not using the function param of cache.get so we can catch and handle the checked exceptions
            Record foundRecord = cache.get(cacheLookupKey, k -> null);

            if (foundRecord == null) {
                foundRecord = new MapRecord(null, Sql2oHelper.fetchFirstResult(sql2o, lookupSql, context, coordinates));
                // Populate the cache if the record is present
                if (foundRecord != null) {
                    cache.put(cacheLookupKey, foundRecord);
                }
            }

            return Optional.ofNullable(foundRecord);
        }
    }

    private static boolean isNotBlank(final String value) {
        return value != null && !value.trim().isEmpty();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.EMPTY_SET;
    }
}
