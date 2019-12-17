package io.activedata.xnifi.neo4j;

import io.activedata.xnifi.core.AbstractXNifiProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * 抽象的NEO4J Cypher查询处理类
 */
public abstract class AbstractNeo4jCypherProcessor extends AbstractXNifiProcessor {

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("neo4j-query")
            .displayName("Cypher查询")
            .description("请指定Neo4J Cypher查询。")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONNECTION_URL = new PropertyDescriptor.Builder()
            .name("neo4j-connection-url")
            .displayName("NEO4J连接字符串")
            .description("请指定NEO4J连接字符串。")
            .required(true)
            .defaultValue("bolt://localhost:7687")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("neo4j-username")
            .displayName("用户名")
            .description("请指定NEO4J连接用户名。")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("neo4j-password")
            .displayName("密码")
            .description("请指定NEO4J连接密码。")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static AllowableValue LOAD_BALANCING_STRATEGY_ROUND_ROBIN = new AllowableValue(Config.LoadBalancingStrategy.ROUND_ROBIN.name(), "Round Robin", "Round Robin Strategy");

    public static AllowableValue LOAD_BALANCING_STRATEGY_LEAST_CONNECTED = new AllowableValue(Config.LoadBalancingStrategy.LEAST_CONNECTED.name(), "Least Connected", "Least Connected Strategy");

    public static final PropertyDescriptor LOAD_BALANCING_STRATEGY = new PropertyDescriptor.Builder()
            .name("neo4j-load-balancing-strategy")
            .displayName("负载均衡策略")
            .description("负载均衡策略：Round Robin（轮询调度）或Least Connected（最少连接）。")
            .required(false)
            .defaultValue(LOAD_BALANCING_STRATEGY_ROUND_ROBIN.getValue())
            .allowableValues(LOAD_BALANCING_STRATEGY_ROUND_ROBIN, LOAD_BALANCING_STRATEGY_LEAST_CONNECTED)
            .build();

    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-time-out")
            .displayName("最大连接超时时间")
            .description("NEO4J连接的最大超时时间，默认为5秒。")
            .defaultValue("5 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_CONNECTION_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-pool-size")
            .displayName("最大连接数")
            .description("NEO4J的最大连接数，默认100个连接。")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_CONNECTION_ACQUISITION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-acquisition-timeout")
            .displayName("最大连接占用时间")
            .description("NEO4J的最大连接占用时间，默认为60秒。")
            .defaultValue("60 second")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor IDLE_TIME_BEFORE_CONNECTION_TEST = new PropertyDescriptor.Builder()
            .name("neo4j-idle-time-before-test")
            .displayName("最大连接空闲时间")
            .description("在检测连接可用前的最大空闲时间，默认为60秒。")
            .defaultValue("60 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_CONNECTION_LIFETIME = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-lifetime")
            .displayName("最大连接存活时间")
            .description("最大连接存活时间，默认为3600秒。")
            .defaultValue("3600 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor ENCRYPTION = new PropertyDescriptor.Builder()
            .name("neo4j-driver-tls-encryption-enabled")
            .displayName("是否使用TLS加密传输数据")
            .description("是否使用TLS加密传输数据，默认为true。")
            .defaultValue("true")
            .required(true)
            .allowableValues("true","false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .sensitive(false)
            .build();

    public static AllowableValue TRUST_SYSTEM_CA_SIGNED_CERTIFICATES =
            new AllowableValue(Config.TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES.name(),
                    "信任系统证书", "信任系统证书");

    public static AllowableValue TRUST_CUSTOM_CA_SIGNED_CERTIFICATES =
            new AllowableValue(Config.TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES.name(),
                    "信任自定义证书", "信任自定义证书");

    public static AllowableValue TRUST_ALL_CERTIFICATES =
            new AllowableValue(Config.TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES.name(),
                    "信任所有证书", "信任所有证书");

    public static final PropertyDescriptor TRUST_STRATEGY = new PropertyDescriptor.Builder()
            .name("neo4j-trust-strategy")
            .displayName("信任策略")
            .description("信任策略：信任所有证书、信任系统证书、信任自定义证书")
            .required(false)
            .defaultValue(TRUST_ALL_CERTIFICATES.getValue())
            .allowableValues(TRUST_ALL_CERTIFICATES, TRUST_SYSTEM_CA_SIGNED_CERTIFICATES, TRUST_CUSTOM_CA_SIGNED_CERTIFICATES)
            .build();

    public static final PropertyDescriptor TRUST_CUSTOM_CA_SIGNED_CERTIFICATES_FILE = new PropertyDescriptor.Builder()
            .name("neo4j-custom-ca-strategy-certificates-file")
            .displayName("自定义证书文件")
            .description("请指定自定义证书文件路径。")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("成功处理的FlowFile队列。")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("处理过程出现错误的FlowFile队列。")
            .build();

    public static final String ERROR_MESSAGE = "neo4j.error.message";
    public static final String NODES_CREATED= "neo4j.nodes.created";
    public static final String RELATIONS_CREATED = "neo4j.relations.created";
    public static final String LABELS_ADDED = "neo4j.labels.added";
    public static final String NODES_DELETED = "neo4j.nodes.deleted";
    public static final String RELATIONS_DELETED = "neo4j.relations.deleted";
    public static final String PROPERTIES_SET = "neo4j.properties.set";
    public static final String ROWS_RETURNED = "neo4j.rows.returned";

    protected volatile Driver neo4JDriver;
    protected volatile String username;
    protected volatile String password;
    protected volatile String connectionUrl;
    protected volatile Integer port;
    protected volatile Config.LoadBalancingStrategy loadBalancingStrategy;

    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        super.beforeProcess(context);

        try {
            neo4JDriver = getDriver(context);
        } catch(Exception e) {
            getLogger().error("取得NEO4J连接时出现错误" + ExceptionUtils.getMessage(e));
            throw new ProcessException("取得NEO4J连接时出现错误：" + ExceptionUtils.getMessage(e));
        }
//        getLogger().info("成功创建NEO4J连接：{}",
//                new Object[] {connectionUrl});
    }

    @Override
    protected void afterProcess() {
//        getLogger().info("关闭NEO4J连接池及驱动。");
        if ( neo4JDriver != null ) {
            neo4JDriver.close();
            neo4JDriver = null;
        }

        super.afterProcess();
    }

    /**
     * 取得NEO4J驱动实例
     * @return Driver instance
     */
    protected Driver getNeo4JDriver() {
        return neo4JDriver;
    }

    protected Driver getDriver(ProcessContext context) {
        connectionUrl = context.getProperty(CONNECTION_URL).evaluateAttributeExpressions().getValue();
        username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        password = context.getProperty(PASSWORD).getValue();

        Config.ConfigBuilder configBuilder = Config.build();
        String loadBalancingStrategyValue = context.getProperty(LOAD_BALANCING_STRATEGY).getValue();
        if ( ! StringUtils.isBlank(loadBalancingStrategyValue) ) {
            configBuilder = configBuilder.withLoadBalancingStrategy(
                    Config.LoadBalancingStrategy.valueOf(loadBalancingStrategyValue));
        }

        configBuilder.withMaxConnectionPoolSize(context.getProperty(MAX_CONNECTION_POOL_SIZE).asInteger());

        configBuilder.withConnectionAcquisitionTimeout(context.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        configBuilder.withMaxConnectionLifetime(context.getProperty(MAX_CONNECTION_ACQUISITION_TIMEOUT).asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        configBuilder.withConnectionLivenessCheckTimeout(context.getProperty(IDLE_TIME_BEFORE_CONNECTION_TEST).asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        if ( context.getProperty(ENCRYPTION).asBoolean() ) {
            configBuilder.withEncryption();
        } else {
            configBuilder.withoutEncryption();
        }

        PropertyValue trustStrategy = context.getProperty(TRUST_STRATEGY);
        if ( trustStrategy.isSet() ) {
            if ( trustStrategy.getValue().equals(TRUST_CUSTOM_CA_SIGNED_CERTIFICATES.getValue())) {
                configBuilder.withTrustStrategy(Config.TrustStrategy.trustCustomCertificateSignedBy(new File(
                        context.getProperty(TRUST_CUSTOM_CA_SIGNED_CERTIFICATES_FILE).getValue())));
            } else if ( trustStrategy.getValue().equals(TRUST_SYSTEM_CA_SIGNED_CERTIFICATES.getValue())) {
                configBuilder.withTrustStrategy(Config.TrustStrategy.trustSystemCertificates());
            } else if ( trustStrategy.getValue().equals(TRUST_ALL_CERTIFICATES.getValue())) {
                configBuilder.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
            }
        }

        return GraphDatabase.driver( connectionUrl, AuthTokens.basic( username, password),
                configBuilder.toConfig());
    }
}
