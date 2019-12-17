package io.activedata.xnifi.processors.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.activedata.xnifi.core.AbstractXNifiProcessor;
import jodd.bean.BeanUtil;
import okhttp3.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A base class for Elasticsearch processors that use the HTTP API
 */
public abstract class AbstractElasticsearchProcessor extends AbstractXNifiProcessor {

    static final String FIELD_INCLUDE_QUERY_PARAM = "_source_include";
    static final String QUERY_QUERY_PARAM = "q";
    static final String SORT_QUERY_PARAM = "sort";
    static final String SIZE_QUERY_PARAM = "size";


    public static final PropertyDescriptor ES_URL = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-url")
            .displayName("ES服务地址")
            .description("请指定ES服务地址，格式为http://host:port/形式。")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-proxy-host")
            .displayName("HTTP代理服务器（可选）")
            .description("请指定HTTP代理服务器的主机IP地址，格式为xxx.xxx.xxx.xxx形式。")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-proxy-port")
            .displayName("HTTP代理服务器端口（可选）")
            .description("请指定HTTP代理服务器端口，格式为数字。")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_USERNAME = new PropertyDescriptor.Builder()
            .name("proxy-username")
            .displayName("HTTP代理服务器用户名（可选）")
            .description("请指定HTTP代理服务器用户名。")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor PROXY_PASSWORD = new PropertyDescriptor.Builder()
            .name("proxy-password")
            .displayName("HTTP代理服务器密码（可选）")
            .description("请指定HTTP代理服务器密码。")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-connect-timeout")
            .displayName("ES服务连接超时时间")
            .description("请指定ES服务的最大连接等待时间，默认为5秒。")
            .required(true)
            .defaultValue("5 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor RESPONSE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("elasticsearch-http-response-timeout")
            .displayName("ES服务响应超时时间")
            .description("请指定ES服务的最大响应等待时间，默认为15秒。")
            .required(true)
            .defaultValue("15 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("TLS/SSL上下文服务（可选）")
            .description("请指定TLS/SSL上下文服务")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    protected static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("charset")
            .displayName("文档字符集")
            .description("请指定请求/响应文档的字符集，默认为UTF-8。")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("username")
            .description("ES服务用户名")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .description("ES服务密码")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private final AtomicReference<OkHttpClient> okHttpClientAtomicReference = new AtomicReference<>();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .dynamic(true)
                .build();
    }

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH, ProxySpec.SOCKS};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE
            = ProxyConfiguration.createProxyConfigPropertyDescriptor(true, PROXY_SPECS);
    static final List<PropertyDescriptor> COMMON_PROPERTY_DESCRIPTORS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ES_URL);
        properties.add(PROP_SSL_CONTEXT_SERVICE);
        properties.add(USERNAME);
        properties.add(PASSWORD);
        properties.add(CONNECT_TIMEOUT);
        properties.add(RESPONSE_TIMEOUT);
//        properties.add(PROXY_CONFIGURATION_SERVICE);
//        properties.add(PROXY_HOST);
//        properties.add(PROXY_PORT);
//        properties.add(PROXY_USERNAME);
//        properties.add(PROXY_PASSWORD);

        COMMON_PROPERTY_DESCRIPTORS = Collections.unmodifiableList(properties);
    }

    protected void createElasticsearchClient(ProcessContext context) throws ProcessException {
        okHttpClientAtomicReference.set(null);

        OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder();

        // Add a proxy if set
        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context, () -> {
            final String proxyHost = context.getProperty(PROXY_HOST).evaluateAttributeExpressions().getValue();
            final Integer proxyPort = context.getProperty(PROXY_PORT).evaluateAttributeExpressions().asInteger();
            if (proxyHost != null && proxyPort != null) {
                final ProxyConfiguration componentProxyConfig = new ProxyConfiguration();
                componentProxyConfig.setProxyType(Proxy.Type.HTTP);
                componentProxyConfig.setProxyServerHost(proxyHost);
                componentProxyConfig.setProxyServerPort(proxyPort);
                componentProxyConfig.setProxyUserName(context.getProperty(PROXY_USERNAME).evaluateAttributeExpressions().getValue());
                componentProxyConfig.setProxyUserPassword(context.getProperty(PROXY_PASSWORD).evaluateAttributeExpressions().getValue());
                return componentProxyConfig;
            }
            return ProxyConfiguration.DIRECT_CONFIGURATION;
        });

        if (!Proxy.Type.DIRECT.equals(proxyConfig.getProxyType())) {
            final Proxy proxy = proxyConfig.createProxy();
            okHttpClient.proxy(proxy);

            if (proxyConfig.hasCredential()) {
                okHttpClient.proxyAuthenticator(new Authenticator() {
                    @Override
                    public Request authenticate(Route route, Response response) throws IOException {
                        final String credential = Credentials.basic(proxyConfig.getProxyUserName(), proxyConfig.getProxyUserPassword());
                        return response.request().newBuilder()
                                .header("Proxy-Authorization", credential)
                                .build();
                    }
                });
            }
        }


        // Set timeouts
        okHttpClient.connectTimeout((context.getProperty(CONNECT_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue()), TimeUnit.MILLISECONDS);
        okHttpClient.readTimeout(context.getProperty(RESPONSE_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS);

        final SSLContextService sslService = context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final SSLContext sslContext = sslService == null ? null : sslService.createSSLContext(SSLContextService.ClientAuth.NONE);

        // check if the ssl context is set and add the factory if so
        if (sslContext != null) {
            okHttpClient.sslSocketFactory(sslContext.getSocketFactory());
        }

        okHttpClientAtomicReference.set(okHttpClient.build());
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        if (validationContext.getProperty(PROXY_HOST).isSet() != validationContext.getProperty(PROXY_PORT).isSet()) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .explanation("Proxy Host and Proxy Port must be both set or empty")
                    .subject("Proxy server configuration")
                    .build());
        }

        ProxyConfiguration.validateProxySpec(validationContext, results, PROXY_SPECS);

        return results;
    }

    protected OkHttpClient getClient() {
        return okHttpClientAtomicReference.get();
    }

    protected boolean isSuccess(int statusCode) {
        return statusCode / 100 == 2;
    }

    protected Response sendRequestToElasticsearch(OkHttpClient client, URL url, String username, String password, String verb, RequestBody body) throws IOException {

        final ComponentLog log = getLogger();
        Request.Builder requestBuilder = new Request.Builder()
                .url(url);
        if ("get".equalsIgnoreCase(verb)) {
            requestBuilder = requestBuilder.get();
        } else if ("put".equalsIgnoreCase(verb)) {
            requestBuilder = requestBuilder.put(body);
        } else {
            throw new IllegalArgumentException("不支持的ES REST API verb: " + verb);
        }

        if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
            String credential = Credentials.basic(username, password);
            requestBuilder = requestBuilder.header("Authorization", credential);
        }
        Request httpRequest = requestBuilder.build();
        log.debug("发送请求到ES服务器{}。", new Object[]{url});

        Response responseHttp = client.newCall(httpRequest).execute();

        // store the status code and message
        int statusCode = responseHttp.code();

        if (statusCode == 0) {
            throw new IllegalStateException("Status code unknown, connection hasn't been attempted.");
        }

        log.debug("Received response from Elasticsearch with status code {}", new Object[]{statusCode});

        return responseHttp;
    }

    protected JsonNode parseJsonResponse(InputStream in) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(in);
    }

    /**
     * 创建批处理命令
     *
     * @param sb
     * @param index
     * @param docType
     * @param indexOp
     * @param idAttr
     * @param rows
     */
    protected void buildCmdBatch(StringBuilder sb, String index, String docType, String indexOp, String idAttr, List<Map> rows) {
        for (Map row : rows) {
            String id = Objects.toString(BeanUtil.silent.getProperty(row, idAttr), "");
            if ("index".equalsIgnoreCase(indexOp)) {
                buildIndexCmd(sb, index, docType, indexOp, id, row);
            } else if ("update".equalsIgnoreCase(indexOp) || "upsert".equalsIgnoreCase(indexOp)) {
                buildUpdateCmd(sb, index, docType, indexOp, id, row);
            } else if ("delete".equalsIgnoreCase(indexOp)) {
                buildDeleteCmd(sb, index, docType, indexOp, id, row);
            }
        }
    }

    /**
     * 创建插入索引的命令，格式如下：
     * { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }\n
     * { "field1" : "value1" }\n
     *
     * @param sb
     * @param index
     * @param docType
     * @param indexOp
     * @param id
     * @param row
     */
    private void buildIndexCmd(StringBuilder sb, String index, String docType, String indexOp, String id, Map row) {
        sb.append("{\"index\": { \"_index\": \"");
        sb.append(StringEscapeUtils.escapeJson(index));
        sb.append("\", \"_type\": \"");
        sb.append(StringEscapeUtils.escapeJson(docType));
        sb.append("\"");
        if (!StringUtils.isEmpty(id)) {
            sb.append(", \"_id\": \"");
            sb.append(StringEscapeUtils.escapeJson(id));
            sb.append("\"");
        }
        sb.append("}}\n");
        sb.append(JSON.toJSONString(row) + "\n");
    }

    /**
     * 创建更新或者插入更新的索引的命令，格式如下：
     * { "update" : {"_id" : "1", "_type" : "type1", "_index" : "test"} }\n
     * { "doc" : {"field2" : "value2", "doc_as_upsert":false} }\n
     * {"update" : {"_id" : "1", "_type" : "type1", "_index" : "test"} }\n
     * { "doc" : {"field2" : "value2", "doc_as_upsert":true} }\n
     *
     * @param sb
     * @param index
     * @param docType
     * @param indexOp
     * @param id
     * @param row
     */
    private void buildUpdateCmd(StringBuilder sb, String index, String docType, String indexOp, String id, Map row) {
        sb.append("{\"update\": { \"_index\": \"");
        sb.append(StringEscapeUtils.escapeJson(index));
        sb.append("\", \"_type\": \"");
        sb.append(StringEscapeUtils.escapeJson(docType));
        sb.append("\", \"_id\": \"");
        sb.append(StringEscapeUtils.escapeJson(id));
        sb.append("\" }\n");
        sb.append("{\"doc\": ");
        sb.append(JSON.toJSONString(row));
        sb.append(", \"doc_as_upsert\": ");
        sb.append(indexOp.equalsIgnoreCase("upsert"));
        sb.append(" }\n");
    }

    /**
     * 创建删除命令，格式如下：
     * { "delete" : { "_index" : "test", "_type" : "type1", "_id" : "2" } }
     * @param sb
     * @param index
     * @param docType
     * @param indexOp
     * @param id
     * @param row
     */
    private void buildDeleteCmd(StringBuilder sb, String index, String docType, String indexOp, String id, Map row) {
        sb.append("{\"delete\": { \"_index\": \"");
        sb.append(StringEscapeUtils.escapeJson(index));
        sb.append("\", \"_type\": \"");
        sb.append(StringEscapeUtils.escapeJson(docType));
        sb.append("\", \"_id\": \"");
        sb.append(StringEscapeUtils.escapeJson(id));
        sb.append("\" }\n");
    }
}

