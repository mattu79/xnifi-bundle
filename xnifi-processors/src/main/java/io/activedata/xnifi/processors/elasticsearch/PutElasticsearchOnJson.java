package io.activedata.xnifi.processors.elasticsearch;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.activedata.xnifi.utils.FlowFileUtils;
import okhttp3.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"elasticsearch", "insert", "update", "upsert", "delete", "write", "put", "http", "json"})
@CapabilityDescription("读取FlowFile中的JSON ROW并插入到ES索引表中。")
@DynamicProperty(
        name = "A URL query parameter",
        value = "The value to set it to",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
        description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutElasticsearchOnJson extends AbstractElasticsearchProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("处理成功的FlowFile队列。")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("处理失败的FlowFile队列。")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("可尝试重新处理的FlowFile队列。")
            .build();

    public static final PropertyDescriptor ID_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("put-es-id-attr")
            .displayName("ID属性（可选）")
            .description("能够确定唯一一条JSON ROW的属性文档属性，如果不设置，则会直接有ES生成。")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("put-es-index")
            .displayName("index文档索引名称")
            .description("请指定需要插入的文档索引名称")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
                    AttributeExpression.ResultType.STRING, true))
            .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("put-es-type")
            .displayName("type文档类型")
            .description("请指定插入的文档类型。")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX_OP = new PropertyDescriptor.Builder()
            .name("put-es-index-op")
            .displayName("索引处理类型")
            .description("请指定索引处理类型，包括index, update, upsert, delete四种方式。")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .defaultValue("index")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("put-es-batch-size")
            .displayName("批处理大小")
            .description("请指定一次批量处理的FlowFile数量。")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        final Set<Relationship> _rels = new HashSet<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        _rels.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_rels);

        final List<PropertyDescriptor> props = new ArrayList<>(COMMON_PROPERTY_DESCRIPTORS);
        props.add(ID_ATTRIBUTE);
        props.add(INDEX);
        props.add(TYPE);
        props.add(CHARSET);
        props.add(BATCH_SIZE);
        props.add(INDEX_OP);

        propertyDescriptors = Collections.unmodifiableList(props);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));
        // Since Expression Language is allowed for index operation, we can't guarantee that we can catch
        // all invalid configurations, but we should catch them as soon as we can. For example, if the
        // Identifier Attribute property is empty, the Index Operation must evaluate to "index".
        String idAttribute = validationContext.getProperty(ID_ATTRIBUTE).getValue();
        String indexOp = validationContext.getProperty(INDEX_OP).getValue();

        if (StringUtils.isEmpty(idAttribute)) {
            switch (indexOp.toLowerCase()) {
                case "update":
                case "upsert":
                case "delete":
                case "":
                    problems.add(new ValidationResult.Builder()
                            .valid(false)
                            .subject(INDEX_OP.getDisplayName())
                            .explanation("If Identifier Attribute is not set, Index Operation must evaluate to \"index\"")
                            .build());
                    break;
                default:
                    break;
            }
        }
        return problems;
    }

    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        super.beforeProcess(context);
        createElasticsearchClient(context);
    }

    protected void process(ProcessContext context, ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();

        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }

        final String idAttr = context.getProperty(ID_ATTRIBUTE).getValue();

        // Authentication
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        final String baseUrl = context.getProperty(ES_URL).evaluateAttributeExpressions().getValue().trim();
        if (StringUtils.isEmpty(baseUrl)) {
            throw new ProcessException("ES服务地址为空。");
        }


        OkHttpClient okHttpClient = getClient();
        final ComponentLog logger = getLogger();

        // Keep track of the list of flow files that need to be transferred. As they are transferred, remove them from the list.
        List<FlowFile> flowFilesToTransfer = new LinkedList<>(flowFiles);

        HttpUrl.Builder urlBuilder = HttpUrl.parse(baseUrl).newBuilder().addPathSegment("_bulk");

        // Find the user-added properties and set them as query parameters on the URL
        for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
            PropertyDescriptor pd = property.getKey();
            if (pd.isDynamic()) {
                if (property.getValue() != null) {
                    urlBuilder = urlBuilder.addQueryParameter(pd.getName(), context.getProperty(pd).evaluateAttributeExpressions().getValue());
                }
            }
        }
        final URL url = urlBuilder.build().url();
        final StringBuilder sb = new StringBuilder();

        for (FlowFile file : flowFiles) {
            final String index = context.getProperty(INDEX).evaluateAttributeExpressions(file).getValue();
            final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(file).getValue());
            if (StringUtils.isEmpty(index)) {
                logger.error("文档索引名称不能为 {}, 该FlowFile将会被放入失败队列。", new Object[]{idAttr, file});
                flowFilesToTransfer.remove(file);
                session.transfer(file, REL_FAILURE);
                continue;
            }
            final String docType = context.getProperty(TYPE).evaluateAttributeExpressions(file).getValue();
            String indexOp = context.getProperty(INDEX_OP).evaluateAttributeExpressions(file).getValue();

            switch (StringUtils.lowerCase(indexOp)) {
                case "index":
                case "update":
                case "upsert":
                case "delete":
                    break;
                default:
                    logger.error("索引处理类型有错 {}, 该FlowFile将会被放入失败队列。", new Object[]{file});
                    flowFilesToTransfer.remove(file);
                    session.transfer(file, REL_FAILURE);
                    continue;
            }

            // The ID must be valid for all operations except "index". For that case,
            // a missing ID indicates one is to be auto-generated by Elasticsearch
            if (idAttr == null && !indexOp.equalsIgnoreCase("index")) {
                logger.error("索引处理类型{}未设置ID属性, 该FlowFile将会被放入失败队列。",
                        new Object[]{indexOp, file});
                flowFilesToTransfer.remove(file);
                session.transfer(file, REL_FAILURE);
                continue;
            }

            try {
                List<Map> rows = FlowFileUtils.readJsonToList(session, file);
                buildCmdBatch(sb, index, docType, indexOp, idAttr, rows);
            } catch (Throwable e) {
                logger.error("FlowFile中包含无效的JSON格式, 该FlowFile将会被放入失败队列并进行惩罚。",
                        new Object[]{indexOp, file});
                flowFilesToTransfer.remove(file);
                file = session.penalize(file);
                session.transfer(file, REL_FAILURE);
                continue;
            }

        }

        if (!flowFilesToTransfer.isEmpty()) {
            RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), sb.toString());
            final Response getResponse;
            try {
                getResponse = sendRequestToElasticsearch(okHttpClient, url, username, password, "PUT", requestBody);
            } catch (final Exception e) {
                if (logger.isDebugEnabled()){
                    logger.error("路由FlowFile时出现错误：" + ExceptionUtils.getStackTrace(e));
                }else{
                    logger.error("路由FlowFile时出现错误：" + ExceptionUtils.getMessage(e));
                }
                flowFilesToTransfer.forEach((flowFileToTransfer) -> {
                    flowFileToTransfer = session.penalize(flowFileToTransfer);
                    session.transfer(flowFileToTransfer, REL_FAILURE);
                });
                flowFilesToTransfer.clear();
                return;
            }

            final int statusCode = getResponse.code();

            if (isSuccess(statusCode)) {
                ResponseBody responseBody = getResponse.body();
                try {
                    final byte[] bodyBytes = responseBody.bytes();

                    JsonNode responseJson = parseJsonResponse(new ByteArrayInputStream(bodyBytes));
                    boolean errors = responseJson.get("errors").asBoolean(false);
                    if (errors) {
                        ArrayNode itemNodeArray = (ArrayNode) responseJson.get("items");
                        if (itemNodeArray.size() > 0) {
                            // All items are returned whether they succeeded or failed, so iterate through the item array
                            // at the same time as the flow file list, moving each to success or failure accordingly,
                            // but only keep the first error for logging
                            String errorReason = null;
                            for (int i = itemNodeArray.size() - 1; i >= 0; i--) {
                                JsonNode itemNode = itemNodeArray.get(i);
                                if (flowFilesToTransfer.size() > i) {
                                    FlowFile flowFile = flowFilesToTransfer.remove(i);
                                    int status = itemNode.findPath("status").asInt();
                                    if (!isSuccess(status)) {
                                        if (errorReason == null) {
                                            // Use "result" if it is present; this happens for status codes like 404 Not Found, which may not have an error/reason
                                            String reason = itemNode.findPath("result").asText();
                                            if (StringUtils.isEmpty(reason)) {
                                                // If there was no result, we expect an error with a string description in the "reason" field
                                                reason = itemNode.findPath("reason").asText();
                                            }
                                            errorReason = reason;
                                            logger.error("Failed to process {} due to {}, transferring to failure",
                                                    new Object[]{flowFile, errorReason});
                                        }
                                        flowFile = session.penalize(flowFile);
                                        session.transfer(flowFile, REL_FAILURE);

                                    } else {
                                        session.transfer(flowFile, REL_SUCCESS);
                                        // Record provenance event
                                        session.getProvenanceReporter().send(flowFile, url.toString());
                                    }
                                }
                            }
                        }
                    }
                    // Transfer any remaining flowfiles to success
                    flowFilesToTransfer.forEach(file -> {
                        session.transfer(file, REL_SUCCESS);
                        // Record provenance event
                        session.getProvenanceReporter().send(file, url.toString());
                    });
                } catch (IOException ioe) {
                    // Something went wrong when parsing the response, log the error and route to failure
                    logger.error("Error parsing Bulk API response: {}", new Object[]{ioe.getMessage()}, ioe);
                    session.transfer(flowFilesToTransfer, REL_FAILURE);
                    context.yield();
                }
            } else if (statusCode / 100 == 5) {
                // 5xx -> RETRY, but a server error might last a while, so yield
                logger.warn("Elasticsearch returned code {} with message {}, transferring flow file to retry. This is likely a server problem, yielding...",
                        new Object[]{statusCode, getResponse.message()});
                session.transfer(flowFilesToTransfer, REL_RETRY);
                context.yield();
            } else {  // 1xx, 3xx, 4xx, etc. -> NO RETRY
                logger.warn("Elasticsearch returned code {} with message {}, transferring flow file to failure", new Object[]{statusCode, getResponse.message()});
                session.transfer(flowFilesToTransfer, REL_FAILURE);
            }
            getResponse.close();
        }
    }
}

