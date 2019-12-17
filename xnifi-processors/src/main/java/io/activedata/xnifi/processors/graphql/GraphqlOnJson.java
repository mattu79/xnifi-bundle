package io.activedata.xnifi.processors.graphql;

import io.activedata.xnifi.core.Input;
import io.activedata.xnifi.core.Output;
import io.activedata.xnifi.core.batch.AbstractJsonRowBatchProcessor;
import io.activedata.xnifi.exceptions.BizException;
import io.activedata.xnifi.exceptions.InvalidEnvironmentException;
import io.activedata.xnifi.expression.Strings;
import io.activedata.xnifi.utils.ScriptContextUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.mvel2.MVEL;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@TriggerSerially
@SupportsBatching
@Tags({"graphql", "json"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("将JSON记录作为参数执行GraphQL查询")
public class GraphqlOnJson extends AbstractJsonRowBatchProcessor {

    private static final String KEY_VARIABLES = "variables";
    private static final String KEY_HEADERS = "headers";

    private GraphqlClient client = new GraphqlClient();

    public static PropertyDescriptor PROP_GRAPHQL_URL = new PropertyDescriptor.Builder()
            .name("graphql.url")
            .displayName("GraphQL服务地址")
            .description("GraphQL服务地址，格式为http://host:port/graphql")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static PropertyDescriptor PROP_GRAPHQL_PAYLOAD = new PropertyDescriptor.Builder()
            .name("graphql.payload")
            .displayName("GraphQL请求内容")
            .description("GraphQL请求内容")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_GRAPHQL_VARIABLES_BUILDER = new PropertyDescriptor.Builder()
            .name("variables.builder")
            .displayName("GraphQL请求参数（variables/headers）构造脚本")
            .description("可使用MVEL脚本对GraphQL请求参数（variables/headers）进行构造和处理，上下文包括attributes/input/output/$Strings/$Dates/$Numbers/$Booleans变量。")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<PropertyDescriptor>();
        props.add(PROP_GRAPHQL_URL);
        props.add(PROP_GRAPHQL_PAYLOAD);
        props.add(PROP_GRAPHQL_VARIABLES_BUILDER);
        props.add(PROP_WRITER_SCHEMA_STRATEGY);
        props.add(PROP_RECORD_INPUT_BUILDER);
        props.add(PROP_RECORD_OUTPUT_BUILDER);
        return props;
    }

    protected volatile Serializable variablesBuilder;

    protected volatile String url;

    protected volatile String payload;

    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        super.beforeProcess(context);

        String variablesScript = context.getProperty(PROP_GRAPHQL_VARIABLES_BUILDER).getValue();

        if (StringUtils.isNotBlank(variablesScript)) {
            variablesBuilder = MVEL.compileExpression(variablesScript);
        }

        url = context.getProperty(PROP_GRAPHQL_URL).getValue();
        payload = context.getProperty(PROP_GRAPHQL_PAYLOAD).getValue();
    }

    @Override
    protected Output handleProcessRow(Map<String, String> attributes, Input input, FlowFile flowFile, ProcessContext context) throws ProcessException {
        Map<String, Object> variables = new HashMap<>();
        Map<String, String> headers = new HashMap<>();
        buildVariables(input, attributes, variables, headers); // 执行VariablesBuilder脚本对variables和headers进行计算

        Output output = new Output();
        try {
            Map<String, Object> results = client.request(url, payload, variables, null, headers);
            Object errors = results.get("errors");
            ComponentLog logger = getLogger();

            if (errors != null){
                List<Map> errorList = (List<Map>) errors;
                StringBuilder sb = new StringBuilder();
                for (Map error : errorList){
                    sb.append(Strings.toString(error.get("message")) + "\r\n");
                }
                throw new BizException("执行GraphQL请求时出现错误：" + sb.toString());
            }else {
                output.putAll(results);
            }
        } catch (IOException e) {
            throw new InvalidEnvironmentException("无法调用GraphQL服务[" + url + "]：" + ExceptionUtils.getMessage(e));
        } catch (Throwable e) {
            throw new BizException("调用GraphQL服务[" + url + "]时出现错误：" + ExceptionUtils.getMessage(e));
        }

        return output;
    }

    /**
     * 构造GraphQL variables和headers
     *
     * @param input
     * @param attributes
     * @return
     */
    private void buildVariables(Input input, Map<String, String> attributes, Map<String, Object> variables, Map<String, String> headers) {
        if (variablesBuilder != null) {
            Map<String, Object> context = ScriptContextUtils.createInputContext(input, attributes); // 初始化variables上下文
            context.put(KEY_VARIABLES, variables); // 初始化variables上下文
            context.put(KEY_HEADERS, headers); // 初始化headers上下文
            try {
                MVEL.executeExpression(variablesBuilder, context);
            } catch (Exception e) {
                getLogger().warn("执行variables.builder脚本时出现错误：" + ExceptionUtils.getStackTrace(e));
            }
        }
    }
}
