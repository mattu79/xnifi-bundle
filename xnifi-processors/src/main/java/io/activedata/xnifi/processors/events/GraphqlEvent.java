package io.activedata.xnifi.processors.events;

import io.activedata.xnifi.core.Input;
import io.activedata.xnifi.core.Output;
import io.activedata.xnifi.core.batch.AbstractJsonPipelineProcessor;
import io.activedata.xnifi.processors.events.utils.GraphqlParserUtils;
import jodd.bean.BeanUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@TriggerSerially
@SupportsBatching
@Tags({"graphql", "event", "json"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("对Graphql事件进行解析和处理")
public class GraphqlEvent extends AbstractJsonPipelineProcessor {

    private static final String[] COPYED_INPUT_PROPS = new String[]{"@timestamp", "pid", "source", "offset", "category", "name", "ip", "id", "trackId", "username", "uuid",
            "uid", "hostname", "use", "tags", "atTime", "date"};

    public static final PropertyDescriptor PROP_PAYLOAD = new PropertyDescriptor.Builder()
            .name("payload")
            .displayName("请求体属性名")
            .description("Graphql请求体的属性名")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("requestPayload")
            .build();

    public static final PropertyDescriptor PROP_VARIABLES = new PropertyDescriptor.Builder()
            .name("variables")
            .displayName("变量属性名")
            .description("Graphql请求变量的属性名")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("requestVariables")
            .build();
    public static final String ATTR_RESP_TYPE = "respType";
    public static final String ATTR_RESP_LENGTH = "respLength";
    public static final String ATTR_RESP_STATUS = "respStatus";
    public static final String ATTR_TYPE = "type";
    public static final String ATTR_ATTRS = "attrs";

    private volatile String attrPayload;
    private volatile String attrVariables;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PROP_PAYLOAD);
        properties.add(PROP_VARIABLES);
        properties.add(PROP_BATCH_SIZE);
        properties.add(PROP_RECORD_INPUT_BUILDER);
        properties.add(PROP_RECORD_OUTPUT_BUILDER);
        return properties;
    }

    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        super.beforeProcess(context);
        writerSchemaStrategy = WSS_ONLY_OUTPUT; // 对Output策略进行重定义，只输出Output的内容，Input中的数据
        attrPayload = context.getProperty(PROP_PAYLOAD).getValue();
        attrVariables = context.getProperty(PROP_VARIABLES).getValue();
    }

    @Override
    protected Output processJson(Map<String, String> attributes, Input input, FlowFile flowFile, ProcessContext context) throws ProcessException {
        Output output = new Output();
        for (String propName : COPYED_INPUT_PROPS) {
            output.put(propName, Objects.toString(input.get(propName), "")); //复制属性
        }

        Object attrs = input.get(ATTR_ATTRS);
        if (attrs instanceof Map) {
            Map attrsMap = (Map) attrs;

            BeanUtil bu = BeanUtil.silent;
            String payload = Objects.toString(bu.getProperty(attrsMap, attrPayload), "");
            String variables = Objects.toString(bu.getProperty(attrsMap, attrVariables), "");

            if (StringUtils.isNotBlank(payload)) {
                Map<String, Object> req = GraphqlParserUtils.parseRequest(payload, variables); // Graphql请求不为空时对请求进行解析
                output.putAll(req); // req中包含type、name、params三个字段
            }else{
                output.put(ATTR_TYPE, "PROFILE"); //为了和Graphql解析出来的结果兼容增加了type属性，name和category属性在profile事件中已有可以不考虑，params可为null
            }

            output.put(ATTR_RESP_TYPE, Objects.toString(attrsMap.get("respType"), ""));
            output.put(ATTR_RESP_LENGTH, Objects.toString(attrsMap.get("respLength"), ""));
            output.put(ATTR_RESP_STATUS, Objects.toString(attrsMap.get("respStatus"), ""));


            Map attrsMapRemain = new LinkedHashMap(attrsMap); // 将attrs中剩下的属性值放入到output中
            attrsMapRemain.remove(attrPayload);
            attrsMapRemain.remove(attrVariables);
            attrsMapRemain.remove(ATTR_RESP_TYPE);
            attrsMapRemain.remove(ATTR_RESP_LENGTH);
            attrsMapRemain.remove(ATTR_RESP_STATUS);
            output.put(ATTR_ATTRS, attrsMapRemain);
        }



        return output;
    }


}
