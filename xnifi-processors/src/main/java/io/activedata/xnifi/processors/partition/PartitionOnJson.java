package io.activedata.xnifi.processors.partition;

import io.activedata.xnifi.core.AbstractXNifiProcessor;
import io.activedata.xnifi.utils.FlowFileUtils;
import io.activedata.xnifi.utils.ScriptContextUtils;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.collections.map.MultiValueMap;
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
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.*;

/**
 *
 */
@TriggerSerially
@SupportsBatching
@Tags({ "partition", "json"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("对多个FLowFile中的JSON记录进行重组分区")
public class PartitionOnJson extends AbstractXNifiProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("成功处理的数据流队列。")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("处理过程出现错误的数据流队列。")
            .build();

    public static final PropertyDescriptor PROP_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch-size")
            .displayName("批处理大小")
            .description("每次读取多少个FlowFile进行处理")
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor PROP_PARTITION_EXPR = new PropertyDescriptor.Builder()
            .name("partition-expr")
            .displayName("分区表达式")
            .description("使用MVEL表达式计算JSON ROW的分区值，如input.?attribute")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final String PATITION_KEY_DEFAULT = "default";
    private static final String ATTR_NAME_PATITION = "partition";
    private static final String ATTR_NAME_ROW_COUNT = "row.count";


    protected volatile int batchSize = 1;

    protected volatile Serializable patitionExpr;

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PROP_BATCH_SIZE);
        props.add(PROP_PARTITION_EXPR);
        return props;
    }

    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        batchSize = context.getProperty(PROP_BATCH_SIZE).asInteger();
        String patitionExprText = context.getProperty(PROP_PARTITION_EXPR).getValue();
        patitionExpr = MVEL.compileExpression(patitionExprText);
        super.beforeProcess(context);
    }

    @Override
    protected void process(ProcessContext context, ProcessSession session) throws ProcessException {
        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.size() == 0) {
            return;
        }

        MultiValueMap patitionMap = new MultiValueMap();

        for (FlowFile flowFile : flowFiles) {
            try {
                final Map<String, String> attributes = createAttributes(context, flowFile);
                List<Map> rows = FlowFileUtils.readJsonToList(session, flowFile);
                for (Map row : rows) {
                    Map<String, Object> inputContext = ScriptContextUtils.createInputContext(row, attributes);
                    Object result = MVEL.executeExpression(patitionExpr, inputContext);
                    String patitionKey = Objects.toString(result, "default");
                    patitionMap.put(patitionKey, row);
                }
                session.remove(flowFile);
            }catch (Throwable e){
                ComponentLog logger = getLogger();
                if (logger.isDebugEnabled()) {
                    getLogger().error("分区JSON ROW时出现无法处理的错误[{}]。", new Object[]{ExceptionUtils.getStackTrace(e)});
                } else {
                    getLogger().error("分区JSON ROW时出现无法处理的错误[{}]。", new Object[]{ExceptionUtils.getMessage(e)});
                }
                session.transfer(flowFile, REL_FAILURE);
            }
        }

        for (Object key : patitionMap.keySet()){
            String patitionKey = Objects.toString(key, PATITION_KEY_DEFAULT);
            Collection rows = patitionMap.getCollection(patitionKey);
            if (rows != null && rows.size() > 0) {
                List patitionRows = new ArrayList(rows);
                FlowFile patitionFlowFile = session.create();
                FlowFileUtils.writeListToJson(session, patitionFlowFile, patitionRows);
                session.putAttribute(patitionFlowFile, ATTR_NAME_PATITION, patitionKey);
                session.putAttribute(patitionFlowFile, ATTR_NAME_ROW_COUNT, String.valueOf(patitionRows.size()));
                session.transfer(patitionFlowFile, REL_SUCCESS);
            }
        }
    }

    /**
     * 创建新的FlowFile属性Map，里面包含原有属性值和动态属性值
     * @param context
     * @param flowFile
     * @return
     */
    protected Map<String, String> createAttributes(ProcessContext context, FlowFile flowFile){
        Map<String, String> attributes = new HashMap<>();
        attributes.putAll(flowFile.getAttributes());
        attributes.putAll(FlowFileUtils.getDynamicProperties(context)); //添加动态属性
        return attributes;
    }
}
