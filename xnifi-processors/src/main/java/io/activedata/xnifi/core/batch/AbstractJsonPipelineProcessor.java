package io.activedata.xnifi.core.batch;

import com.alibaba.fastjson.JSON;
import io.activedata.xnifi.core.AbstractXNifiProcessor;
import io.activedata.xnifi.core.Input;
import io.activedata.xnifi.core.Output;
import io.activedata.xnifi.utils.FlowFileUtils;
import io.activedata.xnifi.utils.ScriptContextUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.components.AllowableValue;
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
 * 一次性可以读取多个FlowFile并进行处理的抽象处理器
 */
public abstract class AbstractJsonPipelineProcessor extends AbstractXNifiProcessor {
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("成功处理的FlowFile队列。")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("处理过程出现错误的FlowFile队列。")
            .build();

    public static final String WSS_BOTH_INPUT_AND_OUTPUT = "BOTH_INPUT_AND_OUTPUT";
    public static final String WSS_ONLY_OUTPUT = "ONLY_OUTPUT";

    public static final PropertyDescriptor PROP_WRITER_SCHEMA_STRATEGY = new PropertyDescriptor.Builder()
            .name("writer.schema.strategy")
            .displayName("输出生成策略")
            .description("请选择输出的生成策略，包括只输出Output、Input和Output都输出两种策略。")
            .defaultValue(WSS_BOTH_INPUT_AND_OUTPUT)
            .allowableValues(new AllowableValue(WSS_BOTH_INPUT_AND_OUTPUT, "输入和输出"), new AllowableValue(WSS_ONLY_OUTPUT, "只有输出"))
            .required(true)
            .build();

    public static final PropertyDescriptor PROP_RECORD_INPUT_BUILDER = new PropertyDescriptor.Builder()
            .name("input.builder")
            .displayName("输入记录构造脚本")
            .description("可使用MVEL脚本对输入记录input进行构造和处理，上下文包括attributes/input/$Strings/$Dates/$Numbers/$Booleans变量。")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_RECORD_OUTPUT_BUILDER = new PropertyDescriptor.Builder()
            .name("output.builder")
            .displayName("输出记录构造脚本")
            .description("，可使用MVEL脚本对输出记录output进行构造和处理，上下文包括attributes/input/output/$Strings/$Dates/$Numbers/$Booleans变量。")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch-size")
            .displayName("批处理大小")
            .description("批量读取多少个FlowFile进行处理")
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .defaultValue("100")
            .build();

    protected volatile int batchSize = 100;
    protected volatile Serializable inputBuilder;
    protected volatile Serializable outputBuilder;
    protected volatile String writerSchemaStrategy;

    protected Input buildInput(Input input, Map<String, String> attributes) {
        if (inputBuilder != null) {
            Map<String, Object> context = ScriptContextUtils.createInputContext(input, attributes);
            try {
                MVEL.executeExpression(inputBuilder, context);
            } catch (Exception e) {
                getLogger().debug("执行input.builder脚本时出现错误：" + ExceptionUtils.getStackTrace(e));
            }
        }

        return input;
    }

    protected Output buildOutput(Input input, Output output,
                                 Map<String, String> attributes) {
        if (outputBuilder != null) {
            Map<String, Object> context = ScriptContextUtils.createOutputContext(input, output, attributes);
            try {
                MVEL.executeExpression(outputBuilder, context);
            } catch (Exception e) {
                getLogger().debug("执行output.builder脚本时出现错误：" + ExceptionUtils.getStackTrace(e));
            }
        }

        return output;
    }


    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        String inputScript = context.getProperty(PROP_RECORD_INPUT_BUILDER).getValue();
        String outputScript = context.getProperty(PROP_RECORD_OUTPUT_BUILDER).getValue();
        batchSize = context.getProperty(PROP_BATCH_SIZE).asInteger();

        if (StringUtils.isNotBlank(inputScript)) {
            inputBuilder = MVEL.compileExpression(inputScript);
        }

        if (StringUtils.isNotBlank(outputScript)) {
            outputBuilder = MVEL.compileExpression(outputScript);
        }

        writerSchemaStrategy = context.getProperty(PROP_WRITER_SCHEMA_STRATEGY).getValue();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PROP_WRITER_SCHEMA_STRATEGY);
        properties.add(PROP_BATCH_SIZE);
        properties.add(PROP_RECORD_INPUT_BUILDER);
        properties.add(PROP_RECORD_OUTPUT_BUILDER);
        properties.addAll(super.getSupportedPropertyDescriptors());
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    protected Map<String, Object> createInputContext(Map<String, Object> input, Map<String, String> attributes) {
        return ScriptContextUtils.createInputContext(input, attributes);
    }

    @Override
    protected void process(ProcessContext context, ProcessSession session) throws ProcessException {
        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.size() == 0) {
            return;
        }

        for (FlowFile flowFile : flowFiles) {
            try {
                Map result = FlowFileUtils.readJson(session, flowFile, "UTF-8");
                Input input = new Input();
                input.putAll(result);
                Map<String, String> attrs = flowFile.getAttributes();
                Output output = processInternal(attrs, input, flowFile, context);
                FlowFile newFlowFile = FlowFileUtils.write(session, flowFile, JSON.toJSONString(output));
                session.transfer(newFlowFile, REL_SUCCESS);
            } catch (Throwable e) {
                ComponentLog logger = getLogger();
                if (logger.isDebugEnabled()) {
                    getLogger().error("处理数据时出现无法处理的错误[{}]。", new Object[]{ExceptionUtils.getStackTrace(e)});
                } else {
                    getLogger().error("处理数据时出现无法处理的错误[{}]。", new Object[]{ExceptionUtils.getMessage(e)});
                }
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    /**
     * 对数据进行处理
     *
     * @param attributes
     * @param input
     * @param original
     * @param context
     * @return
     * @throws ProcessException
     */
    protected Output processInternal(Map<String, String> attributes, Input input, FlowFile original, ProcessContext context) throws ProcessException {
        Output output = new Output();
        input = buildInput(input, attributes); // 对输入的Record进行处理和转换
        output = processJson(attributes, input, original, context);
        output = buildOutput(input, output, attributes);

        //当输出结构策略为BOTH_INPUT_AND_OUTPUT/CUSTOMIZE时，都需要对input和output进行合并
        if (!WSS_ONLY_OUTPUT.equals(writerSchemaStrategy)) {
            Output inheritResult = new Output();
            inheritResult.putAll(input);
            inheritResult.putAll(output);
            output = inheritResult;
        }

        return output;
    }

    /**
     * 对FlowFile中的JSON内容进行处理
     * @param attributes
     * @param input
     * @param flowFile
     * @param context
     * @return
     * @throws ProcessException
     */
    protected abstract Output processJson(Map<String, String> attributes, Input input, FlowFile flowFile, ProcessContext context) throws ProcessException;
}
