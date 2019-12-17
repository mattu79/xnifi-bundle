package io.activedata.xnifi.core.batch;

import io.activedata.xnifi.core.AbstractXNifiProcessor;
import io.activedata.xnifi.core.FailureOutput;
import io.activedata.xnifi.core.Input;
import io.activedata.xnifi.core.Output;
import io.activedata.xnifi.exceptions.BizException;
import io.activedata.xnifi.exceptions.RetrieableException;
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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.Tuple;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.*;

/**
 * 抽象的批处理处理器，引入了ProcessCallback机制；同时为了便于Callback逻辑的实现，该Processor实现了CallbackHandler接口
 *
 * @author MattU
 */
public abstract class AbstractBatchProcessor extends AbstractXNifiProcessor implements CallbackHandler {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("成功处理的FlowFile队列。")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("处理过程出现错误的FlowFile队列。")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("可尝试重新处理的FlowFile队列。")
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
            .description("可使用MVEL脚本对输出记录output进行构造和处理，上下文包括attributes/input/output/$Strings/$Dates/$Numbers/$Booleans变量。")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();


    protected volatile Serializable inputBuilder;
    protected volatile Serializable outputBuilder;
    protected volatile String writerSchemaStrategy;

    protected Input buildInput(Input input, Map<String, String> attributes) {
        if (inputBuilder != null) {
            Map<String, Object> context = ScriptContextUtils.createInputContext(input, attributes);
            try {
                MVEL.executeExpression(inputBuilder, context);
            }catch (Exception e){
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
            }catch (Exception e){
                getLogger().debug("执行output.builder脚本时出现错误：" + ExceptionUtils.getStackTrace(e));
            }
        }

        return output;
    }

    @Override
    protected final void process(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            InputStreamCallback callback = createCallback(this, getLogger(), flowFile, context, session);
            session.read(flowFile, callback);
            session.remove(flowFile);
        }catch (ProcessException e){
            throw e;
        }catch (Throwable e){
            throw new ProcessException("处理FlowFile时出现未知错误：" + ExceptionUtils.getStackTrace(e));
        }
    }

    /**
     * 创建一个能够处理输入的回调
     * @param processor
     * @param logger
     * @param original
     * @param context
     * @param session
     * @return
     */
    protected abstract InputStreamCallback createCallback(AbstractBatchProcessor processor, ComponentLog logger, FlowFile original, ProcessContext context, ProcessSession session);

    /**
     *  对CallbackHandler进行包装
     * @param attributes
     * @param input
     * @param original
     * @param context
     * @return
     * @throws ProcessException
     */
    public final Tuple<Relationship, Output> handleProcess(Map<String, String> attributes, Input input, FlowFile original, ProcessContext context) throws ProcessException {
        Output output = new Output();
        input = buildInput(input, attributes); // 对输入的Record进行处理和转换
        try {
            output = handleProcessRow(attributes, input, original, context);
        } catch (BizException e) {  //自定义错误,不可重新运行错误
            String errorMessage = ExceptionUtils.getStackTrace(e);
            getLogger().warn("处理数据时出现错误，转移到失败队列：" + errorMessage);
            String processorId = getIdentifier();
            String processorName = getName();
            FailureOutput failureOutput = new FailureOutput(processorId, processorName, input, errorMessage);
            return new Tuple<>(REL_FAILURE, failureOutput);
        } catch (RetrieableException e) {   //自定义错误,可以重新运行的错误
            String errorMessage = ExceptionUtils.getStackTrace(e);
            getLogger().warn("处理数据时出现错误，转移到重试队列：" + errorMessage);
            String processorId = getIdentifier();
            String processorName = getName();
            FailureOutput failureOutput = new FailureOutput(processorId, processorName, input, errorMessage);
            return new Tuple<>(REL_RETRY, failureOutput);
        } catch (Exception e) {
            throw e;
        } finally {
            output = buildOutput(input, output, attributes);
        }

        //当输出结构策略为BOTH_INPUT_AND_OUTPUT/CUSTOMIZE时，都需要对input和output进行合并
        if (!WSS_ONLY_OUTPUT.equals(writerSchemaStrategy)) {
            Output inheritResult = new Output();
            inheritResult.putAll(input);
            inheritResult.putAll(output);
            output = inheritResult;
        }

        return new Tuple<>(REL_SUCCESS, output);
    }

    /**
     * 对每条记录进行处理，如果处理过程中出现错误可抛出异常，后续会根据异常类型对记录进行分别处理
     * BizException：该记录会被路由到错误的队列中
     * RetrieableException：该记录会被路由到重试队列中
     * 其他Exception：往整个处理器抛出异常，可能导致处理器停止
     * @param attributes
     * @param input
     * @param flowFile
     * @param context
     * @return
     * @throws ProcessException
     */
    protected abstract Output handleProcessRow(Map<String, String> attributes, Input input, FlowFile flowFile, ProcessContext context) throws ProcessException;

    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        String inputScript = context.getProperty(PROP_RECORD_INPUT_BUILDER).getValue();
        String outputScript = context.getProperty(PROP_RECORD_OUTPUT_BUILDER).getValue();

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
        relationships.add(REL_RETRY);
        return relationships;
    }

    protected final Object execExpr(Serializable compiledExpr, Map<String, Object> input,Map<String, String> attributes) {
        if (compiledExpr != null) {
            Map<String, Object> context = createInputContext(input, attributes);

            Object result = MVEL.executeExpression(compiledExpr, context);
            return result;
        }
        return null;
    }

    protected Map<String, Object> createInputContext(Map<String, Object> input, Map<String, String> attributes) {
        return ScriptContextUtils.createInputContext(input, attributes);
    }
}
