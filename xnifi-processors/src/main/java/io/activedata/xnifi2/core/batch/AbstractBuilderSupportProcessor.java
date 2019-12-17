package io.activedata.xnifi2.core.batch;

import io.activedata.xnifi.utils.ScriptContextUtils;
import io.activedata.xnifi2.core.validators.MvelScriptValidator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.Tuple;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.Map;

public abstract class AbstractBuilderSupportProcessor extends AbstractBatchProcessor {

    public static final PropertyDescriptor PROP_RECORD_INPUT_BUILDER = new PropertyDescriptor.Builder()
            .name("input.builder")
            .displayName("输入记录构造脚本")
            .description("可使用MVEL脚本对输入记录input进行构造和处理，上下文包括attributes/input/$Strings/$Dates/$Numbers/$Booleans变量。")
            .addValidator(MvelScriptValidator.INSTANCE)
            .build();

    public static final PropertyDescriptor PROP_RECORD_OUTPUT_BUILDER = new PropertyDescriptor.Builder()
            .name("output.builder")
            .displayName("输出记录构造脚本")
            .description("可使用MVEL脚本对输出记录output进行构造和处理，上下文包括attributes/input/output/$Strings/$Dates/$Numbers/$Booleans变量。")
            .addValidator(MvelScriptValidator.INSTANCE)
            .build();

    protected volatile Serializable inputBuilder;
    protected volatile Serializable outputBuilder;

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

    protected Output buildOutput(Input input, Output output, Map<String, String> attributes) {
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
        super.beforeProcess(context);

        String inputScript = context.getProperty(PROP_RECORD_INPUT_BUILDER).getValue();
        String outputScript = context.getProperty(PROP_RECORD_OUTPUT_BUILDER).getValue();

        if (StringUtils.isNotBlank(inputScript)) {
            inputBuilder = MVEL.compileExpression(inputScript);
        }

        if (StringUtils.isNotBlank(outputScript)) {
            outputBuilder = MVEL.compileExpression(outputScript);
        }
    }

    /**
     * 根据脚本对Input、Output进行预处理
     * @param attributes
     * @param input
     * @param original
     * @param context
     * @return
     * @throws Exception
     */
    @Override
    public Tuple<Relationship, Output> processRecord(Map<String, String> attributes, Input input, FlowFile original, ProcessContext context) throws Exception {
        Input buildedInput = buildInput(input, attributes);
        Tuple<Relationship, Output> result = super.processRecord(attributes, buildedInput, original, context);
        Output buildedOutput = buildOutput(buildedInput, result.getValue(), attributes);
        return new Tuple<Relationship, Output>(result.getKey(), buildedOutput);
    }
}
