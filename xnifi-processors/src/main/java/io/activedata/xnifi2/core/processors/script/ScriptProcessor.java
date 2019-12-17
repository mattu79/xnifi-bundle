package io.activedata.xnifi2.core.processors.script;

import io.activedata.xnifi.utils.ScriptContextUtils;
import io.activedata.xnifi2.core.batch.AbstractBatchProcessor;
import io.activedata.xnifi2.core.batch.Input;
import io.activedata.xnifi2.core.batch.Output;
import io.activedata.xnifi2.core.validators.MvelScriptValidator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.Tuple;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.*;

@TriggerSerially
@SupportsBatching
@Tags({ "script", "mvel", "json", "record"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("对数据记录进行脚本处理")
public class ScriptProcessor extends AbstractBatchProcessor {

    public static final PropertyDescriptor PROP_SCRIPT = new PropertyDescriptor.Builder()
            .name("script")
            .displayName("记录处理脚本")
            .description("可使用MVEL脚本中的函数对数据进行处理，上下文包括attributes/input/output/$Strings/$Dates/$Numbers/$Booleans变量。")
            .addValidator(MvelScriptValidator.INSTANCE).build();

    private volatile Serializable compileScript;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PROP_INPUT_RECORD_TYPE);
        properties.add(PROP_OUTPUT_RECORD_TYPE);
        properties.add(PROP_OUTPUT_RECORD_EXAMPLE);
        properties.add(PROP_OUTPUT_RECORD_STRATEGY);
        properties.add(PROP_SCRIPT);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    protected Tuple<Relationship, Output> processRecordInternal(Map<String, String> attributes, Input input, FlowFile original, ProcessContext context) {
        Output output = new Output();
        if(compileScript != null){
            Map<String, Object> scriptContext = createScriptContext(attributes, input, output, context);
            MVEL.executeExpression(compileScript, scriptContext);
        }
        return new Tuple<>(REL_SUCCESS, output);
    }

    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        String scriptText = context.getProperty(PROP_SCRIPT).getValue();
        if (StringUtils.isNotBlank(scriptText)){
            try {
                compileScript = MVEL.compileExpression(scriptText);
            }catch (Exception e){
                throw new ProcessException("编译脚本时出现错误：" + ExceptionUtils.getStackTrace(e));
            }
        }else{
            compileScript = null; //当脚本设置为空时，则不执行脚本处理
        }
        super.beforeProcess(context);
    }

    protected Map<String, Object> createScriptContext(Map<String, String> attributes, Map<String, Object> input, Map<String, Object> output,ProcessContext processContext){
        Map<String, Object> context = ScriptContextUtils.createOutputContext(input, output, attributes);
        return context;
    }
}
