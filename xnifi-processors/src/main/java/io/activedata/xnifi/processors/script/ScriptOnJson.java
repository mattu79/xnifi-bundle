package io.activedata.xnifi.processors.script;

import io.activedata.xnifi.core.Input;
import io.activedata.xnifi.core.Output;
import io.activedata.xnifi.core.batch.AbstractJsonRowBatchProcessor;
import io.activedata.xnifi.utils.ScriptContextUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.*;

@TriggerSerially
@SupportsBatching
@Tags({ "script", "mvel", "json"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("对JSON记录进行脚本处理")
public class ScriptOnJson extends AbstractJsonRowBatchProcessor {

    private static final String LOCAL = "$LOCAL";
    private static final String CLUSTER = "$CLUSTER";

    public static final PropertyDescriptor PROP_SCRIPT = new PropertyDescriptor.Builder()
            .name("script")
            .displayName("JSON记录处理脚本")
            .description("可使用MVEL脚本中的函数对数据进行处理，上下文包括attributes/input/output/$Strings/$Dates/$Numbers/$Booleans变量。")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR).build();

    private volatile Serializable compileScript;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PROP_WRITER_SCHEMA_STRATEGY);
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

    @Override
    protected Output handleProcessRow(Map<String, String> attributes, Input input, FlowFile flowFile, ProcessContext context) throws ProcessException {
        Output output = new Output();
        if(compileScript != null){
            Map<String, Object> scriptContext = createScriptContext(attributes, input, output, context);
            try {
                MVEL.executeExpression(compileScript, scriptContext);
            }catch(Exception e){
                throw new ProcessException("执行脚本时出现错误：" + ExceptionUtils.getStackTrace(e));
            }
        }
        return output;
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
        context.put(LOCAL,Scope.LOCAL);
        context.put(CLUSTER,Scope.CLUSTER);
        return context;
    }
}
