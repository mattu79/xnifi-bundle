package io.activedata.xnifi.processors.events.utils;

import com.alibaba.fastjson.JSON;
import graphql.language.*;
import graphql.parser.Parser;
import io.activedata.xnifi.dbutils.utils.NamingUtils;
import jodd.bean.BeanUtil;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * Graphql解析工具类
 */
public class GraphqlParserUtils {
    private static final Map<String, Object> EMPTY_MAP = new HashMap<>();
    private static final Parser parser = new Parser();
    private static final String DEF_KEY_TYPE = "type"; //对应Graphql的操作名称，可以为空
    private static final String DEF_KEY_NAME = "name"; //请求的名字，可以是中文，如查看赛事、查看文章或者GraphQL的查询名称
    private static final String DEF_KEY_CATEGORY = "category"; //这里为了兼容以前的PROFILE事件，所以增加属性category，以便统一处理，为英文形式
    private static final String DEF_KEY_PARAMS = "params";

    public static Map<String, Object> parseRequest(String payload, String variablesText) {
        if (StringUtils.isNotBlank(payload)) {
            payload = StringEscapeUtils.unescapeJava(payload);
            Map<String, Object> variables = EMPTY_MAP;
            try {
                Document doc = parser.parseDocument(payload);
                if (StringUtils.isNotBlank(variablesText))
                    variables = JSON.parseObject(variablesText);
                List<Definition> defs = doc.getDefinitions();
                if (defs != null && defs.size() >= 1) {
                    Definition def = defs.get(0); //只取第一个定义
                    if (def instanceof OperationDefinition) {
                        Map<String, Object> defMap = singleDefinition((OperationDefinition) def, variables);
                        return defMap;
                    }
                }

                throw new RuntimeException("不支持解析此类GraphQL请求：[" + payload + "]。");
            } catch (Exception e) {
                throw e;
            }
        }
        return EMPTY_MAP;
    }

    static Map<String, Object> singleDefinition(OperationDefinition opDef, Map<String, Object> variables) {
        String name = opDef.getName();
        if (StringUtils.isBlank(name)) {
            Map<String, Object> defMap = unamedDefinition(opDef, variables); // 处理首层未命名的定义，如直接{}包含的查询
            return defMap;
        }else{
            Map<String, Object> defMap = namedDefinition(opDef, variables, name); //处理首层有命名的定义
            return defMap;
        }

    }

    private static Map<String, Object> namedDefinition(OperationDefinition opDef, Map<String, Object> variables, String name) {
        Map<String, Object> defMap = new HashMap<>();
        String op = Objects.toString(opDef.getOperation());
        defMap.put(DEF_KEY_TYPE, op);
        defMap.put(DEF_KEY_NAME, name);
//        defMap.put(DEF_KEY_CATEGORY, NamingUtils.camel2underscore(name)); //ES中不支持大写的名称
        defMap.put(DEF_KEY_PARAMS, variables);
        return defMap;
    }

    private static Map<String, Object> unamedDefinition(OperationDefinition opDef, Map<String, Object> variables) {
        Map<String, Object> defMap = new HashMap<>();
        String op = Objects.toString(opDef.getOperation());
        defMap.put(DEF_KEY_TYPE, op);
        SelectionSet selectionSet = opDef.getSelectionSet();
        List<Selection> selections = selectionSet.getSelections();
        if (selections.size() == 1) {
            Selection selection = selections.get(0);
            if (selection instanceof Field) {
                Field field = (Field) selection;
                defMap.put(DEF_KEY_NAME, field.getName());
//                defMap.put(DEF_KEY_CATEGORY, NamingUtils.camel2underscore(field.getName())); //ES中不支持大写的名称
                List<Argument> args = field.getArguments();
                if (args != null && args.size() > 0) {
                    Map<String, Object> argMap = new HashMap<>();
                    defMap.put(DEF_KEY_PARAMS, argMap);
                    for (Argument arg : args) {
                        argMap.put(arg.getName(), toPrimitive(arg.getValue(), variables));
                    }
                }
            }
        }
        return defMap;
    }

    /**
     * 将Argument的Value转换为原生类型
     * @param value
     * @param variables
     * @return
     */
    static Object toPrimitive(Value value, Map<String, Object> variables) {
        if (value == null) return "";

        if (value instanceof StringValue)
            return ((StringValue) value).getValue();
        else if (value instanceof IntValue)
            return Objects.toString(((IntValue) value).getValue(), "");
        else if (value instanceof BooleanValue)
            return Objects.toString(((BooleanValue) value).isValue(), "");
        else if (value instanceof FloatValue)
            return Objects.toString(((FloatValue) value).getValue(), "");
        else if (value instanceof EnumValue)
            return Objects.toString(((EnumValue) value).getName(), "");
        else if (value instanceof NullValue)
            return "";
        else if (value instanceof ArrayValue) {
            List<Object> valueList = new ArrayList<>();
            List<Value> values = ((ArrayValue) value).getValues();
            for (Value elementValue : values){
                valueList.add(toPrimitive(elementValue, variables));
            }
            return valueList;
        }else if (value instanceof ObjectValue){
            List<ObjectField> fields = ((ObjectValue) value).getObjectFields();
            Map<String, Object> valueMap = new HashMap<>();
            for (ObjectField field : fields){
                valueMap.put(field.getName(), toPrimitive(field.getValue(), variables));
            }
            return valueMap;
        }else if (value instanceof VariableReference){
            String varName = ((VariableReference) value).getName();
            Object varValue = BeanUtil.silent.getProperty(variables, varName);
            return Objects.toString(varValue, "");
        } else {
            throw new NotImplementedException("不支持的数据类型：" + value.getClass());
        }
    }


}
