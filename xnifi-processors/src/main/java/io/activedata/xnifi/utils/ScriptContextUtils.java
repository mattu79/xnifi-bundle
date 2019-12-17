package io.activedata.xnifi.utils;

import io.activedata.xnifi.expression.Booleans;
import io.activedata.xnifi.expression.Dates;
import io.activedata.xnifi.expression.Numbers;
import io.activedata.xnifi.expression.Strings;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by MattU on 2018/1/3.
 */
public class ScriptContextUtils {
    protected static final String CTX_KEY_INPUT = "input";
    protected static final String CTX_KEY_OUTPUT = "output";
    protected static final String CTX_KEY_ATTRUBUTES = "attributes";

    protected static final Dates $Dates = new Dates();
    protected static final Strings $Strings = new Strings();
    protected static final Numbers $Numbers = new Numbers();
    protected static final Booleans $Booleans = new Booleans();

    protected static final String CTX_KEY_DATES = "$Dates";
    protected static final String CTX_KEY_STRINGS = "$Strings";
    protected static final String CTX_KEY_NUMBERS = "$Numbers";
    protected static final String CTX_KEY_BOOLEANS = "$Booleans";

    public static Map<String, Object> createContext(){
        Map<String, Object> context = new HashMap<>();
        context.put(CTX_KEY_DATES,$Dates);
        context.put(CTX_KEY_STRINGS,$Strings);
        context.put(CTX_KEY_NUMBERS,$Numbers);
        context.put(CTX_KEY_BOOLEANS,$Booleans);

        return context;
    }

    public static Map<String, Object> createInputContext(Map<String, Object> input, Map<String, String> attributes) {
        Map<String, Object> context = createContext();
        context.put(CTX_KEY_ATTRUBUTES, attributes);
        context.put(CTX_KEY_INPUT, input);

        context.put(CTX_KEY_DATES, $Dates);
        context.put(CTX_KEY_NUMBERS, $Numbers);
        context.put(CTX_KEY_STRINGS, $Strings);
        context.put(CTX_KEY_BOOLEANS, $Booleans);
        return context;
    }

    public static Map<String, Object> createOutputContext(Map<String, Object> input, Map<String, Object> output, Map<String, String> attributes) {
        Map<String, Object> context = createContext();
        context.put(CTX_KEY_ATTRUBUTES, attributes);
        context.put(CTX_KEY_INPUT, input);
        context.put(CTX_KEY_OUTPUT, output);

        context.put(CTX_KEY_DATES, $Dates);
        context.put(CTX_KEY_NUMBERS, $Numbers);
        context.put(CTX_KEY_STRINGS, $Strings);
        context.put(CTX_KEY_BOOLEANS, $Booleans);
        return context;
    }

    public static Object execExpr(Serializable compiledExpr, Map<String, Object> input,
                                    Map<String, String> attributes) {
        if (compiledExpr != null) {
            Map<String, Object> context = createInputContext(input, attributes);

            Object result = MVEL.executeExpression(compiledExpr, context);
            return result;
        }

        return null;
    }
}
