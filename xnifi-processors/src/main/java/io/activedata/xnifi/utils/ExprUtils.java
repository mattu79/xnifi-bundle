package io.activedata.xnifi.utils;

import io.activedata.xnifi.expression.Strings;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.Map;

public class ExprUtils {

    public static final Object exec(Serializable compiledExpr, Map<String, Object> input,
                                        Map<String, String> attributes) {
        if (compiledExpr != null) {
            Map<String, Object> context = ScriptContextUtils.createInputContext(input, attributes);

            Object result = MVEL.executeExpression(compiledExpr, context);
            return result;
        }

        return null;
    }

    public static final String execAsString(Serializable compiledExpr, Map<String, Object> input,
                                     Map<String, String> attributes){
        Object result = exec(compiledExpr, input, attributes);
        return Strings.toString(result);
    }
}
