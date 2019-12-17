package io.activedata.xnifi2.core.validators;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.mvel2.MVEL;

import java.text.MessageFormat;

public class MvelScriptValidator implements Validator {
    public static final MvelScriptValidator INSTANCE = new MvelScriptValidator();

    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
        try {
            MVEL.compileExpression(input);
        }catch (Exception e){
            String message = MessageFormat.format("编译脚本时出现错误：{1}。", subject, e.getMessage());
            return new ValidationResult.Builder()
                    .subject(subject)
                    .valid(false)
                    .explanation(message)
                    .build();
        }
        return new ValidationResult.Builder()
                .valid(true)
                .build();
    }
}
