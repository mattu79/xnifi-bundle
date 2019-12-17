package io.activedata.xnifi2.core.validators;

import com.alibaba.fastjson.JSON;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.text.MessageFormat;

public class JsonValidator implements Validator {
    public static final JsonValidator INSTANCE = new JsonValidator();

    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
        try {
            JSON.parseObject(input);
        } catch (Exception e) {
            String message = MessageFormat.format("JSON文本出现错误：{1}。", subject, e.getMessage());
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
