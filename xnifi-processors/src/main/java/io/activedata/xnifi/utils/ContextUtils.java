package io.activedata.xnifi.utils;

import io.activedata.xnifi.exceptions.InvalidEnvironmentException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by MattU on 2018/1/24.
 */
public class ContextUtils {
    /**
     * 取得某处理器设置的动态属性
     * @param context
     * @param flowFile
     * @return
     */
    public static Map<String, String> getDynamicProperties(ProcessContext context, FlowFile flowFile) throws ProcessException{
        Map<String, String> originalAttrs = flowFile.getAttributes();
        Map<String, String> dynamicProperties = new LinkedHashMap<>(context.getProperties().size());
        for (final PropertyDescriptor property : context.getProperties().keySet()) {
            if (property.isDynamic()) {
                String name = property.getName();
                String value = null;
                if (property.isExpressionLanguageSupported()){
                    try {
                        value = context.getProperty(property).evaluateAttributeExpressions(originalAttrs).getValue();
                    }catch (Exception e){
                        throw new InvalidEnvironmentException("初始化属性时出现错误：", e);
                    }
                }else{
                    value = context.getProperty(property).getValue();
                }

                dynamicProperties.put(name, value);
            }
        }

        return dynamicProperties;
    }

    /**
     * 创建新的FlowFile属性Map，里面包含原有属性值和动态属性值
     * @param context
     * @param flowFile
     * @return
     */
    public static Map<String, String> createAttributes(ProcessContext context, FlowFile flowFile){
        Map<String, String> attributes = new HashMap<>();
        attributes.putAll(flowFile.getAttributes());
        attributes.putAll(getDynamicProperties(context, flowFile)); //添加动态属性
        return attributes;
    }

    public static File validateAndGetFile(String fileDir, String fileName) throws InvalidEnvironmentException {
        Validate.notBlank(fileDir, "文件目录不能为空。");
        Validate.notBlank(fileName, "文件名不能为空。");
        String filePath = FilenameUtils.concat(fileDir, fileName);
        File file = FileUtils.getFile(filePath);
        if (!file.exists()){
            throw new InvalidEnvironmentException("无法在目录[" + fileDir + "]中找到文件[" + fileName + "]");
        }

        return file;
    }
}
