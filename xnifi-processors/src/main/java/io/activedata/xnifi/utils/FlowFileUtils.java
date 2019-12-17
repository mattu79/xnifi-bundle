package io.activedata.xnifi.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by MattU on 2017/12/27.
 */
public class FlowFileUtils {
    private static final List<Map> EMPTY_LIST = new ArrayList<>();

    static final String DEFAULT_CHARSET = "UTF-8";

    public static Object readJsonToObject(ProcessSession session, FlowFile flowFile){
        return readJsonToObject(session, flowFile, null);
    }

    public static Object readJsonToObject(ProcessSession session, FlowFile flowFile, String charset){
        Validate.notNull(session, "参数session不能为null。");
        Validate.notNull(flowFile, "参数flowFile不能为null。");

        if (StringUtils.isBlank(charset))
            charset = DEFAULT_CHARSET;

        try {
            final byte[] content = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);
                }
            });
            Object result = JSON.parse(new String(content, charset));
            return result;
        }catch (Exception e){
            throw new ProcessException(ExceptionUtils.getStackTrace(e));
        }
    }

    public static List<Map> readJsonToList(ProcessSession session, FlowFile flowFile){
        return readJsonToList(session, flowFile, DEFAULT_CHARSET);
    }

    public static Map readJson(ProcessSession session, FlowFile flowFile, String charset){
        Validate.notNull(session, "参数session不能为null。");
        Validate.notNull(flowFile, "参数flowFile不能为null。");

        if (StringUtils.isBlank(charset))
            charset = DEFAULT_CHARSET;

        try {
            final byte[] content = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);
                }
            });
            Map result = JSON.parseObject(new String(content, charset));
            return result;
        }catch (Exception e){
            throw new ProcessException(ExceptionUtils.getStackTrace(e));
        }
    }

    public static List<Map> readJsonToList(ProcessSession session, FlowFile flowFile, String charset) throws ProcessException {
        Validate.notNull(session, "参数session不能为null。");
        Validate.notNull(flowFile, "参数flowFile不能为null。");

        if (StringUtils.isBlank(charset))
            charset = DEFAULT_CHARSET;

        try {
            final byte[] content = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);
                }
            });
            Object result = JSON.parse(new String(content, charset));
            if (result == null) return EMPTY_LIST;

            if (result instanceof List){
                return (List<Map>) result;
            }else if (result instanceof Map) {
                List<Map> list = new ArrayList();
                list.add((Map) result);
                return list;
            } else {
                throw new ProcessException("不支持的数据类型：" + result.getClass());
            }
        }catch (Exception e){
            throw new ProcessException(ExceptionUtils.getStackTrace(e));
        }
    }

    public static FlowFile writeListToJson(ProcessSession session, FlowFile flowFile, List<Map> rows){
        Validate.notNull(session, "参数session不能为null。");
        Validate.notNull(flowFile, "参数flowFile不能为null。");
        Validate.notNull(rows, "参数rows不能为null。");

        String jsonString = JSON.toJSONString(rows);

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(jsonString.getBytes(DEFAULT_CHARSET));
            }
        });

        return flowFile;
    }

    /**
     * 取得某处理器设置的动态属性
     * @param context
     * @return
     */
    public static Map<String, String> getDynamicProperties(ProcessContext context) throws ProcessException{
        Map<String, String> dynamicProperties = new LinkedHashMap<>(context.getProperties().size());
        for (final PropertyDescriptor property : context.getProperties().keySet()) {
            if (property.isDynamic()) {
                String name = property.getName();
                String value = null;
                if (property.isExpressionLanguageSupported()){
                    try {
                        value = context.getProperty(property).getValue();
                    }catch (Exception e){
                        throw new ProcessException(ExceptionUtils.getStackTrace(e));
                    }
                }else{
                    value = context.getProperty(property).getValue();
                }

                dynamicProperties.put(name, value);
            }
        }

        return dynamicProperties;
    }

    public static List<Map> readRows(final ProcessSession session, final FlowFile flowFile) throws UnsupportedEncodingException {
        final byte[] content = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, content, true);
            }
        });

        List<Map> list = JSON.parseArray(new String(content, "UTF-8"), Map.class);
        return list;
    }

    public static FlowFile write(final ProcessSession session, final FlowFile flowFile, String content){
        if (StringUtils.isNotBlank(content)){
            FlowFile newFlowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    byte[] bytes = content.getBytes("UTF-8");
                    IOUtils.write(bytes, out);
                    IOUtils.closeQuietly(out);
                }
            });
            return newFlowFile;
        }

        return flowFile;
    }
}
