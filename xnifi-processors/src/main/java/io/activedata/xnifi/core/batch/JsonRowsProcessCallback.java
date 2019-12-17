package io.activedata.xnifi.core.batch;

import com.alibaba.fastjson.JSON;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.activedata.xnifi.core.Input;
import io.activedata.xnifi.core.Output;
import io.activedata.xnifi.utils.ContextUtils;
import io.activedata.xnifi.utils.FlowFileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 循环处理JsonRows的回调对象
 * <p>
 * Created by MattU on 2018/1/24.
 */
public class JsonRowsProcessCallback implements ProcessCallback {
    private Map<Relationship, List<Output>> jsonRowsMap = new HashMap<>();

    private ComponentLog logger;
    private ProcessContext context;
    private ProcessSession session;
    private FlowFile original;
    private Map<String, String> attributes;
    private CallbackHandler handler;

    public JsonRowsProcessCallback(ComponentLog logger, FlowFile original, ProcessContext context, ProcessSession session, CallbackHandler handler) {
        Validate.notNull(logger, "参数logger不能为null。");
        Validate.notNull(original, "参数original不能为null。");
        Validate.notNull(context, "参数context不能为null。");
        Validate.notNull(session, "参数session不能为null。");

        this.logger = logger;
        this.original = original;
        this.context = context;
        this.session = session;
        this.handler = handler;
        this.attributes = ContextUtils.createAttributes(context, original);
    }

    @Override
    public void process(InputStream in) throws IOException {
        byte[] bytes = new byte[(int) original.getSize()];
        StreamUtils.fillBuffer(in, bytes, true);
        List<Map> jsonRows = null;
        if (bytes.length > 0) {
            String content = new String(bytes, "UTF-8");
            jsonRows = parseToJsonRows(content);
        }

        if (jsonRows != null && jsonRows.size() > 0) {
            for (Map jsonRow : jsonRows) {
                processJsonRow(new Input(jsonRow));
            }
            writeAllJsonRows();
        }
    }

    @Override
    public CallbackHandler getHandler() {
        return handler;
    }

    protected void processJsonRow(Input input) {
        Tuple<Relationship, Output> result = getHandler().handleProcess(attributes, input, original, context);
        Relationship rel = result.getKey();
        Output output = result.getValue();
        List<Output> jsonRows = jsonRowsMap.get(rel);
        if (jsonRows == null) {
            jsonRows = new ArrayList<>();
            jsonRowsMap.put(rel, jsonRows);
        }
        jsonRows.add(output);
    }

    /**
     * 将content解析成List<Map>，如果content不是数组形式，则解析成Map加入到List中
     *
     * @param content
     * @return
     */
    protected List<Map> parseToJsonRows(String content) {
        Object result = JSON.parse(content);
        if (result instanceof JSONArray) {
            return (List<Map>) result;
        } else {
            List<Map> results = new ArrayList<>();
            results.add((Map) result);
            return results;
        }
    }

    /**
     * 将所有处理后的JSONRow写入到FlowFile中
     */
    protected void writeAllJsonRows() {
        for (final Map.Entry<Relationship, List<Output>> entry : jsonRowsMap.entrySet()) {
            final Relationship rel = entry.getKey();
            final List<Output> jsonRows = entry.getValue();
            FlowFile childFlowFile = session.create(original);
            attributes.put("record.count", String.valueOf(jsonRows.size()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
            String json = JSON.toJSONString(jsonRows);
            FlowFileUtils.write(session, childFlowFile, json);
            childFlowFile = session.putAllAttributes(childFlowFile, attributes);
            session.transfer(childFlowFile, rel);

            //session.adjustCounter("Records Processed", writeResult.getRecordCount(), false);
            //session.adjustCounter("Records Routed to " + relationship.getName(), writeResult.getRecordCount(), false);

            session.getProvenanceReporter().route(childFlowFile, rel);
        }
    }
}
