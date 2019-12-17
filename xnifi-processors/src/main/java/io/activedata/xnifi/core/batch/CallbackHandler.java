package io.activedata.xnifi.core.batch;

import io.activedata.xnifi.core.Input;
import io.activedata.xnifi.core.Output;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.util.Tuple;

import java.util.List;
import java.util.Map;

/**
 * 回调的处理接口，用于实现具体的业务逻辑
 * Created by MattU on 2018/1/25.
 */
public interface CallbackHandler {
    /**
     *
     * @return
     */
    List<RecordField> getResultFields();

    /**
     * 读取和处理FlowFile中一条数据记录
     * @param attributes
     * @param input
     * @param original
     * @param context
     * @return
     * @throws ProcessException
     */
    Tuple<Relationship, Output> handleProcess(Map<String, String> attributes, Input input, FlowFile original, ProcessContext context) throws ProcessException;
}
