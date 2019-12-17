package io.activedata.xnifi2.core.batch;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.Tuple;

import java.util.Map;

public interface RecordProcessHandler {
    /**
     * 读取和处理FlowFile中一条数据记录
     * @param attributes
     * @param input
     * @param original
     * @param context
     * @return
     * @throws ProcessException
     */
    Tuple<Relationship, Output> processRecord(Map<String, String> attributes, Input input, FlowFile original, ProcessContext context) throws Exception;
}
