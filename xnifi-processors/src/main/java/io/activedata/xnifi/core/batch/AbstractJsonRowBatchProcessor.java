package io.activedata.xnifi.core.batch;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.serialization.record.RecordField;

import java.util.List;

/**
 * 抽象的基于JSON的记录批处理器
 * Created by MattU on 2018/1/26.
 */
public abstract class AbstractJsonRowBatchProcessor extends AbstractBatchProcessor {
    @Override
    protected InputStreamCallback createCallback(AbstractBatchProcessor processor, ComponentLog logger, FlowFile original, ProcessContext context, ProcessSession session) {
        return new JsonRowsProcessCallback(logger, original, context, session, this);
    }

    /**
     * 在JSON记录处理器中无需设置结果字段
     * @return
     */
    @Override
    public List<RecordField> getResultFields() {
        return null;
    }
}
