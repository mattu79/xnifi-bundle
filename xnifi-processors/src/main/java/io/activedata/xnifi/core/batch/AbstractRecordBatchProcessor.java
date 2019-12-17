package io.activedata.xnifi.core.batch;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by MattU on 2018/1/25.
 */
public abstract class AbstractRecordBatchProcessor extends AbstractBatchProcessor {
    protected InputStreamCallback createCallback(ComponentLog logger, FlowFile original, ProcessContext context, ProcessSession session) {
        return new RecordsProcessCallback(getLogger(), original, context, session, this);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RecordsProcessCallback.PROP_RECORD_READER);
        properties.add(RecordsProcessCallback.PROP_RECORD_WRITER);
        properties.addAll(super.getSupportedPropertyDescriptors());
        return properties;
    }
}
