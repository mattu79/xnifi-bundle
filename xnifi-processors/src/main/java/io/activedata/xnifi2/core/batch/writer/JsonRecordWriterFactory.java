package io.activedata.xnifi2.core.batch.writer;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.json.OutputGrouping;
import org.apache.nifi.json.WriteJsonResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.record.NullSuppression;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.NopSchemaAccessWriter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class JsonRecordWriterFactory implements RecordSetWriterFactory {
    private static final String FORMAT_DATE = "yyyy-MM-dd";
    private static final String FORMAT_TIME = "HH:mm:ss";
    private static final String FORMAT_TIMESTAMP = "yyyy-MM-dd HH:mm:ss";

    private RecordSchema outputSchema;
    private boolean mergeSchema;

    public JsonRecordWriterFactory(RecordSchema outputSchema, boolean mergeSchema) {
        this.outputSchema = outputSchema;
        this.mergeSchema = mergeSchema;
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, RecordSchema inputSchema) throws SchemaNotFoundException, IOException {
        if (outputSchema != null) {
            if (mergeSchema) {
                return DataTypeUtils.merge(inputSchema, outputSchema);
            }else {
                return outputSchema;
            }
        }else {
            throw new SchemaNotFoundException("outputSchema未设置。");
        }
    }

    @Override
    public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out, Map<String, String> variables) throws SchemaNotFoundException, IOException {
        return new WriteJsonResult(logger, schema, new NopSchemaAccessWriter(), out, false, NullSuppression.ALWAYS_SUPPRESS,
                OutputGrouping.OUTPUT_ONELINE, FORMAT_DATE, FORMAT_TIME, FORMAT_TIMESTAMP);
    }

    @Override
    public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out, FlowFile flowFile) throws SchemaNotFoundException, IOException {
        return new WriteJsonResult(logger, schema, new NopSchemaAccessWriter(), out, false, NullSuppression.ALWAYS_SUPPRESS,
                OutputGrouping.OUTPUT_ONELINE, FORMAT_DATE, FORMAT_TIME, FORMAT_TIMESTAMP);
    }

    @Override
    public void initialize(ControllerServiceInitializationContext context) throws InitializationException {

    }

    @Override
    public Collection<ValidationResult> validate(ValidationContext context) {
        return null;
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(String name) {
        return null;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        return null;
    }

    @Override
    public String getIdentifier() {
        return "JsonRecordWriterFactory";
    }
}
