package io.activedata.xnifi2.core.batch.reader;

import org.apache.nifi.avro.AvroReaderWithEmbeddedSchema;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AvroRecordReaderFactory implements RecordReaderFactory {
    @Override
    public RecordReader createRecordReader(Map<String, String> variables, InputStream in, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        return new AvroReaderWithEmbeddedSchema(in);
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
        return null;
    }
}
