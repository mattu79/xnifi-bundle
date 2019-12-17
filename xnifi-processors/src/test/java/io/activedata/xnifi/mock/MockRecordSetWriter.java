package io.activedata.xnifi.mock;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

public class MockRecordSetWriter extends AbstractControllerService implements RecordSetWriterFactory {
    private final String header;
    private final int failAfterN;
    private final boolean quoteValues;
    private final List<RecordField> fields = new ArrayList<>();

    public MockRecordSetWriter() {
        this(null);
    }

    public MockRecordSetWriter(final String header) {
        this(header, true, -1);
    }

    public MockRecordSetWriter(final String header, final boolean quoteValues) {
        this(header, quoteValues, -1);
    }

    public MockRecordSetWriter(final String header, final boolean quoteValues, final int failAfterN) {
        this.header = header;
        this.quoteValues = quoteValues;
        this.failAfterN = failAfterN;
    }
    
    public void addSchemaField(final String fieldName, final RecordFieldType type) {
        fields.add(new RecordField(fieldName, type.getDataType()));
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
    	return new SimpleRecordSchema(fields);
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out) {
        return new RecordSetWriter() {
            private int recordCount = 0;
            private boolean headerWritten = false;

            @Override
            public void flush() throws IOException {
                out.flush();
            }

            @Override
            public WriteResult write(final RecordSet rs) throws IOException {
                if (header != null && !headerWritten) {
                    out.write(header.getBytes());
                    out.write("\n".getBytes());
                    headerWritten = true;
                }

                int recordCount = 0;
                Record record = null;
                while ((record = rs.next()) != null) {
                    if (++recordCount > failAfterN && failAfterN > -1) {
                        throw new IOException("Unit Test intentionally throwing IOException after " + failAfterN + " records were written");
                    }

                    final int numCols = record.getSchema().getFieldCount();

                    int i = 0;
                    for (final String fieldName : record.getSchema().getFieldNames()) {
                        final String val = record.getAsString(fieldName);
                        if (val != null) {
                            if (quoteValues) {
                                out.write("\"".getBytes());
                                out.write(val.getBytes());
                                out.write("\"".getBytes());
                            } else {
                                out.write(val.getBytes());
                            }
                        }

                        if (i++ < numCols - 1) {
                            out.write(",".getBytes());
                        }
                    }
                    //out.write("\n".getBytes());
                }

                return WriteResult.of(recordCount, Collections.emptyMap());
            }

            @Override
            public String getMimeType() {
                return "text/plain";
            }

            @Override
            public WriteResult write(Record record) throws IOException {
                if (++recordCount > failAfterN && failAfterN > -1) {
                    throw new IOException("Unit Test intentionally throwing IOException after " + failAfterN + " records were written");
                }

                if (header != null && !headerWritten) {
                    out.write(header.getBytes());
                    out.write("\n".getBytes());
                    headerWritten = true;
                }

                final int numCols = record.getSchema().getFieldCount();
                int i = 0;
                for (final String fieldName : record.getSchema().getFieldNames()) {
                    final String val = record.getAsString(fieldName);
                    if (val != null) {
                        if (quoteValues) {
                            out.write("\"".getBytes());
                            out.write(val.getBytes());
                            out.write("\"".getBytes());
                        } else {
                            out.write(val.getBytes());
                        }
                    }

                    if (i++ < numCols - 1) {
                        out.write(",".getBytes());
                    }
                }
//                out.write("\n".getBytes());

                return WriteResult.of(1, Collections.emptyMap());
            }

            @Override
            public void close() throws IOException {
                out.close();
            }

            @Override
            public void beginRecordSet() throws IOException {
            }

            @Override
            public WriteResult finishRecordSet() throws IOException {
                return WriteResult.of(recordCount, Collections.emptyMap());
            }
        };
    }
}
