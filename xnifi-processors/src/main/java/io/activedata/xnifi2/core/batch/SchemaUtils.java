package io.activedata.xnifi2.core.batch;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.HashMap;

public class SchemaUtils {
    public static RecordSchema merge(RecordSchema inputSchema, RecordSchema outputSchema){
        MapRecord mapRecord = new MapRecord(inputSchema, new HashMap<>());
        return null;
    }

    public static void main(String[] args) {
        RecordSchema schema = new SimpleRecordSchema(new ArrayList<>());
        MapRecord mapRecord = new MapRecord(schema, new HashMap<>());

        mapRecord.setValue("xxx", "123");
        mapRecord.setValue("yyy", "321");

        System.err.println(mapRecord.getSchema());
    }
}
