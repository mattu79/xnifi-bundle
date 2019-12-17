package io.activedata;

import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TEst {
    public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", "10001");
        map.put("field1", "value1");
        map.put("time1", new Date());
        map.put("time2", new Timestamp(System.currentTimeMillis()));
        RecordSchema recordSchema = DataTypeUtils.inferSchema(map, null, null);
        System.err.println(recordSchema);
        MapRecord mapRecord = new MapRecord(recordSchema, map);
        System.err.println(mapRecord);
    }
}
