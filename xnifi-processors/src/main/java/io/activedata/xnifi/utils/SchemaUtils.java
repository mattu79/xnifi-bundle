package io.activedata.xnifi.utils;

import org.apache.avro.Schema;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.avro.AvroTypeUtil.AVRO_SCHEMA_FORMAT;

/**
 * Created by MattU on 2017/11/29.
 */
public class SchemaUtils {
    /**
     * 递归产生schema
     * 如果返回null代表产生build失败
     */
    public static RecordSchema buildRecordSchema(Map<String, Object> data) {
        Validate.notNull(data, "用于取得Schema的数据不能为null。");
        List<RecordField> fields = new ArrayList<>();
        data.forEach((k, v) -> {
            RecordField buildRecordField = createRecordField(k, v);
            if (buildRecordField != null) {
                fields.add(buildRecordField);
            }
        });

        if (fields.size() == 0) {
            return null;
        }
        return new SimpleRecordSchema(fields);
    }

    public static String generateAvroSchemaJson(Map<String, Object> data){
        RecordSchema recordSchema = buildRecordSchema(data);
        Schema schema = AvroTypeUtil.extractAvroSchema(recordSchema);
        return schema.toString(false);
    }

    static RecordField createRecordField(String fieldName, Object fieldValue) {
        DataType fieldDataType = getFieldDataType(fieldValue);
        return new RecordField(fieldName, fieldDataType);
    }

    static DataType getFieldDataType(Object fieldValue) {
        if (fieldValue != null) {
            if (fieldValue instanceof String) {
                return RecordFieldType.STRING.getDataType();
            } else if (fieldValue instanceof Integer) {
                return RecordFieldType.INT.getDataType();
            } else if (fieldValue instanceof Long) {
                return RecordFieldType.LONG.getDataType();
            } else if (fieldValue instanceof Boolean) {
                return RecordFieldType.BOOLEAN.getDataType();
            } else if (fieldValue instanceof Date) {
                return RecordFieldType.TIMESTAMP.getDataType();
            } else if (fieldValue instanceof List) {
                List list = (List) fieldValue;
                Object e = firstNotNull(list);
                DataType childDataType = getFieldDataType(e);
                return new ArrayDataType(childDataType);
            } else if (fieldValue instanceof Map) {
                Map map = (Map) fieldValue;
                return new RecordDataType(buildRecordSchema(map));
            } else {
                throw new RuntimeException("不支持的字段数据：" + fieldValue);
            }
        }

        return RecordFieldType.STRING.getDataType();
    }

    static Object firstNotNull(Object[] objs) {
        if (objs != null && objs.length > 0) {
            for (int i = 0; i < objs.length; i++) {
                if (objs[i] != null)
                    return objs[i];
            }

        }

        return null;
    }

    static Object firstNotNull(List objs) {
        if (objs != null && objs.size() > 0) {
            for (Object obj : objs) {
                if (obj != null)
                    return obj;
            }
        }

        return null;
    }
}
