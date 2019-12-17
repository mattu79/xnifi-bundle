package io.activedata.xnifi2.core.batch.reader;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.util.*;
import java.util.function.Supplier;

public class JsonReaderWithEmbeddedSchema extends AbstractJsonRowRecordReader  {
    private static final String FORMAT_DATE = "yyyy-MM-dd";
    private static final String FORMAT_TIME = "HH:mm:ss";
    private static final String FORMAT_TIMESTAMP = "yyyy-MM-dd HH:mm:ss";

    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;

    public JsonReaderWithEmbeddedSchema(InputStream in, ComponentLog logger) throws IOException, MalformedRecordException {
        super(in, logger, FORMAT_DATE, FORMAT_TIME, FORMAT_TIMESTAMP);

        final DateFormat df = DataTypeUtils.getDateFormat(FORMAT_DATE);
        final DateFormat tf = DataTypeUtils.getDateFormat(FORMAT_TIME);
        final DateFormat tsf = DataTypeUtils.getDateFormat(FORMAT_TIMESTAMP);

        LAZY_DATE_FORMAT = () -> df;
        LAZY_TIME_FORMAT = () -> tf;
        LAZY_TIMESTAMP_FORMAT = () -> tsf;
    }

    @Override
    protected Record convertJsonNodeToRecord(JsonNode jsonNode, RecordSchema schema, boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException {
        if (jsonNode == null) {
            return null;
        }

        return convertJsonNodeToRecord(jsonNode, schema, null, coerceTypes, false);
    }

    protected Object convertField(final JsonNode fieldNode, final String fieldName, final DataType desiredType, final boolean dropUnknown) throws IOException, MalformedRecordException {
        if (fieldNode == null || fieldNode.isNull()) {
            return null;
        }

        switch (desiredType.getFieldType()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case DOUBLE:
            case FLOAT:
            case INT:
            case LONG:
            case SHORT:
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP: {
                final Object rawValue = getRawNodeValue(fieldNode);
                final Object converted = DataTypeUtils.convertType(rawValue, desiredType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                return converted;
            }
            case MAP: {
                final DataType valueType = ((MapDataType) desiredType).getValueType();

                final Map<String, Object> map = new HashMap<>();
                final Iterator<String> fieldNameItr = fieldNode.getFieldNames();
                while (fieldNameItr.hasNext()) {
                    final String childName = fieldNameItr.next();
                    final JsonNode childNode = fieldNode.get(childName);
                    final Object childValue = convertField(childNode, fieldName, valueType, dropUnknown);
                    map.put(childName, childValue);
                }

                return map;
            }
            case ARRAY: {
                final ArrayNode arrayNode = (ArrayNode) fieldNode;
                final int numElements = arrayNode.size();
                final Object[] arrayElements = new Object[numElements];
                int count = 0;
                for (final JsonNode node : arrayNode) {
                    final DataType elementType = ((ArrayDataType) desiredType).getElementType();
                    final Object converted = convertField(node, fieldName, elementType, dropUnknown);
                    arrayElements[count++] = converted;
                }

                return arrayElements;
            }
            case RECORD: {
                if (fieldNode.isObject()) {
                    RecordSchema childSchema;
                    if (desiredType instanceof RecordDataType) {
                        childSchema = ((RecordDataType) desiredType).getChildSchema();
                    } else {
                        return null;
                    }

                    if (childSchema == null) {
                        final List<RecordField> fields = new ArrayList<>();
                        final Iterator<String> fieldNameItr = fieldNode.getFieldNames();
                        while (fieldNameItr.hasNext()) {
                            fields.add(new RecordField(fieldNameItr.next(), RecordFieldType.STRING.getDataType()));
                        }

                        childSchema = new SimpleRecordSchema(fields);
                    }

                    return convertJsonNodeToRecord(fieldNode, childSchema, fieldName + ".", true, false);
                } else {
                    return null;
                }
            }
            case CHOICE: {
                return DataTypeUtils.convertType(getRawNodeValue(fieldNode, desiredType), desiredType, fieldName);
            }
        }

        return null;
    }

    private Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema, final String fieldNamePrefix,
                                           final boolean coerceTypes, final boolean dropUnknown) throws IOException, MalformedRecordException {

        final Map<String, Object> values = new HashMap<>(schema.getFieldCount() * 2);

        if (dropUnknown) {
            for (final RecordField recordField : schema.getFields()) {
                final JsonNode childNode = getChildNode(jsonNode, recordField);
                if (childNode == null) {
                    continue;
                }

                final String fieldName = recordField.getFieldName();

                final Object value;
                if (coerceTypes) {
                    final DataType desiredType = recordField.getDataType();
                    final String fullFieldName = fieldNamePrefix == null ? fieldName : fieldNamePrefix + fieldName;
                    value = convertField(childNode, fullFieldName, desiredType, dropUnknown);
                } else {
                    value = getRawNodeValue(childNode, recordField == null ? null : recordField.getDataType());
                }

                values.put(fieldName, value);
            }
        } else {
            final Iterator<String> fieldNames = jsonNode.getFieldNames();
            while (fieldNames.hasNext()) {
                final String fieldName = fieldNames.next();
                final JsonNode childNode = jsonNode.get(fieldName);

                final RecordField recordField = schema.getField(fieldName).orElse(null);

                final Object value;
                if (coerceTypes && recordField != null) {
                    final DataType desiredType = recordField.getDataType();
                    final String fullFieldName = fieldNamePrefix == null ? fieldName : fieldNamePrefix + fieldName;
                    value = convertField(childNode, fullFieldName, desiredType, dropUnknown);
                } else {
                    value = getRawNodeValue(childNode, recordField == null ? null : recordField.getDataType());
                }

                values.put(fieldName, value);
            }
        }

        final Supplier<String> supplier = jsonNode::toString;
        return new MapRecord(schema, values, SerializedForm.of(supplier, "application/json"), false, dropUnknown);
    }

    private JsonNode getChildNode(final JsonNode jsonNode, final RecordField field) {
        if (jsonNode.has(field.getFieldName())) {
            return jsonNode.get(field.getFieldName());
        }

        for (final String alias : field.getAliases()) {
            if (jsonNode.has(alias)) {
                return jsonNode.get(alias);
            }
        }

        return null;
    }
}
