package io.activedata.xnifi.core;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by MattU on 2018/1/25.
 */
public class FailureOutput extends Output {
    private static final String KEY_PROCESSOR_NAME = "processorName";
    private static final String KEY_PROCESSOR_ID = "processorId";
    private static final String KEY_INPUT = "input";
    private static final String KEY_ERROR_MESSAGE = "errorMessage";
    private static final String KEY_AT_TIME = "atTime";

    public static final RecordSchema FAILURE_OUTPUT_SCHEMA;

    static{
        RecordField fieldProcessorName = new RecordField(KEY_PROCESSOR_NAME, RecordFieldType.STRING.getDataType(), false);
        RecordField fieldProcessorId = new RecordField(KEY_PROCESSOR_ID, RecordFieldType.STRING.getDataType(), false);
        RecordField fieldInput = new RecordField(KEY_INPUT, RecordFieldType.MAP.getDataType(), false);
        RecordField fieldErrorMessage = new RecordField(KEY_ERROR_MESSAGE, RecordFieldType.STRING.getDataType(), false);
        RecordField fieldAtTime = new RecordField(KEY_AT_TIME, RecordFieldType.STRING.getDataType(), false);
        List<RecordField> fieldList = new ArrayList<>();
        fieldList.add(fieldProcessorId);
        fieldList.add(fieldProcessorName);
        fieldList.add(fieldInput);
        fieldList.add(fieldErrorMessage);
        fieldList.add(fieldAtTime);

        FAILURE_OUTPUT_SCHEMA = new SimpleRecordSchema(fieldList);
    }

    public FailureOutput(String processorId, String processorName, Input input, String errorMessage) {
        super();
        Validate.notBlank(processorId, "processorId不能为空");
        Validate.notBlank(processorName, "processorName不能为空");
        Validate.notNull(input, "input不能为null");
        Validate.notBlank(errorMessage, "errorMessage不能为空");
        put(KEY_PROCESSOR_ID, processorId);
        put(KEY_PROCESSOR_NAME, processorName);
        put(KEY_INPUT, input);
        put(KEY_ERROR_MESSAGE, errorMessage);
    }

    public String getProcessorName() {
        return MapUtils.getString(this, KEY_PROCESSOR_NAME);
    }

    public String getProcessorId() {
        return MapUtils.getString(this, KEY_PROCESSOR_ID);
    }

    public Input getInput() {
        return (Input) MapUtils.getObject(this, KEY_INPUT);
    }

    public String getErrorMessage() {
        return MapUtils.getString(this, KEY_ERROR_MESSAGE);
    }
}
