package io.activedata.xnifi2.core.batch;

import io.activedata.xnifi.expression.Dates;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.serialization.record.Record;

import java.util.Date;
import java.util.LinkedHashMap;

/**
 *
 */
public class Input extends LinkedHashMap<String, Object> {
    private Date atTime = new Date();
    private Record record;

    public Input(Record record) {
        Validate.notNull(record);
        this.record = record;
        putAll(record.toMap());
    }

    public String atTime(){
        return Dates.toStdTime(atTime);
    }
}
