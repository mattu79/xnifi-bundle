package io.activedata.xnifi.core;

import io.activedata.xnifi.expression.Dates;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by MattU on 2018/1/25.
 */
public class Input extends LinkedHashMap {
    private Date atTime = new Date();

    public Input() {
    }

    public Input(Map m) {
        super(m);
    }

    public String atTime(){
        return Dates.toStdTime(atTime);
    }
}
