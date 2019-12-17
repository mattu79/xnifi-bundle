package io.activedata.xnifi.core;

import io.activedata.xnifi.expression.Dates;

import java.util.Date;
import java.util.LinkedHashMap;

/**
 * Created by MattU on 2018/1/25.
 */
public class Output extends LinkedHashMap {
    public static final String KEY_AT_TIME = "atTime";
    public Output() {
        put(KEY_AT_TIME, new Date());
    }

    public Output(Output m) {
        super(m);
        put(KEY_AT_TIME, new Date());
    }

    public String getAtTime(){
        return Dates.toStdTime((Date) get(KEY_AT_TIME));
    }
}
