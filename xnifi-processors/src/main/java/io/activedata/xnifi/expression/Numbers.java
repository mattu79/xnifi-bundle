package io.activedata.xnifi.expression;

import org.apache.commons.lang3.math.NumberUtils;

import java.text.DecimalFormat;

/**
 * Created by MattU on 2017/12/20.
 */
public class Numbers extends NumberUtils {

    public String doubleFormat(double d,String format){
        return new DecimalFormat(format).format(d);
    }

}
