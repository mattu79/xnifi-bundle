package io.activedata.xnifi.dbutils.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by MattU on 2017/12/14.
 */
public class NamingUtils {
    public static String camel2underscore(String camel) {
        camel = camel.replaceAll("([a-z])([A-Z])", "$1_$2");
        return camel.toLowerCase();
    }

    public static String underscore2camel(String underscore) {
        if (!underscore.contains("_")) {
            return underscore;
        }
        StringBuffer buf = new StringBuffer();
        underscore = underscore.toLowerCase();
        Matcher m = Pattern.compile("_([a-z])").matcher(underscore);
        while (m.find()) {
            m.appendReplacement(buf, m.group(1).toUpperCase());
        }
        return m.appendTail(buf).toString();
    }
}
