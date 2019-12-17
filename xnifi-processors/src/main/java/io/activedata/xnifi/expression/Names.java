package io.activedata.xnifi.expression;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 命名转换类
 * Created by MattU on 2016/6/15.
 */
public class Names {

    private static final Pattern PATTERN_UNDERSCORE = Pattern.compile("_([a-z])");

    /**
     * 将驼峰风格转换成下划线风格
     * @param camel
     * @return
     */
    public static String camel2Underscore(String camel) {
        String name = StringUtils.replacePattern(camel, "([a-z])([A-Z])", "$1_$2");
        return StringUtils.uncapitalize(name);
    }

    /**
     * 将下划线风格转成驼峰风格
     * @param underscore
     * @return
     */
    public static String underscore2Camel(String underscore) {
        if (!underscore.contains("_")) {
            return underscore;
        }
        StringBuffer buf = new StringBuffer();
        underscore = underscore.toLowerCase();
        Matcher m = PATTERN_UNDERSCORE.matcher(underscore);
        while (m.find()) {
            m.appendReplacement(buf, m.group(1).toUpperCase());
        }
        return m.appendTail(buf).toString();
    }
}
