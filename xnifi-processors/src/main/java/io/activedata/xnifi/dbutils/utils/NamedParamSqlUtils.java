package io.activedata.xnifi.dbutils.utils;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NamedParamSqlUtils {
    private static final String[] EMPTY_NAMES = new String[0];

    private static final Pattern PTN_NAMED_PARAMS = Pattern.compile("[(,\\s](:\\w+)");

    /**
     * 读取SQL中的命名参数（即':'开头的变量）
     * @param sql
     * @return
     */
    public static String[] paramNames(String sql) {
        Matcher matcher = PTN_NAMED_PARAMS.matcher(sql);
        List<String> names = new ArrayList<>();
        while (matcher.find()) {
            names.add(StringUtils.removeFirst(matcher.group(1), ":"));
        }

        return names.toArray(EMPTY_NAMES);
    }

    /**
     * 将含有命名参数的SQL转为原生带有?参数的SQL
     * @param namedParamSql
     * @return
     */
    public static String toNativeSql(String namedParamSql){
        Matcher matcher = PTN_NAMED_PARAMS.matcher(namedParamSql);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String s = StringUtils.replace(matcher.group(0), matcher.group(1), "?");
            matcher.appendReplacement(sb, s);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    public static void main(String[] args) {
        String sql = "INSERT INTO XXX VALUES(:p1, :p2, 'p3', ':p4', 'xx:p5', :p6)";
        String[] names = paramNames(sql);
        System.err.println(Arrays.toString(names));

        String nativeSql = toNativeSql(sql);
        System.err.println(nativeSql);
    }
}
