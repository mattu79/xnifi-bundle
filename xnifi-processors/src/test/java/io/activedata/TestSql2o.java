package io.activedata;

import org.sql2o.quirks.NoQuirks;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSql2o {
    public static void main(String[] args) {
        Map<String, List<Integer>> params = new HashMap<>();
//        ParameterParser parser = new ParameterParser(params);
//        parser.
        NoQuirks noQuirks = new NoQuirks();
        noQuirks.getSqlParameterParsingStrategy().parseSql("SELECT * FROM tab1 WHERE type = ':TYPE' AND co1 = :col1 AND col2=:col2", params);

        System.err.println(params);
    }
}
