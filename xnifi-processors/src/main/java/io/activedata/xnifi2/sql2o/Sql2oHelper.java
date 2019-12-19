package io.activedata.xnifi2.sql2o;

import org.apache.nifi.dbcp.DBCPService;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class Sql2oHelper {
    public static Sql2o create(DBCPService dbcpService) {
        return new Sql2o(new DataSourceWrapper(dbcpService));
    }

    public static String cacheKey(Map<String, List<Integer>> paramIdxMap, Map<String, String> attributes, Map<String, Object> input) {
        String key = "";
        if (paramIdxMap != null && paramIdxMap.size() > 0) {
            for (Map.Entry<String, List<Integer>> entry : paramIdxMap.entrySet()) {
                String paramName = entry.getKey();
                String paramValue = Objects.toString(getNamedParam(paramName, attributes, input));
                key = key + paramValue + ":";
            }
        }
        return key;
    }

    public static Map<String, Object> fetchFirstResult(Sql2o sql2o, String sql, Map<String, String> attributes, Map<String, Object> input){
        try (Connection con = sql2o.open()){
            Query query = con.createQuery(sql);
            query = Sql2oHelper.addAllParams(query, attributes, input);
            List<Map<String, Object>> results = query.executeAndFetchTable().asList();
            if (results != null && results.size() > 0) {
                Map<String, Object> result = results.get(0);
                return result;
            } else {
                return null;
            }
        }
    }

    /**
     * 从输入或属性中取得参数值
     * 优先从输入记录中取得对应参数值，如果取得的参数为空，则继续从属性中获取
     * @param paramName
     * @param attributes
     * @param input
     * @return
     */
    public static Object getNamedParam(String paramName, Map<String, String> attributes, Map<String, Object> input){
        Object paramValue = null;
        if (input != null)
            paramValue = input.get(paramName);

        if (paramValue == null)
            paramValue = attributes.get(paramName);
        return paramValue;
    }

    public static Query addAllParams(Query query, Map<String, String> attributes, Map<String, Object> input){
        Map<String, List<Integer>> paramIdxMap = query.getParamNameToIdxMap();
        List<Map<String, Object>> results = null;
        if (paramIdxMap != null && paramIdxMap.size() > 0) {
            for (Map.Entry<String, List<Integer>> entry : paramIdxMap.entrySet()){
                String paramName = entry.getKey();
                Object paramValue = Sql2oHelper.getNamedParam(paramName, attributes, input);
                query.addParameter(entry.getKey(), paramValue);
            }
        }
        return query;
    }

}
