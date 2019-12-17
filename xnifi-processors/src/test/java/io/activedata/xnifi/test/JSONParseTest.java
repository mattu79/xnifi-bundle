package io.activedata.xnifi.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import io.activedata.xnifi.expression.Dates;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class JSONParseTest {

    @Test
    public void test(){
        String json1 = "{id:1, name:'1', age: 10, father: {id: 0, name: 'father',son:{id:1, name:'1', father: {id: 0, name: 'father'}}},children: [{id: 3, name: 'xxx', gen1: []},{}]}";
        String json2 = "[{id:1, name:'1', father: {id: 0, name: 'father'}}, {id:2, name:'2', children: [{id: 3, name: 'xxx', children: []},{}]}]";
        Object o1 = JSON.parse(json1);
        Object o2 = JSON.parse(json2);
        System.err.println(o1.getClass());
        System.err.println(o2.getClass());

        List<Map> results = parseToJsonRows(json1);
//        System.err.println(results);
        results = parseToJsonRows(json2);
//        System.err.println(results);

        RecordSchema schema = DataTypeUtils.inferSchema((Map<String, Object>) o1, null, null);
        System.err.println(schema);



//        JSONArray jsonArray = (JSONArray) o2;
//        JSONObject json = (JSONObject) o1;

    }

    protected List<Map> parseToJsonRows(String content){
        Object result = JSON.parse(content);
        if (result instanceof JSONArray){
            return (List<Map>) result;
        }else {
            List<Map> results = new ArrayList<>();
            results.add((Map) result);
            return results;
        }
    }

    @Test
    public void test1(){
        Date date = Dates.fromUtc("20190420T155004Z");
//        System.err.println(DateFormatUtils.formatUTC(date, "YYYY-mm-dd hh:mm:ss"));
        System.err.println(date.getTime());
        System.err.println(Dates.toStdTime(date));
    }
}
