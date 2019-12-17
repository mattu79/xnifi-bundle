package io.activedata.xnifi.expression;

import com.alibaba.fastjson.JSON;
import io.activedata.xnifi.dbutils.utils.NamingUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Created by MattU on 2017/12/27.
 */
public class Strings extends StringUtils {

    public static String toString(Object o){
        return Objects.toString(o, "");
    }

    public static String toJson(Object o) {
        return JSON.toJSONString(o);
    }

    public static String jointStr(Object obj){
        StringBuffer strBf = new StringBuffer();
        jointStr(strBf,obj);
        return strBf.toString();
    }

    public static void jointStr(StringBuffer strBf,Object obj){
        if(obj instanceof List){
            List<Object> allList = (List<Object>) obj;
            allList.forEach(map->{
                jointStr(strBf,map);
            });
        }else if(obj instanceof Map){
            Map<String,Object> map = (Map) obj;
            map.forEach((k,v)->{
                strBf.append("\n");
                strBf.append(k);
                jointStr(strBf,v);
            });
        }else{
            strBf.append(obj);
        }
    }

    public static String createUUID(){
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    public static String camel2Underscore(String camel){
        return NamingUtils.camel2underscore(camel);
    }

    public static String underscore2Camel(String underscore){
        return NamingUtils.underscore2camel(underscore);
    }

}
