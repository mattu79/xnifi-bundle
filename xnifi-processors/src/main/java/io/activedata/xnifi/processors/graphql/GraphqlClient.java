package io.activedata.xnifi.processors.graphql;

import com.alibaba.fastjson.JSON;
import okhttp3.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GraphqlClient {
    public static final MediaType JSON_UTF8
            = MediaType.parse("application/json; charset=utf-8");

    public static final String KEY_QUERY = "query";
    public static final String KEY_VARIABLES = "variables";
    public static final String KEY_OPERATION_NAME = "operationName";

    OkHttpClient client = new OkHttpClient();

    /**
     * 向指定graphql服务提交请求
     *
     * @param url       服务地址
     * @param payload   graphql请求内容
     * @param variables graphql变量
     * @param opName    操作名称，保留，暂时不填。
     * @return
     * @throws IOException
     */
    public Map<String, Object> request(String url, String payload, Map<String, Object> variables, String opName) throws IOException {
        return request(url, payload, variables, opName, null);
    }

    /**
     * 向指定graphql服务提交请求
     *
     * @param url       服务地址
     * @param payload   graphql请求内容
     * @param variables graphql变量
     * @param opName    操作名称，保留，暂时不填。
     * @param headerMap 请求头
     * @return
     * @throws IOException
     */
    public Map<String, Object> request(String url, String payload, Map<String, Object> variables, String opName, Map<String, String> headerMap) throws IOException {
        Map<String, Object> request = new HashMap<>();
        request.put(KEY_QUERY, payload);
        request.put(KEY_VARIABLES, variables);
        request.put(KEY_OPERATION_NAME, opName);
        return post(url, request, headerMap);
    }

    /**
     * 将请求对象转成JSON并POST到某服务
     *
     * @param url     服务地址
     * @param request 请求对象
     * @param headerMap 请求头
     * @return 返回Map对象
     * @throws IOException
     */
    protected Map<String, Object> post(String url, Map<String, Object> request, Map<String, String> headerMap) throws IOException {
        String json = JSON.toJSONString(request);
        String respJson = postJson(url, json, headerMap);
        return JSON.parseObject(respJson);
    }

    /**
     * 向某服务POST JSON文档
     *
     * @param url  服务地址
     * @param json JSON文档内容
     * @param headerMap 请求头
     * @return 接收到的消息内容
     */
    protected String postJson(String url, String json, Map<String, String> headerMap) throws IOException {
        OkHttpClient okHttpClient = new OkHttpClient();
        RequestBody body = RequestBody.create(JSON_UTF8, json);

        Request request = new Request.Builder()
                .url(url)
                .headers(buildHeaders(headerMap))
                .post(body)
                .build();
        Call call = okHttpClient.newCall(request);
        Response response = call.execute();
        return response.body().string();
    }

    private Headers buildHeaders(Map<String, String> headerMap) {
        Headers.Builder hb = new Headers.Builder();
        if (headerMap != null) {
            for (Map.Entry<String, String> entry : headerMap.entrySet()) {
                hb.add(entry.getKey(), entry.getValue());
            }
        }
        return hb.build();
    }
}
