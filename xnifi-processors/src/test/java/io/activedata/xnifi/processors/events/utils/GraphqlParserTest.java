package io.activedata.xnifi.processors.events.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.activedata.xnifi.processors.events.utils.GraphqlParserUtils;
import jodd.bean.BeanUtil;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class GraphqlParserTest {

    /**
     * 测试简单的请求
     */
    @Test
    public void test1(){
        String query = "{\n" +
                "  adPage(limit: 10) {\n" +
                "    rows {\n" +
                "      title\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        Map req = GraphqlParserUtils.parseRequest(query, null);
        Assert.assertEquals("QUERY", req.get("type"));
        Assert.assertEquals("adPage", req.get("name"));
        Object limit = BeanUtil.silent.getProperty(req, "params.limit");
        Assert.assertEquals("10", limit);
    }

    /**
     * 测试有嵌套有数组的复杂的请求
     */
    @Test
    public void test2(){
        String query = "query {\n  orgUserPage(where: {and: [{uid: 4488653068}]}, order: [\"uid\",\"reverse:job\"]) {\n    rows {\n      job\n      org {\n        id\n        name\n        status\n      }\n    }\n  }\n}\n";
        Map req = GraphqlParserUtils.parseRequest(query, "");
        Assert.assertEquals("QUERY", req.get("type"));
        Assert.assertEquals("orgUserPage", req.get("name"));
        List<String> orders = BeanUtil.silent.getProperty(req, "params.order");
        Assert.assertEquals("reverse:job", orders.get(1));
        Assert.assertEquals("4488653068", BeanUtil.silent.getProperty(req, "params.where.and[0].uid"));
    }

    /**
     * 测试变更请求
     */
    @Test
    public void test3(){
        String query = "mutation {\n" +
                "  createAd(input: {position: HOME, title: \"123\", image: \"\", target: LINK, value: \"\", status: 1}) {\n" +
                "    title\n" +
                "  }\n" +
                "}";
        Map req = GraphqlParserUtils.parseRequest(query, "");
        Assert.assertEquals("MUTATION", req.get("type"));
        Assert.assertEquals("createAd", req.get("name"));
        Assert.assertEquals("HOME", BeanUtil.silent.getProperty(req, "params.input.position"));
        Assert.assertEquals("123", BeanUtil.silent.getProperty(req, "params.input.title"));
    }

    /**
     * 测试带变量的变更请求
     */
    @Test
    public void test4(){
        String query = "mutation addAd($title: String!) {\\n  createAd(input: {position: HOME, title: $title, image: \"\", target: LINK, value: \"\", status: 1}) {\\n    title\\n  }\\n}\\n";
        String variable = "{ title: 'xyz' }";
        String operationName = "test4";

        Map req = GraphqlParserUtils.parseRequest(query, variable);
        Assert.assertEquals("MUTATION", req.get("type"));
        Assert.assertEquals("addAd", req.get("name"));
        Assert.assertEquals("xyz", BeanUtil.silent.getProperty(req, "params.title"));

        System.err.println(req);
    }

    @Test
    public void test5(){
        String query = "\\n  query IntrospectionQuery {\\n    __schema {\\n      queryType { name }\\n      mutationType { name }\\n      subscriptionType { name }\\n      types {\\n        ...FullType\\n      }\\n      directives {\\n        name\\n        description\\n        locations\\n        args {\\n          ...InputValue\\n        }\\n      }\\n    }\\n  }\\n\\n  fragment FullType on __Type {\\n    kind\\n    name\\n    description\\n    fields(includeDeprecated: true) {\\n      name\\n      description\\n      args {\\n        ...InputValue\\n      }\\n      type {\\n        ...TypeRef\\n      }\\n      isDeprecated\\n      deprecationReason\\n    }\\n    inputFields {\\n      ...InputValue\\n    }\\n    interfaces {\\n      ...TypeRef\\n    }\\n    enumValues(includeDeprecated: true) {\\n      name\\n      description\\n      isDeprecated\\n      deprecationReason\\n    }\\n    possibleTypes {\\n      ...TypeRef\\n    }\\n  }\\n\\n  fragment InputValue on __InputValue {\\n    name\\n    description\\n    type { ...TypeRef }\\n    defaultValue\\n  }\\n\\n  fragment TypeRef on __Type {\\n    kind\\n    name\\n    ofType {\\n      kind\\n      name\\n      ofType {\\n        kind\\n        name\\n        ofType {\\n          kind\\n          name\\n          ofType {\\n            kind\\n            name\\n            ofType {\\n              kind\\n              name\\n              ofType {\\n                kind\\n                name\\n                ofType {\\n                  kind\\n                  name\\n                }\\n              }\\n            }\\n          }\\n        }\\n      }\\n    }\\n  }\\n";
        Map req = GraphqlParserUtils.parseRequest(query, "");
        System.err.println(req);
    }

    @Test
    public void testArticleDetailQuery(){
        String query = "query articleDetailQuery ($articleid: Long) {\n" +
                "  article(id: $articleid) {\n" +
                "    id\n" +
                "    title\n" +
                "    author\n" +
                "    org {\n" +
                "      id\n" +
                "      name\n" +
                "    }\n" +
                "    ArticleDetail{\n" +
                "      content\n" +
                "    }\n" +
                "  }\n" +
                "  commentPage(where: {targetId: $articleid}, limit: 5 ,order:\"reverse:createAt\") {\n" +
                "    count\n" +
                "    rows {\n" +
                "      content\n" +
                "      user {\n" +
                "        id\n" +
                "        username\n" +
                "        avatar\n" +
                "      }\n" +
                "      createAt\n" +
                "    }\n" +
                "  }\n" +
                "}";

        String variable = "{\"articleid\":\"1610075413\"}";

        Map req = GraphqlParserUtils.parseRequest(query, variable);
        System.err.println(req);
        Assert.assertEquals("QUERY", req.get("type"));
        Assert.assertEquals("articleDetailQuery", req.get("name"));
        Assert.assertEquals("1610075413", BeanUtil.silent.getProperty(req, "params.articleid"));
    }

    @Test
    public void testIllegalRequest(){
        String query = "{userPage(limit:15,offset:0,where:{mobile:的商务分任务同仁堂,username:{like:\"%小%\"}},order:\"reverse:createAt\") {count,limit,rows {id,status,username,mobile,avatar,games,detail {fans,orgs}}}}";
        try {
            Map req = GraphqlParserUtils.parseRequest(query, null);
        }catch (Exception e){
            Assert.assertTrue(e instanceof ParseCancellationException);
        }
    }
}
