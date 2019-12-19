package io.activedata.xnifi2.processors.sql;

import com.alibaba.fastjson.JSON;
import io.activedata.xnifi.test.support.DBCPServiceSimpleImpl;
import io.activedata.xnifi2.sql2o.Sql2oHelper;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

public class BatchSelectProcessorTests {

    private final static String TEST_JSON = "[\n" +
            "  {\n" +
            "    \"trackId\": \"44507a71-a68b-11e8-a897-3746e5131f51\",\n" +
            "    \"use\": \"24\",\n" +
            "    \"ip\": \"127.0.0.1\",\n" +
            "    \"pid\": \"18215\",\n" +
            "    \"name\": \"matt\"" +
            "  },\n" +
            "  {\n" +
            "    \"trackId\": \"469d77b1-a68b-11e8-8614-b797d0ae4769\",\n" +
            "    \"use\": \"9.0\",\n" +
            "    \"ip\": \"127.0.0.1\",\n" +
            "    \"pid\": \"18202\",\n" +
            "    \"name\": \"martin\"" +
            "  }\n" +
            "]";

    private final static String TEST_TABLE_SQL = "CREATE TABLE `TEST_TABLE` (\n" +
            "            `PID` INT(11) NOT NULL,\n" +
            "            `USE` VARCHAR(64) NOT NULL\n" +
            ");";

    DBCPService dbcpService;

    TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(SelectProcessor.class);
        dbcpService = new DBCPServiceSimpleImpl();
        runner.addControllerService("dbcp", dbcpService);
        runner.enableControllerService(dbcpService);
        runner.setProperty(SelectProcessor.PROP_DBCP_SERVICE, "dbcp");
        runner.setProperty(SelectProcessor.PROP_SQL, TEST_TABLE_SQL);
        runner.setProperty(SelectProcessor.PROP_RECORD_OUTPUT_BUILDER, "attributes.test = 'xxx';");
    }

    /**
     * 测试SQL中不存在参数的情况，只进行数据查询
     */
    @Test
    public void test1() throws IOException {
        runner.setProperty(SelectProcessor.PROP_SQL, "SELECT 1.5 AS `use`, 10001 AS pid \n" +
                "FROM DUAL");
        runner.enqueue(TEST_JSON);
        runner.run();
        runner.assertAllFlowFilesTransferred(SelectProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SelectProcessor.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);
        List<String> lines = IOUtils.readLines(new ByteArrayInputStream(flowFile.toByteArray()), Charsets.UTF_8);
        Assert.assertEquals(2, lines.size());
        Map record1 = JSON.parseObject(lines.get(0), Map.class);
        Assert.assertEquals(record1.get("use"), "1.5");
        Assert.assertEquals(record1.get("pid"), "10001");

        Map record2 = JSON.parseObject(lines.get(1), Map.class);
        Assert.assertEquals(record2.get("use"), "1.5");
        Assert.assertEquals(record2.get("pid"), "10001");
    }

    /**
     * 测试SQL中不存在参数的情况，只进行数据查询
     */
    @Test
    public void test2() throws IOException {
        String sql = "INSERT INTO user_edits (username, edits, total) VALUES (:name, :edits, :total)";
        Sql2o sql2o = Sql2oHelper.create(dbcpService);
        try(Connection con = sql2o.open()){
            con.createQuery("TRUNCATE TABLE user_edits").executeUpdate();
            Query query = con.createQuery(sql);
            query.addParameter("name", "matt")
                    .addParameter("edits", 3)
                    .addParameter("total", 10)
                    .addToBatch();

            query.addParameter("name", "martin")
                    .addParameter("edits", 1)
                    .addParameter("total", 3)
                    .addToBatch();
            query.executeBatch();
        }


        runner.setProperty(SelectProcessor.PROP_SQL, "SELECT edits, total FROM user_edits WHERE username = :name");
        runner.enqueue(TEST_JSON);
        runner.run();
        runner.assertAllFlowFilesTransferred(SelectProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SelectProcessor.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);
        List<String> lines = IOUtils.readLines(new ByteArrayInputStream(flowFile.toByteArray()), Charsets.UTF_8);
        Assert.assertEquals(2, lines.size());
        Map record1 = JSON.parseObject(lines.get(0), Map.class);
        Assert.assertEquals(record1.get("name"), "matt");
        Assert.assertEquals(record1.get("edits"), 3);
        Assert.assertEquals(record1.get("total"), 10);

        Map record2 = JSON.parseObject(lines.get(1), Map.class);
        Assert.assertEquals(record2.get("name"), "martin");
        Assert.assertEquals(record2.get("edits"), 1);
        Assert.assertEquals(record2.get("total"), 3);
    }

    /**
     * 测试插入数据的情况
     * @throws UnsupportedEncodingException
     */
    @Test
    public void test31() throws UnsupportedEncodingException {
        String sql = "INSERT INTO TEST_TABLE(`PID`, `USE`) VALUES(:pid, :use)";
        runner.setProperty(SelectProcessor.PROP_SQL, sql);
        runner.setProperty(SelectProcessor.PROP_SQL_MODE, "INSERT/UPDATE/DELETE");
        runner.enqueue("{}");
        runner.run();
        runner.assertAllFlowFilesTransferred(SelectProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SelectProcessor.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);
        String json = new String(flowFile.toByteArray(), "UTF-8");
        List<Map> records = JSON.parseArray(json, Map.class);
        Assert.assertEquals(2, records.size());
        Map record1 = records.get(0);
        Assert.assertEquals(record1.get("result"), Integer.valueOf(1));
    }

    /**
     * 测试更新数据的情况
     * @throws UnsupportedEncodingException
     */
    @Test
    public void test32() throws UnsupportedEncodingException {
        String sql = "UPDATE TEST_TABLE SET `USE`= :use WHERE PID = :pid";
        runner.setProperty(SelectProcessor.PROP_SQL, sql);
        runner.setProperty(SelectProcessor.PROP_SQL_MODE, "INSERT/UPDATE/DELETE");
        runner.enqueue(TEST_JSON);
        runner.run();
        runner.assertAllFlowFilesTransferred(SelectProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SelectProcessor.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);
        String json = new String(flowFile.toByteArray(), "UTF-8");
        System.err.println(json);
        List<Map> records = JSON.parseArray(json, Map.class);
        Assert.assertEquals(2, records.size());
        Map record1 = records.get(0);
        Assert.assertNotNull(record1.get("result"));
    }

    /**
     * 测试查询多条数据的情况
     * @throws UnsupportedEncodingException
     */
    @Test
    public void test4() throws UnsupportedEncodingException {
        String sql = "SELECT * FROM TEST_TABLE LIMIT 1";
        runner.setProperty(SelectProcessor.PROP_SQL, sql);
        runner.enqueue(TEST_JSON);
        runner.run();
        runner.assertAllFlowFilesTransferred(SelectProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SelectProcessor.REL_SUCCESS);
        MockFlowFile flowFile = flowFiles.get(0);
        String json = new String(flowFile.toByteArray(), "UTF-8");
        System.err.println(json);
        List<Map> records = JSON.parseArray(json, Map.class);
        Assert.assertEquals(2, records.size());
        Map record1 = records.get(0);
    }
}
