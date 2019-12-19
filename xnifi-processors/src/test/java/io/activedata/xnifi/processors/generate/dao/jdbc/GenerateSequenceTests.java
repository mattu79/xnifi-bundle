package io.activedata.xnifi.processors.generate.dao.jdbc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.activedata.xnifi.processors.generate.GenerateSequence;
import io.activedata.xnifi.test.support.DBCPServiceSimpleImpl;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.mockito.Mockito.spy;

/**
 * Created by MattU on 2017/12/15.
 */
public class GenerateSequenceTests {

    private DBCPService dbcp;

    private final static String DB_LOCATION = "target/test_db";

    TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(GenerateSequence.class);
        dbcp = spy(new DBCPServiceSimpleImpl());
        runner.addControllerService("dbcp", dbcp);
        runner.enableControllerService(dbcp);
        runner.setProperty(GenerateSequence.PROP_DBCP_SERVICE, "dbcp");
    }

    /**
     * 测试序列的初始化和多次执行
     * @throws IOException
     * @throws MalformedRecordException
     */
    @Test
    public void test0() throws IOException, MalformedRecordException{
        runner.setProperty(GenerateSequence.PROP_SEQ_CODE,"testSeq");
//        runner.setProperty(GenerateSequence.PROP_BATCH_SIZE,"1");
//        runner.setProperty(GenerateSequence.PROP_SEQ_FORMAT,"yyyy-MM-dd");
        runner.setProperty(GenerateSequence.PROP_SEQ_INIT_VALUE,"2018-01-01");
//        runner.setProperty(GenerateSequence.PROP_SEQ_STEP,"1D");

        runner.run(); // 测试前请确认序列未在表中存在

        runner.assertAllFlowFilesTransferred(GenerateSequence.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GenerateSequence.REL_SUCCESS);
        String value = getProperty(flowFiles, 0, "value");
        String prevValue = getProperty(flowFiles, 0, "prevValue");
        Assert.assertEquals(value, "2018-01-01");
        Assert.assertTrue(prevValue == null);

        runner.run();
        runner.assertAllFlowFilesTransferred(GenerateSequence.REL_SUCCESS, 2);
        flowFiles = runner.getFlowFilesForRelationship(GenerateSequence.REL_SUCCESS);
        prevValue = getProperty(flowFiles, 1, "prevValue");
        value = getProperty(flowFiles, 1, "value");
        Assert.assertEquals(prevValue, "2018-01-01");
        Assert.assertEquals(value, "2018-01-02");
    }

    /**
     * 测试序列的批量生成
     * @throws IOException
     * @throws MalformedRecordException
     */
    @Test
    public void test1() throws IOException, MalformedRecordException {
        runner.setProperty(GenerateSequence.PROP_SEQ_CODE,"testSeq1");
        runner.setProperty(GenerateSequence.PROP_BATCH_SIZE,"10");
        runner.setProperty(GenerateSequence.PROP_SEQ_FORMAT,"yyyy-MM-dd");
        runner.setProperty(GenerateSequence.PROP_SEQ_INIT_VALUE,"2017-06-01");
        runner.setProperty(GenerateSequence.PROP_SEQ_MAX_VALUE,"2017-12-30");
        runner.setProperty(GenerateSequence.PROP_SEQ_STEP,"1D");

        runner.run(); // 测试前请确认序列未在表中存在
        runner.assertAllFlowFilesTransferred(GenerateSequence.REL_SUCCESS, 10);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GenerateSequence.REL_SUCCESS);
        String value1 = getProperty(flowFiles, 0, "value");
        String value5 = getProperty(flowFiles, 4, "value");
        String value10 = getProperty(flowFiles, 9, "value");
        Assert.assertEquals(value1, "2017-06-01");
        Assert.assertEquals(value5, "2017-06-05");
        Assert.assertEquals(value10, "2017-06-10");
    }

    /**
     * 测试序列的最大值
     * @throws IOException
     * @throws MalformedRecordException
     */
    @Test
    public void test2() throws IOException, MalformedRecordException {
        runner.setProperty(GenerateSequence.PROP_SEQ_CODE,"testSeq2");
        runner.setProperty(GenerateSequence.PROP_BATCH_SIZE,"100");
        runner.setProperty(GenerateSequence.PROP_SEQ_FORMAT,"yyyy-MM-dd");
        runner.setProperty(GenerateSequence.PROP_SEQ_INIT_VALUE,"2017-06-01");
        runner.setProperty(GenerateSequence.PROP_SEQ_MAX_VALUE,"2017-06-10");
        runner.setProperty(GenerateSequence.PROP_SEQ_STEP,"1D");

        runner.run(10); // 测试前请确认序列未在表中存在
        runner.assertAllFlowFilesTransferred(GenerateSequence.REL_SUCCESS, 10);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GenerateSequence.REL_SUCCESS);
        String value1 = getProperty(flowFiles, 0, "value");
        String value5 = getProperty(flowFiles, 4, "value");
        String value10 = getProperty(flowFiles, 9, "value");
        Assert.assertEquals(value1, "2017-06-01");
        Assert.assertEquals(value5, "2017-06-05");
        Assert.assertEquals(value10, "2017-06-10");

        runner.setProperty(GenerateSequence.PROP_SEQ_MAX_VALUE,"2017-06-10");
    }

    /**
     * 测试在属性max value中使用MVEL表达式@{}
     * @throws IOException
     * @throws MalformedRecordException
     */
    @Test
    public void test3() throws IOException, MalformedRecordException {
        runner.setProperty(GenerateSequence.PROP_SEQ_CODE,"testSeq3");
        runner.setProperty(GenerateSequence.PROP_BATCH_SIZE,"5");
        runner.setProperty(GenerateSequence.PROP_SEQ_FORMAT,"yyyy-MM-dd");
        runner.setProperty(GenerateSequence.PROP_SEQ_INIT_VALUE,"2017-06-01");
        runner.setProperty(GenerateSequence.PROP_SEQ_MAX_VALUE,"@{$Dates.before('1Y')}");
        runner.setProperty(GenerateSequence.PROP_SEQ_STEP,"1D");

        runner.run(5); // 测试前请确认序列未在表中存在
        runner.assertAllFlowFilesTransferred(GenerateSequence.REL_SUCCESS, 25);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GenerateSequence.REL_SUCCESS);
        String value1 = getProperty(flowFiles, 0, "value");
        String value5 = getProperty(flowFiles, 4, "value");
        String value10 = getProperty(flowFiles, 24, "value");
        String maxValue = getProperty(flowFiles, 0, "maxValue");
        Assert.assertEquals(value1, "2017-06-01");
        Assert.assertEquals(value5, "2017-06-05");
        Assert.assertEquals(value10, "2017-06-25");
        System.err.println("max_value: " + maxValue);
    }

    /**
     * 测试在使用format未yyyyMMdd出现的格式不匹配问题
     * @throws IOException
     * @throws MalformedRecordException
     */
    @Test
    public void test4() throws IOException, MalformedRecordException{
        runner.setProperty(GenerateSequence.PROP_SEQ_CODE,"testSeq4");
        runner.setProperty(GenerateSequence.PROP_BATCH_SIZE,"1");
        runner.setProperty(GenerateSequence.PROP_SEQ_FORMAT,"yyyyMMdd");
        runner.setProperty(GenerateSequence.PROP_SEQ_INIT_VALUE,"20170601");
        runner.setProperty(GenerateSequence.PROP_SEQ_STEP,"1D");

        runner.run();
        runner.assertAllFlowFilesTransferred(GenerateSequence.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GenerateSequence.REL_SUCCESS);
        String value1 = getProperty(flowFiles, 0, "value");
        Assert.assertEquals(value1, "20170601");
    }

    private String getProperty(List<MockFlowFile> flowFiles, int index, String propertyName) throws IOException, MalformedRecordException{
        MockFlowFile flowFile = flowFiles.get(index);
        Assert.assertNotNull(flowFile);
        InputStream is = new ByteArrayInputStream(flowFile.toByteArray());
        String content = IOUtils.toString(is, "utf-8");
        JSONObject record = JSON.parseObject(content);
        return record.getString(propertyName);
//        AvroReaderWithEmbeddedSchema reader = new AvroReaderWithEmbeddedSchema(is);
//        Record record = reader.nextRecord();
//        return record.getAsString(propertyName);
    }

//    @BeforeClass
//    public static void setupBeforeClass() throws IOException {
//        System.setProperty("derby.stream.error.file", "target/derby.log");
//
//        // remove previous test database, if any
//        final File dbLocation = new File(DB_LOCATION);
//        try {
//            FileUtils.deleteFile(dbLocation, true);
//        } catch (IOException ignore) {
//            // Do nothing, may not have existed
//        }
//    }

//    @AfterClass
//    public static void cleanUpAfterClass() throws Exception {
//        try {
//            DriverManager.getConnection("derby:" + DB_LOCATION + ";shutdown=true");
//        } catch (SQLNonTransientConnectionException ignore) {
//            // Do nothing, this is what happens at Derby shutdown
//        }
//        // remove previous test database, if any
//        final File dbLocation = new File(DB_LOCATION);
//        try {
//            FileUtils.deleteFile(dbLocation, true);
//        } catch (IOException ignore) {
//            // Do nothing, may not have existed
//        }
//    }
}
