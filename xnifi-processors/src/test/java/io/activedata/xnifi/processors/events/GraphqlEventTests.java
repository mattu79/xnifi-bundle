package io.activedata.xnifi.processors.events;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.activedata.xnifi.processors.events.utils.GraphqlParserTest;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class GraphqlEventTests {
    TestRunner runner;
    String eventJson;

    @Before
    public void setup() throws InitializationException, IOException {
        runner = TestRunners.newTestRunner(GraphqlEvent.class);
        File jsonFile = FileUtils.toFile(GraphqlParserTest.class.getResource("/graphql/event1.json"));
        eventJson = FileUtils.readFileToString(jsonFile, "UTF-8");
    }

    @Test
    public void testIt() throws IOException, MalformedRecordException {
        runner.enqueue(eventJson);
        runner.run();
        runner.assertAllFlowFilesTransferred(GraphqlEvent.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GraphqlEvent.REL_SUCCESS);
        String name = getProperty(flowFiles, 0, "name");
        String category = getProperty(flowFiles, 0, "category");
        Assert.assertEquals(name, "articleDetailQuery");
        Assert.assertEquals(category, "profile");
    }

    private String getProperty(List<MockFlowFile> flowFiles, int index, String propertyName) throws IOException, MalformedRecordException {
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
}