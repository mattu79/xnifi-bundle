package io.activedata.xnifi.mock;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class MockFlowFileUtils {

    public static String getProperty(List<MockFlowFile> flowFiles, int index, String propertyName){
        MockFlowFile flowFile = flowFiles.get(index);
        Assert.assertNotNull(flowFile);
        InputStream is = new ByteArrayInputStream(flowFile.toByteArray());
        String content = null;
        try {
            content = IOUtils.toString(is, "utf-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        JSONObject record = JSON.parseObject(content);
        return record.getString(propertyName);
//        AvroReaderWithEmbeddedSchema reader = new AvroReaderWithEmbeddedSchema(is);
//        Record record = reader.nextRecord();
//        return record.getAsString(propertyName);
    }
}
