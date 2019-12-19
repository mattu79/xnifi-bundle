package io.activedata.xnifi2.processors.script;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ScriptProcessorTest {
    private static final String TEST_JSON = "[\n" +
            "  {\n" +
            "    \"trackId\": \"44507a71-a68b-11e8-a897-3746e5131f51\",\n" +
            "    \"use\": \"24\",\n" +
            "    \"ip\": \"127.0.0.1\",\n" +
            "    \"pid\": \"18215\",\n" +
            "    \"optField\": \"optValue\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"trackId\": \"469d77b1-a68b-11e8-8614-b797d0ae4769\",\n" +
            "    \"use\": \"9.0\",\n" +
            "    \"pid\": \"18202\"\n" +
            "  }\n" +
            "]";

    private static final String TEST_JSON1 = "[{\"id\":3, \"trackId\": \"123\"}, {\"id\": \"5\", \"ip\": \"127.0.01\", \"otherField\": 110}]";

    private static final String OUTPUT_EXAMPLE = "{\n" +
            "  \"trackId\": \"469d77b1-a68b-11e8-8614-b797d0ae4769\",\n" +
            "  \"ip\": \"127.0.0.1\",\n" +
            "  \"at\": \"0\",\n" +
            "  \"pid\": 18202\n" +
            "}";

    TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(ScriptProcessor.class);
        runner.setProperty(ScriptProcessor.PROP_INPUT_RECORD_TYPE, ScriptProcessor.RECORD_TYPE_JSON);
        runner.setProperty(ScriptProcessor.PROP_OUTPUT_RECORD_TYPE, ScriptProcessor.RECORD_TYPE_JSON);
        runner.setProperty(ScriptProcessor.PROP_OUTPUT_RECORD_EXAMPLE, OUTPUT_EXAMPLE);
        runner.setProperty(ScriptProcessor.PROP_OUTPUT_RECORD_STRATEGY, ScriptProcessor.WSS_BOTH_INPUT_AND_OUTPUT);
    }

    @Test
    public void test1(){
//        runner.enqueue(TEST_JSON);
//        runner.setProperty(ScriptProcessor.PROP_SCRIPT, "output.atTime1 = $Dates.now();output.ip = '127.0.0.1';output.pid = 1;output.other = input.optField.toString();");
        runner.enqueue(TEST_JSON1);
        runner.setProperty(ScriptProcessor.PROP_SCRIPT, "output.at = $Dates.now(); output.at1 = $Dates.timestamp();");

        runner.run();

        List<MockFlowFile> flowFileList = runner.getFlowFilesForRelationship(ScriptProcessor.REL_SUCCESS);
//        Assert.assertEquals(flowFileList.size(), 1);
        MockFlowFile flowFile = flowFileList.get(0);
        flowFile.assertContentEquals("");

        flowFileList = runner.getFlowFilesForRelationship(ScriptProcessor.REL_FAILURE);
//        Assert.assertEquals(flowFileList.size(), 1);
//        MockFlowFile flowFile = flowFileList.get(0);
//        flowFile.assertContentEquals("");
    }
}