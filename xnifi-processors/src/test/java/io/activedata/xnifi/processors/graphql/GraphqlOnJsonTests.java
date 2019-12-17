package io.activedata.xnifi.processors.graphql;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class GraphqlOnJsonTests {

    TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(GraphqlOnJson.class);
        runner.setProperty(GraphqlOnJson.PROP_GRAPHQL_URL, "http://localhost:8088/graphql");
    }

    @Test
    public void testQuery() throws IOException, MalformedRecordException {
        String payload = "query ($id: Long) {\n" +
                "  ad(id: $id) {\n" +
                "    id\n" +
                "    title\n" +
                "  }\n" +
                "}";

        StringBuilder sb = new StringBuilder();
        sb.append("variables.id = input.id;");
        sb.append("headers.token = '1';");
        runner.setProperty(GraphqlOnJson.PROP_GRAPHQL_PAYLOAD, payload);
        runner.setProperty(GraphqlOnJson.PROP_GRAPHQL_VARIABLES_BUILDER, sb.toString());
        runner.enqueue("[{id:3},{id:5}]");
        runner.run();

        runner.assertAllFlowFilesTransferred(GraphqlOnJson.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GraphqlOnJson.REL_SUCCESS);
        MockFlowFile mff = flowFiles.get(0);
        System.err.println(new String(mff.toByteArray(), "UTF-8"));
    }

    @Test
    public void testMutation() throws IOException, MalformedRecordException {
        String payload = "mutation createAd($input: AdInput) {\n" +
                "  createAd(input: $input) {\n" +
                "    id\n" +
                "    title\n" +
                "  }\n" +
                "}\n";

        StringBuilder sb = new StringBuilder();
        sb.append("variables.input = ['position': 'HOME'];");
        sb.append("variables.input.title = 'title.xxxxxxxxx';");
        sb.append("variables.input.image = 'image.yyyyyyyyy';");
        sb.append("variables.input.value = 'value.zzzzzzzzz';");
        sb.append("variables.input.target = 'LINK';");
        sb.append("variables.input.status = 0;");

        sb.append("headers['ags-uuid'] = 'ef2c3d3e3b244f5388afe34398495360';");
        sb.append("headers['platform'] = 'e34706c554c79a4cc3854b7bd7d66afd';");
        sb.append("headers['ags-uid'] = '7477568';");
        sb.append("headers['ags-token'] = 'e34706c554c79a4cc3854b7bd7d66afd';");
        sb.append("headers['platformHash'] = '8be46bdb6c3f4db257af01f2abaf9a79';");
        sb.append("headers['client-version'] = '1.0.0';");
        sb.append("headers['client-time'] = $Dates.toDigitalDate($Dates.now())");
        runner.setProperty(GraphqlOnJson.PROP_GRAPHQL_PAYLOAD, payload);
        runner.setProperty(GraphqlOnJson.PROP_GRAPHQL_VARIABLES_BUILDER, sb.toString());
        runner.enqueue("{}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GraphqlOnJson.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GraphqlOnJson.REL_SUCCESS);
        MockFlowFile mff = flowFiles.get(0);
        System.err.println(new String(mff.toByteArray(), "UTF-8"));
    }
}
