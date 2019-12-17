package io.activedata.xnifi.processors.partition;

import io.activedata.xnifi.processors.generate.GenerateSequence;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;

import static org.mockito.Mockito.spy;

public class PartitionOnJsonTests {

    TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(PartitionOnJson.class);
        runner.setProperty(PartitionOnJson.PROP_PARTITION_EXPR, "input.?name");
    }
}
