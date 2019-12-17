package io.activedata.xnifi.processors.generate.dao.jdbc;

import io.activedata.xnifi.DBCPServiceSimpleImpl;
import io.activedata.xnifi.processors.generate.sequence.Sequence;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.util.file.FileUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;

import static org.mockito.Mockito.spy;

/**
 * Created by MattU on 2017/12/14.
 */
public class SequenceDAOImplTests {

    private final static String DB_LOCATION = "target/test_db";

    private DBCPService dbcp;


    private SequenceDAOImpl dao;

    @Test
    public void testSaveSequence() {
        Sequence sequence = new Sequence();
        sequence.setStrategy("SEQ_NUMBER");
        sequence.setCode("seqCode123");
        sequence.setStep("1");
        sequence.setFormat("yyyy-MM-dd");
        sequence.setInitValue("100");
        sequence.setMaxValue("999");
        sequence.setValue("2");
        sequence.setPrevValue("1");
        dao.saveSequence(sequence);

        sequence = dao.getSequence("seqCode123");
        System.err.println(sequence);
        Assert.assertEquals("seqCode123", sequence.getCode());
        Assert.assertEquals("SEQ_NUMBER", sequence.getStrategy());
        Assert.assertEquals("1", sequence.getStep());
        Assert.assertEquals("yyyy-MM-dd", sequence.getFormat());
        Assert.assertEquals("100", sequence.getInitValue());
        Assert.assertEquals("999", sequence.getMaxValue());
        Assert.assertEquals("2", sequence.getValue());
        Assert.assertEquals("1", sequence.getPrevValue());
        Assert.assertNotNull(sequence.getLastUpdateTime());
    }

    @Before
    public void setUp() throws Exception {
        dbcp = spy(new DBCPServiceSimpleImpl());
        Connection connection = dbcp.getConnection();
        dao = new SequenceDAOImpl(connection);
//        dao.createTable();
    }

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ignore) {
            // Do nothing, may not have existed
        }
    }

    @AfterClass
    public static void cleanUpAfterClass() throws Exception {
//        try {
//            DriverManager.getConnection("jdbc:oracle:thin:@192.168.1.222:1521:orcl",);
//            DriverManager.getConnection("dbutils:derby:" + DB_LOCATION + ";shutdown=true");
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
    }
}
