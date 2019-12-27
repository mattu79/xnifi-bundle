package io.activedata.xnifi2.processors.enrich;

import io.activedata.xnifi.test.support.DBCPServiceSimpleImpl;
import io.activedata.xnifi2.support.sql2o.Sql2oHelper;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class EnrichRecordTest {

    private String DB_LOCATION = "test.db";
    private DBCPService dbcp;
    private SqlLookupService sqlLookupService = new SqlLookupService();
    private TestRunner testRunner;

    @Before
    public void setup() throws InitializationException {
        try {
            FileUtils.deleteFile(new File(DB_LOCATION), true);
        } catch (IOException ioe) {
        }
        dbcp = new DBCPServiceSimpleImpl(DB_LOCATION);
        Sql2o sql2o = Sql2oHelper.create(dbcp);
        try(Connection conn = sql2o.open()){
//            conn.createQuery("DROP TABLE IF EXISTS ags_match").executeUpdate();
            conn.createQuery("CREATE TABLE ags_match (" +
                    "id VARCHAR(255) NOT NULL, " +
                    "title VARCHAR(255) NOT NULL, " +
                    "PRIMARY KEY (id)" +
                    ")").executeUpdate();
            conn.createQuery("INSERT INTO ags_match VALUES ('1', 'title1')").executeUpdate();
            conn.createQuery("INSERT INTO ags_match VALUES ('2', 'title1')").executeUpdate();
            conn.createQuery("INSERT INTO ags_match VALUES ('3', 'title1')").executeUpdate();
            conn.createQuery("INSERT INTO ags_match VALUES ('4', 'title1')").executeUpdate();
            conn.createQuery("INSERT INTO ags_match VALUES ('5', 'title1')").executeUpdate();
        }
        testRunner = TestRunners.newTestRunner(EnrichRecord.class);

        testRunner.addControllerService("dbcp", dbcp);
        testRunner.addControllerService("lookup-service", sqlLookupService);
        testRunner.setProperty(EnrichRecord.PROP_INPUT_RECORD_TYPE, EnrichRecord.RECORD_TYPE_JSON);
        testRunner.setProperty(EnrichRecord.PROP_LOOKUP_SERVICE, "lookup-service");
        testRunner.setProperty(EnrichRecord.PROP_ROUTING_STRATEGY, EnrichRecord.ROUTE_TO_MATCHED_UNMATCHED);
        testRunner.setProperty(sqlLookupService, SqlLookupService.PROP_DBCP_SERVICE, "dbcp");
        testRunner.setProperty(sqlLookupService, SqlLookupService.PROP_LOOKUP_SQL, "SELECT * FROM ags_match WHERE id = :id");
        testRunner.enableControllerService(dbcp);
        testRunner.enableControllerService(sqlLookupService);
    }

    @Test
    public void test1() {
        testRunner.enqueue("[{\"id\": 3},{\"id\": 1}, {\"id\": 3}]");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EnrichRecord.REL_MATCHED, 1);
        List<MockFlowFile> mockFlowFiles = testRunner.getFlowFilesForRelationship(EnrichRecord.REL_MATCHED);
        MockFlowFile mockFlowFile = mockFlowFiles.get(0);
        mockFlowFile.assertContentEquals("");
    }
}