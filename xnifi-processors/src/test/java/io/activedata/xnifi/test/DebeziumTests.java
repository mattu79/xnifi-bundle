package io.activedata.xnifi.test;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class DebeziumTests {
    @Test
    public void test(){
        Configuration config = Configuration.create()
                /* begin engine properties */
                .with("connector.class",
                        "io.debezium.connector.mysql.MySqlConnector")
                .with("offset.storage",
                        "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename",
                        "/path/to/storage/offset.dat")
                .with("offset.flush.interval.ms", 60000)
                /* begin connector properties */
                .with("name", "my-sql-connector")
                .with("database.hostname", "127.0.0.1")
                .with("database.port", 3306)
                .with("database.user", "root")
                .with("database.password", "!QAZ2wsx")
                .with("server.id", 1)
                .with("database.server.name", "my_connector")
                .with("database.history",
                        "io.debezium.relational.history.FileDatabaseHistory")
                .with("database.history.file.filename",
                        "/path/to/storage/dbhistory.dat")
                .build();

// Create the engine with this configuration ...
        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .notifying(sourceRecord -> {
                    System.err.println(sourceRecord.toString());
                })
                .build();

// Run the engine asynchronously ...
        Executor executor = Executors.newWorkStealingPool(2);
        executor.execute(engine);

        int i = 0;
        while (i < 1000){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // At some later time ...
        engine.stop();
    }
}
