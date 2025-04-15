package com.learn.server;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class ServerApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    public void testIngestData() {
        Controller controller = new Controller();
        String response = controller.ingestData("clickhouse", "test_table", null);
        assertEquals("Data ingestion completed successfully from clickhouse to file: ", response.substring(0, 55));
    }
}
