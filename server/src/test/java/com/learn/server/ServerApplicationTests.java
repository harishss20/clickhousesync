package com.learn.server;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class ServerApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    public void testIngestData() throws Exception {
        Controller controller = new Controller();

        // Create a mock MultipartFile
        MockMultipartFile mockFile = new MockMultipartFile(
            "file",
            "test_band.csv",
            "text/csv",
            "code,band_name\n34,Band E\n35,Band F\n56,Band G\n37,Band F".getBytes()
        );

        // Call the ingestData method with the mock file and target table
        String response = controller.ingestData(mockFile, "test_table");

        // Assert the response
        assertEquals("Flat file ingestion completed successfully.", response);
    }
}
