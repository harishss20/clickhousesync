package com.learn.server;

import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "http://localhost:3000")
public class Controller {

    private final ClickHouseService clickHouseService = new ClickHouseService();

    // ClickHouse Connection APIs
    @PostMapping("/connect-clickhouse")
    public String connectToClickHouse(@RequestBody Map<String, String> request) {
        String host = request.get("host");
        String port = request.get("port");
        String database = request.get("database");
        String user = request.get("user");
        String password = request.get("password");

        try {
            Connection connection = clickHouseService.connectToClickHouse(host, port, database, user, password);
            return "Connected to ClickHouse successfully!";
        } catch (SQLException e) {
            return "Failed to connect to ClickHouse: " + e.getMessage();
        }
    }

    // ClickHouse Schema APIs
    @GetMapping("/clickhouse-schema")
    public List<String> getClickHouseSchema(@RequestParam String host, @RequestParam String port,
                                            @RequestParam String database, @RequestParam String user,
                                            @RequestParam String password, @RequestParam String tableName) {
        try {
            Connection connection = clickHouseService.connectToClickHouse(host, port, database, user, password);
            return clickHouseService.getTableSchema(connection, tableName);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch schema: " + e.getMessage());
        }
    }

    @PostMapping("/flatfile-schema")
    public String getFlatFileSchema(@RequestParam String filePath, @RequestParam String delimiter) {
        try {
            return "Schema fetched successfully for file: " + filePath;
        } catch (Exception e) {
            return "Failed to fetch schema: " + e.getMessage();
        }
    }

    // ClickHouse Export APIs
    @PostMapping("/export-clickhouse-to-flatfile")
    public String exportClickHouseToFlatFile(@RequestBody Map<String, String> request) {
        String tableName = request.get("tableName");
        String fileName = request.get("fileName");

        if (tableName == null || fileName == null) {
            return "Error: Missing required parameters 'tableName' or 'fileName'.";
        }

        try {
            Connection connection = clickHouseService.connectToClickHouse("localhost", "8123", "default", "default", "1234");
            ResultSet resultSet = clickHouseService.fetchData(connection, tableName);

            String downloadsDir = System.getProperty("user.home") + "\\Downloads";
            Path filePath = Paths.get(downloadsDir, fileName);

            try (FileWriter writer = new FileWriter(filePath.toFile())) {
                int columnCount = resultSet.getMetaData().getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    writer.append(resultSet.getMetaData().getColumnName(i));
                    if (i < columnCount) writer.append(",");
                }
                writer.append("\n");

                while (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        writer.append(resultSet.getString(i));
                        if (i < columnCount) writer.append(",");
                    }
                    writer.append("\n");
                }
            }

            return "Data exported successfully from ClickHouse table '" + tableName + "' to file: " + filePath.toString();
        } catch (Exception e) {
            return "Failed to export data: " + e.getMessage();
        }
    }

    @PostMapping("/ingest-data")
    public String ingestData(@RequestParam String source, @RequestParam String target,
                             @RequestParam(required = false) String columns) {
        System.out.println("Received request parameters:");
        System.out.println("Source: " + source);
        System.out.println("Target: " + target);
        System.out.println("Columns: " + columns);

        if ("clickhouse".equalsIgnoreCase(source) && target.endsWith(".csv")) {
            try {
                // Connect to ClickHouse
                Connection connection = clickHouseService.connectToClickHouse("localhost", "8123", "default", "default", "1234");

                // Fetch data from the specified table
                ResultSet resultSet = clickHouseService.fetchData(connection, target.replace(".csv", ""));

                // Define a file path to write the data
                String tempDir = System.getProperty("java.io.tmpdir");
                Path filePath = Paths.get(tempDir, target);

                // Write data to the file
                try (FileWriter writer = new FileWriter(filePath.toFile())) {
                    int columnCount = resultSet.getMetaData().getColumnCount();

                    // Write header row
                    for (int i = 1; i <= columnCount; i++) {
                        writer.append(resultSet.getMetaData().getColumnName(i));
                        if (i < columnCount) writer.append(",");
                    }
                    writer.append("\n");

                    // Write data rows
                    while (resultSet.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            writer.append(resultSet.getString(i));
                            if (i < columnCount) writer.append(",");
                        }
                        writer.append("\n");
                    }
                }

                return "Data exported successfully from ClickHouse to file: " + filePath.toString();
            } catch (Exception e) {
                e.printStackTrace();
                return "Failed to export data: " + e.getMessage();
            }
        } else if ("clickhouse".equalsIgnoreCase(source)) {
            try {
                // Connect to ClickHouse
                Connection connection = clickHouseService.connectToClickHouse("localhost", "8123", "default", "default", "1234");

                // Fetch data from the specified table
                ResultSet resultSet = clickHouseService.fetchData(connection, target);

                // Define a file path to write the data
                String tempDir = System.getProperty("java.io.tmpdir");
                Path filePath = Paths.get(tempDir, target + "_data.csv");

                // Write data to the file
                try (FileWriter writer = new FileWriter(filePath.toFile())) {
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    while (resultSet.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            writer.append(resultSet.getString(i));
                            if (i < columnCount) writer.append(",");
                        }
                        writer.append("\n");
                    }
                }

                return "Data ingestion completed successfully from " + source + " to file: " + filePath.toString();
            } catch (Exception e) {
                e.printStackTrace();
                return "Failed to ingest data: " + e.getMessage();
            }
        } else if ("flatfile".equalsIgnoreCase(source)) {
            try {
                // Process flat file ingestion
                Path filePath = Paths.get(System.getProperty("java.io.tmpdir"), target);

                // Read the flat file
                List<String> lines = Files.readAllLines(filePath);
                System.out.println("File content:");
                for (String line : lines) {
                    System.out.println(line);
                }

                // Insert data into ClickHouse
                Connection connection = clickHouseService.connectToClickHouse("localhost", "8123", "default", "default", "1234");
                String insertQuery = "INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)";

                try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                    for (int i = 1; i < lines.size(); i++) { // Skip header row
                        String[] values = lines.get(i).split(",");
                        preparedStatement.setInt(1, Integer.parseInt(values[0]));
                        preparedStatement.setString(2, values[1]);
                        preparedStatement.setInt(3, Integer.parseInt(values[2]));
                        preparedStatement.addBatch();
                    }
                    preparedStatement.executeBatch();
                }

                return "Flat file ingestion completed successfully. Inserted " + (lines.size() - 1) + " rows into ClickHouse.";
            } catch (Exception e) {
                e.printStackTrace();
                return "Failed to ingest flat file data: " + e.getMessage();
            }
        } else {
            return "Error: Unsupported source type. Source must be 'clickhouse' or 'flatfile'.";
        }
    }

    @PostMapping("/clickhouse-ingest")
    public String ingestToClickHouse(@RequestParam String host, @RequestParam String port,
                                      @RequestParam String database, @RequestParam String user,
                                      @RequestParam String jwtToken, @RequestParam String tableName,
                                      @RequestBody List<String> columns, @RequestBody ResultSet data) {
        try {
            Connection connection = clickHouseService.connectToClickHouse(host, port, database, user, jwtToken);
            int count = clickHouseService.ingestData(connection, tableName, columns, data);
            return "Successfully ingested " + count + " records into ClickHouse.";
        } catch (SQLException e) {
            return "Failed to ingest data: " + e.getMessage();
        }
    }

    @PostMapping("/upload-file")
    public String uploadFile(@RequestParam("file") MultipartFile file) {
        try {
            // Save the file to a temporary location
            String tempDir = System.getProperty("java.io.tmpdir");
            Path filePath = Paths.get(tempDir, file.getOriginalFilename());
            Files.write(filePath, file.getBytes());

            // Process the file (e.g., read its content)
            List<String> lines = Files.readAllLines(filePath);
            System.out.println("File content:");
            lines.forEach(System.out::println);

            return "File uploaded and processed successfully.";
        } catch (IOException e) {
            return "Failed to upload file: " + e.getMessage();
        }
    }
}