package com.learn.server;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.http.HttpStatus;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import javax.servlet.http.HttpServletResponse;

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

    @GetMapping("/clickhouse-tables")
    public List<String> getClickHouseTables(@RequestParam String host, @RequestParam String port,
                                            @RequestParam String database, @RequestParam String user,
                                            @RequestParam String password) {
        try {
            Connection connection = clickHouseService.connectToClickHouse(host, port, database, user, password);
            return clickHouseService.getTables(connection);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch tables: " + e.getMessage());
        }
    }

    @PostMapping("/flatfile-schema")
    public List<String> getFlatFileSchema(@RequestParam String filePath, @RequestParam String delimiter) {
        try {
            Path path = Paths.get(filePath);
            List<String> lines = Files.readAllLines(path);
            if (lines.isEmpty()) {
                throw new RuntimeException("Flat file is empty.");
            }
            String[] headers = lines.get(0).split(delimiter);
            return Arrays.asList(headers);
        } catch (IOException e) {
            throw new RuntimeException("Failed to fetch schema: " + e.getMessage());
        }
    }

    // ClickHouse Export APIs
    @PostMapping("/export-clickhouse-to-flatfile")
    public String exportClickHouseToFlatFile(@RequestBody Map<String, Object> request) {
        String tableName = (String) request.get("tableName");
        String fileName = (String) request.get("fileName");
        Object columnsObj = request.get("columns");
        List<String> selectedColumns = columnsObj instanceof List ? (List<String>) columnsObj : new ArrayList<>();

        if (tableName == null || fileName == null || selectedColumns.isEmpty()) {
            return "Error: Missing required parameters 'tableName', 'fileName', or 'columns'.";
        }

        try (Connection connection = clickHouseService.connectToClickHouse("localhost", "8123", "default", "default", "1234")) {
            // Validate selected columns against the table schema
            List<String> tableSchema = clickHouseService.getTableSchema(connection, tableName);

            for (String column : selectedColumns) {
                if (!tableSchema.contains(column)) {
                    return "Error: Unknown column '" + column + "' in table '" + tableName + "'.";
                }
            }

            // Construct the SELECT query with only the selected columns
            String columnList = String.join(",", selectedColumns);
            String query = "SELECT " + columnList + " FROM " + tableName;

            ResultSet resultSet = clickHouseService.executeQuery(connection, query);

            String downloadsDir = System.getProperty("user.home") + "\\Downloads";
            Path filePath = Paths.get(downloadsDir, fileName);

            try (FileWriter writer = new FileWriter(filePath.toFile())) {
                // Write header row
                writer.append(String.join(",", selectedColumns));
                writer.append("\n");

                // Write data rows
                while (resultSet.next()) {
                    for (int i = 0; i < selectedColumns.size(); i++) {
                        writer.append(resultSet.getString(selectedColumns.get(i)));
                        if (i < selectedColumns.size() - 1) {
                            writer.append(",");
                        }
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
public ResponseEntity<Map<String, Object>> ingestData(
        @RequestParam("file") MultipartFile file,
        @RequestParam("target") String target) {
    Map<String, Object> response = new HashMap<>();
    try {
        // Save the uploaded file to a temporary location
        Path tempFile = Files.createTempFile("uploaded", file.getOriginalFilename());
        Files.write(tempFile, file.getBytes());

        // Read the file content
        List<String> lines = Files.readAllLines(tempFile);
        if (lines.isEmpty()) {
            response.put("error", "Uploaded file is empty.");
            return ResponseEntity.badRequest().body(response);
        }

        int totalRecords = lines.size() - 1; // Excluding header row

        // Connect to ClickHouse and fetch the target table schema
        try (Connection connection = clickHouseService.connectToClickHouse(
                "localhost", "8123", "default", "default", "1234")) {

            List<String> tableSchema = clickHouseService.getTableSchema(connection, target);

            // Validate the CSV file against the table schema
            String[] headers = lines.get(0).split(",");
            if (headers.length != tableSchema.size()) {
                response.put("error", "The number of columns in the CSV file does not match the schema of the target table '" + target + "'.");
                return ResponseEntity.badRequest().body(response);
            }

            // Construct the INSERT query dynamically
            String insertQuery = "INSERT INTO " + target + " (" + String.join(",", tableSchema) + ") VALUES (" +
                    String.join(",", tableSchema.stream().map(col -> "?").toArray(String[]::new)) + ")";

            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                for (int i = 1; i < lines.size(); i++) { // Skip header row
                    String[] values = lines.get(i).split(",");
                    for (int j = 0; j < tableSchema.size(); j++) {
                        preparedStatement.setObject(j + 1, values[j]);
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
        }

        response.put("message", "Flat file ingestion completed successfully.");
        response.put("totalRecords", totalRecords);
        return ResponseEntity.ok(response);

    } catch (Exception e) {
        response.put("error", "Failed to ingest flat file data: " + e.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}
   

    @PostMapping("/preview-data")
    public List<Map<String, Object>> previewData(@RequestBody Map<String, Object> request) {
        String tableName = (String) request.get("tableName");
        Object columnsObj = request.get("columns");
        List<String> selectedColumns = columnsObj instanceof List ? (List<String>) columnsObj : new ArrayList<>();

        if (tableName == null || selectedColumns.isEmpty()) {
            throw new IllegalArgumentException("Missing required parameters 'tableName' or 'columns'.");
        }

        try (Connection connection = clickHouseService.connectToClickHouse("localhost", "8123", "default", "default", "1234")) {
            // Validate selected columns against the table schema
            List<String> tableSchema = clickHouseService.getTableSchema(connection, tableName);

            for (String column : selectedColumns) {
                if (!tableSchema.contains(column)) {
                    throw new IllegalArgumentException("Error: Column '" + column + "' does not exist in table '" + tableName + "'.");
                }
            }

            String columnList = String.join(",", selectedColumns);
            String query = "SELECT " + columnList + " FROM " + tableName + " LIMIT 100";

            ResultSet resultSet = clickHouseService.executeQuery(connection, query);

            List<Map<String, Object>> previewData = new ArrayList<>();
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                for (String column : selectedColumns) {
                    row.put(column, resultSet.getObject(column));
                }
                previewData.add(row);
            }

            return previewData;
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch preview data: " + e.getMessage());
        }
    }

    
}