package com.learn.server;

import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
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
    public String ingestData(@RequestParam("file") MultipartFile file, @RequestParam("target") String target) {
        try {
            // Save the uploaded file to a temporary location
            Path tempFile = Files.createTempFile("uploaded", file.getOriginalFilename());
            Files.write(tempFile, file.getBytes());

            // Read the file content
            List<String> lines = Files.readAllLines(tempFile);
            if (lines.isEmpty()) {
                return "Error: Uploaded file is empty.";
            }

            // Connect to ClickHouse and fetch the target table schema
            try (Connection connection = clickHouseService.connectToClickHouse("localhost", "8123", "default", "default", "1234")) {
                List<String> tableSchema = clickHouseService.getTableSchema(connection, target);

                // Validate the CSV file against the table schema
                String[] headers = lines.get(0).split(",");
                if (headers.length != tableSchema.size()) {
                    return "Error: The number of columns in the CSV file does not match the schema of the target table '" + target + "'.";
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

            return "Flat file ingestion completed successfully.";
        } catch (Exception e) {
            return "Failed to ingest flat file data: " + e.getMessage();
        }
    }

    @PostMapping("/clickhouse-ingest")
    public String ingestToClickHouse(@RequestParam String host, @RequestParam String port,
                                      @RequestParam String database, @RequestParam String user,
                                      @RequestParam String jwtToken, @RequestParam String tableName,
                                      @RequestBody List<String> columns, @RequestBody ResultSet data) {
        try (Connection connection = clickHouseService.connectToClickHouse(host, port, database, user, jwtToken)) {
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

    @PostMapping("/join-clickhouse-tables")
    public String joinClickHouseTables(@RequestBody Map<String, Object> request) {
        String table1 = (String) request.get("table1");
        String table2 = (String) request.get("table2");
        String joinCondition = (String) request.get("joinCondition");
        String targetTable = (String) request.get("targetTable");
        Object columnsObj = request.get("columns");
        List<String> selectedColumns = columnsObj instanceof List ? (List<String>) columnsObj : new ArrayList<>();

        if (table1 == null || table2 == null || joinCondition == null || targetTable == null || selectedColumns.isEmpty()) {
            return "Error: Missing required parameters 'table1', 'table2', 'joinCondition', 'targetTable', or 'columns'.";
        }

        try (Connection connection = clickHouseService.connectToClickHouse("localhost", "8123", "default", "default", "1234")) {
            // Validate selected columns against the schemas of both tables
            List<String> table1Schema = clickHouseService.getTableSchema(connection, table1);
            List<String> table2Schema = clickHouseService.getTableSchema(connection, table2);

            for (String column : selectedColumns) {
                if (!table1Schema.contains(column) && !table2Schema.contains(column)) {
                    return "Error: Column '" + column + "' does not exist in either table '" + table1 + "' or '" + table2 + "'.";
                }
            }

            // Disambiguate column names by adding unique aliases
            StringBuilder aliasedColumns = new StringBuilder();
            for (String column : selectedColumns) {
                if (table1Schema.contains(column)) {
                    aliasedColumns.append(table1).append(".").append(column).append(" AS ").append(table1).append("_").append(column).append(",");
                } else if (table2Schema.contains(column)) {
                    aliasedColumns.append(table2).append(".").append(column).append(" AS ").append(table2).append("_").append(column).append(",");
                }
            }
            // Remove the trailing comma
            String columnList = aliasedColumns.substring(0, aliasedColumns.length() - 1);

            String query = "CREATE TABLE " + targetTable + " ENGINE = MergeTree() ORDER BY tuple() AS " +
                           "SELECT " + columnList + " FROM " + table1 + " INNER JOIN " + table2 + " ON " + joinCondition;

            clickHouseService.executeQuery(connection, query);

            return "Tables '" + table1 + "' and '" + table2 + "' joined successfully into '" + targetTable + "'.";
        } catch (Exception e) {
            return "Failed to join tables: " + e.getMessage();
        }
    }

    @PostMapping("/join-and-export")
    public String joinAndExportToCSV(@RequestBody Map<String, Object> request) {
        String table1 = (String) request.get("table1");
        String table2 = (String) request.get("table2");
        String joinCondition = (String) request.get("joinCondition");
        Object columnsObj = request.get("columns");
        List<String> selectedColumns = columnsObj instanceof List ? (List<String>) columnsObj : new ArrayList<>();
        String fileName = (String) request.get("fileName");

        if (table1 == null || table2 == null || joinCondition == null || selectedColumns.isEmpty() || fileName == null) {
            return "Error: Missing required parameters 'table1', 'table2', 'joinCondition', 'columns', or 'fileName'.";
        }

        try (Connection connection = clickHouseService.connectToClickHouse("localhost", "8123", "default", "default", "1234")) {
            // Disambiguate column names by adding table prefixes as aliases
            StringBuilder aliasedColumns = new StringBuilder();
            for (String column : selectedColumns) {
                if (column.contains(".")) {
                    aliasedColumns.append(column).append(" AS ").append(column.replace(".", "_")).append(",");
                } else {
                    aliasedColumns.append(table1).append(".").append(column).append(" AS ").append(table1).append("_").append(column).append(",");
                }
            }
            // Remove the trailing comma
            String columnList = aliasedColumns.substring(0, aliasedColumns.length() - 1);

            // Construct the JOIN query with aliased columns
            String query = "SELECT " + columnList + " FROM " + table1 + " INNER JOIN " + table2 + " ON " + joinCondition;

            ResultSet resultSet = clickHouseService.executeQuery(connection, query);

            // Define the file path for the CSV file
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

            return "Data joined and exported successfully to file: " + filePath.toString();
        } catch (Exception e) {
            return "Failed to join and export data: " + e.getMessage();
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

    @PostMapping("/ingest-with-progress")
    public void ingestWithProgress(@RequestBody Map<String, Object> request, HttpServletResponse response) {
        String filePath = (String) request.get("filePath");
        Object columnsObj = request.get("columns");
        List<String> selectedColumns = columnsObj instanceof List ? (List<String>) columnsObj : new ArrayList<>();

        if (filePath == null || selectedColumns.isEmpty()) {
            throw new IllegalArgumentException("Missing required parameters 'filePath' or 'columns'.");
        }

        try {
            Path path = Paths.get(filePath);
            List<String> lines = Files.readAllLines(path);

            if (lines.isEmpty()) {
                throw new RuntimeException("File is empty.");
            }

            response.setContentType("text/event-stream");
            response.setCharacterEncoding("UTF-8");

            PrintWriter writer = response.getWriter();

            int totalLines = lines.size();
            for (int i = 0; i < totalLines; i++) {
                // Simulate ingestion process
                Thread.sleep(50); // Simulate processing time

                // Send progress update
                int progress = (int) (((double) (i + 1) / totalLines) * 100);
                writer.write("data: " + progress + "\n\n");
                writer.flush();
            }

            writer.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to ingest data with progress: " + e.getMessage());
        }
    }
}