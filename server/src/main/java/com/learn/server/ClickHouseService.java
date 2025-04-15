package com.learn.server;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ClickHouseService {

    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://<host>:<port>/<database>";

    public Connection connectToClickHouse(String host, String port, String database, String user, String password) throws SQLException {
        String url = CLICKHOUSE_URL.replace("<host>", host).replace("<port>", port).replace("<database>", database);

        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password); // Use the password here

        return DriverManager.getConnection(url, properties);
    }

    public List<String> getTableSchema(Connection connection, String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();
        String query = "DESCRIBE TABLE " + tableName;
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                columns.add(rs.getString("name"));
            }
        }
        return columns;
    }

    public int ingestData(Connection connection, String tableName, List<String> columns, ResultSet data) throws SQLException {
        String columnList = String.join(",", columns);
        String query = "INSERT INTO " + tableName + " (" + columnList + ") VALUES (" + "?".repeat(columns.size()).replace("", ",") + ")";

        try (PreparedStatement pstmt = connection.prepareStatement(query)) {
            int count = 0;
            while (data.next()) {
                for (int i = 0; i < columns.size(); i++) {
                    pstmt.setObject(i + 1, data.getObject(columns.get(i)));
                }
                pstmt.addBatch();
                count++;
            }
            pstmt.executeBatch();
            return count;
        }
    }

    public ResultSet fetchData(Connection connection, String tableName) throws SQLException {
        String query = "SELECT * FROM " + tableName;
        Statement stmt = connection.createStatement();
        return stmt.executeQuery(query);
    }
}