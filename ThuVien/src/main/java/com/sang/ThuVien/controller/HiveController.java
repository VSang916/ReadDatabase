package com.sang.ThuVien.controller;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/hive")
public class HiveController {

    private static final String HIVE_JDBC_URL = "jdbc:hive2://localhost:10000/default";
    private static final String HIVE_USER = "";
    private static final String HIVE_PASSWORD = "";

    private Connection getConnection() throws SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Hive JDBC Driver not found", e);
        }
        return DriverManager.getConnection(HIVE_JDBC_URL, HIVE_USER, HIVE_PASSWORD);
    }

    @GetMapping("/databases")
    public ResponseEntity<List<String>> getDatabases() {
        List<String> databases = new ArrayList<>();
        try (Connection connection = getConnection();
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW DATABASES")) {

            while (rs.next()) {
                databases.add(rs.getString(1));
            }
        } catch (SQLException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
        return ResponseEntity.ok(databases);
    }

    @GetMapping("/{database}/tables")
    public ResponseEntity<List<String>> getTables(@PathVariable String database) {
        List<String> tables = new ArrayList<>();
        try (Connection connection = getConnection();
             Statement stmt = connection.createStatement()) {

            stmt.execute("USE " + database);
            try (ResultSet rs = stmt.executeQuery("SHOW TABLES")) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
        return ResponseEntity.ok(tables);
    }

    @GetMapping("/{database}/{table}/structure")
    public ResponseEntity<List<Map<String, Object>>> getTableStructure(@PathVariable String database, @PathVariable String table) {
        List<Map<String, Object>> tableStructure = new ArrayList<>();
        try (Connection connection = getConnection();
             Statement stmt = connection.createStatement()) {

            stmt.execute("USE " + database);
            // Query to get the column structure of the table
            try (ResultSet rs = stmt.executeQuery("DESCRIBE " + table)) {
                while (rs.next()) {
                    Map<String, Object> columnDetails = new HashMap<>();
                    columnDetails.put("column_name", rs.getString("col_name"));
                    columnDetails.put("data_type", rs.getString("data_type"));
                    columnDetails.put("comment", rs.getString("comment"));
                    tableStructure.add(columnDetails);
                }
            }
        } catch (SQLException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
        return ResponseEntity.ok(tableStructure);
    }

    @GetMapping("/{database}/{table}/preview")
    public ResponseEntity<List<Map<String, Object>>> previewTable(@PathVariable String database, @PathVariable String table) {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (Connection connection = getConnection();
             Statement stmt = connection.createStatement()) {

            stmt.execute("USE " + database);
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " LIMIT 10")) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(metaData.getColumnName(i), rs.getObject(i));
                    }
                    rows.add(row);
                }
            }
        } catch (SQLException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
        return ResponseEntity.ok(rows);
    }

    @PostMapping("/create-view")
    public ResponseEntity<String> createView(@RequestBody Map<String, String> request) {
        String viewName = request.get("viewName");
        String database = request.get("database");
        String selectQuery = request.get("selectQuery");

        if (viewName == null || selectQuery == null || database == null) {
            return ResponseEntity.badRequest().body("Missing required parameters: viewName, database, selectQuery");
        }

        try (Connection connection = getConnection();
             Statement stmt = connection.createStatement()) {

            stmt.execute("USE " + database);
            String createViewQuery = "CREATE VIEW " + viewName + " AS " + selectQuery;
            stmt.execute(createViewQuery);
        } catch (SQLException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error creating view: " + e.getMessage());
        }

        return ResponseEntity.ok("View created successfully");
    }
}


