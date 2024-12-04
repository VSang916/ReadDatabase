package com.sang.ThuVien.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/sql")
public class SqlController {

    

    // // Postgres
    // Connection jdbcConnection(String db){
    //     String url="jdbc:postgresql://localhost:5432/"+db;
    //     String user="admin";
    //     String password = "admin123";
    //     try{
    //         Class.forName("org.postgresql.Driver");
    //         return DriverManager.getConnection(url, user, password);
    //     }catch(ClassNotFoundException e){
    //         System.out.println(e);
    //     }catch(SQLException e){
    //         System.out.println(e);
    //     }
    //     return null;
    // }

    // // MySQL
    // Connection jdbcConnection(String db){
    //     String url="jdbc:mysql://localhost:3307/"+db;
    //     String user="root";
    //     String password = "password";
    //     try{
    //         Class.forName("com.mysql.cj.jdbc.Driver");
    //         return DriverManager.getConnection(url, user, password);
    //     }catch(ClassNotFoundException e){
    //         System.out.println(e);
    //     }catch(SQLException e){
    //         System.out.println(e);
    //     }
    //     return null;
    // }

    // SQLServer
    Connection jdbcConnection(String db){
        String url="jdbc:sqlserver://localhost:1433;databaseName="+db+";encrypt=true;trustServerCertificate=true";
        String user="sa";
        String password = "password!@!123";
        try{
            Class.forName("org.hibernate.dialect.SQLServerDialect");
            return DriverManager.getConnection(url, user, password);
        }catch(ClassNotFoundException e){
            System.out.println(e);
        }catch(SQLException e){
            System.out.println(e);
        }
        return null;
    }

    @GetMapping("databases")
    public List<String> getDatabases() throws SQLException {
        List<String> databases = new ArrayList<>();
        
        try (Connection connection = jdbcConnection("master")) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet resultSet = metaData.getCatalogs()) {
                
                while (resultSet.next()) {
                    databases.add(resultSet.getString("TABLE_CAT"));
                }
            }
        }
        return databases;
    }

    @GetMapping("/{database}/schemas")
    public List<Map<String, Object>> getSchemas(@PathVariable String database) throws SQLException {
        List<Map<String, Object>> schemas = new ArrayList<>();
        
        try (Connection connection = jdbcConnection(database)) {
            
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet resultSet = metaData.getSchemas(database, null)) {
                while (resultSet.next()) {
                    System.err.println(resultSet.getString(1));
                    Map<String, Object> schema = new HashMap<>();
                    schema.put("database", database);
                    schema.put("schema_name", resultSet.getString("TABLE_SCHEM"));
                    schemas.add(schema);
                }
            }
        }
        return schemas;
    }

    @GetMapping("tables/structure")
    public List<Map<String, Object>> getAllTablesStructure(@RequestParam String database, @RequestParam String schema) throws SQLException {
        List<Map<String, Object>> tableStructures = new ArrayList<>();
        
        try (Connection connection = jdbcConnection(database)) {
            DatabaseMetaData metaData = connection.getMetaData();
            
            try (ResultSet tablesResultSet = metaData.getTables(database, schema, "%", new String[]{"TABLE"})) {
                while (tablesResultSet.next()) {
                    String tableName = tablesResultSet.getString("TABLE_NAME");
                    
                    try (ResultSet columnsResultSet = metaData.getColumns(database, schema, tableName, "%")) {
                        Map<String, Object> tableInfo = new HashMap<>();
                        tableInfo.put("database", database);
                        tableInfo.put("schema", schema);
                        tableInfo.put("table", tableName);

                        List<Map<String, Object>> columns = new ArrayList<>();
                        
                        while (columnsResultSet.next()) {
                            Map<String, Object> column = new HashMap<>();
                            column.put("column_name", columnsResultSet.getString("COLUMN_NAME"));
                            column.put("data_type", columnsResultSet.getString("TYPE_NAME"));
                            column.put("column_size", columnsResultSet.getInt("COLUMN_SIZE"));
                            columns.add(column);
                        }

                        tableInfo.put("columns", columns);

                        tableStructures.add(tableInfo);
                    }
                }
            }
        }
        return tableStructures;
    }


    @GetMapping("/tables")
    public List<Map<String, Object>> getTables(
        @RequestParam(required = false) String database,
        @RequestParam(required = false) String schema) throws SQLException {
        
        List<Map<String, Object>> tables = new ArrayList<>();
        
        try (Connection connection = jdbcConnection(database)) {
            DatabaseMetaData metaData = connection.getMetaData();
            
            if (database != null && schema == null) {
                try (ResultSet resultSet = metaData.getTables(database, null, "%", new String[]{"TABLE"})) {
                    while (resultSet.next()) {
                        Map<String, Object> table = new HashMap<>();
                        table.put("database", database);
                        table.put("schema", resultSet.getString("TABLE_SCHEM"));
                        table.put("table_name", resultSet.getString("TABLE_NAME"));
                        tables.add(table);
                    }
                }
            }
            else if (database != null && schema != null) {
                try (ResultSet resultSet = metaData.getTables(database, schema, "%", new String[]{"TABLE"})) {
                    while (resultSet.next()) {
                        Map<String, Object> table = new HashMap<>();
                        table.put("database", database);
                        table.put("schema", schema);
                        table.put("table_name", resultSet.getString("TABLE_NAME"));
                        tables.add(table);
                    }
                }
            }
            else {
                return tables;
            }
        }
        return tables;
    }


   @PostMapping("/caulenh")
    public ResponseEntity<Map<String, Object>> executeSqlQuery(@RequestBody Map<String, String> request) {
        Map<String, Object> response = new HashMap<>();
        List<Map<String, Object>> results = new ArrayList<>();

        String sqlQuery = request.get("sql");
        String database = request.get("database");
        String schema = request.get("schema");

        if (sqlQuery == null || sqlQuery.trim().isEmpty()) {
            response.put("success", false);
            response.put("message", "SQL query is required.");
            return ResponseEntity.badRequest().body(response);
        }

        try (Connection connection = jdbcConnection(database)) {
            connection.setCatalog(database);
            
            try (Statement statement = connection.createStatement()) {
                // Kiểm tra loại câu lệnh SQL
                String sqlUpperCase = sqlQuery.trim().toUpperCase();
                if (sqlUpperCase.startsWith("SELECT")) {
                    // Xử lý câu lệnh SELECT
                    try (ResultSet resultSet = statement.executeQuery(sqlQuery)) {
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        int columnCount = metaData.getColumnCount();
                        
                        while (resultSet.next()) {
                            Map<String, Object> row = new HashMap<>();
                            for (int i = 1; i <= columnCount; i++) {
                                String columnName = metaData.getColumnName(i);
                                Object columnValue = resultSet.getObject(i);
                                row.put(columnName, columnValue);
                            }
                            results.add(row);
                        }
                        
                        response.put("success", true);
                        if (results.isEmpty()) {
                            response.put("message", "Query executed successfully, but no data found.");
                        } else {
                            response.put("data", results);
                        }
                    }
                } else {
                    // Xử lý các câu lệnh INSERT, UPDATE, DELETE
                    int rowsAffected = statement.executeUpdate(sqlQuery);
                    response.put("success", true);
                    response.put("message", rowsAffected + " row(s) affected.");
                }
            }
        } catch (SQLException e) {
            response.put("success", false);
            response.put("message", "Error executing SQL query: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }

        return ResponseEntity.ok(response);
    }
}
