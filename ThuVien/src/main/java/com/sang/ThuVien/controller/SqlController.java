package com.sang.ThuVien.controller;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.sang.ThuVien.service.SqlService;

@RestController
@RequestMapping("/api/sql")
public class SqlController {

    @Autowired
    private SqlService sqlService;

    // Postgres
    // Connection jdbcConnection(String db){
    //     String url="jdbc:postgresql://localhost:5433/"+db;
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
    Connection jdbcConnection(String db){
        String url="jdbc:mysql://localhost:3308/"+db;
        String user="root";
        String password = "root_password";
        // System.out.println("Connecting to: " + url);
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");
            return DriverManager.getConnection(url, user, password);
        }catch(ClassNotFoundException e){
            System.out.println(e);
        }catch(SQLException e){
            System.out.println(e);
        }
        return null;
    }

    // SQLServer
    // Connection jdbcConnection(String db){
    //     String url="jdbc:sqlserver://localhost:1433;databaseName="+db+";encrypt=true;trustServerCertificate=true";
    //     String user="sa";
    //     String password = "password!@!123";
    //     try{
    //         Class.forName("org.hibernate.dialect.SQLServerDialect");
    //         return DriverManager.getConnection(url, user, password);
    //     }catch(ClassNotFoundException e){
    //         System.out.println(e);
    //     }catch(SQLException e){
    //         System.out.println(e);
    //     }
    //     return null;
    // }

    @GetMapping("/databases")
    public List<String> getDatabases() throws SQLException {
        List<String> databases = new ArrayList<>();
        
        try (Connection connection = jdbcConnection("kqht_mysql")) {
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
                    // System.err.println(resultSet.getString(1));
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

    //lấy quan hệ
    @GetMapping("/{database}/relations")
    public List<Map<String, Object>> getRelations(@PathVariable String database) throws SQLException {
        List<Map<String, Object>> relations = new ArrayList<>();

        // Câu truy vấn lấy thông tin quan hệ giữa các bảng
        String query = "SELECT " +
                    "  kcu.table_schema AS fk_table_schema, " +
                    "  kcu.table_name AS fk_table_name, " +
                    "  kcu.column_name AS fk_column_name, " +
                    "  ccu.table_schema AS pk_table_schema, " +
                    "  ccu.table_name AS pk_table_name, " +
                    "  ccu.column_name AS pk_column_name, " +
                    "  rc.update_rule AS on_update, " +
                    "  rc.delete_rule AS on_delete " +
                    "FROM information_schema.key_column_usage kcu " +
                    "JOIN information_schema.constraint_column_usage ccu " +
                    "  ON kcu.constraint_name = ccu.constraint_name " +
                    "JOIN information_schema.referential_constraints rc " +
                    "  ON kcu.constraint_name = rc.constraint_name " +
                    "WHERE kcu.table_schema = 'public'"; // Chỉnh lại 'public' nếu cần

        try (Connection connection = jdbcConnection(database); // Kết nối với cơ sở dữ liệu
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query)) {

            // Duyệt qua từng bản ghi trong kết quả truy vấn
            while (resultSet.next()) {
                Map<String, Object> relation = new HashMap<>();
                // relation.put("fk_table_schema", resultSet.getString("fk_table_schema"));
                relation.put("fk_table_name", resultSet.getString("fk_table_name"));
                relation.put("fk_column_name", resultSet.getString("fk_column_name"));
                relation.put("pk_table_schema", resultSet.getString("pk_table_schema"));
                relation.put("pk_table_name", resultSet.getString("pk_table_name"));
                relation.put("pk_column_name", resultSet.getString("pk_column_name"));
                relation.put("on_update", resultSet.getString("on_update"));
                relation.put("on_delete", resultSet.getString("on_delete"));
                relations.add(relation);  // Thêm mối quan hệ vào danh sách
            }
        }

        return relations;
    }

    @PostMapping("/create-view")
    public ResponseEntity<Map<String, Object>> createView(@RequestBody Map<String, String> request) {
        Map<String, Object> response = new HashMap<>();
        
        String viewName = request.get("viewName");
        String database = request.get("database");
        String selectQuery = request.get("selectQuery");
        
        if (viewName == null || viewName.trim().isEmpty() || selectQuery == null || selectQuery.trim().isEmpty()) {
            response.put("success", false);
            response.put("message", "View name and select query are required.");
            return ResponseEntity.badRequest().body(response);
        }
        
        try (Connection connection = jdbcConnection(database)) {
            try (Statement statement = connection.createStatement()) {
                String createViewQuery = "CREATE VIEW " + viewName + " AS " + selectQuery;
                statement.execute(createViewQuery);
                
                response.put("success", true);
                response.put("message", "View created successfully.");
            }
        } catch (SQLException e) {
            response.put("success", false);
            response.put("message", "Error creating view: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
        
        return ResponseEntity.ok(response);
    }

    @PostMapping("/create-view-dynamic")
    public ResponseEntity<Map<String, Object>> createViewDynamic(@RequestBody Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();

        List<String> selectedTables = (List<String>) request.get("tables");
        List<String> selectedColumns = (List<String>) request.get("columns");
        String viewName = (String) request.get("viewName");
        String database = (String) request.get("database");
        
        if (viewName == null || viewName.trim().isEmpty() || selectedTables == null || selectedTables.isEmpty() || selectedColumns == null || selectedColumns.isEmpty()) {
            response.put("success", false);
            response.put("message", "View name, tables, and columns are required.");
            return ResponseEntity.badRequest().body(response);
        }

        try {
            try (Connection connection = jdbcConnection(database)) {
                System.err.println(database);
                List<Map<String, Object>> relations = sqlService.getRelationsFromDatabase(database, connection);
                
                String selectQuery = sqlService.buildSelectQuery(selectedTables, selectedColumns, relations);
                System.err.println(selectQuery);
                String createViewQuery = "CREATE VIEW " + viewName + " AS " + selectQuery;
                
                try (Statement statement = connection.createStatement()) {
                    // statement.execute(createViewQuery);
                    response.put("success", true);
                    response.put("message", "View created successfully.");
                }
            }
        } catch (SQLException e) {
            response.put("success", false);
            response.put("message", "Error creating view: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }

        return ResponseEntity.ok(response);
    }


}
