package com.sang.ThuVien.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

@Service
public class SqlService {
    
    // Lấy các quan hệ giữa các bảng
    public List<Map<String, Object>> getRelationsFromDatabase(String database, Connection connection) throws SQLException {
        List<Map<String, Object>> relations = new ArrayList<>();
        String query = "";

        // Kiểm tra loại cơ sở dữ liệu (PostgreSQL, MySQL, SQL Server)
        String dbProductName = connection.getMetaData().getDatabaseProductName().toLowerCase();

        if (dbProductName.contains("postgresql")) {
            // PostgreSQL query
            query = "SELECT " +
                    "  kcu.table_name AS fk_table_name, " +
                    "  kcu.column_name AS fk_column_name, " +
                    "  ccu.table_name AS pk_table_name, " +
                    "  ccu.column_name AS pk_column_name " +
                    "FROM information_schema.key_column_usage kcu " +
                    "JOIN information_schema.constraint_column_usage ccu " +
                    "  ON kcu.constraint_name = ccu.constraint_name " +
                    "WHERE kcu.table_schema = '" + database + "' " +
                    "AND ccu.table_name IS NOT NULL";
        } else if (dbProductName.contains("mysql")) {
            // MySQL query
            query = "SELECT " +
                    "  TABLE_NAME as fk_table_name, " +
                    "  COLUMN_NAME as fk_column_name, " +
                    "  REFERENCED_TABLE_NAME as pk_table_name, " +
                    "  REFERENCED_COLUMN_NAME as pk_column_name " +
                    "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                    "WHERE REFERENCED_TABLE_NAME IS NOT NULL " +
                    "AND TABLE_SCHEMA = '" + database + "'";
        } else if (dbProductName.contains("sqlserver")) {
            // SQL Server query
            query = "SELECT " +
                    "  fk.name AS fk_constraint_name, " +
                    "  tp.name AS fk_table_name, " +
                    "  ref.name AS pk_table_name, " +
                    "  col.name AS fk_column_name, " +
                    "  ref_col.name AS pk_column_name " +
                    "FROM sys.foreign_keys AS fk " +
                    "INNER JOIN sys.tables AS tp ON fk.parent_object_id = tp.object_id " +
                    "INNER JOIN sys.tables AS ref ON fk.referenced_object_id = ref.object_id " +
                    "INNER JOIN sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id " +
                    "INNER JOIN sys.columns AS col ON fkc.parent_column_id = col.column_id AND tp.object_id = col.object_id " +
                    "INNER JOIN sys.columns AS ref_col ON fkc.referenced_column_id = ref_col.column_id AND ref.object_id = ref_col.object_id " +
                    "WHERE tp.name <> ref.name";
        }

        try (
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            while (resultSet.next()) {
                Map<String, Object> relation = new HashMap<>();
                relation.put("fk_table_schema", resultSet.getString("fk_table_schema"));
                relation.put("fk_table_name", resultSet.getString("fk_table_name"));
                relation.put("fk_column_name", resultSet.getString("fk_column_name"));
                relation.put("pk_table_schema", resultSet.getString("pk_table_schema"));
                relation.put("pk_table_name", resultSet.getString("pk_table_name"));
                relation.put("pk_column_name", resultSet.getString("pk_column_name"));
                relation.put("on_update", resultSet.getString("on_update"));
                relation.put("on_delete", resultSet.getString("on_delete"));
                relations.add(relation);
            }
        }
        return relations;
    }

    // Xây dựng câu lệnh SELECT dựa trên các bảng và cột đã chọn
    public String buildSelectQuery(List<String> selectedTables, List<String> selectedColumns, List<Map<String, Object>> relations) {
        StringBuilder selectQuery = new StringBuilder("SELECT ");

        // Thêm các cột vào phần SELECT
        selectQuery.append(String.join(", ", selectedColumns));

        // Xây dựng phần FROM và JOIN
        selectQuery.append(" FROM ");
        selectQuery.append(selectedTables.get(0)); // Bảng đầu tiên

        // Duyệt qua các quan hệ để tạo các câu lệnh JOIN
        for (int i = 1; i < selectedTables.size(); i++) {
            String table1 = selectedTables.get(i - 1);
            String table2 = selectedTables.get(i);

            for (Map<String, Object> relation : relations) {
                if (relation.get("fk_table_name").equals(table1) && relation.get("pk_table_name").equals(table2)) {
                    String fkColumn = (String) relation.get("fk_column_name");
                    String pkColumn = (String) relation.get("pk_column_name");

                    selectQuery.append(" JOIN ").append(table2)
                            .append(" ON ").append(table1).append(".").append(fkColumn)
                            .append(" = ").append(table2).append(".").append(pkColumn);
                    break;
                }
            }
        }

        return selectQuery.toString();
    }
}
