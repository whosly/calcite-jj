package com.whosly.avacita.server.query.mask.rewrite.rule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 脱敏客户端测试类
 */
public class MaskingClientTest {
    private static final Logger LOG = LoggerFactory.getLogger(MaskingClientTest.class);

    public static void main(String[] args) {
        try {
            // 注册Avatica驱动
            Class.forName("org.apache.calcite.avatica.remote.Driver");

            Properties props = new Properties();
            props.put("serialization", "protobuf");

            String url = "jdbc:avatica:remote:url=http://localhost:5888";

            // 测试查询
            testQueries(url, props);

        } catch (Exception e) {
            LOG.error("测试失败", e);
        }
    }

    private static void testQueries(String url, Properties props) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, props)) {
            LOG.info("连接成功，开始测试脱敏功能...");

            // 测试1: 基本查询
            testBasicQuery(conn);

            // 测试2: SELECT * 查询
            testStarQuery(conn);

            // 测试3: 带条件的查询
            testConditionalQuery(conn);

            // 测试4: 带别名的查询
            testAliasQuery(conn);

        } catch (SQLException e) {
            LOG.error("数据库连接或查询失败", e);
            throw e;
        }
    }

    private static void testBasicQuery(Connection conn) throws SQLException {
        LOG.info("=== 测试1: 基本查询 ===");
        String sql = "SELECT id, name, tel, email, cert_no FROM t_emp LIMIT 5";
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            printResultSet(rs, "基本查询结果");
        }
    }

    private static void testStarQuery(Connection conn) throws SQLException {
        LOG.info("=== 测试2: SELECT * 查询 ===");
        String sql = "SELECT * FROM t_emp LIMIT 3";
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            printResultSet(rs, "SELECT * 查询结果");
        }
    }

    private static void testConditionalQuery(Connection conn) throws SQLException {
        LOG.info("=== 测试3: 带条件的查询 ===");
        String sql = "SELECT id, name, tel FROM t_emp WHERE id > 1000 LIMIT 3";
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            printResultSet(rs, "条件查询结果");
        }
    }

    private static void testAliasQuery(Connection conn) throws SQLException {
        LOG.info("=== 测试4: 带别名的查询 ===");
        String sql = "SELECT e.id, e.name, e.tel FROM t_emp e WHERE e.id < 2000 LIMIT 3";
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            printResultSet(rs, "别名查询结果");
        }
    }

    private static void printResultSet(ResultSet rs, String title) throws SQLException {
        if (rs == null) {
            LOG.warn("{}: 结果集为空", title);
            return;
        }

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 获取列宽度
        int[] columnWidths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columnWidths[i] = metaData.getColumnName(i + 1).length();
        }

        // 存储结果
        List<String[]> rows = new ArrayList<>();
        while (rs.next()) {
            String[] row = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                row[i] = rs.getString(i + 1);
                if (row[i] != null) {
                    columnWidths[i] = Math.max(columnWidths[i], row[i].length());
                }
            }
            rows.add(row);
        }

        // 打印表头
        LOG.info("{} ({} 行):", title, rows.size());
        printSeparator(columnWidths);
        StringBuilder header = new StringBuilder("|");
        for (int i = 0; i < columnCount; i++) {
            header.append(String.format(" %-" + columnWidths[i] + "s |", metaData.getColumnName(i + 1)));
        }
        LOG.info(header.toString());

        // 打印数据
        printSeparator(columnWidths);
        for (String[] row : rows) {
            StringBuilder dataRow = new StringBuilder("|");
            for (int i = 0; i < columnCount; i++) {
                dataRow.append(String.format(" %-" + columnWidths[i] + "s |", 
                    row[i] != null ? row[i] : "NULL"));
            }
            LOG.info(dataRow.toString());
        }
        printSeparator(columnWidths);
        LOG.info("");
    }

    private static void printSeparator(int[] widths) {
        StringBuilder separator = new StringBuilder("+");
        for (int width : widths) {
            separator.append("-".repeat(width + 2)).append("+");
        }
        LOG.info(separator.toString());
    }
} 