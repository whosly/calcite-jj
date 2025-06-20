package com.whosly.avacita.client;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AvacitaPrepareClient {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        System.out.println("AvacitaClient");
        Class.forName("org.apache.calcite.avatica.remote.Driver");
//        Class.forName("com.whosly.avacita.driver.EncDriver");

        Properties prop = new Properties();
        prop.put("serialization", "protobuf");

        String url = "jdbc:avatica:remote:url=http://localhost:5888";
//        String url = "jdbc:enc:url=http://localhost:5888";
        String sql = "SELECT * FROM t_emp WHERE id > ?";

        try (Connection conn = DriverManager.getConnection(url, prop)) {
            // 使用显式参数类型指定（避免元数据查询）
            final PreparedStatement pstmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            // 1. 设置参数（示例：查询 id > 2122 的记录）
            pstmt.setInt(1, 2122);

            final ResultSet rs = pstmt.executeQuery();

            printResult(rs);

            rs.close();
        }
    }

    /**
     *
     * @param rs
     * @return rs是否存在数据。 true: 存在数据
     */
    private static boolean printResult(ResultSet rs) throws SQLException {
        if (rs == null) {
            return false;
        }

        boolean dataExists = false;
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
            dataExists = true;

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
        printSeparator(columnWidths);
        for (int i = 0; i < columnCount; i++) {
            System.out.printf("| %-" + columnWidths[i] + "s ", metaData.getColumnName(i + 1));
        }
        System.out.println("|");

        // 打印数据
        printSeparator(columnWidths);
        for (String[] row : rows) {
            for (int i = 0; i < columnCount; i++) {
                System.out.printf("| %-" + columnWidths[i] + "s ", row[i] != null ? row[i] : "NULL");
            }
            System.out.println("|");
        }

        // 打印底部
        printSeparator(columnWidths);
        System.out.println(rows.size() + " rows in set\n");

        return dataExists;
    }

    private static void printSeparator(int[] widths) {
        for (int width : widths) {
            System.out.print("+-");
            System.out.print(repeat("-", width));
            System.out.print("-");
        }
        System.out.println("+");
    }

    /**
     * 实现字符串重复
     */
    private static String repeat(String str, int times) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < times; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}
