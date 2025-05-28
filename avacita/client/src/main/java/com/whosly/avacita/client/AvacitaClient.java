package com.whosly.avacita.client;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AvacitaClient {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        System.out.println("AvacitaClient");
        Class.forName("com.whosly.avacita.driver.EncDriver");

        Properties prop = new Properties();
        prop.put("serialization", "protobuf");

        String url = "jdbc:enc:url=http://localhost:5888";

        // 首先看驱动存不存在，通过Class.forName反射判断；
        // 如果存在Driver，尝试建立链接；
        // 如果建立链接成功了，返回链接。
        // Connection 是 AvaticaJDBC41Connection
        try (Connection conn = DriverManager.getConnection(url, prop)) {
            // 调用AvaticaConnection.createStatement;
            // 交给AvaticaJdbc41Factory创建AvaticaStatement；AvaticaStatement的构造函数会创建StatementHandle（发起远程请求得到响应）;
            // 最后返回AvaticaStatement。
            // 创建Statement, 就是发起远程请求，然后服务端创建Statement，然后把Statement id返回回来，然后就可以根据这个ID直接用
            final Statement stmt = conn.createStatement();

            // AvaticaStatment调用connection.prepareAndExecuteInternal(this, sql, maxRowCount1);
            // 然后调用RemoteMeta.prepareAndExecute;
            // RemoteMeta内部由service发起远程请求，并得到响应。
            // 服务端，收到请求后，首先根据statement id找到statement，然后通过statement执行请求，并返回结果。
//            final ResultSet rs = stmt.executeQuery("SELECT tel FROM t_emp");
//            final ResultSet rs = stmt.executeQuery("SELECT tel, email FROM t_emp");
//            final ResultSet rs = stmt.executeQuery("SELECT name, tel, email FROM t_emp");
//            final ResultSet rs = stmt.executeQuery("SELECT id, tel, cert_no, email FROM t_emp");
//            final ResultSet rs = stmt.executeQuery("SELECT id, name, tel, cert_no, email, address FROM t_emp");
//            final ResultSet rs = stmt.executeQuery("SELECT t.id, t.tel FROM (SELECT id FROM t_emp where id % 2 = 0) tmp, t_emp t where t.id = tmp.id");
//            final ResultSet rs = stmt.executeQuery("SELECT t.id, t.tel FROM (SELECT id, cert_no, email FROM t_emp where id % 2 = 0) tmp, t_emp t where t.id = tmp.id");
//            final ResultSet rs = stmt.executeQuery("SELECT tmp.*, t.* FROM (SELECT id, cert_no, email FROM t_emp where id % 2 = 0) tmp, t_emp t where t.id = tmp.id");
            final ResultSet rs = stmt.executeQuery("SELECT name, tel, email FROM t_emp limit 20");
//            final ResultSet rs = stmt.executeQuery("show databases();");
//            final ResultSet rs = stmt.executeQuery("SHOW TABLES");

            printResult(rs);

            rs.close();
            stmt.close();
        }
    }

    /**
     *
     * @param rs
     * @return rs是否存在数据。 true: 存在数据
     */
    public static boolean printResult(ResultSet rs) throws SQLException {
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