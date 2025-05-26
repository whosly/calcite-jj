package com.whosly.avacita.client;

import java.sql.*;
import java.util.Properties;

public class AvacitaClient {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        System.out.println("AvacitaClient");
        Class.forName("org.apache.calcite.avatica.remote.Driver");

        Properties prop = new Properties();
        prop.put("serialization", "protobuf");

        String url = "jdbc:avatica:remote:url=http://localhost:5888";

        try (Connection conn = DriverManager.getConnection(url, prop)) {
            // 查询数据
            final Statement stmt = conn.createStatement();
            final ResultSet rs = stmt.executeQuery("SELECT * FROM student");
//            printRs(rs);
        }

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM PUBLIC.SCHEMATA");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        rs.close();
        stmt.close();
        conn.close();
    }
}