package com.whoslt.avacita.client;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static com.whosly.avacita.client.AvacitaClient.printResult;

public class InsertTest {
    @Before
    public void before() throws ClassNotFoundException {
        Class.forName("com.whosly.avacita.driver.EncDriver");
    }

    @Test
    public void testInsert() throws SQLException {
        Properties prop = new Properties();
        prop.put("serialization", "protobuf");

        String url = "jdbc:enc:url=http://localhost:5888";

        try (Connection conn = DriverManager.getConnection(url, prop)) {
            final Statement stmt = conn.createStatement();

            String insertSQL = "INSERT INTO `demo`.`t_emp` (`name`, `sex`, `married`, `education`, `tel`, `cert_no`, `email`, `address`, `mgr_id`, `hiredate`, `termdate`, `status`, `create_time`, `update_time`) VALUES ('fengyang', 'M', 92, 101, '18066666666', '331766843269298391', 'syuito@gmail.com', '356 Wall Street', 446, '2018-07-09', '2024-12-10', 54, '2009-05-26 17:46:10', '2025-04-22 15:34:29')";
            final int insertRs = stmt.executeUpdate(insertSQL);
            System.out.println("insertRs : " + insertRs);

            String selectSQL = "select * from `demo`.`t_emp` where `name`='fengyang'";
            final ResultSet rs = stmt.executeQuery(selectSQL);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            printResult(rs);

            String deleteSQL = "DELETE from `demo`.`t_emp` where `name`='fengyang'";
            final int deleteRs = stmt.executeUpdate(deleteSQL);
            System.out.println("deleteRs : " + deleteRs);

            stmt.close();
        }
    }

}
