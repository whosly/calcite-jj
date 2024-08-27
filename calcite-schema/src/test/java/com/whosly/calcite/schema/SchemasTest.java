package com.whosly.calcite.schema;

import com.whosly.calcite.schema.csv.CsvSchemaLoader;
import com.whosly.calcite.schema.memory.MemorySchemaLoader;
import com.whosly.calcite.util.ResultSetUtil;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemasTest {

    @Test
    public void testLoadSchema() throws SQLException {
        Map<String, Schema> schemaMap = new HashMap<>();
        schemaMap.put("bugs1", CsvSchemaLoader.loadSchema("bugfix"));
        schemaMap.put("csv", CsvSchemaLoader.loadSchema("csv"));
        schemaMap.put("hr", MemorySchemaLoader.loadSchema());
//        schemaMap.put("es", ESSchemaLoader.loadSchema("localhost", 9200));

        Connection connection = Schemas.getConnection(schemaMap);
        Assert.assertTrue(connection instanceof CalciteConnection);

        String sql1 = "select * from hr.emps";
        List<List<Object>> rs = executeQuery((CalciteConnection) connection, sql1);
        Assert.assertEquals(rs.size(), 4);

        String sql2 = "select * from csv.sdepts";
        List<List<Object>> rs2 = executeQuery((CalciteConnection) connection, sql2);
        Assert.assertEquals(rs2.size(), 6);
        sql2 = "select * from csv.depts";
        rs2 = executeQuery((CalciteConnection) connection, sql2);
        Assert.assertEquals(rs2.size(), 5);

        String sql3 = "select * from bugs1.date_ord";
        List<List<Object>> rs3 = executeQuery((CalciteConnection) connection, sql3);
        Assert.assertEquals(rs3.size(), 8);
        System.out.println(ResultSetUtil.resultString(rs3));
    }

    /**
     * 执行查询
     */
    private static List<List<Object>> executeQuery(CalciteConnection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);

            return ResultSetUtil.resultList(resultSet);
        } catch (Exception e){
            e.printStackTrace();
            return Collections.emptyList();
        } finally {
            //.
        }
    }

}
