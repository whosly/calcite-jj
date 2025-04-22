package com.whosly.calcite.schema;

import com.whosly.calcite.schema.csv.CsvSchemaLoader;
import com.whosly.calcite.schema.memory.MemorySchemaLoader;
import com.whosly.calcite.schema.mysql.MysqlSchemaLoader;
import com.whosly.calcite.util.ResultSetUtil;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.util.*;

public class SchemasTest {

    @Test
    public void testLoadSchema() throws SQLException {
        Map<String, Schema> schemaMap = new HashMap<>();
        // CsvSchema
        schemaMap.put("bugs1", CsvSchemaLoader.loadSchema("bugfix"));
        schemaMap.put("csv", CsvSchemaLoader.loadSchema("csv"));
        // MemorySchema
        schemaMap.put("hr", MemorySchemaLoader.loadSchema());
        // ESSchema
//        schemaMap.put("es", ESSchemaLoader.loadSchema("localhost", 9200));

        Connection connection = Schemas.getConnection(schemaMap);
        Assert.assertTrue(connection instanceof CalciteConnection);

        // CsvSchema
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

        // MemorySchema
        String sql1 = "select * from hr.emps";
        List<List<Object>> rs = executeQuery((CalciteConnection) connection, sql1);
        Assert.assertEquals(rs.size(), 4);

        // MysqlSchema
        String sql5 = "select * from hr.emps";
        List<List<Object>> rs5 = executeQuery((CalciteConnection) connection, sql5);
        Assert.assertEquals(rs5.size(), 4);
    }

    @Test
    public void testMysqlLoadSchema() throws SQLException {
        Connection connection = Schemas.getConnection();
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        String hostname = "localhost";
        int port = 13307;
        String db = "demo";
        String username = "root";
        String password = "Aa123456.";

        Schema schema = MysqlSchemaLoader.loadSchema(rootSchema, hostname, db, port, username, password);
        // 添加数据源， 将不同数据源schema挂载到RootSchema
        SchemaRegister.reg("demo", schema, rootSchema);

        // Mysql
        String sql = "select * from demo.t1";
        List<List<Object>> rs = executeQuery((CalciteConnection) connection, sql);
        Assert.assertTrue(!rs.isEmpty());
        System.out.println(ResultSetUtil.resultString(rs));
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
