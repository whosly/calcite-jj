package com.whosly.calcite.schema.mysql;

import com.whosly.calcite.schema.SchemaRegister;
import com.whosly.calcite.schema.Schemas;
import com.whosly.calcite.util.ResultSetUtil;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author fengyang <deep_blue_yang@126.com>
 * @date on 2025/4/17
 */
public class MysqlSchemaLoaderTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testLoadSchema() throws SQLException {
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

        Assert.assertTrue(connection instanceof CalciteConnection);

        // Mysql
        String sql = "select * from demo.t1";

        try (var statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);

            List<List<Object>> rs = ResultSetUtil.resultList(resultSet);

            Assert.assertEquals(rs.size(), 18);
            System.out.println(ResultSetUtil.resultString(rs));
        } catch (Exception e){
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

}