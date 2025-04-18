package com.whosly.calcite.schema.csv;

import com.whosly.calcite.util.ResultSetUtil;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.List;
import java.util.Properties;

/**
 * 接入 csv 数据源
 *
 * Calcite会把每个csv文件映射成一个SQL表。
 * csv文件表头指定该列数据类型，根据一定规则映射到对应的SQL类型。如没有指定，则统一映射成VARCHAR。
 *
 * 文件命名为depts.csv，Caclite会构建表名为文件名的table，即depts.
 */
public class CsvSchemaTest {

    private CalciteConnection connection;

    @Before
    public void before() throws SQLException {

        // 2.构建Connection
        // 2.1 设置连接参数
        Properties info = new Properties();
        // 不区分sql大小写
        info.setProperty("caseSensitive", "false");

        // 2.2 获取标准的JDBC Connection
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        // 2.3 获取Calcite封装的Connection
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        // 3.构建RootSchema，在Calcite中，RootSchema是所有数据源schema的parent，多个不同数据源schema可以挂在同一个RootSchema下
        // 以实现查询不同数据源的目的
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // 4.将不同数据源schema挂载到RootSchema，这里添加CsvSchema
        // load databaseName: csv
        rootSchema.add("csv", CsvSchemaLoader.loadSchema("csv"));
        // load databaseName: bugfix
        rootSchema.add("bugfix", CsvSchemaLoader.loadSchema("bugfix"));

        this.connection = calciteConnection;
    }

    @Test
    public void testSchemaCheck() throws SQLException {
        String schema = this.connection.getSchema();
        Assert.assertTrue(StringUtils.isEmpty(schema));

        this.connection.setSchema("bugfix");
        schema = this.connection.getSchema();
        Assert.assertEquals(schema, "bugfix");

        Statement statement = this.connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from long_emps");
        List<List<Object>> rs = ResultSetUtil.resultList(resultSet);
        Assert.assertEquals(rs.size(), 5);
        System.out.println(ResultSetUtil.resultString(rs));
    }

    @Test
    public void testCsvQueryAll() throws SQLException {
        // 5.执行SQL查询，通过SQL方式访问csv文件
        String sql = "select * from csv.depts";

        Statement statement = this.connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        // 6.遍历打印查询结果集
        List<List<Object>> rs = ResultSetUtil.resultList(resultSet);
        Assert.assertEquals(rs.size(), 5);
        System.out.println(ResultSetUtil.resultString(rs));
    }

    @Test
    public void testCsvQueryFilter() throws SQLException {
        // 5.执行SQL查询，通过SQL方式访问csv文件
        String sql = "select * from csv.depts where name like '%l%'";

        Statement statement = this.connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        // 6.遍历打印查询结果集
        List<List<Object>> rs = ResultSetUtil.resultList(resultSet);
        Assert.assertEquals(rs.size(), 2);
        System.out.println(ResultSetUtil.resultString(rs));
    }

    @Test
    public void testBugDateOrd() throws SQLException {
        // 5.执行SQL查询，通过SQL方式访问csv文件
        String sql = "select * from bugfix.date_ord";

        Statement statement = this.connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        // 6.遍历打印查询结果集
        List<List<Object>> rs = ResultSetUtil.resultList(resultSet);
        Assert.assertEquals(rs.size(), 8);
        System.out.println(ResultSetUtil.resultString(rs));
    }

    @Test
    public void testBugLongEmps() throws SQLException {
        // 5.执行SQL查询，通过SQL方式访问csv文件
        String sql = "select * from bugfix.long_emps";

        Statement statement = this.connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        // 6.遍历打印查询结果集
        List<List<Object>> rs = ResultSetUtil.resultList(resultSet);
        Assert.assertEquals(rs.size(), 5);
        System.out.println(ResultSetUtil.resultString(rs));
    }

    /**
     * 不支持文件名为关键字
     */
    @Test(expected = SQLException.class)
    public void testBugWithKeyword() throws SQLException {
        String sql = "select * from bugfix.DATE";

        Statement statement = this.connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        // 不支持文件名为关键字
        Assert.assertTrue(false);
    }
}
