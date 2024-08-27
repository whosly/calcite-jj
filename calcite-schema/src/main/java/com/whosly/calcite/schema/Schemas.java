package com.whosly.calcite.schema;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class Schemas {

    public static Connection getConnection() throws SQLException {
        return Schemas.getConnection(Collections.emptyMap());
    }

    public static Connection getConnection(Map<String, Schema> regSchema) throws SQLException {
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

        // 4. 添加数据源， 将不同数据源schema挂载到RootSchema
        if (regSchema != null && !regSchema.isEmpty()) {
            for (Map.Entry<String, Schema> entry : regSchema.entrySet()) {
                String dbName = entry.getKey();
                Schema schema = entry.getValue();
                SchemaRegister.reg(dbName, schema, rootSchema);
            }
        }

        return calciteConnection;
    }

}
