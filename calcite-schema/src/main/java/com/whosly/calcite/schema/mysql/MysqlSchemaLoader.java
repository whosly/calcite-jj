package com.whosly.calcite.schema.mysql;

import com.whosly.calcite.schema.ISchemaLoader;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.thymeleaf.util.StringUtils;

import javax.sql.DataSource;
import java.util.Properties;

public class MysqlSchemaLoader implements ISchemaLoader {

    /**
     * 添加MySQL数据源
     */
    public static Schema loadSchema(SchemaPlus rootSchema, String hostname, int port, String usrname, String password) {
        return loadSchema(rootSchema, hostname, "", port, usrname, password);
    }

    /**
     * 添加MySQL数据源
     */
    public static Schema loadSchema(SchemaPlus rootSchema, String hostname, String db, int port, String username, String password) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        Properties properties = new Properties();
        properties.setProperty("lex", "MYSQL");

        DataSource dataSource = JdbcSchema.dataSource(
                "jdbc:mysql://" + hostname + ":" + port + (StringUtils.isEmpty(db) ? "" : "/" + db),
                "com.mysql.cj.jdbc.Driver", username, password);

        JdbcSchema schema = JdbcSchema.create(rootSchema, db, dataSource, null, null);

        return schema;
    }

}
