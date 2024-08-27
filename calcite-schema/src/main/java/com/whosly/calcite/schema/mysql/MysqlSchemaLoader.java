//package com.whosly.calcite.schema.mysql;
//
//import com.whosly.calcite.schema.ISchemaLoader;
//import org.apache.calcite.adapter.jdbc.JdbcSchema;
//import org.apache.calcite.schema.Schema;
//import org.apache.calcite.schema.SchemaPlus;
//
//import javax.sql.DataSource;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Properties;
//
//public class MysqlSchemaLoader implements ISchemaLoader {
//
//    public static Schema loadSchema(String hostname, int port, String usrname, String password) {
//
//    }
//
//    // 添加MySQL数据源
//    private static Schema addMysqlDataSource(SchemaPlus rootSchema, String hostname, int port,
//                                           String username, String password, String db) {
////        Properties properties = new Properties();
////        properties.setProperty("lex", "MYSQL");
//
//        DataSource dataSource = JdbcSchema.dataSource("jdbc:mysql://"+hostname+":"+port+"/"+db,
//                "com.mysql.cj.jdbc.Driver", username, password);
//
//        JdbcSchema schema = JdbcSchema.create(rootSchema, "DB", dataSource, null, null);
//
//        return schema;
//    }
//
//}
