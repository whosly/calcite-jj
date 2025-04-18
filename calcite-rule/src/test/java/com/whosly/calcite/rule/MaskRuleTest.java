package com.whosly.calcite.rule;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Properties;

/**
 * @author fengyang <deep_blue_yang@126.com>
 * @date on 2025/4/15
 */
public class MaskRuleTest {
    private static final String SCHEMA = "demo";
    private static final String URL = "jdbc:mysql://localhost:13307/" + SCHEMA;
    private static final String USER = "root";
    private static final String PW = "Aa123456.";
    private static final String TEST_TABLE = "t1";
    private static Connection calciteConnection;
    private static FrameworkConfig  config;

    @BeforeClass
    public static void setup() throws Exception {
        // 1. 创建内存MySQL测试表
        Properties info = new Properties();
        info.setProperty("lex", "MYSQL");      // 使用 MySQL 的词法规则
        info.setProperty("caseSensitive", "false"); // 关闭大小写敏感
        calciteConnection = DriverManager.getConnection("jdbc:calcite:", info);

        CalciteConnection conn = calciteConnection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = conn.getRootSchema();

        DataSource dataSource = initDataSource(URL, USER, PW);
        // 2. 注册MySQL Schema
        JdbcSchema schema = JdbcSchema.create(rootSchema, SCHEMA, dataSource, null, null);
        rootSchema.add(SCHEMA, schema);

//        // 创建HepPlanner并注册规则
//        HepProgramBuilder programBuilder = new HepProgramBuilder();
//        programBuilder.addRuleInstance(new MaskNameRule());
//        HepProgram p = programBuilder.build();

        // 3. 配置优化规则
        config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema(SCHEMA))
                .parserConfig(SqlParser.config().withLex(Lex.MYSQL))
//                .programs(p.build())
                .ruleSets(RuleSets.ofList(
                        new MaskNameRule(), // 注册脱敏规则
                        CoreRules.FILTER_INTO_JOIN // 保留系统默认规则
                ))
//                .ruleSets(RuleSets.ofList(new MaskNameRule())) // 添加自定义规则
                .build();

        // 4. 将规则绑定到连接
        Frameworks.withPlanner((cluster, relOptSchema, root) -> {
            // 规则已通过 config 注册，无需额外操作
            return null;
        }, config);

    }

    @Test
    public void testMaskRule() throws Exception {
        testQuery(config, "SELECT name FROM " + SCHEMA + "." + TEST_TABLE);
        testQuery(config, "SELECT * FROM " + SCHEMA + "." + TEST_TABLE);
    }

    private static void testQuery(FrameworkConfig config, String sql) throws Exception {
        try (Statement stmt = calciteConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            System.out.println("\nQuery: " + sql);
            ResultSetMetaData meta = rs.getMetaData();

            // 打印执行计划
            printExecutionPlan(sql);

            // 打印结果
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    sb.append(meta.getColumnName(i))
                            .append(": ")
                            .append(rs.getString(i))
                            .append(", ");
                }
                System.out.println(sb.toString());
            }
        }
    }

    private static void printExecutionPlan(String sql) throws SQLException {
        try (Statement stmt = calciteConnection.createStatement();
             ResultSet rs = stmt.executeQuery("EXPLAIN PLAN FOR " + sql)) {
            System.out.println("Execution Plan:");
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        }
    }

    /**
     *  初始化 DataSource
     * @param url 连接地址， 如  jdbc:mysql://localhost:3306/your_database?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC
     * @param userName 用户名
     * @param password 密码
     * @return
     */
    private static DruidDataSource initDataSource(String url, String userName, String password) {
        // 初始化 Druid 数据源
        DruidDataSource dataSource = new DruidDataSource();
        // 数据库连接 URL，需要根据实际情况修改数据库名、主机和端口
        dataSource.setUrl(url);
        // 数据库用户名
        dataSource.setUsername(userName);
        // 数据库密码
        dataSource.setPassword(password);
        // 初始化时建立物理连接的个数
        dataSource.setInitialSize(5);
        // 最小连接池数量
        dataSource.setMinIdle(5);
        // 最大连接池数量
        dataSource.setMaxActive(20);
        // 获取连接时最大等待时间，单位毫秒
        dataSource.setMaxWait(60000);
        // 配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        // 配置一个连接在池中最小生存的时间，单位是毫秒
        dataSource.setMinEvictableIdleTimeMillis(300000);
        // 用来检测连接是否有效的 SQL
        dataSource.setValidationQuery("SELECT 1");
        // 建议配置为 true，不影响性能，并且保证安全性
        dataSource.setTestWhileIdle(true);
        // 申请连接时执行 validationQuery 检测连接是否有效，做了这个配置会降低性能
        dataSource.setTestOnBorrow(false);
        // 归还连接时执行 validationQuery 检测连接是否有效，做了这个配置会降低性能
        dataSource.setTestOnReturn(false);
        // 是否缓存 preparedStatement，也就是 PSCache
        dataSource.setPoolPreparedStatements(true);
        // 要启用 PSCache，必须配置大于 0，当大于 0 时，poolPreparedStatements 自动触发修改为 true
        dataSource.setMaxPoolPreparedStatementPerConnectionSize(20);

        return dataSource;
    }
}
