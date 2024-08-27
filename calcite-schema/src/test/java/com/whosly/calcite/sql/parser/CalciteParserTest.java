package com.whosly.calcite.sql.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Calcite 默认语法解析
 */
public class CalciteParserTest {

    private FrameworkConfig config;

    @Before
    public void setUp() throws Exception {
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                        // calcite内置Parser类为SqlParserImpl， 这个类的代码全部是由JavaCC生成
                        .setParserFactory(SqlParserImpl.FACTORY)
                        // 大小是写否敏感，比如说列名、表名、函数名
                        .setCaseSensitive(false)
                        // 设置引用一个标识符，比如说MySQL中的是``, Oracle中的""
                        .setQuoting(Quoting.BACK_TICK)
                        // Quoting策略，不变，变大写或变成小写，代码中的全部设置成变大写
                        .setQuotedCasing(Casing.TO_UPPER)
                        // 当标识符没有被Quoting后的策略
                        .setUnquotedCasing(Casing.TO_UPPER)
                        // 特定语法支持，比如是否支持差集等
                        .setConformance(SqlConformanceEnum.ORACLE_12)
                        // 设置标识符的最大长度，如果你的列名、表较长可以相应的加大这个值
//                        .setIdentifierMaxLength()
                        .build())
                .build();

        this.config = config;
    }

    @Test
    public void test_default_parser() {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);

        String sql = "select ids, name from test where id < 5 and name = 'zhang'";
        SqlParser parser = SqlParser.create(sql, this.config.getParserConfig());
        try {
            SqlNode sqlNode = parser.parseStmt();

            Assert.assertTrue(sqlNode instanceof SqlSelect);
            System.out.println(sqlNode.toString());

            SqlSelect select = (SqlSelect) sqlNode;
            Assert.assertEquals(select.getSelectList().size(), 2);
            Assert.assertEquals(select.getSelectList().toString(), "`IDS`, `NAME`");
        } catch (Exception e) {
            e.printStackTrace();

            Assert.assertEquals("1", "2");
        }
    }

}
