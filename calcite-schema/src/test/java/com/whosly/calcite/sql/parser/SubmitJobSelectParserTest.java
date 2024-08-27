package com.whosly.calcite.sql.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.sql.parser.impl.JackySqlParserImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 自定义语法解析
 *
 * submit job as query
 */
public class SubmitJobSelectParserTest {

    private FrameworkConfig frameworkConfig;

    @Before
    public void setUp() throws Exception {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);

        this.frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.config()
                        .withParserFactory(JackySqlParserImpl.FACTORY)
                        .withCaseSensitive(false)
                        .withQuoting(Quoting.BACK_TICK)
                        .withQuotedCasing(Casing.TO_UPPER)
                        .withUnquotedCasing(Casing.TO_UPPER)
                        .withConformance(SqlConformanceEnum.ORACLE_12))
                .build();
    }

    @Test
    public void test_default_parser() {
        String sql = "submit job as 'select ids, name from test where id < 5'";

        SqlParser parser = SqlParser.create(sql, this.frameworkConfig.getParserConfig());
        try {
            SqlNode sqlNode = parser.parseStmt();

            System.out.println(sqlNode.toString());

        } catch (Exception e) {
            e.printStackTrace();

            Assert.assertEquals("1", "2");
        }
    }


}
