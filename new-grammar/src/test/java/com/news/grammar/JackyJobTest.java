package com.news.grammar;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlJackyJob;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.JackySqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * 新实现语法的测试用例： JACKY JOB query
 */
public class JackyJobTest {

    private FrameworkConfig frameworkConfig;

    @Before
    public void setUp() throws Exception {
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.config()
                        .withParserFactory(JackySqlParserImpl.FACTORY)
                        .withCaseSensitive(false)
                        .withQuoting(Quoting.BACK_TICK)
                        .withQuotedCasing(Casing.TO_UPPER)
                        .withUnquotedCasing(Casing.TO_UPPER)
                        .withConformance(SqlConformanceEnum.ORACLE_12))
                .build();

        this.frameworkConfig = config;
    }

    @Test
    public void testCustomGramJackyJob() throws SqlParseException {
        String sql = "JACKY JOB  'select ids, name from test where id < 5'";

        SqlParser parser = SqlParser.create(sql, this.frameworkConfig.getParserConfig());

        SqlNode sqlNode = parser.parseQuery();

        System.out.println(sqlNode);
        assertTrue(sqlNode instanceof SqlJackyJob);
    }

}
