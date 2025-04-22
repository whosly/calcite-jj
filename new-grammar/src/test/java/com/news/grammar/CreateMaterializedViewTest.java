package com.news.grammar;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.CreateMaterializedView;
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
 * 新实现语法的测试用例：
 *
 * CREATE MATERIALIZED VIEW
 * [ IF NOT EXISTS ] view_name
 * AS query
 */
public class CreateMaterializedViewTest {

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

    /**
     * CREATE MATERIALIZED VIEW
     * [ IF NOT EXISTS ] view_name
     * AS query
     */
    @Test
    public void testCustomGramCreateMaterialized() throws SqlParseException {
        String sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS test.demo.materializationName AS SELECT * FROM `system`";

        SqlParser parser = SqlParser.create(sql, this.frameworkConfig.getParserConfig());

        SqlNode sqlNode = parser.parseQuery();

        System.out.println(sqlNode);
        assertTrue(sqlNode instanceof CreateMaterializedView);
    }
}
