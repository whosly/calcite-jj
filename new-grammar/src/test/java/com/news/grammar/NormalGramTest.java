package com.news.grammar;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.JackySqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.junit.Before;
import org.junit.Test;

/**
 * 常规语法
 */
public class NormalGramTest {

    private FrameworkConfig frameworkConfig;

    @Before
    public void setUp() throws Exception {
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.config()
                        .withParserFactory(JackySqlParserImpl.FACTORY)
                        .withCaseSensitive(false)
                        .withQuoting(Quoting.BACK_TICK)
//                        .withQuoting(Quoting.DOUBLE_QUOTE)
                        .withQuotedCasing(Casing.TO_UPPER)
//                        .withQuotedCasing(Casing.UNCHANGED)
                        .withUnquotedCasing(Casing.TO_UPPER)
                        .withConformance(SqlConformanceEnum.ORACLE_12))
                .build();

        this.frameworkConfig = config;
    }

    @Test
    public void testNormal() throws SqlParseException {
        String sql = "select ids, name from test where id < 5";

        SqlParser parser = SqlParser.create(sql, this.frameworkConfig.getParserConfig());

        try {
            SqlNode sqlNode = parser.parseStmt();

            System.out.println(sqlNode.toString());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
