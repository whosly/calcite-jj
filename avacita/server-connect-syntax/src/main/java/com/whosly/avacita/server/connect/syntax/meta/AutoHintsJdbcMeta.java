package com.whosly.avacita.server.connect.syntax.meta;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;

import java.sql.SQLException;
import java.util.Properties;

/**
 * 自动添加 sql  hint, 带有SQL拦截功能的Meta
 */
public class AutoHintsJdbcMeta extends JdbcMeta {
    private final SqlParser.Config parserConfig;

    public AutoHintsJdbcMeta(String url, Properties info) throws SQLException {
        super(url, info);

        this.parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL)
                .build();
    }

    public AutoHintsJdbcMeta(String url, String user, String password) throws SQLException {
        super(url, user, password);

        this.parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL)
                .build();
    }

    @Override
    public Meta.StatementHandle prepare(Meta.ConnectionHandle ch, String sql, long maxRowCount) {
        String rewrittenSql = rewriteSql(sql);

        // 执行修改后的 SQL
        return super.prepare(ch, rewrittenSql, maxRowCount);
    }

    /**
     * INSERT、DELETE、UPDATE、SELECT
     */
    @Override
    public Meta.ExecuteResult prepareAndExecute(Meta.StatementHandle h, String sql, long maxRowCount, Meta.PrepareCallback callback) throws NoSuchStatementException {
        String rewrittenSql = rewriteSql(sql);

        return super.prepareAndExecute(h, rewrittenSql, maxRowCount, callback);
    }

    /**
     * INSERT、DELETE、UPDATE、SELECT
     */
    @Override
    public Meta.ExecuteResult prepareAndExecute(Meta.StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame, Meta.PrepareCallback callback) throws NoSuchStatementException {
        String rewrittenSql = rewriteSql(sql);

        return super.prepareAndExecute(h, rewrittenSql, maxRowCount, maxRowsInFirstFrame, callback);
    }

    private String rewriteSql(String originalSql) {
        // 修改SQL：添加HINT
        String modifiedSql = "/*+ CALCITE_OPTIMIZER */ " + originalSql;

        System.out.println("[AutoHintsJdbcMeta] \noriginalSql SQL:" + originalSql + "\nModified SQL: " + modifiedSql);
        return modifiedSql;
    }
}
