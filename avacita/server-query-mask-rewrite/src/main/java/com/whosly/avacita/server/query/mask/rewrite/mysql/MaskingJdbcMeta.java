package com.whosly.avacita.server.query.mask.rewrite.mysql;

import com.whosly.avacita.server.query.mask.rewrite.rule.MaskingRuleConfig;
import com.whosly.avacita.server.query.mask.rewrite.rule.MaskingRuleType;
import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MaskingJdbcMeta extends JdbcMeta {
    private static final Logger LOG = LoggerFactory.getLogger(MaskingJdbcMeta.class);

    private static final Set<String> PASSTHROUGH_KEYWORDS = new HashSet<>(Arrays.asList(
            "SHOW", "DESC", "DESCRIBE", "EXPLAIN", "USE", "SET"
    ));

    private final MaskingConfigMeta maskingConfigMeta;
    private SqlParser.Config parserConfig;
    private final Map<String, Signature> signatureCache = new ConcurrentHashMap<>();
    private String currentSchema;
    private Connection dbConnection;
    private final Map<String, List<String>> tableColumnsCache = new ConcurrentHashMap<>();

    public MaskingJdbcMeta(String url, Properties info, MaskingConfigMeta maskingConfigMeta) throws SQLException {
        super(url, info);
        this.maskingConfigMeta = maskingConfigMeta;
        init();
    }

    private void init() {
        this.parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL)
                .setConformance(SqlConformanceEnum.MYSQL_5)
                .build();
    }

    @Override
    public void openConnection(ConnectionHandle ch, java.util.Map<String, String> properties) {
        super.openConnection(ch, properties);
        try {
            this.dbConnection = getConnection(ch.id);
            this.currentSchema = this.dbConnection.getCatalog();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Meta.StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
        try {
            String newSql = rewriteSql(sql);
            LOG.info("prepare重写前SQL: {}", sql);
            LOG.info("prepare重写后SQL: {}", newSql);
            return super.prepare(ch, newSql, maxRowCount);
        } catch (Exception e) {
            throw new RuntimeException("SQL重写失败: " + e.getMessage(), e);
        }
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle sh, String sql, long maxRowCount,
                                           int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {
        try {
            String newSql = rewriteSql(sql);
            LOG.info("重写前SQL: {}", sql);
            LOG.info("重写后SQL: {}", newSql);
            final ExecuteResult result = super.prepareAndExecute(sh, newSql, maxRowCount, maxRowsInFirstFrame, callback);
            if (result.resultSets != null && !result.resultSets.isEmpty()) {
                MetaResultSet mrs = result.resultSets.get(0);
                if (mrs.signature != null) {
                    signatureCache.put(String.valueOf(sh.id), mrs.signature);
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException("SQL重写失败: " + e.getMessage(), e);
        }
    }

    @Override
    public Frame fetch(StatementHandle sh, long offset, int fetchMaxRowCount)
            throws NoSuchStatementException, MissingResultsException {
        return super.fetch(sh, offset, fetchMaxRowCount);
    }

    private boolean isPassthroughSql(String sql) {
        String trimmed = sql.trim().toUpperCase(Locale.ROOT);
        for (String keyword : PASSTHROUGH_KEYWORDS) {
            if (trimmed.startsWith(keyword + " ")) {
                return true;
            }
        }
        return false;
    }

    private String rewriteSql(String sql) throws Exception {
        if (isPassthroughSql(sql)) {
            // 直接返回原SQL，不做任何处理
            return sql;
        }

        LOG.debug("原始 SQL: {}", sql);
        SqlParser parser = SqlParser.create(sql, this.parserConfig);
        SqlNode sqlNode = parser.parseQuery();
        SqlNode rewritten = sqlNode.accept(new MaskingSqlRewriter(this));
        SqlPrettyWriter writer = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);

        // 关键：不加反引号.在 SqlPrettyWriter 上加 setQuoteAllIdentifiers(false)，即可彻底解决函数名被加反引号问题
        // 生成的 SQL 就是 CONCAT(LEFT(tel, 3), '****', RIGHT(tel, 4))
        writer.setQuoteAllIdentifiers(false);
        rewritten.unparse(writer, 0, 0);
        String rewriteSql = writer.toString();
        LOG.debug("改写后 SQL: {}", rewriteSql);
        System.out.println("[MaskingJdbcMeta] \noriginalSql SQL:" + sql + "\nModified SQL: " + rewriteSql);
        return rewriteSql;
    }

    @Override
    public void closeStatement(StatementHandle sh) {
        super.closeStatement(sh);
        signatureCache.remove(String.valueOf(sh.id));
    }

    public Connection getDbConnection() {
        return this.dbConnection;
    }

    public String getCurrentSchema() {
        return this.currentSchema;
    }

    public List<String> getTableColumns(String table) {
        return tableColumnsCache.computeIfAbsent(table, t -> {
            List<String> columns = new ArrayList<>();
            try {
                DatabaseMetaData dbMetaData = dbConnection.getMetaData();
                try (ResultSet rs = dbMetaData.getColumns(currentSchema, null, t, null)) {
                    while (rs.next()) {
                        columns.add(rs.getString("COLUMN_NAME"));
                    }
                }
            } catch (SQLException e) {
                LOG.warn("获取表 {} 列信息失败: {}", t, e.getMessage());
            }
            return columns;
        });
    }

    static class MaskingSqlRewriter extends SqlShuttle {
        private final MaskingJdbcMeta meta;
        private final Map<String, String> aliasToTable = new HashMap<>();

        public MaskingSqlRewriter(MaskingJdbcMeta meta) {
            this.meta = meta;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            switch (call.getKind()) {
                case SELECT:
                    return handleSelect((SqlSelect) call);
                case UNION:
                case INTERSECT:
                case EXCEPT:
                    return super.visit(call);
                case WITH:
                    return handleWith((SqlWith) call);
                case OVER:
                    return handleOver((SqlBasicCall) call);
                default:
                    return super.visit(call);
            }
        }

        private SqlNode handleSelect(SqlSelect select) {
            if (select.getFrom() != null) {
                collectAlias(select.getFrom());
            }

            if (select.getSelectList() != null) {
                SqlNodeList newSelectList = new SqlNodeList(select.getSelectList().getParserPosition());
                for (SqlNode node : select.getSelectList()) {
                    if (node instanceof SqlIdentifier && ((SqlIdentifier) node).isStar()) {
                        newSelectList.addAll(expandStar());
                    } else {
                        newSelectList.add(processSelectItem(node));
                    }
                }
                select.setSelectList(newSelectList);
            }

            if (select.getWhere() != null) select.setWhere(select.getWhere().accept(this));
            if (select.getGroup() != null) {
                select.setGroupBy((SqlNodeList) select.getGroup().accept(this));
            }
            if (select.getHaving() != null) select.setHaving(select.getHaving().accept(this));
            if (select.getOrderList() != null) {
                select.setOrderBy((SqlNodeList) select.getOrderList().accept(this));
            }
            if (select.getFrom() != null) select.setFrom(select.getFrom().accept(this));
            return select;
        }

        private SqlNodeList expandStar() {
            SqlNodeList expandedList = new SqlNodeList(SqlParserPos.ZERO);
            for (String tableAlias : aliasToTable.keySet()) {
                String realTable = aliasToTable.get(tableAlias);
                List<String> columns = meta.getTableColumns(realTable);
                for (String columnName : columns) {
                    SqlIdentifier colIdentifier = new SqlIdentifier(
                            Arrays.asList(tableAlias, columnName), SqlParserPos.ZERO
                    );
                    expandedList.add(processSelectItem(colIdentifier));
                }
            }
            return expandedList;
        }

        private SqlNode handleWith(SqlWith with) {
            SqlNodeList newList = new SqlNodeList(with.withList.getParserPosition());
            for (SqlNode node : with.withList) {
                newList.add(node.accept(this));
            }
            SqlNode newBody = with.body.accept(this);
            return new SqlWith(with.getParserPosition(), newList, newBody);
        }

        private SqlNode handleOver(SqlBasicCall call) {
            List<SqlNode> newOperands = new ArrayList<>();
            for (SqlNode operand : call.getOperandList()) {
                newOperands.add(processSelectItem(operand));
            }
            return call.getOperator().createCall(call.getParserPosition(), newOperands);
        }

        private SqlNode processSelectItem(SqlNode node) {
            if (node instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) node;
                if (id.isStar()) return id;
                String col = id.names.get(id.names.size() - 1);
                String table = resolveTableName(id);
                MaskingRuleConfig rule = meta.maskingConfigMeta.getRule(meta.getCurrentSchema(), table, col);

                if (rule != null) {
                    // 这里直接用带前缀的 id
                    return buildMaskingExpression(id, rule);
                }
            } else if (node instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) node;
                List<SqlNode> newOperands = new ArrayList<>();
                for (SqlNode operand : call.getOperandList()) {
                    newOperands.add(processSelectItem(operand));
                }
                return call.getOperator().createCall(SqlParserPos.ZERO, newOperands);
            } else if (node instanceof SqlSelect) {
                return node.accept(this);
            }
            return node;
        }

        private SqlNode buildMaskingExpression(SqlIdentifier id, MaskingRuleConfig rule) {
            MaskingRuleType ruleType = rule.getRuleType();
            String[] params = rule.getRuleParams();

            SqlOperator concatFunc = new SqlUnresolvedFunction(
                    new SqlIdentifier("CONCAT", SqlParserPos.ZERO), null, null, null, null, SqlFunctionCategory.STRING
            );
            SqlOperator leftFunc = new SqlUnresolvedFunction(
                    new SqlIdentifier("LEFT", SqlParserPos.ZERO), null, null, null, null, SqlFunctionCategory.STRING
            );
            SqlOperator rightFunc = new SqlUnresolvedFunction(
                    new SqlIdentifier("RIGHT", SqlParserPos.ZERO), null, null, null, null, SqlFunctionCategory.STRING
            );

            switch (ruleType) {
                case MASK_FULL:
                    return SqlLiteral.createCharString("******", SqlParserPos.ZERO);
                case MASK_LEFT: {
                    int leftKeep = getIntParam(params, 0, 4);
                    SqlNode leftPart = leftFunc.createCall(SqlParserPos.ZERO, id, SqlLiteral.createExactNumeric(String.valueOf(leftKeep), SqlParserPos.ZERO));
                    SqlNode starLiteralL = SqlLiteral.createCharString("****", SqlParserPos.ZERO);
                    return concatFunc.createCall(SqlParserPos.ZERO, leftPart, starLiteralL);
                }
                case MASK_RIGHT: {
                    int rightKeep = getIntParam(params, 0, 4);
                    SqlNode rightPart = rightFunc.createCall(SqlParserPos.ZERO, id, SqlLiteral.createExactNumeric(String.valueOf(rightKeep), SqlParserPos.ZERO));
                    SqlNode starLiteralR = SqlLiteral.createCharString("****", SqlParserPos.ZERO);
                    return concatFunc.createCall(SqlParserPos.ZERO, starLiteralR, rightPart);
                }
                case MASK_MIDDLE: {
                    int left = getIntParam(params, 0, 3);
                    int right = getIntParam(params, 1, 4);
                    SqlNode leftP = leftFunc.createCall(SqlParserPos.ZERO, id, SqlLiteral.createExactNumeric(String.valueOf(left), SqlParserPos.ZERO));
                    SqlNode rightP = rightFunc.createCall(SqlParserPos.ZERO, id, SqlLiteral.createExactNumeric(String.valueOf(right), SqlParserPos.ZERO));
                    SqlNode starLiteralM = SqlLiteral.createCharString("****", SqlParserPos.ZERO);
                    return concatFunc.createCall(SqlParserPos.ZERO, leftP, starLiteralM, rightP);
                }
                case HASH: {
                    SqlOperator md5Func = new SqlUnresolvedFunction(
                            new SqlIdentifier("MD5", SqlParserPos.ZERO), null, null, null, null, SqlFunctionCategory.STRING
                    );
                    return md5Func.createCall(SqlParserPos.ZERO, id);
                }
                case REGEX: {
                    String pattern = getStringParam(params, 0, ".");
                    String replace = getStringParam(params, 1, "*");
                    SqlOperator regexFunc = new SqlUnresolvedFunction(
                            new SqlIdentifier("REGEXP_REPLACE", SqlParserPos.ZERO), null, null, null, null, SqlFunctionCategory.STRING
                    );
                    return regexFunc.createCall(SqlParserPos.ZERO, id, SqlLiteral.createCharString(pattern, SqlParserPos.ZERO), SqlLiteral.createCharString(replace, SqlParserPos.ZERO));
                }
                case ROUND: {
                    int roundTo = getIntParam(params, 0, 100);
                    SqlOperator roundFunc = new SqlUnresolvedFunction(
                            new SqlIdentifier("ROUND", SqlParserPos.ZERO), null, null, null, null, SqlFunctionCategory.NUMERIC
                    );
                    return roundFunc.createCall(SqlParserPos.ZERO, id, SqlLiteral.createExactNumeric(String.valueOf(roundTo), SqlParserPos.ZERO));
                }
                case KEEP:
                default:
                    return id;
            }
        }

        private int getIntParam(String[] params, int idx, int defaultVal) {
            if (params != null && params.length > idx && params[idx] != null && !params[idx].isEmpty()) {
                try {
                    return Integer.parseInt(params[idx]);
                } catch (NumberFormatException ignore) {}
            }
            return defaultVal;
        }

        private String getStringParam(String[] params, int idx, String defaultVal) {
            if (params != null && params.length > idx && params[idx] != null && !params[idx].isEmpty()) {
                return params[idx];
            }
            return defaultVal;
        }

        private void collectAlias(SqlNode from) {
            if (from instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) from;
                String table = id.names.get(id.names.size() - 1);
                aliasToTable.put(table, table);
            } else if (from.getKind() == SqlKind.AS) {
                SqlBasicCall call = (SqlBasicCall) from;
                SqlNode left = call.operand(0);
                SqlNode right = call.operand(1);
                if (left instanceof SqlIdentifier && right instanceof SqlIdentifier) {
                    String table = ((SqlIdentifier) left).names.get(((SqlIdentifier) left).names.size() - 1);
                    String alias = ((SqlIdentifier) right).getSimple();
                    aliasToTable.put(alias, table);
                }
            } else if (from instanceof SqlJoin) {
                collectAlias(((SqlJoin) from).getLeft());
                collectAlias(((SqlJoin) from).getRight());
            }
        }

        private String resolveTableName(SqlIdentifier id) {
            if (id.names.size() == 2) {
                String alias = id.names.get(0);
                return aliasToTable.getOrDefault(alias, alias);
            }
            if (aliasToTable.size() == 1) {
                return aliasToTable.values().iterator().next();
            }
            return null;
        }
    }
}