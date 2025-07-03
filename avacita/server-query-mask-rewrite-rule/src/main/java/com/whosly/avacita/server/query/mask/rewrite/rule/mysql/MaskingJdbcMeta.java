package com.whosly.avacita.server.query.mask.rewrite.rule.mysql;

import com.whosly.avacita.server.query.mask.rewrite.rule.rules.MaskingConfigMeta;
import com.whosly.avacita.server.query.mask.rewrite.rule.rules.MaskingRuleConfig;
import com.whosly.avacita.server.query.mask.rewrite.rule.rules.MaskingRuleType;
import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.Frameworks;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 带脱敏功能的JdbcMeta实现
 * 支持SQL重写和结果集脱敏
 */
public class MaskingJdbcMeta extends JdbcMeta {
    private static final Logger LOG = LoggerFactory.getLogger(MaskingJdbcMeta.class);

    private final MaskingConfigMeta maskingConfigMeta;
    private final SqlParser.Config parserConfig;
    private final Map<String, List<String>> tableColumnsCache = new ConcurrentHashMap<>();
    private final Connection dbConnection;

    public MaskingJdbcMeta(String url, Properties info, MaskingConfigMeta maskingConfigMeta) throws SQLException {
        super(url, info);
        this.maskingConfigMeta = maskingConfigMeta;
        this.dbConnection = DriverManager.getConnection(url, info);
        
        // 创建支持MySQL语法的解析器配置
        this.parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL)
                .setConformance(SqlConformanceEnum.MYSQL_5)
                .build();
    }

    // ====================== 连接管理 ======================
    @Override
    public void openConnection(ConnectionHandle ch, Map<String, String> properties) {
        super.openConnection(ch, properties);
    }

    // ====================== SQL执行和重写 ======================
    @Override
    public ExecuteResult prepareAndExecute(StatementHandle sh, String sql, long maxRowCount,
                                         int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {
        LOG.info("原始SQL: {}", sql);
        
        try {
            // 重写SQL以应用脱敏规则
            String rewrittenSql = rewriteSqlWithMasking(sql);
            LOG.info("重写后SQL: {}", rewrittenSql);
            
            // 使用重写后的SQL执行查询
            ExecuteResult result = super.prepareAndExecute(sh, rewrittenSql, maxRowCount, maxRowsInFirstFrame, callback);
            
            // 对结果进行脱敏处理
            return maskExecuteResult(result);
            
        } catch (Exception e) {
            LOG.error("SQL重写或执行失败，使用原始SQL: {}", e.getMessage());
            return super.prepareAndExecute(sh, sql, maxRowCount, maxRowsInFirstFrame, callback);
        }
    }

    @Override
    public Frame fetch(StatementHandle sh, long offset, int fetchMaxRowCount)
            throws NoSuchStatementException, MissingResultsException {
        Frame originalFrame = super.fetch(sh, offset, fetchMaxRowCount);
        
        // 对后续数据帧进行脱敏处理
        return maskFrame(originalFrame, sh.signature);
    }

    // ====================== SQL重写逻辑 ======================
    private String rewriteSqlWithMasking(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNode sqlNode = parser.parseQuery();
        
        if (!(sqlNode instanceof SqlSelect)) {
            return sql;
        }
        
        SqlSelect select = (SqlSelect) sqlNode;
        
        // 收集表别名信息
        Map<String, String> tableAliases = collectTableAliases(select);
        
        // 重写SELECT子句
        SqlNodeList newSelectList = rewriteSelectList(select.getSelectList(), tableAliases);
        
        // 创建新的SELECT语句
        SqlSelect newSelect = (SqlSelect) select.getOperator().createCall(
                select.getParserPosition(),
                newSelectList,
                (SqlNode) select.getFrom(),  // 强制转换为SqlNode类型
                select.getWhere(),
                select.getGroup(),
                select.getHaving(),
                select.getOrderList(),
                select.getOffset(),
                select.getFetch()
        );
        
        // 使用MySQL方言格式化SQL
        return newSelect.toSqlString(MysqlSqlDialect.DEFAULT).getSql();
    }

    private Map<String, String> collectTableAliases(SqlSelect select) {
        Map<String, String> aliases = new HashMap<>();
        
        SqlNode from = select.getFrom();
        if (from instanceof SqlJoin) {
            collectAliasesFromJoin((SqlJoin) from, aliases);
        } else if (from instanceof SqlIdentifier) {
            collectAliasesFromIdentifier((SqlIdentifier) from, aliases);
        }
        
        return aliases;
    }

    private void collectAliasesFromJoin(SqlJoin join, Map<String, String> aliases) {
        if (join.getLeft() instanceof SqlJoin) {
            collectAliasesFromJoin((SqlJoin) join.getLeft(), aliases);
        } else if (join.getLeft() instanceof SqlIdentifier) {
            collectAliasesFromIdentifier((SqlIdentifier) join.getLeft(), aliases);
        }
        
        if (join.getRight() instanceof SqlJoin) {
            collectAliasesFromJoin((SqlJoin) join.getRight(), aliases);
        } else if (join.getRight() instanceof SqlIdentifier) {
            collectAliasesFromIdentifier((SqlIdentifier) join.getRight(), aliases);
        }
    }

    private void collectAliasesFromIdentifier(SqlIdentifier identifier, Map<String, String> aliases) {
        if (identifier.names.size() >= 2) {
            String tableName = identifier.names.get(identifier.names.size() - 2);
            String alias = identifier.names.get(identifier.names.size() - 1);
            aliases.put(alias, tableName);
        }
    }

    private SqlNodeList rewriteSelectList(SqlNodeList selectList, Map<String, String> tableAliases) {
        List<SqlNode> newSelectItems = new ArrayList<>();
        
        for (SqlNode selectItem : selectList) {
            if (selectItem instanceof SqlIdentifier) {
                SqlIdentifier identifier = (SqlIdentifier) selectItem;
                
                // 处理SELECT *的情况
                if (identifier.names.size() == 1 && "*".equals(identifier.names.get(0))) {
                    newSelectItems.addAll(expandStarSelect(identifier, tableAliases));
                } else {
                    // 处理普通列
                    SqlNode maskedColumn = applyMaskingToColumn(identifier, tableAliases);
                    newSelectItems.add(maskedColumn);
                }
            } else {
                newSelectItems.add(selectItem);
            }
        }
        
        return new SqlNodeList(newSelectItems, selectList.getParserPosition());
    }

    private List<SqlNode> expandStarSelect(SqlIdentifier starIdentifier, Map<String, String> tableAliases) {
        List<SqlNode> expandedColumns = new ArrayList<>();
        
        // 如果没有表别名，使用默认schema
        String schema = "demo";
        String table = "t_emp"; // 默认表名
        
        // 获取表的所有列
        List<String> columns = getTableColumns(schema, table);
        
        for (String column : columns) {
            SqlIdentifier columnIdentifier = new SqlIdentifier(
                    Arrays.asList(table, column),
                    starIdentifier.getParserPosition()
            );
            
            SqlNode maskedColumn = applyMaskingToColumn(columnIdentifier, tableAliases);
            expandedColumns.add(maskedColumn);
        }
        
        return expandedColumns;
    }

    private List<String> getTableColumns(String schema, String table) {
        String cacheKey = schema + "." + table;
        
        return tableColumnsCache.computeIfAbsent(cacheKey, k -> {
            List<String> columns = new ArrayList<>();
            try {
                DatabaseMetaData metaData = dbConnection.getMetaData();
                ResultSet rs = metaData.getColumns(schema, null, table, null);
                
                while (rs.next()) {
                    columns.add(rs.getString("COLUMN_NAME"));
                }
                rs.close();
            } catch (SQLException e) {
                LOG.error("获取表列信息失败: {}.{}", schema, table, e);
                // 返回默认列
                columns.addAll(Arrays.asList("id", "name", "tel", "email", "salary"));
            }
            return columns;
        });
    }

    private SqlNode applyMaskingToColumn(SqlIdentifier columnIdentifier, Map<String, String> tableAliases) {
        String columnName = columnIdentifier.names.get(columnIdentifier.names.size() - 1);
        String tableName = null;
        String schemaName = "demo";
        
        // 确定表名
        if (columnIdentifier.names.size() >= 2) {
            String possibleTable = columnIdentifier.names.get(columnIdentifier.names.size() - 2);
            tableName = tableAliases.getOrDefault(possibleTable, possibleTable);
        } else {
            // 如果没有表前缀，使用默认表
            tableName = "t_emp";
        }
        
        // 查找脱敏规则
        MaskingRuleConfig rule = maskingConfigMeta.getMatchingRule(schemaName, tableName, columnName);
        
        if (rule != null && rule.getRuleType() != MaskingRuleType.KEEP) {
            return createMaskedColumnExpression(columnIdentifier, rule);
        }
        
        return columnIdentifier;
    }

    private SqlNode createMaskedColumnExpression(SqlIdentifier columnIdentifier, MaskingRuleConfig rule) {
        String columnName = columnIdentifier.names.get(columnIdentifier.names.size() - 1);
        String[] params = rule.getRuleParams();
        
        switch (rule.getRuleType()) {
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
                return columnIdentifier;
        }
    }

    private SqlNode createFullMaskExpression(SqlIdentifier columnIdentifier) {
        return SqlLiteral.createCharString("******", columnIdentifier.getParserPosition());
    }

    private SqlNode createLeftMaskExpression(SqlIdentifier columnIdentifier, int keepChars) {
        // LEFT(column, keepChars) + '******'
        SqlNode leftExpr = SqlStdOperatorTable.instance().getOperator("LEFT").createCall(
                columnIdentifier.getParserPosition(),
                columnIdentifier,
                SqlLiteral.createExactNumeric(String.valueOf(keepChars), columnIdentifier.getParserPosition())
        );

        SqlNode maskLiteral = SqlLiteral.createCharString("******", columnIdentifier.getParserPosition());
        
        return SqlStdOperatorTable.CONCAT.createCall(
                columnIdentifier.getParserPosition(),
                leftExpr,
                maskLiteral
        );
    }

    private SqlNode createRightMaskExpression(SqlIdentifier columnIdentifier, int keepChars) {
        // '******' + RIGHT(column, keepChars)
        SqlNode rightExpr = SqlStdOperatorTable.instance().getOperator("RIGHT").createCall(
                columnIdentifier.getParserPosition(),
                columnIdentifier,
                SqlLiteral.createExactNumeric(String.valueOf(keepChars), columnIdentifier.getParserPosition())
        );

        SqlNode maskLiteral = SqlLiteral.createCharString("******", columnIdentifier.getParserPosition());
        
        return SqlStdOperatorTable.CONCAT.createCall(
                columnIdentifier.getParserPosition(),
                maskLiteral,
                rightExpr
        );
    }

    private SqlNode createMiddleMaskExpression(SqlIdentifier columnIdentifier, int leftChars, int rightChars) {
        // LEFT(column, leftChars) + '******' + RIGHT(column, rightChars)
        SqlNode leftExpr = SqlStdOperatorTable.instance().getOperator("LEFT").createCall(
                columnIdentifier.getParserPosition(),
                columnIdentifier,
                SqlLiteral.createExactNumeric(String.valueOf(leftChars), columnIdentifier.getParserPosition())
        );

        SqlNode rightExpr = SqlStdOperatorTable.instance().getOperator("RIGHT").createCall(
                columnIdentifier.getParserPosition(),
                columnIdentifier,
                SqlLiteral.createExactNumeric(String.valueOf(rightChars), columnIdentifier.getParserPosition())
        );

        SqlNode maskLiteral = SqlLiteral.createCharString("******", columnIdentifier.getParserPosition());
        
        return SqlStdOperatorTable.CONCAT.createCall(
                columnIdentifier.getParserPosition(),
                leftExpr,
                maskLiteral,
                rightExpr
        );
    }

    private SqlNode createHashExpression(SqlIdentifier columnIdentifier) {
        // MD5(column)
        return SqlStdOperatorTable.instance().getOperator("MD5").createCall(
                columnIdentifier.getParserPosition(),
                columnIdentifier
        );

    }

    private SqlNode createRoundExpression(SqlIdentifier columnIdentifier, int roundTo) {
        // ROUND(column / roundTo) * roundTo
        SqlNode division = SqlStdOperatorTable.DIVIDE.createCall(
                columnIdentifier.getParserPosition(),
                columnIdentifier,
                SqlLiteral.createExactNumeric(String.valueOf(roundTo), columnIdentifier.getParserPosition())
        );
        
        SqlNode rounded = SqlStdOperatorTable.ROUND.createCall(
                columnIdentifier.getParserPosition(),
                division
        );
        
        return SqlStdOperatorTable.MULTIPLY.createCall(
                columnIdentifier.getParserPosition(),
                rounded,
                SqlLiteral.createExactNumeric(String.valueOf(roundTo), columnIdentifier.getParserPosition())
        );
    }

    private SqlNode createRegexExpression(SqlIdentifier columnIdentifier, String regex) {
        // REGEXP_REPLACE(column, regex, '*')
        return SqlStdOperatorTable.instance().getOperator("REGEXP_REPLACE").createCall(
                columnIdentifier.getParserPosition(),
                columnIdentifier,
                SqlLiteral.createCharString(regex, columnIdentifier.getParserPosition()),
                SqlLiteral.createCharString("*", columnIdentifier.getParserPosition())
        );
    }

    // ====================== 结果集脱敏处理 ======================
    private ExecuteResult maskExecuteResult(ExecuteResult result) {
        if (result.resultSets == null) {
            return result;
        }
        
        List<MetaResultSet> maskedResultSets = result.resultSets.stream()
                .map(this::maskResultSet)
                .collect(Collectors.toList());
        
        return new ExecuteResult(maskedResultSets);
    }

    private MetaResultSet maskResultSet(MetaResultSet resultSet) {
        if (resultSet.updateCount != -1 || resultSet.signature == null) {
            return resultSet;
        }
        
        if (resultSet.firstFrame != null) {
            Frame maskedFrame = maskFrame(resultSet.firstFrame, resultSet.signature);
            return MetaResultSet.create(
                    resultSet.connectionId,
                    resultSet.statementId,
                    resultSet.ownStatement,
                    resultSet.signature,
                    maskedFrame,
                    resultSet.updateCount
            );
        }
        
        return resultSet;
    }

    private Frame maskFrame(Frame frame, Signature signature) {
        if (frame.rows == null || signature == null || signature.columns == null) {
            return frame;
        }
        
        List<Object> maskedRows = new ArrayList<>();
        
        for (Object row : frame.rows) {
            Object[] dataRow = (Object[]) row;
            Object[] maskedRow = new Object[dataRow.length];
            
            for (int i = 0; i < dataRow.length && i < signature.columns.size(); i++) {
                ColumnMetaData column = signature.columns.get(i);
                maskedRow[i] = applyMaskingToValue(
                        dataRow[i],
                        column.schemaName,
                        column.tableName,
                        column.columnName
                );
            }
            maskedRows.add(maskedRow);
        }
        
        return new Frame(frame.offset, frame.done, maskedRows);
    }

    private Object applyMaskingToValue(Object value, String schema, String table, String column) {
        if (value == null) {
            return null;
        }
        
        MaskingRuleConfig rule = maskingConfigMeta.getMatchingRule(schema, table, column);
        if (rule == null || rule.getRuleType() == MaskingRuleType.KEEP) {
            return value;
        }
        
        return applyMaskingStrategy(value, rule);
    }

    private Object applyMaskingStrategy(Object value, MaskingRuleConfig rule) {
        String[] params = rule.getRuleParams();
        
        switch (rule.getRuleType()) {
            case MASK_FULL:
                return "******";
                
            case MASK_LEFT:
                int keepLeft = params.length > 0 ? Integer.parseInt(params[0]) : 3;
                return maskLeft(String.valueOf(value), keepLeft);
                
            case MASK_RIGHT:
                int keepRight = params.length > 0 ? Integer.parseInt(params[0]) : 3;
                return maskRight(String.valueOf(value), keepRight);
                
            case MASK_MIDDLE:
                int leftChars = params.length > 0 ? Integer.parseInt(params[0]) : 3;
                int rightChars = params.length > 1 ? Integer.parseInt(params[1]) : 4;
                return maskMiddle(String.valueOf(value), leftChars, rightChars);
                
            case HASH:
                return hashValue(String.valueOf(value));
                
            case ROUND:
                int roundTo = params.length > 0 ? Integer.parseInt(params[0]) : 100;
                return roundValue(value, roundTo);
                
            case REGEX:
                String regex = params.length > 0 ? params[0] : ".";
                return regexMask(String.valueOf(value), regex);
                
            default:
                return value;
        }
    }

    private String maskLeft(String value, int keepChars) {
        if (value.length() <= keepChars) {
            return value + "******";
        }
        return value.substring(0, keepChars) + "******";
    }

    private String maskRight(String value, int keepChars) {
        if (value.length() <= keepChars) {
            return "******" + value;
        }
        return "******" + value.substring(value.length() - keepChars);
    }

    private String maskMiddle(String value, int leftChars, int rightChars) {
        if (value.length() <= leftChars + rightChars) {
            return value;
        }
        return value.substring(0, leftChars) + "******" + value.substring(value.length() - rightChars);
    }

    private String hashValue(String value) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            byte[] hashBytes = md.digest(value.getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            return "******";
        }
    }

    private Object roundValue(Object value, int roundTo) {
        if (value instanceof Number) {
            double num = ((Number) value).doubleValue();
            return Math.round(num / roundTo) * roundTo;
        }
        return value;
    }

    private String regexMask(String value, String regex) {
        try {
            return value.replaceAll(regex, "*");
        } catch (Exception e) {
            return value;
        }
    }

    @Override
    public void closeStatement(StatementHandle sh) {
        super.closeStatement(sh);
    }

    @Override
    public void closeConnection(ConnectionHandle ch) {
        super.closeConnection(ch);
        try {
            if (dbConnection != null && !dbConnection.isClosed()) {
                dbConnection.close();
            }
        } catch (SQLException e) {
            LOG.error("关闭数据库连接失败", e);
        }
    }
} 