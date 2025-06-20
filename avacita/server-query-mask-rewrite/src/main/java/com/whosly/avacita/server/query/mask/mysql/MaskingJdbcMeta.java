package com.whosly.avacita.server.query.mask.mysql;

import com.whosly.calcite.schema.Schemas;
import com.whosly.com.whosly.calcite.schema.mysql.MysqlSchemaLoader;
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
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.Frameworks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MaskingJdbcMeta extends JdbcMeta {
    private static final Logger LOG = LoggerFactory.getLogger(MaskingJdbcMeta.class);

    private final MaskingConfigMeta maskingConfigMeta;
    private SqlParser.Config parserConfig;
    private final Map<String, Signature> signatureCache = new ConcurrentHashMap<>();

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
            Connection connection = Schemas.getConnection();
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            String dbName = "demo";
            Schema schema = MysqlSchemaLoader.loadSchema(rootSchema, "localhost", "demo", 13307, "root", "Aa123456.");
            rootSchema.add(dbName, schema);
            SchemaPlus dbSchema = rootSchema.getSubSchema(dbName);
            if (dbSchema != null) {
                dbSchema.add("t_emp_virtual", new AbstractTable() {
                    @Override
                    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                        return typeFactory.builder()
                                .add("tel", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR, 20))
                                .build();
                    }
                });
            } else {
                LOG.error("无法找到数据库 schema: {}", dbName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
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

    private String rewriteSql(String sql) throws Exception {
        LOG.debug("原始 SQL: {}", sql);
        SqlParser parser = SqlParser.create(sql, this.parserConfig);
        SqlNode sqlNode = parser.parseQuery();
        SqlNode rewritten = sqlNode.accept(new MaskingSqlRewriter(maskingConfigMeta));
        SqlPrettyWriter writer = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);
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

    // SQL重写递归处理类 - 兼容 Calcite 1.35.0
    static class MaskingSqlRewriter extends SqlShuttle {
        private final MaskingConfigMeta config;
        private final Map<String, String> aliasToTable = new HashMap<>();

        public MaskingSqlRewriter(MaskingConfigMeta config) {
            this.config = config;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            switch (call.getKind()) {
                case SELECT:
                    return handleSelect((SqlSelect) call);
                case UNION:
                case INTERSECT:
                case EXCEPT:
                    return super.visit(call); // 默认的 visit(SqlCall) 会递归处理 operands
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
                    newSelectList.add(processSelectItem(node));
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
                String col = id.names.get(id.names.size() - 1);
                String table = resolveTableName(id);
                String func = config.getMaskFunc(table, col);
                if (func != null) {
                    SqlOperator op = new SqlUnresolvedFunction(new SqlIdentifier(func, SqlParserPos.ZERO), null, null, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
                    return op.createCall(SqlParserPos.ZERO, id);
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

        private void collectAlias(SqlNode from) {
            if (from instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) from;
                String table = id.names.get(id.names.size() - 1);
                aliasToTable.put(table, table);
            } else if (from.getKind() == SqlKind.AS) {
                SqlBasicCall call = (SqlBasicCall)from;
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
            if (!aliasToTable.isEmpty()) {
                return aliasToTable.values().iterator().next();
            }
            return null;
        }
    }
}