package com.whosly.avacita.server.query.mask.mysql;

import com.whosly.avacita.server.query.mask.ResultSetMeta;
import com.whosly.avacita.server.query.mask.rule.MaskingRule;
import com.whosly.avacita.server.query.mask.util.ValueMaskingStrategy;
import com.whosly.calcite.schema.Schemas;
import com.whosly.com.whosly.calcite.schema.mysql.MysqlSchemaLoader;
import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.jdbc.StatementInfo;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.Frameworks;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class MaskingJdbcMeta extends JdbcMeta {
    private final Logger LOG = LoggerFactory.getLogger(MaskingJdbcMeta.class);

    private MaskingConfigMeta maskingConfigMeta;
    private SqlParser.Config parserConfig;
    private ResultSetMeta currentResultSetMeta;

    /**
     * 创建根 Schema
     */
    private final SchemaPlus rootSchema = Frameworks.createRootSchema(true);

    public MaskingJdbcMeta(String url) throws SQLException {
        super(url);

        init();
    }

    public MaskingJdbcMeta(String url, Properties info) throws SQLException {
        super(url, info);

        init();
    }

    private void init() {
        this.maskingConfigMeta = new MaskingConfigMeta("mask/masking_rules.csv");

        // 创建支持 MySQL 语法的解析器配置
        this.parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL)                  // 设置词法分析器为 MySQL 模式
                .setConformance(SqlConformanceEnum.MYSQL_5)  // 支持 MySQL 5.x 语法
                .build();

//        // 创建框架配置
//        FrameworkConfig config = Frameworks.newConfigBuilder()
//                .defaultSchema(rootSchema)
//                .parserConfig(parserConfig)         // 应用解析器配置
//                .build();
//        this.frameworkConfig = config;
//
//        // 使用配置创建查询规划器
//        Planner planner = Frameworks.getPlanner(config);
    }

    // ====================== 连接管理 ======================
    @Override
    public void openConnection(ConnectionHandle ch, Map<String, String> properties) {
        super.openConnection(ch, properties);

        try {
            Connection connection = Schemas.getConnection();
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            // 以实现查询不同数据源的目的
            SchemaPlus rootSchema = calciteConnection.getRootSchema();

            // 添加数据源， 将不同数据源schema挂载到RootSchema
            String dbName = "demo";
            Schema schema = MysqlSchemaLoader.loadSchema(rootSchema, "localhost", "demo", 13307, "root", "Aa123456.");
            rootSchema.add(dbName, schema);

            // 添加模拟表, 获取子 schema 对象
            SchemaPlus dbSchema = rootSchema.getSubSchema(dbName);
            if (dbSchema != null) {
                // 在指定数据库中添加模拟表
                dbSchema.add("t_emp_virtual", new AbstractTable() {
                    @Override
                    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                        return typeFactory.builder()
                                .add("tel", typeFactory.createSqlType(SqlTypeName.VARCHAR, 20))
                                .build();
                    }
                });
            } else {
                LOG.error("无法找到数据库 schema: {}", dbName);
            }

//            Connection connection = getConnection(ch.id);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeConnection(Meta.ConnectionHandle ch) {
        super.closeConnection(ch);
    }

    // ====================== SQL 执行 ======================
    // prepare
    /**
     * INSERT、DELETE、UPDATE、SELECT
     */
    @Override
    public Meta.ExecuteResult prepareAndExecute(Meta.StatementHandle h, String sql, long maxRowCount,
                                                Meta.PrepareCallback callback) throws NoSuchStatementException {

        try {
//            Connection conn = getConnection(h.connectionId);

            String rewriteSql = rewriteSql(sql);

            ExecuteResult executeResult = super.prepareAndExecute(h, rewriteSql, maxRowCount, callback);

            return executeResult;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * INSERT、DELETE、UPDATE、SELECT
     */
    @Override
    public ExecuteResult prepareAndExecute(StatementHandle stmtHandle, String sql, long maxRowCount,
                                           int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {
        try {
            String rewrittenSql = rewriteSql(sql);

            ExecuteResult executeResult = super.prepareAndExecute(stmtHandle, rewrittenSql, maxRowCount, maxRowsInFirstFrame, callback);

            StatementInfo statementInfo = getStatementCache().getIfPresent(stmtHandle.id);
            if (statementInfo != null) {
                getCurrentColumnList(statementInfo, executeResult);

                LOG.info("prepareAndExecute ResultSetMeta :{}。", this.currentResultSetMeta);
            }

            return executeResult;
        } catch (Exception e) {
            throw new RuntimeException("执行失败: " + e.getMessage().toString());
        }
    }

    /**
     * 是从已执行的查询中获取结果集，此时 ResultSet 可能已处于关闭状态。应确保在 prepareAndExecute 中处理元数据和数据，而不是在 fetch 中。
     */
    @Override
    public Frame fetch(Meta.StatementHandle sh, long offset, int fetchMaxRowCount) throws NoSuchStatementException, MissingResultsException {
        Frame originalFrame = super.fetch(sh, offset, fetchMaxRowCount);

        Signature signature = sh.signature;
        if(signature == null) {
            return desensitizeFrame(originalFrame, this.currentResultSetMeta.getProjectMetaData());
        }

        return desensitizeFrame(originalFrame, signature);
    }

    /**
     * 安全的脱敏处理
     */
    private Frame desensitizeFrame(Frame originalFrame, Signature signature) {
        List<ColumnMetaData> columns = signature.columns;

        return desensitizeFrame(originalFrame, columns);
    }

    /**
     * 安全的脱敏处理
     */
    private Frame desensitizeFrame(Frame originalFrame, List<ColumnMetaData> columns) {
        List<Object> encryptedRows = new ArrayList<>();

        for (Object row : originalFrame.rows) {
            Object[] dataRow = (Object[]) row;
            Object[] encryptedRow = new Object[dataRow.length];

            for (int i = 0; i < dataRow.length; i++) {
                ColumnMetaData column = columns.get(i);

                // 根据列名和规则应用脱敏
                encryptedRow[i] = applyMask(
                        dataRow[i], StringUtils.isEmpty(column.schemaName) ? column.catalogName : column.schemaName,
                        column.tableName, column.columnName
                );
            }

            encryptedRows.add(encryptedRow);
        }

        return new Frame(originalFrame.offset, originalFrame.done, encryptedRows);
    }

    private Object applyMask(Object value, String schema, String table, String column) {
        // 查询脱敏规则并应用
        MaskingRule columnRole = maskingConfigMeta.getRule(schema, table, column);

        if(columnRole == null) {
            return value;
        }

        return ValueMaskingStrategy.mask(value, columnRole.getRuleType());
    }

    private ResultSetMeta getCurrentColumnList(StatementInfo statementInfo, ExecuteResult executeResult) {
        try {
            ResultSet rs = statementInfo.getResultSet();
            if(rs.isClosed()) {
                return ResultSetMeta.empty();
            }

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            List<String> header = new ArrayList<>(columnCount);

            String schemaName = metaData.getSchemaName(1);
            String tableName = metaData.getTableName(1);

            for (int i = 0; i < columnCount; i++) {
                header.add(metaData.getColumnName(i + 1));
            }

            // sql 语句中的列投影
            List<ColumnMetaData> projectMetaData = executeResult.resultSets.stream()
                    .map(m -> {
                        return m.signature.columns;
                    }).toList()
                    .stream().flatMap(List::stream).toList();

            if(StringUtils.isEmpty(schemaName) && projectMetaData != null && !projectMetaData.isEmpty()) {
                schemaName = projectMetaData.stream().findFirst()
                        .map(m -> {
                            if(StringUtils.isEmpty(m.schemaName)) {
                                return m.catalogName;
                            }
                            return m.schemaName;
                        })
                        .orElse("");
            }

            this.currentResultSetMeta = ResultSetMeta.builder()
                    .schemaName(schemaName)
                    .tableName(tableName)
                    .projects(Collections.unmodifiableList(header))
                    .projectMetaData(projectMetaData)
                    .build();

            return this.currentResultSetMeta;
        } catch (SQLException ex) {
            LOG.error("error cause:", ex);
        }

        return ResultSetMeta.empty();
    }

    // ====================== SQL 改写逻辑 ======================
    private String rewriteSql(String sql) throws Exception {
        LOG.debug("原始 SQL: {}", sql);

        SqlParser parser = SqlParser.create(sql, this.parserConfig);
        SqlNode sqlNode = parser.parseQuery();
        if (!(sqlNode instanceof SqlSelect)) {
            return sql;
        }

        String rewriteSql = sql;
        LOG.debug("改写后 SQL: {}", rewriteSql);

        return "/*+ A */ " + rewriteSql;
    }

}
