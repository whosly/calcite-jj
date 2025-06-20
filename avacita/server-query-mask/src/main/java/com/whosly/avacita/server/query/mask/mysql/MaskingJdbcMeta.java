package com.whosly.avacita.server.query.mask.mysql;

import com.whosly.avacita.server.query.mask.ResultSetMeta;
import com.whosly.avacita.server.query.mask.rule.MaskingRuleConfig;
import com.whosly.avacita.server.query.mask.util.ValueMaskingStrategy;
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
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.Frameworks;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * 1. 在 prepareAndExecute 中：调用父类的方法获取原始结果，然后直接对结果中的第一个数据帧（firstFrame）进行脱敏处理。
 * prepareAndExecute 只会对首批数据（firstFrame）进行脱敏
 *
 * 2. 在 fetch 中：拦截后续的数据帧，并对它们进行脱敏处理。
 * 后续通过 fetch 方法分批获取的数据帧（Frame），如果没有在 fetch 里做脱敏处理，这些数据就不会被脱敏。
 */
public class MaskingJdbcMeta extends JdbcMeta {
    private static final Logger LOG = LoggerFactory.getLogger(MaskingJdbcMeta.class);

    private final MaskingConfigMeta maskingConfigMeta;
    private SqlParser.Config parserConfig;
    private ResultSetMeta currentResultSetMeta;

    /**
     * 创建根 Schema
     */
    private final SchemaPlus rootSchema = Frameworks.createRootSchema(true);

    // 用于缓存 statementId -> signature
    private final Map<String, Signature> signatureCache = new ConcurrentHashMap<>();

    public MaskingJdbcMeta(String url, Properties info, MaskingConfigMeta maskingConfigMeta) throws SQLException {
        super(url, info);
        this.maskingConfigMeta = maskingConfigMeta;
        init();
    }

    private void init() {
        // 创建支持 MySQL 语法的解析器配置
        this.parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL)                  // 设置词法分析器为 MySQL 模式
                .setConformance(SqlConformanceEnum.MYSQL_5)  // 支持 MySQL 5.x 语法
                .build();
    }

    // ====================== 连接管理 ======================
    @Override
    public void openConnection(ConnectionHandle ch, java.util.Map<String, String> properties) {
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeConnection(Meta.ConnectionHandle ch) {
        super.closeConnection(ch);
    }

    // ====================== SQL 执行 ======================
    @Override
    public ExecuteResult prepareAndExecute(StatementHandle sh, String sql, long maxRowCount,
                                           int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {
        final ExecuteResult result = super.prepareAndExecute(sh, sql, maxRowCount, maxRowsInFirstFrame, callback);

        // 缓存 signature，便于 fetch 时使用
        if (result.resultSets != null && !result.resultSets.isEmpty()) {
            MetaResultSet mrs = result.resultSets.get(0);
            if (mrs.signature != null) {
                signatureCache.put(String.valueOf(sh.id), mrs.signature);
            }
        }

        final List<MetaResultSet> maskedResultSets = result.resultSets.stream()
                .map(this::maskResultSet)
                .collect(Collectors.toList());

        return new ExecuteResult(maskedResultSets);
    }

    @Override
    public Frame fetch(StatementHandle sh, long offset, int fetchMaxRowCount)
            throws NoSuchStatementException, MissingResultsException {
        Frame originalFrame = super.fetch(sh, offset, fetchMaxRowCount);

        Signature signature = sh.signature;
        if (signature == null) {
            signature = signatureCache.get(String.valueOf(sh.id));
        }
        if (signature != null) {
            return desensitizeFrame(originalFrame, signature);
        }
        return originalFrame;
    }

    /**
     * 对单个 MetaResultSet（包括其 firstFrame）进行脱敏
     */
    private MetaResultSet maskResultSet(MetaResultSet resultSet) {
        // 只处理包含查询结果的 ResultSet
        if (resultSet.updateCount != -1 || resultSet.signature == null || resultSet.firstFrame == null) {
            return resultSet;
        }

        // 对 firstFrame 进行脱敏
        Frame maskedFrame = desensitizeFrame(resultSet.firstFrame, resultSet.signature);

        // 使用脱敏后的 Frame 创建新的 MetaResultSet
        return MetaResultSet.create(
                resultSet.connectionId,
                resultSet.statementId,
                resultSet.ownStatement,
                resultSet.signature,
                maskedFrame,
                resultSet.updateCount
        );
    }

    /**
     * 对数据帧（Frame）中的行数据进行脱敏处理
     */
    private Frame desensitizeFrame(Frame originalFrame, Signature signature) {
        if (originalFrame.rows == null) {
            return originalFrame;
        }
        return desensitizeFrame(originalFrame, signature.columns);
    }

    private Frame desensitizeFrame(Frame originalFrame, List<ColumnMetaData> columns) {
        List<Object> maskedRows = new ArrayList<>();

        for (Object row : originalFrame.rows) {
            Object[] dataRow = (Object[]) row;
            Object[] maskedRow = new Object[dataRow.length];

            for (int i = 0; i < dataRow.length; i++) {
                ColumnMetaData column = columns.get(i);
                // 根据列元数据和规则应用脱敏
                maskedRow[i] = applyMask(
                        dataRow[i],
                        StringUtils.defaultIfEmpty(column.schemaName, column.catalogName),
                        column.tableName,
                        column.columnName
                );
            }
            maskedRows.add(maskedRow);
        }

        return new Frame(originalFrame.offset, originalFrame.done, maskedRows);
    }

    /**
     * 根据规则应用脱敏策略
     */
    private Object applyMask(Object value, String schema, String table, String column) {
        if (value == null) {
            return null;
        }

        MaskingRuleConfig columnRule = maskingConfigMeta.getMatchingRule(schema, table, column);

        if (columnRule == null) {
            return value;
        }

        return ValueMaskingStrategy.mask(value, columnRule);
    }

    // ====================== SQL 改写逻辑 ======================
    private String rewriteSql(String sql) throws Exception {
        LOG.debug("原始 SQL: {}", sql);

        SqlParser parser = SqlParser.create(sql, this.parserConfig);
        SqlNode sqlNode = parser.parseQuery();
        if (!(sqlNode instanceof SqlSelect)) {
            return sql;
        }

        String rewriteSql = "/*+ A */ " + sql;

        LOG.debug("改写后 SQL: {}", rewriteSql);
        System.out.println("[MaskingJdbcMeta] \noriginalSql SQL:" + sql + "\nModified SQL: " + rewriteSql);

        return rewriteSql;
    }

    @Override
    public void closeStatement(StatementHandle sh) {
        // 每当 Avatica Server 关闭一个 Statement 时，都会移除对应的 signature 缓存。 Statement 生命周期和 signature 是一一对应的。
        super.closeStatement(sh);
        signatureCache.remove(String.valueOf(sh.id));
    }
}
