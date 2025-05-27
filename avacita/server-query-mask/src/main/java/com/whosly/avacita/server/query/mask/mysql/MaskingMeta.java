package com.whosly.avacita.server.query.mask.mysql;

import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

// 脱敏配置管理类保持不变（见之前的代码）
// 脱敏规则类保持不变（见之前的代码）
// RelNode 处理器保持不变（见之前的代码）

public class MaskingMeta implements Meta {
    private final Logger LOG = LoggerFactory.getLogger(MaskingMeta.class);
    private final Map<String, ConnectionState> connections = new ConcurrentHashMap<>();
    private final MaskingManager configManager;
    private final CalciteConnection calciteConnection;
    private final FrameworkConfig frameworkConfig;

    public MaskingMeta(CalciteConnection calciteConnection, MaskingManager configManager) {
        this.calciteConnection = calciteConnection;
        this.configManager = configManager;
        this.frameworkConfig = Frameworks.newConfigBuilder()
                .defaultSchema(calciteConnection.getRootSchema())
                .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
                .build();
    }

    // ====================== 连接管理 ======================
    @Override
    public ConnectionHandle newConnection(String url, Properties info, ConnectionProperties connProps) {
        String connId = UUID.randomUUID().toString();
        connections.put(connId, new ConnectionState(connId, info, connProps));
        return new ConnectionHandle(connId);
    }

    @Override
    public void closeConnection(ConnectionHandle ch) {
        connections.remove(ch.id);
    }

    @Override
    public void openConnection(ConnectionHandle ch, Map<String, String> properties) {

    }

    // ====================== SQL 执行 ======================
    @Override
    public ExecuteResult prepareAndExecute(
            StatementHandle stmtHandle,
            String sql,
            long maxRowCount,
            int unknownParam, // 接口中固定参数，暂未明确用途
            PrepareCallback callback
    ) throws NoSuchStatementException {
        try {
            String rewrittenSql = rewriteSql(sql);
            try (Statement stmt = calciteConnection.createStatement()) {
                ResultSet rs = stmt.executeQuery(rewrittenSql);
                return buildExecuteResult(stmtHandle, rs);
            }
        } catch (Exception e) {
            throw new RuntimeException("执行失败: " + e.getMessage().toString());
        }
    }

    private ExecuteResult buildExecuteResult(StatementHandle stmtHandle, ResultSet rs) throws SQLException {
        List<MetaResultSet> results = new ArrayList<>();
        Signature signature = buildSignature(rs);
        Frame firstFrame = buildFrame(rs);
        results.add(MetaResultSet.create(
                stmtHandle.connectionId,
                stmtHandle.id,
                true,
                signature,
                firstFrame
        ));
        return new ExecuteResult(results);
    }

    private Signature buildSignature(ResultSet rs) throws SQLException {
        List<ColumnMetaData> columns = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            // 获取列的 Java 类型
            Class<?> javaType = getJavaType(metaData.getColumnClassName(i));

            boolean autoIncrement = metaData.isAutoIncrement(i);
            int nullableInt = metaData.isNullable(i) == ResultSetMetaData.columnNullable? 1 : 0;
            boolean signed = metaData.isSigned(i);
            int displaySize = metaData.getColumnDisplaySize(i);
            String label = metaData.getColumnLabel(i);
            String schemaName = metaData.getSchemaName(i);
            int precision = metaData.getPrecision(i);
            int scale = metaData.getScale(i);
            String tableName = metaData.getTableName(i);
            ColumnMetaData.AvaticaType type = createAvaticaType(metaData, i);
            boolean readOnly = metaData.isReadOnly(i);
            boolean writable = false;
            boolean definitelyWritable = false;
            String columnClassName = metaData.getColumnClassName(i);

            columns.add(new ColumnMetaData(
                    i - 1,
                    autoIncrement,
                    metaData.isCaseSensitive(i),
                    metaData.isSearchable(i),
                    metaData.isCurrency(i),
                    nullableInt,
                    signed,
                    displaySize,
                    label,
                    metaData.getColumnName(i),
                    schemaName,
                    precision,
                    scale,
                    tableName,
                    metaData.getCatalogName(i),
                    type,
                    readOnly,
                    writable,
                    definitelyWritable,
                    columnClassName
            ));
        }

        return new Signature(
                columns,
                null,
                null,
                null,
                Meta.CursorFactory.ARRAY,
                Meta.StatementType.SELECT
        );
    }

    // 将列的类名转换为 Class 对象
    private Class<?> getJavaType(String className) {
        try {
            switch (className) {
                case "java.lang.String": return String.class;
                case "java.lang.Integer": return Integer.class;
                case "java.lang.Long": return Long.class;
                case "java.lang.Double": return Double.class;
                case "java.lang.Boolean": return Boolean.class;
                case "java.sql.Timestamp": return Timestamp.class;
                case "java.sql.Date": return Date.class;
                default: return Object.class;  // 未知类型默认使用 Object
            }
        } catch (Exception e) {
            LOG.warn("无法识别列类型: {}", className, e);
            return Object.class;
        }
    }

    // 手动创建 AvaticaType（替代不存在的 of 方法）
    private ColumnMetaData.AvaticaType createAvaticaType(ResultSetMetaData metaData, int columnIndex) throws SQLException {
        int sqlType = metaData.getColumnType(columnIndex);
        String typeName = metaData.getColumnTypeName(columnIndex);
        int precision = metaData.getPrecision(columnIndex);
        int scale = metaData.getScale(columnIndex);
        Class<?> javaType = getJavaType(metaData.getColumnClassName(columnIndex));

        // 根据 SQL 类型映射到 Avatica 类型
        ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(javaType);

        return new ColumnMetaData.AvaticaType(
                columnIndex,        // SQL 类型
                metaData.getColumnName(columnIndex),
                rep             // Java 表示
        );
    }

    private Frame buildFrame(ResultSet rs) throws SQLException {
        List<Object> rows = new ArrayList<>();
        while (rs.next()) {
            Object[] row = new Object[rs.getMetaData().getColumnCount()];
            for (int i = 0; i < row.length; i++) {
                row[i] = rs.getObject(i + 1);
            }
            rows.add(row);
        }
        return new Frame(0, rs.isAfterLast(), rows);
    }

    // ====================== 元数据方法（简化实现） ======================
    @Override
    public MetaResultSet getTables(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> types) {
        // 实现表元数据查询（示例返回空结果）
        return MetaResultSet.create(ch.id, 0, true, null, Frame.EMPTY);
    }

    @Override
    public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tablePattern, Pat columnPattern) {
        // 实现列元数据查询（示例返回空结果）
        return MetaResultSet.create(ch.id, 0, true, null, Frame.EMPTY);
    }

    // 其他元数据方法（如 getSchemas、getPrimaryKeys 等）需类似实现，此处省略

    // ====================== 其他必需方法 ======================
    @Override
    public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
        String stmtId = UUID.randomUUID().toString();
        return new StatementHandle(ch.id, Integer.parseInt(stmtId.substring(0, 8)), null);
    }

    @Override
    public void closeStatement(StatementHandle h) {
        // 清理语句资源
    }

    // 省略其他接口方法（如 commit、rollback、executeBatch 等）的实现

    // ====================== SQL 改写逻辑 ======================
    private String rewriteSql(String sql) throws Exception {
        // 与之前示例中的 SQL 解析、RelNode 改写逻辑一致（见之前的 applyMaskingRules 方法）
        // 此处省略重复代码，仅保留核心调用
        SqlParser parser = SqlParser.create(sql, frameworkConfig.getParserConfig());
        SqlNode sqlNode = parser.parseQuery();
        if (!(sqlNode instanceof SqlSelect)) return sql;

        Planner planner = Frameworks.getPlanner(frameworkConfig);
        SqlNode validatedNode = planner.validate(sqlNode);
        RelNode relNode = planner.rel(validatedNode).project();
        RelNode maskedRelNode = applyMaskingRules(relNode, "public", "users"); // 示例表名

        SqlDialect dialect = SqlDialect.create(SqlDialect.DatabaseProduct.CALCITE);
        RelToSqlConverter converter = new RelToSqlConverter(dialect);
        SqlNode convertedNode = converter.visitRoot(maskedRelNode).asStatement();
        return convertedNode.toSqlString(dialect).getSql();
    }

    // 连接状态管理类
    private static class ConnectionState {
        final String id;
        final Properties info;
        final ConnectionProperties props;

        ConnectionState(String id, Properties info, ConnectionProperties props) {
            this.id = id;
            this.info = info;
            this.props = props;
        }
    }

    // 改写 SQL 以应用脱敏规则
    private SqlNode rewriteSqlForMasking(SqlNode sqlNode) throws Exception {
        if (!(sqlNode instanceof SqlSelect)) {
            return sqlNode; // 只处理 SELECT 语句
        }

        SqlSelect select = (SqlSelect) sqlNode;
        SqlNode from = select.getFrom();

        // 提取表名
        String schemaName = "public"; // 默认 schema
        String tableName = null;

//        if (from instanceof SqlBasicCall) {
//            SqlBasicCall call = (SqlBasicCall) from;
//            if (call.getOperator() instanceof SqlStdOperatorTable.AS) {
//                // 处理带别名的表
//                SqlNode operand = call.operand(0);
//                if (operand instanceof SqlIdentifier) {
//                    SqlIdentifier identifier = (SqlIdentifier) operand;
//                    if (identifier.names.size() == 2) {
//                        schemaName = identifier.names.get(0);
//                        tableName = identifier.names.get(1);
//                    } else if (identifier.names.size() == 1) {
//                        tableName = identifier.names.get(0);
//                    }
//                }
//            } else if (from instanceof SqlIdentifier) {
//                // 处理不带别名的表
//                SqlIdentifier identifier = (SqlIdentifier) from;
//                if (identifier.names.size() == 2) {
//                    schemaName = identifier.names.get(0);
//                    tableName = identifier.names.get(1);
//                } else if (identifier.names.size() == 1) {
//                    tableName = identifier.names.get(0);
//                }
//            }
//        }

        if (tableName == null) {
            return sqlNode; // 无法识别表名，不进行脱敏
        }

        // 创建查询计划器
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(connection.getRootSchema())
                .parserConfig(parserConfig)
                .build();
        Planner planner = Frameworks.getPlanner(config);

        // 转换 SQL 为关系表达式
        SqlNode validatedNode = planner.validate(sqlNode);
        RelNode relNode = planner.rel(validatedNode).project();

        // 应用脱敏规则到关系表达式
        RelNode maskedRelNode = applyMaskingRules(relNode, schemaName, tableName);

        // 将修改后的关系表达式转回 SQL
        SqlDialect dialect = new SqlDialect(EMPTY_CONTEXT.withDatabaseProduct(SqlDialect.DatabaseProduct.CALCITE));
        RelToSqlConverter converter = new RelToSqlConverter(dialect);

        return converter.visitRoot(maskedRelNode).asStatement();
    }

    // 应用脱敏规则到关系表达式
    private RelNode applyMaskingRules(RelNode relNode, String schemaName, String tableName) {
        RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();
        MaskingRelShuttle shuttle = new MaskingRelShuttle(rexBuilder, configManager, schemaName, tableName);

        // 遍历关系表达式中的每个 RexNode，应用脱敏规则
        return relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(RelNode other) {
                // 处理投影操作
                if (other instanceof Project) {
                    Project project = (Project) other;
                    List<RexNode> exps = new ArrayList<>();

                    for (RexNode exp : project.getProjects()) {
                        exps.add(exp.accept(shuttle));
                    }

                    return project.copy(
                            project.getTraitSet(),
                            project.getInput(),
                            exps,
                            project.getRowType()
                    );
                }

                return super.visit(other);
            }
        });
    }
}
