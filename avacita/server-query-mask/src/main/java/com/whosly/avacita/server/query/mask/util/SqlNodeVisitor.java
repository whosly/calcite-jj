package com.whosly.avacita.server.query.mask.util;

import com.whosly.avacita.server.query.mask.mysql.MaskingConfigMeta;
import com.whosly.avacita.server.query.mask.rule.MaskingRelShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.SqlDialect.EMPTY_CONTEXT;

public class SqlNodeVisitor {

    private SqlNode rewriteSqlForMasking(SqlNode sqlNode, MaskingConfigMeta maskingConfigMeta) throws Exception {
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
        FrameworkConfig config = null;
//        FrameworkConfig config = Frameworks.newConfigBuilder()
//                .defaultSchema(connection.getRootSchema())
//                .parserConfig(parserConfig)
//                .build();
        Planner planner = Frameworks.getPlanner(config);

        // 转换 SQL 为关系表达式
        SqlNode validatedNode = planner.validate(sqlNode);
        RelNode relNode = planner.rel(validatedNode).project();

        // 应用脱敏规则到关系表达式
        RelNode maskedRelNode = applyMaskingRules(relNode, schemaName, tableName, maskingConfigMeta);

        // 将修改后的关系表达式转回 SQL
        SqlDialect dialect = new SqlDialect(EMPTY_CONTEXT.withDatabaseProduct(SqlDialect.DatabaseProduct.CALCITE));
        RelToSqlConverter converter = new RelToSqlConverter(dialect);

        return converter.visitRoot(maskedRelNode).asStatement();
    }

    // 应用脱敏规则到关系表达式
    private RelNode applyMaskingRules(RelNode relNode, String schemaName, String tableName, MaskingConfigMeta maskingConfigMeta) {
        RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();
        MaskingRelShuttle shuttle = new MaskingRelShuttle(rexBuilder, maskingConfigMeta, schemaName, tableName, relNode);

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
