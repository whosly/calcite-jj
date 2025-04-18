package com.whosly.calcite.rule;

import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @author fengyang <deep_blue_yang@126.com>
 * @date on 2025/4/15
 */
public class MaskNameRule extends RelOptRule {
    private static final String TARGET_TABLE = "t1";
    private static final String TARGET_COLUMN = "name";

    public MaskNameRule() {
        super(
                operand(LogicalProject.class, any()), // 匹配所有Project操作
//                operand(JdbcTableScan.class, none()), // 匹配表扫描节点
                "MaskNameRule"
        );
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return super.matches(call);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        JdbcTableScan scan = call.rel(0);

        // 验证表名
        String tableName = String.join(".", scan.getTable().getQualifiedName());
        if (!tableName.endsWith("." + TARGET_TABLE)) return;

        RelBuilder builder = call.builder();
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
        RelDataType rowType = scan.getRowType();

        List<RexNode> exprs = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String fieldName = rowType.getFieldNames().get(i);
            RexInputRef inputRef = rexBuilder.makeInputRef(scan, i);

            if (fieldName.equalsIgnoreCase(TARGET_COLUMN)) {
                // 构建脱敏表达式
                RexNode maskedExpr = rexBuilder.makeCall(
                        SqlStdOperatorTable.CONCAT,
                        rexBuilder.makeCall(
                                SqlStdOperatorTable.SUBSTRING,
                                inputRef,
                                rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)),
                                rexBuilder.makeExactLiteral(BigDecimal.valueOf(3))
                        ),
                        rexBuilder.makeLiteral("******")
                );
                exprs.add(maskedExpr);
            } else {
                exprs.add(inputRef);
            }
            fieldNames.add(fieldName);
        }

        builder.push(scan);
        builder.project(exprs, fieldNames);
        call.transformTo(builder.build());

//        LogicalProject project = call.rel(0);
//
//        RelBuilder builder = RelBuilder.proto().create(project.getCluster(),
//                // project.getCluster().getPlanner().getContext().get(RelOptSchema.class)
//                null);
//        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
//
//        // 获取输入表结构
//        RelDataType inputRowType = project.getInput().getRowType();
//        int nameFieldIndex = inputRowType.getFieldNames().indexOf(TARGET_COLUMN);
//        if (nameFieldIndex == -1) return; // 表不含name字段
//
//        // 构建新的Project表达式
//        List<RexNode> newProjects = new ArrayList<>();
//        for (RexNode expr : project.getProjects()) {
//            if (isNameField(expr, inputRowType)) {
//                // 对name字段应用脱敏
//                RexNode maskedExpr = buildMaskExpression(rexBuilder, expr);
//                newProjects.add(maskedExpr);
//            } else {
//                newProjects.add(expr);
//            }
//        }
//
//        // 生成新的Project节点
//        builder.push(project.getInput());
//        builder.project(newProjects, project.getRowType().getFieldNames());
//        call.transformTo(builder.build());
    }

    // 判断是否为name字段的直接引用
    private boolean isNameField(RexNode expr, RelDataType inputRowType) {
        return expr.toString().equals("$" + inputRowType.getFieldNames().indexOf(TARGET_COLUMN));
    }

    // 构建脱敏表达式：SUBSTRING(name, 1, 3) || '******'
    private RexNode buildMaskExpression(RexBuilder rexBuilder, RexNode inputRef) {
        return rexBuilder.makeCall(
                SqlStdOperatorTable.CONCAT,
                rexBuilder.makeCall(
                        SqlStdOperatorTable.SUBSTRING,
                        inputRef,
                        rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)),
                        rexBuilder.makeExactLiteral(BigDecimal.valueOf(3))
                ),
                rexBuilder.makeLiteral("******")
        );
    }
}