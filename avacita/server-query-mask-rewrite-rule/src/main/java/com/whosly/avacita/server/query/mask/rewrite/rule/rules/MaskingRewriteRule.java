package com.whosly.avacita.server.query.mask.rewrite.rule.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Project节点脱敏重写规则，支持复杂表达式、Join、别名
 */
public class MaskingRewriteRule extends RelOptRule {
    private static final Logger LOG = LoggerFactory.getLogger(MaskingRewriteRule.class);

    private final MaskingConfigMeta maskingConfigMeta;

    public MaskingRewriteRule(RelOptRuleOperand operand, MaskingConfigMeta maskingConfigMeta) {
        super(operand);
        this.maskingConfigMeta = maskingConfigMeta;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        RelNode input = project.getInput();
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();

        // 追溯底层 TableScan，获取表名、schema（支持Join）
        TableScanInfo tableScanInfo = findTableScanInfo(input, project.getInput().getRowType().getFieldList());
        if (tableScanInfo == null) {
            LOG.debug("未找到底层 TableScan，跳过脱敏重写。");
            return;
        }
        List<String> schemas = tableScanInfo.schemas;
        List<String> tables = tableScanInfo.tables;
        List<RelDataTypeField> inputFields = input.getRowType().getFieldList();

        List<RexNode> newProjects = new ArrayList<>();
        boolean changed = false;

        for (int i = 0; i < project.getProjects().size(); i++) {
            RexNode expr = project.getProjects().get(i);
            RexNode maskedExpr = maskExprRecursive(expr, inputFields, rexBuilder, schemas, tables);
            if (maskedExpr != expr) changed = true;
            newProjects.add(maskedExpr);
        }

        if (changed) {
            LogicalProject newProject = (LogicalProject) project.copy(
                    project.getTraitSet(), input, newProjects, project.getRowType()
            );
            call.transformTo(newProject);
            LOG.info("已对表 {} 应用递归脱敏重写。", tables);
        }
    }

    /**
     * 递归脱敏表达式，支持复杂表达式、Join、别名
     */
    private RexNode maskExprRecursive(RexNode expr, List<RelDataTypeField> inputFields, RexBuilder rexBuilder, List<String> schemas, List<String> tables) {
        if (expr instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) expr;
            RelDataTypeField field = inputFields.get(inputRef.getIndex());
            String column = field.getName();
            // 多表时，尝试所有表
            for (int i = 0; i < tables.size(); i++) {
                String schema = schemas.get(i);
                String table = tables.get(i);
                MaskingRuleConfig rule = maskingConfigMeta.getRule(schema, table, column);
                if (rule != null && rule.getRuleType() != MaskingRuleType.KEEP) {
                    return rule.apply(rexBuilder, inputRef, field.getType());
                }
            }
            return expr;
        } else if (expr instanceof RexCall) {
            RexCall call = (RexCall) expr;
            List<RexNode> newOperands = new ArrayList<>();
            boolean changed = false;
            for (RexNode operand : call.getOperands()) {
                RexNode masked = maskExprRecursive(operand, inputFields, rexBuilder, schemas, tables);
                if (masked != operand) changed = true;
                newOperands.add(masked);
            }
            if (changed) {
                return rexBuilder.makeCall(call.getType(), call.getOperator(), newOperands);
            }
            return expr;
        }
        // 其他类型（如常量）直接返回
        return expr;
    }

    /**
     * 递归查找底层 TableScan 节点，支持Join，返回所有表名/schema
     */
    private TableScanInfo findTableScanInfo(RelNode node, List<RelDataTypeField> inputFields) {
        List<String> schemas = new ArrayList<>();
        List<String> tables = new ArrayList<>();
        boolean found = false;
        if (node instanceof TableScan) {
            List<String> qualifiedName = ((TableScan) node).getTable().getQualifiedName();
            String schema = qualifiedName.size() > 1 ? qualifiedName.get(qualifiedName.size() - 2) : "";
            String table = qualifiedName.get(qualifiedName.size() - 1);
            schemas.add(schema);
            tables.add(table);
            found = true;
        } else if (node instanceof Join) {
            Join join = (Join) node;
            TableScanInfo left = findTableScanInfo(join.getLeft(), join.getLeft().getRowType().getFieldList());
            TableScanInfo right = findTableScanInfo(join.getRight(), join.getRight().getRowType().getFieldList());
            if (left != null) {
                schemas.addAll(left.schemas);
                tables.addAll(left.tables);
                found = true;
            }
            if (right != null) {
                schemas.addAll(right.schemas);
                tables.addAll(right.tables);
                found = true;
            }
        } else {
            for (RelNode input : node.getInputs()) {
                TableScanInfo info = findTableScanInfo(input, input.getRowType().getFieldList());
                if (info != null) {
                    schemas.addAll(info.schemas);
                    tables.addAll(info.tables);
                    found = true;
                }
            }
        }
        if (found) {
            return new TableScanInfo(schemas, tables);
        }
        return null;
    }

    /**
     * 多表信息
     */
    private static class TableScanInfo {
        List<String> schemas;
        List<String> tables;
        TableScanInfo(List<String> schemas, List<String> tables) {
            this.schemas = schemas;
            this.tables = tables;
        }
    }
}