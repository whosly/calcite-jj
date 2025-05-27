package com.whosly.avacita.server.query.mask.rule;

import com.whosly.avacita.server.query.mask.mysql.MaskingManager;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

/**
 * 自定义 RelNode 处理器 - 用于修改查询计划
 */
public class MaskingRelShuttle extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final MaskingManager configManager;
    private final String schemaName;
    private final String tableName;

    public MaskingRelShuttle(RexBuilder rexBuilder, MaskingManager configManager,
                             String schemaName, String tableName) {
        this.rexBuilder = rexBuilder;
        this.configManager = configManager;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        RelDataType rowType = inputRef.getType();
        int index = inputRef.getIndex();

        // 获取字段信息
        String columnName = rowType.getFieldList().get(index).getName();
        String columnId = String.format("%s.%s.%s", schemaName, tableName, columnName);

        // 检查是否有脱敏规则
        MaskingRule rule = configManager.getRule(columnId);
        if (rule != null) {
            return rule.apply(rexBuilder, inputRef, rowType);
        }

        return super.visitInputRef(inputRef);
    }
}