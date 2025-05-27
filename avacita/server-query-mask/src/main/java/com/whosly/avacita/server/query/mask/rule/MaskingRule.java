package com.whosly.avacita.server.query.mask.rule;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class MaskingRule {
    public final String columnId; // schema.table.column
    public final String ruleType; // full, partial, regex, hash, etc.
    public final String params;   // 规则参数

    public MaskingRule(String columnId, String ruleType, String params) {
        this.columnId = columnId;
        this.ruleType = ruleType;
        this.params = params;
    }

    // 应用脱敏规则到 SQL 表达式
    public RexNode apply(RexBuilder rexBuilder, RexNode originalExpr, RelDataType fieldType) {
        switch (ruleType.toLowerCase()) {
            case "full":
                // 完全脱敏 - 替换为固定值
                return createMaskValue(rexBuilder, fieldType);

            case "partial":
                // 部分脱敏 - 保留首尾，中间用*替换
                return createPartialMask(rexBuilder, originalExpr, fieldType);

            case "regex":
                // 正则表达式脱敏
                return createRegexMask(rexBuilder, originalExpr, fieldType, params);

            case "hash":
                // 哈希脱敏
                return createHashMask(rexBuilder, originalExpr, fieldType);

            default:
                // 默认不脱敏
                return originalExpr;
        }
    }

    // 创建完全脱敏值
    private RexNode createMaskValue(RexBuilder rexBuilder, RelDataType fieldType) {
        SqlTypeName typeName = fieldType.getSqlTypeName();
        switch (typeName) {
            case VARCHAR:
            case CHAR:
                return rexBuilder.makeLiteral("******");
            case INTEGER:
            case BIGINT:
                return rexBuilder.makeLiteral(0);
            case DECIMAL:
                return rexBuilder.makeLiteral(0.0);
            case BOOLEAN:
                return rexBuilder.makeLiteral(false);
            default:
                return rexBuilder.makeNullLiteral(fieldType);
        }
    }

    // 创建部分脱敏表达式
    private RexNode createPartialMask(RexBuilder rexBuilder, RexNode originalExpr, RelDataType fieldType) {
        if (!fieldType.getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
            return originalExpr;
        }

        // 保留首尾各1个字符，中间用*替换
        RexCall substrStart = (RexCall) rexBuilder.makeCall(
                SqlStdOperatorTable.SUBSTRING,
                originalExpr,
                rexBuilder.makeLiteral(1),
                rexBuilder.makeLiteral(1)
        );

        RexCall lengthCall = (RexCall) rexBuilder.makeCall(
                SqlStdOperatorTable.LENGTH,
                originalExpr
        );

        RexNode lengthMinusOne = rexBuilder.makeCall(
                SqlStdOperatorTable.MINUS,
                lengthCall,
                rexBuilder.makeLiteral(1)
        );

        RexCall substrEnd = (RexCall) rexBuilder.makeCall(
                SqlStdOperatorTable.SUBSTRING,
                originalExpr,
                lengthMinusOne,
                rexBuilder.makeLiteral(1)
        );

        RexNode starLiteral = rexBuilder.makeLiteral("******");

        return rexBuilder.makeCall(
                SqlStdOperatorTable.CONCAT,
                substrStart,
                starLiteral,
                substrEnd
        );
    }

    // 创建正则表达式脱敏
    private RexNode createRegexMask(RexBuilder rexBuilder, RexNode originalExpr,
                                    RelDataType fieldType, String regex) {
        if (!fieldType.getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
            return originalExpr;
        }

        // 使用正则表达式替换敏感部分
        RexNode regexLiteral = rexBuilder.makeLiteral(regex);
        RexNode replaceLiteral = rexBuilder.makeLiteral("*");

        return rexBuilder.makeCall(
                SqlStdOperatorTable.REGEXP_REPLACE,
                originalExpr,
                regexLiteral,
                replaceLiteral
        );
    }

    // 创建哈希脱敏
    private RexNode createHashMask(RexBuilder rexBuilder, RexNode originalExpr, RelDataType fieldType) {
        if (!fieldType.getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
            return originalExpr;
        }

        // 使用 MD5 哈希脱敏
        return rexBuilder.makeCall(
                SqlStdOperatorTable.MD5,
                originalExpr
        );
    }
}
