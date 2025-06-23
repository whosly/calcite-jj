package com.whosly.avacita.server.query.mask.rewrite.rule.rules;

import lombok.ToString;

@ToString
public class MaskingRuleConfig {
    private final String schema;
    private final String table;
    private final String column;
    private final MaskingRuleType ruleType;
    private final String[] ruleParams;

    public MaskingRuleConfig(String schema, String table, String column, String ruleType, String[] ruleParams) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.ruleType = MaskingRuleType.getByName(ruleType);
        this.ruleParams = ruleParams;
    }

    // Getters
    public String getSchema() { return schema; }
    public String getTable() { return table; }
    public String getColumn() { return column; }
    public MaskingRuleType getRuleType() { return ruleType; }
    public String[] getRuleParams() { return ruleParams; }

    public boolean match(String schema, String table, String column) {
        return this.schema.equalsIgnoreCase(schema)
                && this.table.equalsIgnoreCase(table)
                && this.column.equalsIgnoreCase(column);
    }

//
//    // 应用脱敏规则到 SQL 表达式
//    public RexNode apply(RexBuilder rexBuilder, RexNode originalExpr, RelDataType fieldType) {
//        switch (ruleType.toLowerCase()) {
//            case "full":
//                // 完全脱敏 - 替换为固定值
//                return createMaskValue(rexBuilder, fieldType);
//
//            case "partial":
//                // 部分脱敏 - 保留首尾，中间用*替换
//                return createPartialMask(rexBuilder, originalExpr, fieldType);
//
//            case "regex":
//                // 正则表达式脱敏
//                return createRegexMask(rexBuilder, originalExpr, fieldType, params);
//
//            case "hash":
//                // 哈希脱敏
//                return createHashMask(rexBuilder, originalExpr, fieldType);
//
//            default:
//                // 默认不脱敏
//                return originalExpr;
//        }
//    }
//
//    // 创建完全脱敏值
//    private RexNode createMaskValue(RexBuilder rexBuilder, RelDataType fieldType) {
//        SqlTypeName typeName = fieldType.getSqlTypeName();
//        switch (typeName) {
//            case VARCHAR:
//            case CHAR:
//                return rexBuilder.makeLiteral("******");
//            case INTEGER:
//            case BIGINT:
//                return rexBuilder.makeLiteral(0);
//            case DECIMAL:
//                return rexBuilder.makeLiteral(0.0);
//            case BOOLEAN:
//                return rexBuilder.makeLiteral(false);
//            default:
//                return rexBuilder.makeNullLiteral(fieldType);
//        }
//    }
//
//    // 创建部分脱敏表达式
//    private RexNode createPartialMask(RexBuilder rexBuilder, RexNode originalExpr, RelDataType fieldType) {
//        if (!fieldType.getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
//            return originalExpr;
//        }
//
//        // 保留首尾各1个字符，中间用*替换
//        RexCall substrStart = (RexCall) rexBuilder.makeCall(
//                SqlStdOperatorTable.SUBSTRING,
//                originalExpr,
//                rexBuilder.makeLiteral(1),
//                rexBuilder.makeLiteral(1)
//        );
//
//        RexCall lengthCall = (RexCall) rexBuilder.makeCall(
//                SqlStdOperatorTable.LENGTH,
//                originalExpr
//        );
//
//        RexNode lengthMinusOne = rexBuilder.makeCall(
//                SqlStdOperatorTable.MINUS,
//                lengthCall,
//                rexBuilder.makeLiteral(1)
//        );
//
//        RexCall substrEnd = (RexCall) rexBuilder.makeCall(
//                SqlStdOperatorTable.SUBSTRING,
//                originalExpr,
//                lengthMinusOne,
//                rexBuilder.makeLiteral(1)
//        );
//
//        RexNode starLiteral = rexBuilder.makeLiteral("******");
//
//        return rexBuilder.makeCall(
//                SqlStdOperatorTable.CONCAT,
//                substrStart,
//                starLiteral,
//                substrEnd
//        );
//    }
//
//    // 创建正则表达式脱敏
//    private RexNode createRegexMask(RexBuilder rexBuilder, RexNode originalExpr,
//                                    RelDataType fieldType, String regex) {
//        if (!fieldType.getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
//            return originalExpr;
//        }
//
//        // 使用正则表达式替换敏感部分
//        RexNode regexLiteral = rexBuilder.makeLiteral(regex);
//        RexNode replaceLiteral = rexBuilder.makeLiteral("*");
//
//        return rexBuilder.makeCall(
//                SqlStdOperatorTable.REGEXP_REPLACE,
//                originalExpr,
//                regexLiteral,
//                replaceLiteral
//        );
//    }
//
//    // 创建哈希脱敏
//    private RexNode createHashMask(RexBuilder rexBuilder, RexNode originalExpr, RelDataType fieldType) {
//        if (!fieldType.getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
//            return originalExpr;
//        }
//
//        // 使用 MD5 哈希脱敏
//        return rexBuilder.makeCall(
//                SqlStdOperatorTable.MD5,
//                originalExpr
//        );
//    }
}
