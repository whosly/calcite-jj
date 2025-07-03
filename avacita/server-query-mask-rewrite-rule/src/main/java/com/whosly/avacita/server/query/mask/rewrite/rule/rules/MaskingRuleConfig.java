package com.whosly.avacita.server.query.mask.rewrite.rule.rules;

import lombok.ToString;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

@ToString
public class MaskingRuleConfig {
    private static final Logger LOG = LoggerFactory.getLogger(MaskingRuleConfig.class);

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

    // 应用脱敏规则到 SQL 表达式
    public RexNode apply(RexBuilder rexBuilder, RexNode originalExpr, RelDataType fieldType) {
        if (ruleType == null || ruleType == MaskingRuleType.KEEP) {
            return originalExpr;
        }

        switch (ruleType) {
            case MASK_FULL:
                return createFullMask(rexBuilder, fieldType);
            case MASK_LEFT:
                return createLeftMask(rexBuilder, originalExpr);
            case MASK_RIGHT:
                return createRightMask(rexBuilder, originalExpr);
            case MASK_MIDDLE:
                return createMiddleMask(rexBuilder, originalExpr);
            case HASH:
                return createHashMask(rexBuilder, originalExpr);
            case ROUND:
                return createRoundMask(rexBuilder, originalExpr);
            case REGEX:
                return createRegexMask(rexBuilder, originalExpr);
            default:
                return originalExpr;
        }
    }

    // 创建完全脱敏值
    private RexNode createFullMask(RexBuilder rexBuilder, RelDataType fieldType) {
        SqlTypeName typeName = fieldType.getSqlTypeName();
        switch (typeName) {
            case VARCHAR:
            case CHAR:
                return rexBuilder.makeLiteral("******");
            case INTEGER:
            case BIGINT:
            case DOUBLE:
            case FLOAT:
                return rexBuilder.makeExactLiteral(BigDecimal.ZERO, fieldType);
            case DECIMAL:
                return rexBuilder.makeExactLiteral(BigDecimal.ZERO, fieldType);
            case BOOLEAN:
                return rexBuilder.makeLiteral(false);
            default:
                // For other types, return NULL
                return rexBuilder.makeNullLiteral(fieldType);
        }
    }

    // 使用SqlUnresolvedFunction来表示数据库特定的函数
    private SqlFunction createUnresolvedFunction(String name) {
        return new SqlFunction(name, SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000, null, OperandTypes.ANY, SqlFunctionCategory.STRING);
    }

    // 创建左掩码脱敏表达式
    private RexNode createLeftMask(RexBuilder rexBuilder, RexNode originalExpr) {
        int keepChars = parseParam(0, 3);
        RexNode keepCharsNode = rexBuilder.makeExactLiteral(BigDecimal.valueOf(keepChars));
        RexNode maskedSection = rexBuilder.makeLiteral("******");

        // CONCAT(LEFT(col, keepChars), '******')
        SqlFunction leftFunc = createUnresolvedFunction("LEFT");
        RexNode leftPart = rexBuilder.makeCall(leftFunc, originalExpr, keepCharsNode);

        SqlFunction concatFunc = createUnresolvedFunction("CONCAT");
        return rexBuilder.makeCall(concatFunc, leftPart, maskedSection);
    }

    // 创建右掩码脱敏表达式
    private RexNode createRightMask(RexBuilder rexBuilder, RexNode originalExpr) {
        int keepChars = parseParam(0, 3);
        RexNode keepCharsNode = rexBuilder.makeExactLiteral(BigDecimal.valueOf(keepChars));
        RexNode maskedSection = rexBuilder.makeLiteral("******");

        // CONCAT('******', RIGHT(col, keepChars))
        SqlFunction rightFunc = createUnresolvedFunction("RIGHT");
        RexNode rightPart = rexBuilder.makeCall(rightFunc, originalExpr, keepCharsNode);
        
        SqlFunction concatFunc = createUnresolvedFunction("CONCAT");
        return rexBuilder.makeCall(concatFunc, maskedSection, rightPart);
    }

    // 创建中间掩码脱敏表达式
    private RexNode createMiddleMask(RexBuilder rexBuilder, RexNode originalExpr) {
        int leftChars = parseParam(0, 3);
        int rightChars = parseParam(1, 4);
        RexNode leftCharsNode = rexBuilder.makeExactLiteral(BigDecimal.valueOf(leftChars));
        RexNode rightCharsNode = rexBuilder.makeExactLiteral(BigDecimal.valueOf(rightChars));
        RexNode maskedSection = rexBuilder.makeLiteral("******");

        // CONCAT(LEFT(col, leftChars), '******', RIGHT(col, rightChars))
        SqlFunction leftFunc = createUnresolvedFunction("LEFT");
        RexNode leftPart = rexBuilder.makeCall(leftFunc, originalExpr, leftCharsNode);

        SqlFunction rightFunc = createUnresolvedFunction("RIGHT");
        RexNode rightPart = rexBuilder.makeCall(rightFunc, originalExpr, rightCharsNode);
        
        SqlFunction concatFunc = createUnresolvedFunction("CONCAT");
        return rexBuilder.makeCall(concatFunc, leftPart, maskedSection, rightPart);
    }

    // 创建哈希脱敏
    private RexNode createHashMask(RexBuilder rexBuilder, RexNode originalExpr) {
        // MD5(col)
        SqlFunction md5Func = createUnresolvedFunction("MD5");
        return rexBuilder.makeCall(md5Func, originalExpr);
    }

    // 创建四舍五入脱敏
    private RexNode createRoundMask(RexBuilder rexBuilder, RexNode originalExpr) {
        int roundTo = parseParam(0, -2); // -2 means rounding to the nearest 100

        // ROUND(col, precision)
        RexNode roundToNode = rexBuilder.makeExactLiteral(BigDecimal.valueOf(roundTo));
        return rexBuilder.makeCall(SqlStdOperatorTable.ROUND, originalExpr, roundToNode);
    }

    // 创建正则脱敏
    private RexNode createRegexMask(RexBuilder rexBuilder, RexNode originalExpr) {
        String regex = parseParam(0, "[a-zA-Z0-9]");
        String replacement = parseParam(1, "*");
        
        RexNode regexNode = rexBuilder.makeLiteral(regex);
        RexNode replacementNode = rexBuilder.makeLiteral(replacement);
        
        // REGEXP_REPLACE(col, regex, replacement)
        SqlFunction regexpReplaceFunc = createUnresolvedFunction("REGEXP_REPLACE");
        return rexBuilder.makeCall(regexpReplaceFunc, originalExpr, regexNode, replacementNode);
    }

    private <T> T parseParam(int index, T defaultValue) {
        if (ruleParams != null && ruleParams.length > index) {
            String paramStr = ruleParams[index].trim();
            try {
                if (defaultValue instanceof Integer) {
                    return (T) Integer.valueOf(paramStr);
                } else if (defaultValue instanceof String) {
                    return (T) paramStr;
                }
            } catch (NumberFormatException e) {
                LOG.warn("无法解析参数: {}, 使用默认值: {}", paramStr, defaultValue);
            }
        }
        return defaultValue;
    }
} 