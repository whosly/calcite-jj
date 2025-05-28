package com.whosly.avacita.server.query.mask.rule;

import com.whosly.avacita.server.query.mask.mysql.MaskingConfigMeta;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class MaskingRelShuttle extends RexShuttle {

    private final RexBuilder rexBuilder;
    private final MaskingConfigMeta configManager;
    private final String schemaName;
    private final String tableName;
    private final RelNode relNode; // 新增：保存当前处理的RelNode

    // 自定义操作符
    private static final SqlFunction SUBSTRING = createSubstringFunction();
    private static final SqlFunction CONCAT = createConcatFunction();
    private static final SqlFunction LENGTH = createLengthFunction();
    private static final SqlFunction HASH = createHashFunction();
    private static final SqlFunction REGEXP_REPLACE = createRegexpReplaceFunction();
    private static final SqlOperator AS = SqlStdOperatorTable.AS; // AS操作符位置

    public MaskingRelShuttle(RexBuilder rexBuilder, MaskingConfigMeta configManager,
                             String schemaName, String tableName, RelNode relNode) {
        this.rexBuilder = rexBuilder;
        this.configManager = configManager;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.relNode = relNode; // 新增：传入当前RelNode
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        RelDataType fieldType = inputRef.getType();
        String columnName = getColumnName(inputRef);
        MaskingRule rule = configManager.getRule(schemaName, tableName, columnName);
        if (rule != null) {
            return applyMaskingRule(rule, inputRef, fieldType);
        }
        return super.visitInputRef(inputRef);
    }

    // 修改：使用RelNode获取字段名
    private String getColumnName(RexInputRef inputRef) {
        if (relNode instanceof Project) {
            Project project = (Project) relNode;
            List<RexNode> projects = project.getProjects();
            int index = inputRef.getIndex();
            if (index < projects.size()) {
                RexNode projectNode = projects.get(index);
                if (projectNode instanceof RexCall) {
                    RexCall call = (RexCall) projectNode;
                    if (call.getOperator() == AS) {
                        // 获取AS操作符的第二个操作数（别名）
                        RexNode operand = call.getOperands().get(1);
                        if (operand instanceof RexLiteral) {
                            return ((RexLiteral) operand).getValue2().toString();
                        }
                    }
                }
            }
        }
        // 如果无法获取别名，使用索引位置作为后备
        return "COLUMN_" + inputRef.getIndex();
    }

    private RexNode applyMaskingRule(MaskingRule rule, RexNode originalExpr, RelDataType fieldType) {
        switch (rule.getRuleType().name().toUpperCase()) {
            case "MASK_FULL":
                return createMaskValue(rexBuilder, fieldType);
            case "PARTIAL":
                return createPartialMask(rexBuilder, originalExpr, fieldType);
            case "REGEX":
                return createRegexMask(rexBuilder, originalExpr, fieldType, rule.getRuleParams());
            case "HASH":
                return createHashMask(rexBuilder, originalExpr, fieldType);
            default:
                return originalExpr;
        }
    }

    private RexNode createMaskValue(RexBuilder rexBuilder, RelDataType fieldType) {
        SqlTypeName typeName = fieldType.getSqlTypeName();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        switch (typeName) {
            case VARCHAR:
            case CHAR:
                return rexBuilder.makeLiteral("******", typeFactory.createSqlType(SqlTypeName.VARCHAR));
            case INTEGER:
                return rexBuilder.makeLiteral(0, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
            case BIGINT:
                return rexBuilder.makeLiteral(0L, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
            case DECIMAL:
                return rexBuilder.makeLiteral(0.0, typeFactory.createSqlType(SqlTypeName.DECIMAL), false);
            case BOOLEAN:
                return rexBuilder.makeLiteral(false, typeFactory.createSqlType(SqlTypeName.BOOLEAN));
            default:
                return rexBuilder.makeNullLiteral(fieldType);
        }
    }

    private RexNode createPartialMask(RexBuilder rexBuilder, RexNode originalExpr, RelDataType fieldType) {
        if (!SqlTypeName.VARCHAR.equals(fieldType.getSqlTypeName())) {
            return originalExpr;
        }

        // 保留首尾各1个字符，中间用*替换
        RexNode oneLiteral = rexBuilder.makeLiteral(1,
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER), false);

        RexCall substrStart = (RexCall) rexBuilder.makeCall(
                SUBSTRING,
                originalExpr,
                oneLiteral,
                oneLiteral
        );

        RexCall lengthCall = (RexCall) rexBuilder.makeCall(
                LENGTH,
                originalExpr
        );

        RexNode lengthMinusOne = rexBuilder.makeCall(
                SqlStdOperatorTable.MINUS,
                lengthCall,
                oneLiteral
        );

        RexCall substrEnd = (RexCall) rexBuilder.makeCall(
                SUBSTRING,
                originalExpr,
                lengthMinusOne,
                oneLiteral
        );

        RexNode starLiteral = rexBuilder.makeLiteral("******",
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR));

        return rexBuilder.makeCall(
                CONCAT,
                substrStart,
                starLiteral,
                substrEnd
        );
    }

    private RexNode createRegexMask(RexBuilder rexBuilder, RexNode originalExpr,
                                    RelDataType fieldType, String[] regexParams) {
        if (!SqlTypeName.VARCHAR.equals(fieldType.getSqlTypeName())) {
            return originalExpr;
        }

        if (regexParams.length > 0) {
            RexNode regexLiteral = rexBuilder.makeLiteral(
                    regexParams[0],
                    rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR)
            );
            RexNode replaceLiteral = rexBuilder.makeLiteral(
                    "*",
                    rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR)
            );

            return rexBuilder.makeCall(
                    REGEXP_REPLACE,
                    originalExpr,
                    regexLiteral,
                    replaceLiteral
            );
        }
        return originalExpr;
    }

    private RexNode createHashMask(RexBuilder rexBuilder, RexNode originalExpr, RelDataType fieldType) {
        if (!SqlTypeName.VARCHAR.equals(fieldType.getSqlTypeName())) {
            return originalExpr;
        }

        return rexBuilder.makeCall(
                HASH,
                originalExpr
        );
    }

    // 创建自定义SUBSTRING函数
    private static SqlFunction createSubstringFunction() {
        return new SqlFunction("SUBSTRING",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.VARCHAR_2000,
                null,
                OperandTypes.VARIADIC,
                SqlFunctionCategory.STRING);
    }

    // 创建自定义CONCAT函数
    private static SqlFunction createConcatFunction() {
        return new SqlFunction("CONCAT",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.VARCHAR_2000,
                null,
                OperandTypes.VARIADIC,
                SqlFunctionCategory.STRING);
    }

    // 创建自定义LENGTH函数
    private static SqlFunction createLengthFunction() {
        return new SqlFunction("LENGTH",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.INTEGER,
                null,
                OperandTypes.STRING,
                SqlFunctionCategory.STRING);
    }

    // 创建自定义HASH函数
    private static SqlFunction createHashFunction() {
        return new SqlFunction("HASH",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.VARCHAR_2000,
                null,
                OperandTypes.STRING,
                SqlFunctionCategory.STRING);
    }

    // 创建自定义REGEXP_REPLACE函数
    private static SqlFunction createRegexpReplaceFunction() {
        return new SqlFunction("REGEXP_REPLACE",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.VARCHAR_2000,
                null,
                OperandTypes.STRING,
                SqlFunctionCategory.STRING);
    }
}