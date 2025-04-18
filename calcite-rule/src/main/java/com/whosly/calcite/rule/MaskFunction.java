package com.whosly.calcite.rule;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

/**
* @author fengyang <deep_blue_yang@126.com>
* @date on 2025/4/15
*/
public class MaskFunction extends SqlFunction {
    public MaskFunction() {
        super("MASK",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(SqlTypeName.VARCHAR),
                null,
                OperandTypes.STRING,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    // 运行时实现（示例）
    public static String eval(String input) {
        if (input == null) return null;
        if (input.length() <= 3) return "***";
        return "***" + input.substring(3);
    }
}
