package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

/**
 * 新语法文件
 *
 * CREATE MATERIALIZED VIEW
 * [ IF NOT EXISTS ] view_name
 * AS query
 */
public class CreateMaterializedView
        extends SqlCall
{
    public static final SqlSpecialOperator CREATE_MATERIALIZED_VIEW = new SqlSpecialOperator("CREATE_MATERIALIZED_VIEW", SqlKind.OTHER_DDL);
    SqlIdentifier viewName;
    boolean existenceCheck;
    SqlSelect query;

    public CreateMaterializedView(SqlParserPos pos, SqlIdentifier viewName, boolean existenceCheck, SqlSelect query)
    {
        super(pos);
        this.viewName = viewName;
        this.existenceCheck = existenceCheck;
        this.query = query;
    }

    /**
     * 返回当前 SqlNode 的操作符类型
     */
    @Override
    public SqlOperator getOperator()
    {
        return CREATE_MATERIALIZED_VIEW;
    }

    /**
     * 返回操作符列表，这里返回物化视图的名字和 AS 后面的语句，用于自定义 DDL 的校验。
     */
    @Override
    public List<SqlNode> getOperandList()
    {
        List<SqlNode> operands = new ArrayList<>();
        operands.add(viewName);
        operands.add(SqlLiteral.createBoolean(existenceCheck, SqlParserPos.ZERO));
        operands.add(query);
        return operands;
    }

    /**
     * SqlNode 的解析器，负责将 SqlNode 转换为 Sql。
     *
     * @param writer
     * @param leftPrec
     * @param rightPrec
     */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
    {
        writer.keyword("CREATE MATERIALIZED VIEW");
        if (existenceCheck) {
            writer.keyword("IF NOT EXISTS");
        }
        viewName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        query.unparse(writer, leftPrec, rightPrec);
    }
}
