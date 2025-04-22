package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

/**
 * 新语法文件
 *
 * JACKY JOB query
 */
public class SqlJackyJob extends SqlNode {
    private String jackyString;
    private SqlParserPos pos;

    public SqlJackyJob(SqlParserPos pos, String jackyString){
        super(pos);
        this.pos = pos;
        this.jackyString = jackyString;
    }

    public String getJackyString(){
        System.out.println("getJackyString");
        return this.jackyString;
    }

    @Override
    public SqlNode clone(SqlParserPos sqlParserPos) {
        System.out.println("clone");
        return null;
    }

    @Override
    public void unparse(SqlWriter sqlWriter, int i, int i1) {
        sqlWriter.keyword("jacky");
        sqlWriter.keyword("job");
        sqlWriter.print("\n");
        sqlWriter.keyword("" + jackyString + "");
    }

    @Override
    public void validate(SqlValidator sqlValidator, SqlValidatorScope sqlValidatorScope) {
        System.out.println("validate");
    }

    @Override
    public <R> R accept(SqlVisitor<R> sqlVisitor) {
        System.out.println("accept");
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode sqlNode, Litmus litmus) {
        System.out.println("equalsDeep");
        return false;
    }
}