package org.apache.calcite.sql;

import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

@Getter
public class SqlSubmit extends SqlNode {

    String jobString;

    public SqlSubmit(SqlParserPos pos, String jobString) {
        super(pos);
        this.jobString = jobString;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        //todo
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("submit job as ");
        writer.keyword("" + jobString + "");
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        //todo
    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        //todo
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        //todo
        return false;
    }
}
