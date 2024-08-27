package com.whosly.calcite.sql.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.junit.Before;
import org.junit.Test;

public class CalciteLongParserTest {

    private FrameworkConfig config;

    @Before
    public void setUp() throws Exception {
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                        // calcite内置Parser类为SqlParserImpl， 这个类的代码全部是由JavaCC生成
                        .setParserFactory(SqlParserImpl.FACTORY)
                        // 大小是写否敏感，比如说列名、表名、函数名
                        .setCaseSensitive(false)
                        // 设置引用一个标识符，比如说MySQL中的是``, Oracle中的""
                        .setQuoting(Quoting.BACK_TICK)
                        // Quoting策略，不变，变大写或变成小写，代码中的全部设置成变大写
                        .setQuotedCasing(Casing.TO_UPPER)
                        // 当标识符没有被Quoting后的策略
                        .setUnquotedCasing(Casing.TO_UPPER)
                        // 特定语法支持，比如是否支持差集等
                        .setConformance(SqlConformanceEnum.ORACLE_12)
                        // 设置标识符的最大长度，如果你的列名、表较长可以相应的加大这个值
//                        .setIdentifierMaxLength()
                        .build())
                .build();
//        final FrameworkConfig config = SqlParser.config().withLex(Lex.MYSQL);

        this.config = config;
    }

    @Test
    public void test_parser() throws SqlParseException {
        String sql = "SELECT\n" +
                "\tsu.dept_id `deptId`,\n" +
                "\tsu.user_id,\n" +
                "\tsr.role_id,\n" +
                "\tsu.user_name,\n" +
                "\tsd.dept_name,\n" +
                "\tsr.role_name\n" +
                "FROM\n" +
                "\tsys_user AS su\n" +
                "JOIN sys_dept sd ON su.dept_id = sd.dept_id\n" +
                "JOIN sys_user_role sur ON sur.user_id = su.user_id\n" +
                "JOIN sys_role sr ON sur.role_id = sr.role_id\n" +
                "WHERE\n" +
                "\tsd.dept_name = '研发部门'\n" +
                "\tand su.user_name = 'admin'\n" +
                "\tand su.dept_id = 103\n" +
                "\tor sr.role_name = '超级管理员'\n" +
                "ORDER BY\n" +
                "\tsd.create_time DESC";

        // SqlParser：sql转换器，将sql字符串转换为sql语法树
        SqlParser parser = SqlParser.create(sql, this.config.getParserConfig());

        // SqlNode：sql语法树基础元素
        //    SqlParserPos：为当前元素在sql语法树中位置
        //    SqlKind：节点类型
        SqlNode sqlNode = parser.parseQuery();

        sqlNode.accept(new SqlBasicVisitor<String>() {
            public String visit(SqlCall call) {
                if (call.getKind().equals(SqlKind.SELECT)) {
                    SqlSelect select = (SqlSelect) call;
                    System.out.println("--------------查询列名----------------------------------------");
                    select.getSelectList().forEach(colum -> {
                        if (SqlKind.AS.equals(colum.getKind())) {
                            SqlBasicCall basicCall = (SqlBasicCall) colum;

                            System.out.println(basicCall.getOperandList().get(0).toString() +
                                    " as " + basicCall.getOperandList().get(1).toString());

                        } else if (SqlKind.IDENTIFIER.equals(colum.getKind())) {
                            System.out.println("" + colum.toString());
                        }
                    });

                    System.out.println("--------------From Table Info----------------------------------------");

                    select.getFrom().accept(new SqlBasicVisitor<String>() {
                        public String visit(SqlCall call) {
                            if (call.getKind().equals(SqlKind.JOIN)) {
                                SqlJoin join = (SqlJoin) call;
                                System.out.println("join.getRight:" + join.getRight().toString() +
                                        ",join.getCondition:" + join.getCondition().toString());
                                if (!join.getLeft().getKind().equals(SqlKind.JOIN)) {
                                    System.out.println("join.getLeft::" + join.getLeft().toString());
                                }
                            }
                            return call.getOperator().acceptCall(this, call);
                        }
                    });
                    System.out.println("--------------Where Info----------------------------------------");
                    // SqlBasicVisitor：访问器，泛型为返回值，直接返回即获得指定元素，包含多个访问器其中参数为SqlCall的可以访问完整元素所以使用此方法。
                    //      T visit(SqlCall call)：访问SqlCall元素，返回值call.getOperator().acceptCall(this, call)递归调用。
                    select.getWhere().accept(new SqlBasicVisitor<String>() {
                        // SqlCall：语句节点，用于判断语句类型
                        public String visit(SqlCall call) {
                            if (call.getKind().equals(SqlKind.AND) || call.getKind().equals(SqlKind.OR)) {
                                // SqlBasicCall：最小单位的完整sql节点例如AS,JOIN,AND等
                                SqlBasicCall sql = (SqlBasicCall) call;
                                SqlBasicCall left = (SqlBasicCall) sql.getOperandList().get(0);
                                SqlBasicCall right = (SqlBasicCall) sql.getOperandList().get(1);

                                System.out.println("kind:" + sql.getKind() + ",right:" + right);

                                if (!left.getKind().equals(SqlKind.AND) && !left.getKind().equals(SqlKind.OR)) {
                                    System.out.println("left:" + left);
                                }
                            }
                            return call.getOperator().acceptCall(this, call);
                        }
                    });
                    System.out.println("--------------增加查询条件----------------------------------------");
                    try {
                        SqlNode condition = SqlParser.create("1=1").parseExpression();
                        // 拼接查询条件
                        SqlNode where = SqlUtil.andExpressions(select.getWhere(),condition);
                        select.setWhere(where);
                    } catch (SqlParseException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println("语句:" + select);
                }
                return call.getOperator().acceptCall(this, call);
            }
        });
    }

}
