package com.whosly.avacita.server.query.mask.rewrite.rule.rules;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public class SimpleRewriteRule extends RelOptRule {

    int count =1;

    protected SimpleRewriteRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        //演示demo 为了只从写一次如果正常写要判断更多。这里为了防止递归
        count--;
        if(count <= -1) {
            return;
        }

        LogicalProject project = call.rel(0);
        RelDataType rowType = project.getRowType();
        //获取第一个输出 这里demo里面是id 正常需要判断
        RexNode inputExpr = project.getProjects().get(0);
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        //调用cast 转化为string 这里只是个例子随意调用
        RexNode stringExpr = rexBuilder.makeCast(
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),  // 转换为 VARCHAR 类型
                inputExpr
        );
        //测试嵌套一个SUBSTRING 随意调用
        RexNode substringExpr = rexBuilder.makeCall(
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),  // 返回类型是 VARCHAR
                SqlStdOperatorTable.SUBSTRING,  // substring 函数
                Lists.newArrayList(   stringExpr,
                        rexBuilder.makeLiteral(1, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)),
                        rexBuilder.makeLiteral(10, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT) )
                ));

        //构建新的project列表
        List<RexNode> newProjects = new ArrayList<>();
        newProjects.add(substringExpr);  // 使用截取后的 id 字段
        newProjects.addAll(project.getProjects().subList(1, project.getProjects().size()));  // 保留其他字段

        List<RelDataTypeField> fieldList = rowType.getFieldList();
        //理论上这里需要修改第一个的行类型 但是不修改好像也没报错。
        RelDataType newRowType = project.getCluster().getTypeFactory().createStructType(fieldList);

        // 创建并替换为新的 LogicalProject
        LogicalProject newProject = project.copy(
                project.getTraitSet(),  // 使用原始的 traitSet
                project.getInput(),  // 使用原始的输入
                newProjects,  // 使用修改后的字段表达式
                newRowType   // 使用新输出的行类型
        );

        // 在这里执行转换操作
        call.transformTo(newProject);

    }
}
