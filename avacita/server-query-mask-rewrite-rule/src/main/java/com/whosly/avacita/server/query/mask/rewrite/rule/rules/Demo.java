package com.whosly.avacita.server.query.mask.rewrite.rule.rules;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.tools.*;

import java.util.HashMap;
import java.util.Map;

import static org.apache.calcite.plan.RelOptRule.any;
import static org.apache.calcite.plan.RelOptRule.operand;

public class Demo {
    private Demo() {
    }

    public static void main(String[] args) throws SqlParseException, RelConversionException, ValidationException {

        // 创建一个简单的表 schemaOnlyTable
        AbstractTable schemaOnlyTable = new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
                builder.add("id", SqlTypeName.INTEGER);
                builder.add("name", SqlTypeName.VARCHAR);
                builder.add("age", SqlTypeName.INTEGER);
                return builder.build();
            }
        };

        // 创建 rootSchema 并添加 "test" 和 "user" 表
        SchemaPlus rootSchema = CalciteSchema.createRootSchema(true).plus();
        rootSchema.add("test", new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                Map<String, Table> tables = new HashMap<>();
                tables.put("user", schemaOnlyTable);
                return tables;
            }
        });
        //创建一个简单的规则重写project映射
        RelOptRule myrule = new SimpleRewriteRule(operand(LogicalProject.class, any()), "test");

        // 创建 Planner 配置
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder().defaultSchema(rootSchema);
        FrameworkConfig build = configBuilder.build();
        Planner planner = Frameworks.getPlanner(build);

        // 解析 SQL 查询
        SqlNode parsed = planner.parse("select * from \"test\".\"user\" where \"id\" = 1");
        //sql 校验
        planner.validate(parsed);
        RelRoot rel = planner.rel(parsed);
//    planner中默认使用CBO 优化器由于我们得不到执行代价（得到执行代价需要真正的jdbcschema）手动创建使用RBO

        HepPlanner hepPlanner = new HepPlanner(HepProgram.builder()
                //添加我们的规则
                .addRuleInstance(myrule)
                .build());
        hepPlanner.setRoot(rel.rel);
        //执行优化
        RelNode bestExp = hepPlanner.findBestExp();
        //将执行计划转化为sql
        SqlString sqlString = generateSql(bestExp);
        System.out.println(sqlString);

    }


    private static SqlString generateSql(RelNode rel) {
        //这里使用标准sql 也可以换成其他的
        SqlDialect sqlDialect = AnsiSqlDialect.DEFAULT;
        final JdbcImplementor jdbcImplementor =
                new JdbcImplementor(sqlDialect,
                        (JavaTypeFactory) rel.getCluster().getTypeFactory());
        final JdbcImplementor.Result result =
                jdbcImplementor.visitRoot(rel);
        return result.asStatement().toSqlString(sqlDialect);
    }

}
