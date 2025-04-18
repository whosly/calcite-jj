# SQL 改写功能: 将表中的name字段，在查询的时候进行脱敏

## 目标
1. 使用 calcite 1.35.0 实现， 语言 Java
2. 将表中的name字段，在查询的时候进行脱敏, 输出为掩码格式
3. 涵盖范围为单表查询的 select name 和 select *
4. 假设表名为 t1, 表结构为id, name, age
5. 改写方式使用rule 的方式
6. 需要提供对应的测试用例

## 实现

Rule: MaskNameRule 