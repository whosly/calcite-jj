# calcite-schema
多种数据源加载的示例

关于自定义语法的实现，参考《new-grammar》章节。

关于多种数据源加载的实现，见源代码 [schema](src/main/java/com/whosly/calcite/schema/ISchemaLoader.java)

# com.whosly.calcite.schema

| 类                                                                       | 作用             |   |
|-------------------------------------------------------------------------|----------------|---|
| [Schemas](src/main/java/com/whosly/calcite/schema/Schemas.java)         | 建立数据库连接的辅助类    |   |
| [SchemasTest](src/test/java/com/whosly/calcite/schema/SchemasTest.java) | 各种数据库类型的综合测试用例 |   |

## com.whosly.calcite.schema.csv
csv 文件作为数据源。 SchemaLoader为 [CsvSchemaLoader](src/main/java/com/whosly/calcite/schema/csv/CsvSchemaLoader.java)

在测试用例的resource 下存在两个文件夹，文件夹下存在csv文件。分别以这两个文件夹作为db name， 各自文件夹下的csv文件作为表。

```
-- bugfix
    + DATE.csv
    + DATE_ORD.csv
    + LONG_EMPS.csv
    + WACKY_COLUMN_NAMES.csv
-- csv
    + depts.csv
    + SDEPTS.csv
```

测试用例: [CsvSchemaTest](src/test/java/com/whosly/calcite/schema/csv/CsvSchemaTest.java) . 

在进行查询时，SQL可以如下：
* select * from csv.depts where name like '%l%'
* select * from bugfix.date_ord
* select * from bugfix.long_emps

## com.whosly.calcite.schema.elasticsearch
elasticsearch 文件作为数据源。 SchemaLoader为 [ESSchemaLoader](src/main/java/com/whosly/calcite/schema/elasticsearch/ESSchemaLoader.java)

## com.whosly.calcite.schema.memory
基于内存的数据定义。 SchemaLoader为 [MemorySchemaLoader](src/main/java/com/whosly/calcite/schema/memory/MemorySchemaLoader.java)

| 类                                                                                                    | 作用                       |   |
|------------------------------------------------------------------------------------------------------|--------------------------|---|
| [HrSchema](src/main/java/com/whosly/calcite/schema/memory/HrSchema.java)                             | 内存数据库的Schema数据结构定义和初始数据  |   |
| [MemorySchemaLoader](src/main/java/com/whosly/calcite/schema/memory/MemorySchemaLoader.java)         | 内存数据库的初始化加载              |   |
| [MemoryObjectSchemaTest](src/test/java/com/whosly/calcite/schema/memory/MemoryObjectSchemaTest.java) | 通过SQL访问该内存数据的测试用例        |   |
| [MemorySchemaLoaderTest](src/test/java/com/whosly/calcite/schema/memory/MemorySchemaLoaderTest.java) | MemorySchemaLoader 的测试用例 |   |

## com.whosly.calcite.schema.mysql
mysql 数据库作为数据源。 SchemaLoader为 [MysqlSchemaLoader](src/main/java/com/whosly/calcite/schema/mysql/MysqlSchemaLoader.java)

测试用例 [SchemasTest](src/test/java/com/whosly/calcite/schema/SchemasTest.java#testMysqlLoadSchema)


# 查找冲突依赖

## 查找冲突依赖
```shell
mvn dependency:tree -Dverbose
```

## 分析某个组件的依赖
如 分析 Spring Boot 的依赖
```shell
mvn dependency:tree -Dincludes=org.springframework*
```

## 检查测试依赖
```shell
mvn dependency:tree -Dscope=test
```

## 检查未使用的依赖或缺失的依赖
```shell
mvn dependency:analyze
```

## 如何解决依赖冲突？
在 pom.xml 中通过 <exclusions> 排除冲突的传递性依赖：

```
<dependency>
    <groupId>com.example</groupId>
    <artifactId>example-lib</artifactId>
    <version>1.0</version>
    <exclusions>
        <exclusion>
            <groupId>conflict-group</groupId>
            <artifactId>conflict-artifact</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```
