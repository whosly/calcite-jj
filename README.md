# calcite jj 介绍

calcite parser代码生成逻辑

![code-generate-process](doc/calcite-parser-code-generate-process.png)

# 模块介绍
* [load-parser-jj](./load-parser-jj/)
    * 获取 Calcite 源码中的 Parser.jj 文件
* [parser-jj-generator](./parser-jj-generator/)
    * 根据 parser-jj 模板文件生成 parser-jj 代码文件
* [parser-generator](./parser-generator/)
    * 根据 parser-jj 代码文件文件生成  Parser Java代码
* [auto-generator](./auto-generator/)
    * 根据 parser-jj 模板文件生成 Parser Java代码(不需要对Parser.jj进行定制化修改)。
* [new-grammar](./new-grammar/)
    * 新增自定义语法的例子工程
        * CREATE MATERIALIZED VIEW [ IF NOT EXISTS ] view_name AS query
        * JACKY JOB 'query'
* [calcite-schema](./calcite-schema/)
  * 多种数据源加载的示例
  * 自定义语法 submit job as query 的示例

# 模块

## load-parser-jj 
使用 Maven 插件 maven-dependency-plugin 直接从 Calcite 源码包中进行拷贝。

[README.md](./load-parser-jj/README.md)


## parser-jj-generator
根据 parser-jj 模板生成 parser-jj。

[README.md](./parser-jj-generator/README.md)


## parser-generator
将 parser-jj-generator 模块中生成的 Parser.jj 代码文件生成 Parser Java代码 (路径 target\generated-sources\fmpp\javacc)
copy至此项目中。

[README.md](./parser-generator/README.md)


## auto-generator
如果不需要对Parser.jj进行定制化修改，那么可以通过连续运行两个插件， 根据 parser-jj 模板生成 Parser Java代码。

[README.md](./auto-generator/README.md)


## new-grammar
使用 FreeMarker 模版插件根据 config.fmpp 生成 parser.jj 文件，最后使用 JavaCC 编译插件生成最终的解析器代码。

[README.md](./new-grammar/README.md)


## calcite-schema
多种数据源加载的示例。

[README.md](./calcite-schema/README.md)


## avacita
基于 avacita 实现各种数据库jdbc查询的例子

[README.md](./avacita/README.md)


# 编译
```
mvn clean initialize

mvn package

mvn clean install
```