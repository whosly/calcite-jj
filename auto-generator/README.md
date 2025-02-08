# auto-generator
如果不需要对Parser.jj进行定制化修改，那么可以通过连续运行两个插件， 根据 parser-jj 模板生成 Parser Java代码。

功能包含 parser-jj-generator + parser-generator。

```
mvn clean initialize

mvn package
```
