# parser-jj-generator
根据 parser-jj 模板生成 parser-jj。

## 复制模板文件
从calcite源码包中，将code\src\main\codegen下所有文件复制到自己的代码路径下。

包含 config.fmpp、templates/Parser.jj（模板文件）

## 合并 config.fmpp
把 default_config.fmpp 中的parser属性与config.fmpp中的parser属性合并。

就可以通过mvn package命令生成可用的 Parser.jj（代码文件） 了。
当然，如果有定制化修改的需求，也可以在这个阶段修改config.fmpp

```
mvn clean package
```

此时会在 target\generated-sources\fmpp\javacc 下生成 Parser.jj（代码文件）。
