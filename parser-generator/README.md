# parser-generator
将 parser-jj-generator 模块中生成的 Parser.jj 代码文件生成 Parser Java代码 (路径 target\generated-sources\fmpp\javacc)
copy至此项目中。

```
mvn clean initialize

mvn package
```

此时就可以直接进行编码开发工作了。
```java
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.JackySqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

public class com.news.grammar.Application {
    public static void main(String[] args) {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);

        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                        .setParserFactory(JackySqlParserImpl.FACTORY)
                        .setCaseSensitive(false)
                        .setQuoting(Quoting.BACK_TICK)
                        .setQuotedCasing(Casing.TO_UPPER)
                        .setUnquotedCasing(Casing.TO_UPPER)
                        .setConformance(SqlConformanceEnum.ORACLE_12)
                        .build())
                .build();

        String sql = "select ids, name from test where id < 5";
        SqlParser parser = SqlParser.create(sql, config.getParserConfig());
        try {
            SqlNode sqlNode = parser.parseStmt();

            System.out.println(sqlNode.toString());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

## FAQ JackySqlParserImpl.java:16000:: 可能尚未初始化变量startNum
修改代码。
