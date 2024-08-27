package com.whosly.calcite.schema.csv;

import com.whosly.calcite.schema.ISchemaLoader;
import org.apache.calcite.adapter.csv.CsvSchema;
import org.apache.calcite.adapter.csv.CsvTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.util.Sources;

import java.io.File;
import java.util.Objects;

public class CsvSchemaLoader implements ISchemaLoader {

    public static Schema loadSchema(String pathFolder) {
        // 0.获取csv文件的路径，注意获取到文件所在上层路径就可以了
        String path = resourcePath(pathFolder);

        // 1.构建CsvSchema对象，在Calcite中，不同数据源对应不同Schema，比如CsvSchema、DruidSchema、ElasticsearchSchema等
        CsvSchema csvSchema = new CsvSchema(new File(path), CsvTable.Flavor.SCANNABLE);

        System.out.println("find [" + pathFolder + "] Schema, contains table:" + csvSchema.getTableNames());

//        final Schema schema =
//                CsvSchemaFactory.INSTANCE
//                        .create(calciteConnection.getRootSchema(), null,
//                                ImmutableMap.of("directory",
//                                        resourcePath("csv"), "flavor", "scannable"));

        return csvSchema;
    }

    public static String resourcePath(String path) {
        // String path = Objects.requireNonNull(CsvSchemaLoader.class.getClassLoader().getResource("csv").getPath());
        return Sources.of(Objects.requireNonNull(CsvSchemaLoader.class.getResource("/" + path))).file().getAbsolutePath();
    }
}
