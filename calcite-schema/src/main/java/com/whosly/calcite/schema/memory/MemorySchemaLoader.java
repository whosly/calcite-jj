package com.whosly.calcite.schema.memory;

import com.whosly.calcite.schema.ISchemaLoader;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.schema.Schema;

public class MemorySchemaLoader implements ISchemaLoader {

    public static Schema loadSchema() {
        // 1.构建CsvSchema对象，在Calcite中，不同数据源对应不同Schema，比如CsvSchema、DruidSchema、ElasticsearchSchema等
        ReflectiveSchema reflectiveSchema = new ReflectiveSchema(new HrSchema());

        System.out.println("find [Memory] Schema, contains table:" + reflectiveSchema.getTableNames());

        return reflectiveSchema;
    }
}
