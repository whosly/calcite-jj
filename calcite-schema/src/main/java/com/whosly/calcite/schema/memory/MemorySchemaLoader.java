package com.whosly.calcite.schema.memory;

import com.whosly.calcite.schema.ISchemaLoader;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.Map;

public class MemorySchemaLoader implements ISchemaLoader {

    public static Map<String, Schema> loadSchemaList() {
        // 1.构建 Schema对象，在Calcite中，不同数据源对应不同Schema，比如CsvSchema、DruidSchema、ElasticsearchSchema等

        Map<String, Schema> schemaMap = new HashMap<>();
        schemaMap.put("hr", loadReflectiveSchema());
        schemaMap.put("test", loadTestSchema());

        return schemaMap;
    }

    static Schema loadReflectiveSchema() {
        // load class Schema
        ReflectiveSchema reflectiveSchema = new ReflectiveSchema(new HrSchema());
//        Schema schema = ReflectiveSchema.create(calciteConnection,  rootSchema, "hr", new HrSchema());

        System.out.println("loadReflectiveSchema find [Memory] Schema, contains table:" + reflectiveSchema.getTableNames());

        return reflectiveSchema;
    }

    static Schema loadTestSchema() {
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

        return new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                Map<String, Table> tables = new HashMap<>();
                tables.put("user", schemaOnlyTable);
                return tables;
            }
        };
    }
}
