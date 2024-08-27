package com.whosly.calcite.schema;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.util.HashMap;
import java.util.Map;

public class SchemaRegister {
    private static final Map<String, Schema> SCHEMA = new HashMap<>();

    public static final void reg(final String dbName, final Schema schema, final SchemaPlus rootSchema) {
        rootSchema.add(dbName, schema);

        SCHEMA.put(dbName, schema);
    }
}
