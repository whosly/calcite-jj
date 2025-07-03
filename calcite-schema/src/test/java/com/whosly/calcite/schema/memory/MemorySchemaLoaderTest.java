package com.whosly.calcite.schema.memory;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class MemorySchemaLoaderTest {

    @Test
    public void testLoadSchema() {
        Schema schema = MemorySchemaLoader.loadReflectiveSchema();
        Assert.assertTrue(schema instanceof ReflectiveSchema);

        ReflectiveSchema cs = (ReflectiveSchema) schema;
        Set<String> tableNames =  cs.getTableNames();
        Assert.assertEquals(2, tableNames.size());
        Assert.assertEquals(tableNames.stream().toList().get(0), "emps");
    }

}
