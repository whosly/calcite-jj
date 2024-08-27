package com.whosly.calcite.schema.csv;

import org.apache.calcite.adapter.csv.CsvSchema;
import org.apache.calcite.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class CsvSchemaLoaderTest {

    @Test
    public void testCsvLoadSchema() {
        Schema schema = CsvSchemaLoader.loadSchema("csv");
        Assert.assertTrue(schema instanceof CsvSchema);

        CsvSchema cs = (CsvSchema) schema;
        Set<String> tableNames =  cs.getTableNames();
        Assert.assertEquals(2, tableNames.size());
        Assert.assertEquals(tableNames.stream().toList().get(0), "depts");
    }

    @Test
    public void testBugLoadSchema() {
        Schema schema = CsvSchemaLoader.loadSchema("bugfix");
        Assert.assertTrue(schema instanceof CsvSchema);

        CsvSchema cs = (CsvSchema) schema;
        Set<String> tableNames =  cs.getTableNames();
        Assert.assertEquals(4, tableNames.size());
        Assert.assertEquals(tableNames.stream().toList().get(0), "DATE");
    }

    @Test(expected = NullPointerException.class)
    public void testNullLoadSchema() {
        Schema schema = CsvSchemaLoader.loadSchema("bug");
        Assert.assertTrue(false);
    }

}
