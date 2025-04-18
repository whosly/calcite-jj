package com.whosly.calcite.schema.memory;

import com.whosly.calcite.schema.memory.table.Employee;

/**
 * 基于内存的Database 定义
 */
public class HrSchema {
    public final Employee[] emps = {
            new Employee(100, 10, "Bill", 10000, 1000),
            new Employee(200, 20, "Eric", 8000, 500),
            new Employee(150, 10, "Sebastian", 7000, null),
            new Employee(110, 10, "Theodore", 11500, 250),
    };

    public final Employee[] emps_ref = {
            new Employee(100, 10, "Bill", 10000, 1000),
            new Employee(200, 20, "Eric", 8000, 500),
    };

    @Override
    public String toString() {
        return "HrSchema";
    }
}
