package com.whosly.calcite.schema.memory.table;

public class Employee {
    public int empid;
    public int deptno;
    public String name;
    public float salary;
    public Integer commission;

    public Employee(int empid, int deptno, String name, float salary,
                    Integer commission) {
        this.empid = empid;
        this.deptno = deptno;
        this.name = name;
        this.salary = salary;
        this.commission = commission;
    }

    @Override
    public String toString() {
        return "Employee [empid: " + empid + ", deptno: " + deptno
                + ", name: " + name + "]";
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || obj instanceof Employee
                && empid == ((Employee) obj).empid;
    }
}
