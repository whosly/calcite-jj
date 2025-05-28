package com.whosly.avacita.server.query.mask.util;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SignatureBuilder {

    public Meta.Signature buildSignature(ResultSet rs) throws SQLException {
        List<ColumnMetaData> columns = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            // 获取列的 Java 类型
            Class<?> javaType = getJavaType(metaData.getColumnClassName(i));

            boolean autoIncrement = metaData.isAutoIncrement(i);
            int nullableInt = metaData.isNullable(i) == ResultSetMetaData.columnNullable? 1 : 0;
            boolean signed = metaData.isSigned(i);
            int displaySize = metaData.getColumnDisplaySize(i);
            String label = metaData.getColumnLabel(i);
            String schemaName = metaData.getSchemaName(i);
            int precision = metaData.getPrecision(i);
            int scale = metaData.getScale(i);
            String tableName = metaData.getTableName(i);
            ColumnMetaData.AvaticaType type = createAvaticaType(metaData, i);
            boolean readOnly = metaData.isReadOnly(i);
            boolean writable = false;
            boolean definitelyWritable = false;
            String columnClassName = metaData.getColumnClassName(i);

            columns.add(new ColumnMetaData(
                    i - 1,
                    autoIncrement,
                    metaData.isCaseSensitive(i),
                    metaData.isSearchable(i),
                    metaData.isCurrency(i),
                    nullableInt,
                    signed,
                    displaySize,
                    label,
                    metaData.getColumnName(i),
                    schemaName,
                    precision,
                    scale,
                    tableName,
                    metaData.getCatalogName(i),
                    type,
                    readOnly,
                    writable,
                    definitelyWritable,
                    columnClassName
            ));
        }

        return new Meta.Signature(
                columns,
                null,
                null,
                null,
                Meta.CursorFactory.ARRAY,
                Meta.StatementType.SELECT
        );
    }


    // 将列的类名转换为 Class 对象
    private Class<?> getJavaType(String className) {
        try {
            switch (className) {
                case "java.lang.String": return String.class;
                case "java.lang.Integer": return Integer.class;
                case "java.lang.Long": return Long.class;
                case "java.lang.Double": return Double.class;
                case "java.lang.Boolean": return Boolean.class;
                case "java.sql.Timestamp": return Timestamp.class;
                case "java.sql.Date": return Date.class;
                default: return Object.class;  // 未知类型默认使用 Object
            }
        } catch (Exception e) {
            // 无法识别列类型
            return Object.class;
        }
    }

    // 手动创建 AvaticaType（替代不存在的 of 方法）
    private ColumnMetaData.AvaticaType createAvaticaType(ResultSetMetaData metaData, int columnIndex) throws SQLException {
        int sqlType = metaData.getColumnType(columnIndex);
        String typeName = metaData.getColumnTypeName(columnIndex);
        int precision = metaData.getPrecision(columnIndex);
        int scale = metaData.getScale(columnIndex);
        Class<?> javaType = getJavaType(metaData.getColumnClassName(columnIndex));

        // 根据 SQL 类型映射到 Avatica 类型
        ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(javaType);

        return new ColumnMetaData.AvaticaType(
                columnIndex,        // SQL 类型
                metaData.getColumnName(columnIndex),
                rep             // Java 表示
        );
    }

}
