package com.whosly.avacita.server.query.mask.rule;

import com.whosly.avacita.server.query.mask.util.ValueMaskingStrategy;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.calcite.avatica.util.Cursor;

import java.sql.SQLException;
import java.util.*;

public class MaskingCursor implements Cursor {
    private final Cursor delegate;
    private final Map<Integer, MaskingRuleConfig> columnRules;
    private List<Accessor> accessors;

    MaskingCursor(Cursor delegate, Map<Integer, MaskingRuleConfig> columnRules) {
        this.delegate = delegate;
        this.columnRules = columnRules;
    }

    @Override
    public List<Accessor> createAccessors(List<ColumnMetaData> types,
                                          Calendar calendar, ArrayImpl.Factory factory) {

        List<Accessor> originalAccessors = delegate.createAccessors(types, calendar, factory);
        List<Accessor> maskedAccessors = new ArrayList<>(originalAccessors.size());

        // 为每个列创建脱敏包装器
        for (int i = 0; i < originalAccessors.size(); i++) {
            MaskingRuleConfig rule = columnRules.get(i + 1); // 列序号从1开始
            maskedAccessors.add(new MaskingAccessor(originalAccessors.get(i), rule));
        }

        return maskedAccessors;
    }

    @Override
    public boolean next() throws SQLException {
        return delegate.next();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return delegate.wasNull();
    }

    public Meta.Frame toFrame() throws SQLException {
        List<Object> rows = new ArrayList<>();
        while (this.next()) {
            rows.add(this.getObject());
        }
        return new Meta.Frame(0, true, rows);
    }

    private Object maskData(Object row) {
        if (row instanceof List) {
            List<Object> data = new ArrayList<>((List<Object>) row);
            for (int i = 0; i < data.size(); i++) {
                MaskingRuleConfig rule = columnRules.get(i + 1);
                if (rule != null) {
                    data.set(i, ValueMaskingStrategy.mask(data.get(i), rule));
                }
            }
            return Collections.unmodifiableList(data);
        }
        return row;
    }
}
