package com.whosly.avacita.server.query.mask.util;

import org.apache.calcite.avatica.Meta;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FrameBuilder {

    public Meta.Frame buildFrame(ResultSet rs) throws SQLException {
        List<Object> rows = new ArrayList<>();
        while (rs.next()) {
            Object[] row = new Object[rs.getMetaData().getColumnCount()];
            for (int i = 0; i < row.length; i++) {
                row[i] = rs.getObject(i + 1);
            }
            rows.add(row);
        }
        return new Meta.Frame(0, rs.isAfterLast(), rows);
    }

}
