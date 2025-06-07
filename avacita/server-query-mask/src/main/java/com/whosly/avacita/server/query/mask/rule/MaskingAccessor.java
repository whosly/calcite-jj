package com.whosly.avacita.server.query.mask.rule;

import com.whosly.avacita.server.query.mask.util.ValueMaskingStrategy;
import org.apache.calcite.avatica.util.Cursor;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public class MaskingAccessor implements Cursor.Accessor {
    private final Cursor.Accessor delegate;
    private final MaskingRuleConfig rule;

    MaskingAccessor(Cursor.Accessor delegate, MaskingRuleConfig rule) {
        this.delegate = delegate;
        this.rule = rule;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return delegate.wasNull();
    }

    @Override
    public String getString() throws SQLException {
        String original = delegate.getString();

        return rule != null ? (String) ValueMaskingStrategy.mask(original, rule) : original;
    }

    @Override
    public boolean getBoolean() throws SQLException {
        return delegate.getBoolean();
    }

    @Override
    public byte getByte() throws SQLException {
        return delegate.getByte();
    }

    @Override
    public short getShort() throws SQLException {
        return delegate.getShort();
    }

    @Override
    public int getInt() throws SQLException {
        return delegate.getInt();
    }

    @Override
    public long getLong() throws SQLException {
        return delegate.getLong();
    }

    @Override
    public float getFloat() throws SQLException {
        return delegate.getFloat();
    }

    @Override
    public double getDouble() throws SQLException {
        return delegate.getDouble();
    }

    @Override
    public BigDecimal getBigDecimal() throws SQLException {
        return delegate.getBigDecimal();
    }

    @Override
    public BigDecimal getBigDecimal(int scale) throws SQLException {
        return delegate.getBigDecimal(scale);
    }

    @Override
    public byte[] getBytes() throws SQLException {
        return delegate.getBytes();
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        return delegate.getAsciiStream();
    }

    @Override
    public InputStream getUnicodeStream() throws SQLException {
        return delegate.getUnicodeStream();
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return delegate.getBinaryStream();
    }

    @Override
    public Object getObject() throws SQLException {
        Object original = delegate.getObject();
        return rule != null ? ValueMaskingStrategy.mask(original, rule) : original;
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return delegate.getCharacterStream();
    }

    @Override
    public Object getObject(Map<String, Class<?>> map) throws SQLException {
        Object original = delegate.getObject(map);
        return rule != null ? ValueMaskingStrategy.mask(original, rule) : original;
    }

    @Override
    public Ref getRef() throws SQLException {
        return delegate.getRef();
    }

    @Override
    public Blob getBlob() throws SQLException {
        return delegate.getBlob();
    }

    @Override
    public Clob getClob() throws SQLException {
        return delegate.getClob();
    }

    @Override
    public Array getArray() throws SQLException {
        return delegate.getArray();
    }

    @Override
    public Struct getStruct() throws SQLException {
        return delegate.getStruct();
    }

    @Override
    public Date getDate(Calendar calendar) throws SQLException {
        return delegate.getDate(calendar);
    }

    @Override
    public Time getTime(Calendar calendar) throws SQLException {
        return delegate.getTime(calendar);
    }

    @Override
    public Timestamp getTimestamp(Calendar calendar) throws SQLException {
        return delegate.getTimestamp(calendar);
    }

    @Override
    public URL getURL() throws SQLException {
        return delegate.getURL();
    }

    @Override
    public NClob getNClob() throws SQLException {
        return delegate.getNClob();
    }

    @Override
    public SQLXML getSQLXML() throws SQLException {
        return delegate.getSQLXML();
    }

    @Override
    public String getNString() throws SQLException {
        String original = delegate.getNString();
        return rule != null ? (String) ValueMaskingStrategy.mask(original, rule) : original;
    }

    @Override
    public Reader getNCharacterStream() throws SQLException {
        return delegate.getNCharacterStream();
    }

    @Override
    public <T> T getObject(Class<T> type) throws SQLException {
        T t = delegate.getObject(type);

        if(t instanceof String) {
            return rule != null ? (T) ValueMaskingStrategy.mask(t, rule) : t;

        }

        return t;
    }
}
