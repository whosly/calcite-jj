package com.whosly.avacita.server.query.mask.rule;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Map;
import java.util.function.Function;

public class MaskingResultSet implements ResultSet {
    private final ResultSet delegate;
    private final ResultSetMetaData metaData;
    private final Map<String, Function<Object, Object>> maskingFunctions;

    public MaskingResultSet(
            ResultSet delegate,
            ResultSetMetaData metaData,
            Map<String, Function<Object, Object>> maskingFunctions
    ) throws SQLException {
        this.delegate = delegate;
        this.metaData = new MaskingResultSetMetaData(metaData, maskingFunctions);
        this.maskingFunctions = maskingFunctions;
    }

    // 数据获取方法 - 应用脱敏逻辑
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Object value = delegate.getObject(columnIndex);
        String columnName = metaData.getColumnName(columnIndex);
        return applyMasking(value, columnName);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        String value = delegate.getString(columnIndex);
        String columnName = metaData.getColumnName(columnIndex);
        return (String) applyMasking(value, columnName);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return delegate.getBoolean(columnIndex); // 通常不脱敏布尔值
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return delegate.getByte(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return delegate.getShort(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return delegate.getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return delegate.getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return delegate.getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return delegate.getDouble(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return delegate.getBytes(columnIndex);
    }

    @Override
    public java.sql.Date getDate(int columnIndex) throws SQLException {
        return delegate.getDate(columnIndex);
    }

    @Override
    public java.sql.Time getTime(int columnIndex) throws SQLException {
        return delegate.getTime(columnIndex);
    }

    @Override
    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException {
        return delegate.getTimestamp(columnIndex);
    }

    @Override
    public java.io.Reader getCharacterStream(int columnIndex) throws SQLException {
        return delegate.getCharacterStream(columnIndex);
    }

    @Override
    public java.io.InputStream getAsciiStream(int columnIndex) throws SQLException {
        return delegate.getAsciiStream(columnIndex);
    }

    @Override
    public java.io.InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return delegate.getUnicodeStream(columnIndex);
    }

    @Override
    public java.io.InputStream getBinaryStream(int columnIndex) throws SQLException {
        return delegate.getBinaryStream(columnIndex);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        String value = delegate.getNString(columnIndex);
        String columnName = metaData.getColumnName(columnIndex);
        return (String) applyMasking(value, columnName);
    }

    @Override
    public java.io.Reader getNCharacterStream(int columnIndex) throws SQLException {
        return delegate.getNCharacterStream(columnIndex);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return delegate.getSQLXML(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return delegate.getObject(columnIndex, map);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return delegate.getObject(columnIndex, type);
    }

    @Override
    public java.sql.Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return delegate.getDate(columnIndex, cal);
    }

    @Override
    public java.sql.Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return delegate.getTime(columnIndex, cal);
    }

    @Override
    public java.sql.Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return delegate.getTimestamp(columnIndex, cal);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return delegate.getBigDecimal(columnIndex, scale);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return delegate.getBigDecimal(columnIndex);
    }

    // 位置和导航方法
    @Override
    public boolean next() throws SQLException {
        return delegate.next();
    }

    @Override
    public boolean previous() throws SQLException {
        return delegate.previous();
    }

    @Override
    public boolean first() throws SQLException {
        return delegate.first();
    }

    @Override
    public boolean last() throws SQLException {
        return delegate.last();
    }

    @Override
    public int getRow() throws SQLException {
        return delegate.getRow();
    }

    @Override
    public void beforeFirst() throws SQLException {
        delegate.beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        delegate.afterLast();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        delegate.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return delegate.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        delegate.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return delegate.getFetchSize();
    }

    @Override
    public int getType() throws SQLException {
        return delegate.getType();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return delegate.getConcurrency();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return delegate.rowUpdated();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return delegate.rowInserted();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return delegate.rowDeleted();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        delegate.updateNull(columnIndex);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        delegate.updateBoolean(columnIndex, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        delegate.updateByte(columnIndex, x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        delegate.updateShort(columnIndex, x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        delegate.updateInt(columnIndex, x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        delegate.updateLong(columnIndex, x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        delegate.updateFloat(columnIndex, x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        delegate.updateDouble(columnIndex, x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, java.math.BigDecimal x) throws SQLException {
        delegate.updateBigDecimal(columnIndex, x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        delegate.updateString(columnIndex, x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        delegate.updateBytes(columnIndex, x);
    }

    @Override
    public void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
        delegate.updateDate(columnIndex, x);
    }

    @Override
    public void updateTime(int columnIndex, java.sql.Time x) throws SQLException {
        delegate.updateTime(columnIndex, x);
    }

    @Override
    public void updateTimestamp(int columnIndex, java.sql.Timestamp x) throws SQLException {
        delegate.updateTimestamp(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, java.io.InputStream x, int length) throws SQLException {
        delegate.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, java.io.InputStream x, int length) throws SQLException {
        delegate.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, java.io.Reader x, int length) throws SQLException {
        delegate.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        delegate.updateObject(columnIndex, x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        delegate.updateObject(columnIndex, x);
    }

    @Override
    public void insertRow() throws SQLException {
        delegate.insertRow();
    }

    @Override
    public void updateRow() throws SQLException {
        delegate.updateRow();
    }

    @Override
    public void deleteRow() throws SQLException {
        delegate.deleteRow();
    }

    @Override
    public void refreshRow() throws SQLException {
        delegate.refreshRow();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        delegate.cancelRowUpdates();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        delegate.moveToInsertRow();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        delegate.moveToCurrentRow();
    }

    // 元数据方法
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metaData;
    }

    // 其他方法
    @Override
    public Statement getStatement() throws SQLException {
        return delegate.getStatement();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return delegate.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        delegate.clearWarnings();
    }

    @Override
    public String getCursorName() throws SQLException {
        return delegate.getCursorName();
    }

    // 闭包和资源管理
    @Override
    public void close() throws SQLException {
        delegate.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    // 扩展方法
    @Override
    public boolean absolute(int row) throws SQLException {
        return delegate.absolute(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return delegate.relative(rows);
    }

    @Override
    public boolean isFirst() throws SQLException {
        return delegate.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return delegate.isLast();
    }

    // JDBC 4.0+ 方法
    @Override
    public boolean isBeforeFirst() throws SQLException {
        return delegate.isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return delegate.isAfterLast();
    }

    @Override
    public int getHoldability() throws SQLException {
        return delegate.getHoldability();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return delegate.isWrapperFor(iface);
    }
    @Override
    public String getString(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getString(columnIndex);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBoolean(columnIndex);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getByte(columnIndex);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getShort(columnIndex);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getInt(columnIndex);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getLong(columnIndex);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getFloat(columnIndex);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getDouble(columnIndex);
    }

    @Override
    public java.math.BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBigDecimal(columnIndex, scale);
    }

    @Override
    public java.math.BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBigDecimal(columnIndex);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBytes(columnIndex);
    }

    @Override
    public java.sql.Date getDate(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getDate(columnIndex);
    }

    @Override
    public java.sql.Time getTime(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getTime(columnIndex);
    }

    @Override
    public java.sql.Timestamp getTimestamp(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getTimestamp(columnIndex);
    }

    @Override
    public java.io.InputStream getAsciiStream(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getAsciiStream(columnIndex);
    }

    @Override
    public java.io.InputStream getUnicodeStream(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getUnicodeStream(columnIndex);
    }

    @Override
    public java.io.InputStream getBinaryStream(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBinaryStream(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getObject(columnIndex);
    }

    @Override
    public java.io.Reader getCharacterStream(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getCharacterStream(columnIndex);
    }

    // 检查最后一次读取的列值是否为null
    @Override
    public boolean wasNull() throws SQLException {
        return delegate.wasNull();
    }

    // 根据列名查找列索引
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return delegate.findColumn(columnLabel);
    }

    // 获取指定列名对应的Ref对象
    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getRef(columnIndex);
    }

    // 获取指定列名对应的Blob对象
    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBlob(columnIndex);
    }

    // 获取指定列名对应的Clob对象
    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getClob(columnIndex);
    }

    // 获取指定列名对应的Array对象
    @Override
    public Array getArray(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getArray(columnIndex);
    }

    // 根据列名和指定类型获取对象
    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getObject(columnIndex, type);
    }

    // 获取指定列名对应的日期值，并使用指定日历
    @Override
    public java.sql.Date getDate(String columnLabel, Calendar cal) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getDate(columnIndex, cal);
    }

    // 获取指定列名对应的时间值，并使用指定日历
    @Override
    public java.sql.Time getTime(String columnLabel, Calendar cal) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getTime(columnIndex, cal);
    }

    // 获取指定列名对应的时间戳值，并使用指定日历
    @Override
    public java.sql.Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getTimestamp(columnIndex, cal);
    }

    // 获取指定列名对应的URL对象
    @Override
    public java.net.URL getURL(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getURL(columnIndex);
    }

    // 获取指定列名对应的RowId对象
    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getRowId(columnIndex);
    }

    // 获取指定列索引对应的 Ref 对象
    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return delegate.getRef(columnIndex);
    }

    // 获取指定列索引对应的 Blob 对象
    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return delegate.getBlob(columnIndex);
    }

    // 获取指定列索引对应的 Clob 对象
    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return delegate.getClob(columnIndex);
    }

    // 获取指定列索引对应的 Array 对象
    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return delegate.getArray(columnIndex);
    }

    // 根据列名和类型映射获取对象
    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getObject(columnIndex, map);
    }

    // 获取指定列索引对应的 URL 对象
    @Override
    public java.net.URL getURL(int columnIndex) throws SQLException {
        return delegate.getURL(columnIndex);
    }

    // 获取指定列索引对应的 RowId 对象
    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return delegate.getRowId(columnIndex);
    }

    // 获取指定列索引对应的 NClob 对象
    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return delegate.getNClob(columnIndex);
    }

    // 获取指定列名对应的 NClob 对象
    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getNClob(columnIndex);
    }

    // 获取指定列名对应的 SQLXML 对象
    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getSQLXML(columnIndex);
    }

    // 获取指定列名对应的 NString
    @Override
    public String getNString(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getNString(columnIndex);
    }

    // 获取指定列名对应的 NCharacterStream
    @Override
    public java.io.Reader getNCharacterStream(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getNCharacterStream(columnIndex);
    }

    // 更新指定列名对应的时间值
    @Override
    public void updateTime(String columnLabel, java.sql.Time x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateTime(columnIndex, x);
    }

    // 更新指定列名对应的时间戳值
    @Override
    public void updateTimestamp(String columnLabel, java.sql.Timestamp x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateTimestamp(columnIndex, x);
    }

    // 使用输入流和长度更新指定列名对应的ASCII流
    @Override
    public void updateAsciiStream(String columnLabel, java.io.InputStream x, int length) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateAsciiStream(columnIndex, x, length);
    }

    // 使用输入流和长度更新指定列名对应的二进制流
    @Override
    public void updateBinaryStream(String columnLabel, java.io.InputStream x, int length) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateBinaryStream(columnIndex, x, length);
    }

    // 使用字符流和长度更新指定列名对应的字符流
    @Override
    public void updateCharacterStream(String columnLabel, java.io.Reader x, int length) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateCharacterStream(columnIndex, x, length);
    }

    // 使用对象和精度或长度更新指定列名对应的对象
    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateObject(columnIndex, x, scaleOrLength);
    }

    // 更新指定列名对应的对象
    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateObject(columnIndex, x);
    }

    // 更新指定列名对应的Ref对象
    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateRef(columnIndex, x);
    }

    // 更新指定列名对应的Blob对象
    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateBlob(columnIndex, x);
    }

    // 更新指定列名对应的Clob对象
    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateClob(columnIndex, x);
    }

    // 更新指定列名对应的Array对象
    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateArray(columnIndex, x);
    }

    // 更新指定列名对应的RowId对象
    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateRowId(columnIndex, x);
    }


    // JDBC 4.1+ 方法
    @Override
    public void updateAsciiStream(int columnIndex, java.io.InputStream x, long length) throws SQLException {
        delegate.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, java.io.InputStream x, long length) throws SQLException {
        delegate.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, java.io.Reader x, long length) throws SQLException {
        delegate.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        delegate.updateBlob(columnIndex, x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        delegate.updateClob(columnIndex, x);
    }

    @Override
    public void updateNClob(int columnIndex, NClob x) throws SQLException {
        delegate.updateNClob(columnIndex, x);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML x) throws SQLException {
        delegate.updateSQLXML(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, java.io.InputStream x) throws SQLException {
        delegate.updateAsciiStream(columnIndex, x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, java.io.InputStream x) throws SQLException {
        delegate.updateBinaryStream(columnIndex, x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, java.io.Reader x) throws SQLException {
        delegate.updateCharacterStream(columnIndex, x);
    }

    @Override
    public void updateBlob(int columnIndex, java.io.InputStream x, long length) throws SQLException {
        delegate.updateBlob(columnIndex, x, length);
    }

    @Override
    public void updateClob(int columnIndex, java.io.Reader x, long length) throws SQLException {
        delegate.updateClob(columnIndex, x, length);
    }

    @Override
    public void updateNClob(int columnIndex, java.io.Reader x, long length) throws SQLException {
        delegate.updateNClob(columnIndex, x, length);
    }

    // JDBC 4.2+ 方法
    @Override
    public void updateNString(int columnIndex, String x) throws SQLException {
        delegate.updateNString(columnIndex, x);
    }

    @Override
    public void updateNClob(int columnIndex, java.io.Reader x) throws SQLException {
        delegate.updateNClob(columnIndex, x);
    }
    @Override
    public void updateNull(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateNull(columnIndex);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateBoolean(columnIndex, x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateByte(columnIndex, x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateShort(columnIndex, x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateInt(columnIndex, x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateLong(columnIndex, x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateFloat(columnIndex, x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateDouble(columnIndex, x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, java.math.BigDecimal x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateBigDecimal(columnIndex, x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateString(columnIndex, x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateBytes(columnIndex, x);
    }

    @Override
    public void updateDate(String columnLabel, java.sql.Date x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateDate(columnIndex, x);
    }

    // 更新指定列索引对应的 Ref 对象
    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        delegate.updateRef(columnIndex, x);
    }

    // 更新指定列索引对应的 Array 对象
    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        delegate.updateArray(columnIndex, x);
    }

    // 更新指定列索引对应的 RowId 对象
    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        delegate.updateRowId(columnIndex, x);
    }

    // 更新指定列名对应的 NString
    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateNString(columnIndex, nString);
    }

    // 更新指定列名对应的 NClob
    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateNClob(columnIndex, nClob);
    }

    // 使用输入流和长度更新指定列名对应的 Blob
    @Override
    public void updateBlob(String columnLabel, java.io.InputStream inputStream, long length) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateBlob(columnIndex, inputStream, length);
    }

    // 使用字符流和长度更新指定列名对应的 Clob
    @Override
    public void updateClob(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateClob(columnIndex, reader, length);
    }

    // 使用字符流和长度更新指定列名对应的 NClob
    @Override
    public void updateNClob(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateNClob(columnIndex, reader, length);
    }

    // 使用输入流更新指定列名对应的 Blob
    @Override
    public void updateBlob(String columnLabel, java.io.InputStream inputStream) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateBlob(columnIndex, inputStream);
    }

    // 使用字符流更新指定列名对应的 Clob
    @Override
    public void updateClob(String columnLabel, java.io.Reader reader) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateClob(columnIndex, reader);
    }

    // 使用字符流更新指定列名对应的 NClob
    @Override
    public void updateNClob(String columnLabel, java.io.Reader reader) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateNClob(columnIndex, reader);
    }

    // 更新指定列名对应的 SQLXML
    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateSQLXML(columnIndex, xmlObject);
    }

    // 使用字符流和长度更新指定列索引对应的 NCharacterStream
    @Override
    public void updateNCharacterStream(int columnIndex, java.io.Reader x, long length) throws SQLException {
        delegate.updateNCharacterStream(columnIndex, x, length);
    }

    // 使用字符流和长度更新指定列名对应的 NCharacterStream
    @Override
    public void updateNCharacterStream(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateNCharacterStream(columnIndex, reader, length);
    }

    // 使用输入流和长度更新指定列名对应的 ASCII 流
    @Override
    public void updateAsciiStream(String columnLabel, java.io.InputStream x, long length) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateAsciiStream(columnIndex, x, length);
    }

    // 使用输入流和长度更新指定列名对应的二进制流
    @Override
    public void updateBinaryStream(String columnLabel, java.io.InputStream x, long length) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateBinaryStream(columnIndex, x, length);
    }

    // 使用字符流和长度更新指定列名对应的字符流
    @Override
    public void updateCharacterStream(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateCharacterStream(columnIndex, reader, length);
    }

    // 使用字符流更新指定列索引对应的 NCharacterStream
    @Override
    public void updateNCharacterStream(int columnIndex, java.io.Reader x) throws SQLException {
        delegate.updateNCharacterStream(columnIndex, x);
    }

    // 使用字符流更新指定列名对应的 NCharacterStream
    @Override
    public void updateNCharacterStream(String columnLabel, java.io.Reader reader) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateNCharacterStream(columnIndex, reader);
    }

    // 使用输入流更新指定列名对应的 ASCII 流
    @Override
    public void updateAsciiStream(String columnLabel, java.io.InputStream x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateAsciiStream(columnIndex, x);
    }

    // 使用输入流更新指定列名对应的二进制流
    @Override
    public void updateBinaryStream(String columnLabel, java.io.InputStream x) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateBinaryStream(columnIndex, x);
    }

    // 使用字符流更新指定列名对应的字符流
    @Override
    public void updateCharacterStream(String columnLabel, java.io.Reader reader) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        updateCharacterStream(columnIndex, reader);
    }

    // 使用输入流更新指定列索引对应的 Blob
    @Override
    public void updateBlob(int columnIndex, java.io.InputStream inputStream) throws SQLException {
        delegate.updateBlob(columnIndex, inputStream);
    }

    // 使用字符流更新指定列索引对应的 Clob
    @Override
    public void updateClob(int columnIndex, java.io.Reader reader) throws SQLException {
        delegate.updateClob(columnIndex, reader);
    }

    // 内部辅助方法
    private Object applyMasking(Object value, String columnName) {
        if (value == null || !maskingFunctions.containsKey(columnName)) {
            return value;
        }
        return maskingFunctions.get(columnName).apply(value);
    }
}