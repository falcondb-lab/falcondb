package io.falcondb.jdbc;

import io.falcondb.jdbc.protocol.NativeConnection;
import io.falcondb.jdbc.protocol.WireFormat;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * Forward-only, read-only ResultSet backed by native protocol QueryResult.
 */
public class FalconResultSet implements ResultSet {

    private final NativeConnection.ColumnMeta[] columns;
    private final List<Object[]> rows;
    private int cursor = -1; // before first row
    private boolean closed = false;
    private boolean wasNull = false;

    FalconResultSet(NativeConnection.QueryResult result) {
        this.columns = result.columns;
        this.rows = result.rows;
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (cursor + 1 < rows.size()) {
            cursor++;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean wasNull() { return wasNull; }

    // ── Getters by column index (1-based) ────────────────────────────

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return null;
        return val.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return false;
        if (val instanceof Boolean) return (Boolean) val;
        if (val instanceof Number) return ((Number) val).intValue() != 0;
        return Boolean.parseBoolean(val.toString());
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return 0;
        if (val instanceof Number) return ((Number) val).byteValue();
        return Byte.parseByte(val.toString());
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return 0;
        if (val instanceof Number) return ((Number) val).shortValue();
        return Short.parseShort(val.toString());
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return 0;
        if (val instanceof Number) return ((Number) val).intValue();
        return Integer.parseInt(val.toString());
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return 0;
        if (val instanceof Number) return ((Number) val).longValue();
        return Long.parseLong(val.toString());
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return 0;
        if (val instanceof Number) return ((Number) val).floatValue();
        return Float.parseFloat(val.toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return 0;
        if (val instanceof Number) return ((Number) val).doubleValue();
        return Double.parseDouble(val.toString());
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return null;
        if (val instanceof byte[]) return (byte[]) val;
        return val.toString().getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return null;
        if (val instanceof Integer) {
            // days since epoch
            long millis = ((Integer) val) * 86400000L;
            return new Date(millis);
        }
        return Date.valueOf(val.toString());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return null;
        if (val instanceof Long) {
            // microseconds since midnight
            long millis = ((Long) val) / 1000;
            return new Time(millis);
        }
        return Time.valueOf(val.toString());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return null;
        if (val instanceof Long) {
            // microseconds since epoch
            long millis = ((Long) val) / 1000;
            return new Timestamp(millis);
        }
        return Timestamp.valueOf(val.toString());
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        if (columnIndex < 1 || columnIndex > columns.length) {
            throw new SQLException("Column index out of range: " + columnIndex);
        }
        Object val = rows.get(cursor)[columnIndex - 1];
        wasNull = (val == null);
        return val;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return null;
        if (val instanceof BigDecimal) return (BigDecimal) val;
        return new BigDecimal(val.toString());
    }

    // ── Getters by column name ───────────────────────────────────────

    @Override public String getString(String columnLabel) throws SQLException { return getString(findColumn(columnLabel)); }
    @Override public boolean getBoolean(String columnLabel) throws SQLException { return getBoolean(findColumn(columnLabel)); }
    @Override public byte getByte(String columnLabel) throws SQLException { return getByte(findColumn(columnLabel)); }
    @Override public short getShort(String columnLabel) throws SQLException { return getShort(findColumn(columnLabel)); }
    @Override public int getInt(String columnLabel) throws SQLException { return getInt(findColumn(columnLabel)); }
    @Override public long getLong(String columnLabel) throws SQLException { return getLong(findColumn(columnLabel)); }
    @Override public float getFloat(String columnLabel) throws SQLException { return getFloat(findColumn(columnLabel)); }
    @Override public double getDouble(String columnLabel) throws SQLException { return getDouble(findColumn(columnLabel)); }
    @Override @SuppressWarnings("deprecation") public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException { return getBigDecimal(findColumn(columnLabel)); }
    @Override public byte[] getBytes(String columnLabel) throws SQLException { return getBytes(findColumn(columnLabel)); }
    @Override public Date getDate(String columnLabel) throws SQLException { return getDate(findColumn(columnLabel)); }
    @Override public Time getTime(String columnLabel) throws SQLException { return getTime(findColumn(columnLabel)); }
    @Override public Timestamp getTimestamp(String columnLabel) throws SQLException { return getTimestamp(findColumn(columnLabel)); }
    @Override public Object getObject(String columnLabel) throws SQLException { return getObject(findColumn(columnLabel)); }
    @Override public BigDecimal getBigDecimal(String columnLabel) throws SQLException { return getBigDecimal(findColumn(columnLabel)); }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].name.equalsIgnoreCase(columnLabel)) return i + 1;
        }
        throw new SQLException("Column not found: " + columnLabel);
    }

    // ── Metadata ─────────────────────────────────────────────────────

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new FalconResultSetMetaData(columns);
    }

    // ── Navigation ───────────────────────────────────────────────────

    @Override public boolean isBeforeFirst() { return cursor < 0; }
    @Override public boolean isAfterLast() { return cursor >= rows.size(); }
    @Override public boolean isFirst() { return cursor == 0; }
    @Override public boolean isLast() { return cursor == rows.size() - 1; }
    @Override public void beforeFirst() throws SQLException { cursor = -1; }
    @Override public int getRow() { return cursor >= 0 && cursor < rows.size() ? cursor + 1 : 0; }
    @Override public int getType() { return TYPE_FORWARD_ONLY; }
    @Override public int getConcurrency() { return CONCUR_READ_ONLY; }
    @Override public boolean isClosed() { return closed; }
    @Override public int getHoldability() { return HOLD_CURSORS_OVER_COMMIT; }

    @Override public Statement getStatement() { return null; }
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public String getCursorName() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setFetchDirection(int direction) {}
    @Override public int getFetchDirection() { return FETCH_FORWARD; }
    @Override public void setFetchSize(int rows) {}
    @Override public int getFetchSize() { return 0; }

    // ── Unsupported navigation ───────────────────────────────────────

    @Override public void afterLast() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public boolean first() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public boolean last() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public boolean absolute(int row) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public boolean relative(int rows) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public boolean previous() throws SQLException { throw new SQLFeatureNotSupportedException(); }

    // ── Unsupported update methods ───────────────────────────────────

    @Override public boolean rowUpdated() { return false; }
    @Override public boolean rowInserted() { return false; }
    @Override public boolean rowDeleted() { return false; }
    @Override public void insertRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void deleteRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void refreshRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void cancelRowUpdates() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void moveToInsertRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void moveToCurrentRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }

    // ── Unsupported update setters (all throw) ───────────────────────
    @Override public void updateNull(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBoolean(int c, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateByte(int c, byte x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateShort(int c, short x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateInt(int c, int x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateLong(int c, long x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateFloat(int c, float x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDouble(int c, double x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBigDecimal(int c, BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateString(int c, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBytes(int c, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDate(int c, Date x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTime(int c, Time x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTimestamp(int c, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int c, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int c, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int c, Reader x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(int c, Object x, int s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(int c, Object x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNull(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBoolean(String c, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateByte(String c, byte x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateShort(String c, short x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateInt(String c, int x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateLong(String c, long x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateFloat(String c, float x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDouble(String c, double x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBigDecimal(String c, BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateString(String c, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBytes(String c, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDate(String c, Date x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTime(String c, Time x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTimestamp(String c, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String c, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String c, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String c, Reader x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(String c, Object x, int s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(String c, Object x) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    // ── Unsupported typed getters ────────────────────────────────────
    @Override public InputStream getAsciiStream(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override @SuppressWarnings("deprecation") public InputStream getUnicodeStream(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public InputStream getBinaryStream(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public InputStream getAsciiStream(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override @SuppressWarnings("deprecation") public InputStream getUnicodeStream(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public InputStream getBinaryStream(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Reader getCharacterStream(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Reader getCharacterStream(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Object getObject(int c, Map<String, Class<?>> m) throws SQLException { return getObject(c); }
    @Override public Ref getRef(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Blob getBlob(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Clob getClob(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Array getArray(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Object getObject(String c, Map<String, Class<?>> m) throws SQLException { return getObject(c); }
    @Override public Ref getRef(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Blob getBlob(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Clob getClob(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Array getArray(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Date getDate(int c, Calendar cal) throws SQLException { return getDate(c); }
    @Override public Date getDate(String c, Calendar cal) throws SQLException { return getDate(c); }
    @Override public Time getTime(int c, Calendar cal) throws SQLException { return getTime(c); }
    @Override public Time getTime(String c, Calendar cal) throws SQLException { return getTime(c); }
    @Override public Timestamp getTimestamp(int c, Calendar cal) throws SQLException { return getTimestamp(c); }
    @Override public Timestamp getTimestamp(String c, Calendar cal) throws SQLException { return getTimestamp(c); }
    @Override public URL getURL(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public URL getURL(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public RowId getRowId(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public RowId getRowId(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public NClob getNClob(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public NClob getNClob(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public SQLXML getSQLXML(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public SQLXML getSQLXML(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public String getNString(int c) throws SQLException { return getString(c); }
    @Override public String getNString(String c) throws SQLException { return getString(c); }
    @Override public Reader getNCharacterStream(int c) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Reader getNCharacterStream(String c) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    // Update streams (unsupported)
    @Override public void updateRef(int c, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRef(String c, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int c, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String c, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int c, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String c, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateArray(int c, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateArray(String c, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRowId(int c, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRowId(String c, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNString(int c, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNString(String c, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int c, NClob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String c, NClob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateSQLXML(int c, SQLXML x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateSQLXML(String c, SQLXML x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(int c, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(String c, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int c, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int c, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int c, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String c, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String c, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String c, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int c, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String c, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int c, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String c, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int c, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String c, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(int c, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int c, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int c, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int c, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String c, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String c, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String c, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int c, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String c, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int c, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String c, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int c, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String c, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object val = getObject(columnIndex);
        if (val == null) return null;
        return type.cast(val);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

    private void checkClosed() throws SQLException {
        if (closed) throw new SQLException("ResultSet is closed");
    }

    private void checkRow() throws SQLException {
        if (cursor < 0 || cursor >= rows.size()) {
            throw new SQLException("No current row");
        }
    }
}
