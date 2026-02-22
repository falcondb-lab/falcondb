package io.falcondb.jdbc;

import io.falcondb.jdbc.protocol.NativeConnection;
import io.falcondb.jdbc.protocol.WireFormat;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * JDBC PreparedStatement with client-side parameter binding and batch support.
 */
public class FalconPreparedStatement extends FalconStatement implements PreparedStatement {

    private final String sql;
    private final List<Object[]> batchParams = new ArrayList<>();
    private Object[] currentParams;
    private byte[] columnTypes;
    private int paramCount;

    FalconPreparedStatement(FalconConnection conn, String sql) {
        super(conn);
        this.sql = sql;
        this.paramCount = 0;
        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == '?') paramCount++;
        }
        this.currentParams = new Object[paramCount];
        this.columnTypes = new byte[paramCount];
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        String bound = bindParameters();
        return super.executeQuery(bound);
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkClosed();
        String bound = bindParameters();
        return super.executeUpdate(bound);
    }

    @Override
    public boolean execute() throws SQLException {
        checkClosed();
        String bound = bindParameters();
        return super.execute(bound);
    }

    @Override
    public void addBatch() throws SQLException {
        checkClosed();
        batchParams.add(currentParams.clone());
        currentParams = new Object[paramCount];
    }

    @Override
    public void clearBatch() throws SQLException {
        batchParams.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        if (batchParams.isEmpty()) return new int[0];
        try {
            int[] results = new int[batchParams.size()];
            for (int i = 0; i < batchParams.size(); i++) {
                currentParams = batchParams.get(i);
                String bound = bindParameters();
                NativeConnection.QueryResult result =
                    conn.getNativeConnection().executeQuery(bound, conn.getSessionFlags());
                results[i] = (int) result.rowsAffected;
            }
            batchParams.clear();
            return results;
        } catch (Exception e) {
            throw wrapException(e);
        }
    }

    @Override public void setNull(int i, int t) throws SQLException { setParam(i, null, WireFormat.TYPE_NULL); }
    @Override public void setBoolean(int i, boolean x) throws SQLException { setParam(i, x, WireFormat.TYPE_BOOLEAN); }
    @Override public void setByte(int i, byte x) throws SQLException { setParam(i, (int) x, WireFormat.TYPE_INT32); }
    @Override public void setShort(int i, short x) throws SQLException { setParam(i, (int) x, WireFormat.TYPE_INT32); }
    @Override public void setInt(int i, int x) throws SQLException { setParam(i, x, WireFormat.TYPE_INT32); }
    @Override public void setLong(int i, long x) throws SQLException { setParam(i, x, WireFormat.TYPE_INT64); }
    @Override public void setFloat(int i, float x) throws SQLException { setParam(i, (double) x, WireFormat.TYPE_FLOAT64); }
    @Override public void setDouble(int i, double x) throws SQLException { setParam(i, x, WireFormat.TYPE_FLOAT64); }
    @Override public void setBigDecimal(int i, BigDecimal x) throws SQLException { setParam(i, x != null ? x.toPlainString() : null, WireFormat.TYPE_TEXT); }
    @Override public void setString(int i, String x) throws SQLException { setParam(i, x, WireFormat.TYPE_TEXT); }
    @Override public void setBytes(int i, byte[] x) throws SQLException { setParam(i, x, WireFormat.TYPE_BYTEA); }
    @Override public void setDate(int i, Date x) throws SQLException { setParam(i, x != null ? x.toString() : null, WireFormat.TYPE_TEXT); }
    @Override public void setTime(int i, Time x) throws SQLException { setParam(i, x != null ? x.toString() : null, WireFormat.TYPE_TEXT); }
    @Override public void setTimestamp(int i, Timestamp x) throws SQLException { setParam(i, x != null ? x.toString() : null, WireFormat.TYPE_TEXT); }

    @Override
    public void setObject(int i, Object x) throws SQLException {
        if (x == null) setNull(i, Types.NULL);
        else if (x instanceof String) setString(i, (String) x);
        else if (x instanceof Integer) setInt(i, (Integer) x);
        else if (x instanceof Long) setLong(i, (Long) x);
        else if (x instanceof Double) setDouble(i, (Double) x);
        else if (x instanceof Float) setFloat(i, (Float) x);
        else if (x instanceof Boolean) setBoolean(i, (Boolean) x);
        else if (x instanceof byte[]) setBytes(i, (byte[]) x);
        else setString(i, x.toString());
    }

    @Override public void setObject(int i, Object x, int t) throws SQLException { setObject(i, x); }
    @Override public void setObject(int i, Object x, int t, int s) throws SQLException { setObject(i, x); }

    @Override
    public void clearParameters() throws SQLException {
        currentParams = new Object[paramCount];
        columnTypes = new byte[paramCount];
    }

    @Override public ResultSetMetaData getMetaData() throws SQLException { return null; }
    @Override public ParameterMetaData getParameterMetaData() throws SQLException { throw new SQLFeatureNotSupportedException(); }

    // Unsupported stream/LOB setters
    @Override public void setAsciiStream(int i, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override @SuppressWarnings("deprecation") public void setUnicodeStream(int i, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int i, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int i, Reader r, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setRef(int i, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int i, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int i, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setArray(int i, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setDate(int i, Date x, Calendar c) throws SQLException { setDate(i, x); }
    @Override public void setTime(int i, Time x, Calendar c) throws SQLException { setTime(i, x); }
    @Override public void setTimestamp(int i, Timestamp x, Calendar c) throws SQLException { setTimestamp(i, x); }
    @Override public void setNull(int i, int t, String n) throws SQLException { setNull(i, t); }
    @Override public void setURL(int i, URL x) throws SQLException { setString(i, x.toString()); }
    @Override public void setRowId(int i, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNString(int i, String v) throws SQLException { setString(i, v); }
    @Override public void setNCharacterStream(int i, Reader v, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int i, NClob v) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int i, Reader r, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int i, InputStream s, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int i, Reader r, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setSQLXML(int i, SQLXML x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setAsciiStream(int i, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int i, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int i, Reader r, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setAsciiStream(int i, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int i, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int i, Reader r) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNCharacterStream(int i, Reader v) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int i, Reader r) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int i, InputStream s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int i, Reader r) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    // ── Internal ─────────────────────────────────────────────────────

    private void setParam(int index, Object value, byte typeId) throws SQLException {
        if (index < 1 || index > paramCount) {
            throw new SQLException("Parameter index out of range: " + index);
        }
        currentParams[index - 1] = value;
        columnTypes[index - 1] = typeId;
    }

    /**
     * Client-side parameter binding: replace ? placeholders with literal values.
     */
    String bindParameters() throws SQLException {
        if (paramCount == 0) return sql;

        StringBuilder sb = new StringBuilder(sql.length() + paramCount * 16);
        int paramIdx = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '?' && paramIdx < paramCount) {
                Object val = currentParams[paramIdx];
                if (val == null) {
                    sb.append("NULL");
                } else if (val instanceof Boolean) {
                    sb.append(((Boolean) val) ? "TRUE" : "FALSE");
                } else if (val instanceof Number) {
                    sb.append(val);
                } else if (val instanceof String) {
                    sb.append('\'');
                    sb.append(((String) val).replace("'", "''"));
                    sb.append('\'');
                } else {
                    sb.append('\'');
                    sb.append(val.toString().replace("'", "''"));
                    sb.append('\'');
                }
                paramIdx++;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
