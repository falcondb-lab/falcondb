package io.falcondb.jdbc;

import io.falcondb.jdbc.protocol.NativeConnection;
import io.falcondb.jdbc.protocol.WireFormat;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * JDBC Connection implementation over the FalconDB native protocol.
 */
public class FalconConnection implements Connection {

    private final NativeConnection nativeConn;
    private boolean autoCommit = true;
    private boolean readOnly = false;
    private boolean closed = false;
    private int networkTimeout = 0;
    private String catalog;
    private String schema;

    FalconConnection(String host, int port, String database,
                     String user, String password, int connectTimeoutMs) throws Exception {
        this.nativeConn = new NativeConnection(host, port, database, user, password, connectTimeoutMs);
        this.catalog = database;
        this.schema = "public";
    }

    NativeConnection getNativeConnection() {
        return nativeConn;
    }

    int getSessionFlags() {
        int flags = 0;
        if (autoCommit) flags |= WireFormat.SESSION_AUTOCOMMIT;
        if (readOnly) flags |= WireFormat.SESSION_READ_ONLY;
        return flags;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new FalconStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new FalconPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public String nativeSQL(String sql) { return sql; }

    @Override
    public void setAutoCommit(boolean autoCommit) { this.autoCommit = autoCommit; }

    @Override
    public boolean getAutoCommit() { return autoCommit; }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        if (autoCommit) throw new SQLException("Cannot commit in auto-commit mode");
        try {
            nativeConn.executeQuery("COMMIT", getSessionFlags());
        } catch (Exception e) {
            throw new SQLException("commit failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        if (autoCommit) throw new SQLException("Cannot rollback in auto-commit mode");
        try {
            nativeConn.executeQuery("ROLLBACK", getSessionFlags());
        } catch (Exception e) {
            throw new SQLException("rollback failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            try {
                nativeConn.close();
            } catch (Exception e) {
                throw new SQLException("close failed: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public boolean isClosed() { return closed || nativeConn.isClosed(); }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("DatabaseMetaData not yet implemented");
    }

    @Override
    public void setReadOnly(boolean readOnly) { this.readOnly = readOnly; }

    @Override
    public boolean isReadOnly() { return readOnly; }

    @Override
    public void setCatalog(String catalog) { this.catalog = catalog; }

    @Override
    public String getCatalog() { return catalog; }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        // Accept but only support READ_COMMITTED for now
        if (level != TRANSACTION_READ_COMMITTED && level != TRANSACTION_READ_UNCOMMITTED) {
            throw new SQLException("Unsupported isolation level: " + level);
        }
    }

    @Override
    public int getTransactionIsolation() { return TRANSACTION_READ_COMMITTED; }

    @Override
    public SQLWarning getWarnings() { return null; }

    @Override
    public void clearWarnings() {}

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() { return java.util.Collections.emptyMap(); }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) {}

    @Override
    public void setHoldability(int holdability) {}

    @Override
    public int getHoldability() { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not yet supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not yet supported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not yet supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not yet supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override
    public Blob createBlob() throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override
    public NClob createNClob() throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override
    public SQLXML createSQLXML() throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (closed) return false;
        try {
            return nativeConn.ping(timeout > 0 ? timeout * 1000 : 5000);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void setClientInfo(String name, String value) {}

    @Override
    public void setClientInfo(Properties properties) {}

    @Override
    public String getClientInfo(String name) { return null; }

    @Override
    public Properties getClientInfo() { return new Properties(); }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) { this.schema = schema; }

    @Override
    public String getSchema() { return schema; }

    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
        this.networkTimeout = milliseconds;
    }

    @Override
    public int getNetworkTimeout() { return networkTimeout; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

    private void checkClosed() throws SQLException {
        if (closed) throw new SQLException("Connection is closed");
    }
}
