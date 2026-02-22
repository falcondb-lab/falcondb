package io.falcondb.jdbc;

import io.falcondb.jdbc.protocol.NativeConnection;
import io.falcondb.jdbc.protocol.WireFormat;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Minimal ResultSetMetaData backed by native protocol column metadata.
 */
public class FalconResultSetMetaData implements ResultSetMetaData {

    private final NativeConnection.ColumnMeta[] columns;

    FalconResultSetMetaData(NativeConnection.ColumnMeta[] columns) {
        this.columns = columns;
    }

    @Override
    public int getColumnCount() { return columns.length; }

    @Override
    public boolean isAutoIncrement(int column) { return false; }

    @Override
    public boolean isCaseSensitive(int column) { return true; }

    @Override
    public boolean isSearchable(int column) { return true; }

    @Override
    public boolean isCurrency(int column) { return false; }

    @Override
    public int isNullable(int column) throws SQLException {
        return col(column).nullable ? columnNullable : columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        byte t = col(column).typeId;
        return t == WireFormat.TYPE_INT32 || t == WireFormat.TYPE_INT64
            || t == WireFormat.TYPE_FLOAT64 || t == WireFormat.TYPE_DECIMAL;
    }

    @Override
    public int getColumnDisplaySize(int column) { return 40; }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return col(column).name;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return col(column).name;
    }

    @Override
    public String getSchemaName(int column) { return ""; }

    @Override
    public int getPrecision(int column) throws SQLException {
        return col(column).precision;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return col(column).scale;
    }

    @Override
    public String getTableName(int column) { return ""; }

    @Override
    public String getCatalogName(int column) { return ""; }

    @Override
    public int getColumnType(int column) throws SQLException {
        return WireFormat.typeIdToSqlType(col(column).typeId);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return WireFormat.typeIdToName(col(column).typeId);
    }

    @Override
    public boolean isReadOnly(int column) { return true; }

    @Override
    public boolean isWritable(int column) { return false; }

    @Override
    public boolean isDefinitelyWritable(int column) { return false; }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        byte t = col(column).typeId;
        switch (t) {
            case WireFormat.TYPE_BOOLEAN:   return "java.lang.Boolean";
            case WireFormat.TYPE_INT32:     return "java.lang.Integer";
            case WireFormat.TYPE_INT64:     return "java.lang.Long";
            case WireFormat.TYPE_FLOAT64:   return "java.lang.Double";
            case WireFormat.TYPE_TEXT:      return "java.lang.String";
            case WireFormat.TYPE_TIMESTAMP: return "java.sql.Timestamp";
            case WireFormat.TYPE_DATE:      return "java.sql.Date";
            case WireFormat.TYPE_DECIMAL:   return "java.math.BigDecimal";
            case WireFormat.TYPE_BYTEA:     return "[B";
            default:                        return "java.lang.Object";
        }
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

    private NativeConnection.ColumnMeta col(int column) throws SQLException {
        if (column < 1 || column > columns.length) {
            throw new SQLException("Column index out of range: " + column);
        }
        return columns[column - 1];
    }
}
