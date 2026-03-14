package io.falcondb.proc;

import java.sql.*;
import java.util.*;

/**
 * Thin JDBC helpers for use inside Procedure implementations.
 * Wraps SQLException into ProcException so proc code stays clean.
 */
public class Sql {

    /** Execute a DML (INSERT/UPDATE/DELETE). Returns rows affected. */
    public static int execute(Connection conn, String sql, Object... params) throws ProcException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bind(ps, params);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw ProcException.fromSqlException(e);
        }
    }

    /** Query a single row. Returns empty Optional if no row. */
    public static Optional<Map<String, Object>> queryOne(Connection conn, String sql, Object... params)
            throws ProcException {
        List<Map<String, Object>> rows = queryList(conn, sql, params);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    /** Query a single scalar value from the first column of the first row. */
    public static <T> Optional<T> queryScalar(Connection conn, String sql, Object... params)
            throws ProcException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                @SuppressWarnings("unchecked")
                T val = (T) rs.getObject(1);
                return Optional.ofNullable(val);
            }
        } catch (SQLException e) {
            throw ProcException.fromSqlException(e);
        }
    }

    /** Query multiple rows. Returns list of column-name→value maps. */
    public static List<Map<String, Object>> queryList(Connection conn, String sql, Object... params)
            throws ProcException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                return toList(rs);
            }
        } catch (SQLException e) {
            throw ProcException.fromSqlException(e);
        }
    }

    /** Execute a batch of DML with the same SQL but different param sets. */
    public static int[] executeBatch(Connection conn, String sql, List<Object[]> paramSets)
            throws ProcException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Object[] params : paramSets) {
                bind(ps, params);
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (SQLException e) {
            throw ProcException.fromSqlException(e);
        }
    }

    private static void bind(PreparedStatement ps, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            ps.setObject(i + 1, params[i]);
        }
    }

    private static List<Map<String, Object>> toList(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= cols; i++) {
                row.put(meta.getColumnLabel(i), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }
}
