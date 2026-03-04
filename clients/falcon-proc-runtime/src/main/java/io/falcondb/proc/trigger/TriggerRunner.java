package io.falcondb.proc.trigger;

import io.falcondb.proc.ProcException;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes trigger handlers around DML operations.
 *
 * Typical usage in application code (replacing a direct INSERT/UPDATE/DELETE):
 *
 *   TriggerRunner triggers = new TriggerRunner(registry);
 *
 *   // was: conn.prepareStatement("INSERT INTO orders ...").executeUpdate()
 *   triggers.insert(conn, "orders", newRow, () -> {
 *       Sql.execute(conn, "INSERT INTO orders(id, amount) VALUES (?, ?)", id, amount);
 *       return 1;
 *   });
 */
public class TriggerRunner {

    private final TriggerRegistry registry;

    public TriggerRunner(TriggerRegistry registry) {
        this.registry = registry;
    }

    /**
     * Wrap an INSERT with BEFORE/AFTER trigger firing.
     * The rowSupplier should build the row map that will be passed as NEW.
     */
    public int insert(Connection conn, String table, Map<String, Object> newRow, DmlAction dml)
            throws ProcException {
        Map<String, Object> row = fireBefore(conn, table, TriggerOperation.INSERT, newRow, null);
        if (row == null) return 0; // suppressed

        int affected = invokeDml(dml);
        fireAfter(conn, table, TriggerOperation.INSERT, row, null);
        return affected;
    }

    /**
     * Wrap an UPDATE with BEFORE/AFTER trigger firing.
     */
    public int update(Connection conn, String table,
                      Map<String, Object> newRow, Map<String, Object> oldRow,
                      DmlAction dml) throws ProcException {
        Map<String, Object> row = fireBefore(conn, table, TriggerOperation.UPDATE, newRow, oldRow);
        if (row == null) return 0;

        int affected = invokeDml(dml);
        fireAfter(conn, table, TriggerOperation.UPDATE, row, oldRow);
        return affected;
    }

    /**
     * Wrap a DELETE with BEFORE/AFTER trigger firing.
     */
    public int delete(Connection conn, String table, Map<String, Object> oldRow, DmlAction dml)
            throws ProcException {
        Map<String, Object> row = fireBefore(conn, table, TriggerOperation.DELETE, null, oldRow);
        if (row == null) return 0;

        int affected = invokeDml(dml);
        fireAfter(conn, table, TriggerOperation.DELETE, null, oldRow);
        return affected;
    }

    // ── internals ────────────────────────────────────────────────────────────

    /** Returns the (possibly modified) newRow, or null if suppressed. */
    private Map<String, Object> fireBefore(Connection conn, String table, TriggerOperation op,
                                           Map<String, Object> newRow, Map<String, Object> oldRow)
            throws ProcException {
        List<TriggerHandler> list = registry.get(table, TriggerTiming.BEFORE, op);
        Map<String, Object> current = newRow != null ? new HashMap<>(newRow) : null;

        for (TriggerHandler h : list) {
            TriggerEvent event = TriggerEvent.builder()
                    .timing(TriggerTiming.BEFORE)
                    .operation(op)
                    .table(table)
                    .newRow(current != null ? current : new HashMap<>())
                    .oldRow(oldRow != null ? oldRow : new HashMap<>())
                    .connection(conn)
                    .build();

            TriggerResult result = h.fire(event);

            if (result.isSuppressed()) return null;
            if (result.getAction() == TriggerResult.Action.MODIFY) {
                current = new HashMap<>(result.getModifiedRow());
            }
        }
        return current;
    }

    private void fireAfter(Connection conn, String table, TriggerOperation op,
                           Map<String, Object> newRow, Map<String, Object> oldRow)
            throws ProcException {
        List<TriggerHandler> list = registry.get(table, TriggerTiming.AFTER, op);
        for (TriggerHandler h : list) {
            TriggerEvent event = TriggerEvent.builder()
                    .timing(TriggerTiming.AFTER)
                    .operation(op)
                    .table(table)
                    .newRow(newRow != null ? newRow : new HashMap<>())
                    .oldRow(oldRow != null ? oldRow : new HashMap<>())
                    .connection(conn)
                    .build();
            h.fire(event);
        }
    }

    private int invokeDml(DmlAction dml) throws ProcException {
        try {
            return dml.execute();
        } catch (ProcException e) {
            throw e;
        } catch (Exception e) {
            throw new ProcException("DML failed inside trigger wrapper", "XX000", e);
        }
    }

    @FunctionalInterface
    public interface DmlAction {
        int execute() throws Exception;
    }
}
