package io.falcondb.proc.trigger;

import java.util.Collections;
import java.util.Map;

/**
 * Return value from a trigger handler.
 *
 * For BEFORE ROW triggers: returning a modified newRow allows changing the row
 * before it is written (equivalent to returning NEW in PL/pgSQL).
 * Returning suppress() skips the DML entirely (equivalent to returning NULL).
 *
 * For AFTER triggers: the return value is ignored; use PROCEED.
 */
public class TriggerResult {
    public enum Action { PROCEED, SUPPRESS, MODIFY }

    private final Action action;
    private final Map<String, Object> modifiedRow;

    private TriggerResult(Action action, Map<String, Object> modifiedRow) {
        this.action = action;
        this.modifiedRow = modifiedRow;
    }

    /** Allow the DML to proceed unchanged. */
    public static TriggerResult proceed() {
        return new TriggerResult(Action.PROCEED, Collections.emptyMap());
    }

    /** Cancel the DML (BEFORE trigger only — equivalent to RETURN NULL in PL/pgSQL). */
    public static TriggerResult suppress() {
        return new TriggerResult(Action.SUPPRESS, Collections.emptyMap());
    }

    /** Modify the row before it is written (BEFORE ROW INSERT/UPDATE only). */
    public static TriggerResult modify(Map<String, Object> newRow) {
        return new TriggerResult(Action.MODIFY, Collections.unmodifiableMap(newRow));
    }

    public Action getAction() { return action; }
    public Map<String, Object> getModifiedRow() { return modifiedRow; }
    public boolean isSuppressed() { return action == Action.SUPPRESS; }
}
