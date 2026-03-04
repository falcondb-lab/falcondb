package io.falcondb.proc;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ProcResult {
    public enum Kind { VOID, SCALAR, ROWS }

    private final Kind kind;
    private final Object scalar;
    private final List<Map<String, Object>> rows;
    private final int rowsAffected;

    private ProcResult(Kind kind, Object scalar, List<Map<String, Object>> rows, int rowsAffected) {
        this.kind = kind;
        this.scalar = scalar;
        this.rows = rows;
        this.rowsAffected = rowsAffected;
    }

    public static ProcResult void_(int rowsAffected) {
        return new ProcResult(Kind.VOID, null, Collections.emptyList(), rowsAffected);
    }

    public static ProcResult scalar(Object value) {
        return new ProcResult(Kind.SCALAR, value, Collections.emptyList(), 0);
    }

    public static ProcResult rows(List<Map<String, Object>> rows) {
        return new ProcResult(Kind.ROWS, null, Collections.unmodifiableList(rows), rows.size());
    }

    public Kind getKind() { return kind; }

    public Object getScalar() { return scalar; }

    public List<Map<String, Object>> getRows() { return rows; }

    public int getRowsAffected() { return rowsAffected; }

    public boolean isEmpty() {
        return kind == Kind.VOID || (kind == Kind.ROWS && rows.isEmpty());
    }
}
