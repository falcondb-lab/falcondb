package io.falcondb.proc.trigger;

import java.sql.Connection;
import java.util.Collections;
import java.util.Map;

/**
 * Represents a single trigger firing event, carrying all context that PG would
 * expose to a trigger function via TG_* variables and NEW/OLD records.
 */
public class TriggerEvent {
    private final TriggerTiming timing;
    private final TriggerOperation operation;
    private final String tableName;
    private final String schemaName;
    private final Map<String, Object> newRow;
    private final Map<String, Object> oldRow;
    private final Connection connection;

    private TriggerEvent(Builder b) {
        this.timing = b.timing;
        this.operation = b.operation;
        this.tableName = b.tableName;
        this.schemaName = b.schemaName;
        this.newRow = b.newRow != null ? Collections.unmodifiableMap(b.newRow) : Collections.emptyMap();
        this.oldRow = b.oldRow != null ? Collections.unmodifiableMap(b.oldRow) : Collections.emptyMap();
        this.connection = b.connection;
    }

    public TriggerTiming getTiming() { return timing; }
    public TriggerOperation getOperation() { return operation; }
    public String getTableName() { return tableName; }
    public String getSchemaName() { return schemaName; }

    /** NEW row — populated for INSERT and UPDATE. */
    public Map<String, Object> getNewRow() { return newRow; }

    /** OLD row — populated for UPDATE and DELETE. */
    public Map<String, Object> getOldRow() { return oldRow; }

    public Connection getConnection() { return connection; }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private TriggerTiming timing;
        private TriggerOperation operation;
        private String tableName;
        private String schemaName = "public";
        private Map<String, Object> newRow;
        private Map<String, Object> oldRow;
        private Connection connection;

        public Builder timing(TriggerTiming t) { this.timing = t; return this; }
        public Builder operation(TriggerOperation op) { this.operation = op; return this; }
        public Builder table(String table) { this.tableName = table; return this; }
        public Builder schema(String schema) { this.schemaName = schema; return this; }
        public Builder newRow(Map<String, Object> row) { this.newRow = row; return this; }
        public Builder oldRow(Map<String, Object> row) { this.oldRow = row; return this; }
        public Builder connection(Connection conn) { this.connection = conn; return this; }

        public TriggerEvent build() {
            if (timing == null || operation == null || tableName == null || connection == null) {
                throw new IllegalStateException("timing, operation, tableName, connection are required");
            }
            return new TriggerEvent(this);
        }
    }
}
