package io.falcondb.proc;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Main entry point for calling migrated procedures.
 *
 * Two usage modes:
 *  1. Pass a DataSource — runner manages connection + transaction lifecycle.
 *  2. Pass a Connection directly — caller manages transaction (external txn mode).
 */
public class ProcRunner {
    private final ProcedureRegistry registry;
    private final DataSource dataSource;
    private final String defaultUser;
    private final String defaultDatabase;
    private final int defaultTimeoutSeconds;

    private ProcRunner(Builder b) {
        this.registry = b.registry;
        this.dataSource = b.dataSource;
        this.defaultUser = b.user;
        this.defaultDatabase = b.database;
        this.defaultTimeoutSeconds = b.timeoutSeconds;
    }

    public static Builder builder(ProcedureRegistry registry) {
        return new Builder(registry);
    }

    /**
     * Execute a procedure by name using a managed connection + auto transaction.
     * Commits on success, rolls back on any exception.
     */
    public ProcResult call(String name, ProcArgs args) throws ProcException {
        if (dataSource == null) {
            throw new ProcException("No DataSource configured; use call(Connection, name, args) instead", "08003");
        }
        try (Connection conn = dataSource.getConnection()) {
            boolean autoCommit = conn.getAutoCommit();
            if (autoCommit) conn.setAutoCommit(false);
            try {
                ProcContext ctx = new ProcContext(conn, defaultUser, defaultDatabase, defaultTimeoutSeconds, false);
                ProcResult result = registry.get(name).execute(ctx, args);
                if (!autoCommit || !conn.getAutoCommit()) conn.commit();
                return result;
            } catch (ProcException e) {
                safeRollback(conn);
                throw e;
            } catch (Exception e) {
                safeRollback(conn);
                throw new ProcException("Unexpected error in procedure: " + name, "XX000", e);
            } finally {
                if (autoCommit) safeSetAutoCommit(conn, true);
            }
        } catch (SQLException e) {
            throw new ProcException("Failed to obtain connection", "08001", e);
        }
    }

    /**
     * Execute a procedure using a caller-supplied Connection.
     * Transaction lifecycle is fully managed by the caller.
     */
    public ProcResult call(Connection conn, String name, ProcArgs args) throws ProcException {
        try {
            ProcContext ctx = new ProcContext(conn, defaultUser, defaultDatabase, defaultTimeoutSeconds, true);
            return registry.get(name).execute(ctx, args);
        } catch (ProcException e) {
            throw e;
        } catch (Exception e) {
            throw new ProcException("Unexpected error in procedure: " + name, "XX000", e);
        }
    }

    /** Convenience: call with positional args. */
    public ProcResult call(String name, Object... args) throws ProcException {
        return call(name, ProcArgs.of(args));
    }

    /** Convenience: call with external connection and positional args. */
    public ProcResult call(Connection conn, String name, Object... args) throws ProcException {
        return call(conn, name, ProcArgs.of(args));
    }

    private void safeRollback(Connection conn) {
        try { conn.rollback(); } catch (SQLException ignored) {}
    }

    private void safeSetAutoCommit(Connection conn, boolean v) {
        try { conn.setAutoCommit(v); } catch (SQLException ignored) {}
    }

    public static class Builder {
        private final ProcedureRegistry registry;
        private DataSource dataSource;
        private String user = "app";
        private String database = "default";
        private int timeoutSeconds = 30;

        private Builder(ProcedureRegistry registry) {
            this.registry = registry;
        }

        public Builder dataSource(DataSource ds) {
            this.dataSource = ds;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder timeout(int seconds) {
            this.timeoutSeconds = seconds;
            return this;
        }

        public ProcRunner build() {
            return new ProcRunner(this);
        }
    }
}
