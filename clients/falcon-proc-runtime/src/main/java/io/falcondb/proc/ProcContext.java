package io.falcondb.proc;

import java.sql.Connection;

public class ProcContext {
    private final Connection connection;
    private final String user;
    private final String database;
    private final int timeoutSeconds;
    private final boolean externalTransaction;

    ProcContext(Connection connection, String user, String database, int timeoutSeconds, boolean externalTransaction) {
        this.connection = connection;
        this.user = user;
        this.database = database;
        this.timeoutSeconds = timeoutSeconds;
        this.externalTransaction = externalTransaction;
    }

    public Connection getConnection() {
        return connection;
    }

    public String getUser() {
        return user;
    }

    public String getDatabase() {
        return database;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    /** True if the caller already opened/manages the transaction. Proc must NOT commit/rollback. */
    public boolean isExternalTransaction() {
        return externalTransaction;
    }
}
