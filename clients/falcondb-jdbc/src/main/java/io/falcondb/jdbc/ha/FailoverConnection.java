package io.falcondb.jdbc.ha;

import io.falcondb.jdbc.protocol.FalconSQLException;
import io.falcondb.jdbc.protocol.NativeConnection;
import io.falcondb.jdbc.protocol.WireFormat;

import java.io.Closeable;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HA-aware connection wrapper that auto-reconnects on FENCED_EPOCH / NOT_LEADER.
 * Wraps a {@link NativeConnection} and transparently retries on failover errors.
 */
public class FailoverConnection implements Closeable {

    private static final Logger LOG = Logger.getLogger(FailoverConnection.class.getName());

    private final ClusterTopologyProvider topology;
    private final PrimaryResolver resolver;
    private final FailoverRetryPolicy retryPolicy;
    private final String database;
    private final String user;
    private final String password;
    private final int connectTimeoutMs;

    private volatile NativeConnection conn;
    private volatile String currentHost;
    private volatile int currentPort;

    public FailoverConnection(ClusterTopologyProvider topology, String database,
                              String user, String password, int connectTimeoutMs,
                              FailoverRetryPolicy retryPolicy) throws IOException {
        this.topology = topology;
        this.resolver = new PrimaryResolver(topology);
        this.retryPolicy = retryPolicy;
        this.database = database;
        this.user = user;
        this.password = password;
        this.connectTimeoutMs = connectTimeoutMs;

        connectToPrimary();
    }

    public FailoverConnection(ClusterTopologyProvider topology, String database,
                              String user, String password, int connectTimeoutMs) throws IOException {
        this(topology, database, user, password, connectTimeoutMs, FailoverRetryPolicy.defaultPolicy());
    }

    /**
     * Execute a query with automatic failover retry.
     */
    public NativeConnection.QueryResult executeQuery(String sql, int sessionFlags) throws IOException {
        return executeWithRetry(sql, sessionFlags, !isWriteStatement(sql));
    }

    /**
     * Send a ping to the current connection.
     */
    public boolean ping(int timeoutMs) {
        NativeConnection c = conn;
        if (c == null || c.isClosed()) return false;
        try {
            return c.ping(timeoutMs);
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Get the underlying native connection (may be null if disconnected).
     */
    public NativeConnection getConnection() {
        return conn;
    }

    public boolean isClosed() {
        NativeConnection c = conn;
        return c == null || c.isClosed();
    }

    @Override
    public void close() throws IOException {
        NativeConnection c = conn;
        conn = null;
        if (c != null) {
            c.close();
        }
    }

    public String getCurrentHost() { return currentHost; }
    public int getCurrentPort() { return currentPort; }
    public long getServerEpoch() {
        NativeConnection c = conn;
        return c != null ? c.getServerEpoch() : 0;
    }

    // ── Internal ─────────────────────────────────────────────────────

    private NativeConnection.QueryResult executeWithRetry(String sql, int sessionFlags,
                                                          boolean isReadOnly) throws IOException {
        IOException lastError = null;
        for (int attempt = 0; attempt <= retryPolicy.getMaxRetries(); attempt++) {
            try {
                NativeConnection c = ensureConnected();
                return c.executeQuery(sql, sessionFlags);
            } catch (FalconSQLException e) {
                if (retryPolicy.shouldRetry(attempt, isReadOnly, e)) {
                    LOG.log(Level.WARNING, "Failover retry {0}/{1} for error: {2}",
                        new Object[]{attempt + 1, retryPolicy.getMaxRetries(), e.getMessage()});

                    // Update topology from error info
                    if (e.getNativeErrorCode() == WireFormat.ERR_FENCED_EPOCH) {
                        resolver.invalidate();
                    }

                    // Mark current node as failed
                    topology.markFailed(currentHost, currentPort);
                    closeQuietly();

                    // Backoff before retry
                    if (!retryPolicy.backoff(attempt)) {
                        throw e; // interrupted
                    }

                    // Reconnect to new primary
                    try {
                        connectToPrimary();
                    } catch (IOException reconnectErr) {
                        lastError = reconnectErr;
                        continue;
                    }
                } else {
                    throw e;
                }
            } catch (IOException e) {
                lastError = e;
                // Connection-level failure — try reconnect
                topology.markFailed(currentHost, currentPort);
                closeQuietly();

                if (attempt < retryPolicy.getMaxRetries()) {
                    if (!retryPolicy.backoff(attempt)) break;
                    try {
                        connectToPrimary();
                    } catch (IOException reconnectErr) {
                        lastError = reconnectErr;
                    }
                }
            }
        }
        throw lastError != null ? lastError : new IOException("All retry attempts exhausted");
    }

    private void connectToPrimary() throws IOException {
        ClusterTopologyProvider.NodeInfo node = resolver.resolve();
        if (node == null) {
            throw new IOException("No primary node available in topology");
        }
        connectTo(node.host, node.port);
    }

    private void connectTo(String host, int port) throws IOException {
        LOG.log(Level.INFO, "Connecting to {0}:{1}", new Object[]{host, port});
        this.conn = new NativeConnection(host, port, database, user, password, connectTimeoutMs);
        this.currentHost = host;
        this.currentPort = port;
        LOG.log(Level.INFO, "Connected to {0}:{1} epoch={2} nodeId={3}",
            new Object[]{host, port, conn.getServerEpoch(), conn.getServerNodeId()});
    }

    private NativeConnection ensureConnected() throws IOException {
        NativeConnection c = conn;
        if (c == null || c.isClosed()) {
            connectToPrimary();
            c = conn;
        }
        if (c == null) throw new IOException("Not connected");
        return c;
    }

    private void closeQuietly() {
        NativeConnection c = conn;
        conn = null;
        if (c != null) {
            try { c.close(); } catch (IOException ignored) {}
        }
    }

    private static boolean isWriteStatement(String sql) {
        String upper = sql.trim().toUpperCase();
        return upper.startsWith("INSERT") || upper.startsWith("UPDATE")
            || upper.startsWith("DELETE") || upper.startsWith("CREATE")
            || upper.startsWith("DROP") || upper.startsWith("ALTER")
            || upper.startsWith("TRUNCATE");
    }
}
