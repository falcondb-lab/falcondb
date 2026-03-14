package io.falcondb.jdbc.ha;

import io.falcondb.jdbc.protocol.FalconSQLException;
import io.falcondb.jdbc.protocol.WireFormat;

/**
 * Retry policy for HA-aware failover.
 * Determines whether a failed operation should be retried on a new primary.
 */
public class FailoverRetryPolicy {

    private final int maxRetries;
    private final long baseBackoffMs;
    private final boolean retryReadsOnly;

    public FailoverRetryPolicy(int maxRetries, long baseBackoffMs, boolean retryReadsOnly) {
        this.maxRetries = maxRetries;
        this.baseBackoffMs = baseBackoffMs;
        this.retryReadsOnly = retryReadsOnly;
    }

    /**
     * Default policy: 3 retries, 100ms base backoff, retry reads + idempotent writes.
     */
    public static FailoverRetryPolicy defaultPolicy() {
        return new FailoverRetryPolicy(3, 100, false);
    }

    /**
     * Conservative policy: only retry read-only queries.
     */
    public static FailoverRetryPolicy readOnlyPolicy() {
        return new FailoverRetryPolicy(3, 100, true);
    }

    /**
     * Determine if the given error should trigger a failover attempt.
     */
    public boolean shouldFailover(FalconSQLException e) {
        int code = e.getNativeErrorCode();
        return code == WireFormat.ERR_NOT_LEADER
            || code == WireFormat.ERR_FENCED_EPOCH
            || code == WireFormat.ERR_READ_ONLY;
    }

    /**
     * Determine if the operation should be retried after failover.
     *
     * @param attempt     current attempt number (0-based)
     * @param isReadOnly  whether the operation is read-only
     * @param e           the error that triggered failover
     */
    public boolean shouldRetry(int attempt, boolean isReadOnly, FalconSQLException e) {
        if (attempt >= maxRetries) return false;
        if (!shouldFailover(e)) return false;
        if (retryReadsOnly && !isReadOnly) return false;
        return true;
    }

    /**
     * Compute backoff delay for the given attempt (exponential with jitter).
     */
    public long backoffMs(int attempt) {
        long delay = baseBackoffMs * (1L << Math.min(attempt, 5));
        // Add ~25% jitter
        long jitter = (long) (delay * 0.25 * Math.random());
        return delay + jitter;
    }

    /**
     * Sleep for the backoff duration. Returns false if interrupted.
     */
    public boolean backoff(int attempt) {
        try {
            Thread.sleep(backoffMs(attempt));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public int getMaxRetries() { return maxRetries; }
    public long getBaseBackoffMs() { return baseBackoffMs; }
    public boolean isRetryReadsOnly() { return retryReadsOnly; }
}
