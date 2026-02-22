package io.falcondb.jdbc.ha;

import io.falcondb.jdbc.protocol.FalconSQLException;
import io.falcondb.jdbc.protocol.WireFormat;
import org.junit.Test;
import static org.junit.Assert.*;

public class FailoverRetryPolicyTest {

    @Test
    public void testDefaultPolicy() {
        FailoverRetryPolicy policy = FailoverRetryPolicy.defaultPolicy();
        assertEquals(3, policy.getMaxRetries());
        assertEquals(100, policy.getBaseBackoffMs());
        assertFalse(policy.isRetryReadsOnly());
    }

    @Test
    public void testReadOnlyPolicy() {
        FailoverRetryPolicy policy = FailoverRetryPolicy.readOnlyPolicy();
        assertTrue(policy.isRetryReadsOnly());
    }

    @Test
    public void testShouldFailoverOnFencedEpoch() {
        FailoverRetryPolicy policy = FailoverRetryPolicy.defaultPolicy();
        FalconSQLException e = new FalconSQLException("epoch mismatch", "F0001",
            WireFormat.ERR_FENCED_EPOCH, true);
        assertTrue(policy.shouldFailover(e));
    }

    @Test
    public void testShouldFailoverOnNotLeader() {
        FailoverRetryPolicy policy = FailoverRetryPolicy.defaultPolicy();
        FalconSQLException e = new FalconSQLException("not leader", "F0002",
            WireFormat.ERR_NOT_LEADER, true);
        assertTrue(policy.shouldFailover(e));
    }

    @Test
    public void testShouldNotFailoverOnSyntaxError() {
        FailoverRetryPolicy policy = FailoverRetryPolicy.defaultPolicy();
        FalconSQLException e = new FalconSQLException("bad sql", "42601",
            WireFormat.ERR_SYNTAX_ERROR, false);
        assertFalse(policy.shouldFailover(e));
    }

    @Test
    public void testShouldRetryWithinMaxRetries() {
        FailoverRetryPolicy policy = FailoverRetryPolicy.defaultPolicy();
        FalconSQLException e = new FalconSQLException("fenced", "F0001",
            WireFormat.ERR_FENCED_EPOCH, true);
        assertTrue(policy.shouldRetry(0, true, e));
        assertTrue(policy.shouldRetry(1, true, e));
        assertTrue(policy.shouldRetry(2, true, e));
        assertFalse(policy.shouldRetry(3, true, e)); // exceeds max
    }

    @Test
    public void testReadOnlyPolicyRejectsWriteRetry() {
        FailoverRetryPolicy policy = FailoverRetryPolicy.readOnlyPolicy();
        FalconSQLException e = new FalconSQLException("fenced", "F0001",
            WireFormat.ERR_FENCED_EPOCH, true);
        assertTrue(policy.shouldRetry(0, true, e));   // read-only: ok
        assertFalse(policy.shouldRetry(0, false, e));  // write: rejected
    }

    @Test
    public void testBackoffExponential() {
        FailoverRetryPolicy policy = new FailoverRetryPolicy(5, 100, false);
        long b0 = policy.backoffMs(0);
        long b1 = policy.backoffMs(1);
        long b2 = policy.backoffMs(2);
        // Exponential: base * 2^attempt (+ jitter)
        assertTrue(b0 >= 100 && b0 <= 150);
        assertTrue(b1 >= 200 && b1 <= 300);
        assertTrue(b2 >= 400 && b2 <= 600);
    }
}
