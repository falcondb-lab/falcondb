package io.falcondb.jdbc;

import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.SQLException;

/**
 * Unit tests for FalconPreparedStatement client-side binding logic.
 * These tests do not require a live server connection.
 */
public class FalconPreparedStatementTest {

    // ── bindParameters: basic types ─────────────────────────────────

    @Test
    public void testBindNoParams() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT 1");
        assertEquals("SELECT 1", ps.bindParameters());
    }

    @Test
    public void testBindIntegerParam() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT * FROM t WHERE id = ?");
        ps.setInt(1, 42);
        assertEquals("SELECT * FROM t WHERE id = 42", ps.bindParameters());
    }

    @Test
    public void testBindLongParam() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT * FROM t WHERE id = ?");
        ps.setLong(1, 9999999999L);
        assertEquals("SELECT * FROM t WHERE id = 9999999999", ps.bindParameters());
    }

    @Test
    public void testBindDoubleParam() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT * FROM t WHERE val = ?");
        ps.setDouble(1, 3.14);
        assertEquals("SELECT * FROM t WHERE val = 3.14", ps.bindParameters());
    }

    @Test
    public void testBindStringParam() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT * FROM t WHERE name = ?");
        ps.setString(1, "hello");
        assertEquals("SELECT * FROM t WHERE name = 'hello'", ps.bindParameters());
    }

    @Test
    public void testBindStringWithQuote() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT * FROM t WHERE name = ?");
        ps.setString(1, "it's");
        assertEquals("SELECT * FROM t WHERE name = 'it''s'", ps.bindParameters());
    }

    @Test
    public void testBindNullParam() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT * FROM t WHERE id = ?");
        ps.setNull(1, java.sql.Types.INTEGER);
        assertEquals("SELECT * FROM t WHERE id = NULL", ps.bindParameters());
    }

    @Test
    public void testBindBooleanParam() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT * FROM t WHERE active = ?");
        ps.setBoolean(1, true);
        assertEquals("SELECT * FROM t WHERE active = TRUE", ps.bindParameters());

        ps.setBoolean(1, false);
        assertEquals("SELECT * FROM t WHERE active = FALSE", ps.bindParameters());
    }

    // ── bindParameters: multiple params ─────────────────────────────

    @Test
    public void testBindMultipleParams() throws Exception {
        FalconPreparedStatement ps = createStub(
            "INSERT INTO t (a, b, c) VALUES (?, ?, ?)");
        ps.setInt(1, 1);
        ps.setString(2, "two");
        ps.setDouble(3, 3.0);
        assertEquals("INSERT INTO t (a, b, c) VALUES (1, 'two', 3.0)",
            ps.bindParameters());
    }

    @Test
    public void testBindMixedNullAndValues() throws Exception {
        FalconPreparedStatement ps = createStub(
            "INSERT INTO t (a, b) VALUES (?, ?)");
        ps.setInt(1, 10);
        ps.setNull(2, java.sql.Types.VARCHAR);
        assertEquals("INSERT INTO t (a, b) VALUES (10, NULL)",
            ps.bindParameters());
    }

    // ── clearParameters ─────────────────────────────────────────────

    @Test
    public void testClearParameters() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT * FROM t WHERE id = ?");
        ps.setInt(1, 42);
        ps.clearParameters();
        // After clear, param is null
        assertEquals("SELECT * FROM t WHERE id = NULL", ps.bindParameters());
    }

    // ── setObject dispatching ───────────────────────────────────────

    @Test
    public void testSetObjectString() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT ?");
        ps.setObject(1, "abc");
        assertEquals("SELECT 'abc'", ps.bindParameters());
    }

    @Test
    public void testSetObjectInteger() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT ?");
        ps.setObject(1, Integer.valueOf(7));
        assertEquals("SELECT 7", ps.bindParameters());
    }

    @Test
    public void testSetObjectLong() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT ?");
        ps.setObject(1, Long.valueOf(123456789L));
        assertEquals("SELECT 123456789", ps.bindParameters());
    }

    @Test
    public void testSetObjectBoolean() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT ?");
        ps.setObject(1, Boolean.TRUE);
        assertEquals("SELECT TRUE", ps.bindParameters());
    }

    @Test
    public void testSetObjectNull() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT ?");
        ps.setObject(1, null);
        assertEquals("SELECT NULL", ps.bindParameters());
    }

    @Test
    public void testSetObjectDouble() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT ?");
        ps.setObject(1, Double.valueOf(2.5));
        assertEquals("SELECT 2.5", ps.bindParameters());
    }

    @Test
    public void testSetObjectFloat() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT ?");
        ps.setObject(1, Float.valueOf(1.5f));
        assertEquals("SELECT 1.5", ps.bindParameters());
    }

    // ── parameter index validation ──────────────────────────────────

    @Test(expected = SQLException.class)
    public void testParamIndexTooLow() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT ?");
        ps.setInt(0, 1);
    }

    @Test(expected = SQLException.class)
    public void testParamIndexTooHigh() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT ?");
        ps.setInt(2, 1);
    }

    // ── batch accumulation ──────────────────────────────────────────

    @Test
    public void testAddBatchAccumulates() throws Exception {
        FalconPreparedStatement ps = createStub("INSERT INTO t VALUES (?)");
        ps.setInt(1, 1);
        ps.addBatch();
        ps.setInt(1, 2);
        ps.addBatch();
        ps.setInt(1, 3);
        ps.addBatch();
        // After addBatch, clearBatch should reset
        ps.clearBatch();
        // No exception means batch was cleared
    }

    // ── param count detection ───────────────────────────────────────

    @Test
    public void testParamCountZero() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT 1");
        assertEquals("SELECT 1", ps.bindParameters());
    }

    @Test
    public void testParamCountMultiple() throws Exception {
        FalconPreparedStatement ps = createStub("SELECT ?, ?, ?");
        ps.setInt(1, 1);
        ps.setInt(2, 2);
        ps.setInt(3, 3);
        assertEquals("SELECT 1, 2, 3", ps.bindParameters());
    }

    // ── Helper: create a FalconPreparedStatement without a real connection ──

    /**
     * Creates a FalconPreparedStatement with a null connection for bind-only tests.
     * Only bindParameters() and setter methods are safe to call.
     */
    private static FalconPreparedStatement createStub(String sql) {
        return new FalconPreparedStatement(null, sql);
    }
}
