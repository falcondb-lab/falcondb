package io.falcondb.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P0: Transaction behaviour tests (autoCommit on/off, commit, rollback).
 */
@Timeout(30)
public class TransactionTest {

    private Connection conn;

    @BeforeEach
    void setUp() throws Exception {
        conn = ConnectionTest.getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS txn_test");
            stmt.execute("CREATE TABLE txn_test (id INT PRIMARY KEY, val TEXT)");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) {
            try {
                conn.setAutoCommit(true);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS txn_test");
                }
            } catch (Exception ignored) {}
            conn.close();
        }
    }

    @Test
    void autoCommitTrueByDefault() throws Exception {
        assertTrue(conn.getAutoCommit());
    }

    @Test
    void autoCommitInsertVisible() throws Exception {
        assertTrue(conn.getAutoCommit());
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO txn_test (id, val) VALUES (1, 'auto')");
        }
        // Should be visible immediately in a new statement
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT val FROM txn_test WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("auto", rs.getString(1));
        }
    }

    @Test
    void manualCommit() throws Exception {
        conn.setAutoCommit(false);
        assertFalse(conn.getAutoCommit());

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO txn_test (id, val) VALUES (1, 'committed')");
        }
        conn.commit();

        // Verify data persisted after commit
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT val FROM txn_test WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("committed", rs.getString(1));
        }
    }

    @Test
    void manualRollback() throws Exception {
        conn.setAutoCommit(false);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO txn_test (id, val) VALUES (1, 'rolled_back')");
        }
        conn.rollback();

        // Data should not be visible after rollback
        conn.setAutoCommit(true);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM txn_test")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    void setReadOnlyDoesNotCrash() throws Exception {
        // pgjdbc calls setReadOnly — must not throw even if no-op
        conn.setReadOnly(true);
        assertTrue(conn.isReadOnly());
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
        }
        conn.setReadOnly(false);
        assertFalse(conn.isReadOnly());
    }

    @Test
    void multipleCommitCycles() throws Exception {
        conn.setAutoCommit(false);
        for (int i = 1; i <= 3; i++) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO txn_test (id, val) VALUES (" + i + ", 'v" + i + "')");
            }
            conn.commit();
        }
        conn.setAutoCommit(true);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM txn_test")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }
}
