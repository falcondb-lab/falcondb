package io.falcondb.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P1: PreparedStatement (extended query protocol) tests.
 * pgjdbc uses Parse/Bind/Describe/Execute/Sync under the hood.
 */
@Timeout(30)
public class PreparedStatementTest {

    private Connection conn;

    @BeforeEach
    void setUp() throws Exception {
        conn = ConnectionTest.getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS ps_test");
            stmt.execute("CREATE TABLE ps_test (id INT PRIMARY KEY, name TEXT, active BOOLEAN)");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS ps_test");
            }
            conn.close();
        }
    }

    @Test
    void selectWithIntParam() throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("SELECT $1::int AS val")) {
            ps.setInt(1, 42);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt("val"));
            }
        }
    }

    @Test
    void selectWithTextParam() throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("SELECT $1::text AS val")) {
            ps.setString(1, "hello");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("hello", rs.getString("val"));
            }
        }
    }

    @Test
    void insertWithParams() throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO ps_test (id, name, active) VALUES ($1, $2, $3)")) {
            ps.setInt(1, 1);
            ps.setString(2, "Alice");
            ps.setBoolean(3, true);
            int rows = ps.executeUpdate();
            assertEquals(1, rows);
        }

        // Verify the insert
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, name, active FROM ps_test WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertTrue(rs.getBoolean("active"));
        }
    }

    @Test
    void updateWithParams() throws Exception {
        // Insert first
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO ps_test (id, name, active) VALUES (1, 'Bob', true)");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE ps_test SET name = $1 WHERE id = $2")) {
            ps.setString(1, "Robert");
            ps.setInt(2, 1);
            int rows = ps.executeUpdate();
            assertEquals(1, rows);
        }

        // Verify
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM ps_test WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("Robert", rs.getString("name"));
        }
    }

    @Test
    void deleteWithParams() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO ps_test (id, name, active) VALUES (1, 'X', true)");
            stmt.execute("INSERT INTO ps_test (id, name, active) VALUES (2, 'Y', false)");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM ps_test WHERE id = $1")) {
            ps.setInt(1, 1);
            int rows = ps.executeUpdate();
            assertEquals(1, rows);
        }

        // Verify only id=2 remains
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM ps_test")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void batchInsert() throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO ps_test (id, name, active) VALUES ($1, $2, $3)")) {
            for (int i = 1; i <= 10; i++) {
                ps.setInt(1, i);
                ps.setString(2, "User" + i);
                ps.setBoolean(3, i % 2 == 0);
                ps.addBatch();
            }
            int[] results = ps.executeBatch();
            assertEquals(10, results.length);
        }

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM ps_test")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
        }
    }

    @Test
    void reuseWithDifferentParams() throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("SELECT $1::int + $2::int AS sum")) {
            ps.setInt(1, 10);
            ps.setInt(2, 20);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(30, rs.getInt("sum"));
            }

            ps.setInt(1, 100);
            ps.setInt(2, 200);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(300, rs.getInt("sum"));
            }
        }
    }
}
