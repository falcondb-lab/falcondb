package io.falcondb.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P0: Basic JDBC connection and simple query tests.
 */
@Timeout(30)
public class ConnectionTest {

    @Test
    void connectWithDriverManager() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", TestConfig.user());
        props.setProperty("password", TestConfig.password());
        try (Connection conn = DriverManager.getConnection(TestConfig.jdbcUrl(), props)) {
            assertNotNull(conn);
            assertFalse(conn.isClosed());
        }
    }

    @Test
    void selectOne() throws Exception {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void selectLiteral() throws Exception {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 AS num, 'hello' AS txt")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("num"));
            assertEquals("hello", rs.getString("txt"));
            assertFalse(rs.next());
        }
    }

    @Test
    void multipleStatementsInSequence() throws Exception {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            for (int i = 0; i < 5; i++) {
                try (ResultSet rs = stmt.executeQuery("SELECT " + i + " AS val")) {
                    assertTrue(rs.next());
                    assertEquals(i, rs.getInt(1));
                }
            }
        }
    }

    @Test
    void connectionMetadata() throws Exception {
        try (Connection conn = getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            assertNotNull(meta);
            assertNotNull(meta.getDatabaseProductName());
            assertNotNull(meta.getDatabaseProductVersion());
        }
    }

    static Connection getConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", TestConfig.user());
        props.setProperty("password", TestConfig.password());
        return DriverManager.getConnection(TestConfig.jdbcUrl(), props);
    }
}
