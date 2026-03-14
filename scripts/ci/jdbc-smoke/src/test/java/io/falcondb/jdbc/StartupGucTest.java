package io.falcondb.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P0-3: Verify pgjdbc startup GUC SET/SHOW commands work correctly.
 * pgjdbc sends SET extra_float_digits, SET application_name, etc. on connect.
 */
@Timeout(30)
public class StartupGucTest {

    @Test
    void showServerVersion() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW server_version")) {
            assertTrue(rs.next());
            String version = rs.getString(1);
            assertNotNull(version);
            assertFalse(version.isEmpty());
        }
    }

    @Test
    void showClientEncoding() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW client_encoding")) {
            assertTrue(rs.next());
            assertEquals("UTF8", rs.getString(1));
        }
    }

    @Test
    void showDateStyle() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW DateStyle")) {
            assertTrue(rs.next());
            String ds = rs.getString(1);
            assertNotNull(ds);
            assertTrue(ds.contains("ISO"), "DateStyle should contain ISO, got: " + ds);
        }
    }

    @Test
    void setExtraFloatDigits() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement()) {
            // pgjdbc sends this on connect — must not throw
            stmt.execute("SET extra_float_digits = 3");
            try (ResultSet rs = stmt.executeQuery("SHOW extra_float_digits")) {
                assertTrue(rs.next());
                assertEquals("3", rs.getString(1));
            }
        }
    }

    @Test
    void setApplicationName() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SET application_name = 'jdbc-smoke-test'");
            try (ResultSet rs = stmt.executeQuery("SHOW application_name")) {
                assertTrue(rs.next());
                assertEquals("jdbc-smoke-test", rs.getString(1));
            }
        }
    }

    @Test
    void setClientEncodingUtf8() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SET client_encoding TO 'UTF8'");
            try (ResultSet rs = stmt.executeQuery("SHOW client_encoding")) {
                assertTrue(rs.next());
                assertEquals("UTF8", rs.getString(1));
            }
        }
    }

    @Test
    void setDateStyleIso() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SET DateStyle TO 'ISO'");
        }
    }

    @Test
    void showTransactionIsolation() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW transaction_isolation")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void showIntegerDatetimes() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW integer_datetimes")) {
            assertTrue(rs.next());
            assertEquals("on", rs.getString(1));
        }
    }

    @Test
    void showStandardConformingStrings() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW standard_conforming_strings")) {
            assertTrue(rs.next());
            assertEquals("on", rs.getString(1));
        }
    }

    @Test
    void unknownSetDoesNotCrash() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement()) {
            // Unknown SET should succeed (stored as GUC) — not crash
            stmt.execute("SET some_unknown_var = 'value'");
        }
    }
}
