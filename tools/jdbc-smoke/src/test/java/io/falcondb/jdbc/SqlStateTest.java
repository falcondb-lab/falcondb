package io.falcondb.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P2-2: Verify ErrorResponse SQLSTATE codes are correct for common errors.
 * Spring/MyBatis depend on these for exception translation.
 */
@Timeout(30)
public class SqlStateTest {

    @Test
    void syntaxErrorSqlState() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.executeQuery("SELEC 1"));
            assertNotNull(ex.getSQLState(), "SQLSTATE should not be null");
            // 42601 = syntax_error or 42000 class
            assertTrue(ex.getSQLState().startsWith("42"),
                    "Syntax error should be class 42, got: " + ex.getSQLState());
        }
    }

    @Test
    void undefinedTableSqlState() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.executeQuery("SELECT * FROM no_such_table_xyz"));
            assertNotNull(ex.getSQLState());
            // 42P01 = undefined_table (class 42)
            assertTrue(ex.getSQLState().startsWith("42"),
                    "Undefined table should be class 42, got: " + ex.getSQLState());
        }
    }

    @Test
    void errorResponseHasMessage() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.executeQuery("INVALID SQL GARBAGE"));
            assertNotNull(ex.getMessage());
            assertFalse(ex.getMessage().isEmpty());
        }
    }

    @Test
    void errorDoesNotBreakConnection() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement()) {
            // First: cause an error
            assertThrows(SQLException.class,
                    () -> stmt.executeQuery("SELEC 1"));
            // Then: connection should still be usable
            try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }
}
