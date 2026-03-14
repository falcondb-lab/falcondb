package io.falcondb.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P0: Basic DDL tests (CREATE TABLE / DROP TABLE).
 */
@Timeout(30)
public class DdlTest {

    @Test
    void createAndDropTable() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS ddl_smoke");
            stmt.execute("CREATE TABLE ddl_smoke (id INT PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO ddl_smoke VALUES (1, 'test')");
            try (ResultSet rs = stmt.executeQuery("SELECT name FROM ddl_smoke WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("test", rs.getString(1));
            }
            stmt.execute("DROP TABLE ddl_smoke");
        }
    }

    @Test
    void createTableWithMultipleTypes() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS ddl_types");
            stmt.execute("CREATE TABLE ddl_types (" +
                    "id INT PRIMARY KEY, " +
                    "name TEXT, " +
                    "score FLOAT, " +
                    "active BOOLEAN, " +
                    "big_id BIGINT)");
            stmt.execute("INSERT INTO ddl_types VALUES (1, 'test', 1.5, true, 9999999999)");
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM ddl_types WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("test", rs.getString("name"));
                assertEquals(1.5, rs.getDouble("score"), 0.01);
                assertTrue(rs.getBoolean("active"));
                assertEquals(9999999999L, rs.getLong("big_id"));
            }
            stmt.execute("DROP TABLE ddl_types");
        }
    }

    @Test
    void dropTableIfExists() throws Exception {
        try (Connection conn = ConnectionTest.getConnection();
             Statement stmt = conn.createStatement()) {
            // Should not throw even if table doesn't exist
            stmt.execute("DROP TABLE IF EXISTS nonexistent_table_xyz");
        }
    }
}
