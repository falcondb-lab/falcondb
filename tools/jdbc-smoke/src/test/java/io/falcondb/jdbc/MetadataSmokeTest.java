package io.falcondb.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P2-1: ResultSetMetaData correctness tests.
 * ORM frameworks (MyBatis, Hibernate) rely on metadata from RowDescription.
 */
@Timeout(30)
public class MetadataSmokeTest {

    private Connection conn;

    @BeforeEach
    void setUp() throws Exception {
        conn = ConnectionTest.getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS meta_test");
            stmt.execute("CREATE TABLE meta_test (id INT PRIMARY KEY, name TEXT, score FLOAT, active BOOLEAN)");
            stmt.execute("INSERT INTO meta_test VALUES (1, 'Alice', 95.5, true)");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS meta_test");
            }
            conn.close();
        }
    }

    @Test
    void resultSetMetaDataColumnCount() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, name, score, active FROM meta_test")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(4, meta.getColumnCount());
        }
    }

    @Test
    void resultSetMetaDataColumnNames() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, name, score, active FROM meta_test")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals("id", meta.getColumnName(1).toLowerCase());
            assertEquals("name", meta.getColumnName(2).toLowerCase());
            assertEquals("score", meta.getColumnName(3).toLowerCase());
            assertEquals("active", meta.getColumnName(4).toLowerCase());
        }
    }

    @Test
    void resultSetMetaDataColumnTypes() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, name, score, active FROM meta_test")) {
            ResultSetMetaData meta = rs.getMetaData();
            // id: int4 → Types.INTEGER
            assertEquals(Types.INTEGER, meta.getColumnType(1));
            // name: text → Types.VARCHAR
            assertEquals(Types.VARCHAR, meta.getColumnType(2));
            // score: float8 → Types.DOUBLE
            assertEquals(Types.DOUBLE, meta.getColumnType(3));
            // active: bool → Types.BIT or Types.BOOLEAN
            int boolType = meta.getColumnType(4);
            assertTrue(boolType == Types.BIT || boolType == Types.BOOLEAN,
                    "Expected BIT or BOOLEAN for bool column, got: " + boolType);
        }
    }

    @Test
    void resultSetMetaDataDoesNotThrow() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, name FROM meta_test")) {
            ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                assertDoesNotThrow(() -> meta.getColumnName(i));
                assertDoesNotThrow(() -> meta.getColumnType(i));
                assertDoesNotThrow(() -> meta.getColumnTypeName(i));
                assertDoesNotThrow(() -> meta.getColumnDisplaySize(i));
                assertDoesNotThrow(() -> meta.isNullable(i));
            }
        }
    }

    @Test
    void selectExpressionMetadata() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 AS num, 'hello' AS txt, true AS flag")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(3, meta.getColumnCount());
            assertEquals("num", meta.getColumnLabel(1).toLowerCase());
            assertEquals("txt", meta.getColumnLabel(2).toLowerCase());
            assertEquals("flag", meta.getColumnLabel(3).toLowerCase());
        }
    }
}
