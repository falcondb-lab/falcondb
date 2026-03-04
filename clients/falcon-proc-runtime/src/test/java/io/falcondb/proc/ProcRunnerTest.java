package io.falcondb.proc;

import org.junit.Before;
import org.junit.Test;
import org.h2.jdbcx.JdbcDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ProcRunnerTest {

    private JdbcDataSource ds;
    private ProcedureRegistry registry;

    @Before
    public void setUp() throws SQLException {
        ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        ds.setUser("sa");

        try (Connection conn = ds.getConnection()) {
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS accounts (" +
                "  id BIGINT PRIMARY KEY," +
                "  balance DOUBLE NOT NULL" +
                ")"
            );
            conn.createStatement().execute("DELETE FROM accounts");
            conn.createStatement().execute("INSERT INTO accounts VALUES (1, 1000.0)");
            conn.createStatement().execute("INSERT INTO accounts VALUES (2, 500.0)");
        }

        registry = new ProcedureRegistry();
        registry.register("transfer_funds", new io.falcondb.proc.example.Proc_transfer_funds());
    }

    @Test
    public void testTransferSuccess() throws ProcException {
        ProcRunner runner = ProcRunner.builder(registry).dataSource(ds).build();
        ProcResult result = runner.call("transfer_funds", 1L, 2L, 200.0);

        assertEquals(ProcResult.Kind.VOID, result.getKind());
        assertEquals(2, result.getRowsAffected());

        // verify balances
        try (Connection conn = ds.getConnection()) {
            Double bal1 = Sql.queryScalar(conn, "SELECT balance FROM accounts WHERE id = 1")
                    .map(o -> ((Number) o).doubleValue()).orElse(null);
            Double bal2 = Sql.queryScalar(conn, "SELECT balance FROM accounts WHERE id = 2")
                    .map(o -> ((Number) o).doubleValue()).orElse(null);
            assertEquals(800.0, bal1, 0.001);
            assertEquals(700.0, bal2, 0.001);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testTransferInsufficientFunds() {
        ProcRunner runner = ProcRunner.builder(registry).dataSource(ds).build();
        try {
            runner.call("transfer_funds", 1L, 2L, 9999.0);
            fail("Expected ProcException");
        } catch (ProcException e) {
            assertEquals("P0001", e.getSqlState());
            assertTrue(e.getMessage().contains("Insufficient funds"));
        }
    }

    @Test
    public void testProcNotFound() {
        ProcRunner runner = ProcRunner.builder(registry).dataSource(ds).build();
        try {
            runner.call("no_such_proc");
            fail("Expected ProcException");
        } catch (ProcException e) {
            assertEquals("42883", e.getSqlState());
        }
    }

    @Test
    public void testExternalTransaction() throws ProcException, SQLException {
        ProcRunner runner = ProcRunner.builder(registry).dataSource(ds).build();

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            runner.call(conn, "transfer_funds", 1L, 2L, 100.0);
            conn.rollback(); // caller rolls back
        }

        // balances should be unchanged
        try (Connection conn = ds.getConnection()) {
            Double bal1 = Sql.queryScalar(conn, "SELECT balance FROM accounts WHERE id = 1")
                    .map(o -> ((Number) o).doubleValue()).orElse(null);
            assertEquals(1000.0, bal1, 0.001);
        }
    }

    @Test
    public void testSqlQueryList() throws ProcException, SQLException {
        try (Connection conn = ds.getConnection()) {
            List<Map<String, Object>> rows = Sql.queryList(conn, "SELECT id, balance FROM accounts ORDER BY id");
            assertEquals(2, rows.size());
            assertEquals(1L, ((Number) rows.get(0).get("ID")).longValue());
        }
    }
}
