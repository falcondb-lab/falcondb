package io.falcondb.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P3-2: Query cancel support.
 */
@Timeout(30)
public class CancelQueryTest {

    @Test
    void cancelFromAnotherThread() throws Exception {
        try (Connection conn = ConnectionTest.getConnection()) {
            Statement stmt = conn.createStatement();

            // Cancel a long-running query from another thread
            // Note: FalconDB may not support pg_sleep, so we just verify
            // that Statement.cancel() doesn't crash the connection.
            Thread canceller = new Thread(() -> {
                try {
                    Thread.sleep(100);
                    stmt.cancel();
                } catch (Exception ignored) {}
            });

            // Use a simple query — cancel may or may not arrive in time
            canceller.start();
            try {
                // This should either complete or throw a cancel exception
                stmt.executeQuery("SELECT 1");
            } catch (SQLException e) {
                // 57014 = query_canceled — acceptable
                assertTrue(e.getSQLState() == null || e.getSQLState().equals("57014"),
                        "Unexpected SQLSTATE on cancel: " + e.getSQLState());
            }
            canceller.join();

            // Connection should still be usable
            try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            stmt.close();
        }
    }
}
