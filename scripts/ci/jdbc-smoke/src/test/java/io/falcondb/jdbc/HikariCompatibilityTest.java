package io.falcondb.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * P3-1: HikariCP connection pool compatibility.
 */
@Timeout(30)
public class HikariCompatibilityTest {

    @Test
    void hikariPoolBasic() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(TestConfig.jdbcUrl());
        config.setUsername(TestConfig.user());
        config.setPassword(TestConfig.password());
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(5000);

        try (HikariDataSource ds = new HikariDataSource(config)) {
            // Get a connection from the pool
            try (Connection conn = ds.getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }

            // Get another — may reuse the pooled one
            try (Connection conn = ds.getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 2")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    @Test
    void hikariIsValid() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(TestConfig.jdbcUrl());
        config.setUsername(TestConfig.user());
        config.setPassword(TestConfig.password());
        config.setMaximumPoolSize(2);
        config.setConnectionTimeout(5000);

        try (HikariDataSource ds = new HikariDataSource(config)) {
            try (Connection conn = ds.getConnection()) {
                assertTrue(conn.isValid(5));
            }
        }
    }

    @Test
    void hikariMultipleConcurrent() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(TestConfig.jdbcUrl());
        config.setUsername(TestConfig.user());
        config.setPassword(TestConfig.password());
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(5000);

        try (HikariDataSource ds = new HikariDataSource(config)) {
            Thread[] threads = new Thread[5];
            boolean[] results = new boolean[5];
            for (int i = 0; i < 5; i++) {
                final int idx = i;
                threads[i] = new Thread(() -> {
                    try (Connection conn = ds.getConnection();
                         Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT " + idx)) {
                        rs.next();
                        results[idx] = (rs.getInt(1) == idx);
                    } catch (Exception e) {
                        results[idx] = false;
                    }
                });
                threads[i].start();
            }
            for (Thread t : threads) t.join();
            for (int i = 0; i < 5; i++) {
                assertTrue(results[i], "Thread " + i + " failed");
            }
        }
    }
}
