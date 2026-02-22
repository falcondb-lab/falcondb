package io.falcondb.jdbc;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * DataSource implementation for FalconDB. Compatible with HikariCP.
 *
 * Required properties: host, port, database, user, password.
 * Optional: connectTimeout, fallback, fallbackUrl.
 */
public class FalconDataSource implements DataSource {

    private String host = "localhost";
    private int port = 15433;
    private String database = "";
    private String user = "";
    private String password = "";
    private int connectTimeout = 5000;
    private String fallback = "";
    private String fallbackUrl = "";
    private PrintWriter logWriter;
    private int loginTimeout = 0;

    public FalconDataSource() {}

    public FalconDataSource(String host, int port, String database) {
        this.host = host;
        this.port = port;
        this.database = database;
    }

    // ── Property setters/getters for pool configuration ──────────────

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public String getDatabase() { return database; }
    public void setDatabase(String database) { this.database = database; }

    public String getUser() { return user; }
    public void setUser(String user) { this.user = user; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    public int getConnectTimeout() { return connectTimeout; }
    public void setConnectTimeout(int connectTimeout) { this.connectTimeout = connectTimeout; }

    public String getFallback() { return fallback; }
    public void setFallback(String fallback) { this.fallback = fallback; }

    public String getFallbackUrl() { return fallbackUrl; }
    public void setFallbackUrl(String fallbackUrl) { this.fallbackUrl = fallbackUrl; }

    // ── DataSource interface ─────────────────────────────────────────

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(user, password);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        String url = String.format("jdbc:falcondb://%s:%d/%s?user=%s&password=%s&connectTimeout=%d",
            host, port, database, username, password, connectTimeout);
        if (!fallback.isEmpty()) url += "&fallback=" + fallback;
        if (!fallbackUrl.isEmpty()) url += "&fallbackUrl=" + fallbackUrl;

        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);

        FalconDriver driver = new FalconDriver();
        Connection conn = driver.connect(url, props);
        if (conn == null) {
            throw new SQLException("Failed to create connection to " + host + ":" + port);
        }
        return conn;
    }

    @Override
    public PrintWriter getLogWriter() { return logWriter; }

    @Override
    public void setLogWriter(PrintWriter out) { this.logWriter = out; }

    @Override
    public void setLoginTimeout(int seconds) { this.loginTimeout = seconds; }

    @Override
    public int getLoginTimeout() { return loginTimeout; }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }
}
