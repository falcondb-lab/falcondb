package io.falcondb.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * FalconDB JDBC Driver.
 *
 * URL format: jdbc:falcondb://host:port/database?user=...&password=...
 *
 * Optional URL parameters:
 *   fallback=pgjdbc          - fall back to PostgreSQL JDBC on native failure
 *   fallbackUrl=jdbc:postgresql://...  - explicit fallback URL
 *   connectTimeout=5000      - connection timeout in ms
 */
public class FalconDriver implements Driver {

    private static final String URL_PREFIX = "jdbc:falcondb://";
    private static final int DEFAULT_PORT = 15433;

    static {
        try {
            DriverManager.registerDriver(new FalconDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register FalconDB driver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;

        ParsedUrl parsed = parseUrl(url);
        String user = info.getProperty("user", parsed.user);
        String password = info.getProperty("password", parsed.password);
        int connectTimeout = Integer.parseInt(
            info.getProperty("connectTimeout", String.valueOf(parsed.connectTimeout)));

        if (user == null || user.isEmpty()) {
            throw new SQLException("user is required");
        }
        if (password == null) password = "";

        try {
            return new FalconConnection(parsed.host, parsed.port, parsed.database,
                                        user, password, connectTimeout);
        } catch (Exception e) {
            // Fallback to pgjdbc if configured
            String fallback = info.getProperty("fallback", parsed.params.getOrDefault("fallback", ""));
            String fallbackUrl = info.getProperty("fallbackUrl",
                parsed.params.getOrDefault("fallbackUrl", ""));

            if ("pgjdbc".equalsIgnoreCase(fallback) && !fallbackUrl.isEmpty()) {
                try {
                    return DriverManager.getConnection(fallbackUrl, info);
                } catch (SQLException fe) {
                    throw new SQLException("Native connection failed and pgjdbc fallback also failed: "
                        + e.getMessage() + " / " + fe.getMessage(), fe);
                }
            }
            throw new SQLException("Failed to connect to FalconDB at "
                + parsed.host + ":" + parsed.port + ": " + e.getMessage(), e);
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[]{
            new DriverPropertyInfo("user", info.getProperty("user")),
            new DriverPropertyInfo("password", info.getProperty("password")),
            new DriverPropertyInfo("connectTimeout", "5000"),
            new DriverPropertyInfo("fallback", ""),
            new DriverPropertyInfo("fallbackUrl", ""),
        };
    }

    @Override
    public int getMajorVersion() { return 0; }

    @Override
    public int getMinorVersion() { return 1; }

    @Override
    public boolean jdbcCompliant() { return false; }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    // ── URL parsing ──────────────────────────────────────────────────

    static ParsedUrl parseUrl(String url) throws SQLException {
        if (!url.startsWith(URL_PREFIX)) {
            throw new SQLException("Invalid URL: " + url);
        }
        String rest = url.substring(URL_PREFIX.length());

        // Split off query params
        String path;
        java.util.Map<String, String> params = new java.util.LinkedHashMap<>();
        int qIdx = rest.indexOf('?');
        if (qIdx >= 0) {
            path = rest.substring(0, qIdx);
            String query = rest.substring(qIdx + 1);
            for (String pair : query.split("&")) {
                int eq = pair.indexOf('=');
                if (eq > 0) {
                    params.put(pair.substring(0, eq), pair.substring(eq + 1));
                }
            }
        } else {
            path = rest;
        }

        // Parse host:port/database
        String host;
        int port = DEFAULT_PORT;
        String database = "";

        int slashIdx = path.indexOf('/');
        String hostPort;
        if (slashIdx >= 0) {
            hostPort = path.substring(0, slashIdx);
            database = path.substring(slashIdx + 1);
        } else {
            hostPort = path;
        }

        int colonIdx = hostPort.indexOf(':');
        if (colonIdx >= 0) {
            host = hostPort.substring(0, colonIdx);
            port = Integer.parseInt(hostPort.substring(colonIdx + 1));
        } else {
            host = hostPort;
        }

        if (host.isEmpty()) host = "localhost";

        return new ParsedUrl(host, port, database,
            params.getOrDefault("user", ""),
            params.getOrDefault("password", ""),
            Integer.parseInt(params.getOrDefault("connectTimeout", "5000")),
            params);
    }

    static class ParsedUrl {
        final String host;
        final int port;
        final String database;
        final String user;
        final String password;
        final int connectTimeout;
        final java.util.Map<String, String> params;

        ParsedUrl(String host, int port, String database, String user,
                  String password, int connectTimeout, java.util.Map<String, String> params) {
            this.host = host;
            this.port = port;
            this.database = database;
            this.user = user;
            this.password = password;
            this.connectTimeout = connectTimeout;
            this.params = params;
        }
    }
}
