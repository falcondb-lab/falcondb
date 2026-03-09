package io.falcondb.jdbc;

/**
 * Centralised connection config for all JDBC smoke tests.
 * Override via system properties or env vars.
 */
public final class TestConfig {
    private TestConfig() {}

    public static String host() {
        return prop("falcon.host", "FALCON_HOST", "localhost");
    }

    public static int port() {
        return Integer.parseInt(prop("falcon.port", "FALCON_PORT", "5432"));
    }

    public static String database() {
        return prop("falcon.database", "FALCON_DATABASE", "falcon");
    }

    public static String user() {
        return prop("falcon.user", "FALCON_USER", "falcon");
    }

    public static String password() {
        return prop("falcon.password", "FALCON_PASSWORD", "falcon");
    }

    public static String jdbcUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s", host(), port(), database());
    }

    private static String prop(String sysProp, String envVar, String defaultValue) {
        String v = System.getProperty(sysProp);
        if (v != null && !v.isEmpty()) return v;
        v = System.getenv(envVar);
        if (v != null && !v.isEmpty()) return v;
        return defaultValue;
    }
}
