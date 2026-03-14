package io.falcondb.jdbc;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for FalconDataSource property management.
 * These tests validate bean properties only — no live connection required.
 */
public class FalconDataSourceTest {

    @Test
    public void testDefaultProperties() {
        FalconDataSource ds = new FalconDataSource();
        assertEquals("localhost", ds.getHost());
        assertEquals(15433, ds.getPort());
        assertEquals("", ds.getDatabase());
        assertEquals("", ds.getUser());
        assertEquals("", ds.getPassword());
        assertEquals(5000, ds.getConnectTimeout());
        assertFalse(ds.getSsl());
        assertFalse(ds.getSslTrustAll());
        assertEquals("", ds.getFallback());
        assertEquals("", ds.getFallbackUrl());
    }

    @Test
    public void testConstructorWithHostPortDb() {
        FalconDataSource ds = new FalconDataSource("myhost", 9999, "testdb");
        assertEquals("myhost", ds.getHost());
        assertEquals(9999, ds.getPort());
        assertEquals("testdb", ds.getDatabase());
    }

    @Test
    public void testSslProperties() {
        FalconDataSource ds = new FalconDataSource();
        assertFalse(ds.getSsl());
        assertFalse(ds.getSslTrustAll());

        ds.setSsl(true);
        assertTrue(ds.getSsl());

        ds.setSslTrustAll(true);
        assertTrue(ds.getSslTrustAll());

        ds.setSsl(false);
        assertFalse(ds.getSsl());
    }

    @Test
    public void testAllPropertySetters() {
        FalconDataSource ds = new FalconDataSource();
        ds.setHost("db.example.com");
        ds.setPort(5433);
        ds.setDatabase("production");
        ds.setUser("admin");
        ds.setPassword("secret");
        ds.setConnectTimeout(10000);
        ds.setSsl(true);
        ds.setSslTrustAll(false);
        ds.setFallback("pgjdbc");
        ds.setFallbackUrl("jdbc:postgresql://fallback/db");

        assertEquals("db.example.com", ds.getHost());
        assertEquals(5433, ds.getPort());
        assertEquals("production", ds.getDatabase());
        assertEquals("admin", ds.getUser());
        assertEquals("secret", ds.getPassword());
        assertEquals(10000, ds.getConnectTimeout());
        assertTrue(ds.getSsl());
        assertFalse(ds.getSslTrustAll());
        assertEquals("pgjdbc", ds.getFallback());
        assertEquals("jdbc:postgresql://fallback/db", ds.getFallbackUrl());
    }

    @Test
    public void testLoginTimeout() {
        FalconDataSource ds = new FalconDataSource();
        assertEquals(0, ds.getLoginTimeout());
        ds.setLoginTimeout(30);
        assertEquals(30, ds.getLoginTimeout());
    }

    @Test
    public void testLogWriter() {
        FalconDataSource ds = new FalconDataSource();
        assertNull(ds.getLogWriter());
        java.io.PrintWriter pw = new java.io.PrintWriter(System.out);
        ds.setLogWriter(pw);
        assertSame(pw, ds.getLogWriter());
    }

    @Test
    public void testIsWrapperFor() throws Exception {
        FalconDataSource ds = new FalconDataSource();
        assertTrue(ds.isWrapperFor(FalconDataSource.class));
        assertTrue(ds.isWrapperFor(javax.sql.DataSource.class));
        assertFalse(ds.isWrapperFor(String.class));
    }

    @Test
    public void testUnwrap() throws Exception {
        FalconDataSource ds = new FalconDataSource();
        FalconDataSource unwrapped = ds.unwrap(FalconDataSource.class);
        assertSame(ds, unwrapped);
    }

    @Test(expected = java.sql.SQLException.class)
    public void testUnwrapInvalid() throws Exception {
        FalconDataSource ds = new FalconDataSource();
        ds.unwrap(String.class);
    }
}
