package io.falcondb.jdbc;

import org.junit.Test;
import static org.junit.Assert.*;

public class FalconDriverTest {

    @Test
    public void testAcceptsUrl() {
        FalconDriver driver = new FalconDriver();
        assertTrue(driver.acceptsURL("jdbc:falcondb://localhost:15433/mydb"));
        assertTrue(driver.acceptsURL("jdbc:falcondb://host/db?user=u&password=p"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost/db"));
        assertFalse(driver.acceptsURL(null));
    }

    @Test
    public void testParseUrlFull() throws Exception {
        FalconDriver.ParsedUrl p = FalconDriver.parseUrl(
            "jdbc:falcondb://myhost:9999/testdb?user=admin&password=secret&connectTimeout=3000");
        assertEquals("myhost", p.host);
        assertEquals(9999, p.port);
        assertEquals("testdb", p.database);
        assertEquals("admin", p.user);
        assertEquals("secret", p.password);
        assertEquals(3000, p.connectTimeout);
    }

    @Test
    public void testParseUrlDefaults() throws Exception {
        FalconDriver.ParsedUrl p = FalconDriver.parseUrl("jdbc:falcondb://localhost/mydb");
        assertEquals("localhost", p.host);
        assertEquals(15433, p.port);
        assertEquals("mydb", p.database);
        assertEquals("", p.user);
        assertEquals("", p.password);
        assertEquals(5000, p.connectTimeout);
    }

    @Test
    public void testParseUrlNoDatabase() throws Exception {
        FalconDriver.ParsedUrl p = FalconDriver.parseUrl("jdbc:falcondb://host:1234");
        assertEquals("host", p.host);
        assertEquals(1234, p.port);
        assertEquals("", p.database);
    }

    @Test
    public void testParseUrlEmptyHost() throws Exception {
        FalconDriver.ParsedUrl p = FalconDriver.parseUrl("jdbc:falcondb:///mydb");
        assertEquals("localhost", p.host);
        assertEquals(15433, p.port);
        assertEquals("mydb", p.database);
    }

    @Test
    public void testParseUrlWithFallback() throws Exception {
        FalconDriver.ParsedUrl p = FalconDriver.parseUrl(
            "jdbc:falcondb://host/db?fallback=pgjdbc&fallbackUrl=jdbc:postgresql://host/db");
        assertEquals("pgjdbc", p.params.get("fallback"));
        assertEquals("jdbc:postgresql://host/db", p.params.get("fallbackUrl"));
    }

    @Test
    public void testDriverVersion() {
        FalconDriver driver = new FalconDriver();
        assertEquals(0, driver.getMajorVersion());
        assertEquals(1, driver.getMinorVersion());
        assertFalse(driver.jdbcCompliant());
    }
}
