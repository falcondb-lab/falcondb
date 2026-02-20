# Protocol Compatibility Self-Test Matrix

FalconDB implements the PostgreSQL wire protocol (v3). This document defines
the compatibility test matrix for common PG clients and tools.

---

## Quick Smoke Test

```bash
# Start FalconDB
cargo run -p falcon_server -- --no-wal

# In another terminal
psql -h 127.0.0.1 -p 5433 -U falcon -c "SELECT 1 AS ok;"
```

---

## Client Matrix

| Client | Version | Auth: Trust | Auth: MD5 | Auth: SCRAM-SHA-256 | Status |
|--------|---------|:-----------:|:---------:|:-------------------:|--------|
| **psql** | 14+ | ✅ | ✅ | ✅ | Tested |
| **psql** | 12–13 | ✅ | ✅ | ✅ | Tested |
| **pgbench** | 14+ | ✅ | ✅ | ⚠️ init only | See notes |
| **JDBC** (pgjdbc 42.x) | 42.7+ | ✅ | ✅ | ✅ | Tested |
| **npgsql** (.NET) | 8.x | ✅ | ✅ | ✅ | Untested |
| **psycopg2** (Python) | 2.9+ | ✅ | ✅ | ✅ | Untested |
| **pgx / sqlx** (Rust) | latest | ✅ | ✅ | ✅ | Untested |
| **DBeaver** | 24.x | ✅ | ✅ | ✅ | Partial |

---

## Authentication Test Procedures

### Trust (no auth)

```toml
# falcon.toml
[server.auth]
method = "trust"
```

```bash
psql -h 127.0.0.1 -p 5433 -U anyuser -c "SELECT 1;"
# Expected: succeeds with any username, no password prompt
```

### MD5

```toml
[server.auth]
method = "md5"
username = "falcon"
password = "secret123"
```

```bash
PGPASSWORD=secret123 psql -h 127.0.0.1 -p 5433 -U falcon -c "SELECT 1;"
# Expected: succeeds

PGPASSWORD=wrong psql -h 127.0.0.1 -p 5433 -U falcon -c "SELECT 1;"
# Expected: FATAL: password authentication failed
```

### SCRAM-SHA-256

```toml
[server.auth]
method = "scram-sha-256"
username = "falcon"
password = "secret123"
```

```bash
PGPASSWORD=secret123 psql -h 127.0.0.1 -p 5433 -U falcon -c "SELECT 1;"
# Expected: succeeds (psql 10+ required)

PGPASSWORD=wrong psql -h 127.0.0.1 -p 5433 -U falcon -c "SELECT 1;"
# Expected: FATAL: password authentication failed
```

---

## pgbench Test

```bash
# Initialize (requires CREATE TABLE support)
pgbench -h 127.0.0.1 -p 5433 -U falcon -i -s 1

# Run TPC-B-like workload (5 clients, 10 seconds)
pgbench -h 127.0.0.1 -p 5433 -U falcon -c 5 -T 10
```

**Known limitations:**
- `pgbench -i` requires: `CREATE TABLE`, `INSERT`, `COPY`, `TRUNCATE`, `VACUUM` (VACUUM is a no-op)
- `pgbench` built-in scripts use `BEGIN`/`UPDATE`/`SELECT`/`INSERT`/`END` — all supported
- Custom scripts (`-f`) work if they use supported SQL

---

## JDBC Test

```java
// Minimal JDBC connection test
import java.sql.*;

public class FalconJdbcTest {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://127.0.0.1:5433/falcon";
        try (Connection conn = DriverManager.getConnection(url, "falcon", "")) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 AS result");
            rs.next();
            System.out.println("Result: " + rs.getInt("result"));
            // Expected: Result: 1
        }
    }
}
```

---

## Wire Protocol Feature Coverage

| Feature | Supported | Notes |
|---------|:---------:|-------|
| Simple query | ✅ | Single-statement and multi-statement |
| Extended query (Parse/Bind/Execute) | ✅ | Prepared statements + portals |
| COPY IN (stdin) | ✅ | Text and CSV formats |
| COPY OUT (stdout) | ✅ | Text and CSV formats |
| SSL/TLS handshake | ✅ | Responds 'S' when TLS configured, 'N' otherwise |
| Cancel request | ❌ | Accepted but not acted upon (M3) |
| Notification (LISTEN/NOTIFY) | ❌ | Not implemented |
| Streaming replication protocol | ❌ | Uses custom gRPC instead |

---

## Running the Full Matrix

```bash
# Auth: Trust
cargo run -p falcon_server -- --no-wal &
psql -h 127.0.0.1 -p 5433 -U falcon -c "SELECT 'trust_ok';"
kill %1

# Auth: MD5 (requires config file)
cat > /tmp/falcon_md5.toml << 'EOF'
[server]
pg_listen_addr = "0.0.0.0:5433"
admin_listen_addr = "0.0.0.0:8080"
node_id = 1
max_connections = 128
[server.auth]
method = "md5"
username = "falcon"
password = "test123"
[storage]
memory_limit_bytes = 0
wal_enabled = false
data_dir = "/tmp/falcon_md5_data"
[wal]
group_commit = true
flush_interval_us = 1000
sync_mode = "fdatasync"
segment_size_bytes = 67108864
EOF

cargo run -p falcon_server -- -c /tmp/falcon_md5.toml &
PGPASSWORD=test123 psql -h 127.0.0.1 -p 5433 -U falcon -c "SELECT 'md5_ok';"
kill %1
```
