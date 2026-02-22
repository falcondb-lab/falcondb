# FalconDB JDBC Driver — Compatibility Matrix

## Driver Version: 0.1.0-SNAPSHOT

### Protocol Compatibility

| Feature | Protocol Minor 0 | Protocol Minor 1 |
|---------|-----------------|-----------------|
| Handshake + Auth | ✅ | ✅ |
| Query (SELECT/DML/DDL) | ✅ | ✅ |
| Ping/Pong | ✅ | ✅ |
| Disconnect | ✅ | ✅ |
| Batch Ingest | ❌ | ✅ |
| Epoch Fencing | ❌ | ✅ |
| Binary Params | ❌ | ✅ |
| Pipeline | ❌ | ✅ (client-side) |
| Compression (LZ4) | ❌ | ❌ (planned) |
| TLS | ❌ | ❌ (planned) |

### JDBC Interface Coverage

| Interface | Status | Notes |
|-----------|--------|-------|
| `java.sql.Driver` | ✅ Full | URL parsing, SPI registration |
| `java.sql.Connection` | ✅ Core | `createStatement`, `prepareStatement`, `commit`, `rollback`, `close`, `isValid` |
| `java.sql.Statement` | ✅ Core | `executeQuery`, `executeUpdate`, `execute` |
| `java.sql.PreparedStatement` | ✅ Core | Client-side parameter binding, `executeBatch` |
| `java.sql.ResultSet` | ✅ Core | Forward-only, read-only, all getter types |
| `java.sql.ResultSetMetaData` | ✅ Full | Column name, type, nullable, precision, scale |
| `javax.sql.DataSource` | ✅ Full | HikariCP-compatible properties |
| `java.sql.DatabaseMetaData` | ❌ | Not yet implemented |
| `java.sql.CallableStatement` | ❌ | Not supported |
| `java.sql.Savepoint` | ❌ | Not yet implemented |

### HikariCP Compatibility

| Feature | Status |
|---------|--------|
| `Connection.isValid(timeout)` | ✅ Uses native Ping/Pong |
| `Connection.getNetworkTimeout()` | ✅ |
| `Connection.setNetworkTimeout()` | ✅ |
| `DataSource.getConnection()` | ✅ |
| `DataSource.getConnection(user, pass)` | ✅ |
| Connection pool properties | ✅ `host`, `port`, `database`, `user`, `password`, `connectTimeout` |

### HA / Failover

| Feature | Status | Notes |
|---------|--------|-------|
| `ClusterTopologyProvider` | ✅ | Seed nodes, primary tracking, stale detection |
| `PrimaryResolver` | ✅ | TTL-cached primary resolution |
| `FailoverRetryPolicy` | ✅ | Configurable retries, exponential backoff, read-only mode |
| `FailoverConnection` | ✅ | Auto-reconnect on FENCED_EPOCH / NOT_LEADER |
| pgjdbc fallback | ✅ | Via `fallback=pgjdbc&fallbackUrl=...` URL params |

### Error Handling

| Error Code | Name | Retryable | Driver Action |
|-----------|------|-----------|---------------|
| 1000 | SYNTAX_ERROR | ❌ | Throw `SQLException` |
| 1001 | INVALID_PARAM | ❌ | Throw `SQLException` |
| 2000 | NOT_LEADER | ✅ | Failover to new primary |
| 2001 | FENCED_EPOCH | ✅ | Refresh topology, reconnect |
| 2002 | READ_ONLY | ✅ | Route to primary |
| 2003 | SERIALIZATION_CONFLICT | ✅ | Retry transaction |
| 3000 | INTERNAL_ERROR | ❌ | Throw `SQLException` |
| 3001 | TIMEOUT | ✅ | Retry with backoff |
| 3002 | OVERLOADED | ✅ | Retry with backoff |
| 4000 | AUTH_FAILED | ❌ | Throw `SQLException` |
| 4001 | PERMISSION_DENIED | ❌ | Throw `SQLException` |

### Java Version Support

| Java Version | Status |
|-------------|--------|
| Java 11+ | ✅ Supported (target) |
| Java 8 | ❌ Not tested |
| Java 17+ | ✅ Compatible |

### Known Limitations

1. **Client-side parameter binding**: Parameters are bound by string substitution, not server-side prepared statements. SQL injection risk if used incorrectly — always use `PreparedStatement`.
2. **No streaming ResultSet**: All rows are materialized in memory.
3. **No DatabaseMetaData**: Schema introspection not yet available.
4. **No LOB support**: BLOB/CLOB not supported; use BYTEA/TEXT instead.
5. **Forward-only ResultSet**: No scrollable or updatable result sets.
