# ADR-007: PostgreSQL Wire Protocol Compatibility

- **Status**: Accepted
- **Date**: 2024-06-20
- **Authors**: FalconDB Core Team

## Context

FalconDB targets existing PostgreSQL application ecosystems. Clients (psql, JDBC,
application ORMs) expect a server that speaks the PG wire protocol. The question
is how deeply to implement PG compatibility and which protocol version to target.

### Options Considered

| Option | Pros | Cons |
|--------|------|------|
| **PG wire v3 (pgwire crate)** | Drop-in for existing PG clients; MIT crate handles codec | Must implement PG-specific system catalogs, auth, type OIDs |
| **MySQL protocol** | Larger client ecosystem | Different SQL dialect; not our target market |
| **HTTP/REST** | Universal client support; easy debugging | No transaction semantics; not compatible with existing PG tooling |
| **Custom binary protocol** | Optimal performance; full control | No ecosystem; requires custom drivers |

## Decision

Implement **PostgreSQL wire protocol v3** as the primary client interface, using
the `pgwire` crate (MIT) for message codec and framing:

1. **Simple Query**: full support for `Q` message → parse → execute → return
   `RowDescription` + `DataRow` + `CommandComplete`.

2. **Extended Query**: full support for Parse (`P`), Bind (`B`), Describe (`D`),
   Execute (`E`), Sync (`S`), Close (`C`), and Flush (`H`) messages. This
   enables prepared statements and parameterized queries.

3. **Auth**: Trust auth (M1); MD5 and SCRAM-SHA-256 planned for M2.

4. **TLS**: Optional TLS via `tokio-rustls` (SSLRequest negotiation).

5. **Cancel**: PG cancel request protocol (`CancelRequest` with pid + secret key)
   via `AtomicBool` polling in the executor.

6. **System catalog shim**: `pg_catalog` tables for `\dt`, `\d`, `pg_type`,
   `version()`, `current_database()`, `current_schema()`. Sufficient for psql,
   pgAdmin, and JDBC metadata queries.

7. **Native protocol**: a separate high-performance binary protocol
   (`falcon_protocol_native`) is available for FalconDB-to-FalconDB internal
   communication (replication, distributed queries).

## Consequences

### Positive

- Existing PostgreSQL clients (psql, JDBC, Python psycopg2, Go pgx, Node pg,
  .NET Npgsql) connect without modification.
- JDBC compatibility validated via `falcondb-jdbc` smoke tests in CI.
- ORMs (Hibernate, SQLAlchemy, ActiveRecord) work with PG dialect settings.
- Migration from PostgreSQL requires minimal application changes.

### Negative

- Must maintain PG system catalog compatibility as PostgreSQL evolves.
- Some PG-specific features (LISTEN/NOTIFY, COPY, logical replication protocol)
  are not implemented and return appropriate error messages.
- Type OID mapping requires careful maintenance for JDBC type inference.

### Mitigations

- `COMPAT_MATRIX.md` in `clients/falcondb-jdbc/` tracks JDBC feature support.
- `docs/sql_compatibility.md` documents supported vs unsupported SQL features.
- CI includes JDBC smoke tests to catch compatibility regressions.
- Protocol layer is behind `PgCodec` / `PgConnection` abstractions — the entire
  crate is replaceable without touching the execution engine.

## Wire Format Summary

```
Client → Server (Frontend messages):
  Q  (SimpleQuery)     — SQL text
  P  (Parse)           — prepared statement
  B  (Bind)            — parameter binding
  D  (Describe)        — describe statement/portal
  E  (Execute)         — execute portal
  S  (Sync)            — sync point
  C  (Close)           — close statement/portal
  H  (Flush)           — flush output
  X  (Terminate)       — close connection

Server → Client (Backend messages):
  R  (Authentication)  — auth challenge/OK
  K  (BackendKeyData)  — pid + secret for cancel
  T  (RowDescription)  — column metadata
  D  (DataRow)         — row data
  C  (CommandComplete) — "INSERT 0 1", "SELECT 5", etc.
  Z  (ReadyForQuery)   — transaction status (I/T/E)
  E  (ErrorResponse)   — SQLSTATE + message
  1  (ParseComplete)
  2  (BindComplete)
  n  (NoData)
  t  (ParameterDescription)
```

## References

- [PostgreSQL Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)
- ARCHITECTURE.md §2.1 (protocol_pg)
- `crates/falcon_protocol_pg/` — implementation
- `clients/falcondb-jdbc/COMPAT_MATRIX.md` — JDBC compatibility
- `docs/sql_compatibility.md` — SQL feature matrix
