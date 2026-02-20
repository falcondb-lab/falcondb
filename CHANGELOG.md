# Changelog

All notable changes to FalconDB are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- `--print-default-config` CLI flag for config schema discovery
- E2E two-node failover scripts (Linux + Windows)
- CI failover gate with P0/P1 tiered testing
- Windows developer setup script (`scripts/setup_windows.ps1`)
- Protocol compatibility matrix documentation
- SHOW falcon.* stable output schema documentation
- RPO/RTO analysis and durability policy documentation
- Detailed roadmap with acceptance criteria per milestone
- CONTRIBUTING.md, CODEOWNERS, issue/PR templates
- CI badge, MSRV badge, supported platforms table in README
- SQL coverage and "Not Supported" sections in README

### Changed
- CI workflow: added `failover-gate` (P0/P1 split) and `windows` jobs
- Roadmap: M3 status updated from "Planned" to "Complete"
- Test count updated to 1,081 (from 837) in ARCHITECTURE.md

### Fixed
- `.gitattributes` enforces LF for all text files (eliminates CRLF noise)
- 18 files normalized from CRLF to LF

---

## [0.3.0] — 2026-02-20

### Added — M3: Production Hardening
- Read-only replica enforcement (`FalconError::ReadOnly`, SQLSTATE `25006`)
- Graceful shutdown with configurable drain timeout
- Health check HTTP server (`/health`, `/ready`, `/status`)
- Statement timeout (`SET statement_timeout`, SQLSTATE `57014`)
- Connection limits (`max_connections`, SQLSTATE `53300`)
- Connection idle timeout
- TLS/SSL support (SSLRequest → handshake with cert/key config)
- Query plan cache (LRU, `SHOW falcon.plan_cache`)
- Slow query log (`SET log_min_duration_statement`, `SHOW falcon.slow_queries`)
- Information schema virtual tables (tables, columns, schemata, constraints)
- Views (CREATE/DROP VIEW, CTE-based expansion)
- ALTER TABLE RENAME (column + table)

---

## [0.2.0] — 2026-01-15

### Added — M2: gRPC WAL Streaming
- gRPC WAL streaming via tonic (`SubscribeWal` server-streaming RPC)
- `ReplicaRunner` with exponential backoff and auto-reconnect
- `GetCheckpoint` RPC for new replica bootstrap
- Replica ack tracking (`applied_lsn`, lag monitoring)
- Multi-node CLI flags (`--role`, `--grpc-addr`, `--primary-endpoint`)
- Durability policies: `local-fsync`, `quorum-ack`, `all-ack`
- WAL backpressure (admission control on backlog + lag)
- Replication log capacity with auto-eviction
- `ReplicaRunnerMetrics.lag_lsn` for observability
- Example TOML configs (`examples/primary.toml`, `examples/replica.toml`)

---

## [0.1.0] — 2025-12-01

### Added — M1: Stable OLTP Foundation
- In-memory MVCC storage engine (VersionChain, MemTable, DashMap indexes)
- WAL persistence (segmented, CRC32 checksums, group commit, fdatasync)
- Transaction manager (LocalTxn fast-path OCC+SI, GlobalTxn slow-path 2PC)
- SQL frontend (DDL/DML/SELECT, CTEs, window functions, subqueries, set ops)
- PG wire protocol (simple + extended query, COPY, auth Trust/MD5/SCRAM)
- In-process WAL replication (ShardReplicaGroup, catch-up, promote)
- 5-step fencing failover protocol
- MVCC garbage collection (background GcRunner, safepoint, replica-safe)
- YCSB benchmark harness (fast-path comparison, scale-out, failover)
- Observability (SHOW falcon.*, Prometheus metrics, structured tracing)
- 500+ PG-compatible scalar functions
- JSONB type with operators and functions
- Recursive CTEs
- FK cascading actions (CASCADE, SET NULL, SET DEFAULT)
- Sequence functions (CREATE/DROP SEQUENCE, nextval/currval/setval, SERIAL)
- Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, etc.)
- DATE type with arithmetic and functions
- COPY command (FROM STDIN, TO STDOUT, text/CSV formats)
- EXPLAIN / EXPLAIN ANALYZE
- Snapshot checkpoint with WAL segment purge
- Table statistics collector (ANALYZE TABLE, SHOW falcon.table_stats)

---

## Release Process

### Tagging a Release

```bash
# Update version in Cargo.toml (workspace.package.version)
# Update this CHANGELOG (move Unreleased items to new version section)
git add -A && git commit -m "release: v0.x.0"
git tag v0.x.0
git push origin main --tags
```

### GitHub Release

1. Push the tag: `git push origin v0.x.0`
2. GitHub Actions builds and tests automatically
3. Create a GitHub Release from the tag
4. Copy the relevant CHANGELOG section as release notes

### Version Strategy

- **v0.x.0**: milestone releases (M1, M2, M3, ...)
- **v0.x.y**: patch releases (bug fixes within a milestone)
- **v1.0.0**: first production-ready release (TBD)
