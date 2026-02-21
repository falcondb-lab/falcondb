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

## [0.9.0] — 2026-02-21

### Added — Release Engineering (Production Candidate)
- WAL segment header: every new segment starts with `FALC` magic + format version (8-byte header)
- `WAL_SEGMENT_HEADER_SIZE` constant and backward-compatible reader (legacy headerless segments still readable)
- `DeprecatedFieldChecker`: backward-compatible config schema with 6 deprecated field mappings
  - `[cedar]` → `[server]`, `cedar_data_dir` → `storage.data_dir`, `wal.sync` → `wal.sync_mode`
  - `replication.master_endpoint` → `replication.primary_endpoint`, `replication.slave_mode` → `replication.role`
  - `storage.max_memory` → `memory.node_limit_bytes`
- `SHOW falcon.wire_compat`: version, WAL format, snapshot format, min compatible version
- `docs/wire_compatibility.md`: comprehensive wire/WAL/snapshot/config compatibility policy
- Prometheus metrics: `falcon_compat_wal_format_version`, `falcon_compat_snapshot_format_version`

### Changed
- WAL writer: writes segment header on new segment creation and rotation
- WAL reader: auto-detects and skips segment header on read
- Roadmap v0.9.0: all deliverables marked ✅

---

## [0.8.0] — 2026-02-21

### Added — Chaos Engineering Coverage
- Network partition simulation: `partition_nodes()`, `heal_partition()`, `can_communicate()`
- CPU/IO jitter injection: `JitterConfig` with presets (light/heavy/cpu_only/io_only/disabled)
- `FaultInjectorSnapshot`: combined snapshot of all fault state (base + partition + jitter)
- `ChaosScenario::NetworkPartition` and `ChaosScenario::CpuIoJitter` variants
- `SHOW falcon.fault_injection`: 16-row display (base + partition.* + jitter.*)
- Prometheus metrics: `falcon_chaos_{faults_fired, partition_active, partition_count, partition_heal_count, partition_events, jitter_enabled, jitter_events}`
- 15 new fault injection tests (6 partition + 7 jitter + 2 combined)

---

## [0.7.0] — 2026-02-21

### Added — Deterministic 2PC Transactions
- `CoordinatorDecisionLog`: durable commit decisions with WAL-backed persistence
- `LayeredTimeoutController`: soft/hard timeouts with configurable policy (FailFast/BestEffort)
- `SlowShardTracker`: slow shard detection with hedged requests and fast-abort
- `SHOW falcon.two_phase_config`: decision log, timeout, slow-shard policy metrics
- Prometheus metrics: `falcon_2pc_{decision_log_*, soft_timeouts, hard_timeouts, shard_timeouts, slow_shard_*}`

---

## [0.6.0] — 2026-02-21

### Added — Tail Latency Governance & Backpressure
- `PriorityScheduler`: three-lane priority queue (High/Normal/Low) with backpressure
- `TokenBucket`: rate limiter for DDL/backfill/rebalance operations
- `SHOW falcon.priority_scheduler` and `SHOW falcon.token_bucket`
- Prometheus metrics: `falcon_scheduler_*` (13 gauges), `falcon_token_bucket_*` (8 gauges with labels)

---

## [0.5.0] — 2026-02-21

### Added — Operationally Usable
- `ClusterAdmin`: scale-out/in state machines, leader transfer, rebalance plan
- `ClusterEventLog`: structured event log for all cluster state transitions
- `local_cluster_harness.sh`: 3-node start/stop/failover/smoke test
- `rolling_upgrade_smoke.sh`: rolling upgrade verification
- `ops_playbook.md`: scale-out/in, failover, rolling upgrade procedures
- `SHOW falcon.{admission, hotspots, verification, latency_contract, cluster_events, node_lifecycle, rebalance_plan}`

---

## [0.4.0] — 2026-02-20

### Added — Production Hardening Alpha
- `FalconError` unified error model with `ErrorKind`, SQLSTATE mapping, retry hints
- `bail_user!` / `bail_retryable!` / `bail_transient!` macros
- `ErrorContext` trait (`.ctx()` / `.ctx_with()`)
- `install_panic_hook()` + `catch_request()` + `PanicThrottle` crash domain
- `DiagBundle` + `DiagBundleBuilder` + JSON export
- `deny_unwrap.sh`, `ci_production_gate.sh`, `ci_production_gate_v2.sh`
- `docs/error_model.md`, `docs/production_readiness.md`
- Columnstore vectorized aggregation + multi-tenancy + lag-aware routing
- `SHOW falcon.tenants`, `SHOW falcon.tenant_usage`
- Enterprise security: `SecurityManager`, `AuditLog`, `RoleCatalog` with RBAC
- `SHOW falcon.{license, metering, security, health_score, compat, audit_log, sla_stats}`

### Changed
- Product renamed from CedarDB to FalconDB (all code references, package names, metrics, commands)
- Core path unwrap elimination: 0 unwraps in 4 critical crates

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
