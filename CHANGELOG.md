# Changelog

All notable changes to FalconDB are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added — v2.0 Phase 3: Enterprise Edition Features
- Row-Level Security (RLS): `RlsPolicyManager` with permissive/restrictive policies, role-scoped targeting, superuser bypass (`falcon_common::rls`, 15 tests)
- Transparent Data Encryption (TDE): `KeyManager` with PBKDF2 key derivation, DEK lifecycle, master key rotation (`falcon_storage::encryption`, 11 tests)
- Table Partitioning: Range/Hash/List strategies with routing, pruning, attach/detach (`falcon_storage::partition`, 10 tests)
- Point-in-Time Recovery (PITR): WAL archiving, base backups, restore points, coordinated replay (`falcon_storage::pitr`, 10 tests)
- Change Data Capture (CDC): Replication slots, INSERT/UPDATE/DELETE/COMMIT events, bounded ring buffer (`falcon_storage::cdc`, 9 tests)

### Added — Storage Hardening (7 modules, 61 tests)
- Graded WAL error types with `WalReadError` and `CorruptionLog` (`falcon_storage::storage_error`, 6 tests)
- Phased WAL recovery: Scan→Apply→Validate with `RecoveryModeGuard` (`falcon_storage::recovery`, 10 tests)
- Resource-isolated compaction scheduling with `IoRateLimiter` and priority queue (`falcon_storage::compaction_scheduler`, 11 tests)
- Unified memory budget: 5 categories, 3 escalation levels (Soft/Hard/Emergency) (`falcon_storage::memory_budget`, 10 tests)
- GC safepoint unification with long-txn diagnostics and `LongTxnPolicy` (`falcon_storage::gc_safepoint`, 10 tests)
- Offline diagnostic tools: `sst_verify`, `sst_dump`, `wal_inspect`, `wal_replay_dry_run` (`falcon_storage::storage_tools`, 6 tests)
- Storage fault injection: 6 fault types with probabilistic triggering (`falcon_storage::storage_fault_injection`, 8 tests)
- CI storage gate script (`scripts/ci_storage_gate.sh`)

### Added — Distributed Hardening (6 modules, 62 tests)
- Global epoch/fencing token with `EpochGuard` and `WriteToken` RAII proof (`falcon_cluster::epoch`, 13 tests)
- Raft-managed cluster state machine with `ConsistentClusterState` (`falcon_cluster::consistent_state`, 8 tests)
- Quorum/lease-driven leader authority with `LeaderLease` (`falcon_cluster::leader_lease`, 10 tests)
- Shard migration state machine: Preparing→Copying→CatchingUp→Cutover→Completed (`falcon_cluster::migration`, 10 tests)
- Cross-shard txn throttling with Queue/Reject policy (`falcon_cluster::cross_shard_throttle`, 7 tests)
- Unified control-plane supervisor with `DistributedSupervisor` (`falcon_cluster::supervisor`, 9 tests)
- 8 new Prometheus metric functions for distributed observability
- CI distributed chaos script (`scripts/ci_distributed_chaos.sh`)

### Added — FalconDB Native Protocol
- `falcon_protocol_native` crate: binary protocol encode/decode for 17 message types, LZ4-style compression, type mapping (39 tests)
- `falcon_native_server` crate: TCP server, session state machine, executor bridge, nonce anti-replay tracker (28 tests)
- Protocol specification: `docs/native_protocol.md` (framing, handshake, auth, query, batch, error codes)
- Protocol compatibility matrix: `docs/native_protocol_compat.md` (version negotiation, feature flags)
- Golden test vectors: `tools/native-proto-spec/vectors/golden_vectors.json` (23 vectors)

### Added — Java JDBC Driver (`clients/falcondb-jdbc/`)
- JDBC interfaces: `FalconDriver`, `FalconConnection`, `FalconStatement`, `FalconPreparedStatement`, `FalconResultSet`, `FalconResultSetMetaData`, `FalconDataSource`
- Native protocol client: `WireFormat`, `NativeConnection`, `FalconSQLException`
- HA-aware failover: `ClusterTopologyProvider`, `PrimaryResolver`, `FailoverRetryPolicy`, `FailoverConnection`
- HikariCP compatibility: `isValid(ping)`, `getNetworkTimeout`/`setNetworkTimeout`, `DataSource` properties
- SPI registration: `META-INF/services/java.sql.Driver`
- JDBC URL format: `jdbc:falcondb://host:port/database`
- Driver compatibility matrix: `clients/falcondb-jdbc/COMPAT_MATRIX.md`

### Added — CI Gate Scripts
- `scripts/ci_native_jdbc_smoke.sh` — Rust + Java compile, clippy, test
- `scripts/ci_native_perf_gate.sh` — Release build + protocol performance regression
- `scripts/ci_native_failover_under_load.sh` — Epoch fencing + failover bench

### Test Count
- **2,239 tests** passing, **0 failures** (was 1,976 at 1.0.0-rc.1)

---

## [1.0.0-rc.1] — 2026-02-21

### Added — v1.0 Phase 1: Industrial OLTP Kernel
- LSM storage engine: WAL → MemTable → Flush → L0 → Leveled Compaction
- SST file format with data blocks, index block, bloom filter, and footer
- Per-SST bloom filter for negative-lookup elimination
- LRU block cache with configurable budget and eviction metrics
- MVCC value encoding (txn_id / status / commit_ts / data) with visibility rules
- Persistent `TxnMetaStore` for 2PC crash recovery
- Idempotency key store with TTL and background GC
- Transaction-level audit log (txn_id, affected keys, outcome, epoch)
- TPC-B (`--tpcb`) and LSM KV (`--lsm`) benchmark workloads with P50/P95/P99/Max latency
- CI Phase 1 gates script (`scripts/ci_phase1_gates.sh`)

### Added — v1.0 Phase 2: SQL Completeness & Enterprise Kernel
- `Datum::Decimal(i128, u8)` / `DataType::Decimal(u8, u8)` with full arithmetic and PG wire support
- Composite, covering, and prefix secondary indexes with backfill on creation
- CHECK constraint runtime enforcement (INSERT / UPDATE, SQLSTATE `23514`)
- Transaction `READ ONLY` / `READ WRITE` mode, per-txn timeout, execution summary counters
- `GovernorAbortReason` enum for structured query abort reasons
- Fine-grained admission control: `OperationType` enum, `DdlPermit` RAII guard
- `NodeOperationalMode` (Normal / ReadOnly / Drain) with event-logged transitions
- `RoleCatalog` with transitive role inheritance and circular-dependency detection
- `PrivilegeManager` with GRANT / REVOKE, effective-role resolution, schema default privileges
- Native `DataType::Time`, `DataType::Interval`, `DataType::Uuid` with PG OID mapping

### Added — Release Engineering & Hardening
- `--print-default-config` CLI flag for config schema discovery
- E2E two-node failover scripts (Linux + Windows)
- CI failover gate with P0/P1 tiered testing
- Performance regression CI gate (`scripts/ci_perf_regression_gate.sh`)
- Per-crate code audit report (`docs/crate_audit_report.md`)
- ARCHITECTURE.md split: extended scalar functions moved to `docs/extended_scalar_functions.md`
- e2e chaos & failover `Makefile` targets with structured evidence output
- Backpressure stability benchmark harness (`scripts/bench_backpressure.sh`)
- 2PC fault-injection state-machine tests (`cross_shard_chaos` module)
- RBAC full-path enforcement test matrix (protocol / SQL / internal paths)
- CONTRIBUTING.md, CODEOWNERS, issue/PR templates
- CI badge, MSRV badge, supported platforms table in README

### Changed
- `workspace.version` bumped from `0.1.0` → `1.0.0-rc.1` (aligned with roadmap narrative)
- CI workflow: added `failover-gate` (P0/P1 split) and `windows` jobs
- `slow_query_log.rs`: `Vec` → `VecDeque` for O(1) ring-buffer eviction
- `TenantRegistry`: race-free atomic tenant ID generation (`alloc_tenant_id()`)
- `handler.rs`: `projection_to_field` now bounds-checks column index (no panic on binder bug)
- `deadlock.rs`: production-path `unwrap()` replaced with safe fallbacks + error logging
- `manager.rs`: `TxnState::Committed` set only AFTER `storage.commit_txn()` confirms
- `manager.rs`: `LatencyRecorder` capped at 100K samples per bucket (prevents unbounded growth)
- `infer_func_type`: `CurrentTime` returns `DataType::Time` (was stale `DataType::Text`)

### Fixed
- `.gitattributes` enforces LF for all text files (eliminates CRLF noise)
- 18 files normalized from CRLF to LF
- `SYSTEM_TENANT_ID` import missing in `tenant_registry.rs` test module

### Test Count
- **1,976 tests** passing, **0 failures** (was 1,081 at v0.1.0)

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
