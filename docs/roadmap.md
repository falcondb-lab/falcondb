# FalconDB Roadmap & Milestone Acceptance Criteria

---

## v0.1.0 — M1: Stable OLTP Foundation ✅

**Status**: Released

### Deliverables

| Feature | Acceptance Criteria | Status |
|---------|-------------------|--------|
| MVCC storage engine | VersionChain, MemTable, DashMap indexes | ✅ |
| WAL persistence | Segment rotation, CRC32 checksums, group commit, fdatasync | ✅ |
| Transaction manager | LocalTxn (fast-path OCC+SI), GlobalTxn (slow-path 2PC) | ✅ |
| SQL frontend | DDL/DML/SELECT, CTEs, window functions, subqueries, set ops | ✅ |
| PG wire protocol | Simple + extended query, COPY, auth (Trust/MD5/SCRAM) | ✅ |
| In-process replication | WAL shipping via ShardReplicaGroup, catch-up, promote | ✅ |
| Failover | 5-step fencing protocol, promote semantics | ✅ |
| MVCC GC | Background GcRunner, safepoint, replica-safe | ✅ |
| Benchmarks | YCSB harness, fast-path comparison, scale-out, failover | ✅ |
| Observability | SHOW falcon.*, Prometheus metrics, structured tracing | ✅ |

### Test Coverage

- 1,081 passing tests across 13 crates
- Integration tests for DDL/DML/SELECT end-to-end
- Failover exercise (create → replicate → fence → promote → verify)

### Verification Commands

```bash
cargo test -p falcon_storage          # 226 tests (MVCC, WAL, GC, indexes)
cargo test -p falcon_cluster           # 247 tests (replication, failover, scatter/gather)
cargo test -p falcon_server            # 208 tests (SQL e2e, SHOW, error paths)
cargo test -p falcon_txn               # 53 tests (txn lifecycle, OCC, stats)
cargo test -p falcon_sql_frontend      # 141 tests (parsing, binding, analysis)
cargo test -p falcon_protocol_pg       # 147 tests (PG wire, auth, SHOW commands)
cargo test -p falcon_bench -- --help   # benchmark harness
```

---

## v0.2.0 — M2: gRPC WAL Streaming & Multi-Node 🔄

**Status**: In progress

### Deliverables

| Feature | Acceptance Criteria | Status |
|---------|-------------------|--------|
| gRPC WAL streaming | tonic server/client, `SubscribeWal` server-streaming RPC | ✅ |
| Replica runner | `ReplicaRunner` with exponential backoff, auto-reconnect | ✅ |
| Checkpoint streaming | `GetCheckpoint` RPC for new replica bootstrap | ✅ |
| Ack tracking | Replica reports `applied_lsn`, primary tracks lag | ✅ |
| Multi-node CLI | `--role primary/replica`, `--grpc-addr`, `--primary-endpoint` | ✅ |
| Durability policies | `local-fsync`, `quorum-ack`, `all-ack` config options | ✅ |
| WAL backpressure | Admission control on WAL backlog + replication lag | ✅ |
| Config schema | `--print-default-config`, full TOML schema documented | ✅ |
| Replication log capacity | `max_capacity` eviction, bounded memory growth | ✅ |
| Replica lag metric | `lag_lsn` in ReplicaRunnerMetrics | ✅ |

### Acceptance Gates (must pass before M2 release)

1. **Two-node smoke test**: Primary + Replica start via CLI, data written on
   primary is readable on replica within 2 seconds.
2. **Reconnect resilience**: Kill and restart replica; it reconnects and
   catches up without data loss.
3. **Checkpoint bootstrap**: New replica joins via `GetCheckpoint` RPC,
   receives full snapshot, then switches to WAL streaming.
4. **Failover under load**: pgbench running on primary, promote replica,
   new primary serves reads+writes within 5 seconds.
5. **CI gate**: `scripts/ci_failover_gate.sh` passes with 0 failures.
6. **RPO validation**: Under `quorum-ack`, kill primary after commit — verify
   promoted replica has all committed data.

### Remaining Work

| Task | Priority | Estimate |
|------|----------|----------|
| End-to-end multi-node integration test (psql-based) | P0 | 2 days |
| `quorum-ack` commit path wiring (wait for replica acks) | P0 | 3 days |
| Shard-aware replication (multi-shard per node) | P1 | 3 days |
| Proto file rename: `falcon_replication.proto` | P2 | Done ✅ |
| Documentation: protocol compatibility matrix | P2 | Done ✅ |

---

## v0.3.0 — M3: Production Hardening ✅

**Status**: Complete

### Deliverables

| Feature | Acceptance Criteria | Status | Verify |
|---------|-------------------|--------|--------|
| Read-only replica enforcement | DDL/DML writes rejected on replica (SQLSTATE `25006`) | ✅ | `cargo test -p falcon_server -- read_only` |
| Graceful shutdown | SIGTERM → drain timeout → force close | ✅ | `cargo test -p falcon_protocol_pg -- shutdown` |
| Health checks | HTTP `/health`, `/ready`, `/status` endpoints | ✅ | `cargo test -p falcon_server -- health` |
| Query timeout | `SET statement_timeout`; SQLSTATE `57014` on expiry | ✅ | `cargo test -p falcon_protocol_pg -- statement_timeout` |
| Connection limits | `max_connections` enforced; SQLSTATE `53300` | ✅ | `cargo test -p falcon_protocol_pg -- max_connections` |
| Idle timeout | Idle connections closed after configurable period | ✅ | `cargo test -p falcon_protocol_pg -- idle_timeout` |
| TLS/SSL | SSLRequest → TLS handshake; cert/key config | ✅ | `cargo test -p falcon_protocol_pg -- tls` |
| Cancel request | PG cancel protocol (backend key → kill query) | ⚠️ Partial | Accepted but not acted upon |
| Plan cache | LRU cache with `SHOW falcon.plan_cache` | ✅ | `cargo test -p falcon_protocol_pg -- plan_cache` |
| Slow query log | `SET log_min_duration_statement`, `SHOW falcon.slow_queries` | ✅ | `cargo test -p falcon_protocol_pg -- slow_query` |

### Acceptance Gates

1. All M2 gates still pass.
2. Health endpoint returns 503 during graceful shutdown drain.
3. TLS connection via `psql "sslmode=require"` succeeds (when cert/key configured).

---

## v0.4.0 — M4: Analytics & Multi-Tenancy ✅

**Status**: Complete

### Deliverables

| Feature | Acceptance Criteria | Status | Verify |
|---------|-------------------|--------|--------|
| Columnstore storage | `ColumnStoreTable` with frozen segments, write buffer, `column_scan` | ✅ | `cargo test -p falcon_storage -- columnstore` |
| Analytics node role | `NodeRole::Analytics` enforced; ColumnStore allowed on Analytics/Standalone only | ✅ | `cargo test -p falcon_common -- node_role` |
| `scan_columnar` API | `StorageEngine::scan_columnar()` returns `Vec<Vec<Datum>>` per column | ✅ | `cargo test -p falcon_storage` |
| Vectorized execution | `RecordBatch::from_columns`, `vectorized_filter`, `vectorized_aggregate`, `vectorized_project`, `vectorized_hash_join`, `vectorized_sort` | ✅ | `cargo test -p falcon_executor -- vectorized` |
| Columnar aggregate path | `exec_columnar_aggregate`: COUNT/SUM/AVG/MIN/MAX over ColumnStore bypass row-at-a-time | ✅ | `cargo test -p falcon_protocol_pg -- columnstore` |
| Multi-tenancy: CREATE TENANT | `CREATE TENANT name [MAX_QPS n] [MAX_STORAGE_BYTES n]` SQL command | ✅ | `cargo test -p falcon_protocol_pg -- create_tenant` |
| Multi-tenancy: DROP TENANT | `DROP TENANT name` SQL command with existence check | ✅ | `cargo test -p falcon_protocol_pg -- drop_tenant` |
| Multi-tenancy: SHOW tenants | `SHOW falcon.tenants` — lists all registered tenants | ✅ | `cargo test -p falcon_protocol_pg -- show_falcon_tenants` |
| Multi-tenancy: SHOW tenant_usage | `SHOW falcon.tenant_usage` — per-tenant metrics (txns, QPS, memory) | ✅ | `cargo test -p falcon_protocol_pg -- show_falcon_tenant_usage` |
| Tenant quota enforcement | `TenantRegistry::check_begin_txn` enforces QPS, txn limits, status | ✅ | `cargo test -p falcon_storage -- tenant` |
| Duplicate tenant guard | `register_tenant` rejects duplicate names (case-insensitive) | ✅ | `cargo test -p falcon_protocol_pg -- duplicate` |
| Cross-region lag routing | `DistributedQueryEngine::select_least_lagging_replica` + `route_read_to_replica` | ✅ | `cargo test -p falcon_protocol_pg -- lagging_replica` |
| BoundStatement variants | `ShowTenants`, `ShowTenantUsage`, `CreateTenant`, `DropTenant` in binder/planner/executor | ✅ | `cargo build --workspace` |

### Acceptance Gates

1. **Columnar COUNT(*)**: `SELECT COUNT(*) FROM t` on a ColumnStore table returns correct result via vectorized path (no row-at-a-time scan).
2. **Columnar SUM**: `SELECT SUM(col) FROM t` on a ColumnStore table returns correct aggregate.
3. **CREATE TENANT round-trip**: `CREATE TENANT acme` → `SHOW falcon.tenants` shows `acme` → `DROP TENANT acme` removes it.
4. **Duplicate tenant rejected**: Second `CREATE TENANT acme` returns `ERROR 42710`.
5. **Lag-aware routing**: `select_least_lagging_replica` prefers `lag_lsn=0` over lagging replicas; skips disconnected replicas.
6. **No regressions**: `cargo test --workspace` passes (excluding pre-existing GC benchmark timing flake).

### Verification Commands

```bash
cargo test -p falcon_protocol_pg -- test_columnstore_count_uses_vectorized_path
cargo test -p falcon_protocol_pg -- test_columnstore_sum_uses_vectorized_path
cargo test -p falcon_protocol_pg -- test_create_tenant_basic
cargo test -p falcon_protocol_pg -- test_create_tenant_duplicate_fails
cargo test -p falcon_protocol_pg -- test_drop_tenant_basic
cargo test -p falcon_protocol_pg -- test_show_falcon_tenants
cargo test -p falcon_protocol_pg -- test_show_falcon_tenant_usage
cargo test -p falcon_protocol_pg -- test_select_least_lagging_replica_prefers_caught_up
cargo test -p falcon_protocol_pg -- test_select_least_lagging_replica_skips_disconnected
cargo test -p falcon_protocol_pg -- test_select_least_lagging_replica_empty_returns_none
cargo test --workspace
```

### Implementation Notes

- **Vectorized columnar path** (`executor_columnar.rs`): triggered when table is ColumnStore, query is a pure aggregate (no GROUP BY, no HAVING, no correlated subqueries). Falls back to `exec_aggregate` for complex cases.
- **`scan_columnar`** (`engine_dml.rs`): reads from both frozen segments and write buffer via `ColumnStoreTable::column_scan`.
- **Tenant SQL commands** are intercepted in `handle_system_query` before the SQL parser (non-standard syntax). `SHOW falcon.tenants` / `SHOW falcon.tenant_usage` go through the standard `handle_show_falcon` dispatch.
- **Lag-aware routing** (`query_engine.rs`): `route_read_to_replica` selects the shard with the lowest `lag_lsn` within a configurable threshold, falling back to shard 0 (primary) when no replica qualifies.

---

## Release Cadence

| Version | Status | Key Metric |
|---------|--------|------------|
| v0.1.0 | ✅ Done | 1,081 tests, MVCC + WAL + failover |
| v0.2.0 | ✅ Done | gRPC WAL streaming, two-node e2e |
| v0.3.0 | ✅ Done | Health checks, TLS, query timeout, plan cache |
| v0.4.0 | ✅ Done | Columnstore vectorized agg + multi-tenancy + lag-aware routing |
| **v0.4.x** | ✅ Done | Production Gate Alpha — unwrap=0, error model, crash domain |
| **v0.5.0** | ✅ Done | Operationally Usable — cluster admin, rebalance, scale-out/in |
| **v0.6.0** | ✅ Done | Latency-Controlled OLTP — backpressure, admission, memory budget |
| **v0.7.0** | ✅ Done | Deterministic Transactions — 2PC hardening, in-doubt resolver |
| **v0.8.0** | ✅ Done | Chaos-Ready — chaos matrix, fault injection, chaos CI |
| **v0.9.0** | ✅ Done | Production Candidate — semantic freeze, wire versioning, rolling upgrade |
| **v1.0 Phase 1** | ✅ Done | LSM kernel — 1,917 tests, disk-backed OLTP, MVCC encoding, idempotency |
| **v1.0 Phase 2** | ✅ Done | SQL completeness — 1,976 tests, DECIMAL, composite indexes, RBAC, txn control |
| **1.0.0-rc.1** | ✅ Done | Version aligned, code audit fixes, e2e evidence, RBAC enforcement matrix |
| **v2.0 Phase 3** | ✅ Done | Enterprise — 2,056 tests, RLS, TDE, partitioning, PITR, CDC |
| **Storage Hardening** | ✅ Done | 7 modules, 61 tests — WAL recovery, compaction, memory budget, GC safepoint, fault injection |
| **Distributed Hardening** | ✅ Done | 6 modules, 62 tests — epoch fencing, leader lease, migration, throttle, supervisor |
| **Native Protocol** | ✅ Done | 2,239 tests — binary protocol, JDBC driver, compression, HA failover |
| **v1.0.0** | 📋 Planned | Production-Grade Database Kernel — all gates pass |

---

## Version Philosophy

> **0.x = Engineering and semantics not yet frozen, but already usable in real production.**
> **1.0.0 = Semantic freeze + operational closure + tail latency controlled.**

From v0.4.0 onward:
- ❌ Breaking existing semantics without a spec/invariant/test is forbidden.
- ✅ Every behavioral change must have a spec, invariant, and test.
- Every release must have a **hard Gate** and a **reproducible exercise**.
- All consistency and reliability guarantees must be **provable, replayable, and verifiable**.

> **FalconDB's 1.0 is not "features complete" — it is "failure behavior completely defined."**

---

## v0.4.x — Production Hardening Alpha

**Status**: ✅ Complete
**Positioning**: For engineers and early pilot users. Can run in test / non-critical production. Tail latency not yet guaranteed.

### Core Objective
> Upgrade "it runs" to "it won't crash randomly, errors are understandable, failures are recoverable."

### Problems to Solve
- Core path `unwrap` / `expect` = **0**
- Unified error model (`UserError` / `Retryable` / `Transient` / `InternalBug`)
- Crash domain control (single request/connection/transaction failure ≠ process crash)
- Initial Production Gate

### Deliverables

| Item | Location | Status |
|------|----------|--------|
| `FalconError` unified model + `ErrorKind` + `FalconResult` | `falcon_common::error` | ✅ |
| `bail_user!` / `bail_retryable!` / `bail_transient!` macros | `falcon_common::error` | ✅ |
| `ErrorContext` trait (`.ctx()` / `.ctx_with()`) | `falcon_common::error` | ✅ |
| SQLSTATE + error code mapping | `falcon_common::error` | ✅ |
| `deny_unwrap.sh` — core crate scan | `scripts/deny_unwrap.sh` | ✅ |
| `ci_production_gate.sh` — minimum gate | `scripts/ci_production_gate.sh` | ✅ |
| `ci_production_gate_v2.sh` — 14-gate full gate | `scripts/ci_production_gate_v2.sh` | ✅ |
| `install_panic_hook()` + `catch_request()` + `PanicThrottle` | `falcon_common::crash_domain` | ✅ |
| `DiagBundle` + `DiagBundleBuilder` + JSON export | `falcon_common::diag_bundle` | ✅ |
| Core path unwrap elimination (`handler.rs`, `query_engine.rs`, `online_ddl.rs`) | multiple | ✅ |
| `docs/error_model.md` | `docs/` | ✅ |

### Hard Gates

| Gate | Criterion | Verify |
|------|-----------|--------|
| G1 | `deny_unwrap.sh` 0 hits in core crates | `bash scripts/deny_unwrap.sh` |
| G2 | `ci_failover_gate.sh` P0 all pass | `bash scripts/ci_failover_gate.sh` |
| G3 | Random `kill -9` leader → no panic, system recovers | `ci_failover_gate.sh` |
| G4 | Malformed PG protocol input → no panic, error returned | `cargo test -p falcon_common -- crash_domain` |
| G5 | `cargo test --workspace` 0 failures | `cargo test --workspace` |

---

## v0.5.0 — Operationally Usable

**Status**: ✅ Complete
**Positioning**: Usable in small-scale real production. Ops can intervene manually. Tail latency still not guaranteed.

### Core Objective
> "Not just the author can run it — ops can run it, rescue it, and scale it."

### Problems to Solve
- Cluster ops closure: scale-out, scale-in, rebalance, leader transfer
- All state transitions must have: a state machine, be interruptible, be resumable
- Structured event log for all state transitions

### Deliverables

| Item | Location | Status |
|------|----------|--------|
| `local_cluster_harness.sh` — 3-node start/stop/failover/smoke | `scripts/` | ✅ |
| `rolling_upgrade_smoke.sh` | `scripts/` | ✅ |
| Cluster admin CLI / API (`cluster status`, `add node`, `remove node`) | `falcon_server` | ✅ |
| `rebalance plan` (dry-run) + `rebalance apply` | `falcon_cluster::rebalancer` | ✅ |
| `promote leader` (per-shard) | `falcon_cluster::ha` | ✅ |
| Scale-out state machine (join → catch-up → serve reads → serve writes → rebalance) | `falcon_cluster` | ✅ |
| Scale-in state machine (drain → move leadership → move data → remove membership) | `falcon_cluster` | ✅ |
| Structured state transition event log | `falcon_cluster` | ✅ |
| `ops_playbook.md` — scale-out/in, failover, rolling upgrade | `docs/` | ✅ |

### Hard Gates

| Gate | Criterion | Verify |
|------|-----------|--------|
| G1 | `add node` → rebalance → data verified on new node | `local_cluster_harness.sh scaleout` |
| G2 | `remove node` → no data loss | `local_cluster_harness.sh scalein` |
| G3 | Rolling restart (node by node) → service not interrupted | `rolling_upgrade_smoke.sh` |
| G4 | RTO < 10s (write availability after leader death) | `ci_failover_gate.sh` |
| G5 | Rebalance plan output: shard moves, estimated data volume, estimated time | `SHOW falcon.rebalance_plan` (6 columns) |

---

## v0.6.0 — Latency-Controlled OLTP

**Status**: ✅ Complete
**Positioning**: True OLTP product. Can enter core business, but still requires monitoring.

### Core Objective
> "Not fast on average — P99 doesn't explode."

### Problems to Solve
- Full-chain backpressure
- Admission control
- Global memory budget
- WAL / replication / executor queue visibility

### Deliverables

| Item | Location | Status |
|------|----------|--------|
| Connection / query / write / WAL / apply permits | `falcon_cluster::admission` | ✅ |
| `MemoryBudget` (global + per-shard, soft 80% / hard 95%) | `falcon_cluster::admission` | ✅ |
| `QueryGovernor` (time / rows / bytes / memory per query) | `falcon_executor::governor` | ✅ |
| `ShardCircuitBreaker` + `ClusterCircuitBreaker` | `falcon_cluster::circuit_breaker` | ✅ |
| Queue metrics: length / bytes / wait time / reject count | `falcon_cluster::admission` | ✅ |
| Priority queues: OLTP (high) vs large query / DDL / rebalance (low) | `falcon_executor::priority_scheduler` | ✅ |
| Token bucket for DDL / backfill / rebalance | `falcon_cluster::token_bucket` | ✅ |
| Backpressure smoke in CI gate | `ci_production_gate_v2.sh` | ✅ |

### Hard Gates

| Gate | Criterion | Verify |
|------|-----------|--------|
| G1 | Small memory + sustained writes → no OOM, stable rejection | `ci_production_gate_v2.sh --gate 9` |
| G2 | Mixed load (OLTP + large query): small txn P99 < 10ms | bench + gate |
| G3 | Write storm → system enters stable backoff, not avalanche | admission tests |
| G4 | Memory budget metrics: `used / limit / over-limit count / wait time` all present | observability check |

---

## v0.7.0 — Deterministic Transactions

**Status**: ✅ Complete
**Positioning**: Enterprise-grade transaction kernel. Usable for cross-shard strong-consistency business.

### Core Objective
> "Failures are not random — they are explainable and predictable."

### Problems to Solve
- 2PC tail latency
- In-doubt storm
- Transactions don't spiral under leader churn

### Deliverables

| Item | Location | Status |
|------|----------|--------|
| Commit decision durable point (coordinator spec) | `falcon_cluster::deterministic_2pc` | ✅ |
| `TxnOutcomeCache` (TTL + max-size eviction) | `falcon_cluster::indoubt_resolver` | ✅ |
| `InDoubtResolver` background task (rate-limited, observable) | `falcon_cluster::indoubt_resolver` | ✅ |
| Layered timeout: soft → `Retryable`, hard → abort + resolver | `falcon_cluster::deterministic_2pc` | ✅ |
| Slow-shard policy (configurable: fast-abort or hedged request) | `falcon_cluster::deterministic_2pc` | ✅ |
| `RecoveryTracker` (5-phase structured startup logging) | `falcon_common::request_context` | ✅ |
| `StageLatency` per-query breakdown | `falcon_common::request_context` | ✅ |

### Hard Gates

| Gate | Criterion | Verify |
|------|-----------|--------|
| G1 | 2PC: kill coordinator after prepare → in-doubt resolver converges within 30s | `ci_failover_gate.sh` |
| G2 | Leader churn: P99 does not grow unboundedly | bench under churn |
| G3 | All in-doubt transactions converge (0 stuck after 60s) | `indoubt_resolver` tests |
| G4 | `TxnOutcomeCache` hit rate > 90% under coordinator-query storm | cache tests |

---

## v0.8.0 — Chaos-Ready

**Status**: ✅ Complete
**Positioning**: Acceptable to cloud vendors / large enterprises.

### Core Objective
> "Every failure you're afraid of — we've drilled it."

### Problems to Solve
- Chaos injection coverage
- Failure coverage matrix
- Real-world failure modeling

### Deliverables

| Item | Location | Status |
|------|----------|--------|
| `chaos_injector.sh` (kill / restart / delay / loss / report) | `scripts/` | ✅ |
| `chaos_matrix.md` (30 scenarios, 6 categories, acceptance thresholds) | `docs/` | ✅ |
| Chaos + workload joint exercise (30–60s CI smoke) | `ci_production_gate_v2.sh` | ✅ |
| In-process fault injection (`falcon_cluster::fault_injection`) | `falcon_cluster` | ✅ |
| Chaos report auto-generation | `chaos_injector.sh report` | ✅ |
| Network partition simulation (split-brain) | `falcon_cluster::fault_injection` | ✅ |
| CPU / IO jitter injection | `falcon_cluster::fault_injection` | ✅ |

### Hard Gates

| Gate | Criterion | Verify |
|------|-----------|--------|
| G1 | Chaos running: 0 panics | `ci_production_gate_v2.sh --gate 6` |
| G2 | Chaos running: data consistent after convergence | consistency check |
| G3 | Chaos running: in-doubt txns all converge | `indoubt_resolver` metrics |
| G4 | Chaos report auto-generated as CI artifact | `chaos_injector.sh report` |
| G5 | All 30 chaos matrix scenarios documented with expected behavior | `chaos_matrix.md` |

---

## v0.9.0 — Production Candidate

**Status**: ✅ Complete
**Positioning**: Pre-1.0 freeze. Bug fixes only, no new semantics.

### Core Objective
> "From today, semantics no longer change arbitrarily."

### Problems to Solve
- Semantic freeze
- On-disk / wire versioning
- Rolling upgrade

### Deliverables

| Item | Location | Status |
|------|----------|--------|
| Wire compatibility policy (document + enforce) | `docs/wire_compatibility.md` | ✅ |
| WAL / snapshot versioning (magic + version header) | `falcon_storage::wal` | ✅ |
| `rolling_upgrade_smoke.sh` (3-node rolling upgrade) | `scripts/` | ✅ |
| `ops_playbook.md` (complete) | `docs/` | ✅ |
| `CHANGELOG.md` with semantic versioning | root | ✅ |
| Backward-compatible config schema (deprecated fields warned, not errored) | `falcon_common::config` | ✅ |

### Hard Gates

| Gate | Criterion | Verify |
|------|-----------|--------|
| G1 | v0.9.x → v0.9.y rolling upgrade succeeds (no downtime) | `rolling_upgrade_smoke.sh` |
| G2 | Rollback feasible (downgrade v0.9.y → v0.9.x) | manual exercise |
| G3 | Wire protocol: v0.9.x client connects to v0.9.y server | compatibility test |
| G4 | WAL written by v0.9.x readable by v0.9.y | WAL replay test |
| G5 | All v0.8.0 gates still pass | full gate suite |

---

## v1.0.0 — Production-Grade Database Kernel

**Status**: 📋 Planned
**Positioning**: "I dare to tell users: this is a production database."

### Core Objective
> All gates pass. Production readiness ≥ 9.8/10. Docs, exercises, and diagnostics complete.

### Must Have

| Requirement | Criterion |
|-------------|-----------|
| All gates pass | Every gate from v0.4.x through v0.9.0 passes |
| `deny_unwrap.sh` | 0 hits in all core crates |
| `cargo test --workspace` | 0 failures |
| `ci_production_gate_v2.sh` | All 14 gates pass |
| `ci_failover_gate.sh` | All P0 gates pass |
| Tail latency | P99 < 10ms (OLTP point lookup, no chaos) |
| Failover RTO | < 5s (quorum-ack mode) |
| RPO | 0 bytes (quorum-ack mode) |
| In-doubt convergence | < 30s after coordinator restart |
| Panic count | 0 during any gate exercise |

### Final Deliverables

| Document | Status |
|----------|--------|
| `docs/production_readiness_report.md` | ✅ (updated each release) |
| `docs/chaos_matrix.md` + chaos report | ✅ |
| `docs/ops_playbook.md` | ✅ |
| `docs/error_model.md` | ✅ |
| `docs/observability.md` | ✅ |
| `performance_baseline.md` | ✅ |
| `CHANGELOG.md` | ✅ |

### 1.0 Definition

| Property | Definition |
|----------|------------|
| **Behavior stable** | No semantic changes without a versioned spec |
| **Failures predictable** | Every error has an `ErrorKind`, SQLSTATE, and retry hint |
| **Ops operable** | Any trained ops engineer can scale, failover, and diagnose |
| **Tail latency controlled** | P99 bounded under defined load profiles |
| **Failure behavior defined** | Every failure mode in `chaos_matrix.md` has documented expected behavior |

---

## v1.0 — Phase 1: Industrial/Financial-Grade OLTP Kernel

**Status**: ✅ Complete  
**Goal**: Disk-backed OLTP, deterministic txn semantics, predictable P99, faster than PG

### P0: Storage & Transaction Kernel

| Deliverable | Location | Status |
|------------|----------|--------|
| LSM storage engine (WAL→MemTable→Flush→L0→Compaction) | `falcon_storage::lsm::engine` | ✅ |
| SST file format (data blocks + index + bloom filter + footer) | `falcon_storage::lsm::sst` | ✅ |
| Bloom filter (per-SST negative lookup elimination) | `falcon_storage::lsm::bloom` | ✅ |
| LRU block cache (configurable budget + eviction metrics) | `falcon_storage::lsm::block_cache` | ✅ |
| Leveled compaction (L0→L1 merge, dedup, tombstone removal) | `falcon_storage::lsm::compaction` | ✅ |
| LSM memtable (sorted BTreeMap, freeze/flush lifecycle) | `falcon_storage::lsm::memtable` | ✅ |
| MVCC value encoding (txn_id, status, commit_ts, data) | `falcon_storage::lsm::mvcc_encoding` | ✅ |
| Visibility rules (Prepared=invisible, Committed=ts-gated) | `MvccValue::is_visible()` | ✅ |
| Persistent txn_meta store (txn_id→outcome for 2PC recovery) | `falcon_storage::lsm::mvcc_encoding::TxnMetaStore` | ✅ |
| LSM backpressure (memtable budget flush, L0 stall trigger) | `LsmEngine::maybe_stall_writes()` | ✅ |

### P1: Performance, Idempotency, Audit, Benchmark

| Deliverable | Location | Status |
|------------|----------|--------|
| Idempotency key store (persistent key→result, TTL/GC) | `falcon_storage::lsm::idempotency` | ✅ |
| Transaction-level audit log (txn_id, keys, outcome, epoch) | `falcon_storage::lsm::txn_audit` | ✅ |
| TPC-B (pgbench) benchmark workload | `falcon_bench --tpcb` | ✅ |
| LSM disk-backed KV benchmark | `falcon_bench --lsm` | ✅ |
| P50/P95/P99/Max latency + backpressure reporting | `falcon_bench` all workloads | ✅ |
| CI Phase 1 gates script | `scripts/ci_phase1_gates.sh` | ✅ |

### Test Coverage

| Module | Tests |
|--------|-------|
| `lsm::bloom` | 4 |
| `lsm::block_cache` | 5 |
| `lsm::sst` | 5 |
| `lsm::memtable` | 7 |
| `lsm::compaction` | 4 |
| `lsm::engine` | 8 |
| `lsm::mvcc_encoding` | 15 |
| `lsm::idempotency` | 10 |
| `lsm::txn_audit` | 10 |
| **Total new** | **68** |

### Hard Gates (v1.0 Release)

| Gate | Criterion | Verify |
|------|-----------|--------|
| G1 | Data > 2× memory budget, 10min write, no OOM | `ci_phase1_gates.sh gate2` |
| G2 | kill -9 → restart → data intact | `ci_phase1_gates.sh gate3` |
| G3 | Prepared versions never visible to readers | `test_mvcc_visibility_prepared_never_visible` |
| G4 | In-doubt txn converges after crash | `TxnMetaStore` persistence tests |
| G5 | TPC-B P95/P99 reported, backpressure counted | `falcon_bench --tpcb --export json` |
| G6 | Error model 4-tier full coverage | `ci_phase1_gates.sh gate1` |

---

## v1.0 — Phase 2: SQL Completeness & Enterprise Kernel

**Status**: ✅ Complete  
**Goal**: Full SQL type coverage, constraint enforcement, transaction control, fine-grained access control, and operational robustness.

### P0 v1.1: DECIMAL Type

| Deliverable | Location | Status |
|------------|----------|--------|
| `Datum::Decimal(i128, u8)` — mantissa + scale | `falcon_common::datum` | ✅ |
| `DataType::Decimal(u8, u8)` — precision + scale | `falcon_common::types` | ✅ |
| Decimal encoding in secondary index keys | `falcon_storage::memtable` | ✅ |
| Decimal in stats, sharding, gather, JSONB, COPY, PG wire | multiple | ✅ |
| 11 tests | `falcon_common::datum` | ✅ |

### P0 v1.2: Composite / Covering / Prefix Indexes

| Deliverable | Location | Status |
|------------|----------|--------|
| `SecondaryIndex::column_indices: Vec<usize>` (multi-column) | `falcon_storage::memtable` | ✅ |
| `encode_key()` — composite key encoding by column concatenation | `falcon_storage::memtable` | ✅ |
| `new_composite()`, `new_covering()`, `new_prefix()` constructors | `falcon_storage::memtable` | ✅ |
| `prefix_scan()` for prefix index range queries | `falcon_storage::memtable` | ✅ |
| `create_composite_index`, `create_covering_index`, `create_prefix_index` | `falcon_storage::engine_ddl` | ✅ |
| Backfill from existing table data on index creation | `falcon_storage::engine_ddl` | ✅ |
| 10 tests (encoding, uniqueness, prefix truncation, insert/remove) | `falcon_storage::memtable` | ✅ |

### P0 v1.2: CHECK Constraint Runtime Enforcement

| Deliverable | Location | Status |
|------------|----------|--------|
| `ExecutionError::CheckConstraintViolation(String)` error variant | `falcon_common::error` | ✅ |
| SQLSTATE `23514` (`CHECK_VIOLATION`) constant | `falcon_common::consistency` | ✅ |
| CHECK constraint evaluation on INSERT / UPDATE | `falcon_executor::executor_subquery` | ✅ |
| CHECK + NOT NULL enforcement in `exec_insert_select` | `falcon_executor::executor_dml` | ✅ |

### P0 v1.2: Transaction READ ONLY / READ WRITE + Timeout + Exec Summary

| Deliverable | Location | Status |
|------------|----------|--------|
| `TxnHandle::read_only: bool` — per-transaction access mode | `falcon_txn::manager` | ✅ |
| `TxnHandle::timeout_ms: u64` — per-transaction timeout | `falcon_txn::manager` | ✅ |
| `TxnHandle::exec_summary: TxnExecSummary` — statement/row counters | `falcon_txn::manager` | ✅ |
| `TxnHandle::is_timed_out()`, `elapsed_ms()` helper methods | `falcon_txn::manager` | ✅ |
| `TxnExecSummary` — `record_read/insert/update/delete/statement()` | `falcon_txn::manager` | ✅ |
| `reject_if_txn_read_only()` guard in INSERT / UPDATE / DELETE | `falcon_executor::executor` | ✅ |
| `check_txn_timeout()` guard in INSERT / UPDATE / DELETE | `falcon_executor::executor` | ✅ |
| 8 tests | `falcon_txn::tests` | ✅ |

### P1 v1.3: Query Governor v2 — Structured Abort Reasons

| Deliverable | Location | Status |
|------------|----------|--------|
| `GovernorAbortReason` enum (RowLimit / ByteLimit / Timeout / Memory) | `falcon_executor::governor` | ✅ |
| `QueryGovernor::check_all_v2()` — returns structured abort reason | `falcon_executor::governor` | ✅ |
| `QueryGovernor::abort_reason()` — current abort reason or None | `falcon_executor::governor` | ✅ |
| 6 tests | `falcon_executor::governor` | ✅ |

### P1 v1.4: Fine-Grained Admission Control

| Deliverable | Location | Status |
|------------|----------|--------|
| `OperationType` enum (Read / Write / Ddl) | `falcon_cluster::admission` | ✅ |
| `DdlPermit` RAII guard — auto-releases DDL slot on drop | `falcon_cluster::admission` | ✅ |
| `AdmissionControl::acquire_ddl()` — DDL concurrency limit | `falcon_cluster::admission` | ✅ |
| `AdmissionConfig::max_inflight_ddl` (default: 4) | `falcon_cluster::admission` | ✅ |
| `AdmissionMetrics::ddl_rejected` counter | `falcon_cluster::admission` | ✅ |
| 2 tests | `falcon_cluster::admission` | ✅ |

### P1 v1.5: Node Operational Mode (Drain / ReadOnly / Normal)

| Deliverable | Location | Status |
|------------|----------|--------|
| `NodeOperationalMode` enum (Normal / ReadOnly / Drain) | `falcon_cluster::cluster_ops` | ✅ |
| `NodeModeController` — thread-safe mode transitions with event logging | `falcon_cluster::cluster_ops` | ✅ |
| `allows_reads()`, `allows_writes()`, `allows_ddl()` enforcement methods | `falcon_cluster::cluster_ops` | ✅ |
| `drain()`, `set_read_only()`, `resume()` convenience methods | `falcon_cluster::cluster_ops` | ✅ |
| All mode transitions logged to `ClusterEventLog` | `falcon_cluster::cluster_ops` | ✅ |
| No-op detection (same-mode transitions not logged) | `falcon_cluster::cluster_ops` | ✅ |
| 6 tests | `falcon_cluster::cluster_ops` | ✅ |

### P1 v1.6: Role-Based Access Control + Schema Permissions

| Deliverable | Location | Status |
|------------|----------|--------|
| `RoleCatalog` — in-memory role store with CRUD | `falcon_common::security` | ✅ |
| Transitive role inheritance (`effective_roles()`) | `falcon_common::security` | ✅ |
| Circular inheritance detection (rejected with error) | `falcon_common::security` | ✅ |
| `grant_role()` / `revoke_role()` membership management | `falcon_common::security` | ✅ |
| `PrivilegeManager` — GRANT / REVOKE on tables, schemas, functions | `falcon_common::security` | ✅ |
| `check_privilege()` — checks effective role set against grants | `falcon_common::security` | ✅ |
| `DefaultPrivilege` + `add_schema_default()` — ALTER DEFAULT PRIVILEGES | `falcon_common::security` | ✅ |
| Duplicate grant deduplication | `falcon_common::security` | ✅ |
| `revoke_all_on_object()` — used on DROP TABLE/SCHEMA | `falcon_common::security` | ✅ |
| 17 tests (9 RoleCatalog + 8 PrivilegeManager) | `falcon_common::security` | ✅ |

### Phase 2 Test Coverage Summary

| Feature Area | New Tests |
|-------------|-----------|
| `Datum::Decimal` | 11 |
| Composite / covering / prefix indexes | 10 |
| Transaction READ ONLY + timeout + exec summary | 8 |
| Query Governor v2 (abort reasons) | 6 |
| Node operational mode | 6 |
| Fine-grained admission (DDL permits) | 2 |
| RoleCatalog (transitive inheritance) | 9 |
| PrivilegeManager (GRANT/REVOKE) | 8 |
| **Total new (Phase 2)** | **60** |

### Verification

```bash
cargo test --workspace   # 2,239 pass, 0 failures
cargo test -p falcon_common --lib -- security::tests
cargo test -p falcon_txn --lib -- txn_manager_tests::test_txn_
cargo test -p falcon_executor --lib -- governor::tests
cargo test -p falcon_cluster --lib -- cluster_ops::tests
cargo test -p falcon_cluster --lib -- admission::tests
cargo test -p falcon_storage --lib -- memtable::index_tests
```

---

## v2.0 — Phase 3: Enterprise Edition Features ✅

**Status**: Implemented

**Positioning**: Enterprise-grade capabilities for production deployments — multi-tenant
data isolation, compliance, disaster recovery, and real-time integration.

### P0 v2.1: Row-Level Security (RLS)

| Deliverable | Location | Status |
|------------|----------|--------|
| `RlsPolicy` — per-table policy with USING/WITH CHECK expressions | `falcon_common::rls` | ✅ |
| `PolicyCommand` — ALL/SELECT/INSERT/UPDATE/DELETE targeting | `falcon_common::rls` | ✅ |
| `PolicyPermissiveness` — PERMISSIVE (OR) / RESTRICTIVE (AND) | `falcon_common::rls` | ✅ |
| `RlsPolicyManager` — create/drop policies, enable/disable/force RLS | `falcon_common::rls` | ✅ |
| `combined_using_expr()` — PG-compatible OR+AND policy combination | `falcon_common::rls` | ✅ |
| `combined_check_expr()` — write-side policy combination | `falcon_common::rls` | ✅ |
| `should_bypass()` — superuser/owner bypass logic (respects FORCE) | `falcon_common::rls` | ✅ |
| Role-scoped policies (PUBLIC or specific roles) | `falcon_common::rls` | ✅ |
| `drop_all_policies()` — cleanup on DROP TABLE | `falcon_common::rls` | ✅ |
| Wired into `StorageEngine.rls_manager` | `falcon_storage::engine` | ✅ |
| 15 tests | `falcon_common::rls` | ✅ |

### P0 v2.2: Transparent Data Encryption (TDE)

| Deliverable | Location | Status |
|------------|----------|--------|
| `EncryptionKey` — AES-256 key with PBKDF2 derivation | `falcon_storage::encryption` | ✅ |
| `KeyManager` — master key + DEK lifecycle management | `falcon_storage::encryption` | ✅ |
| `DekId` / `WrappedDek` — encrypted DEK storage with nonce | `falcon_storage::encryption` | ✅ |
| `EncryptionScope` — per-WAL/table/SST/backup key isolation | `falcon_storage::encryption` | ✅ |
| `encrypt_block()` / `decrypt_block()` — data encryption primitives | `falcon_storage::encryption` | ✅ |
| `rotate_master_key()` — re-wrap all DEKs with new passphrase | `falcon_storage::encryption` | ✅ |
| Wired into `StorageEngine.key_manager` | `falcon_storage::engine` | ✅ |
| 11 tests | `falcon_storage::encryption` | ✅ |

### P0 v2.3: Table Partitioning (Range / Hash / List)

| Deliverable | Location | Status |
|------------|----------|--------|
| `PartitionStrategy` — Range / Hash / List strategies | `falcon_storage::partition` | ✅ |
| `RangeBound` — inclusive lower / exclusive upper with MINVALUE/MAXVALUE | `falcon_storage::partition` | ✅ |
| `ListBound` — explicit value set matching | `falcon_storage::partition` | ✅ |
| `PartitionManager` — create/drop/attach/detach partitions | `falcon_storage::partition` | ✅ |
| `route()` — datum-based partition routing for INSERT | `falcon_storage::partition` | ✅ |
| `prune_range()` — partition pruning for range scans | `falcon_storage::partition` | ✅ |
| `prune_list()` — partition pruning for IN-list scans | `falcon_storage::partition` | ✅ |
| Default partition (catches unrouted rows) | `falcon_storage::partition` | ✅ |
| Wired into `StorageEngine.partition_manager` | `falcon_storage::engine` | ✅ |
| 10 tests | `falcon_storage::partition` | ✅ |

### P1 v2.4: Point-in-Time Recovery (PITR)

| Deliverable | Location | Status |
|------------|----------|--------|
| `Lsn` — WAL position type with PG-compatible display format | `falcon_storage::pitr` | ✅ |
| `RecoveryTarget` — Latest / Time / LSN / XID / RestorePoint | `falcon_storage::pitr` | ✅ |
| `WalArchiver` — WAL segment archiving with retention policies | `falcon_storage::pitr` | ✅ |
| `BaseBackup` — consistent snapshot metadata with LSN tracking | `falcon_storage::pitr` | ✅ |
| `RestorePoint` — named recovery points (pg_create_restore_point) | `falcon_storage::pitr` | ✅ |
| `RecoveryExecutor` — coordinated replay with target detection | `falcon_storage::pitr` | ✅ |
| `find_base_backup()` — optimal backup selection for recovery target | `falcon_storage::pitr` | ✅ |
| `segments_for_recovery()` — WAL segment range computation | `falcon_storage::pitr` | ✅ |
| `apply_retention()` — time-based segment cleanup | `falcon_storage::pitr` | ✅ |
| Wired into `StorageEngine.wal_archiver` | `falcon_storage::engine` | ✅ |
| 10 tests | `falcon_storage::pitr` | ✅ |

### P1 v2.5: Change Data Capture (CDC) / Logical Decoding

| Deliverable | Location | Status |
|------------|----------|--------|
| `ReplicationSlot` — consumer position tracking with activate/deactivate | `falcon_storage::cdc` | ✅ |
| `ChangeEvent` — structured INSERT/UPDATE/DELETE/DDL/COMMIT events | `falcon_storage::cdc` | ✅ |
| `CdcManager` — slot management + bounded event ring buffer | `falcon_storage::cdc` | ✅ |
| `emit_insert/update/delete/commit()` — convenience emitters | `falcon_storage::cdc` | ✅ |
| `poll_changes()` — consumer polling with slot-scoped progress | `falcon_storage::cdc` | ✅ |
| `advance_slot()` — consumer acknowledges processed LSN | `falcon_storage::cdc` | ✅ |
| Table filtering per slot | `falcon_storage::cdc` | ✅ |
| Old row values (REPLICA IDENTITY FULL support) | `falcon_storage::cdc` | ✅ |
| Buffer eviction for bounded memory | `falcon_storage::cdc` | ✅ |
| Wired into `StorageEngine.cdc_manager` | `falcon_storage::engine` | ✅ |
| 9 tests | `falcon_storage::cdc` | ✅ |

### Phase 3 Test Coverage Summary

| Feature Area | New Tests |
|-------------|-----------|
| Row-Level Security (RLS) | 15 |
| Transparent Data Encryption (TDE) | 11 |
| Table Partitioning (Range/Hash/List) | 10 |
| Point-in-Time Recovery (PITR) | 10 |
| Change Data Capture (CDC) | 9 |
| **Total new (Phase 3)** | **55** |

### Verification

```bash
cargo test --workspace   # 2,239 pass, 0 failures
cargo test -p falcon_common --lib -- rls::tests
cargo test -p falcon_storage --lib -- encryption::tests
cargo test -p falcon_storage --lib -- partition::tests
cargo test -p falcon_storage --lib -- pitr::tests
cargo test -p falcon_storage --lib -- cdc::tests
```

---

## Storage Hardening ✅

**Status**: Complete  
**Goal**: Production-grade storage reliability — WAL recovery, compaction scheduling, memory budget, GC safepoint, fault injection, offline diagnostics.

### Deliverables (7 modules, 61 tests)

| Module | Location | Tests | Status |
|--------|----------|------:|--------|
| Graded WAL error types + `CorruptionLog` | `falcon_storage::storage_error` | 6 | ✅ |
| Phased WAL recovery (Scan→Apply→Validate) | `falcon_storage::recovery` | 10 | ✅ |
| Resource-isolated compaction scheduling | `falcon_storage::compaction_scheduler` | 11 | ✅ |
| Unified memory budget (5 categories, 3 levels) | `falcon_storage::memory_budget` | 10 | ✅ |
| GC safepoint unification + long-txn diagnostics | `falcon_storage::gc_safepoint` | 10 | ✅ |
| Offline diagnostic tools (sst_verify, wal_inspect) | `falcon_storage::storage_tools` | 6 | ✅ |
| Storage fault injection (6 fault types) | `falcon_storage::storage_fault_injection` | 8 | ✅ |

### CI Gate

`scripts/ci_storage_gate.sh` — 6 gates: hardening tests, full suite, SST verify, WAL corruption resilience, memory budget, clippy

---

## Distributed Hardening ✅

**Status**: Complete  
**Goal**: Production-grade distributed coordination — epoch fencing, leader leases, shard migration, cross-shard throttling, unified supervisor.

### Deliverables (6 modules, 62 tests)

| Module | Location | Tests | Status |
|--------|----------|------:|--------|
| Global epoch/fencing token (`EpochGuard`, `WriteToken`) | `falcon_cluster::epoch` | 13 | ✅ |
| Raft-managed cluster state machine | `falcon_cluster::consistent_state` | 8 | ✅ |
| Quorum/lease-driven leader authority | `falcon_cluster::leader_lease` | 10 | ✅ |
| Shard migration state machine (5-phase) | `falcon_cluster::migration` | 10 | ✅ |
| Cross-shard txn throttling (Queue/Reject) | `falcon_cluster::cross_shard_throttle` | 7 | ✅ |
| Unified control-plane supervisor | `falcon_cluster::supervisor` | 9 | ✅ |

### Observability

8 new Prometheus metric functions: `record_distributed_metrics`, `record_shard_replication_lag`, `record_epoch_fence_event`, `record_lease_metrics`, `record_migration_metrics`, `record_cross_shard_throttle_metrics`, `record_supervisor_metrics`

### CI Gate

`scripts/ci_distributed_chaos.sh` — leader kill, epoch fencing, consistent state recovery, migration interrupt, supervisor degradation

---

## FalconDB Native Protocol ✅

**Status**: Complete  
**Goal**: High-performance binary protocol for Java/JVM clients — replacing PG wire protocol overhead with a purpose-built framing, handshake, compression, and HA-aware failover.

### Rust Crates

| Crate | Modules | Tests | Status |
|-------|---------|------:|--------|
| `falcon_protocol_native` | `types`, `codec`, `compress`, `error` | 39 | ✅ |
| `falcon_native_server` | `server`, `session`, `executor_bridge`, `config`, `error`, `nonce` | 28 | ✅ |

### Protocol Features

| Feature | Status |
|---------|--------|
| Binary framing (LE, 5-byte header, 64 MiB max) | ✅ |
| Handshake + version negotiation (major.minor) | ✅ |
| Feature flags (7 flags: compression, batch, pipeline, epoch, TLS, binary params) | ✅ |
| Password authentication with nonce anti-replay | ✅ |
| Query/Response with binary row encoding + null bitmap | ✅ |
| Batch ingest (columnar rows + per-row error reporting) | ✅ |
| Ping/Pong keepalive | ✅ |
| Graceful disconnect | ✅ |
| LZ4-style compression (negotiated via feature flags) | ✅ |
| Epoch fencing (per-request epoch check) | ✅ |
| StartTLS upgrade (message types defined) | ✅ |

### Java JDBC Driver (`clients/falcondb-jdbc/`)

| Component | Status |
|-----------|--------|
| `FalconDriver` — URL parsing, SPI registration, pgjdbc fallback | ✅ |
| `FalconConnection` — session management, auto-commit, isValid(ping) | ✅ |
| `FalconStatement` / `FalconPreparedStatement` — client-side bind, batch | ✅ |
| `FalconResultSet` / `FalconResultSetMetaData` — forward-only, all getter types | ✅ |
| `FalconDataSource` — HikariCP-compatible properties | ✅ |
| `ClusterTopologyProvider` — seed nodes, primary tracking, stale detection | ✅ |
| `PrimaryResolver` — TTL-cached primary resolution | ✅ |
| `FailoverRetryPolicy` — configurable retries, exponential backoff | ✅ |
| `FailoverConnection` — auto-reconnect on FENCED_EPOCH / NOT_LEADER | ✅ |

### Documentation & Artifacts

- `docs/native_protocol.md` — Full protocol specification
- `docs/native_protocol_compat.md` — Version negotiation and feature flags
- `clients/falcondb-jdbc/COMPAT_MATRIX.md` — JDBC driver compatibility matrix
- `tools/native-proto-spec/vectors/golden_vectors.json` — 23 cross-language test vectors

### CI Gate Scripts

- `scripts/ci_native_jdbc_smoke.sh` — Rust + Java compile, clippy, test
- `scripts/ci_native_perf_gate.sh` — Release build + protocol performance regression
- `scripts/ci_native_failover_under_load.sh` — Epoch fencing + failover bench

### Verification

```bash
cargo test -p falcon_protocol_native  # 39 tests
cargo test -p falcon_native_server    # 28 tests
cargo test --workspace                # 2,239 pass, 0 failures
```

---

## Invariants (Enforced Across All Versions from v0.4.x)

These invariants must never be violated once established:

1. **No silent data loss**: A committed transaction (quorum-ack) must survive any single-node failure.
2. **No process panic from user input**: Any malformed SQL, protocol message, or client behavior must return an error, never panic.
3. **No unbounded resource growth**: Memory, WAL backlog, and connection count must be bounded by configured limits.
4. **No stuck in-doubt transactions**: All in-doubt transactions must converge within the configured resolver timeout.
5. **No silent error swallowing**: Every `Err(_)` in a core path must be logged with context or returned to the caller.
6. **Retryable errors must be retryable**: Any error classified `Retryable` must be safe to retry without side effects.
7. **Circuit breakers must isolate**: A failing shard must not cause cascading failures to healthy shards.
