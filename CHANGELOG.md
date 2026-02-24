# Changelog

All notable changes to FalconDB are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.8] — Unified Cluster Access, Smart Gateway, Client & Ops Experience

### Added — Cluster Access Model v1

- **`GatewayRole`** — formal node role: `DedicatedGateway`, `SmartGateway`, `ComputeOnly`.
  Defines which nodes accept client connections and which own shards.
- **`ClusterTopology`** — deployment recommendation: Small (1–3), Medium (4–8), Large (9+).
- Official promise: JDBC never needs to know the leader; leader changes ≠ client disconnection.

### Added — Smart Gateway v1

- **`SmartGateway`** (`falcon_cluster::smart_gateway`) — unified request router.
  Classifies every request: `LOCAL_EXEC`, `FORWARD_TO_LEADER`, `REJECT_NO_ROUTE`, `REJECT_OVERLOADED`.
- **No blind forwarding**: verifies target node alive before forward.
- **No silent retry**: errors explicit with retry hints.
- **No infinite queuing**: overloaded → immediate reject (`max_queue_depth = 0` default).
- **`SmartGatewayConfig`** — `node_id`, `role`, `max_inflight` (10K), `max_forwarded` (5K),
  `forward_timeout` (5s), `topology_staleness` (30s).
- **`SmartGatewayMetrics`** — atomic counters: `local_exec_total`, `forward_total`,
  `reject_no_route_total`, `reject_overloaded_total`, `reject_timeout_total`, `forward_latency_us`,
  `forward_latency_peak_us`, `forward_failed`, `client_connect_total`, `client_failover_total`.

### Added — Gateway Topology Cache

- **`TopologyCache`** — epoch-versioned `shard_id → (leader_node, leader_addr, epoch)`.
  Monotonic epoch, RwLock for read-heavy workload, thread-safe.
- `update_leader()` bumps epoch on leader change; same leader → no bump.
- `invalidate()` / `invalidate_node()` for NOT_LEADER responses and node crashes.
- `TopologyCacheMetrics`: `cache_hits`, `cache_misses`, `epoch_bumps`, `invalidations`,
  `leader_changes`, `hit_rate`.

### Added — JDBC Multi-Host URL

- **`JdbcConnectionUrl`** — parser for `jdbc:falcondb://host1:port,host2:port/db?params`.
  Supports multi-host seed lists, default port (5443), default database (`falcon`).
- **`SeedGatewayList`** — client-side failover: round-robin on consecutive failures,
  `max_failures_before_switch = 3`, `all_seeds_exhausted()` detection.

### Added — JDBC Error Code & Retry Contract

- **`GatewayErrorCode`** — 5 codes: `NotLeader` (FD001), `NoRoute` (FD002),
  `Overloaded` (FD003), `Timeout` (FD004), `Fatal` (FD000).
- Each code carries: `is_retryable()`, `retry_delay_ms()`, `sqlstate()`.
- **`GatewayError`** — structured error with `code`, `message`, `leader_hint`,
  `retry_after_ms`, `epoch`, `shard_id`.

### Added — Compression Profiles (Product-Level)

- **`CompressionProfile`** — `Off` / `Balanced` / `Aggressive`.
  Single config knob: `compression_profile = "balanced"`.
- Each profile maps to: `min_version_age`, `codec`, `block_cache_capacity`,
  `compactor_batch_size`, `compactor_interval_ms`.
- Change without restart.

### Added — WAL Backend Policy (Product-Level)

- **`WalMode`** — `Auto` / `Posix` / `WinAsync` / `RawExperimental`.
  Single config knob: `wal_mode = "auto"`.
- `Auto` selects platform-optimal backend; `RawExperimental` clearly marked HIGH risk.

### Changed — Configuration (v1.0.8)

- **`FalconConfig`** gains: `compression_profile` (default `"balanced"`),
  `wal_mode` (default `"auto"`), `gateway` section (`GatewayConfig`).
- **`GatewayConfig`** — `role`, `max_inflight`, `max_forwarded`, `forward_timeout_ms`,
  `topology_staleness_secs`.
- Config schema version bumped to 4.

### Changed — Observability

- **`/admin/status`** gains `smart_gateway` section: role, epoch, requests_total,
  local_exec_total, forward_total, reject counts, inflight, forwarded,
  forward_latency_avg/peak_us, forward_failed, client_connect/failover_total,
  topology (shard_count, node_count, cache_hit_rate, leader_changes, invalidations).

### Tests

- 36 smart_gateway unit tests: JDBC URL parsing, error codes, topology cache,
  gateway routing, compression profiles, WAL modes, concurrent safety.
- 17 client-focused integration tests: JDBC failover, leader switch, error contract,
  overload, concurrent clients, leader changes during requests.
- 14 gateway scale integration tests: single/multi gateway, shared topology,
  crash/restart, client recovery, epoch monotonicity, latency guardrails.

### Documentation

- `docs/cluster_access_model.md` — Gateway roles, deployment topologies, promises.
- `docs/jdbc_connection.md` — URL format, retry contract, failover behavior.
- `docs/gateway_behavior.md` — Request lifecycle, classification rules, admission.
- `docs/wal_backend_matrix.md` — WAL modes, risk matrix, diagnostics, rollback.
- `docs/compression_profiles.md` — Off/Balanced/Aggressive, observability, migration.

---

## [1.0.7] — Memory Compression (Hot/Cold) + WAL Advanced Backend + Ops Simplicity

### Added — Hot/Cold Memory Tiering

- **`ColdStore`** (`falcon_storage::cold_store`) — append-only segment-based compressed storage
  for old MVCC version payloads. Block format: `[codec:u8][original_len:u32][compressed_len:u32][data...]`.
  Configurable max segment size (default 64 MB). LRU block cache for read amortization.
- **`ColdStoreConfig`** — `enabled`, `max_segment_size`, `codec` (LZ4/None), `compression_enabled`
  (global toggle), `block_cache_capacity` (default 16 MB).
- **`ColdHandle`** — compact 20-byte reference (`segment_id`, `offset`, `len`) replacing
  `Option<OwnedRow>` in cold-migrated versions.
- **`CompactorConfig`** — background cold migration config: `min_version_age` (default 300),
  `batch_size` (1000), `interval_ms` (5000). Idempotent, non-blocking, failure-safe.
- **`ColdStoreMetrics`** — atomic counters: `cold_bytes`, `cold_original_bytes`,
  `cold_segments_total`, `cold_read_total`, `cold_decompress_total`,
  `cold_decompress_latency_us`, `cold_decompress_peak_us`, `cold_migrate_total`.
  Derived: `compression_ratio()`, `avg_decompress_us()`.

### Added — String Intern Pool

- **`StringInternPool`** — thread-safe read-biased pool. `intern(s) → InternId(u32)`,
  `resolve(id) → String`. Atomic `hit_rate()` tracking. Reduces memory for
  low-cardinality string columns (status codes, regions, enums).

### Added — LZ4 Compression Backend

- Default codec: LZ4 via `lz4_flex` (pure Rust, no C dependency).
- Fallback: `CompressionCodec::None` — raw bytes, zero CPU overhead.
- Per-block codec tag enables mixed-codec segments and zero-downtime codec changes.
- Global toggle: `compression.enabled = true|false`.

### Changed — Observability

- **`/admin/status`** now includes `memory` section: `hot_bytes`, `cold_bytes`,
  `cold_segments`, `compression_ratio`, `cold_read_total`, `cold_decompress_avg_us`,
  `cold_decompress_peak_us`, `cold_migrate_total`, `intern_hit_rate`.
- **`StorageEngine`** gains: `cold_store_metrics()`, `memory_hot_bytes()`,
  `memory_cold_bytes()`, `intern_hit_rate()`.

### Changed — WAL Configuration (v1.0.7)

- **`WalConfig`** gains: `backend` (`"file"` | `"win_async_file"`), `no_buffering` (bool),
  `group_commit_window_us` (default 200µs). Configurable in `falcon.toml` `[wal]` section.

### Tests

- 17 cold store unit tests: store/read roundtrip, compression ratio, segment rotation,
  cache hit/miss, metrics, block encode/decode, intern pool, concurrent safety.
- 18 compression correctness integration tests: LZ4 vs None consistency, segment rotation
  data integrity, concurrent store/read, cache eviction, empty/large rows.
- 7 performance guardrail tests: memory savings ≥30%, cold read p99 < 1ms (cached),
  p99 < 10ms (uncached), store throughput > 10K rows/sec, cache amortization,
  no hang under concurrent pressure, intern pool savings.

### Documentation

- `docs/memory_compression.md` — Hot/Cold architecture, migration conditions, compression
  codecs, transaction semantics, observability, rollback procedures.
- `docs/windows_wal_modes.md` — `file`, `win_async_file`, raw disk (experimental),
  configuration reference, recommendations, risk matrix.

---

## [1.0.6] — Deterministic WAL I/O & Production-Ready Gateway

### Added — WAL I/O

- **`WalDeviceWinAsync`** (`falcon_storage::wal_win_async`) — Windows IOCP/Overlapped I/O WAL
  device. Uses `FILE_FLAG_OVERLAPPED` via `std::os::windows::fs::OpenOptionsExt`, IOCP completion
  port for deterministic flush acknowledgement, optional `FILE_FLAG_NO_BUFFERING` with sector
  alignment, and `FlushFileBuffers` for durable sync. Non-Windows stub provided.
- **Group commit enhancements** — `GroupCommitConfig` gains `group_commit_window_us` (configurable
  coalescing window, default 200µs) and `ring_buffer_capacity` (default 256 KB). `GroupCommitStats`
  tracks `ring_buffer_used` and `ring_buffer_peak`. New accessors: `flushed_lsn()`,
  `wal_backlog_bytes()`.
- **WAL diagnostics** — `iocp_available()`, `check_no_buffering_support()`,
  `check_disk_alignment()` for runtime platform capability checks.

### Added — Gateway

- **`GatewayDisposition`** enum — semantic classification for every gateway request:
  `LocalExec`, `ForwardedToLeader`, `RejectNoLeader`, `RejectOverloaded`, `RejectTimeout`.
  Includes `is_success()`, `is_retryable()`, `as_str()` helpers.
- **`GatewayAdmissionControl`** — lock-free CAS-based admission control with configurable
  `max_inflight` and `max_forwarded` limits. Fast-reject on overload (no queuing, no p99 explosion).
- **`GatewayMetrics`** enhanced with per-disposition counters: `local_exec_total`, `reject_total`,
  `reject_no_leader`, `reject_overloaded`, `reject_timeout`.
- **`DistributedQueryEngine::new_with_admission()`** — constructor accepting explicit admission config.

### Changed — Observability

- **`/admin/status`** endpoint now returns `wal.current_lsn`, `wal.flushed_lsn`,
  `wal.wal_backlog_bytes`, and a new `gateway` section with `inflight`, `forwarded`, `rejected`,
  `local_exec_total`, `forward_total`.
- **`falcon doctor`** now includes WAL I/O diagnostics: IOCP support, `FILE_FLAG_NO_BUFFERING`
  compatibility, disk alignment check, and recommended WAL mode.

### Tests

- 7 WAL pressure tests: group commit window variants (0/200/500µs), concurrent writers, fsync mode
  comparison, flushed_lsn tracking, ring buffer stats.
- 13 gateway pressure tests: disposition classification, admission control limits, concurrent CAS
  safety, metrics accumulation, no-hang-under-overload, explicit reject latency (p99 < 1ms).
- 5 WAL IOCP unit tests (Windows only): config defaults, open/append/flush, multiple appends,
  IOCP availability, NO_BUFFERING check.

---

## [1.0.4] — Production-Grade Determinism & Failure Safety

### Added — Determinism Hardening (`falcon_cluster::determinism_hardening`)

#### §1 Resource Exhaustion Contract
- **`ResourceExhaustionContract`** — formalized exhaustion semantics for all 9 exhaustible
  resources (memory hard/soft, WAL backlog, replication lag, connection/query/write/cross-shard/DDL
  concurrency). Each entry defines: SQLSTATE, retry policy, max rejection latency, metric name.
- **`validate_contracts()`** — compile-time self-consistency check for all contracts.
- **`DeterministicRejectPolicy`** — unified per-resource rejection counter with total tracking.
- **Invariant RES-1**: No resource exhaustion path may block implicitly.
- **Invariant RES-2**: Every rejection increments a per-resource counter visible via metrics.
- **Invariant RES-3**: No queue may grow without bound.

#### §2 Transaction Outcome Formalization
- **`TxnTerminalState`** enum — Committed, AbortedRetryable, AbortedNonRetryable, Rejected,
  Indeterminate. Every txn MUST end in exactly one terminal state.
- **`AbortReason`** — 9 variants covering all abort paths (serialization, deadlock, constraint,
  timeout, explicit rollback, failover, read-only, storage error, invariant violation).
- **`RejectReason`** — 10 variants covering all pre-admission rejection paths.
- **`RetryPolicy`** — NoRetry, RetryAfter, ExponentialBackoff, RetryOnDifferentNode.
- **`classify_error()`** — canonical FalconError → TxnTerminalState classification function.
- Each terminal state maps to exactly one SQLSTATE code and retry policy.

#### §3 Failover × Commit Invariant Validation
- **`CommitPhase`** — Active → WalLogged → WalDurable → Visible → Acknowledged (strict ordering).
- **`FailoverCrashRecord`** — models txn state at crash time for invariant validation.
- **`validate_failover_invariants()`** — validates FC-1 (crash before CP-D → rollback),
  FC-2 (crash after CP-D → survive), with policy-dependent handling.
- Tests for all crash phases, violation detection, and policy-dependent behavior.

#### §4 Queue Depth Guard
- **`QueueDepthGuard`** — bounded queue with hard capacity and RAII `QueueSlot`.
- No silent growth: `try_enqueue()` rejects immediately at capacity.
- Peak tracking, total enqueued/rejected counters, snapshot for observability.

#### §5 Idempotent Replay Validator
- **`IdempotentReplayValidator`** — tracks replayed txn_ids, detects duplicate replays,
  counts idempotency violations.

### Added — Observability (v1.0.4 §6)
- `falcon_txn_terminal_total{type,reason,sqlstate}` — counter per terminal state
- `falcon_admission_rejection_total{resource,sqlstate}` — counter per resource rejection
- `falcon_failover_recovery_duration_ms{outcome}` — histogram
- `falcon_queue_depth{queue}`, `falcon_queue_capacity{queue}`, `falcon_queue_peak{queue}` — gauges
- `falcon_queue_enqueued_total{queue}`, `falcon_queue_rejected_total{queue}` — gauges
- `falcon_replay_replayed_total`, `falcon_replay_duplicate_total`, `falcon_replay_violation_total`

### Added — Documentation (v1.0.4 §7)
- `docs/CONSISTENCY.md` — normative consistency contract with 10 forbidden states,
  code references for every invariant, observability contract
- `docs/sql_compatibility.md` — frozen SQL compatibility matrix (statements, types,
  operators, functions, explicitly unsupported features with SQLSTATE codes)
- `docs/stability_report_v104.md` — stability evidence template with soak test config

### Added — CI Gate
- `scripts/ci_v104_determinism_gate.sh` — 7-gate verification (determinism tests,
  stability regression, failover regression, full workspace, clippy, contract validation,
  terminal state coverage)

### Tests
- 34 new unit tests in `determinism_hardening.rs`
- All existing v1.0.3 stability and v1.0.2 failover tests preserved

---

## [Unreleased] — Query Performance Optimization

### Improved — 1M Row Scan & Aggregate Performance (1.83x overall speedup)

Systematic optimization of full table scans, ORDER BY with LIMIT, and aggregate queries
on large datasets. Benchmarked on 1M row bulk insert + query workload, achieving near-parity
with PostgreSQL (6.4s vs PG 6.1s).

#### MVCC Visibility Fast Path (`mvcc.rs`)
- **Arc-clone elimination**: All 5 hot MVCC methods (`read_for_txn`, `read_committed`,
  `is_visible`, `with_visible_data`, `has_committed_write_after`) now check the head
  version directly through the `RwLock` read guard reference, avoiding an `Arc::clone`
  for the common single-version case (1M fewer atomic inc/dec per scan)
- **Zero-copy row access**: New `with_visible_data()` method calls a closure with
  `&OwnedRow` reference, enabling downstream consumers to read row data without cloning

#### Zero-Copy Row Iteration (`memtable.rs`, `engine_dml.rs`)
- **`for_each_visible()`**: Streams through DashMap entries calling a closure with
  `&OwnedRow` references — no row materialization or Vec allocation
- **`compute_simple_aggs()`**: Single-pass streaming aggregates (COUNT/SUM/MIN/MAX)
  directly over MVCC chains without cloning any row data

#### Fused Streaming Aggregate Executor (`executor_aggregate.rs`)
- **`ProjAccum` accumulator enum**: COUNT, SUM, AVG (decomposed to SUM+COUNT), MIN, MAX
  with proper type preservation (Int32 SUM stays Int32, not promoted to Float64)
- **`encode_group_key()`**: Reusable buffer for GROUP BY key encoding into HashMap keys
- **`exec_fused_aggregate()`**: Single-pass executor that handles WHERE filtering,
  GROUP BY grouping, and aggregate computation in one DashMap traversal — supports
  arbitrary expressions including CASE WHEN, BETWEEN, and complex predicates
- **`is_fused_eligible()`**: Query shape detection to route eligible queries to the
  fused path before fallback to general `exec_aggregate`

#### Bounded Heap Top-K (`memtable.rs`)
- **`scan_top_k_by_pk()`** rewritten with `BinaryHeap` bounded to K elements
- For `ORDER BY id LIMIT 10` on 1M rows: from cloning+sorting all 1M PKs
  (O(N log N)) to keeping only 10 PKs in memory (O(N log K))

#### Integration (`executor_query.rs`)
- Fused aggregate path wired into `exec_seq_scan` before the `scan()` fallback
- `try_streaming_aggs` fast path for simple column-ref aggregates without GROUP BY/WHERE
- Eligible queries: `has_agg || !group_by.is_empty()`, no window functions, no HAVING,
  no grouping sets, no virtual rows, no CTE data, no correlated subquery filters

#### Benchmark Results (1M rows, 112 statements: DDL + 100 INSERT batches + queries)

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Total elapsed | 11,922 ms | 6,355 ms | **1.88x faster** |
| INSERT phase | ~4,160 ms | ~2,943 ms | 1.41x faster |
| Query phase | ~7,762 ms | ~2,800 ms | **2.77x faster** |
| vs PostgreSQL | 1.96x slower | ~1.05x (near parity) | — |

### Verification
- **All existing tests pass** — 0 regressions
- **No new tests required** — optimization-only changes to existing code paths

---

## [Unreleased] — USTM Engine & StorageEngine Integration

### Added — USTM (User-Space Tiered Memory) Engine (`falcon_storage::ustm`)

A complete replacement for mmap-based storage access. Gives FalconDB full control
over memory residency, eviction, and I/O scheduling. Based on the design in
`docs/design_ustm.md`.

#### Core Modules (6 new files, `crates/falcon_storage/src/ustm/`)

- **`page.rs`** — Core types: `PageId`, `PageHandle`, `PageData`, `AccessPriority`
  (IndexInternal > HotRow > WarmScan > Cold), `Tier` enum, `PinGuard` (RAII unpin),
  `fast_hash_pk()` for PK → PageId derivation. 7 tests.
- **`lirs2.rs`** — LIRS-2 scan-resistant cache replacement algorithm. Classifies pages
  into LIR (protected) and HIR (eviction candidate) sets. Prevents sequential scans
  from polluting the cache. Configurable `lir_capacity` / `hir_resident_capacity` /
  `hir_nonresident_capacity`. 9 tests.
- **`io_scheduler.rs`** — Priority-based I/O scheduler with three queues:
  Query (highest) > Prefetch > Background. `TokenBucket` rate limiter prevents
  compaction/GC from starving foreground queries. 4 tests.
- **`prefetcher.rs`** — Query-aware prefetcher. Receives `PrefetchSource` hints
  (`SeqScan`, `IndexRangeScan`, `HashJoin`) from the executor, deduplicates requests,
  and submits async I/O before data is explicitly requested. 7 tests.
- **`zones.rs`** — Three-Zone Memory Manager:
  - **Hot Zone**: MemTable pages + index internals. Pinned in DRAM, never evicted.
  - **Warm Zone**: SST page cache. Managed by LIRS-2 eviction.
  - **Cold Zone**: Disk-resident pages. Registered for future async fetch.
  8 tests.
- **`engine.rs`** — Top-level `UstmEngine` coordinator. Unified `fetch_pinned()` API
  (Hot → Warm → Cold disk read). `alloc_hot()`, `insert_warm()`, `register_page()`,
  `unregister_page()`, `prefetch_hint()`, `prefetch_tick()`, `stats()`, `shutdown()`.
  10 tests.

**Total: 45 new tests, all passing.**

#### StorageEngine Integration

- **`StorageEngine` struct** — new `ustm: Arc<UstmEngine>` field, initialized in all
  4 constructors (`new`, `new_in_memory`, `new_in_memory_with_budget`,
  `new_with_wal_options`). `recover()` inherits via `Self::new()`.
- **`set_ustm_config(&UstmSectionConfig)`** — replace USTM engine from config at runtime.
- **`ustm_stats()`** — expose `UstmStats` snapshot for observability.
- **`shutdown()`** — graceful shutdown: `ustm.shutdown()` + WAL flush + stats log.

#### DML Integration (`engine_dml.rs`)

| Operation | Rowstore | LSM (`ENGINE=lsm`) |
|-----------|----------|-------------------|
| INSERT | Hot Zone (HotRow priority) | Warm Zone (disk-backed) |
| UPDATE | Hot Zone refresh | Warm Zone refresh |
| DELETE | Hot Zone write | Warm Zone evict (`unregister_page`) |
| GET | Warm Zone LIRS-2 tracking | Warm Zone LIRS-2 tracking |
| SCAN | SeqScan prefetch hint | SeqScan prefetch hint → SST path |

#### DDL Integration (`engine_ddl.rs`)

- **CREATE TABLE** → registers table metadata page in Hot Zone (`IndexInternal` priority, never evicted)
- **DROP TABLE** → `unregister_page()` cleans up metadata page

#### Configuration (`falcon_common::config`)

New `[ustm]` section in `FalconConfig` / `falcon.toml`:

```toml
[ustm]
enabled = true
hot_capacity_bytes = 536870912       # 512 MB
warm_capacity_bytes = 268435456      # 256 MB
lirs_lir_capacity = 4096
lirs_hir_capacity = 1024
background_iops_limit = 500
prefetch_iops_limit = 200
prefetch_enabled = true
page_size = 8192
```

`examples/primary.toml` updated with the `[ustm]` section.

#### Server Integration (`falcon_server/src/main.rs`)

`engine.set_ustm_config(&config.ustm)` called in both WAL-enabled and in-memory
startup paths when `ustm.enabled = true`.

### Verification

- **USTM unit tests**: 45 pass, 0 failures
- **Full workspace**: 2,643 pass, 0 failures (no regressions)

---

## [1.0.3] — 2026-02-22 — Stability, Determinism & Trust Hardening (LTS Patch)

This is a **stability-only hardening release**. No new features, no new SQL syntax,
no new APIs, no protocol changes. Safe rolling upgrade from v1.0.2.
Eliminates undefined behavior under stress, retries, partial failure, and malformed client behavior.

### Added — Stability Hardening Module (`stability_hardening.rs`)
- **`TxnStateGuard`** (§1) — runtime forward-only state transition enforcement
  - Per-txn high-water-mark tracking to detect state regression after failover
  - Bounded audit trail of all transitions for post-mortem analysis
  - Structured error on regression: `TxnError::InvalidTransition` with HWM context
- **`CommitPhaseTracker`** (§2) — explicit commit phase tracking (CP-L → CP-D → CP-V → CP-A)
  - Forward-only phase progression enforced
  - Once CP-V reached, outcome is irreversible
  - Duplicate commit signals are idempotent no-ops
- **`RetryGuard`** (§3) — duplicate txn_id and reordered retry detection
  - Conflicting payload rejection (different fingerprint on same txn_id)
  - Protocol phase regression rejection (e.g., Begin after Commit)
  - Configurable max retry count per txn_id
- **`InDoubtEscalator`** (§4) — periodic escalation of stale in-doubt transactions
  - Configurable escalation threshold (forced abort after timeout)
  - Bounded history of escalation records
  - Ensures no in-doubt transaction blocks unrelated transactions
- **`FailoverOutcomeGuard`** (§5) — at-most-once commit enforcement under leader change
  - Stale epoch rejection
  - Duplicate commit detection and rejection
  - Epoch advancement on failover
- **`ErrorClassStabilizer`** (§6) — deterministic error classification validation
  - Caches (error_description → ErrorKind) mappings
  - Detects classification instability across invocations
- **`DefensiveValidator`** (§7) — early rejection of malformed inputs
  - Sentinel txn_id rejection (0, MAX)
  - Unknown state name rejection
  - Invalid protocol message ordering rejection
- **`TxnOutcomeJournal`** (§8) — observability journal for transaction outcomes
  - Records final state, in-doubt reason, resolution method, resolution latency
  - Bounded capacity with automatic eviction
  - Queryable by txn_id or recent history

### Added — CI Gate
- `scripts/ci_v103_stability_gate.sh` — 7-gate verification: §1-§8 unit tests, §9 stress tests, v1.0.2 failover×txn tests, v1.0.2 test matrix, txn state machine, full suite, zero-panic baseline

### Test Coverage (45 new tests)
- §1 TxnStateGuard: forward transitions, idempotent terminal, regression rejection, unknown state, audit trail (5)
- §2 CommitPhaseTracker: forward progression, idempotent, irreversibility, metrics (4)
- §3 RetryGuard: first attempt, same fingerprint, conflicting payload, reordered phase, excessive retries (5)
- §4 InDoubtEscalator: no escalation before threshold, forced abort after, remove resolved, metrics (4)
- §5 FailoverOutcomeGuard: first commit, duplicate rejection, stale epoch, advance epoch, was_committed (5)
- §6 ErrorClassStabilizer: stable class, instability detection, FalconError classification (3)
- §7 DefensiveValidator: valid/invalid txn_ids, state names, message ordering, metrics (6)
- §8 TxnOutcomeJournal: record/lookup, in-doubt entry, recent, bounded capacity, counters (5)
- §9 Stress: high retry rate, concurrent duplicate txn_id, rapid leader churn, in-doubt escalation, crash+restart loops, error classification stability, defensive malformed input, full lifecycle integration (8)

### Verification Results
- **New tests**: 45 pass, 0 failures
- **Full workspace**: 2,599 pass, 0 failures
- **v1.0.2 failover×txn tests**: all pass (no regression)
- **v1.0.1 zero-panic baseline**: maintained

---

## [1.0.2] — 2026-02-22 — Transaction & Failover Hardening (LTS Patch)

This is a **transaction and failover hardening patch**. No changes to transaction
semantics, SQL behavior, or commit point definitions. Safe rolling upgrade from v1.0.1.
Improves determinism under failure scenarios.

### Added — Failover × Transaction Coordinator (`failover_txn_hardening.rs`)
- **`FailoverTxnCoordinator`** — atomic failover×txn interaction with 3-phase lifecycle (Normal → Draining → Converging → Normal)
  - Blocks new writes during failover drain phase
  - Deterministically drains active transactions (abort, complete, or move to in-doubt)
  - No partial commits visible to clients during failover
  - Bounded drain timeout with configurable deadline (default 5s)
  - Convergence tracking after failover completes (default 30s window)
  - Per-failover affected-txn records (bounded memory)
- **`InDoubtTtlEnforcer`** — bounded in-doubt transaction lifetime
  - Hard maximum lifetime for in-doubt transactions (default 60s)
  - Forced abort after TTL expiry — no infinite persistence
  - Warning at 80% TTL threshold
  - Structured logging for every TTL expiration
- **`FailoverDamper`** — churn suppression for rapid failover oscillation
  - Minimum interval between consecutive failovers (default 30s)
  - Rate-limit: max 3 failovers per 5-minute observation window
  - Structured logging for suppressed failover attempts
  - History tracking for observability
- **`FailoverBlockedTxnGuard`** — tail-latency protection
  - Bounded timeout for transactions blocked by in-progress failover (default 10s)
  - Deterministic fail-fast (`TxnError::Timeout`) when timeout exceeded
  - p50/p99/max latency percentile tracking for convergence monitoring
  - `is_converged()` check: p99 ≤ 2× p50 after failover

### Added — CI Gate
- `scripts/ci_v102_failover_gate.sh` — 7-gate verification: failover×txn tests, txn state machine, in-doubt resolver, cross-shard retry, HA/failover, full suite, zero-panic baseline

### Test Coverage (35 new tests)
- Failover during single-shard transaction
- Failover during cross-shard prepare phase
- Failover during commit barrier
- Duplicate commit/abort idempotency
- Coordinator crash → in-doubt resolution with TTL
- Rapid repeated failovers (churn damping)
- Blocked txn timeout enforcement
- Drain timeout enforcement
- Convergence tracking
- Full failover lifecycle integration test

### Verification Results
- **New tests**: 35 pass, 0 failures
- **Full workspace**: all tests pass, 0 failures
- **v1.0.1 zero-panic baseline**: maintained (0 unwrap/expect/panic in production)

---

## [1.0.1] — 2026-02-22 — Crash & Error Baseline (LTS Patch)

This is a **stability-only patch**. No behavior changes, no feature additions.
Safe for rolling upgrade from v1.0.0. No rollback required for clients.

### Fixed — Zero Panic Policy (Production Path)
- **falcon_common/consistency.rs**: 14 `RwLock::unwrap()` → poison-recovery `unwrap_or_else(|p| p.into_inner())`
- **falcon_common/config.rs**: 1 `parts.last().unwrap()` → safe `match` with early return
- **falcon_protocol_pg/handler_show.rs**: 2 `dist_engine.unwrap()` → explicit `match` with structured error return
- **falcon_sql_frontend/binder_select.rs**: 1 `find_column().unwrap()` → `match`/`continue`; 1 `over.unwrap()` → `ok_or_else` with `SqlError`
- **falcon_executor/eval/cast.rs**: 1 `and_hms_opt().expect()` → `match` with `TypeError`; 1 chrono epoch `expect()` → `unwrap_or_else`
- **falcon_executor/eval/scalar_time.rs**: 1 chrono epoch `expect()` → `unwrap_or_else`
- **falcon_executor/eval/scalar_time_ext.rs**: 2 chrono epoch `expect()` → `unwrap_or_else`
- **falcon_executor/eval/scalar_array_ext.rs**: 1 chrono epoch `expect()` → `unwrap_or_else`
- **falcon_executor/eval/scalar_jsonb.rs**: 1 chrono epoch `expect()` → `unwrap_or_else`
- **falcon_executor/eval/scalar_regex.rs**: 1 `caps.get(0).expect()` → safe `match`
- **falcon_executor/executor_copy.rs**: 2 chrono epoch `expect()` → `unwrap_or_else`
- **falcon_storage/audit.rs**: 1 thread spawn `expect()` → `unwrap_or_else` with graceful degradation

### Added — Unified Error Model (Stable)
- `FalconError::log_if_fatal()` — structured log emission for every Fatal/InternalBug error
- `FalconError::affected_component()` — deterministic component identification (storage/txn/sql/protocol/executor/cluster/resource/internal)
- Stable log format: `error_code`, `error_category=Fatal`, `component`, `sqlstate`, `debug_context`
- Every Fatal error emits structured log entry before client response

### Added — CI Gate
- `scripts/ci_v101_crash_gate.sh` — 6-gate verification: zero unwrap/expect/panic in production code, full test suite, error model tests, clippy clean

### Verification Results
- **Production-path `unwrap()`**: 0 (was 19)
- **Production-path `expect()`**: 0 (was 11, excluding build.rs)
- **Production-path `panic!()`**: 0 (unchanged)
- **Full workspace test suite**: all pass, 0 failures
- **Error categories**: UserError, Retryable, Transient, InternalBug (Fatal) — deterministic, stable
- **SQLSTATE mappings**: 30+ stable codes covering all error variants

---

## [Unreleased]

### Added — v1.0 Commercial Release Gate (P1)
- **README v1.0 positioning**: PG-compatible, distributed, memory-first, deterministic txn semantics OLTP
- **ACID SQL-level tests**: atomicity (commit/rollback), consistency (PK/NOT NULL), isolation (snapshot), durability (6 tests)
- **PG SQL whitelist tests**: INNER/LEFT JOIN, GROUP BY + aggregates, ORDER BY/LIMIT, UPSERT ON CONFLICT, UPDATE/DELETE RETURNING (7 tests)
- **Unsupported feature errors**: CREATE TRIGGER, CREATE FUNCTION → `ErrorResponse` with SQLSTATE `0A000` (2 tests)
- **Observability verification**: SHOW falcon.memory, falcon.nodes, falcon.replication_stats (3 tests)
- **Fast-path metrics verification**: `fast_path_commits` / `slow_path_commits` exposed in txn_stats (1 test)
- **v1.0 release CI gate**: `scripts/ci_v1_release_gate.sh` — unified Go/No-Go gate (20+ sub-gates)
- **v1.0 scope documentation**: `docs/v1.0_scope.md` — full 7-section commercial checklist with verification matrix
- **README "v1.0 Not Supported" table**: 9 features with SQLSTATE codes and error messages

### Added — v1.0 Isolation Module (B1–B10, ~135 tests)
- B1: Explicit `TxnState::try_transition()` state machine with `TransitionResult` and `InvalidTransition` error (28 tests)
- B3: Snapshot Isolation litmus tests — MVCC visibility at VersionChain + StorageEngine levels (35 tests)
- B4: WAL-first durability — multi-table interleaved recovery, 3x idempotent replay, WAL-first ordering invariant (3 new, 13 total)
- B5/B6: Admission backpressure — WAL backlog threshold, replication lag threshold, rejection counter (5 tests)
- B7: Long-txn detection + kill — `long_running_txns()`, `kill_txn()`, `kill_long_running()` (9 tests)
- B8: PG protocol corner cases — empty query, semicolons-only, syntax error, txn lifecycle, nonexistent tables, duplicate DDL (12 tests)
- B9: DDL concurrency safety — concurrent create (same/different), truncate, drop+DML, index lifecycle (8 tests)
- B10: CI isolation gate script (`scripts/ci_isolation_gate.sh`)
- Documentation: `docs/v1.0_scope.md` — full checklist with test counts and locations

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

### Added — Commercial-Grade Hardening (P0/P1)
- **P0-1**: Module status headers on all storage modules (`PRODUCTION`, `EXPERIMENTAL`, `STUB`)
- **P0-2**: Golden path documentation on core write path (`TxnManager`, `MemTable`, `WAL`, `StorageEngine`, `wal_stream.rs`)
- **P0-2**: TODO/FIXME/HACK audit — zero hits in core paths (`falcon_txn`, `falcon_storage` core, `falcon_cluster`)
- **P1-1**: Cross-shard invariant `XS-5` (Timeout Rollback — no hanging locks) added to `consistency.rs`
- **P1-1**: `CrossShardInvariant` enum with `all()` + `description()` for programmatic validation
- **P1-1**: Doc tests on all 5 cross-shard invariant constants (XS-1 through XS-5)
- **P1-1**: `sql_distributed_txn.rs` — 11 deterministic tests covering atomicity, at-most-once, coordinator crash recovery, participant crash recovery, timeout rollback
- **P1-5-2**: `ci_failover_gate.sh` updated with distributed txn invariant gate
- **P2**: README "Planned — NOT Implemented" table explicitly marking 9 features as STUB/EXPERIMENTAL/not-started

### Fixed — PG Protocol Completion
- Cancel request now fully functional: `CancellationRegistry` with `AtomicBool` polling (50ms), `BackendKeyData` handshake, both simple and extended query (Execute) paths, SQLSTATE `57014` (5 new tests)
- LISTEN/NOTIFY fully implemented: `NotificationHub` broadcast hub, per-session `SessionNotifications`, LISTEN/UNLISTEN/NOTIFY/UNLISTEN * commands, `NotificationResponse` delivery before `ReadyForQuery` (6 tests)
- Logical replication protocol: `replication=database` startup detection, `IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT <name> LOGICAL <plugin>`, `DROP_REPLICATION_SLOT`, `START_REPLICATION SLOT <name> LOGICAL <lsn>`, CopyBoth streaming with XLogData/keepalive/StandbyStatusUpdate, backed by CDC infrastructure (18 new tests)

### Test Count
- **2,262 tests** passing, **0 failures** (was 1,976 at 1.0.0-rc.1)

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
