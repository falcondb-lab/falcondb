# Changelog

All notable changes to FalconDB are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.0] — GA Release

### Added — SQL Compatibility

- **SQL-level PREPARE/EXECUTE/DEALLOCATE** — plan-based execution path using
  `prepare_statement()` for physical plan generation and typed parameter inference.
  `text_params_to_datum` helper converts SQL EXECUTE text params to typed Datum values.
  Falls back to legacy text substitution when planning is unavailable.
- **SMALLINT (INT2) data type** — `DataType::Int16` across schema, DDL, parser, PG OID (21),
  wire protocol (text + binary decode), COPY, CAST. Runtime values widened to `Datum::Int32`.
- **REAL (FLOAT4) data type** — `DataType::Float32` across schema, DDL, parser, PG OID (700),
  wire protocol (text + binary decode), COPY, CAST. Runtime values widened to `Datum::Float64`.

### Added — Linux Packaging

- **`packaging/systemd/falcondb.service`** — production-hardened systemd unit with
  `ProtectSystem=strict`, `PrivateDevices`, `OOMScoreAdjust=-900`, journald logging.
- **Debian packaging** — `packaging/debian/` (control, postinst, prerm, conffiles) +
  `scripts/build_deb.sh` builder. Creates `falcondb` system user, enables systemd service.
- **RPM packaging** — `packaging/rpm/falcondb.spec` + `scripts/build_rpm.sh` builder.
  Pre-install user creation, `%systemd_post`/`%systemd_preun` macros.
- **`docs/INSTALL_LINUX.md`** — installation guide for .deb, .rpm, and tarball methods.

### Changed — Release Pipeline

- **`.github/workflows/release.yml`** — Linux build job now produces .tar.gz, .deb, and .rpm
  artifacts via dedicated build scripts. All three uploaded to GitHub Releases.

---

## [Unreleased]

### Added — DCG Failover PoC (`falcondb-poc-dcg/`)

- **`falcondb-poc-dcg/`** — Customer-facing, black-box PoC proving FalconDB's
  Deterministic Commit Guarantee (DCG) under crash failover. Designed for
  customer PoC, investor demo, technical due diligence, and internal confidence.
- **`run_demo.sh` / `run_demo.ps1`** — One-command end-to-end demo: start cluster →
  write orders → kill -9 primary → promote replica → verify → PASS/FAIL verdict.
- **`workload/src/main.rs`** — Rust deterministic order writer: single-txn commits,
  logs order_id ONLY after server COMMIT confirmation. No batch, no async buffer.
- **`workload/order_writer.py`** — Python fallback workload generator (psycopg2).
- **Modular scripts** — `start_cluster`, `run_workload`, `kill_primary`,
  `promote_replica`, `verify_results`, `cleanup` — each step independently runnable,
  with both `.sh` and `.ps1` variants.
- **`scripts/verify_results.sh`** — Compares committed_orders.log vs database query
  on survivor. Checks for missing (data loss), phantom (uncommitted appeared),
  and duplicates. Produces `verification_report.txt` + `verification_evidence.json`.
- **`docs/explanation_for_customers.md`** — Non-technical explanation of commit,
  why databases lose data, what FalconDB guarantees, why the demo is trustworthy.
  No jargon (no MVCC, WAL, 2PC, consensus terms).
- **`schema/orders.sql`** — Simple order table (order_id BIGINT PK, created_at, payload).
- **`configs/`** — Primary + replica TOML (WAL enabled, fdatasync, TRUST auth).

### Added — pgbench Performance PoC (`falcondb-poc-pgbench/`)

- **`falcondb-poc-pgbench/`** — Customer-facing pgbench comparison: FalconDB vs
  PostgreSQL under identical conditions. Same hardware, same dataset, same
  concurrency, same durability settings. No shortcuts.
- **`run_benchmark.sh` / `run_benchmark.ps1`** — One-command orchestrator: env check →
  start both servers → init pgbench → warm-up + 3 measured runs each → collect →
  generate `results/report.md` comparison table.
- **Modular scripts** — `check_env`, `start_falcondb`, `start_postgres`,
  `init_pgbench`, `run_pgbench_falcon`, `run_pgbench_postgres`, `collect_results`,
  `cleanup` — each independently runnable, with `.sh` and `.ps1` variants.
- **`scripts/collect_results.sh`** — Parses pgbench TPS + latency from all runs,
  selects median, computes ratio, generates `summary.json` + `report.md`.
- **`conf/falcon.bench.toml`** — FalconDB config (WAL, fdatasync, TRUST, 200 conns).
- **`conf/postgres.bench.conf`** — PostgreSQL config (fsync=on, synchronous_commit=on,
  256MB shared_buffers, fdatasync).
- **`docs/benchmark_methodology.md`** — Full methodology: fairness constraints,
  hardware disclosure, anti-cheating checklist, known limitations, interpretation
  guidance. Designed for technical due diligence.

### Added — Failover Under Load PoC (`falcondb-poc-failover-under-load/`)

- **`falcondb-poc-failover-under-load/`** — Customer-facing PoC proving FalconDB
  correctness during primary crash under sustained OLTP write traffic. Crash
  happens during active writes, not after — the hardest test for any database.
- **`run_demo.sh` / `run_demo.ps1`** — One-command orchestrator: start cluster →
  verify replication → start sustained writer → crash primary during writes →
  promote replica → writer auto-reconnects → measure downtime → verify PASS/FAIL.
- **`workload/src/main.rs`** — Rust commit marker writer with automatic failover
  reconnect. Logs marker_id ONLY after COMMIT confirmation. Retries same marker
  on failure. Supports `STOP_FILE` for graceful termination.
- **Modular scripts** — `start_cluster`, `wait_cluster_ready`, `start_load`,
  `kill_primary`, `promote_replica`, `monitor_downtime`, `verify_integrity`,
  `cleanup` — each independently runnable, `.sh` and `.ps1` variants.
- **`scripts/verify_integrity.sh`** — Compares committed markers log vs database
  on survivor. Checks missing (data loss), phantom (uncommitted appeared), and
  duplicates. Produces `verification_report.txt` + `verification_evidence.json`.
- **`scripts/monitor_downtime.sh`** — Calculates failover window from kill to
  first successful post-promotion commit. Produces `failover_timeline.json`.
- **`docs/explanation_for_customers.md`** — Plain-language explanation of crash
  behavior, what databases get wrong, what FalconDB guarantees, why the demo
  is trustworthy.

### Added — Observability PoC (`falcondb-poc-observability/`)

- **`falcondb-poc-observability/`** — Customer-facing PoC proving FalconDB is
  operable in production: live Prometheus metrics, Grafana dashboard, and
  operational controls (pause/resume rebalance, trigger rebalance, failover).
- **`docker/docker-compose.yml`** — One-command monitoring stack: Prometheus
  (5s scrape) + Grafana with auto-provisioned datasource and dashboard.
- **`dashboards/falcondb_overview.json`** — Pre-built "FalconDB Cluster Overview"
  dashboard: 20 panels covering cluster health, transaction throughput, WAL
  latency, replication lag, rebalancer status, memory, failover, migrations.
  All metrics reference real `falcon_*` Prometheus metrics from falcon_observability.
- **Operational scripts** — `start_cluster`, `start_monitoring`, `generate_activity`,
  `trigger_rebalance`, `pause_rebalance`, `resume_rebalance`, `simulate_failover`,
  `cleanup` — each with `.sh` and `.ps1` variants.
- **`docs/explanation_for_customers.md`** — "Black box DB vs Observable DB",
  metric-to-real-world mapping, operator decision scenarios, dashboard layout.

### Added — Migration PoC (`falcondb-poc-migration/`)

- **`falcondb-poc-migration/`** — Customer-facing PoC proving FalconDB can be
  adopted as a drop-in replacement for PostgreSQL OLTP workloads with minimal
  migration friction. Focus: adoption cost transparency, not performance.
- **`app/demo_app.py`** — Standard psycopg2 application: create order, update
  status, query by PK, list recent orders. Same code, same SQL, two configs.
- **`scripts/migrate_schema.sh`** — Exports PG schema via `pg_dump --schema-only`,
  applies to FalconDB, records applied/failed objects, generates migration report.
- **`scripts/migrate_data.sh`** — Exports PG data via `pg_dump --data-only`,
  imports into FalconDB, verifies row counts match per table, records timing.
- **`scripts/run_app_smoke_test.sh`** — Runs identical workflows against both
  PostgreSQL and FalconDB, produces side-by-side PASS/FAIL comparison table.
- **`output/incompatibilities.json`** — Structured list of known incompatibilities
  with impact level, affected SQL, workaround, and roadmap status.
- **`schema/falcon_compat_notes.md`** — Detailed compatibility boundary document:
  supported features, unsupported features, migration decision checklist.
- All scripts in `.sh` and `.ps1` variants.

### Added — Cost Efficiency PoC (`falcondb-poc-cost-efficiency/`)

- **`falcondb-poc-cost-efficiency/`** — Customer-facing PoC proving FalconDB
  delivers the same OLTP correctness and durability with significantly lower
  resource consumption. Focus: cost-performance ratio and predictability.
- **`workload/src/main.rs`** — Rust rate-limited OLTP workload generator:
  transactional balance transfers at fixed tx/s, latency distribution (p50/p95/p99),
  same binary for both databases.
- **`scripts/collect_resource_usage.sh`** — Per-process CPU/memory/disk IO sampler
  with JSON output (Linux /proc + macOS ps + Windows Get-Process).
- **`scripts/simulate_peak.sh`** — 3-phase peak test: normal → 2x spike → recovery,
  system must recover without restart.
- **`scripts/generate_cost_report.sh`** — Produces Markdown cost comparison with
  AWS on-demand pricing, resource usage tables, and savings calculation.
- **`conf/falcon.small.toml`** — FalconDB small footprint: 512 MB soft / 768 MB hard,
  WAL+fsync enabled, no unsafe optimizations.
- **`conf/postgres.large.conf`** — PostgreSQL production config: 4 GB shared_buffers,
  12 GB cache, fsync on, not artificially weakened.
- **`docs/explanation_for_customers.md`** — Over-provisioning problem, deterministic
  execution model, peak behavior comparison, cost savings math.
- All scripts in `.sh` and `.ps1` variants.

### Added — Backup & PITR PoC (`falcondb-poc-backup-pitr/`)

- **`falcondb-poc-backup-pitr/`** — Customer-facing PoC proving FalconDB can
  safely recover data after catastrophic failure and restore to an exact
  point in time. Focus: recoverability and trust after disaster.
- **`schema/accounts.sql`** — Simple accounts table with deterministic seed
  data (100 accounts, known starting balances).
- **`scripts/generate_data.sh`** — Deterministic transaction generator: one
  UPDATE per transaction, each explicitly committed, logged externally for
  verification.
- **`scripts/take_backup.sh`** — Full consistent backup with JSON manifest
  (backup time, WAL position, checksum, row count, balance).
- **`scripts/record_timestamp.sh`** — Captures precise recovery target T1
  with per-account balance snapshot for later verification.
- **`scripts/induce_disaster.sh`** — Irreversible database destruction:
  kills process, deletes entire data directory.
- **`scripts/restore_from_backup.sh`** — Restores base snapshot, validates
  backup integrity, prepares for WAL replay.
- **`scripts/replay_wal_until.sh`** — Starts FalconDB in recovery mode,
  WAL replay advances database to recovery target T1.
- **`scripts/verify_restored_data.sh`** — Per-account balance comparison
  against T1 snapshot, outputs PASS/FAIL with detailed report.
- **`docs/explanation_for_customers.md`** — Plain-language explanation of
  backup vs PITR, "video rewind" analogy, compliance coverage.
- All scripts in `.sh` and `.ps1` variants.

### Added — MES Work Order System (`falcondb-mes-workorder/`)

- **`falcondb-mes-workorder/`** — Real business system: MES work order +
  operation reporting. Proves FalconDB's DCG through manufacturing semantics:
  "Once the system confirms a report, that production fact never disappears."
- **`schema/init.sql`** — 4 core tables: work_order, operation,
  operation_report (append-only fact ledger), work_order_state_log (audit).
- **`backend/app.py`** — FastAPI REST service with 12 endpoints: work order
  CRUD, operation lifecycle (start/report/complete), invariant verification.
- **`backend/invariants.py`** — 3 business invariant checks: forward-only
  operation status, monotonic reported qty, irreversible completion.
- **`scripts/run_demo.sh`** — Three scenarios: normal production, failover
  during production (kill -9 + recover + verify), concurrent reporting.
- **`scripts/kill_primary.sh`** — Hard crash simulation (SIGKILL / Stop-Process).
- **`scripts/verify_business_state.sh`** — Post-failover verification with
  before/after comparison, human-readable bilingual report.
- **`docs/explanation_for_customers.md`** — Plain-language (Chinese + English)
  explanation for factory management, no database terminology.
- All scripts in `.sh` and `.ps1` variants.

---

## [1.2.1] — Production Readiness, Executor Optimization, Cluster Observability

### Track A — OS-Level Production Tuning

- **`docs/os/windows_server_2022.md`** — Windows Server 2022 tuning guide covering
  IOCP backend, NTFS vs ReFS, antivirus exclusions, Large Pages, TCP RSS/autotuning,
  Windows Service deployment checklist, security baseline (ACLs, service account).
  Includes 10-minute production checklist.
- **`docs/os/rhel_9.md`** — RHEL 9 tuning guide covering XFS/ext4 selection,
  io_uring (with privilege constraints), NUMA pinning, THP/jemalloc, systemd +
  cgroup v2 resource control, BBR/TCP tuning, SELinux and firewall configuration.
  Includes 10-minute production checklist.

### Track B — Executor Hot-Path Optimization

- **`cmp_datum_values` direct dispatch** — Time, Decimal (normalized scale), UUID,
  Bytea, Interval (microsecond approximation), and NULL ordering all handled via
  native comparison. `format!()` fallback restricted to Jsonb, Array, and rare
  cross-type comparisons.
- **`cmp_datum_sort` with configurable NULL ordering** — NULLS FIRST / NULLS LAST
  handled without allocation.

### Track C — Cluster Rebalancer Control & Metrics

- **`ShardRebalancer::pause()` / `resume()` / `is_paused()`** — SLA-safe pause
  prevents new rebalance tasks without interrupting in-flight migrations.
- **`ShardRebalancer::metrics_snapshot() -> RebalancerMetrics`** — point-in-time
  snapshot: runs_completed, total_rows_migrated, is_running, is_paused,
  last_imbalance_ratio, last_completed_tasks, last_failed_tasks, last_duration_ms,
  shard_move_rate.
- **`RebalancerMetrics`** exported publicly from falcon_cluster crate.
- New tests: `test_resume_allows_rebalance`, `test_metrics_snapshot_consistency`.

### Track D — Observability Enhancements (Prometheus)

- **Segment streaming metrics** — `record_segment_streaming_metrics()`: handshakes,
  segments_streamed, segment_bytes, tail_bytes, checksum_failures, error_rollbacks,
  snapshots_created.
- **Rebalancer metrics export** — `record_rebalancer_metrics()`: all 9
  `RebalancerMetrics` fields mapped to `falcon_rebalancer_*` gauges.

### Track E — Benchmark Baseline & Evidence

- **`docs/benchmarks/v1.2_baseline.md`** — workloads W1–W5, hardware disclosure,
  FalconDB config, v1.0 → v1.2 performance delta summary, throughput + latency
  interpretation notes.
- **`evidence/INDEX.md`** — E13 (v1.2 performance baseline), E14 (OS tuning guides),
  E15 (cluster observability & rebalance controls).

---

## [Unreleased]

### Added — Benchmark Suite

- **`scripts/bench_pgbench_vs_postgres.sh`** — pgbench comparison benchmark
  (FalconDB vs PostgreSQL). Configurable scale, concurrency, duration, mode.
  Produces `bench_out/<ts>/REPORT.md` + JSON summary + raw logs + environment snapshot.
- **`scripts/bench_pgbench_vs_postgres.ps1`** — Windows PowerShell variant with
  identical semantics (PowerShell 7+, no admin rights required).
- **`scripts/bench_failover_under_load.sh`** — 2-node primary/replica failover
  under sustained write load. Kills primary, promotes replica, validates no
  phantom commits via tx marker table. Explicit PASS/FAIL verdict.
- **`scripts/bench_kernel_falcon_bench.sh`** — standardized internal kernel
  benchmark wrapper for `falcon_bench`. Produces structured output under
  `bench_out/<ts>/kernel_bench/`.
- **`scripts/ci_bench_smoke.sh`** — CI smoke test ensuring scripts exist,
  parse correctly, and produce expected artifacts. Not a performance test.
- **`docs/benchmark_methodology.md`** — non-negotiable fairness rules, hardware
  disclosure requirements, warm-up/run-count policy, interpretation guidance,
  quick-start reproduction steps.
- **`docs/benchmarks/RESULTS_TEMPLATE.md`** — standard report skeleton with
  environment block, results tables, exact commands, and notes section.
- **`bench_configs/falcon.bench.toml`** — FalconDB config tuned for fair
  benchmarking (WAL enabled, fdatasync, TRUST auth).

### Added — SCRAM-SHA-256 Authentication

- **SCRAM-SHA-256 SASL authentication** — PostgreSQL-compatible SCRAM-SHA-256 (RFC 5802/7677)
  for PG wire protocol. Replaces the broken inline implementation with a proper state-machine
  engine (`auth::scram::ScramServerSession`) using `hmac`/`sha2` crates, constant-time proof
  verification, and cryptographically secure nonce generation (`rand`).
- **Stored verifier support** — `ScramVerifier` parses/serializes PostgreSQL SCRAM format
  (`SCRAM-SHA-256$<iter>:<salt_b64>$<stored_key_b64>:<server_key_b64>`). Verifiers can be
  pre-computed and stored in config; plaintext password never persisted.
- **SASL binary codec** — `decode_sasl_initial_response()` and `decode_sasl_response()` in
  `codec.rs` properly parse SASLInitialResponse (mechanism + binary data) and SASLResponse
  (raw SASL bytes) from PG wire `'p'` messages, fixing compatibility with psql and pgAdmin.
- **User credential config** — `AuthConfig.users: Vec<UserCredential>` for named SCRAM users
  with stored verifiers or plaintext passwords. `AuthConfig.allow_cidrs` for CIDR allowlisting.
- **24 new tests** — 14 SCRAM engine unit tests (verifier roundtrip, full handshake, wrong
  password, bad mechanism, constant-time eq, PBKDF2 vectors) + 10 codec/integration tests
  (SASL encode/decode, wire-level handshake with verifier, config user lookup).
- **`docs/authentication.md`** — TRUST vs SCRAM-SHA-256 guide, protocol flow, verifier format,
  config reference, psql/pgAdmin connection instructions.

### Added — OS Platform Tuning Guides

- **`docs/os/windows_server_2022.md`** — Windows Server 2022 production tuning guide
  covering IOCP WAL backend, NTFS/ReFS selection, Large Pages, antivirus exclusions,
  Windows Service deployment, TCP/RSS tuning, ACL security, and monitoring checklist.
- **`docs/os/rhel_9.md`** — RHEL 9 / Rocky Linux 9 production tuning guide covering
  io_uring (with RHEL 9.3+ unprivileged restrictions), XFS/ext4 tuning, NUMA binding,
  jemalloc/THP, tuned profile, systemd/cgroup v2 service unit, BBR congestion control,
  SELinux integration, and firewalld service definition.

### Changed — Executor Performance

- **`vectorized.rs`**: `datum_cmp` now delegates to `cmp_datum_values` instead of
  double `format!()` allocation. `cmp_datum_values` extended with direct comparisons
  for `Time`, `Decimal` (scale-normalized), `Uuid`, `Bytea`, `Interval` (total µs
  approximation), and `Null`. `format!()` fallback now only fires for `Array`/`Jsonb`
  or genuinely rare cross-type comparisons.

### Added — Cluster Enhancements

- **`rebalancer.rs`**: `ShardRebalancer` gains SLA-safe `pause()`/`resume()`/`is_paused()`
  — paused state skips rebalance cycles without interrupting in-flight migrations.
  New `metrics_snapshot()` returns `RebalancerMetrics` for Prometheus integration.
- **`RebalancerMetrics`** struct: runs_completed, total_rows_migrated, is_running,
  is_paused, last_imbalance_ratio, completed/failed tasks, duration, move rate.
- 2 new tests: `test_pause_prevents_rebalance`, `test_metrics_snapshot`.
- `RebalancerMetrics` exported from `falcon_cluster::lib.rs`.

### Added — Observability

- **`falcon_observability`**: `record_segment_streaming_metrics()` — 7 Prometheus
  gauges for segment replication (handshakes, segments streamed, bytes, checksums,
  rollbacks, snapshots).
- **`falcon_observability`**: `record_rebalancer_metrics()` — 9 Prometheus gauges
  for shard rebalancing (runs, rows migrated, running/paused state, imbalance ratio,
  task counts, duration, move rate).

### Added — Benchmarks & Evidence

- **`docs/benchmarks/v1.2_baseline.md`** — v1.2 performance baseline with 5 workloads
  (W1–W5), v1.0→v1.2 delta comparison, optimization summary table, and monitoring
  section referencing new Prometheus metrics.
- **`evidence/INDEX.md`** — E13 (v1.2 baseline), E14 (OS tuning guides), E15 (cluster
  observability) evidence categories added.

---

## [Unreleased] — TDE, Docker, Linux Packaging

### Added — Transparent Data Encryption (TDE)

- **Real AES-256-GCM encryption** in `falcon_storage::encryption` — replaces XOR stub.
  - `KeyManager`: PBKDF2-HMAC-SHA256 master key derivation (600k iterations),
    DEK generation/wrap/unwrap, master key rotation with DEK re-wrap.
  - `EncryptionKey`: CSPRNG key generation via OS entropy (`getrandom`).
  - `encrypt_block` / `decrypt_block`: per-block AES-256-GCM with random 12-byte nonces.
  - `encrypt_block_with_nonce` / `decrypt_block_with_nonce`: deterministic nonce variant
    for WAL records keyed by LSN.
  - `TdeError` with `DekNotFound`, `DecryptionFailed`, `EncryptionFailed` variants.
  - 23 unit tests covering roundtrip, tamper detection, key isolation, rotation, edge cases.
- **WAL encryption integration** — `WalEncryption` cached cipher context.
  - `WalWriter::set_encryption()` enables transparent per-record encryption on write.
  - `WalReader::read_all_encrypted()` / `read_from_segment_encrypted()` for decryption on read.
  - On-disk format unchanged: `[len:4][crc:4][payload]` where payload is
    `nonce(12) || ciphertext+tag` when TDE is active.
  - 5 WAL encryption tests: roundtrip, wrong-key rejection, no-decryption rejection,
    segment rotation, full KeyManager integration.
- **New crate dependencies**: `aes-gcm 0.10`, `rand 0.8`, `pbkdf2 0.12`, `hmac 0.12`.

### Added — Docker & Linux Packaging

- **`Dockerfile`** — multi-stage build (rust:1.75-bookworm builder + debian:bookworm-slim runtime).
  - Dependency caching layer, stripped release binaries, non-root `falcondb` user.
- **`docker-compose.yml`** — standalone + optional primary/replica cluster profiles.
- **`docker/falcon.toml`** — container-optimized default config.
- **`scripts/build_linux_dist.sh`** — Linux tarball builder with systemd unit, install script.
- **`.dockerignore`** — excludes target/, .git/, benchmarks/results/.

### Added — Documentation

- **`docs/INDEX.md`** — comprehensive documentation index organizing 90+ files by topic.

---

## [Unreleased] — Public Reproducible Benchmark Matrix (4 Engines × 5 Workloads)

### Added — Benchmark Infrastructure

- **4-engine benchmark matrix**: FalconDB vs PostgreSQL vs VoltDB vs SingleStore
  on identical hardware, identical workloads, identical measurement methodology.
- **3 new workloads**:
  - W3: Analytic range scan (`COUNT/SUM/AVG` over 10% of rows)
  - W4: Hot-key contention (10 hot keys, all threads competing)
  - W5: Batch insert throughput (single-row INSERT into append table)
- **VoltDB config**: `benchmarks/voltdb/deployment.xml` + `run.sh` (sync cmd log,
  partitioned tables, PG-wire on port 5444).
- **SingleStore config**: `benchmarks/singlestore/singlestore.cnf` + `run.sh`
  (Docker or native, rowstore tables, MySQL wire).
- **`scripts/run_matrix.sh`** — unified orchestrator: auto-detects installed
  engines, runs all workloads × 4 concurrency levels, generates Markdown matrix
  report. Supports `--quick`, `--engines`, `--workloads` flags.
- **Engine-specific schemas**: `setup_voltdb.sql` (partitioned), `setup_singlestore.sql`
  (rowstore + stored-proc data load), `setup_batch.sql` (W5 table).
- **Fair-comparison disclosure** in README and RESULTS: per-engine architecture,
  expected advantages, known caveats.

### Changed

- **`benchmarks/README.md`** — rewritten for 4-engine methodology, installation
  guides, configuration parity table, fair-comparison policy.
- **`benchmarks/RESULTS.md`** — expanded to 4-engine × 5-workload matrix template
  with per-workload descriptions and engine characteristics table.

---

## [Unreleased] — Distributed & Failover Hardening ("无脑稳")

### Added — Production-Grade Failover Guards

- **`crates/falcon_cluster/src/dist_hardening.rs`** — 5 hardening components:
  - `FailoverPreFlight` — 5-point pre-promotion checklist (lag, quorum, cooldown,
    epoch, candidate). Blocks unsafe promotions.
  - `SplitBrainDetector` — epoch-based stale-write rejection with event history.
    Prevents two nodes serving writes simultaneously after partition heal.
  - `AutoRestartSupervisor` — bounded auto-restart for bg tasks with exponential
    backoff (1s→60s cap), permanent failure after max restarts, success reset.
  - `HealthCheckHysteresis` — debounced health transitions (suspect=2, fail=5,
    recover=3 consecutive). Prevents false-positive failovers.
  - `PromotionSafetyGuard` — atomic-or-rollback promotion protocol. If any step
    fails, old primary is unfenced (no total write outage).
- **`HAReplicaGroup::hardened_promote_best()`** — production-grade promotion
  wiring pre-flight + safety guard + split-brain detector + `evaluate()`.
- **`HAReplicaGroup::check_write_epoch()`** — split-brain write validation.
- **`docs/distributed_hardening.md`** — architecture, forbidden states, test map.
- **`crates/falcon_cluster/tests/dist_hardening_integration.rs`** — 20 integration
  tests (DH-01 through DH-19 + evidence summary).

### Changed

- **`falcon_cluster/src/ha.rs`** — `HAReplicaGroup` gains `split_brain_detector`,
  `pre_flight`, `promotion_guard` fields; `hardened_promote_best()` calls
  `detector.evaluate()` for accurate primary-failed detection.
- **`falcon_cluster/src/lib.rs`** — exports all `dist_hardening` types.

---

## [Unreleased] — Disk Spill & Memory Pressure Hardening

### Added — Production-Grade Spill Infrastructure

- **`SpillMetrics`** in `external_sort.rs` — 6 observable counters:
  `spill_count`, `in_memory_count`, `runs_created`, `bytes_spilled`,
  `bytes_read_back`, `spill_duration_us`. Exposed via `Executor::spill_metrics()`.
- **`SpillConfig`** production defaults — `memory_rows_threshold=500K`,
  `hash_agg_group_limit=1M`, `pressure_spill_trigger="soft"`.
- **Hash aggregation group limit** — rejects at configurable group count with
  `ExecutionError::ResourceExhausted` (SQLSTATE `53000`).
- **`docs/memory_pressure_spill.md`** — production strategy document covering
  3-tier defense (sort spill, agg guard, governor backpressure), forbidden states,
  client error contract, and architecture diagram.
- **`crates/falcon_executor/tests/spill_pressure.rs`** — 18 integration tests:
  SP-1 through SP-17 + evidence summary covering sort metrics, data integrity,
  agg limits, pressure transitions, governor backpressure, and cleanup.

### Changed

- **`falcon_common/src/config.rs`** — `SpillConfig` expanded with serde defaults,
  `hash_agg_group_limit`, and `pressure_spill_trigger` fields.
- **`falcon_common/src/error.rs`** — added `ExecutionError::ResourceExhausted`
  variant with SQLSTATE `53000` and `ErrorKind::UserError` classification.
- **`falcon_executor/src/executor.rs`** — added `hash_agg_group_limit` field,
  `spill_metrics()` public method.
- **`falcon_executor/src/executor_aggregate.rs`** — hash aggregation group count
  guard wired into `map_reduce_visible` GROUP BY path.

---

## [Unreleased] — Failover × In-Flight Txn Determinism Hardening

### Added — Stronger Failover Evidence (P0-2b)

- **`crates/falcon_cluster/tests/failover_determinism.rs`** — 6 new FDE tests:
  - FDE-1: Commit-phase-at-crash → recovery outcome (FC-1, FC-2, FC-3)
  - FDE-2: OCC write conflict during active failover (no phantom, no duplication)
  - FDE-3: Network partition write isolation (0 leaks, no split-brain)
  - FDE-4: In-doubt TTL bounded resolution (5 cycles × 10 txns, 0 remaining)
  - FDE-5: Idempotent WAL replay under double-delivery (FC-4)
  - FDE-SUMMARY: Combined evidence matrix for external audit

### Added — Quantified Partition + Write Conflict SLA

- **`docs/failover_partition_sla.md`** — normative SLA document:
  - 10 quantified guarantees (SLA-1 through SLA-10)
  - 8 forbidden states with test evidence
  - Network partition write isolation proof
  - OCC conflict retry contract (SQLSTATE `40001`, retry 50 ms)
  - RPO/RTO tables by commit policy
  - Client-side JDBC/PG driver behavior contract
  - Configuration reference (9 parameters)

### Changed — Documentation Updates

- **`docs/failover_behavior.md`** — added FDE test coverage table + SLA cross-reference
- **`docs/failover_determinism_report.md`** — added FDE evidence section, updated
  source code table, expanded conclusion with partition/conflict/replay guarantees

---

## [Unreleased] — GA Release Gate

> **Theme**: Formal verification that FalconDB meets all P0 requirements for a
> stable GA release: transaction correctness, WAL durability, failover behavior,
> memory safety, and restart safety.

### Added — GA Automated Tests

- **`crates/falcon_storage/tests/ga_release_gate.rs`** — 19 integration tests:
  - P0-1: Transaction correctness (4 tests) — ACK=Durable, No Phantom Commit,
    At-most-once Commit, Terminal States from WAL
  - P0-2: WAL durability & crash recovery (7 tests) — Crash points 1/2/3,
    idempotent replay (7×), multi-table interleaved recovery, update/delete
    recovery, checkpoint+delta recovery
  - P0-3: Failover behavior (2 tests) — only committed survive, new leader
    accepts writes
  - P0-4: Memory safety (3 tests) — pressure states, tracker accounting,
    global governor 4-tier backpressure
  - P0-5: Restart safety (3 tests) — clean shutdown+restart, 5 crash-restart
    cycles, DDL survives restart

### Added — GA Documentation

- **`docs/crash_point_matrix.md`** — normative crash point behavior matrix with
  5 commit crash points, 3 abort/in-flight scenarios, DDL and checkpoint crash
  points, and 5 formal invariants
- **`docs/failover_behavior.md`** — normative failover transaction behavior with
  6 invariants, per-policy RPO table, failover sequence, client behavior guide,
  and admission control during failover
- **`docs/ga_sql_boundary.md`** — complete SQL whitelist (DML, query features,
  DDL, DCL, transaction control, constraints, data types, protocol) and
  not-supported list with SQLSTATE `0A000` for every unsupported feature
- **`docs/ga_release_checklist.md`** — unified GA release gate checklist

### Added — GA CI Gate

- **`scripts/ci_ga_release_gate.sh`** — unified Go/No-Go gate script covering
  all P0/P1 gates plus build, clippy, full test suite, and pre-existing gates

## [1.2.0] — Commercial-Grade Readiness (P0 + P1 + P2) + Segment-Level Zstd Compression

> **Theme**: FalconDB advances from "technically strong" to "industry-positioned, commercially credible".
> P0 establishes version consistency, failover determinism, reproducible benchmarks, and evidence.
> P1 adds memory control, long-run stability, replication integrity, and production operability.
> P2 locks industry focus (Financial Trading OLTP), defines the product edition, SLA, commercial model, and PoC path.

### Added — P2-1: Industry Focus Decision

- **`docs/industry_focus.md`** — binding strategic decision: Financial Trading & Risk Control OLTP.
  - Target systems: OMS, Risk Engine, Position Keeper, Clearing Ledger.
  - Explicit rejection of Industrial/MES, Energy/Metering with reasoning.
  - PostgreSQL / Oracle / MySQL gap analysis per financial requirement.
  - FalconDB's 7 irreplaceable advantages mapped to architecture.

### Added — P2-2: Industry Edition Product Definition

- **`docs/industry_edition_overview.md`** — FalconDB Financial Edition:
  - 3 core use cases: Order Entry, Real-Time Risk, Clearing Ledger.
  - Transaction model: Snapshot Isolation, OCC, first-committer-wins.
  - Write model: memory-first hot path, WAL group commit, sync replication.
  - SLA commitments with evidence links.
  - Default `falcondb-financial.toml` configuration (zero-tuning start).
  - PG wire compatibility matrix.

### Added — P2-3: Industry SLA & Replacement Proof

- **`docs/industry_sla.md`** — every SLA claim traceable to evidence:
  - Write latency: P99 < 1ms, P99.9 < 5ms.
  - Zero phantom commits (P0-2 proven).
  - Failover: RPO=0 (sync), RTO < 30s, impact window < 3s.
  - FalconDB vs PostgreSQL: 5–20× latency, deterministic failover, built-in backpressure.
  - FalconDB vs Oracle: Apache 2.0 vs $47.5K/core, $8.5M+ savings over 5 years.
  - Explicit non-claims listed.

### Added — P2-4: Commercial Model & License Boundary

- **`docs/commercial_model.md`** — binding product decision:
  - **Core** (Apache 2.0, forever free): storage engine, WAL, replication, PG wire, backpressure, metrics, CLI.
  - **Financial Edition** (commercial): ops console, advanced replication, compliance pack, priority support.
  - Pricing model: per-node/year vs Oracle per-core perpetual.
  - Value equation: $8.5M–$9M savings vs Oracle over 5 years.
  - Revenue projection: path to $10M ARR (40 institutions × $250K).
  - "Never charge for" list: core engine, basic replication, metrics, docs.

### Added — P2-5: Industry PoC Playbook

- **`docs/industry_poc_playbook.md`** — repeatable 2–4 week PoC process:
  - Week 0: Qualification + success criteria agreement.
  - Week 1: Customer schema + workload.
  - Week 2: Performance + resilience measurement.
  - Week 3 (optional): 72h stability.
  - Week 4: Report + decision.
  - Success criteria template (7 measurable criteria).
  - PoC report template.
  - Anti-patterns list (6 "do not" rules).
  - Cost control: < $10K total.

### Added — P2-6: P2 Evidence Pack

- **`evidence/industry/`** with 3 sub-categories:
  - `sla/` — SLA traceability JSON, SLA doc snapshots.
  - `poc/` — PoC playbook snapshots.
  - `comparisons/` — FalconDB vs PG/Oracle structured comparison JSON.
- **`scripts/collect_evidence_p2.sh`** — one-click P2 evidence collection with `--verify` mode.
- **`evidence/INDEX.md`** — updated with P2 categories (E9–E12) and completeness checklist.

### Added — P1-1: Global Memory Budget & Backpressure System

- **`MemoryTracker`** now tracks **7 memory consumers**: MVCC, Index, WriteBuffer, WAL buffer, Replication buffer, Snapshot buffer, SQL execution buffer.
- **`GlobalMemoryGovernor`** — node-wide 4-tier backpressure: None → Soft (delay) → Hard (reject writes) → Emergency (reject all + urgent GC).
- **`MemorySnapshot`** includes all 7 consumers, pressure ratio, and backpressure counters.
- **16 integration tests** (`crates/falcon_storage/tests/memory_backpressure.rs`): all consumers tracked, pressure transitions, governor tiers, concurrent safety, recovery.
- **`docs/memory_backpressure.md`** — architecture, configuration, forbidden behaviors, troubleshooting.
- **Guarantee**: FalconDB will never silently OOM. Under memory pressure it throttles, rejects, or sheds — every action logged and metered.

### Added — P1-2: Long-Run Stability Verification (72h / 7d)

- **`scripts/run_stability_test.sh`** — automated long-run stability test with `--duration 72h` / `--duration 7d`.
  - Samples RSS, TPS, latency, error count, WAL backlog every 60s.
  - Generates timestamped report + metrics CSV.
  - Detects memory leaks (>20% RSS growth) and performance degradation.
- **`evidence/stability/72h_report_template.md`** + **`7d_report_template.md`** — standardized report formats.

### Added — P1-3: Replication Drift & Data Integrity Guard

- **`crates/falcon_cluster/tests/replication_integrity.rs`** — 5 integration tests:
  - Empty cluster consistency, 100-row replication checksum match, drift detection (unshipped records), failover integrity, 1000-row large dataset.
- **Order-independent checksum**: sort rows by PK, hash all column values.
- **Drift detection**: row count mismatch and checksum divergence flagged programmatically.
- **`docs/replication_integrity.md`** — check methods, drift types, recovery paths, scheduling.

### Added — P1-4: Production Operability Baseline

- **`docs/operability_baseline.md`** — complete ops reference:
  - Health endpoints (`/health`, `/ready`, `/status`)
  - Full Prometheus metrics catalog (7 categories, 60+ metrics)
  - Structured log level policy (INFO/WARN/ERROR/FATAL)
  - Troubleshooting runbook: TPS drop, memory pressure, replica lag, high abort rate, WAL latency
  - Recommended Prometheus alerting rules + Grafana dashboard panels

### Added — P1-5: P1 Evidence Pack

- **`evidence/`** extended with 4 new categories:
  - `memory/` — backpressure test output, governor stats JSON
  - `stability/` — 72h/7d report templates + metrics CSV
  - `replication/` — integrity test output, drift detection JSON
  - `operability/` — metrics catalog extract, health check responses
- **`scripts/collect_evidence_p1.sh`** — one-click P1 evidence collection with `--verify` mode.
- **`evidence/INDEX.md`** — updated with P1 categories (E5–E8) and completeness checklist.

### Added — P0-1: Single Source of Truth for Versioning

- **`[workspace.package] version`** is the ONLY version definition. All crates use `version.workspace = true`.
- **`crates/falcon_server/build.rs`** — injects `FALCONDB_GIT_HASH` and `FALCONDB_BUILD_TIME` at compile time.
- **`falcon --version`** now prints version, git hash, build time, config schema, arch, OS.
- **Startup log** includes version, git commit, and build timestamp.
- **`scripts/extract_version.ps1`** — extracts version from Cargo.toml, optionally updates `dist/VERSION`.
- **`scripts/ci_version_check.sh`** — CI gate: verifies all version references are consistent.
- **`scripts/build_windows_dist.ps1`** — auto-generates VERSION file from Cargo.toml (no static copy).
- **`.github/workflows/release.yml`** — tag-triggered release: validates tag vs Cargo.toml, builds Linux + Windows, uploads to GitHub Releases.
- **`docs/versioning.md`** — version management policy and bump procedure.

### Added — P0-2: Failover Determinism Evidence System

- **`crates/falcon_cluster/tests/failover_determinism.rs`** — 10 integration tests (9 matrix + 1 summary).
  - **3 fault types**: LeaderCrash, NetworkPartition, WalStall
  - **3 load types**: ReadHeavy, WriteHeavy, Mixed
  - **Txn classification**: Committed, Aborted, Retried, In-Doubt — explicit per experiment
  - **Metrics**: TPS (before/during/after), p50/p99/p99.9/p99.99 latency, failover time
  - **Invariant**: Zero phantom commits across all 9 experiments ✅
- **`scripts/run_failover_matrix.sh`** — one-click matrix runner with `--ci-nightly` reduced mode.
- **`docs/failover_determinism_report.md`** — SLA boundaries, guarantees, non-guarantees, reproduction steps.

### Added — P0-3: Public Reproducible Benchmark System

- **`benchmarks/`** scaffold with FalconDB vs PostgreSQL comparison:
  - `workloads/setup.sql` — shared schema (100K accounts)
  - `workloads/single_table_oltp.sql` — W1: 70% SELECT, 20% UPDATE, 10% INSERT
  - `workloads/multi_table_txn.sql` — W2: BEGIN → SELECT → INSERT → UPDATE×2 → COMMIT
  - `falcondb/config.toml` + `postgresql/postgresql.conf` — fair parity configs
  - `scripts/run_all.sh` — one-click full benchmark suite with `--quick` mode
  - `scripts/run_workload.sh` — pgbench-based per-workload runner
  - `RESULTS.md` — results template with reproduction steps
  - `README.md` — methodology, hardware requirements, known limitations

### Added — P0-4: Evidence Pack

- **`evidence/`** directory with 4 categories:
  - `versioning/` — CI version gate output, binary version, Cargo.toml version
  - `failover/` — matrix results, summary JSON
  - `benchmarks/` — raw pgbench output, environment capture
  - `ci_reports/` — workspace test results, pass/fail summary
- **`scripts/collect_evidence.sh`** — one-click evidence collection with `--verify` mode.
- **`evidence/INDEX.md`** — master index: every claim → script + raw data.

### Changed — Version Infrastructure

- **`Cargo.toml`** — workspace version updated to `1.2.0`
- **`dist/VERSION`** — updated to `v1.2.0`
- **`README.md`** / **`README_zh.md`** — removed hardcoded version from title, added version badge
- **`.github/workflows/ci.yml`** — added `version-check` job

### P0 Zstd Compression (unchanged from previous entry)

> Compression becomes a first-class segment-level infrastructure primitive.
> All Zstd access goes through a unified `SegmentCodecImpl` trait. No code outside
> `falcon_segment_codec` calls zstd or lz4 directly.

### Added — New Crate: `falcon_segment_codec`

- **`SegmentCodecImpl` trait** — unified compression abstraction for all segment types.
- **`ZstdBlockCodec`** — built on `zstd-safe` 7.2 (NOT the high-level `zstd` crate). Independent blocks, dictionary support, configurable level/checksum.
- **`Lz4BlockCodec`** — LZ4 via `lz4_flex`.
- **`NoneCodec`** — passthrough.
- **`CodecPolicy`** — WAL=None/LZ4, Cold=Zstd (default), Snapshot=Zstd (forced).
- **`DictionaryRegistry`** — load/use pre-trained dictionaries (training external). Dictionaries stored as `DICT_SEGMENT` in SegmentStore, manifest-tracked.
- **`CompressedBlock`** — wire format `[uncompressed_len:u32][compressed_len:u32][block_crc:u32][data]`.
- **`DecompressPool`** — concurrency-limited decompression, never on OLTP executor thread.
- **`DecompressCache`** — LRU byte-capacity limited, keyed by `(segment_id, block_index)`.
- **`StreamingCodecCaps` + `negotiate_streaming_codec()`** — streaming codec orthogonal to segment codec; chunk-level, independently decompressible.
- **`recompress()`** — GC/dictionary-upgrade recompression; always creates new segment.
- **`CompressMetrics` + `CodecMetricsSnapshot`** — full compress/decompress/cache/dictionary observability.
- **`zstd_version()`** — reports libzstd version from zstd-safe.

### Added — Tests

- **34 unit tests** in `falcon_segment_codec` (codec roundtrips, dictionary, cache, pool, recompress, metrics).
- **19 integration tests** in `codec_integration` (CRC verification, dict missing/mismatch, codec policy enforcement, cached read <1ms, ratio ≥3x, throughput ≥100 MB/s, full dictionary lifecycle, streaming negotiation E2E, cache effectiveness).

### Added — Documentation

- `docs/segment_codec_zstd.md` — architecture, why zstd-safe, trait API, block format
- `docs/dictionary_in_segment_store.md` — DICT_SEGMENT lifecycle, registry API
- `docs/streaming_codec_negotiation.md` — orthogonal design, negotiation rules
- `docs/segment_codec_matrix.md` — codec availability matrix by segment kind

### Changed — Updated Documentation

- `docs/cold_segment_format.md` — new block format, Zstd default, dictionary support, decompression isolation
- `docs/memory_compression.md` — Zstd as default codec, falcon_segment_codec references
- `docs/compression_profiles.md` — Balanced/Aggressive now use Zstd, Legacy-LZ4 profile added
- `docs/unified_data_plane_full.md` — falcon_segment_codec integration, dictionary segments
- `docs/bootstrap_flow.md` — DICT_SEGMENT fetch ordering requirement
- `docs/crate_audit_report.md` — falcon_segment_codec crate entry

### Changed — Dependencies

- `Cargo.toml` (workspace) — Added `zstd-safe = "7.2"`, `falcon_segment_codec` path dep
- `crates/falcon_storage/Cargo.toml` — Added `falcon_segment_codec.workspace = true`

---

## [1.1.1] — Enterprise Hardening, Predictable Operations, Cost & Risk Control

> **Theme**: FalconDB enters steady-state operation. All unpredictable behavior eliminated.
> The system behaves the same at 3 a.m. on day 300 as it did on day 1.

### Added — P0: GA Stability

- **`CrashHardeningCoordinator`** — explicit startup/shutdown lifecycle for all 8 component types (DataNode, Gateway, Controller, Compactor, ReplicationWorker, BackupWorker, AuditDrain, SnapshotWorker), deterministic startup/shutdown order, shutdown sentinel for crash detection, recovery action logging (WAL replay, checkpoint restore, cold rebuild, replication resume, in-doubt resolution, index rebuild).
- **`ConfigRollbackManager`** — versioned + checksummed config entries, staged rollout state machine (Staged→Canary→RollingOut→Applied/RolledBack), one-click rollback to any previous version, checksum verification for tamper detection.
- **`ResourceLeakDetector`** — periodic resource sampling (FD, TCP, thread, hot/cold memory, cache), linear regression leak analysis with R² confidence, configurable alarm thresholds per resource type, 72h+ stability verification.
- **`LatencyGuardrailEngine`** — per-path guardrails for 6 hot paths (WAL commit, gateway forward, cold decompress, replication apply, index lookup, txn commit), p99/p999/absolute-max thresholds, automatic backpressure on breach, rolling percentile computation.
- **`BgTaskIsolator`** — CPU/IO/memory quotas for 7 background task types (compaction, cold migration, snapshot, rebalance, backup, index build, GC), dynamic throttle under foreground load, throttle decisions (Allow/Throttle/Reject).

### Added — P1: Enterprise Operations

- **`CostTracker`** — per-table cost breakdown (hot/cold memory, WAL IO, replication traffic, compression ratio, savings), per-shard cost breakdown, cluster cost summary with history, top-N tables by resource usage.
- **`CapacityGuardV2`** — auto-detect memory/WAL/cold pressure with configurable warning/urgent thresholds, generates typed recommendations (ScaleOut, IncreaseCompression, WalGc, ColdCompaction, ShardRebalance, ConnectionPoolResize).
- **`HardenedAuditLog`** — unified event schema with trace ID + span ID, cross-component trace correlation, SIEM-compatible JSON-lines export (ELK/Splunk ready).
- **`PostmortemGenerator`** — auto-capture decision points with trigger metrics/thresholds/outcomes, continuous metric sampling with explicit timestamps, auto-generated postmortem reports with timeline + metric changes + decision points, `falconctl postmortem export` text format.

### Added — P2: Enterprise UX

- **`AdminConsoleV2`** — 10 admin endpoints (/admin/v2/slo, /cost, /capacity, /compression, /timeline, /guardrails, /leaks, /bgtasks, /postmortem, /impact).
- **`ChangeImpactPreview`** — estimate CPU/memory/storage/latency impact before operational changes (compression, add/remove node, replication factor, connection pool, WAL GC, cold compaction), risk assessment (Low/Medium/High), guardrail compliance check.

### Added — Tests

- **37 unit tests** in `ga_hardening` module (crash lifecycle, config rollback, leak detection, guardrails, background isolation, concurrency).
- **28 unit tests** in `cost_capacity` module (cost tracking, capacity guard, audit correlation, postmortem generation, change impact, concurrency).
- **20 integration tests** in `ga_soak_test` (crash recovery, sentinel, multi-cycle recovery, config rollback, staged rollout, 72h leak stability, leak detection, guardrail paths, backpressure, background isolation, cost visibility, capacity guard, audit trace correlation, SIEM export, postmortem replay, change impact scenarios, full 7-day soak simulation).

### Added — Documentation

- `docs/ga_hardening.md` — crash recovery, resource leak detection, shutdown sentinel
- `docs/config_management.md` — versioned config, staged rollout, rollback procedures
- `docs/performance_guardrails.md` — latency guardrails, background task isolation
- `docs/cost_capacity.md` — cost visibility, capacity guard v2, change impact preview
- `docs/postmortem.md` — postmortem generation, audit log hardening, SIEM export

---

## [1.1.0] — Enterprise Control Plane, Security, Lifecycle, Predictable Ops

> **Theme**: FalconDB is a database system acceptable to IT / security departments.

### Added — Control Plane v1

- **`ControllerHAGroup`** — 3-node HA controller with Raft-based leader election, term tracking, heartbeat.
- **`NodeRegistry`** — data-plane node registration, heartbeat-based liveness (Online/Suspect/Offline/Draining/Joining/Upgrading).
- **`ConfigStore`** — versioned dynamic configuration distribution with change history.
- **`ConsistentMetadataStore`** — Raft-backed consistent key-value store with 5 domains (ClusterConfig, ShardPlacement, VersionEpoch, SecurityPolicy, NodeRegistry), CAS support, leader-only writes.
- **`ShardPlacementManager`** — authoritative shard→node mapping, health evaluation (Healthy/UnderReplicated/Migrating/Orphaned).
- **`CommandDispatcher`** — ops command queuing (Drain/Join/Upgrade/Rebalance/SetConfig/ForceElection).

### Added — AuthN/AuthZ v1

- **`AuthnManager`** — multi-credential authentication (password/token/mTLS), account lockout, credential rotation.
- **`EnterpriseRbac`** — RBAC at Cluster/Database/Table scope, wildcard grants, superuser bypass, least-privilege enforcement.
- **13 enterprise permissions** across 3 scope levels.
- Auth failure responses never leak topology information.

### Added — Full-Chain TLS & Key Rotation

- **`CertificateManager`** — manages certs for all 4 link types (Client↔Gateway, Gateway↔DataNode, DataNode↔Replica, DataNode↔Controller).
- Certificate hot-reload via `rotate_cert()` — zero restart, zero disconnect.
- Expiry detection and rotation history tracking.

### Added — Backup/Restore v1

- **`BackupOrchestrator`** — enterprise backup orchestration with Full/Incremental/WAL Archive types.
- Storage targets: Local filesystem, S3, OSS.
- Restore types: Latest, ToLsn, ToTimestamp (PITR), NewCluster.
- Observable lifecycle: Scheduled → Running → Completed/Failed.
- Retention policy and backup history.

### Added — Enterprise Audit Log v1

- **`EnterpriseAuditLog`** — tamper-proof HMAC hash chain, 10 event categories, 3 severity levels.
- SIEM-ready JSON-lines export via `export_jsonl()`.
- `verify_integrity()` detects any chain tampering.
- Thread-safe: hash chain ordering guaranteed under concurrent writes.

### Added — Auto Rebalance / Scale v1

- **`AutoRebalancer`** — computes rebalance plans on node join/leave, rate-limited migrations.
- Migration state machine: Pending → Preparing → Copying → CatchingUp → Cutover → Completed.
- SLA-safe: auto-pause when latency impact detected, configurable `max_concurrent` and `rate_limit_bytes_per_sec`.

### Added — Capacity Planning & Forecast

- **`CapacityPlanner`** — tracks WAL size, memory, cold storage, shard count, connections.
- Linear regression forecasting with time-to-exhaustion prediction.
- Alert levels: OK, Watch, Warning, Critical.

### Added — SLO Engine v1

- **`SloEngine`** — define/compute/export SLOs for availability, latency p99/p999, durability, replication lag, error rate.
- Rolling window evaluation with error budget tracking.
- Prometheus export via `export_prometheus()`.

### Added — Incident & Event Timeline

- **`IncidentTimeline`** — auto-correlate failures, leader changes, ops actions, metric anomalies.
- Incident lifecycle: create → correlate → resolve with root cause and impact.
- Correlation window configurable.

### Added — Admin Console API Model

- **`AdminApiRouter`** — 11 structured endpoints for cluster visualization, shard management, backup, SLO, audit, incidents, capacity, config.

### Added — Enterprise Documentation

- `docs/enterprise_architecture.md` — full system architecture overview
- `docs/security_model.md` — AuthN/AuthZ/TLS/Audit security model
- `docs/backup_restore.md` — backup/restore guide with PITR
- `docs/rbac.md` — RBAC permission model and examples
- `docs/control_plane.md` — control plane operations guide
- `docs/ops_runbook_enterprise.md` — enterprise operational procedures

### Added — Enterprise Compliance Tests

- 20 integration tests in `enterprise_compliance.rs` covering:
  - Controller HA failover, metadata consistency, CAS race prevention
  - Auth lockout/recovery, credential rotation, topology leak prevention
  - RBAC least privilege, role hierarchy
  - TLS rotation across all links, expiry detection
  - Full backup/restore with PITR
  - Audit chain integrity under concurrent load, SIEM export
  - Auto-rebalance on node join, SLA pause/resume
  - SLO multi-metric evaluation, error budget depletion
  - Incident auto-correlation lifecycle
  - Full enterprise lifecycle orchestration

### Test Summary

- **67 new unit tests** (control_plane: 22, enterprise_security: 28, enterprise_ops: 17)
- **20 new integration tests** (enterprise_compliance)
- **0 failures, 0 regressions**

---

## [1.0.9] — Self-Healing, Ops Automation, SLA & Production Readiness

### Added — Failure Detector v1

- **`ClusterFailureDetector`** — cluster-wide node liveness tracker with ALIVE/SUSPECT/DEAD state machine.
- **`NodeLiveness`** — five-state enum: `Alive`, `Suspect`, `Dead`, `Draining`, `Joining`.
- **`NodeHealthRecord`** — per-node health record with LSN, replication lag, version, uptime.
- Configurable `suspect_threshold` (default 3s), `failure_timeout` (default 10s), `max_consecutive_misses` (default 5).
- Monotonic **epoch** counter bumped on every state transition for fencing.

### Added — Automatic Leader Re-election

- **`LeaderElectionCoordinator`** — term-based election with configurable quorum.
- **`ShardElection`** — per-shard election state: Stable / Electing / NoLeader.
- Highest-LSN candidate wins; stale-term candidates rejected.
- Anti-split-brain: term validation + epoch fencing + optional quorum.

### Added — Automatic Replica Catch-up

- **`ReplicaCatchUpCoordinator`** — classifies lag as CaughtUp / WalReplay / SnapshotRequired.
- WAL streaming for small gaps (≤ `snapshot_threshold_lsn`), full snapshot for large gaps.
- Progress tracking with phase, bytes transferred, completion percentage.

### Added — Rolling Upgrade v1

- **`RollingUpgradeCoordinator`** — ordered upgrade: follower → gateway → leader.
- **`ProtocolVersion`** — wire compatibility check (same major, minor within 1 step).
- Per-node lifecycle: Pending → Draining → Upgrading → Rejoining → Complete/Failed.

### Added — Node Drain & Join

- **`NodeLifecycleCoordinator`** — manages graceful drain and join operations.
- Drain phases: RejectingNew → DrainingInflight → TransferringShards → Drained.
- Join phases: Requesting → Syncing → Ready → Active.

### Added — SLA/SLO Metrics

- **`SloTracker`** — sliding-window metrics for write/read availability, p99 latency, replication lag, gateway reject rate.
- **`SloSnapshot`** — point-in-time snapshot for `/admin/slo` endpoint.

### Added — Backpressure v2 (Multi-Signal)

- **`BackpressureController`** — multi-signal admission control: CPU, Memory, WAL backlog, Replication lag.
- Four pressure levels: Normal (100%) → Elevated (80%) → High (50%) → Critical (10%).
- Deterministic admission gate based on request hash.

### Added — falconctl Ops Subcommands

- **`OpsCommand`** enum: `ClusterStatus`, `ClusterHealth`, `NodeDrain`, `NodeJoin`, `UpgradePlan`, `UpgradeApply`.
- **`ClusterStatusResponse`** / **`ClusterHealthResponse`** — structured response types.
- **`ClusterHealthLevel`** — Healthy / Degraded / Critical.

### Added — Ops Audit Log

- **`OpsAuditLog`** — ring-buffer audit log for all cluster operations.
- **`OpsEventType`** — 16 event types: NodeUp/Down/Suspect, LeaderElected/Lost, Drain/Join Start/Complete, Upgrade lifecycle, ConfigChanged, BackpressureChanged, CatchUp Start/Complete.
- Monotonic event IDs with Unix timestamps.

### Added — Documentation

- `docs/self_healing.md` — failure detection architecture, state machine, epoch fencing.
- `docs/rolling_upgrade.md` — upgrade ordering, protocol compatibility, workflow.
- `docs/sla_slo.md` — SLO metrics, /admin/slo endpoint, alerting recommendations.
- `docs/ops_runbook.md` — operational procedures for common scenarios.
- `docs/falconctl_ops.md` — CLI command reference for ops subcommands.

### Added — Tests

- 31 unit tests in `self_healing` module covering all 9 subsystems + 3 concurrency tests.
- 19 integration tests in `self_healing_chaos.rs`: leader kill, follower kill, network jitter, WAL backlog escalation, gateway overload, rolling upgrade lifecycle, drain/join lifecycle, SLO accuracy, concurrent orchestration, full self-healing cycle, multi-signal backpressure, audit completeness.
- 0 failures, 0 regressions.

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
- **README "Not Supported" table**: 9 features with SQLSTATE codes and error messages

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

### Added — v1.0 Phase 3: Enterprise Edition Features
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
