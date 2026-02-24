# Stability Report — FalconDB v1.x

> Date: 2026-02-24  
> Scope: P1 Commercial Readiness Gate  
> Status: **Template — populate with soak test results**

---

## 1. Test Coverage Summary

| Category | Tests | Status |
|----------|-------|--------|
| Workspace total | 2,700+ | ✅ All pass |
| Membership (P0-1) | 23 | ✅ |
| TLS (P0-2) | 9 | ✅ |
| Streaming (P0-3) | 11 | ✅ |
| Plan cache | 7 | ✅ |
| Binary params | 14 | ✅ |
| Catalog/introspection | 12+ | ✅ |
| Logical backup | 10 | ✅ |
| Determinism hardening | 33 | ✅ |
| Distributed hardening | 62 | ✅ |
| Enterprise features | 55 | ✅ |

---

## 2. Soak Test Configuration

```toml
# soak_test_config.toml
[workload]
duration_hours = 24
concurrent_connections = 50
target_tps = 1000
mix = { select = 60, insert = 20, update = 15, delete = 5 }
table_count = 20
rows_per_table = 100000

[fault_injection]
network_partition_interval_min = 60
node_kill_interval_min = 120
memory_pressure_interval_min = 90

[thresholds]
max_p99_ms = 50
max_abort_rate_pct = 5.0
max_failover_sec = 10
min_availability_pct = 99.9
```

### Run Command
```bash
./scripts/soak_test.sh --config soak_test_config.toml --duration 24h
```

---

## 3. Latency Profile (populate after soak)

| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| p50 latency (ms) | _TBD_ | < 5 | |
| p99 latency (ms) | _TBD_ | < 50 | |
| p999 latency (ms) | _TBD_ | < 200 | |
| Max latency (ms) | _TBD_ | < 1000 | |

---

## 4. Transaction Outcomes (populate after soak)

| Outcome | Count | Rate | Target |
|---------|-------|------|--------|
| Committed | _TBD_ | _TBD_ | > 95% |
| Aborted (retryable) | _TBD_ | _TBD_ | < 3% |
| Aborted (non-retryable) | _TBD_ | _TBD_ | < 1% |
| Rejected (admission) | _TBD_ | _TBD_ | < 1% |
| Timeout | _TBD_ | _TBD_ | < 0.1% |

---

## 5. Failover Recovery (populate after soak)

| Scenario | Recovery Time | RPO | Target |
|----------|---------------|-----|--------|
| Primary kill (quorum ack) | _TBD_ | _TBD_ | < 5s, 0 bytes |
| Primary kill (async) | _TBD_ | _TBD_ | < 5s, < 1s |
| Network partition (30s) | _TBD_ | _TBD_ | < 10s |
| Memory pressure (OOM kill) | _TBD_ | _TBD_ | < 15s |

---

## 6. Resource Usage (populate after soak)

| Metric | Peak | Avg | Limit |
|--------|------|-----|-------|
| RSS memory (MB) | _TBD_ | _TBD_ | 4096 |
| CPU utilization (%) | _TBD_ | _TBD_ | 80 |
| Disk write (MB/s) | _TBD_ | _TBD_ | 500 |
| WAL segment count | _TBD_ | _TBD_ | 1000 |
| Open connections | _TBD_ | _TBD_ | 100 |

---

## 7. Feature Availability Evidence

### P1-1: SQL Compatibility
- `docs/sql_compatibility.md` contract version 2
- `scripts/ci_compat_contract_gate.sh` gates: catalog handlers, binary params, unsupported SQLSTATE
- §5 unsupported list cleaned (stale entries for VIEW/CURSOR/LISTEN/GRANT/SEQUENCE removed)

### P1-2: ORM/JDBC Interop
- `information_schema`: tables, columns, schemata, table_constraints, key_column_usage
- `pg_catalog`: pg_type, pg_namespace, pg_class, pg_attribute, pg_index, pg_constraint, pg_database, pg_settings, pg_am, pg_stat_user_tables, pg_statio_user_tables
- JDBC `FalconDatabaseMetaData`: getTables/getColumns/getPrimaryKeys/getIndexInfo + 30 capability methods
- ORM smoke doc: `docs/orm_compat_smoke.md`

### P1-3: Prepared Statements + Binary Params
- Plan cache: schema_generation invalidation, LFU eviction, shared via `Arc<PlanCache>`
- Binary param decode: int32/int64/float64/float32/bool/text/bytea/uuid/decimal (12+ tests)
- Extended query protocol: Parse/Bind/Execute fully wired
- Perf report: `docs/perf_prepared_v1x.md`

### P1-4: Backup/Restore
- `BackupManager`: full/incremental backup lifecycle, checksum, history
- `WalArchiver` (PITR): segment archiving, base backup, restore points, retention
- `logical_backup`: schema_to_ddl, rows_to_insert, SQL text dump with checksum (10 tests)
- `RecoveryExecutor`: coordinated WAL replay with target detection
- `CheckpointData`: physical snapshot backup/restore

### P1-5: Ops Playbook
- `docs/ops_playbook.md`: 8 sections (scale-out/in, rebalance, failover, rolling upgrade, diagnostics, troubleshooting, monitoring)
- Alert thresholds for 7 key metrics

---

## 8. CI Gate Verification

```bash
# P0 contract gate
./scripts/ci_p0_contract_gate.sh

# Compat contract gate
./scripts/ci_compat_contract_gate.sh

# Determinism gate
./scripts/ci_v104_determinism_gate.sh

# Distributed chaos gate
./scripts/ci_distributed_chaos.sh

# Full workspace
cargo test --workspace
cargo clippy --workspace
```

---

## 9. Known Limitations

| Area | Limitation | Impact | Mitigation |
|------|-----------|--------|------------|
| Data types | No SMALLINT/REAL native (widened to INT32/FLOAT64) | Low | Transparent to ORMs |
| Raft | Single-node stub (streaming replication works) | Medium | Use WAL replication |
| COPY | CSV only, no binary format | Low | Use INSERT for binary |
| Full outer join | Not supported | Low | Rewrite as UNION of LEFT/RIGHT |
| Recursive CTE | Not supported | Medium | Application-level recursion |
| Triggers | Not supported | Medium | Application-level hooks |

---

## 10. Sign-Off

| Role | Name | Date | Status |
|------|------|------|--------|
| Engineering | ___________ | ______ | ☐ Approved |
| QA | ___________ | ______ | ☐ Approved |
| Product | ___________ | ______ | ☐ Approved |
