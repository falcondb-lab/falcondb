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

- 932+ tests across 12 crates
- Integration tests for DDL/DML/SELECT end-to-end
- Failover exercise (create → replicate → fence → promote → verify)

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

## v0.3.0 — M3: Production Hardening

**Status**: Planned

### Deliverables

| Feature | Acceptance Criteria |
|---------|-------------------|
| Read-only replica enforcement | DDL/DML writes rejected on replica with PG SQLSTATE `25006` |
| Graceful shutdown | SIGTERM → drain timeout → force close; zero in-flight corruption |
| Health checks | HTTP `/health`, `/ready` endpoints; K8s liveness/readiness probes |
| Query timeout | `SET statement_timeout`; returns SQLSTATE `57014` on expiry |
| Connection limits | `max_connections` enforced; excess connections get SQLSTATE `53300` |
| Idle timeout | Idle connections closed after configurable period |
| TLS/SSL | SSLRequest → TLS handshake; cert/key config in `falcon.toml` |
| Cancel request | PG cancel protocol (backend key → kill query) |
| Plan cache | LRU prepared statement cache with `SHOW falcon.plan_cache` |
| Slow query log | `SET log_min_duration_statement`, `SHOW falcon.slow_queries` |

### Acceptance Gates

1. All M2 gates still pass.
2. `pgbench -c 100 -T 60` completes without connection errors.
3. Health endpoint returns 503 during graceful shutdown drain.
4. TLS connection via `psql "sslmode=require"` succeeds.

---

## v0.4.0 — M4: Analytics & Multi-Tenancy

**Status**: Future

| Feature | Description |
|---------|-------------|
| Columnstore | On-disk columnar storage for analytics workloads |
| Analytics node role | Receives WAL, serves read-only analytical queries |
| Vectorized execution | SIMD-accelerated operators for columnar scans |
| Multi-tenancy | Tenant isolation, resource quotas, metering |
| Cross-region replication | Geo-distributed replicas with lag-aware routing |

---

## Release Cadence

| Milestone | Target | Key Metric |
|-----------|--------|------------|
| M1 | ✅ Done | 932 tests passing |
| M2 | Q1 2026 | Two-node gRPC replication e2e |
| M3 | Q2 2026 | pgbench 100-client stability |
| M4 | Q4 2026 | Columnstore + multi-tenant |
