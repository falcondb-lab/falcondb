# Production Readiness Checklist

Use this checklist before promoting a FalconDB deployment to production.

---

## 1. Hardware & OS

- [ ] **CPU**: 4+ cores dedicated (8+ recommended for OLTP workloads)
- [ ] **RAM**: 16 GB minimum; size `memory_limit_bytes` to ≤ 80% of physical RAM
- [ ] **Storage**: NVMe SSD with ≥ 2× expected data size for WAL + data
- [ ] **File descriptors**: `ulimit -n 65536` or higher (set in systemd / container)
- [ ] **Transparent Huge Pages**: disabled (`echo never > /sys/kernel/mm/transparent_hugepage/enabled`)
- [ ] **Swap**: disabled or `vm.swappiness = 1`
- [ ] **Clock sync**: NTP or chrony active (critical for replication epoch ordering)

## 2. Configuration

- [ ] **`config_version = 3`** in `falcon.toml`
- [ ] **Authentication**: method ≠ `trust` in production; prefer `scram-sha-256`
- [ ] **TLS enabled**: `[server.tls]` with valid cert + key
- [ ] **WAL**: `sync_mode = "fdatasync"` or `fsync` (never `none` in prod)
- [ ] **WAL group commit**: `group_commit = true` for throughput
- [ ] **Statement timeout**: set `statement_timeout_ms` to prevent runaway queries
- [ ] **Idle timeout**: `idle_timeout_ms` ≤ 300000 to reclaim idle connections
- [ ] **Max connections**: sized to expected concurrency + headroom
- [ ] **Shutdown drain**: `shutdown_drain_timeout_secs ≥ 15` for graceful shutdown

## 3. Replication & HA

- [ ] **Replication mode**: `quorum-ack` for RPO=0 (or `local-fsync` + accept RPO > 0)
- [ ] **At least 1 replica** in a different failure domain (rack/AZ)
- [ ] **Failover tested**: run `make e2e_failover` or PoC failover demo before go-live
- [ ] **Deterministic Commit Guarantee (DCG)**: verified via `SHOW falcon.verification`
- [ ] **Replication lag monitoring**: alert if `falcon_replication_lag_bytes` > threshold

## 4. Security

- [ ] **SCRAM-SHA-256** authentication enabled
- [ ] **TLS/mTLS** for all client and replication connections
- [ ] **RBAC configured**: least-privilege roles; no default superuser access
- [ ] **Auth rate limiting**: `max_failures_per_window ≤ 5`, lockout enabled
- [ ] **SQL firewall**: reviewed and enabled if applicable
- [ ] **Audit logging**: enabled for compliance-sensitive workloads
- [ ] **`cargo deny`** clean: no known CVEs in dependencies

## 5. Observability

- [ ] **Prometheus scraping**: `/metrics` endpoint reachable from monitoring stack
- [ ] **Grafana dashboard**: imported from `docs/observability/grafana_dashboard.json`
- [ ] **Alerting rules**: loaded from `docs/observability/alerting_rules.yml`
- [ ] **Slow query log**: `slow_query_threshold_ms` set (e.g., 100–500 ms)
- [ ] **Structured logging**: JSON format enabled, forwarded to log aggregator
- [ ] **OTLP export** (optional): see [opentelemetry.md](opentelemetry.md)
- [ ] **Health checks**: `/health` and `/ready` endpoints wired to orchestrator

## 6. Backup & Recovery

- [ ] **WAL archival**: configured for point-in-time recovery
- [ ] **Backup schedule**: automated, verified with restore test
- [ ] **PITR tested**: at least one end-to-end restore drill completed
- [ ] **Backup storage**: separate from primary data volume

## 7. Performance

- [ ] **Baseline established**: run `make bench` or TPC-C / pgbench before go-live
- [ ] **Connection pooling**: application-side pooling (HikariCP, pgbouncer, etc.)
- [ ] **GC tuning**: `gc.interval_ms` appropriate for write volume
- [ ] **Plan cache**: verify `SHOW falcon.plan_cache` shows reasonable hit rate
- [ ] **No analytical queries** on OLTP primary (use replica read routing)

## 8. Kubernetes (if applicable)

- [ ] **Helm chart** deployed from `deploy/helm/falcondb/`
- [ ] **PVC**: `persistence.enabled = true` with appropriate `storageClass`
- [ ] **PDB**: `podDisruptionBudget.enabled = true` with `minAvailable: 1`
- [ ] **Resource limits**: CPU and memory limits set to avoid noisy-neighbor
- [ ] **Anti-affinity**: pods spread across nodes / AZs
- [ ] **Liveness/readiness probes**: verified healthy via `/health` and `/ready`
- [ ] **ServiceMonitor**: enabled if using Prometheus Operator

## 9. Operational Procedures

- [ ] **Runbook**: documented for common operations (restart, failover, scale, backup)
- [ ] **Rolling upgrade procedure**: tested per [rolling_upgrade.md](rolling_upgrade.md)
- [ ] **Incident response**: alerting → PagerDuty/Slack integration verified
- [ ] **Capacity planning**: growth projections for next 6–12 months
- [ ] **Chaos testing**: at least one gameday drill (`make chaos`) completed

## 10. Compliance & Evidence

- [ ] **Evidence pack**: `make gate` generates `_evidence/` directory
- [ ] **SLA metrics**: baseline captured per [SLA.md](SLA.md)
- [ ] **Version pinned**: `dist/VERSION` matches deployed binary
- [ ] **Change log**: release notes reviewed in `CHANGELOG.md`

---

## Quick Validation Commands

```bash
# Health check
curl -s http://localhost:8080/health

# Verify DCG
psql -h 127.0.0.1 -p 5443 -c "SHOW falcon.verification"

# Check replication status
psql -h 127.0.0.1 -p 5443 -c "SHOW falcon.replication_stats"

# View active connections
psql -h 127.0.0.1 -p 5443 -c "SHOW falcon.connections"

# Collect full evidence
make gate
```
