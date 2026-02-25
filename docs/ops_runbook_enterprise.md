# FalconDB Enterprise Ops Runbook

## Overview
This runbook covers operational procedures for FalconDB v1.1.0 enterprise
deployments, including the control plane, security, backup, and automation.

## Scenario: Node Failure

### Detection
1. Controller node registry detects missing heartbeat
2. Node transitions: Online → Suspect → Offline
3. Timeline event recorded: `NodeFailure(node_id)`
4. Incident auto-correlation groups related events

### Response
1. Controller removes node from shard placements
2. Leader election triggered for affected shards
3. Auto-rebalancer schedules shard migrations to remaining nodes
4. SLO engine tracks availability impact

### Verification
- `GET /admin/nodes` — confirm node state is OFFLINE
- `GET /admin/shards` — confirm no orphaned shards
- `GET /admin/slo` — confirm SLOs recovering
- `GET /admin/incidents` — review correlated incident

## Scenario: Certificate Rotation

### Pre-Rotation
1. Generate new certificates from enterprise CA
2. Verify cert validity: `not_before` < now < `not_after`
3. Check all 4 link types have current certs

### Rotation Steps
```rust
cert_mgr.rotate_cert(TlsLinkType::ClientToGateway, "/new/cert.pem", "/new/key.pem", "new-fingerprint", not_before, not_after);
```

### Post-Rotation
- Verify: `cert_mgr.all_links_covered()` returns empty Vec
- Verify: `cert_mgr.expiring_certs(86400)` returns no near-expiry certs
- Check rotation history: `cert_mgr.rotation_history(10)`
- Audit log records: category=SECURITY, action="CERT_ROTATION"

### Rollback
- Keep old cert files for 24h after rotation
- If issues detected, rotate back to old cert

## Scenario: Backup & PITR Restore

### Scheduled Full Backup
1. `schedule_backup(Full, target, "cron")` → job_id
2. `start_backup(job_id, current_lsn)`
3. Backup engine writes data + metadata
4. `complete_backup(job_id, bytes, tables, end_lsn, checksum)`
5. Audit log: category=BACKUP_RESTORE

### Point-in-Time Recovery
1. Identify target timestamp from incident timeline
2. `schedule_restore(source, backup_id, ToTimestamp, None, Some(target_ts))`
3. Monitor progress via `get_restore_job(restore_id)`
4. Verify data consistency after restore

### Backup Verification
- Check `latest_full_backup()` is recent
- Compare backup checksum with source
- Test restore to staging cluster periodically

## Scenario: Permission Escalation Attempt

### Detection
- Audit log shows repeated AUTHZ denials from same actor
- `query_by_category(Authorization, 100)` shows pattern
- SLO engine may show elevated error rate

### Investigation
1. Check actor's effective roles
2. Review recent grant/revoke activity in audit log
3. Determine if legitimate user or compromised account

### Response
- If compromised: `disable_user(user_id)`
- If misconfigured: adjust grants via RBAC
- Record incident in timeline

## Scenario: Auto-Rebalance

### Trigger: Node Join
1. New node registered via `NodeRegistry::register()`
2. Auto-rebalancer computes plan: `compute_plan(assignments, available_nodes)`
3. Migrations scheduled up to `max_concurrent` limit
4. Each migration follows: Pending → Preparing → Copying → CatchingUp → Cutover → Completed

### SLA Protection
- If latency exceeds `max_sla_impact_ms`, rebalancer pauses automatically
- Resume manually or wait for metrics to recover
- Track via `RebalanceMetrics::sla_pauses`

### Monitoring
- `GET /admin/rebalance` — active and pending migrations
- `active_migrations()` — current count
- `pending_tasks()` — queued migrations

## Scenario: SLO Breach

### Detection
1. SLO engine evaluates all defined SLOs
2. Breach detected when actual < target (availability) or actual > target (latency)
3. Error budget depletion tracked
4. Timeline event: `SloBreached(slo_id)`

### Response
1. Check incident timeline for root cause
2. Review capacity alerts for resource pressure
3. Check backpressure controller for admission state
4. Consider: scaling, config tuning, or workload management

### Export & Alerting
- Prometheus: `export_prometheus()` returns metric lines
- `/admin/slo` returns current evaluations
- Integrate with alerting (PagerDuty, OpsGenie)

## Health Check Thresholds

| Component | Metric | Watch | Warning | Critical |
|-----------|--------|-------|---------|----------|
| Availability | SLO actual | < 99.95% | < 99.9% | < 99.0% |
| Latency p99 | ms | > 20ms | > 50ms | > 100ms |
| Replication lag | LSN | > 100 | > 1000 | > 10000 |
| Resource usage | % | > 70% | > 85% | > 95% |
| Cert expiry | hours | < 72h | < 24h | < 6h |
| Error budget | remaining | < 50% | < 20% | < 5% |
