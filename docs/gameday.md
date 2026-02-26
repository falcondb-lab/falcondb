# FalconDB — Gameday / Failure Drill Playbook

> **Purpose**: Provide SREs and on-call engineers with step-by-step failure drills that
> validate FalconDB's behavior under real failure conditions. Each drill is designed to
> be run in a staging environment and takes < 15 minutes.

---

## Prerequisites

- FalconDB cluster: 1 primary + 1–2 replicas (can use `examples/primary.toml` and `examples/replica.toml`)
- `psql` client connected to primary
- `kill`, `iptables`/`netsh`, and basic shell access to nodes
- Monitoring dashboard or `curl` access to admin endpoint (`:8080/health`)

---

## Drill Index

| # | Drill | Tests Property | Duration | Risk Level |
|---|-------|---------------|----------|------------|
| GD-1 | [Primary Process Kill](#gd-1-primary-process-kill) | Crash recovery, WAL durability | 5 min | Low |
| GD-2 | [Primary → Replica Failover](#gd-2-primary--replica-failover) | Failover correctness, epoch fencing | 10 min | Low |
| GD-3 | [Network Partition (Primary ↔ Replica)](#gd-3-network-partition) | Replication lag handling, split-brain prevention | 10 min | Medium |
| GD-4 | [Disk Full on Primary](#gd-4-disk-full-on-primary) | WAL admission control, graceful degradation | 10 min | Medium |
| GD-5 | [Memory Pressure](#gd-5-memory-pressure) | Backpressure, OOM prevention | 10 min | Low |
| GD-6 | [Slow Replica / Replication Lag](#gd-6-slow-replica) | Lag monitoring, admission throttling | 5 min | Low |
| GD-7 | [Concurrent Failover Stress](#gd-7-concurrent-failover-stress) | Multi-failover data integrity | 15 min | Low |

---

## GD-1: Primary Process Kill

**What this tests**: WAL crash recovery — committed data survives a hard kill.

### Steps

```bash
# 1. Insert known data
psql -h primary -p 5433 -U falcon -c "
  CREATE TABLE IF NOT EXISTS gameday_gd1 (id INT PRIMARY KEY, val TEXT);
  INSERT INTO gameday_gd1 VALUES (1, 'before_kill'), (2, 'also_before_kill')
    ON CONFLICT DO NOTHING;
"

# 2. Verify data exists
psql -h primary -p 5433 -U falcon -c "SELECT * FROM gameday_gd1 ORDER BY id;"
# Expected: 2 rows

# 3. Hard kill the primary (simulates crash)
kill -9 $(pidof falcon)

# 4. Restart the primary
./target/release/falcon -c examples/primary.toml &

# 5. Wait for startup (watch logs for "FalconDB ready")
sleep 5

# 6. Verify data survived
psql -h primary -p 5433 -U falcon -c "SELECT * FROM gameday_gd1 ORDER BY id;"
```

### Expected Result

| Check | Expected |
|-------|----------|
| Row count after recovery | **2** (both rows survive) |
| Row values | `before_kill`, `also_before_kill` — unchanged |
| Recovery time | < 5 seconds (depends on WAL size) |
| Log message | `Recovering from WAL at ...` followed by `FalconDB ready` |

### Failure Indicators

- ❌ Row count < 2 → WAL corruption or sync_mode misconfigured (check PS-1, PS-2)
- ❌ Recovery hangs → WAL segment may be corrupted; check for CRC errors in logs
- ❌ Different row values → MVCC visibility bug; escalate to engineering

### Automated Equivalent

```bash
cargo test -p falcon_storage --test consistency_wal test_wal4_wal5
cargo test -p falcon_storage --test consistency_wal test_crash_before_wal_write
cargo test -p falcon_storage --test consistency_wal test_crash_after_wal_write_before_commit
```

---

## GD-2: Primary → Replica Failover

**What this tests**: Failover preserves committed data and fences the old primary.

### Steps

```bash
# 1. Insert data on primary and confirm replication
psql -h primary -p 5433 -U falcon -c "
  CREATE TABLE IF NOT EXISTS gameday_gd2 (id INT PRIMARY KEY, val TEXT);
  INSERT INTO gameday_gd2 VALUES (1, 'replicated_1'), (2, 'replicated_2')
    ON CONFLICT DO NOTHING;
"

# 2. Verify replica has the data (wait for replication lag to clear)
sleep 2
psql -h replica -p 5434 -U falcon -c "SELECT * FROM gameday_gd2 ORDER BY id;"
# Expected: 2 rows (same as primary)

# 3. Kill the primary
kill -9 $(pidof falcon)  # on primary node

# 4. Promote the replica
# (In production, this is triggered by the control plane or manual command)
psql -h replica -p 5434 -U falcon -c "SELECT falcon_promote();"
# Or use the admin API:
# curl -X POST http://replica:8080/admin/promote

# 5. Verify new primary has all data
psql -h replica -p 5434 -U falcon -c "SELECT * FROM gameday_gd2 ORDER BY id;"

# 6. Verify new primary accepts writes
psql -h replica -p 5434 -U falcon -c "
  INSERT INTO gameday_gd2 VALUES (3, 'after_failover');
  SELECT * FROM gameday_gd2 ORDER BY id;
"
```

### Expected Result

| Check | Expected |
|-------|----------|
| Rows on new primary | **2** (pre-failover) → **3** (after new write) |
| Old primary (if restarted) | Fenced — refuses writes, epoch rejected |
| Failover time | < 10 seconds |
| Phantom commits | **Zero** — no data appears that wasn't committed |

### Automated Equivalent

```bash
cargo test -p falcon_cluster --test consistency_replication test_fail1
cargo test -p falcon_cluster --test consistency_replication test_fail2
cargo test -p falcon_cluster --test consistency_replication test_fail3
```

---

## GD-3: Network Partition

**What this tests**: FalconDB handles network splits without split-brain or data corruption.

### Steps

```bash
# 1. Insert baseline data
psql -h primary -p 5433 -U falcon -c "
  CREATE TABLE IF NOT EXISTS gameday_gd3 (id INT PRIMARY KEY, val TEXT);
  INSERT INTO gameday_gd3 VALUES (1, 'before_partition') ON CONFLICT DO NOTHING;
"

# 2. Partition: block gRPC replication traffic (Linux)
sudo iptables -A INPUT -p tcp --dport 50051 -j DROP
sudo iptables -A OUTPUT -p tcp --dport 50051 -j DROP

# (Windows alternative)
# netsh advfirewall firewall add rule name="block_grpc" dir=in action=block protocol=tcp localport=50051

# 3. Insert more data on primary (replication will stall)
psql -h primary -p 5433 -U falcon -c "
  INSERT INTO gameday_gd3 VALUES (2, 'during_partition') ON CONFLICT DO NOTHING;
"

# 4. Check replication status
psql -h primary -p 5433 -U falcon -c "SHOW falcon.replication_status;"
# Expected: replica lag increasing, status = "lagging" or "disconnected"

# 5. Heal the partition
sudo iptables -D INPUT -p tcp --dport 50051 -j DROP
sudo iptables -D OUTPUT -p tcp --dport 50051 -j DROP

# 6. Wait for replication to catch up
sleep 5
psql -h replica -p 5434 -U falcon -c "SELECT * FROM gameday_gd3 ORDER BY id;"
```

### Expected Result

| Check | Expected |
|-------|----------|
| Primary during partition | Continues accepting writes (no stall) |
| Replica during partition | Stale — missing rows written during partition |
| After heal | Replica catches up; both nodes have identical data |
| Replication lag metric | Spikes during partition, returns to ~0 after heal |

---

## GD-4: Disk Full on Primary

**What this tests**: WAL admission control rejects new writes when disk is full.

### Steps

```bash
# 1. Fill the disk (create a large file in the WAL directory)
dd if=/dev/zero of=/var/lib/falcondb/data/fill_disk.tmp bs=1M count=50000
# (Adjust count to fill available space; leave ~10 MB free)

# 2. Attempt a write
psql -h primary -p 5433 -U falcon -c "
  INSERT INTO gameday_gd3 VALUES (99, 'disk_full_test');
"
# Expected: ERROR with message about WAL write failure or disk space

# 3. Clean up
rm /var/lib/falcondb/data/fill_disk.tmp

# 4. Verify writes resume
psql -h primary -p 5433 -U falcon -c "
  INSERT INTO gameday_gd3 VALUES (99, 'after_cleanup');
  SELECT * FROM gameday_gd3 WHERE id = 99;
"
```

### Expected Result

| Check | Expected |
|-------|----------|
| Write during disk full | **Rejected** with error (not silently dropped) |
| Existing data | **Intact** — no corruption from failed write |
| After cleanup | Writes resume normally |

---

## GD-5: Memory Pressure

**What this tests**: Backpressure mechanism prevents OOM kill.

### Steps

```bash
# 1. Configure tight memory limits in falcon.toml
# [memory]
# shard_soft_limit_bytes = 104857600    # 100 MB
# shard_hard_limit_bytes = 209715200    # 200 MB

# 2. Load data until soft limit is hit
psql -h primary -p 5433 -U falcon -c "
  CREATE TABLE IF NOT EXISTS gameday_gd5 (id INT PRIMARY KEY, payload TEXT);
"
for i in $(seq 1 100000); do
  psql -h primary -p 5433 -U falcon -c \
    "INSERT INTO gameday_gd5 VALUES ($i, repeat('x', 1000)) ON CONFLICT DO NOTHING;" 2>/dev/null
done

# 3. Check memory state
psql -h primary -p 5433 -U falcon -c "SHOW falcon.memory_status;"
# Expected: state = "pressure" or "critical"

# 4. Attempt more writes
psql -h primary -p 5433 -U falcon -c "
  INSERT INTO gameday_gd5 VALUES (999999, repeat('y', 10000));
"
# Expected: rejected or delayed (depending on pressure_policy)

# 5. Trigger GC
psql -h primary -p 5433 -U falcon -c "SHOW falcon.gc_stats;"
```

### Expected Result

| Check | Expected |
|-------|----------|
| State at soft limit | `pressure` — writes delayed or rejected |
| State at hard limit | `critical` — all new txns rejected |
| Process alive | **Yes** — no OOM kill |
| Existing data | **Intact** — reads still work |

---

## GD-6: Slow Replica

**What this tests**: Replication lag monitoring and admission throttling.

### Steps

```bash
# 1. Slow down replica by adding artificial delay
# (Restart replica with artificially high poll_interval_ms)
# In replica.toml: replication.poll_interval_ms = 5000

# 2. Write burst on primary
for i in $(seq 1 10000); do
  psql -h primary -p 5433 -U falcon -c \
    "INSERT INTO gameday_gd3 VALUES ($((1000+i)), 'burst_$i') ON CONFLICT DO NOTHING;" 2>/dev/null
done

# 3. Check replication lag
psql -h primary -p 5433 -U falcon -c "SHOW falcon.replication_lag;"

# 4. If admission control is configured (wal.replication_lag_admission_threshold_ms > 0),
#    verify that new writes are throttled when lag exceeds threshold
psql -h primary -p 5433 -U falcon -c "
  INSERT INTO gameday_gd3 VALUES (99999, 'should_be_throttled');
"

# 5. Restore normal polling and verify catch-up
# Reset replica.toml to poll_interval_ms = 100, restart replica
```

### Expected Result

| Check | Expected |
|-------|----------|
| Lag metric | Increases during burst, decreases after |
| Admission control (if configured) | Rejects/delays writes when lag exceeds threshold |
| Catch-up | Replica eventually converges to primary state |

---

## GD-7: Concurrent Failover Stress

**What this tests**: Data integrity survives multiple sequential failovers.

### Steps

```bash
# This drill uses the automated test harness:
cargo test -p falcon_cluster --test consistency_replication test_multiple_failovers_maintain_data -- --nocapture

# For a manual version with a live cluster:
# 1. Write data → 2. Failover → 3. Write more → 4. Failover again → 5. Verify all data
```

### Automated Matrix (CI-integrated)

```bash
# Run the full failover determinism matrix (9 scenarios × 3 fault types × 3 load types):
cargo test -p falcon_cluster --test failover_determinism -- --nocapture

# Or use the script:
bash scripts/run_failover_matrix.sh
```

### Expected Result

| Check | Expected |
|-------|----------|
| Data after 2+ failovers | **All committed rows survive** |
| Phantom commits | **Zero** |
| Epoch monotonicity | Strictly increasing after each promote |

---

## Post-Drill Checklist

After each drill, verify these invariants:

- [ ] All committed data is intact (query and count rows)
- [ ] No phantom commits (no data that was never committed)
- [ ] Replication lag returns to ~0 (if replicas are present)
- [ ] Health endpoint returns `200 OK` with `"ready": true`
- [ ] Metrics endpoint is responsive (`curl http://node:8080/metrics`)
- [ ] Log files show no `PANIC` or `InvariantViolation` entries

---

## Scheduling Recommendations

| Drill | Frequency | Environment |
|-------|-----------|-------------|
| GD-1 (process kill) | Weekly | Staging |
| GD-2 (failover) | Weekly | Staging |
| GD-3 (network partition) | Monthly | Staging |
| GD-4 (disk full) | Monthly | Staging (dedicated disk) |
| GD-5 (memory pressure) | Monthly | Staging |
| GD-6 (slow replica) | Monthly | Staging |
| GD-7 (multi-failover) | Every release | CI (automated) |

---

## Escalation

If any drill produces unexpected results:

1. **Capture logs**: `journalctl -u falcondb` or `falcon_data/logs/falcon.log`
2. **Capture metrics**: `curl http://node:8080/metrics > metrics_snapshot.txt`
3. **Run consistency check**: `cargo test -p falcon_storage --test consistency_wal`
4. **File issue** with logs, metrics, and drill number (e.g. "GD-1 failure: row count mismatch")
