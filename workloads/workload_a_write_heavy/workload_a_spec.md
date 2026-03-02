# Workload A: Write-Heavy

## Goal

Stress-test the WAL write + fsync path. Every transaction commits a single write
and waits for WAL flush before returning. Measures raw write throughput under
full durability.

## Operation Mix

| Op | Weight | SQL |
|----|--------|-----|
| INSERT | 90% | `INSERT INTO orders (order_id, customer_id, amount) VALUES (...)` |
| UPDATE | 10% | `UPDATE orders SET amount = ... WHERE order_id = ...` |

## Schema

Single table `orders` with BIGINT PK. Pre-seeded with 10K rows for UPDATE targets.

## Durability

- FalconDB: `wal_enabled = true`, `sync_mode = "fdatasync"`, `group_commit = true`
- PostgreSQL: `fsync = on`, `synchronous_commit = on`

No async commit. No unlogged tables. No batch commit bypass.

## Parameters

| Param | Default | Description |
|-------|---------|-------------|
| `--clients` | 16 | Concurrent pgbench clients |
| `--duration` | 60 | Test duration in seconds |
| `--target` | falcondb | `falcondb` or `postgres` |

## Output Metrics

| Metric | Source | Description |
|--------|--------|-------------|
| **TPS** | pgbench | Transactions per second (excluding conn) |
| **Avg Latency** | pgbench | Average transaction latency in ms |
| **P99 Latency** | pgbench `-P` | 99th percentile latency |
| **Rows Inserted** | verify.sql | Net new rows after workload |
| **PK Unique** | verify.sql | 0 duplicates = pass |

## Reproducibility

Run 3 times, take the median TPS. Variance should be ≤5% across runs on same hardware.

## Usage

```bash
# FalconDB
./run.sh --target falcondb --clients 16 --duration 60

# PostgreSQL
./run.sh --target postgres --clients 16 --duration 60
```
