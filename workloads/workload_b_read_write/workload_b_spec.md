# Workload B: Read-Write OLTP

## Goal

Simulate a realistic OLTP workload (order/account pattern). Tests the executor,
MVCC read path, and write path under mixed concurrency.

## Operation Mix

| Op | Weight | SQL |
|----|--------|-----|
| SELECT | 50% | `SELECT ... FROM users WHERE user_id = :uid` |
| INSERT | 25% | `INSERT INTO orders (order_id, user_id, amount) VALUES (...)` |
| UPDATE | 25% | `UPDATE users SET balance = balance + :amount WHERE user_id = :uid` |

## Schema

- `users` — 50K pre-seeded accounts (BIGINT PK)
- `orders` — 100K pre-seeded orders (BIGINT PK), grows during test

All lookups are PK point queries. No JOINs. Short transactions.

## Durability

Same as Workload A — fsync + synchronous commit on both targets.

## Parameters

| Param | Default | Description |
|-------|---------|-------------|
| `--clients` | 16 | Concurrent pgbench clients |
| `--duration` | 60 | Seconds |
| `--target` | falcondb | `falcondb` or `postgres` |

## Output Metrics

| Metric | Source |
|--------|--------|
| **TPS** | pgbench |
| **Avg Latency** | pgbench |
| **New Orders** | post - pre order count |
| **PK Unique** | verify.sql |

## Usage

```bash
./run.sh --target falcondb --clients 16 --duration 60
./run.sh --target postgres --clients 16 --duration 60
```
