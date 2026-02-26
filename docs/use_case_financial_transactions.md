# FalconDB Use Case: Financial Transaction Processing

> **Audience**: CTOs, engineering leads, and investors evaluating FalconDB for
> financial services workloads.

---

## The Problem

Financial transaction systems have a non-negotiable requirement: **when the system
says "transfer complete", the money must actually have moved.** A phantom commit —
where the system reports success but the data is lost — is a regulatory violation,
a customer trust breach, and potentially a financial loss.

Traditional databases handle this with `synchronous_commit = on` (PostgreSQL) or
similar settings, but these are **configuration options that can be accidentally
turned off**, silently degrading the guarantee.

---

## Why FalconDB

FalconDB's **Deterministic Commit Guarantee (DCG)** is not a configuration option —
it is the default behavior. The system is architected so that:

1. **No ACK before fsync**: The client never receives "committed" until the WAL is
   physically written to disk (`fsync`). There is no gap between "acknowledged" and "durable."

2. **No phantom commits after failover**: When a replica is promoted to primary, its
   committed set is always a subset of the old primary's. No transactions appear out of nowhere.

3. **Deterministic crash recovery**: After a crash, every committed transaction is
   visible and every uncommitted transaction is cleanly rolled back. No manual intervention.

These properties are not claims — they are **tested in CI on every commit**:
- `cargo test -p falcon_storage --test consistency_wal` (10 WAL/crash tests)
- `cargo test -p falcon_cluster --test consistency_replication` (10 replication/failover tests)

---

## Architecture Fit

### Typical Financial Transaction Flow

```
Mobile App → API Gateway → FalconDB (Primary)
                               │
                               ├── WAL fsync (crash-safe)
                               ├── Replicate to Replica (async or sync)
                               └── ACK to API Gateway → "Transfer Complete"
```

### FalconDB Components Used

| Component | Role in Financial System |
|-----------|------------------------|
| **Fast-path commit** | Single-shard transfers (same-bank, same-account-group) — sub-millisecond |
| **2PC slow-path** | Cross-shard transfers (different account groups) — ~1ms with full atomicity |
| **WAL + fsync** | Crash safety — the ledger is always consistent |
| **Epoch fencing** | Prevents split-brain — old primary can never accept stale writes after failover |
| **Commit policies** | `LocalWalSync` for single-node, `PrimaryPlusReplicaAck` for zero-loss failover |

### What FalconDB Replaces

| Current Stack | FalconDB Replacement | Benefit |
|--------------|---------------------|---------|
| PostgreSQL + manual failover scripts | FalconDB with built-in promote/fencing | No custom failover code to maintain |
| Application-level idempotency | FalconDB's at-most-once commit (XS-2) | Simpler application logic |
| Two-phase commit via application code | FalconDB's built-in 2PC coordinator | Atomic cross-account transfers without application coordination |

---

## Sample Schema

```sql
-- Core ledger tables
CREATE TABLE accounts (
    account_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    balance BIGINT NOT NULL DEFAULT 0,       -- stored in cents
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    status TEXT NOT NULL DEFAULT 'active',
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE transactions (
    txn_id BIGSERIAL PRIMARY KEY,
    from_account BIGINT REFERENCES accounts(account_id),
    to_account BIGINT REFERENCES accounts(account_id),
    amount BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_txn_from ON transactions(from_account);
CREATE INDEX idx_txn_to ON transactions(to_account);
```

### Transfer Operation (Single-Shard — Fast Path)

```sql
BEGIN;
UPDATE accounts SET balance = balance - 10000 WHERE account_id = 1001 AND balance >= 10000;
UPDATE accounts SET balance = balance + 10000 WHERE account_id = 1002;
INSERT INTO transactions (from_account, to_account, amount, currency, status)
    VALUES (1001, 1002, 10000, 'USD', 'completed');
COMMIT;
-- When this returns OK, the transfer is crash-safe. Period.
```

---

## Performance Expectations

| Metric | Value | Notes |
|--------|-------|-------|
| Transfer TPS (single-shard) | 40,000–50,000 | Sub-ms per transfer, 8 concurrent clients |
| Transfer TPS (cross-shard) | 8,000–12,000 | 2PC overhead, still sub-5ms p99 |
| Crash recovery time | 1–5 seconds | Depends on WAL size since last checkpoint |
| Failover time | 2–8 seconds | Detection + promote + epoch bump |
| Data loss (LocalWalSync) | **Zero** | Fsync before ACK — no exceptions |

See [docs/benchmarks/v1.0_baseline.md](benchmarks/v1.0_baseline.md) for reproduction steps.

---

## Compliance Considerations

| Requirement | How FalconDB Addresses It |
|-------------|--------------------------|
| **Durability (SOX, PCI-DSS)** | WAL fsync before ACK; DCG verified by CI tests |
| **Audit trail** | Enterprise audit log with HMAC chain (tamper-evident) |
| **Access control** | SCRAM-SHA-256 auth, RBAC at table level, TLS encryption |
| **Disaster recovery** | WAL-shipping replication, configurable backup/PITR |
| **Incident response** | Structured logs, Prometheus metrics, health endpoints |

---

## Evaluation Checklist

For a PoC evaluation of FalconDB in a financial transaction context:

- [ ] Run consistency tests: `cargo test -p falcon_storage --test consistency_wal`
- [ ] Run failover tests: `cargo test -p falcon_cluster --test consistency_replication`
- [ ] Run Gameday GD-1 (process kill) and GD-2 (failover) drills — see [docs/gameday.md](gameday.md)
- [ ] Benchmark with your transfer workload using `pgbench`
- [ ] Enable production safety mode: `[production_safety] enforce = true`
- [ ] Review non-goals: [docs/non_goals.md](non_goals.md) — confirm your workload is OLTP

---

## What FalconDB Does NOT Do for Financial Systems

- **Does not replace a payment gateway** — FalconDB is the ledger, not the payment rail
- **Does not provide multi-region active-active** — single-region with failover only
- **Does not handle regulatory reporting/analytics** — use a separate OLAP system
- **Does not provide PCI-DSS certification** — the database supports compliance, but certification is an organizational process

See [docs/non_goals.md](non_goals.md) for the full scope declaration.
