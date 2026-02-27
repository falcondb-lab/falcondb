# Why Failover Under Load Is the Hardest Test

This document explains — in plain language — why this demo matters,
what it proves, and why you should trust the results.

---

## The Problem: Crashes Don't Wait for a Good Time

Imagine you run an online store. Customers are placing orders. Your database
is busy — hundreds of transactions per second. Then, without warning, the
primary server dies. Power failure. Hardware fault. Kernel panic. It doesn't
matter why — it's gone.

The question is: **what happened to the orders that were being saved?**

Specifically:

- The customer clicked "Place Order"
- Your application sent the order to the database
- The database said **"Confirmed"**
- Then the server crashed

**Is that order still there?** Or did it vanish?

---

## What Most Systems Get Wrong

Many databases handle crashes in one of two ways:

### 1. "We'll figure it out later" (Eventual Consistency)

Some systems acknowledge your write before it's fully safe. They promise
that the data will "eventually" be consistent. This means:

- Your application thinks the order is saved
- But if the server crashes right after confirmation, the order might be gone
- The backup might not have received it yet
- You won't know until you check — and by then, the customer has left

### 2. "We need a moment" (Long Recovery)

Some systems are safe but slow to recover. The backup exists, but:

- It takes minutes to detect the crash
- It takes more minutes to promote the backup
- During that time, your application is stuck
- Even after recovery, you're not sure if the last few seconds of data survived

### 3. "We lost a few" (The Gray Zone)

The most dangerous failure mode. The system recovers, looks healthy, but:

- A few transactions from the last second before the crash are missing
- No error was reported
- No alarm was raised
- You find out days later when a customer complains

---

## What FalconDB Guarantees

FalconDB makes one clear promise:

> **If the database said "Confirmed" to your application,
> that data will be on the backup server.**

This is not a "best effort" promise. It is a guarantee that holds even when:

- The primary crashes without warning
- The crash happens during active writes
- There is no time for a graceful shutdown
- The crash is as violent as possible (`kill -9`)

**How?** Every confirmed write is flushed to the write-ahead log (WAL) and
replicated to the backup before the database says "Confirmed." This means
the backup always has everything the application was told is safe.

---

## What This Demo Does

This demo recreates the worst-case scenario:

```
Timeline:
─────────────────────────────────────────────────────────────
  |  Start cluster (primary + backup)
  |  Start writing commit markers (sustained load)
  |  ................................................
  |  ............ ACTIVE WRITES ......................
  |  ............ CRASH PRIMARY ......................  ← kill -9
  |  ............ (writer detects failure) ...........
  |  Promote backup to primary
  |  ............ (writer reconnects) ................
  |  ............ CONTINUES WRITING ..................
  |  ................................................
  |  All markers written
  |  VERIFY: every confirmed marker is present
  |  Result: PASS
─────────────────────────────────────────────────────────────
```

### Key Points

- **The crash happens during active writes** — not after
- **The crash is violent** — `kill -9`, not a graceful stop
- **The writer reconnects automatically** — to the new primary
- **Every confirmed marker is checked** — not a sample, not a summary

---

## Why You Can Trust This Demo

### 1. The writer is honest

The writer only records a marker as "committed" **after** the database
confirms it. It does not pre-log, batch, or buffer. One transaction,
one confirmation, one log entry.

### 2. The crash is real

We use `kill -9` (Linux) or `Stop-Process -Force` (Windows). This is
the most violent way to terminate a process. There is no signal handler,
no final flush, no cleanup. The process simply stops existing.

### 3. The verification is independent

After the crash:
- We read the writer's log (what it was told is safe)
- We query the surviving database (what actually survived)
- We compare them

If anything is missing, the demo says **FAIL**. There is no "close enough."

### 4. Everything is recorded

Every output file is preserved:
- The committed markers log
- The surviving markers from the database
- The exact timestamps of crash and promotion
- The downtime measurement
- The full verification report

You can inspect every file. Nothing is hidden.

### 5. No tricks

- WAL is enabled (you can check the config)
- Replication is enabled (verified with a canary write)
- No batch commits (one marker per transaction)
- No retry masking (failures are recorded separately)
- No post-crash data cleanup

---

## What "Downtime" Means Here

This demo also measures how long the system was unavailable:

- **Start of downtime**: the moment the primary was killed
- **End of downtime**: the moment the new primary accepted its first write

Typical downtime in this demo is **1-3 seconds**. This number depends on:
- How fast the promotion command executes
- How fast the writer detects the failure and reconnects
- Hardware speed

**Downtime is informational, not a pass/fail metric.** The purpose of this
demo is correctness, not speed. But it's useful to know.

---

## What This Demo Does NOT Prove

Let's be clear about the boundaries:

- **Network partitions**: We crash a server, not the network
- **Multi-region failover**: Both servers are on the same machine
- **Multiple writers**: One writer is used for determinism
- **Performance during failover**: This is not a speed test
- **Long-running recovery**: The demo uses a small dataset

For performance comparison, see **PoC #2** (`falcondb-poc-pgbench/`).

---

## For Your Technical Team

If your engineers want to dig deeper:

1. **Inspect the configs**: `configs/primary.toml` and `configs/replica.toml`
   show WAL, fsync, and replication settings
2. **Read the writer source**: `workload/src/main.rs` — every line of the
   commit logic is visible
3. **Check the verification**: `scripts/verify_integrity.sh` — the comparison
   logic is straightforward
4. **Run it yourself**: The entire demo runs with one command on any machine
   with FalconDB and psql installed

---

## The Bottom Line

This demo answers one question:

> **"What happens if my primary crashes at peak traffic?"**

FalconDB's answer:

> **Every confirmed transaction survives. The backup takes over.
> Your application reconnects. No data is lost.**

This is not about speed. This is about trust.
