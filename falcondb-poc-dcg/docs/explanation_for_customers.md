# Understanding the Deterministic Commit Guarantee

This document explains, in plain language, what FalconDB's Deterministic Commit
Guarantee means and why the PoC demo is trustworthy.

No database expertise required.

---

## What Does "Commit" Mean?

When your application saves something to a database — a payment, an order, a
medical record — it goes through a process:

1. Your app sends the data to the database
2. The database writes it down
3. The database sends back a confirmation: **"Saved"**

That confirmation is called a **commit**. It is the database's promise that
your data is safe.

The question is: **how safe?**

---

## How Databases Can Lose Data

Most databases use a technique called "write-ahead logging." Before changing
the actual data, they write a note about what they're about to do. Think of it
like a pilot's pre-flight checklist — write down the plan before executing it.

But there's a gap between writing the note and actually making it permanent on
disk. If the server crashes during that gap, the note might be lost. And if the
note is lost, the data change is lost too — even though your app received a
"Saved" confirmation.

This is called **acknowledged data loss**: the database said "Saved" but the
data is gone.

### Common Causes

| Cause | What Happens |
|-------|-------------|
| **Power failure** | Server loses power mid-write |
| **Hardware failure** | Disk or memory fails |
| **Software crash** | Database process is killed |
| **Operating system crash** | Kernel panic, blue screen |

### Why It's Hard to Fix

The challenge is speed vs. safety. Making data truly permanent (flushing to
disk) is slow. Many databases cut corners to be faster:

- They confirm "Saved" before the data reaches disk
- They batch multiple saves together and flush once
- They rely on the operating system's write cache

These shortcuts make the database faster, but they create a window where
confirmed data can be lost.

---

## What FalconDB Does Differently

FalconDB takes a different approach. When your app sends data and gets back
"Saved", FalconDB has already:

1. **Written the data to a permanent log on disk** — not just to memory
2. **Forced the operating system to flush** — the data is physically on the
   storage device, not in a cache
3. **Sent the data to a backup server** — so it exists in two places

Only after all of this does FalconDB say "Saved."

This is the **Deterministic Commit Guarantee (DCG)**:

> If FalconDB says "Saved", the data is on disk and replicated.
> No exceptions. No edge cases. No "usually."

### Why "Deterministic"?

The word "deterministic" means "always the same outcome." It's not
probabilistic ("usually safe") or eventual ("will be safe eventually"). It's
a hard guarantee: every confirmed save is durable, every time.

---

## Why the Demo Is Trustworthy

The PoC demo is designed to be the hardest possible test:

### 1. We Use a Hard Crash

We don't ask the server to stop politely. We use `kill -9` on Linux or
`Stop-Process -Force` on Windows. This is the most violent way to terminate
a process — the operating system kills it instantly. The server gets:

- No warning
- No chance to flush buffers
- No chance to send final messages
- No graceful shutdown procedure

This simulates a real power failure or hardware crash.

### 2. Every Order Is Individual

We don't batch orders together. Each order is:

1. Begin transaction
2. Insert one order
3. Commit

And we only record the order ID *after* the commit confirmation comes back.
There's no way for an order to appear in our "confirmed" list unless the
server actually said "Saved."

### 3. Verification Is Independent

After the crash, we connect to the backup server and query every order. We
compare two lists:

- **What the server confirmed** (our log file)
- **What the backup server has** (database query)

If every confirmed order is in the database, the guarantee held. If even one
is missing, it failed.

### 4. You Can Inspect Everything

All configuration files are in the `configs/` directory. The confirmation log
is a plain text file (one order ID per line). The verification report is
human-readable. Nothing is hidden.

---

## What About the Backup Server?

The demo uses two servers:

```
Your App  →  Primary Server  →  Backup Server
              (port 5433)        (port 5434)
```

The primary server handles your requests. The backup server receives a copy
of everything in real time. When the primary crashes, the backup takes over.

The key insight: by the time the primary says "Saved", the data is already
on its way to the backup. The backup's copy may be a few seconds behind, but
any data the primary confirmed is guaranteed to arrive.

---

## What This Means for Your Business

| Scenario | Without DCG | With FalconDB DCG |
|----------|------------|-------------------|
| Server crashes after confirming a payment | Payment might be lost | Payment is safe |
| Power outage during peak traffic | Recent orders may disappear | All confirmed orders survive |
| Hardware failure on primary | Manual recovery, possible data loss | Automatic failover, zero data loss |

### The Bottom Line

If your system receives a "Saved" confirmation from FalconDB, you can tell
your customer "Your order is confirmed" with complete confidence. The data
will survive any single-server failure.

---

## Try It Yourself

Run the demo:

```bash
./run_demo.sh
```

Read the result:

```
Result: PASS
No committed data was lost.
```

That's the proof.

---

## Questions?

**"What if both servers crash at the same time?"**
That's beyond the scope of this demo (and extremely rare with proper
infrastructure). FalconDB supports multi-replica configurations for
even higher resilience.

**"Does this slow down the database?"**
There is a small cost — flushing to disk takes time. FalconDB uses
optimizations (group commit, efficient I/O) to minimize this cost.
See the benchmarks directory for performance numbers.

**"Can I run this on my own servers?"**
Yes. Copy the `falcondb-poc-dcg` directory, build FalconDB, install `psql`,
and run `./run_demo.sh`. No license or internet required.

**"What if I get a FAIL result?"**
Please contact us with the `output/` directory contents. A FAIL result
means we have a bug and we want to fix it immediately.
