# Why Cost Efficiency Matters More Than Raw Speed

This document explains — in plain language — why most databases cost more
than they should, how FalconDB changes the equation, and what that means
for your infrastructure budget.

---

## The Problem: Over-Provisioning as Insurance

Most production databases are over-provisioned. Not because the team is
wasteful, but because **uncertainty is expensive**.

```
What you need:           What you deploy:

  ┌──────────┐            ┌──────────────────────┐
  │  Actual   │            │                      │
  │  Load     │            │   "Just in case"     │
  │  ████████ │            │   ░░░░░░░░░░░░░░░░░░ │
  │  ████████ │            │   ░░░░░░░░████████░░ │
  │  ████████ │            │   ░░░░░░░░████████░░ │
  └──────────┘            └──────────────────────┘
  4 vCPU / 16 GB          16 vCPU / 64 GB
  $200/mo                  $800/mo
```

Why? Because traditional databases have **unpredictable behavior under load**:
- Latency spikes at random times
- Memory usage grows without clear limits
- Vacuum storms consume CPU unexpectedly
- Replication lag spikes under write bursts

So teams add 2–4x headroom. Not for performance — for **safety margin**.

---

## The Root Cause: Non-Deterministic Behavior

Traditional databases do a lot of speculative and background work:

| Activity | What It Does | Cost |
|----------|-------------|------|
| **Shared buffer management** | Cache pages hoping they'll be reused | Gigabytes of RAM |
| **Autovacuum** | Clean up dead tuples (unpredictable timing) | CPU spikes |
| **Background writer** | Flush dirty pages (unpredictable IO) | Disk throughput |
| **Query plan variability** | Same query, different execution plans | Latency variance |
| **Connection overhead** | Per-connection memory allocation | RAM per client |

None of this is "wrong" — it's how PostgreSQL works. But it means:

> **You can't predict how much resource a given workload will actually need.**

So you over-provision, because the alternative is downtime.

---

## FalconDB's Approach: Deterministic Execution

FalconDB takes a different approach:

```
Traditional DB:                  FalconDB:

  "How much RAM do I need?"       "How much RAM do I need?"
  "It depends on the workload,     "768 MB hard limit.
   cache hit ratio, vacuum          That's the maximum.
   schedule, connection count..."   Under any load."
```

### What "Deterministic" Means in Practice

| Property | Traditional | FalconDB |
|----------|-----------|----------|
| **Memory usage** | Grows with cache pressure | Bounded by configured limit |
| **Latency under load** | Varies (vacuum, buffer eviction) | Consistent (no background storms) |
| **CPU usage** | Spikes from background processes | Proportional to actual work |
| **Recovery after peak** | May take minutes (buffer re-warming) | Immediate (no cache dependency) |

---

## What This Means for Cost

### Traditional Sizing

```
Workload: 1,000 tx/s OLTP

PostgreSQL sizing:
  - 8 vCPU (need headroom for vacuum + background writer)
  - 64 GB RAM (need large shared_buffers + cache)
  - Instance: r6g.2xlarge
  - Monthly: ~$294/mo

  Why so large?
  - shared_buffers = 4 GB (standard 25% rule)
  - effective_cache_size = 12 GB
  - Need headroom for autovacuum CPU spikes
  - Need headroom for connection memory
```

### FalconDB Sizing

```
Same workload: 1,000 tx/s OLTP

FalconDB sizing:
  - 4 vCPU (proportional to actual work)
  - 8 GB RAM (512 MB soft limit is sufficient)
  - Instance: c6g.xlarge
  - Monthly: ~$99/mo

  Why smaller?
  - No large buffer pool needed
  - No autovacuum CPU overhead
  - Memory is bounded, not hoped-for
  - CPU usage is predictable
```

### The Math

```
Same SLA. Same durability. Same workload.

PostgreSQL: ~$294/mo
FalconDB:   ~$99/mo

Savings: ~66%
```

Over 3 years across 10 database instances:

```
PostgreSQL: $294 × 10 × 36 = $105,840
FalconDB:   $99  × 10 × 36 = $35,640

Difference: $70,200
```

That's **not** a performance improvement. That's a **cost reduction**
with the same business outcome.

---

## Peak Load Behavior

The real test is what happens during a traffic spike:

### Traditional DB During 2x Peak

```
Time →
            Normal      Peak (2x)      Recovery
CPU:     ████░░░░░   █████████░   ████████░░   ← buffer re-warming
Latency: ──────────  ─────/\────  ────/\──────  ← spikes during & after
Memory:  ████████░░  ██████████   ██████████   ← stays high
```

- CPU spikes past 80%
- Latency spikes 3–5x
- Memory stays elevated after peak
- Recovery takes minutes (cache warming)

### FalconDB During 2x Peak

```
Time →
            Normal      Peak (2x)      Recovery
CPU:     ████░░░░░   ████████░░   ████░░░░░   ← proportional
Latency: ──────────  ──────────   ──────────   ← stable
Memory:  ███░░░░░░   ███░░░░░░   ███░░░░░░   ← bounded
```

- CPU increases proportionally (no background storms)
- Latency stays within 2x (not 5x)
- Memory doesn't change (bounded)
- Recovery is immediate (no cache dependency)

---

## What This PoC Proves

This demo runs the **same workload** against both databases with:
- ✅ Full durability (WAL + fsync on both)
- ✅ Same target transaction rate
- ✅ Same OLTP pattern (transactional updates)
- ✅ A peak load simulation (2x for 30 seconds)

It measures:
- **Throughput**: Can both sustain the target rate?
- **Latency**: How stable is the tail latency?
- **Resources**: How much CPU and memory does each actually use?
- **Recovery**: How quickly does each return to normal after peak?

It does **NOT** prove:
- ❌ That FalconDB is always faster
- ❌ That PostgreSQL is bad
- ❌ That these numbers apply to every workload
- ❌ That you should migrate without testing

---

## The Decision Framework

```
Question: "Should we consider FalconDB for cost reasons?"

If your workload is:
  ✅ Standard OLTP (INSERT/UPDATE/SELECT with transactions)
  ✅ Latency-sensitive (p95/p99 matters)
  ✅ Running on multiple instances
  ✅ Over-provisioned "just in case"

Then:
  → This PoC gives you concrete numbers to evaluate.
  → Run it on your own hardware with your own workload.
  → Compare the resource usage, not just the TPS.

If your workload is:
  ⚠️ Analytics-heavy (large scans, window functions)
  ⚠️ Extension-dependent (PostGIS, pg_trgm)
  ⚠️ Single-instance with plenty of headroom

Then:
  → Cost savings may be smaller.
  → Migration effort may outweigh savings.
```

---

## The Bottom Line

> **FalconDB lets you meet the same business SLAs
> with less hardware and more predictability.**

This is not about being "faster". It's about being **efficient**.

The difference between a $300/month database and a $100/month database,
multiplied across your fleet, multiplied across years, is the difference
between a project that gets funded and one that doesn't.

**Predictability is not just a technical property. It's a budget property.**
