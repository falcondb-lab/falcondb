# FalconDB — Failover Under Load · 5-Minute Demo

> **在高并发写入下，FalconDB 在 leader 被强制 kill 后完成自动 failover，
> 且所有已提交事务在 failover 后完整可读。**

---

## What This Demo Proves

Your application is saving money transfers. The database server crashes — not
a graceful shutdown, a hard kill during peak traffic. What happens to the
transactions the server already confirmed?

**FalconDB's answer: nothing is lost.** Every transaction the server confirmed
as "committed" is still there after failover. No exceptions.

---

## Run It (One Command)

### Prerequisites

| Tool | Purpose |
|------|---------|
| **Docker** (with Compose) | Runs the 3-node cluster |
| **psql** | Talks to the database |
| **Rust toolchain** | Builds the writer binary |

### Full Demo (≈5 minutes)

```bash
./demo.sh all
```

This single command will:
1. Build and start a **3-node Raft cluster**
2. Begin **20,000 sustained write transactions**
3. **SIGKILL the leader** during peak writes (no graceful shutdown)
4. Wait for **automatic failover** (new leader elected, writer reconnects)
5. Verify **0 data loss**

### Step-by-Step (Manual)

```bash
./demo.sh start      # Start cluster + begin writes
./demo.sh failover   # Kill the leader mid-write
./demo.sh verify     # Check: did we lose any committed data?
./demo.sh teardown   # Clean up
```

---

## What You Should See

```
═══  Phase 1: Starting 3-Node FalconDB Cluster  ═══

  ✓ 3-node cluster started
  ✓ All 3 nodes healthy

═══  Starting Sustained Write Load (20000 transactions)  ═══

  [1/8s] 1247 committed
  [2/8s] 2891 committed
  ...

═══  Phase 2: Killing the Leader (SIGKILL)  ═══

  → Detected leader: falcon-node1 (pg port 5433)

  >>> docker kill --signal=KILL falcon-node1 <<<

  → Committed before crash: 9482
  ✓ Leader killed

  → Writes resumed! (9520 committed, was 9482 at kill time)
  ✓ Writer finished. Total committed: 20000

═══  Phase 3: Consistency Verification  ═══

FAILOVER UNDER LOAD RESULT
--------------------------
Committed before failover: 20000
Committed after failover:  20000
Missing commits:           0
Phantom commits:           0
Duplicates:                0
Failover duration:         2s

RESULT: PASS (0 data loss)

  ════════════════════════════════════════════════════════
    PASS — 0 data loss under failover.
    All 20000 committed transactions survived.
  ════════════════════════════════════════════════════════
```

---

## How It Works

| Step | What Happens |
|------|-------------|
| **Writer commits** | Each transaction: `BEGIN → INSERT → COMMIT`. Only logged after server confirms. |
| **Leader is killed** | `docker kill --signal=KILL` — instant death. No flush, no cleanup. |
| **Raft elects new leader** | Remaining 2 nodes form quorum, elect new leader within seconds. |
| **Writer reconnects** | Automatically retries on all 3 endpoints. Resumes from exact failure point. |
| **Verify** | Compare client-side commit log vs database rows. Zero discrepancy = pass. |

### Why This Is Hard

Most databases can survive a planned shutdown. The hard test is:
**crash during active writes**. The transaction is half-written, the WAL is
mid-flush, the replication stream is mid-send. FalconDB's Deterministic Commit
Guarantee (DCG) ensures: if the server said "committed", it's durable — period.

---

## Output Files

| File | Contents |
|------|----------|
| `output/timeline.log` | Timestamped event log (start, kill, resume, verify) |
| `output/result.txt` | Human-readable verdict (for investors / slides) |
| `output/committed.log` | Client-side commit record (commit_id + timestamp) |
| `output/writer_metrics.json` | Writer stats (throughput, reconnects, latency) |

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `COMMIT_COUNT` | 20000 | Total transactions to write |
| `RAMP_SEC` | 8 | Seconds of writes before killing leader |

Example: run with 50K transactions and 15s ramp:

```bash
COMMIT_COUNT=50000 RAMP_SEC=15 ./demo.sh all
```
