# FalconDB — Failover Under Load (Correctness + Availability)

> **This demo proves that FalconDB does not lose acknowledged commits
> even when the primary server crashes during peak write traffic.**

Most databases can survive a crash if nothing is happening. The real question
is: what happens when the server crashes while your application is actively
saving data? Do you lose the last few seconds of work? Do some transactions
vanish silently? Does the system enter an ambiguous state?

FalconDB's answer: **nothing is lost**. Every transaction the server confirmed
is present on the backup, even through a crash during peak traffic.

---

## What Happens in This Demo

1. We start **two FalconDB servers** — a primary and a backup
2. We verify that replication is fully caught up
3. We start a **sustained write workload** (50,000 commit markers)
4. While the workload is actively running, we **crash the primary**
5. The backup takes over as the new primary
6. The writer **automatically reconnects** and finishes its work
7. We measure how long the system was unavailable
8. We verify: **every confirmed commit survived the crash**

The answer should be **PASS — zero data loss**.

---

## Run It (One Command)

### What You Need

| Tool | What It Does | How to Get It |
|------|-------------|---------------|
| **FalconDB** | The database being tested | `cargo build -p falcon_server --release` |
| **psql** | Talks to the database | Comes with PostgreSQL |

### Linux / macOS

```bash
cargo build -p falcon_server --release
./falcondb-poc-failover-under-load/run_demo.sh
```

### Windows (PowerShell 7+)

```powershell
cargo build -p falcon_server --release
.\falcondb-poc-failover-under-load\run_demo.ps1
```

### What You Should See

```
  === Step 4/8: Letting load ramp up (5s) ===
  [1/5s] 4,231 markers committed
  [2/5s] 8,762 markers committed
  ...

  === Step 5/8: KILLING the primary (DURING ACTIVE WRITES) ===
  >>> kill -9 12345 <<<
  > The primary is being killed DURING ACTIVE WRITES.
  > Markers committed before crash: 12,481

  === Step 8/8: Measuring downtime and verifying integrity ===

  ============================================================
    FalconDB PoC #3 — Failover Under Load: Verification Report
  ============================================================

    Total committed markers (acknowledged): 50,000
    Markers after failover:                 50,000

    Missing (data loss):             0
    Phantom (unexpected):            0
    Duplicates:                      0

    Downtime window:                 1.7s

    Result: PASS
    All acknowledged commits survived the failover.
  ============================================================
```

---

## Run It Step by Step

```bash
# 1. Start the two-server cluster
./scripts/start_cluster.sh

# 2. Verify replication is working
./scripts/wait_cluster_ready.sh

# 3. Start the write workload (runs in background)
./scripts/start_load.sh

# 4. Wait a few seconds for load to build up
sleep 5

# 5. Crash the primary (while writes are happening)
./scripts/kill_primary.sh

# 6. Promote the backup to primary
./scripts/promote_replica.sh

# 7. Wait for the writer to reconnect and finish
# (it will automatically reconnect to the new primary)

# 8. Measure downtime and verify
./scripts/monitor_downtime.sh
./scripts/verify_integrity.sh

# 9. Clean up
./scripts/cleanup.sh
```

---

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `FALCON_BIN` | `target/release/falcon_server` | Path to the FalconDB program |
| `MARKER_COUNT` | `50000` | Number of commit markers to write |
| `LOAD_RAMP_SEC` | `5` | Seconds of active writing before crash |

Example with more markers and longer ramp:

```bash
MARKER_COUNT=200000 LOAD_RAMP_SEC=15 ./run_demo.sh
```

---

## Output Files

| File | What It Contains |
|------|-----------------|
| `verification_report.txt` | PASS/FAIL verdict with counts |
| `verification_evidence.json` | Machine-readable results |
| `failover_timeline.json` | Downtime measurement and event timestamps |
| `committed_markers.log` | Every marker the server confirmed (with timestamp) |
| `surviving_markers.txt` | Every marker found after the crash |
| `missing_markers.txt` | Markers that were lost (should be empty) |
| `phantom_markers.txt` | Markers that appeared without confirmation (should be empty) |
| `load_metrics.json` | Writer statistics (commits, failures, reconnects) |
| `kill_timestamp.txt` | Exact time the primary was killed |
| `promote_timestamp.txt` | Exact time the backup became primary |

---

## How This Differs from PoC #1

| | PoC #1 (DCG) | PoC #3 (Failover Under Load) |
|---|---|---|
| **When is the crash?** | After all writes finish | During active writes |
| **Is the writer running?** | No — writes are done | Yes — writing at full speed |
| **Does the writer reconnect?** | No | Yes — to the new primary |
| **What does it prove?** | Committed data is durable | Committed data is durable *and* the system recovers |
| **Downtime measured?** | No | Yes |

PoC #1 proves safety at rest. PoC #3 proves safety under fire.

---

## What This Demo Proves

- FalconDB does not lose acknowledged commits during a crash
- The system recovers automatically (writer reconnects)
- Downtime is short and measurable
- No phantom commits appear (data consistency is maintained)
- No duplicates are created during the failover

## What This Demo Does NOT Prove

- **Performance under failover** — This is not a speed test
- **Multi-region failover** — Both servers run on the same machine
- **Network partitions** — We test a server crash, not a network split
- **Multiple concurrent writers** — One writer is used for determinism

---

## Anti-Cheating: How We Keep This Honest

- **WAL is enabled** — Every write goes through the write-ahead log
- **fsync is on** — Data is flushed to disk before confirming
- **Replication is active** — The backup receives data in real time
- **No batch commits** — Each marker is its own transaction
- **No retry masking** — Failed markers are recorded, not silently retried
- **No post-hoc cleanup** — The verification checks raw data, no cleanup runs first
- **Hard crash only** — `kill -9`, not a graceful stop
- **Crash during writes** — Not after writes finish

---

## For More Detail

See [docs/explanation_for_customers.md](docs/explanation_for_customers.md) for:
- Why failover under load is the hardest test
- What most databases get wrong
- What FalconDB guarantees
- Why this demo is trustworthy

---

## Directory Structure

```
falcondb-poc-failover-under-load/
├── README.md                           ← You are here
├── run_demo.sh / .ps1                  ← One-command demo
├── configs/
│   ├── primary.toml                    ← Primary server config
│   └── replica.toml                    ← Backup server config
├── schema/
│   └── tx_markers.sql                  ← Commit marker table
├── workload/
│   ├── Cargo.toml                      ← Rust workload generator
│   └── src/main.rs                     ← Writer with failover reconnect
├── scripts/
│   ├── start_cluster.sh / .ps1         ← Start both servers
│   ├── wait_cluster_ready.sh / .ps1    ← Verify replication
│   ├── start_load.sh / .ps1            ← Start background writer
│   ├── kill_primary.sh / .ps1          ← Crash during writes
│   ├── promote_replica.sh / .ps1       ← Promote backup
│   ├── monitor_downtime.sh / .ps1      ← Measure failover window
│   ├── verify_integrity.sh / .ps1      ← PASS/FAIL verification
│   └── cleanup.sh / .ps1              ← Stop and clean
├── output/                             ← Results appear here
└── docs/
    └── explanation_for_customers.md
```
