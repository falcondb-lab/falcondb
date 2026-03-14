# FalconDB — Deterministic Commit Guarantee (DCG) Proof

> **This demo proves that FalconDB never loses a committed transaction,
> even if the primary server crashes.**

When your application sends a "save" command to FalconDB and gets back "OK",
that data is safe. Even if the server loses power one millisecond later. Even
if the backup server has to take over. Your data is there.

This is called the **Deterministic Commit Guarantee (DCG)**.

---

## What Happens in This Demo

1. We start **two FalconDB servers** — a primary and a backup
2. We write **1,000 orders** to the primary (like a payment system would)
3. We record every order that the server confirmed as saved
4. We **crash the primary server** (hard kill — simulating a power failure)
5. The backup server takes over
6. We check: **is every confirmed order still there?**

The answer should be **yes, every single one**.

---

## Run It (One Command)

### What You Need

| Tool | What It Does | How to Get It |
|------|-------------|---------------|
| **FalconDB** | The database being tested | `cargo build -p falcon_server --release` |
| **psql** | Talks to the database | Comes with PostgreSQL ([download](https://www.postgresql.org/download/)) |

### Linux / macOS

```bash
# Build FalconDB (first time only)
cargo build -p falcon_server --release

# Run the demo
./falcondb-poc-dcg/run_demo.sh
```

### Windows (PowerShell 7+)

```powershell
# Build FalconDB (first time only)
cargo build -p falcon_server --release

# Run the demo
.\falcondb-poc-dcg\run_demo.ps1
```

### What You Should See

```
  === Step 2/6: Writing 1000 orders to primary ===
  + Workload complete: 1000 orders committed

  === Step 4/6: KILLING the primary (hard crash) ===
  >>> kill -9 12345 <<<
  + Primary killed at 2026-02-27T04:00:05Z

  === Step 6/6: VERIFICATION ===
  ============================================================
    FalconDB DCG PoC — Verification Report
  ============================================================

    Committed orders (acknowledged): 1000
    Orders after failover:           1000
    Missing (data loss):             0
    Phantom (unexpected):            0

    Result: PASS
    No committed data was lost.
  ============================================================
```

---

## Run It Step by Step

If you prefer to run each step manually:

```bash
# 1. Start the two-server cluster
./scripts/start_cluster.sh

# 2. Write orders to the primary
./scripts/run_workload.sh

# 3. Wait a few seconds for the backup to sync
sleep 5

# 4. Crash the primary (hard kill)
./scripts/kill_primary.sh

# 5. Promote the backup to primary
./scripts/promote_replica.sh

# 6. Verify: are all confirmed orders still there?
./scripts/verify_results.sh

# 7. Clean up
./scripts/cleanup.sh
```

---

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `FALCON_BIN` | `target/release/falcon_server` | Path to the FalconDB program |
| `ORDER_COUNT` | `1000` | Number of orders to write |
| `REPL_WAIT_SEC` | `5` | Seconds to wait for backup sync before crash |

Example with more orders:

```bash
ORDER_COUNT=10000 ./run_demo.sh
```

---

## Output Files

After running, check the `output/` directory:

| File | What It Contains |
|------|-----------------|
| `verification_report.txt` | Human-readable PASS/FAIL verdict |
| `verification_evidence.json` | Machine-readable results |
| `committed_orders.log` | Every order the server confirmed as saved |
| `surviving_orders.txt` | Every order found after the crash |
| `missing_orders.txt` | Orders that were lost (should be empty) |
| `phantom_orders.txt` | Orders that appeared without confirmation (should be empty) |
| `workload_summary.json` | Workload statistics |
| `primary.log` | Primary server log |
| `replica.log` | Backup server log |

---

## What This Demo Proves

**Claim**: If the server says "saved", the data is safe — even through a crash.

**How we test it**:
- We do NOT give the server a chance to shut down gracefully
- We use `kill -9` (the most violent way to stop a process)
- We verify every single confirmed order, one by one

**What PASS means**: Zero data loss. Every order that the server confirmed as
committed was found on the backup server after the crash.

## What This Demo Does NOT Prove

- **Performance** — This is not a speed test. See `benchmarks/` for that.
- **Multi-region failover** — Both servers run on the same machine.
- **Network failures** — We test a server crash, not a network problem.
- **Graceful failover** — We deliberately use a hard crash. Graceful failover
  is easier and covered elsewhere.

---

## Anti-Cheating: How We Keep This Honest

This demo does NOT take any shortcuts:

- **WAL is enabled** — Every write goes through the write-ahead log
- **fsync is on** — Data is flushed to disk before confirming
- **Replication is active** — The backup receives data in real time
- **No batch commits** — Each order is its own transaction
- **No buffered acknowledgements** — The client logs an order ONLY after the
  server confirms it
- **Hard crash only** — `kill -9`, not a graceful stop

The configuration files are in `configs/` — inspect them yourself.

---

## For More Detail

See [docs/explanation_for_customers.md](docs/explanation_for_customers.md) for
a plain-language explanation of:
- What "commit" means
- Why databases can lose data during crashes
- What FalconDB does differently
- Why this demo is trustworthy

---

## Directory Structure

```
falcondb-poc-dcg/
├── README.md                    ← You are here
├── run_demo.sh / .ps1           ← One-command demo
├── configs/
│   ├── primary.toml             ← Primary server config
│   └── replica.toml             ← Backup server config
├── schema/
│   └── orders.sql               ← Table definition
├── workload/
│   ├── Cargo.toml               ← Rust workload generator
│   ├── src/main.rs              ← (Rust) order writer
│   ├── order_writer.py          ← (Python fallback)
│   └── requirements.txt
├── scripts/
│   ├── start_cluster.sh / .ps1  ← Start both servers
│   ├── run_workload.sh / .ps1   ← Write orders
│   ├── kill_primary.sh / .ps1   ← Crash the primary
│   ├── promote_replica.sh / .ps1← Promote backup
│   ├── verify_results.sh / .ps1 ← Check results
│   └── cleanup.sh / .ps1        ← Stop and clean
├── output/                      ← Results appear here
└── docs/
    └── explanation_for_customers.md
```

---

## Reproduce on Any Machine

1. Copy this directory to the target machine
2. Build FalconDB: `cargo build -p falcon_server --release`
3. Install `psql` (PostgreSQL client tools)
4. Run: `./run_demo.sh` (Linux/macOS) or `.\run_demo.ps1` (Windows)
5. Read: `output/verification_report.txt`

No FalconDB license, account, or internet connection required.
