# FalconDB — Backup, Restore & Point-in-Time Recovery

> **"If someone deletes data by mistake, or a machine is completely lost,
> can I reliably get my data back?"**

**Yes. This demo proves it.**

This PoC runs a complete disaster recovery cycle: generate business data,
take a backup, keep writing, record an exact recovery point, **destroy
the database entirely**, then restore it to the exact second — and verify
every account balance matches.

---

## What This Demo Does

1. Start FalconDB with a simple accounts table (100 accounts)
2. Generate 500 transactions (money transfers between accounts)
3. **Take a full backup** — this is your safety net
4. Generate 500 more transactions
5. **Record the exact recovery target** (T1) — snapshot every balance
6. Generate 200 more transactions (these will be intentionally lost)
7. **Destroy the database** — delete the entire data directory
8. **Restore from backup** — copy the snapshot back
9. **Replay the transaction log** — advance to time T1
10. **Verify** — compare every account balance against the T1 snapshot

The demo ends with **PASS** or **FAIL**. PASS means every account has
exactly the balance it had at the recovery target — not one cent more,
not one cent less.

---

## Run It

### Prerequisites

| Tool | Purpose |
|------|---------|
| **FalconDB** | `cargo build -p falcon_server --release` |
| **psql** | PostgreSQL client (for SQL commands) |
| **Python 3** | Verification script |

### Step-by-Step

```bash
# 1. Start FalconDB + create schema
./scripts/start_cluster.sh

# 2. Generate initial business data (500 transactions)
./scripts/generate_data.sh --count 500 --phase pre_backup

# 3. Take a full backup (T0)
./scripts/take_backup.sh

# 4. Generate more data (500 transactions after backup)
./scripts/generate_data.sh --count 500 --phase post_backup

# 5. Record the recovery target (T1)
./scripts/record_timestamp.sh

# 6. Generate data AFTER T1 (this data will be lost — intentionally)
./scripts/generate_data.sh --count 200 --phase post_t1

# 7. DESTROY the database
./scripts/induce_disaster.sh

# 8. Restore from backup (back to T0)
./scripts/restore_from_backup.sh

# 9. Replay transaction log to T1
./scripts/replay_wal_until.sh

# 10. Verify recovery
./scripts/verify_restored_data.sh
```

Windows users: use the `.ps1` variants of each script.

---

## What Happens at Each Step

### Before the Disaster

```
T0 (backup)          T1 (recovery target)     T2 (more writes)
  │                        │                        │
  ▼                        ▼                        ▼
──┬────────────────────────┬────────────────────────┬──
  │  500 transactions      │  500 transactions      │  200 transactions
  │  (in backup)           │  (in transaction log)  │  (will be lost)
```

### After the Disaster

```
Database: DESTROYED (data directory deleted)
Backup:   INTACT (separate directory)
Log:      INTACT (WAL archive)
```

### After Recovery

```
Restored state = backup + transaction log replay up to T1

  Transactions before T1:   1,000  ← all applied ✓
  Transactions after T1:      200  ← NOT applied ✓
  Per-account balances:     exact match ✓

  Result: PASS
```

---

## Why This Matters

### Nightly Backup vs. Point-in-Time Recovery

| Scenario | Nightly Backup | Point-in-Time Recovery |
|----------|---------------|----------------------|
| Delete at 2:15 PM, discover at 4 PM | Lose 14+ hours | Lose nothing |
| Bug corrupts data for 2 hours | Lose everything since midnight | Restore to pre-bug second |
| Disk fails at 3 AM Saturday | Restore to Friday midnight | Restore to 2:59 AM |

**The difference is hours of lost data vs. zero lost data.**

### What "Point-in-Time" Actually Means

Think of it like this:
- A **backup** is a photograph of your database
- The **transaction log** is a video of everything that happened after
- **Recovery** is rewinding the video to exactly the right moment

You pick the exact second. The database ends up in that exact state.
Nothing after that second is applied.

---

## How FalconDB Does This

FalconDB has built-in infrastructure for PITR:

- **`BackupManager`** — creates consistent snapshots with checksums,
  tracks backup history, records WAL positions
- **`WalArchiver`** — archives completed WAL segments, maintains segment
  inventory, supports retention policies
- **`RecoveryExecutor`** — replays WAL records sequentially, supports
  `RecoveryTarget::Time` for timestamp-based recovery, tracks replay
  progress (segments/records replayed, current position)

These are not external tools bolted on — they are part of FalconDB's
storage engine (`falcon_storage`).

---

## Verification

The verification step compares **every account balance** in the restored
database against the snapshot taken at T1:

```
Verification Result
===================
Accounts (expected): 100
Accounts (restored): 100

Per-account balance comparison:
  matches=100
  mismatches=0
  missing=0
  extra=0

Result: PASS
Database restored exactly to target time.
```

If even one account has a different balance, the result is **FAIL**.

---

## What This PoC Does NOT Claim

- ❌ "Recovery is instant" (it takes time proportional to the log size)
- ❌ "Zero data loss in all scenarios" (you lose data after T1 — by design)
- ❌ "Works without backups" (you must take backups)
- ❌ "Replaces a proper backup strategy" (this is one component)

## What This PoC DOES Prove

- ✅ Database can be completely destroyed and fully recovered
- ✅ Recovery reaches an exact point in time
- ✅ Every row matches the expected state at the target time
- ✅ Transactions after the target time are correctly excluded
- ✅ Recovery is deterministic and verifiable

---

## Anti-Cheating Rules

- WAL is enabled (no write-ahead log = no recovery)
- No WAL entries are skipped during replay
- No manual data fixes after restore
- No partial restore (full database or nothing)
- Restore works on a clean data directory (the old one is deleted)
- Disaster is real: `rm -rf` on the data directory

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FALCON_BIN` | `target/release/falcon_server` | Path to FalconDB binary |

All other settings are fixed for reproducibility.

---

## Output Files

| File | Content |
|------|---------|
| `output/backup_manifest.json` | Backup metadata (time, position, checksum) |
| `output/recovery_target.txt` | T1 timestamp (human + machine readable) |
| `output/state_at_t1.csv` | Per-account balances at T1 (expected state) |
| `output/txn_log_*.csv` | Transaction logs per phase |
| `output/restore_report.txt` | Restore procedure results |
| `output/wal_replay.log` | WAL replay details |
| `output/verification_result.txt` | Final PASS/FAIL with comparison |
| `output/wal_segments.log` | WAL segment inventory |

---

## For Compliance Teams

This demo provides evidence that FalconDB supports:
- **Consistent backups** with integrity checksums
- **Continuous transaction logging** (WAL)
- **Point-in-time recovery** to any second
- **Verifiable recovery** with per-row validation

Suitable for: SOX, GDPR, HIPAA, PCI-DSS audit requirements.

For the full explanation, see [docs/explanation_for_customers.md](docs/explanation_for_customers.md).

---

## Directory Structure

```
falcondb-poc-backup-pitr/
├── README.md                               ← You are here
├── schema/
│   └── accounts.sql                        ← Simple account table + seed data
├── scripts/
│   ├── start_cluster.sh / .ps1             ← Start FalconDB + apply schema
│   ├── generate_data.sh / .ps1             ← Generate deterministic transactions
│   ├── take_backup.sh / .ps1               ← Full backup + manifest
│   ├── record_timestamp.sh / .ps1          ← Capture recovery target T1
│   ├── induce_disaster.sh / .ps1           ← DESTROY the database
│   ├── restore_from_backup.sh / .ps1       ← Restore base snapshot
│   ├── replay_wal_until.sh / .ps1          ← WAL replay to T1
│   ├── verify_restored_data.sh / .ps1      ← Compare and PASS/FAIL
│   └── cleanup.sh / .ps1                   ← Stop everything
├── output/                                 ← Generated results
│   ├── backup_manifest.json
│   ├── recovery_target.txt
│   ├── verification_result.txt
│   └── ...
└── docs/
    └── explanation_for_customers.md        ← Why PITR matters (plain language)
```
