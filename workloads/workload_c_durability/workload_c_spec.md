# Workload C: Durability & Crash Recovery

## Goal

Validate CONSISTENCY.md commit point invariants under crash. Proves that
FalconDB does not silently lose committed data after `kill -9`.

## Invariants Tested

| Invariant | Statement |
|-----------|-----------|
| **CP-4** | Crash after DurableCommit MUST NOT lose the transaction |
| **FC-1** | Crash before CP-D → txn rolled back |
| **FC-2** | Crash after CP-D → txn survives recovery |
| **FC-4** | WAL replay is idempotent (no double commit) |

## Scenario

1. Start FalconDB with WAL + fdatasync
2. Write N transactions, each individually committed
3. Client logs every confirmed `seq_id` to a local file
4. After K commits, `kill -9` the server (no graceful shutdown)
5. Restart server (WAL recovery runs automatically)
6. Compare client log vs database — any discrepancy = **FAIL**

Runs multiple rounds (default 3) to catch intermittent issues.

## Checks

| Check | Pass Condition |
|-------|---------------|
| **No lost commits** | Every client-confirmed seq_id exists in DB |
| **No double commits** | Zero duplicate PKs |
| **No dirty reads** | No corrupt/empty payloads |
| **No phantom rows** | Informational — rows in DB but not in client log (uncommitted at crash, recovered) |

## Parameters

| Param | Default | Description |
|-------|---------|-------------|
| `--rounds` / `-Rounds` | 3 | Number of crash/recovery cycles |
| `--count` / `-WriteCount` | 5000 | Writes per round |
| `--kill-after` / `-WriteBeforeKill` | 3000 | Kill after this many commits |
| `--falcon-bin` / `-FalconBin` | `target/release/falcon.exe` | Server binary path |

## Output

`results/recovery_report_<timestamp>.json` — machine-readable, CI-ingestible.

## Usage

```bash
# Linux / macOS
cargo build --release
./crash_runner.sh --rounds 3 --count 5000 --kill-after 3000
```

```powershell
# Windows
cargo build --release
.\crash_runner.ps1
.\crash_runner.ps1 -FalconBin .\target\release\falcon.exe -Rounds 3 -WriteCount 5000 -WriteBeforeKill 3000
```
