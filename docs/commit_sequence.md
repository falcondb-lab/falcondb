# FalconDB — Commit / ACK Sequence

> **Purpose**: Show exactly when FalconDB considers a transaction "committed" and why
> "success never gets taken back." Usable in investor decks, PoC presentations, and
> engineering reviews.

---

## The One-Sentence Explanation

> FalconDB waits until data is **physically written to disk** (WAL fsync) before telling
> your application "success." Other databases may acknowledge sooner and hope for the best.

---

## 1. Commit Phases (Single-Shard Fast Path)

This is the most common path — ~95% of OLTP transactions in a well-sharded deployment.

```
  Client              FalconDB              WAL (disk)           Replica
    │                    │                     │                    │
    │── INSERT/UPDATE ──▶│                     │                    │
    │                    │                     │                    │
    │                    │── OCC validate ────▶│                    │
    │                    │   (read-set check)  │                    │
    │                    │                     │                    │
    │                    │── CP-L: WAL record ─▶│                   │
    │                    │   (in memory buffer) │                   │
    │                    │                     │                    │
    │                    │── CP-D: fsync() ────▶│                   │
    │                    │   ┌─────────────────┐│                   │
    │                    │   │ DATA IS NOW ON  ││                   │
    │                    │   │ DISK. CRASH-SAFE││                   │
    │                    │   └─────────────────┘│                   │
    │                    │                     │                    │
    │                    │── CP-V: mark visible │                   │
    │                    │   (MVCC readers can  │                   │
    │                    │    see it now)       │                   │
    │                    │                     │                    │
    │◀── CP-A: "OK" ────│                     │                    │
    │    SUCCESS ACK     │                     │                    │
    │                    │                     │                    │
    │                    │── WAL ship (async) ─────────────────────▶│
    │                    │   (replication)      │                   │
    │                    │                     │                    │
```

### Phase Legend

| Phase | Code Name | What Happens | Can Crash Lose Data? |
|-------|-----------|--------------|---------------------|
| **CP-L** | `WalLogged` | WAL record written to memory buffer | ⚠️ Yes — not yet on disk |
| **CP-D** | `WalDurable` | `fsync()` completes — data is on physical storage | **No** — survives any crash |
| **CP-V** | `Visible` | Transaction marked committed in MVCC | **No** — already durable |
| **CP-A** | `Acknowledged` | Client receives success response | **No** — already durable |

**The critical insight**: The client ACK (CP-A) happens **after** CP-D (fsync). This is the
Deterministic Commit Guarantee. If the process crashes between CP-D and CP-A, the client
gets a connection error — but the data is safe on disk and will be recovered.

---

## 2. Cross-Shard (2PC) Path

For transactions touching multiple shards, a coordinator ensures atomicity.

```
  Client          Coordinator         Shard A            Shard B          Decision Log
    │                 │                  │                  │                  │
    │── BEGIN ───────▶│                  │                  │                  │
    │── UPDATE A ────▶│── PREPARE ──────▶│                  │                  │
    │── UPDATE B ────▶│── PREPARE ──────────────────────────▶                  │
    │── COMMIT ──────▶│                  │                  │                  │
    │                 │◀─ PREPARED OK ───│                  │                  │
    │                 │◀─ PREPARED OK ──────────────────────│                  │
    │                 │                  │                  │                  │
    │                 │── LOG COMMIT DECISION ──────────────────────────────────▶
    │                 │   ┌──────────────────────────────────────────────────┐  │
    │                 │   │ DURABLE COMMIT POINT: even if coordinator       │  │
    │                 │   │ crashes now, recovery will complete the commit. │  │
    │                 │   └──────────────────────────────────────────────────┘  │
    │                 │                  │                  │                  │
    │                 │── COMMIT ────────▶                  │                  │
    │                 │── COMMIT ────────────────────────────▶                  │
    │                 │                  │                  │                  │
    │◀── "OK" ───────│                  │                  │                  │
    │                 │                  │                  │                  │
```

**Key property**: The commit decision is logged to the `CoordinatorDecisionLog` **before**
sending COMMIT to any participant. If the coordinator crashes:
- **After decision log write**: Recovery reads the log and completes the commit on all shards.
- **Before decision log write**: Recovery aborts all participants (no partial commits).

---

## 3. How FalconDB Differs from Typical Databases

```
                    Traditional DB                    FalconDB
                    ──────────────                    ────────
                    
  WAL write ────────── ✓                              ✓
  (memory buffer)      │                              │
                       │                              │
  Client ACK ──────── ✓ ◀── HERE!                    │  (not yet!)
                       │     Some DBs ACK here        │
                       │     before fsync.             │
                       │                              │
  fsync ───────────── ✓                          ✓ ◀── fsync FIRST
                       │                              │
                                                 ✓ ◀── THEN client ACK
                                                      
  ❌ If crash happens       ✅ If crash happens
     between ACK and           after ACK, data
     fsync, client thinks      is ALWAYS on disk.
     data is committed but
     it may be lost.
```

### The Gap That FalconDB Eliminates

In databases with `synchronous_commit = off` (PostgreSQL) or equivalent async flush modes:

1. Client receives "COMMIT OK"
2. Process crashes before WAL fsync
3. Data is **lost** — but the client already told the user "success"
4. This is a **phantom commit**: the application believes data exists, but it doesn't

FalconDB's DCG eliminates this gap by construction: no ACK is sent until fsync completes.

---

## 4. Crash Scenarios — What Happens?

| Crash Point | Client Saw | Data Status | Recovery Result |
|-------------|-----------|-------------|-----------------|
| Before CP-L (no WAL write) | Nothing | Not written | Transaction doesn't exist — correct |
| Between CP-L and CP-D | Nothing | In memory only | Transaction rolled back — correct |
| Between CP-D and CP-A | Connection error | **On disk** | Transaction recovered — client retries and finds data |
| After CP-A | "COMMIT OK" | **On disk** | Transaction recovered — everything consistent |

**The only ambiguous case**: crash between CP-D and CP-A. The client gets a connection error
and doesn't know if the commit succeeded. FalconDB classifies this as `Indeterminate`
(PG SQLSTATE `08006`). The client should reconnect and check — the data **will** be there.

---

## 5. Verification

Every claim in this document is backed by a runnable test:

| Claim | Test | Command |
|-------|------|---------|
| CP-D happens before CP-A | `test_consistency_validation_helpers` | `cargo test -p falcon_cluster --test consistency_replication test_consistency_validation` |
| Crash after fsync = data survives | `test_wal4_wal5_replay_convergence_and_completeness` | `cargo test -p falcon_storage --test consistency_wal test_wal4_wal5` |
| Crash before commit = rollback | `test_crash_after_wal_write_before_commit` | `cargo test -p falcon_storage --test consistency_wal test_crash_after` |
| No phantom commits on replica | `test_rep2_no_phantom_commits` | `cargo test -p falcon_cluster --test consistency_replication test_rep2` |
| 2PC all-or-nothing | `test_xs1_cross_shard_atomicity_commit/abort` | `cargo test -p falcon_cluster --test consistency_replication test_xs1` |

Full consistency verification (~60 seconds):
```bash
cargo test -p falcon_storage --test consistency_wal
cargo test -p falcon_cluster --test consistency_replication
```

---

## Appendix: Code References

| Component | File | Key Function |
|-----------|------|-------------|
| Commit point model | `crates/falcon_common/src/consistency.rs` §1 | `CommitPoint` enum |
| Commit policy | `crates/falcon_common/src/consistency.rs` §2 | `CommitPolicy` enum |
| Phase tracker | `crates/falcon_cluster/src/stability_hardening.rs` §2 | `CommitPhaseTracker` |
| Fast-path commit | `crates/falcon_txn/src/manager.rs` | `TxnManager::commit()` |
| Group commit (fsync) | `crates/falcon_storage/src/group_commit.rs` | `GroupCommitSyncer` |
| 2PC decision log | `crates/falcon_cluster/src/deterministic_2pc.rs` | `CoordinatorDecisionLog` |
| Crash point model | `crates/falcon_common/src/consistency.rs` §5 | `CrashPoint` enum |
