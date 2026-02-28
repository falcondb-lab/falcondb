//! RPO = 0 integration tests for SemiSync / Sync WAL replication.
//!
//! These tests verify that after a primary "crash" (simulated by stopping writes),
//! a replica that has acknowledged a WAL record contains the committed data —
//! i.e. no committed writes are lost.
//!
//! ## Scenarios
//!
//! 1. **SemiSync basic** — primary blocks on `ship_wal_record_sync(SemiSync)` until
//!    the replica's background thread applies and acks; data visible on replica.
//!
//! 2. **SemiSync timeout** — if the replica is frozen, the call returns `Err` after
//!    the timeout, correctly signalling the quorum was not reached.
//!
//! 3. **Async vs SemiSync lag** — under Async the replica can lag; under SemiSync
//!    the replica is guaranteed to have the record before the primary returns.
//!
//! 4. **Sync mode (all replicas)** — all replicas must ack before primary returns.
//!
//! 5. **SyncWalAck monotone progress** — an ack at LSN k also satisfies waits for LSN < k.
//!
//! 6. **Primary crash simulation** — primary writes with SemiSync; replica applies;
//!    we simulate a crash by dropping the primary and reading the replica.
//!    The committed row must be present.

use std::sync::Arc;
use std::time::Duration;

use falcon_cluster::replication::{
    ReplicationLog, ShardReplicaGroup, SyncMode, SyncWalAck, WalAckTimeout,
};
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::{ColumnId, DataType, ShardId, TableId, Timestamp, TxnId};
use falcon_storage::engine::StorageEngine;
use falcon_storage::wal::WalRecord;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn simple_schema() -> TableSchema {
    TableSchema {
        id: TableId(1),
        name: "t".into(),
        columns: vec![ColumnDef {
            id: ColumnId(0),
            name: "id".into(),
            data_type: DataType::Int32,
            nullable: false,
            is_primary_key: true,
            default_value: None,
            is_serial: false,
        }],
        primary_key_columns: vec![0],
        ..Default::default()
    }
}

/// Returns an Insert-only WAL record (row is NOT yet visible — uncommitted).
/// Use for lag / timeout tests where visibility doesn't matter.
fn insert_record(id: i32) -> WalRecord {
    WalRecord::Insert {
        txn_id: TxnId(id as u64 + 1000),
        table_id: TableId(1),
        row: OwnedRow::new(vec![Datum::Int32(id)]),
    }
}

/// Ship [Insert, CommitTxn] through `group.ship_wal_record_sync(SemiSync)` so
/// the row is visible after the replica replays both records.
fn ship_sync_committed(
    group: &ShardReplicaGroup,
    id: i32,
    timeout: Duration,
) -> Result<u64, falcon_common::error::FalconError> {
    let txn_id = TxnId(id as u64 + 1000);
    let commit_ts = Timestamp(id as u64 + 2000);
    // Ship Insert — blocks until 1 replica acks.
    let lsn = group.ship_wal_record_sync(
        WalRecord::Insert {
            txn_id,
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(id)]),
        },
        SyncMode::SemiSync,
        timeout,
    )?;
    // Ship CommitTxn — also blocks until 1 replica acks.
    group.ship_wal_record_sync(
        WalRecord::CommitTxn { txn_id, commit_ts },
        SyncMode::SemiSync,
        timeout,
    )?;
    Ok(lsn)
}

fn row_exists(engine: &StorageEngine, id: i32) -> bool {
    let rows = engine
        .scan(TableId(1), TxnId(u64::MAX), Timestamp(u64::MAX))
        .unwrap_or_default();
    rows.iter()
        .any(|(_pk, r)| r.values.first() == Some(&Datum::Int32(id)))
}

// ---------------------------------------------------------------------------
// 1. SemiSync basic: primary blocks until replica acks
// ---------------------------------------------------------------------------

#[test]
fn test_semisync_blocks_until_replica_acks() {
    let group = ShardReplicaGroup::new(ShardId(0), &[simple_schema()]).unwrap();

    // Extract Arc handles the replica thread needs — no Mutex on the whole group.
    let log = group.log.clone();

    let log_for_thread = log.clone();
    let replica_storage_t = group.replicas[0].storage.clone();
    let replica_thread = std::thread::spawn(move || {
        // Continuously poll the log, apply each record, and ack immediately.
        let mut applied_lsn = 0u64;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            let pending = log_for_thread.read_from(applied_lsn);
            if pending.is_empty() {
                std::thread::sleep(Duration::from_millis(2));
                continue;
            }
            let mut write_sets = std::collections::HashMap::new();
            for rec in &pending {
                falcon_cluster::replication::apply_wal_record_to_engine(
                    &replica_storage_t,
                    &rec.record,
                    &mut write_sets,
                )
                .unwrap();
                applied_lsn = applied_lsn.max(rec.lsn);
                log_for_thread.sync_ack.record_ack(0, applied_lsn);
            }
        }
    });

    // Primary ships Insert+CommitTxn with SemiSync — each blocks until replica acks.
    let lsn = ship_sync_committed(&group, 42, Duration::from_secs(5))
        .expect("SemiSync should succeed once replica acks");

    replica_thread.join().unwrap();

    assert!(lsn > 0, "LSN must be positive");

    // Verify replica has the committed row.
    assert!(
        row_exists(&group.replicas[0].storage, 42),
        "replica must have row id=42 after SemiSync commit"
    );
}

// ---------------------------------------------------------------------------
// 2. SemiSync timeout: replica never acks → Err returned
// ---------------------------------------------------------------------------

#[test]
fn test_semisync_timeout_when_replica_frozen() {
    let group = ShardReplicaGroup::new(ShardId(0), &[simple_schema()]).unwrap();

    // No replica thread — nobody will ack.
    let result = group.ship_wal_record_sync(
        insert_record(99), // Insert only; commit doesn't matter for timeout test
        SyncMode::SemiSync,
        Duration::from_millis(100),
    );

    assert!(
        result.is_err(),
        "SemiSync must return Err when replica never acks within timeout"
    );
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("WalAckTimeout"),
        "error message should contain 'WalAckTimeout', got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// 3. Async vs SemiSync lag comparison
// ---------------------------------------------------------------------------

#[test]
fn test_async_allows_replica_to_lag_semisync_does_not() {
    let group = ShardReplicaGroup::new(ShardId(0), &[simple_schema()]).unwrap();

    // Async: returns immediately; replica has NOT applied yet.
    let lsn_async = group.ship_wal_record(insert_record(1));
    assert!(lsn_async > 0);

    // Replica applied_lsn is still 0 — lag is non-zero.
    let lag_async = group.replication_lag();
    assert!(
        lag_async[0].1 > 0,
        "Async: replica should lag behind primary"
    );

    // SemiSync with no running replica thread must time out immediately.
    let result = group.ship_wal_record_sync(
        insert_record(2),
        SyncMode::SemiSync,
        Duration::from_millis(50),
    );
    assert!(result.is_err(), "SemiSync with frozen replica must time out");
}

// ---------------------------------------------------------------------------
// 4. Sync mode: all replicas must ack
// ---------------------------------------------------------------------------

#[test]
fn test_sync_mode_requires_all_replica_acks() {
    // Single replica group.
    let group = ShardReplicaGroup::new(ShardId(0), &[simple_schema()]).unwrap();
    assert_eq!(group.replicas.len(), 1);

    // Ack from replica 0 immediately (simulates instant apply).
    let sync_ack = group.log.sync_ack.clone();

    // Spawn thread that acks after a short delay.
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        sync_ack.record_ack(0, 999); // ack ahead of any LSN we'll issue
    });

    let result = group.ship_wal_record_sync(
        insert_record(7), // visibility not checked; just testing ack gate
        SyncMode::Sync,
        Duration::from_secs(2),
    );
    assert!(result.is_ok(), "Sync should succeed: {result:?}");
}

// ---------------------------------------------------------------------------
// 5. SyncWalAck monotone: ack at k satisfies waits for lsn ≤ k
// ---------------------------------------------------------------------------

#[test]
fn test_sync_wal_ack_monotone_progress() {
    let ack = SyncWalAck::new();

    // Ack at LSN 10.
    ack.record_ack(0, 10);

    // Wait for LSN 5 (already satisfied).
    let r = ack.wait_for_acks(5, 1, Duration::from_millis(10));
    assert!(r.is_ok(), "LSN 5 should be satisfied by ack at 10");

    // Wait for LSN 10 (exactly satisfied).
    let r = ack.wait_for_acks(10, 1, Duration::from_millis(10));
    assert!(r.is_ok(), "LSN 10 should be satisfied by ack at 10");

    // Wait for LSN 11 (not yet satisfied).
    let r = ack.wait_for_acks(11, 1, Duration::from_millis(50));
    assert!(r.is_err(), "LSN 11 should time out");
}

// ---------------------------------------------------------------------------
// 6. Primary crash simulation — committed data survives on replica
// ---------------------------------------------------------------------------

#[test]
fn test_primary_crash_rpo_zero_data_survives_on_replica() {
    let group = ShardReplicaGroup::new(ShardId(0), &[simple_schema()]).unwrap();

    // Extract Arcs before the group is moved/consumed by calls.
    let replica_storage = group.replicas[0].storage.clone();
    let log = group.log.clone();

    // The replica thread polls the log, applies each record, acks immediately.
    // Two rows × (Insert + CommitTxn) = 4 WAL records total.
    let log_t = log.clone();
    let replica_storage_t = replica_storage.clone();
    let replica_thread = std::thread::spawn(move || {
        let mut applied_lsn = 0u64;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            let pending = log_t.read_from(applied_lsn);
            if pending.is_empty() {
                std::thread::sleep(Duration::from_millis(2));
                continue;
            }
            let mut write_sets = std::collections::HashMap::new();
            for rec in &pending {
                falcon_cluster::replication::apply_wal_record_to_engine(
                    &replica_storage_t,
                    &rec.record,
                    &mut write_sets,
                )
                .unwrap();
                applied_lsn = applied_lsn.max(rec.lsn);
                log_t.sync_ack.record_ack(0, applied_lsn);
            }
        }
    });

    // Primary commits two rows with SemiSync (Insert + CommitTxn each, RPO = 0).
    ship_sync_committed(&group, 100, Duration::from_secs(5)).expect("SemiSync commit row 100");
    ship_sync_committed(&group, 101, Duration::from_secs(5)).expect("SemiSync commit row 101");

    replica_thread.join().unwrap();

    // Simulate primary crash: drop `group` (primary storage is gone).
    drop(group);

    // Data must be on the replica — RPO = 0.
    assert!(
        row_exists(&replica_storage, 100),
        "row 100 must survive primary crash on replica"
    );
    assert!(
        row_exists(&replica_storage, 101),
        "row 101 must survive primary crash on replica"
    );
}

// ---------------------------------------------------------------------------
// 7. SyncMode::required_acks edge cases
// ---------------------------------------------------------------------------

#[test]
fn test_sync_mode_required_acks() {
    assert_eq!(SyncMode::Async.required_acks(3), 0);
    assert_eq!(SyncMode::SemiSync.required_acks(3), 1);
    assert_eq!(SyncMode::SemiSync.required_acks(0), 0); // no replicas → 0
    assert_eq!(SyncMode::Sync.required_acks(3), 3);
    assert_eq!(SyncMode::Sync.required_acks(0), 0);
}

// ---------------------------------------------------------------------------
// 8. ReplicationLog::append_and_wait with required_acks=0 returns immediately
// ---------------------------------------------------------------------------

#[test]
fn test_append_and_wait_async_returns_immediately() {
    let log = ReplicationLog::new();
    // required_acks=0 == Async mode: no wait at all.
    let lsn = log
        .append_and_wait(insert_record(5), 0, Duration::from_millis(1))
        .expect("Async append_and_wait should never time out");
    assert!(lsn > 0);
}

// ---------------------------------------------------------------------------
// 9. Multiple concurrent replicas — SemiSync satisfied by first ack
// ---------------------------------------------------------------------------

#[test]
fn test_semisync_satisfied_by_first_of_two_replicas() {
    let ack = Arc::new(SyncWalAck::new());
    let ack2 = ack.clone();

    // Replica 0 acks after 20 ms; replica 1 never acks.
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        ack2.record_ack(0, 1);
    });

    // Wait for 1 ack with a generous timeout.
    let result = ack.wait_for_acks(1, 1, Duration::from_secs(2));
    assert!(result.is_ok(), "SemiSync should be satisfied by replica 0");
    assert_eq!(result.unwrap(), 1, "exactly one replica acked");
}

// ---------------------------------------------------------------------------
// 10. WalAckTimeout Display
// ---------------------------------------------------------------------------

#[test]
fn test_wal_ack_timeout_display() {
    let err = WalAckTimeout {
        target_lsn: 42,
        required: 2,
        got: 1,
    };
    let s = err.to_string();
    assert!(s.contains("42"));
    assert!(s.contains("2"));
    assert!(s.contains("1"));
}
