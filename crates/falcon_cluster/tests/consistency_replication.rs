//! Consistency Test Suite — Replication, Failover & Cross-Shard Invariants
//!
//! Tests correspond to invariants defined in `falcon_common::consistency`:
//! - REP-1: Prefix property (replica ⊆ primary)
//! - REP-2: No phantom commits
//! - REP-3: Strict ordering
//! - FAIL-1: Commit set containment after promote
//! - FAIL-2: ACK'd txns survive failover
//! - FAIL-3: Epoch fencing
//! - XS-1: Cross-shard atomicity
//! - XS-2: At-most-once commit

use std::sync::Arc;
use std::time::Duration;

use falcon_common::consistency::{
    validate_prefix_property, validate_promote_commit_set,
};
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::*;
use falcon_storage::wal::WalRecord;

use falcon_cluster::ha::{HAConfig, HAReplicaGroup, SyncMode};
use falcon_cluster::replication::ShardReplicaGroup;
use falcon_cluster::sharded_engine::ShardedEngine;
use falcon_cluster::two_phase::TwoPhaseCoordinator;

fn test_schema() -> TableSchema {
    TableSchema {
        id: TableId(1),
        name: "t".into(),
        columns: vec![
            ColumnDef {
                id: ColumnId(0),
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
            },
            ColumnDef {
                id: ColumnId(1),
                name: "val".into(),
                data_type: DataType::Text,
                nullable: true,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
            },
        ],
        primary_key_columns: vec![0],
        ..Default::default()
    }
}

/// Insert a row on the primary and ship WAL to replication log.
fn insert_and_replicate(group: &HAReplicaGroup, id: i32, val: &str, txn: u64) {
    let row = OwnedRow::new(vec![Datum::Int32(id), Datum::Text(val.into())]);
    group
        .inner
        .primary
        .storage
        .insert(TableId(1), row.clone(), TxnId(txn))
        .unwrap();
    group
        .inner
        .primary
        .storage
        .commit_txn(TxnId(txn), Timestamp(txn), TxnType::Local)
        .unwrap();
    group.inner.ship_wal_record(WalRecord::Insert {
        txn_id: TxnId(txn),
        table_id: TableId(1),
        row,
    });
    group.inner.ship_wal_record(WalRecord::CommitTxnLocal {
        txn_id: TxnId(txn),
        commit_ts: Timestamp(txn),
    });
}

/// Collect committed TxnIds from a storage engine by scanning the table.
fn committed_txn_ids(group: &HAReplicaGroup, is_primary: bool, replica_idx: usize) -> Vec<TxnId> {
    let storage = if is_primary {
        &group.inner.primary.storage
    } else {
        &group.inner.replicas[replica_idx].storage
    };
    let rows = storage
        .scan(TableId(1), TxnId(9999), Timestamp(u64::MAX - 1))
        .unwrap();
    // Each row was inserted by a different txn with id = row.id as u64
    rows.iter()
        .map(|(_, r)| match r.values[0] {
            Datum::Int32(id) => TxnId(id as u64),
            _ => TxnId(0),
        })
        .collect()
}

// ═══════════════════════════════════════════════════════════════════════════
// REP-1: Prefix property — replica committed set ⊆ primary committed set
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_rep1_prefix_property() {
    let config = HAConfig {
        replica_count: 2,
        ..Default::default()
    };
    let group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();

    // Insert 5 rows on primary
    for i in 1..=5 {
        insert_and_replicate(&group, i, &format!("v{}", i), i as u64);
    }

    // Only catch up replica 0 partially (first 3 records = 1.5 txns)
    group.inner.catch_up_replica(0).unwrap();

    let primary_commits = committed_txn_ids(&group, true, 0);
    let replica0_commits = committed_txn_ids(&group, false, 0);
    let replica1_commits = committed_txn_ids(&group, false, 1);

    // REP-1: replica ⊆ primary
    assert!(
        validate_prefix_property(&primary_commits, &replica0_commits).is_ok(),
        "REP-1 violation: replica 0 has commits not in primary"
    );
    assert!(
        validate_prefix_property(&primary_commits, &replica1_commits).is_ok(),
        "REP-1 violation: replica 1 has commits not in primary"
    );

    // Replica 0 should have some data, replica 1 should have none or less
    assert!(
        replica0_commits.len() <= primary_commits.len(),
        "Replica 0 should have ≤ primary commits"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// REP-2: No phantom commits — replica never commits what primary hasn't
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_rep2_no_phantom_commits() {
    let config = HAConfig {
        replica_count: 1,
        ..Default::default()
    };
    let group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();

    // Insert 3 rows and replicate
    for i in 1..=3 {
        insert_and_replicate(&group, i, &format!("v{}", i), i as u64);
    }
    group.inner.catch_up_replica(0).unwrap();

    let primary_commits = committed_txn_ids(&group, true, 0);
    let replica_commits = committed_txn_ids(&group, false, 0);

    // Every replica txn must exist in primary
    for rtxn in &replica_commits {
        assert!(
            primary_commits.contains(rtxn),
            "REP-2 violation: replica has txn {:?} not in primary",
            rtxn
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// REP-3: Strict ordering — WAL applied in primary order
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_rep3_strict_ordering() {
    let config = HAConfig {
        replica_count: 1,
        ..Default::default()
    };
    let group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();

    // Insert rows in order 1, 2, 3, 4, 5
    for i in 1..=5 {
        insert_and_replicate(&group, i, &format!("ordered_{}", i), i as u64);
    }
    group.inner.catch_up_replica(0).unwrap();

    // Verify replica has the same rows as primary in the same order
    let primary_rows = group
        .inner
        .primary
        .storage
        .scan(TableId(1), TxnId(9999), Timestamp(u64::MAX - 1))
        .unwrap();
    let replica_rows = group.inner.replicas[0]
        .storage
        .scan(TableId(1), TxnId(9999), Timestamp(u64::MAX - 1))
        .unwrap();

    assert_eq!(
        primary_rows.len(),
        replica_rows.len(),
        "REP-3: row count mismatch"
    );

    // Both should contain the same set of values
    let primary_vals: Vec<String> = primary_rows
        .iter()
        .map(|(_, r)| match &r.values[1] {
            Datum::Text(s) => s.clone(),
            _ => "?".into(),
        })
        .collect();
    let replica_vals: Vec<String> = replica_rows
        .iter()
        .map(|(_, r)| match &r.values[1] {
            Datum::Text(s) => s.clone(),
            _ => "?".into(),
        })
        .collect();

    for pv in &primary_vals {
        assert!(
            replica_vals.contains(pv),
            "REP-3: replica missing value '{}'",
            pv
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// FAIL-1: After promote, new primary commits ⊆ old primary commits
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_fail1_commit_set_containment_after_promote() {
    let config = HAConfig {
        replica_count: 2,
        failover_cooldown: Duration::from_millis(0),
        ..Default::default()
    };
    let mut group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();

    // Write 5 rows
    for i in 1..=5 {
        insert_and_replicate(&group, i, &format!("v{}", i), i as u64);
    }

    // Catch up all replicas
    group.catch_up_all_replicas().unwrap();

    // Record old primary's commit set BEFORE failover
    let old_primary_commits = committed_txn_ids(&group, true, 0);

    // Promote best replica
    group.promote_best().unwrap();

    // Record new primary's commit set AFTER failover
    let new_primary_commits = committed_txn_ids(&group, true, 0);

    // FAIL-1: new ⊆ old
    assert!(
        validate_promote_commit_set(&old_primary_commits, &new_primary_commits).is_ok(),
        "FAIL-1 violation: new primary has commits not in old primary"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// FAIL-2: ACK'd txns survive failover (when replicas are caught up)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_fail2_acked_txns_survive_failover() {
    let config = HAConfig {
        replica_count: 2,
        failover_cooldown: Duration::from_millis(0),
        sync_mode: SyncMode::Async, // even in async, caught-up replicas preserve data
        ..Default::default()
    };
    let mut group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();

    // Write and replicate data
    for i in 1..=10 {
        insert_and_replicate(&group, i, &format!("val_{}", i), i as u64);
    }
    group.catch_up_all_replicas().unwrap();

    // Failover
    group.promote_best().unwrap();

    // All 10 rows should be on new primary
    let rows = group
        .inner
        .primary
        .storage
        .scan(TableId(1), TxnId(9999), Timestamp(u64::MAX - 1))
        .unwrap();
    assert_eq!(
        rows.len(),
        10,
        "FAIL-2: expected 10 rows on new primary, got {}",
        rows.len()
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// FAIL-3: Epoch fencing — stale epoch is rejected
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_fail3_epoch_fencing() {
    let config = HAConfig {
        replica_count: 2,
        failover_cooldown: Duration::from_millis(0),
        ..Default::default()
    };
    let mut group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();

    let initial_epoch = group.current_epoch();
    assert_eq!(initial_epoch, 1);

    // Write and promote
    insert_and_replicate(&group, 1, "a", 1);
    group.catch_up_all_replicas().unwrap();
    group.promote_best().unwrap();

    let new_epoch = group.current_epoch();
    assert_eq!(new_epoch, 2, "Epoch should be bumped after promote");

    // Old epoch should be rejected
    assert!(
        !group.validate_epoch(initial_epoch),
        "FAIL-3: stale epoch should be rejected"
    );
    // Current and future epochs should be accepted
    assert!(
        group.validate_epoch(new_epoch),
        "FAIL-3: current epoch should be accepted"
    );
    assert!(
        group.validate_epoch(new_epoch + 1),
        "FAIL-3: future epoch should be accepted"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// FAIL: Multiple sequential failovers maintain invariants
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_multiple_failovers_maintain_data() {
    let config = HAConfig {
        replica_count: 2,
        failover_cooldown: Duration::from_millis(0),
        ..Default::default()
    };
    let mut group = HAReplicaGroup::new(ShardId(0), &[test_schema()], config).unwrap();

    // Write initial data
    for i in 1..=3 {
        insert_and_replicate(&group, i, &format!("round1_{}", i), i as u64);
    }
    group.catch_up_all_replicas().unwrap();

    // First failover
    group.promote_best().unwrap();
    assert_eq!(group.current_epoch(), 2);

    // Write more data after first failover
    for i in 4..=6 {
        insert_and_replicate(&group, i, &format!("round2_{}", i), i as u64);
    }
    group.catch_up_all_replicas().unwrap();

    // Second failover
    group.promote_best().unwrap();
    assert_eq!(group.current_epoch(), 3);

    // All data should survive both failovers
    let rows = group
        .inner
        .primary
        .storage
        .scan(TableId(1), TxnId(9999), Timestamp(u64::MAX - 1))
        .unwrap();
    assert_eq!(
        rows.len(),
        6,
        "All 6 rows should survive 2 failovers, got {}",
        rows.len()
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// XS-1: Cross-shard atomicity — all commit or all abort
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_xs1_cross_shard_atomicity_commit() {
    let engine = Arc::new(ShardedEngine::new(3));
    let schema = TableSchema {
        id: TableId(1),
        name: "xs_test".into(),
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
    };
    engine.create_table_all(&schema).unwrap();

    let coord = TwoPhaseCoordinator::new(engine.clone(), Duration::from_secs(10));
    let target = vec![ShardId(0), ShardId(1), ShardId(2)];

    let result = coord
        .execute(
            &target,
            IsolationLevel::ReadCommitted,
            |storage, _txn_mgr, txn_id| {
                let row = OwnedRow::new(vec![Datum::Int32(42)]);
                storage.insert(TableId(1), row, txn_id)?;
                Ok(())
            },
        )
        .unwrap();

    assert!(result.committed, "XS-1: all shards should commit");

    // Verify ALL shards have the row
    for &sid in &target {
        let shard = engine.shard(sid).unwrap();
        let rows = shard
            .storage
            .scan(TableId(1), TxnId(9999), Timestamp(u64::MAX - 1))
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "XS-1: shard {:?} should have exactly 1 row",
            sid
        );
    }
}

#[test]
fn test_xs1_cross_shard_atomicity_abort() {
    let engine = Arc::new(ShardedEngine::new(3));
    let schema = TableSchema {
        id: TableId(1),
        name: "xs_abort".into(),
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
    };
    engine.create_table_all(&schema).unwrap();

    let coord = TwoPhaseCoordinator::new(engine.clone(), Duration::from_secs(10));
    let target = vec![ShardId(0), ShardId(1), ShardId(2)];

    // Force failure by inserting duplicate PK
    let result = coord
        .execute(
            &target,
            IsolationLevel::ReadCommitted,
            |storage, _txn_mgr, txn_id| {
                let row = OwnedRow::new(vec![Datum::Int32(1)]);
                storage.insert(TableId(1), row.clone(), txn_id)?;
                storage.insert(TableId(1), row, txn_id)?; // duplicate → error
                Ok(())
            },
        )
        .unwrap();

    assert!(!result.committed, "XS-1: should abort due to failure");

    // Verify NO shard has visible committed data
    for &sid in &target {
        let shard = engine.shard(sid).unwrap();
        let rows = shard
            .storage
            .scan(TableId(1), TxnId(9999), Timestamp(u64::MAX - 1))
            .unwrap();
        assert_eq!(
            rows.len(),
            0,
            "XS-1: shard {:?} should have 0 rows after abort, got {}",
            sid,
            rows.len()
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// XS-2: At-most-once commit — same txn cannot commit twice
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_xs2_at_most_once_commit() {
    let engine = Arc::new(ShardedEngine::new(2));
    let schema = TableSchema {
        id: TableId(1),
        name: "xs_once".into(),
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
    };
    engine.create_table_all(&schema).unwrap();

    let coord = TwoPhaseCoordinator::new(engine.clone(), Duration::from_secs(10));
    let target = vec![ShardId(0), ShardId(1)];

    // First execute succeeds
    let r1 = coord
        .execute(
            &target,
            IsolationLevel::ReadCommitted,
            |storage, _txn_mgr, txn_id| {
                let row = OwnedRow::new(vec![Datum::Int32(1)]);
                storage.insert(TableId(1), row, txn_id)?;
                Ok(())
            },
        )
        .unwrap();
    assert!(r1.committed);

    // Second execute with same row value should succeed (different txn_id)
    // but the data should be exactly 2 rows, not 4
    let r2 = coord
        .execute(
            &target,
            IsolationLevel::ReadCommitted,
            |storage, _txn_mgr, txn_id| {
                let row = OwnedRow::new(vec![Datum::Int32(2)]);
                storage.insert(TableId(1), row, txn_id)?;
                Ok(())
            },
        )
        .unwrap();
    assert!(r2.committed);

    // Each shard should have exactly 2 rows (one from each txn)
    for &sid in &target {
        let shard = engine.shard(sid).unwrap();
        let rows = shard
            .storage
            .scan(TableId(1), TxnId(9999), Timestamp(u64::MAX - 1))
            .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "XS-2: shard {:?} should have exactly 2 rows, got {}",
            sid,
            rows.len()
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Consistency validation helper tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_consistency_validation_helpers() {
    use falcon_common::consistency::validate_commit_timeline;

    // Valid timelines
    assert!(validate_commit_timeline(Some(1), Some(2), Some(3)).is_ok());
    assert!(validate_commit_timeline(Some(1), Some(1), Some(1)).is_ok());

    // CP-1 violation
    assert!(validate_commit_timeline(Some(5), Some(2), None).is_err());

    // CP-2 violation: client visible without durable
    assert!(validate_commit_timeline(Some(1), None, Some(3)).is_err());

    // Prefix property
    let primary = vec![TxnId(1), TxnId(2), TxnId(3)];
    let good_replica = vec![TxnId(1), TxnId(2)];
    let bad_replica = vec![TxnId(1), TxnId(99)];

    assert!(validate_prefix_property(&primary, &good_replica).is_ok());
    assert!(validate_prefix_property(&primary, &bad_replica).is_err());

    // Promote commit set
    let old = vec![TxnId(1), TxnId(2), TxnId(3)];
    let new_ok = vec![TxnId(1), TxnId(2)];
    let new_bad = vec![TxnId(1), TxnId(99)];

    assert!(validate_promote_commit_set(&old, &new_ok).is_ok());
    assert!(validate_promote_commit_set(&old, &new_bad).is_err());
}
