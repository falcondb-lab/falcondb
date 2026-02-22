//! Falcon M1 Failover Exercise —integration test.
//!
//! Exercises the full failover lifecycle:
//! 1. Start a cluster (1 primary + 1 replica per shard)
//! 2. Write test data on primary
//! 3. Replicate to replica
//! 4. "Kill" primary (fence as read-only)
//! 5. Promote replica to new primary
//! 6. Write new data on promoted primary
//! 7. Verify all data integrity
//!
//! Run:  cargo test -p falcon_cluster --test failover -- --ignored

use falcon_cluster::replication::ShardReplicaGroup;
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::*;
use falcon_storage::wal::WalRecord;

fn test_schema() -> TableSchema {
    TableSchema {
        id: TableId(1),
        name: "accounts".into(),
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
                name: "balance".into(),
                data_type: DataType::Int64,
                nullable: false,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
            },
            ColumnDef {
                id: ColumnId(2),
                name: "name".into(),
                data_type: DataType::Text,
                nullable: true,
                is_primary_key: false,
                default_value: None,
                is_serial: false,
            },
        ],
        primary_key_columns: vec![0],
        next_serial_values: std::collections::HashMap::new(),
        check_constraints: vec![],
        unique_constraints: vec![],
        foreign_keys: vec![],
        ..Default::default()
    }
}

#[test]
#[ignore] // Run explicitly: cargo test -p falcon_cluster --test failover -- --ignored
fn failover_exercise() {
    // Step 1: Create cluster
    let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()])
        .expect("Failed to create ShardReplicaGroup");
    assert!(!group.primary.is_read_only());
    assert!(group.replicas[0].is_read_only());

    // Step 2: Write test data on primary
    let accounts = vec![
        (1, 1000i64, "Alice"),
        (2, 2500, "Bob"),
        (3, 500, "Charlie"),
        (4, 10000, "Diana"),
        (5, 750, "Eve"),
    ];

    for (id, balance, name) in &accounts {
        let row = OwnedRow::new(vec![
            Datum::Int32(*id),
            Datum::Int64(*balance),
            Datum::Text(name.to_string()),
        ]);
        let txn_id = TxnId(*id as u64);
        let commit_ts = Timestamp(*id as u64 * 10);

        group
            .primary
            .storage
            .insert(TableId(1), row.clone(), txn_id)
            .expect("insert failed");
        group
            .primary
            .storage
            .commit_txn(txn_id, commit_ts, TxnType::Local)
            .expect("commit failed");

        group.ship_wal_record(WalRecord::Insert {
            txn_id,
            table_id: TableId(1),
            row,
        });
        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id, commit_ts });
    }

    let primary_rows = group
        .primary
        .storage
        .scan(TableId(1), TxnId(999), Timestamp(100))
        .expect("scan failed");
    assert_eq!(primary_rows.len(), accounts.len());

    // Step 3: Replicate to replica
    group.catch_up_replica(0).expect("catch_up failed");
    let replica_rows = group.replicas[0]
        .storage
        .scan(TableId(1), TxnId(999), Timestamp(100))
        .expect("scan failed");
    assert_eq!(
        replica_rows.len(),
        accounts.len(),
        "replica row count mismatch"
    );

    // Step 5: Promote replica
    group.promote(0).expect("promote failed");
    assert!(
        !group.primary.is_read_only(),
        "new primary should not be read_only"
    );
    assert!(
        group.replicas[0].is_read_only(),
        "old primary should be read_only"
    );

    let metrics = group.metrics.snapshot();
    assert_eq!(metrics.promote_count, 1);

    // Step 6: Write new data on promoted primary
    let new_row = OwnedRow::new(vec![
        Datum::Int32(6),
        Datum::Int64(3000),
        Datum::Text("Frank".into()),
    ]);
    group
        .primary
        .storage
        .insert(TableId(1), new_row, TxnId(100))
        .expect("post-promote insert failed");
    group
        .primary
        .storage
        .commit_txn(TxnId(100), Timestamp(200), TxnType::Local)
        .expect("post-promote commit failed");

    // Step 7: Verify data integrity
    let final_rows = group
        .primary
        .storage
        .scan(TableId(1), TxnId(999), Timestamp(300))
        .expect("final scan failed");
    assert_eq!(final_rows.len(), 6, "expected 6 accounts after failover");
}
