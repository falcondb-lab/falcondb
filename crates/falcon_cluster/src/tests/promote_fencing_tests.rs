    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, ShardId, TableId, Timestamp, TxnId};
    use falcon_storage::wal::WalRecord;

    use crate::replication::{ReplicaRole, ShardReplicaGroup};

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
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        }
    }

    fn insert_on_primary(
        group: &ShardReplicaGroup,
        txn_id: TxnId,
        commit_ts: Timestamp,
        id: i32,
        value: &str,
    ) {
        let row = OwnedRow::new(vec![Datum::Int32(id), Datum::Text(value.into())]);
        group
            .primary
            .storage
            .insert(TableId(1), row.clone(), txn_id)
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(txn_id, commit_ts, falcon_common::types::TxnType::Local)
            .unwrap();
        group.ship_wal_record(WalRecord::Insert {
            txn_id,
            table_id: TableId(1),
            row,
        });
        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id, commit_ts });
    }

    #[test]
    fn test_old_primary_fenced_after_promote() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Primary should not be read-only initially
        assert!(!group.primary.is_read_only());

        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "before");
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // New primary should not be read-only
        assert!(!group.primary.is_read_only());
        assert_eq!(group.primary.current_role(), ReplicaRole::Primary);

        // Old primary (now replica at index 0) should be fenced
        assert!(group.replicas[0].is_read_only());
        assert_eq!(group.replicas[0].current_role(), ReplicaRole::Replica);
    }

    #[test]
    fn test_promote_records_metrics() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Initial metrics
        let snap = group.metrics.snapshot();
        assert_eq!(snap.promote_count, 0);

        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "v");
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        let snap = group.metrics.snapshot();
        assert_eq!(snap.promote_count, 1);
        // failover_time_ms may be 0 (sub-ms) but should not panic
    }

    #[test]
    fn test_promote_with_routing_records_metrics() {
        use crate::routing::shard_map::ShardMap;
        use falcon_common::types::NodeId;

        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();
        let mut shard_map = ShardMap::uniform(2, NodeId(1));

        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "v");
        group.catch_up_replica(0).unwrap();
        group
            .promote_with_routing(0, &mut shard_map, NodeId(2))
            .unwrap();

        let snap = group.metrics.snapshot();
        assert_eq!(snap.promote_count, 1);
        assert_eq!(shard_map.get_shard(ShardId(0)).unwrap().leader, NodeId(2));
    }

    #[test]
    fn test_new_primary_accepts_writes_after_promote() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "before");
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // New primary should accept writes (unfenced)
        assert!(!group.primary.is_read_only());
        let row = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("after".into())]);
        group
            .primary
            .storage
            .insert(TableId(1), row, TxnId(10))
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(
                TxnId(10),
                Timestamp(10),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();

        let rows = group
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_replica_starts_read_only() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();
        assert!(group.replicas[0].is_read_only());
        assert!(!group.primary.is_read_only());
    }

    #[test]
    fn test_promote_updates_engine_replication_stats() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Before promote, new primary's engine stats should be zero
        let snap_before = group.replicas[0].storage.replication_stats_snapshot();
        assert_eq!(snap_before.promote_count, 0);

        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "v");
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // After promote, new primary's engine stats should reflect the failover
        let snap_after = group.primary.storage.replication_stats_snapshot();
        assert_eq!(snap_after.promote_count, 1);
        // Duration may be 0 for sub-ms operations, but should not panic
    }

    #[test]
    fn test_double_promote_accumulates_engine_stats() {
        let schemas = vec![test_schema()];
        let mut group = ShardReplicaGroup::new(ShardId(0), &schemas).unwrap();

        // First promote: primary(A)  鈫?replica(B) promotes to primary
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "v1");
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // Second promote: primary(B)  鈫?replica(A) promotes back
        let row = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("v2".into())]);
        group
            .primary
            .storage
            .insert(TableId(1), row.clone(), TxnId(2))
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(TxnId(2), Timestamp(2), falcon_common::types::TxnType::Local)
            .unwrap();
        group.ship_wal_record(WalRecord::Insert {
            txn_id: TxnId(2),
            table_id: TableId(1),
            row,
        });
        group.ship_wal_record(WalRecord::CommitTxnLocal {
            txn_id: TxnId(2),
            commit_ts: Timestamp(2),
        });
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // The current primary should have 1 promote recorded on its engine
        // (it was the target of the second promote)
        let snap = group.primary.storage.replication_stats_snapshot();
        assert_eq!(snap.promote_count, 1);

        // Cluster-level metrics should show 2 total promotes
        let cluster_snap = group.metrics.snapshot();
        assert_eq!(cluster_snap.promote_count, 2);
    }
