    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, ShardId, TableId, Timestamp, TxnId};
    use falcon_storage::wal::WalRecord;

    use crate::replication::{ReplicaRole, ShardReplicaGroup};

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "replicated".into(),
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
                    name: "value".into(),
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

    /// Insert a row on primary via WAL records and ship to replication log.
    fn insert_on_primary(
        group: &ShardReplicaGroup,
        txn_id: TxnId,
        commit_ts: Timestamp,
        id: i32,
        value: &str,
    ) {
        let row = OwnedRow::new(vec![Datum::Int32(id), Datum::Text(value.into())]);

        // Execute on primary engine
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

        // Ship WAL records
        group.ship_wal_record(WalRecord::Insert {
            txn_id,
            table_id: TableId(1),
            row,
        });
        group.ship_wal_record(WalRecord::CommitTxnLocal { txn_id, commit_ts });
    }

    #[test]
    fn test_replication_basic_catch_up() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Insert on primary
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "hello");
        insert_on_primary(&group, TxnId(2), Timestamp(2), 2, "world");

        // Replica should have 0 rows before catch-up
        let replica = &group.replicas[0];
        let rows = replica
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 0, "replica should be empty before catch-up");

        // Catch up
        let applied = group.catch_up_replica(0).unwrap();
        assert_eq!(
            applied, 4,
            "should apply 4 WAL records (2 insert + 2 commit)"
        );

        // Replica should now have 2 rows
        let rows = replica
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 2, "replica should have 2 rows after catch-up");
    }

    #[test]
    fn test_replication_lag_tracking() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "a");

        let lag = group.replication_lag();
        assert_eq!(lag.len(), 1);
        assert_eq!(lag[0].0, 0); // replica index
        assert!(lag[0].1 > 0, "replica should be behind primary");

        // Catch up
        group.catch_up_replica(0).unwrap();
        let lag = group.replication_lag();
        assert_eq!(lag[0].1, 0, "replica should be caught up");
    }

    #[test]
    fn test_committed_data_survives_promote() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Insert data on primary
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "committed_before_crash");
        insert_on_primary(&group, TxnId(2), Timestamp(2), 2, "also_committed");

        // Catch up replica
        group.catch_up_replica(0).unwrap();

        // Promote replica to primary (simulates primary crash + failover)
        group.promote(0).unwrap();

        // Verify the new primary has the committed data
        assert_eq!(group.primary.current_role(), ReplicaRole::Primary);
        let rows = group
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 2, "committed data should survive promote");
    }

    #[test]
    fn test_uncommitted_not_visible_after_promote() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Committed txn
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "committed");

        // Uncommitted txn: insert but no commit WAL record shipped
        let row = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("uncommitted".into())]);
        group
            .primary
            .storage
            .insert(TableId(1), row.clone(), TxnId(2))
            .unwrap();
        // Ship only the insert, not the commit
        group.ship_wal_record(WalRecord::Insert {
            txn_id: TxnId(2),
            table_id: TableId(1),
            row,
        });

        // Catch up and promote
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // The uncommitted txn should NOT be visible on the new primary
        let rows = group
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "only committed txn should be visible after promote"
        );
        assert_eq!(rows[0].1.values[1], Datum::Text("committed".into()));
    }

    #[test]
    fn test_replica_reconnect_catch_up() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Phase 1: insert 3 rows, catch up
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "a");
        insert_on_primary(&group, TxnId(2), Timestamp(2), 2, "b");
        insert_on_primary(&group, TxnId(3), Timestamp(3), 3, "c");
        group.catch_up_replica(0).unwrap();

        // Phase 2: "disconnect"  鈥?insert more rows without catching up
        insert_on_primary(&group, TxnId(4), Timestamp(4), 4, "d");
        insert_on_primary(&group, TxnId(5), Timestamp(5), 5, "e");

        // Verify lag
        let lag = group.replication_lag();
        assert!(lag[0].1 > 0, "should have lag after disconnect");

        // Phase 3: "reconnect"  鈥?catch up from last applied LSN
        let applied = group.catch_up_replica(0).unwrap();
        assert_eq!(
            applied, 4,
            "should catch up 4 records (2 insert + 2 commit)"
        );

        let rows = group.replicas[0]
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(
            rows.len(),
            5,
            "replica should have all 5 rows after catch-up"
        );

        let lag = group.replication_lag();
        assert_eq!(lag[0].1, 0, "should be caught up");
    }

    #[test]
    fn test_promote_swaps_roles() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        assert_eq!(group.primary.current_role(), ReplicaRole::Primary);
        assert_eq!(group.replicas[0].current_role(), ReplicaRole::Replica);

        group.promote(0).unwrap();

        assert_eq!(group.primary.current_role(), ReplicaRole::Primary);
        assert_eq!(group.replicas[0].current_role(), ReplicaRole::Replica);
    }

    #[test]
    fn test_new_writes_after_promote() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Initial data
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "before");
        group.catch_up_replica(0).unwrap();

        // Promote
        group.promote(0).unwrap();

        // Write to the new primary
        let row = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("after_promote".into())]);
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
        assert_eq!(rows.len(), 2, "new primary should accept new writes");
    }

    #[test]
    fn test_promote_with_routing_updates_shard_map() {
        use crate::routing::shard_map::ShardMap;
        use falcon_common::types::NodeId;

        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();
        let mut shard_map = ShardMap::uniform(2, NodeId(1));

        // Verify initial leader
        assert_eq!(shard_map.get_shard(ShardId(0)).unwrap().leader, NodeId(1));

        // Insert data, catch up
        insert_on_primary(&group, TxnId(1), Timestamp(1), 1, "data");
        group.catch_up_replica(0).unwrap();

        // Promote with routing update: new leader is NodeId(2)
        group
            .promote_with_routing(0, &mut shard_map, NodeId(2))
            .unwrap();

        // Verify shard_map was updated
        assert_eq!(
            shard_map.get_shard(ShardId(0)).unwrap().leader,
            NodeId(2),
            "shard_map leader should be updated after promote_with_routing"
        );

        // Verify new primary still has the data
        let rows = group
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_replication_lag_timeline() {
        // Simulates replication lag over time with a burst of writes,
        // gradual catch-up, and a second burst  鈥?produces data suitable
        // for a "replication lag over time" graph.
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        let mut lag_timeline: Vec<(u32, u64)> = Vec::new(); // (step, lag)

        // Phase 1: burst of 10 writes, lag grows
        for i in 1..=10u32 {
            insert_on_primary(
                &group,
                TxnId(i as u64),
                Timestamp(i as u64),
                i as i32,
                &format!("v{}", i),
            );
            let lag = group.replication_lag()[0].1;
            lag_timeline.push((i, lag));
        }
        assert!(
            lag_timeline.last().unwrap().1 > 0,
            "lag should be > 0 after burst"
        );

        // Phase 2: catch up
        group.catch_up_replica(0).unwrap();
        lag_timeline.push((11, group.replication_lag()[0].1));
        assert_eq!(
            lag_timeline.last().unwrap().1,
            0,
            "lag should be 0 after catch-up"
        );

        // Phase 3: second burst of 5 writes
        for i in 11..=15u32 {
            insert_on_primary(
                &group,
                TxnId(i as u64),
                Timestamp(i as u64),
                i as i32,
                &format!("v{}", i),
            );
            let lag = group.replication_lag()[0].1;
            lag_timeline.push((i + 1, lag));
        }
        assert!(lag_timeline.last().unwrap().1 > 0, "lag should grow again");

        // Phase 4: catch up again
        group.catch_up_replica(0).unwrap();
        lag_timeline.push((17, group.replication_lag()[0].1));
        assert_eq!(lag_timeline.last().unwrap().1, 0);

        // Verify timeline shape: lag increases during bursts, drops to 0 on catch-up
        assert!(
            lag_timeline.len() >= 16,
            "timeline should have enough data points"
        );
    }
