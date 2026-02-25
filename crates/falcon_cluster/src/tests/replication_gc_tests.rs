    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, ShardId, TableId, Timestamp, TxnId};
    use falcon_storage::gc::{compute_safepoint, GcConfig, GcStats};
    use falcon_storage::wal::WalRecord;

    use crate::replication::ShardReplicaGroup;

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
    fn test_gc_on_primary_does_not_break_replica_catchup() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Write 3 versions of key 1 on primary
        let base_row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v1".into())]);
        let pk = falcon_storage::memtable::encode_pk(&base_row, &[0]);

        // Insert v1
        group
            .primary
            .storage
            .insert(TableId(1), base_row.clone(), TxnId(1))
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(
                TxnId(1),
                Timestamp(10),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        group.ship_wal_record(WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: base_row,
        });

        group.ship_wal_record(WalRecord::CommitTxnLocal {
            txn_id: TxnId(1),
            commit_ts: Timestamp(10),
        });

        // Update to v2
        let row2 = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v2".into())]);
        group
            .primary
            .storage
            .update(TableId(1), &pk, row2.clone(), TxnId(2))
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(
                TxnId(2),
                Timestamp(20),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        group.ship_wal_record(WalRecord::Update {
            txn_id: TxnId(2),
            table_id: TableId(1),
            pk: pk.clone(),
            new_row: row2,
        });
        group.ship_wal_record(WalRecord::CommitTxnLocal {
            txn_id: TxnId(2),
            commit_ts: Timestamp(20),
        });

        // Update to v3
        let row3 = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v3".into())]);
        group
            .primary
            .storage
            .update(TableId(1), &pk, row3.clone(), TxnId(3))
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(
                TxnId(3),
                Timestamp(30),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        group.ship_wal_record(WalRecord::Update {
            txn_id: TxnId(3),
            table_id: TableId(1),
            pk: pk.clone(),
            new_row: row3,
        });
        group.ship_wal_record(WalRecord::CommitTxnLocal {
            txn_id: TxnId(3),
            commit_ts: Timestamp(30),
        });

        // GC on primary at watermark 25  鈥?reclaims ts=10
        let result = group.primary.storage.gc_sweep(Timestamp(25));
        assert_eq!(result.reclaimed_versions, 1);

        // Replica catch-up should still work (WAL replay is independent of in-memory GC)
        group.catch_up_replica(0).unwrap();

        // Verify replica has the data
        let rows = group.replicas[0]
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 1);
        // Replica should have the latest value
        assert_eq!(rows[0].1.values[1], Datum::Text("v3".into()));
    }

    #[test]
    fn test_gc_safepoint_respects_replica_lag() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Write 3 rows on primary
        for i in 1..=3u32 {
            insert_on_primary(
                &group,
                TxnId(i as u64),
                Timestamp(i as u64 * 10),
                i as i32,
                &format!("v{}", i),
            );
        }

        // Simulate: replica has NOT caught up (applied_ts = 0).
        // In a real system, replica_safe_ts would come from the replica's
        // applied LSN mapped to a timestamp.
        let min_active_ts = Timestamp(100); // no active txns blocking
        let replica_safe_ts = Timestamp(15); // replica only applied up to ts=15

        let safepoint = compute_safepoint(min_active_ts, replica_safe_ts);
        assert_eq!(safepoint, Timestamp(14));

        // GC with replica-aware safepoint: should NOT reclaim anything
        // because all commits are at ts=10, 20, 30 and safepoint=14
        // means only ts=10 is eligible, but each key has a single version.
        let config = GcConfig {
            min_chain_length: 0,
            ..Default::default()
        };
        let stats = GcStats::new();
        let result = group
            .primary
            .storage
            .run_gc_with_config(safepoint, &config, &stats);
        // No versions to reclaim (each key has exactly 1 version)
        assert_eq!(result.reclaimed_versions, 0);

        // Now catch up replica and recalculate
        group.catch_up_replica(0).unwrap();
        // After catch-up, replica_safe_ts would advance to 30
        let safepoint2 = compute_safepoint(min_active_ts, Timestamp(100));
        assert_eq!(safepoint2, Timestamp(99));

        // Still 0 reclaimed  鈥?each key has 1 version (no multi-version chains)
        let result2 = group
            .primary
            .storage
            .run_gc_with_config(safepoint2, &config, &stats);
        assert_eq!(result2.reclaimed_versions, 0);
    }

    #[test]
    fn test_gc_and_promote_combined() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Write 2 rows, GC on primary, then promote
        insert_on_primary(&group, TxnId(1), Timestamp(10), 1, "before");
        insert_on_primary(&group, TxnId(2), Timestamp(20), 2, "before2");

        // GC on primary (no multi-version chains, nothing to reclaim)
        group.primary.storage.gc_sweep(Timestamp(25));

        // Catch up and promote
        group.catch_up_replica(0).unwrap();
        group.promote(0).unwrap();

        // New primary should have both rows
        let rows = group
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 2);

        // New primary should be writable
        let row = OwnedRow::new(vec![Datum::Int32(3), Datum::Text("after".into())]);
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
                Timestamp(30),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();

        let rows2 = group
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows2.len(), 3);
    }

    /// Comprehensive M1 acceptance test exercising the full lifecycle:
    /// 1. Create cluster (1 primary + 1 replica)
    /// 2. Write multiple rows with updates (multi-version chains)
    /// 3. Replicate to replica
    /// 4. GC on primary (reclaim old versions)
    /// 5. Promote replica to primary
    /// 6. Write new data on promoted primary
    /// 7. Verify all committed data is intact (zero data loss)
    /// 8. Verify replication stats reflect the promote
    /// 9. Verify GC stats reflect the sweep
    #[test]
    fn test_m1_full_lifecycle_acceptance() {
        let mut group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // 鈹€鈹€ Phase 1: Write data with multi-version chains 鈹€鈹€
        // Insert 5 rows
        for i in 1..=5 {
            insert_on_primary(
                &group,
                TxnId(i as u64),
                Timestamp(i as u64 * 10),
                i,
                &format!("original_{}", i),
            );
        }

        // Update rows 1-3 to create multi-version chains
        let pk1 = falcon_storage::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int32(1), Datum::Text("x".into())]),
            &[0],
        );
        let pk2 = falcon_storage::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int32(2), Datum::Text("x".into())]),
            &[0],
        );
        let pk3 = falcon_storage::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int32(3), Datum::Text("x".into())]),
            &[0],
        );

        let upd1 = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("updated_1".into())]);
        group
            .primary
            .storage
            .update(TableId(1), &pk1, upd1.clone(), TxnId(10))
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(
                TxnId(10),
                Timestamp(60),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        group.ship_wal_record(WalRecord::Update {
            txn_id: TxnId(10),
            table_id: TableId(1),
            pk: pk1.clone(),
            new_row: upd1,
        });
        group.ship_wal_record(WalRecord::CommitTxnLocal {
            txn_id: TxnId(10),
            commit_ts: Timestamp(60),
        });

        let upd2 = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("updated_2".into())]);
        group
            .primary
            .storage
            .update(TableId(1), &pk2, upd2.clone(), TxnId(11))
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(
                TxnId(11),
                Timestamp(70),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        group.ship_wal_record(WalRecord::Update {
            txn_id: TxnId(11),
            table_id: TableId(1),
            pk: pk2.clone(),
            new_row: upd2,
        });
        group.ship_wal_record(WalRecord::CommitTxnLocal {
            txn_id: TxnId(11),
            commit_ts: Timestamp(70),
        });

        let upd3 = OwnedRow::new(vec![Datum::Int32(3), Datum::Text("updated_3".into())]);
        group
            .primary
            .storage
            .update(TableId(1), &pk3, upd3.clone(), TxnId(12))
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(
                TxnId(12),
                Timestamp(80),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        group.ship_wal_record(WalRecord::Update {
            txn_id: TxnId(12),
            table_id: TableId(1),
            pk: pk3.clone(),
            new_row: upd3,
        });
        group.ship_wal_record(WalRecord::CommitTxnLocal {
            txn_id: TxnId(12),
            commit_ts: Timestamp(80),
        });

        // 鈹€鈹€ Phase 2: GC on primary 鈹€鈹€
        // Watermark must be >= max update ts (80) so updated versions become
        // the "latest visible at watermark", making the original versions
        // (ts=10,20,30) obsolete and reclaimable in chains of length 2.
        let gc_result = group.primary.storage.gc_sweep(Timestamp(85));
        assert!(
            gc_result.reclaimed_versions >= 3,
            "GC should reclaim at least 3 old versions, got {}",
            gc_result.reclaimed_versions
        );

        // Verify GC stats on primary
        let gc_snap = group.primary.storage.gc_stats_snapshot();
        assert!(gc_snap.total_sweeps >= 1);
        assert!(gc_snap.total_reclaimed_versions >= 3);

        // 鈹€鈹€ Phase 3: Replicate and promote 鈹€鈹€
        group.catch_up_replica(0).unwrap();

        // Verify replica has all 5 rows before promote
        let replica_rows = group.replicas[0]
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(200))
            .unwrap();
        assert_eq!(
            replica_rows.len(),
            5,
            "replica should have all 5 rows before promote"
        );

        // Promote
        group.promote(0).unwrap();

        // 鈹€鈹€ Phase 4: Verify on new primary 鈹€鈹€
        // Check replication stats
        let repl_snap = group.primary.storage.replication_stats_snapshot();
        assert_eq!(repl_snap.promote_count, 1, "promote_count should be 1");

        // Cluster metrics
        let cluster_snap = group.metrics.snapshot();
        assert_eq!(cluster_snap.promote_count, 1);

        // New primary should not be read-only
        assert!(!group.primary.is_read_only());

        // All 5 rows should be intact with updated values for rows 1-3
        let rows = group
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(200))
            .unwrap();
        assert_eq!(rows.len(), 5, "new primary should have all 5 rows");

        // Verify updated values
        let row1 = rows
            .iter()
            .find(|(_, r)| r.values[0] == Datum::Int32(1))
            .unwrap();
        assert_eq!(row1.1.values[1], Datum::Text("updated_1".into()));
        let row2 = rows
            .iter()
            .find(|(_, r)| r.values[0] == Datum::Int32(2))
            .unwrap();
        assert_eq!(row2.1.values[1], Datum::Text("updated_2".into()));
        let row3 = rows
            .iter()
            .find(|(_, r)| r.values[0] == Datum::Int32(3))
            .unwrap();
        assert_eq!(row3.1.values[1], Datum::Text("updated_3".into()));
        // Rows 4-5 should still have original values
        let row4 = rows
            .iter()
            .find(|(_, r)| r.values[0] == Datum::Int32(4))
            .unwrap();
        assert_eq!(row4.1.values[1], Datum::Text("original_4".into()));
        let row5 = rows
            .iter()
            .find(|(_, r)| r.values[0] == Datum::Int32(5))
            .unwrap();
        assert_eq!(row5.1.values[1], Datum::Text("original_5".into()));

        // 鈹€鈹€ Phase 5: Write on promoted primary 鈹€鈹€
        let new_row = OwnedRow::new(vec![Datum::Int32(6), Datum::Text("post_promote".into())]);
        group
            .primary
            .storage
            .insert(TableId(1), new_row, TxnId(100))
            .unwrap();
        group
            .primary
            .storage
            .commit_txn(
                TxnId(100),
                Timestamp(200),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();

        let final_rows = group
            .primary
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(300))
            .unwrap();
        assert_eq!(
            final_rows.len(),
            6,
            "post-promote write should succeed, total 6 rows"
        );

        let row6 = final_rows
            .iter()
            .find(|(_, r)| r.values[0] == Datum::Int32(6))
            .unwrap();
        assert_eq!(row6.1.values[1], Datum::Text("post_promote".into()));
    }

    #[test]
    fn test_grpc_transport_with_timeout() {
        let transport = crate::grpc_transport::GrpcTransport::with_timeout(
            "http://127.0.0.1:50099".to_string(),
            ShardId(0),
            std::time::Duration::from_millis(2500),
        );
        assert_eq!(transport.endpoint, "http://127.0.0.1:50099");
        assert_eq!(transport.shard_id, ShardId(0));
        assert_eq!(transport.get_ack_lsn(0), 0);
        assert_eq!(transport.get_ack_lsn(99), 0);
    }

    #[test]
    fn test_replication_config_new_fields_default() {
        let config = falcon_common::config::ReplicationConfig::default();
        assert_eq!(config.max_backoff_ms, 30_000);
        assert_eq!(config.connect_timeout_ms, 5_000);
        assert_eq!(config.poll_interval_ms, 100);
    }

    #[test]
    fn test_replication_config_serde_roundtrip() {
        let config = falcon_common::config::ReplicationConfig {
            role: falcon_common::config::NodeRole::Replica,
            grpc_listen_addr: "0.0.0.0:50052".to_string(),
            primary_endpoint: "http://127.0.0.1:50051".to_string(),
            max_records_per_chunk: 500,
            poll_interval_ms: 50,
            max_backoff_ms: 10_000,
            connect_timeout_ms: 3_000,
            shard_count: 2,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deser: falcon_common::config::ReplicationConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.role, falcon_common::config::NodeRole::Replica);
        assert_eq!(deser.max_records_per_chunk, 500);
        assert_eq!(deser.poll_interval_ms, 50);
        assert_eq!(deser.max_backoff_ms, 10_000);
        assert_eq!(deser.connect_timeout_ms, 3_000);
        assert_eq!(deser.shard_count, 2);
    }

    #[test]
    fn test_exponential_backoff_calculation() {
        let poll_ms: u64 = 100;
        let max_backoff_ms: u64 = 30_000;

        // Simulate consecutive errors and verify backoff
        let backoff = |errors: u32| -> u64 {
            std::cmp::min(poll_ms * 2u64.saturating_pow(errors), max_backoff_ms)
        };

        assert_eq!(backoff(1), 200); // 100 * 2^1
        assert_eq!(backoff(2), 400); // 100 * 2^2
        assert_eq!(backoff(3), 800); // 100 * 2^3
        assert_eq!(backoff(5), 3200); // 100 * 2^5
        assert_eq!(backoff(8), 25600); // 100 * 2^8
        assert_eq!(backoff(9), 30_000); // capped at max
        assert_eq!(backoff(20), 30_000); // still capped
        assert_eq!(backoff(0), 100); // 100 * 2^0 = 100 (no error)
    }
