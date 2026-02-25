    use crate::engine::StorageEngine;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId, TxnType};
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };

    fn test_schema(name: &str) -> TableSchema {
        TableSchema {
            id: TableId(99),
            name: name.to_string(),
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
                    data_type: DataType::Int32,
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
    fn test_wal_observer_fires_on_all_operations() {
        let counter = Arc::new(AtomicU64::new(0));
        let counter_clone = counter.clone();

        let mut engine = StorageEngine::new_in_memory();
        engine.set_wal_observer(Box::new(move |_record| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        }));

        // DDL: create_table (1 WAL record)
        let schema = test_schema("t1");
        let table_id = engine.create_table(schema).unwrap();
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "create_table should fire observer"
        );

        // DML: insert (1 WAL record)
        let txn1 = TxnId(1);
        engine
            .insert(
                table_id,
                OwnedRow::new(vec![Datum::Int32(1), Datum::Int32(100)]),
                txn1,
            )
            .unwrap();
        assert_eq!(
            counter.load(Ordering::SeqCst),
            2,
            "insert should fire observer"
        );

        // DML: update (1 WAL record)
        let pk = engine
            .insert(
                table_id,
                OwnedRow::new(vec![Datum::Int32(2), Datum::Int32(200)]),
                txn1,
            )
            .unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 3);
        engine
            .update(
                table_id,
                &pk,
                OwnedRow::new(vec![Datum::Int32(2), Datum::Int32(999)]),
                txn1,
            )
            .unwrap();
        assert_eq!(
            counter.load(Ordering::SeqCst),
            4,
            "update should fire observer"
        );

        // DML: delete (1 WAL record)
        engine.delete(table_id, &pk, txn1).unwrap();
        assert_eq!(
            counter.load(Ordering::SeqCst),
            5,
            "delete should fire observer"
        );

        // Txn: commit_local (1 WAL record)
        engine
            .commit_txn(txn1, Timestamp(10), TxnType::Local)
            .unwrap();
        assert_eq!(
            counter.load(Ordering::SeqCst),
            6,
            "commit should fire observer"
        );

        // Txn: abort_local (1 WAL record)
        let txn2 = TxnId(2);
        engine
            .insert(
                table_id,
                OwnedRow::new(vec![Datum::Int32(3), Datum::Int32(300)]),
                txn2,
            )
            .unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 7);
        engine.abort_txn(txn2, TxnType::Local).unwrap();
        assert_eq!(
            counter.load(Ordering::SeqCst),
            8,
            "abort should fire observer"
        );

        // DDL: drop_table (1 WAL record)
        engine.drop_table("t1").unwrap();
        assert_eq!(
            counter.load(Ordering::SeqCst),
            9,
            "drop_table should fire observer"
        );
    }

    #[test]
    fn test_wal_observer_not_set_no_panic() {
        // Engine without observer should work normally
        let engine = StorageEngine::new_in_memory();
        let schema = test_schema("t2");
        let table_id = engine.create_table(schema).unwrap();
        let txn = TxnId(10);
        engine
            .insert(
                table_id,
                OwnedRow::new(vec![Datum::Int32(1), Datum::Int32(1)]),
                txn,
            )
            .unwrap();
        engine
            .commit_txn(txn, Timestamp(1), TxnType::Local)
            .unwrap();
    }

    #[test]
    fn test_snapshot_checkpoint_data() {
        let engine = StorageEngine::new_in_memory();
        let schema = test_schema("ckpt_test");
        let table_id = engine.create_table(schema).unwrap();

        // Insert and commit some rows
        let txn = TxnId(1);
        engine
            .insert(
                table_id,
                OwnedRow::new(vec![Datum::Int32(1), Datum::Int32(10)]),
                txn,
            )
            .unwrap();
        engine
            .insert(
                table_id,
                OwnedRow::new(vec![Datum::Int32(2), Datum::Int32(20)]),
                txn,
            )
            .unwrap();
        engine
            .commit_txn(txn, Timestamp(5), TxnType::Local)
            .unwrap();

        let ckpt = engine.snapshot_checkpoint_data();
        assert_eq!(ckpt.catalog.table_count(), 1);
        assert!(ckpt.catalog.find_table("ckpt_test").is_some());
        assert_eq!(ckpt.table_data.len(), 1);
        assert_eq!(ckpt.table_data[0].1.len(), 2); // 2 rows
                                                   // In-memory engine has no WAL, so lsn/segment are 0
        assert_eq!(ckpt.wal_lsn, 0);
        assert_eq!(ckpt.wal_segment_id, 0);
    }

    // 鈹€鈹€ ReplicaAckTracker tests 鈹€鈹€

    #[test]
    fn test_replica_ack_tracker_no_replicas() {
        use crate::engine::ReplicaAckTracker;
        let tracker = ReplicaAckTracker::new();
        // No replicas  鈫?no constraint
        assert_eq!(tracker.min_replica_safe_ts(), Timestamp::MAX);
        assert_eq!(tracker.replica_count(), 0);
    }

    #[test]
    fn test_replica_ack_tracker_single_replica() {
        use crate::engine::ReplicaAckTracker;
        let tracker = ReplicaAckTracker::new();
        tracker.update_ack(0, 50);
        assert_eq!(tracker.replica_count(), 1);
        assert_eq!(tracker.min_replica_safe_ts(), Timestamp(50));

        // Advance
        tracker.update_ack(0, 100);
        assert_eq!(tracker.min_replica_safe_ts(), Timestamp(100));

        // Backward ack is ignored
        tracker.update_ack(0, 80);
        assert_eq!(tracker.min_replica_safe_ts(), Timestamp(100));
    }

    #[test]
    fn test_replica_ack_tracker_multiple_replicas() {
        use crate::engine::ReplicaAckTracker;
        let tracker = ReplicaAckTracker::new();
        tracker.update_ack(0, 100);
        tracker.update_ack(1, 50);
        tracker.update_ack(2, 75);
        assert_eq!(tracker.replica_count(), 3);
        // min is replica 1 at 50
        assert_eq!(tracker.min_replica_safe_ts(), Timestamp(50));

        // Advance replica 1
        tracker.update_ack(1, 90);
        // min is now replica 2 at 75
        assert_eq!(tracker.min_replica_safe_ts(), Timestamp(75));
    }

    #[test]
    fn test_replica_ack_tracker_remove_replica() {
        use crate::engine::ReplicaAckTracker;
        let tracker = ReplicaAckTracker::new();
        tracker.update_ack(0, 100);
        tracker.update_ack(1, 50);
        assert_eq!(tracker.min_replica_safe_ts(), Timestamp(50));

        tracker.remove_replica(1);
        assert_eq!(tracker.replica_count(), 1);
        assert_eq!(tracker.min_replica_safe_ts(), Timestamp(100));

        tracker.remove_replica(0);
        assert_eq!(tracker.min_replica_safe_ts(), Timestamp::MAX);
    }

    #[test]
    fn test_replica_ack_tracker_snapshot() {
        use crate::engine::ReplicaAckTracker;
        let tracker = ReplicaAckTracker::new();
        tracker.update_ack(0, 100);
        tracker.update_ack(1, 50);
        let mut snap = tracker.snapshot();
        snap.sort_by_key(|&(id, _)| id);
        assert_eq!(snap, vec![(0, 100), (1, 50)]);
    }

    #[test]
    fn test_engine_replica_ack_tracker_integration() {
        let engine = StorageEngine::new_in_memory();
        // Initially no replicas  鈫?MAX
        assert_eq!(
            engine.replica_ack_tracker().min_replica_safe_ts(),
            Timestamp::MAX
        );

        // Register replicas
        engine.replica_ack_tracker().update_ack(0, 50);
        engine.replica_ack_tracker().update_ack(1, 30);
        assert_eq!(
            engine.replica_ack_tracker().min_replica_safe_ts(),
            Timestamp(30)
        );

        // GC safepoint should be constrained by replica
        use crate::gc::compute_safepoint;
        let min_active = Timestamp(100);
        let replica_ts = engine.replica_ack_tracker().min_replica_safe_ts();
        let safepoint = compute_safepoint(min_active, replica_ts);
        assert_eq!(safepoint, Timestamp(29)); // min(100, 30) - 1

        // Advance replica 1
        engine.replica_ack_tracker().update_ack(1, 80);
        let replica_ts = engine.replica_ack_tracker().min_replica_safe_ts();
        let safepoint = compute_safepoint(min_active, replica_ts);
        assert_eq!(safepoint, Timestamp(49)); // min(100, 50) - 1
    }

    #[test]
    fn test_gc_runner_with_dynamic_replica_acks() {
        use crate::gc::{GcConfig, GcRunner, SafepointProvider};
        use std::sync::atomic::{AtomicU64, Ordering as AOrdering};
        use std::sync::Arc;

        struct DynamicProvider {
            ts: AtomicU64,
            engine: Arc<StorageEngine>,
        }
        impl SafepointProvider for DynamicProvider {
            fn min_active_ts(&self) -> Timestamp {
                Timestamp(self.ts.load(AOrdering::Relaxed))
            }
            fn replica_safe_ts(&self) -> Timestamp {
                self.engine.replica_ack_tracker().min_replica_safe_ts()
            }
        }

        fn mk_row(v: i32) -> OwnedRow {
            OwnedRow::new(vec![Datum::Int32(v), Datum::Int32(v * 10)])
        }

        let schema = test_schema("gc_dyn_replica");
        let engine = Arc::new(StorageEngine::new_in_memory());
        engine.create_table(schema).unwrap();

        // Create 3 versions for key 1
        engine.insert(TableId(99), mk_row(1), TxnId(1)).unwrap();
        engine
            .commit_txn(TxnId(1), Timestamp(10), TxnType::Local)
            .unwrap();
        let pk = crate::memtable::encode_pk(&mk_row(1), &[0]);
        engine
            .update(TableId(99), &pk, mk_row(2), TxnId(2))
            .unwrap();
        engine
            .commit_txn(TxnId(2), Timestamp(20), TxnType::Local)
            .unwrap();
        engine
            .update(TableId(99), &pk, mk_row(3), TxnId(3))
            .unwrap();
        engine
            .commit_txn(TxnId(3), Timestamp(30), TxnType::Local)
            .unwrap();

        // Register a slow replica at ts=5  鈥?should block GC
        engine.replica_ack_tracker().update_ack(0, 5);

        let provider = Arc::new(DynamicProvider {
            ts: AtomicU64::new(100),
            engine: engine.clone(),
        });
        let config = GcConfig {
            interval_ms: 10,
            min_chain_length: 0,
            ..Default::default()
        };

        let mut runner = GcRunner::start(engine.clone(), provider.clone(), config.clone())
            .expect("spawn in test");
        std::thread::sleep(std::time::Duration::from_millis(50));
        runner.stop();

        // Safepoint is min(100, 5) - 1 = 4, so nothing should be reclaimed
        // (all versions committed at ts >= 10, which is > 4)
        let snap = engine.gc_stats_snapshot();
        assert_eq!(snap.total_reclaimed_versions, 0, "replica holding back GC");

        // Now advance replica ack to ts=100
        engine.replica_ack_tracker().update_ack(0, 100);

        let mut runner2 = GcRunner::start(engine.clone(), provider, config).expect("spawn in test");
        std::thread::sleep(std::time::Duration::from_millis(50));
        runner2.stop();

        let snap2 = engine.gc_stats_snapshot();
        assert!(
            snap2.total_reclaimed_versions > 0,
            "GC should reclaim after replica catches up"
        );
    }
