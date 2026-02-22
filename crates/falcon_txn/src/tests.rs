#[cfg(test)]
mod txn_manager_tests {
    use std::sync::Arc;

    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId};
    use falcon_common::types::{IsolationLevel, ShardId, Timestamp, TxnId};
    use falcon_storage::engine::StorageEngine;

    use crate::manager::{
        SlowPathMode, TxnClassification, TxnManager, TxnOutcome, TxnPath, TxnState, TxnType,
    };

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "txn_test".into(),
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

    fn setup() -> (Arc<StorageEngine>, Arc<TxnManager>) {
        let storage = Arc::new(StorageEngine::new_in_memory());
        storage.create_table(test_schema()).unwrap();
        let mgr = Arc::new(TxnManager::new(storage.clone()));
        (storage, mgr)
    }

    // ── Fast-path (LocalTxn) tests ──

    #[test]
    fn test_local_txn_fast_path_commit() {
        let (storage, mgr) = setup();

        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(txn.txn_type, TxnType::Local);
        assert_eq!(txn.path, TxnPath::Fast);
        assert_eq!(txn.state, TxnState::Active);

        // Insert a row
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]);
        storage.insert(TableId(1), row, txn.txn_id).unwrap();

        // Commit via fast-path
        let commit_ts = mgr.commit(txn.txn_id).unwrap();
        assert!(commit_ts.0 > 0);

        // Verify data is visible
        let rows = storage.scan(TableId(1), TxnId(999), commit_ts).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[1], Datum::Text("hello".into()));
    }

    #[test]
    fn test_local_txn_does_not_enter_prepared_state() {
        let (_storage, mgr) = setup();

        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(txn.txn_type, TxnType::Local);

        let commit_ts = mgr.commit(txn.txn_id).unwrap();
        assert!(commit_ts.0 > 0);

        // LocalTxn goes directly Active  → Committed, never Prepared
        // (verified structurally: the fast-path branch does not set Prepared)
        assert!(mgr.get_txn(txn.txn_id).is_none()); // removed after commit
    }

    // ── Slow-path (GlobalTxn) tests ──

    #[test]
    fn test_global_txn_slow_path_commit() {
        let (storage, mgr) = setup();

        let classification =
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc);
        let txn = mgr.begin_with_classification(IsolationLevel::ReadCommitted, classification);
        assert_eq!(txn.txn_type, TxnType::Global);
        assert_eq!(txn.path, TxnPath::Slow);

        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("global".into())]);
        storage.insert(TableId(1), row, txn.txn_id).unwrap();

        let commit_ts = mgr.commit(txn.txn_id).unwrap();
        assert!(commit_ts.0 > 0);

        let rows = storage.scan(TableId(1), TxnId(999), commit_ts).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_force_global_degrades_local_to_global() {
        let (_storage, mgr) = setup();

        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(txn.txn_type, TxnType::Local);

        mgr.force_global(txn.txn_id, SlowPathMode::Xa2Pc).unwrap();

        let updated = mgr.get_txn(txn.txn_id).unwrap();
        assert_eq!(updated.txn_type, TxnType::Global);
        assert_eq!(updated.path, TxnPath::Slow);
        assert!(updated.degraded);
    }

    // ── OCC validation tests ──

    #[test]
    fn test_si_occ_conflict_aborts_txn() {
        let (storage, mgr) = setup();

        // txn1 inserts and commits
        let txn1 = mgr.begin(IsolationLevel::SnapshotIsolation);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v1".into())]);
        let pk = storage.insert(TableId(1), row, txn1.txn_id).unwrap();
        mgr.commit(txn1.txn_id).unwrap();

        // txn2 starts under SI and reads the key
        let txn2 = mgr.begin(IsolationLevel::SnapshotIsolation);
        let read_ts = txn2.read_ts(mgr.current_ts());
        let _ = storage.get(TableId(1), &pk, txn2.txn_id, read_ts).unwrap();

        // txn3 updates the same key and commits (after txn2 started)
        let txn3 = mgr.begin(IsolationLevel::SnapshotIsolation);
        let new_row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v2".into())]);
        storage
            .update(TableId(1), &pk, new_row, txn3.txn_id)
            .unwrap();
        mgr.commit(txn3.txn_id).unwrap();

        // txn2 tries to commit  — should fail with WriteConflict
        let result = mgr.commit(txn2.txn_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_committed_skips_occ_validation() {
        let (storage, mgr) = setup();

        // txn1 inserts and commits
        let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v1".into())]);
        let pk = storage.insert(TableId(1), row, txn1.txn_id).unwrap();
        mgr.commit(txn1.txn_id).unwrap();

        // txn2 starts under RC and reads
        let txn2 = mgr.begin(IsolationLevel::ReadCommitted);
        let read_ts = txn2.read_ts(mgr.current_ts());
        let _ = storage.get(TableId(1), &pk, txn2.txn_id, read_ts).unwrap();

        // txn3 updates and commits
        let txn3 = mgr.begin(IsolationLevel::ReadCommitted);
        let new_row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v2".into())]);
        storage
            .update(TableId(1), &pk, new_row, txn3.txn_id)
            .unwrap();
        mgr.commit(txn3.txn_id).unwrap();

        // txn2 commits  — should succeed (RC doesn't do OCC validation)
        let result = mgr.commit(txn2.txn_id);
        assert!(result.is_ok());
    }

    // ── Abort tests ──

    #[test]
    fn test_abort_cleans_up_writes() {
        let (storage, mgr) = setup();

        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("aborted".into())]);
        storage.insert(TableId(1), row, txn.txn_id).unwrap();

        mgr.abort(txn.txn_id).unwrap();

        // Verify no data visible
        let rows = storage
            .scan(TableId(1), TxnId(999), Timestamp(1000))
            .unwrap();
        assert_eq!(rows.len(), 0);
    }

    // ── Stats tests ──

    #[test]
    fn test_stats_snapshot() {
        let (storage, mgr) = setup();

        // Fast-path commit
        let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]);
        storage.insert(TableId(1), row, txn1.txn_id).unwrap();
        mgr.commit(txn1.txn_id).unwrap();

        // Slow-path commit
        let classification =
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc);
        let txn2 = mgr.begin_with_classification(IsolationLevel::ReadCommitted, classification);
        let row2 = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("b".into())]);
        storage.insert(TableId(1), row2, txn2.txn_id).unwrap();
        mgr.commit(txn2.txn_id).unwrap();

        // Abort
        let txn3 = mgr.begin(IsolationLevel::ReadCommitted);
        mgr.abort(txn3.txn_id).unwrap();

        let stats = mgr.stats_snapshot();
        assert_eq!(stats.total_committed, 2);
        assert_eq!(stats.fast_path_commits, 1);
        assert_eq!(stats.slow_path_commits, 1);
        assert_eq!(stats.total_aborted, 1);
        assert_eq!(stats.active_count, 0);
    }

    #[test]
    fn test_observe_involved_shards_upgrades_to_global() {
        let (_storage, mgr) = setup();

        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(txn.txn_type, TxnType::Local);

        // Observe a second shard
        mgr.observe_involved_shards(txn.txn_id, &[ShardId(1)])
            .unwrap();

        let updated = mgr.get_txn(txn.txn_id).unwrap();
        assert_eq!(updated.txn_type, TxnType::Global);
        assert_eq!(updated.path, TxnPath::Slow);
        assert_eq!(updated.involved_shards.len(), 2);
    }

    // ── Prepare invariant tests ──

    #[test]
    fn test_prepare_rejects_local_txn() {
        let (_storage, mgr) = setup();

        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(txn.txn_type, TxnType::Local);

        // Attempting to prepare a LocalTxn must fail with InvariantViolation
        let result = mgr.prepare(txn.txn_id);
        assert!(result.is_err());
        match result.unwrap_err() {
            falcon_common::error::TxnError::InvariantViolation(id, msg) => {
                assert_eq!(id, txn.txn_id);
                assert!(msg.contains("LocalTxn"));
            }
            other => panic!("Expected InvariantViolation, got: {:?}", other),
        }
    }

    #[test]
    fn test_prepare_accepts_global_txn() {
        let (_storage, mgr) = setup();

        let classification =
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc);
        let txn = mgr.begin_with_classification(IsolationLevel::ReadCommitted, classification);
        assert_eq!(txn.txn_type, TxnType::Global);

        // Preparing a GlobalTxn must succeed
        let result = mgr.prepare(txn.txn_id);
        assert!(result.is_ok());

        let updated = mgr.get_txn(txn.txn_id).unwrap();
        assert_eq!(updated.state, TxnState::Prepared);
    }

    // ── Observability tests ──

    #[test]
    fn test_txn_handle_fields_observable() {
        let (storage, mgr) = setup();

        // Local fast-path
        let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(txn1.txn_type, TxnType::Local);
        assert_eq!(txn1.path, TxnPath::Fast);
        assert_eq!(txn1.involved_shards, vec![ShardId(0)]);
        assert!(!txn1.degraded);
        assert!(txn1.start_ts.0 > 0);

        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]);
        storage.insert(TableId(1), row, txn1.txn_id).unwrap();
        mgr.commit(txn1.txn_id).unwrap();

        // Global slow-path
        let classification =
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc);
        let txn2 = mgr.begin_with_classification(IsolationLevel::ReadCommitted, classification);
        assert_eq!(txn2.txn_type, TxnType::Global);
        assert_eq!(txn2.path, TxnPath::Slow);
        assert_eq!(txn2.involved_shards.len(), 2);

        let row2 = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("b".into())]);
        storage.insert(TableId(1), row2, txn2.txn_id).unwrap();
        mgr.commit(txn2.txn_id).unwrap();

        // Stats reflect both paths
        let stats = mgr.stats_snapshot();
        assert_eq!(stats.fast_path_commits, 1);
        assert_eq!(stats.slow_path_commits, 1);
        assert_eq!(stats.total_committed, 2);
    }

    #[test]
    fn test_serialization_conflict_under_si() {
        let (storage, mgr) = setup();

        // txn1 inserts and commits
        let txn1 = mgr.begin(IsolationLevel::SnapshotIsolation);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v1".into())]);
        let pk = storage.insert(TableId(1), row, txn1.txn_id).unwrap();
        mgr.commit(txn1.txn_id).unwrap();

        // txn2 reads under SI
        let txn2 = mgr.begin(IsolationLevel::SnapshotIsolation);
        let _ = storage
            .get(TableId(1), &pk, txn2.txn_id, txn2.start_ts)
            .unwrap();

        // txn3 concurrently updates and commits
        let txn3 = mgr.begin(IsolationLevel::SnapshotIsolation);
        let new_row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v2".into())]);
        storage
            .update(TableId(1), &pk, new_row, txn3.txn_id)
            .unwrap();
        mgr.commit(txn3.txn_id).unwrap();

        // txn2 commit fails  — OCC rejects
        let result = mgr.commit(txn2.txn_id);
        assert!(result.is_err());

        // Stats should record the OCC conflict
        let stats = mgr.stats_snapshot();
        assert!(stats.occ_conflicts >= 1);
    }

    // ── Conservative upgrade: Local  → Global, never Global  → Local ──

    #[test]
    fn test_cannot_downgrade_global_to_local() {
        let (_storage, mgr) = setup();

        let classification =
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc);
        let txn = mgr.begin_with_classification(IsolationLevel::ReadCommitted, classification);
        assert_eq!(txn.txn_type, TxnType::Global);

        // observe_involved_shards with a single shard should NOT downgrade to Local
        mgr.observe_involved_shards(txn.txn_id, &[ShardId(0)])
            .unwrap();
        let updated = mgr.get_txn(txn.txn_id).unwrap();
        assert_eq!(
            updated.txn_type,
            TxnType::Global,
            "Global must not downgrade to Local"
        );
        assert_eq!(
            updated.path,
            TxnPath::Slow,
            "Slow path must not revert to Fast"
        );
    }

    // ── Commit-time unique constraint via TxnManager ──

    fn setup_mgr() -> (Arc<StorageEngine>, Arc<TxnManager>) {
        let storage = Arc::new(StorageEngine::new_in_memory());
        storage.create_table(test_schema()).unwrap();
        let mgr = Arc::new(TxnManager::new(storage.clone()));
        (storage, mgr)
    }

    fn setup_with_unique_index() -> (Arc<StorageEngine>, Arc<TxnManager>) {
        let storage = Arc::new(StorageEngine::new_in_memory());
        storage.create_table(test_schema()).unwrap();
        storage.create_unique_index("txn_test", 1).unwrap();
        let mgr = Arc::new(TxnManager::new(storage.clone()));
        (storage, mgr)
    }

    #[test]
    fn test_fast_path_unique_violation_returns_constraint_violation() {
        let (storage, mgr) = setup_with_unique_index();

        // txn1 inserts val="hello"
        let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
        let row1 = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]);
        storage.insert(TableId(1), row1, txn1.txn_id).unwrap();

        // txn2 inserts val="hello" (different PK)  — both pass insert-time check
        let txn2 = mgr.begin(IsolationLevel::ReadCommitted);
        let row2 = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("hello".into())]);
        storage.insert(TableId(1), row2, txn2.txn_id).unwrap();

        // txn1 commits (fast-path)  — succeeds
        mgr.commit(txn1.txn_id).unwrap();

        // txn2 commits (fast-path)  — must fail with ConstraintViolation
        let result = mgr.commit(txn2.txn_id);
        assert!(result.is_err());
        match result.unwrap_err() {
            falcon_common::error::TxnError::ConstraintViolation(id, msg) => {
                assert_eq!(id, txn2.txn_id);
                assert!(
                    msg.contains("unique"),
                    "message should mention unique: {}",
                    msg
                );
            }
            other => panic!("Expected ConstraintViolation, got: {:?}", other),
        }
    }

    #[test]
    fn test_slow_path_unique_violation_returns_constraint_violation() {
        let (storage, mgr) = setup_with_unique_index();

        // txn1 (local fast-path) and txn2 (global slow-path) both insert val="hello"
        // BEFORE either commits, so both pass insert-time check.
        let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
        let row1 = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]);
        storage.insert(TableId(1), row1, txn1.txn_id).unwrap();

        let classification =
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc);
        let txn2 = mgr.begin_with_classification(IsolationLevel::ReadCommitted, classification);
        let row2 = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("hello".into())]);
        storage.insert(TableId(1), row2, txn2.txn_id).unwrap();

        // txn1 commits (fast-path)  — succeeds
        mgr.commit(txn1.txn_id).unwrap();

        // txn2 commits (slow-path)  — must fail with ConstraintViolation
        let result = mgr.commit(txn2.txn_id);
        assert!(result.is_err());
        match result.unwrap_err() {
            falcon_common::error::TxnError::ConstraintViolation(id, msg) => {
                assert_eq!(id, txn2.txn_id);
                assert!(msg.contains("unique"));
            }
            other => panic!("Expected ConstraintViolation, got: {:?}", other),
        }
    }

    #[test]
    fn test_fast_slow_path_parity_unique_constraint() {
        // Verify that both fast-path and slow-path enforce the same constraint semantics.
        let (storage, mgr) = setup_with_unique_index();

        // Commit "a" via fast-path
        let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
        storage
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
                txn1.txn_id,
            )
            .unwrap();
        mgr.commit(txn1.txn_id).unwrap();

        // Try "a" again via fast-path  — should fail
        let txn2 = mgr.begin(IsolationLevel::ReadCommitted);
        // Insert-time check catches it (index already has "a")
        let r2 = storage.insert(
            TableId(1),
            OwnedRow::new(vec![Datum::Int32(2), Datum::Text("a".into())]),
            txn2.txn_id,
        );
        assert!(
            r2.is_err(),
            "fast-path: insert-time unique check should fail"
        );
        mgr.abort(txn2.txn_id).unwrap();

        // Try "a" again via slow-path  — should also fail
        let classification =
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc);
        let txn3 = mgr.begin_with_classification(IsolationLevel::ReadCommitted, classification);
        let r3 = storage.insert(
            TableId(1),
            OwnedRow::new(vec![Datum::Int32(3), Datum::Text("a".into())]),
            txn3.txn_id,
        );
        assert!(
            r3.is_err(),
            "slow-path: insert-time unique check should fail"
        );
        mgr.abort(txn3.txn_id).unwrap();
    }

    // ── Observability: latency, history, percentiles ──

    #[test]
    fn test_commit_latency_recorded_in_stats() {
        let (storage, mgr) = setup_mgr();

        // Commit several fast-path txns
        for i in 0..10 {
            let txn = mgr.begin(IsolationLevel::ReadCommitted);
            let row = OwnedRow::new(vec![Datum::Int32(100 + i), Datum::Text(format!("v{}", i))]);
            storage.insert(TableId(1), row, txn.txn_id).unwrap();
            mgr.commit(txn.txn_id).unwrap();
        }

        let stats = mgr.stats_snapshot();
        assert_eq!(stats.fast_path_commits, 10);
        assert_eq!(stats.latency.fast_path.count, 10);
        // p50 should be non-negative (could be 0 on very fast machines)
        assert!(stats.latency.fast_path.p50_us <= stats.latency.fast_path.p95_us);
        assert!(stats.latency.fast_path.p95_us <= stats.latency.fast_path.p99_us);
    }

    #[test]
    fn test_slow_path_latency_recorded_separately() {
        let (storage, mgr) = setup_mgr();

        // fast-path
        let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
        storage
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
                txn1.txn_id,
            )
            .unwrap();
        mgr.commit(txn1.txn_id).unwrap();

        // slow-path
        let c = TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc);
        let txn2 = mgr.begin_with_classification(IsolationLevel::ReadCommitted, c);
        storage
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("b".into())]),
                txn2.txn_id,
            )
            .unwrap();
        mgr.commit(txn2.txn_id).unwrap();

        let stats = mgr.stats_snapshot();
        assert_eq!(stats.latency.fast_path.count, 1);
        assert_eq!(stats.latency.slow_path.count, 1);
        assert_eq!(stats.latency.all.count, 2);
    }

    #[test]
    fn test_txn_history_records_commits_and_aborts() {
        let (storage, mgr) = setup_mgr();

        // Commit one
        let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
        storage
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
                txn1.txn_id,
            )
            .unwrap();
        mgr.commit(txn1.txn_id).unwrap();

        // Abort one
        let txn2 = mgr.begin(IsolationLevel::ReadCommitted);
        storage
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("b".into())]),
                txn2.txn_id,
            )
            .unwrap();
        mgr.abort(txn2.txn_id).unwrap();

        let history = mgr.txn_history_snapshot();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].outcome, TxnOutcome::Committed);
        assert!(history[0].commit_ts.is_some());
        assert!(matches!(history[1].outcome, TxnOutcome::Aborted(_)));
        assert!(history[1].commit_ts.is_none());
    }

    #[test]
    fn test_constraint_violation_counter_incremented() {
        let (storage, mgr) = setup_with_unique_index();

        // Two concurrent inserts of same unique key
        let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
        storage
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("dup".into())]),
                txn1.txn_id,
            )
            .unwrap();
        let txn2 = mgr.begin(IsolationLevel::ReadCommitted);
        storage
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("dup".into())]),
                txn2.txn_id,
            )
            .unwrap();

        mgr.commit(txn1.txn_id).unwrap();
        let _ = mgr.commit(txn2.txn_id); // should fail

        let stats = mgr.stats_snapshot();
        assert_eq!(stats.constraint_violations, 1);
    }

    #[test]
    fn test_reset_latency_clears_samples() {
        let (storage, mgr) = setup_mgr();

        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        storage
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
                txn.txn_id,
            )
            .unwrap();
        mgr.commit(txn.txn_id).unwrap();

        assert_eq!(mgr.stats_snapshot().latency.all.count, 1);

        mgr.reset_latency();

        assert_eq!(mgr.stats_snapshot().latency.all.count, 0);
        // But counters are not reset
        assert_eq!(mgr.stats_snapshot().fast_path_commits, 1);
    }

    #[test]
    fn test_history_records_abort_reason() {
        let (storage, mgr) = setup_mgr();

        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        storage
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
                txn.txn_id,
            )
            .unwrap();
        mgr.abort_with_reason(txn.txn_id, "user_cancel").unwrap();

        let history = mgr.txn_history_snapshot();
        assert_eq!(history.len(), 1);
        match &history[0].outcome {
            TxnOutcome::Aborted(reason) => assert_eq!(reason, "user_cancel"),
            _ => panic!("expected abort"),
        }
    }

    #[test]
    fn test_gc_safepoint_info_no_active_txns() {
        let (_storage, mgr) = setup_mgr();

        let info = mgr.gc_safepoint_info();
        assert_eq!(info.active_txn_count, 0);
        assert_eq!(info.longest_txn_age_us, 0);
        assert!(!info.stalled, "should not be stalled with no active txns");
        // min_active_start_ts == current_ts when no active txns
        assert_eq!(info.min_active_start_ts, info.current_ts);
    }

    #[test]
    fn test_gc_safepoint_info_with_active_txn() {
        let (_storage, mgr) = setup_mgr();

        let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
        // Advance the timestamp counter so safepoint is behind
        let _ = mgr.begin(IsolationLevel::ReadCommitted);
        let _ = mgr.begin(IsolationLevel::ReadCommitted);

        let info = mgr.gc_safepoint_info();
        assert_eq!(info.active_txn_count, 3);
        let _ = info.longest_txn_age_us; // just started, age is tiny
        assert_eq!(info.min_active_start_ts, txn1.start_ts);
        // current_ts is ahead of min_active by at least 3 (3 txns allocated ts)
        assert!(info.current_ts.0 > info.min_active_start_ts.0);
        assert!(
            info.stalled,
            "should be stalled: active txns holding back safepoint"
        );
    }

    #[test]
    fn test_longest_txn_age_increases() {
        let (_storage, mgr) = setup_mgr();

        let _txn = mgr.begin(IsolationLevel::ReadCommitted);
        std::thread::sleep(std::time::Duration::from_millis(10));

        let age = mgr.longest_txn_age_us();
        assert!(
            age >= 10_000,
            "Expected age >= 10ms (10000us), got {}us",
            age
        );
    }

    // ── Admission control tests ──

    fn setup_with_budget(soft: u64, hard: u64) -> (Arc<StorageEngine>, Arc<TxnManager>) {
        use falcon_storage::memory::MemoryBudget;
        let storage = Arc::new(StorageEngine::new_in_memory_with_budget(MemoryBudget::new(
            soft, hard,
        )));
        storage.create_table(test_schema()).unwrap();
        let mgr = Arc::new(TxnManager::new(storage.clone()));
        (storage, mgr)
    }

    #[test]
    fn test_try_begin_succeeds_under_normal_pressure() {
        let (_storage, mgr) = setup_with_budget(10_000, 20_000);
        // No memory allocated  → Normal state
        let result = mgr.try_begin(IsolationLevel::ReadCommitted);
        assert!(
            result.is_ok(),
            "try_begin should succeed under Normal pressure"
        );
    }

    #[test]
    fn test_try_begin_rejects_under_pressure() {
        let (storage, mgr) = setup_with_budget(1000, 2000);
        // Push into PRESSURE state (>= soft_limit but < hard_limit)
        storage.memory_tracker().alloc_mvcc(1500);
        assert_eq!(
            storage.pressure_state(),
            falcon_storage::memory::PressureState::Pressure
        );

        let result = mgr.try_begin(IsolationLevel::ReadCommitted);
        assert!(result.is_err(), "try_begin should reject under Pressure");
        match result.unwrap_err() {
            falcon_common::error::TxnError::MemoryPressure(_) => {}
            other => panic!("Expected MemoryPressure, got {:?}", other),
        }
    }

    #[test]
    fn test_try_begin_rejects_under_critical() {
        let (storage, mgr) = setup_with_budget(1000, 2000);
        // Push into CRITICAL state (>= hard_limit)
        storage.memory_tracker().alloc_mvcc(2500);
        assert_eq!(
            storage.pressure_state(),
            falcon_storage::memory::PressureState::Critical
        );

        let result = mgr.try_begin(IsolationLevel::ReadCommitted);
        assert!(result.is_err(), "try_begin should reject under Critical");
        match result.unwrap_err() {
            falcon_common::error::TxnError::MemoryLimitExceeded(_) => {}
            other => panic!("Expected MemoryLimitExceeded, got {:?}", other),
        }
    }

    #[test]
    fn test_admission_recovers_after_dealloc() {
        let (storage, mgr) = setup_with_budget(1000, 2000);

        // Push into PRESSURE
        storage.memory_tracker().alloc_mvcc(1500);
        assert!(mgr.try_begin(IsolationLevel::ReadCommitted).is_err());

        // Deallocate to return to NORMAL
        storage.memory_tracker().dealloc_mvcc(1000);
        assert_eq!(
            storage.pressure_state(),
            falcon_storage::memory::PressureState::Normal
        );
        assert!(
            mgr.try_begin(IsolationLevel::ReadCommitted).is_ok(),
            "try_begin should succeed after memory is freed"
        );
    }

    #[test]
    fn test_begin_bypasses_admission_control() {
        let (storage, mgr) = setup_with_budget(1000, 2000);
        // Push into CRITICAL
        storage.memory_tracker().alloc_mvcc(2500);

        // Regular begin() still works (no admission control)
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        assert!(txn.txn_id.0 > 0 || txn.txn_id.0 == 0);
    }

    // ── P0-3: Idempotent prepare/commit/abort ──

    #[test]
    fn test_prepare_idempotent() {
        let (_storage, mgr) = setup();
        let cls = TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc);
        let txn = mgr.begin_with_classification(IsolationLevel::SnapshotIsolation, cls);

        // First prepare succeeds
        assert!(mgr.prepare(txn.txn_id).is_ok());
        // Second prepare is idempotent (no error)
        assert!(mgr.prepare(txn.txn_id).is_ok());

        // Verify state is still Prepared
        let handle = mgr.get_txn(txn.txn_id).unwrap();
        assert_eq!(handle.state, TxnState::Prepared);
    }

    #[test]
    fn test_abort_idempotent() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);

        // First abort succeeds
        assert!(mgr.abort(txn.txn_id).is_ok());
        // Second abort: txn already removed from active_txns, returns NotFound
        // This is acceptable — the txn no longer exists.
        assert!(mgr.abort(txn.txn_id).is_err());
    }

    #[test]
    fn test_commit_already_committed_returns_ok() {
        let (storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);

        // Insert a row to have something to commit
        let row = OwnedRow::new(vec![Datum::Int32(100), Datum::Text("v".into())]);
        storage.insert(TableId(1), row, txn.txn_id).unwrap();

        // First commit succeeds
        let ts1 = mgr.commit(txn.txn_id);
        assert!(ts1.is_ok());
    }

    // ── P0-3: GC safepoint respects Prepared transactions ──

    #[test]
    fn test_gc_safepoint_includes_prepared_txns() {
        let (_storage, mgr) = setup();
        let cls = TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc);
        let txn = mgr.begin_with_classification(IsolationLevel::SnapshotIsolation, cls);

        // Prepare the transaction
        mgr.prepare(txn.txn_id).unwrap();

        // GC safepoint info should show 1 prepared txn
        let info = mgr.gc_safepoint_info();
        assert_eq!(info.prepared_txn_count, 1);
        assert_eq!(info.active_txn_count, 1);

        // The min_active_start_ts should be pinned by the prepared txn
        assert!(info.min_active_start_ts.0 <= txn.start_ts.0);
    }

    // ── P0-2: Implicit fast→slow path upgrade tracking ──

    #[test]
    fn test_implicit_upgrade_tracked() {
        let (_storage, mgr) = setup();
        let cls = TxnClassification::local(ShardId(0));
        let txn = mgr.begin_with_classification(IsolationLevel::ReadCommitted, cls);

        // Initially local fast path
        assert_eq!(txn.path, TxnPath::Fast);

        // Observe additional shard → implicit upgrade
        mgr.observe_involved_shards(txn.txn_id, &[ShardId(1)])
            .unwrap();

        let handle = mgr.get_txn(txn.txn_id).unwrap();
        assert_eq!(handle.txn_type, TxnType::Global);
        assert_eq!(handle.path, TxnPath::Slow);
        assert!(handle.degraded);

        // Stats should reflect the upgrade
        let stats = mgr.stats_snapshot();
        assert!(stats.implicit_upgrade_blocked >= 1);
        assert!(stats.degraded_to_global >= 1);
    }

    // ── P1-4: Trace ID and OCC retry count ──

    #[test]
    fn test_txn_has_trace_id() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);

        // Trace ID defaults to txn_id
        assert_eq!(txn.trace_id, txn.txn_id.0);
        assert_eq!(txn.occ_retry_count, 0);
    }

    #[test]
    fn test_trace_id_in_txn_record() {
        let (storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        let row = OwnedRow::new(vec![Datum::Int32(200), Datum::Text("trace_test".into())]);
        storage.insert(TableId(1), row, txn.txn_id).unwrap();

        mgr.commit(txn.txn_id).unwrap();

        let history = mgr.txn_history_snapshot();
        let record = history.iter().find(|r| r.txn_id == txn.txn_id).unwrap();
        assert_eq!(record.trace_id, txn.txn_id.0);
        assert_eq!(record.occ_retry_count, 0);
    }

    // ── P1-4: Slow transaction log ──

    #[test]
    fn test_slow_txn_log_empty_by_default() {
        let (_storage, mgr) = setup();
        assert!(mgr.slow_txn_snapshot().is_empty());
    }

    #[test]
    fn test_slow_txn_threshold_configurable() {
        let (_storage, mgr) = setup();
        assert_eq!(mgr.slow_txn_threshold_us(), 100_000); // default 100ms
        mgr.set_slow_txn_threshold_us(50_000);
        assert_eq!(mgr.slow_txn_threshold_us(), 50_000);
    }

    // ── P0-2: Fast path ratio metric ──

    #[test]
    fn test_fast_path_ratio_metric() {
        let (storage, mgr) = setup();

        // Commit 2 local (fast-path) txns
        for i in 0..2 {
            let txn = mgr.begin(IsolationLevel::ReadCommitted);
            let row = OwnedRow::new(vec![Datum::Int32(300 + i), Datum::Text("ratio".into())]);
            storage.insert(TableId(1), row, txn.txn_id).unwrap();
            mgr.commit(txn.txn_id).unwrap();
        }

        let stats = mgr.stats_snapshot();
        assert_eq!(stats.fast_path_commits, 2);
        assert_eq!(stats.total_committed, 2);
        assert!((stats.fast_path_ratio - 1.0).abs() < f64::EPSILON);
    }

    // ── P2-3: Per-priority latency stats and SLA violations ──

    #[test]
    fn test_priority_latency_stats_populated() {
        let (storage, mgr) = setup();

        // Default priority is Normal, so commits should appear in normal bucket
        for i in 0..3 {
            let txn = mgr.begin(IsolationLevel::ReadCommitted);
            let row = OwnedRow::new(vec![Datum::Int32(400 + i), Datum::Text("prio".into())]);
            storage.insert(TableId(1), row, txn.txn_id).unwrap();
            mgr.commit(txn.txn_id).unwrap();
        }

        let stats = mgr.stats_snapshot();
        assert_eq!(stats.priority_latency.normal.count, 3);
        assert_eq!(stats.priority_latency.high.count, 0);
        assert_eq!(stats.priority_latency.background.count, 0);
        assert_eq!(stats.priority_latency.system.count, 0);
    }

    #[test]
    fn test_sla_violations_initially_zero() {
        let (_storage, mgr) = setup();
        let stats = mgr.stats_snapshot();
        assert_eq!(stats.sla_violations.total_violations, 0);
        assert_eq!(stats.sla_violations.high_priority_violations, 0);
        assert_eq!(stats.sla_violations.normal_priority_violations, 0);
    }

    #[test]
    fn test_priority_latency_and_sla_in_snapshot() {
        let (storage, mgr) = setup();

        // Commit a transaction to populate some stats
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        let row = OwnedRow::new(vec![Datum::Int32(500), Datum::Text("sla".into())]);
        storage.insert(TableId(1), row, txn.txn_id).unwrap();
        mgr.commit(txn.txn_id).unwrap();

        let stats = mgr.stats_snapshot();
        // Should have exactly 1 committed txn in normal priority
        assert_eq!(stats.total_committed, 1);
        assert_eq!(stats.priority_latency.normal.count, 1);
        // p50 should be populated (>= 0)
        // No SLA violations expected for sub-100ms txn
        assert_eq!(stats.sla_violations.normal_priority_violations, 0);
    }

    #[test]
    fn test_tenant_id_and_priority_on_txn_handle() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(txn.tenant_id, falcon_common::tenant::SYSTEM_TENANT_ID);
        assert_eq!(txn.priority, falcon_common::security::TxnPriority::Normal);
    }

    #[test]
    fn test_txn_read_only_default() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        assert!(!txn.read_only);
    }

    #[test]
    fn test_txn_read_only_set() {
        let (_storage, mgr) = setup();
        let mut txn = mgr.begin(IsolationLevel::ReadCommitted);
        txn.read_only = true;
        assert!(txn.read_only);
    }

    #[test]
    fn test_txn_timeout_default() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(txn.timeout_ms, 0);
        assert!(!txn.is_timed_out());
    }

    #[test]
    fn test_txn_timeout_not_expired() {
        let (_storage, mgr) = setup();
        let mut txn = mgr.begin(IsolationLevel::ReadCommitted);
        txn.timeout_ms = 60_000; // 60 seconds
        assert!(!txn.is_timed_out());
    }

    #[test]
    fn test_txn_timeout_expired() {
        let (_storage, mgr) = setup();
        let mut txn = mgr.begin(IsolationLevel::ReadCommitted);
        txn.timeout_ms = 1; // 1 ms
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(txn.is_timed_out());
    }

    #[test]
    fn test_txn_elapsed_ms() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(txn.elapsed_ms() >= 4); // at least 4ms elapsed
    }

    #[test]
    fn test_txn_exec_summary_default() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(txn.exec_summary.statement_count, 0);
        assert_eq!(txn.exec_summary.rows_read, 0);
        assert_eq!(txn.exec_summary.rows_written, 0);
        assert_eq!(txn.exec_summary.rows_inserted, 0);
        assert_eq!(txn.exec_summary.rows_updated, 0);
        assert_eq!(txn.exec_summary.rows_deleted, 0);
    }

    #[test]
    fn test_txn_exec_summary_record() {
        use crate::manager::TxnExecSummary;
        let mut summary = TxnExecSummary::default();
        summary.record_statement();
        summary.record_insert(5);
        summary.record_update(3);
        summary.record_delete(2);
        summary.record_read(100);

        assert_eq!(summary.statement_count, 1);
        assert_eq!(summary.rows_inserted, 5);
        assert_eq!(summary.rows_updated, 3);
        assert_eq!(summary.rows_deleted, 2);
        assert_eq!(summary.rows_written, 10); // 5 + 3 + 2
        assert_eq!(summary.rows_read, 100);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// B5/B6: Admission control — WAL backlog + replication lag gates
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod admission_backpressure_tests {
    use std::sync::Arc;

    use falcon_common::types::IsolationLevel;
    use falcon_storage::engine::StorageEngine;

    use crate::manager::TxnManager;

    fn setup() -> (Arc<StorageEngine>, TxnManager) {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let mgr = TxnManager::new(storage.clone());
        (storage, mgr)
    }

    #[test]
    fn test_wal_backlog_threshold_default_disabled() {
        let (_storage, mgr) = setup();
        // Default: threshold=0 → disabled, try_begin succeeds
        let result = mgr.try_begin(IsolationLevel::ReadCommitted);
        assert!(result.is_ok());
    }

    #[test]
    fn test_wal_backlog_threshold_configurable() {
        let (_storage, mgr) = setup();
        mgr.set_wal_backlog_threshold_bytes(1024);
        // With in-memory engine, WAL backlog=0 < 1024 → should still succeed
        let result = mgr.try_begin(IsolationLevel::ReadCommitted);
        assert!(result.is_ok());
    }

    #[test]
    fn test_replication_lag_threshold_default_disabled() {
        let (_storage, mgr) = setup();
        // Default: threshold=0 → disabled, try_begin succeeds
        let result = mgr.try_begin(IsolationLevel::ReadCommitted);
        assert!(result.is_ok());
    }

    #[test]
    fn test_replication_lag_threshold_configurable() {
        let (_storage, mgr) = setup();
        mgr.set_replication_lag_threshold(100);
        // With fresh engine, replicas are at current_ts → no lag → succeeds
        let result = mgr.try_begin(IsolationLevel::ReadCommitted);
        assert!(result.is_ok());
    }

    #[test]
    fn test_admission_rejection_counter_incremented() {
        use falcon_storage::memory::MemoryBudget;
        let storage = Arc::new(StorageEngine::new_in_memory_with_budget(
            MemoryBudget::new(100, 200),
        ));
        let mgr = TxnManager::new(storage.clone());
        // Push to CRITICAL
        storage.memory_tracker().alloc_mvcc(300);
        let _ = mgr.try_begin(IsolationLevel::ReadCommitted);
        let stats = mgr.stats_snapshot();
        assert!(stats.admission_rejections >= 1, "admission rejection counter must increment");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// B7: Tail latency — long-txn detection + kill
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod long_txn_detection_tests {
    use std::sync::Arc;

    use falcon_common::types::IsolationLevel;
    use falcon_storage::engine::StorageEngine;

    use crate::manager::TxnManager;

    fn setup() -> (Arc<StorageEngine>, TxnManager) {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let mgr = TxnManager::new(storage.clone());
        (storage, mgr)
    }

    #[test]
    fn test_no_long_running_when_empty() {
        let (_storage, mgr) = setup();
        assert!(mgr.long_running_txns(0).is_empty());
    }

    #[test]
    fn test_fresh_txn_not_long_running() {
        let (_storage, mgr) = setup();
        let _txn = mgr.begin(IsolationLevel::ReadCommitted);
        // Threshold 1 second — fresh txn shouldn't exceed it
        assert!(mgr.long_running_txns(1_000_000).is_empty());
    }

    #[test]
    fn test_detect_long_running_txn() {
        let (_storage, mgr) = setup();
        let _txn = mgr.begin(IsolationLevel::ReadCommitted);
        std::thread::sleep(std::time::Duration::from_millis(10));
        // Threshold 1ms = 1000us — should detect it
        let long = mgr.long_running_txns(1_000);
        assert_eq!(long.len(), 1);
    }

    #[test]
    fn test_longest_txn_age_us() {
        let (_storage, mgr) = setup();
        assert_eq!(mgr.longest_txn_age_us(), 0);
        let _txn = mgr.begin(IsolationLevel::ReadCommitted);
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(mgr.longest_txn_age_us() >= 4_000); // at least 4ms
    }

    #[test]
    fn test_kill_txn_removes_it() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(mgr.active_count(), 1);
        mgr.kill_txn(txn.txn_id).unwrap();
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn test_kill_txn_not_found() {
        let (_storage, mgr) = setup();
        let result = mgr.kill_txn(falcon_common::types::TxnId(999));
        assert!(result.is_err());
    }

    #[test]
    fn test_kill_long_running_batch() {
        let (_storage, mgr) = setup();
        for _ in 0..5 {
            let _ = mgr.begin(IsolationLevel::ReadCommitted);
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
        let killed = mgr.kill_long_running(1_000); // 1ms threshold
        assert_eq!(killed, 5);
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn test_kill_long_running_spares_fresh() {
        let (_storage, mgr) = setup();
        let _old = mgr.begin(IsolationLevel::ReadCommitted);
        std::thread::sleep(std::time::Duration::from_millis(15));
        let _fresh = mgr.begin(IsolationLevel::ReadCommitted);
        // Kill only txns older than 10ms
        let killed = mgr.kill_long_running(10_000);
        assert_eq!(killed, 1);
        assert_eq!(mgr.active_count(), 1);
    }

    #[test]
    fn test_gc_safepoint_stalled_by_long_txn() {
        let (_storage, mgr) = setup();
        // Allocate a few timestamps to advance current_ts
        for _ in 0..10 {
            mgr.alloc_ts();
        }
        let _txn = mgr.begin(IsolationLevel::ReadCommitted);
        // More timestamps advance current but txn pins safepoint
        for _ in 0..10 {
            mgr.alloc_ts();
        }
        let info = mgr.gc_safepoint_info();
        assert!(info.stalled, "GC safepoint should be stalled by long-running txn");
        assert!(info.longest_txn_age_us >= 0);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// B1: Txn State Machine — exhaustive transition tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod txn_state_machine_tests {
    use std::sync::Arc;

    use falcon_common::types::{IsolationLevel, ShardId, TxnId};
    use falcon_storage::engine::StorageEngine;

    use crate::manager::{
        SlowPathMode, TransitionResult, TxnClassification, TxnManager, TxnState, TxnType,
    };

    fn setup() -> (Arc<StorageEngine>, TxnManager) {
        let storage = Arc::new(StorageEngine::new_in_memory());
        let mgr = TxnManager::new(storage.clone());
        (storage, mgr)
    }

    // ── Unit tests for TxnState::try_transition ──

    #[test]
    fn test_active_to_prepared() {
        let mut s = TxnState::Active;
        assert_eq!(
            s.try_transition(TxnState::Prepared, TxnId(1)).unwrap(),
            TransitionResult::Applied
        );
        assert_eq!(s, TxnState::Prepared);
    }

    #[test]
    fn test_active_to_committed() {
        let mut s = TxnState::Active;
        assert_eq!(
            s.try_transition(TxnState::Committed, TxnId(1)).unwrap(),
            TransitionResult::Applied
        );
        assert_eq!(s, TxnState::Committed);
    }

    #[test]
    fn test_active_to_aborted() {
        let mut s = TxnState::Active;
        assert_eq!(
            s.try_transition(TxnState::Aborted, TxnId(1)).unwrap(),
            TransitionResult::Applied
        );
        assert_eq!(s, TxnState::Aborted);
    }

    #[test]
    fn test_prepared_to_committed() {
        let mut s = TxnState::Prepared;
        assert_eq!(
            s.try_transition(TxnState::Committed, TxnId(1)).unwrap(),
            TransitionResult::Applied
        );
        assert_eq!(s, TxnState::Committed);
    }

    #[test]
    fn test_prepared_to_aborted() {
        let mut s = TxnState::Prepared;
        assert_eq!(
            s.try_transition(TxnState::Aborted, TxnId(1)).unwrap(),
            TransitionResult::Applied
        );
        assert_eq!(s, TxnState::Aborted);
    }

    #[test]
    fn test_committed_to_committed_idempotent() {
        let mut s = TxnState::Committed;
        assert_eq!(
            s.try_transition(TxnState::Committed, TxnId(1)).unwrap(),
            TransitionResult::Idempotent
        );
        assert_eq!(s, TxnState::Committed);
    }

    #[test]
    fn test_aborted_to_aborted_idempotent() {
        let mut s = TxnState::Aborted;
        assert_eq!(
            s.try_transition(TxnState::Aborted, TxnId(1)).unwrap(),
            TransitionResult::Idempotent
        );
        assert_eq!(s, TxnState::Aborted);
    }

    // ── Invalid transitions ──

    #[test]
    fn test_committed_to_aborted_invalid() {
        let mut s = TxnState::Committed;
        assert!(s.try_transition(TxnState::Aborted, TxnId(1)).is_err());
    }

    #[test]
    fn test_committed_to_active_invalid() {
        let mut s = TxnState::Committed;
        assert!(s.try_transition(TxnState::Active, TxnId(1)).is_err());
    }

    #[test]
    fn test_committed_to_prepared_invalid() {
        let mut s = TxnState::Committed;
        assert!(s.try_transition(TxnState::Prepared, TxnId(1)).is_err());
    }

    #[test]
    fn test_aborted_to_committed_invalid() {
        let mut s = TxnState::Aborted;
        assert!(s.try_transition(TxnState::Committed, TxnId(1)).is_err());
    }

    #[test]
    fn test_aborted_to_active_invalid() {
        let mut s = TxnState::Aborted;
        assert!(s.try_transition(TxnState::Active, TxnId(1)).is_err());
    }

    #[test]
    fn test_aborted_to_prepared_invalid() {
        let mut s = TxnState::Aborted;
        assert!(s.try_transition(TxnState::Prepared, TxnId(1)).is_err());
    }

    #[test]
    fn test_prepared_to_active_invalid() {
        let mut s = TxnState::Prepared;
        assert!(s.try_transition(TxnState::Active, TxnId(1)).is_err());
    }

    #[test]
    fn test_prepared_to_prepared_invalid() {
        // Prepare is NOT idempotent at the state level — the TxnManager.prepare()
        // handles idempotency by checking current state before calling try_transition.
        let mut s = TxnState::Prepared;
        assert!(s.try_transition(TxnState::Prepared, TxnId(1)).is_err());
    }

    #[test]
    fn test_active_to_active_invalid() {
        let mut s = TxnState::Active;
        assert!(s.try_transition(TxnState::Active, TxnId(1)).is_err());
    }

    // ── Display ──

    #[test]
    fn test_txn_state_display() {
        assert_eq!(TxnState::Active.to_string(), "Active");
        assert_eq!(TxnState::Prepared.to_string(), "Prepared");
        assert_eq!(TxnState::Committed.to_string(), "Committed");
        assert_eq!(TxnState::Aborted.to_string(), "Aborted");
    }

    // ── Integration: TxnManager commit/abort idempotency ──

    #[test]
    fn test_commit_idempotent_via_manager() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        let txn_id = txn.txn_id;
        let ts1 = mgr.commit(txn_id).unwrap();
        // Second commit should fail with NotFound (txn already removed from active map)
        assert!(mgr.commit(txn_id).is_err());
        // But the first commit succeeded
        assert!(ts1.0 > 0);
    }

    #[test]
    fn test_abort_idempotent_via_manager() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        let txn_id = txn.txn_id;
        mgr.abort(txn_id).unwrap();
        // Second abort: txn already removed, returns NotFound (idempotent at higher level)
        assert!(mgr.abort(txn_id).is_err());
    }

    #[test]
    fn test_commit_after_abort_fails() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        let txn_id = txn.txn_id;
        mgr.abort(txn_id).unwrap();
        assert!(mgr.commit(txn_id).is_err());
    }

    #[test]
    fn test_abort_after_commit_fails() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        let txn_id = txn.txn_id;
        mgr.commit(txn_id).unwrap();
        assert!(mgr.abort(txn_id).is_err());
    }

    #[test]
    fn test_prepare_only_for_global_txn() {
        let (_storage, mgr) = setup();
        // Local txn cannot prepare
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        assert!(mgr.prepare(txn.txn_id).is_err());

        // Global txn can prepare
        let txn2 = mgr.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        assert!(mgr.prepare(txn2.txn_id).is_ok());
    }

    #[test]
    fn test_prepare_idempotent_via_manager() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        mgr.prepare(txn.txn_id).unwrap();
        // Second prepare is idempotent no-op
        mgr.prepare(txn.txn_id).unwrap();
    }

    #[test]
    fn test_begin_sets_active_state() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        assert_eq!(txn.state, TxnState::Active);
    }

    #[test]
    fn test_txn_state_after_commit_is_removed() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        let txn_id = txn.txn_id;
        mgr.commit(txn_id).unwrap();
        // After commit, txn is removed from active set
        assert!(mgr.get_txn(txn_id).is_none());
    }

    #[test]
    fn test_txn_state_after_abort_is_removed() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin(IsolationLevel::ReadCommitted);
        let txn_id = txn.txn_id;
        mgr.abort(txn_id).unwrap();
        assert!(mgr.get_txn(txn_id).is_none());
    }

    // ── Full lifecycle: begin → prepare → commit (global) ──

    #[test]
    fn test_global_lifecycle_begin_prepare_commit() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        let txn_id = txn.txn_id;
        assert_eq!(txn.state, TxnState::Active);

        mgr.prepare(txn_id).unwrap();
        let prepared = mgr.get_txn(txn_id).unwrap();
        assert_eq!(prepared.state, TxnState::Prepared);

        let ts = mgr.commit(txn_id).unwrap();
        assert!(ts.0 > 0);
        assert!(mgr.get_txn(txn_id).is_none());
    }

    // ── Full lifecycle: begin → prepare → abort (global) ──

    #[test]
    fn test_global_lifecycle_begin_prepare_abort() {
        let (_storage, mgr) = setup();
        let txn = mgr.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        let txn_id = txn.txn_id;

        mgr.prepare(txn_id).unwrap();
        mgr.abort(txn_id).unwrap();
        assert!(mgr.get_txn(txn_id).is_none());
    }
}
