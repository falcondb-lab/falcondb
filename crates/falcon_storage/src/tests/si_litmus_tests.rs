    use crate::engine::StorageEngine;
    use crate::memtable::encode_pk;
    use crate::mvcc::VersionChain;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId, TxnType};

    fn row1(v: i32) -> OwnedRow {
        OwnedRow::new(vec![Datum::Int32(v)])
    }

    fn row2(k: i32, v: &str) -> OwnedRow {
        OwnedRow::new(vec![Datum::Int32(k), Datum::Text(v.into())])
    }

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(900),
            name: "si_test".into(),
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

    fn engine_with_table() -> StorageEngine {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(test_schema()).unwrap();
        engine
    }

    // 鈹€鈹€ 1. Basic visibility 鈹€鈹€

    #[test]
    fn si_01_uncommitted_invisible_to_others() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(42)));
        // Uncommitted version is invisible to any reader
        assert!(chain.read_committed(Timestamp(999)).is_none());
    }

    #[test]
    fn si_02_own_writes_visible() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(42)));
        // Own txn sees its uncommitted write
        assert_eq!(
            chain.read_for_txn(TxnId(1), Timestamp(0)).unwrap().values[0],
            Datum::Int32(42)
        );
    }

    #[test]
    fn si_03_committed_visible_at_commit_ts() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(10)));
        chain.commit(TxnId(1), Timestamp(5));
        assert!(chain.read_committed(Timestamp(5)).is_some());
    }

    #[test]
    fn si_04_committed_invisible_before_commit_ts() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(10)));
        chain.commit(TxnId(1), Timestamp(5));
        assert!(chain.read_committed(Timestamp(4)).is_none());
    }

    #[test]
    fn si_05_committed_visible_after_commit_ts() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(10)));
        chain.commit(TxnId(1), Timestamp(5));
        assert!(chain.read_committed(Timestamp(100)).is_some());
    }

    // 鈹€鈹€ 2. Snapshot consistency 鈹€鈹€

    #[test]
    fn si_06_snapshot_reads_point_in_time() {
        let chain = VersionChain::new();
        // v1: committed at ts=5
        chain.prepend(TxnId(1), Some(row1(100)));
        chain.commit(TxnId(1), Timestamp(5));
        // v2: committed at ts=10
        chain.prepend(TxnId(2), Some(row1(200)));
        chain.commit(TxnId(2), Timestamp(10));
        // Read at ts=7 should see v1 (100), not v2 (200)
        let r = chain.read_committed(Timestamp(7)).unwrap();
        assert_eq!(r.values[0], Datum::Int32(100));
    }

    #[test]
    fn si_07_snapshot_sees_latest_at_ts() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(100)));
        chain.commit(TxnId(1), Timestamp(5));
        chain.prepend(TxnId(2), Some(row1(200)));
        chain.commit(TxnId(2), Timestamp(10));
        // Read at ts=10 should see v2 (200)
        let r = chain.read_committed(Timestamp(10)).unwrap();
        assert_eq!(r.values[0], Datum::Int32(200));
    }

    #[test]
    fn si_08_snapshot_multiple_updates_correct_version() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(10)));
        chain.commit(TxnId(1), Timestamp(1));
        chain.prepend(TxnId(2), Some(row1(20)));
        chain.commit(TxnId(2), Timestamp(3));
        chain.prepend(TxnId(3), Some(row1(30)));
        chain.commit(TxnId(3), Timestamp(5));
        chain.prepend(TxnId(4), Some(row1(40)));
        chain.commit(TxnId(4), Timestamp(7));

        assert_eq!(
            chain.read_committed(Timestamp(2)).unwrap().values[0],
            Datum::Int32(10)
        );
        assert_eq!(
            chain.read_committed(Timestamp(4)).unwrap().values[0],
            Datum::Int32(20)
        );
        assert_eq!(
            chain.read_committed(Timestamp(6)).unwrap().values[0],
            Datum::Int32(30)
        );
        assert_eq!(
            chain.read_committed(Timestamp(8)).unwrap().values[0],
            Datum::Int32(40)
        );
    }

    // 鈹€鈹€ 3. Abort visibility 鈹€鈹€

    #[test]
    fn si_09_aborted_never_visible() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(42)));
        chain.abort(TxnId(1));
        assert!(chain.read_committed(Timestamp(999)).is_none());
    }

    #[test]
    fn si_10_abort_restores_prior_version() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(100)));
        chain.commit(TxnId(1), Timestamp(5));
        chain.prepend(TxnId(2), Some(row1(200)));
        chain.abort(TxnId(2));
        // After aborting T2, reads should still see T1's value
        let r = chain.read_committed(Timestamp(10)).unwrap();
        assert_eq!(r.values[0], Datum::Int32(100));
    }

    #[test]
    fn si_11_abort_own_writes_gone() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(42)));
        chain.abort(TxnId(1));
        // Own writes also gone after abort
        assert!(chain.read_for_txn(TxnId(1), Timestamp(0)).is_none());
    }

    // 鈹€鈹€ 4. Tombstone / delete 鈹€鈹€

    #[test]
    fn si_12_delete_tombstone_hides_row() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(42)));
        chain.commit(TxnId(1), Timestamp(5));
        // Delete = prepend None
        chain.prepend(TxnId(2), None);
        chain.commit(TxnId(2), Timestamp(10));
        // Read at ts=7 (before delete) sees the row
        assert!(chain.read_committed(Timestamp(7)).is_some());
        // Read at ts=10 (after delete) sees nothing
        assert!(chain.read_committed(Timestamp(10)).is_none());
    }

    #[test]
    fn si_13_delete_then_reinsert() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(42)));
        chain.commit(TxnId(1), Timestamp(5));
        chain.prepend(TxnId(2), None); // delete
        chain.commit(TxnId(2), Timestamp(10));
        chain.prepend(TxnId(3), Some(row1(99))); // reinsert
        chain.commit(TxnId(3), Timestamp(15));

        assert_eq!(
            chain.read_committed(Timestamp(7)).unwrap().values[0],
            Datum::Int32(42)
        );
        assert!(chain.read_committed(Timestamp(12)).is_none());
        assert_eq!(
            chain.read_committed(Timestamp(15)).unwrap().values[0],
            Datum::Int32(99)
        );
    }

    // 鈹€鈹€ 5. Concurrent txn isolation 鈹€鈹€

    #[test]
    fn si_14_concurrent_writers_isolation() {
        let chain = VersionChain::new();
        // T1 inserts, uncommitted
        chain.prepend(TxnId(1), Some(row1(100)));
        // T2 reads 鈥?should not see T1
        assert!(chain.read_committed(Timestamp(50)).is_none());
        assert!(chain.read_for_txn(TxnId(2), Timestamp(50)).is_none());
        // T1 commits
        chain.commit(TxnId(1), Timestamp(10));
        // T2 with snapshot at ts=5 still doesn't see it
        assert!(chain.read_committed(Timestamp(5)).is_none());
        // T2 with snapshot at ts=10 sees it
        assert!(chain.read_committed(Timestamp(10)).is_some());
    }

    #[test]
    fn si_15_two_txns_different_snapshots() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(10)));
        chain.commit(TxnId(1), Timestamp(5));
        // T2 starts at ts=3, T3 starts at ts=7
        // T2 should NOT see T1's write, T3 should
        assert!(chain.read_committed(Timestamp(3)).is_none());
        assert!(chain.read_committed(Timestamp(7)).is_some());
    }

    #[test]
    fn si_16_write_skew_both_read_old() {
        // Classic write-skew: T1 and T2 both read, then write different keys.
        // Under SI, both see the old snapshot 鈥?this is expected behavior.
        let chain_a = VersionChain::new();
        let chain_b = VersionChain::new();
        // Initial: A=1, B=1
        chain_a.prepend(TxnId(0), Some(row1(1)));
        chain_a.commit(TxnId(0), Timestamp(1));
        chain_b.prepend(TxnId(0), Some(row1(1)));
        chain_b.commit(TxnId(0), Timestamp(1));

        // T1 reads A, T2 reads B (both at snapshot ts=2)
        let a_for_t1 = chain_a.read_committed(Timestamp(2)).unwrap();
        let b_for_t2 = chain_b.read_committed(Timestamp(2)).unwrap();
        assert_eq!(a_for_t1.values[0], Datum::Int32(1));
        assert_eq!(b_for_t2.values[0], Datum::Int32(1));

        // T1 writes B=0, T2 writes A=0
        chain_b.prepend(TxnId(1), Some(row1(0)));
        chain_a.prepend(TxnId(2), Some(row1(0)));
        chain_b.commit(TxnId(1), Timestamp(3));
        chain_a.commit(TxnId(2), Timestamp(4));

        // After both commit: A=0, B=0 (write skew allowed under SI)
        assert_eq!(
            chain_a.read_committed(Timestamp(5)).unwrap().values[0],
            Datum::Int32(0)
        );
        assert_eq!(
            chain_b.read_committed(Timestamp(5)).unwrap().values[0],
            Datum::Int32(0)
        );
    }

    // 鈹€鈹€ 6. Read-own-writes 鈹€鈹€

    #[test]
    fn si_17_read_own_uncommitted_update() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(10)));
        chain.commit(TxnId(1), Timestamp(5));
        // T2 updates to 20 (uncommitted)
        chain.prepend(TxnId(2), Some(row1(20)));
        // T2 reads own write
        assert_eq!(
            chain.read_for_txn(TxnId(2), Timestamp(5)).unwrap().values[0],
            Datum::Int32(20)
        );
        // Other txn still sees 10
        assert_eq!(
            chain.read_for_txn(TxnId(3), Timestamp(5)).unwrap().values[0],
            Datum::Int32(10)
        );
    }

    #[test]
    fn si_18_read_own_uncommitted_delete() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(42)));
        chain.commit(TxnId(1), Timestamp(5));
        // T2 deletes (uncommitted)
        chain.prepend(TxnId(2), None);
        // T2 sees the tombstone (None)
        assert!(chain.read_for_txn(TxnId(2), Timestamp(10)).is_none());
        // Other txn still sees the row
        assert!(chain.read_for_txn(TxnId(3), Timestamp(10)).is_some());
    }

    // 鈹€鈹€ 7. GC safety 鈹€鈹€

    #[test]
    fn si_19_gc_preserves_visible_version() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(10)));
        chain.commit(TxnId(1), Timestamp(5));
        chain.prepend(TxnId(2), Some(row1(20)));
        chain.commit(TxnId(2), Timestamp(10));
        // GC at watermark=10: should keep v2, drop v1
        let result = chain.gc(Timestamp(10));
        assert!(result.reclaimed_versions >= 1);
        // v2 still readable
        assert_eq!(
            chain.read_committed(Timestamp(10)).unwrap().values[0],
            Datum::Int32(20)
        );
    }

    #[test]
    fn si_20_gc_does_not_drop_uncommitted() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(10)));
        // Don't commit 鈥?GC should not touch uncommitted
        chain.gc(Timestamp(100));
        // Still readable by own txn
        assert!(chain.read_for_txn(TxnId(1), Timestamp(0)).is_some());
    }

    // 鈹€鈹€ 8. StorageEngine integration 鈹€鈹€

    #[test]
    fn si_21_engine_insert_invisible_before_commit() {
        let engine = engine_with_table();
        engine
            .insert(TableId(900), row2(1, "hello"), TxnId(1))
            .unwrap();
        // Scan by another txn 鈥?should not see uncommitted row
        let rows = engine.scan(TableId(900), TxnId(2), Timestamp(100)).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn si_22_engine_insert_visible_after_commit() {
        let engine = engine_with_table();
        engine
            .insert(TableId(900), row2(1, "hello"), TxnId(1))
            .unwrap();
        engine
            .commit_txn(TxnId(1), Timestamp(10), TxnType::Local)
            .unwrap();
        let rows = engine.scan(TableId(900), TxnId(2), Timestamp(10)).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn si_23_engine_snapshot_isolation_two_versions() {
        let engine = engine_with_table();
        let pk = engine
            .insert(TableId(900), row2(1, "v1"), TxnId(1))
            .unwrap();
        engine
            .commit_txn(TxnId(1), Timestamp(5), TxnType::Local)
            .unwrap();

        engine
            .update(TableId(900), &pk, row2(1, "v2"), TxnId(2))
            .unwrap();
        engine
            .commit_txn(TxnId(2), Timestamp(10), TxnType::Local)
            .unwrap();

        // Read at ts=7 sees v1
        let rows_7 = engine.scan(TableId(900), TxnId(99), Timestamp(7)).unwrap();
        assert_eq!(rows_7.len(), 1);
        assert_eq!(rows_7[0].1.values[1], Datum::Text("v1".into()));

        // Read at ts=10 sees v2
        let rows_10 = engine.scan(TableId(900), TxnId(99), Timestamp(10)).unwrap();
        assert_eq!(rows_10.len(), 1);
        assert_eq!(rows_10[0].1.values[1], Datum::Text("v2".into()));
    }

    #[test]
    fn si_24_engine_delete_invisible_to_old_snapshot() {
        let engine = engine_with_table();
        let pk = engine
            .insert(TableId(900), row2(1, "alive"), TxnId(1))
            .unwrap();
        engine
            .commit_txn(TxnId(1), Timestamp(5), TxnType::Local)
            .unwrap();

        engine.delete(TableId(900), &pk, TxnId(2)).unwrap();
        engine
            .commit_txn(TxnId(2), Timestamp(10), TxnType::Local)
            .unwrap();

        // Snapshot before delete still sees the row
        let rows = engine.scan(TableId(900), TxnId(99), Timestamp(7)).unwrap();
        assert_eq!(rows.len(), 1);
        // Snapshot at delete time: row gone
        let rows = engine.scan(TableId(900), TxnId(99), Timestamp(10)).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn si_25_engine_aborted_txn_invisible() {
        let engine = engine_with_table();
        engine
            .insert(TableId(900), row2(1, "will_abort"), TxnId(1))
            .unwrap();
        engine.abort_txn(TxnId(1), TxnType::Local).unwrap();

        let rows = engine
            .scan(TableId(900), TxnId(99), Timestamp(999))
            .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn si_26_engine_own_writes_visible_in_txn() {
        let engine = engine_with_table();
        let pk = engine
            .insert(TableId(900), row2(1, "mine"), TxnId(1))
            .unwrap();
        // Same txn can read its own writes
        let r = engine.get(TableId(900), &pk, TxnId(1), Timestamp(0));
        assert!(r.is_ok());
        assert!(r.unwrap().is_some());
    }

    #[test]
    fn si_27_engine_multiple_rows_snapshot() {
        let engine = engine_with_table();
        for i in 1..=5 {
            engine
                .insert(TableId(900), row2(i, &format!("r{}", i)), TxnId(1))
                .unwrap();
        }
        engine
            .commit_txn(TxnId(1), Timestamp(5), TxnType::Local)
            .unwrap();

        // Update row 3
        let pk3 = encode_pk(&row2(3, "r3"), &[0]);
        engine
            .update(TableId(900), &pk3, row2(3, "r3_updated"), TxnId(2))
            .unwrap();
        engine
            .commit_txn(TxnId(2), Timestamp(10), TxnType::Local)
            .unwrap();

        // Snapshot at ts=7: all 5 rows, row 3 still "r3"
        let rows = engine.scan(TableId(900), TxnId(99), Timestamp(7)).unwrap();
        assert_eq!(rows.len(), 5);
        let row3 = rows
            .iter()
            .find(|(_, r)| r.values[0] == Datum::Int32(3))
            .unwrap();
        assert_eq!(row3.1.values[1], Datum::Text("r3".into()));

        // Snapshot at ts=10: all 5 rows, row 3 is "r3_updated"
        let rows = engine.scan(TableId(900), TxnId(99), Timestamp(10)).unwrap();
        assert_eq!(rows.len(), 5);
        let row3 = rows
            .iter()
            .find(|(_, r)| r.values[0] == Datum::Int32(3))
            .unwrap();
        assert_eq!(row3.1.values[1], Datum::Text("r3_updated".into()));
    }

    #[test]
    fn si_28_engine_concurrent_inserts_isolation() {
        let engine = engine_with_table();
        // T1 inserts row 1, T2 inserts row 2 鈥?both uncommitted
        engine
            .insert(TableId(900), row2(1, "t1"), TxnId(1))
            .unwrap();
        engine
            .insert(TableId(900), row2(2, "t2"), TxnId(2))
            .unwrap();

        // T3 sees nothing
        let rows = engine.scan(TableId(900), TxnId(3), Timestamp(50)).unwrap();
        assert!(rows.is_empty());

        // Commit T1 at ts=10
        engine
            .commit_txn(TxnId(1), Timestamp(10), TxnType::Local)
            .unwrap();

        // T3 at ts=10 sees only T1's row
        let rows = engine.scan(TableId(900), TxnId(3), Timestamp(10)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[1], Datum::Text("t1".into()));

        // Commit T2 at ts=20
        engine
            .commit_txn(TxnId(2), Timestamp(20), TxnType::Local)
            .unwrap();

        // T3 at ts=20 sees both
        let rows = engine.scan(TableId(900), TxnId(3), Timestamp(20)).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn si_29_engine_update_abort_restores_old() {
        let engine = engine_with_table();
        let pk = engine
            .insert(TableId(900), row2(1, "original"), TxnId(1))
            .unwrap();
        engine
            .commit_txn(TxnId(1), Timestamp(5), TxnType::Local)
            .unwrap();

        // T2 updates, then aborts
        engine
            .update(TableId(900), &pk, row2(1, "modified"), TxnId(2))
            .unwrap();
        engine.abort_txn(TxnId(2), TxnType::Local).unwrap();

        // Original value restored
        let rows = engine
            .scan(TableId(900), TxnId(99), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[1], Datum::Text("original".into()));
    }

    #[test]
    fn si_30_engine_delete_abort_restores_row() {
        let engine = engine_with_table();
        let pk = engine
            .insert(TableId(900), row2(1, "keep_me"), TxnId(1))
            .unwrap();
        engine
            .commit_txn(TxnId(1), Timestamp(5), TxnType::Local)
            .unwrap();

        // T2 deletes, then aborts
        engine.delete(TableId(900), &pk, TxnId(2)).unwrap();
        engine.abort_txn(TxnId(2), TxnType::Local).unwrap();

        // Row still exists
        let rows = engine
            .scan(TableId(900), TxnId(99), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    // 鈹€鈹€ 9. Edge cases 鈹€鈹€

    #[test]
    fn si_31_empty_chain_returns_none() {
        let chain = VersionChain::new();
        assert!(chain.read_committed(Timestamp(999)).is_none());
        assert!(chain.read_for_txn(TxnId(1), Timestamp(999)).is_none());
    }

    #[test]
    fn si_32_commit_at_ts_zero_invisible() {
        // Commit timestamp 0 is reserved for "uncommitted" 鈥?should never be visible
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(42)));
        // Manually check: commit_ts defaults to 0
        assert!(chain.read_committed(Timestamp(999)).is_none());
    }

    #[test]
    fn si_33_read_at_ts_zero() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(42)));
        chain.commit(TxnId(1), Timestamp(1));
        // Read at ts=0 should not see commit at ts=1
        assert!(chain.read_committed(Timestamp(0)).is_none());
    }

    #[test]
    fn si_34_many_versions_correct_snapshot() {
        let chain = VersionChain::new();
        for i in 1..=20u64 {
            chain.prepend(TxnId(i), Some(row1(i as i32)));
            chain.commit(TxnId(i), Timestamp(i * 10));
        }
        // Read at ts=55 should see version committed at ts=50 (i=5, value=5)
        let r = chain.read_committed(Timestamp(55)).unwrap();
        assert_eq!(r.values[0], Datum::Int32(5));

        // Read at ts=200 should see the latest (i=20, value=20)
        let r = chain.read_committed(Timestamp(200)).unwrap();
        assert_eq!(r.values[0], Datum::Int32(20));
    }

    #[test]
    fn si_35_interleaved_commit_abort() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(10)));
        chain.commit(TxnId(1), Timestamp(5));
        chain.prepend(TxnId(2), Some(row1(20)));
        chain.abort(TxnId(2));
        chain.prepend(TxnId(3), Some(row1(30)));
        chain.commit(TxnId(3), Timestamp(15));

        // At ts=10: should see T1's value (T2 aborted, T3 not yet committed)
        assert_eq!(
            chain.read_committed(Timestamp(10)).unwrap().values[0],
            Datum::Int32(10)
        );
        // At ts=15: should see T3's value
        assert_eq!(
            chain.read_committed(Timestamp(15)).unwrap().values[0],
            Datum::Int32(30)
        );
    }
