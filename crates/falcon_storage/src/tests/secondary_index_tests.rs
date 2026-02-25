    use crate::engine::StorageEngine;
    use crate::memtable::encode_column_value;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

    fn idx_schema() -> TableSchema {
        TableSchema {
            id: TableId(200),
            name: "idx_test".into(),
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
                    name: "name".into(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "age".into(),
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
    fn test_index_maintained_on_insert() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();

        // Create index on column 1 (name)
        engine.create_index("idx_test", 1).unwrap();

        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        engine.insert(TableId(200), row, txn1).unwrap();

        // Under Approach A: index is NOT updated until commit
        let key = encode_column_value(&Datum::Text("alice".into()));
        let pks_before = engine.index_lookup(TableId(200), 1, &key).unwrap();
        assert!(
            pks_before.is_empty(),
            "uncommitted insert should not appear in index"
        );

        // After commit, index should have the entry
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();
        let pks = engine.index_lookup(TableId(200), 1, &key).unwrap();
        assert_eq!(pks.len(), 1);
    }

    #[test]
    fn test_index_maintained_on_update() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        engine.create_index("idx_test", 1).unwrap();

        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        let pk = engine.insert(TableId(200), row, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // Update name from "alice" to "bob"
        let txn2 = TxnId(2);
        let new_row = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("bob".into()),
            Datum::Int32(30),
        ]);
        engine.update(TableId(200), &pk, new_row, txn2).unwrap();

        // Under Approach A: index still shows "alice" (uncommitted update not reflected)
        let old_key = encode_column_value(&Datum::Text("alice".into()));
        assert_eq!(
            engine
                .index_lookup(TableId(200), 1, &old_key)
                .unwrap()
                .len(),
            1,
            "uncommitted update should not change index"
        );

        // After commit, old entry removed, new entry added
        engine.commit_txn_local(txn2, Timestamp(20)).unwrap();
        assert!(
            engine
                .index_lookup(TableId(200), 1, &old_key)
                .unwrap()
                .is_empty(),
            "old index entry should be removed after commit"
        );
        let new_key = encode_column_value(&Datum::Text("bob".into()));
        assert_eq!(
            engine
                .index_lookup(TableId(200), 1, &new_key)
                .unwrap()
                .len(),
            1,
            "new index entry should exist after commit"
        );
    }

    #[test]
    fn test_index_maintained_on_delete() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        engine.create_index("idx_test", 1).unwrap();

        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        let pk = engine.insert(TableId(200), row, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // Delete the row
        let txn2 = TxnId(2);
        engine.delete(TableId(200), &pk, txn2).unwrap();

        // Under Approach A: index still shows "alice" (uncommitted delete not reflected)
        let key = encode_column_value(&Datum::Text("alice".into()));
        assert_eq!(
            engine.index_lookup(TableId(200), 1, &key).unwrap().len(),
            1,
            "uncommitted delete should not change index"
        );

        // After commit, index entry removed
        engine.commit_txn_local(txn2, Timestamp(20)).unwrap();
        assert!(
            engine
                .index_lookup(TableId(200), 1, &key)
                .unwrap()
                .is_empty(),
            "index entry should be removed after commit"
        );
    }

    #[test]
    fn test_index_multiple_rows_same_value() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        engine.create_index("idx_test", 2).unwrap(); // index on age

        let txn1 = TxnId(1);
        // Two rows with same age=25
        let row1 = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(25),
        ]);
        let row2 = OwnedRow::new(vec![
            Datum::Int32(2),
            Datum::Text("bob".into()),
            Datum::Int32(25),
        ]);
        let pk1 = engine.insert(TableId(200), row1, txn1).unwrap();
        engine.insert(TableId(200), row2, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        let key = encode_column_value(&Datum::Int32(25));
        let pks = engine.index_lookup(TableId(200), 2, &key).unwrap();
        assert_eq!(pks.len(), 2, "both rows should appear in index");

        // Delete one row and commit
        let txn2 = TxnId(2);
        engine.delete(TableId(200), &pk1, txn2).unwrap();
        engine.commit_txn_local(txn2, Timestamp(20)).unwrap();

        let pks_after = engine.index_lookup(TableId(200), 2, &key).unwrap();
        assert_eq!(
            pks_after.len(),
            1,
            "only one row should remain in index after committed delete"
        );
    }

    #[test]
    fn test_index_multiple_indexes_maintained() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        engine.create_index("idx_test", 1).unwrap(); // index on name
        engine.create_index("idx_test", 2).unwrap(); // index on age

        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        let pk = engine.insert(TableId(200), row, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // Both indexes should have entries
        let name_key = encode_column_value(&Datum::Text("alice".into()));
        let age_key = encode_column_value(&Datum::Int32(30));
        assert_eq!(
            engine
                .index_lookup(TableId(200), 1, &name_key)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            engine
                .index_lookup(TableId(200), 2, &age_key)
                .unwrap()
                .len(),
            1
        );

        // Update both columns and commit
        let txn2 = TxnId(2);
        let new_row = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("bob".into()),
            Datum::Int32(40),
        ]);
        engine.update(TableId(200), &pk, new_row, txn2).unwrap();
        engine.commit_txn_local(txn2, Timestamp(20)).unwrap();

        // Old entries removed from both indexes after commit
        assert!(engine
            .index_lookup(TableId(200), 1, &name_key)
            .unwrap()
            .is_empty());
        assert!(engine
            .index_lookup(TableId(200), 2, &age_key)
            .unwrap()
            .is_empty());

        // New entries present in both indexes
        let new_name_key = encode_column_value(&Datum::Text("bob".into()));
        let new_age_key = encode_column_value(&Datum::Int32(40));
        assert_eq!(
            engine
                .index_lookup(TableId(200), 1, &new_name_key)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            engine
                .index_lookup(TableId(200), 2, &new_age_key)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn test_index_rebuild_from_data() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();

        // Insert rows WITHOUT an index
        let txn1 = TxnId(1);
        let row1 = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        let row2 = OwnedRow::new(vec![
            Datum::Int32(2),
            Datum::Text("bob".into()),
            Datum::Int32(25),
        ]);
        engine.insert(TableId(200), row1, txn1).unwrap();
        engine.insert(TableId(200), row2, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // Now create index  鈥?it should backfill existing rows
        engine.create_index("idx_test", 1).unwrap();

        let alice_key = encode_column_value(&Datum::Text("alice".into()));
        let bob_key = encode_column_value(&Datum::Text("bob".into()));
        assert_eq!(
            engine
                .index_lookup(TableId(200), 1, &alice_key)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            engine
                .index_lookup(TableId(200), 1, &bob_key)
                .unwrap()
                .len(),
            1
        );

        // Subsequent inserts should maintain the index after commit
        let txn2 = TxnId(2);
        let row3 = OwnedRow::new(vec![
            Datum::Int32(3),
            Datum::Text("charlie".into()),
            Datum::Int32(35),
        ]);
        engine.insert(TableId(200), row3, txn2).unwrap();
        engine.commit_txn_local(txn2, Timestamp(20)).unwrap();

        let charlie_key = encode_column_value(&Datum::Text("charlie".into()));
        assert_eq!(
            engine
                .index_lookup(TableId(200), 1, &charlie_key)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn test_index_no_index_no_overhead() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();

        // Insert/update/delete without any index  鈥?should work fine
        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        let pk = engine.insert(TableId(200), row, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        let txn2 = TxnId(2);
        let new_row = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("bob".into()),
            Datum::Int32(40),
        ]);
        engine.update(TableId(200), &pk, new_row, txn2).unwrap();
        engine.commit_txn_local(txn2, Timestamp(20)).unwrap();

        let txn3 = TxnId(3);
        engine.delete(TableId(200), &pk, txn3).unwrap();
        engine.commit_txn_local(txn3, Timestamp(30)).unwrap();

        // No crash, no errors  鈥?index maintenance is a no-op when no indexes exist
    }

    #[test]
    fn test_abort_does_not_affect_index() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        engine.create_index("idx_test", 1).unwrap();

        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        engine.insert(TableId(200), row, txn1).unwrap();

        // Under Approach A: index should NOT have the entry (uncommitted)
        let key = encode_column_value(&Datum::Text("alice".into()));
        assert!(
            engine
                .index_lookup(TableId(200), 1, &key)
                .unwrap()
                .is_empty(),
            "uncommitted insert should not appear in index"
        );

        // Abort the transaction
        engine.abort_txn_local(txn1).unwrap();

        // Index still empty  鈥?abort is a no-op for indexes under Approach A
        assert!(
            engine
                .index_lookup(TableId(200), 1, &key)
                .unwrap()
                .is_empty(),
            "index should remain empty after abort"
        );
    }

    #[test]
    fn test_abort_preserves_committed_index_on_update() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        engine.create_index("idx_test", 1).unwrap();

        // Insert and commit a row
        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        let pk = engine.insert(TableId(200), row, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // Update name to "bob" in txn2 (uncommitted)
        let txn2 = TxnId(2);
        let new_row = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("bob".into()),
            Datum::Int32(30),
        ]);
        engine.update(TableId(200), &pk, new_row, txn2).unwrap();

        let alice_key = encode_column_value(&Datum::Text("alice".into()));
        let bob_key = encode_column_value(&Datum::Text("bob".into()));

        // Under Approach A: index still shows "alice" (committed), "bob" not in index
        assert_eq!(
            engine
                .index_lookup(TableId(200), 1, &alice_key)
                .unwrap()
                .len(),
            1,
            "committed index entry should remain during uncommitted update"
        );
        assert!(
            engine
                .index_lookup(TableId(200), 1, &bob_key)
                .unwrap()
                .is_empty(),
            "uncommitted update should not appear in index"
        );

        // Abort txn2
        engine.abort_txn_local(txn2).unwrap();

        // After abort: "alice" still in index (was never removed), "bob" still absent
        assert_eq!(
            engine
                .index_lookup(TableId(200), 1, &alice_key)
                .unwrap()
                .len(),
            1,
            "committed index entry should remain after abort"
        );
        assert!(
            engine
                .index_lookup(TableId(200), 1, &bob_key)
                .unwrap()
                .is_empty(),
            "aborted update should not appear in index"
        );
    }

    #[test]
    fn test_unique_index_rejects_duplicate() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        engine.create_unique_index("idx_test", 1).unwrap();

        let txn1 = TxnId(1);
        let row1 = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        engine.insert(TableId(200), row1, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // Insert another row with same name  鈥?should fail
        let txn2 = TxnId(2);
        let row2 = OwnedRow::new(vec![
            Datum::Int32(2),
            Datum::Text("alice".into()),
            Datum::Int32(25),
        ]);
        let result = engine.insert(TableId(200), row2, txn2);
        assert!(
            result.is_err(),
            "unique index should reject duplicate value"
        );
    }

    #[test]
    fn test_unique_index_allows_after_delete() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        engine.create_unique_index("idx_test", 1).unwrap();

        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        let pk = engine.insert(TableId(200), row, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // Delete the row
        let txn2 = TxnId(2);
        engine.delete(TableId(200), &pk, txn2).unwrap();
        engine.commit_txn_local(txn2, Timestamp(20)).unwrap();

        // Now inserting "alice" again should succeed
        let txn3 = TxnId(3);
        let row2 = OwnedRow::new(vec![
            Datum::Int32(2),
            Datum::Text("alice".into()),
            Datum::Int32(25),
        ]);
        let result = engine.insert(TableId(200), row2, txn3);
        assert!(
            result.is_ok(),
            "unique index should allow value after delete"
        );
    }

    #[test]
    fn test_unique_index_backfill_rejects_existing_duplicates() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();

        // Insert two rows with same name (no index yet)
        let txn1 = TxnId(1);
        let row1 = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        let row2 = OwnedRow::new(vec![
            Datum::Int32(2),
            Datum::Text("alice".into()),
            Datum::Int32(25),
        ]);
        engine.insert(TableId(200), row1, txn1).unwrap();
        engine.insert(TableId(200), row2, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // Creating a unique index should fail due to existing duplicates
        let result = engine.create_unique_index("idx_test", 1);
        assert!(
            result.is_err(),
            "unique index creation should fail with existing duplicates"
        );
    }

    #[test]
    fn test_engine_run_gc_truncates_old_versions() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();

        // Insert and update a row multiple times to build up version chains
        let txn1 = TxnId(1);
        let row1 = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("v1".into()),
            Datum::Int32(10),
        ]);
        let pk = engine.insert(TableId(200), row1, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        let txn2 = TxnId(2);
        let row2 = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("v2".into()),
            Datum::Int32(20),
        ]);
        engine.update(TableId(200), &pk, row2, txn2).unwrap();
        engine.commit_txn_local(txn2, Timestamp(20)).unwrap();

        let txn3 = TxnId(3);
        let row3 = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("v3".into()),
            Datum::Int32(30),
        ]);
        engine.update(TableId(200), &pk, row3, txn3).unwrap();
        engine.commit_txn_local(txn3, Timestamp(30)).unwrap();

        // GC with watermark=20 should keep v2 (ts=20) and v3 (ts=30), drop v1 (ts=10)
        let chains = engine.run_gc(Timestamp(20));
        assert!(chains > 0, "should process at least 1 chain");

        // v3 still visible at ts=30
        let results = engine.scan(TableId(200), TxnId(4), Timestamp(35)).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.values[1], Datum::Text("v3".into()));

        // v2 still visible at ts=20
        let results = engine.scan(TableId(200), TxnId(5), Timestamp(20)).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.values[1], Datum::Text("v2".into()));
    }

    // 鈹€鈹€ Commit-time unique constraint re-validation tests 鈹€鈹€

    #[test]
    fn test_concurrent_insert_same_unique_key_one_wins() {
        // Two txns both insert the same unique key concurrently.
        // Both pass insert-time check (index is empty).
        // First to commit wins; second must fail at commit-time.
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        engine.create_unique_index("idx_test", 1).unwrap();

        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        // Both insert "alice" with different PKs
        let row1 = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        let row2 = OwnedRow::new(vec![
            Datum::Int32(2),
            Datum::Text("alice".into()),
            Datum::Int32(25),
        ]);
        engine.insert(TableId(200), row1, txn1).unwrap();
        engine.insert(TableId(200), row2, txn2).unwrap();

        // txn1 commits first  鈥?succeeds
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 tries to commit  鈥?must fail (UniqueViolation)
        let result = engine.commit_txn_local(txn2, Timestamp(20));
        assert!(
            result.is_err(),
            "second concurrent insert of same unique key must fail at commit"
        );
        match result.unwrap_err() {
            falcon_common::error::StorageError::UniqueViolation { column_idx, .. } => {
                assert_eq!(column_idx, 1);
            }
            other => panic!("Expected UniqueViolation, got: {:?}", other),
        }

        // Only txn1's row should be visible
        let rows = engine.scan(TableId(200), TxnId(3), Timestamp(30)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[0], Datum::Int32(1));
    }

    #[test]
    fn test_sequential_commit_same_unique_key_second_fails() {
        // txn1 commits "alice", then txn2 tries to commit "alice" with different PK.
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        engine.create_unique_index("idx_test", 1).unwrap();

        let txn1 = TxnId(1);
        let row1 = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        engine.insert(TableId(200), row1, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 started after txn1 committed  鈥?insert-time check catches it
        let txn2 = TxnId(2);
        let row2 = OwnedRow::new(vec![
            Datum::Int32(2),
            Datum::Text("alice".into()),
            Datum::Int32(25),
        ]);
        let insert_result = engine.insert(TableId(200), row2, txn2);
        assert!(
            insert_result.is_err(),
            "insert should fail against committed unique key"
        );
    }

    #[test]
    fn test_update_causing_unique_key_conflict_at_commit() {
        // txn1 commits "alice" (pk=1) and "bob" (pk=2).
        // txn2 updates pk=2 to name="alice"  鈥?passes at DML time (no conflict yet
        // because the index check at update time is against committed index which
        // has "bob" for pk=2). At commit, re-validation catches it.
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        engine.create_unique_index("idx_test", 1).unwrap();

        let txn1 = TxnId(1);
        let row1 = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        let row2 = OwnedRow::new(vec![
            Datum::Int32(2),
            Datum::Text("bob".into()),
            Datum::Int32(25),
        ]);
        engine.insert(TableId(200), row1, txn1).unwrap();
        let pk2 = engine.insert(TableId(200), row2, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 updates pk=2 to name="alice"  鈥?conflicts with pk=1
        let txn2 = TxnId(2);
        let updated_row = OwnedRow::new(vec![
            Datum::Int32(2),
            Datum::Text("alice".into()),
            Datum::Int32(25),
        ]);
        engine
            .update(TableId(200), &pk2, updated_row, txn2)
            .unwrap();

        // Commit should fail due to unique constraint on "alice"
        let result = engine.commit_txn_local(txn2, Timestamp(20));
        assert!(
            result.is_err(),
            "update causing unique conflict must fail at commit"
        );
        match result.unwrap_err() {
            falcon_common::error::StorageError::UniqueViolation { column_idx, .. } => {
                assert_eq!(column_idx, 1);
            }
            other => panic!("Expected UniqueViolation, got: {:?}", other),
        }
    }

    #[test]
    fn test_commit_time_unique_atomicity_no_partial_commit() {
        // If unique validation fails, NO writes should be committed.
        // Setup: txn1 and txn2 both start before any commit.
        // txn1 inserts "alice" (pk=1). txn2 inserts "carol" (pk=10) and "alice" (pk=20).
        // Both pass insert-time check (index is empty).
        // txn1 commits first. txn2 commit fails  鈥?neither "carol" nor "alice" should commit.
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        engine.create_unique_index("idx_test", 1).unwrap();

        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        // txn1 inserts "alice"
        let row1 = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        engine.insert(TableId(200), row1, txn1).unwrap();

        // txn2 inserts "carol" AND "alice"  鈥?both pass insert-time check (index empty)
        let row_carol = OwnedRow::new(vec![
            Datum::Int32(10),
            Datum::Text("carol".into()),
            Datum::Int32(40),
        ]);
        let row_alice2 = OwnedRow::new(vec![
            Datum::Int32(20),
            Datum::Text("alice".into()),
            Datum::Int32(22),
        ]);
        engine.insert(TableId(200), row_carol, txn2).unwrap();
        engine.insert(TableId(200), row_alice2, txn2).unwrap();

        // txn1 commits first  鈥?succeeds, "alice" now in index
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 commit should fail  鈥?"alice" conflicts
        let result = engine.commit_txn_local(txn2, Timestamp(20));
        assert!(result.is_err());

        // Only txn1's row (pk=1, alice) should be visible  鈥?carol should NOT be committed
        let rows = engine.scan(TableId(200), TxnId(3), Timestamp(30)).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "atomicity: no partial commit on unique violation"
        );
        assert_eq!(rows[0].1.values[0], Datum::Int32(1));
    }

    #[test]
    fn test_no_unique_index_commit_always_succeeds() {
        // Without unique indexes, commit-time validation is a no-op.
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        // Non-unique index
        engine.create_index("idx_test", 1).unwrap();

        let txn1 = TxnId(1);
        let txn2 = TxnId(2);
        let row1 = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        let row2 = OwnedRow::new(vec![
            Datum::Int32(2),
            Datum::Text("alice".into()),
            Datum::Int32(25),
        ]);
        engine.insert(TableId(200), row1, txn1).unwrap();
        engine.insert(TableId(200), row2, txn2).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();
        engine.commit_txn_local(txn2, Timestamp(20)).unwrap();

        let rows = engine.scan(TableId(200), TxnId(3), Timestamp(30)).unwrap();
        assert_eq!(rows.len(), 2, "non-unique index allows duplicate values");
    }

    #[test]
    fn test_index_scan_returns_correct_rows() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(idx_schema()).unwrap();
        engine.create_index("idx_test", 1).unwrap();

        let txn1 = TxnId(1);
        let row1 = OwnedRow::new(vec![
            Datum::Int32(1),
            Datum::Text("alice".into()),
            Datum::Int32(30),
        ]);
        let row2 = OwnedRow::new(vec![
            Datum::Int32(2),
            Datum::Text("bob".into()),
            Datum::Int32(25),
        ]);
        let row3 = OwnedRow::new(vec![
            Datum::Int32(3),
            Datum::Text("alice".into()),
            Datum::Int32(35),
        ]);
        engine.insert(TableId(200), row1, txn1).unwrap();
        engine.insert(TableId(200), row2, txn1).unwrap();
        engine.insert(TableId(200), row3, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // Index scan for "alice" should return 2 rows
        let key = encode_column_value(&Datum::Text("alice".into()));
        let results = engine
            .index_scan(TableId(200), 1, &key, TxnId(2), Timestamp(15))
            .unwrap();
        assert_eq!(results.len(), 2, "index scan should return 2 alice rows");

        // Index scan for "bob" should return 1 row
        let bob_key = encode_column_value(&Datum::Text("bob".into()));
        let bob_results = engine
            .index_scan(TableId(200), 1, &bob_key, TxnId(2), Timestamp(15))
            .unwrap();
        assert_eq!(bob_results.len(), 1, "index scan should return 1 bob row");

        // Index scan for non-existent value should return 0 rows
        let none_key = encode_column_value(&Datum::Text("charlie".into()));
        let none_results = engine
            .index_scan(TableId(200), 1, &none_key, TxnId(2), Timestamp(15))
            .unwrap();
        assert!(
            none_results.is_empty(),
            "index scan for non-existent value should return 0 rows"
        );
    }
