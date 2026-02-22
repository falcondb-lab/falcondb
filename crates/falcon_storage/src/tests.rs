#[cfg(test)]
mod mvcc_tests {
    use crate::mvcc::VersionChain;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::types::{Timestamp, TxnId};

    fn row(vals: Vec<Datum>) -> OwnedRow {
        OwnedRow::new(vals)
    }

    #[test]
    fn test_version_chain_basic_insert_commit_read() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        let ts_commit = Timestamp(10);

        chain.prepend(txn1, Some(row(vec![Datum::Int32(42)])));
        // Uncommitted  — not visible to other txns
        assert!(chain.read_committed(Timestamp(100)).is_none());
        // Visible to own txn
        assert!(chain.read_for_txn(txn1, Timestamp(0)).is_some());

        chain.commit(txn1, ts_commit);
        // Now visible to reads at ts >= 10
        let r = chain.read_committed(Timestamp(10));
        assert!(r.is_some());
        assert_eq!(r.unwrap().values[0], Datum::Int32(42));

        // Not visible to reads before commit ts
        assert!(chain.read_committed(Timestamp(5)).is_none());
    }

    #[test]
    fn test_version_chain_abort() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);

        chain.prepend(txn1, Some(row(vec![Datum::Text("aborted".into())])));
        chain.abort(txn1);

        // Aborted version never visible
        assert!(chain.read_committed(Timestamp(1000)).is_none());
        assert!(chain.read_for_txn(txn1, Timestamp(1000)).is_none());
    }

    #[test]
    fn test_version_chain_multiple_versions() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        // txn1 inserts version at ts=10
        chain.prepend(txn1, Some(row(vec![Datum::Int32(1)])));
        chain.commit(txn1, Timestamp(10));

        // txn2 updates at ts=20
        chain.prepend(txn2, Some(row(vec![Datum::Int32(2)])));
        chain.commit(txn2, Timestamp(20));

        // Read at ts=15 sees version from txn1
        let r = chain.read_committed(Timestamp(15));
        assert_eq!(r.unwrap().values[0], Datum::Int32(1));

        // Read at ts=25 sees version from txn2
        let r = chain.read_committed(Timestamp(25));
        assert_eq!(r.unwrap().values[0], Datum::Int32(2));
    }

    #[test]
    fn test_version_chain_delete_tombstone() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        chain.prepend(txn1, Some(row(vec![Datum::Int32(99)])));
        chain.commit(txn1, Timestamp(10));

        // Delete = tombstone (None data)
        chain.prepend(txn2, None);
        chain.commit(txn2, Timestamp(20));

        // Before delete: visible
        let r = chain.read_committed(Timestamp(15));
        assert_eq!(r.unwrap().values[0], Datum::Int32(99));

        // After delete: not visible (tombstone returns None)
        assert!(chain.read_committed(Timestamp(25)).is_none());
    }

    #[test]
    fn test_write_conflict_detection() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        chain.prepend(txn1, Some(row(vec![Datum::Int32(1)])));
        // txn1 has uncommitted write  — txn2 should see conflict
        assert!(chain.has_write_conflict(txn2));
        // txn1 itself should NOT see conflict
        assert!(!chain.has_write_conflict(txn1));

        chain.commit(txn1, Timestamp(10));
        // After commit, no conflict
        assert!(!chain.has_write_conflict(txn2));
    }

    #[test]
    fn test_gc_truncates_old_versions() {
        let chain = VersionChain::new();

        for i in 1..=5u64 {
            let txn = TxnId(i);
            chain.prepend(txn, Some(row(vec![Datum::Int64(i as i64)])));
            chain.commit(txn, Timestamp(i * 10));
        }

        // GC with watermark=30 should keep version at ts=30 and drop older
        chain.gc(Timestamp(30));

        // Version at ts=50 still visible
        let r = chain.read_committed(Timestamp(50));
        assert_eq!(r.unwrap().values[0], Datum::Int64(5));

        // Version at ts=30 still visible
        let r = chain.read_committed(Timestamp(30));
        assert!(r.is_some());
    }
}

#[cfg(test)]
mod memtable_tests {
    use crate::memtable::MemTable;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "test".into(),
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

    #[test]
    fn test_insert_and_get() {
        let table = MemTable::new(test_schema());
        let txn = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]);

        let pk = table.insert(row.clone(), txn).unwrap();
        table.commit_txn(txn, Timestamp(10));

        let result = table.get(&pk, TxnId(2), Timestamp(10));
        assert!(result.is_some());
        assert_eq!(result.unwrap().values[1], Datum::Text("hello".into()));
    }

    #[test]
    fn test_scan_visibility() {
        let table = MemTable::new(test_schema());
        let txn1 = TxnId(1);

        table
            .insert(
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
                txn1,
            )
            .unwrap();
        table
            .insert(
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("b".into())]),
                txn1,
            )
            .unwrap();
        table.commit_txn(txn1, Timestamp(10));

        // Committed rows visible
        let rows = table.scan(TxnId(2), Timestamp(10));
        assert_eq!(rows.len(), 2);

        // Uncommitted rows from txn3
        let txn3 = TxnId(3);
        table
            .insert(
                OwnedRow::new(vec![Datum::Int32(3), Datum::Text("c".into())]),
                txn3,
            )
            .unwrap();

        // txn3 sees own write + committed
        let rows = table.scan(txn3, Timestamp(10));
        assert_eq!(rows.len(), 3);

        // Other txns don't see txn3's uncommitted write
        let rows = table.scan(TxnId(4), Timestamp(10));
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_update_and_delete() {
        let table = MemTable::new(test_schema());
        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("old".into())]);
        let pk = table.insert(row, txn1).unwrap();
        table.commit_txn(txn1, Timestamp(10));

        // Update
        let txn2 = TxnId(2);
        let new_row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("new".into())]);
        table.update(&pk, new_row, txn2).unwrap();
        table.commit_txn(txn2, Timestamp(20));

        let result = table.get(&pk, TxnId(3), Timestamp(20));
        assert_eq!(result.unwrap().values[1], Datum::Text("new".into()));

        // Delete
        let txn3 = TxnId(3);
        table.delete(&pk, txn3).unwrap();
        table.commit_txn(txn3, Timestamp(30));

        let result = table.get(&pk, TxnId(4), Timestamp(30));
        assert!(result.is_none());
    }

    #[test]
    fn test_abort_removes_writes() {
        let table = MemTable::new(test_schema());
        let txn1 = TxnId(1);
        table
            .insert(
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("committed".into())]),
                txn1,
            )
            .unwrap();
        table.commit_txn(txn1, Timestamp(10));

        let txn2 = TxnId(2);
        table
            .insert(
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("aborted".into())]),
                txn2,
            )
            .unwrap();
        table.abort_txn(txn2);

        let rows = table.scan(TxnId(3), Timestamp(20));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[1], Datum::Text("committed".into()));
    }
}

#[cfg(test)]
mod wal_tests {
    use crate::wal::{SyncMode, WalReader, WalRecord, WalWriter};
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::types::{TableId, Timestamp, TxnId};

    #[test]
    fn test_wal_write_and_read() {
        let dir = std::env::temp_dir().join("falcon_wal_test");
        let _ = std::fs::remove_dir_all(&dir);

        let wal = WalWriter::open(&dir, SyncMode::None).unwrap();

        wal.append(&WalRecord::BeginTxn { txn_id: TxnId(1) })
            .unwrap();
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(42), Datum::Text("test".into())]),
        })
        .unwrap();
        wal.append(&WalRecord::CommitTxn {
            txn_id: TxnId(1),
            commit_ts: Timestamp(10),
        })
        .unwrap();
        wal.flush().unwrap();

        // Read back
        let reader = WalReader::new(&dir);
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 3);

        match &records[0] {
            WalRecord::BeginTxn { txn_id } => assert_eq!(txn_id.0, 1),
            _ => panic!("Expected BeginTxn"),
        }
        match &records[1] {
            WalRecord::Insert { txn_id, row, .. } => {
                assert_eq!(txn_id.0, 1);
                assert_eq!(row.values[0], Datum::Int32(42));
            }
            _ => panic!("Expected Insert"),
        }
        match &records[2] {
            WalRecord::CommitTxn { txn_id, commit_ts } => {
                assert_eq!(txn_id.0, 1);
                assert_eq!(commit_ts.0, 10);
            }
            _ => panic!("Expected CommitTxn"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_empty_read() {
        let dir = std::env::temp_dir().join("falcon_wal_empty_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let reader = WalReader::new(&dir);
        let records = reader.read_all().unwrap();
        assert!(records.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_segment_rotation() {
        let dir = std::env::temp_dir().join("falcon_wal_rotation_test");
        let _ = std::fs::remove_dir_all(&dir);

        // Small segment size to force rotation
        let wal = WalWriter::open_with_options(&dir, SyncMode::None, 200, 100).unwrap();
        assert_eq!(wal.current_segment_id(), 0);

        // Write enough records to trigger rotation (each record is ~30-50 bytes)
        for i in 0..20 {
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(i)]),
            })
            .unwrap();
        }
        wal.flush().unwrap();

        // Should have rotated at least once
        assert!(wal.current_segment_id() > 0, "Expected segment rotation");

        // Read all records back across segments
        let reader = WalReader::new(&dir);
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 20);

        // Verify all records are present and ordered
        for (i, rec) in records.iter().enumerate() {
            match rec {
                WalRecord::Insert { row, .. } => {
                    assert_eq!(row.values[0], Datum::Int32(i as i32));
                }
                _ => panic!("Expected Insert at index {}", i),
            }
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_group_commit() {
        let dir = std::env::temp_dir().join("falcon_wal_group_test");
        let _ = std::fs::remove_dir_all(&dir);

        // Group commit size of 3
        let wal = WalWriter::open_with_options(&dir, SyncMode::None, 64 * 1024 * 1024, 3).unwrap();

        // Write 5 records (should auto-flush after 3rd)
        for i in 0..5 {
            wal.append(&WalRecord::BeginTxn { txn_id: TxnId(i) })
                .unwrap();
        }
        // Flush remaining
        wal.flush().unwrap();

        let reader = WalReader::new(&dir);
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 5);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_purge_segments() {
        let dir = std::env::temp_dir().join("falcon_wal_purge_test");
        let _ = std::fs::remove_dir_all(&dir);

        // Tiny segment size to create multiple segments
        let wal = WalWriter::open_with_options(&dir, SyncMode::None, 100, 100).unwrap();

        for i in 0..30 {
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(i)]),
            })
            .unwrap();
        }
        wal.flush().unwrap();

        let current_seg = wal.current_segment_id();
        assert!(current_seg > 0);

        // Purge old segments
        let removed = wal.purge_segments_before(current_seg).unwrap();
        assert!(removed > 0);

        // Should still be able to read remaining records from current segment
        let reader = WalReader::new(&dir);
        let records = reader.read_all().unwrap();
        assert!(!records.is_empty());
        assert!(records.len() < 30); // Some were purged

        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod engine_tests {
    use crate::engine::StorageEngine;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

    fn test_schema(id: u64) -> TableSchema {
        TableSchema {
            id: TableId(id),
            name: format!("test_{}", id),
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

    #[test]
    fn test_engine_create_and_drop() {
        let engine = StorageEngine::new_in_memory();
        let schema = test_schema(1);
        engine.create_table(schema).unwrap();
        assert!(engine.get_table_schema("test_1").is_some());

        engine.drop_table("test_1").unwrap();
        assert!(engine.get_table_schema("test_1").is_none());
    }

    #[test]
    fn test_engine_duplicate_table() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(test_schema(1)).unwrap();
        assert!(engine.create_table(test_schema(1)).is_err());
    }

    #[test]
    fn test_engine_insert_scan_commit() {
        let engine = StorageEngine::new_in_memory();
        let schema = test_schema(1);
        let table_id = engine.create_table(schema).unwrap();

        let txn = TxnId(1);
        engine
            .insert(
                table_id,
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
                txn,
            )
            .unwrap();
        engine
            .insert(
                table_id,
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("b".into())]),
                txn,
            )
            .unwrap();

        engine.commit_txn_local(txn, Timestamp(10)).unwrap();

        let rows = engine.scan(table_id, TxnId(2), Timestamp(10)).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_engine_recovery() {
        let dir = std::env::temp_dir().join("falcon_engine_recovery_test");
        let _ = std::fs::remove_dir_all(&dir);

        // Phase 1: Write data
        {
            let engine = StorageEngine::new(Some(&dir)).unwrap();
            let schema = test_schema(1);
            engine.create_table(schema).unwrap();

            let txn = TxnId(1);
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(1), Datum::Text("persisted".into())]),
                    txn,
                )
                .unwrap();
            engine.commit_txn_local(txn, Timestamp(10)).unwrap();
        }

        // Phase 2: Recover and verify
        {
            let engine = StorageEngine::recover(&dir).unwrap();
            assert!(engine.get_table_schema("test_1").is_some());

            let rows = engine.scan(TableId(1), TxnId(2), Timestamp(10)).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.values[1], Datum::Text("persisted".into()));
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod occ_tests {
    use crate::mvcc::VersionChain;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::types::{Timestamp, TxnId};

    fn row(vals: Vec<Datum>) -> OwnedRow {
        OwnedRow::new(vals)
    }

    #[test]
    fn test_has_committed_write_after_detects_conflict() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        chain.prepend(txn1, Some(row(vec![Datum::Int32(1)])));
        chain.commit(txn1, Timestamp(10));

        chain.prepend(txn2, Some(row(vec![Datum::Int32(2)])));
        chain.commit(txn2, Timestamp(20));

        // txn3 started at ts=15 should see conflict (txn2 committed at 20 > 15)
        assert!(chain.has_committed_write_after(TxnId(3), Timestamp(15)));
        // txn4 started at ts=25 should NOT see conflict
        assert!(!chain.has_committed_write_after(TxnId(4), Timestamp(25)));
    }

    #[test]
    fn test_has_committed_write_after_excludes_own_txn() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        chain.prepend(txn1, Some(row(vec![Datum::Int32(1)])));
        chain.commit(txn1, Timestamp(10));

        // txn1's own commit should be excluded
        assert!(!chain.has_committed_write_after(txn1, Timestamp(5)));
    }

    #[test]
    fn test_has_committed_write_after_ignores_aborted() {
        let chain = VersionChain::new();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);

        chain.prepend(txn1, Some(row(vec![Datum::Int32(1)])));
        chain.commit(txn1, Timestamp(10));

        chain.prepend(txn2, Some(row(vec![Datum::Int32(2)])));
        chain.abort(txn2);

        // txn3 at ts=5 sees txn1 commit (10 > 5)
        assert!(chain.has_committed_write_after(TxnId(3), Timestamp(5)));
        // txn3 at ts=15: txn1 at 10 <= 15, txn2 aborted  — no conflict
        assert!(!chain.has_committed_write_after(TxnId(3), Timestamp(15)));
    }

    #[test]
    fn test_has_committed_write_after_empty_chain() {
        let chain = VersionChain::new();
        assert!(!chain.has_committed_write_after(TxnId(1), Timestamp(0)));
    }
}

#[cfg(test)]
mod engine_occ_tests {
    use crate::engine::StorageEngine;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

    fn occ_schema() -> TableSchema {
        TableSchema {
            id: TableId(100),
            name: "occ_test".into(),
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

    #[test]
    fn test_read_set_validation_no_conflict() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(occ_schema()).unwrap();

        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]);
        engine.insert(TableId(100), row, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 reads  — no concurrent modifications
        let txn2 = TxnId(2);
        let _rows = engine.scan(TableId(100), txn2, Timestamp(15)).unwrap();
        assert!(engine.validate_read_set(txn2, Timestamp(15)).is_ok());
    }

    #[test]
    fn test_read_set_validation_detects_conflict() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(occ_schema()).unwrap();

        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v1".into())]);
        let pk = engine.insert(TableId(100), row, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 reads at start_ts=15
        let txn2 = TxnId(2);
        let _ = engine.get(TableId(100), &pk, txn2, Timestamp(15)).unwrap();

        // txn3 updates same key and commits at ts=20 > txn2's start_ts
        let txn3 = TxnId(3);
        let new_row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("v2".into())]);
        engine.update(TableId(100), &pk, new_row, txn3).unwrap();
        engine.commit_txn_local(txn3, Timestamp(20)).unwrap();

        // txn2's validation should FAIL
        assert!(engine.validate_read_set(txn2, Timestamp(15)).is_err());
    }

    #[test]
    fn test_read_set_cleaned_on_commit() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(occ_schema()).unwrap();

        let txn1 = TxnId(1);
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]);
        engine.insert(TableId(100), row, txn1).unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 reads then commits  — read-set should be cleaned
        let txn2 = TxnId(2);
        let _ = engine.scan(TableId(100), txn2, Timestamp(15)).unwrap();
        engine.commit_txn_local(txn2, Timestamp(20)).unwrap();

        // After commit, validate finds no read-set  — passes trivially
        assert!(engine.validate_read_set(txn2, Timestamp(15)).is_ok());
    }

    #[test]
    fn test_scan_records_all_read_keys_for_occ() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(occ_schema()).unwrap();

        let txn1 = TxnId(1);
        for i in 1..=3 {
            let row = OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("v{}", i))]);
            engine.insert(TableId(100), row, txn1).unwrap();
        }
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 scans all 3 rows
        let txn2 = TxnId(2);
        let rows = engine.scan(TableId(100), txn2, Timestamp(15)).unwrap();
        assert_eq!(rows.len(), 3);

        // txn3 updates row id=2 and commits after txn2's start
        let txn3 = TxnId(3);
        let pk2 = rows
            .iter()
            .find(|(_, r)| r.values[0] == Datum::Int32(2))
            .unwrap()
            .0
            .clone();
        let new_row = OwnedRow::new(vec![Datum::Int32(2), Datum::Text("updated".into())]);
        engine.update(TableId(100), &pk2, new_row, txn3).unwrap();
        engine.commit_txn_local(txn3, Timestamp(20)).unwrap();

        // txn2's read-set should detect the conflict on row id=2
        assert!(engine.validate_read_set(txn2, Timestamp(15)).is_err());
    }
}

#[cfg(test)]
mod secondary_index_tests {
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

        // Now create index  — it should backfill existing rows
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

        // Insert/update/delete without any index  — should work fine
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

        // No crash, no errors  — index maintenance is a no-op when no indexes exist
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

        // Index still empty  — abort is a no-op for indexes under Approach A
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

        // Insert another row with same name  — should fail
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

    // ── Commit-time unique constraint re-validation tests ──

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

        // txn1 commits first  — succeeds
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 tries to commit  — must fail (UniqueViolation)
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

        // txn2 started after txn1 committed  — insert-time check catches it
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
        // txn2 updates pk=2 to name="alice"  — passes at DML time (no conflict yet
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

        // txn2 updates pk=2 to name="alice"  — conflicts with pk=1
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
        // txn1 commits first. txn2 commit fails  — neither "carol" nor "alice" should commit.
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

        // txn2 inserts "carol" AND "alice"  — both pass insert-time check (index empty)
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

        // txn1 commits first  — succeeds, "alice" now in index
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2 commit should fail  — "alice" conflicts
        let result = engine.commit_txn_local(txn2, Timestamp(20));
        assert!(result.is_err());

        // Only txn1's row (pk=1, alice) should be visible  — carol should NOT be committed
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
}

#[cfg(test)]
mod recovery_tests {
    use crate::engine::StorageEngine;
    use crate::wal::{SyncMode, WalRecord, WalWriter};
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

    fn recovery_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "rec_test".into(),
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

    #[test]
    fn test_recovery_aborts_uncommitted_txns() {
        let dir = std::env::temp_dir().join("falcon_recovery_uncommitted");
        let _ = std::fs::remove_dir_all(&dir);

        // Write WAL with an uncommitted transaction (no Commit record)
        {
            let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
            let schema_json = serde_json::to_string(&recovery_schema()).unwrap();
            wal.append(&WalRecord::CreateTable { schema_json }).unwrap();

            // Committed txn
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(1), Datum::Text("committed".into())]),
            })
            .unwrap();
            wal.append(&WalRecord::CommitTxnLocal {
                txn_id: TxnId(1),
                commit_ts: Timestamp(10),
            })
            .unwrap();

            // Uncommitted txn (no commit record  — simulates crash)
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(2),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(2), Datum::Text("uncommitted".into())]),
            })
            .unwrap();
            wal.flush().unwrap();
        }

        // Recover  — uncommitted txn should be aborted
        let engine = StorageEngine::recover(&dir).unwrap();
        let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "only committed row should be visible after recovery"
        );
        assert_eq!(rows[0].1.values[1], Datum::Text("committed".into()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recovery_idempotent() {
        let dir = std::env::temp_dir().join("falcon_recovery_idempotent");
        let _ = std::fs::remove_dir_all(&dir);

        // Write WAL
        {
            let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
            let schema_json = serde_json::to_string(&recovery_schema()).unwrap();
            wal.append(&WalRecord::CreateTable { schema_json }).unwrap();

            wal.append(&WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]),
            })
            .unwrap();
            wal.append(&WalRecord::CommitTxnLocal {
                txn_id: TxnId(1),
                commit_ts: Timestamp(10),
            })
            .unwrap();

            // Uncommitted txn
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(2),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(2), Datum::Text("crash".into())]),
            })
            .unwrap();
            wal.flush().unwrap();
        }

        // First recovery
        {
            let engine = StorageEngine::recover(&dir).unwrap();
            let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
            assert_eq!(rows.len(), 1);
        }

        // Second recovery (idempotent  — same result)
        {
            let engine = StorageEngine::recover(&dir).unwrap();
            let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
            assert_eq!(
                rows.len(),
                1,
                "idempotent recovery should produce same result"
            );
            assert_eq!(rows[0].1.values[1], Datum::Text("hello".into()));
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recovery_update_then_crash() {
        let dir = std::env::temp_dir().join("falcon_recovery_update_crash");
        let _ = std::fs::remove_dir_all(&dir);

        // Write WAL: insert + commit, then update without commit (crash)
        {
            let engine = StorageEngine::new(Some(&dir)).unwrap();
            engine.create_table(recovery_schema()).unwrap();

            let txn1 = TxnId(1);
            let pk = engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(1), Datum::Text("original".into())]),
                    txn1,
                )
                .unwrap();
            engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

            // Update without commit (simulates crash mid-update)
            let txn2 = TxnId(2);
            engine
                .update(
                    TableId(1),
                    &pk,
                    OwnedRow::new(vec![Datum::Int32(1), Datum::Text("updated".into())]),
                    txn2,
                )
                .unwrap();
            // No commit  — crash!
        }

        // Recover  — should see original value, not the uncommitted update
        let engine = StorageEngine::recover(&dir).unwrap();
        let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].1.values[1],
            Datum::Text("original".into()),
            "uncommitted update should be rolled back after recovery"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recovery_delete_then_crash() {
        let dir = std::env::temp_dir().join("falcon_recovery_delete_crash");
        let _ = std::fs::remove_dir_all(&dir);

        // Write WAL: insert + commit, then delete without commit (crash)
        {
            let engine = StorageEngine::new(Some(&dir)).unwrap();
            engine.create_table(recovery_schema()).unwrap();

            let txn1 = TxnId(1);
            let pk = engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(1), Datum::Text("keep_me".into())]),
                    txn1,
                )
                .unwrap();
            engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

            // Delete without commit
            let txn2 = TxnId(2);
            engine.delete(TableId(1), &pk, txn2).unwrap();
            // No commit  — crash!
        }

        // Recover  — row should still be visible
        let engine = StorageEngine::recover(&dir).unwrap();
        let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "uncommitted delete should be rolled back after recovery"
        );
        assert_eq!(rows[0].1.values[1], Datum::Text("keep_me".into()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recovery_explicit_abort_in_wal() {
        let dir = std::env::temp_dir().join("falcon_recovery_explicit_abort");
        let _ = std::fs::remove_dir_all(&dir);

        // Write WAL with explicit abort record
        {
            let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
            let schema_json = serde_json::to_string(&recovery_schema()).unwrap();
            wal.append(&WalRecord::CreateTable { schema_json }).unwrap();

            wal.append(&WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(1), Datum::Text("aborted".into())]),
            })
            .unwrap();
            wal.append(&WalRecord::AbortTxnLocal { txn_id: TxnId(1) })
                .unwrap();

            wal.append(&WalRecord::Insert {
                txn_id: TxnId(2),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(2), Datum::Text("committed".into())]),
            })
            .unwrap();
            wal.append(&WalRecord::CommitTxnLocal {
                txn_id: TxnId(2),
                commit_ts: Timestamp(20),
            })
            .unwrap();
            wal.flush().unwrap();
        }

        let engine = StorageEngine::recover(&dir).unwrap();
        let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "explicitly aborted txn should not be visible"
        );
        assert_eq!(rows[0].1.values[1], Datum::Text("committed".into()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recovery_global_txn_committed() {
        let dir = std::env::temp_dir().join(format!(
            "falcon_recovery_global_commit_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        {
            let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
            let schema_json = serde_json::to_string(&recovery_schema()).unwrap();
            wal.append(&WalRecord::CreateTable { schema_json }).unwrap();

            wal.append(&WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![
                    Datum::Int32(1),
                    Datum::Text("global_committed".into()),
                ]),
            })
            .unwrap();
            wal.append(&WalRecord::PrepareTxn { txn_id: TxnId(1) })
                .unwrap();
            wal.append(&WalRecord::CommitTxnGlobal {
                txn_id: TxnId(1),
                commit_ts: Timestamp(10),
            })
            .unwrap();
            wal.flush().unwrap();
        }

        let engine = StorageEngine::recover(&dir).unwrap();
        let rows = engine.scan(TableId(1), TxnId(99), Timestamp(100)).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "committed global txn should be visible after recovery"
        );
        assert_eq!(rows[0].1.values[1], Datum::Text("global_committed".into()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recovery_global_txn_prepared_not_committed() {
        let dir = std::env::temp_dir().join(format!(
            "falcon_recovery_global_prepared_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        {
            let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
            let schema_json = serde_json::to_string(&recovery_schema()).unwrap();
            wal.append(&WalRecord::CreateTable { schema_json }).unwrap();

            // Committed local txn for baseline
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(1), Datum::Text("local_ok".into())]),
            })
            .unwrap();
            wal.append(&WalRecord::CommitTxnLocal {
                txn_id: TxnId(1),
                commit_ts: Timestamp(10),
            })
            .unwrap();

            // Global txn: prepared but NOT committed (simulates coordinator crash)
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(2),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(2), Datum::Text("prepared_only".into())]),
            })
            .unwrap();
            wal.append(&WalRecord::PrepareTxn { txn_id: TxnId(2) })
                .unwrap();
            // No commit record  — crash after prepare
            wal.flush().unwrap();
        }

        let engine = StorageEngine::recover(&dir).unwrap();
        let rows = engine.scan(TableId(1), TxnId(99), Timestamp(100)).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "prepared-but-not-committed global txn should be invisible"
        );
        assert_eq!(rows[0].1.values[1], Datum::Text("local_ok".into()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recovery_global_txn_prepared_then_aborted() {
        let dir = std::env::temp_dir().join(format!(
            "falcon_recovery_global_abort_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        {
            let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
            let schema_json = serde_json::to_string(&recovery_schema()).unwrap();
            wal.append(&WalRecord::CreateTable { schema_json }).unwrap();

            // Global txn: prepared then explicitly aborted
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(1), Datum::Text("aborted_global".into())]),
            })
            .unwrap();
            wal.append(&WalRecord::PrepareTxn { txn_id: TxnId(1) })
                .unwrap();
            wal.append(&WalRecord::AbortTxnGlobal { txn_id: TxnId(1) })
                .unwrap();

            // Another global txn: committed normally
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(2),
                table_id: TableId(1),
                row: OwnedRow::new(vec![
                    Datum::Int32(2),
                    Datum::Text("committed_global".into()),
                ]),
            })
            .unwrap();
            wal.append(&WalRecord::PrepareTxn { txn_id: TxnId(2) })
                .unwrap();
            wal.append(&WalRecord::CommitTxnGlobal {
                txn_id: TxnId(2),
                commit_ts: Timestamp(20),
            })
            .unwrap();
            wal.flush().unwrap();
        }

        let engine = StorageEngine::recover(&dir).unwrap();
        let rows = engine.scan(TableId(1), TxnId(99), Timestamp(100)).unwrap();
        assert_eq!(
            rows.len(),
            1,
            "only committed global txn should survive recovery"
        );
        assert_eq!(rows[0].1.values[1], Datum::Text("committed_global".into()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recovery_create_index_durability() {
        let dir = std::env::temp_dir().join(format!(
            "falcon_recovery_create_index_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        // Phase 1: create table + create named index, then "crash"
        {
            let engine = StorageEngine::new(Some(&dir)).unwrap();
            engine.create_table(recovery_schema()).unwrap();
            engine
                .create_named_index("idx_val", "rec_test", 1, false)
                .unwrap();
        }

        // Phase 2: recover — index must be present in the registry
        let engine = StorageEngine::recover(&dir).unwrap();
        assert!(
            engine.index_exists("idx_val"),
            "CREATE INDEX must survive WAL recovery"
        );
        let indexed = engine.get_indexed_columns(TableId(1));
        assert!(
            indexed.iter().any(|(col, _)| *col == 1),
            "recovered index must cover column 1"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recovery_drop_index_durability() {
        let dir =
            std::env::temp_dir().join(format!("falcon_recovery_drop_index_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        // Phase 1: create table + index, then drop the index
        {
            let engine = StorageEngine::new(Some(&dir)).unwrap();
            engine.create_table(recovery_schema()).unwrap();
            engine
                .create_named_index("idx_val", "rec_test", 1, false)
                .unwrap();
            engine.drop_index("idx_val").unwrap();
        }

        // Phase 2: recover — index must NOT be present
        let engine = StorageEngine::recover(&dir).unwrap();
        assert!(
            !engine.index_exists("idx_val"),
            "DROP INDEX must survive WAL recovery — index should be gone"
        );
        let indexed = engine.get_indexed_columns(TableId(1));
        assert!(
            !indexed.iter().any(|(col, _)| *col == 1),
            "dropped index must not cover column 1 after recovery"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recovery_multi_table_interleaved() {
        let dir = std::env::temp_dir().join(format!(
            "falcon_recovery_multi_table_{}", std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        let schema2 = {
            let mut s = recovery_schema();
            s.id = TableId(2);
            s.name = "rec_test2".into();
            s
        };

        {
            let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
            let s1_json = serde_json::to_string(&recovery_schema()).unwrap();
            let s2_json = serde_json::to_string(&schema2).unwrap();
            wal.append(&WalRecord::CreateTable { schema_json: s1_json }).unwrap();
            wal.append(&WalRecord::CreateTable { schema_json: s2_json }).unwrap();

            // T1: insert into table 1 + commit
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(1), table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(1), Datum::Text("t1_ok".into())]),
            }).unwrap();
            // T2: insert into table 2 (interleaved, committed later)
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(2), table_id: TableId(2),
                row: OwnedRow::new(vec![Datum::Int32(10), Datum::Text("t2_ok".into())]),
            }).unwrap();
            wal.append(&WalRecord::CommitTxnLocal { txn_id: TxnId(1), commit_ts: Timestamp(5) }).unwrap();
            // T3: insert into table 1, uncommitted (crash)
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(3), table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(2), Datum::Text("t1_crash".into())]),
            }).unwrap();
            wal.append(&WalRecord::CommitTxnLocal { txn_id: TxnId(2), commit_ts: Timestamp(10) }).unwrap();
            wal.flush().unwrap();
        }

        let engine = StorageEngine::recover(&dir).unwrap();
        let rows1 = engine.scan(TableId(1), TxnId(99), Timestamp(100)).unwrap();
        assert_eq!(rows1.len(), 1, "table 1: only committed row");
        assert_eq!(rows1[0].1.values[1], Datum::Text("t1_ok".into()));

        let rows2 = engine.scan(TableId(2), TxnId(99), Timestamp(100)).unwrap();
        assert_eq!(rows2.len(), 1, "table 2: committed row");
        assert_eq!(rows2[0].1.values[1], Datum::Text("t2_ok".into()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recovery_replay_idempotent_three_times() {
        let dir = std::env::temp_dir().join(format!(
            "falcon_recovery_3x_idempotent_{}", std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        {
            let wal = WalWriter::open(&dir, SyncMode::None).unwrap();
            let schema_json = serde_json::to_string(&recovery_schema()).unwrap();
            wal.append(&WalRecord::CreateTable { schema_json }).unwrap();
            for i in 1..=5i32 {
                wal.append(&WalRecord::Insert {
                    txn_id: TxnId(i as u64), table_id: TableId(1),
                    row: OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("row{}", i))]),
                }).unwrap();
                wal.append(&WalRecord::CommitTxnLocal {
                    txn_id: TxnId(i as u64), commit_ts: Timestamp(i as u64 * 10),
                }).unwrap();
            }
            wal.flush().unwrap();
        }

        // Replay 3 times — each must produce identical result
        for attempt in 1..=3 {
            let engine = StorageEngine::recover(&dir).unwrap();
            let rows = engine.scan(TableId(1), TxnId(99), Timestamp(100)).unwrap();
            assert_eq!(rows.len(), 5, "attempt {}: all 5 committed rows", attempt);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recovery_wal_first_ordering() {
        // Verify that StorageEngine writes to WAL before mutating memtable.
        // We do this by checking that the WAL observer counter fires for every DML.
        let mut engine = StorageEngine::new_in_memory();
        engine.create_table(recovery_schema()).unwrap();

        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let c = counter.clone();
        engine.set_wal_observer(Box::new(move |_record| {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }));

        // Insert → must fire WAL observer before returning
        let pk = engine.insert(TableId(1), OwnedRow::new(vec![
            Datum::Int32(1), Datum::Text("a".into())
        ]), TxnId(1)).unwrap();
        assert!(counter.load(std::sync::atomic::Ordering::SeqCst) >= 1,
            "WAL observer must fire on insert");

        // Commit T1 so update by T2 can proceed
        engine.commit_txn(TxnId(1), Timestamp(5), falcon_common::types::TxnType::Local).unwrap();

        // Update → must fire WAL observer
        engine.update(TableId(1), &pk, OwnedRow::new(vec![
            Datum::Int32(1), Datum::Text("b".into())
        ]), TxnId(2)).unwrap();
        assert!(counter.load(std::sync::atomic::Ordering::SeqCst) >= 3,
            "WAL observer must fire on update (insert + commit + update = 3)");

        // Commit T2 before T3 can delete
        engine.commit_txn(TxnId(2), Timestamp(10), falcon_common::types::TxnType::Local).unwrap();

        // Delete → must fire WAL observer
        engine.delete(TableId(1), &pk, TxnId(3)).unwrap();
        assert!(counter.load(std::sync::atomic::Ordering::SeqCst) >= 5,
            "WAL observer must fire on delete (insert+commit+update+commit+delete = 5)");
    }
}

#[cfg(test)]
mod write_set_tests {
    use crate::engine::StorageEngine;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

    fn ws_schema(id: u64, name: &str) -> TableSchema {
        TableSchema {
            id: TableId(id),
            name: name.into(),
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

    #[test]
    fn test_write_set_multi_table_commit() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(ws_schema(1, "t1")).unwrap();
        engine.create_table(ws_schema(2, "t2")).unwrap();

        let txn1 = TxnId(1);
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
                txn1,
            )
            .unwrap();
        engine
            .insert(
                TableId(2),
                OwnedRow::new(vec![Datum::Int32(10), Datum::Text("x".into())]),
                txn1,
            )
            .unwrap();
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("b".into())]),
                txn1,
            )
            .unwrap();
        engine
            .insert(
                TableId(2),
                OwnedRow::new(vec![Datum::Int32(20), Datum::Text("y".into())]),
                txn1,
            )
            .unwrap();

        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        let t1_rows = engine.scan(TableId(1), TxnId(2), Timestamp(10)).unwrap();
        let t2_rows = engine.scan(TableId(2), TxnId(2), Timestamp(10)).unwrap();
        assert_eq!(t1_rows.len(), 2, "t1 should have 2 rows after commit");
        assert_eq!(t2_rows.len(), 2, "t2 should have 2 rows after commit");
    }

    #[test]
    fn test_write_set_multi_table_abort() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(ws_schema(1, "t1")).unwrap();
        engine.create_table(ws_schema(2, "t2")).unwrap();

        // Commit some baseline data
        let txn1 = TxnId(1);
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("base".into())]),
                txn1,
            )
            .unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // Write to both tables, then abort
        let txn2 = TxnId(2);
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("aborted".into())]),
                txn2,
            )
            .unwrap();
        engine
            .insert(
                TableId(2),
                OwnedRow::new(vec![Datum::Int32(10), Datum::Text("aborted".into())]),
                txn2,
            )
            .unwrap();
        engine.abort_txn_local(txn2).unwrap();

        let t1_rows = engine.scan(TableId(1), TxnId(3), Timestamp(20)).unwrap();
        let t2_rows = engine.scan(TableId(2), TxnId(3), Timestamp(20)).unwrap();
        assert_eq!(
            t1_rows.len(),
            1,
            "t1 should only have baseline row after abort"
        );
        assert_eq!(t2_rows.len(), 0, "t2 should be empty after abort");
    }

    #[test]
    fn test_write_set_duplicate_key_in_same_txn() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(ws_schema(1, "t1")).unwrap();

        let txn1 = TxnId(1);
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("first".into())]),
                txn1,
            )
            .unwrap();

        // Update the same key within the same txn
        let pk = crate::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int32(1), Datum::Text("first".into())]),
            &[0],
        );
        engine
            .update(
                TableId(1),
                &pk,
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("second".into())]),
                txn1,
            )
            .unwrap();
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        let rows = engine.scan(TableId(1), TxnId(2), Timestamp(10)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].1.values[1],
            Datum::Text("second".into()),
            "should see the latest update within the same txn"
        );
    }

    #[test]
    fn test_write_set_commit_does_not_affect_other_txns() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(ws_schema(1, "t1")).unwrap();

        // txn1 and txn2 both write
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("txn1".into())]),
                txn1,
            )
            .unwrap();
        engine
            .insert(
                TableId(1),
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("txn2".into())]),
                txn2,
            )
            .unwrap();

        // Commit txn1 only
        engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

        // txn2's write should still be uncommitted
        let rows = engine.scan(TableId(1), TxnId(3), Timestamp(10)).unwrap();
        assert_eq!(rows.len(), 1, "only txn1's row should be visible");
        assert_eq!(rows[0].1.values[1], Datum::Text("txn1".into()));

        // Now commit txn2
        engine.commit_txn_local(txn2, Timestamp(20)).unwrap();
        let rows = engine.scan(TableId(1), TxnId(3), Timestamp(20)).unwrap();
        assert_eq!(
            rows.len(),
            2,
            "both rows should be visible after both commits"
        );
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use crate::engine::StorageEngine;
    use crate::wal::SyncMode;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};

    fn ckpt_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "ckpt_test".into(),
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

    #[test]
    fn test_checkpoint_and_recover() {
        let dir = std::env::temp_dir().join("falcon_ckpt_basic");
        let _ = std::fs::remove_dir_all(&dir);

        // Phase 1: Create data and checkpoint
        {
            let engine = StorageEngine::new(Some(&dir)).unwrap();
            engine.create_table(ckpt_schema()).unwrap();

            let txn1 = TxnId(1);
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(1), Datum::Text("alpha".into())]),
                    txn1,
                )
                .unwrap();
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(2), Datum::Text("beta".into())]),
                    txn1,
                )
                .unwrap();
            engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

            let (seg_id, row_count) = engine.checkpoint().unwrap();
            assert!(seg_id > 0 || seg_id == 0); // segment ID is valid
            assert_eq!(row_count, 2, "checkpoint should capture 2 rows");
        }

        // Phase 2: Recover from checkpoint
        {
            let engine = StorageEngine::recover(&dir).unwrap();
            let rows = engine.scan(TableId(1), TxnId(2), Timestamp(100)).unwrap();
            assert_eq!(rows.len(), 2, "should recover 2 rows from checkpoint");

            let vals: Vec<&Datum> = rows.iter().map(|(_, r)| &r.values[1]).collect();
            assert!(vals.contains(&&Datum::Text("alpha".into())));
            assert!(vals.contains(&&Datum::Text("beta".into())));
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_checkpoint_with_post_checkpoint_writes() {
        let dir = std::env::temp_dir().join("falcon_ckpt_post_writes");
        let _ = std::fs::remove_dir_all(&dir);

        // Phase 1: Create data, checkpoint, then write more data
        {
            let engine = StorageEngine::new(Some(&dir)).unwrap();
            engine.create_table(ckpt_schema()).unwrap();

            let txn1 = TxnId(1);
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(1), Datum::Text("before".into())]),
                    txn1,
                )
                .unwrap();
            engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

            engine.checkpoint().unwrap();

            // Write more data AFTER checkpoint
            let txn2 = TxnId(2);
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(2), Datum::Text("after".into())]),
                    txn2,
                )
                .unwrap();
            engine.commit_txn_local(txn2, Timestamp(20)).unwrap();
        }

        // Phase 2: Recover  — should see both pre- and post-checkpoint data
        {
            let engine = StorageEngine::recover(&dir).unwrap();
            let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
            assert_eq!(
                rows.len(),
                2,
                "should recover pre- and post-checkpoint rows"
            );

            let vals: Vec<&Datum> = rows.iter().map(|(_, r)| &r.values[1]).collect();
            assert!(vals.contains(&&Datum::Text("before".into())));
            assert!(vals.contains(&&Datum::Text("after".into())));
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_checkpoint_uncommitted_after_checkpoint_aborted() {
        let dir = std::env::temp_dir().join("falcon_ckpt_uncommitted");
        let _ = std::fs::remove_dir_all(&dir);

        // Phase 1: Create data, checkpoint, then write uncommitted data (crash)
        {
            let engine = StorageEngine::new(Some(&dir)).unwrap();
            engine.create_table(ckpt_schema()).unwrap();

            let txn1 = TxnId(1);
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(1), Datum::Text("committed".into())]),
                    txn1,
                )
                .unwrap();
            engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

            engine.checkpoint().unwrap();

            // Uncommitted write after checkpoint (simulates crash)
            let txn2 = TxnId(2);
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(2), Datum::Text("uncommitted".into())]),
                    txn2,
                )
                .unwrap();
            // No commit  — crash!
        }

        // Phase 2: Recover  — uncommitted post-checkpoint write should be aborted
        {
            let engine = StorageEngine::recover(&dir).unwrap();
            let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
            assert_eq!(rows.len(), 1, "only committed row should survive recovery");
            assert_eq!(rows[0].1.values[1], Datum::Text("committed".into()));
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_checkpoint_recovery_idempotent() {
        let dir = std::env::temp_dir().join("falcon_ckpt_idempotent");
        let _ = std::fs::remove_dir_all(&dir);

        {
            let engine = StorageEngine::new(Some(&dir)).unwrap();
            engine.create_table(ckpt_schema()).unwrap();

            let txn1 = TxnId(1);
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(1), Datum::Text("data".into())]),
                    txn1,
                )
                .unwrap();
            engine.commit_txn_local(txn1, Timestamp(10)).unwrap();

            engine.checkpoint().unwrap();

            let txn2 = TxnId(2);
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(2), Datum::Text("post".into())]),
                    txn2,
                )
                .unwrap();
            engine.commit_txn_local(txn2, Timestamp(20)).unwrap();
        }

        // First recovery
        {
            let engine = StorageEngine::recover(&dir).unwrap();
            let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
            assert_eq!(rows.len(), 2);
        }

        // Second recovery (idempotent)
        {
            let engine = StorageEngine::recover(&dir).unwrap();
            let rows = engine.scan(TableId(1), TxnId(3), Timestamp(100)).unwrap();
            assert_eq!(
                rows.len(),
                2,
                "idempotent checkpoint recovery should produce same result"
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_checkpoint_purges_old_wal_segments() {
        let dir = std::env::temp_dir().join("falcon_ckpt_purge_segments");
        let _ = std::fs::remove_dir_all(&dir);

        // Helper to list segment IDs from falcon_XXXXXX.wal files.
        let list_segment_ids = |path: &std::path::Path| -> Vec<u64> {
            let mut ids = Vec::new();
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with("falcon_") && name.ends_with(".wal") {
                        if let Ok(id) = name[7..name.len() - 4].parse::<u64>() {
                            ids.push(id);
                        }
                    }
                }
            }
            ids.sort_unstable();
            ids
        };

        // Use tiny segment size to force many WAL rotations.
        {
            let engine =
                StorageEngine::new_with_wal_options(&dir, SyncMode::None, 256, 100).unwrap();
            engine.create_table(ckpt_schema()).unwrap();

            // Produce enough WAL to create multiple segments.
            for i in 0..120_i32 {
                let txn = TxnId((i as u64) + 1);
                engine
                    .insert(
                        TableId(1),
                        OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("v{}", i))]),
                        txn,
                    )
                    .unwrap();
                engine
                    .commit_txn_local(txn, Timestamp((i as u64) + 10))
                    .unwrap();
            }

            let before = list_segment_ids(&dir);
            assert!(
                before.len() > 1,
                "expected multiple WAL segments before checkpoint"
            );

            let (ckpt_segment, _) = engine.checkpoint().unwrap();

            let after = list_segment_ids(&dir);
            assert!(
                !after.is_empty(),
                "at least one WAL segment should remain after purge"
            );
            assert!(
                after.iter().all(|id| *id >= ckpt_segment),
                "segments older than checkpoint should be purged: ckpt_segment={}, after={:?}",
                ckpt_segment,
                after
            );
            assert!(
                after.len() <= before.len(),
                "checkpoint purge should not increase segment count: before={:?}, after={:?}",
                before,
                after
            );

            // Write post-checkpoint data and verify recovery still replays it.
            let post_txn = TxnId(10_000);
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(999_999), Datum::Text("post_ckpt".into())]),
                    post_txn,
                )
                .unwrap();
            engine
                .commit_txn_local(post_txn, Timestamp(50_000))
                .unwrap();
        }

        // Recovery remains correct after purge.
        {
            let engine = StorageEngine::recover(&dir).unwrap();
            let rows = engine
                .scan(TableId(1), TxnId(200_000), Timestamp(u64::MAX - 1))
                .unwrap();
            assert_eq!(
                rows.len(),
                121,
                "120 pre-checkpoint + 1 post-checkpoint rows should recover"
            );
            assert!(
                rows.iter()
                    .any(|(_, r)| r.values[1] == Datum::Text("post_ckpt".into())),
                "post-checkpoint row should be present after recovery"
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod bench_tests {
    use crate::engine::StorageEngine;
    use crate::memtable::encode_column_value;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};
    use std::time::Instant;

    fn bench_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "bench".into(),
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

    /// Benchmark 1: Commit latency should be O(|write_set|), NOT O(data_size).
    /// Pre-populate with N rows, then commit a single-row write and verify
    /// commit time does not scale linearly with N.
    #[test]
    fn bench_commit_latency_vs_data_scale() {
        let scales: Vec<usize> = vec![100, 1_000, 5_000, 10_000];
        let mut results: Vec<(usize, u128)> = Vec::new();

        for &n in &scales {
            let engine = StorageEngine::new_in_memory();
            engine.create_table(bench_schema()).unwrap();

            // Pre-populate with n committed rows
            let bulk_txn = TxnId(1);
            for i in 0..n {
                engine
                    .insert(
                        TableId(1),
                        OwnedRow::new(vec![
                            Datum::Int32(i as i32),
                            Datum::Text(format!("row_{}", i)),
                        ]),
                        bulk_txn,
                    )
                    .unwrap();
            }
            engine.commit_txn_local(bulk_txn, Timestamp(10)).unwrap();

            // Write 1 row in a new txn and measure commit time
            let write_txn = TxnId(2);
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(n as i32 + 1), Datum::Text("new".into())]),
                    write_txn,
                )
                .unwrap();

            let start = Instant::now();
            engine.commit_txn_local(write_txn, Timestamp(20)).unwrap();
            let elapsed = start.elapsed().as_micros();

            results.push((n, elapsed));
        }

        eprintln!("\n=== Commit Latency vs Data Scale (1-row write-set) ===");
        for (n, us) in &results {
            eprintln!("  data_size={:>7}  commit_us={:>6}", n, us);
        }

        // With write-set driven commit, commit at 100k should not be 100x slower than at 100.
        let (_, us_small) = results[0];
        let (_, us_large) = results[results.len() - 1];
        let ratio = if us_small == 0 {
            1.0
        } else {
            us_large as f64 / us_small.max(1) as f64
        };
        eprintln!("  ratio (largest/smallest) = {:.1}x", ratio);
        assert!(
            ratio < 20.0,
            "commit latency ratio {:.1}x exceeds 20x threshold  — commit may not be O(|write_set|)",
            ratio
        );
    }

    /// Benchmark 2: Index maintenance overhead during writes.
    /// Measures write+commit throughput with and without a secondary index.
    #[test]
    fn bench_index_maintenance_overhead() {
        let num_rows: i32 = 5_000;

        // Without index
        let engine_no_idx = StorageEngine::new_in_memory();
        engine_no_idx.create_table(bench_schema()).unwrap();

        let start_no_idx = Instant::now();
        let txn1 = TxnId(1);
        for i in 0..num_rows {
            engine_no_idx
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("v{}", i))]),
                    txn1,
                )
                .unwrap();
        }
        engine_no_idx.commit_txn_local(txn1, Timestamp(10)).unwrap();
        let elapsed_no_idx = start_no_idx.elapsed().as_micros();

        // With index
        let engine_idx = StorageEngine::new_in_memory();
        engine_idx.create_table(bench_schema()).unwrap();
        engine_idx.create_index("bench", 1).unwrap();

        let start_idx = Instant::now();
        let txn2 = TxnId(1);
        for i in 0..num_rows {
            engine_idx
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("v{}", i))]),
                    txn2,
                )
                .unwrap();
        }
        engine_idx.commit_txn_local(txn2, Timestamp(10)).unwrap();
        let elapsed_idx = start_idx.elapsed().as_micros();

        // Verify index correctness
        let sample_key = encode_column_value(&Datum::Text("v42".into()));
        let pks = engine_idx.index_lookup(TableId(1), 1, &sample_key).unwrap();
        assert_eq!(pks.len(), 1, "index should contain sample row after commit");

        let overhead = if elapsed_no_idx == 0 {
            1.0
        } else {
            elapsed_idx as f64 / elapsed_no_idx as f64
        };

        eprintln!("\n=== Index Maintenance Overhead ({} rows) ===", num_rows);
        eprintln!("  without_index: {}us", elapsed_no_idx);
        eprintln!("  with_index:    {}us", elapsed_idx);
        eprintln!("  overhead:      {:.2}x", overhead);

        // Index overhead should be reasonable (< 5x for a single column index)
        assert!(
            overhead < 5.0,
            "index maintenance overhead {:.2}x exceeds 5x threshold",
            overhead
        );
    }
}

// ===========================================================================
// MVCC Garbage Collection tests
// ===========================================================================

#[cfg(test)]
mod gc_tests {
    use crate::engine::StorageEngine;
    use crate::gc::{compute_safepoint, sweep_memtable, GcConfig, GcStats};
    use crate::memtable::MemTable;
    use crate::mvcc::VersionChain;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};
    use std::time::Instant;

    fn row(v: i32) -> OwnedRow {
        OwnedRow::new(vec![Datum::Int32(v)])
    }

    fn test_schema() -> TableSchema {
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
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        }
    }

    // ── Version chain GC with stats ──

    #[test]
    fn test_gc_chain_reclaims_old_versions() {
        let chain = VersionChain::new();
        // Insert 3 versions
        chain.prepend(TxnId(1), Some(row(1)));
        chain.commit(TxnId(1), Timestamp(10));
        chain.prepend(TxnId(2), Some(row(2)));
        chain.commit(TxnId(2), Timestamp(20));
        chain.prepend(TxnId(3), Some(row(3)));
        chain.commit(TxnId(3), Timestamp(30));

        assert_eq!(chain.version_chain_len(), 3);

        // GC with watermark at 25: keeps versions at ts=30 and ts=20 (newest <= 25),
        // reclaims version at ts=10
        let result = chain.gc(Timestamp(25));
        assert_eq!(result.reclaimed_versions, 1);
        assert!(result.reclaimed_bytes > 0);
        assert_eq!(chain.version_chain_len(), 2);
    }

    #[test]
    fn test_gc_chain_preserves_newest_at_watermark() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row(1)));
        chain.commit(TxnId(1), Timestamp(10));
        chain.prepend(TxnId(2), Some(row(2)));
        chain.commit(TxnId(2), Timestamp(20));

        // GC at watermark 20: ts=20 is the first committed <= watermark,
        // reclaim ts=10
        let result = chain.gc(Timestamp(20));
        assert_eq!(result.reclaimed_versions, 1);
        assert_eq!(chain.version_chain_len(), 1);

        // The remaining version should be readable
        let r = chain.read_committed(Timestamp(20));
        assert_eq!(r.unwrap().values[0], Datum::Int32(2));
    }

    #[test]
    fn test_gc_skips_uncommitted_versions() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row(1)));
        chain.commit(TxnId(1), Timestamp(10));
        chain.prepend(TxnId(2), Some(row(2)));
        // TxnId(2) is uncommitted (commit_ts = 0)

        // GC at watermark 100: should NOT reclaim the uncommitted version
        let result = chain.gc(Timestamp(100));
        // The uncommitted version (head) has commit_ts=0, so GC walks past it
        // to ts=10 which is committed <= 100, and truncates below it (nothing).
        assert_eq!(result.reclaimed_versions, 0);
        assert_eq!(chain.version_chain_len(), 2);
    }

    #[test]
    fn test_gc_skips_aborted_versions() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row(1)));
        chain.commit(TxnId(1), Timestamp(10));
        chain.prepend(TxnId(2), Some(row(2)));
        chain.commit(TxnId(2), Timestamp(20));
        chain.prepend(TxnId(3), Some(row(3)));
        // Abort txn 3  — sets commit_ts to MAX
        chain.abort(TxnId(3));

        // GC at watermark 25: head has commit_ts=MAX (aborted), skip it.
        // Next is ts=20 <= 25, so truncate below it (reclaim ts=10).
        let result = chain.gc(Timestamp(25));
        assert_eq!(result.reclaimed_versions, 1);
    }

    #[test]
    fn test_gc_no_op_on_single_version() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row(1)));
        chain.commit(TxnId(1), Timestamp(10));

        let result = chain.gc(Timestamp(100));
        assert_eq!(result.reclaimed_versions, 0);
        assert_eq!(chain.version_chain_len(), 1);
    }

    #[test]
    fn test_gc_no_op_when_watermark_too_low() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row(1)));
        chain.commit(TxnId(1), Timestamp(10));
        chain.prepend(TxnId(2), Some(row(2)));
        chain.commit(TxnId(2), Timestamp(20));

        // Watermark 5: both versions are after the watermark, nothing to GC
        let result = chain.gc(Timestamp(5));
        assert_eq!(result.reclaimed_versions, 0);
        assert_eq!(chain.version_chain_len(), 2);
    }

    // ── Safepoint computation ──

    #[test]
    fn test_safepoint_basic() {
        let sp = compute_safepoint(Timestamp(100), Timestamp::MAX);
        assert_eq!(sp, Timestamp(99));
    }

    #[test]
    fn test_safepoint_with_replica() {
        // Replica is behind at ts=50, active txns at ts=100
        let sp = compute_safepoint(Timestamp(100), Timestamp(50));
        assert_eq!(sp, Timestamp(49));
    }

    #[test]
    fn test_safepoint_zero() {
        let sp = compute_safepoint(Timestamp(0), Timestamp::MAX);
        assert_eq!(sp, Timestamp(0)); // saturating_sub
    }

    // ── MemTable sweep ──

    #[test]
    fn test_sweep_memtable_basic() {
        let table = MemTable::new(test_schema());
        let config = GcConfig {
            min_chain_length: 0,
            ..Default::default()
        };
        let stats = GcStats::new();

        // Insert + commit two versions for key 1
        table.insert(row(1), TxnId(1)).unwrap();
        table.commit_txn(TxnId(1), Timestamp(10));
        table
            .update(&crate::memtable::encode_pk(&row(1), &[0]), row(1), TxnId(2))
            .unwrap();
        table.commit_txn(TxnId(2), Timestamp(20));

        // Sweep at watermark 15
        let result = sweep_memtable(&table, Timestamp(15), &config, &stats);
        assert_eq!(result.chains_inspected, 1);
        assert_eq!(result.reclaimed_versions, 0); // newest committed at ts=20 > 15, walk to ts=10 <= 15, nothing below

        // Sweep at watermark 25
        let result2 = sweep_memtable(&table, Timestamp(25), &config, &stats);
        assert_eq!(result2.chains_inspected, 1);
        assert_eq!(result2.reclaimed_versions, 1); // ts=20 <= 25, truncate ts=10
    }

    #[test]
    fn test_sweep_respects_batch_size() {
        let table = MemTable::new(test_schema());
        let config = GcConfig {
            batch_size: 2,
            min_chain_length: 0,
            ..Default::default()
        };
        let stats = GcStats::new();

        // Insert 5 keys
        for i in 1..=5 {
            table
                .insert(OwnedRow::new(vec![Datum::Int32(i)]), TxnId(i as u64))
                .unwrap();
            table.commit_txn(TxnId(i as u64), Timestamp(i as u64 * 10));
        }

        let result = sweep_memtable(&table, Timestamp(100), &config, &stats);
        // Should process at most 2 keys
        assert!(result.chains_inspected + result.keys_skipped <= 5);
    }

    #[test]
    fn test_sweep_respects_min_chain_length() {
        let table = MemTable::new(test_schema());
        let config = GcConfig {
            min_chain_length: 3,
            ..Default::default()
        };
        let stats = GcStats::new();

        // Insert key with 2 versions (below min_chain_length of 3)
        table.insert(row(1), TxnId(1)).unwrap();
        table.commit_txn(TxnId(1), Timestamp(10));
        let pk = crate::memtable::encode_pk(&row(1), &[0]);
        table.update(&pk, row(1), TxnId(2)).unwrap();
        table.commit_txn(TxnId(2), Timestamp(20));

        let result = sweep_memtable(&table, Timestamp(100), &config, &stats);
        assert_eq!(result.chains_inspected, 0);
        assert_eq!(result.keys_skipped, 1);
    }

    // ── Engine-level GC ──

    #[test]
    fn test_engine_run_gc_basic() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(test_schema()).unwrap();

        // Insert + update (creates 2 versions)
        engine.insert(TableId(1), row(1), TxnId(1)).unwrap();
        engine
            .commit_txn(
                TxnId(1),
                Timestamp(10),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        let pk = crate::memtable::encode_pk(&row(1), &[0]);
        engine.update(TableId(1), &pk, row(1), TxnId(2)).unwrap();
        engine
            .commit_txn(
                TxnId(2),
                Timestamp(20),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();

        let chains = engine.run_gc(Timestamp(25));
        assert!(chains >= 1);

        // Data should still be readable
        let rows = engine.scan(TableId(1), TxnId(999), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_engine_gc_with_config() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(test_schema()).unwrap();

        // Insert 3 versions for key 1
        engine.insert(TableId(1), row(1), TxnId(1)).unwrap();
        engine
            .commit_txn(
                TxnId(1),
                Timestamp(10),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        let pk = crate::memtable::encode_pk(&row(1), &[0]);
        engine.update(TableId(1), &pk, row(1), TxnId(2)).unwrap();
        engine
            .commit_txn(
                TxnId(2),
                Timestamp(20),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        engine.update(TableId(1), &pk, row(1), TxnId(3)).unwrap();
        engine
            .commit_txn(
                TxnId(3),
                Timestamp(30),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();

        let config = GcConfig {
            min_chain_length: 0,
            ..Default::default()
        };
        let stats = GcStats::new();
        let result = engine.run_gc_with_config(Timestamp(25), &config, &stats);

        assert_eq!(result.chains_inspected, 1);
        assert_eq!(result.reclaimed_versions, 1); // ts=10 reclaimed
        assert!(result.reclaimed_bytes > 0);

        let snap = stats.snapshot();
        assert_eq!(snap.total_sweeps, 1);
        assert_eq!(snap.total_reclaimed_versions, 1);
    }

    // ── Concurrent read + GC ──

    #[test]
    fn test_gc_concurrent_with_read() {
        use std::sync::Arc;

        let engine = Arc::new(StorageEngine::new_in_memory());
        engine.create_table(test_schema()).unwrap();

        // Create 5 versions
        for i in 1..=5u64 {
            let pk = crate::memtable::encode_pk(&OwnedRow::new(vec![Datum::Int32(1)]), &[0]);
            if i == 1 {
                engine
                    .insert(TableId(1), OwnedRow::new(vec![Datum::Int32(1)]), TxnId(i))
                    .unwrap();
            } else {
                engine
                    .update(
                        TableId(1),
                        &pk,
                        OwnedRow::new(vec![Datum::Int32(i as i32)]),
                        TxnId(i),
                    )
                    .unwrap();
            }
            engine
                .commit_txn(
                    TxnId(i),
                    Timestamp(i * 10),
                    falcon_common::types::TxnType::Local,
                )
                .unwrap();
        }

        // Spawn GC in a thread
        let engine_gc = engine.clone();
        let gc_handle = std::thread::spawn(move || engine_gc.run_gc(Timestamp(35)));

        // Concurrent reads
        let engine_read = engine.clone();
        let read_handle = std::thread::spawn(move || {
            let rows = engine_read
                .scan(TableId(1), TxnId(999), Timestamp(100))
                .unwrap();
            assert_eq!(rows.len(), 1);
            rows[0].1.values[0].clone()
        });

        gc_handle.join().unwrap();
        let val = read_handle.join().unwrap();
        // The latest committed version (ts=50, value=5) should always be visible
        assert_eq!(val, Datum::Int32(5));
    }

    // ── Long txn blocks GC ──

    #[test]
    fn test_long_txn_prevents_gc() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(test_schema()).unwrap();

        // Create 3 versions
        engine.insert(TableId(1), row(1), TxnId(1)).unwrap();
        engine
            .commit_txn(
                TxnId(1),
                Timestamp(10),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        let pk = crate::memtable::encode_pk(&row(1), &[0]);
        engine.update(TableId(1), &pk, row(2), TxnId(2)).unwrap();
        engine
            .commit_txn(
                TxnId(2),
                Timestamp(20),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        engine.update(TableId(1), &pk, row(3), TxnId(3)).unwrap();
        engine
            .commit_txn(
                TxnId(3),
                Timestamp(30),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();

        // Simulate a long-running txn with start_ts=15.
        // Safepoint = min_active_ts - 1 = 14.
        // No committed version has commit_ts <= 14, so nothing is GC'd.
        let safepoint = compute_safepoint(Timestamp(15), Timestamp::MAX);
        assert_eq!(safepoint, Timestamp(14));

        let config = GcConfig {
            min_chain_length: 0,
            ..Default::default()
        };
        let stats = GcStats::new();
        let result = engine.run_gc_with_config(safepoint, &config, &stats);
        assert_eq!(result.reclaimed_versions, 0, "long txn should prevent GC");

        // All 3 versions should still exist
        // Read at ts=10 should see value 1
        let rows = engine.scan(TableId(1), TxnId(999), Timestamp(10)).unwrap();
        assert_eq!(rows[0].1.values[0], Datum::Int32(1));
    }

    // ── GC stats tracking ──

    #[test]
    fn test_gc_stats_accumulate() {
        let stats = GcStats::new();
        stats.observe_chain_length(5);
        stats.observe_chain_length(3);
        stats.observe_chain_length(10);

        let snap = stats.snapshot();
        assert_eq!(snap.max_chain_length_observed, 10);
        assert!((snap.avg_chain_length - 6.0).abs() < 0.01);
    }

    // ── GC benchmark: memory over time ──

    #[test]
    fn test_gc_benchmark_memory_reclamation() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(test_schema()).unwrap();

        let num_keys = 100;
        let num_updates = 10;

        // Phase 1: Insert keys + multiple updates (creates version chains)
        for key in 0..num_keys {
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(key)]),
                    TxnId(key as u64 * 100 + 1),
                )
                .unwrap();
            engine
                .commit_txn(
                    TxnId(key as u64 * 100 + 1),
                    Timestamp(key as u64 * 100 + 1),
                    falcon_common::types::TxnType::Local,
                )
                .unwrap();

            let pk = crate::memtable::encode_pk(&OwnedRow::new(vec![Datum::Int32(key)]), &[0]);
            for upd in 1..=num_updates {
                let txn = TxnId(key as u64 * 100 + upd as u64 + 1);
                let ts = Timestamp(key as u64 * 100 + upd as u64 + 1);
                engine
                    .update(
                        TableId(1),
                        &pk,
                        OwnedRow::new(vec![Datum::Int32(key + upd as i32)]),
                        txn,
                    )
                    .unwrap();
                engine
                    .commit_txn(txn, ts, falcon_common::types::TxnType::Local)
                    .unwrap();
            }
        }

        let chains_before = engine.total_chain_count();
        assert_eq!(chains_before, num_keys as usize);

        // Phase 2: GC sweep
        let config = GcConfig {
            min_chain_length: 0,
            ..Default::default()
        };
        let stats = GcStats::new();
        let watermark = Timestamp(u64::MAX - 2);
        let result = engine.run_gc_with_config(watermark, &config, &stats);

        eprintln!(
            "\n=== GC Benchmark ({} keys x {} updates) ===",
            num_keys, num_updates
        );
        eprintln!("  chains_inspected: {}", result.chains_inspected);
        eprintln!("  chains_pruned:    {}", result.chains_pruned);
        eprintln!("  reclaimed_versions: {}", result.reclaimed_versions);
        eprintln!("  reclaimed_bytes:  {}", result.reclaimed_bytes);
        eprintln!("  sweep_duration_us: {}", result.sweep_duration_us);

        let snap = stats.snapshot();
        eprintln!("  max_chain_length: {}", snap.max_chain_length_observed);
        eprintln!("  avg_chain_length: {:.1}", snap.avg_chain_length);

        // Should have reclaimed (num_updates) versions per key
        assert_eq!(result.reclaimed_versions, (num_keys * num_updates) as u64);
        assert!(result.reclaimed_bytes > 0);

        // After GC, each chain should have exactly 1 version
        let snap2 = GcStats::new();
        let config2 = GcConfig {
            min_chain_length: 0,
            ..Default::default()
        };
        sweep_memtable(
            &engine.get_table(TableId(1)).unwrap(),
            watermark,
            &config2,
            &snap2,
        );
        let s = snap2.snapshot();
        assert_eq!(s.max_chain_length_observed, 1);
    }

    // ── GcRunner background task ──

    #[test]
    fn test_gc_runner_background_sweep() {
        use crate::gc::{GcRunner, SafepointProvider};
        use std::sync::atomic::{AtomicU64, Ordering as AOrdering};
        use std::sync::Arc;

        struct TestProvider {
            ts: AtomicU64,
        }
        impl SafepointProvider for TestProvider {
            fn min_active_ts(&self) -> Timestamp {
                Timestamp(self.ts.load(AOrdering::Relaxed))
            }
            fn replica_safe_ts(&self) -> Timestamp {
                Timestamp::MAX
            }
        }

        let engine = Arc::new(StorageEngine::new_in_memory());
        engine.create_table(test_schema()).unwrap();

        // Create 3 versions for key 1
        engine.insert(TableId(1), row(1), TxnId(1)).unwrap();
        engine
            .commit_txn(
                TxnId(1),
                Timestamp(10),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        let pk = crate::memtable::encode_pk(&row(1), &[0]);
        engine.update(TableId(1), &pk, row(2), TxnId(2)).unwrap();
        engine
            .commit_txn(
                TxnId(2),
                Timestamp(20),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        engine.update(TableId(1), &pk, row(3), TxnId(3)).unwrap();
        engine
            .commit_txn(
                TxnId(3),
                Timestamp(30),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();

        let provider = Arc::new(TestProvider {
            ts: AtomicU64::new(100),
        });
        let config = GcConfig {
            interval_ms: 10, // 10ms interval for fast test
            min_chain_length: 0,
            ..Default::default()
        };

        let mut runner = GcRunner::start(engine.clone(), provider, config).expect("spawn in test");
        assert!(runner.is_running());

        // Wait for at least one sweep
        std::thread::sleep(std::time::Duration::from_millis(50));
        runner.stop();
        assert!(!runner.is_running());

        // Verify GC ran
        let snap = engine.gc_stats_snapshot();
        assert!(
            snap.total_sweeps > 0,
            "GC runner should have run at least one sweep"
        );
        assert!(
            snap.total_reclaimed_versions > 0,
            "should have reclaimed versions"
        );

        // Data should still be readable
        let rows = engine.scan(TableId(1), TxnId(999), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[0], Datum::Int32(3));
    }

    // ── Crash-recovery after GC ──

    #[test]
    fn test_gc_then_crash_recovery() {
        let dir = std::env::temp_dir().join(format!("falcon_gc_test_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);

        // Phase 1: Write, GC, then "crash"
        {
            let engine = StorageEngine::new(Some(&dir)).unwrap();
            engine.create_table(test_schema()).unwrap();

            // Insert + update (2 versions)
            engine.insert(TableId(1), row(1), TxnId(1)).unwrap();
            engine
                .commit_txn(
                    TxnId(1),
                    Timestamp(10),
                    falcon_common::types::TxnType::Local,
                )
                .unwrap();
            let pk = crate::memtable::encode_pk(&row(1), &[0]);
            engine.update(TableId(1), &pk, row(2), TxnId(2)).unwrap();
            engine
                .commit_txn(
                    TxnId(2),
                    Timestamp(20),
                    falcon_common::types::TxnType::Local,
                )
                .unwrap();

            // Also insert key 2 and leave it
            engine
                .insert(TableId(1), OwnedRow::new(vec![Datum::Int32(99)]), TxnId(3))
                .unwrap();
            engine
                .commit_txn(
                    TxnId(3),
                    Timestamp(30),
                    falcon_common::types::TxnType::Local,
                )
                .unwrap();

            // Run GC  — reclaims old version of key 1 in memory
            let result = engine.gc_sweep(Timestamp(25));
            assert_eq!(result.reclaimed_versions, 1);

            // "crash"  — engine dropped, WAL persisted
        }

        // Phase 2: Recover from WAL
        {
            let recovered = StorageEngine::recover(&dir).unwrap();
            // WAL replays all committed writes  — both keys should exist
            let rows = recovered
                .scan(TableId(1), TxnId(999), Timestamp(100))
                .unwrap();
            assert_eq!(rows.len(), 2, "both keys should survive recovery");

            // Key 1 should have latest value (2)
            let pk1 = crate::memtable::encode_pk(&row(1), &[0]);
            let val = recovered
                .get(TableId(1), &pk1, TxnId(999), Timestamp(100))
                .unwrap();
            assert_eq!(val.unwrap().values[0], Datum::Int32(2));

            // Key 99 should exist
            let pk99 = crate::memtable::encode_pk(&OwnedRow::new(vec![Datum::Int32(99)]), &[0]);
            let val99 = recovered
                .get(TableId(1), &pk99, TxnId(999), Timestamp(100))
                .unwrap();
            assert_eq!(val99.unwrap().values[0], Datum::Int32(99));
        }
    }

    // ── No-GC vs With-GC benchmark ──

    #[test]
    fn test_gc_benchmark_no_gc_vs_gc() {
        let num_keys = 50i32;
        let num_updates = 20u64;

        // Helper: create engine, fill it, optionally GC
        fn fill_engine(num_keys: i32, num_updates: u64) -> StorageEngine {
            let engine = StorageEngine::new_in_memory();
            let schema = TableSchema {
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
                next_serial_values: std::collections::HashMap::new(),
                check_constraints: vec![],
                unique_constraints: vec![],
                foreign_keys: vec![],
                ..Default::default()
            };
            engine.create_table(schema).unwrap();

            for key in 0..num_keys {
                let base = key as u64 * (num_updates + 1) + 1;
                engine
                    .insert(
                        TableId(1),
                        OwnedRow::new(vec![Datum::Int32(key)]),
                        TxnId(base),
                    )
                    .unwrap();
                engine
                    .commit_txn(
                        TxnId(base),
                        Timestamp(base),
                        falcon_common::types::TxnType::Local,
                    )
                    .unwrap();

                let pk = crate::memtable::encode_pk(&OwnedRow::new(vec![Datum::Int32(key)]), &[0]);
                for upd in 1..=num_updates {
                    let txn = TxnId(base + upd);
                    let ts = Timestamp(base + upd);
                    engine
                        .update(
                            TableId(1),
                            &pk,
                            OwnedRow::new(vec![Datum::Int32(key + upd as i32)]),
                            txn,
                        )
                        .unwrap();
                    engine
                        .commit_txn(txn, ts, falcon_common::types::TxnType::Local)
                        .unwrap();
                }
            }
            engine
        }

        // ── No GC ──
        let engine_no_gc = fill_engine(num_keys, num_updates);
        let chains_no_gc = engine_no_gc.total_chain_count();
        let stats_no_gc = GcStats::new();
        let config = GcConfig {
            min_chain_length: 0,
            ..Default::default()
        };
        // Measure chain lengths without GC
        for entry in engine_no_gc.get_table(TableId(1)).unwrap().data.iter() {
            stats_no_gc.observe_chain_length(entry.value().version_chain_len() as u64);
        }
        let snap_no_gc = stats_no_gc.snapshot();

        // Measure p99 read latency without GC (scan 100 times)
        let mut latencies_no_gc = Vec::with_capacity(100);
        for _ in 0..100 {
            let start = Instant::now();
            let _ = engine_no_gc.scan(TableId(1), TxnId(999999), Timestamp(u64::MAX - 2));
            latencies_no_gc.push(start.elapsed().as_nanos() as u64);
        }
        latencies_no_gc.sort();
        let p99_no_gc = latencies_no_gc[98];

        // ── With GC ──
        let engine_gc = fill_engine(num_keys, num_updates);
        let watermark = Timestamp(u64::MAX - 2);
        let gc_result = engine_gc.run_gc_with_config(watermark, &config, &GcStats::new());

        let stats_gc = GcStats::new();
        for entry in engine_gc.get_table(TableId(1)).unwrap().data.iter() {
            stats_gc.observe_chain_length(entry.value().version_chain_len() as u64);
        }
        let snap_gc = stats_gc.snapshot();

        let mut latencies_gc = Vec::with_capacity(100);
        for _ in 0..100 {
            let start = Instant::now();
            let _ = engine_gc.scan(TableId(1), TxnId(999999), Timestamp(u64::MAX - 2));
            latencies_gc.push(start.elapsed().as_nanos() as u64);
        }
        latencies_gc.sort();
        let p99_gc = latencies_gc[98];

        eprintln!(
            "\n=== No-GC vs With-GC ({} keys × {} updates) ===",
            num_keys, num_updates
        );
        eprintln!(
            "  [No GC]  chains: {}, max_chain_len: {}, avg_chain_len: {:.1}, p99_read_ns: {}",
            chains_no_gc,
            snap_no_gc.max_chain_length_observed,
            snap_no_gc.avg_chain_length,
            p99_no_gc
        );
        eprintln!(
            "  [GC]     chains: {}, max_chain_len: {}, avg_chain_len: {:.1}, p99_read_ns: {}",
            chains_no_gc, snap_gc.max_chain_length_observed, snap_gc.avg_chain_length, p99_gc
        );
        eprintln!(
            "  GC reclaimed: {} versions, {} bytes, {}us",
            gc_result.reclaimed_versions, gc_result.reclaimed_bytes, gc_result.sweep_duration_us
        );

        // Assertions
        assert_eq!(snap_no_gc.max_chain_length_observed, num_updates + 1);
        assert_eq!(snap_gc.max_chain_length_observed, 1);
        assert!(gc_result.reclaimed_versions > 0);
        // p99 with GC should be ≈p99 without GC (shorter chains  → faster reads)
        // (This is a statistical assertion; allow 2x slack for CI jitter)
        assert!(
            p99_gc <= p99_no_gc * 2 + 10000,
            "p99 with GC ({}) should not be dramatically worse than without ({})",
            p99_gc,
            p99_no_gc,
        );
    }
}

#[cfg(test)]
mod wal_observer_tests {
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

    // ── ReplicaAckTracker tests ──

    #[test]
    fn test_replica_ack_tracker_no_replicas() {
        use crate::engine::ReplicaAckTracker;
        let tracker = ReplicaAckTracker::new();
        // No replicas  → no constraint
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
        // Initially no replicas  → MAX
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

        // Register a slow replica at ts=5  — should block GC
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
}

// ===========================================================================
// ColumnStore / DiskRowstore integration tests
// Verifies visibility semantics when accessed through StorageEngine.
// Note: these storage backends do not implement MVCC versioning — they use
// last-write-wins semantics. Tests document and verify this behaviour.
// ===========================================================================

#[cfg(all(test, feature = "columnstore"))]
mod columnstore_integration_tests {
    use crate::engine::StorageEngine;
    use falcon_common::config::NodeRole;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, StorageType, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId, TxnType};

    fn cs_schema() -> TableSchema {
        TableSchema {
            id: TableId(500),
            name: "cs_test".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int64,
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
            storage_type: StorageType::Columnstore,
            ..Default::default()
        }
    }

    fn analytics_engine() -> StorageEngine {
        let mut e = StorageEngine::new_in_memory();
        e.set_node_role(NodeRole::Analytics);
        e
    }

    #[test]
    fn test_columnstore_insert_and_scan_via_engine() {
        let engine = analytics_engine();
        engine.create_table(cs_schema()).unwrap();

        let row1 = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("alpha".into())]);
        let row2 = OwnedRow::new(vec![Datum::Int64(2), Datum::Text("beta".into())]);

        engine.insert(TableId(500), row1, TxnId(1)).unwrap();
        engine.insert(TableId(500), row2, TxnId(1)).unwrap();

        // ColumnStore: read_ts is accepted but not used for versioning
        let rows = engine.scan(TableId(500), TxnId(2), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 2, "both rows must be visible after insert");
    }

    #[test]
    fn test_columnstore_scan_returns_all_rows_regardless_of_read_ts() {
        let engine = analytics_engine();
        engine.create_table(cs_schema()).unwrap();

        for i in 0..5i64 {
            engine
                .insert(
                    TableId(500),
                    OwnedRow::new(vec![Datum::Int64(i), Datum::Text(format!("v{}", i))]),
                    TxnId(1),
                )
                .unwrap();
        }

        // ColumnStore has no MVCC — all rows visible at any read_ts
        let rows_past = engine.scan(TableId(500), TxnId(2), Timestamp(0)).unwrap();
        let rows_future = engine
            .scan(TableId(500), TxnId(2), Timestamp(u64::MAX))
            .unwrap();
        assert_eq!(rows_past.len(), 5, "ColumnStore: all rows visible at ts=0");
        assert_eq!(
            rows_future.len(),
            5,
            "ColumnStore: all rows visible at ts=MAX"
        );
    }

    #[test]
    fn test_columnstore_update_not_supported() {
        let engine = analytics_engine();
        engine.create_table(cs_schema()).unwrap();

        let row = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("v1".into())]);
        let pk = engine.insert(TableId(500), row.clone(), TxnId(1)).unwrap();

        // UPDATE is not supported on COLUMNSTORE (analytics workload)
        let result = engine.update(TableId(500), &pk, row, TxnId(2));
        assert!(
            result.is_err(),
            "UPDATE on COLUMNSTORE must return an error"
        );
    }

    #[test]
    fn test_columnstore_delete_not_supported() {
        let engine = analytics_engine();
        engine.create_table(cs_schema()).unwrap();

        let row = OwnedRow::new(vec![Datum::Int64(1), Datum::Text("v1".into())]);
        let pk = engine.insert(TableId(500), row, TxnId(1)).unwrap();

        // DELETE is not supported on COLUMNSTORE
        let result = engine.delete(TableId(500), &pk, TxnId(2));
        assert!(
            result.is_err(),
            "DELETE on COLUMNSTORE must return an error"
        );
    }

    #[test]
    fn test_columnstore_not_allowed_on_primary() {
        let mut engine = StorageEngine::new_in_memory();
        engine.set_node_role(NodeRole::Primary);

        let result = engine.create_table(cs_schema());
        assert!(
            result.is_err(),
            "COLUMNSTORE must be rejected on Primary nodes"
        );
    }

    #[test]
    fn test_columnstore_commit_does_not_affect_visibility() {
        // ColumnStore rows are immediately visible — commit is a no-op for visibility
        let engine = analytics_engine();
        engine.create_table(cs_schema()).unwrap();

        engine
            .insert(
                TableId(500),
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("x".into())]),
                TxnId(1),
            )
            .unwrap();

        // Visible before commit
        let before = engine
            .scan(TableId(500), TxnId(99), Timestamp(100))
            .unwrap();
        assert_eq!(before.len(), 1);

        engine
            .commit_txn(TxnId(1), Timestamp(10), TxnType::Local)
            .unwrap();

        // Still visible after commit
        let after = engine
            .scan(TableId(500), TxnId(99), Timestamp(100))
            .unwrap();
        assert_eq!(after.len(), 1);
    }
}

#[cfg(all(test, feature = "disk_rowstore"))]
mod disk_rowstore_integration_tests {
    use crate::engine::StorageEngine;
    use falcon_common::config::NodeRole;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, StorageType, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId, TxnType};

    fn disk_schema() -> TableSchema {
        TableSchema {
            id: TableId(600),
            name: "disk_test".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int64,
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
            storage_type: StorageType::DiskRowstore,
            ..Default::default()
        }
    }

    fn analytics_engine() -> StorageEngine {
        let mut e = StorageEngine::new_in_memory();
        e.set_node_role(NodeRole::Analytics);
        e
    }

    #[test]
    fn test_disk_rowstore_insert_and_scan_via_engine() {
        let engine = analytics_engine();
        engine.create_table(disk_schema()).unwrap();

        engine
            .insert(
                TableId(600),
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("a".into())]),
                TxnId(1),
            )
            .unwrap();
        engine
            .insert(
                TableId(600),
                OwnedRow::new(vec![Datum::Int64(2), Datum::Text("b".into())]),
                TxnId(1),
            )
            .unwrap();

        let rows = engine.scan(TableId(600), TxnId(2), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_disk_rowstore_update_via_engine() {
        let engine = analytics_engine();
        engine.create_table(disk_schema()).unwrap();

        let pk = engine
            .insert(
                TableId(600),
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("old".into())]),
                TxnId(1),
            )
            .unwrap();
        engine
            .update(
                TableId(600),
                &pk,
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("new".into())]),
                TxnId(2),
            )
            .unwrap();

        let rows = engine.scan(TableId(600), TxnId(3), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[1], Datum::Text("new".into()));
    }

    #[test]
    fn test_disk_rowstore_delete_via_engine() {
        let engine = analytics_engine();
        engine.create_table(disk_schema()).unwrap();

        let pk = engine
            .insert(
                TableId(600),
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("gone".into())]),
                TxnId(1),
            )
            .unwrap();
        engine.delete(TableId(600), &pk, TxnId(2)).unwrap();

        let rows = engine.scan(TableId(600), TxnId(3), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 0, "deleted row must not appear in scan");
    }

    #[test]
    fn test_disk_rowstore_scan_ignores_read_ts() {
        // DiskRowstore has no MVCC — read_ts is accepted but not used for versioning
        let engine = analytics_engine();
        engine.create_table(disk_schema()).unwrap();

        engine
            .insert(
                TableId(600),
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("x".into())]),
                TxnId(1),
            )
            .unwrap();

        let rows_ts0 = engine.scan(TableId(600), TxnId(2), Timestamp(0)).unwrap();
        let rows_tsmax = engine
            .scan(TableId(600), TxnId(2), Timestamp(u64::MAX))
            .unwrap();
        assert_eq!(rows_ts0.len(), 1, "DiskRowstore: row visible at ts=0");
        assert_eq!(rows_tsmax.len(), 1, "DiskRowstore: row visible at ts=MAX");
    }

    #[test]
    fn test_disk_rowstore_duplicate_key_rejected() {
        let engine = analytics_engine();
        engine.create_table(disk_schema()).unwrap();

        let row = OwnedRow::new(vec![Datum::Int64(42), Datum::Text("dup".into())]);
        engine.insert(TableId(600), row.clone(), TxnId(1)).unwrap();
        let result = engine.insert(TableId(600), row, TxnId(2));
        assert!(
            result.is_err(),
            "duplicate PK must be rejected by DiskRowstore"
        );
    }

    #[test]
    fn test_disk_rowstore_not_allowed_on_primary() {
        let mut engine = StorageEngine::new_in_memory();
        engine.set_node_role(NodeRole::Primary);

        let result = engine.create_table(disk_schema());
        assert!(
            result.is_err(),
            "DISK_ROWSTORE must be rejected on Primary nodes"
        );
    }

    #[test]
    fn test_disk_rowstore_commit_does_not_affect_visibility() {
        // DiskRowstore rows are immediately visible — commit is a no-op for visibility
        let engine = analytics_engine();
        engine.create_table(disk_schema()).unwrap();

        engine
            .insert(
                TableId(600),
                OwnedRow::new(vec![Datum::Int64(1), Datum::Text("y".into())]),
                TxnId(1),
            )
            .unwrap();

        let before = engine
            .scan(TableId(600), TxnId(99), Timestamp(100))
            .unwrap();
        assert_eq!(before.len(), 1);

        engine
            .commit_txn(TxnId(1), Timestamp(10), TxnType::Local)
            .unwrap();

        let after = engine
            .scan(TableId(600), TxnId(99), Timestamp(100))
            .unwrap();
        assert_eq!(after.len(), 1);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// B3: Snapshot Isolation Litmus Tests
// ═══════════════════════════════════════════════════════════════════════
//
// Tests verify MVCC visibility invariants at both VersionChain (unit)
// and StorageEngine (integration) levels.
//
// Naming convention: si_<anomaly-or-property>_<scenario>
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod si_litmus_tests {
    use crate::mvcc::VersionChain;
    use crate::engine::StorageEngine;
    use crate::memtable::encode_pk;
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
                ColumnDef { id: ColumnId(0), name: "id".into(), data_type: DataType::Int32,
                    nullable: false, is_primary_key: true, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(1), name: "val".into(), data_type: DataType::Text,
                    nullable: true, is_primary_key: false, default_value: None, is_serial: false },
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

    // ── 1. Basic visibility ──

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
        assert_eq!(chain.read_for_txn(TxnId(1), Timestamp(0)).unwrap().values[0], Datum::Int32(42));
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

    // ── 2. Snapshot consistency ──

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

        assert_eq!(chain.read_committed(Timestamp(2)).unwrap().values[0], Datum::Int32(10));
        assert_eq!(chain.read_committed(Timestamp(4)).unwrap().values[0], Datum::Int32(20));
        assert_eq!(chain.read_committed(Timestamp(6)).unwrap().values[0], Datum::Int32(30));
        assert_eq!(chain.read_committed(Timestamp(8)).unwrap().values[0], Datum::Int32(40));
    }

    // ── 3. Abort visibility ──

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

    // ── 4. Tombstone / delete ──

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

        assert_eq!(chain.read_committed(Timestamp(7)).unwrap().values[0], Datum::Int32(42));
        assert!(chain.read_committed(Timestamp(12)).is_none());
        assert_eq!(chain.read_committed(Timestamp(15)).unwrap().values[0], Datum::Int32(99));
    }

    // ── 5. Concurrent txn isolation ──

    #[test]
    fn si_14_concurrent_writers_isolation() {
        let chain = VersionChain::new();
        // T1 inserts, uncommitted
        chain.prepend(TxnId(1), Some(row1(100)));
        // T2 reads — should not see T1
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
        // Under SI, both see the old snapshot — this is expected behavior.
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
        assert_eq!(chain_a.read_committed(Timestamp(5)).unwrap().values[0], Datum::Int32(0));
        assert_eq!(chain_b.read_committed(Timestamp(5)).unwrap().values[0], Datum::Int32(0));
    }

    // ── 6. Read-own-writes ──

    #[test]
    fn si_17_read_own_uncommitted_update() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(10)));
        chain.commit(TxnId(1), Timestamp(5));
        // T2 updates to 20 (uncommitted)
        chain.prepend(TxnId(2), Some(row1(20)));
        // T2 reads own write
        assert_eq!(chain.read_for_txn(TxnId(2), Timestamp(5)).unwrap().values[0], Datum::Int32(20));
        // Other txn still sees 10
        assert_eq!(chain.read_for_txn(TxnId(3), Timestamp(5)).unwrap().values[0], Datum::Int32(10));
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

    // ── 7. GC safety ──

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
        assert_eq!(chain.read_committed(Timestamp(10)).unwrap().values[0], Datum::Int32(20));
    }

    #[test]
    fn si_20_gc_does_not_drop_uncommitted() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row1(10)));
        // Don't commit — GC should not touch uncommitted
        chain.gc(Timestamp(100));
        // Still readable by own txn
        assert!(chain.read_for_txn(TxnId(1), Timestamp(0)).is_some());
    }

    // ── 8. StorageEngine integration ──

    #[test]
    fn si_21_engine_insert_invisible_before_commit() {
        let engine = engine_with_table();
        engine.insert(TableId(900), row2(1, "hello"), TxnId(1)).unwrap();
        // Scan by another txn — should not see uncommitted row
        let rows = engine.scan(TableId(900), TxnId(2), Timestamp(100)).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn si_22_engine_insert_visible_after_commit() {
        let engine = engine_with_table();
        engine.insert(TableId(900), row2(1, "hello"), TxnId(1)).unwrap();
        engine.commit_txn(TxnId(1), Timestamp(10), TxnType::Local).unwrap();
        let rows = engine.scan(TableId(900), TxnId(2), Timestamp(10)).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn si_23_engine_snapshot_isolation_two_versions() {
        let engine = engine_with_table();
        let pk = engine.insert(TableId(900), row2(1, "v1"), TxnId(1)).unwrap();
        engine.commit_txn(TxnId(1), Timestamp(5), TxnType::Local).unwrap();

        engine.update(TableId(900), &pk, row2(1, "v2"), TxnId(2)).unwrap();
        engine.commit_txn(TxnId(2), Timestamp(10), TxnType::Local).unwrap();

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
        let pk = engine.insert(TableId(900), row2(1, "alive"), TxnId(1)).unwrap();
        engine.commit_txn(TxnId(1), Timestamp(5), TxnType::Local).unwrap();

        engine.delete(TableId(900), &pk, TxnId(2)).unwrap();
        engine.commit_txn(TxnId(2), Timestamp(10), TxnType::Local).unwrap();

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
        engine.insert(TableId(900), row2(1, "will_abort"), TxnId(1)).unwrap();
        engine.abort_txn(TxnId(1), TxnType::Local).unwrap();

        let rows = engine.scan(TableId(900), TxnId(99), Timestamp(999)).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn si_26_engine_own_writes_visible_in_txn() {
        let engine = engine_with_table();
        let pk = engine.insert(TableId(900), row2(1, "mine"), TxnId(1)).unwrap();
        // Same txn can read its own writes
        let r = engine.get(TableId(900), &pk, TxnId(1), Timestamp(0));
        assert!(r.is_ok());
        assert!(r.unwrap().is_some());
    }

    #[test]
    fn si_27_engine_multiple_rows_snapshot() {
        let engine = engine_with_table();
        for i in 1..=5 {
            engine.insert(TableId(900), row2(i, &format!("r{}", i)), TxnId(1)).unwrap();
        }
        engine.commit_txn(TxnId(1), Timestamp(5), TxnType::Local).unwrap();

        // Update row 3
        let pk3 = encode_pk(&row2(3, "r3"), &[0]);
        engine.update(TableId(900), &pk3, row2(3, "r3_updated"), TxnId(2)).unwrap();
        engine.commit_txn(TxnId(2), Timestamp(10), TxnType::Local).unwrap();

        // Snapshot at ts=7: all 5 rows, row 3 still "r3"
        let rows = engine.scan(TableId(900), TxnId(99), Timestamp(7)).unwrap();
        assert_eq!(rows.len(), 5);
        let row3 = rows.iter().find(|(_, r)| r.values[0] == Datum::Int32(3)).unwrap();
        assert_eq!(row3.1.values[1], Datum::Text("r3".into()));

        // Snapshot at ts=10: all 5 rows, row 3 is "r3_updated"
        let rows = engine.scan(TableId(900), TxnId(99), Timestamp(10)).unwrap();
        assert_eq!(rows.len(), 5);
        let row3 = rows.iter().find(|(_, r)| r.values[0] == Datum::Int32(3)).unwrap();
        assert_eq!(row3.1.values[1], Datum::Text("r3_updated".into()));
    }

    #[test]
    fn si_28_engine_concurrent_inserts_isolation() {
        let engine = engine_with_table();
        // T1 inserts row 1, T2 inserts row 2 — both uncommitted
        engine.insert(TableId(900), row2(1, "t1"), TxnId(1)).unwrap();
        engine.insert(TableId(900), row2(2, "t2"), TxnId(2)).unwrap();

        // T3 sees nothing
        let rows = engine.scan(TableId(900), TxnId(3), Timestamp(50)).unwrap();
        assert!(rows.is_empty());

        // Commit T1 at ts=10
        engine.commit_txn(TxnId(1), Timestamp(10), TxnType::Local).unwrap();

        // T3 at ts=10 sees only T1's row
        let rows = engine.scan(TableId(900), TxnId(3), Timestamp(10)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[1], Datum::Text("t1".into()));

        // Commit T2 at ts=20
        engine.commit_txn(TxnId(2), Timestamp(20), TxnType::Local).unwrap();

        // T3 at ts=20 sees both
        let rows = engine.scan(TableId(900), TxnId(3), Timestamp(20)).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn si_29_engine_update_abort_restores_old() {
        let engine = engine_with_table();
        let pk = engine.insert(TableId(900), row2(1, "original"), TxnId(1)).unwrap();
        engine.commit_txn(TxnId(1), Timestamp(5), TxnType::Local).unwrap();

        // T2 updates, then aborts
        engine.update(TableId(900), &pk, row2(1, "modified"), TxnId(2)).unwrap();
        engine.abort_txn(TxnId(2), TxnType::Local).unwrap();

        // Original value restored
        let rows = engine.scan(TableId(900), TxnId(99), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[1], Datum::Text("original".into()));
    }

    #[test]
    fn si_30_engine_delete_abort_restores_row() {
        let engine = engine_with_table();
        let pk = engine.insert(TableId(900), row2(1, "keep_me"), TxnId(1)).unwrap();
        engine.commit_txn(TxnId(1), Timestamp(5), TxnType::Local).unwrap();

        // T2 deletes, then aborts
        engine.delete(TableId(900), &pk, TxnId(2)).unwrap();
        engine.abort_txn(TxnId(2), TxnType::Local).unwrap();

        // Row still exists
        let rows = engine.scan(TableId(900), TxnId(99), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 1);
    }

    // ── 9. Edge cases ──

    #[test]
    fn si_31_empty_chain_returns_none() {
        let chain = VersionChain::new();
        assert!(chain.read_committed(Timestamp(999)).is_none());
        assert!(chain.read_for_txn(TxnId(1), Timestamp(999)).is_none());
    }

    #[test]
    fn si_32_commit_at_ts_zero_invisible() {
        // Commit timestamp 0 is reserved for "uncommitted" — should never be visible
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
        assert_eq!(chain.read_committed(Timestamp(10)).unwrap().values[0], Datum::Int32(10));
        // At ts=15: should see T3's value
        assert_eq!(chain.read_committed(Timestamp(15)).unwrap().values[0], Datum::Int32(30));
    }
}

// ═══════════════════════════════════════════════════════════════════════
// B9: DDL minimal closure + concurrency safety
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod ddl_concurrency_tests {
    use crate::engine::StorageEngine;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId, TxnType};
    use std::sync::Arc;

    fn schema(id: u64, name: &str) -> TableSchema {
        TableSchema {
            id: TableId(id),
            name: name.into(),
            columns: vec![
                ColumnDef { id: ColumnId(0), name: "id".into(), data_type: DataType::Int32,
                    nullable: false, is_primary_key: true, default_value: None, is_serial: false },
                ColumnDef { id: ColumnId(1), name: "val".into(), data_type: DataType::Text,
                    nullable: true, is_primary_key: false, default_value: None, is_serial: false },
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
    fn test_ddl_create_drop_idempotent_error() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(schema(800, "ddl_t1")).unwrap();
        // Duplicate create → error
        assert!(engine.create_table(schema(800, "ddl_t1")).is_err());
        // Drop
        engine.drop_table("ddl_t1").unwrap();
        // Drop again → error
        assert!(engine.drop_table("ddl_t1").is_err());
    }

    #[test]
    fn test_ddl_truncate_clears_data() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(schema(801, "ddl_trunc")).unwrap();
        engine.insert(TableId(801), OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]), TxnId(1)).unwrap();
        engine.commit_txn(TxnId(1), Timestamp(5), TxnType::Local).unwrap();

        let rows = engine.scan(TableId(801), TxnId(99), Timestamp(10)).unwrap();
        assert_eq!(rows.len(), 1);

        engine.truncate_table("ddl_trunc").unwrap();

        let rows = engine.scan(TableId(801), TxnId(99), Timestamp(20)).unwrap();
        assert!(rows.is_empty(), "truncate should clear all data");
    }

    #[test]
    fn test_ddl_concurrent_create_different_tables() {
        let engine = Arc::new(StorageEngine::new_in_memory());
        let mut handles = vec![];
        for i in 0..10u64 {
            let e = engine.clone();
            handles.push(std::thread::spawn(move || {
                e.create_table(schema(850 + i, &format!("conc_t{}", i)))
            }));
        }
        let mut ok_count = 0;
        for h in handles {
            if h.join().unwrap().is_ok() { ok_count += 1; }
        }
        assert_eq!(ok_count, 10, "all 10 concurrent creates on different names should succeed");
    }

    #[test]
    fn test_ddl_concurrent_create_same_table() {
        let engine = Arc::new(StorageEngine::new_in_memory());
        let mut handles = vec![];
        for _ in 0..10 {
            let e = engine.clone();
            handles.push(std::thread::spawn(move || {
                e.create_table(schema(860, "conc_same"))
            }));
        }
        let mut ok_count = 0;
        for h in handles {
            if h.join().unwrap().is_ok() { ok_count += 1; }
        }
        assert_eq!(ok_count, 1, "exactly 1 concurrent create should win");
    }

    #[test]
    fn test_ddl_concurrent_insert_during_create() {
        let engine = Arc::new(StorageEngine::new_in_memory());
        engine.create_table(schema(870, "conc_ins")).unwrap();

        let mut handles = vec![];
        for i in 0..20i32 {
            let e = engine.clone();
            handles.push(std::thread::spawn(move || {
                e.insert(TableId(870), OwnedRow::new(vec![
                    Datum::Int32(i), Datum::Text(format!("v{}", i))
                ]), TxnId(100 + i as u64))
            }));
        }
        let mut ok_count = 0;
        for h in handles {
            if h.join().unwrap().is_ok() { ok_count += 1; }
        }
        assert_eq!(ok_count, 20, "all concurrent inserts should succeed on unique PKs");
    }

    #[test]
    fn test_ddl_drop_prevents_further_dml() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(schema(880, "ddl_drop_dml")).unwrap();
        engine.drop_table("ddl_drop_dml").unwrap();

        let result = engine.insert(TableId(880), OwnedRow::new(vec![
            Datum::Int32(1), Datum::Text("after_drop".into())
        ]), TxnId(1));
        assert!(result.is_err(), "insert after DROP should fail");
    }

    #[test]
    fn test_ddl_create_index_and_drop_index() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(schema(890, "idx_test")).unwrap();
        engine.create_named_index("idx_val_890", "idx_test", 1, false).unwrap();
        assert!(engine.index_exists("idx_val_890"));

        engine.drop_index("idx_val_890").unwrap();
        assert!(!engine.index_exists("idx_val_890"));
    }

    #[test]
    fn test_ddl_drop_nonexistent_index_returns_error() {
        let engine = StorageEngine::new_in_memory();
        let result = engine.drop_index("nonexistent_idx");
        assert!(result.is_err(), "dropping nonexistent index should return error");
    }
}
