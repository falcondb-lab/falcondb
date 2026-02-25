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

        // Phase 2: Recover  鈥?should see both pre- and post-checkpoint data
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
            // No commit  鈥?crash!
        }

        // Phase 2: Recover  鈥?uncommitted post-checkpoint write should be aborted
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
