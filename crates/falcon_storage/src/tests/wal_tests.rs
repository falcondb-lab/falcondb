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
