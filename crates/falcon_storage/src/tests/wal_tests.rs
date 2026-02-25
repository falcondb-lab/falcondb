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

    // ── TDE WAL encryption tests ─────────────────────────────────────────

    #[test]
    fn test_wal_tde_encrypt_decrypt_roundtrip() {
        use crate::encryption::EncryptionKey;
        use crate::wal::WalEncryption;

        let dir = std::env::temp_dir().join("falcon_wal_tde_roundtrip");
        let _ = std::fs::remove_dir_all(&dir);

        let key = EncryptionKey::generate();
        let enc = WalEncryption::from_key(&key);

        let mut wal = WalWriter::open(&dir, SyncMode::None).unwrap();
        wal.set_encryption(enc);
        assert!(wal.is_encrypted());

        // Write several record types
        wal.append(&WalRecord::BeginTxn { txn_id: TxnId(1) }).unwrap();
        wal.append(&WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: OwnedRow::new(vec![Datum::Int32(42), Datum::Text("encrypted".into())]),
        }).unwrap();
        wal.append(&WalRecord::CommitTxn {
            txn_id: TxnId(1),
            commit_ts: Timestamp(100),
        }).unwrap();
        wal.flush().unwrap();

        // Read back with decryption
        let dec = WalEncryption::from_key(&key);
        let reader = WalReader::new(&dir);
        let records = reader.read_all_encrypted(Some(&dec)).unwrap();
        assert_eq!(records.len(), 3);

        match &records[0] {
            WalRecord::BeginTxn { txn_id } => assert_eq!(txn_id.0, 1),
            other => panic!("Expected BeginTxn, got {:?}", other),
        }
        match &records[1] {
            WalRecord::Insert { row, .. } => {
                assert_eq!(row.values[0], Datum::Int32(42));
                assert_eq!(row.values[1], Datum::Text("encrypted".into()));
            }
            other => panic!("Expected Insert, got {:?}", other),
        }
        match &records[2] {
            WalRecord::CommitTxn { commit_ts, .. } => assert_eq!(commit_ts.0, 100),
            other => panic!("Expected CommitTxn, got {:?}", other),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_tde_wrong_key_fails() {
        use crate::encryption::EncryptionKey;
        use crate::wal::WalEncryption;

        let dir = std::env::temp_dir().join("falcon_wal_tde_wrong_key");
        let _ = std::fs::remove_dir_all(&dir);

        let key1 = EncryptionKey::generate();
        let enc = WalEncryption::from_key(&key1);

        let mut wal = WalWriter::open(&dir, SyncMode::None).unwrap();
        wal.set_encryption(enc);
        wal.append(&WalRecord::BeginTxn { txn_id: TxnId(1) }).unwrap();
        wal.flush().unwrap();

        // Try to read with a different key — should get 0 records (decryption fails)
        let key2 = EncryptionKey::generate();
        let wrong_dec = WalEncryption::from_key(&key2);
        let reader = WalReader::new(&dir);
        let records = reader.read_all_encrypted(Some(&wrong_dec)).unwrap();
        assert_eq!(records.len(), 0, "Wrong key should fail to decrypt any records");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_tde_without_decryption_fails() {
        use crate::encryption::EncryptionKey;
        use crate::wal::WalEncryption;

        let dir = std::env::temp_dir().join("falcon_wal_tde_no_dec");
        let _ = std::fs::remove_dir_all(&dir);

        let key = EncryptionKey::generate();
        let enc = WalEncryption::from_key(&key);

        let mut wal = WalWriter::open(&dir, SyncMode::None).unwrap();
        wal.set_encryption(enc);
        wal.append(&WalRecord::BeginTxn { txn_id: TxnId(1) }).unwrap();
        wal.flush().unwrap();

        // Reading without decryption context — bincode deserialize should fail
        let reader = WalReader::new(&dir);
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 0, "Encrypted WAL unreadable without decryption");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_tde_segment_rotation() {
        use crate::encryption::EncryptionKey;
        use crate::wal::WalEncryption;

        let dir = std::env::temp_dir().join("falcon_wal_tde_rotation");
        let _ = std::fs::remove_dir_all(&dir);

        let key = EncryptionKey::generate();
        let enc = WalEncryption::from_key(&key);

        // Tiny segment size to force rotation
        let mut wal = WalWriter::open_with_options(&dir, SyncMode::None, 200, 100).unwrap();
        wal.set_encryption(enc);

        for i in 0..20 {
            wal.append(&WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(i)]),
            }).unwrap();
        }
        wal.flush().unwrap();

        assert!(wal.current_segment_id() > 0, "Expected segment rotation");

        // Read all records back across encrypted segments
        let dec = WalEncryption::from_key(&key);
        let reader = WalReader::new(&dir);
        let records = reader.read_all_encrypted(Some(&dec)).unwrap();
        assert_eq!(records.len(), 20);

        for (i, rec) in records.iter().enumerate() {
            match rec {
                WalRecord::Insert { row, .. } => {
                    assert_eq!(row.values[0], Datum::Int32(i as i32));
                }
                other => panic!("Expected Insert at index {}, got {:?}", i, other),
            }
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_tde_keymanager_integration() {
        use crate::encryption::{EncryptionScope, KeyManager};
        use crate::wal::WalEncryption;

        let dir = std::env::temp_dir().join("falcon_wal_tde_km");
        let _ = std::fs::remove_dir_all(&dir);

        // Use full KeyManager flow: passphrase → master key → DEK → WalEncryption
        let mut km = KeyManager::new("my-secret-passphrase");
        let dek_id = km.generate_dek(EncryptionScope::Wal);
        let enc = WalEncryption::from_key_manager(&km, dek_id).expect("unwrap DEK");

        let mut wal = WalWriter::open(&dir, SyncMode::None).unwrap();
        wal.set_encryption(enc);

        wal.append(&WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(42),
            row: OwnedRow::new(vec![Datum::Text("enterprise".into())]),
        }).unwrap();
        wal.flush().unwrap();

        // Decrypt with the same KeyManager
        let dec = WalEncryption::from_key_manager(&km, dek_id).expect("unwrap DEK");
        let reader = WalReader::new(&dir);
        let records = reader.read_all_encrypted(Some(&dec)).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            WalRecord::Insert { table_id, row, .. } => {
                assert_eq!(table_id.0, 42);
                assert_eq!(row.values[0], Datum::Text("enterprise".into()));
            }
            other => panic!("Expected Insert, got {:?}", other),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
