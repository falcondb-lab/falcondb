    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, ShardId, TableId, Timestamp, TxnId};
    use falcon_storage::wal::WalRecord;

    use crate::replication::{
        ChannelTransport, InProcessTransport, LsnWalRecord, ReplicationLog, ReplicationTransport,
        ShardReplicaGroup, WalChunk,
    };
    use std::sync::Arc;

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

    #[test]
    fn test_wal_chunk_checksum() {
        let records = vec![
            LsnWalRecord {
                lsn: 1,
                record: WalRecord::BeginTxn { txn_id: TxnId(1) },
            },
            LsnWalRecord {
                lsn: 2,
                record: WalRecord::BeginTxn { txn_id: TxnId(2) },
            },
        ];
        let chunk = WalChunk::from_records(ShardId(0), records);
        assert!(chunk.verify_checksum(), "checksum should be valid");
        assert_eq!(chunk.start_lsn, 1);
        assert_eq!(chunk.end_lsn, 2);
        assert_eq!(chunk.len(), 2);
    }

    #[test]
    fn test_wal_chunk_empty() {
        let chunk = WalChunk::from_records(ShardId(0), vec![]);
        assert!(chunk.is_empty());
        assert!(chunk.verify_checksum());
        assert_eq!(chunk.start_lsn, 0);
        assert_eq!(chunk.end_lsn, 0);
    }

    #[test]
    fn test_wal_chunk_serde_roundtrip() {
        let row = falcon_common::datum::OwnedRow::new(vec![
            Datum::Int32(42),
            Datum::Text("hello".into()),
        ]);
        let records = vec![
            LsnWalRecord {
                lsn: 1,
                record: WalRecord::Insert {
                    txn_id: TxnId(1),
                    table_id: TableId(1),
                    row,
                },
            },
            LsnWalRecord {
                lsn: 2,
                record: WalRecord::CommitTxnLocal {
                    txn_id: TxnId(1),
                    commit_ts: Timestamp(10),
                },
            },
        ];
        let chunk = WalChunk::from_records(ShardId(0), records);

        // JSON round-trip (simulates gRPC/tonic JSON serialization)
        let json = serde_json::to_string(&chunk).expect("WalChunk should serialize to JSON");
        let decoded: WalChunk =
            serde_json::from_str(&json).expect("WalChunk should deserialize from JSON");

        assert_eq!(decoded.shard_id, chunk.shard_id);
        assert_eq!(decoded.start_lsn, chunk.start_lsn);
        assert_eq!(decoded.end_lsn, chunk.end_lsn);
        assert_eq!(decoded.checksum, chunk.checksum);
        assert!(
            decoded.verify_checksum(),
            "deserialized chunk checksum should be valid"
        );
        assert_eq!(decoded.len(), 2);
    }

    #[test]
    fn test_wal_chunk_json_roundtrip_global_txn() {
        let records = vec![
            LsnWalRecord {
                lsn: 1,
                record: WalRecord::BeginTxn { txn_id: TxnId(1) },
            },
            LsnWalRecord {
                lsn: 2,
                record: WalRecord::CommitTxnGlobal {
                    txn_id: TxnId(1),
                    commit_ts: Timestamp(5),
                },
            },
        ];
        let chunk = WalChunk::from_records(ShardId(3), records);

        let json = serde_json::to_string(&chunk).expect("WalChunk should serialize to JSON");
        let decoded: WalChunk =
            serde_json::from_str(&json).expect("WalChunk should deserialize from JSON");

        assert_eq!(decoded.shard_id, ShardId(3));
        assert_eq!(decoded.len(), 2);
        assert!(decoded.verify_checksum());
    }

    #[test]
    fn test_in_process_transport_pull_and_ack() {
        let log = Arc::new(ReplicationLog::new());
        let transport = InProcessTransport::new(ShardId(0), log.clone());

        // Append records to the log
        log.append(WalRecord::BeginTxn { txn_id: TxnId(1) });
        log.append(WalRecord::BeginTxn { txn_id: TxnId(2) });
        log.append(WalRecord::BeginTxn { txn_id: TxnId(3) });

        // Pull from LSN 0  鈥?should get all 3
        let chunk = transport.pull_wal_chunk(ShardId(0), 0, 100).unwrap();
        assert_eq!(chunk.len(), 3);
        assert!(chunk.verify_checksum());

        // Pull from LSN 2  鈥?should get only LSN 3
        let chunk2 = transport.pull_wal_chunk(ShardId(0), 2, 100).unwrap();
        assert_eq!(chunk2.len(), 1);
        assert_eq!(chunk2.start_lsn, 3);

        // Ack
        transport.ack_wal(ShardId(0), 0, 3).unwrap();
        assert_eq!(transport.get_ack_lsn(0), 3);
    }

    #[test]
    fn test_in_process_transport_shard_mismatch() {
        let log = Arc::new(ReplicationLog::new());
        let transport = InProcessTransport::new(ShardId(0), log);
        let result = transport.pull_wal_chunk(ShardId(1), 0, 100);
        assert!(result.is_err(), "should fail on shard mismatch");
    }

    #[test]
    fn test_channel_transport_push_pull() {
        let transport = ChannelTransport::new(ShardId(0));

        let records = vec![
            LsnWalRecord {
                lsn: 1,
                record: WalRecord::BeginTxn { txn_id: TxnId(1) },
            },
            LsnWalRecord {
                lsn: 2,
                record: WalRecord::BeginTxn { txn_id: TxnId(2) },
            },
        ];
        let chunk = WalChunk::from_records(ShardId(0), records);
        transport.push_chunk(chunk).unwrap();

        // Pull  鈥?should get the chunk
        let pulled = transport.pull_wal_chunk(ShardId(0), 0, 100).unwrap();
        assert_eq!(pulled.len(), 2);
        assert!(pulled.verify_checksum());

        // Pull again  鈥?nothing pending
        let empty = transport.pull_wal_chunk(ShardId(0), 0, 100).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_channel_transport_ack() {
        let transport = ChannelTransport::new(ShardId(0));
        assert_eq!(transport.get_ack_lsn(0), 0);

        transport.ack_wal(ShardId(0), 0, 5).unwrap();
        assert_eq!(transport.get_ack_lsn(0), 5);

        transport.ack_wal(ShardId(0), 0, 10).unwrap();
        assert_eq!(transport.get_ack_lsn(0), 10);

        // Different replica
        transport.ack_wal(ShardId(0), 1, 3).unwrap();
        assert_eq!(transport.get_ack_lsn(1), 3);
    }

    #[test]
    fn test_apply_chunk_to_replica() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        // Build a chunk manually
        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]);
        let records = vec![
            LsnWalRecord {
                lsn: 1,
                record: WalRecord::Insert {
                    txn_id: TxnId(1),
                    table_id: TableId(1),
                    row: row.clone(),
                },
            },
            LsnWalRecord {
                lsn: 2,
                record: WalRecord::CommitTxnLocal {
                    txn_id: TxnId(1),
                    commit_ts: Timestamp(1),
                },
            },
        ];
        let chunk = WalChunk::from_records(ShardId(0), records);

        // Apply chunk to replica
        let applied = group.apply_chunk_to_replica(0, &chunk).unwrap();
        assert_eq!(applied, 2);

        // Verify replica has the data
        let rows = group.replicas[0]
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[0], Datum::Int32(1));
    }

    #[test]
    fn test_apply_chunk_idempotent() {
        let group = ShardReplicaGroup::new(ShardId(0), &[test_schema()]).unwrap();

        let row = OwnedRow::new(vec![Datum::Int32(1), Datum::Text("hello".into())]);
        let records = vec![
            LsnWalRecord {
                lsn: 1,
                record: WalRecord::Insert {
                    txn_id: TxnId(1),
                    table_id: TableId(1),
                    row,
                },
            },
            LsnWalRecord {
                lsn: 2,
                record: WalRecord::CommitTxnLocal {
                    txn_id: TxnId(1),
                    commit_ts: Timestamp(1),
                },
            },
        ];
        let chunk = WalChunk::from_records(ShardId(0), records);

        // Apply once
        group.apply_chunk_to_replica(0, &chunk).unwrap();
        // Apply again (should be idempotent  鈥?skip already-applied LSNs)
        let applied2 = group.apply_chunk_to_replica(0, &chunk).unwrap();
        assert_eq!(applied2, 2); // returns chunk size but skips internally

        // Still just 1 row
        let rows = group.replicas[0]
            .storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_in_process_transport_pull_with_limit() {
        let log = Arc::new(ReplicationLog::new());
        let transport = InProcessTransport::new(ShardId(0), log.clone());

        for i in 1..=10u64 {
            log.append(WalRecord::BeginTxn { txn_id: TxnId(i) });
        }

        // Pull with limit of 3
        let chunk = transport.pull_wal_chunk(ShardId(0), 0, 3).unwrap();
        assert_eq!(chunk.len(), 3);
        assert_eq!(chunk.start_lsn, 1);
        assert_eq!(chunk.end_lsn, 3);
    }
