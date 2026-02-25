    use std::sync::Arc;

    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::types::{ShardId, TableId, Timestamp, TxnId};
    use falcon_storage::wal::WalRecord;

    use crate::grpc_transport::{
        decode_wal_chunk, decode_wal_record, encode_wal_chunk, encode_wal_record, GrpcTransport,
        WalReplicationService,
    };
    use crate::replication::{AsyncReplicationTransport, LsnWalRecord, ReplicationLog, WalChunk};

    #[test]
    fn test_wal_record_encode_decode_roundtrip() {
        let record = LsnWalRecord {
            lsn: 42,
            record: WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: OwnedRow::new(vec![Datum::Int32(7), Datum::Text("hello".into())]),
            },
        };
        let bytes = encode_wal_record(&record).unwrap();
        let decoded = decode_wal_record(&bytes).unwrap();
        assert_eq!(decoded.lsn, 42);
    }

    #[test]
    fn test_wal_chunk_encode_decode_roundtrip() {
        let records = vec![
            LsnWalRecord {
                lsn: 1,
                record: WalRecord::BeginTxn { txn_id: TxnId(1) },
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
        let bytes = encode_wal_chunk(&chunk).unwrap();
        let decoded = decode_wal_chunk(&bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.start_lsn, 1);
        assert_eq!(decoded.end_lsn, 2);
        assert!(decoded.verify_checksum());
    }

    #[test]
    fn test_wal_replication_service_handle_pull() {
        let log = Arc::new(ReplicationLog::new());
        log.append(WalRecord::BeginTxn { txn_id: TxnId(1) });
        log.append(WalRecord::BeginTxn { txn_id: TxnId(2) });
        log.append(WalRecord::BeginTxn { txn_id: TxnId(3) });

        let svc = WalReplicationService::new();
        svc.register_shard(ShardId(0), log);

        // Pull all from LSN 0
        let chunk = svc.handle_pull(ShardId(0), 0, 100).unwrap();
        assert_eq!(chunk.len(), 3);

        // Pull from LSN 2  鈥?should get only LSN 3
        let chunk2 = svc.handle_pull(ShardId(0), 2, 100).unwrap();
        assert_eq!(chunk2.len(), 1);
        assert_eq!(chunk2.start_lsn, 3);

        // Pull with limit
        let chunk3 = svc.handle_pull(ShardId(0), 0, 2).unwrap();
        assert_eq!(chunk3.len(), 2);
    }

    #[test]
    fn test_wal_replication_service_unknown_shard() {
        let svc = WalReplicationService::new();
        let result = svc.handle_pull(ShardId(99), 0, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_wal_replication_service_ack_and_lag() {
        let log = Arc::new(ReplicationLog::new());
        log.append(WalRecord::BeginTxn { txn_id: TxnId(1) });
        log.append(WalRecord::BeginTxn { txn_id: TxnId(2) });

        let svc = WalReplicationService::new();
        svc.register_shard(ShardId(0), log);

        // No acks yet
        assert_eq!(svc.get_ack_lsn(ShardId(0), 0), 0);

        // Ack LSN 1 from replica 0
        svc.handle_ack(ShardId(0), 0, 1);
        assert_eq!(svc.get_ack_lsn(ShardId(0), 0), 1);

        // Lag: primary at LSN 2, replica at LSN 1  鈫?lag = 1
        let lags = svc.replication_lag(ShardId(0));
        assert_eq!(lags.len(), 1);
        assert_eq!(lags[0], (0, 1));
    }

    #[tokio::test]
    async fn test_grpc_transport_pull_connection_refused() {
        // GrpcTransport now actually connects via gRPC  鈥?connecting to a
        // non-existent server should return a connection error.
        let transport = GrpcTransport::new("http://127.0.0.1:19999".into(), ShardId(0));
        let result =
            AsyncReplicationTransport::pull_wal_chunk(&transport, ShardId(0), 0, 100).await;
        assert!(result.is_err(), "pull to non-existent server should fail");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("gRPC connect error"),
            "Expected connect error, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_grpc_transport_ack_connection_refused() {
        let transport = GrpcTransport::new("http://127.0.0.1:19999".into(), ShardId(0));
        let result = AsyncReplicationTransport::ack_wal(&transport, ShardId(0), 0, 10).await;
        assert!(result.is_err(), "ack to non-existent server should fail");
    }

    #[tokio::test]
    async fn test_grpc_e2e_subscribe_and_ack() {
        use crate::proto::wal_replication_server::WalReplicationServer;

        // --- Set up server with some WAL records ---
        let log = Arc::new(crate::replication::ReplicationLog::new());
        log.append(falcon_storage::wal::WalRecord::BeginTxn { txn_id: TxnId(1) });
        log.append(falcon_storage::wal::WalRecord::BeginTxn { txn_id: TxnId(2) });
        log.append(falcon_storage::wal::WalRecord::CommitTxnLocal {
            txn_id: TxnId(1),
            commit_ts: Timestamp(100),
        });

        let svc = crate::grpc_transport::WalReplicationService::new();
        svc.register_shard(ShardId(0), log);

        // --- Start tonic server on a random port ---
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            tonic::transport::Server::builder()
                .add_service(WalReplicationServer::new(svc))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        // Give the server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // --- Client: pull WAL chunk ---
        let endpoint = format!("http://127.0.0.1:{}", addr.port());
        let transport = GrpcTransport::new(endpoint, ShardId(0));

        let chunk = AsyncReplicationTransport::pull_wal_chunk(&transport, ShardId(0), 0, 100)
            .await
            .unwrap();

        assert_eq!(chunk.len(), 3, "Should get all 3 WAL records");
        assert!(chunk.verify_checksum());
        assert_eq!(chunk.start_lsn, 1);
        assert_eq!(chunk.end_lsn, 3);

        // --- Client: pull from LSN 2 (should get records 3 only) ---
        let chunk2 = AsyncReplicationTransport::pull_wal_chunk(&transport, ShardId(0), 2, 100)
            .await
            .unwrap();
        assert_eq!(chunk2.len(), 1);
        assert_eq!(chunk2.start_lsn, 3);

        // --- Client: ack WAL ---
        AsyncReplicationTransport::ack_wal(&transport, ShardId(0), 0, 3)
            .await
            .unwrap();
        assert_eq!(transport.get_ack_lsn(0), 3);

        // Clean up
        server_handle.abort();
    }

    // --- Proto roundtrip tests ---

    #[test]
    fn test_proto_wal_chunk_roundtrip() {
        use crate::grpc_transport::{proto_to_wal_chunk, wal_chunk_to_proto};

        let records = vec![
            LsnWalRecord {
                lsn: 1,
                record: falcon_storage::wal::WalRecord::BeginTxn { txn_id: TxnId(1) },
            },
            LsnWalRecord {
                lsn: 2,
                record: falcon_storage::wal::WalRecord::Insert {
                    txn_id: TxnId(1),
                    table_id: falcon_common::types::TableId(10),
                    row: falcon_common::datum::OwnedRow::new(vec![
                        falcon_common::datum::Datum::Int32(42),
                        falcon_common::datum::Datum::Text("hello".into()),
                    ]),
                },
            },
            LsnWalRecord {
                lsn: 3,
                record: falcon_storage::wal::WalRecord::CommitTxnLocal {
                    txn_id: TxnId(1),
                    commit_ts: Timestamp(100),
                },
            },
        ];

        let chunk = WalChunk::from_records(ShardId(7), records);
        assert_eq!(chunk.len(), 3);
        assert_eq!(chunk.shard_id, ShardId(7));
        assert!(chunk.verify_checksum());

        // Domain  鈫?Proto
        let proto_msg = wal_chunk_to_proto(&chunk).unwrap();
        assert_eq!(proto_msg.shard_id, 7);
        assert_eq!(proto_msg.start_lsn, 1);
        assert_eq!(proto_msg.end_lsn, 3);
        assert_eq!(proto_msg.records.len(), 3);
        assert_eq!(proto_msg.checksum, chunk.checksum);

        // Proto  鈫?Domain
        let roundtripped = proto_to_wal_chunk(&proto_msg).unwrap();
        assert_eq!(roundtripped.shard_id, ShardId(7));
        assert_eq!(roundtripped.start_lsn, 1);
        assert_eq!(roundtripped.end_lsn, 3);
        assert_eq!(roundtripped.len(), 3);
        assert!(roundtripped.verify_checksum());

        // Verify record contents survived roundtrip
        assert_eq!(roundtripped.records[0].lsn, 1);
        assert_eq!(roundtripped.records[1].lsn, 2);
        assert_eq!(roundtripped.records[2].lsn, 3);
    }

    #[test]
    fn test_proto_empty_chunk_roundtrip() {
        use crate::grpc_transport::{proto_to_wal_chunk, wal_chunk_to_proto};

        let chunk = WalChunk::empty(ShardId(0));
        let proto_msg = wal_chunk_to_proto(&chunk).unwrap();
        let roundtripped = proto_to_wal_chunk(&proto_msg).unwrap();
        assert!(roundtripped.is_empty());
        assert_eq!(roundtripped.shard_id, ShardId(0));
    }

    // --- Checkpoint streaming tests ---

    use crate::grpc_transport::{CheckpointAssembler, CheckpointStreamer};

    #[test]
    fn test_checkpoint_streamer_single_chunk() {
        let data = vec![1u8; 100];
        let streamer = CheckpointStreamer::from_bytes(data.clone(), 42, 256);
        assert_eq!(streamer.num_chunks(), 1);
        assert_eq!(streamer.total_bytes(), 100);
        let chunks = streamer.chunks();
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].total_chunks, 1);
        assert_eq!(chunks[0].checkpoint_lsn, 42);
        assert_eq!(chunks[0].data, data);
    }

    #[test]
    fn test_checkpoint_streamer_multi_chunk() {
        let data = vec![0xABu8; 1000];
        let streamer = CheckpointStreamer::from_bytes(data, 99, 300);
        // 1000 / 300 = 4 chunks (300, 300, 300, 100)
        assert_eq!(streamer.num_chunks(), 4);
        assert_eq!(streamer.total_bytes(), 1000);
        for (i, chunk) in streamer.chunks().iter().enumerate() {
            assert_eq!(chunk.chunk_index, i as u32);
            assert_eq!(chunk.total_chunks, 4);
            assert_eq!(chunk.checkpoint_lsn, 99);
        }
        assert_eq!(streamer.chunks()[3].data.len(), 100);
    }

    #[test]
    fn test_checkpoint_streamer_empty() {
        let streamer = CheckpointStreamer::from_bytes(vec![], 0, 256);
        assert_eq!(streamer.num_chunks(), 0);
        assert_eq!(streamer.total_bytes(), 0);
    }

    #[test]
    fn test_checkpoint_assembler_in_order() {
        let data = vec![0xCDu8; 500];
        let streamer = CheckpointStreamer::from_bytes(data.clone(), 10, 200);
        assert_eq!(streamer.num_chunks(), 3);

        let mut assembler = CheckpointAssembler::new(3, 10);
        assert!(!assembler.is_complete());

        for chunk in streamer.chunks() {
            assembler.add_chunk(chunk).unwrap();
        }
        assert!(assembler.is_complete());
        let reassembled = assembler.assemble().unwrap();
        assert_eq!(reassembled, data);
        assert_eq!(assembler.checkpoint_lsn(), 10);
    }

    #[test]
    fn test_checkpoint_assembler_out_of_order() {
        let data = vec![0xEFu8; 600];
        let streamer = CheckpointStreamer::from_bytes(data.clone(), 20, 200);
        let chunks = streamer.chunks();
        assert_eq!(chunks.len(), 3);

        let mut assembler = CheckpointAssembler::new(3, 20);
        // Add chunks out of order: 2, 0, 1
        assert!(!assembler.add_chunk(&chunks[2]).unwrap());
        assert!(!assembler.add_chunk(&chunks[0]).unwrap());
        assert!(assembler.add_chunk(&chunks[1]).unwrap());
        assert_eq!(assembler.assemble().unwrap(), data);
    }

    #[test]
    fn test_checkpoint_assembler_incomplete_fails() {
        let mut assembler = CheckpointAssembler::new(3, 5);
        let chunk = crate::grpc_transport::CheckpointChunk {
            chunk_index: 0,
            total_chunks: 3,
            data: vec![1, 2, 3],
            checkpoint_lsn: 5,
        };
        assembler.add_chunk(&chunk).unwrap();
        assert!(!assembler.is_complete());
        assert!(assembler.assemble().is_err());
    }

    #[test]
    fn test_checkpoint_assembler_out_of_range_chunk() {
        let mut assembler = CheckpointAssembler::new(2, 5);
        let bad_chunk = crate::grpc_transport::CheckpointChunk {
            chunk_index: 5,
            total_chunks: 2,
            data: vec![1],
            checkpoint_lsn: 5,
        };
        assert!(assembler.add_chunk(&bad_chunk).is_err());
    }

    #[test]
    fn test_checkpoint_data_full_roundtrip() {
        use falcon_common::schema::{Catalog, ColumnDef, TableSchema};
        use falcon_common::types::{ColumnId, DataType};
        use falcon_storage::wal::CheckpointData;

        // Build a realistic CheckpointData with schema + rows
        let schema = TableSchema {
            id: TableId(1),
            name: "users".into(),
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
            ],
            primary_key_columns: vec![0],
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        };

        let mut catalog = Catalog::default();
        catalog.add_table(schema);

        let rows = vec![
            (
                vec![0, 0, 0, 1],
                OwnedRow::new(vec![Datum::Int32(1), Datum::Text("alice".into())]),
            ),
            (
                vec![0, 0, 0, 2],
                OwnedRow::new(vec![Datum::Int32(2), Datum::Text("bob".into())]),
            ),
        ];

        let ckpt = CheckpointData {
            catalog,
            table_data: vec![(TableId(1), rows)],
            wal_segment_id: 3,
            wal_lsn: 42,
        };

        // Stream  鈫?assemble roundtrip with small chunk size to force multi-chunk
        let streamer = CheckpointStreamer::from_checkpoint_data(&ckpt).unwrap();
        assert!(streamer.num_chunks() >= 1);

        let first = &streamer.chunks()[0];
        let mut assembler = CheckpointAssembler::new(first.total_chunks, first.checkpoint_lsn);
        for chunk in streamer.chunks() {
            assembler.add_chunk(chunk).unwrap();
        }
        assert!(assembler.is_complete());

        let restored = assembler.assemble_checkpoint().unwrap();
        assert_eq!(restored.wal_lsn, 42);
        assert_eq!(restored.wal_segment_id, 3);
        assert_eq!(restored.catalog.table_count(), 1);
        assert!(restored.catalog.find_table("users").is_some());
        assert_eq!(restored.table_data.len(), 1);
        assert_eq!(restored.table_data[0].1.len(), 2);
        assert_eq!(
            restored.table_data[0].1[0].1.values[1],
            Datum::Text("alice".into())
        );
        assert_eq!(
            restored.table_data[0].1[1].1.values[1],
            Datum::Text("bob".into())
        );
    }
