    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, ShardId, TableId, Timestamp, TxnId, TxnType};
    use falcon_storage::engine::StorageEngine;
    use falcon_storage::wal::WalRecord;

    use crate::grpc_transport::WalReplicationService;
    use crate::proto::wal_replication_server::WalReplicationServer;
    use crate::replication::{ReplicaRunner, ReplicaRunnerConfig, ReplicationLog};

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "accounts".into(),
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
                    name: "balance".into(),
                    data_type: DataType::Int64,
                    nullable: false,
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

    /// Full E2E test: primary gRPC server 鈫?ReplicaRunner 鈫?replica StorageEngine.
    ///
    /// The replica starts EMPTY (no pre-created tables). The ReplicaRunner
    /// bootstraps via GetCheckpoint, then subscribes to WAL for incremental updates.
    #[tokio::test]
    async fn test_replica_runner_e2e_cross_node() {
        // --- Primary setup ---
        let primary_storage = Arc::new(StorageEngine::new_in_memory());
        primary_storage.create_table(test_schema()).unwrap();

        // Write initial data on primary BEFORE starting the server so the
        // checkpoint will contain this data.
        let row1 = OwnedRow::new(vec![Datum::Int32(1), Datum::Int64(1000)]);
        primary_storage
            .insert(TableId(1), row1.clone(), TxnId(1))
            .unwrap();
        primary_storage
            .commit_txn(TxnId(1), Timestamp(10), TxnType::Local)
            .unwrap();

        let row2 = OwnedRow::new(vec![Datum::Int32(2), Datum::Int64(2500)]);
        primary_storage
            .insert(TableId(1), row2.clone(), TxnId(2))
            .unwrap();
        primary_storage
            .commit_txn(TxnId(2), Timestamp(20), TxnType::Local)
            .unwrap();

        let log = Arc::new(ReplicationLog::new());
        // Ship the initial WAL records so the replica can replay them after checkpoint.
        log.append(WalRecord::Insert {
            txn_id: TxnId(1),
            table_id: TableId(1),
            row: row1,
        });
        log.append(WalRecord::CommitTxnLocal {
            txn_id: TxnId(1),
            commit_ts: Timestamp(10),
        });
        log.append(WalRecord::Insert {
            txn_id: TxnId(2),
            table_id: TableId(1),
            row: row2,
        });
        log.append(WalRecord::CommitTxnLocal {
            txn_id: TxnId(2),
            commit_ts: Timestamp(20),
        });

        let svc = WalReplicationService::new();
        svc.register_shard(ShardId(0), log.clone());
        // Wire primary storage so GetCheckpoint RPC works.
        svc.set_storage(primary_storage.clone());

        // Start gRPC server on random port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let _ = tonic::transport::Server::builder()
                .add_service(WalReplicationServer::new(svc))
                .serve_with_incoming(incoming)
                .await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // --- Replica setup: starts EMPTY, bootstraps via checkpoint ---
        let replica_storage = Arc::new(StorageEngine::new_in_memory());

        let config = ReplicaRunnerConfig {
            primary_endpoint: format!("http://127.0.0.1:{}", addr.port()),
            shard_id: ShardId(0),
            replica_id: 0,
            max_records_per_chunk: 100,
            ack_interval_chunks: 1,
            initial_backoff: std::time::Duration::from_millis(50),
            max_backoff: std::time::Duration::from_secs(1),
            connect_timeout: std::time::Duration::from_secs(5),
        };

        let runner = ReplicaRunner::new(config, replica_storage.clone());
        let handle = runner.start();

        // --- Wait for checkpoint bootstrap + WAL catch-up ---
        // The checkpoint contains 2 rows; after bootstrap applied_lsn = ckpt_lsn (0 for
        // in-memory engine with no WAL). Then WAL records are replayed on top.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "Replica did not catch up within timeout (applied_lsn={})",
                    handle.metrics().applied_lsn.load(Ordering::Relaxed)
                );
            }
            // Check if replica has the table (schema bootstrapped from checkpoint)
            if replica_storage.get_table_schema("accounts").is_some() {
                let rows = replica_storage
                    .scan(TableId(1), TxnId(999), Timestamp(100))
                    .unwrap_or_default();
                if rows.len() >= 2 {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // --- Verify replica has the data ---
        let rows = replica_storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert!(
            rows.len() >= 2,
            "Replica should have at least 2 rows after bootstrap"
        );

        let snap = handle.metrics().snapshot();
        assert!(snap.connected);

        // --- Write MORE data on primary ---
        let row3 = OwnedRow::new(vec![Datum::Int32(3), Datum::Int64(500)]);
        primary_storage
            .insert(TableId(1), row3.clone(), TxnId(3))
            .unwrap();
        primary_storage
            .commit_txn(TxnId(3), Timestamp(30), TxnType::Local)
            .unwrap();

        log.append(WalRecord::Insert {
            txn_id: TxnId(3),
            table_id: TableId(1),
            row: row3,
        });
        log.append(WalRecord::CommitTxnLocal {
            txn_id: TxnId(3),
            commit_ts: Timestamp(30),
        });

        // Wait for replica to apply the new WAL record
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if tokio::time::Instant::now() > deadline {
                panic!("Replica did not catch up to new data");
            }
            let rows = replica_storage
                .scan(TableId(1), TxnId(999), Timestamp(100))
                .unwrap_or_default();
            if rows.len() >= 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let rows = replica_storage
            .scan(TableId(1), TxnId(999), Timestamp(100))
            .unwrap();
        assert!(
            rows.len() >= 3,
            "Replica should have 3 rows after incremental catch-up"
        );

        // --- Clean shutdown ---
        handle.stop().await;
        server_handle.abort();
    }

    /// Test that ReplicaRunner retries on connection failure and increments reconnect_count.
    #[tokio::test]
    async fn test_replica_runner_reconnect_on_failure() {
        // Use an unreachable endpoint so every connect attempt fails fast.
        let replica_storage = Arc::new(StorageEngine::new_in_memory());

        let config = ReplicaRunnerConfig {
            primary_endpoint: "http://127.0.0.1:1".into(), // port 1  鈥?refused
            shard_id: ShardId(0),
            replica_id: 0,
            max_records_per_chunk: 100,
            ack_interval_chunks: 1,
            initial_backoff: std::time::Duration::from_millis(30),
            max_backoff: std::time::Duration::from_millis(60),
            connect_timeout: std::time::Duration::from_millis(100),
        };

        let runner = ReplicaRunner::new(config, replica_storage);
        let handle = runner.start();

        // Wait for a few reconnect cycles (each ~30-100ms)
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;

        // Runner should still be alive, retrying
        assert!(
            handle.is_running(),
            "Runner should still be alive while retrying"
        );

        let snap = handle.metrics().snapshot();
        assert!(
            snap.reconnect_count >= 2,
            "Expected at least 2 reconnect attempts, got {}",
            snap.reconnect_count
        );
        assert!(
            !snap.connected,
            "Should not be connected to unreachable endpoint"
        );
        assert_eq!(snap.applied_lsn, 0, "No data should have been applied");

        handle.stop().await;
    }

    /// Test that ReplicaRunner stops cleanly when signaled.
    #[tokio::test]
    async fn test_replica_runner_clean_stop() {
        // Point at a non-existent server  鈥?runner will keep retrying
        let replica_storage = Arc::new(StorageEngine::new_in_memory());
        let config = ReplicaRunnerConfig {
            primary_endpoint: "http://127.0.0.1:1".into(), // unlikely to be open
            shard_id: ShardId(0),
            replica_id: 0,
            initial_backoff: std::time::Duration::from_millis(50),
            max_backoff: std::time::Duration::from_millis(100),
            connect_timeout: std::time::Duration::from_millis(100),
            ..ReplicaRunnerConfig::default()
        };

        let runner = ReplicaRunner::new(config, replica_storage);
        let handle = runner.start();

        // Let it attempt a few reconnects
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(handle.is_running());

        // Stop should complete within a reasonable time
        let stop_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        handle.stop().await;
        assert!(
            tokio::time::Instant::now() < stop_deadline,
            "Stop took too long"
        );
    }

    /// Test ReplicationLog::trim_before removes records at or below the safe LSN.
    #[test]
    fn test_replication_log_trim_before() {
        use crate::replication::ReplicationLog;
        use falcon_common::types::TxnId;
        use falcon_storage::wal::WalRecord;

        let log = ReplicationLog::new();
        log.append(WalRecord::BeginTxn { txn_id: TxnId(1) }); // LSN 1
        log.append(WalRecord::BeginTxn { txn_id: TxnId(2) }); // LSN 2
        log.append(WalRecord::BeginTxn { txn_id: TxnId(3) }); // LSN 3
        log.append(WalRecord::BeginTxn { txn_id: TxnId(4) }); // LSN 4
        assert_eq!(log.len(), 4);
        assert_eq!(log.current_lsn(), 4);

        // Trim records with LSN <= 2
        let removed = log.trim_before(2);
        assert_eq!(removed, 2, "Should remove LSN 1 and 2");
        assert_eq!(log.len(), 2, "Should have LSN 3 and 4 remaining");
        assert_eq!(log.min_lsn(), 3, "Minimum remaining LSN should be 3");

        // read_from(0) should still return LSN 3 and 4
        let records = log.read_from(0);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].lsn, 3);
        assert_eq!(records[1].lsn, 4);

        // Trim everything
        let removed2 = log.trim_before(4);
        assert_eq!(removed2, 2);
        assert_eq!(log.len(), 0);
        assert_eq!(log.min_lsn(), 0);

        // Trim on empty log is a no-op
        let removed3 = log.trim_before(100);
        assert_eq!(removed3, 0);
    }

    /// Test ReplicationLog capacity cap: oldest records are evicted when full.
    #[test]
    fn test_replication_log_capacity_eviction() {
        use crate::replication::ReplicationLog;
        use falcon_common::types::TxnId;
        use falcon_storage::wal::WalRecord;

        let log = ReplicationLog::with_capacity(3);
        assert_eq!(log.max_capacity(), 3);
        assert_eq!(log.evicted_records(), 0);

        let lsn1 = log.append(WalRecord::BeginTxn { txn_id: TxnId(1) });
        let lsn2 = log.append(WalRecord::BeginTxn { txn_id: TxnId(2) });
        let lsn3 = log.append(WalRecord::BeginTxn { txn_id: TxnId(3) });
        assert_eq!(log.len(), 3);
        assert_eq!(log.evicted_records(), 0);

        // 4th append should evict LSN 1
        let lsn4 = log.append(WalRecord::BeginTxn { txn_id: TxnId(4) });
        assert_eq!(log.len(), 3, "capacity cap: still 3 records");
        assert_eq!(log.evicted_records(), 1, "one record evicted");
        assert_eq!(log.min_lsn(), lsn2, "LSN 1 evicted, min is now LSN 2");

        // 5th append evicts LSN 2
        let lsn5 = log.append(WalRecord::BeginTxn { txn_id: TxnId(5) });
        assert_eq!(log.evicted_records(), 2);
        assert_eq!(log.min_lsn(), lsn3);

        // read_from(0) returns only the 3 retained records
        let records = log.read_from(0);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].lsn, lsn3);
        assert_eq!(records[1].lsn, lsn4);
        assert_eq!(records[2].lsn, lsn5);

        // current_lsn is still the highest ever assigned
        assert_eq!(log.current_lsn(), lsn5);
        let _ = (lsn1, lsn2); // suppress unused warnings
    }

    /// Test ReplicaRunnerMetricsSnapshot.lag_lsn is computed correctly.
    #[test]
    fn test_replica_runner_metrics_lag_lsn() {
        use crate::replication::ReplicaRunnerMetrics;
        use std::sync::atomic::Ordering;

        let m = ReplicaRunnerMetrics::default();

        // Initially both are 0 鈫?lag is 0
        let snap = m.snapshot();
        assert_eq!(snap.applied_lsn, 0);
        assert_eq!(snap.primary_lsn, 0);
        assert_eq!(snap.lag_lsn, 0);

        // Primary is at 100, replica applied 80 鈫?lag = 20
        m.primary_lsn.store(100, Ordering::Relaxed);
        m.applied_lsn.store(80, Ordering::SeqCst);
        let snap = m.snapshot();
        assert_eq!(snap.lag_lsn, 20);

        // Fully caught up
        m.applied_lsn.store(100, Ordering::SeqCst);
        let snap = m.snapshot();
        assert_eq!(snap.lag_lsn, 0);

        // applied > primary (shouldn't happen in practice) 鈫?saturating_sub gives 0
        m.applied_lsn.store(110, Ordering::SeqCst);
        let snap = m.snapshot();
        assert_eq!(snap.lag_lsn, 0);
    }

    /// Test StorageEngine::apply_checkpoint_data bootstraps a replica correctly.
    #[test]
    fn test_apply_checkpoint_data_bootstrap() {
        use falcon_common::datum::{Datum, OwnedRow};
        use falcon_common::schema::{Catalog, ColumnDef, TableSchema};
        use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};
        use falcon_storage::engine::StorageEngine;
        use falcon_storage::wal::CheckpointData;

        // Build a checkpoint with one table and two rows.
        let schema = TableSchema {
            id: TableId(1),
            name: "accounts".into(),
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
                    name: "balance".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        };

        let mut catalog = Catalog::default();
        catalog.add_table(schema.clone());

        // Encode PK bytes for the two rows (Int32 big-endian with sign flip)
        let pk1 = falcon_storage::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int32(1), Datum::Int64(1000)]),
            &[0],
        );
        let pk2 = falcon_storage::memtable::encode_pk(
            &OwnedRow::new(vec![Datum::Int32(2), Datum::Int64(2500)]),
            &[0],
        );

        let ckpt = CheckpointData {
            catalog,
            table_data: vec![(
                TableId(1),
                vec![
                    (
                        pk1,
                        OwnedRow::new(vec![Datum::Int32(1), Datum::Int64(1000)]),
                    ),
                    (
                        pk2,
                        OwnedRow::new(vec![Datum::Int32(2), Datum::Int64(2500)]),
                    ),
                ],
            )],
            wal_segment_id: 0,
            wal_lsn: 42,
        };

        // Apply to a fresh empty engine
        let engine = StorageEngine::new_in_memory();
        engine.apply_checkpoint_data(&ckpt).unwrap();

        // Verify schema was restored
        assert!(engine.get_table_schema("accounts").is_some());

        // Verify rows were restored
        let rows = engine
            .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
            .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "Both rows should be present after checkpoint apply"
        );

        // Apply again (idempotent: clears and re-applies)
        engine.apply_checkpoint_data(&ckpt).unwrap();
        let rows2 = engine
            .scan(TableId(1), TxnId(999), Timestamp(u64::MAX - 1))
            .unwrap();
        assert_eq!(
            rows2.len(),
            2,
            "Idempotent re-apply should still have 2 rows"
        );
    }
