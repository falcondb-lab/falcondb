    use std::sync::Arc;

    use falcon_common::types::{ShardId, TxnId};
    use falcon_storage::wal::WalRecord;

    use crate::replication::{
        AsyncReplicationTransport, ChannelTransport, InProcessTransport, LsnWalRecord,
        ReplicationLog, WalChunk,
    };

    #[tokio::test]
    async fn test_async_pull_via_in_process_transport() {
        let log = Arc::new(ReplicationLog::new());
        let transport = InProcessTransport::new(ShardId(0), log.clone());

        log.append(WalRecord::BeginTxn { txn_id: TxnId(1) });
        log.append(WalRecord::BeginTxn { txn_id: TxnId(2) });

        // Use the async trait method (blanket impl delegates to sync)
        let chunk = AsyncReplicationTransport::pull_wal_chunk(&transport, ShardId(0), 0, 100)
            .await
            .unwrap();
        assert_eq!(chunk.len(), 2);
        assert!(chunk.verify_checksum());
    }

    #[tokio::test]
    async fn test_async_ack_via_in_process_transport() {
        let log = Arc::new(ReplicationLog::new());
        let transport = InProcessTransport::new(ShardId(0), log);

        AsyncReplicationTransport::ack_wal(&transport, ShardId(0), 0, 5)
            .await
            .unwrap();
        assert_eq!(transport.get_ack_lsn(0), 5);
    }

    #[tokio::test]
    async fn test_async_pull_via_channel_transport() {
        let transport = ChannelTransport::new(ShardId(0));

        let records = vec![LsnWalRecord {
            lsn: 1,
            record: WalRecord::BeginTxn { txn_id: TxnId(1) },
        }];
        let chunk = WalChunk::from_records(ShardId(0), records);
        transport.push_chunk(chunk).unwrap();

        let pulled = AsyncReplicationTransport::pull_wal_chunk(&transport, ShardId(0), 0, 100)
            .await
            .unwrap();
        assert_eq!(pulled.len(), 1);
        assert!(pulled.verify_checksum());
    }
