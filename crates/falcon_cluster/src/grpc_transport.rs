//! M2 gRPC transport layer for WAL replication.
//!
//! This module provides:
//! - `GrpcTransport`: client-side transport implementing `AsyncReplicationTransport`
//!   that sends/receives WAL chunks over a tonic gRPC channel.
//! - `WalReplicationService`: server-side handler that serves WAL chunks from a
//!   `ReplicationLog` to connected replicas.
//!
//! Wire format: `WalChunk` and `LsnWalRecord` are serialized as JSON bytes in the
//! proto `WalRecordEntry.record_payload` field. This avoids proto schema drift —
//! the Rust types are the source of truth, proto is the envelope.
//!
//! Requires: `cargo build -p falcon_cluster --features grpc-codegen` to generate
//! the tonic client/server stubs from `proto/falcon_replication.proto`.
//! Until then, this module provides the architecture and conversion logic.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;

use falcon_common::error::FalconError;
use falcon_common::types::ShardId;

use crate::replication::{
    AsyncReplicationTransport, LsnWalRecord, ReplicationLog, WalChunk,
};

// ---------------------------------------------------------------------------
// Wire ↔ Domain conversion helpers
// ---------------------------------------------------------------------------

/// Serialize a `LsnWalRecord` to JSON bytes for the proto `record_payload` field.
pub fn encode_wal_record(record: &LsnWalRecord) -> Result<Vec<u8>, FalconError> {
    serde_json::to_vec(record)
        .map_err(|e| FalconError::Internal(format!("WAL record serialization error: {}", e)))
}

/// Deserialize a `LsnWalRecord` from JSON bytes received in proto `record_payload`.
pub fn decode_wal_record(payload: &[u8]) -> Result<LsnWalRecord, FalconError> {
    serde_json::from_slice(payload)
        .map_err(|e| FalconError::Internal(format!("WAL record deserialization error: {}", e)))
}

/// Serialize a `WalChunk` to JSON bytes for network transport.
pub fn encode_wal_chunk(chunk: &WalChunk) -> Result<Vec<u8>, FalconError> {
    serde_json::to_vec(chunk)
        .map_err(|e| FalconError::Internal(format!("WalChunk serialization error: {}", e)))
}

/// Deserialize a `WalChunk` from JSON bytes received over the network.
pub fn decode_wal_chunk(payload: &[u8]) -> Result<WalChunk, FalconError> {
    let chunk: WalChunk = serde_json::from_slice(payload)
        .map_err(|e| FalconError::Internal(format!("WalChunk deserialization error: {}", e)))?;
    if !chunk.verify_checksum() {
        return Err(FalconError::Internal("WalChunk checksum mismatch after deserialization".into()));
    }
    Ok(chunk)
}

// ---------------------------------------------------------------------------
// GrpcTransport — client-side async transport
// ---------------------------------------------------------------------------

/// Client-side gRPC transport for WAL replication.
///
/// In M2, this will hold a `tonic::transport::Channel` and call the generated
/// `WalReplicationClient` stubs. For now, it wraps a target endpoint address
/// and provides the conversion/serialization logic.
///
/// Architecture:
/// ```text
/// Replica                          Primary
/// ┌──────────┐   SubscribeWal     ┌──────────────────┐
/// │GrpcTransport│ ──gRPC stream──▶ │WalReplicationSvc │
/// │  (client)   │ ◀──WalChunks──── │  (server)        │
/// └──────────┘   AckWal(unary)    └──────────────────┘
/// ```
pub struct GrpcTransport {
    /// Target primary endpoint (e.g. "http://primary:50051").
    pub endpoint: String,
    /// Shard this transport is bound to.
    pub shard_id: ShardId,
    /// Local ack tracking (client-side bookkeeping before sending ack RPCs).
    ack_lsns: Mutex<HashMap<usize, u64>>,
    /// Cached tonic channel for connection reuse.
    channel: tokio::sync::Mutex<Option<tonic::transport::Channel>>,
    /// Connect timeout duration.
    connect_timeout: std::time::Duration,
}

impl GrpcTransport {
    pub fn new(endpoint: String, shard_id: ShardId) -> Self {
        Self::with_timeout(endpoint, shard_id, std::time::Duration::from_secs(5))
    }

    pub fn with_timeout(endpoint: String, shard_id: ShardId, connect_timeout: std::time::Duration) -> Self {
        Self {
            endpoint,
            shard_id,
            ack_lsns: Mutex::new(HashMap::new()),
            channel: tokio::sync::Mutex::new(None),
            connect_timeout,
        }
    }

    /// Get or create a cached tonic channel. Invalidates on error.
    async fn get_channel(&self) -> Result<tonic::transport::Channel, FalconError> {
        let mut guard = self.channel.lock().await;
        if let Some(ref ch) = *guard {
            return Ok(ch.clone());
        }
        let ch = tonic::transport::Endpoint::from_shared(self.endpoint.clone())
            .map_err(|e| FalconError::Internal(format!("Invalid endpoint: {}", e)))?
            .connect_timeout(self.connect_timeout)
            .connect()
            .await
            .map_err(|e| FalconError::Internal(format!("gRPC connect error: {}", e)))?;
        *guard = Some(ch.clone());
        Ok(ch)
    }

    /// Invalidate the cached channel (e.g. after a transport error).
    async fn invalidate_channel(&self) {
        let mut guard = self.channel.lock().await;
        *guard = None;
    }

    /// Get the locally tracked ack LSN for a replica.
    pub fn get_ack_lsn(&self, replica_id: usize) -> u64 {
        self.ack_lsns.lock().get(&replica_id).copied().unwrap_or(0)
    }
}

impl GrpcTransport {
    /// Download a full checkpoint from the primary for new replica bootstrap.
    ///
    /// The primary streams checkpoint chunks; this method reassembles them
    /// into `CheckpointData` and returns the LSN at which the checkpoint was taken.
    /// The replica should then start WAL streaming from that LSN.
    pub async fn download_checkpoint(
        &self,
        shard_id: ShardId,
    ) -> Result<(falcon_storage::wal::CheckpointData, u64), FalconError> {
        use crate::proto::wal_replication_client::WalReplicationClient;
        use tokio_stream::StreamExt;

        let channel = self.get_channel().await?;
        let mut client = WalReplicationClient::new(channel);

        let request = crate::proto::GetCheckpointRequest {
            shard_id: shard_id.0,
        };

        let response = match client.get_checkpoint(request).await {
            Ok(r) => r,
            Err(e) => {
                self.invalidate_channel().await;
                return Err(FalconError::Internal(format!("gRPC get_checkpoint error: {}", e)));
            }
        };

        let mut stream = response.into_inner();
        let mut assembler: Option<CheckpointAssembler> = None;

        while let Some(result) = stream.next().await {
            match result {
                Ok(proto_chunk) => {
                    let chunk = CheckpointChunk {
                        chunk_index: proto_chunk.chunk_index,
                        total_chunks: proto_chunk.total_chunks,
                        data: proto_chunk.data,
                        checkpoint_lsn: proto_chunk.checkpoint_lsn,
                    };
                    let asm = assembler.get_or_insert_with(|| {
                        CheckpointAssembler::new(chunk.total_chunks, chunk.checkpoint_lsn)
                    });
                    let complete = asm.add_chunk(&chunk)?;
                    if complete {
                        break;
                    }
                }
                Err(e) => {
                    self.invalidate_channel().await;
                    return Err(FalconError::Internal(format!(
                        "gRPC checkpoint stream error: {}", e
                    )));
                }
            }
        }

        let asm = assembler.ok_or_else(|| {
            FalconError::Internal("Checkpoint stream returned no chunks".into())
        })?;
        let lsn = asm.checkpoint_lsn();
        let data = asm.assemble_checkpoint()?;
        Ok((data, lsn))
    }

    /// Open a persistent WAL subscription stream.
    /// Returns a receiver that yields `WalChunk`s as the primary produces them.
    /// The stream stays open until the primary disconnects or an error occurs.
    pub async fn subscribe_wal_stream(
        &self,
        shard_id: ShardId,
        from_lsn: u64,
        max_records: usize,
    ) -> Result<tokio::sync::mpsc::Receiver<Result<WalChunk, FalconError>>, FalconError> {
        use crate::proto::wal_replication_client::WalReplicationClient;
        use tokio_stream::StreamExt;

        let channel = self.get_channel().await?;
        let mut client = WalReplicationClient::new(channel);

        let request = crate::proto::SubscribeWalRequest {
            shard_id: shard_id.0,
            from_lsn,
            max_records_per_chunk: max_records as u32,
        };

        let response = match client.subscribe_wal(request).await {
            Ok(r) => r,
            Err(e) => {
                self.invalidate_channel().await;
                return Err(FalconError::Internal(format!("gRPC subscribe_wal error: {}", e)));
            }
        };

        let mut stream = response.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(8);

        tokio::spawn(async move {
            loop {
                match stream.next().await {
                    Some(Ok(msg)) => {
                        match proto_to_wal_chunk(&msg) {
                            Ok(chunk) => {
                                if tx.send(Ok(chunk)).await.is_err() {
                                    break; // consumer dropped
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                break;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        let _ = tx.send(Err(FalconError::Internal(
                            format!("gRPC stream error: {}", e),
                        ))).await;
                        break;
                    }
                    None => break, // stream ended
                }
            }
        });

        Ok(rx)
    }
}

#[async_trait::async_trait]
impl AsyncReplicationTransport for GrpcTransport {
    async fn pull_wal_chunk(
        &self,
        shard_id: ShardId,
        from_lsn: u64,
        max_records: usize,
    ) -> Result<WalChunk, FalconError> {
        use crate::proto::wal_replication_client::WalReplicationClient;
        use tokio_stream::StreamExt;

        let channel = self.get_channel().await?;
        let mut client = WalReplicationClient::new(channel);

        let request = crate::proto::SubscribeWalRequest {
            shard_id: shard_id.0,
            from_lsn,
            max_records_per_chunk: max_records as u32,
        };

        let response = match client.subscribe_wal(request).await {
            Ok(r) => r,
            Err(e) => {
                self.invalidate_channel().await;
                return Err(FalconError::Internal(format!("gRPC subscribe_wal error: {}", e)));
            }
        };

        let mut stream = response.into_inner();
        match stream.next().await {
            Some(Ok(msg)) => proto_to_wal_chunk(&msg),
            Some(Err(e)) => {
                self.invalidate_channel().await;
                Err(FalconError::Internal(format!("gRPC stream error: {}", e)))
            }
            None => Ok(WalChunk::empty(shard_id)),
        }
    }

    async fn ack_wal(
        &self,
        shard_id: ShardId,
        replica_id: usize,
        applied_lsn: u64,
    ) -> Result<(), FalconError> {
        use crate::proto::wal_replication_client::WalReplicationClient;

        let channel = self.get_channel().await?;
        let mut client = WalReplicationClient::new(channel);

        let request = crate::proto::AckWalRequest {
            shard_id: shard_id.0,
            replica_id: replica_id as u64,
            applied_lsn,
        };

        if let Err(e) = client.ack_wal(request).await {
            self.invalidate_channel().await;
            return Err(FalconError::Internal(format!("gRPC ack_wal error: {}", e)));
        }

        self.ack_lsns.lock().insert(replica_id, applied_lsn);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WalReplicationService — server-side handler
// ---------------------------------------------------------------------------

/// Server-side WAL replication service.
///
/// Uses `DashMap` for all shared state so the struct is `Send + Sync` and can
/// be wrapped in `Arc<Self>` as required by tonic's generated server trait.
pub struct WalReplicationService {
    /// Replication logs indexed by shard ID.
    logs: DashMap<ShardId, Arc<ReplicationLog>>,
    /// Ack LSNs per (shard, replica) for lag tracking.
    ack_lsns: DashMap<(ShardId, usize), u64>,
    /// Storage engine reference for checkpoint serving.
    storage: parking_lot::RwLock<Option<Arc<falcon_storage::engine::StorageEngine>>>,
}

impl WalReplicationService {
    pub fn new() -> Self {
        Self {
            logs: DashMap::new(),
            ack_lsns: DashMap::new(),
            storage: parking_lot::RwLock::new(None),
        }
    }

    /// Register a replication log for a shard.
    pub fn register_shard(&self, shard_id: ShardId, log: Arc<ReplicationLog>) {
        self.logs.insert(shard_id, log);
    }

    /// Set the storage engine for checkpoint serving.
    pub fn set_storage(&self, storage: Arc<falcon_storage::engine::StorageEngine>) {
        *self.storage.write() = Some(storage);
    }

    /// Handle a SubscribeWal request: return a WalChunk from the given LSN.
    pub fn handle_pull(
        &self,
        shard_id: ShardId,
        from_lsn: u64,
        max_records: usize,
    ) -> Result<WalChunk, FalconError> {
        let log = self.logs.get(&shard_id).ok_or_else(|| {
            FalconError::Internal(format!("No replication log for shard {:?}", shard_id))
        })?;
        let records: Vec<LsnWalRecord> = log
            .read_from(from_lsn)
            .into_iter()
            .take(max_records)
            .collect();
        Ok(WalChunk::from_records(shard_id, records))
    }

    /// Handle an AckWal request: record the replica's applied LSN.
    ///
    /// Updates both the local `ack_lsns` map (for lag tracking / log trimming)
    /// and the `StorageEngine.replica_ack_tracker` (for GC safepoint computation).
    /// Without the latter, GC safepoint never advances on the primary when using
    /// gRPC replication.
    pub fn handle_ack(
        &self,
        shard_id: ShardId,
        replica_id: usize,
        applied_lsn: u64,
    ) {
        self.ack_lsns.insert((shard_id, replica_id), applied_lsn);

        // Forward the ack to the storage engine's replica tracker so that the
        // GC safepoint can advance.  The tracker uses Timestamp(lsn) as the
        // safe timestamp — this is intentional: LSN and logical timestamp are
        // monotonically correlated in FalconDB's single-node WAL model.
        if let Some(ref storage) = *self.storage.read() {
            storage
                .replica_ack_tracker()
                .update_ack(replica_id, applied_lsn);
        }
    }

    /// Get the acked LSN for a specific (shard, replica).
    pub fn get_ack_lsn(&self, shard_id: ShardId, replica_id: usize) -> u64 {
        self.ack_lsns.get(&(shard_id, replica_id)).map(|v| *v).unwrap_or(0)
    }

    /// Get replication lag for all replicas of a shard.
    pub fn replication_lag(&self, shard_id: ShardId) -> Vec<(usize, u64)> {
        let log = match self.logs.get(&shard_id) {
            Some(l) => l.clone(),
            None => return vec![],
        };
        let primary_lsn = log.current_lsn();
        self.ack_lsns
            .iter()
            .filter(|entry| entry.key().0 == shard_id)
            .map(|entry| (entry.key().1, primary_lsn.saturating_sub(*entry.value())))
            .collect()
    }
}

impl Default for WalReplicationService {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Domain ↔ Proto conversions
// ---------------------------------------------------------------------------

/// Convert a domain `WalChunk` to a proto `WalChunkMessage`.
pub fn wal_chunk_to_proto(chunk: &WalChunk) -> Result<crate::proto::WalChunkMessage, FalconError> {
    let records = chunk
        .records
        .iter()
        .map(|r| {
            let payload = serde_json::to_vec(r)
                .map_err(|e| FalconError::Internal(format!("Record serialization: {}", e)))?;
            Ok(crate::proto::WalRecordEntry {
                lsn: r.lsn,
                record_payload: payload,
            })
        })
        .collect::<Result<Vec<_>, FalconError>>()?;

    Ok(crate::proto::WalChunkMessage {
        shard_id: chunk.shard_id.0,
        start_lsn: chunk.start_lsn,
        end_lsn: chunk.end_lsn,
        records,
        checksum: chunk.checksum,
    })
}

/// Convert a proto `WalChunkMessage` to a domain `WalChunk`.
pub fn proto_to_wal_chunk(msg: &crate::proto::WalChunkMessage) -> Result<WalChunk, FalconError> {
    let records = msg
        .records
        .iter()
        .map(|entry| {
            serde_json::from_slice::<LsnWalRecord>(&entry.record_payload)
                .map_err(|e| FalconError::Internal(format!("Record deserialization: {}", e)))
        })
        .collect::<Result<Vec<_>, FalconError>>()?;

    let chunk = WalChunk::from_records(ShardId(msg.shard_id), records);
    Ok(chunk)
}

// ---------------------------------------------------------------------------
// Tonic WalReplication server trait implementation
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl crate::proto::wal_replication_server::WalReplication for WalReplicationService {
    type SubscribeWalStream = tokio_stream::wrappers::ReceiverStream<
        Result<crate::proto::WalChunkMessage, tonic::Status>,
    >;

    async fn subscribe_wal(
        &self,
        request: tonic::Request<crate::proto::SubscribeWalRequest>,
    ) -> Result<tonic::Response<Self::SubscribeWalStream>, tonic::Status> {
        let req = request.into_inner();
        let shard_id = ShardId(req.shard_id);
        let from_lsn = req.from_lsn;
        let max_records = req.max_records_per_chunk as usize;

        let log = self.logs.get(&shard_id).map(|r| r.clone()).ok_or_else(|| {
            tonic::Status::not_found(format!("No replication log for shard {:?}", shard_id))
        })?;

        let (tx, rx) = tokio::sync::mpsc::channel(8);

        // Clone the ack_lsns map reference for use in the trim task.
        let ack_lsns_ref = self.ack_lsns.clone();

        tokio::spawn(async move {
            let mut cursor = from_lsn;
            let mut chunks_sent: u64 = 0;
            // Trim the in-memory log every N chunks to prevent unbounded growth.
            const TRIM_INTERVAL: u64 = 100;

            loop {
                // Read any new records since cursor
                let records: Vec<LsnWalRecord> = log
                    .read_from(cursor)
                    .into_iter()
                    .take(max_records)
                    .collect();

                if !records.is_empty() {
                    let chunk = WalChunk::from_records(shard_id, records);
                    cursor = chunk.end_lsn;

                    match wal_chunk_to_proto(&chunk) {
                        Ok(proto_msg) => {
                            if tx.send(Ok(proto_msg)).await.is_err() {
                                break; // client disconnected
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(tonic::Status::internal(e.to_string()))).await;
                            break;
                        }
                    }

                    chunks_sent += 1;

                    // Periodically trim the log: discard records that every
                    // known replica has already acked past.
                    if chunks_sent.is_multiple_of(TRIM_INTERVAL) {
                        let min_acked = ack_lsns_ref
                            .iter()
                            .filter(|e| e.key().0 == shard_id)
                            .map(|e| *e.value())
                            .min()
                            .unwrap_or(0);
                        if min_acked > 0 {
                            let trimmed = log.trim_before(min_acked);
                            if trimmed > 0 {
                                tracing::debug!(
                                    shard_id = shard_id.0,
                                    trimmed,
                                    min_acked,
                                    "ReplicationLog trimmed",
                                );
                            }
                        }
                    }
                } else {
                    // No new records — wait for notification or timeout.
                    // Timeout ensures the stream stays alive (heartbeat-like).
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        log.notified(),
                    ).await;
                    // Check if client is still connected
                    if tx.is_closed() {
                        break;
                    }
                }
            }
        });

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
    }

    async fn ack_wal(
        &self,
        request: tonic::Request<crate::proto::AckWalRequest>,
    ) -> Result<tonic::Response<crate::proto::AckWalResponse>, tonic::Status> {
        let req = request.into_inner();
        self.handle_ack(
            ShardId(req.shard_id),
            req.replica_id as usize,
            req.applied_lsn,
        );
        Ok(tonic::Response::new(crate::proto::AckWalResponse {
            success: true,
        }))
    }

    type GetCheckpointStream = tokio_stream::wrappers::ReceiverStream<
        Result<crate::proto::CheckpointChunk, tonic::Status>,
    >;

    async fn get_checkpoint(
        &self,
        _request: tonic::Request<crate::proto::GetCheckpointRequest>,
    ) -> Result<tonic::Response<Self::GetCheckpointStream>, tonic::Status> {
        let storage_opt = self.storage.read().clone();
        let storage = storage_opt.ok_or_else(|| {
            tonic::Status::unavailable("No StorageEngine configured for checkpoint serving")
        })?;

        let ckpt_data = storage.snapshot_checkpoint_data();
        let streamer = CheckpointStreamer::from_checkpoint_data(&ckpt_data)
            .map_err(|e| tonic::Status::internal(format!("Checkpoint serialization: {}", e)))?;

        let chunks: Vec<CheckpointChunk> = streamer.chunks().to_vec();
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            for chunk in chunks {
                let proto_chunk = crate::proto::CheckpointChunk {
                    chunk_index: chunk.chunk_index,
                    total_chunks: chunk.total_chunks,
                    data: chunk.data,
                    checkpoint_lsn: chunk.checkpoint_lsn,
                };
                if tx.send(Ok(proto_chunk)).await.is_err() {
                    break; // client disconnected
                }
            }
        });

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
    }
}

// ---------------------------------------------------------------------------
// CheckpointStreamer — chunked checkpoint transfer for replica bootstrap
// ---------------------------------------------------------------------------

/// Default chunk size for checkpoint streaming (256 KB).
const DEFAULT_CHECKPOINT_CHUNK_SIZE: usize = 256 * 1024;

/// A single chunk of checkpoint data for streaming over gRPC.
#[derive(Debug, Clone)]
pub struct CheckpointChunk {
    pub chunk_index: u32,
    pub total_chunks: u32,
    pub data: Vec<u8>,
    pub checkpoint_lsn: u64,
}

/// Splits serialized checkpoint data into fixed-size chunks for streaming.
///
/// Usage (server-side):
/// ```ignore
/// let engine: &StorageEngine = ...;
/// let streamer = CheckpointStreamer::from_engine(engine)?;
/// for chunk in streamer.chunks() {
///     // Send chunk over gRPC stream
/// }
/// ```
pub struct CheckpointStreamer {
    chunks: Vec<CheckpointChunk>,
}

impl CheckpointStreamer {
    /// Create a streamer from raw checkpoint bytes.
    /// Empty data produces zero chunks (no stream messages sent).
    pub fn from_bytes(data: Vec<u8>, checkpoint_lsn: u64, chunk_size: usize) -> Self {
        if data.is_empty() {
            return Self { chunks: Vec::new() };
        }
        let total_bytes = data.len();
        let total_chunks = total_bytes.div_ceil(chunk_size) as u32;
        let chunks: Vec<CheckpointChunk> = data
            .chunks(chunk_size)
            .enumerate()
            .map(|(i, slice)| CheckpointChunk {
                chunk_index: i as u32,
                total_chunks,
                data: slice.to_vec(),
                checkpoint_lsn,
            })
            .collect();
        Self { chunks }
    }

    /// Create a streamer from a `CheckpointData` struct using bincode serialization.
    pub fn from_checkpoint_data(
        ckpt: &falcon_storage::wal::CheckpointData,
    ) -> Result<Self, FalconError> {
        let bytes = serde_json::to_vec(ckpt)
            .map_err(|e| FalconError::Internal(format!("Checkpoint serialization error: {}", e)))?;
        Ok(Self::from_bytes(bytes, ckpt.wal_lsn, DEFAULT_CHECKPOINT_CHUNK_SIZE))
    }

    /// Get all chunks for iteration / streaming.
    pub fn chunks(&self) -> &[CheckpointChunk] {
        &self.chunks
    }

    /// Number of chunks.
    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    /// Total bytes across all chunks.
    pub fn total_bytes(&self) -> usize {
        self.chunks.iter().map(|c| c.data.len()).sum()
    }
}

/// Reassemble checkpoint data from a stream of chunks.
pub struct CheckpointAssembler {
    received: Vec<Option<Vec<u8>>>,
    total_chunks: u32,
    checkpoint_lsn: u64,
}

impl CheckpointAssembler {
    /// Create an assembler expecting `total_chunks` chunks.
    pub fn new(total_chunks: u32, checkpoint_lsn: u64) -> Self {
        Self {
            received: vec![None; total_chunks as usize],
            total_chunks,
            checkpoint_lsn,
        }
    }

    /// Add a received chunk. Returns true if all chunks have been received.
    pub fn add_chunk(&mut self, chunk: &CheckpointChunk) -> Result<bool, FalconError> {
        let idx = chunk.chunk_index as usize;
        if idx >= self.total_chunks as usize {
            return Err(FalconError::Internal(format!(
                "Chunk index {} out of range (total {})", idx, self.total_chunks
            )));
        }
        self.received[idx] = Some(chunk.data.clone());
        Ok(self.is_complete())
    }

    /// Check if all chunks have been received.
    pub fn is_complete(&self) -> bool {
        self.received.iter().all(|c| c.is_some())
    }

    /// Reassemble the full checkpoint bytes. Fails if incomplete.
    pub fn assemble(&self) -> Result<Vec<u8>, FalconError> {
        if !self.is_complete() {
            let missing: Vec<usize> = self.received.iter().enumerate()
                .filter(|(_, c)| c.is_none())
                .map(|(i, _)| i)
                .collect();
            return Err(FalconError::Internal(format!(
                "Checkpoint incomplete: missing chunks {:?}", missing
            )));
        }
        let mut data = Vec::new();
        for (i, chunk) in self.received.iter().enumerate() {
            match chunk.as_ref() {
                Some(bytes) => data.extend_from_slice(bytes),
                None => {
                    return Err(FalconError::Internal(format!(
                        "Checkpoint reassembly: chunk {} is None (should have been caught by completeness check)", i
                    )));
                }
            }
        }
        Ok(data)
    }

    /// Reassemble and deserialize into `CheckpointData`.
    pub fn assemble_checkpoint(&self) -> Result<falcon_storage::wal::CheckpointData, FalconError> {
        let bytes = self.assemble()?;
        serde_json::from_slice(&bytes)
            .map_err(|e| FalconError::Internal(format!("Checkpoint deserialization error: {}", e)))
    }

    /// The LSN at which the checkpoint was taken.
    pub fn checkpoint_lsn(&self) -> u64 {
        self.checkpoint_lsn
    }
}
