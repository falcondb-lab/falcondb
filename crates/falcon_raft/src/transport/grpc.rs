//! gRPC-based Raft transport using tonic.
//!
//! Implements the `RaftTransport` service defined in `falcon_raft.proto`.
//! Client side: `GrpcTransport` sends RPCs to peers.
//! Server side: `RaftTransportServer` receives RPCs and dispatches to local raft node.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use falcon_proto::falcon_raft as pb;
use falcon_proto::falcon_raft::raft_transport_client::RaftTransportClient;

use super::{PeerCircuitBreaker, RaftTransportConfig, TransportError, TransportMetrics};

// ═══════════════════════════════════════════════════════════════════════════
// §1 — gRPC Transport Client
// ═══════════════════════════════════════════════════════════════════════════

/// Per-peer connection state.
struct PeerConnection {
    client: RaftTransportClient<Channel>,
    circuit_breaker: PeerCircuitBreaker,
}

/// gRPC-based transport for Raft inter-node communication.
///
/// Maintains a connection pool (one channel per peer) with circuit breakers.
/// Thread-safe: can be shared across multiple Raft replication streams.
pub struct GrpcTransport {
    config: RaftTransportConfig,
    /// Peer ID → address mapping.
    peers: RwLock<HashMap<u64, String>>,
    /// Peer ID → connection (lazily created).
    connections: Mutex<HashMap<u64, PeerConnection>>,
    /// Cumulative metrics.
    metrics: Arc<TransportMetrics>,
}

impl GrpcTransport {
    /// Create a new gRPC transport.
    pub fn new(config: RaftTransportConfig) -> Self {
        Self {
            config,
            peers: RwLock::new(HashMap::new()),
            connections: Mutex::new(HashMap::new()),
            metrics: Arc::new(TransportMetrics::new()),
        }
    }

    /// Register a peer's address.
    pub fn add_peer(&self, peer_id: u64, addr: String) {
        self.peers.write().insert(peer_id, addr);
    }

    /// Remove a peer.
    pub fn remove_peer(&self, peer_id: u64) {
        self.peers.write().remove(&peer_id);
    }

    /// Get metrics reference.
    pub const fn metrics(&self) -> &Arc<TransportMetrics> {
        &self.metrics
    }

    /// Stream a full snapshot to a peer in chunks.
    pub async fn stream_snapshot(
        &self,
        peer_id: u64,
        snapshot_data: Vec<u8>,
        meta: super::SnapshotMetadata,
    ) -> Result<(), TransportError> {
        let mut client = self.get_or_connect(peer_id).await?;
        let chunk_size = self.config.max_snapshot_chunk_bytes;
        let total_bytes = snapshot_data.len() as u64;

        // Check total size limit
        if self.config.max_snapshot_total_bytes > 0 && total_bytes > self.config.max_snapshot_total_bytes {
            return Err(TransportError::PayloadTooLarge {
                size: total_bytes as usize,
                max: self.config.max_snapshot_total_bytes as usize,
            });
        }

        let mut chunks = Vec::new();
        let mut offset = 0u64;
        let mut chunk_index = 0u32;

        while (offset as usize) < snapshot_data.len() {
            let end = ((offset as usize) + chunk_size).min(snapshot_data.len());
            let chunk_data = snapshot_data[offset as usize..end].to_vec();
            let done = end == snapshot_data.len();
            let checksum = crc32_fast(&chunk_data);

            let pb_meta = if chunk_index == 0 {
                Some(pb::SnapshotMeta {
                    snapshot_id: meta.snapshot_id.clone(),
                    last_log_id: Some(pb::LogIdProto {
                        leader_id: 0,
                        term: meta.last_included_term,
                        index: meta.last_included_index,
                    }),
                    last_membership: meta.membership_data.clone(),
                })
            } else {
                None
            };

            chunks.push(pb::SnapshotChunk {
                meta: pb_meta,
                offset,
                data: chunk_data,
                done,
                chunk_index,
                chunk_checksum: checksum,
            });

            offset = end as u64;
            chunk_index += 1;
        }

        let stream = tokio_stream::iter(chunks);
        let start = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_millis(
                self.config.snapshot_chunk_timeout_ms * u64::from(chunk_index.max(1)),
            ),
            client.stream_snapshot(tonic::Request::new(stream)),
        )
        .await;

        match result {
            Ok(Ok(resp)) => {
                let inner = resp.into_inner();
                self.metrics.record_snapshot_bytes_sent(total_bytes);
                if inner.success {
                    self.record_peer_success(peer_id).await;
                    Ok(())
                } else {
                    Err(TransportError::RemoteRejected(inner.error_message))
                }
            }
            Ok(Err(status)) => {
                self.record_peer_failure(peer_id).await;
                Err(grpc_status_to_transport_error(status))
            }
            Err(_) => {
                self.record_peer_failure(peer_id).await;
                Err(TransportError::Timeout(format!(
                    "snapshot stream to peer {} timed out after {}ms",
                    peer_id,
                    start.elapsed().as_millis()
                )))
            }
        }
    }

    /// Send a generic serialized RPC (bincode-encoded openraft request/response).
    ///
    /// All RPC types are multiplexed through the `install_snapshot` gRPC endpoint
    /// using a 1-byte discriminator prefix. The server side demuxes and dispatches
    /// to the appropriate openraft method.
    ///
    /// Wire format: `[rpc_type: u8] [bincode payload...]`
    pub async fn send_rpc(
        &self,
        peer_id: u64,
        rpc_type: RpcType,
        request_bytes: Vec<u8>,
    ) -> Result<Vec<u8>, TransportError> {
        self.check_payload_size(request_bytes.len())?;

        // Check circuit breaker
        {
            let mut conns = self.connections.lock().await;
            if let Some(conn) = conns.get_mut(&peer_id) {
                if !conn.circuit_breaker.allow_request() {
                    return Err(TransportError::Unreachable(format!(
                        "circuit breaker open for peer {peer_id}"
                    )));
                }
            }
        }

        let mut client = self.get_or_connect(peer_id).await?;
        let _start = Instant::now();

        // Pack: [discriminator byte][bincode payload]
        let mut payload = Vec::with_capacity(1 + request_bytes.len());
        payload.push(rpc_type as u8);
        payload.extend_from_slice(&request_bytes);

        let req = pb::InstallSnapshotRequest { data: payload };
        let r = tokio::time::timeout(
            self.config.request_timeout(),
            client.install_snapshot(tonic::Request::new(req)),
        )
        .await;

        let result = match r {
            Ok(Ok(resp)) => Ok(resp.into_inner().data),
            Ok(Err(status)) => Err(grpc_status_to_transport_error(status)),
            Err(_) => Err(TransportError::Timeout(format!(
                "{rpc_type:?} to peer {peer_id} timed out"
            ))),
        };

        match &result {
            Ok(_) => {
                self.metrics.record_success();
                self.record_peer_success(peer_id).await;
            }
            Err(e) => {
                self.metrics.record_failure(e);
                self.record_peer_failure(peer_id).await;
            }
        }

        result
    }

    // ── Internal helpers ──

    async fn get_or_connect(
        &self,
        peer_id: u64,
    ) -> Result<RaftTransportClient<Channel>, TransportError> {
        let mut conns = self.connections.lock().await;
        if let Some(conn) = conns.get(&peer_id) {
            return Ok(conn.client.clone());
        }

        let addr = self
            .peers
            .read()
            .get(&peer_id)
            .cloned()
            .ok_or_else(|| TransportError::Unreachable(format!("peer {peer_id} not registered")))?;

        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| TransportError::Internal(format!("invalid addr {addr}: {e}")))?
            .connect_timeout(self.config.connect_timeout())
            .timeout(self.config.request_timeout());

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| TransportError::Unreachable(format!("connect to {addr}: {e}")))?;

        let client = RaftTransportClient::new(channel);
        conns.insert(
            peer_id,
            PeerConnection {
                client: client.clone(),
                circuit_breaker: PeerCircuitBreaker::new(self.config.circuit_breaker.clone()),
            },
        );
        Ok(client)
    }

    const fn check_payload_size(&self, size: usize) -> Result<(), TransportError> {
        if size > self.config.max_rpc_payload_bytes {
            return Err(TransportError::PayloadTooLarge {
                size,
                max: self.config.max_rpc_payload_bytes,
            });
        }
        Ok(())
    }

    async fn record_peer_success(&self, peer_id: u64) {
        let mut conns = self.connections.lock().await;
        if let Some(conn) = conns.get_mut(&peer_id) {
            conn.circuit_breaker.record_success();
        }
    }

    async fn record_peer_failure(&self, peer_id: u64) {
        let mut conns = self.connections.lock().await;
        if let Some(conn) = conns.get_mut(&peer_id) {
            conn.circuit_breaker.record_failure();
        }
    }
}

/// RPC type discriminator for the generic send_rpc path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RpcType {
    AppendEntries = 1,
    RequestVote = 2,
    InstallSnapshot = 3,
}

impl RpcType {
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::AppendEntries),
            2 => Some(Self::RequestVote),
            3 => Some(Self::InstallSnapshot),
            _ => None,
        }
    }
}

/// Convert a tonic Status to a TransportError.
fn grpc_status_to_transport_error(status: tonic::Status) -> TransportError {
    match status.code() {
        tonic::Code::Unavailable | tonic::Code::Aborted => {
            TransportError::Unreachable(status.message().to_owned())
        }
        tonic::Code::DeadlineExceeded => TransportError::Timeout(status.message().to_owned()),
        tonic::Code::ResourceExhausted => TransportError::PayloadTooLarge {
            size: 0,
            max: 0,
        },
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
            TransportError::RemoteRejected(status.message().to_owned())
        }
        tonic::Code::Unimplemented => TransportError::Unsupported(status.message().to_owned()),
        _ => TransportError::Internal(format!("{}: {}", status.code(), status.message())),
    }
}

/// Public wrapper for crc32 used by server.rs.
pub fn crc32_fast_pub(data: &[u8]) -> u32 {
    crc32_fast(data)
}

/// Simple CRC32 (non-cryptographic, for integrity checks).
pub(crate) fn crc32_fast(data: &[u8]) -> u32 {
    let mut hash: u32 = 0xFFFFFFFF;
    for &byte in data {
        hash ^= u32::from(byte);
        for _ in 0..8 {
            if hash & 1 != 0 {
                hash = (hash >> 1) ^ 0xEDB88320;
            } else {
                hash >>= 1;
            }
        }
    }
    !hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_type_roundtrip() {
        assert_eq!(RpcType::from_u8(1), Some(RpcType::AppendEntries));
        assert_eq!(RpcType::from_u8(2), Some(RpcType::RequestVote));
        assert_eq!(RpcType::from_u8(3), Some(RpcType::InstallSnapshot));
        assert_eq!(RpcType::from_u8(0), None);
        assert_eq!(RpcType::from_u8(255), None);
    }

    #[test]
    fn test_grpc_status_mapping() {
        let e = grpc_status_to_transport_error(tonic::Status::unavailable("down"));
        assert!(matches!(e, TransportError::Unreachable(_)));

        let e = grpc_status_to_transport_error(tonic::Status::deadline_exceeded("slow"));
        assert!(matches!(e, TransportError::Timeout(_)));

        let e = grpc_status_to_transport_error(tonic::Status::unimplemented("nope"));
        assert!(matches!(e, TransportError::Unsupported(_)));

        let e =
            grpc_status_to_transport_error(tonic::Status::permission_denied("auth"));
        assert!(matches!(e, TransportError::RemoteRejected(_)));
    }

    #[test]
    fn test_crc32() {
        let c1 = crc32_fast(b"hello");
        let c2 = crc32_fast(b"hello");
        assert_eq!(c1, c2);
        let c3 = crc32_fast(b"world");
        assert_ne!(c1, c3);
        // Empty data should still produce a valid checksum
        let c0 = crc32_fast(b"");
        assert_eq!(c0, 0); // CRC32 of empty = 0x00000000
    }

    #[test]
    fn test_grpc_transport_add_remove_peer() {
        let transport = GrpcTransport::new(RaftTransportConfig::default());
        transport.add_peer(1, "127.0.0.1:9100".into());
        transport.add_peer(2, "127.0.0.1:9200".into());
        assert_eq!(transport.peers.read().len(), 2);
        transport.remove_peer(1);
        assert_eq!(transport.peers.read().len(), 1);
    }
}
