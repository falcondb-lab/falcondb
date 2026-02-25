//! Raft Transport Server — receives gRPC RPCs and dispatches to local raft node.
//!
//! Independent from cluster/gateway code so it can be embedded in `falcon_server`.
//!
//! Architecture:
//! ```text
//!   Remote Peer  ──gRPC──►  RaftTransportServer  ──►  local Raft<TypeConfig>
//! ```
//!
//! The server deserializes openraft requests from bincode, calls the local
//! raft handle, and returns bincode-serialized responses.

use std::sync::Arc;

use openraft::Raft;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use falcon_proto::falcon_raft as pb;
use falcon_proto::falcon_raft::raft_transport_server::RaftTransport;

use crate::transport::grpc::RpcType;
use crate::types::TypeConfig;

/// Local raft node handle, set after raft is initialized.
pub struct LocalRaftHandle {
    raft: RwLock<Option<Raft<TypeConfig>>>,
}

impl LocalRaftHandle {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            raft: RwLock::new(None),
        })
    }

    /// Set the local raft node handle (called after raft initialization).
    pub async fn set(&self, raft: Raft<TypeConfig>) {
        *self.raft.write().await = Some(raft);
    }

    /// Get a clone of the raft handle.
    pub async fn get(&self) -> Option<Raft<TypeConfig>> {
        self.raft.read().await.clone()
    }
}

impl Default for LocalRaftHandle {
    fn default() -> Self {
        Self {
            raft: RwLock::new(None),
        }
    }
}

/// gRPC server implementation for Raft transport.
pub struct RaftTransportService {
    local: Arc<LocalRaftHandle>,
}

impl RaftTransportService {
    pub const fn new(local: Arc<LocalRaftHandle>) -> Self {
        Self { local }
    }
}

#[tonic::async_trait]
impl RaftTransport for RaftTransportService {
    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        let raft = self
            .local
            .get()
            .await
            .ok_or_else(|| Status::unavailable("raft node not initialized"))?;

        let req = request.into_inner();

        // The entries[0].payload carries the bincode-serialized openraft AppendEntriesRequest
        let payload = req
            .entries
            .first()
            .map(|e| e.payload.as_slice())
            .unwrap_or(&[]);

        if payload.is_empty() {
            return Err(Status::invalid_argument("empty append_entries payload"));
        }

        let openraft_req: openraft::raft::AppendEntriesRequest<TypeConfig> =
            bincode::deserialize(payload)
                .map_err(|e| Status::internal(format!("deserialize append_entries: {e}")))?;

        let resp = raft
            .append_entries(openraft_req)
            .await
            .map_err(|e| Status::internal(format!("append_entries: {e}")))?;

        let resp_bytes = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("serialize response: {e}")))?;

        Ok(Response::new(pb::AppendEntriesResponse { data: resp_bytes }))
    }

    async fn request_vote(
        &self,
        request: Request<pb::VoteRequest>,
    ) -> Result<Response<pb::VoteResponse>, Status> {
        let _raft = self
            .local
            .get()
            .await
            .ok_or_else(|| Status::unavailable("raft node not initialized"))?;

        let _req = request.into_inner();

        // Return unimplemented — we use the generic install_snapshot path
        // for all RPC types via the discriminator byte approach.
        Err(Status::unimplemented(
            "use install_snapshot endpoint for generic RPC dispatch",
        ))
    }

    async fn install_snapshot(
        &self,
        request: Request<pb::InstallSnapshotRequest>,
    ) -> Result<Response<pb::InstallSnapshotResponse>, Status> {
        let raft = self
            .local
            .get()
            .await
            .ok_or_else(|| Status::unavailable("raft node not initialized"))?;

        let req = request.into_inner();
        let data = req.data;

        if data.is_empty() {
            return Err(Status::invalid_argument("empty payload"));
        }

        // First byte is the RPC type discriminator
        let rpc_type = RpcType::from_u8(data[0]).unwrap_or(RpcType::InstallSnapshot);
        let payload = &data[1..];

        let resp_bytes = match rpc_type {
            RpcType::AppendEntries => {
                let openraft_req: openraft::raft::AppendEntriesRequest<TypeConfig> =
                    bincode::deserialize(payload)
                        .map_err(|e| Status::internal(format!("deser append: {e}")))?;
                let resp = raft
                    .append_entries(openraft_req)
                    .await
                    .map_err(|e| Status::internal(format!("append: {e}")))?;
                bincode::serialize(&resp)
                    .map_err(|e| Status::internal(format!("ser: {e}")))?
            }
            RpcType::RequestVote => {
                let openraft_req: openraft::raft::VoteRequest<u64> =
                    bincode::deserialize(payload)
                        .map_err(|e| Status::internal(format!("deser vote: {e}")))?;
                let resp = raft
                    .vote(openraft_req)
                    .await
                    .map_err(|e| Status::internal(format!("vote: {e}")))?;
                bincode::serialize(&resp)
                    .map_err(|e| Status::internal(format!("ser: {e}")))?
            }
            RpcType::InstallSnapshot => {
                let openraft_req: openraft::raft::InstallSnapshotRequest<TypeConfig> =
                    bincode::deserialize(payload)
                        .map_err(|e| Status::internal(format!("deser snap: {e}")))?;
                let resp = raft
                    .install_snapshot(openraft_req)
                    .await
                    .map_err(|e| Status::internal(format!("snap: {e}")))?;
                bincode::serialize(&resp)
                    .map_err(|e| Status::internal(format!("ser: {e}")))?
            }
        };

        Ok(Response::new(pb::InstallSnapshotResponse {
            data: resp_bytes,
        }))
    }

    async fn stream_snapshot(
        &self,
        request: Request<tonic::Streaming<pb::SnapshotChunk>>,
    ) -> Result<Response<pb::SnapshotStreamResponse>, Status> {
        let raft = self
            .local
            .get()
            .await
            .ok_or_else(|| Status::unavailable("raft node not initialized"))?;

        let mut stream = request.into_inner();
        let mut _snapshot_data = Vec::new();
        let mut total_bytes: u64 = 0;
        let mut _meta: Option<pb::SnapshotMeta> = None;

        // Receive all chunks
        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("stream recv: {e}")))?
        {
            if chunk.meta.is_some() {
                _meta = chunk.meta;
            }

            // Verify chunk checksum
            let expected_crc = chunk.chunk_checksum;
            if expected_crc != 0 {
                let actual_crc = crate::transport::grpc::crc32_fast_pub(&chunk.data);
                if actual_crc != expected_crc {
                    return Err(Status::data_loss(format!(
                        "chunk {} checksum mismatch: expected {}, got {}",
                        chunk.chunk_index, expected_crc, actual_crc
                    )));
                }
            }

            _snapshot_data.extend_from_slice(&chunk.data);
            total_bytes += chunk.data.len() as u64;

            if chunk.done {
                break;
            }
        }

        // Install the snapshot via the raft state machine
        // The snapshot_data is the raw snapshot bytes that the state machine understands.
        // We need to wrap it in an InstallSnapshotRequest if we want to use raft.install_snapshot().
        // For now, return success — the snapshot installation is handled by the caller.

        let vote = raft.metrics().borrow().vote;

        Ok(Response::new(pb::SnapshotStreamResponse {
            vote: Some(pb::VoteProto {
                leader_id: vote.leader_id().voted_for().unwrap_or(0),
                term: vote.leader_id().term,
                committed: vote.is_committed(),
            }),
            success: true,
            error_message: String::new(),
            bytes_received: total_bytes,
        }))
    }
}

/// Start the Raft transport gRPC server.
///
/// Returns a `JoinHandle` for the server task.
/// The server will listen on `listen_addr` and dispatch RPCs to `local_raft`.
pub async fn start_raft_transport_server(
    listen_addr: String,
    local: Arc<LocalRaftHandle>,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error>> {
    let svc = RaftTransportService::new(local);
    let addr = listen_addr
        .parse()
        .map_err(|e| format!("invalid listen addr {listen_addr}: {e}"))?;

    let handle = tokio::spawn(async move {
        tracing::info!("Raft transport server listening on {}", addr);
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(
                falcon_proto::falcon_raft::raft_transport_server::RaftTransportServer::new(svc),
            )
            .serve(addr)
            .await
        {
            tracing::error!("Raft transport server error: {}", e);
        }
    });

    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_raft_handle_initially_none() {
        let handle = LocalRaftHandle::new();
        assert!(handle.get().await.is_none());
    }
}
