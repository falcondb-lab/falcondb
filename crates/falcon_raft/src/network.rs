//! Network layer for openraft.
//!
//! Provides three implementations:
//!
//! 1. **`RouterNetworkFactory`** — in-process multi-node network for testing
//!    and single-binary clusters. RPCs are dispatched directly to the target
//!    node's `Raft` handle via a shared `Arc<RaftRouter>`. Zero latency,
//!    deterministic. Supports `full_snapshot` via in-process delivery.
//!
//! 2. **`GrpcNetworkFactory`** — production multi-node network using gRPC.
//!    Sends bincode-serialized openraft requests over the wire via
//!    `GrpcTransport`. Supports snapshot streaming, circuit breakers, and
//!    retry/backoff.
//!
//! 3. **`NetworkFactory`** (single-node stub) — backward compatibility.
//!    All RPCs fail with `Unreachable`. Correct for single-node mode.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::sync::Arc;

use openraft::error::{RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, SnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::{BasicNode, Raft, Vote};
use parking_lot::RwLock;

use crate::transport::grpc::{GrpcTransport, RpcType};
use crate::transport::{RaftTransportConfig, TransportError};
use crate::types::TypeConfig;

// ---------------------------------------------------------------------------
// RaftRouter — shared registry of all in-process Raft nodes
// ---------------------------------------------------------------------------

/// Shared registry mapping node_id → Raft handle.
/// Used by `RouterNetworkFactory` to dispatch RPCs directly to target nodes.
#[derive(Default)]
pub struct RaftRouter {
    nodes: RwLock<BTreeMap<u64, Raft<TypeConfig>>>,
}

impl RaftRouter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register a Raft node so it can receive in-process RPCs.
    pub fn add_node(&self, id: u64, raft: Raft<TypeConfig>) {
        self.nodes.write().insert(id, raft);
    }

    /// Remove a node (e.g. on shutdown or simulated failure).
    pub fn remove_node(&self, id: u64) {
        self.nodes.write().remove(&id);
    }

    /// Get a clone of the Raft handle for a node (cheap — Raft is Arc-backed).
    pub fn get_node(&self, id: u64) -> Option<Raft<TypeConfig>> {
        self.nodes.read().get(&id).cloned()
    }

    /// Number of registered nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.read().len()
    }
}

// ---------------------------------------------------------------------------
// RouterNetworkFactory — in-process multi-node network
// ---------------------------------------------------------------------------

/// Network factory that creates in-process connections via `RaftRouter`.
/// Use this for multi-node clusters within a single process (tests, embedded).
pub struct RouterNetworkFactory {
    router: Arc<RaftRouter>,
}

impl RouterNetworkFactory {
    pub const fn new(router: Arc<RaftRouter>) -> Self {
        Self { router }
    }
}

impl RaftNetworkFactory<TypeConfig> for RouterNetworkFactory {
    type Network = RouterConnection;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        RouterConnection {
            target,
            router: self.router.clone(),
        }
    }
}

/// In-process connection to a specific target node.
pub struct RouterConnection {
    target: u64,
    router: Arc<RaftRouter>,
}

impl RouterConnection {
    #[allow(clippy::result_large_err)]
    fn get_target(&self) -> Result<Raft<TypeConfig>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.router.get_node(self.target).ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::NotConnected,
                format!("node {} not found in router", self.target),
            )))
        })
    }
}

impl RaftNetwork<TypeConfig> for RouterConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let raft = self.get_target()?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let raft = self.get_target()?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        let raft = self.router.get_node(self.target).ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::NotConnected,
                format!("node {} not found in router", self.target),
            )))
        })?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<TypeConfig>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, openraft::error::Fatal<u64>>>
    {
        // In-process snapshot delivery: read the snapshot data and call
        // install_snapshot on the target node directly.
        let raft = self.router.get_node(self.target).ok_or_else(|| {
            StreamingError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::NotConnected,
                format!("node {} not found in router", self.target),
            )))
        })?;

        // Read the full snapshot into memory (in-process, so no streaming needed)
        let mut data = Vec::new();
        let mut cursor = snapshot.snapshot;
        cursor
            .read_to_end(&mut data)
            .map_err(|e| {
                StreamingError::Unreachable(Unreachable::new(&io::Error::other(
                    format!("read snapshot: {e}"),
                )))
            })?;

        let install_req = InstallSnapshotRequest {
            vote,
            meta: snapshot.meta.clone(),
            offset: 0,
            data,
            done: true,
        };

        let resp = raft
            .install_snapshot(install_req)
            .await
            .map_err(|e| {
                StreamingError::Unreachable(Unreachable::new(&io::Error::other(
                    format!("install_snapshot: {e}"),
                )))
            })?;

        Ok(SnapshotResponse { vote: resp.vote })
    }
}

// ---------------------------------------------------------------------------
// GrpcNetworkFactory — production multi-node network via gRPC
// ---------------------------------------------------------------------------

/// Network factory that creates gRPC-based connections to remote raft peers.
/// Use this for production multi-node clusters.
pub struct GrpcNetworkFactory {
    transport: Arc<GrpcTransport>,
}

impl GrpcNetworkFactory {
    pub const fn new(transport: Arc<GrpcTransport>) -> Self {
        Self { transport }
    }

    /// Create with default config and peer addresses.
    pub fn with_peers(peers: Vec<(u64, String)>) -> Self {
        let transport = GrpcTransport::new(RaftTransportConfig::default());
        for (id, addr) in peers {
            transport.add_peer(id, addr);
        }
        Self {
            transport: Arc::new(transport),
        }
    }

    /// Get the underlying transport (for metrics, peer management, etc.).
    pub const fn transport(&self) -> &Arc<GrpcTransport> {
        &self.transport
    }
}

impl RaftNetworkFactory<TypeConfig> for GrpcNetworkFactory {
    type Network = GrpcConnection;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        GrpcConnection {
            target,
            transport: self.transport.clone(),
        }
    }
}

/// gRPC-backed connection to a specific remote raft node.
pub struct GrpcConnection {
    target: u64,
    transport: Arc<GrpcTransport>,
}

impl GrpcConnection {
    fn transport_err_to_rpc_err<E: std::error::Error>(
        &self,
        err: TransportError,
    ) -> RPCError<u64, BasicNode, RaftError<u64, E>> {
        RPCError::Unreachable(Unreachable::new(&io::Error::other(
            format!("{err}"),
        )))
    }
}

impl RaftNetwork<TypeConfig> for GrpcConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let req_bytes = bincode::serialize(&rpc).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&io::Error::other(
                format!("serialize: {e}"),
            )))
        })?;

        let resp_bytes = self
            .transport
            .send_rpc(self.target, RpcType::AppendEntries, req_bytes)
            .await
            .map_err(|e| self.transport_err_to_rpc_err(e))?;

        bincode::deserialize(&resp_bytes).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&io::Error::other(
                format!("deserialize: {e}"),
            )))
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let req_bytes = bincode::serialize(&rpc).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&io::Error::other(
                format!("serialize: {e}"),
            )))
        })?;

        let resp_bytes = self
            .transport
            .send_rpc(self.target, RpcType::RequestVote, req_bytes)
            .await
            .map_err(|e| self.transport_err_to_rpc_err(e))?;

        bincode::deserialize(&resp_bytes).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&io::Error::other(
                format!("deserialize: {e}"),
            )))
        })
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        let req_bytes = bincode::serialize(&rpc).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&io::Error::other(
                format!("serialize: {e}"),
            )))
        })?;

        let resp_bytes = self
            .transport
            .send_rpc(self.target, RpcType::InstallSnapshot, req_bytes)
            .await
            .map_err(|e| self.transport_err_to_rpc_err(e))?;

        bincode::deserialize(&resp_bytes).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&io::Error::other(
                format!("deserialize: {e}"),
            )))
        })
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<TypeConfig>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, openraft::error::Fatal<u64>>>
    {
        // Read snapshot data
        let mut data = Vec::new();
        let mut cursor = snapshot.snapshot;
        cursor.read_to_end(&mut data).map_err(|e| {
            StreamingError::Unreachable(Unreachable::new(&io::Error::other(
                format!("read snapshot: {e}"),
            )))
        })?;

        // Stream snapshot via gRPC chunked transfer
        let meta = crate::transport::SnapshotMetadata {
            snapshot_id: snapshot.meta.snapshot_id.clone(),
            last_included_index: snapshot.meta.last_log_id.map_or(0, |id| id.index),
            last_included_term: snapshot.meta.last_log_id.map_or(0, |id| id.leader_id.term),
            membership_data: bincode::serialize(&snapshot.meta.last_membership)
                .unwrap_or_default(),
            total_bytes: data.len() as u64,
            checksum: 0,
        };

        self.transport
            .stream_snapshot(self.target, data, meta)
            .await
            .map_err(|e| {
                StreamingError::Unreachable(Unreachable::new(&io::Error::other(
                    format!("stream_snapshot: {e}"),
                )))
            })?;

        // Return a placeholder vote — the actual vote is determined by the receiver.
        Ok(SnapshotResponse { vote })
    }
}

// ---------------------------------------------------------------------------
// NetworkFactory — single-node stub (backward compat)
// ---------------------------------------------------------------------------

/// Single-node network factory — returns a stub that errors on all RPCs.
/// Correct for single-node mode where no inter-node replication occurs.
pub struct NetworkFactory;

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        NetworkConnection
    }
}

/// Stub network connection — all RPCs fail with Unreachable.
pub struct NetworkConnection;

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(RPCError::Unreachable(Unreachable::new(&io::Error::new(
            io::ErrorKind::NotConnected,
            "single-node mode",
        ))))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(RPCError::Unreachable(Unreachable::new(&io::Error::new(
            io::ErrorKind::NotConnected,
            "single-node mode",
        ))))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        Err(RPCError::Unreachable(Unreachable::new(&io::Error::new(
            io::ErrorKind::NotConnected,
            "single-node mode",
        ))))
    }

    async fn full_snapshot(
        &mut self,
        _vote: Vote<u64>,
        _snapshot: Snapshot<TypeConfig>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, openraft::error::Fatal<u64>>>
    {
        Err(StreamingError::Unreachable(Unreachable::new(
            &io::Error::new(io::ErrorKind::NotConnected, "single-node mode"),
        )))
    }
}
