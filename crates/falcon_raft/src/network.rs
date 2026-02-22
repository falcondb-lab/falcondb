//! Network layer for openraft.
//!
//! Provides two implementations:
//!
//! 1. **`InProcessRouter`** / **`RouterNetworkFactory`** — in-process multi-node
//!    network for testing and single-binary clusters. RPCs are dispatched directly
//!    to the target node's `Raft` handle via a shared `Arc<RaftRouter>`.
//!    No gRPC, no sockets — zero latency, deterministic.
//!
//! 2. **`NetworkFactory`** (single-node stub) — kept for backward compatibility.
//!    All RPCs fail with `Unreachable`. Correct for single-node mode.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use openraft::error::{RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::{BasicNode, Raft, Vote};
use parking_lot::RwLock;

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
    pub fn new(router: Arc<RaftRouter>) -> Self {
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
        _vote: Vote<u64>,
        _snapshot: Snapshot<TypeConfig>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, openraft::error::Fatal<u64>>>
    {
        Err(StreamingError::Unreachable(Unreachable::new(
            &io::Error::new(
                io::ErrorKind::Unsupported,
                "full_snapshot not supported in in-process mode; use install_snapshot",
            ),
        )))
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
