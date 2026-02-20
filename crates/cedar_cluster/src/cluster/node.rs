//! Cluster node types: identity, status, and membership info.

use cedar_common::types::NodeId;
use serde::{Deserialize, Serialize};

/// Information about a cluster node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub host: String,
    pub pg_port: u16,
    pub rpc_port: u16,
    pub status: NodeStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Active,
    Draining,
    Down,
}
