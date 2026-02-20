//! Openraft type configuration for FalconDB.

use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

/// Application request data — proposed to the Raft log.
///
/// Variants:
/// - `Write`: a serialized WAL record (raw bytes). Applied to the state machine
///   by calling the registered `ApplyCallback`.
/// - `Noop`: a no-op heartbeat entry (used for leader confirmation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FalconRequest {
    /// A write operation (serialized WAL record or opaque payload).
    Write { data: Vec<u8> },
    /// No-op entry — used to confirm leadership without side effects.
    Noop,
}

/// Application response data — returned after applying a log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FalconResponse {
    Ok,
    /// Returned for Noop entries.
    Noop,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D            = FalconRequest,
        R            = FalconResponse,
        NodeId       = u64,
        Node         = BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Callback invoked by the state machine when a `Write` entry is applied.
/// The `data` bytes are the serialized WAL record payload.
/// Returns `Ok(())` on success or an error string on failure.
pub type ApplyCallback = Box<dyn Fn(&[u8]) -> Result<(), String> + Send + Sync>;
