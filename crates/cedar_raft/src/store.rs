//! In-memory Raft log storage and state machine for CedarDB.
//!
//! Implements openraft 0.9 `RaftLogStorage`, `RaftLogReader`,
//! `RaftStateMachine`, and `RaftSnapshotBuilder` traits.
//!
//! The `StateMachine` optionally holds an `ApplyCallback` that is called
//! for every `CedarRequest::Write` entry applied. This allows the Raft
//! state machine to forward committed WAL records to a `StorageEngine`.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftLogId, RaftSnapshotBuilder,
    SnapshotMeta, StorageError, StoredMembership, Vote,
};
use tokio::sync::Mutex;

use crate::types::{CedarResponse, TypeConfig};

/// Thread-safe apply callback: called for each committed `Write` entry.
pub type ApplyFn = Arc<dyn Fn(&[u8]) -> Result<(), String> + Send + Sync>;

// ---------------------------------------------------------------------------
// Log Store
// ---------------------------------------------------------------------------

/// Shared inner state for the log store.
struct LogStoreInner {
    vote: Option<Vote<u64>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    committed: Option<LogId<u64>>,
    purged: Option<LogId<u64>>,
}

impl LogStoreInner {
    fn new() -> Self {
        Self {
            vote: None,
            log: BTreeMap::new(),
            committed: None,
            purged: None,
        }
    }
}

/// In-memory log store — implements `RaftLogStorage` for openraft.
///
/// Uses `Arc<Mutex<...>>` internally so that the reader returned by
/// `get_log_reader` always sees the latest appended entries.
#[derive(Clone)]
pub struct LogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

impl Default for LogStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LogStore {
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(LogStoreInner::new())) }
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        let last = inner.log.iter().next_back().map(|(_, e)| *e.get_log_id());
        Ok(LogState {
            last_purged_log_id: inner.purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // Clone shares the Arc — reader sees all future appends.
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.inner.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.inner.lock().await;
        for entry in entries {
            let idx = entry.get_log_id().index;
            inner.log.insert(idx, entry);
        }
        // In-memory: data is immediately "persisted"
        drop(inner);
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        inner.purged = Some(log_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// State Machine
// ---------------------------------------------------------------------------

/// In-memory state machine — implements `RaftStateMachine` for openraft.
///
/// When `apply_fn` is set, every committed `CedarRequest::Write` entry
/// is forwarded to the callback (e.g. to apply WAL records to StorageEngine).
pub struct StateMachine {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    /// Raw payloads of all applied Write entries (for snapshot serialization).
    data: Vec<Vec<u8>>,
    snapshot_idx: u64,
    current_snapshot: Option<StoredSnapshot>,
    /// Optional callback invoked for each committed Write entry.
    apply_fn: Option<ApplyFn>,
}

#[derive(Debug, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<u64, BasicNode>,
    pub data: Vec<u8>,
}

impl Default for StateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachine {
    /// Create a state machine with no apply callback (data stored in-memory only).
    pub fn new() -> Self {
        Self {
            last_applied: None,
            last_membership: StoredMembership::new(None, openraft::Membership::new(vec![], None)),
            data: Vec::new(),
            snapshot_idx: 0,
            current_snapshot: None,
            apply_fn: None,
        }
    }

    /// Create a state machine that forwards committed Write entries to `apply_fn`.
    /// Use this to integrate Raft with a `StorageEngine` WAL replay path.
    pub fn with_apply_fn(apply_fn: ApplyFn) -> Self {
        Self {
            last_applied: None,
            last_membership: StoredMembership::new(None, openraft::Membership::new(vec![], None)),
            data: Vec::new(),
            snapshot_idx: 0,
            current_snapshot: None,
            apply_fn: Some(apply_fn),
        }
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<CedarResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        for entry in entries {
            self.last_applied = Some(*entry.get_log_id());

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(CedarResponse::Ok);
                }
                EntryPayload::Normal(ref req) => {
                    match req {
                        crate::types::CedarRequest::Write { ref data } => {
                            self.data.push(data.clone());
                            // Forward to StorageEngine if a callback is registered.
                            if let Some(ref cb) = self.apply_fn {
                                if let Err(e) = cb(data) {
                                    tracing::error!(
                                        log_index = entry.get_log_id().index,
                                        error = %e,
                                        "Raft apply callback failed"
                                    );
                                    // Return storage error to openraft so it can handle it.
                                    return Err(StorageError::IO {
                                        source: openraft::StorageIOError::write_state_machine(
                                            &std::io::Error::other(
                                                format!("apply callback: {}", e),
                                            ),
                                        ),
                                    });
                                }
                            }
                            responses.push(CedarResponse::Ok);
                        }
                        crate::types::CedarRequest::Noop => {
                            responses.push(CedarResponse::Noop);
                        }
                    }
                }
                EntryPayload::Membership(ref mem) => {
                    self.last_membership =
                        StoredMembership::new(Some(*entry.get_log_id()), mem.clone());
                    responses.push(CedarResponse::Ok);
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        StateMachine {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            data: self.data.clone(),
            snapshot_idx: self.snapshot_idx,
            current_snapshot: self.current_snapshot.clone(),
            apply_fn: self.apply_fn.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();

        // Deserialize state from snapshot data
        if let Ok(data) = serde_json::from_slice::<Vec<Vec<u8>>>(&new_snapshot.data) {
            self.data = data;
        }

        self.current_snapshot = Some(new_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        match &self.current_snapshot {
            Some(snap) => Ok(Some(Snapshot {
                meta: snap.meta.clone(),
                snapshot: Box::new(Cursor::new(snap.data.clone())),
            })),
            None => Ok(None),
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let data = serde_json::to_vec(&self.data).unwrap_or_default();

        self.snapshot_idx += 1;
        let snapshot_id = format!("snap-{}-{}", self.snapshot_idx,
            self.last_applied.map_or(0, |id| id.index));

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        self.current_snapshot = Some(snapshot);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}
