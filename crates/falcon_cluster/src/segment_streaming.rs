//! # Replication & Snapshot via Segment-Level Streaming
//!
//! **Replication moves segments, not records.**
//! WAL segments are immutable history blocks.
//!
//! ## Design invariants
//! - Replication unit = WAL segment (not individual records)
//! - Leader never replays WAL to serve followers
//! - Follower can catch up from any LSN via segment streaming
//! - All I/O is sequential (no random reads)
//! - WAL segments are immutable once sealed — replication is a read-only view
//! - Snapshot = segment cut + metadata (not a data export)
//!
//! ## Two replication paths
//! - **Segment Streaming** (fast path): follower lags ≥1 complete segment
//! - **Tail Streaming** (slow path): follower lags <1 segment (same segment as leader)

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};

use falcon_storage::structured_lsn::{
    SegmentHeader, StructuredLsn, SEGMENT_HEADER_SIZE,
};

/// Simple CRC32 checksum (djb2-style) to avoid external crate dependency.
/// For production, this would use hardware-accelerated CRC32C.
fn compute_crc32(data: &[u8]) -> u32 {
    let mut hash: u32 = 5381;
    for &b in data {
        hash = hash.wrapping_mul(33).wrapping_add(u32::from(b));
    }
    hash
}

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Node Identity & Configuration
// ═══════════════════════════════════════════════════════════════════════════

/// Unique node identifier in the replication cluster.
pub type NodeId = u64;

/// Configuration for segment-level replication.
#[derive(Debug, Clone)]
pub struct SegmentReplicationConfig {
    /// Chunk size for segment streaming (bytes). Default 4 MB.
    pub chunk_size: u64,
    /// Maximum in-flight chunks before backpressure kicks in.
    pub max_inflight_chunks: u32,
    /// Tail streaming batch size (bytes). Default 64 KB.
    pub tail_batch_size: u64,
    /// Segment size (must match WAL allocator). Default 256 MB.
    pub segment_size: u64,
}

impl Default for SegmentReplicationConfig {
    fn default() -> Self {
        Self {
            chunk_size: 4 * 1024 * 1024,      // 4 MB
            max_inflight_chunks: 8,
            tail_batch_size: 64 * 1024,        // 64 KB
            segment_size: 256 * 1024 * 1024,   // 256 MB
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Follower State Machine
// ═══════════════════════════════════════════════════════════════════════════

/// The state a follower maintains for segment-level replication.
#[derive(Debug, Clone)]
pub struct FollowerState {
    /// Node ID of this follower.
    pub node_id: NodeId,
    /// Last fully applied LSN (structured: segment + offset).
    pub local_last_lsn: StructuredLsn,
    /// Current local segment ID being written to.
    pub local_segment_id: u64,
    /// Current offset within the local segment.
    pub local_segment_offset: u64,
    /// Set of segment IDs that have been fully received and sealed locally.
    pub sealed_segments: BTreeSet<u64>,
    /// Current replication phase.
    pub phase: FollowerPhase,
}

/// Follower replication phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowerPhase {
    /// Initial state — no replication started.
    Init,
    /// Handshake sent, waiting for leader decision.
    Handshaking,
    /// Receiving sealed segments from leader.
    SegmentStreaming,
    /// Receiving tail of the active segment.
    TailStreaming,
    /// Fully caught up, waiting for new data.
    CaughtUp,
    /// Error state — will retry from last sealed segment.
    Error,
}

impl fmt::Display for FollowerPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Init => write!(f, "INIT"),
            Self::Handshaking => write!(f, "HANDSHAKING"),
            Self::SegmentStreaming => write!(f, "SEGMENT_STREAMING"),
            Self::TailStreaming => write!(f, "TAIL_STREAMING"),
            Self::CaughtUp => write!(f, "CAUGHT_UP"),
            Self::Error => write!(f, "ERROR"),
        }
    }
}

impl FollowerState {
    /// Create a new follower state (empty node, no data).
    pub const fn new_empty(node_id: NodeId) -> Self {
        Self {
            node_id,
            local_last_lsn: StructuredLsn::ZERO,
            local_segment_id: 0,
            local_segment_offset: 0,
            sealed_segments: BTreeSet::new(),
            phase: FollowerPhase::Init,
        }
    }

    /// Create a follower state from a recovered position.
    pub const fn from_recovered(
        node_id: NodeId,
        last_lsn: StructuredLsn,
        sealed_segments: BTreeSet<u64>,
    ) -> Self {
        Self {
            node_id,
            local_last_lsn: last_lsn,
            local_segment_id: last_lsn.segment_id(),
            local_segment_offset: last_lsn.offset(),
            sealed_segments,
            phase: FollowerPhase::Init,
        }
    }

    /// Mark a segment as sealed (fully received and persisted).
    pub fn seal_segment(&mut self, segment_id: u64) {
        self.sealed_segments.insert(segment_id);
        // Advance to next segment
        if segment_id >= self.local_segment_id {
            self.local_segment_id = segment_id + 1;
            self.local_segment_offset = 0;
            self.local_last_lsn = StructuredLsn::new(segment_id + 1, 0);
        }
    }

    /// Update tail position (within the active segment).
    pub const fn advance_tail(&mut self, new_offset: u64) {
        if new_offset > self.local_segment_offset {
            self.local_segment_offset = new_offset;
            self.local_last_lsn = StructuredLsn::new(self.local_segment_id, new_offset);
        }
    }

    /// Compute the set of segment IDs missing between this follower and a target.
    pub fn missing_segments(&self, leader_segment_id: u64) -> Vec<u64> {
        let mut missing = Vec::new();
        for seg in 0..leader_segment_id {
            if !self.sealed_segments.contains(&seg) {
                missing.push(seg);
            }
        }
        missing
    }

    /// Roll back to last known good state on error.
    pub fn rollback_to_last_sealed(&mut self) {
        self.phase = FollowerPhase::Error;
        if let Some(&last_sealed) = self.sealed_segments.iter().next_back() {
            self.local_segment_id = last_sealed + 1;
            self.local_segment_offset = 0;
            self.local_last_lsn = StructuredLsn::new(last_sealed + 1, 0);
        } else {
            self.local_segment_id = 0;
            self.local_segment_offset = 0;
            self.local_last_lsn = StructuredLsn::ZERO;
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Replication Handshake
// ═══════════════════════════════════════════════════════════════════════════

/// Message sent by follower to leader to initiate replication.
#[derive(Debug, Clone)]
pub struct ReplicationHello {
    pub node_id: NodeId,
    pub last_lsn: StructuredLsn,
    pub last_segment_id: u64,
    pub last_segment_offset: u64,
    /// Sealed segment IDs the follower already has.
    pub sealed_segment_ids: Vec<u64>,
}

impl ReplicationHello {
    /// Build a hello message from follower state.
    pub fn from_follower(state: &FollowerState) -> Self {
        Self {
            node_id: state.node_id,
            last_lsn: state.local_last_lsn,
            last_segment_id: state.local_segment_id,
            last_segment_offset: state.local_segment_offset,
            sealed_segment_ids: state.sealed_segments.iter().copied().collect(),
        }
    }
}

/// Leader's decision after evaluating a follower's hello.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationPath {
    /// Follower needs complete segments. Ship sealed segments first,
    /// then switch to tail streaming.
    SegmentStreaming {
        /// Segment IDs to stream (in order).
        segments_to_send: Vec<u64>,
    },
    /// Follower is on the same segment as leader — just send tail bytes.
    TailStreaming {
        /// Segment ID both leader and follower are on.
        segment_id: u64,
        /// Offset to start streaming from.
        from_offset: u64,
    },
    /// Follower is already caught up.
    AlreadyCaughtUp,
}

impl fmt::Display for ReplicationPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SegmentStreaming { segments_to_send } =>
                write!(f, "SEGMENT_STREAMING({} segments)", segments_to_send.len()),
            Self::TailStreaming { segment_id, from_offset } =>
                write!(f, "TAIL_STREAMING(seg={segment_id}, offset={from_offset})"),
            Self::AlreadyCaughtUp => write!(f, "CAUGHT_UP"),
        }
    }
}

/// Leader-side state for deciding replication path.
#[derive(Debug, Clone)]
pub struct LeaderState {
    /// Current active (unsealed) segment ID.
    pub current_segment_id: u64,
    /// Current write offset in the active segment.
    pub current_offset: u64,
    /// Set of sealed segment IDs available for streaming.
    pub sealed_segments: BTreeSet<u64>,
}

impl LeaderState {
    pub const fn new(current_segment_id: u64, current_offset: u64) -> Self {
        Self {
            current_segment_id,
            current_offset,
            sealed_segments: BTreeSet::new(),
        }
    }

    /// Decide replication path for a follower based on its hello message.
    pub fn decide_path(&self, hello: &ReplicationHello) -> ReplicationPath {
        let follower_sealed: BTreeSet<u64> = hello.sealed_segment_ids.iter().copied().collect();

        // Find segments the follower is missing
        let mut missing: Vec<u64> = self.sealed_segments
            .iter()
            .filter(|seg_id| !follower_sealed.contains(seg_id))
            .copied()
            .collect();
        missing.sort();

        if !missing.is_empty() {
            // Follower needs sealed segments
            ReplicationPath::SegmentStreaming {
                segments_to_send: missing,
            }
        } else if hello.last_segment_id == self.current_segment_id
            && hello.last_segment_offset < self.current_offset
        {
            // Same segment, but follower is behind on the tail
            ReplicationPath::TailStreaming {
                segment_id: self.current_segment_id,
                from_offset: hello.last_segment_offset,
            }
        } else {
            ReplicationPath::AlreadyCaughtUp
        }
    }

    /// Seal the current segment and advance to a new one.
    pub fn seal_current_segment(&mut self) {
        self.sealed_segments.insert(self.current_segment_id);
        self.current_segment_id += 1;
        self.current_offset = 0;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Segment Streaming Protocol
// ═══════════════════════════════════════════════════════════════════════════

/// A single chunk of a WAL segment being streamed.
///
/// Segments are sent as a sequence of chunks (4–16 MB each).
/// The follower writes them sequentially and verifies CRC32.
#[derive(Debug, Clone)]
pub struct SegmentChunk {
    /// Segment ID this chunk belongs to.
    pub segment_id: u64,
    /// Byte offset within the segment where this chunk starts.
    pub offset: u64,
    /// Chunk data (raw bytes from the segment file).
    pub data: Vec<u8>,
    /// CRC32 of the data.
    pub checksum: u32,
    /// Whether this is the last chunk of the segment.
    pub is_last: bool,
}

impl SegmentChunk {
    /// Compute checksum of the data.
    pub fn compute_checksum(data: &[u8]) -> u32 {
        compute_crc32(data)
    }

    /// Verify chunk integrity.
    pub fn verify(&self) -> bool {
        self.checksum == Self::compute_checksum(&self.data)
    }
}

/// A complete segment stream message — header + all chunks describe one segment.
#[derive(Debug, Clone)]
pub struct StreamSegment {
    /// Segment ID.
    pub segment_id: u64,
    /// Total segment size.
    pub segment_size: u64,
    /// Last valid data offset (everything beyond this is garbage/zero).
    pub last_valid_offset: u64,
    /// Segment header for validation.
    pub header: Vec<u8>,
}

/// Iterator that produces chunks from a segment's raw data.
///
/// In production, this reads from the WAL segment file.
/// For testing, it operates on in-memory bytes.
pub struct SegmentChunkIterator {
    segment_id: u64,
    data: Vec<u8>,
    last_valid_offset: u64,
    chunk_size: u64,
    current_offset: u64,
}

impl SegmentChunkIterator {
    /// Create an iterator over a segment's data.
    pub const fn new(segment_id: u64, data: Vec<u8>, last_valid_offset: u64, chunk_size: u64) -> Self {
        Self {
            segment_id,
            data,
            last_valid_offset,
            chunk_size,
            current_offset: 0,
        }
    }

    /// Total bytes that will be streamed.
    pub const fn total_bytes(&self) -> u64 {
        self.last_valid_offset
    }

    /// Bytes already produced.
    pub const fn bytes_produced(&self) -> u64 {
        self.current_offset
    }
}

impl Iterator for SegmentChunkIterator {
    type Item = SegmentChunk;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset >= self.last_valid_offset {
            return None;
        }

        let remaining = self.last_valid_offset - self.current_offset;
        let chunk_len = remaining.min(self.chunk_size) as usize;
        let start = self.current_offset as usize;
        let end = (start + chunk_len).min(self.data.len());

        if start >= self.data.len() {
            return None;
        }

        let chunk_data = self.data[start..end].to_vec();
        let checksum = SegmentChunk::compute_checksum(&chunk_data);
        let offset = self.current_offset;
        self.current_offset += chunk_data.len() as u64;

        let is_last = self.current_offset >= self.last_valid_offset;

        Some(SegmentChunk {
            segment_id: self.segment_id,
            offset,
            data: chunk_data,
            checksum,
            is_last,
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Tail Streaming Protocol
// ═══════════════════════════════════════════════════════════════════════════

/// A batch of tail bytes from the active (unsealed) segment.
#[derive(Debug, Clone)]
pub struct TailBatch {
    /// Segment ID.
    pub segment_id: u64,
    /// Byte offset within the segment where this batch starts.
    pub from_offset: u64,
    /// Raw bytes.
    pub data: Vec<u8>,
    /// CRC32 of the data.
    pub checksum: u32,
}

impl TailBatch {
    /// Create a tail batch from raw data.
    pub fn new(segment_id: u64, from_offset: u64, data: Vec<u8>) -> Self {
        let checksum = compute_crc32(&data);
        Self {
            segment_id,
            from_offset,
            data,
            checksum,
        }
    }

    /// Verify batch integrity.
    pub fn verify(&self) -> bool {
        let checksum = compute_crc32(&self.data);
        self.checksum == checksum
    }

    /// The LSN at the end of this batch.
    pub const fn end_lsn(&self) -> StructuredLsn {
        StructuredLsn::new(self.segment_id, self.from_offset + self.data.len() as u64)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — Snapshot = Segment Cut + Metadata
// ═══════════════════════════════════════════════════════════════════════════

/// A snapshot is NOT a data export — it is a deterministic cut point
/// at a segment boundary plus the set of sealed segments up to that point.
#[derive(Debug, Clone)]
pub struct SnapshotManifest {
    /// Unique snapshot ID.
    pub snapshot_id: u64,
    /// LSN at the snapshot cut point (always a segment boundary).
    pub snapshot_lsn: StructuredLsn,
    /// Sealed segment IDs included in this snapshot (≤ snapshot_lsn.segment_id()).
    pub sealed_segments: Vec<u64>,
    /// Schema metadata (serialized catalog).
    pub schema_metadata: Vec<u8>,
    /// Optional transaction metadata for in-doubt resolution.
    pub txn_metadata: Option<Vec<u8>>,
    /// Creation timestamp (unix seconds).
    pub created_at: u64,
}

impl SnapshotManifest {
    /// Create a snapshot at the current segment boundary.
    pub fn create(
        snapshot_id: u64,
        leader: &LeaderState,
        schema_metadata: Vec<u8>,
    ) -> Self {
        // Snapshot LSN is at the start of the current (unsealed) segment.
        // All sealed segments up to this point are included.
        let snapshot_lsn = StructuredLsn::new(leader.current_segment_id, 0);
        let sealed: Vec<u64> = leader.sealed_segments.iter().copied().collect();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            snapshot_id,
            snapshot_lsn,
            sealed_segments: sealed,
            schema_metadata,
            txn_metadata: None,
            created_at: now,
        }
    }

    /// Check if a follower needs this snapshot (has none of the segments).
    pub fn follower_needs_full_snapshot(&self, follower: &FollowerState) -> bool {
        follower.sealed_segments.is_empty()
            && follower.local_last_lsn == StructuredLsn::ZERO
    }

    /// Compute the set of segments a follower needs from this snapshot.
    pub fn segments_for_follower(&self, follower: &FollowerState) -> Vec<u64> {
        self.sealed_segments
            .iter()
            .filter(|seg| !follower.sealed_segments.contains(seg))
            .copied()
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 — Replication Coordinator (Leader Side)
// ═══════════════════════════════════════════════════════════════════════════

/// Manages segment-level replication for all followers.
///
/// Invariants enforced:
/// - ❌ Replication never modifies WAL
/// - ❌ Snapshot never writes to WAL
/// - ✅ Sealed segments are immutable
/// - ✅ Replication is a read-only view of WAL
pub struct SegmentReplicationCoordinator {
    config: SegmentReplicationConfig,
    leader: RwLock<LeaderState>,
    followers: RwLock<HashMap<NodeId, FollowerState>>,
    /// In-memory segment store (for testing / in-process mode).
    /// In production, this would be backed by the filesystem.
    segment_store: RwLock<HashMap<u64, Vec<u8>>>,
    pub metrics: SegmentStreamingMetrics,
}

impl SegmentReplicationCoordinator {
    /// Create a new coordinator.
    pub fn new(config: SegmentReplicationConfig, leader_segment_id: u64, leader_offset: u64) -> Self {
        Self {
            config,
            leader: RwLock::new(LeaderState::new(leader_segment_id, leader_offset)),
            followers: RwLock::new(HashMap::new()),
            segment_store: RwLock::new(HashMap::new()),
            metrics: SegmentStreamingMetrics::default(),
        }
    }

    /// Register a sealed segment's data (for testing / in-process mode).
    pub fn register_segment_data(&self, segment_id: u64, data: Vec<u8>) {
        self.segment_store.write().insert(segment_id, data);
        self.leader.write().sealed_segments.insert(segment_id);
    }

    /// Register the active (unsealed) segment's data for tail streaming.
    /// Does NOT mark the segment as sealed.
    pub fn register_active_segment_data(&self, segment_id: u64, data: Vec<u8>) {
        self.segment_store.write().insert(segment_id, data);
    }

    /// Update leader's current write position.
    pub fn update_leader_position(&self, segment_id: u64, offset: u64) {
        let mut leader = self.leader.write();
        leader.current_segment_id = segment_id;
        leader.current_offset = offset;
    }

    /// Seal the leader's current segment and advance.
    pub fn seal_leader_segment(&self) {
        self.leader.write().seal_current_segment();
    }

    /// Handle a replication handshake from a follower.
    /// Returns the decided replication path.
    pub fn handle_hello(&self, hello: ReplicationHello) -> ReplicationPath {
        let leader = self.leader.read();
        let path = leader.decide_path(&hello);

        // Track follower state
        let follower_state = FollowerState::from_recovered(
            hello.node_id,
            hello.last_lsn,
            hello.sealed_segment_ids.iter().copied().collect(),
        );
        self.followers.write().insert(hello.node_id, follower_state);

        self.metrics.handshakes_total.fetch_add(1, Ordering::Relaxed);

        path
    }

    /// Stream a sealed segment to a follower as chunks.
    /// Returns an iterator of `SegmentChunk`s.
    pub fn stream_segment(&self, segment_id: u64) -> Option<SegmentChunkIterator> {
        let store = self.segment_store.read();
        let data = store.get(&segment_id)?;

        // Parse last_valid_offset from the segment header if possible
        let last_valid = if data.len() >= SEGMENT_HEADER_SIZE as usize {
            if let Some(hdr) = SegmentHeader::from_bytes(data) {
                if hdr.validate() {
                    hdr.last_valid_offset
                } else {
                    data.len() as u64
                }
            } else {
                data.len() as u64
            }
        } else {
            data.len() as u64
        };

        self.metrics.segments_streamed_total.fetch_add(1, Ordering::Relaxed);

        Some(SegmentChunkIterator::new(
            segment_id,
            data.clone(),
            last_valid,
            self.config.chunk_size,
        ))
    }

    /// Get tail bytes for a follower on the active segment.
    /// In production, this reads from the active WAL file.
    /// Here we read from the segment store for testing.
    pub fn get_tail_batch(
        &self,
        segment_id: u64,
        from_offset: u64,
    ) -> Option<TailBatch> {
        let store = self.segment_store.read();
        // Try the active segment data (may be in store for testing)
        let data = store.get(&segment_id)?;

        let leader = self.leader.read();
        let end_offset = if segment_id == leader.current_segment_id {
            leader.current_offset
        } else {
            data.len() as u64
        };

        if from_offset >= end_offset {
            return None; // already caught up
        }

        let batch_end = (from_offset + self.config.tail_batch_size).min(end_offset);
        let start = from_offset as usize;
        let end = batch_end as usize;

        if start >= data.len() || end > data.len() {
            return None;
        }

        let batch_data = data[start..end].to_vec();
        self.metrics.tail_bytes_total.fetch_add(batch_data.len() as u64, Ordering::Relaxed);

        Some(TailBatch::new(segment_id, from_offset, batch_data))
    }

    /// Apply a received segment chunk on the follower side.
    /// Returns `Ok(true)` if the segment is complete, `Ok(false)` if more chunks needed.
    pub fn follower_receive_chunk(
        &self,
        node_id: NodeId,
        chunk: &SegmentChunk,
    ) -> Result<bool, StreamingError> {
        // Verify integrity
        if !chunk.verify() {
            self.metrics.checksum_failures.fetch_add(1, Ordering::Relaxed);
            return Err(StreamingError::ChecksumMismatch {
                segment_id: chunk.segment_id,
                offset: chunk.offset,
            });
        }

        self.metrics.segment_bytes_total.fetch_add(chunk.data.len() as u64, Ordering::Relaxed);

        // Update follower state
        let mut followers = self.followers.write();
        if let Some(state) = followers.get_mut(&node_id) {
            state.phase = FollowerPhase::SegmentStreaming;
            if chunk.is_last {
                state.seal_segment(chunk.segment_id);
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Apply a received tail batch on the follower side.
    pub fn follower_receive_tail(
        &self,
        node_id: NodeId,
        batch: &TailBatch,
    ) -> Result<(), StreamingError> {
        if !batch.verify() {
            self.metrics.checksum_failures.fetch_add(1, Ordering::Relaxed);
            return Err(StreamingError::ChecksumMismatch {
                segment_id: batch.segment_id,
                offset: batch.from_offset,
            });
        }

        self.metrics.tail_bytes_total.fetch_add(batch.data.len() as u64, Ordering::Relaxed);

        let mut followers = self.followers.write();
        if let Some(state) = followers.get_mut(&node_id) {
            state.phase = FollowerPhase::TailStreaming;
            state.advance_tail(batch.from_offset + batch.data.len() as u64);
        }

        Ok(())
    }

    /// Handle follower error — roll back to last sealed segment.
    pub fn follower_handle_error(&self, node_id: NodeId) {
        let mut followers = self.followers.write();
        if let Some(state) = followers.get_mut(&node_id) {
            state.rollback_to_last_sealed();
            self.metrics.error_rollbacks.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get the current state of a follower.
    pub fn get_follower_state(&self, node_id: NodeId) -> Option<FollowerState> {
        self.followers.read().get(&node_id).cloned()
    }

    /// Get replication lag for a follower (in segments and bytes).
    pub fn follower_lag(&self, node_id: NodeId) -> Option<ReplicationLag> {
        let followers = self.followers.read();
        let follower = followers.get(&node_id)?;
        let leader = self.leader.read();

        let segment_lag = leader.current_segment_id.saturating_sub(follower.local_segment_id);
        let byte_lag = if follower.local_segment_id == leader.current_segment_id {
            leader.current_offset.saturating_sub(follower.local_segment_offset)
        } else {
            // Rough estimate: remaining segments × segment_size + tail offset
            segment_lag * self.config.segment_size + leader.current_offset
        };

        Some(ReplicationLag {
            segment_lag,
            byte_lag,
            follower_lsn: follower.local_last_lsn,
        })
    }

    /// Get a snapshot of the leader state.
    pub fn leader_snapshot(&self) -> LeaderState {
        self.leader.read().clone()
    }

    /// Create a snapshot manifest at the current segment boundary.
    pub fn create_snapshot(&self, snapshot_id: u64, schema_metadata: Vec<u8>) -> SnapshotManifest {
        let leader = self.leader.read();
        SnapshotManifest::create(snapshot_id, &leader, schema_metadata)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §8 — Chunk Streaming with Backpressure
// ═══════════════════════════════════════════════════════════════════════════

/// Backpressure-aware segment stream sender.
///
/// Tracks in-flight chunks and pauses sending when the limit is reached.
/// The receiver must ack chunks to allow more to be sent.
pub struct BackpressureSender {
    max_inflight: u32,
    inflight: AtomicU64,
    /// Segments queued for streaming.
    queue: Mutex<Vec<u64>>,
    /// Whether streaming has been cancelled.
    cancelled: std::sync::atomic::AtomicBool,
}

impl BackpressureSender {
    pub const fn new(max_inflight: u32) -> Self {
        Self {
            max_inflight,
            inflight: AtomicU64::new(0),
            queue: Mutex::new(Vec::new()),
            cancelled: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Check if we can send another chunk.
    pub fn can_send(&self) -> bool {
        !self.cancelled.load(Ordering::Relaxed)
            && self.inflight.load(Ordering::Relaxed) < u64::from(self.max_inflight)
    }

    /// Mark a chunk as sent (increases inflight count).
    pub fn mark_sent(&self) {
        self.inflight.fetch_add(1, Ordering::Relaxed);
    }

    /// Ack a chunk (decreases inflight count).
    pub fn ack(&self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Current inflight count.
    pub fn inflight_count(&self) -> u64 {
        self.inflight.load(Ordering::Relaxed)
    }

    /// Cancel streaming.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Whether streaming has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Resume after cancel.
    pub fn resume(&self) {
        self.cancelled.store(false, Ordering::Relaxed);
    }

    /// Enqueue segments for streaming.
    pub fn enqueue_segments(&self, segments: &[u64]) {
        self.queue.lock().extend_from_slice(segments);
    }

    /// Dequeue next segment to stream (if backpressure allows).
    pub fn next_segment(&self) -> Option<u64> {
        if !self.can_send() {
            return None;
        }
        let mut queue = self.queue.lock();
        if queue.is_empty() {
            return None;
        }
        Some(queue.remove(0))
    }

    /// Remaining segments in queue.
    pub fn queue_len(&self) -> usize {
        self.queue.lock().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §9 — Error Handling
// ═══════════════════════════════════════════════════════════════════════════

/// Errors during segment streaming.
#[derive(Debug, Clone)]
pub enum StreamingError {
    /// CRC32 checksum mismatch on received chunk.
    ChecksumMismatch { segment_id: u64, offset: u64 },
    /// Follower disk is full.
    DiskFull { node_id: NodeId },
    /// Network interrupted during streaming.
    NetworkInterrupted { reason: String },
    /// Leader changed during streaming (epoch mismatch).
    LeaderChanged { old_leader: NodeId, new_leader: NodeId },
    /// Segment not found on leader.
    SegmentNotFound { segment_id: u64 },
    /// Follower not registered.
    UnknownFollower { node_id: NodeId },
}

impl fmt::Display for StreamingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChecksumMismatch { segment_id, offset } =>
                write!(f, "checksum mismatch: segment={segment_id}, offset={offset}"),
            Self::DiskFull { node_id } =>
                write!(f, "disk full on node {node_id}"),
            Self::NetworkInterrupted { reason } =>
                write!(f, "network interrupted: {reason}"),
            Self::LeaderChanged { old_leader, new_leader } =>
                write!(f, "leader changed: {old_leader} → {new_leader}"),
            Self::SegmentNotFound { segment_id } =>
                write!(f, "segment {segment_id} not found"),
            Self::UnknownFollower { node_id } =>
                write!(f, "unknown follower: node {node_id}"),
        }
    }
}

/// Recovery action after a streaming error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorRecoveryAction {
    /// Retry from the last sealed segment (re-handshake).
    RetryFromLastSealed,
    /// Retry just the current chunk.
    RetryChunk,
    /// Abort replication (fatal).
    Abort,
}

/// Decide recovery action based on error type.
pub const fn decide_recovery(error: &StreamingError) -> ErrorRecoveryAction {
    match error {
        StreamingError::ChecksumMismatch { .. } => ErrorRecoveryAction::RetryChunk,
        StreamingError::DiskFull { .. } => ErrorRecoveryAction::Abort,
        StreamingError::NetworkInterrupted { .. }
        | StreamingError::LeaderChanged { .. }
        | StreamingError::SegmentNotFound { .. }
        | StreamingError::UnknownFollower { .. } => ErrorRecoveryAction::RetryFromLastSealed,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §10 — Replication Lag
// ═══════════════════════════════════════════════════════════════════════════

/// Replication lag for a follower.
#[derive(Debug, Clone)]
pub struct ReplicationLag {
    /// Number of complete segments the follower is behind.
    pub segment_lag: u64,
    /// Estimated byte lag.
    pub byte_lag: u64,
    /// Follower's current LSN.
    pub follower_lsn: StructuredLsn,
}

impl fmt::Display for ReplicationLag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "lag: {} segments, {} bytes (follower at {})",
            self.segment_lag, self.byte_lag, self.follower_lsn
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §11 — Metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Observability metrics for segment-level replication.
#[derive(Debug, Default)]
pub struct SegmentStreamingMetrics {
    /// Total handshakes processed.
    pub handshakes_total: AtomicU64,
    /// Total sealed segments streamed to followers.
    pub segments_streamed_total: AtomicU64,
    /// Total bytes streamed via segment streaming.
    pub segment_bytes_total: AtomicU64,
    /// Total bytes streamed via tail streaming.
    pub tail_bytes_total: AtomicU64,
    /// Total checksum failures detected.
    pub checksum_failures: AtomicU64,
    /// Total error rollbacks (follower rolled back to last sealed).
    pub error_rollbacks: AtomicU64,
    /// Total snapshots created.
    pub snapshots_created: AtomicU64,
}

impl SegmentStreamingMetrics {
    pub fn snapshot(&self) -> SegmentStreamingMetricsSnapshot {
        SegmentStreamingMetricsSnapshot {
            handshakes_total: self.handshakes_total.load(Ordering::Relaxed),
            segments_streamed_total: self.segments_streamed_total.load(Ordering::Relaxed),
            segment_bytes_total: self.segment_bytes_total.load(Ordering::Relaxed),
            tail_bytes_total: self.tail_bytes_total.load(Ordering::Relaxed),
            checksum_failures: self.checksum_failures.load(Ordering::Relaxed),
            error_rollbacks: self.error_rollbacks.load(Ordering::Relaxed),
            snapshots_created: self.snapshots_created.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of streaming metrics.
#[derive(Debug, Clone, Default)]
pub struct SegmentStreamingMetricsSnapshot {
    pub handshakes_total: u64,
    pub segments_streamed_total: u64,
    pub segment_bytes_total: u64,
    pub tail_bytes_total: u64,
    pub checksum_failures: u64,
    pub error_rollbacks: u64,
    pub snapshots_created: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// §12 — Unit Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_segment_data(segment_id: u64, payload_size: usize) -> Vec<u8> {
        let hdr = SegmentHeader::new(segment_id, 256 * 1024 * 1024, StructuredLsn::new(segment_id, 0));
        let mut hdr_mod = hdr;
        hdr_mod.last_valid_offset = SEGMENT_HEADER_SIZE + payload_size as u64;
        hdr_mod.checksum = hdr_mod.compute_checksum();
        let mut data = hdr_mod.to_bytes();
        data.extend(vec![0xAB; payload_size]);
        data
    }

    // -- Follower State --

    #[test]
    fn test_follower_state_new_empty() {
        let state = FollowerState::new_empty(1);
        assert_eq!(state.node_id, 1);
        assert_eq!(state.local_last_lsn, StructuredLsn::ZERO);
        assert!(state.sealed_segments.is_empty());
        assert_eq!(state.phase, FollowerPhase::Init);
    }

    #[test]
    fn test_follower_seal_segment() {
        let mut state = FollowerState::new_empty(1);
        state.seal_segment(0);
        assert!(state.sealed_segments.contains(&0));
        assert_eq!(state.local_segment_id, 1);
        assert_eq!(state.local_segment_offset, 0);
    }

    #[test]
    fn test_follower_advance_tail() {
        let mut state = FollowerState::new_empty(1);
        state.advance_tail(5000);
        assert_eq!(state.local_segment_offset, 5000);
        assert_eq!(state.local_last_lsn, StructuredLsn::new(0, 5000));
    }

    #[test]
    fn test_follower_missing_segments() {
        let mut state = FollowerState::new_empty(1);
        state.seal_segment(0);
        state.seal_segment(2);
        let missing = state.missing_segments(5);
        assert_eq!(missing, vec![1, 3, 4]);
    }

    #[test]
    fn test_follower_rollback() {
        let mut state = FollowerState::new_empty(1);
        state.seal_segment(0);
        state.seal_segment(1);
        state.advance_tail(5000);
        state.rollback_to_last_sealed();
        assert_eq!(state.phase, FollowerPhase::Error);
        assert_eq!(state.local_segment_id, 2);
        assert_eq!(state.local_segment_offset, 0);
    }

    #[test]
    fn test_follower_rollback_no_sealed() {
        let mut state = FollowerState::new_empty(1);
        state.advance_tail(5000);
        state.rollback_to_last_sealed();
        assert_eq!(state.local_last_lsn, StructuredLsn::ZERO);
    }

    // -- ReplicationHello & Path Decision --

    #[test]
    fn test_handshake_segment_streaming_path() {
        let mut leader = LeaderState::new(3, 10000);
        leader.sealed_segments.insert(0);
        leader.sealed_segments.insert(1);
        leader.sealed_segments.insert(2);

        let follower = FollowerState::new_empty(1);
        let hello = ReplicationHello::from_follower(&follower);
        let path = leader.decide_path(&hello);

        match path {
            ReplicationPath::SegmentStreaming { segments_to_send } => {
                assert_eq!(segments_to_send, vec![0, 1, 2]);
            }
            _ => panic!("expected SegmentStreaming"),
        }
    }

    #[test]
    fn test_handshake_tail_streaming_path() {
        let leader = LeaderState::new(2, 50000);

        let mut follower = FollowerState::new_empty(1);
        follower.seal_segment(0);
        follower.seal_segment(1);
        follower.local_segment_id = 2;
        follower.local_segment_offset = 10000;
        follower.local_last_lsn = StructuredLsn::new(2, 10000);

        let hello = ReplicationHello::from_follower(&follower);
        let path = leader.decide_path(&hello);

        match path {
            ReplicationPath::TailStreaming { segment_id, from_offset } => {
                assert_eq!(segment_id, 2);
                assert_eq!(from_offset, 10000);
            }
            _ => panic!("expected TailStreaming"),
        }
    }

    #[test]
    fn test_handshake_already_caught_up() {
        let leader = LeaderState::new(2, 50000);

        let mut follower = FollowerState::new_empty(1);
        follower.seal_segment(0);
        follower.seal_segment(1);
        follower.local_segment_id = 2;
        follower.local_segment_offset = 50000;
        follower.local_last_lsn = StructuredLsn::new(2, 50000);

        let hello = ReplicationHello::from_follower(&follower);
        assert_eq!(leader.decide_path(&hello), ReplicationPath::AlreadyCaughtUp);
    }

    // -- Segment Chunk Iterator --

    #[test]
    fn test_segment_chunk_iterator() {
        let data = vec![0xAB; 10000];
        let iter = SegmentChunkIterator::new(0, data, 10000, 4000);
        let chunks: Vec<SegmentChunk> = iter.collect();

        assert_eq!(chunks.len(), 3); // 4000 + 4000 + 2000
        assert_eq!(chunks[0].data.len(), 4000);
        assert_eq!(chunks[0].offset, 0);
        assert!(!chunks[0].is_last);
        assert_eq!(chunks[1].data.len(), 4000);
        assert_eq!(chunks[1].offset, 4000);
        assert!(!chunks[1].is_last);
        assert_eq!(chunks[2].data.len(), 2000);
        assert_eq!(chunks[2].offset, 8000);
        assert!(chunks[2].is_last);
    }

    #[test]
    fn test_segment_chunk_checksum() {
        let chunk = SegmentChunk {
            segment_id: 0,
            offset: 0,
            data: vec![1, 2, 3, 4],
            checksum: compute_crc32(&[1, 2, 3, 4]),
            is_last: true,
        };
        assert!(chunk.verify());
    }

    #[test]
    fn test_segment_chunk_checksum_corrupt() {
        let chunk = SegmentChunk {
            segment_id: 0,
            offset: 0,
            data: vec![1, 2, 3, 4],
            checksum: 0xDEAD,
            is_last: true,
        };
        assert!(!chunk.verify());
    }

    // -- Tail Batch --

    #[test]
    fn test_tail_batch_verify() {
        let batch = TailBatch::new(0, 1000, vec![10, 20, 30]);
        assert!(batch.verify());
        assert_eq!(batch.end_lsn(), StructuredLsn::new(0, 1003));
    }

    // -- Snapshot Manifest --

    #[test]
    fn test_snapshot_manifest_creation() {
        let mut leader = LeaderState::new(3, 5000);
        leader.sealed_segments.insert(0);
        leader.sealed_segments.insert(1);
        leader.sealed_segments.insert(2);

        let snap = SnapshotManifest::create(1, &leader, b"schema".to_vec());
        assert_eq!(snap.snapshot_lsn, StructuredLsn::new(3, 0));
        assert_eq!(snap.sealed_segments, vec![0, 1, 2]);
        assert!(snap.created_at > 0);
    }

    #[test]
    fn test_snapshot_follower_needs() {
        let mut leader = LeaderState::new(3, 0);
        leader.sealed_segments.extend([0, 1, 2]);
        let snap = SnapshotManifest::create(1, &leader, vec![]);

        let empty_follower = FollowerState::new_empty(1);
        assert!(snap.follower_needs_full_snapshot(&empty_follower));

        let mut partial = FollowerState::new_empty(2);
        partial.seal_segment(0);
        let needed = snap.segments_for_follower(&partial);
        assert_eq!(needed, vec![1, 2]);
    }

    // -- Coordinator End-to-End --

    #[test]
    fn test_coordinator_segment_streaming_e2e() {
        let coord = SegmentReplicationCoordinator::new(
            SegmentReplicationConfig { chunk_size: 4096, ..Default::default() },
            3, 5000,
        );

        // Register 3 sealed segments
        for seg in 0..3 {
            coord.register_segment_data(seg, make_segment_data(seg, 8192));
        }

        // Follower with no data sends hello
        let follower = FollowerState::new_empty(1);
        let hello = ReplicationHello::from_follower(&follower);
        let path = coord.handle_hello(hello);

        match &path {
            ReplicationPath::SegmentStreaming { segments_to_send } => {
                assert_eq!(*segments_to_send, vec![0, 1, 2]);
            }
            _ => panic!("expected SegmentStreaming, got {}", path),
        }

        // Stream segment 0
        let iter = coord.stream_segment(0).expect("segment 0 should exist");
        let chunks: Vec<SegmentChunk> = iter.collect();
        assert!(!chunks.is_empty());

        // Follower receives chunks
        for chunk in &chunks {
            let complete = coord.follower_receive_chunk(1, chunk).unwrap();
            if chunk.is_last {
                assert!(complete);
            }
        }

        let state = coord.get_follower_state(1).unwrap();
        assert!(state.sealed_segments.contains(&0));
    }

    #[test]
    fn test_coordinator_tail_streaming_e2e() {
        let coord = SegmentReplicationCoordinator::new(
            SegmentReplicationConfig {
                chunk_size: 4096,
                tail_batch_size: 1024,
                ..Default::default()
            },
            2, 50000,
        );

        // Register sealed segments 0,1 on leader side
        coord.register_segment_data(0, vec![0u8; 1000]);
        coord.register_segment_data(1, vec![0u8; 1000]);

        // Register active (unsealed) segment data
        let mut active_data = vec![0u8; 50000];
        for i in 0..50000 { active_data[i] = (i % 256) as u8; }
        coord.register_active_segment_data(2, active_data);

        // Follower on same segment but behind
        let mut follower = FollowerState::new_empty(1);
        follower.seal_segment(0);
        follower.seal_segment(1);
        follower.local_segment_id = 2;
        follower.local_segment_offset = 10000;
        follower.local_last_lsn = StructuredLsn::new(2, 10000);

        let hello = ReplicationHello::from_follower(&follower);
        let path = coord.handle_hello(hello);
        assert!(matches!(path, ReplicationPath::TailStreaming { .. }));

        // Get tail batch
        let batch = coord.get_tail_batch(2, 10000).unwrap();
        assert!(batch.verify());
        assert_eq!(batch.from_offset, 10000);
        assert_eq!(batch.data.len(), 1024);

        // Follower receives tail
        coord.follower_receive_tail(1, &batch).unwrap();
        let state = coord.get_follower_state(1).unwrap();
        assert_eq!(state.local_segment_offset, 11024);
    }

    #[test]
    fn test_coordinator_checksum_failure() {
        let coord = SegmentReplicationCoordinator::new(Default::default(), 1, 0);

        // Register follower
        let follower = FollowerState::new_empty(1);
        let hello = ReplicationHello::from_follower(&follower);
        coord.handle_hello(hello);

        // Corrupt chunk
        let bad_chunk = SegmentChunk {
            segment_id: 0,
            offset: 0,
            data: vec![1, 2, 3],
            checksum: 0xBAD,
            is_last: false,
        };

        let result = coord.follower_receive_chunk(1, &bad_chunk);
        assert!(result.is_err());
        assert_eq!(coord.metrics.checksum_failures.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_coordinator_error_rollback() {
        let coord = SegmentReplicationCoordinator::new(Default::default(), 3, 0);

        // Register follower with some progress
        let mut follower = FollowerState::new_empty(1);
        follower.seal_segment(0);
        follower.seal_segment(1);
        let hello = ReplicationHello::from_follower(&follower);
        coord.handle_hello(hello);

        // Trigger error rollback
        coord.follower_handle_error(1);
        let state = coord.get_follower_state(1).unwrap();
        assert_eq!(state.phase, FollowerPhase::Error);
        assert_eq!(state.local_segment_id, 2);
        assert_eq!(state.local_segment_offset, 0);
    }

    #[test]
    fn test_coordinator_replication_lag() {
        let coord = SegmentReplicationCoordinator::new(
            SegmentReplicationConfig {
                segment_size: 1000, // small for testing
                ..Default::default()
            },
            5, 500,
        );

        let follower = FollowerState::new_empty(1);
        let hello = ReplicationHello::from_follower(&follower);
        coord.handle_hello(hello);

        let lag = coord.follower_lag(1).unwrap();
        assert_eq!(lag.segment_lag, 5);
        assert!(lag.byte_lag > 0);
    }

    // -- Backpressure --

    #[test]
    fn test_backpressure_sender() {
        let bp = BackpressureSender::new(2);
        assert!(bp.can_send());

        bp.mark_sent();
        bp.mark_sent();
        assert!(!bp.can_send()); // at limit

        bp.ack();
        assert!(bp.can_send());
        assert_eq!(bp.inflight_count(), 1);
    }

    #[test]
    fn test_backpressure_cancel_resume() {
        let bp = BackpressureSender::new(10);
        assert!(bp.can_send());

        bp.cancel();
        assert!(!bp.can_send());
        assert!(bp.is_cancelled());

        bp.resume();
        assert!(bp.can_send());
    }

    #[test]
    fn test_backpressure_queue() {
        let bp = BackpressureSender::new(2);
        bp.enqueue_segments(&[0, 1, 2, 3]);
        assert_eq!(bp.queue_len(), 4);

        assert_eq!(bp.next_segment(), Some(0));
        bp.mark_sent();
        assert_eq!(bp.next_segment(), Some(1));
        bp.mark_sent();
        // At limit, can't dequeue
        assert_eq!(bp.next_segment(), None);
        bp.ack();
        assert_eq!(bp.next_segment(), Some(2));
    }

    // -- Error Recovery --

    #[test]
    fn test_error_recovery_decisions() {
        assert_eq!(
            decide_recovery(&StreamingError::ChecksumMismatch { segment_id: 0, offset: 0 }),
            ErrorRecoveryAction::RetryChunk
        );
        assert_eq!(
            decide_recovery(&StreamingError::DiskFull { node_id: 1 }),
            ErrorRecoveryAction::Abort
        );
        assert_eq!(
            decide_recovery(&StreamingError::NetworkInterrupted { reason: "timeout".into() }),
            ErrorRecoveryAction::RetryFromLastSealed
        );
        assert_eq!(
            decide_recovery(&StreamingError::LeaderChanged { old_leader: 1, new_leader: 2 }),
            ErrorRecoveryAction::RetryFromLastSealed
        );
    }

    // -- Leader State --

    #[test]
    fn test_leader_seal_segment() {
        let mut leader = LeaderState::new(0, 50000);
        leader.seal_current_segment();
        assert!(leader.sealed_segments.contains(&0));
        assert_eq!(leader.current_segment_id, 1);
        assert_eq!(leader.current_offset, 0);
    }

    // -- Metrics --

    #[test]
    fn test_metrics_snapshot() {
        let m = SegmentStreamingMetrics::default();
        m.handshakes_total.fetch_add(5, Ordering::Relaxed);
        m.segments_streamed_total.fetch_add(10, Ordering::Relaxed);
        m.segment_bytes_total.fetch_add(1_000_000, Ordering::Relaxed);

        let snap = m.snapshot();
        assert_eq!(snap.handshakes_total, 5);
        assert_eq!(snap.segments_streamed_total, 10);
        assert_eq!(snap.segment_bytes_total, 1_000_000);
    }

    // -- Display impls --

    #[test]
    fn test_display_impls() {
        assert_eq!(format!("{}", FollowerPhase::SegmentStreaming), "SEGMENT_STREAMING");
        assert_eq!(format!("{}", FollowerPhase::TailStreaming), "TAIL_STREAMING");
        assert_eq!(format!("{}", FollowerPhase::CaughtUp), "CAUGHT_UP");

        let path = ReplicationPath::SegmentStreaming { segments_to_send: vec![0, 1, 2] };
        assert!(format!("{}", path).contains("3 segments"));

        let err = StreamingError::ChecksumMismatch { segment_id: 5, offset: 100 };
        assert!(format!("{}", err).contains("segment=5"));
    }
}
