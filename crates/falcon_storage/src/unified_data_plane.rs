//! # Unified Data Plane: Segments + Manifest + Streaming
//!
//! **Segments are the only physical truth. Manifest describes truth; streaming moves truth.**
//!
//! WAL, Cold Store, and Snapshot all share the same segment format, IO interface,
//! and streaming protocol. Recovery, replication, and rebalance use one code path.
//!
//! ## Segment kinds
//! - `WalSegment`: append-only, LSN = segment + offset
//! - `ColdSegment`: immutable compressed blocks (row/version payloads)
//! - `SnapshotSegment`: snapshot data chunk (cold + wal cut combination)
//!
//! ## Manifest
//! - Append-only, epoch-versioned
//! - Describes which segments constitute the current database state
//! - Portable: replicated alongside segments

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};

use crate::structured_lsn::StructuredLsn;
use crate::csn::Csn;

// ═══════════════════════════════════════════════════════════════════════════
// §A1 — Segment Kind & Codec
// ═══════════════════════════════════════════════════════════════════════════

/// The three segment kinds in the unified data plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum SegmentKind {
    /// Append-only WAL segment. LSN = segment_id + offset.
    Wal = 0,
    /// Immutable compressed cold-store segment (row payloads).
    Cold = 1,
    /// Snapshot data chunk.
    Snapshot = 2,
}

impl SegmentKind {
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Wal),
            1 => Some(Self::Cold),
            2 => Some(Self::Snapshot),
            _ => None,
        }
    }
}

impl fmt::Display for SegmentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wal => write!(f, "WAL"),
            Self::Cold => write!(f, "COLD"),
            Self::Snapshot => write!(f, "SNAPSHOT"),
        }
    }
}

/// Compression codec for segment body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SegmentCodec {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

impl SegmentCodec {
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::None),
            1 => Some(Self::Lz4),
            2 => Some(Self::Zstd),
            _ => None,
        }
    }
}

impl fmt::Display for SegmentCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Lz4 => write!(f, "lz4"),
            Self::Zstd => write!(f, "zstd"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §A1b — Logical Range (routing / recovery key)
// ═══════════════════════════════════════════════════════════════════════════

/// Logical range metadata — how this segment maps to the logical data space.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalRange {
    /// WAL segment: [start_lsn, end_lsn)
    Wal { start_lsn: StructuredLsn, end_lsn: StructuredLsn },
    /// Cold segment: key range [min_key, max_key], table/shard scope.
    Cold { table_id: u64, shard_id: u64, min_key: Vec<u8>, max_key: Vec<u8> },
    /// Snapshot segment: snapshot_id + chunk_index.
    Snapshot { snapshot_id: u64, chunk_index: u32 },
}

impl fmt::Display for LogicalRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wal { start_lsn, end_lsn } =>
                write!(f, "WAL[{start_lsn}..{end_lsn})"),
            Self::Cold { table_id, shard_id, .. } =>
                write!(f, "COLD[table={table_id},shard={shard_id}]"),
            Self::Snapshot { snapshot_id, chunk_index } =>
                write!(f, "SNAP[id={snapshot_id},chunk={chunk_index}]"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §A2 — Unified Segment Header (4K aligned)
// ═══════════════════════════════════════════════════════════════════════════

/// Magic bytes for unified segment format.
pub const UNIFIED_SEGMENT_MAGIC: u32 = 0x46535547; // "FSUG" = Falcon Segment Unified

/// Header version.
pub const UNIFIED_HEADER_VERSION: u16 = 1;

/// Size of the serialized header (4K aligned).
pub const UNIFIED_HEADER_SIZE: u64 = 4096;

/// Unified segment header — shared by WAL, Cold, and Snapshot segments.
///
/// Always 4K-aligned for direct I/O compatibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnifiedSegmentHeader {
    /// Magic bytes (FSUG).
    pub magic: u32,
    /// Header format version.
    pub version: u16,
    /// Segment kind (wal/cold/snapshot).
    pub kind: SegmentKind,
    /// Unique segment identifier.
    pub segment_id: u64,
    /// Total segment size in bytes (including header).
    pub segment_size: u64,
    /// Creation timestamp (unix epoch seconds).
    pub created_at_epoch: u64,
    /// Compression codec for the body.
    pub codec: SegmentCodec,
    /// CRC32 checksum of header fields (excluding this field).
    pub checksum: u32,
    /// Last valid data offset within the segment body.
    pub last_valid_offset: u64,
    /// Logical range (serialized inline).
    pub logical_range: LogicalRange,
}

impl UnifiedSegmentHeader {
    /// Create a new WAL segment header.
    pub fn new_wal(segment_id: u64, segment_size: u64, start_lsn: StructuredLsn) -> Self {
        let mut h = Self {
            magic: UNIFIED_SEGMENT_MAGIC,
            version: UNIFIED_HEADER_VERSION,
            kind: SegmentKind::Wal,
            segment_id,
            segment_size,
            created_at_epoch: now_epoch(),
            codec: SegmentCodec::None,
            checksum: 0,
            last_valid_offset: UNIFIED_HEADER_SIZE,
            logical_range: LogicalRange::Wal {
                start_lsn,
                end_lsn: start_lsn,
            },
        };
        h.checksum = h.compute_checksum();
        h
    }

    /// Create a new cold segment header.
    pub fn new_cold(
        segment_id: u64,
        segment_size: u64,
        codec: SegmentCodec,
        table_id: u64,
        shard_id: u64,
    ) -> Self {
        let mut h = Self {
            magic: UNIFIED_SEGMENT_MAGIC,
            version: UNIFIED_HEADER_VERSION,
            kind: SegmentKind::Cold,
            segment_id,
            segment_size,
            created_at_epoch: now_epoch(),
            codec,
            checksum: 0,
            last_valid_offset: UNIFIED_HEADER_SIZE,
            logical_range: LogicalRange::Cold {
                table_id,
                shard_id,
                min_key: Vec::new(),
                max_key: Vec::new(),
            },
        };
        h.checksum = h.compute_checksum();
        h
    }

    /// Create a new snapshot segment header.
    pub fn new_snapshot(
        segment_id: u64,
        segment_size: u64,
        snapshot_id: u64,
        chunk_index: u32,
    ) -> Self {
        let mut h = Self {
            magic: UNIFIED_SEGMENT_MAGIC,
            version: UNIFIED_HEADER_VERSION,
            kind: SegmentKind::Snapshot,
            segment_id,
            segment_size,
            created_at_epoch: now_epoch(),
            codec: SegmentCodec::None,
            checksum: 0,
            last_valid_offset: UNIFIED_HEADER_SIZE,
            logical_range: LogicalRange::Snapshot {
                snapshot_id,
                chunk_index,
            },
        };
        h.checksum = h.compute_checksum();
        h
    }

    /// Compute CRC32 of header fields.
    pub fn compute_checksum(&self) -> u32 {
        let mut hash: u32 = 5381;
        for b in self.magic.to_le_bytes() { hash = hash.wrapping_mul(33).wrapping_add(u32::from(b)); }
        for b in self.version.to_le_bytes() { hash = hash.wrapping_mul(33).wrapping_add(u32::from(b)); }
        hash = hash.wrapping_mul(33).wrapping_add(self.kind as u32);
        for b in self.segment_id.to_le_bytes() { hash = hash.wrapping_mul(33).wrapping_add(u32::from(b)); }
        for b in self.segment_size.to_le_bytes() { hash = hash.wrapping_mul(33).wrapping_add(u32::from(b)); }
        for b in u32::from(self.codec as u8).to_le_bytes() { hash = hash.wrapping_mul(33).wrapping_add(u32::from(b)); }
        hash
    }

    /// Validate header integrity.
    pub fn validate(&self) -> bool {
        self.magic == UNIFIED_SEGMENT_MAGIC
            && self.version == UNIFIED_HEADER_VERSION
            && self.checksum == self.compute_checksum()
    }

    /// Serialize header to fixed-size buffer (simplified — stores key fields).
    /// In production this would be a proper 4K page. Here we serialize compactly
    /// then zero-pad to 4K.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(UNIFIED_HEADER_SIZE as usize);
        buf.extend_from_slice(&self.magic.to_le_bytes());
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf.push(self.kind as u8);
        buf.extend_from_slice(&self.segment_id.to_le_bytes());
        buf.extend_from_slice(&self.segment_size.to_le_bytes());
        buf.extend_from_slice(&self.created_at_epoch.to_le_bytes());
        buf.push(self.codec as u8);
        buf.extend_from_slice(&self.checksum.to_le_bytes());
        buf.extend_from_slice(&self.last_valid_offset.to_le_bytes());
        // Logical range tag + data
        match &self.logical_range {
            LogicalRange::Wal { start_lsn, end_lsn } => {
                buf.push(0); // tag
                buf.extend_from_slice(&start_lsn.raw().to_le_bytes());
                buf.extend_from_slice(&end_lsn.raw().to_le_bytes());
            }
            LogicalRange::Cold { table_id, shard_id, min_key, max_key } => {
                buf.push(1);
                buf.extend_from_slice(&table_id.to_le_bytes());
                buf.extend_from_slice(&shard_id.to_le_bytes());
                buf.extend_from_slice(&(min_key.len() as u32).to_le_bytes());
                buf.extend_from_slice(min_key);
                buf.extend_from_slice(&(max_key.len() as u32).to_le_bytes());
                buf.extend_from_slice(max_key);
            }
            LogicalRange::Snapshot { snapshot_id, chunk_index } => {
                buf.push(2);
                buf.extend_from_slice(&snapshot_id.to_le_bytes());
                buf.extend_from_slice(&chunk_index.to_le_bytes());
            }
        }
        // Pad to 4K
        buf.resize(UNIFIED_HEADER_SIZE as usize, 0);
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 48 { return None; }
        let magic = u32::from_le_bytes(data[0..4].try_into().ok()?);
        let version = u16::from_le_bytes(data[4..6].try_into().ok()?);
        let kind = SegmentKind::from_u8(data[6])?;
        let segment_id = u64::from_le_bytes(data[7..15].try_into().ok()?);
        let segment_size = u64::from_le_bytes(data[15..23].try_into().ok()?);
        let created_at_epoch = u64::from_le_bytes(data[23..31].try_into().ok()?);
        let codec = SegmentCodec::from_u8(data[31])?;
        let checksum = u32::from_le_bytes(data[32..36].try_into().ok()?);
        let last_valid_offset = u64::from_le_bytes(data[36..44].try_into().ok()?);
        let range_tag = data[44];
        let logical_range = match range_tag {
            0 => {
                if data.len() < 61 { return None; }
                let s = StructuredLsn::from_raw(u64::from_le_bytes(data[45..53].try_into().ok()?));
                let e = StructuredLsn::from_raw(u64::from_le_bytes(data[53..61].try_into().ok()?));
                LogicalRange::Wal { start_lsn: s, end_lsn: e }
            }
            1 => {
                if data.len() < 69 { return None; }
                let table_id = u64::from_le_bytes(data[45..53].try_into().ok()?);
                let shard_id = u64::from_le_bytes(data[53..61].try_into().ok()?);
                let mk_len = u32::from_le_bytes(data[61..65].try_into().ok()?) as usize;
                if data.len() < 69 + mk_len { return None; }
                let min_key = data[65..65+mk_len].to_vec();
                let off = 65 + mk_len;
                if data.len() < off + 4 { return None; }
                let xk_len = u32::from_le_bytes(data[off..off+4].try_into().ok()?) as usize;
                if data.len() < off + 4 + xk_len { return None; }
                let max_key = data[off+4..off+4+xk_len].to_vec();
                LogicalRange::Cold { table_id, shard_id, min_key, max_key }
            }
            2 => {
                if data.len() < 57 { return None; }
                let snapshot_id = u64::from_le_bytes(data[45..53].try_into().ok()?);
                let chunk_index = u32::from_le_bytes(data[53..57].try_into().ok()?);
                LogicalRange::Snapshot { snapshot_id, chunk_index }
            }
            _ => return None,
        };
        Some(Self {
            magic, version, kind, segment_id, segment_size,
            created_at_epoch, codec, checksum, last_valid_offset,
            logical_range,
        })
    }
}

fn now_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ═══════════════════════════════════════════════════════════════════════════
// §A3 — Manifest (append-only, epoch-versioned)
// ═══════════════════════════════════════════════════════════════════════════

/// A manifest entry describing one segment in the system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    pub segment_id: u64,
    pub kind: SegmentKind,
    pub size_bytes: u64,
    pub codec: SegmentCodec,
    pub logical_range: LogicalRange,
    /// Whether this segment is sealed (immutable).
    pub sealed: bool,
}

/// The Manifest: describes the current database state as a set of segments.
///
/// - Append-only: entries are added, never removed in-place (GC creates new epoch)
/// - Epoch-versioned: every mutation increments the epoch
/// - Checksummed: can be verified
/// - Portable: replicated alongside segments
#[derive(Debug, Clone)]
pub struct Manifest {
    /// Monotonically increasing epoch.
    pub epoch: u64,
    /// All segment entries, keyed by segment_id.
    pub segments: BTreeMap<u64, ManifestEntry>,
    /// Snapshot cutpoint (if any).
    pub snapshot_cutpoint: Option<SnapshotCutpoint>,
    /// History of epoch transitions (for audit / rollback).
    pub history: Vec<ManifestDelta>,
}

/// A snapshot cutpoint within the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotCutpoint {
    pub snapshot_id: u64,
    pub cut_lsn: StructuredLsn,
    pub cut_csn: Csn,
    pub epoch: u64,
}

/// A delta applied to the manifest (for replication / audit).
#[derive(Debug, Clone)]
pub struct ManifestDelta {
    pub from_epoch: u64,
    pub to_epoch: u64,
    pub added_segments: Vec<u64>,
    pub removed_segments: Vec<u64>,
    pub new_cutpoint: Option<SnapshotCutpoint>,
}

impl Manifest {
    /// Create an empty manifest at epoch 0.
    pub const fn new() -> Self {
        Self {
            epoch: 0,
            segments: BTreeMap::new(),
            snapshot_cutpoint: None,
            history: Vec::new(),
        }
    }

    /// Add a segment. Increments epoch.
    pub fn add_segment(&mut self, entry: ManifestEntry) {
        let old_epoch = self.epoch;
        self.epoch += 1;
        let seg_id = entry.segment_id;
        self.segments.insert(seg_id, entry);
        self.history.push(ManifestDelta {
            from_epoch: old_epoch,
            to_epoch: self.epoch,
            added_segments: vec![seg_id],
            removed_segments: vec![],
            new_cutpoint: None,
        });
    }

    /// Seal a segment. Increments epoch.
    pub fn seal_segment(&mut self, segment_id: u64) -> bool {
        if let Some(entry) = self.segments.get_mut(&segment_id) {
            if !entry.sealed {
                entry.sealed = true;
                self.epoch += 1;
                return true;
            }
        }
        false
    }

    /// Remove segments (GC). Increments epoch.
    pub fn remove_segments(&mut self, segment_ids: &[u64]) -> usize {
        let old_epoch = self.epoch;
        let mut removed = Vec::new();
        for &id in segment_ids {
            if self.segments.remove(&id).is_some() {
                removed.push(id);
            }
        }
        if !removed.is_empty() {
            self.epoch += 1;
            self.history.push(ManifestDelta {
                from_epoch: old_epoch,
                to_epoch: self.epoch,
                added_segments: vec![],
                removed_segments: removed.clone(),
                new_cutpoint: None,
            });
        }
        removed.len()
    }

    /// Set a snapshot cutpoint. Increments epoch.
    pub fn set_snapshot_cutpoint(&mut self, cutpoint: SnapshotCutpoint) {
        let old_epoch = self.epoch;
        self.epoch += 1;
        self.history.push(ManifestDelta {
            from_epoch: old_epoch,
            to_epoch: self.epoch,
            added_segments: vec![],
            removed_segments: vec![],
            new_cutpoint: Some(cutpoint.clone()),
        });
        self.snapshot_cutpoint = Some(cutpoint);
    }

    /// Get all segments of a given kind.
    pub fn segments_by_kind(&self, kind: SegmentKind) -> Vec<&ManifestEntry> {
        self.segments.values().filter(|e| e.kind == kind).collect()
    }

    /// Get all sealed segment IDs.
    pub fn sealed_segment_ids(&self) -> BTreeSet<u64> {
        self.segments.iter()
            .filter(|(_, e)| e.sealed)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get all segment IDs.
    pub fn all_segment_ids(&self) -> BTreeSet<u64> {
        self.segments.keys().copied().collect()
    }

    /// Total bytes across all segments.
    pub fn total_bytes(&self) -> u64 {
        self.segments.values().map(|e| e.size_bytes).sum()
    }

    /// Compute delta from an older epoch.
    pub fn delta_since(&self, from_epoch: u64) -> Vec<&ManifestDelta> {
        self.history.iter().filter(|d| d.from_epoch >= from_epoch).collect()
    }

    /// Compute which segments a peer is missing given their segment set.
    pub fn missing_segments(&self, have: &BTreeSet<u64>) -> Vec<u64> {
        self.segments.keys()
            .filter(|id| !have.contains(id))
            .copied()
            .collect()
    }

    /// Serialize manifest to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.epoch.to_le_bytes());
        buf.extend_from_slice(&(self.segments.len() as u32).to_le_bytes());
        for (id, entry) in &self.segments {
            buf.extend_from_slice(&id.to_le_bytes());
            buf.push(entry.kind as u8);
            buf.extend_from_slice(&entry.size_bytes.to_le_bytes());
            buf.push(entry.codec as u8);
            buf.push(if entry.sealed { 1 } else { 0 });
        }
        // Cutpoint
        match &self.snapshot_cutpoint {
            Some(cp) => {
                buf.push(1);
                buf.extend_from_slice(&cp.snapshot_id.to_le_bytes());
                buf.extend_from_slice(&cp.cut_lsn.raw().to_le_bytes());
                buf.extend_from_slice(&cp.cut_csn.raw().to_le_bytes());
                buf.extend_from_slice(&cp.epoch.to_le_bytes());
            }
            None => buf.push(0),
        }
        buf
    }
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §B1 — Segment Store (unified IO substrate)
// ═══════════════════════════════════════════════════════════════════════════

/// Errors from the segment store.
#[derive(Debug, Clone)]
pub enum SegmentStoreError {
    NotFound { segment_id: u64 },
    AlreadyExists { segment_id: u64 },
    ReadOutOfBounds { segment_id: u64, offset: u64, len: u64 },
    AlreadySealed { segment_id: u64 },
    ChecksumMismatch { segment_id: u64 },
    IoError(String),
}

impl fmt::Display for SegmentStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { segment_id } => write!(f, "segment {segment_id} not found"),
            Self::AlreadyExists { segment_id } => write!(f, "segment {segment_id} already exists"),
            Self::ReadOutOfBounds { segment_id, offset, len } =>
                write!(f, "read out of bounds: seg={segment_id} off={offset} len={len}"),
            Self::AlreadySealed { segment_id } => write!(f, "segment {segment_id} already sealed"),
            Self::ChecksumMismatch { segment_id } => write!(f, "checksum mismatch: segment {segment_id}"),
            Self::IoError(msg) => write!(f, "IO error: {msg}"),
        }
    }
}

/// In-memory segment backing (for testing and in-process usage).
struct SegmentData {
    header: UnifiedSegmentHeader,
    body: Vec<u8>,
    sealed: bool,
}

/// The unified Segment Store — provides open/create/read/write/seal/verify
/// for all segment kinds.
///
/// Backend-agnostic: default implementation is in-memory.
/// In production, backed by file system, Windows async, or raw disk.
pub struct SegmentStore {
    segments: RwLock<HashMap<u64, SegmentData>>,
    next_id: AtomicU64,
    pub metrics: SegmentStoreMetrics,
}

impl SegmentStore {
    pub fn new() -> Self {
        Self {
            segments: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            metrics: SegmentStoreMetrics::default(),
        }
    }

    /// Allocate the next segment ID.
    pub fn next_segment_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Create a new segment with the given header.
    pub fn create_segment(&self, header: UnifiedSegmentHeader) -> Result<u64, SegmentStoreError> {
        let id = header.segment_id;
        let mut segs = self.segments.write();
        if segs.contains_key(&id) {
            return Err(SegmentStoreError::AlreadyExists { segment_id: id });
        }
        let body = header.to_bytes(); // starts with header bytes
        segs.insert(id, SegmentData {
            header,
            body,
            sealed: false,
        });
        self.metrics.segments_created.fetch_add(1, Ordering::Relaxed);
        self.metrics.store_bytes_total.fetch_add(UNIFIED_HEADER_SIZE, Ordering::Relaxed);
        Ok(id)
    }

    /// Open (get header of) an existing segment.
    pub fn open_segment(&self, segment_id: u64) -> Result<UnifiedSegmentHeader, SegmentStoreError> {
        let segs = self.segments.read();
        segs.get(&segment_id)
            .map(|s| s.header.clone())
            .ok_or(SegmentStoreError::NotFound { segment_id })
    }

    /// Read a chunk from a segment.
    pub fn read_chunk(&self, segment_id: u64, offset: u64, len: u64) -> Result<Vec<u8>, SegmentStoreError> {
        let segs = self.segments.read();
        let seg = segs.get(&segment_id)
            .ok_or(SegmentStoreError::NotFound { segment_id })?;
        let start = offset as usize;
        let end = start + len as usize;
        if end > seg.body.len() {
            return Err(SegmentStoreError::ReadOutOfBounds { segment_id, offset, len });
        }
        self.metrics.read_bytes_total.fetch_add(len, Ordering::Relaxed);
        Ok(seg.body[start..end].to_vec())
    }

    /// Append a chunk to a segment (WAL segments only — append at end).
    pub fn write_chunk(&self, segment_id: u64, data: &[u8]) -> Result<u64, SegmentStoreError> {
        let mut segs = self.segments.write();
        let seg = segs.get_mut(&segment_id)
            .ok_or(SegmentStoreError::NotFound { segment_id })?;
        if seg.sealed {
            return Err(SegmentStoreError::AlreadySealed { segment_id });
        }
        let offset = seg.body.len() as u64;
        seg.body.extend_from_slice(data);
        seg.header.last_valid_offset = seg.body.len() as u64;
        self.metrics.write_bytes_total.fetch_add(data.len() as u64, Ordering::Relaxed);
        self.metrics.store_bytes_total.fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(offset)
    }

    /// Write a chunk at a specific offset (for receiving streamed data).
    pub fn write_chunk_at(&self, segment_id: u64, offset: u64, data: &[u8]) -> Result<(), SegmentStoreError> {
        let mut segs = self.segments.write();
        let seg = segs.get_mut(&segment_id)
            .ok_or(SegmentStoreError::NotFound { segment_id })?;
        if seg.sealed {
            return Err(SegmentStoreError::AlreadySealed { segment_id });
        }
        let end = offset as usize + data.len();
        if end > seg.body.len() {
            seg.body.resize(end, 0);
        }
        seg.body[offset as usize..end].copy_from_slice(data);
        if end as u64 > seg.header.last_valid_offset {
            seg.header.last_valid_offset = end as u64;
        }
        self.metrics.write_bytes_total.fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Seal a segment (mark immutable, set last_valid_offset).
    pub fn seal_segment(&self, segment_id: u64) -> Result<(), SegmentStoreError> {
        let mut segs = self.segments.write();
        let seg = segs.get_mut(&segment_id)
            .ok_or(SegmentStoreError::NotFound { segment_id })?;
        if seg.sealed {
            return Err(SegmentStoreError::AlreadySealed { segment_id });
        }
        seg.sealed = true;
        seg.header.last_valid_offset = seg.body.len() as u64;
        seg.header.checksum = seg.header.compute_checksum();
        self.metrics.segments_sealed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Verify a segment's header checksum.
    pub fn verify_segment(&self, segment_id: u64) -> Result<bool, SegmentStoreError> {
        let segs = self.segments.read();
        let seg = segs.get(&segment_id)
            .ok_or(SegmentStoreError::NotFound { segment_id })?;
        Ok(seg.header.validate())
    }

    /// Check if a segment is sealed.
    pub fn is_sealed(&self, segment_id: u64) -> Result<bool, SegmentStoreError> {
        let segs = self.segments.read();
        let seg = segs.get(&segment_id)
            .ok_or(SegmentStoreError::NotFound { segment_id })?;
        Ok(seg.sealed)
    }

    /// Get the body size of a segment.
    pub fn segment_size(&self, segment_id: u64) -> Result<u64, SegmentStoreError> {
        let segs = self.segments.read();
        let seg = segs.get(&segment_id)
            .ok_or(SegmentStoreError::NotFound { segment_id })?;
        Ok(seg.body.len() as u64)
    }

    /// Delete a segment (GC).
    pub fn delete_segment(&self, segment_id: u64) -> Result<u64, SegmentStoreError> {
        let mut segs = self.segments.write();
        let seg = segs.remove(&segment_id)
            .ok_or(SegmentStoreError::NotFound { segment_id })?;
        let freed = seg.body.len() as u64;
        self.metrics.gc_bytes_total.fetch_add(freed, Ordering::Relaxed);
        self.metrics.segments_deleted.fetch_add(1, Ordering::Relaxed);
        Ok(freed)
    }

    /// List all segment IDs.
    pub fn list_segments(&self) -> Vec<u64> {
        self.segments.read().keys().copied().collect()
    }

    /// Check if a segment exists.
    pub fn exists(&self, segment_id: u64) -> bool {
        self.segments.read().contains_key(&segment_id)
    }

    /// Get the full body of a segment (for streaming).
    pub fn get_segment_body(&self, segment_id: u64) -> Result<Vec<u8>, SegmentStoreError> {
        let segs = self.segments.read();
        let seg = segs.get(&segment_id)
            .ok_or(SegmentStoreError::NotFound { segment_id })?;
        Ok(seg.body.clone())
    }
}

impl Default for SegmentStore {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §B2 — Segment Streaming Protocol
// ═══════════════════════════════════════════════════════════════════════════

/// Stream begin message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamBegin {
    pub manifest_epoch: u64,
    pub stream_id: u64,
    pub protocol_version: u8,
}

/// Segment header within a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSegmentHeader {
    pub segment_id: u64,
    pub kind: SegmentKind,
    pub size: u64,
    pub codec: SegmentCodec,
    pub checksum: u32,
}

/// A chunk within a stream.
#[derive(Debug, Clone)]
pub struct StreamChunk {
    pub segment_id: u64,
    pub chunk_index: u32,
    pub data: Vec<u8>,
    pub crc: u32,
    pub is_last: bool,
}

impl StreamChunk {
    pub fn compute_crc(data: &[u8]) -> u32 {
        let mut hash: u32 = 5381;
        for &b in data { hash = hash.wrapping_mul(33).wrapping_add(u32::from(b)); }
        hash
    }

    pub fn verify(&self) -> bool {
        self.crc == Self::compute_crc(&self.data)
    }
}

/// Stream end message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEnd {
    pub stream_id: u64,
    pub segments_transferred: u32,
    pub bytes_transferred: u64,
}

/// Resume point for interrupted streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamResumePoint {
    pub stream_id: u64,
    pub last_segment_id: u64,
    pub last_chunk_index: u32,
}

// ═══════════════════════════════════════════════════════════════════════════
// §C — Replication Handshake (Manifest + Segments)
// ═══════════════════════════════════════════════════════════════════════════

/// Follower → Leader handshake.
#[derive(Debug, Clone)]
pub struct ReplicationHandshake {
    pub node_id: u64,
    pub last_manifest_epoch: u64,
    pub have_segments: BTreeSet<u64>,
    pub protocol_version: u8,
}

/// Leader → Follower response.
#[derive(Debug, Clone)]
pub struct ReplicationResponse {
    /// Full manifest or delta.
    pub manifest_bytes: Vec<u8>,
    pub manifest_epoch: u64,
    /// Segments the follower needs to fetch.
    pub required_segments: Vec<u64>,
    /// Whether to use tail streaming for the active WAL segment.
    pub tail_streaming: Option<TailStreamInfo>,
}

/// Info for tail streaming the active WAL segment.
#[derive(Debug, Clone)]
pub struct TailStreamInfo {
    pub segment_id: u64,
    pub from_offset: u64,
}

/// Decide what a follower needs given the leader's manifest and the follower's state.
pub fn compute_replication_plan(
    leader_manifest: &Manifest,
    handshake: &ReplicationHandshake,
) -> ReplicationResponse {
    let required = leader_manifest.missing_segments(&handshake.have_segments);

    // Determine if tail streaming is needed for the active WAL segment
    let tail = leader_manifest.segments.values()
        .find(|e| e.kind == SegmentKind::Wal && !e.sealed)
        .map(|e| TailStreamInfo {
            segment_id: e.segment_id,
            from_offset: UNIFIED_HEADER_SIZE, // follower starts from header
        });

    ReplicationResponse {
        manifest_bytes: leader_manifest.to_bytes(),
        manifest_epoch: leader_manifest.epoch,
        required_segments: required,
        tail_streaming: tail,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §D — Snapshot = Manifest Cut + Segment Set
// ═══════════════════════════════════════════════════════════════════════════

/// A snapshot: manifest epoch + cutpoint + the set of segment IDs that
/// constitute a consistent point-in-time image.
#[derive(Debug, Clone)]
pub struct SnapshotDefinition {
    pub snapshot_id: u64,
    pub manifest_epoch: u64,
    pub cutpoint: SnapshotCutpoint,
    /// WAL segment IDs included (sealed, up to cut_lsn).
    pub wal_segments: Vec<u64>,
    /// Cold segment IDs included.
    pub cold_segments: Vec<u64>,
    /// Snapshot-specific segments (if any extra chunks were created).
    pub snapshot_segments: Vec<u64>,
}

impl SnapshotDefinition {
    /// Create a snapshot from the current manifest state.
    pub fn create(snapshot_id: u64, manifest: &Manifest, cut_lsn: StructuredLsn, cut_csn: Csn) -> Self {
        let cutpoint = SnapshotCutpoint {
            snapshot_id,
            cut_lsn,
            cut_csn,
            epoch: manifest.epoch,
        };
        let wal_segments: Vec<u64> = manifest.segments.iter()
            .filter(|(_, e)| e.kind == SegmentKind::Wal && e.sealed)
            .map(|(id, _)| *id)
            .collect();
        let cold_segments: Vec<u64> = manifest.segments.iter()
            .filter(|(_, e)| e.kind == SegmentKind::Cold)
            .map(|(id, _)| *id)
            .collect();
        let snapshot_segments: Vec<u64> = manifest.segments.iter()
            .filter(|(_, e)| e.kind == SegmentKind::Snapshot)
            .map(|(id, _)| *id)
            .collect();
        Self {
            snapshot_id,
            manifest_epoch: manifest.epoch,
            cutpoint,
            wal_segments,
            cold_segments,
            snapshot_segments,
        }
    }

    /// All segment IDs in this snapshot.
    pub fn all_segments(&self) -> Vec<u64> {
        let mut all = Vec::new();
        all.extend(&self.wal_segments);
        all.extend(&self.cold_segments);
        all.extend(&self.snapshot_segments);
        all
    }

    /// Segments a follower needs from this snapshot.
    pub fn missing_for(&self, have: &BTreeSet<u64>) -> Vec<u64> {
        self.all_segments().into_iter().filter(|id| !have.contains(id)).collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §E — Cold Store as COLD_SEGMENT
// ═══════════════════════════════════════════════════════════════════════════

/// A cold segment descriptor — managed by the compactor.
#[derive(Debug, Clone)]
pub struct ColdSegmentDescriptor {
    pub segment_id: u64,
    pub table_id: u64,
    pub shard_id: u64,
    pub codec: SegmentCodec,
    pub original_bytes: u64,
    pub compressed_bytes: u64,
    pub row_count: u64,
}

impl ColdSegmentDescriptor {
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_bytes == 0 { return 1.0; }
        self.original_bytes as f64 / self.compressed_bytes as f64
    }
}

/// A handle from hot data to a cold segment offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UnifiedColdHandle {
    pub segment_id: u64,
    pub offset: u64,
    pub len: u32,
}

// ═══════════════════════════════════════════════════════════════════════════
// §F — Unified Recovery / Bootstrap
// ═══════════════════════════════════════════════════════════════════════════

/// Recovery phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPhase {
    LoadManifest,
    VerifySegments,
    FetchMissing,
    ReplayWalTail,
    Ready,
    Failed,
}

impl fmt::Display for RecoveryPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LoadManifest => write!(f, "LOAD_MANIFEST"),
            Self::VerifySegments => write!(f, "VERIFY_SEGMENTS"),
            Self::FetchMissing => write!(f, "FETCH_MISSING"),
            Self::ReplayWalTail => write!(f, "REPLAY_WAL_TAIL"),
            Self::Ready => write!(f, "READY"),
            Self::Failed => write!(f, "FAILED"),
        }
    }
}

/// Unified recovery coordinator.
///
/// Same path for: new node bootstrap, crash recovery, snapshot restore, catch-up.
pub struct RecoveryCoordinator {
    pub phase: RecoveryPhase,
    pub manifest: Manifest,
    pub verified_segments: BTreeSet<u64>,
    pub missing_segments: Vec<u64>,
    pub fetched_segments: BTreeSet<u64>,
}

impl RecoveryCoordinator {
    /// Start recovery from a manifest.
    pub const fn new(manifest: Manifest) -> Self {
        Self {
            phase: RecoveryPhase::LoadManifest,
            manifest,
            verified_segments: BTreeSet::new(),
            missing_segments: Vec::new(),
            fetched_segments: BTreeSet::new(),
        }
    }

    /// Start recovery from empty (bootstrap from leader).
    pub const fn new_empty() -> Self {
        Self::new(Manifest::new())
    }

    /// Phase 1: Load manifest (already done in constructor). Advance to verify.
    pub const fn advance_to_verify(&mut self) {
        self.phase = RecoveryPhase::VerifySegments;
    }

    /// Phase 2: Verify which segments exist in the store.
    pub fn verify_segments(&mut self, store: &SegmentStore) {
        self.verified_segments.clear();
        self.missing_segments.clear();
        for seg_id in self.manifest.all_segment_ids() {
            if store.exists(seg_id) {
                match store.verify_segment(seg_id) {
                    Ok(true) => { self.verified_segments.insert(seg_id); }
                    _ => { self.missing_segments.push(seg_id); }
                }
            } else {
                self.missing_segments.push(seg_id);
            }
        }
        if self.missing_segments.is_empty() {
            self.phase = RecoveryPhase::ReplayWalTail;
        } else {
            self.phase = RecoveryPhase::FetchMissing;
        }
    }

    /// Phase 3: After fetching missing segments, re-verify.
    pub fn mark_fetched(&mut self, segment_id: u64) {
        self.fetched_segments.insert(segment_id);
        self.missing_segments.retain(|&id| id != segment_id);
        if self.missing_segments.is_empty() {
            self.phase = RecoveryPhase::ReplayWalTail;
        }
    }

    /// Phase 4: After WAL tail replay, ready to serve.
    pub const fn mark_ready(&mut self) {
        self.phase = RecoveryPhase::Ready;
    }

    /// Mark as failed.
    pub const fn mark_failed(&mut self) {
        self.phase = RecoveryPhase::Failed;
    }

    /// Is recovery complete?
    pub fn is_ready(&self) -> bool {
        self.phase == RecoveryPhase::Ready
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §G — Segment GC
// ═══════════════════════════════════════════════════════════════════════════

/// GC decision for a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcDecision {
    /// Keep — still referenced.
    Keep,
    /// Eligible for deletion.
    Eligible,
    /// Currently being streamed — defer.
    DeferStreaming,
}

/// Segment GC coordinator.
pub struct SegmentGc {
    /// Segments currently being streamed (protected from GC).
    streaming_segments: Mutex<HashSet<u64>>,
    /// WAL retention: all follower durable LSNs must exceed before GC.
    min_durable_lsn: AtomicU64,
    pub metrics: GcMetrics,
}

/// GC metrics.
#[derive(Debug, Default)]
pub struct GcMetrics {
    pub segments_evaluated: AtomicU64,
    pub segments_deleted: AtomicU64,
    pub bytes_freed: AtomicU64,
    pub deferred_streaming: AtomicU64,
}

impl SegmentGc {
    pub fn new() -> Self {
        Self {
            streaming_segments: Mutex::new(HashSet::new()),
            min_durable_lsn: AtomicU64::new(0),
            metrics: GcMetrics::default(),
        }
    }

    /// Mark a segment as being streamed (protect from GC).
    pub fn mark_streaming(&self, segment_id: u64) {
        self.streaming_segments.lock().insert(segment_id);
    }

    /// Unmark a segment as streaming.
    pub fn unmark_streaming(&self, segment_id: u64) {
        self.streaming_segments.lock().remove(&segment_id);
    }

    /// Update the minimum durable LSN across all followers.
    pub fn update_min_durable_lsn(&self, lsn: StructuredLsn) {
        self.min_durable_lsn.store(lsn.raw(), Ordering::Relaxed);
    }

    /// Evaluate whether a segment can be GC'd.
    pub fn evaluate(&self, entry: &ManifestEntry, manifest: &Manifest) -> GcDecision {
        self.metrics.segments_evaluated.fetch_add(1, Ordering::Relaxed);

        // Check if currently streaming
        if self.streaming_segments.lock().contains(&entry.segment_id) {
            self.metrics.deferred_streaming.fetch_add(1, Ordering::Relaxed);
            return GcDecision::DeferStreaming;
        }

        // Must be sealed
        if !entry.sealed {
            return GcDecision::Keep;
        }

        match entry.kind {
            SegmentKind::Wal => {
                // WAL: can GC if all followers past this segment's LSN range
                if let LogicalRange::Wal { end_lsn, .. } = &entry.logical_range {
                    let min_durable = StructuredLsn::from_raw(
                        self.min_durable_lsn.load(Ordering::Relaxed)
                    );
                    if *end_lsn <= min_durable {
                        return GcDecision::Eligible;
                    }
                }
                GcDecision::Keep
            }
            SegmentKind::Cold => {
                // Cold: eligible only if replaced in manifest (not present)
                if !manifest.segments.contains_key(&entry.segment_id) {
                    GcDecision::Eligible
                } else {
                    GcDecision::Keep
                }
            }
            SegmentKind::Snapshot => {
                // Snapshot: eligible if past the current snapshot cutpoint
                match (&manifest.snapshot_cutpoint, &entry.logical_range) {
                    (Some(cp), LogicalRange::Snapshot { snapshot_id, .. }) => {
                        if *snapshot_id < cp.snapshot_id {
                            GcDecision::Eligible
                        } else {
                            GcDecision::Keep
                        }
                    }
                    _ => GcDecision::Keep,
                }
            }
        }
    }

    /// Run a GC pass: evaluate all segments, return eligible IDs.
    pub fn gc_pass(&self, manifest: &Manifest) -> Vec<u64> {
        let mut eligible = Vec::new();
        for entry in manifest.segments.values() {
            if self.evaluate(entry, manifest) == GcDecision::Eligible {
                eligible.push(entry.segment_id);
            }
        }
        eligible
    }

    /// Execute GC: delete eligible segments from the store and manifest.
    pub fn execute_gc(
        &self,
        manifest: &mut Manifest,
        store: &SegmentStore,
    ) -> (u64, u64) {
        let eligible = self.gc_pass(manifest);
        let mut freed_bytes = 0u64;
        let mut freed_count = 0u64;
        for seg_id in &eligible {
            if let Ok(bytes) = store.delete_segment(*seg_id) {
                freed_bytes += bytes;
                freed_count += 1;
            }
        }
        if !eligible.is_empty() {
            manifest.remove_segments(&eligible);
        }
        self.metrics.segments_deleted.fetch_add(freed_count, Ordering::Relaxed);
        self.metrics.bytes_freed.fetch_add(freed_bytes, Ordering::Relaxed);
        (freed_count, freed_bytes)
    }
}

impl Default for SegmentGc {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §H — Metrics
// ═══════════════════════════════════════════════════════════════════════════

/// Segment store metrics.
#[derive(Debug, Default)]
pub struct SegmentStoreMetrics {
    pub segments_created: AtomicU64,
    pub segments_sealed: AtomicU64,
    pub segments_deleted: AtomicU64,
    pub store_bytes_total: AtomicU64,
    pub read_bytes_total: AtomicU64,
    pub write_bytes_total: AtomicU64,
    pub gc_bytes_total: AtomicU64,
    pub stream_bytes_total: AtomicU64,
    pub stream_resume_total: AtomicU64,
}

impl SegmentStoreMetrics {
    pub fn snapshot(&self) -> SegmentStoreMetricsSnapshot {
        SegmentStoreMetricsSnapshot {
            segments_created: self.segments_created.load(Ordering::Relaxed),
            segments_sealed: self.segments_sealed.load(Ordering::Relaxed),
            segments_deleted: self.segments_deleted.load(Ordering::Relaxed),
            store_bytes_total: self.store_bytes_total.load(Ordering::Relaxed),
            read_bytes_total: self.read_bytes_total.load(Ordering::Relaxed),
            write_bytes_total: self.write_bytes_total.load(Ordering::Relaxed),
            gc_bytes_total: self.gc_bytes_total.load(Ordering::Relaxed),
            stream_bytes_total: self.stream_bytes_total.load(Ordering::Relaxed),
            stream_resume_total: self.stream_resume_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SegmentStoreMetricsSnapshot {
    pub segments_created: u64,
    pub segments_sealed: u64,
    pub segments_deleted: u64,
    pub store_bytes_total: u64,
    pub read_bytes_total: u64,
    pub write_bytes_total: u64,
    pub gc_bytes_total: u64,
    pub stream_bytes_total: u64,
    pub stream_resume_total: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// §I — Unit Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // -- Segment Header --

    #[test]
    fn test_wal_header_roundtrip() {
        let h = UnifiedSegmentHeader::new_wal(5, 256 * 1024 * 1024, StructuredLsn::new(5, 0));
        assert!(h.validate());
        let bytes = h.to_bytes();
        assert_eq!(bytes.len(), UNIFIED_HEADER_SIZE as usize);
        let recovered = UnifiedSegmentHeader::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.segment_id, 5);
        assert_eq!(recovered.kind, SegmentKind::Wal);
        assert!(recovered.validate());
    }

    #[test]
    fn test_cold_header_roundtrip() {
        let h = UnifiedSegmentHeader::new_cold(10, 64 * 1024 * 1024, SegmentCodec::Lz4, 1, 0);
        assert!(h.validate());
        let bytes = h.to_bytes();
        let recovered = UnifiedSegmentHeader::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.kind, SegmentKind::Cold);
        assert_eq!(recovered.codec, SegmentCodec::Lz4);
    }

    #[test]
    fn test_snapshot_header_roundtrip() {
        let h = UnifiedSegmentHeader::new_snapshot(20, 1024 * 1024, 1, 0);
        assert!(h.validate());
        let bytes = h.to_bytes();
        let recovered = UnifiedSegmentHeader::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.kind, SegmentKind::Snapshot);
        if let LogicalRange::Snapshot { snapshot_id, chunk_index } = recovered.logical_range {
            assert_eq!(snapshot_id, 1);
            assert_eq!(chunk_index, 0);
        } else {
            panic!("expected snapshot range");
        }
    }

    // -- Manifest --

    #[test]
    fn test_manifest_add_and_seal() {
        let mut m = Manifest::new();
        assert_eq!(m.epoch, 0);

        m.add_segment(ManifestEntry {
            segment_id: 0,
            kind: SegmentKind::Wal,
            size_bytes: 1000,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(0, 0),
                end_lsn: StructuredLsn::new(0, 1000),
            },
            sealed: false,
        });
        assert_eq!(m.epoch, 1);
        assert_eq!(m.segments.len(), 1);

        assert!(m.seal_segment(0));
        assert_eq!(m.epoch, 2);
        assert!(m.segments[&0].sealed);
    }

    #[test]
    fn test_manifest_remove_and_gc() {
        let mut m = Manifest::new();
        for i in 0..5 {
            m.add_segment(ManifestEntry {
                segment_id: i,
                kind: SegmentKind::Wal,
                size_bytes: 100,
                codec: SegmentCodec::None,
                logical_range: LogicalRange::Wal {
                    start_lsn: StructuredLsn::ZERO,
                    end_lsn: StructuredLsn::ZERO,
                },
                sealed: true,
            });
        }
        assert_eq!(m.segments.len(), 5);
        let removed = m.remove_segments(&[0, 1, 2]);
        assert_eq!(removed, 3);
        assert_eq!(m.segments.len(), 2);
    }

    #[test]
    fn test_manifest_snapshot_cutpoint() {
        let mut m = Manifest::new();
        m.set_snapshot_cutpoint(SnapshotCutpoint {
            snapshot_id: 1,
            cut_lsn: StructuredLsn::new(3, 0),
            cut_csn: Csn::new(100),
            epoch: m.epoch,
        });
        assert!(m.snapshot_cutpoint.is_some());
        assert_eq!(m.snapshot_cutpoint.as_ref().unwrap().snapshot_id, 1);
    }

    #[test]
    fn test_manifest_missing_segments() {
        let mut m = Manifest::new();
        for i in 0..5 {
            m.add_segment(ManifestEntry {
                segment_id: i,
                kind: SegmentKind::Wal,
                size_bytes: 100,
                codec: SegmentCodec::None,
                logical_range: LogicalRange::Wal {
                    start_lsn: StructuredLsn::ZERO,
                    end_lsn: StructuredLsn::ZERO,
                },
                sealed: true,
            });
        }
        let have: BTreeSet<u64> = [0, 1, 3].iter().copied().collect();
        let missing = m.missing_segments(&have);
        assert_eq!(missing, vec![2, 4]);
    }

    #[test]
    fn test_manifest_serialization() {
        let mut m = Manifest::new();
        m.add_segment(ManifestEntry {
            segment_id: 0,
            kind: SegmentKind::Wal,
            size_bytes: 1000,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::ZERO,
                end_lsn: StructuredLsn::ZERO,
            },
            sealed: true,
        });
        let bytes = m.to_bytes();
        assert!(!bytes.is_empty());
        // Epoch should be encoded
        let epoch = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        assert_eq!(epoch, 1);
    }

    // -- Segment Store --

    #[test]
    fn test_segment_store_create_read_write() {
        let store = SegmentStore::new();
        let hdr = UnifiedSegmentHeader::new_wal(0, 1024 * 1024, StructuredLsn::new(0, 0));
        store.create_segment(hdr).unwrap();

        // Write data
        let offset = store.write_chunk(0, b"hello world").unwrap();
        assert_eq!(offset, UNIFIED_HEADER_SIZE); // after header

        // Read it back
        let data = store.read_chunk(0, offset, 11).unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn test_segment_store_seal() {
        let store = SegmentStore::new();
        let hdr = UnifiedSegmentHeader::new_wal(0, 1024, StructuredLsn::ZERO);
        store.create_segment(hdr).unwrap();

        store.write_chunk(0, b"data").unwrap();
        store.seal_segment(0).unwrap();

        // Cannot write after seal
        assert!(store.write_chunk(0, b"more").is_err());
        assert!(store.is_sealed(0).unwrap());
    }

    #[test]
    fn test_segment_store_verify() {
        let store = SegmentStore::new();
        let hdr = UnifiedSegmentHeader::new_cold(0, 1024, SegmentCodec::Lz4, 1, 0);
        store.create_segment(hdr).unwrap();
        store.seal_segment(0).unwrap();
        assert!(store.verify_segment(0).unwrap());
    }

    #[test]
    fn test_segment_store_delete() {
        let store = SegmentStore::new();
        let hdr = UnifiedSegmentHeader::new_wal(0, 1024, StructuredLsn::ZERO);
        store.create_segment(hdr).unwrap();
        store.write_chunk(0, &vec![0u8; 100]).unwrap();

        let freed = store.delete_segment(0).unwrap();
        assert!(freed > 0);
        assert!(!store.exists(0));
    }

    #[test]
    fn test_segment_store_not_found() {
        let store = SegmentStore::new();
        assert!(store.open_segment(999).is_err());
        assert!(store.read_chunk(999, 0, 10).is_err());
    }

    // -- Streaming Protocol --

    #[test]
    fn test_stream_chunk_verify() {
        let data = b"segment chunk data".to_vec();
        let crc = StreamChunk::compute_crc(&data);
        let chunk = StreamChunk {
            segment_id: 0,
            chunk_index: 0,
            data: data.clone(),
            crc,
            is_last: true,
        };
        assert!(chunk.verify());
    }

    #[test]
    fn test_stream_chunk_corrupt() {
        let chunk = StreamChunk {
            segment_id: 0,
            chunk_index: 0,
            data: b"data".to_vec(),
            crc: 0xDEAD,
            is_last: false,
        };
        assert!(!chunk.verify());
    }

    // -- Replication --

    #[test]
    fn test_replication_plan_empty_follower() {
        let mut m = Manifest::new();
        for i in 0..3 {
            m.add_segment(ManifestEntry {
                segment_id: i,
                kind: SegmentKind::Wal,
                size_bytes: 1000,
                codec: SegmentCodec::None,
                logical_range: LogicalRange::Wal {
                    start_lsn: StructuredLsn::new(i, 0),
                    end_lsn: StructuredLsn::new(i, 1000),
                },
                sealed: true,
            });
        }

        let handshake = ReplicationHandshake {
            node_id: 1,
            last_manifest_epoch: 0,
            have_segments: BTreeSet::new(),
            protocol_version: 1,
        };

        let resp = compute_replication_plan(&m, &handshake);
        assert_eq!(resp.required_segments.len(), 3);
        assert_eq!(resp.manifest_epoch, 3);
    }

    #[test]
    fn test_replication_plan_partial_follower() {
        let mut m = Manifest::new();
        for i in 0..5 {
            m.add_segment(ManifestEntry {
                segment_id: i,
                kind: SegmentKind::Wal,
                size_bytes: 1000,
                codec: SegmentCodec::None,
                logical_range: LogicalRange::Wal {
                    start_lsn: StructuredLsn::new(i, 0),
                    end_lsn: StructuredLsn::new(i, 1000),
                },
                sealed: true,
            });
        }

        let handshake = ReplicationHandshake {
            node_id: 2,
            last_manifest_epoch: 2,
            have_segments: [0, 1, 2].iter().copied().collect(),
            protocol_version: 1,
        };

        let resp = compute_replication_plan(&m, &handshake);
        assert_eq!(resp.required_segments, vec![3, 4]);
    }

    // -- Snapshot --

    #[test]
    fn test_snapshot_definition() {
        let mut m = Manifest::new();
        for i in 0..3 {
            m.add_segment(ManifestEntry {
                segment_id: i,
                kind: SegmentKind::Wal,
                size_bytes: 1000,
                codec: SegmentCodec::None,
                logical_range: LogicalRange::Wal {
                    start_lsn: StructuredLsn::new(i, 0),
                    end_lsn: StructuredLsn::new(i, 1000),
                },
                sealed: true,
            });
        }
        m.add_segment(ManifestEntry {
            segment_id: 100,
            kind: SegmentKind::Cold,
            size_bytes: 5000,
            codec: SegmentCodec::Lz4,
            logical_range: LogicalRange::Cold {
                table_id: 1, shard_id: 0, min_key: vec![], max_key: vec![],
            },
            sealed: true,
        });

        let snap = SnapshotDefinition::create(
            1, &m, StructuredLsn::new(3, 0), Csn::new(50),
        );
        assert_eq!(snap.wal_segments, vec![0, 1, 2]);
        assert_eq!(snap.cold_segments, vec![100]);
        assert_eq!(snap.all_segments().len(), 4);
    }

    // -- Recovery --

    #[test]
    fn test_recovery_full_lifecycle() {
        let store = SegmentStore::new();

        // Create segments in store
        for i in 0..3u64 {
            let hdr = UnifiedSegmentHeader::new_wal(i, 1024, StructuredLsn::new(i, 0));
            store.create_segment(hdr).unwrap();
            store.write_chunk(i, &vec![0xAB; 100]).unwrap();
            store.seal_segment(i).unwrap();
        }

        // Build manifest
        let mut manifest = Manifest::new();
        for i in 0..3u64 {
            manifest.add_segment(ManifestEntry {
                segment_id: i,
                kind: SegmentKind::Wal,
                size_bytes: 1000,
                codec: SegmentCodec::None,
                logical_range: LogicalRange::Wal {
                    start_lsn: StructuredLsn::new(i, 0),
                    end_lsn: StructuredLsn::new(i, 1000),
                },
                sealed: true,
            });
        }

        // Recovery
        let mut rc = RecoveryCoordinator::new(manifest);
        rc.advance_to_verify();
        rc.verify_segments(&store);
        // All segments exist → skip fetch, go to replay
        assert_eq!(rc.phase, RecoveryPhase::ReplayWalTail);
        assert!(rc.missing_segments.is_empty());
        rc.mark_ready();
        assert!(rc.is_ready());
    }

    #[test]
    fn test_recovery_with_missing_segments() {
        let store = SegmentStore::new();

        // Only segment 0 in store
        let hdr = UnifiedSegmentHeader::new_wal(0, 1024, StructuredLsn::ZERO);
        store.create_segment(hdr).unwrap();
        store.seal_segment(0).unwrap();

        // Manifest has 3 segments
        let mut manifest = Manifest::new();
        for i in 0..3u64 {
            manifest.add_segment(ManifestEntry {
                segment_id: i,
                kind: SegmentKind::Wal,
                size_bytes: 100,
                codec: SegmentCodec::None,
                logical_range: LogicalRange::Wal {
                    start_lsn: StructuredLsn::ZERO,
                    end_lsn: StructuredLsn::ZERO,
                },
                sealed: true,
            });
        }

        let mut rc = RecoveryCoordinator::new(manifest);
        rc.advance_to_verify();
        rc.verify_segments(&store);
        assert_eq!(rc.phase, RecoveryPhase::FetchMissing);
        assert_eq!(rc.missing_segments.len(), 2);

        // Simulate fetch
        for seg_id in [1u64, 2] {
            let hdr = UnifiedSegmentHeader::new_wal(seg_id, 1024, StructuredLsn::ZERO);
            store.create_segment(hdr).unwrap();
            store.seal_segment(seg_id).unwrap();
            rc.mark_fetched(seg_id);
        }
        assert_eq!(rc.phase, RecoveryPhase::ReplayWalTail);
        rc.mark_ready();
        assert!(rc.is_ready());
    }

    // -- GC --

    #[test]
    fn test_gc_wal_segment() {
        let gc = SegmentGc::new();
        gc.update_min_durable_lsn(StructuredLsn::new(5, 0));

        let entry = ManifestEntry {
            segment_id: 2,
            kind: SegmentKind::Wal,
            size_bytes: 1000,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(2, 0),
                end_lsn: StructuredLsn::new(3, 0),
            },
            sealed: true,
        };
        let m = Manifest::new();
        assert_eq!(gc.evaluate(&entry, &m), GcDecision::Eligible);
    }

    #[test]
    fn test_gc_streaming_protection() {
        let gc = SegmentGc::new();
        gc.update_min_durable_lsn(StructuredLsn::new(10, 0));
        gc.mark_streaming(2);

        let entry = ManifestEntry {
            segment_id: 2,
            kind: SegmentKind::Wal,
            size_bytes: 1000,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::new(2, 0),
                end_lsn: StructuredLsn::new(3, 0),
            },
            sealed: true,
        };
        let m = Manifest::new();
        assert_eq!(gc.evaluate(&entry, &m), GcDecision::DeferStreaming);

        gc.unmark_streaming(2);
        assert_eq!(gc.evaluate(&entry, &m), GcDecision::Eligible);
    }

    #[test]
    fn test_gc_execute() {
        let store = SegmentStore::new();
        let mut manifest = Manifest::new();

        // Add 3 WAL segments, all sealed
        for i in 0..3u64 {
            let hdr = UnifiedSegmentHeader::new_wal(i, 1024, StructuredLsn::new(i, 0));
            store.create_segment(hdr).unwrap();
            store.write_chunk(i, &vec![0u8; 500]).unwrap();
            store.seal_segment(i).unwrap();
            manifest.add_segment(ManifestEntry {
                segment_id: i,
                kind: SegmentKind::Wal,
                size_bytes: 500,
                codec: SegmentCodec::None,
                logical_range: LogicalRange::Wal {
                    start_lsn: StructuredLsn::new(i, 0),
                    end_lsn: StructuredLsn::new(i + 1, 0),
                },
                sealed: true,
            });
        }

        let gc = SegmentGc::new();
        gc.update_min_durable_lsn(StructuredLsn::new(2, 0)); // segments 0,1 eligible

        let (count, bytes) = gc.execute_gc(&mut manifest, &store);
        assert_eq!(count, 2);
        assert!(bytes > 0);
        assert!(!store.exists(0));
        assert!(!store.exists(1));
        assert!(store.exists(2)); // still needed
    }

    // -- Cold Segment Descriptor --

    #[test]
    fn test_cold_segment_descriptor() {
        let desc = ColdSegmentDescriptor {
            segment_id: 10,
            table_id: 1,
            shard_id: 0,
            codec: SegmentCodec::Lz4,
            original_bytes: 10000,
            compressed_bytes: 3000,
            row_count: 500,
        };
        let ratio = desc.compression_ratio();
        assert!(ratio > 3.0 && ratio < 3.5);
    }

    // -- Metrics --

    #[test]
    fn test_segment_store_metrics() {
        let store = SegmentStore::new();
        let hdr = UnifiedSegmentHeader::new_wal(0, 1024, StructuredLsn::ZERO);
        store.create_segment(hdr).unwrap();
        store.write_chunk(0, &vec![0u8; 100]).unwrap();
        store.seal_segment(0).unwrap();

        let snap = store.metrics.snapshot();
        assert_eq!(snap.segments_created, 1);
        assert_eq!(snap.segments_sealed, 1);
        assert!(snap.write_bytes_total >= 100);
    }

    // -- Display --

    #[test]
    fn test_display_impls() {
        assert_eq!(format!("{}", SegmentKind::Wal), "WAL");
        assert_eq!(format!("{}", SegmentKind::Cold), "COLD");
        assert_eq!(format!("{}", SegmentKind::Snapshot), "SNAPSHOT");
        assert_eq!(format!("{}", SegmentCodec::Lz4), "lz4");
        assert_eq!(format!("{}", RecoveryPhase::Ready), "READY");
    }

    // -- Manifest History / Delta --

    #[test]
    fn test_manifest_delta_history() {
        let mut m = Manifest::new();
        m.add_segment(ManifestEntry {
            segment_id: 0,
            kind: SegmentKind::Wal,
            size_bytes: 100,
            codec: SegmentCodec::None,
            logical_range: LogicalRange::Wal {
                start_lsn: StructuredLsn::ZERO,
                end_lsn: StructuredLsn::ZERO,
            },
            sealed: false,
        });
        m.add_segment(ManifestEntry {
            segment_id: 1,
            kind: SegmentKind::Cold,
            size_bytes: 200,
            codec: SegmentCodec::Lz4,
            logical_range: LogicalRange::Cold {
                table_id: 1, shard_id: 0, min_key: vec![], max_key: vec![],
            },
            sealed: true,
        });

        assert_eq!(m.history.len(), 2);
        let deltas = m.delta_since(0);
        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].added_segments, vec![0]);
        assert_eq!(deltas[1].added_segments, vec![1]);
    }
}
