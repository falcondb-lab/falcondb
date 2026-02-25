//! # Structured LSN (Segment + Offset) & Reservation-Based Allocation
//!
//! **LSN is a physical address, not an abstract counter.**
//!
//! ```text
//! LSN (64-bit) = (segment_id << SEGMENT_OFFSET_BITS) | offset
//! ```
//!
//! ## Design invariants (non-negotiable)
//! - LSN globally monotonically increasing
//! - Within a segment, offset monotonically increasing
//! - On segment switch, offset resets to 0
//! - LSN uniquely determines WAL physical position
//! - No per-record atomic operation — reservation-based batch allocation
//! - Crash recovery rebuilds LSN state from segment headers alone
//!
//! ## Prohibited patterns
//! - ❌ Side table to look up LSN
//! - ❌ Implicit inference from "current file pointer"
//! - ❌ Per-record `AtomicU64::fetch_add`

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Structured LSN Type
// ═══════════════════════════════════════════════════════════════════════════

/// Number of bits allocated to the offset within a segment.
/// 28 bits = 256 MB max segment size. Configurable at compile time.
pub const DEFAULT_SEGMENT_OFFSET_BITS: u32 = 28;

/// Maximum offset value for the default bit width.
pub const DEFAULT_MAX_SEGMENT_SIZE: u64 = 1 << DEFAULT_SEGMENT_OFFSET_BITS; // 256 MB

/// A structured Log Sequence Number encoding both segment ID and byte offset.
///
/// Layout: `(segment_id << offset_bits) | offset`
///
/// This is a **physical address** into the WAL, not an opaque counter.
/// Given an LSN, you can compute the exact segment file and byte position.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StructuredLsn(u64);

impl StructuredLsn {
    /// The zero LSN — represents "no position" / "before any WAL record".
    pub const ZERO: Self = Self(0);

    /// The invalid/sentinel LSN.
    pub const INVALID: Self = Self(u64::MAX);

    /// Construct from raw u64 (for deserialization / recovery).
    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Construct from segment ID and offset.
    #[inline]
    pub const fn new(segment_id: u64, offset: u64) -> Self {
        Self((segment_id << DEFAULT_SEGMENT_OFFSET_BITS) | (offset & (DEFAULT_MAX_SEGMENT_SIZE - 1)))
    }

    /// Extract the segment ID.
    #[inline]
    pub const fn segment_id(self) -> u64 {
        self.0 >> DEFAULT_SEGMENT_OFFSET_BITS
    }

    /// Extract the byte offset within the segment.
    #[inline]
    pub const fn offset(self) -> u64 {
        self.0 & (DEFAULT_MAX_SEGMENT_SIZE - 1)
    }

    /// Get the raw u64 representation.
    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Advance by `bytes` within the same segment. Returns None if overflow.
    #[inline]
    pub const fn advance(self, bytes: u64) -> Option<Self> {
        let new_offset = self.offset() + bytes;
        if new_offset > DEFAULT_MAX_SEGMENT_SIZE {
            None // would cross segment boundary
        } else {
            Some(Self::new(self.segment_id(), new_offset))
        }
    }

    /// Advance to the start of the next segment.
    #[inline]
    pub const fn next_segment_start(self) -> Self {
        Self::new(self.segment_id() + 1, 0)
    }

    /// Check if this LSN is at the start of a segment (offset == 0).
    #[inline]
    pub const fn is_segment_start(self) -> bool {
        self.offset() == 0
    }

    /// Bytes remaining in the current segment.
    #[inline]
    pub const fn remaining_in_segment(self) -> u64 {
        DEFAULT_MAX_SEGMENT_SIZE - self.offset()
    }

    /// Convert a legacy flat LSN (simple counter) to a structured LSN.
    /// Used during migration from v3 WAL format.
    #[inline]
    pub const fn from_legacy(flat_lsn: u64, segment_size: u64) -> Self {
        // Legacy LSN didn't encode segment info, so we can only approximate.
        // For new WAL files, this is the identity mapping at segment 0.
        Self::new(0, flat_lsn % segment_size)
    }
}

impl fmt::Debug for StructuredLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LSN({}/{})", self.segment_id(), self.offset())
    }
}

impl fmt::Display for StructuredLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.segment_id(), self.offset())
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — WAL Segment Header (Physical Layout)
// ═══════════════════════════════════════════════════════════════════════════

/// Magic bytes identifying a structured WAL segment (v2 format).
pub const SEGMENT_MAGIC: [u8; 8] = *b"FWAL_V02";

/// Size of the segment header, 4K-aligned for direct I/O compatibility.
pub const SEGMENT_HEADER_SIZE: u64 = 4096;

/// WAL segment header — the **only** trusted recovery anchor point.
///
/// Written at the start of every segment file. During crash recovery,
/// scanning segment headers is sufficient to rebuild the global LSN state.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct SegmentHeader {
    /// Magic bytes: `FWAL_V02`
    pub magic: [u8; 8],
    /// Format version (for online upgrade compatibility).
    pub version: u32,
    /// Padding for alignment.
    pub _pad0: u32,
    /// Logical segment ID (monotonically increasing).
    pub segment_id: u64,
    /// Maximum segment size in bytes.
    pub segment_size: u64,
    /// LSN of the first record in this segment.
    pub start_lsn: u64,
    /// Byte offset of the last valid (completely written) record.
    /// Updated on flush/seal. Recovery stops here.
    pub last_valid_offset: u64,
    /// CRC32 of the header fields (excluding this field itself).
    pub checksum: u32,
    /// Padding to 64 bytes for cache-line alignment.
    pub _pad1: u32,
}

impl SegmentHeader {
    /// Create a new segment header.
    pub fn new(segment_id: u64, segment_size: u64, start_lsn: StructuredLsn) -> Self {
        let mut hdr = Self {
            magic: SEGMENT_MAGIC,
            version: 2,
            _pad0: 0,
            segment_id,
            segment_size,
            start_lsn: start_lsn.raw(),
            last_valid_offset: SEGMENT_HEADER_SIZE,
            checksum: 0,
            _pad1: 0,
        };
        hdr.checksum = hdr.compute_checksum();
        hdr
    }

    /// Compute CRC32 over header fields (excluding the checksum field).
    pub fn compute_checksum(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.magic);
        hasher.update(&self.version.to_le_bytes());
        hasher.update(&self.segment_id.to_le_bytes());
        hasher.update(&self.segment_size.to_le_bytes());
        hasher.update(&self.start_lsn.to_le_bytes());
        hasher.update(&self.last_valid_offset.to_le_bytes());
        hasher.finalize()
    }

    /// Validate the header: magic, version, and checksum.
    pub fn validate(&self) -> bool {
        self.magic == SEGMENT_MAGIC
            && self.version >= 2
            && self.checksum == self.compute_checksum()
    }

    /// Serialize to bytes (for writing to disk).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; SEGMENT_HEADER_SIZE as usize];
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..12].copy_from_slice(&self.version.to_le_bytes());
        buf[16..24].copy_from_slice(&self.segment_id.to_le_bytes());
        buf[24..32].copy_from_slice(&self.segment_size.to_le_bytes());
        buf[32..40].copy_from_slice(&self.start_lsn.to_le_bytes());
        buf[40..48].copy_from_slice(&self.last_valid_offset.to_le_bytes());
        buf[48..52].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 56 {
            return None;
        }
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&data[0..8]);
        let version = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        let segment_id = u64::from_le_bytes([
            data[16], data[17], data[18], data[19],
            data[20], data[21], data[22], data[23],
        ]);
        let segment_size = u64::from_le_bytes([
            data[24], data[25], data[26], data[27],
            data[28], data[29], data[30], data[31],
        ]);
        let start_lsn = u64::from_le_bytes([
            data[32], data[33], data[34], data[35],
            data[36], data[37], data[38], data[39],
        ]);
        let last_valid_offset = u64::from_le_bytes([
            data[40], data[41], data[42], data[43],
            data[44], data[45], data[46], data[47],
        ]);
        let checksum = u32::from_le_bytes([data[48], data[49], data[50], data[51]]);
        Some(Self {
            magic,
            version,
            _pad0: 0,
            segment_id,
            segment_size,
            start_lsn,
            last_valid_offset,
            checksum,
            _pad1: 0,
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — LSN → Physical Position Calculation
// ═══════════════════════════════════════════════════════════════════════════

/// Resolve an LSN to a physical file position.
///
/// **This is the ONLY way to locate a WAL record.** No side tables, no
/// implicit file pointers.
#[derive(Debug, Clone, Copy)]
pub struct PhysicalPosition {
    /// Segment file identifier.
    pub segment_id: u64,
    /// Byte offset within the segment file (includes header).
    pub file_offset: u64,
}

/// Compute the physical position for an LSN.
///
/// The offset in the LSN is a *data offset* (starting after the segment
/// header). The file offset adds `SEGMENT_HEADER_SIZE`.
#[inline]
pub const fn lsn_to_physical(lsn: StructuredLsn) -> PhysicalPosition {
    PhysicalPosition {
        segment_id: lsn.segment_id(),
        file_offset: SEGMENT_HEADER_SIZE + lsn.offset(),
    }
}

/// Reconstruct an LSN from a physical position.
#[inline]
pub const fn physical_to_lsn(segment_id: u64, file_offset: u64) -> StructuredLsn {
    let data_offset = file_offset.saturating_sub(SEGMENT_HEADER_SIZE);
    StructuredLsn::new(segment_id, data_offset)
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Reservation-Based LSN Allocation
// ═══════════════════════════════════════════════════════════════════════════

/// A reserved contiguous LSN range. The holder can write records into this
/// range without any global atomic operations.
///
/// **Invariants:**
/// - `base_lsn.segment_id() == limit_lsn.segment_id()` (never crosses segment)
/// - All bytes in `[base_lsn, limit_lsn)` belong exclusively to the holder
/// - Once exhausted, the holder must request a new reservation
#[derive(Debug, Clone, Copy)]
pub struct Reservation {
    /// First usable LSN in this reservation.
    pub base_lsn: StructuredLsn,
    /// One-past-the-end LSN (exclusive upper bound).
    pub limit_lsn: StructuredLsn,
}

impl Reservation {
    /// Total bytes in this reservation.
    #[inline]
    pub const fn size(&self) -> u64 {
        self.limit_lsn.raw() - self.base_lsn.raw()
    }

    /// Check if a byte count fits in this reservation starting at `current`.
    #[inline]
    pub const fn fits(&self, current: StructuredLsn, bytes: u64) -> bool {
        current.raw() + bytes <= self.limit_lsn.raw()
    }

    /// Whether this reservation has been fully consumed.
    #[inline]
    pub const fn is_exhausted(&self, current: StructuredLsn) -> bool {
        current.raw() >= self.limit_lsn.raw()
    }
}

/// Configuration for the LSN allocator.
#[derive(Debug, Clone)]
pub struct LsnAllocatorConfig {
    /// Maximum segment size in bytes. Must be a power of 2.
    pub segment_size: u64,
    /// Default reservation size in bytes.
    /// Larger = fewer global atomic touches, but more wasted space on rollover.
    pub default_reservation_bytes: u64,
    /// Record alignment in bytes (8, 16, 64, 4096). Must be a power of 2.
    pub record_alignment: u64,
}

impl Default for LsnAllocatorConfig {
    fn default() -> Self {
        Self {
            segment_size: DEFAULT_MAX_SEGMENT_SIZE, // 256 MB
            default_reservation_bytes: 256 * 1024,  // 256 KB
            record_alignment: 8,                     // 8-byte aligned
        }
    }
}

impl LsnAllocatorConfig {
    /// Validate configuration. Panics on invalid values.
    pub fn validate(&self) {
        assert!(self.segment_size.is_power_of_two(), "segment_size must be power of 2");
        assert!(self.segment_size <= DEFAULT_MAX_SEGMENT_SIZE,
            "segment_size must be <= {DEFAULT_MAX_SEGMENT_SIZE} bytes");
        assert!(self.record_alignment.is_power_of_two(), "record_alignment must be power of 2");
        assert!(self.record_alignment <= 4096, "record_alignment must be <= 4096");
        assert!(self.default_reservation_bytes > 0, "default_reservation_bytes must be > 0");
        assert!(self.default_reservation_bytes <= self.segment_size,
            "default_reservation_bytes must be <= segment_size");
        assert!(self.segment_size.is_multiple_of(self.record_alignment),
            "segment_size must be a multiple of record_alignment");
    }
}

/// Global LSN allocator — the single point of truth for LSN assignment.
///
/// Writers call `reserve()` to get a contiguous LSN range, then allocate
/// within that range locally without touching any global state.
///
/// **Concurrency model:**
/// - `reserve()` takes a mutex (serialized, but infrequent)
/// - Within a reservation, allocation is local (zero contention)
/// - Segment rollover is serialized inside `reserve()`
pub struct LsnAllocator {
    /// Protected global state — only touched during `reserve()`.
    inner: Mutex<LsnAllocatorInner>,
    config: LsnAllocatorConfig,
    pub metrics: LsnAllocatorMetrics,
}

struct LsnAllocatorInner {
    /// Current segment ID.
    current_segment_id: u64,
    /// Next allocatable offset within the current segment.
    current_offset: u64,
}

/// Metrics for LSN allocation observability.
#[derive(Debug, Default)]
pub struct LsnAllocatorMetrics {
    /// Total reservation batches handed out.
    pub reservation_batches_total: AtomicU64,
    /// Total bytes reserved across all reservations.
    pub reserved_bytes_total: AtomicU64,
    /// Total segment rollovers.
    pub segment_rollover_total: AtomicU64,
    /// Sum of (used_bytes / segment_size) at rollover time, ×1000 for precision.
    /// Divide by `segment_rollover_total` to get average utilization.
    pub segment_utilization_sum_permille: AtomicU64,
    /// Number of reservations that triggered a rollover.
    pub reservation_triggered_rollover: AtomicU64,
}

impl LsnAllocatorMetrics {
    /// Average reservation size in bytes.
    pub fn avg_reservation_bytes(&self) -> f64 {
        let batches = self.reservation_batches_total.load(Ordering::Relaxed);
        if batches == 0 { return 0.0; }
        self.reserved_bytes_total.load(Ordering::Relaxed) as f64 / batches as f64
    }

    /// Average segment utilization at rollover (0.0–1.0).
    pub fn avg_segment_utilization(&self) -> f64 {
        let rollovers = self.segment_rollover_total.load(Ordering::Relaxed);
        if rollovers == 0 { return 1.0; }
        self.segment_utilization_sum_permille.load(Ordering::Relaxed) as f64
            / rollovers as f64 / 1000.0
    }
}

/// Result of a `reserve()` call.
#[derive(Debug)]
pub enum ReserveResult {
    /// Reservation granted within the current segment.
    Ok(Reservation),
    /// Reservation granted, but a segment rollover occurred first.
    /// The caller may need to write the new segment header.
    OkWithRollover {
        reservation: Reservation,
        /// The sealed segment's ID (the one that was just closed).
        sealed_segment_id: u64,
        /// The byte offset of the last valid data in the sealed segment.
        sealed_last_valid_offset: u64,
        /// The new segment's header (caller must write to disk).
        new_header: SegmentHeader,
    },
}

impl ReserveResult {
    /// Get the reservation regardless of whether rollover occurred.
    pub const fn reservation(&self) -> &Reservation {
        match self {
            Self::Ok(r) => r,
            Self::OkWithRollover { reservation, .. } => reservation,
        }
    }

    /// Whether a segment rollover was triggered.
    pub const fn had_rollover(&self) -> bool {
        matches!(self, Self::OkWithRollover { .. })
    }
}

impl LsnAllocator {
    /// Create a new allocator starting at the given segment and offset.
    ///
    /// For fresh WAL: `segment_id=0, initial_offset=0`.
    /// For recovery: pass the segment/offset recovered from the last
    /// valid segment header.
    pub fn new(config: LsnAllocatorConfig, segment_id: u64, initial_offset: u64) -> Self {
        config.validate();
        Self {
            inner: Mutex::new(LsnAllocatorInner {
                current_segment_id: segment_id,
                current_offset: initial_offset,
            }),
            config,
            metrics: LsnAllocatorMetrics::default(),
        }
    }

    /// Reserve a contiguous LSN range of at least `min_bytes`.
    ///
    /// The actual reservation may be larger (up to `default_reservation_bytes`)
    /// to amortize the cost of future reserves.
    ///
    /// **This is the ONLY function that touches global state.**
    /// It is serialized by a mutex, but called infrequently
    /// (once per reservation, not once per record).
    pub fn reserve(&self, min_bytes: u64) -> ReserveResult {
        let reserve_bytes = min_bytes.max(self.config.default_reservation_bytes);
        // Align up
        let aligned = self.align_up(reserve_bytes);

        let mut inner = self.inner.lock();

        let remaining = self.config.segment_size.saturating_sub(inner.current_offset);

        if aligned <= remaining {
            // Fits in current segment — fast path
            let base = StructuredLsn::new(inner.current_segment_id, inner.current_offset);
            let limit = StructuredLsn::new(inner.current_segment_id, inner.current_offset + aligned);
            inner.current_offset += aligned;

            self.metrics.reservation_batches_total.fetch_add(1, Ordering::Relaxed);
            self.metrics.reserved_bytes_total.fetch_add(aligned, Ordering::Relaxed);

            ReserveResult::Ok(Reservation { base_lsn: base, limit_lsn: limit })
        } else {
            // Need segment rollover
            let sealed_segment_id = inner.current_segment_id;
            let sealed_offset = inner.current_offset;

            // Record utilization of the sealed segment
            let util_permille = if self.config.segment_size > 0 {
                (sealed_offset * 1000) / self.config.segment_size
            } else {
                0
            };
            self.metrics.segment_utilization_sum_permille.fetch_add(util_permille, Ordering::Relaxed);
            self.metrics.segment_rollover_total.fetch_add(1, Ordering::Relaxed);
            self.metrics.reservation_triggered_rollover.fetch_add(1, Ordering::Relaxed);

            // Advance to next segment
            inner.current_segment_id += 1;
            inner.current_offset = 0;

            let new_seg_id = inner.current_segment_id;
            let start_lsn = StructuredLsn::new(new_seg_id, 0);

            // Allocate within the new segment
            let actual_reserve = aligned.min(self.config.segment_size);
            let base = StructuredLsn::new(new_seg_id, 0);
            let limit = StructuredLsn::new(new_seg_id, actual_reserve);
            inner.current_offset = actual_reserve;

            self.metrics.reservation_batches_total.fetch_add(1, Ordering::Relaxed);
            self.metrics.reserved_bytes_total.fetch_add(actual_reserve, Ordering::Relaxed);

            let header = SegmentHeader::new(new_seg_id, self.config.segment_size, start_lsn);

            ReserveResult::OkWithRollover {
                reservation: Reservation { base_lsn: base, limit_lsn: limit },
                sealed_segment_id,
                sealed_last_valid_offset: sealed_offset,
                new_header: header,
            }
        }
    }

    /// Reserve exactly `bytes` (no over-allocation). Used for group commit
    /// where the exact batch size is known.
    pub fn reserve_exact(&self, bytes: u64) -> ReserveResult {
        let aligned = self.align_up(bytes);
        let mut inner = self.inner.lock();

        let remaining = self.config.segment_size.saturating_sub(inner.current_offset);

        if aligned <= remaining {
            let base = StructuredLsn::new(inner.current_segment_id, inner.current_offset);
            let limit = StructuredLsn::new(inner.current_segment_id, inner.current_offset + aligned);
            inner.current_offset += aligned;

            self.metrics.reservation_batches_total.fetch_add(1, Ordering::Relaxed);
            self.metrics.reserved_bytes_total.fetch_add(aligned, Ordering::Relaxed);

            ReserveResult::Ok(Reservation { base_lsn: base, limit_lsn: limit })
        } else {
            let sealed_segment_id = inner.current_segment_id;
            let sealed_offset = inner.current_offset;
            let util_permille = if self.config.segment_size > 0 {
                (sealed_offset * 1000) / self.config.segment_size
            } else { 0 };
            self.metrics.segment_utilization_sum_permille.fetch_add(util_permille, Ordering::Relaxed);
            self.metrics.segment_rollover_total.fetch_add(1, Ordering::Relaxed);
            self.metrics.reservation_triggered_rollover.fetch_add(1, Ordering::Relaxed);

            inner.current_segment_id += 1;
            inner.current_offset = 0;

            let new_seg_id = inner.current_segment_id;
            let start_lsn = StructuredLsn::new(new_seg_id, 0);
            let actual = aligned.min(self.config.segment_size);
            let base = StructuredLsn::new(new_seg_id, 0);
            let limit = StructuredLsn::new(new_seg_id, actual);
            inner.current_offset = actual;

            self.metrics.reservation_batches_total.fetch_add(1, Ordering::Relaxed);
            self.metrics.reserved_bytes_total.fetch_add(actual, Ordering::Relaxed);

            let header = SegmentHeader::new(new_seg_id, self.config.segment_size, start_lsn);
            ReserveResult::OkWithRollover {
                reservation: Reservation { base_lsn: base, limit_lsn: limit },
                sealed_segment_id,
                sealed_last_valid_offset: sealed_offset,
                new_header: header,
            }
        }
    }

    /// Get current position (segment_id, offset) without reserving.
    pub fn current_position(&self) -> (u64, u64) {
        let inner = self.inner.lock();
        (inner.current_segment_id, inner.current_offset)
    }

    /// Get the current LSN (the next byte to be allocated).
    pub fn current_lsn(&self) -> StructuredLsn {
        let inner = self.inner.lock();
        StructuredLsn::new(inner.current_segment_id, inner.current_offset)
    }

    /// Recover allocator state from a segment header.
    /// Called during crash recovery after scanning segment files.
    pub fn recover_from_header(&self, header: &SegmentHeader) {
        let mut inner = self.inner.lock();
        if header.segment_id >= inner.current_segment_id {
            inner.current_segment_id = header.segment_id;
            inner.current_offset = header.last_valid_offset;
        }
    }

    /// Align a byte count up to the configured record alignment.
    #[inline]
    const fn align_up(&self, bytes: u64) -> u64 {
        let mask = self.config.record_alignment - 1;
        (bytes + mask) & !mask
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §5 — Writer-Local LSN Cursor
// ═══════════════════════════════════════════════════════════════════════════

/// A writer-local cursor that allocates LSNs from a reservation without
/// any global synchronization.
///
/// Each writer thread holds one of these. When the reservation is exhausted,
/// the writer must call `allocator.reserve()` to get a new one.
pub struct WriterCursor {
    /// Current write position within the reservation.
    current: StructuredLsn,
    /// The active reservation.
    reservation: Reservation,
}

impl WriterCursor {
    /// Create a cursor from a reservation.
    pub const fn new(reservation: Reservation) -> Self {
        Self {
            current: reservation.base_lsn,
            reservation,
        }
    }

    /// Allocate `bytes` from this cursor. Returns the LSN of the start of
    /// the allocated region, or `None` if the reservation is exhausted.
    ///
    /// **Zero global synchronization.** This is a local pointer bump.
    #[inline]
    pub const fn allocate(&mut self, bytes: u64) -> Option<StructuredLsn> {
        if self.reservation.fits(self.current, bytes) {
            let lsn = self.current;
            self.current = StructuredLsn::from_raw(self.current.raw() + bytes);
            Some(lsn)
        } else {
            None
        }
    }

    /// Remaining bytes in the current reservation.
    #[inline]
    pub const fn remaining(&self) -> u64 {
        self.reservation.limit_lsn.raw().saturating_sub(self.current.raw())
    }

    /// Whether the reservation is fully consumed.
    #[inline]
    pub const fn is_exhausted(&self) -> bool {
        self.reservation.is_exhausted(self.current)
    }

    /// The current write position.
    #[inline]
    pub const fn position(&self) -> StructuredLsn {
        self.current
    }

    /// The reservation this cursor is operating within.
    #[inline]
    pub const fn reservation(&self) -> &Reservation {
        &self.reservation
    }

    /// Replace the reservation with a new one (after re-reserving).
    pub const fn reset(&mut self, reservation: Reservation) {
        self.current = reservation.base_lsn;
        self.reservation = reservation;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §6 — WAL Record Framing (Physical Layout)
// ═══════════════════════════════════════════════════════════════════════════

/// WAL record frame header.
///
/// **Important:** The record does NOT store its own LSN. The LSN is
/// computed from the record's physical position (segment + offset).
///
/// Layout: `[record_len: u32][flags: u16][reserved: u16][crc: u32][payload: record_len]`
pub const RECORD_FRAME_HEADER_SIZE: u64 = 12; // 4 + 2 + 2 + 4

/// Record flags.
pub mod record_flags {
    /// Normal record.
    pub const NORMAL: u16 = 0x0000;
    /// Record carries a transaction ID in the payload prefix.
    pub const HAS_TXID: u16 = 0x0001;
    /// Padding record (skip during replay).
    pub const PADDING: u16 = 0x8000;
}

/// Encode a record frame header.
#[inline]
pub fn encode_record_header(record_len: u32, flags: u16, crc: u32) -> [u8; 12] {
    let mut buf = [0u8; 12];
    buf[0..4].copy_from_slice(&record_len.to_le_bytes());
    buf[4..6].copy_from_slice(&flags.to_le_bytes());
    // bytes 6..8 reserved (zero)
    buf[8..12].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Decode a record frame header.
#[inline]
pub fn decode_record_header(data: &[u8]) -> Option<(u32, u16, u32)> {
    if data.len() < 12 { return None; }
    let len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let flags = u16::from_le_bytes([data[4], data[5]]);
    let crc = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
    Some((len, flags, crc))
}

/// Compute the total on-disk size of a record (header + payload), aligned.
#[inline]
pub const fn aligned_record_size(payload_len: u64, alignment: u64) -> u64 {
    let raw = RECORD_FRAME_HEADER_SIZE + payload_len;
    let mask = alignment - 1;
    (raw + mask) & !mask
}

// ═══════════════════════════════════════════════════════════════════════════
// §7 — Crash Recovery from Segment Headers
// ═══════════════════════════════════════════════════════════════════════════

/// Result of scanning WAL segment files for recovery.
#[derive(Debug, Clone)]
pub struct RecoveryState {
    /// Recovered segment headers, sorted by segment_id.
    pub segments: Vec<RecoveredSegment>,
    /// The LSN to resume allocation from.
    pub resume_lsn: StructuredLsn,
}

#[derive(Debug, Clone)]
pub struct RecoveredSegment {
    pub segment_id: u64,
    pub start_lsn: StructuredLsn,
    pub last_valid_offset: u64,
    pub header_valid: bool,
}

/// Scan segment headers from raw header bytes to build recovery state.
/// In production, the caller reads the first 4096 bytes of each segment file.
pub fn recover_from_headers(headers: &[(u64, Vec<u8>)]) -> RecoveryState {
    let mut segments: Vec<RecoveredSegment> = Vec::new();

    for (_seg_file_id, data) in headers {
        if let Some(hdr) = SegmentHeader::from_bytes(data) {
            let valid = hdr.validate();
            segments.push(RecoveredSegment {
                segment_id: hdr.segment_id,
                start_lsn: StructuredLsn::from_raw(hdr.start_lsn),
                last_valid_offset: if valid { hdr.last_valid_offset } else { SEGMENT_HEADER_SIZE },
                header_valid: valid,
            });
        }
    }

    segments.sort_by_key(|s| s.segment_id);

    let resume_lsn = segments.last().map_or(StructuredLsn::ZERO, |last| {
        // Resume after the last valid data in the last segment
        StructuredLsn::new(last.segment_id, last.last_valid_offset)
    });

    RecoveryState { segments, resume_lsn }
}

// ═══════════════════════════════════════════════════════════════════════════
// §8 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // -- StructuredLsn Tests --

    #[test]
    fn test_lsn_construction_and_extraction() {
        let lsn = StructuredLsn::new(5, 1024);
        assert_eq!(lsn.segment_id(), 5);
        assert_eq!(lsn.offset(), 1024);
    }

    #[test]
    fn test_lsn_ordering() {
        let a = StructuredLsn::new(0, 100);
        let b = StructuredLsn::new(0, 200);
        let c = StructuredLsn::new(1, 0);
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn test_lsn_advance() {
        let lsn = StructuredLsn::new(0, 100);
        let advanced = lsn.advance(50).unwrap();
        assert_eq!(advanced.segment_id(), 0);
        assert_eq!(advanced.offset(), 150);
    }

    #[test]
    fn test_lsn_advance_overflow() {
        let lsn = StructuredLsn::new(0, DEFAULT_MAX_SEGMENT_SIZE - 10);
        assert!(lsn.advance(20).is_none());
    }

    #[test]
    fn test_lsn_next_segment() {
        let lsn = StructuredLsn::new(3, 5000);
        let next = lsn.next_segment_start();
        assert_eq!(next.segment_id(), 4);
        assert_eq!(next.offset(), 0);
    }

    #[test]
    fn test_lsn_remaining_in_segment() {
        let lsn = StructuredLsn::new(0, 1000);
        assert_eq!(lsn.remaining_in_segment(), DEFAULT_MAX_SEGMENT_SIZE - 1000);
    }

    #[test]
    fn test_lsn_display() {
        let lsn = StructuredLsn::new(7, 4096);
        assert_eq!(format!("{}", lsn), "7/4096");
        assert_eq!(format!("{:?}", lsn), "LSN(7/4096)");
    }

    #[test]
    fn test_lsn_raw_roundtrip() {
        let lsn = StructuredLsn::new(42, 123456);
        let raw = lsn.raw();
        let recovered = StructuredLsn::from_raw(raw);
        assert_eq!(recovered.segment_id(), 42);
        assert_eq!(recovered.offset(), 123456);
    }

    #[test]
    fn test_lsn_zero_and_invalid() {
        assert_eq!(StructuredLsn::ZERO.raw(), 0);
        assert_eq!(StructuredLsn::INVALID.raw(), u64::MAX);
        assert!(StructuredLsn::ZERO < StructuredLsn::INVALID);
    }

    // -- Segment Header Tests --

    #[test]
    fn test_segment_header_roundtrip() {
        let hdr = SegmentHeader::new(5, DEFAULT_MAX_SEGMENT_SIZE, StructuredLsn::new(5, 0));
        assert!(hdr.validate());

        let bytes = hdr.to_bytes();
        assert_eq!(bytes.len(), SEGMENT_HEADER_SIZE as usize);

        let recovered = SegmentHeader::from_bytes(&bytes).unwrap();
        assert!(recovered.validate());
        assert_eq!(recovered.segment_id, 5);
        assert_eq!(recovered.segment_size, DEFAULT_MAX_SEGMENT_SIZE);
        assert_eq!(recovered.start_lsn, StructuredLsn::new(5, 0).raw());
    }

    #[test]
    fn test_segment_header_checksum_detects_corruption() {
        let hdr = SegmentHeader::new(0, DEFAULT_MAX_SEGMENT_SIZE, StructuredLsn::ZERO);
        let mut bytes = hdr.to_bytes();
        // Corrupt one byte
        bytes[20] ^= 0xFF;
        let corrupted = SegmentHeader::from_bytes(&bytes).unwrap();
        assert!(!corrupted.validate());
    }

    // -- Physical Position Tests --

    #[test]
    fn test_lsn_to_physical() {
        let lsn = StructuredLsn::new(3, 1000);
        let pos = lsn_to_physical(lsn);
        assert_eq!(pos.segment_id, 3);
        assert_eq!(pos.file_offset, SEGMENT_HEADER_SIZE + 1000);
    }

    #[test]
    fn test_physical_to_lsn_roundtrip() {
        let original = StructuredLsn::new(10, 8192);
        let pos = lsn_to_physical(original);
        let recovered = physical_to_lsn(pos.segment_id, pos.file_offset);
        assert_eq!(recovered, original);
    }

    // -- Reservation Allocator Tests --

    #[test]
    fn test_allocator_basic_reserve() {
        let alloc = LsnAllocator::new(LsnAllocatorConfig::default(), 0, 0);
        let result = alloc.reserve(1024);
        assert!(!result.had_rollover());
        let r = result.reservation();
        assert_eq!(r.base_lsn.segment_id(), 0);
        assert_eq!(r.base_lsn.offset(), 0);
        // Should be at least default_reservation_bytes (256 KB), aligned
        assert!(r.size() >= 256 * 1024);
    }

    #[test]
    fn test_allocator_sequential_reserves() {
        let alloc = LsnAllocator::new(LsnAllocatorConfig::default(), 0, 0);
        let r1 = alloc.reserve(1024);
        let r2 = alloc.reserve(1024);

        let res1 = r1.reservation();
        let res2 = r2.reservation();

        // r2 starts where r1 ends
        assert_eq!(res2.base_lsn.raw(), res1.limit_lsn.raw());
        // Both in same segment
        assert_eq!(res1.base_lsn.segment_id(), res2.base_lsn.segment_id());
    }

    #[test]
    fn test_allocator_segment_rollover() {
        let config = LsnAllocatorConfig {
            segment_size: 4096, // tiny segment for testing
            default_reservation_bytes: 1024,
            record_alignment: 8,
        };
        let alloc = LsnAllocator::new(config, 0, 0);

        // Fill up first segment
        let _r1 = alloc.reserve(2048);
        let _r2 = alloc.reserve(2048);

        // This should trigger rollover
        let r3 = alloc.reserve(1024);
        assert!(r3.had_rollover());
        let res3 = r3.reservation();
        assert_eq!(res3.base_lsn.segment_id(), 1);
        assert_eq!(res3.base_lsn.offset(), 0);

        assert_eq!(alloc.metrics.segment_rollover_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_allocator_exact_reserve() {
        let alloc = LsnAllocator::new(LsnAllocatorConfig {
            segment_size: 4096,
            default_reservation_bytes: 256,
            record_alignment: 8,
        }, 0, 0);

        let r = alloc.reserve_exact(128);
        assert!(!r.had_rollover());
        let res = r.reservation();
        assert_eq!(res.size(), 128);
    }

    #[test]
    fn test_allocator_recover_from_header() {
        let alloc = LsnAllocator::new(LsnAllocatorConfig::default(), 0, 0);
        let hdr = SegmentHeader::new(5, DEFAULT_MAX_SEGMENT_SIZE, StructuredLsn::new(5, 0));
        let mut hdr_with_data = hdr;
        hdr_with_data.last_valid_offset = 10000;
        alloc.recover_from_header(&hdr_with_data);

        let (seg, off) = alloc.current_position();
        assert_eq!(seg, 5);
        assert_eq!(off, 10000);
    }

    #[test]
    fn test_allocator_metrics() {
        let config = LsnAllocatorConfig {
            segment_size: 4096,
            default_reservation_bytes: 1024,
            record_alignment: 8,
        };
        let alloc = LsnAllocator::new(config, 0, 0);

        alloc.reserve(512);
        alloc.reserve(512);
        alloc.reserve(512);
        alloc.reserve(512);
        // 4 × 1024 = 4096 → fills the segment
        let r5 = alloc.reserve(512);
        assert!(r5.had_rollover());

        assert!(alloc.metrics.reservation_batches_total.load(Ordering::Relaxed) >= 5);
        assert_eq!(alloc.metrics.segment_rollover_total.load(Ordering::Relaxed), 1);
        assert!(alloc.metrics.avg_segment_utilization() > 0.5);
    }

    // -- Writer Cursor Tests --

    #[test]
    fn test_writer_cursor_allocate() {
        let res = Reservation {
            base_lsn: StructuredLsn::new(0, 0),
            limit_lsn: StructuredLsn::new(0, 1024),
        };
        let mut cursor = WriterCursor::new(res);

        let lsn1 = cursor.allocate(100).unwrap();
        assert_eq!(lsn1, StructuredLsn::new(0, 0));

        let lsn2 = cursor.allocate(200).unwrap();
        assert_eq!(lsn2, StructuredLsn::new(0, 100));

        assert_eq!(cursor.remaining(), 724);
    }

    #[test]
    fn test_writer_cursor_exhaustion() {
        let res = Reservation {
            base_lsn: StructuredLsn::new(0, 0),
            limit_lsn: StructuredLsn::new(0, 100),
        };
        let mut cursor = WriterCursor::new(res);

        assert!(cursor.allocate(50).is_some());
        assert!(cursor.allocate(50).is_some());
        assert!(cursor.allocate(1).is_none()); // exhausted
        assert!(cursor.is_exhausted());
    }

    #[test]
    fn test_writer_cursor_reset() {
        let res1 = Reservation {
            base_lsn: StructuredLsn::new(0, 0),
            limit_lsn: StructuredLsn::new(0, 100),
        };
        let mut cursor = WriterCursor::new(res1);
        cursor.allocate(100);
        assert!(cursor.is_exhausted());

        let res2 = Reservation {
            base_lsn: StructuredLsn::new(0, 100),
            limit_lsn: StructuredLsn::new(0, 200),
        };
        cursor.reset(res2);
        assert!(!cursor.is_exhausted());
        assert_eq!(cursor.remaining(), 100);
    }

    // -- Record Framing Tests --

    #[test]
    fn test_record_header_roundtrip() {
        let header = encode_record_header(256, record_flags::HAS_TXID, 0xDEADBEEF);
        let (len, flags, crc) = decode_record_header(&header).unwrap();
        assert_eq!(len, 256);
        assert_eq!(flags, record_flags::HAS_TXID);
        assert_eq!(crc, 0xDEADBEEF);
    }

    #[test]
    fn test_aligned_record_size() {
        assert_eq!(aligned_record_size(100, 8), 112); // 12 + 100 = 112 (already aligned)
        assert_eq!(aligned_record_size(1, 8), 16);    // 12 + 1 = 13 → 16
        assert_eq!(aligned_record_size(0, 8), 16);    // 12 + 0 = 12 → 16 (header is 12, needs align to 16)
        assert_eq!(aligned_record_size(100, 4096), 4096); // 112 → 4096
    }

    // -- Recovery Tests --

    #[test]
    fn test_recovery_from_headers() {
        let h0 = SegmentHeader::new(0, DEFAULT_MAX_SEGMENT_SIZE, StructuredLsn::new(0, 0));
        let mut h0_mod = h0;
        h0_mod.last_valid_offset = 50000;
        h0_mod.checksum = h0_mod.compute_checksum();

        let h1 = SegmentHeader::new(1, DEFAULT_MAX_SEGMENT_SIZE, StructuredLsn::new(1, 0));
        let mut h1_mod = h1;
        h1_mod.last_valid_offset = 12000;
        h1_mod.checksum = h1_mod.compute_checksum();

        let headers = vec![
            (0, h0_mod.to_bytes()),
            (1, h1_mod.to_bytes()),
        ];

        let state = recover_from_headers(&headers);
        assert_eq!(state.segments.len(), 2);
        assert_eq!(state.resume_lsn.segment_id(), 1);
        assert_eq!(state.resume_lsn.offset(), 12000);
    }

    #[test]
    fn test_recovery_empty() {
        let state = recover_from_headers(&[]);
        assert_eq!(state.segments.len(), 0);
        assert_eq!(state.resume_lsn, StructuredLsn::ZERO);
    }

    #[test]
    fn test_recovery_corrupted_header_skipped() {
        let h0 = SegmentHeader::new(0, DEFAULT_MAX_SEGMENT_SIZE, StructuredLsn::new(0, 0));
        let mut bytes = h0.to_bytes();
        bytes[20] ^= 0xFF; // corrupt

        let headers = vec![(0, bytes)];
        let state = recover_from_headers(&headers);
        assert_eq!(state.segments.len(), 1);
        assert!(!state.segments[0].header_valid);
        // Falls back to SEGMENT_HEADER_SIZE
        assert_eq!(state.resume_lsn.offset(), SEGMENT_HEADER_SIZE);
    }

    // -- Concurrent Reserve Tests --

    #[test]
    fn test_allocator_concurrent_reserve() {
        use std::sync::Arc;
        let alloc = Arc::new(LsnAllocator::new(LsnAllocatorConfig::default(), 0, 0));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let a = Arc::clone(&alloc);
            handles.push(std::thread::spawn(move || {
                let mut lsns = Vec::new();
                for _ in 0..100 {
                    let result = a.reserve(1024);
                    let r = result.reservation();
                    lsns.push((r.base_lsn.raw(), r.limit_lsn.raw()));
                }
                lsns
            }));
        }

        let mut all_ranges: Vec<(u64, u64)> = Vec::new();
        for h in handles {
            all_ranges.extend(h.join().unwrap());
        }

        // Verify no overlaps: sort by base, check adjacent
        all_ranges.sort_by_key(|r| r.0);
        for i in 1..all_ranges.len() {
            assert!(
                all_ranges[i].0 >= all_ranges[i - 1].1,
                "Reservation overlap detected: {:?} and {:?}",
                all_ranges[i - 1], all_ranges[i]
            );
        }

        assert_eq!(
            alloc.metrics.reservation_batches_total.load(Ordering::Relaxed),
            800
        );
    }

    #[test]
    fn test_allocator_concurrent_rollover() {
        use std::sync::Arc;
        let config = LsnAllocatorConfig {
            segment_size: 8192,
            default_reservation_bytes: 512,
            record_alignment: 8,
        };
        let alloc = Arc::new(LsnAllocator::new(config, 0, 0));
        let mut handles = Vec::new();

        for _ in 0..4 {
            let a = Arc::clone(&alloc);
            handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    a.reserve(512);
                }
            }));
        }

        for h in handles { h.join().unwrap(); }

        // 200 reserves × 512 bytes = 100 KB, segment=8KB → ~12 rollovers
        let rollovers = alloc.metrics.segment_rollover_total.load(Ordering::Relaxed);
        assert!(rollovers >= 10, "expected ≥10 rollovers, got {}", rollovers);
    }

    // -- Alignment Tests --

    #[test]
    fn test_alignment_4096() {
        let config = LsnAllocatorConfig {
            segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            default_reservation_bytes: 8192,
            record_alignment: 4096,
        };
        let alloc = LsnAllocator::new(config, 0, 0);
        let r = alloc.reserve_exact(100);
        // 100 bytes → aligned to 4096
        assert_eq!(r.reservation().size(), 4096);
    }

    #[test]
    fn test_alignment_8() {
        let config = LsnAllocatorConfig {
            segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            default_reservation_bytes: 256,
            record_alignment: 8,
        };
        let alloc = LsnAllocator::new(config, 0, 0);
        let r = alloc.reserve_exact(100);
        assert_eq!(r.reservation().size(), 104); // 100 → 104 (next multiple of 8)
    }

    // -- Performance Baseline Tests --

    #[test]
    fn test_perf_reservation_vs_atomic() {
        // Demonstrate that reservation requires far fewer atomic ops
        // than per-record allocation.
        let config = LsnAllocatorConfig {
            segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            default_reservation_bytes: 64 * 1024, // 64 KB reservations
            record_alignment: 8,
        };
        let alloc = LsnAllocator::new(config, 0, 0);

        let record_size = 128u64; // typical WAL record
        let total_records = 10_000u64;
        let total_bytes = total_records * record_size;

        // With reservation: ~total_bytes / 64KB ≈ 20 atomic touches
        let mut reserved = 0u64;
        let mut atomic_touches = 0u64;
        while reserved < total_bytes {
            let result = alloc.reserve(record_size);
            let r = result.reservation();
            reserved += r.size();
            atomic_touches += 1;
        }

        // Without reservation: 10,000 atomic touches (one per record)
        let baseline_touches = total_records;

        let reduction_pct = (1.0 - atomic_touches as f64 / baseline_touches as f64) * 100.0;
        assert!(
            reduction_pct >= 90.0,
            "Expected ≥90% reduction in atomic ops, got {:.1}% (reservations={}, baseline={})",
            reduction_pct, atomic_touches, baseline_touches
        );
    }
}
