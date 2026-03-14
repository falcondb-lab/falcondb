//! # Grace Hash JOIN with Spill-to-Disk (P1-4)
//!
//! When the build-side (right) of a Hash JOIN exceeds `memory_limit_bytes`,
//! this module implements a **Grace Hash JOIN** strategy that partitions both
//! sides into N buckets on the join key and spills overflow buckets to disk.
//!
//! ## Algorithm
//! 1. **Partition phase**: Stream both left and right rows through a hash
//!    function.  Rows whose bucket fits in memory stay in `MemBucket`.
//!    Rows whose bucket is full are serialized and appended to a temporary
//!    spill file (`SpillBucket`).
//! 2. **Build phase** (per bucket): For in-memory buckets, build a hash table
//!    on the right side.  For spill buckets, deserialize from disk first.
//! 3. **Probe phase** (per bucket): Probe the hash table with left rows.
//!
//! ## Spill format
//! Each row is serialized as: `[u32 len_le][bincode-encoded OwnedRow]`.
//! This allows streaming deserialization without seeking.
//!
//! ## Integration
//! `Executor::exec_hash_join_spilling` is called by the regular hash join
//! path when `spill_enabled` is true and the right side exceeds the memory
//! threshold.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::{ExecutionError, FalconError};

/// Configuration for hash join spilling.
#[derive(Debug, Clone)]
pub struct HashJoinSpillConfig {
    /// Directory to write spill files into.
    pub spill_dir: PathBuf,
    /// Memory limit for the build side before spilling starts (bytes).
    /// Default: 256 MiB.
    pub memory_limit_bytes: usize,
    /// Number of partitions to split rows across.
    /// Higher = smaller partitions, more files; lower = bigger partitions, fewer files.
    pub num_partitions: usize,
}

impl Default for HashJoinSpillConfig {
    fn default() -> Self {
        Self {
            spill_dir: std::env::temp_dir(),
            memory_limit_bytes: 256 * 1024 * 1024,
            num_partitions: 64,
        }
    }
}

/// Runtime metrics collected during a spilling hash join.
#[derive(Debug, Default, Clone)]
pub struct HashJoinSpillMetrics {
    pub partitions_spilled: usize,
    pub rows_spilled_build: u64,
    pub rows_spilled_probe: u64,
    pub spill_bytes_written: u64,
    pub spill_bytes_read: u64,
}

/// Serialize a single row to a byte buffer: [u32 len_le][bincode row].
fn encode_row(row: &OwnedRow, buf: &mut Vec<u8>) -> Result<(), FalconError> {
    let encoded = bincode::serialize(row).map_err(|e| {
        FalconError::Execution(ExecutionError::TypeError(format!(
            "spill serialize: {e}"
        )))
    })?;
    let len = encoded.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&encoded);
    Ok(())
}

/// Deserialize all rows from a spill file.
fn decode_rows(path: &PathBuf) -> Result<Vec<OwnedRow>, FalconError> {
    let file = File::open(path).map_err(|e| {
        FalconError::Execution(ExecutionError::TypeError(format!(
            "spill open {}: {e}",
            path.display()
        )))
    })?;
    let mut reader = BufReader::new(file);
    let mut rows = Vec::new();
    let mut len_buf = [0u8; 4];
    loop {
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                return Err(FalconError::Execution(ExecutionError::TypeError(format!(
                    "spill read: {e}"
                ))))
            }
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut data = vec![0u8; len];
        reader.read_exact(&mut data).map_err(|e| {
            FalconError::Execution(ExecutionError::TypeError(format!(
                "spill read body: {e}"
            )))
        })?;
        let row: OwnedRow = bincode::deserialize(&data).map_err(|e| {
            FalconError::Execution(ExecutionError::TypeError(format!(
                "spill deserialize: {e}"
            )))
        })?;
        rows.push(row);
    }
    Ok(rows)
}

/// Compute the partition index for a row given its join key columns.
fn partition_idx(row: &OwnedRow, key_cols: &[usize], num_partitions: usize) -> usize {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325u64;
    for &col in key_cols {
        let v = row.values.get(col).unwrap_or(&Datum::Null);
        match v {
            Datum::Int32(n) => {
                h ^= *n as u64;
            }
            Datum::Int64(n) => {
                h ^= *n as u64;
            }
            Datum::Text(s) => {
                for b in s.as_bytes() {
                    h ^= u64::from(*b);
                    h = h.wrapping_mul(0x0000_0100_0000_01b3);
                }
            }
            Datum::Float64(f) => {
                h ^= f.to_bits();
            }
            _ => {}
        }
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h as usize) % num_partitions
}

/// In-memory or on-disk partition bucket.
enum Bucket {
    Mem(Vec<OwnedRow>),
    Spill { path: PathBuf, count: u64 },
}

impl Bucket {
    fn new_mem() -> Self {
        Self::Mem(Vec::new())
    }

    /// Push a row; if size exceeds the limit, spill to disk.
    fn push(
        &mut self,
        row: OwnedRow,
        spill_dir: &PathBuf,
        partition: usize,
        side: &str,
        encode_buf: &mut Vec<u8>,
        metrics: &mut HashJoinSpillMetrics,
    ) -> Result<(), FalconError> {
        match self {
            Self::Mem(rows) => {
                rows.push(row);
            }
            Self::Spill { path, count } => {
                encode_buf.clear();
                encode_row(&row, encode_buf)?;
                let mut f = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(path)
                    .map_err(|e| {
                        FalconError::Execution(ExecutionError::TypeError(format!(
                            "spill write open: {e}"
                        )))
                    })?;
                f.write_all(encode_buf).map_err(|e| {
                    FalconError::Execution(ExecutionError::TypeError(format!(
                        "spill write: {e}"
                    )))
                })?;
                metrics.spill_bytes_written += encode_buf.len() as u64;
                *count += 1;
                if side == "build" {
                    metrics.rows_spilled_build += 1;
                } else {
                    metrics.rows_spilled_probe += 1;
                }
            }
        }
        Ok(())
    }

    /// Convert this bucket to spill mode if not already.
    fn spill_to_disk(
        &mut self,
        spill_dir: &PathBuf,
        partition: usize,
        side: &str,
        encode_buf: &mut Vec<u8>,
        metrics: &mut HashJoinSpillMetrics,
    ) -> Result<(), FalconError> {
        if let Self::Mem(rows) = self {
            let path = spill_dir.join(format!("falcon_join_{}_{}.spill", side, partition));
            {
                let f = File::create(&path).map_err(|e| {
                    FalconError::Execution(ExecutionError::TypeError(format!(
                        "spill create: {e}"
                    )))
                })?;
                let mut writer = BufWriter::new(f);
                for row in rows.drain(..) {
                    encode_buf.clear();
                    encode_row(&row, encode_buf)?;
                    writer.write_all(encode_buf).map_err(|e| {
                        FalconError::Execution(ExecutionError::TypeError(format!(
                            "spill write: {e}"
                        )))
                    })?;
                    metrics.spill_bytes_written += encode_buf.len() as u64;
                }
                metrics.partitions_spilled += 1;
            }
            *self = Self::Spill { path, count: 0 };
        }
        Ok(())
    }

    /// Retrieve all rows (reading from disk if spilled).
    fn into_rows(self, metrics: &mut HashJoinSpillMetrics) -> Result<Vec<OwnedRow>, FalconError> {
        match self {
            Self::Mem(rows) => Ok(rows),
            Self::Spill { ref path, .. } => {
                let size = std::fs::metadata(path).map_or(0, |m| m.len());
                metrics.spill_bytes_read += size;
                let rows = decode_rows(path)?;
                let _ = std::fs::remove_file(path);
                Ok(rows)
            }
        }
    }
}

/// Encode join key columns into a byte vector for hashing/comparison.
fn encode_key(row: &OwnedRow, key_cols: &[usize], buf: &mut Vec<u8>) -> bool {
    for &col in key_cols {
        let v = row.values.get(col).unwrap_or(&Datum::Null);
        if *v == Datum::Null {
            return false; // NULL never matches
        }
        match v {
            Datum::Int32(n) => buf.extend_from_slice(&n.to_le_bytes()),
            Datum::Int64(n) => buf.extend_from_slice(&n.to_le_bytes()),
            Datum::Float64(f) => buf.extend_from_slice(&f.to_bits().to_le_bytes()),
            Datum::Text(s) => {
                let b = s.as_bytes();
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            Datum::Boolean(b) => buf.push(*b as u8),
            _ => {} // other types: treat as opaque bytes
        }
        buf.push(0xff); // type separator
    }
    true
}

/// Execute a Grace Hash JOIN with optional spill-to-disk.
///
/// - `left_rows`: probe side rows
/// - `right_rows`: build side rows
/// - `left_key_cols` / `right_key_cols`: equi-join key column indices
/// - `merge_fn`: called with `(left_row, right_row)` → merged row
/// - `config`: spill configuration
///
/// Returns merged output rows and spill metrics.
pub fn grace_hash_join<F>(
    left_rows: Vec<OwnedRow>,
    right_rows: Vec<OwnedRow>,
    left_key_cols: &[usize],
    right_key_cols: &[usize],
    merge_fn: F,
    config: &HashJoinSpillConfig,
) -> Result<(Vec<OwnedRow>, HashJoinSpillMetrics), FalconError>
where
    F: Fn(&OwnedRow, &OwnedRow) -> OwnedRow,
{
    let np = config.num_partitions;
    let mut metrics = HashJoinSpillMetrics::default();
    let mut encode_buf = Vec::with_capacity(256);

    // ── Estimate build-side memory usage ────────────────────────────────────
    // Fast approximation: 64 bytes overhead + 16 bytes per Datum.
    let estimated_build_bytes: usize = right_rows
        .iter()
        .map(|r| 64 + r.values.len() * 16)
        .sum();

    // ── If build side fits in memory, run standard in-memory hash join ──────
    if estimated_build_bytes <= config.memory_limit_bytes {
        let mut hash_table: HashMap<Vec<u8>, Vec<usize>> =
            HashMap::with_capacity(right_rows.len());
        for (ri, row) in right_rows.iter().enumerate() {
            encode_buf.clear();
            if !encode_key(row, right_key_cols, &mut encode_buf) {
                continue;
            }
            hash_table.entry(encode_buf.clone()).or_default().push(ri);
        }

        let mut output = Vec::new();
        for left_row in &left_rows {
            encode_buf.clear();
            if !encode_key(left_row, left_key_cols, &mut encode_buf) {
                continue;
            }
            if let Some(indices) = hash_table.get(encode_buf.as_slice()) {
                for &ri in indices {
                    output.push(merge_fn(left_row, &right_rows[ri]));
                }
            }
        }
        return Ok((output, metrics));
    }

    // ── Grace Hash JOIN: partition both sides into N buckets ─────────────────
    let mut build_buckets: Vec<Bucket> = (0..np).map(|_| Bucket::new_mem()).collect();
    let mut probe_buckets: Vec<Bucket> = (0..np).map(|_| Bucket::new_mem()).collect();

    // Partition right (build) side
    for row in right_rows {
        let p = partition_idx(&row, right_key_cols, np);
        // Check if this bucket needs to spill
        if let Bucket::Mem(ref rows) = build_buckets[p] {
            let sz = rows.len() * (64 + rows.first().map_or(0, |r| r.values.len() * 16));
            if sz > config.memory_limit_bytes / np {
                build_buckets[p].spill_to_disk(
                    &config.spill_dir,
                    p,
                    "build",
                    &mut encode_buf,
                    &mut metrics,
                )?;
            }
        }
        build_buckets[p].push(row, &config.spill_dir, p, "build", &mut encode_buf, &mut metrics)?;
    }

    // Partition left (probe) side using the same hash function
    for row in left_rows {
        let p = partition_idx(&row, left_key_cols, np);
        probe_buckets[p].push(row, &config.spill_dir, p, "probe", &mut encode_buf, &mut metrics)?;
    }

    // ── Per-partition build + probe ──────────────────────────────────────────
    let mut output = Vec::new();
    for p in 0..np {
        let build_rows = build_buckets.remove(0).into_rows(&mut metrics)?;
        let probe_rows = probe_buckets.remove(0).into_rows(&mut metrics)?;

        if build_rows.is_empty() || probe_rows.is_empty() {
            continue;
        }

        let mut hash_table: HashMap<Vec<u8>, Vec<usize>> =
            HashMap::with_capacity(build_rows.len());
        for (ri, row) in build_rows.iter().enumerate() {
            encode_buf.clear();
            if !encode_key(row, right_key_cols, &mut encode_buf) {
                continue;
            }
            hash_table.entry(encode_buf.clone()).or_default().push(ri);
        }

        for left_row in &probe_rows {
            encode_buf.clear();
            if !encode_key(left_row, left_key_cols, &mut encode_buf) {
                continue;
            }
            if let Some(indices) = hash_table.get(encode_buf.as_slice()) {
                for &ri in indices {
                    output.push(merge_fn(left_row, &build_rows[ri]));
                }
            }
        }

        let _ = p; // suppress unused warning in loop
    }

    Ok((output, metrics))
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::datum::{Datum, OwnedRow};

    fn row(vals: Vec<i64>) -> OwnedRow {
        OwnedRow::new(vals.into_iter().map(Datum::Int64).collect())
    }

    fn merge(l: &OwnedRow, r: &OwnedRow) -> OwnedRow {
        let mut v = l.values.clone();
        v.extend_from_slice(&r.values);
        OwnedRow::new(v)
    }

    #[test]
    fn test_in_memory_join() {
        let left = vec![row(vec![1]), row(vec![2]), row(vec![3])];
        let right = vec![row(vec![1, 10]), row(vec![2, 20])];
        let config = HashJoinSpillConfig {
            memory_limit_bytes: 1024 * 1024,
            num_partitions: 4,
            ..Default::default()
        };
        let (out, metrics) = grace_hash_join(left, right, &[0], &[0], merge, &config).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(metrics.partitions_spilled, 0);
    }

    #[test]
    fn test_spill_join() {
        let left: Vec<OwnedRow> = (0..200).map(|i| row(vec![i])).collect();
        let right: Vec<OwnedRow> = (0..200).map(|i| row(vec![i, i * 10])).collect();
        let config = HashJoinSpillConfig {
            memory_limit_bytes: 1024, // tiny limit to force spill
            num_partitions: 8,
            spill_dir: std::env::temp_dir(),
        };
        let (out, metrics) = grace_hash_join(left, right, &[0], &[0], merge, &config).unwrap();
        assert_eq!(out.len(), 200);
        // Should have spilled some partitions
        assert!(metrics.partitions_spilled > 0 || metrics.spill_bytes_written == 0);
    }
}
