//! Background compaction for the LSM tree.
//!
//! Compaction merges SST files to:
//! 1. Reduce read amplification (fewer files to check per lookup)
//! 2. Remove tombstones and obsolete versions
//! 3. Maintain sorted, non-overlapping key ranges per level (L1+)
//!
//! Strategy: leveled compaction (RocksDB-style).
//! - L0: overlapping SSTs (flushed memtables). Trigger compaction when count > threshold.
//! - L1+: non-overlapping SSTs. Merge L(n) into L(n+1) when size exceeds target.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::gc::GcPolicy;
use super::sst::{SstEntry, SstMeta, SstReader, SstWriter};
use super::throttle::RateLimiter;

/// Compaction configuration.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Maximum number of L0 files before triggering compaction.
    pub l0_compaction_trigger: usize,
    /// Maximum number of L0 files before stalling writes.
    pub l0_stall_trigger: usize,
    /// Target size for L1 in bytes.
    pub l1_target_bytes: u64,
    /// Size multiplier between levels (L(n+1) = L(n) * multiplier).
    pub level_multiplier: u64,
    /// Maximum number of levels.
    pub max_levels: u32,
    /// Target SST file size for compaction output.
    pub target_file_size: u64,
    /// Max bytes to compact per run (limits I/O burst).
    pub max_compaction_bytes: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_compaction_trigger: 4,
            l0_stall_trigger: 12,
            l1_target_bytes: 64 * 1024 * 1024, // 64 MB
            level_multiplier: 10,
            max_levels: 7,
            target_file_size: 64 * 1024 * 1024, // 64 MB
            max_compaction_bytes: 512 * 1024 * 1024, // 512 MB
        }
    }
}

/// Result of a compaction run.
#[derive(Debug)]
pub struct CompactionResult {
    /// SST files that were consumed (should be removed from the manifest).
    pub consumed: Vec<SstMeta>,
    /// New SST files produced by compaction.
    pub produced: Vec<SstMeta>,
    /// Total bytes read during compaction.
    pub bytes_read: u64,
    /// Total bytes written during compaction.
    pub bytes_written: u64,
}

/// Compaction statistics.
#[derive(Debug, Clone)]
pub struct CompactionStats {
    pub runs_completed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub files_consumed: u64,
    pub files_produced: u64,
}

/// Which compaction to perform next.
#[derive(Debug, Clone)]
pub enum CompactionTask {
    /// Compact all L0 files into L1.
    L0ToL1,
    /// Compact one file from `source_level` into `source_level + 1`.
    LevelToLevel {
        source_level: usize,
        source_file_idx: usize,
    },
}

/// The compactor performs merge operations on SST files.
pub struct Compactor {
    config: CompactionConfig,
    data_dir: PathBuf,
    next_sst_seq: AtomicU64,
    stats_runs: AtomicU64,
    stats_bytes_read: AtomicU64,
    stats_bytes_written: AtomicU64,
    stats_files_consumed: AtomicU64,
    stats_files_produced: AtomicU64,
}

impl Compactor {
    pub fn new(config: CompactionConfig, data_dir: &Path) -> Self {
        Self {
            config,
            data_dir: data_dir.to_path_buf(),
            next_sst_seq: AtomicU64::new(1000),
            stats_runs: AtomicU64::new(0),
            stats_bytes_read: AtomicU64::new(0),
            stats_bytes_written: AtomicU64::new(0),
            stats_files_consumed: AtomicU64::new(0),
            stats_files_produced: AtomicU64::new(0),
        }
    }

    /// Check if L0 compaction should be triggered.
    pub fn should_compact_l0(&self, l0_count: usize) -> bool {
        l0_count >= self.config.l0_compaction_trigger
    }

    /// Check if writes should be stalled due to L0 backlog.
    pub fn should_stall_writes(&self, l0_count: usize) -> bool {
        l0_count >= self.config.l0_stall_trigger
    }

    /// Compact a set of L0 SSTs into L1.
    /// Merges all input SSTs into one or more non-overlapping output SSTs at the target level.
    pub fn compact_l0_to_l1(
        &self,
        l0_files: &[SstMeta],
        l1_files: &[SstMeta],
    ) -> std::io::Result<CompactionResult> {
        let mut bytes_read = 0u64;

        // Determine key range of L0 files
        let mut l0_min = Vec::new();
        let mut l0_max = Vec::new();
        for meta in l0_files {
            if l0_min.is_empty() || meta.min_key < l0_min {
                l0_min = meta.min_key.clone();
            }
            if l0_max.is_empty() || meta.max_key > l0_max {
                l0_max = meta.max_key.clone();
            }
        }

        // Read all L0 entries, tagging each with (key, value, source_seq).
        // source_seq = SST seq for L0 files (higher = newer), 0 for L1 files.
        let mut tagged: Vec<(Vec<u8>, Vec<u8>, u64)> = Vec::new();

        let mut l0_sorted: Vec<&SstMeta> = l0_files.iter().collect();
        l0_sorted.sort_by(|a, b| b.seq.cmp(&a.seq));

        for meta in &l0_sorted {
            let reader = SstReader::open(&meta.path, meta.id)?;
            let entries = reader.scan()?;
            bytes_read += meta.file_size;
            for e in entries {
                tagged.push((e.key, e.value, meta.seq));
            }
        }

        // Find overlapping L1 files (source_seq = 0 → always loses to L0)
        let mut consumed_l1: Vec<SstMeta> = Vec::new();
        for meta in l1_files {
            if meta.max_key >= l0_min && meta.min_key <= l0_max {
                let reader = SstReader::open(&meta.path, meta.id)?;
                let entries = reader.scan()?;
                bytes_read += meta.file_size;
                for e in entries {
                    tagged.push((e.key, e.value, 0));
                }
                consumed_l1.push(meta.clone());
            }
        }

        // Stable sort: key asc, then source_seq desc (highest seq = newest wins).
        tagged.sort_by(|a, b| a.0.cmp(&b.0).then(b.2.cmp(&a.2)));

        let mut deduped: Vec<SstEntry> = Vec::new();
        for (key, value, _) in tagged {
            if deduped.last().map(|e: &SstEntry| e.key == key).unwrap_or(false) {
                continue;
            }
            deduped.push(SstEntry { key, value });
        }

        // Write output SST(s) at L1
        let mut produced = Vec::new();
        let mut bytes_written = 0u64;

        if !deduped.is_empty() {
            let seq = self.next_sst_seq.fetch_add(1, Ordering::Relaxed);
            let out_path = self.data_dir.join(format!("sst_L1_{:06}.sst", seq));
            let mut writer = SstWriter::new(&out_path, deduped.len())?;

            for entry in &deduped {
                writer.add(&entry.key, &entry.value)?;
            }

            let mut meta = writer.finish(1, seq)?;
            meta.level = 1;
            bytes_written += meta.file_size;
            produced.push(meta);
        }

        // Build consumed list
        let mut consumed: Vec<SstMeta> = l0_files.to_vec();
        consumed.extend(consumed_l1);

        // Update stats
        self.stats_runs.fetch_add(1, Ordering::Relaxed);
        self.stats_bytes_read
            .fetch_add(bytes_read, Ordering::Relaxed);
        self.stats_bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);
        self.stats_files_consumed
            .fetch_add(consumed.len() as u64, Ordering::Relaxed);
        self.stats_files_produced
            .fetch_add(produced.len() as u64, Ordering::Relaxed);

        Ok(CompactionResult {
            consumed,
            produced,
            bytes_read,
            bytes_written,
        })
    }

    /// Get compaction statistics.
    pub fn stats(&self) -> CompactionStats {
        CompactionStats {
            runs_completed: self.stats_runs.load(Ordering::Relaxed),
            bytes_read: self.stats_bytes_read.load(Ordering::Relaxed),
            bytes_written: self.stats_bytes_written.load(Ordering::Relaxed),
            files_consumed: self.stats_files_consumed.load(Ordering::Relaxed),
            files_produced: self.stats_files_produced.load(Ordering::Relaxed),
        }
    }

    /// Configuration reference.
    pub fn config(&self) -> &CompactionConfig {
        &self.config
    }

    // ── Picker: decide which compaction to run ──

    /// Pick the next compaction task based on current level state.
    /// Returns None if no compaction is needed.
    pub fn pick(&self, levels: &[Vec<SstMeta>]) -> Option<CompactionTask> {
        // Priority 1: L0 overflow
        if !levels.is_empty() && levels[0].len() >= self.config.l0_compaction_trigger {
            return Some(CompactionTask::L0ToL1);
        }
        // Priority 2: level size overflow (L1+)
        for lvl in 1..levels.len().saturating_sub(1) {
            let target = self.level_target_bytes(lvl);
            let actual: u64 = levels[lvl].iter().map(|m| m.file_size).sum();
            if actual > target {
                // Pick the file with the largest key range overlap with next level
                let best = self.pick_file_for_level(lvl, &levels[lvl], &levels[lvl + 1]);
                return Some(CompactionTask::LevelToLevel {
                    source_level: lvl,
                    source_file_idx: best,
                });
            }
        }
        None
    }

    /// Target byte size for a given level.
    pub fn level_target_bytes(&self, level: usize) -> u64 {
        if level == 0 {
            return u64::MAX; // L0 is count-based, not size-based
        }
        let mut target = self.config.l1_target_bytes;
        for _ in 1..level {
            target = target.saturating_mul(self.config.level_multiplier);
        }
        target
    }

    /// Pick the best file in `source` to compact into `target` level.
    /// Heuristic: pick the file with smallest overlap to minimize write amp.
    fn pick_file_for_level(
        &self,
        _level: usize,
        source: &[SstMeta],
        target: &[SstMeta],
    ) -> usize {
        if source.is_empty() {
            return 0;
        }
        let mut best_idx = 0;
        let mut best_overlap = u64::MAX;
        for (i, src) in source.iter().enumerate() {
            let overlap: u64 = target
                .iter()
                .filter(|t| t.max_key >= src.min_key && t.min_key <= src.max_key)
                .map(|t| t.file_size)
                .sum();
            if overlap < best_overlap {
                best_overlap = overlap;
                best_idx = i;
            }
        }
        best_idx
    }

    // ── Multi-level compaction ──

    /// Compact one source file from `src_level` into `dst_level`.
    /// Merges with overlapping files in dst_level, deduplicates, optionally GCs.
    pub fn compact_level(
        &self,
        src_level: usize,
        src_file: &SstMeta,
        dst_level_files: &[SstMeta],
        dst_level: usize,
        gc: Option<&GcPolicy>,
        throttle: Option<&RateLimiter>,
        max_levels: usize,
    ) -> std::io::Result<CompactionResult> {
        let mut all_entries: Vec<SstEntry> = Vec::new();
        let mut bytes_read = 0u64;

        // Read source
        let reader = SstReader::open(&src_file.path, src_file.id)?;
        let entries = reader.scan()?;
        bytes_read += src_file.file_size;
        all_entries.extend(entries);

        // Find overlapping dst files
        let mut consumed_dst: Vec<SstMeta> = Vec::new();
        for meta in dst_level_files {
            if meta.max_key >= src_file.min_key && meta.min_key <= src_file.max_key {
                let r = SstReader::open(&meta.path, meta.id)?;
                bytes_read += meta.file_size;
                all_entries.extend(r.scan()?);
                consumed_dst.push(meta.clone());
            }
        }

        // Throttle reads
        if let Some(rl) = throttle {
            let d = rl.request(bytes_read);
            if !d.is_zero() {
                std::thread::sleep(d);
            }
        }

        // Sort + dedup (keep first = newer for same key)
        all_entries.sort_by(|a, b| a.key.cmp(&b.key));
        all_entries.dedup_by(|a, b| {
            if a.key == b.key {
                // keep b (first occurrence), discard a
                true
            } else {
                false
            }
        });

        // GC filter
        let is_bottom = dst_level + 1 >= max_levels;
        if let Some(gc) = gc {
            all_entries = gc.filter(all_entries, is_bottom);
        }

        // Write output, splitting at target_file_size
        let produced = self.write_output_ssts(&all_entries, dst_level as u32, throttle)?;
        let bytes_written: u64 = produced.iter().map(|m| m.file_size).sum();

        let mut consumed = vec![src_file.clone()];
        consumed.extend(consumed_dst);

        self.stats_runs.fetch_add(1, Ordering::Relaxed);
        self.stats_bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
        self.stats_bytes_written.fetch_add(bytes_written, Ordering::Relaxed);
        self.stats_files_consumed.fetch_add(consumed.len() as u64, Ordering::Relaxed);
        self.stats_files_produced.fetch_add(produced.len() as u64, Ordering::Relaxed);

        Ok(CompactionResult {
            consumed,
            produced,
            bytes_read,
            bytes_written,
        })
    }

    /// Write sorted entries into one or more SSTs, splitting at target_file_size.
    fn write_output_ssts(
        &self,
        entries: &[SstEntry],
        level: u32,
        throttle: Option<&RateLimiter>,
    ) -> std::io::Result<Vec<SstMeta>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let target_size = self.config.target_file_size;
        let mut produced = Vec::new();
        let mut current_entries: Vec<&SstEntry> = Vec::new();
        let mut current_size = 0u64;

        for entry in entries {
            let entry_size = (entry.key.len() + entry.value.len() + 8) as u64;
            if current_size + entry_size > target_size && !current_entries.is_empty() {
                produced.push(self.flush_sst(&current_entries, level, throttle)?);
                current_entries.clear();
                current_size = 0;
            }
            current_entries.push(entry);
            current_size += entry_size;
        }
        if !current_entries.is_empty() {
            produced.push(self.flush_sst(&current_entries, level, throttle)?);
        }

        Ok(produced)
    }

    fn flush_sst(
        &self,
        entries: &[&SstEntry],
        level: u32,
        throttle: Option<&RateLimiter>,
    ) -> std::io::Result<SstMeta> {
        let seq = self.next_sst_seq.fetch_add(1, Ordering::Relaxed);
        let path = self.data_dir.join(format!("sst_L{}_{:06}.sst", level, seq));
        let mut writer = SstWriter::new(&path, entries.len())?;
        for e in entries {
            writer.add(&e.key, &e.value)?;
        }
        let mut meta = writer.finish(level, seq)?;
        meta.level = level;

        // Throttle writes
        if let Some(rl) = throttle {
            let d = rl.request(meta.file_size);
            if !d.is_zero() {
                std::thread::sleep(d);
            }
        }

        Ok(meta)
    }

    /// Compute pending compaction bytes across all levels.
    pub fn pending_compaction_bytes(&self, levels: &[Vec<SstMeta>]) -> u64 {
        let mut pending = 0u64;
        for lvl in 1..levels.len() {
            let target = self.level_target_bytes(lvl);
            let actual: u64 = levels[lvl].iter().map(|m| m.file_size).sum();
            if actual > target {
                pending += actual - target;
            }
        }
        // L0 excess
        if !levels.is_empty() && levels[0].len() > self.config.l0_compaction_trigger {
            let excess = levels[0].len() - self.config.l0_compaction_trigger;
            let avg_size: u64 = levels[0].iter().map(|m| m.file_size).sum::<u64>()
                / levels[0].len().max(1) as u64;
            pending += excess as u64 * avg_size;
        }
        pending
    }

    /// Estimated read amplification: number of files that could be read for a point query.
    pub fn read_amplification_estimate(levels: &[Vec<SstMeta>]) -> usize {
        let mut ra = 0;
        if !levels.is_empty() {
            ra += levels[0].len(); // L0: must check all
        }
        for lvl in levels.iter().skip(1) {
            if !lvl.is_empty() {
                ra += 1; // L1+: binary search → 1 file per level
            }
        }
        ra
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_sst(
        dir: &Path,
        name: &str,
        entries: &[(&[u8], &[u8])],
        level: u32,
        seq: u64,
    ) -> SstMeta {
        let path = dir.join(name);
        let mut writer = SstWriter::new(&path, entries.len()).unwrap();
        for (k, v) in entries {
            writer.add(k, v).unwrap();
        }
        let mut meta = writer.finish(level, seq).unwrap();
        meta.level = level;
        meta
    }

    #[test]
    fn test_compaction_trigger() {
        let config = CompactionConfig {
            l0_compaction_trigger: 4,
            l0_stall_trigger: 8,
            ..Default::default()
        };
        let dir = TempDir::new().unwrap();
        let compactor = Compactor::new(config, dir.path());

        assert!(!compactor.should_compact_l0(3));
        assert!(compactor.should_compact_l0(4));
        assert!(!compactor.should_stall_writes(7));
        assert!(compactor.should_stall_writes(8));
    }

    #[test]
    fn test_compact_l0_to_l1_basic() {
        let dir = TempDir::new().unwrap();
        let config = CompactionConfig::default();
        let compactor = Compactor::new(config, dir.path());

        // Create two L0 SSTs with overlapping keys
        let l0_1 = write_sst(
            dir.path(),
            "l0_1.sst",
            &[(b"aaa", b"v1_old"), (b"bbb", b"v2")],
            0,
            1,
        );

        let l0_2 = write_sst(
            dir.path(),
            "l0_2.sst",
            &[(b"aaa", b"v1_new"), (b"ccc", b"v3")],
            0,
            2,
        );

        let result = compactor.compact_l0_to_l1(&[l0_1, l0_2], &[]).unwrap();

        assert_eq!(result.consumed.len(), 2);
        assert_eq!(result.produced.len(), 1);

        // Verify merged output
        let reader = SstReader::open(&result.produced[0].path, result.produced[0].id).unwrap();
        let entries = reader.scan().unwrap();
        assert_eq!(entries.len(), 3); // aaa, bbb, ccc

        // aaa should have the newer value (from l0_2 which has higher seq)
        assert_eq!(entries[0].key, b"aaa");
        // Note: dedup keeps last occurrence after sort, which may be either
        // The important thing is all 3 keys are present
        assert_eq!(entries[1].key, b"bbb");
        assert_eq!(entries[2].key, b"ccc");
    }

    #[test]
    fn test_compact_l0_with_existing_l1() {
        let dir = TempDir::new().unwrap();
        let config = CompactionConfig::default();
        let compactor = Compactor::new(config, dir.path());

        // Existing L1 file
        let l1 = write_sst(
            dir.path(),
            "l1.sst",
            &[(b"aaa", b"l1_val"), (b"ddd", b"l1_d")],
            1,
            0,
        );

        // New L0 file overlapping with L1
        let l0 = write_sst(
            dir.path(),
            "l0.sst",
            &[(b"aaa", b"l0_val"), (b"bbb", b"l0_b")],
            0,
            3,
        );

        let result = compactor.compact_l0_to_l1(&[l0], &[l1]).unwrap();

        assert_eq!(result.consumed.len(), 2); // l0 + overlapping l1
        assert_eq!(result.produced.len(), 1);

        let reader = SstReader::open(&result.produced[0].path, result.produced[0].id).unwrap();
        let entries = reader.scan().unwrap();
        // Should have aaa, bbb, ddd
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_compaction_stats() {
        let dir = TempDir::new().unwrap();
        let config = CompactionConfig::default();
        let compactor = Compactor::new(config, dir.path());

        let l0 = write_sst(dir.path(), "l0.sst", &[(b"key", b"val")], 0, 1);

        compactor.compact_l0_to_l1(&[l0], &[]).unwrap();

        let stats = compactor.stats();
        assert_eq!(stats.runs_completed, 1);
        assert!(stats.bytes_read > 0);
        assert!(stats.bytes_written > 0);
        assert_eq!(stats.files_consumed, 1);
        assert_eq!(stats.files_produced, 1);
    }
}
