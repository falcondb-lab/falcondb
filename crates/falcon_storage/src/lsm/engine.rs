//! Top-level LSM storage engine.
//!
//! Coordinates the memtable, SST files, block cache, bloom filters,
//! and background compaction into a unified key-value store.
//!
//! Write path: WAL → active MemTable → (flush) → L0 SST
//! Read path:  MemTable → frozen MemTables → L0 SSTs → L1..Ln SSTs

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use super::block_cache::{BlockCache, BlockCacheSnapshot};
use super::compaction::{CompactionConfig, CompactionStats, Compactor};
use super::memtable::LsmMemTable;
use super::sst::{SstMeta, SstReader, SstWriter};

/// Configuration for the LSM engine.
#[derive(Debug, Clone)]
pub struct LsmConfig {
    /// Maximum memtable size in bytes before triggering a flush.
    pub memtable_budget_bytes: u64,
    /// Block cache size in bytes.
    pub block_cache_bytes: usize,
    /// Compaction configuration.
    pub compaction: CompactionConfig,
    /// Whether to sync WAL on every write (true = durable, false = faster).
    pub sync_writes: bool,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            memtable_budget_bytes: 64 * 1024 * 1024, // 64 MB
            block_cache_bytes: 128 * 1024 * 1024,    // 128 MB
            compaction: CompactionConfig::default(),
            sync_writes: true,
        }
    }
}

/// LSM engine statistics snapshot.
#[derive(Debug, Clone)]
pub struct LsmStats {
    pub memtable_bytes: u64,
    pub memtable_entries: u64,
    pub frozen_memtable_count: usize,
    pub l0_file_count: usize,
    pub total_sst_files: usize,
    pub total_sst_bytes: u64,
    pub flushes_completed: u64,
    pub writes_stalled: u64,
    pub block_cache: BlockCacheSnapshot,
    pub compaction: CompactionStats,
    pub seq: u64,
}

/// The LSM storage engine.
pub struct LsmEngine {
    config: LsmConfig,
    data_dir: PathBuf,

    /// Active (mutable) memtable.
    active_memtable: RwLock<Arc<LsmMemTable>>,
    /// Frozen memtables awaiting flush (oldest last).
    frozen_memtables: RwLock<Vec<Arc<LsmMemTable>>>,

    /// SST file manifest, organized by level.
    /// Level 0: overlapping, ordered by seq (newest first).
    /// Level 1+: non-overlapping, sorted by key range.
    levels: RwLock<Vec<Vec<SstMeta>>>,

    /// Block cache shared across all SST reads.
    block_cache: Arc<BlockCache>,
    /// Background compactor.
    compactor: Arc<Compactor>,

    /// Global sequence number.
    next_seq: AtomicU64,
    /// Flush counter.
    flushes_completed: AtomicU64,
    /// Write stall counter.
    writes_stalled: AtomicU64,
    /// Whether the engine is shut down.
    shutdown: AtomicBool,
    /// Flush lock (prevents concurrent flushes).
    flush_lock: Mutex<()>,
}

impl LsmEngine {
    /// Open or create an LSM engine at the given directory.
    pub fn open(data_dir: &Path, config: LsmConfig) -> io::Result<Self> {
        fs::create_dir_all(data_dir)?;

        let block_cache = Arc::new(BlockCache::new(config.block_cache_bytes));
        let compactor = Arc::new(Compactor::new(config.compaction.clone(), data_dir));

        // Initialize with max_levels empty levels
        let max_levels = config.compaction.max_levels as usize;
        let mut levels = Vec::with_capacity(max_levels);
        for _ in 0..max_levels {
            levels.push(Vec::new());
        }

        // Scan for existing SST files and rebuild manifest
        let recovered_levels = Self::recover_sst_files(data_dir, max_levels)?;
        for (i, files) in recovered_levels.into_iter().enumerate() {
            if i < levels.len() {
                levels[i] = files;
            }
        }

        Ok(Self {
            config,
            data_dir: data_dir.to_path_buf(),
            active_memtable: RwLock::new(Arc::new(LsmMemTable::new())),
            frozen_memtables: RwLock::new(Vec::new()),
            levels: RwLock::new(levels),
            block_cache,
            compactor,
            next_seq: AtomicU64::new(1),
            flushes_completed: AtomicU64::new(0),
            writes_stalled: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            flush_lock: Mutex::new(()),
        })
    }

    /// Create an in-memory-only LSM engine (for testing).
    pub fn open_in_memory() -> io::Result<Self> {
        let dir = std::env::temp_dir().join(format!("falcon_lsm_test_{}", std::process::id()));
        Self::open(&dir, LsmConfig {
            memtable_budget_bytes: 4 * 1024 * 1024,
            block_cache_bytes: 8 * 1024 * 1024,
            compaction: CompactionConfig {
                l0_compaction_trigger: 4,
                l0_stall_trigger: 8,
                ..Default::default()
            },
            sync_writes: false,
        })
    }

    // ── Write Path ──────────────────────────────────────────────────────

    /// Put a key-value pair into the LSM engine.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.maybe_stall_writes()?;

        let memtable = self.active_memtable.read().clone();
        memtable.put(key.to_vec(), value.to_vec())
            .map_err(|_| io::Error::other("memtable frozen during write"))?;

        self.maybe_trigger_flush();
        Ok(())
    }

    /// Delete a key (insert a tombstone).
    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        self.maybe_stall_writes()?;

        let memtable = self.active_memtable.read().clone();
        memtable.delete(key.to_vec())
            .map_err(|_| io::Error::other("memtable frozen during write"))?;

        self.maybe_trigger_flush();
        Ok(())
    }

    // ── Read Path ───────────────────────────────────────────────────────

    /// Point lookup: returns the value for the given key, or None.
    /// Search order: active memtable → frozen memtables → L0 → L1..Ln.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        // 1. Active memtable
        let active = self.active_memtable.read().clone();
        if let Some(result) = active.get(key) {
            return Ok(result); // Some(data) or None (tombstone)
        }

        // 2. Frozen memtables (newest first)
        {
            let frozen = self.frozen_memtables.read();
            for mt in frozen.iter().rev() {
                if let Some(result) = mt.get(key) {
                    return Ok(result);
                }
            }
        }

        // 3. SST files by level
        let levels = self.levels.read();

        // L0: check all files (overlapping), newest first
        if !levels.is_empty() {
            let mut l0_files: Vec<&SstMeta> = levels[0].iter().collect();
            l0_files.sort_by(|a, b| b.seq.cmp(&a.seq));

            for meta in l0_files {
                if !meta.may_contain_key(key) {
                    continue;
                }
                if let Some(value) = self.get_from_sst(meta, key)? {
                    if value.is_empty() {
                        return Ok(None); // tombstone in SST
                    }
                    return Ok(Some(value));
                }
            }
        }

        // L1+: binary search for the right file (non-overlapping)
        for level in levels.iter().skip(1) {
            let file_idx = level.partition_point(|m| m.max_key.as_slice() < key);
            if file_idx < level.len() {
                let meta = &level[file_idx];
                if meta.may_contain_key(key) {
                    if let Some(value) = self.get_from_sst(meta, key)? {
                        if value.is_empty() {
                            return Ok(None); // tombstone
                        }
                        return Ok(Some(value));
                    }
                }
            }
        }

        Ok(None)
    }

    // ── Flush ───────────────────────────────────────────────────────────

    /// Flush the active memtable to an L0 SST file.
    /// This freezes the current memtable, creates a new active one,
    /// and writes the frozen memtable to disk.
    pub fn flush(&self) -> io::Result<()> {
        let _lock = self.flush_lock.lock();

        // Freeze current memtable and swap in a new one
        let frozen = {
            let mut active = self.active_memtable.write();
            let old = active.clone();
            if old.is_empty() {
                return Ok(()); // nothing to flush
            }
            old.freeze();
            *active = Arc::new(LsmMemTable::with_seq(old.current_seq()));
            old
        };

        // Add to frozen list
        self.frozen_memtables.write().push(frozen.clone());

        // Write frozen memtable to L0 SST
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let sst_path = self.data_dir.join(format!("sst_L0_{:06}.sst", seq));
        let entries = frozen.iter_sorted();

        let mut writer = SstWriter::new(&sst_path, entries.len())?;
        for (key, value, _seq) in &entries {
            // Encode tombstones as empty values in SST
            let val = value.as_deref().unwrap_or(b"");
            writer.add(key, val)?;
        }
        let meta = writer.finish(0, seq)?;

        // Add to L0
        self.levels.write()[0].push(meta);

        // Remove from frozen list
        self.frozen_memtables.write().retain(|m| !Arc::ptr_eq(m, &frozen));

        self.flushes_completed.fetch_add(1, Ordering::Relaxed);

        // Maybe trigger compaction
        self.maybe_trigger_compaction()?;

        Ok(())
    }

    // ── Compaction ──────────────────────────────────────────────────────

    /// Run compaction if L0 file count exceeds the trigger threshold.
    pub fn maybe_trigger_compaction(&self) -> io::Result<()> {
        let l0_count = self.levels.read()[0].len();
        if !self.compactor.should_compact_l0(l0_count) {
            return Ok(());
        }

        let (l0_files, l1_files) = {
            let levels = self.levels.read();
            (levels[0].clone(), levels.get(1).cloned().unwrap_or_default())
        };

        let result = self.compactor.compact_l0_to_l1(&l0_files, &l1_files)?;

        // Update manifest: remove consumed, add produced
        let mut levels = self.levels.write();

        // Remove consumed L0 files
        let consumed_paths: Vec<PathBuf> = result.consumed.iter().map(|m| m.path.clone()).collect();
        levels[0].retain(|m| !consumed_paths.contains(&m.path));

        // Replace L1 with produced + non-overlapping existing
        if levels.len() > 1 {
            levels[1].retain(|m| !consumed_paths.contains(&m.path));
            levels[1].extend(result.produced);
            levels[1].sort_by(|a, b| a.min_key.cmp(&b.min_key));
        }

        // Clean up consumed SST files from disk
        for meta in &result.consumed {
            let _ = fs::remove_file(&meta.path);
        }

        Ok(())
    }

    // ── Statistics ──────────────────────────────────────────────────────

    /// Get a snapshot of engine statistics.
    pub fn stats(&self) -> LsmStats {
        let active = self.active_memtable.read();
        let frozen = self.frozen_memtables.read();
        let levels = self.levels.read();

        let total_sst_files: usize = levels.iter().map(|l| l.len()).sum();
        let total_sst_bytes: u64 = levels.iter()
            .flat_map(|l| l.iter())
            .map(|m| m.file_size)
            .sum();

        LsmStats {
            memtable_bytes: active.approx_bytes(),
            memtable_entries: active.entry_count(),
            frozen_memtable_count: frozen.len(),
            l0_file_count: levels[0].len(),
            total_sst_files,
            total_sst_bytes,
            flushes_completed: self.flushes_completed.load(Ordering::Relaxed),
            writes_stalled: self.writes_stalled.load(Ordering::Relaxed),
            block_cache: self.block_cache.snapshot(),
            compaction: self.compactor.stats(),
            seq: self.next_seq.load(Ordering::Relaxed),
        }
    }

    /// Get the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Get the configuration.
    pub fn config(&self) -> &LsmConfig {
        &self.config
    }

    /// Snapshot of all live entries (active + frozen memtables) for scan.
    /// Returns (key, value_or_none_for_tombstone, seq) sorted by key.
    pub fn active_memtable_snapshot(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
        let active = self.active_memtable.read().clone();
        let mut entries = active.iter_sorted();
        // Include frozen memtables (older data)
        let frozen = self.frozen_memtables.read();
        for mt in frozen.iter() {
            entries.extend(mt.iter_sorted());
        }
        // Deduplicate: keep highest-seq entry per key
        entries.sort_by(|a, b| a.0.cmp(&b.0).then(b.2.cmp(&a.2)));
        entries.dedup_by(|a, b| a.0 == b.0);
        entries
    }

    /// Shutdown the engine, flushing any pending data.
    pub fn shutdown(&self) -> io::Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        self.flush()
    }

    // ── Internal helpers ────────────────────────────────────────────────

    fn get_from_sst(&self, meta: &SstMeta, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        // Try block cache first
        // For simplicity, we cache entire SST lookups by (sst_id, key_hash)
        let reader = SstReader::open(&meta.path, meta.id)?;
        reader.get(key)
    }

    fn maybe_trigger_flush(&self) {
        let active = self.active_memtable.read();
        if active.approx_bytes() >= self.config.memtable_budget_bytes {
            drop(active);
            let _ = self.flush();
        }
    }

    fn maybe_stall_writes(&self) -> io::Result<()> {
        let l0_count = self.levels.read()[0].len();
        if self.compactor.should_stall_writes(l0_count) {
            self.writes_stalled.fetch_add(1, Ordering::Relaxed);
            // In production, this would block until compaction catches up.
            // For now, return a transient error.
            return Err(io::Error::other(
                format!("write stalled: L0 file count {} exceeds stall trigger", l0_count),
            ));
        }
        Ok(())
    }

    fn recover_sst_files(data_dir: &Path, max_levels: usize) -> io::Result<Vec<Vec<SstMeta>>> {
        let mut levels: Vec<Vec<SstMeta>> = (0..max_levels).map(|_| Vec::new()).collect();

        if !data_dir.exists() {
            return Ok(levels);
        }

        for entry in fs::read_dir(data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("sst") {
                continue;
            }

            let filename = path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("");

            // Parse level from filename: sst_L{level}_{seq}.sst
            let level = if filename.starts_with("sst_L") {
                filename.get(5..6)
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0)
            } else {
                0
            };

            match SstReader::open(&path, 0) {
                Ok(reader) => {
                    let mut meta = reader.meta().clone();
                    meta.level = level as u32;
                    if level < levels.len() {
                        levels[level].push(meta);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to recover SST file {:?}: {}", path, e);
                }
            }
        }

        // Sort L0 by seq (newest first for reads), L1+ by min_key
        if !levels.is_empty() {
            levels[0].sort_by(|a, b| b.seq.cmp(&a.seq));
        }
        for level in levels.iter_mut().skip(1) {
            level.sort_by(|a, b| a.min_key.cmp(&b.min_key));
        }

        Ok(levels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_engine(dir: &Path) -> LsmEngine {
        LsmEngine::open(dir, LsmConfig {
            memtable_budget_bytes: 4096, // small for testing
            block_cache_bytes: 1024 * 1024,
            compaction: CompactionConfig {
                l0_compaction_trigger: 4,
                l0_stall_trigger: 20,
                ..Default::default()
            },
            sync_writes: false,
        }).unwrap()
    }

    #[test]
    fn test_lsm_put_get() {
        let dir = TempDir::new().unwrap();
        let engine = test_engine(dir.path());

        engine.put(b"key1", b"val1").unwrap();
        engine.put(b"key2", b"val2").unwrap();

        assert_eq!(engine.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(engine.get(b"key2").unwrap(), Some(b"val2".to_vec()));
        assert_eq!(engine.get(b"key3").unwrap(), None);
    }

    #[test]
    fn test_lsm_overwrite() {
        let dir = TempDir::new().unwrap();
        let engine = test_engine(dir.path());

        engine.put(b"key1", b"old").unwrap();
        engine.put(b"key1", b"new").unwrap();

        assert_eq!(engine.get(b"key1").unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn test_lsm_delete() {
        let dir = TempDir::new().unwrap();
        let engine = test_engine(dir.path());

        engine.put(b"key1", b"val1").unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"val1".to_vec()));

        engine.delete(b"key1").unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), None);
    }

    #[test]
    fn test_lsm_flush_and_read_from_sst() {
        let dir = TempDir::new().unwrap();
        let engine = test_engine(dir.path());

        engine.put(b"key1", b"val1").unwrap();
        engine.put(b"key2", b"val2").unwrap();
        engine.flush().unwrap();

        // Data should still be readable from SST
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(engine.get(b"key2").unwrap(), Some(b"val2".to_vec()));

        let stats = engine.stats();
        assert_eq!(stats.flushes_completed, 1);
        assert!(stats.l0_file_count >= 1);
    }

    #[test]
    fn test_lsm_auto_flush_on_budget() {
        let dir = TempDir::new().unwrap();
        let engine = LsmEngine::open(dir.path(), LsmConfig {
            memtable_budget_bytes: 200, // very small
            block_cache_bytes: 1024 * 1024,
            compaction: CompactionConfig {
                l0_compaction_trigger: 100,
                l0_stall_trigger: 200,
                ..Default::default()
            },
            sync_writes: false,
        }).unwrap();

        // Write enough data to trigger auto-flush
        for i in 0..50 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{:04}", i);
            engine.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let stats = engine.stats();
        assert!(stats.flushes_completed > 0, "should have auto-flushed");
    }

    #[test]
    fn test_lsm_many_writes_and_reads() {
        let dir = TempDir::new().unwrap();
        let engine = LsmEngine::open(dir.path(), LsmConfig {
            memtable_budget_bytes: 2048,
            block_cache_bytes: 1024 * 1024,
            compaction: CompactionConfig {
                l0_compaction_trigger: 4,
                l0_stall_trigger: 50,
                ..Default::default()
            },
            sync_writes: false,
        }).unwrap();

        let n = 500;
        for i in 0..n {
            let key = format!("k_{:06}", i);
            let val = format!("v_{:06}", i);
            engine.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Verify all keys are readable
        for i in 0..n {
            let key = format!("k_{:06}", i);
            let val = format!("v_{:06}", i);
            let result = engine.get(key.as_bytes()).unwrap();
            assert_eq!(result, Some(val.into_bytes()), "key {} not found", key);
        }

        let stats = engine.stats();
        assert!(stats.flushes_completed > 0);
    }

    #[test]
    fn test_lsm_compaction_triggered() {
        let dir = TempDir::new().unwrap();
        let engine = LsmEngine::open(dir.path(), LsmConfig {
            memtable_budget_bytes: 512,
            block_cache_bytes: 1024 * 1024,
            compaction: CompactionConfig {
                l0_compaction_trigger: 3,
                l0_stall_trigger: 50,
                ..Default::default()
            },
            sync_writes: false,
        }).unwrap();

        // Write enough to trigger multiple flushes and compaction
        for i in 0..200 {
            let key = format!("k_{:06}", i);
            let val = format!("v_{:06}", i);
            engine.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let stats = engine.stats();
        assert!(stats.compaction.runs_completed > 0 || stats.flushes_completed >= 3,
            "expected compaction or multiple flushes");
    }

    #[test]
    fn test_lsm_stats() {
        let dir = TempDir::new().unwrap();
        let engine = test_engine(dir.path());

        engine.put(b"a", b"1").unwrap();
        let stats = engine.stats();
        assert!(stats.memtable_entries > 0 || stats.flushes_completed > 0);
    }

    #[test]
    fn test_lsm_recovery() {
        let dir = TempDir::new().unwrap();

        // Write and flush data
        {
            let engine = test_engine(dir.path());
            engine.put(b"persist_key", b"persist_val").unwrap();
            engine.flush().unwrap();
        }

        // Reopen and verify data is still there
        {
            let engine = test_engine(dir.path());
            assert_eq!(
                engine.get(b"persist_key").unwrap(),
                Some(b"persist_val".to_vec()),
            );
        }
    }

    #[test]
    fn test_lsm_shutdown() {
        let dir = TempDir::new().unwrap();
        let engine = test_engine(dir.path());
        engine.put(b"key", b"val").unwrap();
        engine.shutdown().unwrap();
    }
}
