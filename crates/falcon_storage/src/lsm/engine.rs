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

use super::block_cache::{BlockCache, BlockCacheSnapshot, BlockKey, CachedBlock};
use super::compaction::{CompactionConfig, CompactionStats, CompactionTask, Compactor};
use super::gc::GcPolicy;
use super::manifest::Manifest;
use super::memtable::LsmMemTable;
use super::sst::{SstMeta, SstReader, SstWriter};
use super::throttle::{BackpressureConfig, BackpressureController, BackpressureLevel, RateLimiter};
use super::workers::BgWorkers;

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
    /// Backpressure thresholds.
    pub backpressure: BackpressureConfig,
    /// Flush I/O rate limit (bytes/sec, 0 = unlimited).
    pub flush_rate_bytes_per_sec: u64,
    /// Compaction I/O rate limit (bytes/sec, 0 = unlimited).
    pub compaction_rate_bytes_per_sec: u64,
    /// MVCC GC safepoint timestamp (0 = disabled).
    pub gc_safepoint: u64,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            memtable_budget_bytes: 64 * 1024 * 1024, // 64 MB
            block_cache_bytes: 128 * 1024 * 1024,    // 128 MB
            compaction: CompactionConfig::default(),
            sync_writes: true,
            backpressure: BackpressureConfig::default(),
            flush_rate_bytes_per_sec: 0,
            compaction_rate_bytes_per_sec: 0,
            gc_safepoint: 0,
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
    pub writes_delayed: u64,
    pub block_cache: BlockCacheSnapshot,
    pub compaction: CompactionStats,
    pub read_amplification: usize,
    pub pending_compaction_bytes: u64,
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
    /// Persistent manifest log.
    manifest: Mutex<Manifest>,
    /// Backpressure controller.
    backpressure: BackpressureController,
    /// Flush rate limiter.
    flush_limiter: RateLimiter,
    /// Compaction rate limiter.
    compaction_limiter: RateLimiter,
    /// MVCC GC policy.
    gc_policy: GcPolicy,

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
    /// Optional background flush/compaction workers (set after construction).
    bg_workers: RwLock<Option<BgWorkers>>,
    /// Optional TDE block encryption for SST files (interior-mutable for post-Arc setup).
    block_crypto: RwLock<Option<std::sync::Arc<dyn super::sst::BlockCrypto>>>,
}


impl LsmEngine {
    /// Open or create an LSM engine at the given directory.
    pub fn open(data_dir: &Path, config: LsmConfig) -> io::Result<Self> {
        fs::create_dir_all(data_dir)?;

        let block_cache = Arc::new(BlockCache::new(config.block_cache_bytes));
        let compactor = Arc::new(Compactor::new(config.compaction.clone(), data_dir));
        let manifest = Manifest::open(data_dir)?;
        let backpressure = BackpressureController::new(config.backpressure.clone());
        let flush_limiter = RateLimiter::new(config.flush_rate_bytes_per_sec);
        let compaction_limiter = RateLimiter::new(config.compaction_rate_bytes_per_sec);
        let gc_policy = GcPolicy::new(config.gc_safepoint);

        let max_levels = config.compaction.max_levels as usize;

        // Try manifest replay first, fall back to filename-based recovery
        let mut levels = Manifest::replay(data_dir, max_levels)?;
        let has_manifest_data = levels.iter().any(|l| !l.is_empty());
        if !has_manifest_data {
            let recovered = Self::recover_sst_files(data_dir, max_levels)?;
            for (i, files) in recovered.into_iter().enumerate() {
                if i < levels.len() {
                    levels[i] = files;
                }
            }
        }

        // Determine starting seq from existing SSTs
        let max_seq = levels.iter().flat_map(|l| l.iter()).map(|m| m.seq).max().unwrap_or(0);

        Ok(Self {
            config,
            data_dir: data_dir.to_path_buf(),
            active_memtable: RwLock::new(Arc::new(LsmMemTable::new())),
            frozen_memtables: RwLock::new(Vec::new()),
            levels: RwLock::new(levels),
            block_cache,
            compactor,
            manifest: Mutex::new(manifest),
            backpressure,
            flush_limiter,
            compaction_limiter,
            gc_policy,
            next_seq: AtomicU64::new(max_seq + 1),
            flushes_completed: AtomicU64::new(0),
            writes_stalled: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            flush_lock: Mutex::new(()),
            bg_workers: RwLock::new(None),
            block_crypto: RwLock::new(None),
        })
    }

    /// Attach background flush/compaction workers.
    pub fn set_bg_workers(&self, workers: BgWorkers) {
        *self.bg_workers.write() = Some(workers);
    }

    /// Enable TDE block encryption for SST files.
    pub fn set_block_crypto(&self, crypto: std::sync::Arc<dyn super::sst::BlockCrypto>) {
        *self.block_crypto.write() = Some(crypto);
    }

    /// Get a clone of the block crypto Arc (for passing to SstWriter/SstReader).
    pub fn block_crypto(&self) -> Option<std::sync::Arc<dyn super::sst::BlockCrypto>> {
        self.block_crypto.read().clone()
    }

    /// Create an in-memory-only LSM engine (for testing).
    pub fn open_in_memory() -> io::Result<Self> {
        let dir = std::env::temp_dir().join(format!("falcon_lsm_test_{}", std::process::id()));
        Self::open(
            &dir,
            LsmConfig {
                memtable_budget_bytes: 4 * 1024 * 1024,
                block_cache_bytes: 8 * 1024 * 1024,
                compaction: CompactionConfig {
                    l0_compaction_trigger: 4,
                    l0_stall_trigger: 8,
                    ..Default::default()
                },
                sync_writes: false,
                ..Default::default()
            },
        )
    }

    // ── Write Path ──────────────────────────────────────────────────────

    /// Put a key-value pair into the LSM engine.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.check_backpressure()?;

        let memtable = self.active_memtable.read().clone();
        memtable
            .put(key.to_vec(), value.to_vec())
            .map_err(|_| io::Error::other("memtable frozen during write"))?;

        self.maybe_trigger_flush();
        Ok(())
    }

    /// Delete a key (insert a tombstone).
    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        self.check_backpressure()?;

        let memtable = self.active_memtable.read().clone();
        memtable
            .delete(key.to_vec())
            .map_err(|_| io::Error::other("memtable frozen during write"))?;

        self.maybe_trigger_flush();
        Ok(())
    }

    /// Batch put: apply multiple key-value pairs with one lock per shard.
    pub fn put_batch(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> io::Result<()> {
        self.check_backpressure()?;
        let memtable = self.active_memtable.read().clone();
        memtable
            .put_batch(pairs)
            .map_err(|_| io::Error::other("memtable frozen during batch write"))?;
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

        let mut writer = SstWriter::new_with_crypto(
            &sst_path, entries.len(), self.block_crypto(),
        )?;
        for (key, value, _seq) in &entries {
            let val = value.as_deref().unwrap_or(b"");
            writer.add(key, val)?;
        }
        let meta = writer.finish(0, seq)?;

        // Throttle flush I/O
        let d = self.flush_limiter.request(meta.file_size);
        if !d.is_zero() {
            std::thread::sleep(d);
        }

        // Record in manifest
        self.manifest.lock().record_add(&meta)?;

        // Add to L0
        self.levels.write()[0].push(meta);

        // Remove from frozen list
        self.frozen_memtables
            .write()
            .retain(|m| !Arc::ptr_eq(m, &frozen));

        self.flushes_completed.fetch_add(1, Ordering::Relaxed);
        self.update_backpressure_counters();

        // Maybe trigger compaction
        self.maybe_trigger_compaction()?;

        Ok(())
    }

    // ── Compaction ──────────────────────────────────────────────────────

    /// Run compaction if needed, using the picker to select tasks.
    pub fn maybe_trigger_compaction(&self) -> io::Result<()> {
        let task = {
            let levels = self.levels.read();
            self.compactor.pick(&levels)
        };

        let task = match task {
            Some(t) => t,
            None => return Ok(()),
        };

        match task {
            CompactionTask::L0ToL1 => self.run_l0_compaction(),
            CompactionTask::LevelToLevel { source_level, source_file_idx } => {
                self.run_level_compaction(source_level, source_file_idx)
            }
        }
    }

    fn run_l0_compaction(&self) -> io::Result<()> {
        let (l0_files, l1_files) = {
            let levels = self.levels.read();
            (
                levels[0].clone(),
                levels.get(1).cloned().unwrap_or_default(),
            )
        };

        let max_levels = self.config.compaction.max_levels as usize;
        let crypto = self.block_crypto();
        let result = self.compactor.compact_l0_to_l1(
            &l0_files,
            &l1_files,
            Some(&self.gc_policy),
            max_levels,
            Some(&self.compaction_limiter),
            crypto.as_ref(),
        )?;
        self.apply_compaction_result(&result, 0, 1)?;
        Ok(())
    }

    fn run_level_compaction(&self, src_level: usize, src_file_idx: usize) -> io::Result<()> {
        let (src_file, dst_files, max_levels) = {
            let levels = self.levels.read();
            let dst_level = src_level + 1;
            if src_level >= levels.len() || dst_level >= levels.len() {
                return Ok(());
            }
            if src_file_idx >= levels[src_level].len() {
                return Ok(());
            }
            (
                levels[src_level][src_file_idx].clone(),
                levels[dst_level].clone(),
                levels.len(),
            )
        };

        let dst_level = src_level + 1;
        let result = self.compactor.compact_level(
            src_level,
            &src_file,
            &dst_files,
            dst_level,
            Some(&self.gc_policy),
            Some(&self.compaction_limiter),
            max_levels,
            self.block_crypto().as_ref(),
        )?;
        self.apply_compaction_result(&result, src_level, dst_level)?;
        Ok(())
    }

    fn apply_compaction_result(
        &self,
        result: &super::compaction::CompactionResult,
        src_level: usize,
        dst_level: usize,
    ) -> io::Result<()> {
        // Update manifest
        {
            let mut mf = self.manifest.lock();
            for meta in &result.consumed {
                mf.record_remove(meta)?;
            }
            for meta in &result.produced {
                mf.record_add(meta)?;
            }
        }

        // Update in-memory levels
        let consumed_paths: Vec<PathBuf> = result.consumed.iter().map(|m| m.path.clone()).collect();
        {
            let mut levels = self.levels.write();
            if src_level < levels.len() {
                levels[src_level].retain(|m| !consumed_paths.contains(&m.path));
            }
            if dst_level < levels.len() {
                levels[dst_level].retain(|m| !consumed_paths.contains(&m.path));
                levels[dst_level].extend(result.produced.clone());
                levels[dst_level].sort_by(|a, b| a.min_key.cmp(&b.min_key));
            }
        }

        // Clean up consumed SST files
        for meta in &result.consumed {
            let _ = fs::remove_file(&meta.path);
        }

        self.update_backpressure_counters();
        Ok(())
    }

    // ── Statistics ──────────────────────────────────────────────────────

    /// Get a snapshot of engine statistics.
    pub fn stats(&self) -> LsmStats {
        let active = self.active_memtable.read();
        let frozen = self.frozen_memtables.read();
        let levels = self.levels.read();

        let total_sst_files: usize = levels.iter().map(|l| l.len()).sum();
        let total_sst_bytes: u64 = levels
            .iter()
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
            writes_delayed: self.backpressure.writes_delayed(),
            block_cache: self.block_cache.snapshot(),
            compaction: self.compactor.stats(),
            read_amplification: Compactor::read_amplification_estimate(&levels),
            pending_compaction_bytes: self.compactor.pending_compaction_bytes(&levels),
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

    /// Full scan: merge memtable + frozen memtables + all SST levels.
    /// Returns (key, value) pairs sorted by key, deduplicated (newest wins).
    /// Tombstones (empty value) are excluded from the result.
    pub fn scan_all(&self) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Memtable entries carry real seq numbers (always higher than any SST seq).
        let mut entries = self.active_memtable_snapshot();

        // SST entries get a virtual seq based on level + file seq so that
        // L0 (newest flush) beats L1+, and higher SST seq beats lower.
        // All virtual seqs are capped below the minimum memtable seq (u64::MAX >> 2).
        let base: u64 = 1 << 40; // large enough to stay below memtable seqs
        let levels = self.levels.read();
        for (level_idx, level) in levels.iter().enumerate() {
            for meta in level {
                // L0 gets higher virtual seq than L1+ (level_idx 0 → highest).
                // Within L0, higher SST seq = newer flush.
                let virtual_seq = base
                    .saturating_sub(level_idx as u64 * (1 << 20))
                    .saturating_add(meta.seq);
                let reader = SstReader::open_with_crypto(&meta.path, meta.id, self.block_crypto())?;
                for e in reader.scan()? {
                    entries.push((e.key, if e.value.is_empty() { None } else { Some(e.value) }, virtual_seq));
                }
            }
        }
        drop(levels);

        // Sort by key then seq descending; dedup keeps the first (newest) per key.
        entries.sort_by(|a, b| a.0.cmp(&b.0).then(b.2.cmp(&a.2)));
        entries.dedup_by(|a, b| a.0 == b.0);

        Ok(entries
            .into_iter()
            .filter_map(|(k, v, _)| v.map(|val| (k, val)))
            .collect())
    }

    /// Range scan: same merge logic as scan_all but filtered to [start, end).
    /// Pass `None` for an open-ended bound.
    pub fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let in_range = |k: &[u8]| -> bool {
            start.map_or(true, |s| k >= s) && end.map_or(true, |e| k < e)
        };

        let mut entries: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = self
            .active_memtable_snapshot()
            .into_iter()
            .filter(|(k, _, _)| in_range(k))
            .collect();

        let base: u64 = 1 << 40;
        let levels = self.levels.read();
        for (level_idx, level) in levels.iter().enumerate() {
            for meta in level {
                // Skip SST files whose key range doesn't overlap the query range.
                if let Some(e) = end {
                    if meta.min_key.as_slice() >= e {
                        continue;
                    }
                }
                if let Some(s) = start {
                    if !meta.max_key.is_empty() && meta.max_key.as_slice() < s {
                        continue;
                    }
                }
                let virtual_seq = base
                    .saturating_sub(level_idx as u64 * (1 << 20))
                    .saturating_add(meta.seq);
                let reader = SstReader::open_with_crypto(&meta.path, meta.id, self.block_crypto())?;
                for e in reader.scan()? {
                    if in_range(&e.key) {
                        entries.push((e.key, if e.value.is_empty() { None } else { Some(e.value) }, virtual_seq));
                    }
                }
            }
        }
        drop(levels);

        entries.sort_by(|a, b| a.0.cmp(&b.0).then(b.2.cmp(&a.2)));
        entries.dedup_by(|a, b| a.0 == b.0);

        Ok(entries
            .into_iter()
            .filter_map(|(k, v, _)| v.map(|val| (k, val)))
            .collect())
    }

    /// Shutdown the engine, flushing any pending data.
    pub fn shutdown(&self) -> io::Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        self.flush()
    }

    // ── Internal helpers ────────────────────────────────────────────────

    fn get_from_sst(&self, meta: &SstMeta, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let reader = SstReader::open_with_crypto(&meta.path, meta.id, self.block_crypto())?;

        // Bloom + index → find the block that may contain the key
        let Some((block_offset, block_len)) = reader.find_block_for_key(key) else {
            return Ok(None);
        };

        // Check cache with real (sst_id, block_offset)
        let bk = BlockKey { sst_id: meta.id, offset: block_offset };
        if let Some(cached) = self.block_cache.get(&bk) {
            return SstReader::search_block_pub(&cached.data, key);
        }

        // Cache miss: read block from disk, cache it, then search
        let block_data = reader.read_block_raw(block_offset, block_len)?;
        let result = SstReader::search_block_pub(&block_data, key)?;
        self.block_cache.insert(bk, CachedBlock { data: block_data });
        Ok(result)
    }

    fn maybe_trigger_flush(&self) {
        let needs_flush = self.active_memtable.read().approx_bytes()
            >= self.config.memtable_budget_bytes;
        if !needs_flush {
            return;
        }
        // Wake the background flush worker; it will call flush() which handles
        // the freeze+swap+SST-write sequence under flush_lock.
        if let Some(bg) = self.bg_workers.read().as_ref() {
            bg.notify_flush();
        } else {
            // No background worker attached — fall back to inline flush.
            let _ = self.flush();
        }
    }

    fn check_backpressure(&self) -> io::Result<()> {
        match self.backpressure.level() {
            BackpressureLevel::Normal => Ok(()),
            BackpressureLevel::Delayed => {
                self.backpressure.check_write().map_err(io::Error::from)
            }
            BackpressureLevel::Stopped => {
                self.writes_stalled.fetch_add(1, Ordering::Relaxed);
                self.backpressure.check_write().map_err(io::Error::from)
            }
        }
    }

    fn update_backpressure_counters(&self) {
        let levels = self.levels.read();
        let frozen_count = self.frozen_memtables.read().len();
        self.backpressure.update_immutable_count(frozen_count);
        if !levels.is_empty() {
            self.backpressure.update_l0_count(levels[0].len());
        }
        self.backpressure.update_compaction_backlog(
            self.compactor.pending_compaction_bytes(&levels),
        );
    }

    /// Update the MVCC GC safepoint.
    pub fn set_gc_safepoint(&self, ts: u64) {
        self.gc_policy.set_safe_gc_ts(ts);
    }

    /// Get the backpressure controller reference.
    pub fn backpressure(&self) -> &BackpressureController {
        &self.backpressure
    }

    /// Compact the manifest log (rewrite from current state).
    pub fn compact_manifest(&self) -> io::Result<()> {
        let levels = self.levels.read();
        self.manifest.lock().compact(&levels)
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

            let filename = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");

            // Parse level from filename: sst_L{level}_{seq}.sst
            let level = if filename.starts_with("sst_L") {
                filename
                    .get(5..6)
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
        LsmEngine::open(
            dir,
            LsmConfig {
                memtable_budget_bytes: 4096, // small for testing
                block_cache_bytes: 1024 * 1024,
                compaction: CompactionConfig {
                    l0_compaction_trigger: 4,
                    l0_stall_trigger: 20,
                    ..Default::default()
                },
                sync_writes: false,
                ..Default::default()
            },
        )
        .unwrap()
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
        let engine = LsmEngine::open(
            dir.path(),
            LsmConfig {
                memtable_budget_bytes: 200, // very small
                block_cache_bytes: 1024 * 1024,
                compaction: CompactionConfig {
                    l0_compaction_trigger: 100,
                    l0_stall_trigger: 200,
                    ..Default::default()
                },
                sync_writes: false,
                ..Default::default()
            },
        )
        .unwrap();

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
        let engine = LsmEngine::open(
            dir.path(),
            LsmConfig {
                memtable_budget_bytes: 2048,
                block_cache_bytes: 1024 * 1024,
                compaction: CompactionConfig {
                    l0_compaction_trigger: 4,
                    l0_stall_trigger: 50,
                    ..Default::default()
                },
                sync_writes: false,
                ..Default::default()
            },
        )
        .unwrap();

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
        let engine = LsmEngine::open(
            dir.path(),
            LsmConfig {
                memtable_budget_bytes: 512,
                block_cache_bytes: 1024 * 1024,
                compaction: CompactionConfig {
                    l0_compaction_trigger: 3,
                    l0_stall_trigger: 50,
                    ..Default::default()
                },
                sync_writes: false,
                ..Default::default()
            },
        )
        .unwrap();

        // Write enough to trigger multiple flushes and compaction
        for i in 0..200 {
            let key = format!("k_{:06}", i);
            let val = format!("v_{:06}", i);
            engine.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let stats = engine.stats();
        assert!(
            stats.compaction.runs_completed > 0 || stats.flushes_completed >= 3,
            "expected compaction or multiple flushes"
        );
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

    #[test]
    fn test_lsm_manifest_recovery() {
        let dir = TempDir::new().unwrap();
        {
            let engine = test_engine(dir.path());
            for i in 0..30 {
                let key = format!("mk_{:04}", i);
                engine.put(key.as_bytes(), b"v").unwrap();
            }
            engine.flush().unwrap();
            // Second batch → second SST
            for i in 30..60 {
                let key = format!("mk_{:04}", i);
                engine.put(key.as_bytes(), b"v2").unwrap();
            }
            engine.flush().unwrap();
        }
        // Manifest should have recorded both SSTs
        let manifest_path = dir.path().join("MANIFEST");
        assert!(manifest_path.exists());

        // Reopen via manifest replay
        let engine = test_engine(dir.path());
        assert_eq!(engine.get(b"mk_0000").unwrap(), Some(b"v".to_vec()));
        assert_eq!(engine.get(b"mk_0059").unwrap(), Some(b"v2".to_vec()));
        let stats = engine.stats();
        assert!(stats.l0_file_count >= 2 || stats.total_sst_files >= 2);
    }

    #[test]
    fn test_lsm_manifest_compact() {
        let dir = TempDir::new().unwrap();
        let engine = test_engine(dir.path());
        for i in 0..20 {
            let key = format!("mc_{:04}", i);
            engine.put(key.as_bytes(), b"x").unwrap();
        }
        engine.flush().unwrap();
        engine.compact_manifest().unwrap();

        // Reopen should still work
        drop(engine);
        let engine = test_engine(dir.path());
        assert_eq!(engine.get(b"mc_0000").unwrap(), Some(b"x".to_vec()));
    }

    #[test]
    fn test_lsm_stats_extended() {
        let dir = TempDir::new().unwrap();
        let engine = test_engine(dir.path());
        engine.put(b"s1", b"v1").unwrap();
        engine.flush().unwrap();
        let stats = engine.stats();
        assert_eq!(stats.writes_delayed, 0);
        assert!(stats.read_amplification >= 0);
    }

    #[test]
    fn test_lsm_gc_safepoint() {
        let dir = TempDir::new().unwrap();
        let engine = test_engine(dir.path());
        engine.set_gc_safepoint(100);
        engine.put(b"gc1", b"v1").unwrap();
        assert_eq!(engine.get(b"gc1").unwrap(), Some(b"v1".to_vec()));
    }

    #[test]
    fn test_lsm_seq_continues_after_reopen() {
        let dir = TempDir::new().unwrap();
        let seq1;
        {
            let engine = test_engine(dir.path());
            engine.put(b"r1", b"v1").unwrap();
            engine.flush().unwrap();
            seq1 = engine.stats().seq;
        }
        {
            let engine = test_engine(dir.path());
            assert!(engine.stats().seq >= seq1, "seq should be >= after reopen");
        }
    }
}
