//! §C — Zstd Dictionary Lifecycle
//!
//! Manages zstd dictionary training, storage, retrieval, and lifecycle.
//! Dictionaries are stored as special segments in the SegmentStore and
//! tracked in the Manifest via dictionary_id → segment_id mapping.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::unified_data_plane::{
    SegmentStore, SegmentStoreError, UnifiedSegmentHeader, UNIFIED_HEADER_SIZE,
};

/// Dictionary entry in the DictionaryStore.
#[derive(Debug, Clone)]
pub struct DictionaryEntry {
    pub dictionary_id: u64,
    pub segment_id: u64,
    pub table_id: u64,
    pub schema_version: u64,
    pub data: Vec<u8>,
    pub checksum: u32,
    pub created_at_epoch: u64,
    pub sample_count: u64,
}

/// Dictionary store — manages dictionary lifecycle.
pub struct DictionaryStore {
    dictionaries: RwLock<BTreeMap<u64, DictionaryEntry>>,
    table_dict: RwLock<BTreeMap<u64, u64>>,
    next_dict_id: AtomicU64,
    pub metrics: DictionaryMetrics,
}

#[derive(Debug, Default)]
pub struct DictionaryMetrics {
    pub dictionaries_created: AtomicU64,
    pub dictionaries_used: AtomicU64,
    pub dictionary_bytes_total: AtomicU64,
    pub training_runs: AtomicU64,
    pub dict_hit: AtomicU64,
    pub dict_miss: AtomicU64,
}

impl DictionaryMetrics {
    pub fn hit_rate(&self) -> f64 {
        let hit = self.dict_hit.load(Ordering::Relaxed) as f64;
        let miss = self.dict_miss.load(Ordering::Relaxed) as f64;
        if hit + miss == 0.0 {
            return 0.0;
        }
        hit / (hit + miss)
    }
}

impl DictionaryStore {
    pub fn new() -> Self {
        Self {
            dictionaries: RwLock::new(BTreeMap::new()),
            table_dict: RwLock::new(BTreeMap::new()),
            next_dict_id: AtomicU64::new(1),
            metrics: DictionaryMetrics::default(),
        }
    }

    pub fn train_dictionary(
        &self,
        table_id: u64,
        schema_version: u64,
        samples: &[Vec<u8>],
        max_dict_size: usize,
    ) -> Result<DictionaryEntry, String> {
        if samples.is_empty() {
            return Err("no samples for dictionary training".to_owned());
        }
        let dict_data = zstd::dict::from_samples(samples, max_dict_size)
            .map_err(|e| format!("zstd dict training: {e}"))?;
        let dict_id = self.next_dict_id.fetch_add(1, Ordering::Relaxed);
        let checksum = djb2_crc(&dict_data);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entry = DictionaryEntry {
            dictionary_id: dict_id,
            segment_id: 0,
            table_id,
            schema_version,
            data: dict_data,
            checksum,
            created_at_epoch: now,
            sample_count: samples.len() as u64,
        };
        self.metrics
            .dictionaries_created
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .dictionary_bytes_total
            .fetch_add(entry.data.len() as u64, Ordering::Relaxed);
        self.metrics.training_runs.fetch_add(1, Ordering::Relaxed);
        Ok(entry)
    }

    pub fn register(&self, entry: DictionaryEntry) {
        let dict_id = entry.dictionary_id;
        let table_id = entry.table_id;
        self.dictionaries.write().insert(dict_id, entry);
        self.table_dict.write().insert(table_id, dict_id);
    }

    pub fn get(&self, dictionary_id: u64) -> Option<DictionaryEntry> {
        let entry = self.dictionaries.read().get(&dictionary_id).cloned();
        if entry.is_some() {
            self.metrics.dict_hit.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.dict_miss.fetch_add(1, Ordering::Relaxed);
        }
        entry
    }

    pub fn get_for_table(&self, table_id: u64) -> Option<DictionaryEntry> {
        let dict_id = *self.table_dict.read().get(&table_id)?;
        self.get(dict_id)
    }

    pub fn list(&self) -> Vec<u64> {
        self.dictionaries.read().keys().copied().collect()
    }

    pub fn store_as_segment(
        &self,
        entry: &mut DictionaryEntry,
        store: &SegmentStore,
    ) -> Result<u64, SegmentStoreError> {
        let seg_id = store.next_segment_id();
        let hdr = UnifiedSegmentHeader::new_snapshot(
            seg_id,
            entry.data.len() as u64,
            entry.dictionary_id,
            0,
        );
        store.create_segment(hdr)?;
        store.write_chunk(seg_id, &entry.data)?;
        store.seal_segment(seg_id)?;
        entry.segment_id = seg_id;
        Ok(seg_id)
    }

    pub fn load_from_segment(
        &self,
        store: &SegmentStore,
        segment_id: u64,
        dictionary_id: u64,
        table_id: u64,
        schema_version: u64,
    ) -> Result<DictionaryEntry, String> {
        let body = store
            .get_segment_body(segment_id)
            .map_err(|e| format!("load dict segment: {e}"))?;
        let data = if body.len() > UNIFIED_HEADER_SIZE as usize {
            body[UNIFIED_HEADER_SIZE as usize..].to_vec()
        } else {
            return Err("dict segment too small".to_owned());
        };
        let checksum = djb2_crc(&data);
        Ok(DictionaryEntry {
            dictionary_id,
            segment_id,
            table_id,
            schema_version,
            data,
            checksum,
            created_at_epoch: 0,
            sample_count: 0,
        })
    }
}

impl Default for DictionaryStore {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn djb2_crc(data: &[u8]) -> u32 {
    data.iter().fold(5381u32, |acc, &b| {
        acc.wrapping_mul(33).wrapping_add(u32::from(b))
    })
}
