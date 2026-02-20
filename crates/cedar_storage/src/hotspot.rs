//! DK §5: Hotspot detection and mitigation.
//!
//! Tracks access frequency per shard/table/key-range and raises alerts
//! when hotspots are detected. Provides mitigation suggestions.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use cedar_common::kernel::{HotspotAlert, HotspotKind, HotspotMitigation};
use cedar_common::types::ShardId;

/// Configuration for hotspot detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotspotConfig {
    /// Detection window (seconds).
    pub window_secs: u64,
    /// Minimum access count within the window to flag as a hotspot.
    pub shard_threshold: u64,
    /// Table-level hotspot threshold.
    pub table_threshold: u64,
    /// Severity threshold (0.0-1.0) above which mitigations are recommended.
    pub mitigation_severity: f64,
}

impl Default for HotspotConfig {
    fn default() -> Self {
        Self {
            window_secs: 10,
            shard_threshold: 1000,
            table_threshold: 500,
            mitigation_severity: 0.7,
        }
    }
}

/// Per-target access counter (lock-free).
#[derive(Debug)]
struct AccessCounter {
    count: AtomicU64,
    window_start: Instant,
}

impl AccessCounter {
    fn new() -> Self {
        Self { count: AtomicU64::new(0), window_start: Instant::now() }
    }

    fn increment(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn get(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
    }
}

/// Hotspot detector — tracks access frequencies and detects hotspots.
pub struct HotspotDetector {
    config: HotspotConfig,
    /// Shard-level access counters.
    shard_counters: DashMap<ShardId, AccessCounter>,
    /// Table-level access counters (by table name).
    table_counters: DashMap<String, AccessCounter>,
    /// Total alerts generated.
    total_alerts: AtomicU64,
}

impl HotspotDetector {
    pub fn new(config: HotspotConfig) -> Self {
        Self {
            config,
            shard_counters: DashMap::new(),
            table_counters: DashMap::new(),
            total_alerts: AtomicU64::new(0),
        }
    }

    /// Record a shard access.
    pub fn record_shard_access(&self, shard_id: ShardId) {
        self.shard_counters
            .entry(shard_id)
            .or_insert_with(AccessCounter::new)
            .increment();
    }

    /// Record a table access.
    pub fn record_table_access(&self, table_name: &str) {
        self.table_counters
            .entry(table_name.to_string())
            .or_insert_with(AccessCounter::new)
            .increment();
    }

    /// Detect hotspots based on current counters.
    pub fn detect(&self) -> Vec<HotspotAlert> {
        let mut alerts = Vec::new();

        // Find the max shard count for severity normalization
        let max_shard_count = self.shard_counters.iter()
            .map(|e| e.value().get())
            .max()
            .unwrap_or(0);

        // Shard hotspots
        for entry in self.shard_counters.iter() {
            let count = entry.value().get();
            if count >= self.config.shard_threshold {
                let severity = if max_shard_count > 0 {
                    (count as f64 / max_shard_count as f64).min(1.0)
                } else {
                    0.0
                };
                let mitigation = if severity >= self.config.mitigation_severity {
                    HotspotMitigation::WriteThrottle
                } else {
                    HotspotMitigation::None
                };
                alerts.push(HotspotAlert {
                    kind: HotspotKind::Shard,
                    target: format!("shard_{}", entry.key().0),
                    access_count: count,
                    severity,
                    mitigation,
                });
            }
        }

        // Table hotspots
        let max_table_count = self.table_counters.iter()
            .map(|e| e.value().get())
            .max()
            .unwrap_or(0);

        for entry in self.table_counters.iter() {
            let count = entry.value().get();
            if count >= self.config.table_threshold {
                let severity = if max_table_count > 0 {
                    (count as f64 / max_table_count as f64).min(1.0)
                } else {
                    0.0
                };
                let mitigation = if severity >= 0.9 {
                    HotspotMitigation::ReshardAdvice
                } else if severity >= self.config.mitigation_severity {
                    HotspotMitigation::LocalQueue
                } else {
                    HotspotMitigation::None
                };
                alerts.push(HotspotAlert {
                    kind: HotspotKind::Table,
                    target: entry.key().clone(),
                    access_count: count,
                    severity,
                    mitigation,
                });
            }
        }

        self.total_alerts.fetch_add(alerts.len() as u64, Ordering::Relaxed);
        alerts
    }

    /// Reset all counters (start a new detection window).
    pub fn reset_window(&self) {
        for entry in self.shard_counters.iter() {
            entry.value().reset();
        }
        for entry in self.table_counters.iter() {
            entry.value().reset();
        }
    }

    /// Total alerts generated since creation.
    pub fn total_alerts(&self) -> u64 {
        self.total_alerts.load(Ordering::Relaxed)
    }

    /// Number of tracked shards.
    pub fn tracked_shards(&self) -> usize {
        self.shard_counters.len()
    }

    /// Number of tracked tables.
    pub fn tracked_tables(&self) -> usize {
        self.table_counters.len()
    }
}

impl Default for HotspotDetector {
    fn default() -> Self {
        Self::new(HotspotConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_hotspots_below_threshold() {
        let detector = HotspotDetector::new(HotspotConfig {
            shard_threshold: 100,
            table_threshold: 100,
            ..Default::default()
        });
        for _ in 0..50 {
            detector.record_shard_access(ShardId(0));
        }
        let alerts = detector.detect();
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_shard_hotspot_detected() {
        let detector = HotspotDetector::new(HotspotConfig {
            shard_threshold: 100,
            ..Default::default()
        });
        for _ in 0..200 {
            detector.record_shard_access(ShardId(0));
        }
        for _ in 0..10 {
            detector.record_shard_access(ShardId(1));
        }
        let alerts = detector.detect();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].kind, HotspotKind::Shard);
        assert_eq!(alerts[0].target, "shard_0");
        assert_eq!(alerts[0].access_count, 200);
    }

    #[test]
    fn test_table_hotspot_detected() {
        let detector = HotspotDetector::new(HotspotConfig {
            table_threshold: 50,
            ..Default::default()
        });
        for _ in 0..100 {
            detector.record_table_access("orders");
        }
        for _ in 0..10 {
            detector.record_table_access("users");
        }
        let alerts = detector.detect();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].kind, HotspotKind::Table);
        assert_eq!(alerts[0].target, "orders");
    }

    #[test]
    fn test_hotspot_severity_triggers_mitigation() {
        let detector = HotspotDetector::new(HotspotConfig {
            shard_threshold: 10,
            mitigation_severity: 0.7,
            ..Default::default()
        });
        for _ in 0..100 {
            detector.record_shard_access(ShardId(0));
        }
        for _ in 0..10 {
            detector.record_shard_access(ShardId(1));
        }
        let alerts = detector.detect();
        let hot = alerts.iter().find(|a| a.target == "shard_0").unwrap();
        assert_eq!(hot.mitigation, HotspotMitigation::WriteThrottle);
    }

    #[test]
    fn test_reset_window() {
        let detector = HotspotDetector::new(HotspotConfig {
            shard_threshold: 10,
            ..Default::default()
        });
        for _ in 0..50 {
            detector.record_shard_access(ShardId(0));
        }
        assert_eq!(detector.detect().len(), 1);
        detector.reset_window();
        assert_eq!(detector.detect().len(), 0);
    }

    #[test]
    fn test_total_alerts_counter() {
        let detector = HotspotDetector::new(HotspotConfig {
            shard_threshold: 5,
            ..Default::default()
        });
        for _ in 0..10 {
            detector.record_shard_access(ShardId(0));
            detector.record_shard_access(ShardId(1));
        }
        let alerts = detector.detect();
        assert_eq!(alerts.len(), 2);
        assert_eq!(detector.total_alerts(), 2);
    }
}
