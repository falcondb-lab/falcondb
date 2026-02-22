//! DK §9: Self-verification and consistency audit engine.
//!
//! Provides infrastructure for proving correctness:
//! - State hash computation for replica comparison
//! - WAL replay checksum verification
//! - Range hash for partial data verification
//! - Sampled transaction replay
//! - Audit report generation

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use falcon_common::kernel::{VerificationAuditReport, VerificationResult, VerificationType};

/// Configuration for the consistency verifier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifierConfig {
    /// How often to run state hash checks (seconds, 0 = disabled).
    pub state_hash_interval_secs: u64,
    /// How often to run WAL replay checks (seconds, 0 = disabled).
    pub wal_replay_interval_secs: u64,
    /// Fraction of data to sample for range hash (0.0–1.0).
    pub range_hash_sample_ratio: f64,
    /// Maximum number of recent results to keep.
    pub max_results_history: usize,
    /// Whether to enable transaction replay sampling.
    pub txn_replay_enabled: bool,
    /// Fraction of committed transactions to replay-verify.
    pub txn_replay_sample_ratio: f64,
}

impl Default for VerifierConfig {
    fn default() -> Self {
        Self {
            state_hash_interval_secs: 300, // 5 minutes
            wal_replay_interval_secs: 600, // 10 minutes
            range_hash_sample_ratio: 0.1,  // 10%
            max_results_history: 100,
            txn_replay_enabled: false,
            txn_replay_sample_ratio: 0.001, // 0.1%
        }
    }
}

/// Consistency verifier — runs periodic checks and produces audit reports.
pub struct ConsistencyVerifier {
    config: VerifierConfig,
    results: Mutex<Vec<VerificationResult>>,
    total_checks: AtomicU64,
    total_passed: AtomicU64,
    total_failed: AtomicU64,
    anomalies_detected: AtomicU64,
}

impl ConsistencyVerifier {
    pub fn new(config: VerifierConfig) -> Self {
        Self {
            config,
            results: Mutex::new(Vec::new()),
            total_checks: AtomicU64::new(0),
            total_passed: AtomicU64::new(0),
            total_failed: AtomicU64::new(0),
            anomalies_detected: AtomicU64::new(0),
        }
    }

    /// Compute a state hash over a set of key-value pairs.
    /// In production this would hash the actual MVCC data; here we provide
    /// the interface and a reference implementation.
    pub fn compute_state_hash(data: &[(Vec<u8>, Vec<u8>)]) -> u64 {
        let mut hasher = DefaultHasher::new();
        for (k, v) in data {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Verify state hash: compare local hash with an expected hash from a replica.
    pub fn verify_state_hash(
        &self,
        scope: &str,
        local_data: &[(Vec<u8>, Vec<u8>)],
        expected_hash: u64,
        now_unix: u64,
    ) -> VerificationResult {
        let computed = Self::compute_state_hash(local_data);
        let passed = computed == expected_hash;
        let result = VerificationResult {
            check_type: VerificationType::StateHash,
            passed,
            scope: scope.into(),
            computed_hash: format!("{:016x}", computed),
            expected_hash: Some(format!("{:016x}", expected_hash)),
            checked_at: now_unix,
            error: if passed {
                None
            } else {
                Some("state hash mismatch".into())
            },
        };
        self.record_result(result.clone());
        result
    }

    /// Verify a WAL segment checksum.
    pub fn verify_wal_checksum(
        &self,
        segment_id: &str,
        computed_checksum: u64,
        expected_checksum: u64,
        now_unix: u64,
    ) -> VerificationResult {
        let passed = computed_checksum == expected_checksum;
        let result = VerificationResult {
            check_type: VerificationType::WalReplayChecksum,
            passed,
            scope: segment_id.into(),
            computed_hash: format!("{:016x}", computed_checksum),
            expected_hash: Some(format!("{:016x}", expected_checksum)),
            checked_at: now_unix,
            error: if passed {
                None
            } else {
                Some("WAL checksum mismatch".into())
            },
        };
        self.record_result(result.clone());
        result
    }

    /// Verify a range hash (partial data verification).
    pub fn verify_range_hash(
        &self,
        range_desc: &str,
        local_hash: u64,
        replica_hash: u64,
        now_unix: u64,
    ) -> VerificationResult {
        let passed = local_hash == replica_hash;
        let result = VerificationResult {
            check_type: VerificationType::RangeHash,
            passed,
            scope: range_desc.into(),
            computed_hash: format!("{:016x}", local_hash),
            expected_hash: Some(format!("{:016x}", replica_hash)),
            checked_at: now_unix,
            error: if passed {
                None
            } else {
                Some("range hash mismatch".into())
            },
        };
        self.record_result(result.clone());
        result
    }

    /// Record a transaction replay verification result.
    pub fn record_txn_replay(
        &self,
        txn_desc: &str,
        replay_matches: bool,
        now_unix: u64,
    ) -> VerificationResult {
        let result = VerificationResult {
            check_type: VerificationType::TxnReplay,
            passed: replay_matches,
            scope: txn_desc.into(),
            computed_hash: String::new(),
            expected_hash: None,
            checked_at: now_unix,
            error: if replay_matches {
                None
            } else {
                Some("txn replay mismatch".into())
            },
        };
        self.record_result(result.clone());
        result
    }

    fn record_result(&self, result: VerificationResult) {
        self.total_checks.fetch_add(1, Ordering::Relaxed);
        if result.passed {
            self.total_passed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.total_failed.fetch_add(1, Ordering::Relaxed);
            self.anomalies_detected.fetch_add(1, Ordering::Relaxed);
        }
        let mut results = self.results.lock();
        results.push(result);
        if results.len() > self.config.max_results_history {
            results.remove(0);
        }
    }

    /// Generate an audit report.
    pub fn audit_report(&self, now_unix: u64) -> VerificationAuditReport {
        let results = self.results.lock();
        let total = self.total_checks.load(Ordering::Relaxed);
        let passed = self.total_passed.load(Ordering::Relaxed);
        let failed = self.total_failed.load(Ordering::Relaxed);
        let coverage = self.config.range_hash_sample_ratio;
        VerificationAuditReport {
            total_checks: total,
            passed,
            failed,
            coverage_ratio: coverage,
            generated_at: now_unix,
            results: results.clone(),
            anomalies_detected: failed > 0,
        }
    }

    /// Total checks performed.
    pub fn total_checks(&self) -> u64 {
        self.total_checks.load(Ordering::Relaxed)
    }

    /// Total anomalies detected.
    pub fn anomalies(&self) -> u64 {
        self.anomalies_detected.load(Ordering::Relaxed)
    }

    /// Config access.
    pub fn config(&self) -> &VerifierConfig {
        &self.config
    }
}

impl Default for ConsistencyVerifier {
    fn default() -> Self {
        Self::new(VerifierConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_hash_deterministic() {
        let data = vec![
            (b"key1".to_vec(), b"val1".to_vec()),
            (b"key2".to_vec(), b"val2".to_vec()),
        ];
        let h1 = ConsistencyVerifier::compute_state_hash(&data);
        let h2 = ConsistencyVerifier::compute_state_hash(&data);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_state_hash_differs_on_change() {
        let d1 = vec![(b"k".to_vec(), b"v1".to_vec())];
        let d2 = vec![(b"k".to_vec(), b"v2".to_vec())];
        assert_ne!(
            ConsistencyVerifier::compute_state_hash(&d1),
            ConsistencyVerifier::compute_state_hash(&d2),
        );
    }

    #[test]
    fn test_verify_state_hash_pass() {
        let verifier = ConsistencyVerifier::default();
        let data = vec![(b"a".to_vec(), b"b".to_vec())];
        let expected = ConsistencyVerifier::compute_state_hash(&data);
        let result = verifier.verify_state_hash("shard_0", &data, expected, 1000);
        assert!(result.passed);
        assert!(result.error.is_none());
        assert_eq!(verifier.total_checks(), 1);
        assert_eq!(verifier.anomalies(), 0);
    }

    #[test]
    fn test_verify_state_hash_fail() {
        let verifier = ConsistencyVerifier::default();
        let data = vec![(b"a".to_vec(), b"b".to_vec())];
        let result = verifier.verify_state_hash("shard_0", &data, 0xDEADBEEF, 1000);
        assert!(!result.passed);
        assert!(result.error.is_some());
        assert_eq!(verifier.anomalies(), 1);
    }

    #[test]
    fn test_verify_wal_checksum() {
        let verifier = ConsistencyVerifier::default();
        let result = verifier.verify_wal_checksum("seg_001", 12345, 12345, 2000);
        assert!(result.passed);
        let result2 = verifier.verify_wal_checksum("seg_002", 111, 222, 2001);
        assert!(!result2.passed);
        assert_eq!(verifier.total_checks(), 2);
    }

    #[test]
    fn test_verify_range_hash() {
        let verifier = ConsistencyVerifier::default();
        let result = verifier.verify_range_hash("table:users[0-1000]", 999, 999, 3000);
        assert!(result.passed);
    }

    #[test]
    fn test_txn_replay() {
        let verifier = ConsistencyVerifier::default();
        let r1 = verifier.record_txn_replay("txn_42", true, 4000);
        assert!(r1.passed);
        let r2 = verifier.record_txn_replay("txn_43", false, 4001);
        assert!(!r2.passed);
        assert_eq!(verifier.anomalies(), 1);
    }

    #[test]
    fn test_audit_report() {
        let verifier = ConsistencyVerifier::new(VerifierConfig {
            max_results_history: 5,
            ..Default::default()
        });
        for i in 0..10 {
            let data = vec![(format!("k{}", i).into_bytes(), b"v".to_vec())];
            let h = ConsistencyVerifier::compute_state_hash(&data);
            verifier.verify_state_hash(&format!("shard_{}", i), &data, h, 5000 + i as u64);
        }
        let report = verifier.audit_report(6000);
        assert_eq!(report.total_checks, 10);
        assert_eq!(report.passed, 10);
        assert_eq!(report.failed, 0);
        assert!(!report.anomalies_detected);
        // Capped at 5 results
        assert_eq!(report.results.len(), 5);
    }

    #[test]
    fn test_audit_report_with_failures() {
        let verifier = ConsistencyVerifier::default();
        let data = vec![(b"k".to_vec(), b"v".to_vec())];
        let h = ConsistencyVerifier::compute_state_hash(&data);
        verifier.verify_state_hash("ok", &data, h, 1000);
        verifier.verify_state_hash("bad", &data, 0, 1001);
        let report = verifier.audit_report(2000);
        assert_eq!(report.total_checks, 2);
        assert_eq!(report.passed, 1);
        assert_eq!(report.failed, 1);
        assert!(report.anomalies_detected);
        assert!(report.summary().contains("failed=1"));
    }
}
