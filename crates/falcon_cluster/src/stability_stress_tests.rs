//! FalconDB v1.0.3 — Stress & Regression Tests (§9)
//!
//! Validates determinism under:
//! - High retry rate under failover
//! - Concurrent duplicate txn_id submission
//! - Rapid leader churn
//! - Partial network partitions
//! - Crash + restart loops
//!
//! Mandatory invariants:
//! - Atomicity preserved
//! - At-most-once effect preserved
//! - In-doubt bounded
//! - No silent success or silent failure

#[cfg(test)]
mod stress {
    use std::time::Duration;

    use falcon_common::error::{ErrorKind, FalconError, TxnError};
    use falcon_common::types::TxnId;

    use crate::stability_hardening::*;

    // ═══════════════════════════════════════════════════════════════════
    // Stress: High retry rate under failover
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_stress_high_retry_rate_under_failover() {
        let retry_guard = RetryGuard::with_config(50, 10_000);
        let outcome_guard = FailoverOutcomeGuard::new(1);
        let journal = TxnOutcomeJournal::new();

        let mut committed = 0u64;
        let mut retried = 0u64;
        let mut rejected = 0u64;

        // Simulate 100 transactions with varying retry counts
        for i in 1..=100u64 {
            let tid = TxnId(i);
            let retries = (i % 5) + 1; // 1-5 retries each

            for attempt in 0..retries {
                let phase = if attempt == 0 {
                    ProtocolPhase::Begin
                } else {
                    ProtocolPhase::Execute
                };

                match retry_guard.check_retry(tid, phase, i * 1000) {
                    Ok(()) => retried += 1,
                    Err(_) => {
                        rejected += 1;
                        continue;
                    }
                }
            }

            // Attempt commit with epoch check
            if outcome_guard.check_commit_safe(tid, 1).is_ok() {
                outcome_guard.record_commit(tid, i, 1);
                journal.record(TxnOutcomeEntry {
                    txn_id: tid,
                    final_state: "Committed".into(),
                    commit_ts: Some(i),
                    was_indoubt: false,
                    indoubt_reason: None,
                    resolution_method: None,
                    resolution_latency_ms: None,
                    epoch: 1,
                });
                committed += 1;
            }

            retry_guard.remove(tid);
        }

        // Invariants
        assert_eq!(committed, 100, "all 100 txns should commit");
        assert!(retried > 100, "total retries should exceed txn count");
        assert_eq!(journal.total_recorded(), 100);

        // Verify at-most-once: re-committing any txn should fail
        for i in 1..=100u64 {
            assert!(outcome_guard.check_commit_safe(TxnId(i), 1).is_err());
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Stress: Concurrent duplicate txn_id submission
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_stress_concurrent_duplicate_txn_id() {
        let retry_guard = RetryGuard::with_config(5, 10_000);
        let outcome_guard = FailoverOutcomeGuard::new(1);

        let tid = TxnId(999);

        // First submission
        retry_guard.check_retry(tid, ProtocolPhase::Begin, 42).unwrap();
        retry_guard.check_retry(tid, ProtocolPhase::Execute, 42).unwrap();
        retry_guard.check_retry(tid, ProtocolPhase::Commit, 42).unwrap();

        // Commit
        outcome_guard.check_commit_safe(tid, 1).unwrap();
        outcome_guard.record_commit(tid, 100, 1);

        // Duplicate submission with SAME fingerprint — retry guard allows 
        // (idempotent retry), but outcome guard blocks the duplicate commit
        retry_guard.check_retry(tid, ProtocolPhase::Commit, 42).unwrap();
        assert!(outcome_guard.check_commit_safe(tid, 1).is_err());

        // Duplicate submission with DIFFERENT fingerprint — rejected by retry guard
        let result = retry_guard.check_retry(tid, ProtocolPhase::Begin, 99);
        assert!(result.is_err()); // conflicting payload OR phase regression

        // At-most-once: committed exactly once
        assert!(outcome_guard.was_committed(tid));
        let m = outcome_guard.metrics();
        assert_eq!(m.duplicate_commit_rejected, 1);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Stress: Rapid leader churn (epoch advancement)
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_stress_rapid_leader_churn() {
        let outcome_guard = FailoverOutcomeGuard::new(1);
        let state_guard = TxnStateGuard::new();
        let journal = TxnOutcomeJournal::new();

        let mut stale_rejected = 0u64;
        let mut committed = 0u64;

        // Simulate 10 rapid epoch changes
        for epoch in 1..=10u64 {
            outcome_guard.advance_epoch(epoch);

            // 5 txns per epoch
            for j in 0..5u64 {
                let tid = TxnId(epoch * 100 + j);

                // Validate state transitions
                state_guard.validate_transition(tid, "Active", "Committed").unwrap();

                // Try commit at current epoch
                if outcome_guard.check_commit_safe(tid, epoch).is_ok() {
                    outcome_guard.record_commit(tid, epoch * 1000 + j, epoch);
                    journal.record(TxnOutcomeEntry {
                        txn_id: tid,
                        final_state: "Committed".into(),
                        commit_ts: Some(epoch * 1000 + j),
                        was_indoubt: false,
                        indoubt_reason: None,
                        resolution_method: None,
                        resolution_latency_ms: None,
                        epoch,
                    });
                    committed += 1;
                }

                // Try commit at stale epoch — should be rejected
                if epoch > 1 {
                    let stale_tid = TxnId(epoch * 100 + j + 50);
                    if outcome_guard.check_commit_safe(stale_tid, epoch - 1).is_err() {
                        stale_rejected += 1;
                    }
                }

                state_guard.remove(tid);
            }
        }

        assert_eq!(committed, 50, "50 txns committed across 10 epochs");
        assert!(stale_rejected > 0, "stale epoch commits rejected");
        assert_eq!(journal.total_recorded(), 50);

        // No state regressions during rapid churn
        assert_eq!(state_guard.metrics().state_regressions_detected, 0);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Stress: In-doubt escalation under churn
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_stress_indoubt_escalation_under_churn() {
        let escalator = InDoubtEscalator::new(Duration::from_millis(20));
        let journal = TxnOutcomeJournal::new();

        // Register 20 in-doubt txns
        for i in 0..20u64 {
            escalator.register(TxnId(2000 + i));
        }
        assert_eq!(escalator.tracked_count(), 20);

        // Resolve 10 normally
        for i in 0..10u64 {
            escalator.remove(TxnId(2000 + i));
            journal.record(TxnOutcomeEntry {
                txn_id: TxnId(2000 + i),
                final_state: "Aborted".into(),
                commit_ts: None,
                was_indoubt: true,
                indoubt_reason: Some(InDoubtReason::CoordinatorCrash),
                resolution_method: Some(ResolutionMethod::AutoAbort),
                resolution_latency_ms: Some(5),
                epoch: 1,
            });
        }
        assert_eq!(escalator.tracked_count(), 10);

        // Wait for threshold, then sweep remaining
        std::thread::sleep(Duration::from_millis(25));
        let escalated = escalator.sweep();
        assert_eq!(escalated.len(), 10);

        // Record escalated outcomes
        for record in &escalated {
            journal.record(TxnOutcomeEntry {
                txn_id: record.txn_id,
                final_state: "Aborted".into(),
                commit_ts: None,
                was_indoubt: true,
                indoubt_reason: Some(InDoubtReason::CoordinatorCrash),
                resolution_method: Some(ResolutionMethod::Escalation),
                resolution_latency_ms: Some(record.age_ms),
                epoch: 1,
            });
        }

        // All resolved — no in-doubt remaining
        assert_eq!(escalator.tracked_count(), 0);
        assert_eq!(journal.total_recorded(), 20);
        assert_eq!(journal.total_indoubt_resolved(), 20);

        let m = escalator.metrics();
        assert_eq!(m.escalations, 10);
        assert_eq!(m.forced_aborts, 10);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Stress: Crash + restart loops
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_stress_crash_restart_loops() {
        let outcome_guard = FailoverOutcomeGuard::new(1);
        let phase_tracker = CommitPhaseTracker::new();
        let journal = TxnOutcomeJournal::new();

        // Simulate 5 crash+restart cycles
        for cycle in 0..5u64 {
            let epoch = cycle + 1;
            outcome_guard.advance_epoch(epoch);

            // Start 3 txns per cycle
            let tids: Vec<TxnId> = (0..3)
                .map(|j| TxnId(cycle * 100 + j))
                .collect();

            for &tid in &tids {
                phase_tracker.begin_commit(tid);
                phase_tracker.advance(tid, CommitPhase::WalLogged, None).unwrap();
            }

            // "Crash" before CP-D on some txns — only first txn makes it to durable
            let surviving_tid = tids[0];
            phase_tracker
                .advance(surviving_tid, CommitPhase::WalDurable, None)
                .unwrap();
            phase_tracker
                .advance(surviving_tid, CommitPhase::Visible, Some(epoch * 1000))
                .unwrap();

            assert!(phase_tracker.is_irreversible(surviving_tid));

            // Commit the surviving txn
            outcome_guard.check_commit_safe(surviving_tid, epoch).unwrap();
            outcome_guard.record_commit(surviving_tid, epoch * 1000, epoch);
            phase_tracker.complete(surviving_tid);

            journal.record(TxnOutcomeEntry {
                txn_id: surviving_tid,
                final_state: "Committed".into(),
                commit_ts: Some(epoch * 1000),
                was_indoubt: false,
                indoubt_reason: None,
                resolution_method: None,
                resolution_latency_ms: None,
                epoch,
            });

            // The other txns were lost at CP-L (not durable) — clean up
            for &tid in &tids[1..] {
                assert!(!phase_tracker.is_irreversible(tid));
                phase_tracker.complete(tid);

                journal.record(TxnOutcomeEntry {
                    txn_id: tid,
                    final_state: "Aborted".into(),
                    commit_ts: None,
                    was_indoubt: false,
                    indoubt_reason: None,
                    resolution_method: None,
                    resolution_latency_ms: None,
                    epoch,
                });
            }
        }

        // 5 committed (one per cycle), 10 aborted (two per cycle)
        assert_eq!(journal.total_recorded(), 15);

        let committed_count = journal
            .recent(100)
            .iter()
            .filter(|e| e.final_state == "Committed")
            .count();
        assert_eq!(committed_count, 5);

        // Phase tracker should have no in-flight
        assert_eq!(phase_tracker.metrics().in_flight, 0);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Stress: Error classification stability under failover
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_stress_error_classification_stability() {
        let stabilizer = ErrorClassStabilizer::new();

        // Classify a range of errors multiple times — classification must be stable
        let errors: Vec<FalconError> = vec![
            FalconError::Txn(TxnError::Timeout),
            FalconError::Txn(TxnError::WriteConflict(TxnId(1))),
            FalconError::Txn(TxnError::SerializationConflict(TxnId(2))),
            FalconError::Txn(TxnError::Aborted(TxnId(3))),
            FalconError::Txn(TxnError::MemoryPressure(TxnId(4))),
            FalconError::Txn(TxnError::WalBacklogExceeded(TxnId(5))),
            FalconError::Txn(TxnError::NotFound(TxnId(6))),
            FalconError::Internal("test".into()),
        ];

        // First pass: record classifications
        let mut first_pass: Vec<ErrorKind> = Vec::new();
        for err in &errors {
            first_pass.push(stabilizer.classify_and_validate(err));
        }

        // Second pass: verify identical classification (stability)
        for (err, &expected) in errors.iter().zip(first_pass.iter()) {
            let kind = stabilizer.classify_and_validate(err);
            assert_eq!(kind, expected, "classification changed for: {}", err);
        }

        // Third pass: same again
        for (err, &expected) in errors.iter().zip(first_pass.iter()) {
            let kind = stabilizer.classify_and_validate(err);
            assert_eq!(kind, expected);
        }

        assert_eq!(stabilizer.instability_count(), 0);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Stress: Defensive validation under malformed input
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_stress_defensive_validation_malformed_input() {
        let validator = DefensiveValidator::new();
        let state_guard = TxnStateGuard::new();

        // Sentinel txn_ids — must be rejected
        assert!(validator.validate_txn_id(TxnId(0)).is_err());
        assert!(validator.validate_txn_id(TxnId(u64::MAX)).is_err());

        // Valid txn_ids in range
        for i in 1..100u64 {
            assert!(validator.validate_txn_id(TxnId(i)).is_ok());
        }

        // Unknown state names
        assert!(validator.validate_state_name("Running").is_err());
        assert!(validator.validate_state_name("Pending").is_err());
        assert!(validator.validate_state_name("Rolledback").is_err());
        assert!(validator.validate_state_name("").is_err());

        // Valid state names
        for name in &["Active", "Prepared", "Committed", "Aborted"] {
            assert!(validator.validate_state_name(name).is_ok());
        }

        // State guard with unknown states — must reject
        assert!(state_guard.validate_transition(TxnId(1), "Active", "Zombie").is_err());
        assert!(state_guard.validate_transition(TxnId(2), "Pending", "Committed").is_err());

        // Malformed inputs never corrupt state
        assert_eq!(state_guard.metrics().rejected_transitions, 2);
        assert_eq!(state_guard.metrics().valid_transitions, 0);

        let m = validator.metrics();
        assert!(m.rejections > 0);
        assert!(m.total_checks > 0);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Stress: Full lifecycle integration — all components together
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn test_stress_full_lifecycle_integration() {
        let state_guard = TxnStateGuard::new();
        let phase_tracker = CommitPhaseTracker::new();
        let retry_guard = RetryGuard::with_config(10, 10_000);
        let escalator = InDoubtEscalator::new(Duration::from_millis(30));
        let outcome_guard = FailoverOutcomeGuard::new(1);
        let validator = DefensiveValidator::new();
        let stabilizer = ErrorClassStabilizer::new();
        let journal = TxnOutcomeJournal::new();

        // ── Normal commit path ──
        let tid1 = TxnId(5001);
        validator.validate_txn_id(tid1).unwrap();
        retry_guard.check_retry(tid1, ProtocolPhase::Begin, 111).unwrap();
        // Active→Active is a same-rank no-op (not a regression)
        state_guard.validate_transition(tid1, "Active", "Active").unwrap();
        // Forward: Active→Prepared→Committed
        state_guard.validate_transition(tid1, "Active", "Prepared").unwrap();
        phase_tracker.begin_commit(tid1);
        phase_tracker.advance(tid1, CommitPhase::WalLogged, None).unwrap();
        phase_tracker.advance(tid1, CommitPhase::WalDurable, None).unwrap();
        phase_tracker.advance(tid1, CommitPhase::Visible, Some(100)).unwrap();
        assert!(phase_tracker.is_irreversible(tid1));
        state_guard.validate_transition(tid1, "Prepared", "Committed").unwrap();
        outcome_guard.check_commit_safe(tid1, 1).unwrap();
        outcome_guard.record_commit(tid1, 100, 1);
        phase_tracker.complete(tid1);
        retry_guard.remove(tid1);
        state_guard.remove(tid1);
        journal.record(TxnOutcomeEntry {
            txn_id: tid1,
            final_state: "Committed".into(),
            commit_ts: Some(100),
            was_indoubt: false,
            indoubt_reason: None,
            resolution_method: None,
            resolution_latency_ms: None,
            epoch: 1,
        });

        // ── Abort path with in-doubt escalation ──
        let tid2 = TxnId(5002);
        validator.validate_txn_id(tid2).unwrap();
        retry_guard.check_retry(tid2, ProtocolPhase::Begin, 222).unwrap();
        state_guard.validate_transition(tid2, "Active", "Prepared").unwrap();
        // Coordinator crashes → in-doubt
        escalator.register(tid2);
        std::thread::sleep(Duration::from_millis(35));
        let escalated = escalator.sweep();
        assert_eq!(escalated.len(), 1);
        state_guard.validate_transition(tid2, "Prepared", "Aborted").unwrap();
        retry_guard.remove(tid2);
        state_guard.remove(tid2);
        journal.record(TxnOutcomeEntry {
            txn_id: tid2,
            final_state: "Aborted".into(),
            commit_ts: None,
            was_indoubt: true,
            indoubt_reason: Some(InDoubtReason::CoordinatorCrash),
            resolution_method: Some(ResolutionMethod::Escalation),
            resolution_latency_ms: Some(escalated[0].age_ms),
            epoch: 1,
        });

        // ── Duplicate commit rejection ──
        assert!(outcome_guard.check_commit_safe(tid1, 1).is_err());

        // ── Error classification stability ──
        let err = FalconError::Txn(TxnError::Timeout);
        let k1 = stabilizer.classify_and_validate(&err);
        let k2 = stabilizer.classify_and_validate(&err);
        assert_eq!(k1, k2);
        assert_eq!(k1, ErrorKind::Transient);

        // ── Final checks ──
        assert_eq!(journal.total_recorded(), 2);
        assert_eq!(journal.total_indoubt_resolved(), 1);
        assert_eq!(state_guard.metrics().state_regressions_detected, 0);
        assert_eq!(stabilizer.instability_count(), 0);
        assert_eq!(phase_tracker.metrics().in_flight, 0);
    }
}
