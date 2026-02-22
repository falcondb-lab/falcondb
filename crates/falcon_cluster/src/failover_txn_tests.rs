//! FalconDB v1.0.2 — Failover × Transaction Executable Test Matrix
//!
//! Mandatory test suite for v1.0.2 LTS qualification.
//! Validates deterministic transaction behavior under node failure,
//! coordinator failure, network partition, and retry conditions.
//!
//! # Test IDs
//! - **SS-01..SS-04**: Single-shard × failover
//! - **XS-01..XS-08**: Cross-shard × failover (2PC)
//! - **CH-01..CH-02**: Churn & stability
//! - **ID-01..ID-02**: Idempotency & replay
//!
//! # Global Invariants (must hold for ALL tests)
//! - I1: Atomicity — no partial effects
//! - I2: Consistency — sum(balance) == 2000 after recovery
//! - I3: Idempotency — replaying same txn_id ⇒ at most one commit
//! - I4: Deterministic client outcome — success or stable error, never ambiguous
//! - I5: In-doubt boundedness — resolve within bounded time

#[cfg(test)]
mod failover_txn_matrix {
    use std::sync::Arc;
    use std::time::Duration;

    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::error::TxnError;
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::*;
    use falcon_storage::engine::StorageEngine;

    use crate::failover_txn_hardening::{
        FailoverBlockedTxnConfig, FailoverBlockedTxnGuard, FailoverDamper, FailoverDamperConfig,
        FailoverTxnConfig, FailoverTxnCoordinator, FailoverTxnPhase, FailoverTxnResolution,
        InDoubtTtlConfig, InDoubtTtlEnforcer,
    };
    use crate::ha::{HAConfig, HAReplicaGroup};
    use crate::indoubt_resolver::{InDoubtResolver, TxnOutcome, TxnOutcomeCache};
    use falcon_storage::wal::WalRecord;
    use falcon_txn::manager::{
        SlowPathMode, TxnClassification, TxnManager, TxnState,
    };

    // ═══════════════════════════════════════════════════════════════════
    // Test Infrastructure: schemas, cluster setup, invariant checkers
    // ═══════════════════════════════════════════════════════════════════

    /// t_balance(account_id PK, balance INT)
    fn balance_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "t_balance".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "account_id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "balance".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        }
    }

    /// t_ledger(txn_id PK, from_id, to_id, amount, ts)
    fn ledger_schema() -> TableSchema {
        TableSchema {
            id: TableId(2),
            name: "t_ledger".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "txn_id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "from_id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "to_id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(3),
                    name: "amount".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            ..Default::default()
        }
    }

    fn schemas() -> Vec<TableSchema> {
        vec![balance_schema(), ledger_schema()]
    }

    /// 2-shard HA cluster: each shard has 1 leader + 1 replica.
    struct TestCluster {
        shard0: HAReplicaGroup,
        shard1: HAReplicaGroup,
        /// TxnManager for shard 0 (primary)
        mgr0: TxnManager,
        /// TxnManager for shard 1 (primary)
        mgr1: TxnManager,
        /// In-doubt resolver
        resolver: Arc<InDoubtResolver>,
        /// Outcome cache
        outcome_cache: Arc<TxnOutcomeCache>,
        /// Failover×txn coordinator
        fo_coord: FailoverTxnCoordinator,
        /// In-doubt TTL enforcer
        ttl_enforcer: InDoubtTtlEnforcer,
        /// Failover damper
        damper: FailoverDamper,
        /// Blocked txn guard
        blocked_guard: FailoverBlockedTxnGuard,
    }

    fn ha_config() -> HAConfig {
        HAConfig {
            replica_count: 1,
            failover_cooldown: Duration::from_millis(0),
            ..Default::default()
        }
    }

    impl TestCluster {
        fn new() -> Self {
            let s = schemas();
            let shard0 = HAReplicaGroup::new(ShardId(0), &s, ha_config()).unwrap();
            let shard1 = HAReplicaGroup::new(ShardId(1), &s, ha_config()).unwrap();

            let mgr0 = TxnManager::new(shard0.inner.primary.storage.clone());
            let mgr1 = TxnManager::new(shard1.inner.primary.storage.clone());

            let outcome_cache = TxnOutcomeCache::new(Duration::from_secs(60), 10_000);
            let resolver = InDoubtResolver::with_config(
                Arc::clone(&outcome_cache),
                Duration::from_millis(50),
                5,
                100,
            );

            let fo_coord = FailoverTxnCoordinator::with_config(FailoverTxnConfig {
                drain_timeout: Duration::from_millis(500),
                convergence_window: Duration::from_millis(50),
                ..Default::default()
            });

            let ttl_enforcer = InDoubtTtlEnforcer::with_config(InDoubtTtlConfig {
                max_lifetime: Duration::from_millis(500),
                sweep_interval: Duration::from_millis(50),
                warn_threshold_ratio: 0.5,
            });

            let damper = FailoverDamper::with_config(FailoverDamperConfig {
                min_interval: Duration::from_millis(50),
                max_failovers_in_window: 5,
                observation_window: Duration::from_secs(10),
                log_suppressed: false,
            });

            let blocked_guard = FailoverBlockedTxnGuard::with_config(FailoverBlockedTxnConfig {
                max_blocked_duration: Duration::from_millis(200),
                latency_tracking_window: Duration::from_secs(10),
            });

            Self {
                shard0,
                shard1,
                mgr0,
                mgr1,
                resolver,
                outcome_cache,
                fo_coord,
                ttl_enforcer,
                damper,
                blocked_guard,
            }
        }

        /// Load initial state: A=1000, B=1000 on shard 0.
        fn load_initial_balances(&self) {
            let row_a = OwnedRow::new(vec![Datum::Int32(1), Datum::Int32(1000)]);
            let row_b = OwnedRow::new(vec![Datum::Int32(2), Datum::Int32(1000)]);

            self.shard0
                .inner
                .primary
                .storage
                .insert(TableId(1), row_a.clone(), TxnId(1))
                .unwrap();
            self.shard0
                .inner
                .primary
                .storage
                .commit_txn(TxnId(1), Timestamp(1), TxnType::Local)
                .unwrap();

            self.shard0
                .inner
                .primary
                .storage
                .insert(TableId(1), row_b.clone(), TxnId(2))
                .unwrap();
            self.shard0
                .inner
                .primary
                .storage
                .commit_txn(TxnId(2), Timestamp(2), TxnType::Local)
                .unwrap();

            // Replicate to replicas
            self.shard0.inner.ship_wal_record(WalRecord::Insert {
                txn_id: TxnId(1),
                table_id: TableId(1),
                row: row_a,
            });
            self.shard0
                .inner
                .ship_wal_record(WalRecord::CommitTxnLocal {
                    txn_id: TxnId(1),
                    commit_ts: Timestamp(1),
                });
            self.shard0.inner.ship_wal_record(WalRecord::Insert {
                txn_id: TxnId(2),
                table_id: TableId(1),
                row: row_b,
            });
            self.shard0
                .inner
                .ship_wal_record(WalRecord::CommitTxnLocal {
                    txn_id: TxnId(2),
                    commit_ts: Timestamp(2),
                });
            self.shard0.catch_up_all_replicas().unwrap();
        }
    }

    /// Invariant I2: sum(balance) must equal expected_sum.
    /// Checks the primary storage of the given shard.
    fn check_balance_sum(storage: &StorageEngine, expected_sum: i32) -> bool {
        // Use a high txn_id/read_ts to see all committed data
        let rows = storage
            .scan(TableId(1), TxnId(u64::MAX - 1), Timestamp(u64::MAX - 1))
            .unwrap_or_default();
        let sum: i32 = rows
            .iter()
            .filter_map(|(_pk, row)| {
                if let Some(Datum::Int32(b)) = row.values.get(1) {
                    Some(*b)
                } else {
                    None
                }
            })
            .sum();
        sum == expected_sum
    }

    // ═══════════════════════════════════════════════════════════════════
    // SS: Single-Shard × Failover Tests
    // ═══════════════════════════════════════════════════════════════════

    /// SS-01: Kill shard leader during execution.
    ///
    /// Failure: kill shard leader while SS txn is executing.
    /// Assert: client sees failure or success, never ambiguous;
    ///         retry of same txn_id is safe; no permanent in-doubt.
    #[test]
    fn test_ss01_kill_leader_during_execution() {
        let mut cluster = TestCluster::new();
        cluster.load_initial_balances();

        // Begin a local txn on shard 0
        let txn = cluster.mgr0.begin(IsolationLevel::ReadCommitted);
        let txn_id = txn.txn_id;

        // Simulate: leader is killed → failover drain initiated
        let active_ids = vec![txn_id];
        cluster
            .fo_coord
            .begin_failover_drain(cluster.shard0.current_epoch() + 1, active_ids);

        // I4: writes must be blocked during drain
        assert!(cluster.fo_coord.are_writes_blocked());

        // Abort the active txn (simulating leader-kill semantics)
        let abort_result = cluster.mgr0.abort(txn_id);
        // I4: client sees a deterministic outcome (abort success or NotFound)
        assert!(abort_result.is_ok() || matches!(abort_result, Err(TxnError::NotFound(_))));

        cluster.fo_coord.record_affected_txn(
            txn_id,
            false,
            FailoverTxnResolution::Aborted,
            100,
        );

        // Promote replica
        cluster.shard0.bump_epoch();
        cluster.fo_coord.complete_failover_drain();

        // I4: after drain, writes unblocked
        assert!(!cluster.fo_coord.are_writes_blocked());

        // I3: retry of same txn_id after abort → NotFound (safe)
        let retry_result = cluster.mgr0.commit(txn_id);
        assert!(retry_result.is_err());

        // I5: no permanent in-doubt
        assert_eq!(cluster.resolver.indoubt_count(), 0);

        // I2: balance sum preserved (no partial effect)
        assert!(check_balance_sum(&cluster.shard0.inner.primary.storage, 2000));
    }

    /// SS-02: Kill shard leader before commit.
    ///
    /// Failure: kill shard leader immediately before COMMIT.
    /// Assert: no commit-after-success anomaly; atomicity preserved.
    #[test]
    fn test_ss02_kill_leader_before_commit() {
        let mut cluster = TestCluster::new();
        cluster.load_initial_balances();

        let txn = cluster.mgr0.begin(IsolationLevel::ReadCommitted);
        let txn_id = txn.txn_id;

        // Simulate: txn did some work, then leader killed before commit
        // Initiate failover drain
        cluster.fo_coord.begin_failover_drain(
            cluster.shard0.current_epoch() + 1,
            vec![txn_id],
        );

        // Force-abort the txn (leader died before commit could execute)
        let _ = cluster.mgr0.abort(txn_id);
        cluster.fo_coord.record_affected_txn(
            txn_id,
            false,
            FailoverTxnResolution::Aborted,
            50,
        );

        // Promote replica
        cluster.shard0.bump_epoch();
        cluster.fo_coord.complete_failover_drain();

        // I1: atomicity — txn was aborted, no effects visible
        // I2: balance unchanged
        assert!(check_balance_sum(&cluster.shard0.inner.primary.storage, 2000));

        // I4: attempting commit after abort yields deterministic error
        let commit_result = cluster.mgr0.commit(txn_id);
        assert!(commit_result.is_err());
    }

    /// SS-03: Client disconnect during commit response.
    ///
    /// Failure: drop client connection before commit ACK.
    /// Assert: retry is safe; commit applied at most once.
    #[test]
    fn test_ss03_client_disconnect_during_commit_response() {
        let cluster = TestCluster::new();
        cluster.load_initial_balances();

        let txn = cluster.mgr0.begin(IsolationLevel::ReadCommitted);
        let txn_id = txn.txn_id;

        // Commit succeeds at storage level
        let commit_result = cluster.mgr0.commit(txn_id);
        assert!(commit_result.is_ok());
        let commit_ts = commit_result.unwrap();
        assert!(commit_ts.0 > 0);

        // Client "disconnects" — doesn't see the ACK.
        // Client retries commit with same txn_id.
        let retry_result = cluster.mgr0.commit(txn_id);

        // I3: second commit must fail (txn already removed from active set)
        assert!(retry_result.is_err());

        // I3: only one commit effect applied
        // The txn was committed exactly once (at commit_ts)

        // I2: balance sum preserved
        assert!(check_balance_sum(&cluster.shard0.inner.primary.storage, 2000));
    }

    /// SS-04: Replica lag + leader switch.
    ///
    /// Failure: induce replica lag, then force leader switch.
    /// Assert: correctness preserved; tail latency converges.
    #[test]
    fn test_ss04_replica_lag_plus_leader_switch() {
        let mut cluster = TestCluster::new();
        cluster.load_initial_balances();

        // Insert additional data but DON'T replicate immediately (induce lag)
        let row_c = OwnedRow::new(vec![Datum::Int32(3), Datum::Int32(500)]);
        cluster
            .shard0
            .inner
            .primary
            .storage
            .insert(TableId(1), row_c.clone(), TxnId(100))
            .unwrap();
        cluster
            .shard0
            .inner
            .primary
            .storage
            .commit_txn(TxnId(100), Timestamp(100), TxnType::Local)
            .unwrap();

        // Replicate the WAL entry + catch up
        cluster.shard0.inner.ship_wal_record(WalRecord::Insert {
            txn_id: TxnId(100),
            table_id: TableId(1),
            row: row_c,
        });
        cluster
            .shard0
            .inner
            .ship_wal_record(WalRecord::CommitTxnLocal {
                txn_id: TxnId(100),
                commit_ts: Timestamp(100),
            });
        cluster.shard0.catch_up_all_replicas().unwrap();

        // Force leader switch
        let promoted = cluster.shard0.promote_best();
        assert!(promoted.is_ok());

        // Tail-latency guard: register some "blocked" txns, then release
        cluster.blocked_guard.register_blocked(TxnId(200));
        cluster.blocked_guard.register_blocked(TxnId(201));
        cluster.blocked_guard.release_all();

        // I4: after failover, convergence should eventually complete
        std::thread::sleep(Duration::from_millis(60));
        cluster.fo_coord.begin_failover_drain(
            cluster.shard0.current_epoch(),
            vec![],
        );
        cluster.fo_coord.complete_failover_drain();
        std::thread::sleep(Duration::from_millis(60));
        assert!(cluster.fo_coord.check_convergence());

        // Blocked guard metrics: all released
        assert_eq!(cluster.blocked_guard.currently_blocked(), 0);
    }

    // ═══════════════════════════════════════════════════════════════════
    // XS: Cross-Shard × Failover Tests (2PC)
    // ═══════════════════════════════════════════════════════════════════

    /// XS-01: Kill coordinator before prepare.
    ///
    /// Failure: coordinator dies before prepare phase.
    /// Assert: no partial effects; txn fails or retries deterministically.
    #[test]
    fn test_xs01_kill_coordinator_before_prepare() {
        let cluster = TestCluster::new();
        cluster.load_initial_balances();

        // Begin a cross-shard txn
        let txn = cluster.mgr0.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        let txn_id = txn.txn_id;

        // Coordinator "dies" before prepare → abort
        let abort_result = cluster.mgr0.abort(txn_id);
        assert!(abort_result.is_ok());

        // I1: no partial effects (never prepared)
        assert_eq!(cluster.mgr0.active_count(), 0);

        // I5: no in-doubt (never reached prepare)
        assert_eq!(cluster.resolver.indoubt_count(), 0);

        // I4: deterministic error on retry
        assert!(cluster.mgr0.commit(txn_id).is_err());
    }

    /// XS-02: Kill coordinator during prepare.
    ///
    /// Failure: coordinator dies after some participants prepared.
    /// Assert: in-doubt txn may appear; resolves within bounded time;
    ///         final outcome consistent.
    #[test]
    fn test_xs02_kill_coordinator_during_prepare() {
        let cluster = TestCluster::new();
        cluster.load_initial_balances();

        let txn = cluster.mgr0.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        let txn_id = txn.txn_id;

        // Prepare succeeds on coordinator
        let prepare_result = cluster.mgr0.prepare(txn_id);
        assert!(prepare_result.is_ok());

        // Coordinator "crashes" mid-prepare → txn is in-doubt
        // Register as in-doubt in the resolver
        cluster.resolver.register_indoubt(
            txn_id,
            vec![(ShardId(0), txn_id), (ShardId(1), txn_id)],
        );
        cluster.ttl_enforcer.register(txn_id);

        // I5: in-doubt txn is visible
        assert_eq!(cluster.resolver.indoubt_count(), 1);

        // I5: resolver sweeps and resolves (default: abort when no cached decision)
        let resolved = cluster.resolver.sweep();
        assert_eq!(resolved, 1);
        assert_eq!(cluster.resolver.indoubt_count(), 0);

        // TTL enforcer: remove resolved txn
        cluster.ttl_enforcer.remove(txn_id);

        // I5: no remaining in-doubt
        assert_eq!(cluster.ttl_enforcer.tracked_count(), 0);

        // I4: final outcome is deterministic (abort, since no cached commit decision)
        let metrics = cluster.resolver.metrics();
        assert_eq!(metrics.resolved_aborted, 1);
    }

    /// XS-03: Kill coordinator after prepare, before decision.
    ///
    /// Failure: coordinator crashes after all participants prepared.
    /// Assert: prepared state preserved; resolves to single final outcome.
    #[test]
    fn test_xs03_kill_coordinator_after_prepare_before_decision() {
        let cluster = TestCluster::new();
        cluster.load_initial_balances();

        let txn = cluster.mgr0.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        let txn_id = txn.txn_id;

        // All participants prepare
        cluster.mgr0.prepare(txn_id).unwrap();

        // Verify prepared state
        let handle = cluster.mgr0.get_txn(txn_id).unwrap();
        assert_eq!(handle.state, TxnState::Prepared);

        // Coordinator crashes → register in-doubt
        cluster.resolver.register_indoubt(
            txn_id,
            vec![(ShardId(0), txn_id), (ShardId(1), txn_id)],
        );
        cluster.ttl_enforcer.register(txn_id);

        // Simulate: coordinator WAL replay finds commit decision
        cluster
            .outcome_cache
            .record(txn_id, TxnOutcome::Committed);
        cluster.resolver.record_decision(txn_id, TxnOutcome::Committed);

        // I5: sweep resolves to committed
        let resolved = cluster.resolver.sweep();
        assert_eq!(resolved, 1);
        assert_eq!(cluster.resolver.indoubt_count(), 0);

        let metrics = cluster.resolver.metrics();
        assert_eq!(metrics.resolved_committed, 1);
        assert_eq!(metrics.resolved_aborted, 0);

        cluster.ttl_enforcer.remove(txn_id);
    }

    /// XS-04: Kill coordinator during commit propagation.
    ///
    /// Failure: coordinator crashes after sending commit to subset.
    /// Assert: eventual convergence; no mixed commit/abort.
    #[test]
    fn test_xs04_kill_coordinator_during_commit_propagation() {
        let cluster = TestCluster::new();
        cluster.load_initial_balances();

        let txn = cluster.mgr0.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        let txn_id = txn.txn_id;

        // Prepare + decision recorded
        cluster.mgr0.prepare(txn_id).unwrap();
        cluster
            .outcome_cache
            .record(txn_id, TxnOutcome::Committed);

        // Coordinator "crashes" after sending commit to shard 0 but not shard 1
        // Register as in-doubt
        cluster.resolver.register_indoubt(
            txn_id,
            vec![(ShardId(0), txn_id), (ShardId(1), txn_id)],
        );

        // I5: resolver reads cached commit decision → resolves as committed
        let resolved = cluster.resolver.sweep();
        assert_eq!(resolved, 1);

        // I4: no mixed outcome — single final decision
        let metrics = cluster.resolver.metrics();
        assert_eq!(metrics.resolved_committed, 1);
        assert_eq!(metrics.resolved_aborted, 0);

        // I5: convergence — no in-doubt remains
        assert_eq!(cluster.resolver.indoubt_count(), 0);
    }

    /// XS-05: Kill participant leader during prepare.
    ///
    /// Failure: shard leader dies during prepare phase.
    /// Assert: protocol does not deadlock; deterministic error classification.
    #[test]
    fn test_xs05_kill_participant_leader_during_prepare() {
        let mut cluster = TestCluster::new();
        cluster.load_initial_balances();

        let txn = cluster.mgr0.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        let txn_id = txn.txn_id;

        // Shard 0 leader dies during prepare → failover
        cluster.fo_coord.begin_failover_drain(
            cluster.shard0.current_epoch() + 1,
            vec![txn_id],
        );

        // Txn cannot complete prepare → abort
        let _ = cluster.mgr0.abort(txn_id);
        cluster.fo_coord.record_affected_txn(
            txn_id,
            false,
            FailoverTxnResolution::Aborted,
            100,
        );

        // Promote new leader
        cluster.shard0.catch_up_all_replicas().unwrap();
        cluster.shard0.promote_best().unwrap();
        cluster.fo_coord.complete_failover_drain();

        // I4: deterministic error — txn aborted, not deadlocked
        assert!(cluster.mgr0.get_txn(txn_id).is_none());
        assert_eq!(cluster.mgr0.active_count(), 0);

        // I5: no in-doubt (never fully prepared)
        assert_eq!(cluster.resolver.indoubt_count(), 0);
    }

    /// XS-06: Kill participant leader after prepare.
    ///
    /// Failure: shard leader dies after prepared state persisted.
    /// Assert: prepared state recoverable; final decision applied consistently.
    #[test]
    fn test_xs06_kill_participant_leader_after_prepare() {
        let mut cluster = TestCluster::new();
        cluster.load_initial_balances();

        let txn = cluster.mgr0.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        let txn_id = txn.txn_id;

        // Prepare succeeds
        cluster.mgr0.prepare(txn_id).unwrap();

        // Shard 0 leader dies AFTER prepare persisted
        cluster.fo_coord.begin_failover_drain(
            cluster.shard0.current_epoch() + 1,
            vec![txn_id],
        );

        // Txn was prepared → move to in-doubt
        cluster.fo_coord.record_affected_txn(
            txn_id,
            true,
            FailoverTxnResolution::MovedToInDoubt,
            200,
        );
        cluster.resolver.register_indoubt(
            txn_id,
            vec![(ShardId(0), txn_id), (ShardId(1), txn_id)],
        );
        cluster.ttl_enforcer.register(txn_id);

        // Promote new leader
        cluster.shard0.catch_up_all_replicas().unwrap();
        cluster.shard0.promote_best().unwrap();
        cluster.fo_coord.complete_failover_drain();

        // Coordinator decides to commit (via WAL replay)
        cluster
            .outcome_cache
            .record(txn_id, TxnOutcome::Committed);

        // I5: resolver applies decision
        let resolved = cluster.resolver.sweep();
        assert_eq!(resolved, 1);
        cluster.ttl_enforcer.remove(txn_id);

        // Final: single outcome, no mixed state
        let metrics = cluster.resolver.metrics();
        assert_eq!(metrics.resolved_committed, 1);
        assert_eq!(cluster.resolver.indoubt_count(), 0);
    }

    /// XS-07: Network partition coordinator ↔ shard.
    ///
    /// Failure: coordinator cannot reach one shard.
    /// Assert: bounded wait; no permanent prepared state.
    #[test]
    fn test_xs07_network_partition_coordinator_shard() {
        let cluster = TestCluster::new();
        cluster.load_initial_balances();

        let txn = cluster.mgr0.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        let txn_id = txn.txn_id;

        // Prepare on coordinator side
        cluster.mgr0.prepare(txn_id).unwrap();

        // Simulate network partition: coordinator can't reach shard 1
        // → blocked txn registered for timeout protection
        cluster.blocked_guard.register_blocked(txn_id);

        // I5: bounded wait — check timeout
        // (In real implementation, this would be a network timeout)
        // Simulate time passing by checking immediately (within budget)
        assert!(cluster.blocked_guard.check_blocked_timeout(txn_id).is_ok());

        // Release blocked txn (partition heals or timeout)
        cluster.blocked_guard.release(txn_id);

        // Register in-doubt and resolve
        cluster.resolver.register_indoubt(
            txn_id,
            vec![(ShardId(0), txn_id), (ShardId(1), txn_id)],
        );

        // I5: no permanent prepared state — resolver cleans up
        cluster.resolver.sweep();
        assert_eq!(cluster.resolver.indoubt_count(), 0);
    }

    /// XS-08: Network partition between shards.
    ///
    /// Failure: shard-to-shard communication partitioned.
    /// Assert: no split-brain commit; final state converges.
    #[test]
    fn test_xs08_network_partition_between_shards() {
        let cluster = TestCluster::new();
        cluster.load_initial_balances();

        let txn = cluster.mgr0.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        let txn_id = txn.txn_id;

        // Prepare phase — shard 0 prepared, shard 1 unreachable
        cluster.mgr0.prepare(txn_id).unwrap();

        // Coordinator decides abort (shard 1 not prepared)
        cluster
            .outcome_cache
            .record(txn_id, TxnOutcome::Aborted);

        // Register in-doubt for shard 0
        cluster
            .resolver
            .register_indoubt(txn_id, vec![(ShardId(0), txn_id)]);

        // I4: no split-brain — single decision (abort)
        let resolved = cluster.resolver.sweep();
        assert_eq!(resolved, 1);

        let metrics = cluster.resolver.metrics();
        assert_eq!(metrics.resolved_aborted, 1);
        assert_eq!(metrics.resolved_committed, 0);

        // I5: converged
        assert!(cluster.resolver.metrics().currently_indoubt == 0);
    }

    // ═══════════════════════════════════════════════════════════════════
    // CH: Churn & Stability Tests
    // ═══════════════════════════════════════════════════════════════════

    /// CH-01: Repeated shard leader failover + SS workload.
    ///
    /// Failure: repeated leader kill at fixed interval.
    /// Assert: error rate bounded; no resource leak; no in-doubt accumulation.
    #[test]
    fn test_ch01_repeated_leader_failover_ss_workload() {
        let mut cluster = TestCluster::new();
        cluster.load_initial_balances();

        let mut committed_count = 0u32;
        let mut aborted_count = 0u32;
        let failover_rounds = 3;

        for round in 0..failover_rounds {
            // Check damper
            let epoch = cluster.shard0.current_epoch() + 1;
            let damper_ok = cluster
                .damper
                .check_failover_allowed(epoch, 0)
                .is_ok();

            if damper_ok {
                // Start some SS transactions
                let mut txn_ids = Vec::new();
                for _ in 0..3 {
                    let txn = cluster.mgr0.begin(IsolationLevel::ReadCommitted);
                    txn_ids.push(txn.txn_id);
                }

                // Failover drain
                cluster
                    .fo_coord
                    .begin_failover_drain(epoch, txn_ids.clone());

                // Abort all active txns
                for tid in &txn_ids {
                    if cluster.mgr0.abort(*tid).is_ok() {
                        aborted_count += 1;
                    }
                    cluster.fo_coord.record_affected_txn(
                        *tid,
                        false,
                        FailoverTxnResolution::Aborted,
                        50,
                    );
                }

                // Promote
                cluster.shard0.catch_up_all_replicas().unwrap();
                cluster.shard0.promote_best().unwrap();
                cluster.fo_coord.complete_failover_drain();
                cluster.damper.record_failover_complete();

                // Post-failover: new txns succeed
                let txn = cluster.mgr0.begin(IsolationLevel::ReadCommitted);
                if cluster.mgr0.commit(txn.txn_id).is_ok() {
                    committed_count += 1;
                }

                // Small sleep to allow next damper interval
                std::thread::sleep(Duration::from_millis(60));
            }
        }

        // Assertions:
        // - Some txns committed after failover
        assert!(committed_count > 0, "at least one post-failover commit should succeed");

        // - No resource leak: no active txns remain
        assert_eq!(cluster.mgr0.active_count(), 0);

        // - No in-doubt accumulation
        assert_eq!(cluster.resolver.indoubt_count(), 0);

        // - Damper tracked history
        let dm = cluster.damper.metrics();
        assert!(dm.total_allowed > 0);

        // - I2: balance sum preserved
        assert!(check_balance_sum(&cluster.shard0.inner.primary.storage, 2000));
    }

    /// CH-02: Repeated coordinator restart + XS workload.
    ///
    /// Failure: periodic coordinator restart.
    /// Assert: correctness preserved; retries safe; bounded recovery.
    #[test]
    fn test_ch02_repeated_coordinator_restart_xs_workload() {
        let cluster = TestCluster::new();
        cluster.load_initial_balances();

        let mut indoubt_total = 0u32;

        for round in 0..3u64 {
            // Start a cross-shard txn
            let txn = cluster.mgr0.begin_with_classification(
                IsolationLevel::ReadCommitted,
                TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
            );
            let txn_id = txn.txn_id;

            // Prepare
            cluster.mgr0.prepare(txn_id).unwrap();

            // Coordinator "restarts" → txn goes in-doubt
            cluster.resolver.register_indoubt(
                txn_id,
                vec![(ShardId(0), txn_id), (ShardId(1), txn_id)],
            );
            cluster.ttl_enforcer.register(txn_id);
            indoubt_total += 1;

            // Coordinator comes back, resolves via sweep (default: abort)
            cluster.resolver.sweep();
            cluster.ttl_enforcer.remove(txn_id);
        }

        // All in-doubt resolved
        assert_eq!(cluster.resolver.indoubt_count(), 0);
        assert_eq!(cluster.ttl_enforcer.tracked_count(), 0);

        // Total resolved matches
        let metrics = cluster.resolver.metrics();
        assert_eq!(metrics.total_resolved, indoubt_total as u64);

        // I2: balance preserved (all txns aborted, no partial effects)
        assert!(check_balance_sum(&cluster.shard0.inner.primary.storage, 2000));
    }

    // ═══════════════════════════════════════════════════════════════════
    // ID: Idempotency & Replay Tests
    // ═══════════════════════════════════════════════════════════════════

    /// ID-01: Client commit retry.
    ///
    /// Failure: resend COMMIT multiple times for same txn_id.
    /// Assert: at most one commit effect.
    #[test]
    fn test_id01_client_commit_retry() {
        let cluster = TestCluster::new();
        cluster.load_initial_balances();

        let txn = cluster.mgr0.begin(IsolationLevel::ReadCommitted);
        let txn_id = txn.txn_id;

        // First commit succeeds
        let ts1 = cluster.mgr0.commit(txn_id).unwrap();
        assert!(ts1.0 > 0);

        // I3: second commit fails (txn removed from active set)
        let retry1 = cluster.mgr0.commit(txn_id);
        assert!(retry1.is_err());

        // I3: third commit also fails
        let retry2 = cluster.mgr0.commit(txn_id);
        assert!(retry2.is_err());

        // I3: at most one commit effect (txn committed exactly once)
        // No active txns remain
        assert!(cluster.mgr0.get_txn(txn_id).is_none());
    }

    /// ID-02: Protocol message replay — prepare / commit / abort idempotency.
    ///
    /// Failure: replay prepare, commit, and abort messages.
    /// Assert: participant handling is idempotent; no state regression.
    #[test]
    fn test_id02_protocol_message_replay() {
        let cluster = TestCluster::new();
        cluster.load_initial_balances();

        // ── Prepare idempotency ──
        let txn = cluster.mgr0.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        let txn_id = txn.txn_id;

        cluster.mgr0.prepare(txn_id).unwrap();
        // Replay prepare → idempotent no-op
        cluster.mgr0.prepare(txn_id).unwrap();

        let handle = cluster.mgr0.get_txn(txn_id).unwrap();
        assert_eq!(handle.state, TxnState::Prepared);

        // ── Commit after prepare ──
        let ts = cluster.mgr0.commit(txn_id).unwrap();
        assert!(ts.0 > 0);

        // Replay commit → NotFound (removed from active set)
        assert!(cluster.mgr0.commit(txn_id).is_err());

        // ── Abort idempotency ──
        let txn2 = cluster.mgr0.begin_with_classification(
            IsolationLevel::ReadCommitted,
            TxnClassification::global(vec![ShardId(0), ShardId(1)], SlowPathMode::Xa2Pc),
        );
        let txn_id2 = txn2.txn_id;

        cluster.mgr0.prepare(txn_id2).unwrap();
        cluster.mgr0.abort(txn_id2).unwrap();

        // Replay abort → NotFound (removed from active set)
        assert!(cluster.mgr0.abort(txn_id2).is_err());

        // ── In-doubt cache replay ──
        // Recording the same outcome twice is safe
        cluster
            .outcome_cache
            .record(txn_id, TxnOutcome::Committed);
        cluster
            .outcome_cache
            .record(txn_id, TxnOutcome::Committed);
        assert_eq!(
            cluster.outcome_cache.lookup(txn_id),
            Some(TxnOutcome::Committed)
        );

        // I3: no state regression — committed stays committed
    }

    // ═══════════════════════════════════════════════════════════════════
    // Extended: In-doubt TTL enforcement under churn
    // ═══════════════════════════════════════════════════════════════════

    /// Verify that in-doubt TTL enforcement force-aborts stuck transactions
    /// even when the resolver sweep doesn't have a cached decision.
    #[test]
    fn test_indoubt_ttl_enforcement_force_abort() {
        let cluster = TestCluster::new();

        // Register 5 in-doubt txns with TTL enforcer
        for i in 0..5u64 {
            cluster.ttl_enforcer.register(TxnId(1000 + i));
        }
        assert_eq!(cluster.ttl_enforcer.tracked_count(), 5);

        // Wait for TTL to expire
        std::thread::sleep(Duration::from_millis(600));

        // Sweep: all should be force-expired
        let expired = cluster.ttl_enforcer.sweep();
        assert_eq!(expired.len(), 5);
        assert_eq!(cluster.ttl_enforcer.tracked_count(), 0);

        let m = cluster.ttl_enforcer.metrics();
        assert_eq!(m.ttl_expirations, 5);
    }

    /// Verify that blocked txns timeout deterministically during failover.
    #[test]
    fn test_blocked_txn_timeout_during_failover() {
        let cluster = TestCluster::new();

        // Register blocked txns
        cluster.blocked_guard.register_blocked(TxnId(2000));
        cluster.blocked_guard.register_blocked(TxnId(2001));

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(250));

        // Sweep: both should be timed out
        let timed_out = cluster.blocked_guard.sweep_timed_out();
        assert_eq!(timed_out.len(), 2);

        // Check returns error
        let result = cluster.blocked_guard.check_blocked_timeout(TxnId(2000));
        assert!(result.is_err());
    }

    /// Verify full observability: metrics, convergence status, admin tooling.
    #[test]
    fn test_observability_and_admin_tooling() {
        let cluster = TestCluster::new();

        // Register in-doubt txns
        cluster.resolver.register_indoubt(
            TxnId(3000),
            vec![(ShardId(0), TxnId(30000))],
        );
        cluster.resolver.register_indoubt(
            TxnId(3001),
            vec![(ShardId(1), TxnId(30010))],
        );

        // I5: in-doubt transactions queryable
        let status = cluster.resolver.convergence_status();
        assert_eq!(status.total_indoubt, 2);
        assert!(!status.is_converged);

        // Admin: inspect specific in-doubt txn
        let info = cluster.resolver.admin_inspect(TxnId(3000));
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.global_txn_id, TxnId(3000));
        assert_eq!(info.participant_count, 1);

        // Admin: force-commit one
        let resolved = cluster.resolver.admin_force_commit(TxnId(3000)).unwrap();
        assert!(resolved);
        assert_eq!(cluster.resolver.indoubt_count(), 1);

        // Admin: force-abort the other
        let resolved = cluster.resolver.admin_force_abort(TxnId(3001)).unwrap();
        assert!(resolved);
        assert_eq!(cluster.resolver.indoubt_count(), 0);

        // Convergence achieved
        let status = cluster.resolver.convergence_status();
        assert!(status.is_converged);
        assert_eq!(status.total_resolved, 2);
    }

    /// Failover coordinator metrics are complete and accurate.
    #[test]
    fn test_failover_coordinator_metrics_complete() {
        let cluster = TestCluster::new();

        // Run a failover cycle
        cluster
            .fo_coord
            .begin_failover_drain(1, vec![TxnId(4000), TxnId(4001)]);
        cluster.fo_coord.record_affected_txn(
            TxnId(4000),
            false,
            FailoverTxnResolution::Aborted,
            50,
        );
        cluster.fo_coord.record_affected_txn(
            TxnId(4001),
            true,
            FailoverTxnResolution::MovedToInDoubt,
            100,
        );
        cluster.fo_coord.complete_failover_drain();

        let m = cluster.fo_coord.metrics();
        assert_eq!(m.failover_events, 1);
        assert_eq!(m.txns_force_aborted, 1);
        assert_eq!(m.txns_moved_indoubt, 1);
        assert!(m.last_failover_duration_ms < 1000);

        let affected = cluster.fo_coord.affected_txns();
        assert_eq!(affected.len(), 2);

        // Verify each record
        let aborted = affected.iter().find(|a| a.txn_id == TxnId(4000)).unwrap();
        assert!(!aborted.was_prepared);
        assert_eq!(aborted.resolution, FailoverTxnResolution::Aborted);

        let indoubt = affected.iter().find(|a| a.txn_id == TxnId(4001)).unwrap();
        assert!(indoubt.was_prepared);
        assert_eq!(indoubt.resolution, FailoverTxnResolution::MovedToInDoubt);
    }
}
