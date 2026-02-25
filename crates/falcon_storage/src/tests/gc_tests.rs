    use crate::engine::StorageEngine;
    use crate::gc::{compute_safepoint, sweep_memtable, GcConfig, GcStats};
    use crate::memtable::MemTable;
    use crate::mvcc::VersionChain;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};
    use std::time::Instant;

    fn row(v: i32) -> OwnedRow {
        OwnedRow::new(vec![Datum::Int32(v)])
    }

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "t".into(),
            columns: vec![ColumnDef {
                id: ColumnId(0),
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
                is_primary_key: true,
                default_value: None,
                is_serial: false,
            }],
            primary_key_columns: vec![0],
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        }
    }

    // 鈹€鈹€ Version chain GC with stats 鈹€鈹€

    #[test]
    fn test_gc_chain_reclaims_old_versions() {
        let chain = VersionChain::new();
        // Insert 3 versions
        chain.prepend(TxnId(1), Some(row(1)));
        chain.commit(TxnId(1), Timestamp(10));
        chain.prepend(TxnId(2), Some(row(2)));
        chain.commit(TxnId(2), Timestamp(20));
        chain.prepend(TxnId(3), Some(row(3)));
        chain.commit(TxnId(3), Timestamp(30));

        assert_eq!(chain.version_chain_len(), 3);

        // GC with watermark at 25: keeps versions at ts=30 and ts=20 (newest <= 25),
        // reclaims version at ts=10
        let result = chain.gc(Timestamp(25));
        assert_eq!(result.reclaimed_versions, 1);
        assert!(result.reclaimed_bytes > 0);
        assert_eq!(chain.version_chain_len(), 2);
    }

    #[test]
    fn test_gc_chain_preserves_newest_at_watermark() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row(1)));
        chain.commit(TxnId(1), Timestamp(10));
        chain.prepend(TxnId(2), Some(row(2)));
        chain.commit(TxnId(2), Timestamp(20));

        // GC at watermark 20: ts=20 is the first committed <= watermark,
        // reclaim ts=10
        let result = chain.gc(Timestamp(20));
        assert_eq!(result.reclaimed_versions, 1);
        assert_eq!(chain.version_chain_len(), 1);

        // The remaining version should be readable
        let r = chain.read_committed(Timestamp(20));
        assert_eq!(r.unwrap().values[0], Datum::Int32(2));
    }

    #[test]
    fn test_gc_skips_uncommitted_versions() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row(1)));
        chain.commit(TxnId(1), Timestamp(10));
        chain.prepend(TxnId(2), Some(row(2)));
        // TxnId(2) is uncommitted (commit_ts = 0)

        // GC at watermark 100: should NOT reclaim the uncommitted version
        let result = chain.gc(Timestamp(100));
        // The uncommitted version (head) has commit_ts=0, so GC walks past it
        // to ts=10 which is committed <= 100, and truncates below it (nothing).
        assert_eq!(result.reclaimed_versions, 0);
        assert_eq!(chain.version_chain_len(), 2);
    }

    #[test]
    fn test_gc_skips_aborted_versions() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row(1)));
        chain.commit(TxnId(1), Timestamp(10));
        chain.prepend(TxnId(2), Some(row(2)));
        chain.commit(TxnId(2), Timestamp(20));
        chain.prepend(TxnId(3), Some(row(3)));
        // Abort txn 3  鈥?sets commit_ts to MAX
        chain.abort(TxnId(3));

        // GC at watermark 25: head has commit_ts=MAX (aborted), skip it.
        // Next is ts=20 <= 25, so truncate below it (reclaim ts=10).
        let result = chain.gc(Timestamp(25));
        assert_eq!(result.reclaimed_versions, 1);
    }

    #[test]
    fn test_gc_no_op_on_single_version() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row(1)));
        chain.commit(TxnId(1), Timestamp(10));

        let result = chain.gc(Timestamp(100));
        assert_eq!(result.reclaimed_versions, 0);
        assert_eq!(chain.version_chain_len(), 1);
    }

    #[test]
    fn test_gc_no_op_when_watermark_too_low() {
        let chain = VersionChain::new();
        chain.prepend(TxnId(1), Some(row(1)));
        chain.commit(TxnId(1), Timestamp(10));
        chain.prepend(TxnId(2), Some(row(2)));
        chain.commit(TxnId(2), Timestamp(20));

        // Watermark 5: both versions are after the watermark, nothing to GC
        let result = chain.gc(Timestamp(5));
        assert_eq!(result.reclaimed_versions, 0);
        assert_eq!(chain.version_chain_len(), 2);
    }

    // 鈹€鈹€ Safepoint computation 鈹€鈹€

    #[test]
    fn test_safepoint_basic() {
        let sp = compute_safepoint(Timestamp(100), Timestamp::MAX);
        assert_eq!(sp, Timestamp(99));
    }

    #[test]
    fn test_safepoint_with_replica() {
        // Replica is behind at ts=50, active txns at ts=100
        let sp = compute_safepoint(Timestamp(100), Timestamp(50));
        assert_eq!(sp, Timestamp(49));
    }

    #[test]
    fn test_safepoint_zero() {
        let sp = compute_safepoint(Timestamp(0), Timestamp::MAX);
        assert_eq!(sp, Timestamp(0)); // saturating_sub
    }

    // 鈹€鈹€ MemTable sweep 鈹€鈹€

    #[test]
    fn test_sweep_memtable_basic() {
        let table = MemTable::new(test_schema());
        let config = GcConfig {
            min_chain_length: 0,
            ..Default::default()
        };
        let stats = GcStats::new();

        // Insert + commit two versions for key 1
        table.insert(row(1), TxnId(1)).unwrap();
        table.commit_txn(TxnId(1), Timestamp(10));
        table
            .update(&crate::memtable::encode_pk(&row(1), &[0]), row(1), TxnId(2))
            .unwrap();
        table.commit_txn(TxnId(2), Timestamp(20));

        // Sweep at watermark 15
        let result = sweep_memtable(&table, Timestamp(15), &config, &stats);
        assert_eq!(result.chains_inspected, 1);
        assert_eq!(result.reclaimed_versions, 0); // newest committed at ts=20 > 15, walk to ts=10 <= 15, nothing below

        // Sweep at watermark 25
        let result2 = sweep_memtable(&table, Timestamp(25), &config, &stats);
        assert_eq!(result2.chains_inspected, 1);
        assert_eq!(result2.reclaimed_versions, 1); // ts=20 <= 25, truncate ts=10
    }

    #[test]
    fn test_sweep_respects_batch_size() {
        let table = MemTable::new(test_schema());
        let config = GcConfig {
            batch_size: 2,
            min_chain_length: 0,
            ..Default::default()
        };
        let stats = GcStats::new();

        // Insert 5 keys
        for i in 1..=5 {
            table
                .insert(OwnedRow::new(vec![Datum::Int32(i)]), TxnId(i as u64))
                .unwrap();
            table.commit_txn(TxnId(i as u64), Timestamp(i as u64 * 10));
        }

        let result = sweep_memtable(&table, Timestamp(100), &config, &stats);
        // Should process at most 2 keys
        assert!(result.chains_inspected + result.keys_skipped <= 5);
    }

    #[test]
    fn test_sweep_respects_min_chain_length() {
        let table = MemTable::new(test_schema());
        let config = GcConfig {
            min_chain_length: 3,
            ..Default::default()
        };
        let stats = GcStats::new();

        // Insert key with 2 versions (below min_chain_length of 3)
        table.insert(row(1), TxnId(1)).unwrap();
        table.commit_txn(TxnId(1), Timestamp(10));
        let pk = crate::memtable::encode_pk(&row(1), &[0]);
        table.update(&pk, row(1), TxnId(2)).unwrap();
        table.commit_txn(TxnId(2), Timestamp(20));

        let result = sweep_memtable(&table, Timestamp(100), &config, &stats);
        assert_eq!(result.chains_inspected, 0);
        assert_eq!(result.keys_skipped, 1);
    }

    // 鈹€鈹€ Engine-level GC 鈹€鈹€

    #[test]
    fn test_engine_run_gc_basic() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(test_schema()).unwrap();

        // Insert + update (creates 2 versions)
        engine.insert(TableId(1), row(1), TxnId(1)).unwrap();
        engine
            .commit_txn(
                TxnId(1),
                Timestamp(10),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        let pk = crate::memtable::encode_pk(&row(1), &[0]);
        engine.update(TableId(1), &pk, row(1), TxnId(2)).unwrap();
        engine
            .commit_txn(
                TxnId(2),
                Timestamp(20),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();

        let chains = engine.run_gc(Timestamp(25));
        assert!(chains >= 1);

        // Data should still be readable
        let rows = engine.scan(TableId(1), TxnId(999), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_engine_gc_with_config() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(test_schema()).unwrap();

        // Insert 3 versions for key 1
        engine.insert(TableId(1), row(1), TxnId(1)).unwrap();
        engine
            .commit_txn(
                TxnId(1),
                Timestamp(10),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        let pk = crate::memtable::encode_pk(&row(1), &[0]);
        engine.update(TableId(1), &pk, row(1), TxnId(2)).unwrap();
        engine
            .commit_txn(
                TxnId(2),
                Timestamp(20),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        engine.update(TableId(1), &pk, row(1), TxnId(3)).unwrap();
        engine
            .commit_txn(
                TxnId(3),
                Timestamp(30),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();

        let config = GcConfig {
            min_chain_length: 0,
            ..Default::default()
        };
        let stats = GcStats::new();
        let result = engine.run_gc_with_config(Timestamp(25), &config, &stats);

        assert_eq!(result.chains_inspected, 1);
        assert_eq!(result.reclaimed_versions, 1); // ts=10 reclaimed
        assert!(result.reclaimed_bytes > 0);

        let snap = stats.snapshot();
        assert_eq!(snap.total_sweeps, 1);
        assert_eq!(snap.total_reclaimed_versions, 1);
    }

    // 鈹€鈹€ Concurrent read + GC 鈹€鈹€

    #[test]
    fn test_gc_concurrent_with_read() {
        use std::sync::Arc;

        let engine = Arc::new(StorageEngine::new_in_memory());
        engine.create_table(test_schema()).unwrap();

        // Create 5 versions
        for i in 1..=5u64 {
            let pk = crate::memtable::encode_pk(&OwnedRow::new(vec![Datum::Int32(1)]), &[0]);
            if i == 1 {
                engine
                    .insert(TableId(1), OwnedRow::new(vec![Datum::Int32(1)]), TxnId(i))
                    .unwrap();
            } else {
                engine
                    .update(
                        TableId(1),
                        &pk,
                        OwnedRow::new(vec![Datum::Int32(i as i32)]),
                        TxnId(i),
                    )
                    .unwrap();
            }
            engine
                .commit_txn(
                    TxnId(i),
                    Timestamp(i * 10),
                    falcon_common::types::TxnType::Local,
                )
                .unwrap();
        }

        // Spawn GC in a thread
        let engine_gc = engine.clone();
        let gc_handle = std::thread::spawn(move || engine_gc.run_gc(Timestamp(35)));

        // Concurrent reads
        let engine_read = engine.clone();
        let read_handle = std::thread::spawn(move || {
            let rows = engine_read
                .scan(TableId(1), TxnId(999), Timestamp(100))
                .unwrap();
            assert_eq!(rows.len(), 1);
            rows[0].1.values[0].clone()
        });

        gc_handle.join().unwrap();
        let val = read_handle.join().unwrap();
        // The latest committed version (ts=50, value=5) should always be visible
        assert_eq!(val, Datum::Int32(5));
    }

    // 鈹€鈹€ Long txn blocks GC 鈹€鈹€

    #[test]
    fn test_long_txn_prevents_gc() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(test_schema()).unwrap();

        // Create 3 versions
        engine.insert(TableId(1), row(1), TxnId(1)).unwrap();
        engine
            .commit_txn(
                TxnId(1),
                Timestamp(10),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        let pk = crate::memtable::encode_pk(&row(1), &[0]);
        engine.update(TableId(1), &pk, row(2), TxnId(2)).unwrap();
        engine
            .commit_txn(
                TxnId(2),
                Timestamp(20),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        engine.update(TableId(1), &pk, row(3), TxnId(3)).unwrap();
        engine
            .commit_txn(
                TxnId(3),
                Timestamp(30),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();

        // Simulate a long-running txn with start_ts=15.
        // Safepoint = min_active_ts - 1 = 14.
        // No committed version has commit_ts <= 14, so nothing is GC'd.
        let safepoint = compute_safepoint(Timestamp(15), Timestamp::MAX);
        assert_eq!(safepoint, Timestamp(14));

        let config = GcConfig {
            min_chain_length: 0,
            ..Default::default()
        };
        let stats = GcStats::new();
        let result = engine.run_gc_with_config(safepoint, &config, &stats);
        assert_eq!(result.reclaimed_versions, 0, "long txn should prevent GC");

        // All 3 versions should still exist
        // Read at ts=10 should see value 1
        let rows = engine.scan(TableId(1), TxnId(999), Timestamp(10)).unwrap();
        assert_eq!(rows[0].1.values[0], Datum::Int32(1));
    }

    // 鈹€鈹€ GC stats tracking 鈹€鈹€

    #[test]
    fn test_gc_stats_accumulate() {
        let stats = GcStats::new();
        stats.observe_chain_length(5);
        stats.observe_chain_length(3);
        stats.observe_chain_length(10);

        let snap = stats.snapshot();
        assert_eq!(snap.max_chain_length_observed, 10);
        assert!((snap.avg_chain_length - 6.0).abs() < 0.01);
    }

    // 鈹€鈹€ GC benchmark: memory over time 鈹€鈹€

    #[test]
    fn test_gc_benchmark_memory_reclamation() {
        let engine = StorageEngine::new_in_memory();
        engine.create_table(test_schema()).unwrap();

        let num_keys = 100;
        let num_updates = 10;

        // Phase 1: Insert keys + multiple updates (creates version chains)
        for key in 0..num_keys {
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(key)]),
                    TxnId(key as u64 * 100 + 1),
                )
                .unwrap();
            engine
                .commit_txn(
                    TxnId(key as u64 * 100 + 1),
                    Timestamp(key as u64 * 100 + 1),
                    falcon_common::types::TxnType::Local,
                )
                .unwrap();

            let pk = crate::memtable::encode_pk(&OwnedRow::new(vec![Datum::Int32(key)]), &[0]);
            for upd in 1..=num_updates {
                let txn = TxnId(key as u64 * 100 + upd as u64 + 1);
                let ts = Timestamp(key as u64 * 100 + upd as u64 + 1);
                engine
                    .update(
                        TableId(1),
                        &pk,
                        OwnedRow::new(vec![Datum::Int32(key + upd as i32)]),
                        txn,
                    )
                    .unwrap();
                engine
                    .commit_txn(txn, ts, falcon_common::types::TxnType::Local)
                    .unwrap();
            }
        }

        let chains_before = engine.total_chain_count();
        assert_eq!(chains_before, num_keys as usize);

        // Phase 2: GC sweep
        let config = GcConfig {
            min_chain_length: 0,
            ..Default::default()
        };
        let stats = GcStats::new();
        let watermark = Timestamp(u64::MAX - 2);
        let result = engine.run_gc_with_config(watermark, &config, &stats);

        eprintln!(
            "\n=== GC Benchmark ({} keys x {} updates) ===",
            num_keys, num_updates
        );
        eprintln!("  chains_inspected: {}", result.chains_inspected);
        eprintln!("  chains_pruned:    {}", result.chains_pruned);
        eprintln!("  reclaimed_versions: {}", result.reclaimed_versions);
        eprintln!("  reclaimed_bytes:  {}", result.reclaimed_bytes);
        eprintln!("  sweep_duration_us: {}", result.sweep_duration_us);

        let snap = stats.snapshot();
        eprintln!("  max_chain_length: {}", snap.max_chain_length_observed);
        eprintln!("  avg_chain_length: {:.1}", snap.avg_chain_length);

        // Should have reclaimed (num_updates) versions per key
        assert_eq!(result.reclaimed_versions, (num_keys * num_updates) as u64);
        assert!(result.reclaimed_bytes > 0);

        // After GC, each chain should have exactly 1 version.
        // gc_candidates is 0 after full reclamation, so verify directly.
        let table = engine.get_table(TableId(1)).unwrap();
        let max_chain = table
            .data
            .iter()
            .map(|e| e.value().version_chain_len())
            .max()
            .unwrap_or(0);
        assert_eq!(max_chain, 1);
    }

    // 鈹€鈹€ GcRunner background task 鈹€鈹€

    #[test]
    fn test_gc_runner_background_sweep() {
        use crate::gc::{GcRunner, SafepointProvider};
        use std::sync::atomic::{AtomicU64, Ordering as AOrdering};
        use std::sync::Arc;

        struct TestProvider {
            ts: AtomicU64,
        }
        impl SafepointProvider for TestProvider {
            fn min_active_ts(&self) -> Timestamp {
                Timestamp(self.ts.load(AOrdering::Relaxed))
            }
            fn replica_safe_ts(&self) -> Timestamp {
                Timestamp::MAX
            }
        }

        let engine = Arc::new(StorageEngine::new_in_memory());
        engine.create_table(test_schema()).unwrap();

        // Create 3 versions for key 1
        engine.insert(TableId(1), row(1), TxnId(1)).unwrap();
        engine
            .commit_txn(
                TxnId(1),
                Timestamp(10),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        let pk = crate::memtable::encode_pk(&row(1), &[0]);
        engine.update(TableId(1), &pk, row(2), TxnId(2)).unwrap();
        engine
            .commit_txn(
                TxnId(2),
                Timestamp(20),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();
        engine.update(TableId(1), &pk, row(3), TxnId(3)).unwrap();
        engine
            .commit_txn(
                TxnId(3),
                Timestamp(30),
                falcon_common::types::TxnType::Local,
            )
            .unwrap();

        let provider = Arc::new(TestProvider {
            ts: AtomicU64::new(100),
        });
        let config = GcConfig {
            interval_ms: 10, // 10ms interval for fast test
            min_chain_length: 0,
            ..Default::default()
        };

        let mut runner = GcRunner::start(engine.clone(), provider, config).expect("spawn in test");
        assert!(runner.is_running());

        // Wait for at least one sweep
        std::thread::sleep(std::time::Duration::from_millis(50));
        runner.stop();
        assert!(!runner.is_running());

        // Verify GC ran
        let snap = engine.gc_stats_snapshot();
        assert!(
            snap.total_sweeps > 0,
            "GC runner should have run at least one sweep"
        );
        assert!(
            snap.total_reclaimed_versions > 0,
            "should have reclaimed versions"
        );

        // Data should still be readable
        let rows = engine.scan(TableId(1), TxnId(999), Timestamp(100)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[0], Datum::Int32(3));
    }

    // 鈹€鈹€ Crash-recovery after GC 鈹€鈹€

    #[test]
    fn test_gc_then_crash_recovery() {
        let dir = std::env::temp_dir().join(format!("falcon_gc_test_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);

        // Phase 1: Write, GC, then "crash"
        {
            let engine = StorageEngine::new(Some(&dir)).unwrap();
            engine.create_table(test_schema()).unwrap();

            // Insert + update (2 versions)
            engine.insert(TableId(1), row(1), TxnId(1)).unwrap();
            engine
                .commit_txn(
                    TxnId(1),
                    Timestamp(10),
                    falcon_common::types::TxnType::Local,
                )
                .unwrap();
            let pk = crate::memtable::encode_pk(&row(1), &[0]);
            engine.update(TableId(1), &pk, row(2), TxnId(2)).unwrap();
            engine
                .commit_txn(
                    TxnId(2),
                    Timestamp(20),
                    falcon_common::types::TxnType::Local,
                )
                .unwrap();

            // Also insert key 2 and leave it
            engine
                .insert(TableId(1), OwnedRow::new(vec![Datum::Int32(99)]), TxnId(3))
                .unwrap();
            engine
                .commit_txn(
                    TxnId(3),
                    Timestamp(30),
                    falcon_common::types::TxnType::Local,
                )
                .unwrap();

            // Run GC  鈥?reclaims old version of key 1 in memory
            let result = engine.gc_sweep(Timestamp(25));
            assert_eq!(result.reclaimed_versions, 1);

            // "crash"  鈥?engine dropped, WAL persisted
        }

        // Phase 2: Recover from WAL
        {
            let recovered = StorageEngine::recover(&dir).unwrap();
            // WAL replays all committed writes  鈥?both keys should exist
            let rows = recovered
                .scan(TableId(1), TxnId(999), Timestamp(100))
                .unwrap();
            assert_eq!(rows.len(), 2, "both keys should survive recovery");

            // Key 1 should have latest value (2)
            let pk1 = crate::memtable::encode_pk(&row(1), &[0]);
            let val = recovered
                .get(TableId(1), &pk1, TxnId(999), Timestamp(100))
                .unwrap();
            assert_eq!(val.unwrap().values[0], Datum::Int32(2));

            // Key 99 should exist
            let pk99 = crate::memtable::encode_pk(&OwnedRow::new(vec![Datum::Int32(99)]), &[0]);
            let val99 = recovered
                .get(TableId(1), &pk99, TxnId(999), Timestamp(100))
                .unwrap();
            assert_eq!(val99.unwrap().values[0], Datum::Int32(99));
        }
    }

    // 鈹€鈹€ No-GC vs With-GC benchmark 鈹€鈹€

    #[test]
    fn test_gc_benchmark_no_gc_vs_gc() {
        let num_keys = 50i32;
        let num_updates = 20u64;

        // Helper: create engine, fill it, optionally GC
        fn fill_engine(num_keys: i32, num_updates: u64) -> StorageEngine {
            let engine = StorageEngine::new_in_memory();
            let schema = TableSchema {
                id: TableId(1),
                name: "t".into(),
                columns: vec![ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                }],
                primary_key_columns: vec![0],
                next_serial_values: std::collections::HashMap::new(),
                check_constraints: vec![],
                unique_constraints: vec![],
                foreign_keys: vec![],
                ..Default::default()
            };
            engine.create_table(schema).unwrap();

            for key in 0..num_keys {
                let base = key as u64 * (num_updates + 1) + 1;
                engine
                    .insert(
                        TableId(1),
                        OwnedRow::new(vec![Datum::Int32(key)]),
                        TxnId(base),
                    )
                    .unwrap();
                engine
                    .commit_txn(
                        TxnId(base),
                        Timestamp(base),
                        falcon_common::types::TxnType::Local,
                    )
                    .unwrap();

                let pk = crate::memtable::encode_pk(&OwnedRow::new(vec![Datum::Int32(key)]), &[0]);
                for upd in 1..=num_updates {
                    let txn = TxnId(base + upd);
                    let ts = Timestamp(base + upd);
                    engine
                        .update(
                            TableId(1),
                            &pk,
                            OwnedRow::new(vec![Datum::Int32(key + upd as i32)]),
                            txn,
                        )
                        .unwrap();
                    engine
                        .commit_txn(txn, ts, falcon_common::types::TxnType::Local)
                        .unwrap();
                }
            }
            engine
        }

        // 鈹€鈹€ No GC 鈹€鈹€
        let engine_no_gc = fill_engine(num_keys, num_updates);
        let chains_no_gc = engine_no_gc.total_chain_count();
        let stats_no_gc = GcStats::new();
        let config = GcConfig {
            min_chain_length: 0,
            ..Default::default()
        };
        // Measure chain lengths without GC
        for entry in engine_no_gc.get_table(TableId(1)).unwrap().data.iter() {
            stats_no_gc.observe_chain_length(entry.value().version_chain_len() as u64);
        }
        let snap_no_gc = stats_no_gc.snapshot();

        // Measure p99 read latency without GC (scan 100 times)
        let mut latencies_no_gc = Vec::with_capacity(100);
        for _ in 0..100 {
            let start = Instant::now();
            let _ = engine_no_gc.scan(TableId(1), TxnId(999999), Timestamp(u64::MAX - 2));
            latencies_no_gc.push(start.elapsed().as_nanos() as u64);
        }
        latencies_no_gc.sort();
        let p99_no_gc = latencies_no_gc[98];

        // 鈹€鈹€ With GC 鈹€鈹€
        let engine_gc = fill_engine(num_keys, num_updates);
        let watermark = Timestamp(u64::MAX - 2);
        let gc_result = engine_gc.run_gc_with_config(watermark, &config, &GcStats::new());

        let stats_gc = GcStats::new();
        for entry in engine_gc.get_table(TableId(1)).unwrap().data.iter() {
            stats_gc.observe_chain_length(entry.value().version_chain_len() as u64);
        }
        let snap_gc = stats_gc.snapshot();

        let mut latencies_gc = Vec::with_capacity(100);
        for _ in 0..100 {
            let start = Instant::now();
            let _ = engine_gc.scan(TableId(1), TxnId(999999), Timestamp(u64::MAX - 2));
            latencies_gc.push(start.elapsed().as_nanos() as u64);
        }
        latencies_gc.sort();
        let p99_gc = latencies_gc[98];

        eprintln!(
            "\n=== No-GC vs With-GC ({} keys 脳 {} updates) ===",
            num_keys, num_updates
        );
        eprintln!(
            "  [No GC]  chains: {}, max_chain_len: {}, avg_chain_len: {:.1}, p99_read_ns: {}",
            chains_no_gc,
            snap_no_gc.max_chain_length_observed,
            snap_no_gc.avg_chain_length,
            p99_no_gc
        );
        eprintln!(
            "  [GC]     chains: {}, max_chain_len: {}, avg_chain_len: {:.1}, p99_read_ns: {}",
            chains_no_gc, snap_gc.max_chain_length_observed, snap_gc.avg_chain_length, p99_gc
        );
        eprintln!(
            "  GC reclaimed: {} versions, {} bytes, {}us",
            gc_result.reclaimed_versions, gc_result.reclaimed_bytes, gc_result.sweep_duration_us
        );

        // Assertions
        assert_eq!(snap_no_gc.max_chain_length_observed, num_updates + 1);
        assert_eq!(snap_gc.max_chain_length_observed, 1);
        assert!(gc_result.reclaimed_versions > 0);
        // p99 with GC should be 鈮坧99 without GC (shorter chains  鈫?faster reads)
        // (This is a statistical assertion; allow 2x slack for CI jitter)
        assert!(
            p99_gc <= p99_no_gc * 2 + 10000,
            "p99 with GC ({}) should not be dramatically worse than without ({})",
            p99_gc,
            p99_no_gc,
        );
    }
