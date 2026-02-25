    use crate::engine::StorageEngine;
    use crate::memtable::encode_column_value;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, TableId, Timestamp, TxnId};
    use std::time::Instant;

    fn bench_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "bench".into(),
            columns: vec![
                ColumnDef {
                    id: ColumnId(0),
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(1),
                    name: "val".into(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
            ],
            primary_key_columns: vec![0],
            next_serial_values: std::collections::HashMap::new(),
            check_constraints: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            ..Default::default()
        }
    }

    /// Benchmark 1: Commit latency should be O(|write_set|), NOT O(data_size).
    /// Pre-populate with N rows, then commit a single-row write and verify
    /// commit time does not scale linearly with N.
    #[test]
    fn bench_commit_latency_vs_data_scale() {
        let scales: Vec<usize> = vec![100, 1_000, 5_000, 10_000];
        let mut results: Vec<(usize, u128)> = Vec::new();

        for &n in &scales {
            let engine = StorageEngine::new_in_memory();
            engine.create_table(bench_schema()).unwrap();

            // Pre-populate with n committed rows
            let bulk_txn = TxnId(1);
            for i in 0..n {
                engine
                    .insert(
                        TableId(1),
                        OwnedRow::new(vec![
                            Datum::Int32(i as i32),
                            Datum::Text(format!("row_{}", i)),
                        ]),
                        bulk_txn,
                    )
                    .unwrap();
            }
            engine.commit_txn_local(bulk_txn, Timestamp(10)).unwrap();

            // Write 1 row in a new txn and measure commit time
            let write_txn = TxnId(2);
            engine
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(n as i32 + 1), Datum::Text("new".into())]),
                    write_txn,
                )
                .unwrap();

            let start = Instant::now();
            engine.commit_txn_local(write_txn, Timestamp(20)).unwrap();
            let elapsed = start.elapsed().as_micros();

            results.push((n, elapsed));
        }

        eprintln!("\n=== Commit Latency vs Data Scale (1-row write-set) ===");
        for (n, us) in &results {
            eprintln!("  data_size={:>7}  commit_us={:>6}", n, us);
        }

        // With write-set driven commit, commit at 100k should not be 100x slower than at 100.
        let (_, us_small) = results[0];
        let (_, us_large) = results[results.len() - 1];
        let ratio = if us_small == 0 {
            1.0
        } else {
            us_large as f64 / us_small.max(1) as f64
        };
        eprintln!("  ratio (largest/smallest) = {:.1}x", ratio);
        assert!(
            ratio < 20.0,
            "commit latency ratio {:.1}x exceeds 20x threshold  鈥?commit may not be O(|write_set|)",
            ratio
        );
    }

    /// Benchmark 2: Index maintenance overhead during writes.
    /// Measures write+commit throughput with and without a secondary index.
    #[test]
    fn bench_index_maintenance_overhead() {
        let num_rows: i32 = 5_000;

        // Without index
        let engine_no_idx = StorageEngine::new_in_memory();
        engine_no_idx.create_table(bench_schema()).unwrap();

        let start_no_idx = Instant::now();
        let txn1 = TxnId(1);
        for i in 0..num_rows {
            engine_no_idx
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("v{}", i))]),
                    txn1,
                )
                .unwrap();
        }
        engine_no_idx.commit_txn_local(txn1, Timestamp(10)).unwrap();
        let elapsed_no_idx = start_no_idx.elapsed().as_micros();

        // With index
        let engine_idx = StorageEngine::new_in_memory();
        engine_idx.create_table(bench_schema()).unwrap();
        engine_idx.create_index("bench", 1).unwrap();

        let start_idx = Instant::now();
        let txn2 = TxnId(1);
        for i in 0..num_rows {
            engine_idx
                .insert(
                    TableId(1),
                    OwnedRow::new(vec![Datum::Int32(i), Datum::Text(format!("v{}", i))]),
                    txn2,
                )
                .unwrap();
        }
        engine_idx.commit_txn_local(txn2, Timestamp(10)).unwrap();
        let elapsed_idx = start_idx.elapsed().as_micros();

        // Verify index correctness
        let sample_key = encode_column_value(&Datum::Text("v42".into()));
        let pks = engine_idx.index_lookup(TableId(1), 1, &sample_key).unwrap();
        assert_eq!(pks.len(), 1, "index should contain sample row after commit");

        let overhead = if elapsed_no_idx == 0 {
            1.0
        } else {
            elapsed_idx as f64 / elapsed_no_idx as f64
        };

        eprintln!("\n=== Index Maintenance Overhead ({} rows) ===", num_rows);
        eprintln!("  without_index: {}us", elapsed_no_idx);
        eprintln!("  with_index:    {}us", elapsed_idx);
        eprintln!("  overhead:      {:.2}x", overhead);

        // Index overhead should be reasonable (< 5x for a single column index)
        assert!(
            overhead < 5.0,
            "index maintenance overhead {:.2}x exceeds 5x threshold",
            overhead
        );
    }
