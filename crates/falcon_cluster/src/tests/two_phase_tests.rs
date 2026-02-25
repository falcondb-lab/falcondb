    use std::sync::Arc;
    use std::time::Duration;

    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{
        ColumnId, DataType, IsolationLevel, ShardId, TableId, Timestamp, TxnId,
    };

    use crate::sharded_engine::ShardedEngine;
    use crate::two_phase::TwoPhaseCoordinator;

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "tpc_test".into(),
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

    #[test]
    fn test_two_phase_commit_all_succeed() {
        let engine = Arc::new(ShardedEngine::new(3));
        engine.create_table_all(&test_schema()).unwrap();

        let coord = TwoPhaseCoordinator::new(engine.clone(), Duration::from_secs(10));
        let target = vec![ShardId(0), ShardId(1), ShardId(2)];

        let result = coord
            .execute(
                &target,
                IsolationLevel::ReadCommitted,
                |storage, _txn_mgr, txn_id| {
                    let row = OwnedRow::new(vec![Datum::Int32(42)]);
                    storage.insert(TableId(1), row, txn_id)?;
                    Ok(())
                },
            )
            .unwrap();

        assert!(result.committed, "all shards should commit");
        assert_eq!(result.participants.len(), 3);
        assert!(result.participants.iter().all(|p| p.prepared));

        // Verify data is visible on all shards
        for &sid in &target {
            let shard = engine.shard(sid).unwrap();
            let rows = shard
                .storage
                .scan(TableId(1), TxnId(999), Timestamp(1000))
                .unwrap();
            assert_eq!(rows.len(), 1, "shard {:?} should have 1 row", sid);
        }
    }

    #[test]
    fn test_two_phase_abort_on_failure() {
        let engine = Arc::new(ShardedEngine::new(3));
        engine.create_table_all(&test_schema()).unwrap();

        let coord = TwoPhaseCoordinator::new(engine.clone(), Duration::from_secs(10));
        let target = vec![ShardId(0), ShardId(1), ShardId(2)];

        let result = coord
            .execute(
                &target,
                IsolationLevel::ReadCommitted,
                |storage, _txn_mgr, txn_id| {
                    // Insert on all shards  鈥?but shard 2 (3rd) gets a duplicate PK
                    // which we simulate by inserting the same key twice.
                    let row = OwnedRow::new(vec![Datum::Int32(1)]);
                    storage.insert(TableId(1), row.clone(), txn_id)?;
                    // Insert duplicate  鈥?this will fail on the storage layer
                    storage.insert(TableId(1), row, txn_id)?;
                    Ok(())
                },
            )
            .unwrap();

        // Should abort because the write_fn fails (duplicate PK insert).
        assert!(!result.committed, "should abort due to failure");
    }

    #[test]
    fn test_two_phase_metrics() {
        let engine = Arc::new(ShardedEngine::new(2));
        engine.create_table_all(&test_schema()).unwrap();

        let coord = TwoPhaseCoordinator::new(engine.clone(), Duration::from_secs(10));
        let target = vec![ShardId(0), ShardId(1)];

        let result = coord
            .execute(
                &target,
                IsolationLevel::ReadCommitted,
                |storage, _txn_mgr, txn_id| {
                    let row = OwnedRow::new(vec![Datum::Int32(1)]);
                    storage.insert(TableId(1), row, txn_id)?;
                    Ok(())
                },
            )
            .unwrap();

        assert!(result.committed);
        assert!(result.prepare_latency_us > 0 || result.commit_latency_us == 0);
        assert_eq!(result.participants.len(), 2);
    }

    #[test]
    fn test_merge_bool_and_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // BoolAnd merge: all true  鈫?true; one false  鈫?false.
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Int32),
                ("ba".into(), DataType::Boolean),
            ],
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Boolean(true)])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Boolean(false)])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::BoolAnd(1)]);
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].values[1],
            Datum::Boolean(false),
            "true AND false = false"
        );
    }

    #[test]
    fn test_merge_bool_or_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // BoolOr merge: all false  鈫?false; one true  鈫?true.
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Int32),
                ("bo".into(), DataType::Boolean),
            ],
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Boolean(false)])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Boolean(true)])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::BoolOr(1)]);
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].values[1],
            Datum::Boolean(true),
            "false OR true = true"
        );
    }

    #[test]
    fn test_merge_string_agg_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // STRING_AGG merge: concatenate partial strings with separator.
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Int32),
                ("sa".into(), DataType::Text),
            ],
            rows: vec![OwnedRow::new(vec![
                Datum::Int32(1),
                Datum::Text("a,b".into()),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Int32(1),
                Datum::Text("c,d".into()),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::StringAgg(1, ",".into())]);
        assert_eq!(merged.len(), 1);
        match &merged[0].values[1] {
            Datum::Text(s) => {
                assert_eq!(s, "a,b,c,d", "STRING_AGG should concat with separator");
            }
            other => panic!("Expected Text, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_string_agg_with_null() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // STRING_AGG with NULL: NULL should be skipped.
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Int32),
                ("sa".into(), DataType::Text),
            ],
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Null])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Int32(1),
                Datum::Text("x".into()),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::StringAgg(1, ",".into())]);
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].values[1],
            Datum::Text("x".into()),
            "NULL should be skipped"
        );
    }

    #[test]
    fn test_merge_array_agg_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Int32),
                ("aa".into(), DataType::Array(Box::new(DataType::Int32))),
            ],
            rows: vec![OwnedRow::new(vec![
                Datum::Int32(1),
                Datum::Array(vec![Datum::Int32(10), Datum::Int32(20)]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Int32(1),
                Datum::Array(vec![Datum::Int32(30)]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::ArrayAgg(1)]);
        assert_eq!(merged.len(), 1);
        match &merged[0].values[1] {
            Datum::Array(arr) => {
                assert_eq!(arr.len(), 3, "Should concatenate arrays: [10,20] ++ [30]");
                assert_eq!(arr[0], Datum::Int32(10));
                assert_eq!(arr[1], Datum::Int32(20));
                assert_eq!(arr[2], Datum::Int32(30));
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_array_agg_with_null() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Int32),
                ("aa".into(), DataType::Array(Box::new(DataType::Int32))),
            ],
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Null])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Int32(1),
                Datum::Array(vec![Datum::Int32(5)]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::ArrayAgg(1)]);
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].values[1],
            Datum::Array(vec![Datum::Int32(5)]),
            "NULL shard should be skipped"
        );
    }

    #[test]
    fn test_cmp_datum_cross_type() {
        use crate::distributed_exec::compare_datums;
        // Int32 vs Int64
        assert_eq!(
            compare_datums(Some(&Datum::Int32(5)), Some(&Datum::Int64(10))),
            std::cmp::Ordering::Less
        );
        // Float64 vs Int32
        assert_eq!(
            compare_datums(Some(&Datum::Float64(3.5)), Some(&Datum::Int32(3))),
            std::cmp::Ordering::Greater
        );
        // Null sorts before everything
        assert_eq!(
            compare_datums(Some(&Datum::Null), Some(&Datum::Int32(0))),
            std::cmp::Ordering::Less
        );
        // Timestamp comparison
        assert_eq!(
            compare_datums(Some(&Datum::Timestamp(100)), Some(&Datum::Timestamp(200))),
            std::cmp::Ordering::Less
        );
        // Text comparison
        assert_eq!(
            compare_datums(
                Some(&Datum::Text("apple".into())),
                Some(&Datum::Text("banana".into()))
            ),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn test_datum_add_cross_type() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // SUM merge with Int32 + Int64 (cross-type)
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Int32),
                ("s".into(), DataType::Int64),
            ],
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Int32(10)])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![Datum::Int32(1), Datum::Int64(20)])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::Sum(1)]);
        assert_eq!(merged.len(), 1);
        // Int32(10) + Int64(20) should produce Int64(30)
        assert_eq!(merged[0].values[1], Datum::Int64(30));
    }

    #[test]
    fn test_merge_count_distinct_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Shard 0: group "a" has distinct values [1, 2, 3]
        // Shard 1: group "a" has distinct values [2, 3, 4]
        // After merge: deduplicated = {1, 2, 3, 4}  鈫?count = 4
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Text),
                ("cd".into(), DataType::Int64),
            ],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(1), Datum::Int32(2), Datum::Int32(3)]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(2), Datum::Int32(3), Datum::Int32(4)]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::CountDistinct(1)]);
        assert_eq!(merged.len(), 1);
        // Deduplicated {1,2,3,4}  鈫?count = 4
        assert_eq!(merged[0].values[1], Datum::Int64(4));
    }

    #[test]
    fn test_merge_count_distinct_with_null_shard() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Shard 0: group "x" has [10, 20]
        // Shard 1: group "x" has NULL (no rows matched)
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Text),
                ("cd".into(), DataType::Int64),
            ],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("x".into()),
                Datum::Array(vec![Datum::Int32(10), Datum::Int32(20)]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![Datum::Text("x".into()), Datum::Null])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::CountDistinct(1)]);
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].values[1],
            Datum::Int64(2),
            "NULL shard should be skipped, count=2"
        );
    }

    #[test]
    fn test_merge_sum_distinct_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Shard 0: group "a" has distinct values [10, 20, 30]
        // Shard 1: group "a" has distinct values [20, 30, 40]
        // After merge: unique = {10, 20, 30, 40}  鈫?sum = 100
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Text),
                ("sd".into(), DataType::Int64),
            ],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(10), Datum::Int32(20), Datum::Int32(30)]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(20), Datum::Int32(30), Datum::Int32(40)]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::SumDistinct(1)]);
        assert_eq!(merged.len(), 1);
        // Unique {10,20,30,40}: Int32 values get promoted to Int64 via datum_add
        assert_eq!(merged[0].values[1], Datum::Int64(100));
    }

    #[test]
    fn test_merge_avg_distinct_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Shard 0: group "a" has distinct values [10, 20, 30]
        // Shard 1: group "a" has distinct values [20, 30, 40]
        // After merge: unique = {10, 20, 30, 40}  鈫?avg = 100/4 = 25.0
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Text),
                ("ad".into(), DataType::Float64),
            ],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(10), Datum::Int32(20), Datum::Int32(30)]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(20), Datum::Int32(30), Datum::Int32(40)]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::AvgDistinct(1)]);
        assert_eq!(merged.len(), 1);
        // Unique {10,20,30,40}: avg = 100/4 = 25.0
        assert_eq!(merged[0].values[1], Datum::Float64(25.0));
    }

    #[test]
    fn test_merge_string_agg_distinct_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Shard 0: group "a" has distinct values ["hello", "world"]
        // Shard 1: group "a" has distinct values ["world", "foo"]
        // After merge: unique = {"hello", "world", "foo"}  鈫?joined with ","
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Text),
                ("sa".into(), DataType::Text),
            ],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![
                    Datum::Text("hello".into()),
                    Datum::Text("world".into()),
                ]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Text("world".into()), Datum::Text("foo".into())]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(
            &[sr1, sr2],
            &[0],
            &[AggMerge::StringAggDistinct(1, ",".into())],
        );
        assert_eq!(merged.len(), 1);
        // Should be 3 unique values joined with ","
        match &merged[0].values[1] {
            Datum::Text(s) => {
                let parts: Vec<&str> = s.split(',').collect();
                assert_eq!(parts.len(), 3, "3 unique values joined");
                assert!(parts.contains(&"hello"));
                assert!(parts.contains(&"world"));
                assert!(parts.contains(&"foo"));
            }
            other => panic!("Expected Text, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_array_agg_distinct_across_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Shard 0: group "a" has distinct values [1, 2, 3]
        // Shard 1: group "a" has distinct values [2, 3, 4]
        // After merge: unique = {1, 2, 3, 4}  鈫?returned as array
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Text),
                ("aa".into(), DataType::Int32),
            ],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(1), Datum::Int32(2), Datum::Int32(3)]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(2), Datum::Int32(3), Datum::Int32(4)]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::ArrayAggDistinct(1)]);
        assert_eq!(merged.len(), 1);
        match &merged[0].values[1] {
            Datum::Array(arr) => {
                assert_eq!(arr.len(), 4, "4 unique values");
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_distinct_null_handling() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Edge case: arrays contain Null values. Nulls should be deduped like any other value.
        // Shard 0: [10, Null, 20]
        // Shard 1: [Null, 20, 30]
        // Unique non-null: {10, 20, 30}, Null appears but is deduped
        // CountDistinct should count all unique including Null = 4
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Text),
                ("cd".into(), DataType::Int64),
            ],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(10), Datum::Null, Datum::Int32(20)]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Null, Datum::Int32(20), Datum::Int32(30)]),
            ])],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::CountDistinct(1)]);
        assert_eq!(merged.len(), 1);
        // Unique values: {10, Null, 20, 30} = 4
        assert_eq!(merged[0].values[1], Datum::Int64(4));
    }

    #[test]
    fn test_merge_distinct_all_null_shards() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Edge case: both shards return Null (no data for the group).
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Text),
                ("cd".into(), DataType::Int64),
            ],
            rows: vec![OwnedRow::new(vec![Datum::Text("a".into()), Datum::Null])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![Datum::Text("a".into()), Datum::Null])],
            latency_us: 0,
        };

        // CountDistinct: all Null  鈫?0
        let merged = merge_two_phase_agg(
            &[sr1.clone(), sr2.clone()],
            &[0],
            &[AggMerge::CountDistinct(1)],
        );
        assert_eq!(merged[0].values[1], Datum::Int64(0));

        // SumDistinct: all Null  鈫?Null
        let merged = merge_two_phase_agg(
            &[sr1.clone(), sr2.clone()],
            &[0],
            &[AggMerge::SumDistinct(1)],
        );
        assert!(matches!(merged[0].values[1], Datum::Null));

        // AvgDistinct: all Null  鈫?Null
        let merged = merge_two_phase_agg(
            &[sr1.clone(), sr2.clone()],
            &[0],
            &[AggMerge::AvgDistinct(1)],
        );
        assert!(matches!(merged[0].values[1], Datum::Null));

        // StringAggDistinct: all Null  鈫?Null
        let merged = merge_two_phase_agg(
            &[sr1.clone(), sr2.clone()],
            &[0],
            &[AggMerge::StringAggDistinct(1, ",".into())],
        );
        assert!(matches!(merged[0].values[1], Datum::Null));

        // ArrayAggDistinct: all Null  鈫?Null
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::ArrayAggDistinct(1)]);
        assert!(matches!(merged[0].values[1], Datum::Null));
    }

    #[test]
    fn test_merge_distinct_empty_arrays() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Edge case: shards return empty arrays (group exists but no matching values).
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Text),
                ("cd".into(), DataType::Int64),
            ],
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![]),
            ])],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![OwnedRow::new(vec![
                Datum::Text("a".into()),
                Datum::Array(vec![Datum::Int32(42)]),
            ])],
            latency_us: 0,
        };

        // CountDistinct: empty + [42]  鈫?1
        let merged = merge_two_phase_agg(
            &[sr1.clone(), sr2.clone()],
            &[0],
            &[AggMerge::CountDistinct(1)],
        );
        assert_eq!(merged[0].values[1], Datum::Int64(1));

        // SumDistinct: empty + [42]  鈫?42
        let merged = merge_two_phase_agg(
            &[sr1.clone(), sr2.clone()],
            &[0],
            &[AggMerge::SumDistinct(1)],
        );
        // Int32(42) gets promoted to Int64 via datum_add
        assert_eq!(merged[0].values[1], Datum::Int64(42));

        // AvgDistinct: empty + [42]  鈫?42.0
        let merged = merge_two_phase_agg(
            &[sr1.clone(), sr2.clone()],
            &[0],
            &[AggMerge::AvgDistinct(1)],
        );
        assert_eq!(merged[0].values[1], Datum::Float64(42.0));
    }

    #[test]
    fn test_merge_multiple_groups_distinct() {
        use crate::distributed_exec::{merge_two_phase_agg, AggMerge, ShardResult};
        // Multiple groups: group "a" and "b" across 2 shards.
        let sr1 = ShardResult {
            shard_id: ShardId(0),
            columns: vec![
                ("grp".into(), DataType::Text),
                ("cd".into(), DataType::Int64),
            ],
            rows: vec![
                OwnedRow::new(vec![
                    Datum::Text("a".into()),
                    Datum::Array(vec![Datum::Int32(1), Datum::Int32(2)]),
                ]),
                OwnedRow::new(vec![
                    Datum::Text("b".into()),
                    Datum::Array(vec![Datum::Int32(10)]),
                ]),
            ],
            latency_us: 0,
        };
        let sr2 = ShardResult {
            shard_id: ShardId(1),
            columns: sr1.columns.clone(),
            rows: vec![
                OwnedRow::new(vec![
                    Datum::Text("a".into()),
                    Datum::Array(vec![Datum::Int32(2), Datum::Int32(3)]),
                ]),
                OwnedRow::new(vec![
                    Datum::Text("b".into()),
                    Datum::Array(vec![Datum::Int32(10), Datum::Int32(20)]),
                ]),
            ],
            latency_us: 0,
        };
        let merged = merge_two_phase_agg(&[sr1, sr2], &[0], &[AggMerge::CountDistinct(1)]);
        assert_eq!(merged.len(), 2);
        // Sort by group name for deterministic assertion
        let mut rows = merged;
        rows.sort_by(|a, b| format!("{:?}", a.values[0]).cmp(&format!("{:?}", b.values[0])));
        // Group "a": unique {1,2,3}  鈫?3
        assert_eq!(rows[0].values[0], Datum::Text("a".into()));
        assert_eq!(rows[0].values[1], Datum::Int64(3));
        // Group "b": unique {10,20}  鈫?2
        assert_eq!(rows[1].values[0], Datum::Text("b".into()));
        assert_eq!(rows[1].values[1], Datum::Int64(2));
    }
