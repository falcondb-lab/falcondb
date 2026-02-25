    use std::sync::Arc;
    use std::time::Duration;

    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_common::schema::{ColumnDef, TableSchema};
    use falcon_common::types::{ColumnId, DataType, IsolationLevel, ShardId, TableId};

    use crate::distributed_exec::{AggMerge, DistributedExecutor, GatherStrategy, SubPlan};
    use crate::sharded_engine::ShardedEngine;

    fn test_schema() -> TableSchema {
        TableSchema {
            id: TableId(1),
            name: "users".into(),
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
                    name: "name".into(),
                    data_type: DataType::Text,
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    is_serial: false,
                },
                ColumnDef {
                    id: ColumnId(2),
                    name: "age".into(),
                    data_type: DataType::Int32,
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

    /// Setup: 4-shard engine with test table, distribute rows round-robin.
    fn setup_sharded(num_shards: u64, num_rows: i32) -> Arc<ShardedEngine> {
        let engine = Arc::new(ShardedEngine::new(num_shards));
        engine.create_table_all(&test_schema()).unwrap();

        // Insert rows, distributing round-robin across shards.
        for i in 0..num_rows {
            let shard_idx = (i as u64) % num_shards;
            let shard = engine.shard(ShardId(shard_idx)).unwrap();
            let txn = shard.txn_mgr.begin(IsolationLevel::ReadCommitted);
            let row = OwnedRow::new(vec![
                Datum::Int32(i),
                Datum::Text(format!("user_{}", i)),
                Datum::Int32(20 + (i % 30)),
            ]);
            shard.storage.insert(TableId(1), row, txn.txn_id).unwrap();
            shard.txn_mgr.commit(txn.txn_id).unwrap();
        }

        engine
    }

    #[test]
    fn test_scatter_gather_full_scan_union() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        let subplan = SubPlan::new("scan all", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let result_rows: Vec<OwnedRow> = rows.into_iter().map(|(_, r)| r).collect();
            let columns = vec![
                ("id".into(), DataType::Int32),
                ("name".into(), DataType::Text),
                ("age".into(), DataType::Int32),
            ];
            Ok((columns, result_rows))
        });

        let all_shards = engine.shard_ids();
        let ((cols, rows), metrics) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::Union {
                    distinct: false,
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        assert_eq!(cols.len(), 3);
        assert_eq!(rows.len(), 100, "should gather all 100 rows from 4 shards");
        assert_eq!(metrics.shards_participated, 4);
        assert_eq!(metrics.total_rows_gathered, 100);
    }

    #[test]
    fn test_scatter_gather_filter() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // Filter: age > 40 (rows with id where 20 + (id % 30) > 40 => id % 30 > 20)
        let subplan = SubPlan::new("scan with filter", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let filtered: Vec<OwnedRow> = rows
                .into_iter()
                .filter_map(|(_, r)| {
                    if let Some(Datum::Int32(age)) = r.values.get(2) {
                        if *age > 40 {
                            Some(r)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();

            Ok((
                vec![
                    ("id".into(), DataType::Int32),
                    ("name".into(), DataType::Text),
                    ("age".into(), DataType::Int32),
                ],
                filtered,
            ))
        });

        let all_shards = engine.shard_ids();
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::Union {
                    distinct: false,
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        // Verify all returned rows have age > 40
        for row in &rows {
            if let Some(Datum::Int32(age)) = row.values.get(2) {
                assert!(*age > 40, "filter failed: age {} <= 40", age);
            }
        }
        assert!(!rows.is_empty(), "should have some rows matching filter");
    }

    #[test]
    fn test_scatter_gather_two_phase_count() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // Partial COUNT(*) on each shard
        let subplan = SubPlan::new("partial count", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let count = rows.len() as i64;
            Ok((
                vec![("count".into(), DataType::Int64)],
                vec![OwnedRow::new(vec![Datum::Int64(count)])],
            ))
        });

        let all_shards = engine.shard_ids();
        let ((cols, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::TwoPhaseAgg {
                    group_by_indices: vec![],
                    agg_merges: vec![AggMerge::Count(0)],
                    avg_fixups: vec![],
                    visible_columns: None,
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        assert_eq!(cols.len(), 1);
        assert_eq!(rows.len(), 1, "COUNT should produce exactly 1 row");
        match rows[0].values.get(0) {
            Some(Datum::Int64(total)) => assert_eq!(*total, 100, "COUNT(*) across 4 shards"),
            other => panic!("expected Int64, got {:?}", other),
        }
    }

    #[test]
    fn test_scatter_gather_two_phase_sum() {
        let engine = setup_sharded(2, 10);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // SUM(age) - expected: sum of (20 + i%30) for i=0..9
        let expected_sum: i64 = (0..10).map(|i: i64| 20 + (i % 30)).sum();

        let subplan = SubPlan::new("partial sum(age)", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let mut sum: i64 = 0;
            for (_, r) in &rows {
                if let Some(Datum::Int32(age)) = r.values.get(2) {
                    sum += *age as i64;
                }
            }
            Ok((
                vec![("sum_age".into(), DataType::Int64)],
                vec![OwnedRow::new(vec![Datum::Int64(sum)])],
            ))
        });

        let all_shards = engine.shard_ids();
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::TwoPhaseAgg {
                    group_by_indices: vec![],
                    agg_merges: vec![AggMerge::Sum(0)],
                    avg_fixups: vec![],
                    visible_columns: None,
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        match rows[0].values.get(0) {
            Some(Datum::Int64(total)) => assert_eq!(*total, expected_sum),
            other => panic!("expected Int64, got {:?}", other),
        }
    }

    #[test]
    fn test_scatter_gather_two_phase_min_max() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // MIN(id), MAX(id) - expected: 0, 99
        let subplan = SubPlan::new("partial min/max(id)", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let mut min_id = i32::MAX;
            let mut max_id = i32::MIN;
            for (_, r) in &rows {
                if let Some(Datum::Int32(id)) = r.values.get(0) {
                    if *id < min_id {
                        min_id = *id;
                    }
                    if *id > max_id {
                        max_id = *id;
                    }
                }
            }
            Ok((
                vec![
                    ("min_id".into(), DataType::Int32),
                    ("max_id".into(), DataType::Int32),
                ],
                vec![OwnedRow::new(vec![
                    Datum::Int32(min_id),
                    Datum::Int32(max_id),
                ])],
            ))
        });

        let all_shards = engine.shard_ids();
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::TwoPhaseAgg {
                    group_by_indices: vec![],
                    agg_merges: vec![AggMerge::Min(0), AggMerge::Max(1)],
                    avg_fixups: vec![],
                    visible_columns: None,
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Datum::Int32(0));
        assert_eq!(rows[0].values[1], Datum::Int32(99));
    }

    #[test]
    fn test_scatter_gather_two_phase_avg_fixups() {
        // Test AVG decomposition via closure API: SUM + hidden COUNT  鈫?AVG fixup
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // Subplan: each shard returns [SUM(id), COUNT(id)]
        let subplan = SubPlan::new("partial_avg", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let mut sum = 0i64;
            let mut count = 0i64;
            for (_, row) in &rows {
                if let Datum::Int32(v) = &row.values[0] {
                    sum += *v as i64;
                    count += 1;
                }
            }
            Ok((
                vec![
                    ("sum_id".into(), DataType::Int64),
                    ("count_id".into(), DataType::Int64),
                ],
                vec![OwnedRow::new(vec![Datum::Int64(sum), Datum::Int64(count)])],
            ))
        });

        let all_shards = engine.shard_ids();
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::TwoPhaseAgg {
                    group_by_indices: vec![],
                    agg_merges: vec![AggMerge::Sum(0), AggMerge::Count(1)],
                    avg_fixups: vec![(0, 1)], // row[0] = row[0] / row[1]
                    visible_columns: Some(1), // only show AVG result (col 0)
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        assert_eq!(rows.len(), 1);
        // ids 0..99, sum=4950, count=100, avg=49.5
        assert_eq!(rows[0].values[0], Datum::Float64(49.5));
        assert_eq!(
            rows[0].values.len(),
            1,
            "hidden COUNT column should be truncated"
        );
    }

    #[test]
    fn test_scatter_gather_two_phase_having() {
        // Test HAVING filter via closure API
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // Subplan: each shard returns [age_group, count] where age_group = id / 10
        let subplan = SubPlan::new("partial_grouped", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let mut groups: std::collections::HashMap<i32, i64> = std::collections::HashMap::new();
            for (_, row) in &rows {
                if let Datum::Int32(id) = &row.values[0] {
                    *groups.entry(id / 10).or_default() += 1;
                }
            }
            let result_rows: Vec<OwnedRow> = groups
                .into_iter()
                .map(|(g, c)| OwnedRow::new(vec![Datum::Int32(g), Datum::Int64(c)]))
                .collect();
            Ok((
                vec![
                    ("age_group".into(), DataType::Int32),
                    ("cnt".into(), DataType::Int64),
                ],
                result_rows,
            ))
        });

        let all_shards = engine.shard_ids();
        // HAVING: only keep groups where merged count > 5
        let having_fn: Arc<dyn Fn(&OwnedRow) -> bool + Send + Sync> =
            Arc::new(|row| matches!(row.values.get(1), Some(Datum::Int64(c)) if *c > 5));
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::TwoPhaseAgg {
                    group_by_indices: vec![0],
                    agg_merges: vec![AggMerge::Count(1)],
                    avg_fixups: vec![],
                    visible_columns: None,
                    having: Some(having_fn),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        // 100 ids  鈫?groups 0..9, each with 10 ids  鈫?merged count = 10 per group.
        // HAVING count > 5  鈫?all 10 groups pass.
        assert_eq!(rows.len(), 10, "all 10 groups should pass HAVING count > 5");
        for row in &rows {
            match row.values.get(1) {
                Some(Datum::Int64(c)) => assert!(*c > 5, "count should be > 5"),
                other => panic!("Expected Int64, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_scatter_gather_two_phase_order_limit_offset() {
        // Test post-merge ORDER BY + LIMIT + OFFSET via closure API
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        // Subplan: each shard returns [age_group, count]
        let subplan = SubPlan::new("partial_grouped", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let mut groups: std::collections::HashMap<i32, i64> = std::collections::HashMap::new();
            for (_, row) in &rows {
                if let Datum::Int32(id) = &row.values[0] {
                    *groups.entry(id / 10).or_default() += 1;
                }
            }
            let result_rows: Vec<OwnedRow> = groups
                .into_iter()
                .map(|(g, c)| OwnedRow::new(vec![Datum::Int32(g), Datum::Int64(c)]))
                .collect();
            Ok((
                vec![
                    ("age_group".into(), DataType::Int32),
                    ("cnt".into(), DataType::Int64),
                ],
                result_rows,
            ))
        });

        let all_shards = engine.shard_ids();
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::TwoPhaseAgg {
                    group_by_indices: vec![0],
                    agg_merges: vec![AggMerge::Count(1)],
                    avg_fixups: vec![],
                    visible_columns: None,
                    having: None,
                    order_by: vec![(0, true)], // ORDER BY age_group ASC
                    limit: Some(3),
                    offset: Some(2),
                },
            )
            .unwrap();

        // 10 groups (0..9), sorted ASC, OFFSET 2 LIMIT 3  鈫?groups 2, 3, 4
        assert_eq!(rows.len(), 3, "LIMIT 3 after OFFSET 2");
        assert_eq!(rows[0].values[0], Datum::Int32(2));
        assert_eq!(rows[1].values[0], Datum::Int32(3));
        assert_eq!(rows[2].values[0], Datum::Int32(4));
    }

    #[test]
    fn test_scatter_gather_order_by_limit() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        let subplan = SubPlan::new("scan all for sort", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;

            let result_rows: Vec<OwnedRow> = rows.into_iter().map(|(_, r)| r).collect();
            Ok((
                vec![
                    ("id".into(), DataType::Int32),
                    ("name".into(), DataType::Text),
                    ("age".into(), DataType::Int32),
                ],
                result_rows,
            ))
        });

        let all_shards = engine.shard_ids();
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::OrderByLimit {
                    sort_columns: vec![(0, true)], // ORDER BY id ASC
                    limit: Some(10),               // LIMIT 10
                    offset: None,
                },
            )
            .unwrap();

        assert_eq!(rows.len(), 10);
        // Verify ordering
        for i in 0..10 {
            assert_eq!(rows[i].values[0], Datum::Int32(i as i32));
        }
    }

    #[test]
    fn test_scatter_gather_order_by_with_offset() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        let subplan = SubPlan::new("scan_ids", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows: Vec<OwnedRow> = storage
                .scan(TableId(1), txn.txn_id, read_ts)?
                .into_iter()
                .map(|(_, r)| r)
                .collect();
            txn_mgr.commit(txn.txn_id)?;
            Ok((vec![("id".into(), DataType::Int32)], rows))
        });

        let all_shards = engine.shard_ids();
        // ORDER BY id ASC, OFFSET 5, LIMIT 10
        let ((_, rows), _) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::OrderByLimit {
                    sort_columns: vec![(0, true)],
                    limit: Some(10),
                    offset: Some(5),
                },
            )
            .unwrap();

        assert_eq!(rows.len(), 10, "OFFSET 5 LIMIT 10 should return 10 rows");
        // First row should be id=5 (after skipping 0..4)
        assert_eq!(rows[0].values[0], Datum::Int32(5));
        assert_eq!(rows[9].values[0], Datum::Int32(14));
    }

    #[test]
    fn test_scatter_gather_timeout() {
        let engine = setup_sharded(4, 100);
        // Set timeout to 0 (immediate timeout)
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_nanos(1));

        let subplan = SubPlan::new("slow scan", |storage, txn_mgr| {
            // Simulate a slow operation
            std::thread::sleep(Duration::from_millis(10));
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;
            Ok((vec![], rows.into_iter().map(|(_, r)| r).collect()))
        });

        let all_shards = engine.shard_ids();
        let result = exec.scatter_gather(
            &subplan,
            &all_shards,
            &GatherStrategy::Union {
                distinct: false,
                limit: None,
                offset: None,
            },
        );

        assert!(result.is_err(), "should timeout");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("timeout"),
            "error should mention timeout: {}",
            err_msg
        );
    }

    #[test]
    fn test_scatter_gather_metrics() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        let subplan = SubPlan::new("scan", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;
            Ok((vec![], rows.into_iter().map(|(_, r)| r).collect()))
        });

        let all_shards = engine.shard_ids();
        let (_, metrics) = exec
            .scatter_gather(
                &subplan,
                &all_shards,
                &GatherStrategy::Union {
                    distinct: false,
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        assert_eq!(metrics.shards_participated, 4);
        assert_eq!(metrics.total_rows_gathered, 100);
        assert!(metrics.total_latency_us > 0 || metrics.max_subplan_latency_us == 0);
    }

    #[test]
    fn test_scatter_gather_partial_shards() {
        let engine = setup_sharded(4, 100);
        let exec = DistributedExecutor::new(engine.clone(), Duration::from_secs(5));

        let subplan = SubPlan::new("scan", |storage, txn_mgr| {
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let read_ts = txn.read_ts(txn_mgr.current_ts());
            let rows = storage.scan(TableId(1), txn.txn_id, read_ts)?;
            txn_mgr.commit(txn.txn_id)?;
            Ok((
                vec![("id".into(), DataType::Int32)],
                rows.into_iter().map(|(_, r)| r).collect(),
            ))
        });

        // Only query 2 of 4 shards
        let target = vec![ShardId(0), ShardId(1)];
        let ((_, rows), metrics) = exec
            .scatter_gather(
                &subplan,
                &target,
                &GatherStrategy::Union {
                    distinct: false,
                    limit: None,
                    offset: None,
                },
            )
            .unwrap();

        assert_eq!(metrics.shards_participated, 2);
        assert_eq!(rows.len(), 50, "2 of 4 shards should have 50 rows");
    }
