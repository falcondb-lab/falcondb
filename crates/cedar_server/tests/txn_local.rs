#![allow(clippy::approx_constant, clippy::useless_vec)]

mod common;
use common::*;

#[test]
fn test_fast_path_local_txn_commit() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE fp_test (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    // Local txn (single-shard) should use fast-path
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    assert_eq!(txn.txn_type, cedar_txn::TxnType::Local);
    assert_eq!(txn.path, cedar_txn::TxnPath::Fast);

    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO fp_test VALUES (1, 'fast')", Some(&txn)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO fp_test VALUES (2, 'path')", Some(&txn)).unwrap();

    let commit_ts = txn_mgr.commit(txn.txn_id).unwrap();
    assert!(commit_ts.0 > 0);

    // Verify data visible after commit
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT * FROM fp_test", Some(&txn2)).unwrap();
    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        _ => panic!("Expected Query"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

#[test]
fn test_fast_path_stats_tracking() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE stats_test (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    let before = txn_mgr.stats_snapshot();

    // Commit a local fast-path txn
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO stats_test VALUES (1, 'a')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Abort a txn
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO stats_test VALUES (2, 'b')", Some(&txn2)).unwrap();
    txn_mgr.abort(txn2.txn_id).unwrap();

    let after = txn_mgr.stats_snapshot();
    assert_eq!(after.fast_path_commits, before.fast_path_commits + 1);
    assert_eq!(after.total_aborted, before.total_aborted + 1);
    assert_eq!(after.total_committed, before.total_committed + 1);
}

#[test]
fn test_si_occ_conflict_via_sql() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE occ_test (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    // txn1: insert initial data
    let txn1 = txn_mgr.begin(IsolationLevel::SnapshotIsolation);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO occ_test VALUES (1, 'v1')", Some(&txn1)).unwrap();
    txn_mgr.commit(txn1.txn_id).unwrap();

    // txn2: read under SI (captures read-set)
    let txn2 = txn_mgr.begin(IsolationLevel::SnapshotIsolation);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT * FROM occ_test WHERE id = 1", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
        }
        _ => panic!("Expected Query"),
    }

    // txn3: update same row and commit (concurrent modification)
    let txn3 = txn_mgr.begin(IsolationLevel::SnapshotIsolation);
    run_sql(&storage, &txn_mgr, &executor,
        "UPDATE occ_test SET val = 'v2' WHERE id = 1", Some(&txn3)).unwrap();
    txn_mgr.commit(txn3.txn_id).unwrap();

    // txn2: try to commit 鈥?should fail (OCC conflict)
    let result = txn_mgr.commit(txn2.txn_id);
    assert!(result.is_err(), "Expected OCC conflict for txn2");

    // Verify stats recorded the conflict
    let stats = txn_mgr.stats_snapshot();
    assert!(stats.occ_conflicts > 0);
}

#[test]
fn test_read_committed_no_occ_conflict() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE rc_test (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    // txn1: insert initial data
    let txn1 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO rc_test VALUES (1, 'v1')", Some(&txn1)).unwrap();
    txn_mgr.commit(txn1.txn_id).unwrap();

    // txn2: read under RC
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "SELECT * FROM rc_test WHERE id = 1", Some(&txn2)).unwrap();

    // txn3: update same row and commit
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "UPDATE rc_test SET val = 'v2' WHERE id = 1", Some(&txn3)).unwrap();
    txn_mgr.commit(txn3.txn_id).unwrap();

    // txn2: commit should succeed (RC doesn't do OCC)
    let result = txn_mgr.commit(txn2.txn_id);
    assert!(result.is_ok(), "RC should not trigger OCC conflict");
}

#[test]
fn test_global_txn_classification_from_plan() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE t2 (id INT PRIMARY KEY, ref_id INT)", None).unwrap();

    // A JOIN query touches two tables 鈥?planner should classify as not single_shard_proven
    let stmts = parse_sql("SELECT * FROM t1 JOIN t2 ON t1.id = t2.ref_id")
        .unwrap();
    let catalog = storage.get_catalog();
    let mut binder = Binder::new(catalog);
    let bound = binder.bind(&stmts[0]).unwrap();
    let plan = Planner::plan(&bound).unwrap();

    let hint = plan.routing_hint();
    assert!(!hint.single_shard_proven, "JOIN should not be single_shard_proven");
    assert_eq!(hint.planned_txn_type(), cedar_planner::PlannedTxnType::Global);
}

#[test]
fn test_single_table_classification_from_plan() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE single_t (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    // Single-table INSERT 鈥?should be local
    let stmts = parse_sql("INSERT INTO single_t VALUES (1, 'test')").unwrap();
    let catalog = storage.get_catalog();
    let mut binder = Binder::new(catalog);
    let bound = binder.bind(&stmts[0]).unwrap();
    let plan = Planner::plan(&bound).unwrap();

    let hint = plan.routing_hint();
    assert!(hint.single_shard_proven, "Single-table INSERT should be single_shard_proven");
    assert_eq!(hint.planned_txn_type(), cedar_planner::PlannedTxnType::Local);
}

#[test]
fn test_show_cedar_txn_stats() {
    let (storage, txn_mgr, executor) = setup();

    // Create table and do some DML to generate stats
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE stats_t (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    let txn1 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO stats_t VALUES (1, 'a')", Some(&txn1)).unwrap();
    txn_mgr.commit(txn1.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    txn_mgr.abort(txn2.txn_id).unwrap();

    // Execute SHOW cedar_txn_stats (no txn needed)
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SHOW cedar_txn_stats", None).unwrap();

    match result {
        cedar_executor::ExecutionResult::Query { columns, rows } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].0, "metric");
            assert_eq!(columns[1].0, "value");
            assert_eq!(rows.len(), 7, "should have 7 metric rows");

            // Verify specific metrics
            let find_metric = |name: &str| -> i64 {
                for row in &rows {
                    if row.values[0] == Datum::Text(name.into()) {
                        if let Datum::Int64(v) = row.values[1] {
                            return v;
                        }
                    }
                }
                panic!("metric {} not found", name);
            };

            assert_eq!(find_metric("total_committed"), 1);
            assert_eq!(find_metric("fast_path_commits"), 1);
            assert_eq!(find_metric("total_aborted"), 1);
        }
        _ => panic!("Expected Query result from SHOW cedar_txn_stats"),
    }
}

#[test]
fn test_index_scan_via_sql() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE idx_scan_t (id INT PRIMARY KEY, name TEXT, age INT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE INDEX idx_name ON idx_scan_t (name)", None).unwrap();

    // Insert several rows
    let txn1 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO idx_scan_t VALUES (1, 'alice', 30)", Some(&txn1)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO idx_scan_t VALUES (2, 'bob', 25)", Some(&txn1)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO idx_scan_t VALUES (3, 'alice', 35)", Some(&txn1)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO idx_scan_t VALUES (4, 'charlie', 40)", Some(&txn1)).unwrap();
    txn_mgr.commit(txn1.txn_id).unwrap();

    // SELECT with WHERE on indexed column 鈥?should use index scan
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, name, age FROM idx_scan_t WHERE name = 'alice'", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();

    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2, "index scan should return 2 alice rows");
            // Both rows should have name = 'alice'
            for row in &rows {
                assert_eq!(row.values[1], Datum::Text("alice".into()));
            }
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_unique_index_via_sql() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE uniq_t (id INT PRIMARY KEY, email TEXT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE UNIQUE INDEX idx_email ON uniq_t (email)", None).unwrap();

    let txn1 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO uniq_t VALUES (1, 'alice@test.com')", Some(&txn1)).unwrap();
    txn_mgr.commit(txn1.txn_id).unwrap();

    // Duplicate email should fail
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO uniq_t VALUES (2, 'alice@test.com')", Some(&txn2));
    assert!(result.is_err(), "unique index should reject duplicate email");
}

#[test]
fn test_index_scan_with_and_filter() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE idx_and_t (id INT PRIMARY KEY, name TEXT, age INT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE INDEX idx_and_name ON idx_and_t (name)", None).unwrap();

    let txn1 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO idx_and_t VALUES (1, 'alice', 30)", Some(&txn1)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO idx_and_t VALUES (2, 'alice', 25)", Some(&txn1)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO idx_and_t VALUES (3, 'bob', 30)", Some(&txn1)).unwrap();
    txn_mgr.commit(txn1.txn_id).unwrap();

    // WHERE name = 'alice' AND age = 30 鈥?index scan on name, post-filter on age
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, name, age FROM idx_and_t WHERE name = 'alice' AND age = 30", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();

    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1, "should return 1 row matching both conditions");
            assert_eq!(rows[0].values[0], Datum::Int32(1));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_show_cedar_gc() {
    let (storage, txn_mgr, executor) = setup();

    // Create table and do some DML to generate version chains
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE gc_t (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    let txn1 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO gc_t VALUES (1, 'v1')", Some(&txn1)).unwrap();
    txn_mgr.commit(txn1.txn_id).unwrap();

    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "UPDATE gc_t SET val = 'v2' WHERE id = 1", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();

    // Execute SHOW cedar_gc (no txn needed)
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SHOW cedar_gc", None).unwrap();

    match result {
        cedar_executor::ExecutionResult::Query { columns, rows } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].0, "metric");
            assert_eq!(columns[1].0, "value");
            assert_eq!(rows.len(), 2, "should have 2 metric rows");

            // Verify watermark and chains_processed metrics exist
            let find_metric = |name: &str| -> i64 {
                for row in &rows {
                    if row.values[0] == Datum::Text(name.into()) {
                        if let Datum::Int64(v) = row.values[1] {
                            return v;
                        }
                    }
                }
                panic!("metric {} not found", name);
            };

            let watermark = find_metric("watermark_ts");
            assert!(watermark > 0, "watermark should be positive");
            let chains = find_metric("chains_processed");
            assert!(chains >= 1, "should process at least 1 chain");
        }
        _ => panic!("Expected Query result from SHOW cedar_gc"),
    }
}

#[test]
fn test_update_with_index_scan() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE upd_idx_t (id INT PRIMARY KEY, name TEXT, score INT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE INDEX idx_upd_name ON upd_idx_t (name)", None).unwrap();

    let txn1 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO upd_idx_t VALUES (1, 'alice', 10)", Some(&txn1)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO upd_idx_t VALUES (2, 'bob', 20)", Some(&txn1)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO upd_idx_t VALUES (3, 'alice', 30)", Some(&txn1)).unwrap();
    txn_mgr.commit(txn1.txn_id).unwrap();

    // UPDATE with WHERE on indexed column 鈥?should use index scan
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "UPDATE upd_idx_t SET score = 99 WHERE name = 'alice'", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();

    match result {
        cedar_executor::ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 2, "should update 2 alice rows");
        }
        _ => panic!("Expected Dml result"),
    }

    // Verify the update took effect
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, score FROM upd_idx_t WHERE name = 'alice' ORDER BY id", Some(&txn3)).unwrap();
    txn_mgr.commit(txn3.txn_id).unwrap();

    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[1], Datum::Int32(99));
            assert_eq!(rows[1].values[1], Datum::Int32(99));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_delete_with_index_scan() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE del_idx_t (id INT PRIMARY KEY, name TEXT, val INT)", None).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE INDEX idx_del_name ON del_idx_t (name)", None).unwrap();

    let txn1 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO del_idx_t VALUES (1, 'alice', 10)", Some(&txn1)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO del_idx_t VALUES (2, 'bob', 20)", Some(&txn1)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO del_idx_t VALUES (3, 'alice', 30)", Some(&txn1)).unwrap();
    txn_mgr.commit(txn1.txn_id).unwrap();

    // DELETE with WHERE on indexed column 鈥?should use index scan
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "DELETE FROM del_idx_t WHERE name = 'alice'", Some(&txn2)).unwrap();
    txn_mgr.commit(txn2.txn_id).unwrap();

    match result {
        cedar_executor::ExecutionResult::Dml { rows_affected, .. } => {
            assert_eq!(rows_affected, 2, "should delete 2 alice rows");
        }
        _ => panic!("Expected Dml result"),
    }

    // Verify only bob remains
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, name FROM del_idx_t", Some(&txn3)).unwrap();
    txn_mgr.commit(txn3.txn_id).unwrap();

    match result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1, "only bob should remain");
            assert_eq!(rows[0].values[1], Datum::Text("bob".into()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_pk_point_lookup() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE pk_t (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    let txn1 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO pk_t VALUES (1, 'a')", Some(&txn1)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO pk_t VALUES (2, 'b')", Some(&txn1)).unwrap();
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO pk_t VALUES (3, 'c')", Some(&txn1)).unwrap();
    txn_mgr.commit(txn1.txn_id).unwrap();

    // SELECT with WHERE pk = literal 鈥?should use PK point lookup
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id, val FROM pk_t WHERE id = 2", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Int32(2));
            assert_eq!(rows[0].values[1], Datum::Text("b".into()));
        }
        _ => panic!("Expected Query result"),
    }

    // UPDATE with WHERE pk = literal
    run_sql(&storage, &txn_mgr, &executor,
        "UPDATE pk_t SET val = 'updated' WHERE id = 2", Some(&txn2)).unwrap();
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT val FROM pk_t WHERE id = 2", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("updated".into()));
        }
        _ => panic!("Expected Query result"),
    }

    // DELETE with WHERE pk = literal
    run_sql(&storage, &txn_mgr, &executor,
        "DELETE FROM pk_t WHERE id = 2", Some(&txn2)).unwrap();
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT COUNT(*) FROM pk_t", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Int64(2), "should have 2 rows after deleting id=2");
        }
        _ => panic!("Expected Query result"),
    }

    // Non-existent PK should return empty
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT id FROM pk_t WHERE id = 999", Some(&txn2)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 0, "non-existent PK should return 0 rows");
        }
        _ => panic!("Expected Query result"),
    }
    txn_mgr.commit(txn2.txn_id).unwrap();
}

// ===========================================================================
// SQL error path integration tests
// ===========================================================================

#[test]
fn test_duplicate_pk_insert_fails() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE dup_pk (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO dup_pk VALUES (1, 'first')", Some(&txn)).unwrap();
    let err = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO dup_pk VALUES (1, 'second')", Some(&txn));
    assert!(err.is_err(), "Duplicate PK insert should fail");
    txn_mgr.abort(txn.txn_id).unwrap();
}

#[test]
fn test_not_null_violation() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE nn (id INT PRIMARY KEY, name TEXT NOT NULL)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let err = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO nn VALUES (1, NULL)", Some(&txn));
    assert!(err.is_err(), "NULL in NOT NULL column should fail");
    txn_mgr.abort(txn.txn_id).unwrap();
}

#[test]
fn test_select_from_nonexistent_table() {
    let (storage, txn_mgr, executor) = setup();
    let err = run_sql(&storage, &txn_mgr, &executor,
        "SELECT * FROM ghost_table", None);
    assert!(err.is_err(), "SELECT from nonexistent table should fail");
}

#[test]
fn test_drop_nonexistent_table_fails() {
    let (storage, txn_mgr, executor) = setup();
    let err = run_sql(&storage, &txn_mgr, &executor,
        "DROP TABLE no_such_table", None);
    assert!(err.is_err(), "DROP nonexistent table should fail");
}

#[test]
fn test_insert_wrong_column_count() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE wc (id INT PRIMARY KEY, a INT, b INT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let err = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO wc VALUES (1, 2)", Some(&txn));
    assert!(err.is_err(), "INSERT with wrong column count should fail");
    txn_mgr.abort(txn.txn_id).unwrap();
}

#[test]
fn test_duplicate_pk_does_not_corrupt_subsequent_queries() {
    let (storage, txn_mgr, executor) = setup();
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE resilient (id INT PRIMARY KEY, val TEXT)", None).unwrap();

    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO resilient VALUES (1, 'ok')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Second txn: duplicate PK should fail but not corrupt state
    let txn2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let _ = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO resilient VALUES (1, 'dup')", Some(&txn2));
    txn_mgr.abort(txn2.txn_id).unwrap();

    // Third txn: data should still be intact
    let txn3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT * FROM resilient", Some(&txn3)).unwrap();
    match &result {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1, "only original row should exist");
            assert_eq!(rows[0].values[1], Datum::Text("ok".into()));
        }
        _ => panic!("Expected Query"),
    }
    txn_mgr.commit(txn3.txn_id).unwrap();
}

#[test]
fn test_show_node_role_via_sql() {
    let (storage, txn_mgr, executor) = setup();
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SHOW cedar.node_role", None).unwrap();

    match result {
        cedar_executor::ExecutionResult::Query { columns, rows } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Datum::Text("role".into()));
            // Value is present (either env var or default "standalone")
            match &rows[0].values[1] {
                Datum::Text(v) => assert!(!v.is_empty(), "role should not be empty"),
                _ => panic!("Expected Text value for role"),
            }
        }
        _ => panic!("Expected Query result from SHOW cedar.node_role"),
    }
}

#[test]
fn test_show_wal_stats_via_sql() {
    let (storage, txn_mgr, executor) = setup();

    // Do some work to generate WAL stats
    run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE ws_t (id INT PRIMARY KEY, val TEXT)", None).unwrap();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO ws_t VALUES (1, 'hello')", Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    let result = run_sql(&storage, &txn_mgr, &executor,
        "SHOW cedar.wal_stats", None).unwrap();

    match result {
        cedar_executor::ExecutionResult::Query { columns, rows } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(rows.len(), 4);

            let find_metric = |name: &str| -> String {
                rows.iter()
                    .find(|r| r.values[0] == Datum::Text(name.into()))
                    .map(|r| match &r.values[1] {
                        Datum::Text(v) => v.clone(),
                        other => format!("{:?}", other),
                    })
                    .unwrap_or_else(|| panic!("metric '{}' not found", name))
            };

            assert_eq!(find_metric("wal_enabled"), "false"); // in-memory engine
            let records: u64 = find_metric("records_written").parse().unwrap();
            // create_table(1) + insert(1) + commit(1) = at least 3 records
            assert!(records >= 3, "Expected at least 3 records, got {}", records);
        }
        _ => panic!("Expected Query result from SHOW cedar.wal_stats"),
    }
}

// 鈹€鈹€ Read-only executor tests 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

fn setup_read_only() -> (Arc<StorageEngine>, Arc<TxnManager>, Arc<Executor>) {
    let storage = Arc::new(StorageEngine::new_in_memory());
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    // Create a table first using a writable executor
    let rw_executor = Executor::new(storage.clone(), txn_mgr.clone());
    let stmts = cedar_sql_frontend::parser::parse_sql(
        "CREATE TABLE ro_t (id INT PRIMARY KEY, val TEXT)"
    ).unwrap();
    let catalog = storage.get_catalog();
    let mut binder = Binder::new(catalog);
    let bound = binder.bind(&stmts[0]).unwrap();
    let plan = cedar_planner::Planner::plan(&bound).unwrap();
    rw_executor.execute(&plan, None).unwrap();

    // Insert a row
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let stmts2 = cedar_sql_frontend::parser::parse_sql(
        "INSERT INTO ro_t VALUES (1, 'hello')"
    ).unwrap();
    let catalog2 = storage.get_catalog();
    let mut binder2 = Binder::new(catalog2);
    let bound2 = binder2.bind(&stmts2[0]).unwrap();
    let plan2 = cedar_planner::Planner::plan(&bound2).unwrap();
    rw_executor.execute(&plan2, Some(&txn)).unwrap();
    txn_mgr.commit(txn.txn_id).unwrap();

    // Now create read-only executor
    let executor = Arc::new(Executor::new_read_only(storage.clone(), txn_mgr.clone()));
    (storage, txn_mgr, executor)
}

#[test]
fn test_read_only_select_allowed() {
    let (storage, txn_mgr, executor) = setup_read_only();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT * FROM ro_t", Some(&txn));
    assert!(result.is_ok(), "SELECT should succeed on read-only executor");
    match result.unwrap() {
        cedar_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_read_only_insert_rejected() {
    let (storage, txn_mgr, executor) = setup_read_only();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "INSERT INTO ro_t VALUES (2, 'world')", Some(&txn));
    assert!(result.is_err(), "INSERT should fail on read-only executor");
    let err = result.unwrap_err();
    assert_eq!(err.pg_sqlstate(), "25006");
    assert!(err.to_string().contains("read-only"), "Error should mention read-only: {}", err);
}

#[test]
fn test_read_only_create_table_rejected() {
    let (storage, txn_mgr, executor) = setup_read_only();
    let result = run_sql(&storage, &txn_mgr, &executor,
        "CREATE TABLE new_t (id INT PRIMARY KEY)", None);
    assert!(result.is_err(), "CREATE TABLE should fail on read-only executor");
    let err = result.unwrap_err();
    assert_eq!(err.pg_sqlstate(), "25006");
}

#[test]
fn test_read_only_update_rejected() {
    let (storage, txn_mgr, executor) = setup_read_only();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "UPDATE ro_t SET val = 'changed' WHERE id = 1", Some(&txn));
    assert!(result.is_err(), "UPDATE should fail on read-only executor");
    assert_eq!(result.unwrap_err().pg_sqlstate(), "25006");
}

#[test]
fn test_read_only_delete_rejected() {
    let (storage, txn_mgr, executor) = setup_read_only();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let result = run_sql(&storage, &txn_mgr, &executor,
        "DELETE FROM ro_t WHERE id = 1", Some(&txn));
    assert!(result.is_err(), "DELETE should fail on read-only executor");
    assert_eq!(result.unwrap_err().pg_sqlstate(), "25006");
}

#[test]
fn test_read_only_show_commands_allowed() {
    let (storage, txn_mgr, executor) = setup_read_only();
    // SHOW commands should work even on read-only executors
    let r1 = run_sql(&storage, &txn_mgr, &executor, "SHOW cedar.node_role", None);
    assert!(r1.is_ok(), "SHOW cedar.node_role should work on read-only executor");
    let r2 = run_sql(&storage, &txn_mgr, &executor, "SHOW cedar.wal_stats", None);
    assert!(r2.is_ok(), "SHOW cedar.wal_stats should work on read-only executor");
    let r3 = run_sql(&storage, &txn_mgr, &executor, "SHOW cedar_txn_stats", None);
    assert!(r3.is_ok(), "SHOW cedar_txn_stats should work on read-only executor");
}

// 鈹€鈹€ SHOW cedar.connections tests 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

#[test]
fn test_show_connections_returns_metrics() {
    let (storage, txn_mgr, executor) = setup();
    let result = run_sql(&storage, &txn_mgr, &executor, "SHOW cedar.connections", None);
    assert!(result.is_ok(), "SHOW cedar.connections should succeed");
    match result.unwrap() {
        ExecutionResult::Query { columns, rows } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].0, "metric");
            assert_eq!(columns[1].0, "value");
            assert_eq!(rows.len(), 2);
            // First row: active_connections
            assert_eq!(rows[0].values[0], Datum::Text("active_connections".into()));
            // Second row: max_connections
            assert_eq!(rows[1].values[0], Datum::Text("max_connections".into()));
        }
        _ => panic!("Expected Query result from SHOW cedar.connections"),
    }
}

#[test]
fn test_show_connections_with_counter() {
    let storage = Arc::new(StorageEngine::new_in_memory());
    let txn_mgr = Arc::new(TxnManager::new(storage.clone()));
    let mut executor = Executor::new(storage.clone(), txn_mgr.clone());
    // Wire a connection counter with a known value
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(42));
    executor.set_connection_info(counter, 100);
    let executor = Arc::new(executor);

    let result = run_sql(&storage, &txn_mgr, &executor, "SHOW cedar.connections", None);
    match result.unwrap() {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[1], Datum::Text("42".into()));
            assert_eq!(rows[1].values[1], Datum::Text("100".into()));
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_show_connections_on_read_only_executor() {
    let (storage, txn_mgr, executor) = setup_read_only();
    let result = run_sql(&storage, &txn_mgr, &executor, "SHOW cedar.connections", None);
    assert!(result.is_ok(), "SHOW cedar.connections should work on read-only executor");
}

#[test]
fn test_explain_show_connections() {
    let (storage, txn_mgr, executor) = setup();
    let result = run_sql(&storage, &txn_mgr, &executor, "EXPLAIN SHOW cedar.connections", None);
    assert!(result.is_ok(), "EXPLAIN SHOW cedar.connections should succeed");
    match result.unwrap() {
        ExecutionResult::Query { rows, columns } => {
            assert_eq!(columns[0].0, "QUERY PLAN");
            assert!(!rows.is_empty());
            if let Datum::Text(ref line) = rows[0].values[0] {
                assert!(line.contains("ShowConnections"), "Plan should contain ShowConnections, got: {}", line);
            } else {
                panic!("Expected text plan output");
            }
        }
        _ => panic!("Expected Query for EXPLAIN"),
    }
}

#[test]
fn test_show_connections_default_values() {
    // Without set_connection_info, defaults should be 0/0
    let (storage, txn_mgr, executor) = setup();
    let result = run_sql(&storage, &txn_mgr, &executor, "SHOW cedar.connections", None).unwrap();
    match result {
        ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[1], Datum::Text("0".into()), "default active should be 0");
            assert_eq!(rows[1].values[1], Datum::Text("0".into()), "default max should be 0");
        }
        _ => panic!("Expected Query result"),
    }
}
