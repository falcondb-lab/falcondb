#![allow(clippy::approx_constant, clippy::useless_vec)]

mod common;
use common::*;

#[test]
fn test_p369_rms_range25_abs_norm25_wwyv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE25 - 1x3 [[-3,1,5]], range = 5 - (-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE25(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE25");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE25"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM25 - 3x1 [[-3],[1],[5]], col 0  → |−3|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM25(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM25");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM25"),
    }

    // STRING_WWYV_ENCODE - ("a", "b", "c")  → "a&b&c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_WWYV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a&b&c".to_string()));
        }
        _ => panic!("Expected Query for STRING_WWYV_ENCODE"),
    }

    // STRING_WWYV_DECODE - "x&y&z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_WWYV_DECODE('x&y&z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_WWYV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p370_abs_dev25_log_range25_xxyv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_DEV25 - 1x3 [[-3,1,5]], mean=1, MAD=(4+0+4)/3≈.667
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_DEV25(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.6667).abs() < 0.01, "Expected ~2.6667, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_DEV25");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_DEV25"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE25 - 3x1 [[-3],[1],[5]], col 0  → ln(5)-ln(1)≈.6094
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE25(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.6094).abs() < 0.01, "Expected ~1.6094, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE25");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE25"),
    }

    // STRING_XXYV_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_XXYV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_XXYV_ENCODE"),
    }

    // STRING_XXYV_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_XXYV_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_XXYV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p371_log_norm25_rms_dev25_yyyv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_NORM25 - 1x3 [[2,3,5]], ln(2)+ln(3)+ln(5)≈.401
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_NORM25(ARRAY[2,3,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.401).abs() < 0.01, "Expected ~3.401, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_NORM25");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_NORM25"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_DEV25 - 3x1 [[-3],[1],[5]], col 0, mean=1, stddev≈.266
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_DEV25(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.266).abs() < 0.01, "Expected ~3.266, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_DEV25");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_DEV25"),
    }

    // STRING_YYYV_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_YYYV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_YYYV_ENCODE"),
    }

    // STRING_YYYV_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_YYYV_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_YYYV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p372_abs_range25_log_norm25_zzyv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_RANGE25 - 1x3 [[-3,1,5]], |v|=[3,1,5], range=5-1=4
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_RANGE25(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.0).abs() < 0.001, "Expected ~4.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_RANGE25");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_RANGE25"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_NORM25 - 3x1 [[2],[3],[5]], col 0  → ln(2)+ln(3)+ln(5)≈.401
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_NORM25(ARRAY[2,3,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.401).abs() < 0.01, "Expected ~3.401, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_NORM25");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_NORM25"),
    }

    // STRING_ZZYV_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_ZZYV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_ZZYV_ENCODE"),
    }

    // STRING_ZZYV_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_ZZYV_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_ZZYV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p373_rms_norm26_abs_dev26_aazv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM26 - 1x2 [[3,4]], RMS = sqrt((9+16)/2) = sqrt(12.5) ≈3.536
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM26(ARRAY[3,4], 1, 2, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.536).abs() < 0.01, "Expected ~3.536, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM26");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM26"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV26 - 3x1 [[2],[4],[6]], col 0  → mean=4, MAD=(2+0+2)/3≈.333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV26(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV26");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV26"),
    }

    // STRING_AAZV_ENCODE - ("a", "b", "c")  → "a|b|c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_AAZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a|b|c".to_string()));
        }
        _ => panic!("Expected Query for STRING_AAZV_ENCODE"),
    }

    // STRING_AAZV_DECODE - "x|y|z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_AAZV_DECODE('x|y|z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_AAZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p374_abs_dev26_log_range26_bbzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_DEV26 - 1x3 [[2,4,6]], mean=4, MAD=(2+0+2)/3≈.333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_DEV26(ARRAY[2,4,6], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_DEV26");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_DEV26"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE26 - 2x1 [[1],[10]], col 0  → ln(1)=0, ln(10)≈.303, range≈.303
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE26(ARRAY[1,10], 2, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.303).abs() < 0.01, "Expected ~2.303, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE26");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE26"),
    }

    // STRING_BBZV_ENCODE - ("a", "b", "c")  → "a;b;c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_BBZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a;b;c".to_string()));
        }
        _ => panic!("Expected Query for STRING_BBZV_ENCODE"),
    }

    // STRING_BBZV_DECODE - "x;y;z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_BBZV_DECODE('x;y;z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_BBZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p375_log_range26_rms_norm26_cczv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_RANGE26 - 1x2 [[1,10]], ln(1)=0, ln(10)≈.303, range≈.303
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_RANGE26(ARRAY[1,10], 1, 2, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.303).abs() < 0.01, "Expected ~2.303, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_RANGE26");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_RANGE26"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM26 - 2x1 [[3],[4]], col 0  → sqrt((9+16)/2)=sqrt(12.5)≈.536
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM26(ARRAY[3,4], 2, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.536).abs() < 0.01, "Expected ~3.536, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM26");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM26"),
    }

    // STRING_CCZV_ENCODE - ("a", "b", "c")  → "a:b:c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_CCZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a:b:c".to_string()));
        }
        _ => panic!("Expected Query for STRING_CCZV_ENCODE"),
    }

    // STRING_CCZV_DECODE - "x:y:z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_CCZV_DECODE('x:y:z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_CCZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p376_rms_range27_abs_norm27_ddzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE27 - 1x3 [[-3,1,5]], range = 5 - (-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE27(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE27");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE27"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM27 - 3x1 [[-3],[1],[5]], col 0  → |−3|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM27(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM27");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM27"),
    }

    // STRING_DDZV_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_DDZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_DDZV_ENCODE"),
    }

    // STRING_DDZV_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_DDZV_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_DDZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p377_abs_dev27_log_range27_eezv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_DEV27 - 1x3 [[2,4,6]], mean=4, MAD=(|2-4|+|4-4|+|6-4|)/3 = 4/3 ≈1.333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_DEV27(ARRAY[2,4,6], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_DEV27");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_DEV27"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE27 - 3x1 [[1],[2],[8]], col 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079  → range=2.079
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE27(ARRAY[1,2,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE27");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE27"),
    }

    // STRING_EEZV_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_EEZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_EEZV_ENCODE"),
    }

    // STRING_EEZV_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_EEZV_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_EEZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p378_log_dev27_abs_dev27_ffzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV27 - 1x3 [[1,2,8]], ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean=0.924, stddev≈.856
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV27(ARRAY[1,2,8], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.856).abs() < 0.01, "Expected ~0.856, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV27");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV27"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV27 - 3x1 [[2],[4],[6]], col 0, mean=4, MAD=(2+0+2)/3 ≈1.333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV27(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV27");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV27"),
    }

    // STRING_FFZV_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_FFZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_FFZV_ENCODE"),
    }

    // STRING_FFZV_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_FFZV_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_FFZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p379_rms_range28_abs_norm28_ggzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE28 - 1x3 [[-3,1,5]], range = 5 - (-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE28(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE28");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE28"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM28 - 3x1 [[-3],[1],[5]], col 0  → |−3|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM28(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM28");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM28"),
    }

    // STRING_GGZV_ENCODE - ("a", "b", "c")  → "a@b@c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_GGZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a@b@c".to_string()));
        }
        _ => panic!("Expected Query for STRING_GGZV_ENCODE"),
    }

    // STRING_GGZV_DECODE - "x@y@z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_GGZV_DECODE('x@y@z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_GGZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p380_log_range28_rms_norm28_hhzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_RANGE28 - 1x3 [[1,2,8]], ln(1)=0, ln(2)≈.693, ln(8)≈.079  → range ≈2.079
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_RANGE28(ARRAY[1,2,8], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_RANGE28");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_RANGE28"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM28 - 3x1 [[3],[4],[0]], col 0  → sqrt((9+16+0)/3) = sqrt(25/3) ≈2.887
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM28(ARRAY[3,4,0], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.887).abs() < 0.01, "Expected ~2.887, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM28");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM28"),
    }

    // STRING_HHZV_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_HHZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_HHZV_ENCODE"),
    }

    // STRING_HHZV_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_HHZV_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_HHZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p381_abs_range28_log_range28_iizv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_RANGE28 - 1x3 [[-3,1,5]], |vals|=[3,1,5], range = 5 - 1 = 4
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_RANGE28(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.0).abs() < 0.001, "Expected ~4.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_RANGE28");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_RANGE28"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE28 - 3x1 [[1],[2],[8]], col 0  → ln(1)=0, ln(2)≈.693, ln(8)≈.079  → range ≈2.079
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE28(ARRAY[1,2,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE28");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE28"),
    }

    // STRING_IIZV_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_IIZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_IIZV_ENCODE"),
    }

    // STRING_IIZV_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_IIZV_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_IIZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p382_rms_dev28_abs_dev28_jjzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_DEV28 - 1x3 [[2,4,6]], mean=4, var=((−3)²+0²+2²)/3=8/3, stddev≈.633
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_DEV28(ARRAY[2,4,6], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.633).abs() < 0.01, "Expected ~1.633, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_DEV28");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_DEV28"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV28 - 3x1 [[2],[4],[6]], col 0, mean=4, MAD=(2+0+2)/3≈.333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV28(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV28");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV28"),
    }

    // STRING_JJZV_ENCODE - ("a", "b", "c")  → "a`b`c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_JJZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a`b`c".to_string()));
        }
        _ => panic!("Expected Query for STRING_JJZV_ENCODE"),
    }

    // STRING_JJZV_DECODE - "x`y`z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_JJZV_DECODE('x`y`z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_JJZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p383_log_dev28_rms_dev28_kkzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV28 - 1x3 [[1,2,4]], ln(1)=0, ln(2)≈.693, ln(4)≈.386, mean≈.693, stddev≈.566
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV28(ARRAY[1,2,4], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.566).abs() < 0.01, "Expected ~0.566, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV28");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV28"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_DEV28 - 3x1 [[2],[4],[6]], col 0, mean=4, stddev≈.633
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_DEV28(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.633).abs() < 0.01, "Expected ~1.633, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_DEV28");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_DEV28"),
    }

    // STRING_KKZV_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_KKZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_KKZV_ENCODE"),
    }

    // STRING_KKZV_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_KKZV_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_KKZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p384_abs_norm28_log_norm28_llzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM28 - 1x3 [[-3,1,5]], row 0  → |-3|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM28(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM28");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM28"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_NORM28 - 3x1 [[2],[4],[6]], col 0  → ln(2)+ln(4)+ln(6) ≈0.693+1.386+1.791 = 3.871
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_NORM28(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.871).abs() < 0.01, "Expected ~3.871, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_NORM28");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_NORM28"),
    }

    // STRING_LLZV_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_LLZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_LLZV_ENCODE"),
    }

    // STRING_LLZV_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_LLZV_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_LLZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p385_rms_range29_abs_norm29_mmzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE29 - 1x3 [[-3,1,5]], range = 5 - (-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE29(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE29");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE29"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM29 - 3x1 [[-3],[1],[5]], col 0  → |-3|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM29(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM29");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM29"),
    }

    // STRING_MMZV_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_MMZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_MMZV_ENCODE"),
    }

    // STRING_MMZV_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_MMZV_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_MMZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p386_log_range29_rms_norm29_nnzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_RANGE29 - 1x3 [[1,2,8]], row 0  → ln(8)-ln(1) = 2.0794
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_RANGE29(ARRAY[1,2,8], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.0794).abs() < 0.01, "Expected ~2.0794, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_RANGE29");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_RANGE29"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM29 - 3x1 [[3],[4],[0]], col 0  → sqrt((9+16+0)/3) ≈2.8868
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM29(ARRAY[3,4,0], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.8868).abs() < 0.01, "Expected ~2.8868, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM29");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM29"),
    }

    // STRING_NNZV_ENCODE - ("a", "b", "c")  → "a@b@c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_NNZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a@b@c".to_string()));
        }
        _ => panic!("Expected Query for STRING_NNZV_ENCODE"),
    }

    // STRING_NNZV_DECODE - "x@y@z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_NNZV_DECODE('x@y@z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_NNZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p387_abs_range29_log_range29_oozv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_RANGE29 - 1x3 [[-3,1,5]], row 0  → |vals|=[3,1,5], range=5-1=4
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_RANGE29(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.0).abs() < 0.001, "Expected ~4.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_RANGE29");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_RANGE29"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE29 - 3x1 [[1],[2],[8]], col 0  → ln(8)-ln(1) = 2.0794
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE29(ARRAY[1,2,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.0794).abs() < 0.01, "Expected ~2.0794, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE29");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE29"),
    }

    // STRING_OOZV_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_OOZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_OOZV_ENCODE"),
    }

    // STRING_OOZV_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_OOZV_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_OOZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p388_rms_range30_abs_norm30_ppzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE30 - 1x3 [[-3,1,5]], range = 5 - (-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE30(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE30");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE30"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM30 - 3x1 [[-3],[1],[5]], col 0  → |−3|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM30(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM30");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM30"),
    }

    // STRING_PPZV_ENCODE - ("a", "b", "c")  → "a`b`c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_PPZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a`b`c".to_string()));
        }
        _ => panic!("Expected Query for STRING_PPZV_ENCODE"),
    }

    // STRING_PPZV_DECODE - "x`y`z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_PPZV_DECODE('x`y`z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_PPZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p389_log_norm30_rms_dev30_qqzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_NORM30 - 1x3 [[1,2,3]], row 0  → ln(1)+ln(2)+ln(3) ≈1.7918
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_NORM30(ARRAY[1,2,3], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.7918).abs() < 0.01, "Expected ~1.7918, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_NORM30");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_NORM30"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_DEV30 - 3x1 [[-3],[1],[5]], col 0  → stddev ≈3.266
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_DEV30(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.266).abs() < 0.01, "Expected ~3.266, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_DEV30");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_DEV30"),
    }

    // STRING_QQZV_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_QQZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_QQZV_ENCODE"),
    }

    // STRING_QQZV_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_QQZV_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_QQZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p390_abs_norm30_log_norm30_rrzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM30 - 1x3 [[-3,1,5]], row 0  → |−3|+|1|+|5| = 9.0
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM30(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM30");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM30"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_NORM30 - 3x1 [[2],[4],[8]], col 0  → ln(2)+ln(4)+ln(8) ≈4.1589
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_NORM30(ARRAY[2,4,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.1589).abs() < 0.01, "Expected ~4.1589, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_NORM30");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_NORM30"),
    }

    // STRING_RRZV_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_RRZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_RRZV_ENCODE"),
    }

    // STRING_RRZV_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_RRZV_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_RRZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p391_rms_range31_abs_norm31_sszv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE31 - 1x3 [[-3,1,5]], range = 5 - (-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE31(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE31");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE31"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM31 - 3x1 [[-3],[1],[5]], col 0  → |−3|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM31(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM31");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM31"),
    }

    // STRING_SSZV_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_SSZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_SSZV_ENCODE"),
    }

    // STRING_SSZV_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_SSZV_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_SSZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p392_log_norm31_rms_dev31_ttzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_NORM31 - 1x3 [[1,2,4]], sum of ln(|v|) = ln(1)+ln(2)+ln(4) = 0+0.693+1.386 ≈2.079
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_NORM31(ARRAY[1,2,4], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_NORM31");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_NORM31"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_DEV31 - 3x1 [[2],[4],[6]], col 0  → mean=4, var=((2-4)²+(4-4)²+(6-4)²)/3=8/3, stddev≈.633
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_DEV31(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.633).abs() < 0.01, "Expected ~1.633, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_DEV31");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_DEV31"),
    }

    // STRING_TTZV_ENCODE - ("a", "b", "c")  → "a`b`c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_TTZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a`b`c".to_string()));
        }
        _ => panic!("Expected Query for STRING_TTZV_ENCODE"),
    }

    // STRING_TTZV_DECODE - "x`y`z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_TTZV_DECODE('x`y`z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_TTZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p393_abs_dev31_log_range31_uuzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_DEV31 - 1x3 [[2,4,6]], mean=4, MAD=(|2-4|+|4-4|+|6-4|)/3=4/3≈.333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_DEV31(ARRAY[2,4,6], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_DEV31");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_DEV31"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE31 - 3x1 [[1],[2],[8]], col 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, range=2.079-0=2.079
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE31(ARRAY[1,2,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE31");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE31"),
    }

    // STRING_UUZV_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_UUZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_UUZV_ENCODE"),
    }

    // STRING_UUZV_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_UUZV_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_UUZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p394_rms_norm31_abs_dev31_vvzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM31 - 1x3 [[3,4,0]], RMS = sqrt((9+16+0)/3) = sqrt(25/3) ≈2.886
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM31(ARRAY[3,4,0], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.886).abs() < 0.01, "Expected ~2.886, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM31");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM31"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV31 - 3x1 [[2],[4],[6]], col 0, mean=4, MAD=(2+0+2)/3≈.333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV31(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV31");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV31"),
    }

    // STRING_VVZV_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_VVZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_VVZV_ENCODE"),
    }

    // STRING_VVZV_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_VVZV_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_VVZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p395_log_range31_rms_norm31_wwzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_RANGE31 - 1x3 [[1,2,8]], ln(1)=0, ln(2)≈.693, ln(8)≈.079, range≈.079
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_RANGE31(ARRAY[1,2,8], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_RANGE31");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_RANGE31"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM31 - 3x1 [[3],[4],[0]], col 0, RMS = sqrt((9+16+0)/3) = sqrt(25/3) ≈2.886
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM31(ARRAY[3,4,0], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.886).abs() < 0.01, "Expected ~2.886, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM31");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM31"),
    }

    // STRING_WWZV_ENCODE - ("a", "b", "c")  → "a=b=c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_WWZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_WWZV_ENCODE"),
    }

    // STRING_WWZV_DECODE - "x=y=z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_WWZV_DECODE('x=y=z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_WWZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p396_abs_norm31_log_dev31_xxzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM31 - 1x3 [[-3,1,5]], row 0  → |−3|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM31(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM31");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM31"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV31 - 3x1 [[1],[10],[100]], col 0
    // ln(1)=0, ln(10)≈.302, ln(100)≈.605, mean≈.302, stddev≈.881
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV31(ARRAY[1,10,100], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.881).abs() < 0.01, "Expected ~1.881, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV31");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV31"),
    }

    // STRING_XXZV_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_XXZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_XXZV_ENCODE"),
    }

    // STRING_XXZV_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_XXZV_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_XXZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p397_rms_range32_abs_norm32_yyzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE32 - 1x3 [[-3,1,5]], range = 5 - (-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE32(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE32"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM32 - 3x1 [[-3],[1],[5]], col 0  → |−3|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM32(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM32"),
    }

    // STRING_YYZV_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_YYZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_YYZV_ENCODE"),
    }

    // STRING_YYZV_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_YYZV_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_YYZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p398_log_range32_rms_norm32_zzzv() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_RANGE32 - 1x3 [[1,2,8]], ln(1)=0, ln(2)=0.693, ln(8)=2.079  → range = 2.079
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_RANGE32(ARRAY[1,2,8], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_RANGE32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_RANGE32"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM32 - 3x1 [[1],[2],[3]], col 0  → sqrt((1+4+9)/3) = sqrt(14/3) ≈2.160
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM32(ARRAY[1,2,3], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.160).abs() < 0.01, "Expected ~2.160, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM32"),
    }

    // STRING_ZZZV_ENCODE - ("a", "b", "c")  → "a`b`c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_ZZZV_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a`b`c".to_string()));
        }
        _ => panic!("Expected Query for STRING_ZZZV_ENCODE"),
    }

    // STRING_ZZZV_DECODE - "x`y`z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_ZZZV_DECODE('x`y`z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_ZZZV_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p399_abs_dev32_log_range32_aaaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_DEV32 - 1x3 [[2,4,6]], mean=4, MAD=(|2-4|+|4-4|+|6-4|)/3 = (2+0+2)/3 ≈1.333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_DEV32(ARRAY[2,4,6], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_DEV32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_DEV32"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE32 - 3x1 [[1],[2],[8]], col 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079  → range=2.079
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE32(ARRAY[1,2,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE32"),
    }

    // STRING_AAAW_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_AAAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_AAAW_ENCODE"),
    }

    // STRING_AAAW_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_AAAW_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_AAAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p400_rms_norm32_abs_dev32_bbaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM32 - 1x3 [[3,4,0]], RMS = sqrt((9+16+0)/3) = sqrt(25/3) ≈2.886
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM32(ARRAY[3,4,0], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.886).abs() < 0.01, "Expected ~2.886, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM32"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV32 - 3x1 [[2],[4],[6]], col 0  → mean=4, MAD=(2+0+2)/3 ≈1.333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV32(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV32"),
    }

    // STRING_BBAW_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_BBAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_BBAW_ENCODE"),
    }

    // STRING_BBAW_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_BBAW_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_BBAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p401_log_norm32_rms_range32_ccaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_NORM32 - 1x3 [[1,2,4]], sum of ln(|v|) = ln(1)+ln(2)+ln(4) = 0+0.693+1.386 ≈2.079
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_NORM32(ARRAY[1,2,4], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_NORM32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_NORM32"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_RANGE32 - 3x1 [[-3],[1],[5]], col 0  → range = 5 - (-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_RANGE32(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_RANGE32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_RANGE32"),
    }

    // STRING_CCAW_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_CCAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_CCAW_ENCODE"),
    }

    // STRING_CCAW_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_CCAW_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_CCAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p402_abs_range32_log_norm32_ddaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_RANGE32 - 1x3 [[-3,1,5]], |v| = [3,1,5], range = 5 - 1 = 4
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_RANGE32(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.0).abs() < 0.001, "Expected ~4.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_RANGE32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_RANGE32"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_NORM32 - 3x1 [[1],[2],[4]], col 0  → ln(1)+ln(2)+ln(4) = 0+0.693+1.386 ≈2.079
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_NORM32(ARRAY[1,2,4], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_NORM32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_NORM32"),
    }

    // STRING_DDAW_ENCODE - ("a", "b", "c")  → "a=b=c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_DDAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_DDAW_ENCODE"),
    }

    // STRING_DDAW_DECODE - "x=y=z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_DDAW_DECODE('x=y=z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_DDAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p403_rms_dev32_abs_range32_eeaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_DEV32 - 1x3 [[2,4,6]], mean=4, var=(4+0+4)/3=2.667, stddev≈.633
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_DEV32(ARRAY[2,4,6], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.633).abs() < 0.01, "Expected ~1.633, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_DEV32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_DEV32"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_RANGE32 - 3x1 [[-3],[1],[5]], |v|=[3,1,5], range=5-1=4
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_RANGE32(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.0).abs() < 0.001, "Expected ~4.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_RANGE32");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_RANGE32"),
    }

    // STRING_EEAW_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_EEAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_EEAW_ENCODE"),
    }

    // STRING_EEAW_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_EEAW_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_EEAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p404_log_range33_rms_dev33_ffaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_RANGE33 - 1x3 [[1,2,8]], ln(1)=0, ln(2)≈.693, ln(8)≈.079, range≈.079
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_RANGE33(ARRAY[1,2,8], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_RANGE33");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_RANGE33"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_DEV33 - 3x1 [[2],[4],[6]], mean=4, var=(4+0+4)/3≈.333, stddev≈.633
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_DEV33(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.633).abs() < 0.01, "Expected ~1.633, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_DEV33");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_DEV33"),
    }

    // STRING_FFAW_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_FFAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_FFAW_ENCODE"),
    }

    // STRING_FFAW_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_FFAW_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_FFAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p405_abs_range34_log_norm34_ggaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_RANGE34 - 1x3 [[-3,1,5]], |v|=[3,1,5], range = 5 - 1 = 4
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_RANGE34(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.0).abs() < 0.001, "Expected ~4.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_RANGE34");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_RANGE34"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_NORM34 - 3x1 [[2],[4],[6]], col 0  → ln(2)+ln(4)+ln(6) ≈0.693+1.386+1.791 = 3.871
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_NORM34(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.871).abs() < 0.01, "Expected ~3.871, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_NORM34");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_NORM34"),
    }

    // STRING_GGAW_ENCODE - ("a", "b", "c")  → "a=b=c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_GGAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_GGAW_ENCODE"),
    }

    // STRING_GGAW_DECODE - "x=y=z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_GGAW_DECODE('x=y=z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_GGAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p406_log_norm35_abs_dev35_hhaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_NORM35 - 1x3 [[2,4,6]], row 0  → ln(2)+ln(4)+ln(6) ≈0.693+1.386+1.791 = 3.871
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_NORM35(ARRAY[2,4,6], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.871).abs() < 0.01, "Expected ~3.871, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_NORM35");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_NORM35"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV35 - 3x1 [[2],[4],[6]], col 0  → mean=4, MAD=(|2-4|+|4-4|+|6-4|)/3 = 4/3 ≈1.333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV35(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV35");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV35"),
    }

    // STRING_HHAW_ENCODE - ("a", "b", "c")  → "a|b|c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_HHAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a|b|c".to_string()));
        }
        _ => panic!("Expected Query for STRING_HHAW_ENCODE"),
    }

    // STRING_HHAW_DECODE - "x|y|z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_HHAW_DECODE('x|y|z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_HHAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p407_rms_range35_log_dev35_iiaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE35 - 1x3 [[-3,1,5]], range = 5 - (-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE35(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE35");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE35"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV35 - 3x1 [[1],[2],[4]], col 0  → ln(1)=0, ln(2)=0.693, ln(4)=1.386, mean=0.693, var=((0-0.693)²+(0.693-0.693)²+(1.386-0.693)²)/3=0.3204, stddev≈.566
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV35(ARRAY[1,2,4], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.566).abs() < 0.01, "Expected ~0.566, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV35");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV35"),
    }

    // STRING_IIAW_ENCODE - ("a", "b", "c")  → "a:b:c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_IIAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a:b:c".to_string()));
        }
        _ => panic!("Expected Query for STRING_IIAW_ENCODE"),
    }

    // STRING_IIAW_DECODE - "x:y:z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_IIAW_DECODE('x:y:z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_IIAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p408_abs_norm35_rms_range35_jjaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM35 - 1x3 [[-3,1,5]], row 0  → |−3|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM35(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM35");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM35"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_RANGE35 - 3x1 [[-3],[1],[5]], col 0  → range = 5 - (-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_RANGE35(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_RANGE35");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_RANGE35"),
    }

    // STRING_JJAW_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_JJAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_JJAW_ENCODE"),
    }

    // STRING_JJAW_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_JJAW_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_JJAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p409_log_norm36_abs_dev36_kkaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_NORM36 - 1x3 [[1,2,4]], row 0  → ln(1)+ln(2)+ln(4) = 0+0.6931+1.3863 = 2.0794
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_NORM36(ARRAY[1,2,4], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.0794).abs() < 0.01, "Expected ~2.0794, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_NORM36");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_NORM36"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV36 - 3x1 [[2],[4],[6]], col 0  → mean=4, MAD=(|2-4|+|4-4|+|6-4|)/3 = 4/3 ≈1.3333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV36(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.3333).abs() < 0.01, "Expected ~1.3333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV36");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV36"),
    }

    // STRING_KKAW_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_KKAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_KKAW_ENCODE"),
    }

    // STRING_KKAW_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_KKAW_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_KKAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p410_rms_norm36_log_norm36_llaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM36 - 1x3 [[3,4,0]], row 0  → sqrt((9+16+0)/3) = sqrt(25/3) ≈2.8868
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM36(ARRAY[3,4,0], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.8868).abs() < 0.01, "Expected ~2.8868, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM36");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM36"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_NORM36 - 3x1 [[1],[2],[4]], col 0  → ln(1)+ln(2)+ln(4) = 0+0.6931+1.3863 = 2.0794
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_NORM36(ARRAY[1,2,4], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.0794).abs() < 0.01, "Expected ~2.0794, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_NORM36");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_NORM36"),
    }

    // STRING_LLAW_ENCODE - ("a", "b", "c")  → "a@b@c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_LLAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a@b@c".to_string()));
        }
        _ => panic!("Expected Query for STRING_LLAW_ENCODE"),
    }

    // STRING_LLAW_DECODE - "x@y@z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_LLAW_DECODE('x@y@z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_LLAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}
