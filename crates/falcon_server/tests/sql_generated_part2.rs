#![allow(clippy::approx_constant, clippy::useless_vec)]

mod common;
use common::*;

#[test]
fn test_p411_abs_dev36_rms_norm36_mmaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_DEV36 - 1x3 [[2,4,6]], row 0  → mean=4, MAD=(|2-4|+|4-4|+|6-4|)/3 = (2+0+2)/3 ≈1.3333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_DEV36(ARRAY[2,4,6], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.3333).abs() < 0.01, "Expected ~1.3333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_DEV36");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_DEV36"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM36 - 3x1 [[3],[4],[0]], col 0  → sqrt((9+16+0)/3) = sqrt(25/3) ≈2.8868
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM36(ARRAY[3,4,0], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.8868).abs() < 0.01, "Expected ~2.8868, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM36");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM36"),
    }

    // STRING_MMAW_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_MMAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_MMAW_ENCODE"),
    }

    // STRING_MMAW_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_MMAW_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_MMAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p412_log_dev36_abs_range36_nnaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV36 - 1x3 [[1,2,8]], row 0  → ln(1)=0, ln(2)≈.6931, ln(8)≈.0794
    // mean≈.9242, var≈(0-0.9242)²+(0.6931-0.9242)²+(2.0794-0.9242)²)/3 ≈(0.8541+0.0534+1.3352)/3 ≈0.7476
    // stddev≈.8646
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV36(ARRAY[1,2,8], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV36");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV36"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_RANGE36 - 3x1 [[-3],[1],[5]], col 0  → |vals|=[3,1,5], range=5-1=4
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_RANGE36(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.0).abs() < 0.001, "Expected ~4.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_RANGE36");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_RANGE36"),
    }

    // STRING_NNAW_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_NNAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_NNAW_ENCODE"),
    }

    // STRING_NNAW_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_NNAW_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_NNAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p413_rms_range36_log_dev36_ooaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE36 - 1x3 [[-3,1,5]], row 0  → range = 5 - (-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE36(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE36");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE36"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV36 - 3x1 [[1],[2],[8]], col 0  → ln(1)=0, ln(2)≈.6931, ln(8)≈.0794
    // mean≈.9242, stddev≈.8646
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV36(ARRAY[1,2,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV36");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV36"),
    }

    // STRING_OOAW_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_OOAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_OOAW_ENCODE"),
    }

    // STRING_OOAW_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_OOAW_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_OOAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p414_abs_norm37_rms_dev37_ppaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM37 - 1x3 [[-3,1,5]], row 0  → |−2|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM37(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM37");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM37"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_DEV37 - 3x1 [[2],[4],[6]], col 0  → mean=4, var=((4+0+4)/3)≈.6667, stddev≈.6330
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_DEV37(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.6330).abs() < 0.01, "Expected ~1.6330, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_DEV37");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_DEV37"),
    }

    // STRING_PPAW_ENCODE - ("a", "b", "c")  → "a=b=c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_PPAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_PPAW_ENCODE"),
    }

    // STRING_PPAW_DECODE - "x=y=z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_PPAW_DECODE('x=y=z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_PPAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p415_rms_dev37_abs_norm37_qqaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_DEV37 - 1x3 [[2,4,6]], row 0  → mean=4, var=((4+0+4)/3)≈.6667, stddev≈.6330
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_DEV37(ARRAY[2,4,6], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.6330).abs() < 0.01, "Expected ~1.6330, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_DEV37");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_DEV37"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM37 - 3x1 [[-3],[1],[5]], col 0  → |−2|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM37(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM37");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM37"),
    }

    // STRING_QQAW_ENCODE - ("a", "b", "c")  → "a:b:c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_QQAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a:b:c".to_string()));
        }
        _ => panic!("Expected Query for STRING_QQAW_ENCODE"),
    }

    // STRING_QQAW_DECODE - "x:y:z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_QQAW_DECODE('x:y:z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_QQAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p416_log_norm37_rms_range37_rraw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_NORM37 - 1x3 [[1,2,3]], row 0  → ln(1)+ln(2)+ln(3) = 0+0.6931+1.0986 ≈1.7918
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_NORM37(ARRAY[1,2,3], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.7918).abs() < 0.01, "Expected ~1.7918, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_NORM37");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_NORM37"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_RANGE37 - 3x1 [[-3],[1],[5]], col 0  → range = 5 - (-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_RANGE37(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_RANGE37");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_RANGE37"),
    }

    // STRING_RRAW_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_RRAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_RRAW_ENCODE"),
    }

    // STRING_RRAW_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_RRAW_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_RRAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p417_abs_dev37_log_norm37_ssaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_DEV37 - 1x3 [[2,4,6]], row 0  → mean=4, MAD=(|2-4|+|4-4|+|6-4|)/3 = (2+0+2)/3 ≈1.3333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_DEV37(ARRAY[2,4,6], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.3333).abs() < 0.01, "Expected ~1.3333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_DEV37");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_DEV37"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_NORM37 - 3x1 [[1],[2],[3]], col 0  → ln(1)+ln(2)+ln(3) = 0+0.6931+1.0986 ≈1.7918
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_NORM37(ARRAY[1,2,3], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.7918).abs() < 0.01, "Expected ~1.7918, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_NORM37");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_NORM37"),
    }

    // STRING_SSAW_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_SSAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_SSAW_ENCODE"),
    }

    // STRING_SSAW_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_SSAW_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_SSAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p418_log_dev37_abs_dev37_ttaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV37 - 1x3 [[1,2,4]], row 0  → ln(1)=0, ln(2)=0.6931, ln(4)=1.3863
    // mean=0.6931, var=((0-0.6931)²+(0.6931-0.6931)²+(1.3863-0.6931)²)/3 = (0.4804+0+0.4804)/3 = 0.3203, stddev ≈0.566
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV37(ARRAY[1,2,4], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.566).abs() < 0.01, "Expected ~0.566, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV37");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV37"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV37 - 3x1 [[2],[4],[6]], col 0  → mean=4, MAD=(|2-4|+|4-4|+|6-4|)/3 = 4/3 ≈1.3333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV37(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.3333).abs() < 0.01, "Expected ~1.3333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV37");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV37"),
    }

    // STRING_TTAW_ENCODE - ("a", "b", "c")  → "a=b=c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_TTAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_TTAW_ENCODE"),
    }

    // STRING_TTAW_DECODE - "x=y=z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_TTAW_DECODE('x=y=z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_TTAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p419_rms_norm37_log_dev37_uuaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM37 - 1x3 [[3,4,0]], row 0  → sqrt((9+16+0)/3) = sqrt(25/3) ≈2.8868
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM37(ARRAY[3,4,0], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.8868).abs() < 0.01, "Expected ~2.8868, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM37");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM37"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV37 - 3x1 [[1],[2],[4]], col 0  → ln(1)=0, ln(2)=0.6931, ln(4)=1.3863
    // mean=0.6931, var=((0-0.6931)²+(0.6931-0.6931)²+(1.3863-0.6931)²)/3 = 0.3203, stddev ≈0.566
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV37(ARRAY[1,2,4], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.566).abs() < 0.01, "Expected ~0.566, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV37");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV37"),
    }

    // STRING_UUAW_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_UUAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_UUAW_ENCODE"),
    }

    // STRING_UUAW_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_UUAW_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_UUAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p420_rms_range38_abs_norm38_vvaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE38 - 1x3 [[-3,1,5]], range = 5 - (-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE38(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE38");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE38"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM38 - 3x1 [[-3],[1],[5]], col 0  → |−2|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM38(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM38");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM38"),
    }

    // STRING_VVAW_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_VVAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_VVAW_ENCODE"),
    }

    // STRING_VVAW_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_VVAW_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_VVAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p421_abs_norm39_rms_dev39_wwaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM39 - 1x3 [[-3,1,5]], row 0  → |−2|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM39(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM39");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM39"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_DEV39 - 3x1 [[-3],[1],[5]], col 0  → mean=1, stddev=sqrt(32/3)≈.266
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_DEV39(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.266).abs() < 0.01, "Expected ~3.266, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_DEV39");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_DEV39"),
    }

    // STRING_WWAW_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_WWAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_WWAW_ENCODE"),
    }

    // STRING_WWAW_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_WWAW_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_WWAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p422_rms_dev39_abs_norm39_xxaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_DEV39 - 1x3 [[-3,1,5]], row 0  → mean=1, stddev=sqrt(32/3)≈.266
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_DEV39(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.266).abs() < 0.01, "Expected ~3.266, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_DEV39");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_DEV39"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM39 - 3x1 [[-3],[1],[5]], col 0  → |−2|+|1|+|5| = 9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM39(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.001, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM39");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM39"),
    }

    // STRING_XXAW_ENCODE - ("a", "b", "c")  → "a@b@c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_XXAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a@b@c".to_string()));
        }
        _ => panic!("Expected Query for STRING_XXAW_ENCODE"),
    }

    // STRING_XXAW_DECODE - "x@y@z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_XXAW_DECODE('x@y@z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_XXAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p423_log_norm39_rms_range39_yyaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_NORM39 - 1x3 [[2,3,5]], row 0  → ln(2)+ln(3)+ln(5)≈.401
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_NORM39(ARRAY[2,3,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.401).abs() < 0.01, "Expected ~3.401, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_NORM39");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_NORM39"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_RANGE39 - 3x1 [[-3],[1],[5]], col 0  → max(5)-min(-3) = 8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_RANGE39(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_RANGE39");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_RANGE39"),
    }

    // STRING_YYAW_ENCODE - ("a", "b", "c")  → "a=b=c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_YYAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_YYAW_ENCODE"),
    }

    // STRING_YYAW_DECODE - "x=y=z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_YYAW_DECODE('x=y=z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_YYAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p424_abs_dev39_log_norm39_zzaw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_DEV39 - 1x3 [[-3,1,5]], row 0  → mean=1, MAD=(|−2|+|0|+|4|)/3≈.667
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_DEV39(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.667).abs() < 0.01, "Expected ~2.667, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_DEV39");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_DEV39"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_NORM39 - 3x1 [[2],[3],[5]], col 0  → ln(2)+ln(3)+ln(5)≈.401
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_NORM39(ARRAY[2,3,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.401).abs() < 0.01, "Expected ~3.401, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_NORM39");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_NORM39"),
    }

    // STRING_ZZAW_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_ZZAW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_ZZAW_ENCODE"),
    }

    // STRING_ZZAW_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_ZZAW_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_ZZAW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p425_rms_range39_abs_dev39_aabw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE39 - 1x3 [[-3,1,5]], row 0  → max=5, min=-3, range=8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE39(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.001, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE39");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE39"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV39 - 3x1 [[-3],[1],[5]], col 0  → mean=1, MAD=(4+0+4)/3≈.667
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV39(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.667).abs() < 0.01, "Expected ~2.667, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV39");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV39"),
    }

    // STRING_AABW_ENCODE - ("a", "b", "c")  → "a|b|c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_AABW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a|b|c".to_string()));
        }
        _ => panic!("Expected Query for STRING_AABW_ENCODE"),
    }

    // STRING_AABW_DECODE - "x|y|z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_AABW_DECODE('x|y|z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_AABW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p426_log_dev39_rms_norm39_bbbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV39 - 1x3 [[1,2,8]], row 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean≈.924, stddev≈.854
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV39(ARRAY[1,2,8], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV39");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV39"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM39 - 3x1 [[-3],[1],[5]], col 0  → RMS=sqrt((9+1+25)/3)=sqrt(35/3)≈.416
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM39(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM39");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM39"),
    }

    // STRING_BBBW_ENCODE - ("a", "b", "c")  → "a:b:c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_BBBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a:b:c".to_string()));
        }
        _ => panic!("Expected Query for STRING_BBBW_ENCODE"),
    }

    // STRING_BBBW_DECODE - "x:y:z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_BBBW_DECODE('x:y:z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_BBBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p427_rms_norm40_log_dev40_ccbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM40 - 1x3 [[-3,1,5]], row 0  → RMS=sqrt((9+1+25)/3)=sqrt(35/3)≈.416
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM40(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM40");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM40"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV40 - 3x1 [[1],[2],[8]], col 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean≈.924, stddev≈.8645
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV40(ARRAY[1,2,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV40");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV40"),
    }

    // STRING_CCBW_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_CCBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_CCBW_ENCODE"),
    }

    // STRING_CCBW_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_CCBW_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_CCBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p428_abs_norm41_rms_dev41_ddbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM41 - 1x3 [[-3,1,5]], row 0  → |−2|+|1|+|5|=9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM41(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.01, "Expected 9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM41");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM41"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_DEV41 - 3x1 [[2],[6],[4]], col 0  → mean=4, var=((−2)²+2²+0²)/3=8/3, stddev≈.633
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_DEV41(ARRAY[2,6,4], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.633).abs() < 0.01, "Expected ~1.633, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_DEV41");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_DEV41"),
    }

    // STRING_DDBW_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_DDBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_DDBW_ENCODE"),
    }

    // STRING_DDBW_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_DDBW_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_DDBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p429_rms_dev41_abs_norm41_eebw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_DEV41 - 1x3 [[2,6,4]], row 0  → mean=4, var=((−2)²+2²+0²)/3=8/3, stddev≈.633
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_DEV41(ARRAY[2,6,4], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.633).abs() < 0.01, "Expected ~1.633, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_DEV41");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_DEV41"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM41 - 3x1 [[-3],[1],[5]], col 0  → |−2|+|1|+|5|=9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM41(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.01, "Expected 9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM41");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM41"),
    }

    // STRING_EEBW_ENCODE - ("a", "b", "c")  → "a@b@c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_EEBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a@b@c".to_string()));
        }
        _ => panic!("Expected Query for STRING_EEBW_ENCODE"),
    }

    // STRING_EEBW_DECODE - "x@y@z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_EEBW_DECODE('x@y@z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_EEBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p430_log_dev41_rms_dev42_ffbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV41 - 1x3 [[1,2,8]], row 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean≈.924, stddev≈.8645
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV41(ARRAY[1,2,8], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV41");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV41"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_DEV42 - 3x1 [[2],[6],[4]], col 0  → mean=4, var=((−2)²+2²+0²)/3=8/3, stddev≈.633
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_DEV42(ARRAY[2,6,4], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.633).abs() < 0.01, "Expected ~1.633, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_DEV42");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_DEV42"),
    }

    // STRING_FFBW_ENCODE - ("a", "b", "c")  → "a=b=c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_FFBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_FFBW_ENCODE"),
    }

    // STRING_FFBW_DECODE - "x=y=z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_FFBW_DECODE('x=y=z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_FFBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p431_abs_norm42_log_dev42_ggbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM42 - 1x3 [[-3,1,5]], row 0  → |−2|+|1|+|5|=9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM42(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.01, "Expected 9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM42");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM42"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV42 - 3x1 [[1],[2],[8]], col 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean≈.924, stddev≈.8645
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV42(ARRAY[1,2,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV42");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV42"),
    }

    // STRING_GGBW_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_GGBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_GGBW_ENCODE"),
    }

    // STRING_GGBW_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_GGBW_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_GGBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p432_rms_range42_abs_dev42_hhbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE42 - 1x3 [[-3,1,5]], row 0  → max=5, min=-3, range=8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE42(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.01, "Expected 8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE42");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE42"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV42 - 3x1 [[2],[4],[6]], col 0  → mean=4, MAD=(2+0+2)/3≈.333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV42(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV42");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV42"),
    }

    // STRING_HHBW_ENCODE - ("a", "b", "c")  → "a|b|c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_HHBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a|b|c".to_string()));
        }
        _ => panic!("Expected Query for STRING_HHBW_ENCODE"),
    }

    // STRING_HHBW_DECODE - "x|y|z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_HHBW_DECODE('x|y|z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_HHBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p433_log_dev42_rms_norm42_iibw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV42 - 1x3 [[1,2,8]], row 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean≈.924, stddev≈.8645
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV42(ARRAY[1,2,8], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV42");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV42"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM42 - 3x1 [[-3],[1],[5]], col 0  → RMS=sqrt((9+1+25)/3)=sqrt(35/3)≈.416
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM42(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM42");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM42"),
    }

    // STRING_IIBW_ENCODE - ("a", "b", "c")  → "a:b:c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_IIBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a:b:c".to_string()));
        }
        _ => panic!("Expected Query for STRING_IIBW_ENCODE"),
    }

    // STRING_IIBW_DECODE - "x:y:z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_IIBW_DECODE('x:y:z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_IIBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p434_rms_norm43_log_dev43_jjbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM43 - 1x3 [[-3,1,5]], row 0  → RMS=sqrt((9+1+25)/3)=sqrt(35/3)≈.416
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM43(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM43");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM43"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV43 - 3x1 [[1],[2],[8]], col 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean≈.924, stddev≈.8645
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV43(ARRAY[1,2,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV43");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV43"),
    }

    // STRING_JJBW_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_JJBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_JJBW_ENCODE"),
    }

    // STRING_JJBW_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_JJBW_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_JJBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p435_rms_range43_abs_dev43_kkbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE43 - 1x3 [[-3,1,5]], row 0  → range=5-(-3)=8.0
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE43(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.01, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE43");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE43"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV43 - 3x1 [[2],[4],[10]], col 0  → mean≈.333, MAD≈.111
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV43(ARRAY[2,4,10], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.111).abs() < 0.01, "Expected ~3.111, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV43");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV43"),
    }

    // STRING_KKBW_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_KKBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_KKBW_ENCODE"),
    }

    // STRING_KKBW_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_KKBW_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_KKBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p436_abs_norm43_rms_dev43_llbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM43 - 1x3 [[-3,1,5]], row 0  → |−2|+|1|+|5|=9.0
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM43(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.01, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM43");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM43"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_DEV43 - 3x1 [[2],[4],[10]], col 0  → mean≈.333, stddev≈.399
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_DEV43(ARRAY[2,4,10], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.399).abs() < 0.01, "Expected ~3.399, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_DEV43");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_DEV43"),
    }

    // STRING_LLBW_ENCODE - ("a", "b", "c")  → "a@b@c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_LLBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a@b@c".to_string()));
        }
        _ => panic!("Expected Query for STRING_LLBW_ENCODE"),
    }

    // STRING_LLBW_DECODE - "x@y@z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_LLBW_DECODE('x@y@z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_LLBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p437_rms_dev43_abs_norm43_mmbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_DEV43 - 1x3 [[-3,1,5]], row 0  → mean=1, stddev=sqrt(((16+0+16)/3))≈.266
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_DEV43(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.266).abs() < 0.01, "Expected ~3.266, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_DEV43");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_DEV43"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM43 - 3x1 [[2],[4],[10]], col 0  → |2|+|4|+|10|=16.0
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM43(ARRAY[2,4,10], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 16.0).abs() < 0.01, "Expected ~16.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM43");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM43"),
    }

    // STRING_MMBW_ENCODE - ("a", "b", "c")  → "a=b=c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_MMBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_MMBW_ENCODE"),
    }

    // STRING_MMBW_DECODE - "x=y=z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_MMBW_DECODE('x=y=z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_MMBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p438_log_dev43_rms_dev44_nnbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV43 - 1x3 [[1,2,8]], row 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean≈.924, stddev≈.8645
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV43(ARRAY[1,2,8], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV43");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV43"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_DEV44 - 3x1 [[-3],[1],[5]], col 0  → mean=1, stddev=sqrt((16+0+16)/3)≈.266
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_DEV44(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.266).abs() < 0.01, "Expected ~3.266, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_DEV44");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_DEV44"),
    }

    // STRING_NNBW_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_NNBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_NNBW_ENCODE"),
    }

    // STRING_NNBW_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_NNBW_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_NNBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p439_abs_dev44_log_dev44_oobw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_DEV44 - 1x3 [[-3,1,5]], row 0  → mean=1, MAD=(4+0+4)/3≈.667
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_DEV44(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.667).abs() < 0.01, "Expected ~2.667, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_DEV44");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_DEV44"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV44 - 3x1 [[1],[2],[8]], col 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean≈.924, stddev≈.8645
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV44(ARRAY[1,2,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV44");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV44"),
    }

    // STRING_OOBW_ENCODE - ("a", "b", "c")  → "a|b|c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_OOBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a|b|c".to_string()));
        }
        _ => panic!("Expected Query for STRING_OOBW_ENCODE"),
    }

    // STRING_OOBW_DECODE - "x|y|z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_OOBW_DECODE('x|y|z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_OOBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p440_rms_range44_abs_dev44_ppbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE44 - 1x3 [[-3,1,5]], row 0  → range=5-(-3)=8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE44(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.01, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE44");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE44"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV44 - 3x1 [[2],[4],[9]], col 0  → mean=5, MAD=(3+1+4)/3=2.667
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV44(ARRAY[2,4,9], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.667).abs() < 0.01, "Expected ~2.667, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV44");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV44"),
    }

    // STRING_PPBW_ENCODE - ("a", "b", "c")  → "a:b:c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_PPBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a:b:c".to_string()));
        }
        _ => panic!("Expected Query for STRING_PPBW_ENCODE"),
    }

    // STRING_PPBW_DECODE - "x:y:z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_PPBW_DECODE('x:y:z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_PPBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p441_abs_norm44_rms_range44_qqbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM44 - 1x3 [[-3,1,5]], row 0  → |−2|+|1|+|5|=9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM44(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.01, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM44");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM44"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_RANGE44 - 3x1 [[2],[4],[9]], col 0  → range=9-2=7
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_RANGE44(ARRAY[2,4,9], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 7.0).abs() < 0.01, "Expected ~7.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_RANGE44");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_RANGE44"),
    }

    // STRING_QQBW_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_QQBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_QQBW_ENCODE"),
    }

    // STRING_QQBW_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_QQBW_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_QQBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p442_log_dev44_abs_norm44_rrbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV44 - 1x3 [[1,2,8]], row 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean≈.924, stddev≈.8645
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV44(ARRAY[1,2,8], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV44");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV44"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM44 - 3x1 [[-3],[1],[5]], col 0  → |−2|+|1|+|5|=9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM44(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.01, "Expected ~9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM44");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM44"),
    }

    // STRING_RRBW_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_RRBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_RRBW_ENCODE"),
    }

    // STRING_RRBW_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_RRBW_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_RRBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p443_rms_norm45_log_dev45_ssbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM45 - 1x3 [[-3,1,5]], row 0  → RMS=sqrt((9+1+25)/3)=sqrt(35/3)≈.416
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM45(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM45");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM45"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV45 - 3x1 [[1],[2],[8]], col 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean≈.924, stddev≈.8645
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV45(ARRAY[1,2,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV45");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV45"),
    }

    // STRING_SSBW_ENCODE - ("a", "b", "c")  → "a@b@c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_SSBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a@b@c".to_string()));
        }
        _ => panic!("Expected Query for STRING_SSBW_ENCODE"),
    }

    // STRING_SSBW_DECODE - "x@y@z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_SSBW_DECODE('x@y@z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_SSBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p444_rms_range45_abs_dev45_ttbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE45 - 1x3 [[-3,1,5]], row 0  → range=5-(-3)=8
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE45(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.01, "Expected 8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE45");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE45"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV45 - 3x1 [[2],[4],[6]], col 0  → mean=4, MAD=(2+0+2)/3≈.333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV45(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV45");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV45"),
    }

    // STRING_TTBW_ENCODE - ("a", "b", "c")  → "a=b=c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_TTBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_TTBW_ENCODE"),
    }

    // STRING_TTBW_DECODE - "x=y=z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_TTBW_DECODE('x=y=z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_TTBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p445_abs_norm45_rms_range45_uubw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM45 - 1x3 [[-3,1,5]], row 0  → |−2|+|1|+|5|=9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM45(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.01, "Expected 9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM45");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM45"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_RANGE45 - 3x1 [[2],[4],[6]], col 0  → range=6-2=4
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_RANGE45(ARRAY[2,4,6], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.0).abs() < 0.01, "Expected 4.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_RANGE45");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_RANGE45"),
    }

    // STRING_UUBW_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_UUBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_UUBW_ENCODE"),
    }

    // STRING_UUBW_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_UUBW_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_UUBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p446_log_dev45_abs_norm45_vvbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV45 - 1x3 [[1,2,8]], row 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean≈.924, stddev≈.8645
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV45(ARRAY[1,2,8], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV45");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV45"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM45 - 3x1 [[-3],[1],[5]], col 0  → |−2|+|1|+|5|=9
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM45(ARRAY[-3,1,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 9.0).abs() < 0.01, "Expected 9.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM45");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM45"),
    }

    // STRING_VVBW_ENCODE - ("a", "b", "c")  → "a|b|c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_VVBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a|b|c".to_string()));
        }
        _ => panic!("Expected Query for STRING_VVBW_ENCODE"),
    }

    // STRING_VVBW_DECODE - "x|y|z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_VVBW_DECODE('x|y|z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_VVBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p447_rms_norm46_log_dev46_wwbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM46 - 1x3 [[-3,1,5]], row 0  → RMS=sqrt((9+1+25)/3)=sqrt(35/3)≈.416
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM46(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM46");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM46"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV46 - 3x1 [[1],[2],[8]], col 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean≈.924, stddev≈.8645
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV46(ARRAY[1,2,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV46");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV46"),
    }

    // STRING_WWBW_ENCODE - ("a", "b", "c")  → "a:b:c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_WWBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a:b:c".to_string()));
        }
        _ => panic!("Expected Query for STRING_WWBW_ENCODE"),
    }

    // STRING_WWBW_DECODE - "x:y:z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_WWBW_DECODE('x:y:z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_WWBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p448_abs_dev46_rms_norm46_xxbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_DEV46 - 1x4 [[2,4,6,8]], row 0  → mean=5, MAD=(3+1+1+3)/4=2.0
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_DEV46(ARRAY[2,4,6,8], 1, 4, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.0).abs() < 0.01, "Expected ~2.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_DEV46");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_DEV46"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM46 - 3x1 [[3],[4],[5]], col 0  → RMS=sqrt((9+16+25)/3)=sqrt(50/3)≈.082
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM46(ARRAY[3,4,5], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.082).abs() < 0.01, "Expected ~4.082, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM46");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM46"),
    }

    // STRING_XXBW_ENCODE - ("a", "b", "c")  → "a~b~c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_XXBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_XXBW_ENCODE"),
    }

    // STRING_XXBW_DECODE - "x~y~z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_XXBW_DECODE('x~y~z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_XXBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p449_rms_range46_abs_dev46_yybw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE46 - 1x4 [[1,5,3,9]], row 0  → range=9-1=8.0
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE46(ARRAY[1,5,3,9], 1, 4, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.01, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE46");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE46"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV46 - 3x1 [[2],[6],[4]], col 0  → mean=4, MAD=(2+2+0)/3≈.333
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV46(ARRAY[2,6,4], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV46");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV46"),
    }

    // STRING_YYBW_ENCODE - ("a", "b", "c")  → "a^b^c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_YYBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_YYBW_ENCODE"),
    }

    // STRING_YYBW_DECODE - "x^y^z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_YYBW_DECODE('x^y^z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_YYBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p450_abs_norm46_rms_range46_zzbw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM46 - 1x3 [[-3,1,5]], row 0  → mean(|v|)=(3+1+5)/3≈.0
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM46(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM46");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM46"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_RANGE46 - 3x1 [[2],[8],[4]], col 0  → range=8-2=6.0
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_RANGE46(ARRAY[2,8,4], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 6.0).abs() < 0.01, "Expected ~6.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_RANGE46");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_RANGE46"),
    }

    // STRING_ZZBW_ENCODE - ("a", "b", "c")  → "a@b@c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_ZZBW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a@b@c".to_string()));
        }
        _ => panic!("Expected Query for STRING_ZZBW_ENCODE"),
    }

    // STRING_ZZBW_DECODE - "x@y@z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_ZZBW_DECODE('x@y@z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_ZZBW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p451_rms_norm47_log_dev47_aacw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM47 - 1x3 [[-3,1,5]], row 0  → RMS=sqrt((9+1+25)/3)=sqrt(35/3)≈.416
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM47(ARRAY[-3,1,5], 1, 3, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM47");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM47"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV47 - 3x1 [[1],[2],[8]], col 0  → ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean≈.924, stddev≈.8645
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV47(ARRAY[1,2,8], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV47");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV47"),
    }

    // STRING_AACW_ENCODE - ("a", "b", "c")  → "a=b=c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_AACW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_AACW_ENCODE"),
    }

    // STRING_AACW_DECODE - "x=y=z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_AACW_DECODE('x=y=z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_AACW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p452_abs_dev47_rms_norm47_bbcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_DEV47 - 1x4 [[2,4,6,8]], row 0  → mean=5, MAD=(3+1+1+3)/4=2.0
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_DEV47(ARRAY[2,4,6,8], 1, 4, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.0).abs() < 0.01, "Expected ~2.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_DEV47");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_DEV47"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM47 - 3x1 [[3],[4],[0]], col 0  → RMS=sqrt((9+16+0)/3)=sqrt(25/3)≈.887
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM47(ARRAY[3,4,0], 3, 1, 0)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.887).abs() < 0.01, "Expected ~2.887, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM47");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM47"),
    }

    // STRING_BBCW_ENCODE - ("a", "b", "c")  → "a%b%c"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_BBCW_ENCODE('a', 'b', 'c')",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_BBCW_ENCODE"),
    }

    // STRING_BBCW_DECODE - "x%y%z" field 1  → "y"
    let result = run_sql(
        &storage,
        &txn_mgr,
        &executor,
        "SELECT STRING_BBCW_DECODE('x%y%z', 1)",
        Some(&txn),
    )
    .unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_BBCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}
