#![allow(clippy::approx_constant, clippy::useless_vec)]

mod common;
use common::*;

#[test]
fn test_p453_log_dev47_abs_dev47_cccw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV47 - 1x3 [[1,2,8]], row 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean鈮?.924, stddev鈮?.8645
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV47(ARRAY[1,2,8], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV47");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV47"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV47 - 3x1 [[2],[4],[6]], col 0 鈫?mean=4, MAD=(2+0+2)/3鈮?.333
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV47(ARRAY[2,4,6], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV47");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV47"),
    }

    // STRING_CCCW_ENCODE - ("a", "b", "c") 鈫?"a|b|c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_CCCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a|b|c".to_string()));
        }
        _ => panic!("Expected Query for STRING_CCCW_ENCODE"),
    }

    // STRING_CCCW_DECODE - "x|y|z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_CCCW_DECODE('x|y|z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_CCCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p454_rms_range47_abs_norm47_ddcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE47 - 1x3 [[-3,1,5]], row 0 鈫?range = 5-(-3) = 8
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE47(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.01, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE47");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE47"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM47 - 3x1 [[-2],[4],[-6]], col 0 鈫?mean(|v|) = (2+4+6)/3 = 4.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM47(ARRAY[-2,4,-6], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.0).abs() < 0.01, "Expected ~4.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM47");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM47"),
    }

    // STRING_DDCW_ENCODE - ("a", "b", "c") 鈫?"a:b:c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_DDCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a:b:c".to_string()));
        }
        _ => panic!("Expected Query for STRING_DDCW_ENCODE"),
    }

    // STRING_DDCW_DECODE - "x:y:z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_DDCW_DECODE('x:y:z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_DDCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p455_rms_norm48_log_dev48_eecw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM48 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM48(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM48");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM48"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV48 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean鈮?.924, stddev鈮?.8645
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV48(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV48");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV48"),
    }

    // STRING_EECW_ENCODE - ("a", "b", "c") 鈫?"a~b~c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_EECW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_EECW_ENCODE"),
    }

    // STRING_EECW_DECODE - "x~y~z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_EECW_DECODE('x~y~z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_EECW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p456_rms_range48_abs_dev48_ffcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE48 - 1x3 [[-3,1,5]], row 0 鈫?range=5-(-3)=8
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE48(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.01, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE48");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE48"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV48 - 3x1 [[2],[4],[6]], col 0 鈫?mean=4, MAD=(2+0+2)/3鈮?.333
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV48(ARRAY[2,4,6], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV48");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV48"),
    }

    // STRING_FFCW_ENCODE - ("a", "b", "c") 鈫?"a^b^c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_FFCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_FFCW_ENCODE"),
    }

    // STRING_FFCW_DECODE - "x^y^z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_FFCW_DECODE('x^y^z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_FFCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p457_log_dev48_rms_norm48_ggcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV48 - 1x3 [[1,2,8]], row 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079, mean鈮?.924, stddev鈮?.8645
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV48(ARRAY[1,2,8], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8645).abs() < 0.01, "Expected ~0.8645, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV48");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV48"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM48 - 3x1 [[3],[4],[5]], col 0 鈫?RMS=sqrt((9+16+25)/3)=sqrt(50/3)鈮?.082
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM48(ARRAY[3,4,5], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.082).abs() < 0.01, "Expected ~4.082, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM48");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM48"),
    }

    // STRING_GGCW_ENCODE - ("a", "b", "c") 鈫?"a@b@c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_GGCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a@b@c".to_string()));
        }
        _ => panic!("Expected Query for STRING_GGCW_ENCODE"),
    }

    // STRING_GGCW_DECODE - "x@y@z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_GGCW_DECODE('x@y@z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_GGCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p458_abs_norm48_log_range48_hhcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM48 - 1x3 [[-3,1,5]], row 0 鈫?mean(|v|)=(3+1+5)/3鈮?.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM48(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM48");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM48"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE48 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079, range=2.079-0=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE48(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE48");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE48"),
    }

    // STRING_HHCW_ENCODE - ("a", "b", "c") 鈫?"a=b=c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_HHCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_HHCW_ENCODE"),
    }

    // STRING_HHCW_DECODE - "x=y=z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_HHCW_DECODE('x=y=z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_HHCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p459_rms_range49_abs_dev49_iicw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_RANGE49 - 1x3 [[-3,1,5]], row 0 鈫?range=5-(-3)=8.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_RANGE49(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 8.0).abs() < 0.01, "Expected ~8.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_RANGE49");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_RANGE49"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV49 - 3x1 [[1],[5],[3]], col 0 鈫?mean=3, MAD=(2+2+0)/3鈮?.333
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV49(ARRAY[1,5,3], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.333).abs() < 0.01, "Expected ~1.333, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV49");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV49"),
    }

    // STRING_IICW_ENCODE - ("a", "b", "c") 鈫?"a%b%c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_IICW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_IICW_ENCODE"),
    }

    // STRING_IICW_DECODE - "x%y%z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_IICW_DECODE('x%y%z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_IICW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p460_abs_norm49_log_range49_jjcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM49 - 1x3 [[-3,1,5]], row 0 鈫?mean(|v|)=(3+1+5)/3鈮?.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM49(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM49");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM49"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE49 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079, range=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE49(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE49");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE49"),
    }

    // STRING_JJCW_ENCODE - ("a", "b", "c") 鈫?"a|b|c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_JJCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a|b|c".to_string()));
        }
        _ => panic!("Expected Query for STRING_JJCW_ENCODE"),
    }

    // STRING_JJCW_DECODE - "x|y|z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_JJCW_DECODE('x|y|z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_JJCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p461_log_dev49_rms_norm49_kkcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV49 - 1x3 [[-3,1,5]], row 0 鈫?ln(3)=1.0986, ln(1)=0, ln(5)=1.6094; mean鈮?.9027, stddev鈮?.6715
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV49(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.6715).abs() < 0.01, "Expected ~0.6715, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV49");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV49"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM49 - 3x1 [[2],[4],[6]], col 0 鈫?RMS=sqrt((4+16+36)/3)=sqrt(56/3)鈮?.3205
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM49(ARRAY[2,4,6], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.3205).abs() < 0.01, "Expected ~4.3205, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM49");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM49"),
    }

    // STRING_KKCW_ENCODE - ("a", "b", "c") 鈫?"a:b:c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_KKCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a:b:c".to_string()));
        }
        _ => panic!("Expected Query for STRING_KKCW_ENCODE"),
    }

    // STRING_KKCW_DECODE - "x:y:z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_KKCW_DECODE('x:y:z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_KKCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p462_rms_norm49_log_dev49_llcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM49 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM49(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM49");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM49"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV49 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8515
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV49(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV49");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV49"),
    }

    // STRING_LLCW_ENCODE - ("a", "b", "c") 鈫?"a~b~c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_LLCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_LLCW_ENCODE"),
    }

    // STRING_LLCW_DECODE - "x~y~z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_LLCW_DECODE('x~y~z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_LLCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p463_abs_norm50_log_range50_mmcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM50 - 1x3 [[-3,4,5]], row 0 鈫?mean(3,4,5)=4.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM50(ARRAY[-3,4,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.0).abs() < 0.01, "Expected ~4.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM50");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM50"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE50 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range鈮?.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE50(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE50");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE50"),
    }

    // STRING_MMCW_ENCODE - ("a", "b", "c") 鈫?"a^b^c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_MMCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_MMCW_ENCODE"),
    }

    // STRING_MMCW_DECODE - "x^y^z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_MMCW_DECODE('x^y^z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_MMCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p464_rms_norm50_log_dev50_nncw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM50 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM50(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM50");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM50"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV50 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV50(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV50");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV50"),
    }

    // STRING_NNCW_ENCODE - ("a", "b", "c") 鈫?"a@b@c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_NNCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a@b@c".to_string()));
        }
        _ => panic!("Expected Query for STRING_NNCW_ENCODE"),
    }

    // STRING_NNCW_DECODE - "x@y@z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_NNCW_DECODE('x@y@z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_NNCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p465_log_dev50_rms_norm50_oocw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV50 - 1x3 [[1,2,8]], row 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV50(ARRAY[1,2,8], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV50");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV50"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM50 - 3x1 [[-3],[1],[5]], col 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM50(ARRAY[-3,1,5], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM50");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM50"),
    }

    // STRING_OOCW_ENCODE - ("a", "b", "c") 鈫?"a=b=c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_OOCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_OOCW_ENCODE"),
    }

    // STRING_OOCW_DECODE - "x=y=z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_OOCW_DECODE('x=y=z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_OOCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p466_log_range50_abs_norm50_ppcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_RANGE50 - 1x3 [[1,2,8]], row 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079-0=2.079
    // But ln(1)=0 means |v|=1>0 so ln(1)=0 is included. Wait, ln(|1|)=0. range = max-min = 2.079-0 = 2.079
    // Actually v=1, |v|>0 so included: ln(1)=0; v=2: ln(2)=0.693; v=8: ln(8)=2.079. range=2.079-0=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_RANGE50(ARRAY[1,2,8], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_RANGE50");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_RANGE50"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_NORM50 - 3x1 [[-3],[1],[5]], col 0 鈫?mean(|-3|,|1|,|5|)=mean(3,1,5)=3.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_NORM50(ARRAY[-3,1,5], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_NORM50");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_NORM50"),
    }

    // STRING_PPCW_ENCODE - ("a", "b", "c") 鈫?"a%b%c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_PPCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_PPCW_ENCODE"),
    }

    // STRING_PPCW_DECODE - "x%y%z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_PPCW_DECODE('x%y%z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_PPCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p467_abs_norm51_abs_dev51_qqcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM51 - 1x3 [[-3,1,5]], row 0 鈫?mean(3,1,5)=3.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM51(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM51");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM51"),
    }

    // ARRAY_MATRIX_COLUMN_ABS_DEV51 - 3x1 [[-3],[1],[5]], col 0 鈫?abs vals [3,1,5], mean=3, var=((0+4+4)/3)=2.667, stddev鈮?.633
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_ABS_DEV51(ARRAY[-3,1,5], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.633).abs() < 0.01, "Expected ~1.633, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_ABS_DEV51");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_ABS_DEV51"),
    }

    // STRING_QQCW_ENCODE - ("a", "b", "c") 鈫?"a|b|c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_QQCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a|b|c".to_string()));
        }
        _ => panic!("Expected Query for STRING_QQCW_ENCODE"),
    }

    // STRING_QQCW_DECODE - "x|y|z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_QQCW_DECODE('x|y|z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_QQCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p468_abs_dev51_log_range51_rrcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_DEV51 - 1x3 [[-3,1,5]], row 0 鈫?abs vals [3,1,5], mean=3, var=((0+4+4)/3)=2.667, stddev鈮?.633
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_DEV51(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.633).abs() < 0.01, "Expected ~1.633, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_DEV51");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_DEV51"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE51 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE51(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE51");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE51"),
    }

    // STRING_RRCW_ENCODE - ("a", "b", "c") 鈫?"a:b:c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_RRCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a:b:c".to_string()));
        }
        _ => panic!("Expected Query for STRING_RRCW_ENCODE"),
    }

    // STRING_RRCW_DECODE - "x:y:z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_RRCW_DECODE('x:y:z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_RRCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p469_log_dev51_rms_norm51_sscw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_LOG_DEV51 - 1x3 [[-3,1,5]], row 0 鈫?ln(3)=1.099, ln(1)=0, ln(5)=1.609; mean鈮?.903, stddev鈮?.660
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_LOG_DEV51(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.672).abs() < 0.01, "Expected ~0.672, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_LOG_DEV51");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_LOG_DEV51"),
    }

    // ARRAY_MATRIX_COLUMN_RMS_NORM51 - 3x1 [[2],[4],[6]], col 0 鈫?RMS=sqrt((4+16+36)/3)=sqrt(56/3)鈮?.320
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_RMS_NORM51(ARRAY[2,4,6], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.320).abs() < 0.01, "Expected ~4.320, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_RMS_NORM51");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_RMS_NORM51"),
    }

    // STRING_SSCW_ENCODE - ("a", "b", "c") 鈫?"a~b~c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_SSCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_SSCW_ENCODE"),
    }

    // STRING_SSCW_DECODE - "x~y~z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_SSCW_DECODE('x~y~z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_SSCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p470_rms_norm51_log_dev51_ttcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM51 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM51(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM51");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM51"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV51 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.865
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV51(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.865).abs() < 0.01, "Expected ~0.865, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV51");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV51"),
    }

    // STRING_TTCW_ENCODE - ("a", "b", "c") 鈫?"a^b^c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_TTCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_TTCW_ENCODE"),
    }

    // STRING_TTCW_DECODE - "x^y^z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_TTCW_DECODE('x^y^z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_TTCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p471_abs_norm52_log_range52_uucw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM52 - 1x3 [[-3,1,5]], row 0 鈫?mean(|v|)=(3+1+5)/3=3.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM52(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM52");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM52"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE52 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079-0=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE52(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE52");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE52"),
    }

    // STRING_UUCW_ENCODE - ("a", "b", "c") 鈫?"a@b@c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_UUCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a@b@c".to_string()));
        }
        _ => panic!("Expected Query for STRING_UUCW_ENCODE"),
    }

    // STRING_UUCW_DECODE - "x@y@z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_UUCW_DECODE('x@y@z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_UUCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p472_rms_norm52_log_dev52_vvcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM52 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM52(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM52");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM52"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV52 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV52(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV52");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV52"),
    }

    // STRING_VVCW_ENCODE - ("a", "b", "c") 鈫?"a=b=c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_VVCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_VVCW_ENCODE"),
    }

    // STRING_VVCW_DECODE - "x=y=z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_VVCW_DECODE('x=y=z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_VVCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p473_abs_norm53_log_range53_wwcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM53 - 1x3 [[-4,2,6]], row 0 鈫?mean(|vals|)=(4+2+6)/3=4.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM53(ARRAY[-4,2,6], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.0).abs() < 0.01, "Expected ~4.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM53");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM53"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE53 - 3x1 [[1],[3],[9]], col 0 鈫?ln(1)=0, ln(3)=1.099, ln(9)=2.197; range=2.197
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE53(ARRAY[1,3,9], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.197).abs() < 0.01, "Expected ~2.197, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE53");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE53"),
    }

    // STRING_WWCW_ENCODE - ("a", "b", "c") 鈫?"a%b%c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_WWCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_WWCW_ENCODE"),
    }

    // STRING_WWCW_DECODE - "x%y%z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_WWCW_DECODE('x%y%z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_WWCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p474_rms_norm53_log_dev53_xxcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM53 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM53(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM53");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM53"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV53 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV53(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV53");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV53"),
    }

    // STRING_XXCW_ENCODE - ("a", "b", "c") 鈫?"a|b|c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_XXCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a|b|c".to_string()));
        }
        _ => panic!("Expected Query for STRING_XXCW_ENCODE"),
    }

    // STRING_XXCW_DECODE - "x|y|z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_XXCW_DECODE('x|y|z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_XXCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p475_abs_norm54_log_range54_yycw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM54 - 1x3 [[-3,4,-1]], row 0 鈫?mean(|鈭?|,|4|,|鈭?|)=(3+4+1)/3鈮?.667
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM54(ARRAY[-3,4,-1], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.667).abs() < 0.01, "Expected ~2.667, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM54");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM54"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE54 - 3x1 [[2],[5],[10]], col 0 鈫?ln(2)=0.693, ln(5)=1.609, ln(10)=2.303; range=2.303鈭?.693鈮?.609
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE54(ARRAY[2,5,10], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 1.609).abs() < 0.01, "Expected ~1.609, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE54");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE54"),
    }

    // STRING_YYCW_ENCODE - ("a", "b", "c") 鈫?"a:b:c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_YYCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a:b:c".to_string()));
        }
        _ => panic!("Expected Query for STRING_YYCW_ENCODE"),
    }

    // STRING_YYCW_DECODE - "x:y:z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_YYCW_DECODE('x:y:z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_YYCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p476_rms_norm54_log_dev54_zzcw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM54 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM54(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM54");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM54"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV54 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV54(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV54");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV54"),
    }

    // STRING_ZZCW_ENCODE - ("a", "b", "c") 鈫?"a~b~c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_ZZCW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_ZZCW_ENCODE"),
    }

    // STRING_ZZCW_DECODE - "x~y~z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_ZZCW_DECODE('x~y~z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_ZZCW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p477_abs_norm55_log_range55_aadw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM55 - 1x3 [[-3,1,5]], row 0 鈫?mean(|v|)=(3+1+5)/3=3.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM55(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM55");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM55"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE55 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079-0=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE55(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE55");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE55"),
    }

    // STRING_AADW_ENCODE - ("a", "b", "c") 鈫?"a+b+c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_AADW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a+b+c".to_string()));
        }
        _ => panic!("Expected Query for STRING_AADW_ENCODE"),
    }

    // STRING_AADW_DECODE - "x+y+z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_AADW_DECODE('x+y+z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_AADW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p478_rms_norm55_log_dev55_bbdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM55 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM55(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM55");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM55"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV55 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV55(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV55");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV55"),
    }

    // STRING_BBDW_ENCODE - ("a", "b", "c") 鈫?"a*b*c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_BBDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a*b*c".to_string()));
        }
        _ => panic!("Expected Query for STRING_BBDW_ENCODE"),
    }

    // STRING_BBDW_DECODE - "x*y*z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_BBDW_DECODE('x*y*z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_BBDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p479_abs_norm56_log_range56_ccdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM56 - 1x3 [[-3,1,5]], row 0 鈫?mean(|v|)=(3+1+5)/3=3.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM56(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM56");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM56"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE56 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079-0=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE56(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE56");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE56"),
    }

    // STRING_CCDW_ENCODE - ("a", "b", "c") 鈫?"a#b#c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_CCDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a#b#c".to_string()));
        }
        _ => panic!("Expected Query for STRING_CCDW_ENCODE"),
    }

    // STRING_CCDW_DECODE - "x#y#z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_CCDW_DECODE('x#y#z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_CCDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p480_rms_norm56_log_dev56_dddw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM56 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM56(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM56");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM56"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV56 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV56(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV56");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV56"),
    }

    // STRING_DDDW_ENCODE - ("a", "b", "c") 鈫?"a&b&c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_DDDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a&b&c".to_string()));
        }
        _ => panic!("Expected Query for STRING_DDDW_ENCODE"),
    }

    // STRING_DDDW_DECODE - "x&y&z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_DDDW_DECODE('x&y&z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_DDDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p481_abs_norm57_log_range57_eedw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM57 - 1x3 [[-3,1,5]], row 0 鈫?mean(|vals|)=(3+1+5)/3鈮?.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM57(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM57");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM57"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE57 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079-0=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE57(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE57");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE57"),
    }

    // STRING_EEDW_ENCODE - ("a", "b", "c") 鈫?"a!b!c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_EEDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a!b!c".to_string()));
        }
        _ => panic!("Expected Query for STRING_EEDW_ENCODE"),
    }

    // STRING_EEDW_DECODE - "x!y!z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_EEDW_DECODE('x!y!z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_EEDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p482_rms_norm57_log_dev57_ffdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM57 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM57(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM57");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM57"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV57 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV57(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV57");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV57"),
    }

    // STRING_FFDW_ENCODE - ("a", "b", "c") 鈫?"a~b~c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_FFDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_FFDW_ENCODE"),
    }

    // STRING_FFDW_DECODE - "x~y~z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_FFDW_DECODE('x~y~z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_FFDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p483_abs_norm58_log_range58_ggdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM58 - 1x3 [[-3,1,5]], row 0 鈫?mean(|v|)=(3+1+5)/3=3.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM58(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM58");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM58"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE58 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE58(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE58");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE58"),
    }

    // STRING_GGDW_ENCODE - ("a", "b", "c") 鈫?"a^b^c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_GGDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_GGDW_ENCODE"),
    }

    // STRING_GGDW_DECODE - "x^y^z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_GGDW_DECODE('x^y^z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_GGDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p484_rms_norm58_log_dev58_hhdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM58 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM58(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM58");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM58"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV58 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV58(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV58");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV58"),
    }

    // STRING_HHDW_ENCODE - ("a", "b", "c") 鈫?"a@b@c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_HHDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a@b@c".to_string()));
        }
        _ => panic!("Expected Query for STRING_HHDW_ENCODE"),
    }

    // STRING_HHDW_DECODE - "x@y@z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_HHDW_DECODE('x@y@z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_HHDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p485_abs_norm59_log_range59_iidw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM59 - 1x3 [[-3,1,5]], row 0 鈫?mean(|v|)=(3+1+5)/3鈮?.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM59(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM59");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM59"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE59 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079-0=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE59(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE59");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE59"),
    }

    // STRING_IIDW_ENCODE - ("a", "b", "c") 鈫?"a=b=c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_IIDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_IIDW_ENCODE"),
    }

    // STRING_IIDW_DECODE - "x=y=z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_IIDW_DECODE('x=y=z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_IIDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p486_rms_norm59_log_dev59_jjdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM59 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM59(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM59");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM59"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV59 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV59(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV59");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV59"),
    }

    // STRING_JJDW_ENCODE - ("a", "b", "c") 鈫?"a%b%c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_JJDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_JJDW_ENCODE"),
    }

    // STRING_JJDW_DECODE - "x%y%z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_JJDW_DECODE('x%y%z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_JJDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p487_abs_norm60_log_range60_kkdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM60 - 1x3 [[-3,1,5]], row 0 鈫?mean(|v|)=(3+1+5)/3鈮?.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM60(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM60");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM60"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE60 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079-0=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE60(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE60");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE60"),
    }

    // STRING_KKDW_ENCODE - ("a", "b", "c") 鈫?"a|b|c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_KKDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a|b|c".to_string()));
        }
        _ => panic!("Expected Query for STRING_KKDW_ENCODE"),
    }

    // STRING_KKDW_DECODE - "x|y|z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_KKDW_DECODE('x|y|z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_KKDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p488_rms_norm60_log_dev60_lldw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM60 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM60(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM60");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM60"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV60 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV60(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV60");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV60"),
    }

    // STRING_LLDW_ENCODE - ("a", "b", "c") 鈫?"a:b:c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_LLDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a:b:c".to_string()));
        }
        _ => panic!("Expected Query for STRING_LLDW_ENCODE"),
    }

    // STRING_LLDW_DECODE - "x:y:z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_LLDW_DECODE('x:y:z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_LLDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p489_abs_norm61_log_range61_mmdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM61 - 1x3 [[-3,1,5]], row 0 鈫?mean(|v|)=(3+1+5)/3=3.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM61(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM61");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM61"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE61 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079-0=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE61(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE61");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE61"),
    }

    // STRING_MMDW_ENCODE - ("a", "b", "c") 鈫?"a+b+c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_MMDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a+b+c".to_string()));
        }
        _ => panic!("Expected Query for STRING_MMDW_ENCODE"),
    }

    // STRING_MMDW_DECODE - "x+y+z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_MMDW_DECODE('x+y+z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_MMDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p490_rms_norm61_log_dev61_nndw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM61 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM61(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM61");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM61"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV61 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV61(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV61");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV61"),
    }

    // STRING_NNDW_ENCODE - ("a", "b", "c") 鈫?"a*b*c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_NNDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a*b*c".to_string()));
        }
        _ => panic!("Expected Query for STRING_NNDW_ENCODE"),
    }

    // STRING_NNDW_DECODE - "x*y*z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_NNDW_DECODE('x*y*z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_NNDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p491_abs_norm62_log_range62_oodw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM62 - 1x3 [[-3,1,5]], row 0 鈫?mean(|vals|)=(3+1+5)/3=3.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM62(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM62");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM62"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE62 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079-0=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE62(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE62");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE62"),
    }

    // STRING_OODW_ENCODE - ("a", "b", "c") 鈫?"a#b#c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_OODW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a#b#c".to_string()));
        }
        _ => panic!("Expected Query for STRING_OODW_ENCODE"),
    }

    // STRING_OODW_DECODE - "x#y#z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_OODW_DECODE('x#y#z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_OODW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p492_rms_norm62_log_dev62_ppdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM62 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM62(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM62");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM62"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV62 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV62(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV62");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV62"),
    }

    // STRING_PPDW_ENCODE - ("a", "b", "c") 鈫?"a&b&c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_PPDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a&b&c".to_string()));
        }
        _ => panic!("Expected Query for STRING_PPDW_ENCODE"),
    }

    // STRING_PPDW_DECODE - "x&y&z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_PPDW_DECODE('x&y&z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_PPDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p493_abs_norm63_log_range63_qqdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM63 - 1x3 [[-4,2,6]], row 0 鈫?mean(|v|)=(4+2+6)/3鈮?.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM63(ARRAY[-4,2,6], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 4.0).abs() < 0.01, "Expected ~4.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM63");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM63"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE63 - 3x1 [[1],[3],[9]], col 0 鈫?ln(1)=0, ln(3)=1.099, ln(9)=2.197; range=2.197
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE63(ARRAY[1,3,9], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.197).abs() < 0.01, "Expected ~2.197, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE63");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE63"),
    }

    // STRING_QQDW_ENCODE - ("a", "b", "c") 鈫?"a!b!c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_QQDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a!b!c".to_string()));
        }
        _ => panic!("Expected Query for STRING_QQDW_ENCODE"),
    }

    // STRING_QQDW_DECODE - "x!y!z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_QQDW_DECODE('x!y!z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_QQDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p494_rms_norm63_log_dev63_rrdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM63 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM63(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM63");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM63"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV63 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV63(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV63");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV63"),
    }

    // STRING_RRDW_ENCODE - ("a", "b", "c") 鈫?"a~b~c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_RRDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a~b~c".to_string()));
        }
        _ => panic!("Expected Query for STRING_RRDW_ENCODE"),
    }

    // STRING_RRDW_DECODE - "x~y~z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_RRDW_DECODE('x~y~z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_RRDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p495_abs_norm64_log_range64_ssdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM64 - 1x3 [[-3,1,5]], row 0 鈫?mean(|v|)=(3+1+5)/3=3.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM64(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM64");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM64"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE64 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE64(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE64");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE64"),
    }

    // STRING_SSDW_ENCODE - ("a", "b", "c") 鈫?"a^b^c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_SSDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a^b^c".to_string()));
        }
        _ => panic!("Expected Query for STRING_SSDW_ENCODE"),
    }

    // STRING_SSDW_DECODE - "x^y^z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_SSDW_DECODE('x^y^z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_SSDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p496_rms_norm64_log_dev64_ttdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM64 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM64(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM64");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM64"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV64 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV64(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV64");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV64"),
    }

    // STRING_TTDW_ENCODE - ("a", "b", "c") 鈫?"a@b@c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_TTDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a@b@c".to_string()));
        }
        _ => panic!("Expected Query for STRING_TTDW_ENCODE"),
    }

    // STRING_TTDW_DECODE - "x@y@z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_TTDW_DECODE('x@y@z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_TTDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p497_abs_norm65_log_range65_uudw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM65 - 1x3 [[-3,1,5]], row 0 鈫?(3+1+5)/3鈮?.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM65(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM65");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM65"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE65 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079-0=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE65(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE65");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE65"),
    }

    // STRING_UUDW_ENCODE - ("a", "b", "c") 鈫?"a=b=c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_UUDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a=b=c".to_string()));
        }
        _ => panic!("Expected Query for STRING_UUDW_ENCODE"),
    }

    // STRING_UUDW_DECODE - "x=y=z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_UUDW_DECODE('x=y=z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_UUDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p498_rms_norm65_log_dev65_vvdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_RMS_NORM65 - 1x3 [[-3,1,5]], row 0 鈫?RMS=sqrt((9+1+25)/3)=sqrt(35/3)鈮?.416
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_RMS_NORM65(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.416).abs() < 0.01, "Expected ~3.416, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_RMS_NORM65");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_RMS_NORM65"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_DEV65 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; mean鈮?.924, stddev鈮?.8646
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_DEV65(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 0.8646).abs() < 0.01, "Expected ~0.8646, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_DEV65");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_DEV65"),
    }

    // STRING_VVDW_ENCODE - ("a", "b", "c") 鈫?"a%b%c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_VVDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a%b%c".to_string()));
        }
        _ => panic!("Expected Query for STRING_VVDW_ENCODE"),
    }

    // STRING_VVDW_DECODE - "x%y%z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_VVDW_DECODE('x%y%z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_VVDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}

#[test]
fn test_p499_abs_norm66_log_range66_wwdw() {
    let (storage, txn_mgr, executor) = setup();
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

    // ARRAY_MATRIX_ROW_ABS_NORM66 - 1x3 [[-3,1,5]], row 0 鈫?mean(|v|)=(3+1+5)/3=3.0
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_ROW_ABS_NORM66(ARRAY[-3,1,5], 1, 3, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_ROW_ABS_NORM66");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_ROW_ABS_NORM66"),
    }

    // ARRAY_MATRIX_COLUMN_LOG_RANGE66 - 3x1 [[1],[2],[8]], col 0 鈫?ln(1)=0, ln(2)=0.693, ln(8)=2.079; range=2.079-0=2.079
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT ARRAY_MATRIX_COLUMN_LOG_RANGE66(ARRAY[1,2,8], 3, 1, 0)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            if let Datum::Float64(v) = rows[0].values[0] {
                assert!((v - 2.079).abs() < 0.01, "Expected ~2.079, got {}", v);
            } else {
                panic!("Expected Float64 from ARRAY_MATRIX_COLUMN_LOG_RANGE66");
            }
        }
        _ => panic!("Expected Query for ARRAY_MATRIX_COLUMN_LOG_RANGE66"),
    }

    // STRING_WWDW_ENCODE - ("a", "b", "c") 鈫?"a|b|c"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_WWDW_ENCODE('a', 'b', 'c')", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("a|b|c".to_string()));
        }
        _ => panic!("Expected Query for STRING_WWDW_ENCODE"),
    }

    // STRING_WWDW_DECODE - "x|y|z" field 1 鈫?"y"
    let result = run_sql(&storage, &txn_mgr, &executor,
        "SELECT STRING_WWDW_DECODE('x|y|z', 1)", Some(&txn)).unwrap();
    match &result {
        falcon_executor::ExecutionResult::Query { rows, .. } => {
            assert_eq!(rows[0].values[0], Datum::Text("y".to_string()));
        }
        _ => panic!("Expected Query for STRING_WWDW_DECODE"),
    }

    txn_mgr.commit(txn.txn_id).unwrap();
}
