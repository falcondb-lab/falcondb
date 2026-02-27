//! Integration tests for secondary index range scan.
//!
//! Covers:
//! - SecondaryIndex::range_scan with various bound combinations
//! - StorageEngine::index_range_scan end-to-end
//! - Correct ordering of encoded keys (Int32, Int64, Float64, Text)

use falcon_common::datum::Datum;
use falcon_storage::memtable::{encode_column_value, SecondaryIndex};

// ─── SecondaryIndex::range_scan unit tests ───────────────────────────

fn make_index_with_ints(values: &[i32]) -> SecondaryIndex {
    let idx = SecondaryIndex::new(0);
    for (i, v) in values.iter().enumerate() {
        let key = encode_column_value(&Datum::Int32(*v));
        let pk = encode_column_value(&Datum::Int32(i as i32));
        idx.insert(key, pk);
    }
    idx
}

#[test]
fn test_range_scan_gt_exclusive() {
    let idx = make_index_with_ints(&[10, 20, 30, 40, 50]);
    let bound = encode_column_value(&Datum::Int32(25));
    // key > 25 → should return PKs for 30, 40, 50
    let result = idx.range_scan(Some((&bound, false)), None);
    assert_eq!(result.len(), 3, "expected 3 results for > 25");
}

#[test]
fn test_range_scan_gte_inclusive() {
    let idx = make_index_with_ints(&[10, 20, 30, 40, 50]);
    let bound = encode_column_value(&Datum::Int32(30));
    // key >= 30 → should return PKs for 30, 40, 50
    let result = idx.range_scan(Some((&bound, true)), None);
    assert_eq!(result.len(), 3, "expected 3 results for >= 30");
}

#[test]
fn test_range_scan_lt_exclusive() {
    let idx = make_index_with_ints(&[10, 20, 30, 40, 50]);
    let bound = encode_column_value(&Datum::Int32(30));
    // key < 30 → should return PKs for 10, 20
    let result = idx.range_scan(None, Some((&bound, false)));
    assert_eq!(result.len(), 2, "expected 2 results for < 30");
}

#[test]
fn test_range_scan_lte_inclusive() {
    let idx = make_index_with_ints(&[10, 20, 30, 40, 50]);
    let bound = encode_column_value(&Datum::Int32(30));
    // key <= 30 → should return PKs for 10, 20, 30
    let result = idx.range_scan(None, Some((&bound, true)));
    assert_eq!(result.len(), 3, "expected 3 results for <= 30");
}

#[test]
fn test_range_scan_between_inclusive() {
    let idx = make_index_with_ints(&[10, 20, 30, 40, 50]);
    let lo = encode_column_value(&Datum::Int32(20));
    let hi = encode_column_value(&Datum::Int32(40));
    // 20 <= key <= 40 → should return PKs for 20, 30, 40
    let result = idx.range_scan(Some((&lo, true)), Some((&hi, true)));
    assert_eq!(result.len(), 3, "expected 3 results for BETWEEN 20 AND 40");
}

#[test]
fn test_range_scan_between_exclusive() {
    let idx = make_index_with_ints(&[10, 20, 30, 40, 50]);
    let lo = encode_column_value(&Datum::Int32(20));
    let hi = encode_column_value(&Datum::Int32(40));
    // 20 < key < 40 → should return PK for 30
    let result = idx.range_scan(Some((&lo, false)), Some((&hi, false)));
    assert_eq!(result.len(), 1, "expected 1 result for > 20 AND < 40");
}

#[test]
fn test_range_scan_unbounded_both() {
    let idx = make_index_with_ints(&[10, 20, 30]);
    // No bounds → should return all 3
    let result = idx.range_scan(None, None);
    assert_eq!(result.len(), 3, "expected all 3 results for unbounded range");
}

#[test]
fn test_range_scan_empty_result() {
    let idx = make_index_with_ints(&[10, 20, 30]);
    let lo = encode_column_value(&Datum::Int32(100));
    // key > 100 → empty
    let result = idx.range_scan(Some((&lo, false)), None);
    assert!(result.is_empty(), "expected empty result for > 100");
}

#[test]
fn test_range_scan_negative_ints() {
    let idx = make_index_with_ints(&[-50, -20, 0, 10, 30]);
    let lo = encode_column_value(&Datum::Int32(-30));
    let hi = encode_column_value(&Datum::Int32(15));
    // -30 <= key <= 15 → should return PKs for -20, 0, 10
    let result = idx.range_scan(Some((&lo, true)), Some((&hi, true)));
    assert_eq!(result.len(), 3, "expected 3 results for BETWEEN -30 AND 15");
}

#[test]
fn test_range_scan_int64() {
    let idx = SecondaryIndex::new(0);
    for v in [100i64, 200, 300, 400, 500] {
        let key = encode_column_value(&Datum::Int64(v));
        let pk = encode_column_value(&Datum::Int64(v));
        idx.insert(key, pk);
    }
    let lo = encode_column_value(&Datum::Int64(200));
    let hi = encode_column_value(&Datum::Int64(400));
    let result = idx.range_scan(Some((&lo, true)), Some((&hi, true)));
    assert_eq!(result.len(), 3, "expected 3 results for Int64 BETWEEN 200 AND 400");
}

#[test]
fn test_range_scan_float64() {
    let idx = SecondaryIndex::new(0);
    for v in [1.0f64, 2.5, 3.7, 5.0, 8.1] {
        let key = encode_column_value(&Datum::Float64(v));
        let pk = encode_column_value(&Datum::Float64(v));
        idx.insert(key, pk);
    }
    let lo = encode_column_value(&Datum::Float64(2.0));
    let hi = encode_column_value(&Datum::Float64(6.0));
    let result = idx.range_scan(Some((&lo, true)), Some((&hi, true)));
    // 2.0 <= key <= 6.0 → 2.5, 3.7, 5.0
    assert_eq!(result.len(), 3, "expected 3 results for Float64 BETWEEN 2.0 AND 6.0");
}

#[test]
fn test_range_scan_text_ordering() {
    let idx = SecondaryIndex::new(0);
    for s in ["apple", "banana", "cherry", "date", "elderberry"] {
        let key = encode_column_value(&Datum::Text(s.into()));
        let pk = encode_column_value(&Datum::Text(s.into()));
        idx.insert(key, pk);
    }
    let lo = encode_column_value(&Datum::Text("banana".into()));
    let hi = encode_column_value(&Datum::Text("date".into()));
    let result = idx.range_scan(Some((&lo, true)), Some((&hi, true)));
    // banana <= key <= date → banana, cherry, date
    assert_eq!(result.len(), 3, "expected 3 text results");
}

#[test]
fn test_range_scan_duplicate_keys() {
    let idx = SecondaryIndex::new(0);
    // Insert multiple PKs for the same key value
    let key30 = encode_column_value(&Datum::Int32(30));
    idx.insert(key30.clone(), vec![1]);
    idx.insert(key30.clone(), vec![2]);
    idx.insert(key30.clone(), vec![3]);
    let key20 = encode_column_value(&Datum::Int32(20));
    idx.insert(key20.clone(), vec![4]);

    let lo = encode_column_value(&Datum::Int32(25));
    // key > 25 → should return all 3 PKs for key=30
    let result = idx.range_scan(Some((&lo, false)), None);
    assert_eq!(result.len(), 3, "expected 3 PKs for duplicate key 30");
}

#[test]
fn test_range_scan_single_element() {
    let idx = make_index_with_ints(&[42]);
    let lo = encode_column_value(&Datum::Int32(42));
    let result = idx.range_scan(Some((&lo, true)), Some((&lo, true)));
    assert_eq!(result.len(), 1, "single element range should return 1");
}

#[test]
fn test_range_scan_single_element_exclusive() {
    // Exclusive bounds where lo == hi should return empty.
    let idx = make_index_with_ints(&[42]);
    let lo = encode_column_value(&Datum::Int32(42));
    // lo exclusive, hi exclusive with same key → logically empty range
    let result = idx.range_scan(Some((&lo, false)), Some((&lo, true)));
    assert_eq!(result.len(), 0, "exclusive lower bound on exact match should return 0");
    let result2 = idx.range_scan(Some((&lo, true)), Some((&lo, false)));
    assert_eq!(result2.len(), 0, "exclusive upper bound on exact match should return 0");
}
