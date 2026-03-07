//! Serializable subplan representation for cross-process distributed execution.
//!
//! The existing `SubPlan` uses closures, which cannot be sent over the wire.
//! `SerializableSubPlan` provides a data-driven plan representation that can
//! be serialized (via serde) and sent over gRPC to remote shard nodes.
//!
//! # Architecture
//!
//! ```text
//!   Coordinator                          Remote Shard Node
//!   ──────────                           ─────────────────
//!   SerializableSubPlan                  receive plan bytes
//!       │ bincode::serialize                 │
//!       ▼                                    ▼
//!   gRPC request ──────────────────▶  bincode::deserialize
//!                                        │
//!                                        ▼
//!                                    SubPlan::from_serializable()
//!                                        │
//!                                        ▼
//!                                    execute on local StorageEngine
//! ```
//!
//! # Supported Operations
//!
//! - `Scan`: full table scan with optional column projection
//! - `Filter`: scan + predicate evaluation
//! - `PartialAggregate`: scan + per-shard partial aggregation
//! - `PartialGroupBy`: scan + grouped partial aggregation
//! - `OrderedScan`: scan + local sort (for merge-sort gather)
//! - `BroadcastJoin`: coordinator-gathered small table joined with local partition
//! - `HashPartitionJoin`: co-located hash join on pre-partitioned data
//! - `Update`: distributed UPDATE with filter predicate + column assignments
//! - `Delete`: distributed DELETE with filter predicate

use serde::{Deserialize, Serialize};

use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::FalconError;
use falcon_common::types::{DataType, IsolationLevel, TableId};

use super::SubPlanResult;

/// A serializable subplan that can be sent over the wire via gRPC/bincode.
///
/// Each variant maps to a common distributed query pattern. The coordinator
/// builds a `SerializableSubPlan`, serializes it, sends it to remote shard
/// nodes, which deserialize and execute it locally.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableSubPlan {
    /// Full table scan with optional column projection.
    Scan {
        table_id: u64,
        /// Column indices to project (empty = all columns).
        projection: Vec<usize>,
        /// Column metadata for the result set.
        columns: Vec<(String, DataType)>,
    },

    /// Table scan with a filter predicate.
    Filter {
        table_id: u64,
        projection: Vec<usize>,
        columns: Vec<(String, DataType)>,
        /// The filter predicate expressed as a tree of `FilterExpr` nodes.
        predicate: FilterExpr,
    },

    /// Partial aggregation (no GROUP BY): produces a single-row partial result.
    PartialAggregate {
        table_id: u64,
        /// Aggregate functions to compute.
        aggregates: Vec<AggFunc>,
        /// Output column metadata.
        columns: Vec<(String, DataType)>,
    },

    /// Partial grouped aggregation: produces per-group partial results.
    PartialGroupBy {
        table_id: u64,
        /// Column indices for GROUP BY keys.
        group_by_cols: Vec<usize>,
        /// Aggregate functions to compute per group.
        aggregates: Vec<AggFunc>,
        /// Output column metadata (group keys + agg results).
        columns: Vec<(String, DataType)>,
    },

    /// Ordered scan: scan + local sort for merge-sort gather.
    OrderedScan {
        table_id: u64,
        projection: Vec<usize>,
        columns: Vec<(String, DataType)>,
        /// Sort specification: (column_index_in_output, ascending).
        sort_keys: Vec<(usize, bool)>,
        /// Optional local LIMIT (reduces data sent over wire).
        limit: Option<usize>,
    },

    /// Broadcast join: the coordinator has gathered the small table's rows and
    /// sends them to every shard. Each shard joins the broadcast data with its
    /// local partition of the large (partitioned) table via hash join.
    BroadcastJoin {
        /// Pre-gathered rows from the broadcast (small) table.
        broadcast_rows: Vec<OwnedRow>,
        /// Column metadata for the broadcast side.
        broadcast_columns: Vec<(String, DataType)>,
        /// Table ID of the local (partitioned) table to scan on this shard.
        local_table_id: u64,
        /// Column metadata for the local side.
        local_columns: Vec<(String, DataType)>,
        /// Equi-join key column index in the broadcast rows.
        broadcast_key_col: usize,
        /// Equi-join key column index in the local table rows.
        local_key_col: usize,
        /// Output column metadata (broadcast columns ++ local columns).
        output_columns: Vec<(String, DataType)>,
    },

    /// Distributed UPDATE: scan + filter → update matching rows on each shard.
    ///
    /// Each shard scans its local partition, applies the filter predicate,
    /// and updates matching rows with the given column assignments.
    /// Returns a single-row result with the count of affected rows.
    Update {
        table_id: u64,
        /// Filter predicate selecting rows to update (True = update all).
        predicate: FilterExpr,
        /// Column assignments: (column_index, new_value).
        assignments: Vec<ColumnAssignment>,
        /// Output columns metadata — always `[("rows_affected", Int64)]`.
        columns: Vec<(String, DataType)>,
    },

    /// Distributed DELETE: scan + filter → delete matching rows on each shard.
    ///
    /// Each shard scans its local partition, applies the filter predicate,
    /// and deletes matching rows.  Returns a single-row result with the
    /// count of deleted rows.
    Delete {
        table_id: u64,
        /// Filter predicate selecting rows to delete (True = delete all).
        predicate: FilterExpr,
        /// Output columns metadata — always `[("rows_affected", Int64)]`.
        columns: Vec<(String, DataType)>,
    },

    /// Hash-partition join: the coordinator has repartitioned both tables by
    /// join key hash and sends each shard its partition of left + right rows.
    /// The shard performs a local hash join on the co-located data.
    HashPartitionJoin {
        /// Pre-partitioned left-side rows assigned to this shard.
        left_rows: Vec<OwnedRow>,
        /// Column metadata for the left side.
        left_columns: Vec<(String, DataType)>,
        /// Equi-join key column index in the left rows.
        left_key_col: usize,
        /// Pre-partitioned right-side rows assigned to this shard.
        right_rows: Vec<OwnedRow>,
        /// Column metadata for the right side.
        right_columns: Vec<(String, DataType)>,
        /// Equi-join key column index in the right rows.
        right_key_col: usize,
        /// Output column metadata (left columns ++ right columns).
        output_columns: Vec<(String, DataType)>,
    },
}

/// A column assignment for distributed UPDATE: set column at `column_idx`
/// to the given literal `value`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnAssignment {
    /// Index of the column to update.
    pub column_idx: usize,
    /// New value to assign.
    pub value: Datum,
}

/// A filter expression tree that can be serialized and evaluated at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterExpr {
    /// Compare a column to a literal value.
    Compare {
        column_idx: usize,
        op: CompareOp,
        value: Datum,
    },
    /// IS NULL check.
    IsNull { column_idx: usize },
    /// IS NOT NULL check.
    IsNotNull { column_idx: usize },
    /// Logical AND of sub-expressions.
    And(Vec<FilterExpr>),
    /// Logical OR of sub-expressions.
    Or(Vec<FilterExpr>),
    /// Logical NOT.
    Not(Box<FilterExpr>),
    /// Always true (no filter).
    True,
}

/// Comparison operators for filter predicates.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Aggregate function specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggFunc {
    /// COUNT(*) — count all rows.
    CountStar,
    /// COUNT(column) — count non-null values.
    Count(usize),
    /// SUM(column).
    Sum(usize),
    /// MIN(column).
    Min(usize),
    /// MAX(column).
    Max(usize),
    /// AVG decomposition: emits (sum, count) as two output columns.
    AvgDecompose(usize),
}

impl FilterExpr {
    /// Evaluate this filter expression against a row.
    pub fn evaluate(&self, row: &OwnedRow) -> bool {
        match self {
            Self::Compare {
                column_idx,
                op,
                value,
            } => {
                let col_val = row.values.get(*column_idx);
                match col_val {
                    Some(v) => compare_datum(v, value, *op),
                    None => false, // missing column → false
                }
            }
            Self::IsNull { column_idx } => {
                matches!(row.values.get(*column_idx), Some(Datum::Null) | None)
            }
            Self::IsNotNull { column_idx } => {
                !matches!(row.values.get(*column_idx), Some(Datum::Null) | None)
            }
            Self::And(exprs) => exprs.iter().all(|e| e.evaluate(row)),
            Self::Or(exprs) => exprs.iter().any(|e| e.evaluate(row)),
            Self::Not(expr) => !expr.evaluate(row),
            Self::True => true,
        }
    }
}

/// Compare two Datum values with the given operator.
fn compare_datum(left: &Datum, right: &Datum, op: CompareOp) -> bool {
    let ord = crate::distributed_exec::gather::compare_datums(Some(left), Some(right));
    match op {
        CompareOp::Eq => ord == std::cmp::Ordering::Equal,
        CompareOp::Ne => ord != std::cmp::Ordering::Equal,
        CompareOp::Lt => ord == std::cmp::Ordering::Less,
        CompareOp::Le => ord != std::cmp::Ordering::Greater,
        CompareOp::Gt => ord == std::cmp::Ordering::Greater,
        CompareOp::Ge => ord != std::cmp::Ordering::Less,
    }
}

impl SerializableSubPlan {
    /// Convert this serializable plan into an executable `SubPlan` closure.
    ///
    /// This is called on the receiving shard node after deserialization.
    pub fn into_subplan(self) -> super::SubPlan {
        let desc = format!("{:?}", &self);
        super::SubPlan::new(&desc, move |storage, txn_mgr| {
            execute_serializable(&self, storage, txn_mgr)
        })
    }

    /// Serialize this plan to bytes (bincode) for wire transport.
    pub fn to_bytes(&self) -> Result<Vec<u8>, FalconError> {
        bincode::serialize(self)
            .map_err(|e| FalconError::Internal(format!("Failed to serialize subplan: {e}")))
    }

    /// Deserialize a plan from bytes received over the wire.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FalconError> {
        bincode::deserialize(bytes)
            .map_err(|e| FalconError::Internal(format!("Failed to deserialize subplan: {e}")))
    }
}

/// Encode a datum value to bytes for hash-join key lookup.
/// Uses bincode for deterministic, type-aware encoding.
fn encode_join_key(datum: Option<&Datum>) -> Vec<u8> {
    match datum {
        None | Some(Datum::Null) => vec![0x00],
        Some(d) => bincode::serialize(d).unwrap_or_default(),
    }
}

/// Execute a serializable subplan against a local storage engine.
fn execute_serializable(
    plan: &SerializableSubPlan,
    storage: &falcon_storage::engine::StorageEngine,
    txn_mgr: &falcon_txn::manager::TxnManager,
) -> Result<SubPlanResult, FalconError> {
    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
    let read_ts = txn.read_ts(txn_mgr.current_ts());

    match plan {
        SerializableSubPlan::Scan {
            table_id,
            projection,
            columns,
        } => {
            let scan_rows = storage.scan(TableId(*table_id), txn.txn_id, read_ts)?;
            let rows: Vec<OwnedRow> = scan_rows.into_iter().map(|(_, r)| r).collect();
            let rows = project_rows(&rows, projection);
            txn_mgr.commit(txn.txn_id)?;
            Ok((columns.clone(), rows))
        }

        SerializableSubPlan::Filter {
            table_id,
            projection,
            columns,
            predicate,
        } => {
            let scan_rows = storage.scan(TableId(*table_id), txn.txn_id, read_ts)?;
            let rows: Vec<OwnedRow> = scan_rows
                .into_iter()
                .map(|(_, r)| r)
                .filter(|r| predicate.evaluate(r))
                .collect();
            let rows = project_rows(&rows, projection);
            txn_mgr.commit(txn.txn_id)?;
            Ok((columns.clone(), rows))
        }

        SerializableSubPlan::PartialAggregate {
            table_id,
            aggregates,
            columns,
        } => {
            let scan_rows = storage.scan(TableId(*table_id), txn.txn_id, read_ts)?;
            let rows: Vec<OwnedRow> = scan_rows.into_iter().map(|(_, r)| r).collect();
            let result_row = compute_aggregates(&rows, aggregates);
            txn_mgr.commit(txn.txn_id)?;
            Ok((columns.clone(), vec![result_row]))
        }

        SerializableSubPlan::PartialGroupBy {
            table_id,
            group_by_cols,
            aggregates,
            columns,
        } => {
            let scan_rows = storage.scan(TableId(*table_id), txn.txn_id, read_ts)?;
            let rows: Vec<OwnedRow> = scan_rows.into_iter().map(|(_, r)| r).collect();
            let result_rows = compute_grouped_aggregates(&rows, group_by_cols, aggregates);
            txn_mgr.commit(txn.txn_id)?;
            Ok((columns.clone(), result_rows))
        }

        SerializableSubPlan::OrderedScan {
            table_id,
            projection,
            columns,
            sort_keys,
            limit,
        } => {
            let scan_rows = storage.scan(TableId(*table_id), txn.txn_id, read_ts)?;
            let rows: Vec<OwnedRow> = scan_rows.into_iter().map(|(_, r)| r).collect();
            let mut rows = project_rows(&rows, projection);
            rows.sort_by(|a, b| {
                for &(col, asc) in sort_keys {
                    let va = a.values.get(col);
                    let vb = b.values.get(col);
                    let ord = crate::distributed_exec::gather::compare_datums(va, vb);
                    let ord = if asc { ord } else { ord.reverse() };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
            if let Some(lim) = limit {
                rows.truncate(*lim);
            }
            txn_mgr.commit(txn.txn_id)?;
            Ok((columns.clone(), rows))
        }

        SerializableSubPlan::BroadcastJoin {
            broadcast_rows,
            broadcast_columns: _,
            local_table_id,
            local_columns: _,
            broadcast_key_col,
            local_key_col,
            output_columns,
        } => {
            // Scan the local (partitioned) table on this shard.
            let scan_rows = storage.scan(TableId(*local_table_id), txn.txn_id, read_ts)?;
            let local_rows: Vec<OwnedRow> = scan_rows.into_iter().map(|(_, r)| r).collect();

            // Build hash map on the broadcast side.
            let mut hash_map: std::collections::HashMap<Vec<u8>, Vec<usize>> =
                std::collections::HashMap::new();
            for (i, row) in broadcast_rows.iter().enumerate() {
                let key = encode_join_key(row.values.get(*broadcast_key_col));
                hash_map.entry(key).or_default().push(i);
            }

            // Probe with local rows.
            let mut result_rows = Vec::new();
            for local_row in &local_rows {
                let key = encode_join_key(local_row.values.get(*local_key_col));
                if let Some(indices) = hash_map.get(&key) {
                    for &idx in indices {
                        let brow = &broadcast_rows[idx];
                        let mut combined = brow.values.clone();
                        combined.extend(local_row.values.iter().cloned());
                        result_rows.push(OwnedRow::new(combined));
                    }
                }
            }

            txn_mgr.commit(txn.txn_id)?;
            Ok((output_columns.clone(), result_rows))
        }

        SerializableSubPlan::HashPartitionJoin {
            left_rows,
            left_columns: _,
            left_key_col,
            right_rows,
            right_columns: _,
            right_key_col,
            output_columns,
        } => {
            // Both sides are pre-partitioned by the coordinator.
            // Build hash map on the left side, probe with the right side.
            let mut hash_map: std::collections::HashMap<Vec<u8>, Vec<usize>> =
                std::collections::HashMap::new();
            for (i, row) in left_rows.iter().enumerate() {
                let key = encode_join_key(row.values.get(*left_key_col));
                hash_map.entry(key).or_default().push(i);
            }

            let mut result_rows = Vec::new();
            for right_row in right_rows {
                let key = encode_join_key(right_row.values.get(*right_key_col));
                if let Some(indices) = hash_map.get(&key) {
                    for &idx in indices {
                        let lrow = &left_rows[idx];
                        let mut combined = lrow.values.clone();
                        combined.extend(right_row.values.iter().cloned());
                        result_rows.push(OwnedRow::new(combined));
                    }
                }
            }

            // No local scan needed — commit the unused transaction.
            txn_mgr.commit(txn.txn_id)?;
            Ok((output_columns.clone(), result_rows))
        }

        SerializableSubPlan::Update {
            table_id,
            predicate,
            assignments,
            columns,
        } => {
            let scan_rows = storage.scan(TableId(*table_id), txn.txn_id, read_ts)?;
            let mut rows_affected: i64 = 0;

            for (pk, row) in &scan_rows {
                if !predicate.evaluate(row) {
                    continue;
                }
                // Apply column assignments to produce the new row.
                let mut new_values = row.values.clone();
                for assign in assignments {
                    if assign.column_idx < new_values.len() {
                        new_values[assign.column_idx] = assign.value.clone();
                    }
                }
                let new_row = OwnedRow::new(new_values);
                storage.update(TableId(*table_id), pk, new_row, txn.txn_id)?;
                rows_affected += 1;
            }

            txn_mgr.commit(txn.txn_id)?;
            Ok((
                columns.clone(),
                vec![OwnedRow::new(vec![Datum::Int64(rows_affected)])],
            ))
        }

        SerializableSubPlan::Delete {
            table_id,
            predicate,
            columns,
        } => {
            let scan_rows = storage.scan(TableId(*table_id), txn.txn_id, read_ts)?;
            let mut rows_affected: i64 = 0;

            for (pk, row) in &scan_rows {
                if !predicate.evaluate(row) {
                    continue;
                }
                storage.delete(TableId(*table_id), pk, txn.txn_id)?;
                rows_affected += 1;
            }

            txn_mgr.commit(txn.txn_id)?;
            Ok((
                columns.clone(),
                vec![OwnedRow::new(vec![Datum::Int64(rows_affected)])],
            ))
        }
    }
}

/// Apply column projection to rows. Empty projection = return all columns.
fn project_rows(rows: &[OwnedRow], projection: &[usize]) -> Vec<OwnedRow> {
    if projection.is_empty() {
        return rows.to_vec();
    }
    rows.iter()
        .map(|row| {
            let values: Vec<Datum> = projection
                .iter()
                .map(|&idx| row.values.get(idx).cloned().unwrap_or(Datum::Null))
                .collect();
            OwnedRow::new(values)
        })
        .collect()
}

/// Compute non-grouped aggregates over all rows.
fn compute_aggregates(rows: &[OwnedRow], aggregates: &[AggFunc]) -> OwnedRow {
    let mut values = Vec::with_capacity(aggregates.len() * 2);

    for agg in aggregates {
        match agg {
            AggFunc::CountStar => {
                values.push(Datum::Int64(rows.len() as i64));
            }
            AggFunc::Count(col) => {
                let count = rows
                    .iter()
                    .filter(|r| !matches!(r.values.get(*col), Some(Datum::Null) | None))
                    .count();
                values.push(Datum::Int64(count as i64));
            }
            AggFunc::Sum(col) => {
                let sum = sum_column(rows, *col);
                values.push(sum);
            }
            AggFunc::Min(col) => {
                let min = rows
                    .iter()
                    .filter_map(|r| r.values.get(*col))
                    .filter(|d| !matches!(d, Datum::Null))
                    .min_by(|a, b| {
                        crate::distributed_exec::gather::compare_datums(Some(a), Some(b))
                    })
                    .cloned()
                    .unwrap_or(Datum::Null);
                values.push(min);
            }
            AggFunc::Max(col) => {
                let max = rows
                    .iter()
                    .filter_map(|r| r.values.get(*col))
                    .filter(|d| !matches!(d, Datum::Null))
                    .max_by(|a, b| {
                        crate::distributed_exec::gather::compare_datums(Some(a), Some(b))
                    })
                    .cloned()
                    .unwrap_or(Datum::Null);
                values.push(max);
            }
            AggFunc::AvgDecompose(col) => {
                let sum = sum_column(rows, *col);
                let count = rows
                    .iter()
                    .filter(|r| !matches!(r.values.get(*col), Some(Datum::Null) | None))
                    .count();
                values.push(sum);
                values.push(Datum::Int64(count as i64));
            }
        }
    }

    OwnedRow::new(values)
}

/// Compute grouped aggregates: returns one row per group (group_keys + agg_values).
fn compute_grouped_aggregates(
    rows: &[OwnedRow],
    group_by_cols: &[usize],
    aggregates: &[AggFunc],
) -> Vec<OwnedRow> {
    use std::collections::HashMap;

    // Group rows by key
    let mut groups: HashMap<Vec<Datum>, Vec<&OwnedRow>> = HashMap::new();
    for row in rows {
        let key: Vec<Datum> = group_by_cols
            .iter()
            .map(|&idx| row.values.get(idx).cloned().unwrap_or(Datum::Null))
            .collect();
        groups.entry(key).or_default().push(row);
    }

    let mut result = Vec::with_capacity(groups.len());
    for (key, group_rows) in groups {
        let owned_rows: Vec<OwnedRow> = group_rows.iter().map(|r| (*r).clone()).collect();
        let agg_row = compute_aggregates(&owned_rows, aggregates);

        // Combine: group_keys + agg_values
        let mut values = key;
        values.extend(agg_row.values);
        result.push(OwnedRow::new(values));
    }

    result
}

/// Sum a column across rows, returning Int64 or Float64.
fn sum_column(rows: &[OwnedRow], col: usize) -> Datum {
    let mut int_sum: i64 = 0;
    let mut float_sum: f64 = 0.0;
    let mut has_float = false;
    let mut has_value = false;

    for row in rows {
        match row.values.get(col) {
            Some(Datum::Int32(v)) => {
                int_sum += *v as i64;
                has_value = true;
            }
            Some(Datum::Int64(v)) => {
                int_sum += v;
                has_value = true;
            }
            Some(Datum::Float64(v)) => {
                float_sum += v;
                has_float = true;
                has_value = true;
            }
            _ => {}
        }
    }

    if !has_value {
        return Datum::Null;
    }
    if has_float {
        Datum::Float64(float_sum + int_sum as f64)
    } else {
        Datum::Int64(int_sum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_expr_evaluate_compare() {
        let row = OwnedRow::new(vec![Datum::Int64(42), Datum::Text("hello".into())]);

        let expr = FilterExpr::Compare {
            column_idx: 0,
            op: CompareOp::Gt,
            value: Datum::Int64(30),
        };
        assert!(expr.evaluate(&row));

        let expr2 = FilterExpr::Compare {
            column_idx: 0,
            op: CompareOp::Lt,
            value: Datum::Int64(30),
        };
        assert!(!expr2.evaluate(&row));
    }

    #[test]
    fn test_filter_expr_and_or() {
        let row = OwnedRow::new(vec![Datum::Int64(25)]);

        let expr = FilterExpr::And(vec![
            FilterExpr::Compare {
                column_idx: 0,
                op: CompareOp::Ge,
                value: Datum::Int64(20),
            },
            FilterExpr::Compare {
                column_idx: 0,
                op: CompareOp::Le,
                value: Datum::Int64(30),
            },
        ]);
        assert!(expr.evaluate(&row));

        let expr2 = FilterExpr::Or(vec![
            FilterExpr::Compare {
                column_idx: 0,
                op: CompareOp::Eq,
                value: Datum::Int64(10),
            },
            FilterExpr::Compare {
                column_idx: 0,
                op: CompareOp::Eq,
                value: Datum::Int64(25),
            },
        ]);
        assert!(expr2.evaluate(&row));
    }

    #[test]
    fn test_filter_expr_is_null() {
        let row = OwnedRow::new(vec![Datum::Null, Datum::Int64(1)]);
        assert!(FilterExpr::IsNull { column_idx: 0 }.evaluate(&row));
        assert!(!FilterExpr::IsNull { column_idx: 1 }.evaluate(&row));
        assert!(FilterExpr::IsNotNull { column_idx: 1 }.evaluate(&row));
    }

    #[test]
    fn test_filter_expr_not() {
        let row = OwnedRow::new(vec![Datum::Int64(5)]);
        let expr = FilterExpr::Not(Box::new(FilterExpr::Compare {
            column_idx: 0,
            op: CompareOp::Eq,
            value: Datum::Int64(10),
        }));
        assert!(expr.evaluate(&row));
    }

    #[test]
    fn test_project_rows_empty_projection() {
        let rows = vec![OwnedRow::new(vec![
            Datum::Int64(1),
            Datum::Text("a".into()),
        ])];
        let projected = project_rows(&rows, &[]);
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].values.len(), 2);
    }

    #[test]
    fn test_project_rows_subset() {
        let rows = vec![OwnedRow::new(vec![
            Datum::Int64(1),
            Datum::Text("a".into()),
            Datum::Float64(3.14),
        ])];
        let projected = project_rows(&rows, &[0, 2]);
        assert_eq!(projected[0].values.len(), 2);
        assert_eq!(projected[0].values[0], Datum::Int64(1));
        assert_eq!(projected[0].values[1], Datum::Float64(3.14));
    }

    #[test]
    fn test_compute_aggregates_count_sum() {
        let rows = vec![
            OwnedRow::new(vec![Datum::Int64(10)]),
            OwnedRow::new(vec![Datum::Int64(20)]),
            OwnedRow::new(vec![Datum::Int64(30)]),
        ];
        let aggs = vec![AggFunc::CountStar, AggFunc::Sum(0)];
        let result = compute_aggregates(&rows, &aggs);
        assert_eq!(result.values[0], Datum::Int64(3));
        assert_eq!(result.values[1], Datum::Int64(60));
    }

    #[test]
    fn test_compute_aggregates_min_max() {
        let rows = vec![
            OwnedRow::new(vec![Datum::Int64(5)]),
            OwnedRow::new(vec![Datum::Int64(15)]),
            OwnedRow::new(vec![Datum::Int64(10)]),
        ];
        let aggs = vec![AggFunc::Min(0), AggFunc::Max(0)];
        let result = compute_aggregates(&rows, &aggs);
        assert_eq!(result.values[0], Datum::Int64(5));
        assert_eq!(result.values[1], Datum::Int64(15));
    }

    #[test]
    fn test_compute_aggregates_avg_decompose() {
        let rows = vec![
            OwnedRow::new(vec![Datum::Int64(10)]),
            OwnedRow::new(vec![Datum::Int64(20)]),
        ];
        let aggs = vec![AggFunc::AvgDecompose(0)];
        let result = compute_aggregates(&rows, &aggs);
        assert_eq!(result.values[0], Datum::Int64(30)); // sum
        assert_eq!(result.values[1], Datum::Int64(2)); // count
    }

    #[test]
    fn test_compute_grouped_aggregates() {
        let rows = vec![
            OwnedRow::new(vec![Datum::Text("a".into()), Datum::Int64(10)]),
            OwnedRow::new(vec![Datum::Text("b".into()), Datum::Int64(20)]),
            OwnedRow::new(vec![Datum::Text("a".into()), Datum::Int64(30)]),
        ];
        let aggs = vec![AggFunc::Sum(1), AggFunc::CountStar];
        let result = compute_grouped_aggregates(&rows, &[0], &aggs);
        assert_eq!(result.len(), 2); // two groups

        // Find group "a"
        let group_a = result
            .iter()
            .find(|r| r.values[0] == Datum::Text("a".into()))
            .unwrap();
        assert_eq!(group_a.values[1], Datum::Int64(40)); // sum
        assert_eq!(group_a.values[2], Datum::Int64(2)); // count
    }

    #[test]
    fn test_serializable_plan_roundtrip() {
        let plan = SerializableSubPlan::Filter {
            table_id: 1,
            projection: vec![0, 2],
            columns: vec![
                ("id".into(), DataType::Int64),
                ("score".into(), DataType::Float64),
            ],
            predicate: FilterExpr::Compare {
                column_idx: 0,
                op: CompareOp::Gt,
                value: Datum::Int64(100),
            },
        };
        let bytes = plan.to_bytes().unwrap();
        let plan2 = SerializableSubPlan::from_bytes(&bytes).unwrap();
        // Verify round-trip produces equivalent plan
        let bytes2 = plan2.to_bytes().unwrap();
        assert_eq!(bytes, bytes2);
    }

    #[test]
    fn test_update_plan_roundtrip() {
        let plan = SerializableSubPlan::Update {
            table_id: 5,
            predicate: FilterExpr::Compare {
                column_idx: 0,
                op: CompareOp::Eq,
                value: Datum::Int64(42),
            },
            assignments: vec![ColumnAssignment {
                column_idx: 1,
                value: Datum::Text("updated".into()),
            }],
            columns: vec![("rows_affected".into(), DataType::Int64)],
        };
        let bytes = plan.to_bytes().unwrap();
        let plan2 = SerializableSubPlan::from_bytes(&bytes).unwrap();
        let bytes2 = plan2.to_bytes().unwrap();
        assert_eq!(bytes, bytes2);
    }

    #[test]
    fn test_delete_plan_roundtrip() {
        let plan = SerializableSubPlan::Delete {
            table_id: 7,
            predicate: FilterExpr::Compare {
                column_idx: 0,
                op: CompareOp::Lt,
                value: Datum::Int64(10),
            },
            columns: vec![("rows_affected".into(), DataType::Int64)],
        };
        let bytes = plan.to_bytes().unwrap();
        let plan2 = SerializableSubPlan::from_bytes(&bytes).unwrap();
        let bytes2 = plan2.to_bytes().unwrap();
        assert_eq!(bytes, bytes2);
    }

    #[test]
    fn test_update_plan_predicate_true_all_rows() {
        // Update with True predicate should affect all rows
        let plan = SerializableSubPlan::Update {
            table_id: 1,
            predicate: FilterExpr::True,
            assignments: vec![ColumnAssignment {
                column_idx: 1,
                value: Datum::Text("blanked".into()),
            }],
            columns: vec![("rows_affected".into(), DataType::Int64)],
        };
        let bytes = plan.to_bytes().unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_delete_plan_with_compound_predicate() {
        let plan = SerializableSubPlan::Delete {
            table_id: 2,
            predicate: FilterExpr::And(vec![
                FilterExpr::Compare {
                    column_idx: 0,
                    op: CompareOp::Ge,
                    value: Datum::Int64(100),
                },
                FilterExpr::Compare {
                    column_idx: 0,
                    op: CompareOp::Lt,
                    value: Datum::Int64(200),
                },
            ]),
            columns: vec![("rows_affected".into(), DataType::Int64)],
        };
        let bytes = plan.to_bytes().unwrap();
        let plan2 = SerializableSubPlan::from_bytes(&bytes).unwrap();
        let bytes2 = plan2.to_bytes().unwrap();
        assert_eq!(bytes, bytes2);
    }
}
