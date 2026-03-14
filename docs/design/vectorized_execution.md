# Vectorized Execution Engine — Design Roadmap

## Status: **In Progress** (Phase 1 complete, Phase 2 partially implemented)

## Implemented

- **Columnstore GROUP BY AGG pushdown** (`exec_columnar_group_agg` in
  `executor_columnar.rs`): vectorized hash aggregation on ColumnStore
  `RecordBatch` with zone-map filter pushdown. Calls `vectorized_hash_agg()`
  for pure column-vector GROUP BY (SUM, COUNT, MIN, MAX). Supports ORDER BY /
  LIMIT / OFFSET / DISTINCT post-processing. 5 unit tests.
- **Fused streaming aggregates** (`exec_fused_aggregate`): single-pass
  WHERE+GROUP BY+aggregate over MVCC chains without row materialization.
- **Cost-based optimizer** feeds statistics to the planner for scan/join strategy
  selection (see `falcon_planner/src/cost.rs`).

## Motivation

FalconDB's current row-at-a-time executor (Volcano-style iterator model)
handles OLTP workloads efficiently. For analytical queries, the columnstore
AGG pushdown path provides vectorized execution on columnar data. Further
vectorization of the row-store path is planned.

**Measured gap**: ~4.3× slower than PostgreSQL on analytical workloads
(aggregation, hash join, sort-heavy queries). OLTP point-lookup performance
is on par or better thanks to the in-memory row store.

## Goal

Introduce a **columnar batch execution layer** that processes rows in
vectors (typically 1024 rows per batch) through each operator, improving:

- **CPU cache utilization**: columnar layout keeps same-column data contiguous
- **SIMD opportunities**: batch operations on typed arrays (filter, hash, compare)
- **Branch prediction**: tight typed loops instead of per-row virtual dispatch
- **Function call overhead**: amortized across batch instead of per-row

## Design

### Phase 1: Batch Infrastructure (v1.4)

1. **`VectorBatch`** struct: fixed-width columnar batch with null bitmap
   ```
   struct VectorBatch {
       columns: Vec<TypedColumn>,
       len: usize,          // rows in this batch (≤ 1024)
       selection: Option<SelectionVector>,
   }
   ```
2. **`TypedColumn`** enum: `Int32(Vec<i32>)`, `Int64(Vec<i64>)`,
   `Float64(Vec<f64>)`, `Text(StringArray)`, `Bytea(Vec<Vec<u8>>)`, etc.
3. **Selection vector**: late materialization — filters produce a selection
   vector instead of copying surviving rows.

### Phase 2: Vectorized Operators (v1.5)

| Operator | Strategy |
|----------|----------|
| **SeqScan** | Emit `VectorBatch` from row store / LSM |
| **Filter** | Evaluate predicate on batch → selection vector |
| **Project** | Evaluate expressions column-at-a-time |
| **HashAgg** | Vectorized hash + aggregate accumulators |
| **HashJoin** | Vectorized probe with SIMD hash comparison |
| **Sort** | Radix sort on key columns, then merge |
| **Limit/Offset** | Truncate batch |

### Phase 3: Adaptive Execution (v1.6+)

- **Morsel-driven parallelism**: partition work into morsels, schedule across
  worker threads without explicit partitioning operators.
- **Adaptive operator selection**: fall back to row-at-a-time for operators
  where vectorization overhead exceeds benefit (e.g., single-row lookups).
- **SIMD kernels**: hand-tuned AVX2/NEON kernels for filter, hash, and
  comparison on fixed-width types.

## Non-Goals

- **Full columnar storage**: FalconDB remains a row-store-first database.
  Vectorization applies to the execution layer only.
- **Replacing the OLTP fast path**: single-shard point lookups continue
  through the existing optimized row-at-a-time path.

## Compatibility

- The vectorized engine will be **transparent to SQL clients** — same query
  syntax, same wire protocol, same results.
- `EXPLAIN ANALYZE` will show `VectorScan`, `VectorHashAgg`, etc. when the
  vectorized path is chosen.
- A session-level GUC `SET falcon.vectorize = on|off|auto` will control
  opt-in during the rollout phase.

## Performance Targets

| Workload | Current | Target (Phase 2) | Target (Phase 3) |
|----------|---------|-------------------|-------------------|
| TPC-H Q1 (aggregation) | 4.3× slower than PG | 1.5× slower | On par |
| TPC-H Q6 (filter-heavy) | 3.8× slower | 1.2× slower | Faster |
| OLTP point lookup | 1.0× (baseline) | 1.0× (unchanged) | 1.0× |
| OLTP small range scan | 1.0× | 1.1× (slight improvement) | 1.2× |

## References

- [Vectorwise/MonetDB: Vectorized execution](https://www.cidrdb.org/cidr2005/papers/P19.pdf)
- [Morsel-Driven Parallelism (HyPer)](https://db.in.tum.de/~leis/papers/morsels.pdf)
- [DuckDB execution engine](https://duckdb.org/docs/internals/vector)
- Apache Arrow columnar format (potential interop for `TypedColumn`)

## Timeline

| Milestone | Version | ETA |
|-----------|---------|-----|
| `VectorBatch` + `TypedColumn` types | v1.4.0 | Q3 2026 |
| Vectorized SeqScan + Filter + Project | v1.4.0 | Q3 2026 |
| Vectorized HashAgg + HashJoin | v1.5.0 | Q4 2026 |
| Adaptive row↔vector switching | v1.5.0 | Q4 2026 |
| SIMD kernels + morsel parallelism | v1.6.0 | Q1 2027 |
| GA with `falcon.vectorize = auto` default | v1.6.0 | Q1 2027 |
