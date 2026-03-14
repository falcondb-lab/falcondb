// benches/storage/bench_oltp_scenarios.rs
//
// FalconDB OLTP capability benchmark — 5 canonical scenarios.
//
// Scenarios
// ─────────
//  1. Point Select      — single-row read by PK (hot path latency)
//  2. Insert            — single-row insert + commit (write throughput)
//  3. Update by PK      — single-row update by PK (MVCC write path)
//  4. Short Transaction — BEGIN / 3×update / COMMIT (txn overhead)
//  5. Batch Insert      — 1 000-row bulk insert in one transaction
//  6. Hot Row Update    — repeated UPDATE of one row (hot-row opt validation)
//
// Run: cargo bench --bench bench_oltp_scenarios

use std::sync::Arc;

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::schema::{ColumnDef, TableSchema};
use falcon_common::types::{DataType, IsolationLevel, Timestamp};
use falcon_storage::engine::StorageEngine;
use falcon_txn::manager::TxnManager;

// ── Schema & data helpers ────────────────────────────────────────────────────

fn oltp_schema() -> TableSchema {
    TableSchema {
        id: falcon_common::types::TableId(1),
        name: "accounts".into(),
        columns: vec![
            ColumnDef::new(
                falcon_common::types::ColumnId(0),
                "id".into(),
                DataType::Int64,
                false,
                true,
                None,
                false,
            ),
            ColumnDef::new(
                falcon_common::types::ColumnId(1),
                "balance".into(),
                DataType::Int64,
                false,
                false,
                None,
                false,
            ),
            ColumnDef::new(
                falcon_common::types::ColumnId(2),
                "name".into(),
                DataType::Text,
                true,
                false,
                None,
                false,
            ),
        ],
        primary_key_columns: vec![0],
        next_serial_values: Default::default(),
        check_constraints: vec![],
        unique_constraints: vec![],
        foreign_keys: vec![],
        storage_type: Default::default(),
        shard_key: vec![],
        sharding_policy: Default::default(),
        range_bounds: vec![],
        dynamic_defaults: Default::default(),
        generated_columns: Default::default(),
        partition_spec: None,
    }
}

#[inline]
fn make_row(id: i64, balance: i64) -> OwnedRow {
    OwnedRow::new(vec![
        Datum::Int64(id),
        Datum::Int64(balance),
        Datum::Text(format!("acct_{id:010}")),
    ])
}

/// Encode PK as bytes (single Int64 column → little-endian 8 bytes).
fn pk_bytes(id: i64) -> Vec<u8> {
    id.to_le_bytes().to_vec()
}

/// Seed the engine with `n` committed rows (ids 0..n).
fn seed_engine(n: u64) -> (Arc<StorageEngine>, TxnManager, falcon_common::types::TableId) {
    let engine = Arc::new(StorageEngine::new_in_memory());
    let txn_mgr = TxnManager::new(Arc::clone(&engine));
    let schema = oltp_schema();
    let table_id = engine.create_table(schema).expect("create_table");
    for i in 0..n {
        let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
        let _ = engine.insert(table_id, make_row(i as i64, i as i64 * 100), txn.txn_id);
        txn_mgr.commit(txn.txn_id).ok();
    }
    (engine, txn_mgr, table_id)
}

// ── Scenario 1: Point Select ─────────────────────────────────────────────────

/// Measures the end-to-end latency of a single-row read by PK.
/// Hot path: hash lookup → MVCC visibility check → return Arc<OwnedRow>.
/// Target (in-memory): P50 < 500 ns, P99 < 2 µs.
fn bench_point_select(c: &mut Criterion) {
    let (engine, txn_mgr, table_id) = seed_engine(100_000);
    let mut rng = falcon_bench::dataset::XorShift64::new(42);

    c.bench_function("oltp/point_select", |b| {
        b.iter(|| {
            let id = (rng.next() % 100_000) as i64;
            let pk = pk_bytes(id);
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let row = engine.get(
                black_box(table_id),
                black_box(&pk),
                txn.txn_id,
                txn.start_ts,
            );
            txn_mgr.commit(txn.txn_id).ok();
            black_box(row)
        })
    });
}

// ── Scenario 2: Insert ───────────────────────────────────────────────────────

/// Measures single-row INSERT + COMMIT throughput.
/// Covers: PK encoding → DashMap slot alloc → version chain init → WAL append → OCC commit.
/// Target (in-memory): > 500 K TPS.
fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("oltp/insert");

    for table_rows in [0u64, 10_000, 100_000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(table_rows),
            &table_rows,
            |b, &n| {
                let (engine, txn_mgr, table_id) = seed_engine(n);
                let mut next_id = n as i64;
                b.iter(|| {
                    let row = make_row(next_id, next_id * 100);
                    next_id += 1;
                    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
                    let r = engine.insert(black_box(table_id), black_box(row), txn.txn_id);
                    txn_mgr.commit(txn.txn_id).ok();
                    black_box(r)
                });
            },
        );
    }
    group.finish();
}

// ── Scenario 3: Update by PK ─────────────────────────────────────────────────

/// Measures single-row UPDATE by PK + COMMIT latency.
/// Covers: PK hash lookup → write conflict check → new MVCC version → WAL → commit.
/// Target (in-memory): > 300 K TPS.
fn bench_update_by_pk(c: &mut Criterion) {
    let mut group = c.benchmark_group("oltp/update_by_pk");

    for table_rows in [1_000u64, 100_000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(table_rows),
            &table_rows,
            |b, &n| {
                let (engine, txn_mgr, table_id) = seed_engine(n);
                let mut rng = falcon_bench::dataset::XorShift64::new(99);
                b.iter(|| {
                    let id = (rng.next() % n) as i64;
                    let pk = pk_bytes(id);
                    let new_row = make_row(id, rng.next() as i64 % 1_000_000);
                    let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
                    let r = engine.update(
                        black_box(table_id),
                        black_box(&pk),
                        black_box(new_row),
                        txn.txn_id,
                    );
                    txn_mgr.commit(txn.txn_id).ok();
                    black_box(r)
                });
            },
        );
    }
    group.finish();
}

// ── Scenario 4: Short Transaction (BEGIN / 3×update / COMMIT) ────────────────

/// Measures a realistic OLTP short transaction: debit A, credit B, audit log.
/// Stresses per-txn overhead: version chain × 3, single WAL flush, OCC validation.
/// Target (in-memory): > 150 K TPS.
fn bench_short_transaction(c: &mut Criterion) {
    let (engine, txn_mgr, table_id) = seed_engine(10_000);
    let mut rng = falcon_bench::dataset::XorShift64::new(7);

    c.bench_function("oltp/short_txn_3updates", |b| {
        b.iter(|| {
            let a     = (rng.next() % 10_000) as i64;
            let b_id  = (rng.next() % 10_000) as i64;
            let log   = (rng.next() % 10_000) as i64;
            let delta = (rng.next() % 1_000) as i64;

            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);

            let _ = engine.update(table_id, &pk_bytes(a),     make_row(a,     delta.wrapping_neg()), txn.txn_id);
            let _ = engine.update(table_id, &pk_bytes(b_id),  make_row(b_id,  delta),                txn.txn_id);
            let _ = engine.update(table_id, &pk_bytes(log),   make_row(log,   0),                    txn.txn_id);

            black_box(txn_mgr.commit(txn.txn_id).ok())
        })
    });
}

// ── Scenario 5: Batch Insert ─────────────────────────────────────────────────

/// Measures bulk INSERT in a single transaction.
/// Target: > 5 M rows/s (in-memory).
fn bench_batch_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("oltp/batch_insert");

    for batch_size in [100u64, 1_000, 10_000] {
        group.throughput(Throughput::Elements(batch_size));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &bs| {
                let (engine, txn_mgr, table_id) = seed_engine(0);
                let mut next_id = 0i64;
                b.iter_batched(
                    || {
                        let rows: Vec<OwnedRow> = (0..bs)
                            .map(|i| make_row(next_id + i as i64, i as i64 * 10))
                            .collect();
                        next_id += bs as i64;
                        rows
                    },
                    |rows| {
                        let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
                        for row in rows {
                            let _ = engine.insert(black_box(table_id), black_box(row), txn.txn_id);
                        }
                        black_box(txn_mgr.commit(txn.txn_id).ok())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

// ── Scenario 6: Hot Row Update ────────────────────────────────────────────────

/// Repeated UPDATE of the same single row across many sequential transactions.
/// Validates Hot Row Optimization (coalesce_hot_rows / try_in_place_update).
/// Without optimization: chain grows O(N) → read cost O(N) + GC pressure.
/// With optimization: chain stays depth-1.
fn bench_hot_row_update(c: &mut Criterion) {
    let (engine, txn_mgr, table_id) = seed_engine(1_000);
    let hot_pk = pk_bytes(0);

    c.bench_function("oltp/hot_row_update", |b| {
        b.iter(|| {
            let new_row = make_row(0, black_box(42));
            let txn = txn_mgr.begin(IsolationLevel::ReadCommitted);
            let r = engine.update(black_box(table_id), black_box(&hot_pk), black_box(new_row), txn.txn_id);
            txn_mgr.commit(txn.txn_id).ok();
            black_box(r)
        })
    });
}

// ── Criterion groups ──────────────────────────────────────────────────────────

criterion_group!(
    oltp_benches,
    bench_point_select,
    bench_insert,
    bench_update_by_pk,
    bench_short_transaction,
    bench_batch_insert,
    bench_hot_row_update,
);
criterion_main!(oltp_benches);
