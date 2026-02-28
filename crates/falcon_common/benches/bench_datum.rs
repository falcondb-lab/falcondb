use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use falcon_common::datum::Datum;
use falcon_common::error::{
    ClusterError, ExecutionError, FalconError, ProtocolError, SqlError, StorageError, TxnError,
};
use falcon_common::types::TxnId;

// ── Datum clone ────────────────────────────────────────────────────────────

fn bench_datum_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("datum/clone");
    let cases: Vec<(&str, Datum)> = vec![
        ("null", Datum::Null),
        ("bool", Datum::Boolean(true)),
        ("int32", Datum::Int32(42)),
        ("int64", Datum::Int64(123_456_789)),
        ("float64", Datum::Float64(3.14159)),
        ("text_short", Datum::Text("hello".into())),
        ("text_100", Datum::Text("x".repeat(100))),
        ("timestamp", Datum::Timestamp(1_700_000_000_000_000)),
    ];
    for (name, datum) in &cases {
        group.bench_with_input(BenchmarkId::new("type", name), datum, |b, d| {
            b.iter(|| black_box(d.clone()))
        });
    }
    group.finish();
}

// ── Datum Display ──────────────────────────────────────────────────────────

fn bench_datum_display(c: &mut Criterion) {
    let mut group = c.benchmark_group("datum/display");
    let cases: Vec<(&str, Datum)> = vec![
        ("int64", Datum::Int64(42)),
        ("float64", Datum::Float64(3.14159)),
        ("text", Datum::Text("hello world".into())),
        ("bool", Datum::Boolean(true)),
    ];
    for (name, datum) in &cases {
        group.bench_with_input(BenchmarkId::new("type", name), datum, |b, d| {
            b.iter(|| black_box(format!("{d}")))
        });
    }
    group.finish();
}

// ── Datum to_pg_text ───────────────────────────────────────────────────────

fn bench_datum_to_pg_text(c: &mut Criterion) {
    let mut group = c.benchmark_group("datum/to_pg_text");
    let cases: Vec<(&str, Datum)> = vec![
        ("int32", Datum::Int32(42)),
        ("int64", Datum::Int64(123_456_789)),
        ("float64", Datum::Float64(3.14159)),
        ("text", Datum::Text("hello".into())),
        ("bool", Datum::Boolean(false)),
        ("null", Datum::Null),
    ];
    for (name, datum) in &cases {
        group.bench_with_input(BenchmarkId::new("type", name), datum, |b, d| {
            b.iter(|| black_box(d.to_pg_text()))
        });
    }
    group.finish();
}

// ── Datum arithmetic (SUM path) ────────────────────────────────────────────

fn bench_datum_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("datum/add");
    let pairs: Vec<(&str, Datum, Datum)> = vec![
        ("int64+int64", Datum::Int64(100), Datum::Int64(200)),
        ("float64+float64", Datum::Float64(1.5), Datum::Float64(2.5)),
        ("int32+int32", Datum::Int32(10), Datum::Int32(20)),
        (
            "decimal+decimal",
            Datum::Decimal(12345, 2),
            Datum::Decimal(67890, 2),
        ),
    ];
    for (name, a, b) in &pairs {
        group.bench_with_input(BenchmarkId::new("pair", name), &(a, b), |bench, (a, b)| {
            bench.iter(|| black_box(a.add(b)))
        });
    }
    group.finish();
}

// ── Datum comparison (PartialOrd, used in ORDER BY) ────────────────────────

fn bench_datum_cmp(c: &mut Criterion) {
    let mut group = c.benchmark_group("datum/cmp");
    let pairs: Vec<(&str, Datum, Datum)> = vec![
        ("int64", Datum::Int64(100), Datum::Int64(200)),
        ("text", Datum::Text("abc".into()), Datum::Text("def".into())),
        ("float64", Datum::Float64(1.0), Datum::Float64(2.0)),
    ];
    for (name, a, b) in &pairs {
        group.bench_with_input(BenchmarkId::new("pair", name), &(a, b), |bench, (a, b)| {
            bench.iter(|| black_box(a.partial_cmp(b)))
        });
    }
    group.finish();
}

// ── Error classification (hot path: every error response) ──────────────────

fn bench_error_kind(c: &mut Criterion) {
    let errors: Vec<(&str, FalconError)> = vec![
        (
            "sql_parse",
            FalconError::Sql(SqlError::Parse("bad".into())),
        ),
        (
            "write_conflict",
            FalconError::Txn(TxnError::WriteConflict(TxnId(1))),
        ),
        ("timeout", FalconError::Txn(TxnError::Timeout)),
        ("not_leader", FalconError::Cluster(ClusterError::NotLeader)),
        (
            "div_zero",
            FalconError::Execution(ExecutionError::DivisionByZero),
        ),
        (
            "auth_failed",
            FalconError::Protocol(ProtocolError::AuthFailed),
        ),
    ];

    let mut group = c.benchmark_group("error/kind");
    for (name, err) in &errors {
        group.bench_with_input(BenchmarkId::new("variant", name), err, |b, e| {
            b.iter(|| black_box(e.kind()))
        });
    }
    group.finish();
}

fn bench_error_sqlstate(c: &mut Criterion) {
    let errors: Vec<(&str, FalconError)> = vec![
        (
            "sql_parse",
            FalconError::Sql(SqlError::Parse("x".into())),
        ),
        (
            "unique_violation",
            FalconError::Storage(StorageError::DuplicateKey),
        ),
        (
            "serialization",
            FalconError::Txn(TxnError::WriteConflict(TxnId(1))),
        ),
        (
            "auth_failed",
            FalconError::Protocol(ProtocolError::AuthFailed),
        ),
    ];

    let mut group = c.benchmark_group("error/pg_sqlstate");
    for (name, err) in &errors {
        group.bench_with_input(BenchmarkId::new("variant", name), err, |b, e| {
            b.iter(|| black_box(e.pg_sqlstate()))
        });
    }
    group.finish();
}

// ── Datum::parse_decimal ───────────────────────────────────────────────────

fn bench_parse_decimal(c: &mut Criterion) {
    let mut group = c.benchmark_group("datum/parse_decimal");
    let inputs = vec![
        ("short", "123.45"),
        ("long", "99999999999999.99"),
        ("no_frac", "42"),
        ("negative", "-0.001"),
    ];
    for (name, s) in &inputs {
        group.bench_with_input(BenchmarkId::new("input", name), s, |b, s| {
            b.iter(|| black_box(Datum::parse_decimal(s)))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_datum_clone,
    bench_datum_display,
    bench_datum_to_pg_text,
    bench_datum_add,
    bench_datum_cmp,
    bench_error_kind,
    bench_error_sqlstate,
    bench_parse_decimal,
);
criterion_main!(benches);
