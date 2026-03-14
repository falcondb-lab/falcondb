use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use falcon_sql_frontend::parse_sql;

fn bench_parse_select_simple(c: &mut Criterion) {
    let sql = "SELECT id, name, email FROM users WHERE id = 42";
    c.bench_function("parse/select_simple", |b| {
        b.iter(|| parse_sql(black_box(sql)).unwrap())
    });
}

fn bench_parse_select_join(c: &mut Criterion) {
    let sql = "SELECT o.id, c.name, o.total \
               FROM orders o \
               JOIN customers c ON o.customer_id = c.id \
               WHERE o.total > 100 AND c.active = true \
               ORDER BY o.total DESC LIMIT 50";
    c.bench_function("parse/select_join", |b| {
        b.iter(|| parse_sql(black_box(sql)).unwrap())
    });
}

fn bench_parse_insert(c: &mut Criterion) {
    let sql = "INSERT INTO users (id, name, email, created_at) VALUES (1, 'Alice', 'a@b.com', '2024-01-01')";
    c.bench_function("parse/insert", |b| {
        b.iter(|| parse_sql(black_box(sql)).unwrap())
    });
}

fn bench_parse_update(c: &mut Criterion) {
    let sql =
        "UPDATE accounts SET balance = balance - 100 WHERE account_id = 42 AND balance >= 100";
    c.bench_function("parse/update", |b| {
        b.iter(|| parse_sql(black_box(sql)).unwrap())
    });
}

fn bench_parse_complex_subquery(c: &mut Criterion) {
    let sql = "SELECT d.name, sub.avg_salary \
               FROM departments d \
               JOIN (SELECT department_id, AVG(salary) AS avg_salary \
                     FROM employees GROUP BY department_id HAVING AVG(salary) > 50000) sub \
               ON d.id = sub.department_id \
               ORDER BY sub.avg_salary DESC";
    c.bench_function("parse/complex_subquery", |b| {
        b.iter(|| parse_sql(black_box(sql)).unwrap())
    });
}

fn bench_parse_create_table(c: &mut Criterion) {
    let sql = "CREATE TABLE orders (\
                 id BIGINT PRIMARY KEY, \
                 customer_id BIGINT NOT NULL, \
                 total FLOAT8 NOT NULL DEFAULT 0.0, \
                 status TEXT NOT NULL DEFAULT 'pending', \
                 created_at TIMESTAMP NOT NULL DEFAULT NOW(), \
                 CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id))";
    c.bench_function("parse/create_table", |b| {
        b.iter(|| parse_sql(black_box(sql)).unwrap())
    });
}

fn bench_parse_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse/select_columns_scaling");
    for ncols in [1, 5, 10, 25, 50] {
        let cols: Vec<String> = (0..ncols).map(|i| format!("col_{i}")).collect();
        let sql = format!("SELECT {} FROM wide_table WHERE id = 1", cols.join(", "));
        group.bench_with_input(BenchmarkId::from_parameter(ncols), &sql, |b, sql| {
            b.iter(|| parse_sql(black_box(sql)).unwrap())
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_parse_select_simple,
    bench_parse_select_join,
    bench_parse_insert,
    bench_parse_update,
    bench_parse_complex_subquery,
    bench_parse_create_table,
    bench_parse_scaling,
);
criterion_main!(benches);
