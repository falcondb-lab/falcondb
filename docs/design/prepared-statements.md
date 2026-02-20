# Prepared Statements Design

## Overview

FalconDB supports parameterized SQL through two complementary mechanisms:

1. **Extended Query Protocol** — the PostgreSQL wire-protocol messages (`Parse`, `Bind`,
   `Describe`, `Execute`, `Close`, `Sync`) used by drivers such as libpq, JDBC, and
   node-postgres.
2. **SQL-level commands** — `PREPARE`, `EXECUTE`, and `DEALLOCATE` statements sent via
   the simple query protocol.

Both paths share the same session-level storage (`PreparedStatement`, `Portal`) and
reuse the planner/executor infrastructure.

## Architecture

```
Client
  │
  ├─ Simple Query ──────── handle_query() ──────────────────────┐
  │                                                              │
  ├─ PREPARE / EXECUTE ─── handle_query() → bind_params() ──────┤
  │                                                              ▼
  │                                                         Executor
  ├─ Parse ─────────────── prepare_statement()                   ▲
  │                          parse → bind → plan                 │
  ├─ Bind ──────────────── decode_param_value() ─────────────────┤
  │                          create Portal (plan + Datum[])      │
  ├─ Execute ───────────── execute_plan(plan, params)  ──────────┘
  │
  ├─ Describe ──────────── ParameterDescription + RowDescription
  ├─ Close ─────────────── drop statement or portal
  └─ Sync ──────────────── ReadyForQuery
```

## Key Data Structures

### PreparedStatement (`falcon_protocol_pg::session`)

| Field                | Type                              | Description                                    |
|----------------------|-----------------------------------|------------------------------------------------|
| `query`              | `String`                          | Original SQL text                              |
| `param_types`        | `Vec<i32>`                        | Effective parameter OIDs (client or inferred)  |
| `plan`               | `Option<PhysicalPlan>`            | Pre-planned physical plan (`None` → legacy)    |
| `inferred_param_types` | `Vec<Option<DataType>>`         | Types inferred by the binder                   |
| `row_desc`           | `Vec<FieldDescriptionCompact>`    | Cached column metadata for Describe            |

### Portal (`falcon_protocol_pg::session`)

| Field       | Type                   | Description                                  |
|-------------|------------------------|----------------------------------------------|
| `plan`      | `Option<PhysicalPlan>` | Plan from the parent PreparedStatement       |
| `params`    | `Vec<Datum>`           | Concrete parameter values after decoding     |
| `bound_sql` | `String`               | Text-substituted SQL (legacy fallback)       |

Both are stored in per-session `HashMap`s inside `PgSession`:

- `prepared_statements: HashMap<String, PreparedStatement>`
- `portals: HashMap<String, Portal>`

## Execution Paths

### Plan-based path (preferred)

1. **Parse** — `prepare_statement()` parses the SQL, runs the binder with
   `bind_with_params_lenient`, and produces a `PhysicalPlan`. Parameter types are
   inferred from schema context (e.g., `WHERE id = $1` infers `Int64` if `id` is
   `BIGINT`).
2. **Bind** — Parameter byte arrays are decoded via `decode_param_value()` into typed
   `Datum` values using the inferred type hints. A `Portal` is created with the plan
   and concrete parameters.
3. **Execute** — `execute_plan()` calls `executor.execute_with_params(plan, txn, params)`
   which substitutes `Datum` values directly into the physical plan, avoiding SQL
   re-parsing.

### Legacy text-substitution path (fallback)

If `prepare_statement()` fails (e.g., unsupported syntax), the statement is stored
with `plan: None`. At Bind time, `bind_params()` performs textual `$1`/`$2`/…
substitution into the original SQL string. At Execute time, the bound SQL is sent
through `handle_query()` — the same path as the simple query protocol.

### SQL-level PREPARE / EXECUTE

`PREPARE name AS query` stores a `PreparedStatement` with `plan: None` (no upfront
planning). `EXECUTE name(params)` calls `bind_params()` to produce a concrete SQL
string and routes it through `handle_query()`. This always uses the legacy path.

## Parameter Type Inference

The binder (`bind_with_params_lenient`) infers parameter types from their usage context:

- **Column comparisons** — `WHERE col = $1` → type of `col`
- **INSERT values** — `INSERT INTO t(col) VALUES ($1)` → type of `col`
- **Expressions** — `$1 + 1` → numeric type from context

Inferred types are returned as `Vec<Option<DataType>>`. `None` entries indicate
parameters whose type could not be determined; these fall back to text decoding at
Bind time.

## Parameter Value Decoding

`decode_param_value()` converts raw PG wire bytes into `Datum` using the inferred
type hint:

| Hint          | Parsing strategy               | Fallback       |
|---------------|--------------------------------|----------------|
| `Int32`       | `parse::<i32>()`               | `Datum::Text`  |
| `Int64`       | `parse::<i64>()`               | `Datum::Text`  |
| `Float64`     | `parse::<f64>()`               | `Datum::Text`  |
| `Boolean`     | t/true/1/yes/on → true, etc.   | `Datum::Text`  |
| `Text`        | direct string                  | —              |
| `Timestamp`   | stored as text                 | —              |
| `Date`        | stored as text                 | —              |
| `None`        | try i64 → f64 → text           | —              |

## Prometheus Metrics

All metrics are defined in `falcon_observability` and recorded in
`falcon_protocol_pg::server`.

### Counters

| Metric                              | Labels            | Description                                   |
|-------------------------------------|-------------------|-----------------------------------------------|
| `falcon_prepared_stmt_ops_total`     | `op`, `path`      | Extended protocol operations (parse, bind, execute, describe, close) |
| `falcon_prepared_stmt_sql_cmds_total`| `cmd`             | SQL-level commands (prepare, execute, deallocate) |

### Histograms

| Metric                                    | Labels    | Description                           |
|-------------------------------------------|-----------|---------------------------------------|
| `falcon_prepared_stmt_parse_duration_us`   | `success` | Parse (prepare_statement) latency     |
| `falcon_prepared_stmt_bind_duration_us`    | —         | Bind operation latency                |
| `falcon_prepared_stmt_execute_duration_us` | `success` | Execute operation latency             |
| `falcon_prepared_stmt_param_count`         | —         | Number of parameters per Bind         |

### Gauges

| Metric                              | Description                              |
|--------------------------------------|------------------------------------------|
| `falcon_prepared_stmt_active`         | Current prepared statements in session   |
| `falcon_prepared_stmt_portals_active` | Current portals in session               |

### Label values

- **`op`**: `parse`, `bind`, `execute`, `describe`, `close`
- **`path`**: `plan` (plan-based), `legacy` (text-substitution), `statement`/`portal` (for describe/close), `empty` (no-op execute)
- **`cmd`**: `prepare`, `execute`, `deallocate`
- **`success`**: `true`, `false`

## Lifecycle Example

```
Client                          Server
  │                               │
  ├── Parse(S1, "SELECT $1")  ──→ prepare_statement() → PhysicalPlan
  │                               store PreparedStatement{plan: Some(...)}
  │                               record: parse_duration, ops_total(parse,plan)
  │                               ◄── ParseComplete
  │
  ├── Bind(P1, S1, [42])     ──→ decode_param_value(42, Int64) → Datum::Int64
  │                               store Portal{plan, params: [Int64(42)]}
  │                               record: bind_duration, param_count, ops_total(bind,plan)
  │                               ◄── BindComplete
  │
  ├── Execute(P1)             ──→ execute_plan(plan, [Int64(42)])
  │                               record: execute_duration, ops_total(execute,plan)
  │                               ◄── DataRow / CommandComplete
  │
  ├── Sync                    ──→ ◄── ReadyForQuery
  │
  ├── Close(S, S1)            ──→ remove PreparedStatement
  │                               record: ops_total(close,statement)
  │                               ◄── CloseComplete
```

## Limitations

- **No plan caching across executions** — each Parse re-plans the query. A future
  plan cache (`falcon_protocol_pg::plan_cache`) is in progress.
- **SQL-level EXECUTE always uses legacy path** — parameters are text-substituted
  rather than passed as typed `Datum` values.
- **Timestamp/Date/Array/Jsonb parameters** are stored as text and rely on the
  executor's implicit casting.
- **Gauges are per-session** — in a multi-connection environment, the Prometheus
  gauge reflects the last session that recorded, not a global aggregate. Use the
  counter metrics for aggregate analysis.
