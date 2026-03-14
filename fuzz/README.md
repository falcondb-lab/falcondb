# Fuzz Testing

This directory contains [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) targets for FalconDB.

## Prerequisites

```bash
# Requires nightly toolchain
rustup install nightly
cargo install cargo-fuzz --locked
```

## Targets

| Target | Description | Max Input |
|--------|-------------|-----------|
| `fuzz_sql_parser` | Feeds arbitrary UTF-8 strings into `falcon_sql_frontend::parser::parse_sql` | 4 KB |
| `fuzz_pg_codec` | Feeds arbitrary bytes into the PG wire protocol decoders (`decode_startup`, `decode_message`) | 64 KB |

## Running Locally

```bash
# From the repository root:

# Fuzz the SQL parser (runs indefinitely until Ctrl-C or a crash)
cargo +nightly fuzz run fuzz_sql_parser -- -max_len=4096

# Fuzz the PG wire codec
cargo +nightly fuzz run fuzz_pg_codec -- -max_len=65536

# Run with a time limit (e.g., 5 minutes)
cargo +nightly fuzz run fuzz_sql_parser -- -max_total_time=300

# Replay only the seed corpus (useful as a quick regression check)
cargo +nightly fuzz run fuzz_sql_parser -- -runs=0

# List all available targets
cargo +nightly fuzz list
```

## CI Integration

Fuzz testing is integrated into CI via `.github/workflows/fuzz.yml`:

- **PRs** (corpus regression): Replays the seed corpus through each target on every
  PR that touches `crates/falcon_sql_frontend/`, `crates/falcon_protocol_pg/`, or
  `fuzz/`. Fast (~1-2 min). Any panic = CI failure.
- **Nightly** (exploratory): Runs each target for 10 minutes to discover new crashes.
  Crash artifacts are uploaded and preserved for 90 days.
- **Manual dispatch**: Trigger from the Actions tab with a custom duration.

## Seed Corpus

Seed inputs live in `corpus/<target>/`. Adding representative inputs helps the
fuzzer explore interesting code paths faster.

**SQL parser corpus** (`corpus/fuzz_sql_parser/`):
- `select_basic.sql` — simple SELECT with WHERE/ORDER/LIMIT
- `ddl_create.sql` — CREATE TABLE with constraints and defaults
- `join_complex.sql` — LEFT JOIN with GROUP BY/HAVING
- `subquery.sql` — IN subquery + EXISTS
- `window_fn.sql` — PARTITION BY, ROWS BETWEEN
- `insert_values.sql` — multi-row INSERT
- `update_returning.sql` — UPDATE with conditional WHERE
- `delete_subquery.sql` — DELETE with NOT IN subquery
- `cte_recursive.sql` — recursive CTE
- `alter_table.sql` — ALTER TABLE ADD COLUMN
- `union_intersect.sql` — UNION ALL / INTERSECT
- `case_expression.sql` — CASE WHEN / COALESCE
- `create_index.sql` — partial unique index
- `transaction_cmds.sql` — BEGIN/SAVEPOINT/ROLLBACK TO/COMMIT
- `cast_and_types.sql` — CAST, ::, EXTRACT, BETWEEN
- `drop_truncate.sql` — DROP IF EXISTS, TRUNCATE
- `like_patterns.sql` — LIKE/ILIKE with ESCAPE
- `multi_join.sql` — INNER/LEFT/RIGHT/CROSS JOIN
- `edge_empty.sql` — empty input
- `edge_semicolons.sql` — degenerate semicolons

**PG codec corpus** (`corpus/fuzz_pg_codec/`):
- `simple_query.bin` — simple query message
- `startup_v3.bin` — v3 startup with user/database
- `parse_bind_exec.bin` — extended query protocol sequence
- `terminate.bin` — termination message
- `empty_input.bin` — zero-length input

## Reproducing Crashes

```bash
cargo +nightly fuzz run fuzz_sql_parser artifacts/fuzz_sql_parser/<crash_file>
```

Crashes are saved in `artifacts/<target>/`. File a bug with the crash input attached.

## Adding a New Fuzz Target

1. Create `fuzz_targets/<name>.rs` with a `fuzz_target!` macro
2. Add a `[[bin]]` entry in `fuzz/Cargo.toml`
3. Add seed corpus files in `corpus/<name>/`
4. Add the target name to the matrix in `.github/workflows/fuzz.yml`
