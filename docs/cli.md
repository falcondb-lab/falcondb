# fsql — FalconDB CLI Client

**Current version: v0.2**

`fsql` is a lightweight, psql-compatible interactive SQL client for FalconDB.
It connects via the PostgreSQL wire protocol and supports interactive REPL,
single-command execution, and SQL file execution.

---

## v0.1 vs v0.2 Feature Comparison

| Feature | v0.1 | v0.2 |
|---------|------|------|
| Interactive REPL | ✅ | ✅ |
| `-c` single command | ✅ | ✅ |
| `-f` file execution | ✅ | ✅ |
| Meta-commands (`\q \dt \d \l \c \x`) | ✅ | ✅ |
| Table / CSV / JSON / expanded output | ✅ | ✅ |
| Persistent command history (`~/.fsql_history`) | ❌ | ✅ |
| Tab completion (SQL keywords + meta-commands) | ❌ | ✅ |
| Improved prompt (`user@host:port/db`) | ❌ | ✅ |
| Execution timing (`\timing`) | ❌ | ✅ |
| Pager support (`$PAGER` / `less`) | ❌ | ✅ |
| Improved expanded output (aligned, NULL explicit) | ❌ | ✅ |

---

## Installation

Build from source (requires Rust 1.75+):

```bash
cargo build --release -p falcon_cli
# Binary: target/release/fsql
```

Or install directly:

```bash
cargo install --path crates/falcon_cli
```

---

## Connection Options

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `-h, --host` | `PGHOST` | `localhost` | Database host |
| `-p, --port` | `PGPORT` | `5432` | Database port |
| `-U, --user` | `PGUSER` | `falcon` | Database user |
| `-d, --dbname` | `PGDATABASE` | `falcon` | Database name |
| `-W, --password` | `PGPASSWORD` | _(prompt)_ | Password |
| `--sslmode` | — | `prefer` | SSL mode (disable/prefer/require) |

Command-line flags override environment variables.

---

## Execution Modes

### Interactive REPL

```bash
fsql
fsql -h myhost -U myuser -d mydb
```

- Multi-line SQL input supported
- Statements execute after the terminating `;`
- Meta-commands (starting with `\`) execute immediately

### Single Command (`-c`)

```bash
fsql -c "SELECT version()"
fsql -c "INSERT INTO t VALUES (1, 'hello')"
```

Exits with code `0` on success, non-zero on error.

### File Execution (`-f`)

```bash
fsql -f schema.sql
fsql -f seed.sql -v ON_ERROR_STOP=1
```

- Executes all statements in the file sequentially
- `-v ON_ERROR_STOP=1` stops on the first error and exits non-zero

---

## Output Formats

| Flag | Description |
|------|-------------|
| _(default)_ | ASCII table with column headers |
| `-t, --tuples-only` | Values only, no headers or row counts |
| `--expanded` | Vertical key/value layout per row |
| `--csv` | CSV output with header row |
| `--json` | JSON array output |

### Examples

```bash
# Default table output
fsql -c "SELECT id, name FROM users LIMIT 5"

# Tuples only (for scripting)
fsql -t -c "SELECT id FROM users"

# CSV
fsql --csv -c "SELECT * FROM orders"

# JSON
fsql --json -c "SELECT * FROM products LIMIT 10"

# Expanded (vertical)
fsql --expanded -c "SELECT * FROM users WHERE id = 1"
```

---

## Meta-Commands

Meta-commands are entered in the REPL and do not end with `;`.

| Command | Description |
|---------|-------------|
| `\q` | Quit fsql |
| `\?` | Show help |
| `\conninfo` | Show current connection information |
| `\dt` | List tables |
| `\d TABLE` | Describe table schema (columns, types, nullability) |
| `\l` | List databases |
| `\c DBNAME` | Reconnect to a different database |
| `\x` | Toggle expanded output mode |
| `\timing` | Toggle execution timing (v0.2) |

### Example Session

```
fsql=> \conninfo
You are connected to database "falcon" as user "falcon" on host "localhost" port 5432.

fsql=> \dt
 Schema | Name  | Type
--------+-------+------
 public | users | BASE TABLE
 public | orders| BASE TABLE

fsql=> \d users
Table "users":
  Column   | Type    | Nullable | Default
-----------+---------+----------+--------
  id       | integer | NO       |
  name     | text    | YES      |

fsql=> SELECT * FROM users WHERE id = 1;
 id | name
----+-------
  1 | alice
(1 row)

fsql=> \x
Expanded display is on.

fsql=> SELECT * FROM users WHERE id = 1;
-[ RECORD 1 ]
id   | 1
name | alice

fsql=> \q
```

---

---

## v0.2 New Features

### Command History

History is persisted to `~/.fsql_history` automatically:
- Loaded on REPL startup
- Saved on REPL exit (`\q` or Ctrl-D)
- Empty lines are not stored
- Use Up/Down arrow keys to navigate history

### Tab Completion

Press `Tab` to complete:
- **SQL keywords** — `SELECT`, `FROM`, `WHERE`, `INSERT`, `JOIN`, etc. (case-insensitive, completes uppercase)
- **Meta-commands** — `\q`, `\dt`, `\d`, `\l`, `\c`, `\x`, `\timing`, `\conninfo`, `\?`

Completion does **not** query the server (no schema/table name completion in v0.2).

### Improved Prompt

The REPL prompt now shows connection context:

```
fsql (falcon@localhost:5432/mydb)>
```

When inside a multi-line statement (waiting for `;`):

```
fsql ...>
```

### Execution Timing

Toggle timing with `\timing`:

```
fsql (falcon@localhost:5432/mydb)> \timing
Timing is on.

fsql (falcon@localhost:5432/mydb)> SELECT count(*) FROM orders;
 count
-------
 12345
(1 row)
Time: 1.234 ms

fsql (falcon@localhost:5432/mydb)> \timing
Timing is off.
```

Timing applies only to SQL execution, not meta-commands.

### Pager Support

Large result sets are automatically piped through a pager:

- Uses `$PAGER` environment variable if set
- Falls back to `less -S -F -X`
- **Disabled automatically** when:
  - `--csv` or `--json` output mode is active
  - stdout is not a TTY (e.g. piped to a file)

```bash
# Pager active (interactive terminal)
fsql -c "SELECT * FROM large_table"

# Pager disabled (redirected output)
fsql -c "SELECT * FROM large_table" > output.txt
fsql --csv -c "SELECT * FROM large_table" > data.csv
```

### Expanded Output Improvements

`\x` / `--expanded` now shows:
- Column names aligned to the widest column name
- `NULL` values displayed explicitly as `NULL`
- Clear record separators

```
-[ RECORD 1 ]
id   | 1
name | alice
dept | NULL
```

---

---

## v0.3 New Features — Client-Side Import / Export

### \export — Export Query Results to CSV

```
\export <query> TO <file> [OPTIONS]
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `WITH HEADER` | on | Write column names as first row |
| `WITHOUT HEADER` | — | Suppress header row |
| `DELIMITER '<c>'` | `,` | Field delimiter (`,` `\t` `|` `;`) |
| `NULL AS '<s>'` | `''` | String to use for NULL values |
| `QUOTE '<c>'` | `"` | Quote character |
| `ESCAPE '<c>'` | `"` | Escape character (doubled-quote style) |
| `ENCODING 'utf-8'` | utf-8 | Accepted but only UTF-8 supported |
| `OVERWRITE` | off | Overwrite existing file |

**Examples:**

```sql
-- Basic export
\export SELECT * FROM orders TO /tmp/orders.csv

-- Tab-separated, no header, overwrite
\export SELECT id, name FROM users TO /tmp/users.tsv DELIMITER '\t' WITHOUT HEADER OVERWRITE

-- Export with explicit NULL representation
\export SELECT * FROM products TO /tmp/products.csv NULL AS 'NULL'

-- Quoted query (for queries with spaces or special chars)
\export 'SELECT a, b FROM t WHERE a > 10' TO /tmp/result.csv
```

**Behavior:**
- Executes query via normal SQL path (not server-side COPY)
- Streams rows to file row-by-row (no full buffering)
- Fails fast if the output file exists and `OVERWRITE` is not specified
- Fails fast if the output directory does not exist
- Prints `Exported N rows to '<file>'.` on success
- Respects `\timing` if enabled

---

### \import — Import CSV into Table

```
\import <file> INTO <table> [OPTIONS]
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `WITH HEADER` | on | First row is column names |
| `WITHOUT HEADER` | — | No header; use table column order |
| `DELIMITER '<c>'` | `,` | Field delimiter |
| `NULL AS '<s>'` | `''` | String that represents NULL |
| `QUOTE '<c>'` | `"` | Quote character |
| `ESCAPE '<c>'` | `"` | Escape character |
| `BATCH SIZE <n>` | 1000 | Rows per INSERT batch |
| `ON ERROR STOP` | — | Abort on first INSERT error |
| `ON ERROR CONTINUE` | default | Skip failed rows, continue |

**Examples:**

```sql
-- Basic import (CSV with header)
\import /tmp/orders.csv INTO orders

-- Tab-separated, no header
\import /tmp/users.tsv INTO users DELIMITER '\t' WITHOUT HEADER

-- Large file with bigger batch and error stop
\import /tmp/big.csv INTO staging BATCH SIZE 5000 ON ERROR STOP

-- Import with NULL string
\import /tmp/data.csv INTO mytable NULL AS 'NULL'
```

**Behavior:**
- Reads CSV file client-side (not server-side COPY)
- With `WITH HEADER`: matches columns by name
- Without header: uses table column order from `information_schema`
- Validates column count on every row before INSERT
- Generates individual `INSERT` statements per row (no COPY protocol)
- Prints periodic progress every 10,000 rows
- Prints final summary: `Import complete: N total, N inserted, N failed.`
- Respects `\timing` if enabled

---

### Differences from psql `\copy`

| Aspect | fsql `\import`/`\export` | psql `\copy` |
|--------|--------------------------|--------------|
| Protocol | Client-side INSERT/SELECT | Server-side COPY protocol |
| Performance | Moderate (row-by-row INSERT) | Fast (bulk COPY) |
| Binary format | Not supported | Supported |
| Streaming pipes | Not supported | Supported |
| NULL handling | Via `NULL AS` option | Via `NULL AS` option |
| Error recovery | `ON ERROR CONTINUE` | Aborts on first error |
| Schema inference | Not supported | Not supported |

### v0.3 Limitations

- No binary COPY format
- No streaming pipes (stdin/stdout)
- No parallel import/export
- No implicit type casting (server handles types)
- Import uses individual INSERTs — not suitable for bulk loading millions of rows
- Only UTF-8 encoding supported

---

---

## v0.4 New Features — Operational Introspection (Read-Only)

All v0.4 commands are **read-only**. They never mutate cluster state.
They work in both interactive REPL and `-c` script mode.
Output respects `--json`, `\x` (expanded), and `\timing`.

---

### \cluster — Cluster Topology

```
\cluster [nodes | leaders | shards]
```

| Sub-command | Description |
|-------------|-------------|
| `\cluster` | Overview: cluster ID, node count, term/epoch |
| `\cluster nodes` | Node ID, address, role, health, uptime |
| `\cluster leaders` | Shard ID, leader node, replica count |
| `\cluster shards` | Shard ID, partition, leader, followers, state |

**Examples:**
```sql
\cluster
\cluster nodes
\cluster shards
```

Falls back to `pg_settings` / connection info when `falcon.cluster_*` views are not present.

---

### \txn — Transaction Visibility

```
\txn [active | prepared | <txn_id>]
```

| Sub-command | Description |
|-------------|-------------|
| `\txn` / `\txn active` | Active transactions: ID, state, duration, shard scope |
| `\txn prepared` | In-doubt (prepared) transactions |
| `\txn <id>` | Full detail for a specific transaction |

**Examples:**
```sql
\txn active
\txn prepared
\txn 12345
```

Falls back to `pg_stat_activity` / `pg_prepared_xacts` when `falcon.active_transactions` is not present.

---

### \consistency — Consistency & WAL State

```
\consistency [status | in-doubt]
```

| Sub-command | Description |
|-------------|-------------|
| `\consistency` / `\consistency status` | Commit policy, WAL durability, replication mode |
| `\consistency in-doubt` | Unresolved in-doubt transactions with recovery recommendation |

**Examples:**
```sql
\consistency status
\consistency in-doubt
```

Falls back to `pg_settings` (`synchronous_commit`, `wal_level`, etc.) when `falcon.consistency_config` is not present.

---

### \node — Node Health & Resources

```
\node [stats | memory | wal]
```

| Sub-command | Description |
|-------------|-------------|
| `\node` / `\node stats` | Active connections, transactions, server version |
| `\node memory` | Buffer sizes, work_mem, cache settings |
| `\node wal` | Current WAL LSN, WAL level, replica count |

**Examples:**
```sql
\node stats
\node memory
\node wal
```

Falls back to `pg_stat_activity`, `pg_settings`, `pg_stat_replication` when `falcon.node_*` views are not present.

---

### \failover — Failover Events (Read-Only)

```
\failover [status | history]
```

| Sub-command | Description |
|-------------|-------------|
| `\failover` / `\failover status` | Current recovery state, ongoing reconfiguration |
| `\failover history` | Recent failover events: time, shard, old/new leader, reason |

**Examples:**
```sql
\failover status
\failover history
```

Falls back to `pg_is_in_recovery()` when `falcon.failover_*` views are not present.

---

### Operational Commands — Design Notes

- **All commands are read-only** — no cluster state is mutated
- **Graceful fallback** — when FalconDB-specific views (`falcon.*`) are absent, standard PostgreSQL catalog views are used
- **Deterministic output** — suitable for scripting and CI
- **Pager-aware** — large outputs automatically routed through pager
- **JSON-compatible** — use `--json` for machine-readable output

```bash
# Script mode examples
fsql -c "\cluster nodes"
fsql --json -c "\txn active"
fsql -c "\consistency status"
fsql -c "\node stats"
fsql -c "\failover status"
```

---

---

## v0.5 New Features — Management Commands (Controlled, Auditable)

All v0.5 management commands follow a **two-phase model**:
1. **Plan phase** (default) — validates intent, shows scope and risk, never mutates state
2. **Apply phase** (`--apply`) — executes with guardrails, produces audit record

**No command auto-applies. No command skips the plan phase.**

### New CLI Flags

| Flag | Description |
|------|-------------|
| `--apply` | Execute the management command (skip plan-only mode) |
| `--yes` | Skip interactive confirmation prompt (use with `--apply`) |

---

### \node drain \<node_id\> — Drain a Node

```
\node drain <node_id>
```

**Plan output includes:** node role, health status, active transaction count, risk level, warnings.

**Apply:** routes no new transactions to the node; in-flight transactions complete normally.

```bash
# Plan (default — safe, no mutation)
fsql -c "\node drain node1"

# Apply with confirmation
fsql --apply -c "\node drain node1"

# Apply without prompt (CI/scripts)
fsql --apply --yes -c "\node drain node1"
```

**Risk:** MEDIUM (HIGH if node is a shard leader — may trigger election)

---

### \node resume \<node_id\> — Resume a Node

```
\node resume <node_id>
```

Re-enables transaction routing to a previously drained node. Verifies node health before resuming.

```bash
fsql -c "\node resume node1"
fsql --apply --yes -c "\node resume node1"
```

**Risk:** LOW (HIGH if node health is degraded)

---

### \shard move \<shard_id\> to \<node_id\> — Move Shard Leader

```
\shard move <shard_id> to <node_id>
```

**Plan output includes:** current leader, target leader, replica set, shard state, expected impact, risk level.

**Apply:** initiates leader transition via `falcon.admin_move_shard_leader()`.

```bash
# Plan
fsql -c "\shard move 3 to node2"

# Apply
fsql --apply --yes -c "\shard move 3 to node2"
```

**Risk:** MEDIUM (HIGH if shard is in recovering/rebalancing state)

---

### \cluster mode readonly|readwrite — Set Cluster Access Mode

```
\cluster mode readonly
\cluster mode readwrite
```

**`readonly`:** Rejects new write transactions; reads and admin inspection remain available.
**`readwrite`:** Restores normal operation.

**Plan output includes:** current mode, active writer count, pending transitions, warnings.

```bash
# Plan readonly transition
fsql -c "\cluster mode readonly"

# Apply (incident response)
fsql --apply --yes -c "\cluster mode readonly"

# Restore normal operation
fsql --apply --yes -c "\cluster mode readwrite"
```

**Risk:** HIGH (readonly), MEDIUM (readwrite)

---

### \txn resolve \<txn_id\> [commit|abort] — Resolve In-Doubt Transaction

```
\txn resolve <txn_id> [commit | abort]
```

**Only allowed for in-doubt (prepared) transactions.**

**Plan output includes:** transaction state, shards involved, commit point (CP-L/CP-D/CP-V), elapsed time, safe resolution options.

**Apply:** calls `falcon.admin_resolve_transaction()`. Resolution is idempotent and produces an audit record.

```bash
# Plan (inspect the in-doubt transaction)
fsql -c "\txn resolve txn-42"

# Apply — commit
fsql --apply --yes -c "\txn resolve txn-42 commit"

# Apply — abort
fsql --apply --yes -c "\txn resolve txn-42 abort"
```

**Risk:** HIGH — resolution is irreversible

---

### \failover simulate \<node_id\> — Dry-Run Failover Simulation

```
\failover simulate <node_id>
```

**Dry-run only — never mutates cluster state.**

**Output includes:** affected shards, leader candidates, estimated RTO, data safety status.

```bash
fsql -c "\failover simulate node1"
```

---

### \audit [recent|\<event_id\>] — View Audit Log

```
\audit
\audit recent
\audit <event_id>
```

Displays management command audit records from `falcon.audit_log`.

**Each record includes:** timestamp, operator identity, command issued, plan summary, apply result, outcome.

```bash
fsql -c "\audit recent"
fsql -c "\audit evt-42"
```

---

### Management Commands — Safety Guarantees

- **No auto-apply** — every command defaults to plan mode
- **No partial execution** — apply is atomic or rejected
- **No silent fallback** — errors are explicit and actionable
- **Audit trail** — every apply produces a record in `falcon.audit_log`
- **Privilege-checked server-side** — requires `admin` or `operator` role
- **Read-only commands unchanged** — all v0.4 introspection commands remain unaffected

### Operational Examples

```bash
# Scenario 1: Drain node before maintenance window
fsql -c "\node drain node1"          # inspect plan
fsql --apply --yes -c "\node drain node1"   # execute

# Scenario 2: Readonly mode during incident
fsql -c "\cluster mode readonly"     # inspect plan
fsql --apply --yes -c "\cluster mode readonly"

# Scenario 3: Resolve stuck in-doubt transaction
fsql -c "\txn prepared"              # find in-doubt txns
fsql -c "\txn resolve txn-99"        # inspect resolution plan
fsql --apply --yes -c "\txn resolve txn-99 commit"

# Scenario 4: Understand failover impact before maintenance
fsql -c "\failover simulate node2"

# Scenario 5: Review recent management actions
fsql -c "\audit recent"
```

---

---

## v0.6 New Features — Policy & Automation (Policy-First Edition)

`fsql` v0.6 introduces a **policy definition interface** — operators declare rules and guardrails; FalconDB executes actions only within approved policies.

**Core principle:** No automated action may run without a registered, enabled policy with satisfied conditions and passing guardrails.

---

### Policy Lifecycle

```
create (disabled) → enable → [automation evaluates] → fires (if conditions met + guardrails pass)
                  ↓
               disable → delete (audit records preserved)
```

All policy mutations follow the v0.5 plan/apply model. `\policy create` and `\policy delete` default to plan mode; add `--apply` to execute.

---

### \policy — Policy Management

```
\policy list
\policy show <policy_id>
\policy create <key=value ...>
\policy enable <policy_id>
\policy disable <policy_id>
\policy delete <policy_id>
\policy simulate <policy_id>
\policy audit [<policy_id>]
```

| Sub-command | Description |
|-------------|-------------|
| `list` | List all policies with status, scope, severity |
| `show <id>` | Full policy detail including guardrail config |
| `create` | Define a new policy (starts disabled) |
| `enable <id>` | Activate policy for automation evaluation |
| `disable <id>` | Deactivate without deleting |
| `delete <id>` | Permanently remove (audit records preserved) |
| `simulate <id>` | Dry-run: evaluate conditions + guardrails, no mutation |
| `audit [<id>]` | View audit records for a policy |

---

### \policy create — Declarative Policy Definition

```
\policy create id=<id> condition=<cond> action=<action> [options...]
```

**Required fields:**

| Field | Description |
|-------|-------------|
| `id` | Unique policy identifier |
| `condition` | Trigger condition (see below) |
| `action` | Action to execute when condition is met |

**Optional fields:**

| Field | Default | Description |
|-------|---------|-------------|
| `description` | (id) | Human-readable description |
| `scope` | `cluster` | `cluster`, `shard`, or `node` |
| `severity` | `warning` | `info`, `warning`, `critical` |
| `max_frequency` | `3` | Max fires per time window |
| `time_window` | `3600` | Time window in seconds |
| `cooldown` | `300` | Minimum seconds between fires |
| `risk_ceiling` | `MEDIUM` | `LOW`, `MEDIUM`, or `HIGH` |
| `health_prereq` | `healthy` | Required cluster health state |

**Supported Conditions:**

| Condition | Description |
|-----------|-------------|
| `node_unavailable_secs=N` | Node unreachable for N seconds |
| `shard_leader_unavailable` | Shard has no reachable leader |
| `wal_lag_bytes=N` | WAL replication lag exceeds N bytes |
| `memory_pct=N` | Memory pressure exceeds N% |
| `in_doubt_txns=N` | In-doubt transactions exceed N |
| `cluster_readonly` | Cluster is in readonly mode |

**Supported Actions:**

| Action | Description |
|--------|-------------|
| `drain_node=<node_id>` | Drain the specified node |
| `move_shard=<shard>:<node>` | Move shard leader to target node |
| `set_cluster_readonly` | Switch cluster to readonly mode |
| `alert=<message>` | Emit an alert event |
| `require_human_approval` | Block automation, require human action |

**Examples:**

```bash
# Plan a policy (default — no mutation)
fsql -c "\policy create id=drain-on-unavail \
  condition=node_unavailable_secs=60 \
  action=drain_node=node1 \
  severity=critical \
  risk_ceiling=MEDIUM \
  cooldown=600"

# Apply (create the policy, starts disabled)
fsql --apply --yes -c "\policy create id=drain-on-unavail \
  condition=node_unavailable_secs=60 action=drain_node=node1"

# Enable it
fsql --apply --yes -c "\policy enable drain-on-unavail"

# Simulate before enabling
fsql -c "\policy simulate drain-on-unavail"
```

---

### Guardrails

Every policy has four guardrails evaluated before any action fires:

| Guardrail | Description |
|-----------|-------------|
| **Health prerequisite** | Cluster must be in required health state |
| **Frequency limit** | Policy may not fire more than N times per window |
| **Cooldown** | Minimum seconds between consecutive fires |
| **Risk ceiling** | Action risk level must not exceed ceiling |

If any guardrail fails, the policy is **blocked** — no action is taken and the block is recorded in the audit log.

---

### \policy simulate \<policy_id\> — Dry-Run Evaluation

```
\policy simulate <policy_id>
```

Evaluates the policy against live cluster state without executing any action.

**Output includes:**
- Condition evaluation result (met / not met)
- Guardrail evaluation (pass / blocked with reason)
- Would-fire verdict
- Risk assessment

```bash
fsql -c "\policy simulate drain-on-unavail"
```

**Always dry-run — never mutates cluster state.**

---

### \automation — Automation Engine Control

```
\automation status
\automation events
\automation pause
\automation resume
```

| Sub-command | Description |
|-------------|-------------|
| `status` | Engine state, enabled/disabled policy counts, last evaluation |
| `events` | Recent automation events with outcomes |
| `pause` | Pause automation engine (no policies will fire) |
| `resume` | Resume automation engine |

```bash
# Check automation engine state
fsql -c "\automation status"

# Review recent automation activity
fsql -c "\automation events"

# Pause during maintenance
fsql --apply --yes -c "\automation pause"

# Resume after maintenance
fsql --apply --yes -c "\automation resume"
```

**Note:** `pause` and `resume` call `falcon.admin_pause_automation()` / `falcon.admin_resume_automation()` on the server. Requires FalconDB server v1.2+.

---

### Policy & Automation — Safety Guarantees

- **No automation without a policy** — automation engine only fires registered, enabled policies
- **No policy fires without passing guardrails** — all four guardrail checks must pass
- **Simulation never mutates** — `\policy simulate` is always a dry-run
- **Full audit trail** — every policy evaluation, fire, and block is recorded
- **Policies start disabled** — explicit `\policy enable` required before automation can fire
- **Pause is instant** — `\automation pause` stops all automation immediately

### Operational Examples

```bash
# Scenario 1: Auto-drain unhealthy node
fsql --apply --yes -c "\policy create id=auto-drain \
  condition=node_unavailable_secs=30 action=drain_node=node1 \
  severity=critical cooldown=300 risk_ceiling=MEDIUM"
fsql --apply --yes -c "\policy enable auto-drain"
fsql -c "\policy simulate auto-drain"

# Scenario 2: Protect cluster during incident
fsql --apply --yes -c "\policy create id=incident-guard \
  condition=in_doubt_txns=10 action=require_human_approval \
  severity=critical risk_ceiling=LOW"
fsql --apply --yes -c "\policy enable incident-guard"

# Scenario 3: Prevent unsafe shard movement
fsql --apply --yes -c "\policy create id=shard-guard \
  condition=shard_leader_unavailable action=alert=shard_leader_lost \
  risk_ceiling=LOW cooldown=60"
fsql -c "\policy list"
fsql -c "\policy audit incident-guard"
```

---

---

## v0.7 New Features — Integration & Ecosystem (Machine-Oriented Edition)

`fsql` v0.7 becomes a **bridge between FalconDB and external systems** — automation platforms, ITSM, Kubernetes controllers, and CI pipelines — while enforcing all policy guardrails and auditability.

---

### Machine Mode (`--mode machine`)

```bash
fsql --mode machine -c "\events list"
fsql --mode machine -c "\integration list"
fsql --mode machine -c "\cluster status"
```

**Behavior:**
- All output is JSON with a **stable schema** (no human-oriented formatting)
- Pager disabled
- No interactive prompts
- Exit codes strictly enforced (`0` = success, `1` = error)
- Designed for CronJobs, Kubernetes operators, CI pipelines

**Output envelope schema:**

```json
{
  "fsql": {
    "version": "0.7",
    "command": "\\events list",
    "success": true,
    "operator": "alice",
    "source": "automation",
    "timestamp_unix": 1708000000,
    "output": { ... }
  }
}
```

**Error envelope:**

```json
{
  "fsql": {
    "version": "0.7",
    "command": "\\cluster status",
    "success": false,
    "operator": "ci-bot",
    "source": "automation",
    "timestamp_unix": 1708000000,
    "error": "connection refused"
  }
}
```

---

### Identity & Attribution

Every command carries operator identity and source attribution, propagated into audit logs and integration events.

**CLI flags:**

| Flag | Env var | Description |
|------|---------|-------------|
| `--operator <name>` | `FSQL_OPERATOR` | Operator identity (default: `$USER`) |
| `--source <type>` | `FSQL_SOURCE` | Source: `human` or `automation` (default: `human`) |

```bash
# Human operator
fsql --operator alice --source human -c "\cluster status"

# CI pipeline
FSQL_OPERATOR=ci-bot FSQL_SOURCE=automation \
  fsql --mode machine -c "\events list"

# Kubernetes job
fsql --mode machine --operator k8s-operator --source automation \
  -c "\automation status"
```

---

### \integration — Integration Registry

```
\integration list
\integration add webhook <url>
\integration remove <integration_id>
\integration disable <integration_id>
\integration enable <integration_id>
```

| Sub-command | Description |
|-------------|-------------|
| `list` | List all registered integrations |
| `add webhook <url>` | Register a webhook endpoint |
| `remove <id>` | Permanently remove an integration |
| `disable <id>` | Pause delivery without removing |
| `enable <id>` | Resume delivery |

**Webhook delivery guarantees:**
- At-least-once delivery
- Ordered per integration
- Exponential backoff on failure (30s → 60s → 120s → 240s → 300s max)
- All delivery attempts are auditable

```bash
# Register a webhook
fsql -c "\integration add webhook https://hooks.example.com/falcondb"

# List integrations
fsql --mode machine -c "\integration list"

# Disable during maintenance
fsql -c "\integration disable int-1"
```

**Events delivered to webhooks:**
- Policy evaluation (fired / blocked)
- Automation execution
- Management apply (drain, mode change, shard move)
- Failover / recovery events

---

### \events — Event Subscription & Replay

```
\events list
\events tail
\events replay <event_id>
```

| Sub-command | Description |
|-------------|-------------|
| `list` | List recent events (up to 100) |
| `tail` | Show last 20 events |
| `replay <id>` | Replay a specific event (logged to audit trail) |

**Event schema:**

| Field | Description |
|-------|-------------|
| `event_id` | Unique event identifier |
| `event_time` | Timestamp |
| `event_type` | Type (policy.fired, management.apply, failover.detected, etc.) |
| `severity` | `info`, `warning`, `critical` |
| `source` | `human` or `automation` |
| `operator` | Identity of the actor |
| `payload` | Structured event data |

```bash
# List recent events
fsql -c "\events list"

# Machine-readable event stream
fsql --mode machine -c "\events tail"

# Replay a specific event
fsql -c "\events replay evt-42"
```

**Replay is logged to the audit trail.** Replayed events do not re-execute actions — they re-deliver the event payload to registered integrations.

---

### Kubernetes / Cloud-Friendly Usage

`fsql` v0.7 is designed for non-interactive execution in cloud environments:

```yaml
# Kubernetes CronJob example
apiVersion: batch/v1
kind: CronJob
metadata:
  name: falcondb-policy-check
spec:
  schedule: "*/5 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: fsql
            image: falcondb/fsql:0.7
            command:
            - fsql
            - --mode
            - machine
            - --operator
            - k8s-cronjob
            - --source
            - automation
            - -c
            - \automation status
            env:
            - name: PGHOST
              value: falcondb-service
            - name: PGPORT
              value: "5432"
```

```bash
# CI pipeline usage
fsql --mode machine \
     --operator "$CI_USER" \
     --source automation \
     -c "\policy list" | jq '.fsql.output'

# Incident notification flow
fsql --mode machine -c "\events tail" \
  | jq '.fsql.output.text[]' \
  | grep "critical" \
  | xargs -I{} curl -X POST https://pagerduty.example.com/alert -d {}
```

---

### Integration Safety Model

- Integrations are **explicitly registered** — no implicit delivery
- Webhook delivery is **policy-bound** — disabled integrations receive no events
- **Manual disable** is instant: `\integration disable <id>`
- All delivery attempts are **auditable** via `\audit recent`
- Machine mode enforces **deterministic exit codes** — safe for CI gate usage

---

### v0.7 — Complete CLI Flag Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--mode machine` | `interactive` | JSON-only output, no pager, no prompts |
| `--operator <name>` | `$USER` | Operator identity for audit attribution |
| `--source <type>` | `human` | Source: `human` or `automation` |
| `--apply` | false | Execute management commands (v0.5+) |
| `--yes` | false | Skip confirmation prompt (v0.5+) |
| `--json` | false | JSON output for query results |
| `-x` | false | Expanded output mode |

---

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | Connection error or fatal error |
| `3` | SQL error with `ON_ERROR_STOP=1` active |

---

## Environment Variables

```bash
export PGHOST=localhost
export PGPORT=5432
export PGUSER=falcon
export PGDATABASE=falcon
export PGPASSWORD=secret

fsql -c "SELECT 1"
```

---

## Logging

`fsql` uses `RUST_LOG` for log level control:

```bash
RUST_LOG=debug fsql -c "SELECT 1"
RUST_LOG=warn  fsql -f seed.sql
```

Default log level is `warn`. Logs are written to stderr.

---

## CI Usage

```bash
# Quick smoke test
fsql -c "SELECT 1"

# File execution with error stop
fsql -f scripts/smoke.sql -v ON_ERROR_STOP=1

# Pipe meta-commands
printf "\\dt\n\\q\n" | fsql

# Check exit code
fsql -c "SELECT 1" && echo "OK" || echo "FAIL"
```

See `scripts/cli_smoke.sh` for the full CI smoke test.
