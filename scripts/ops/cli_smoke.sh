#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# FalconDB CLI (fsql) Smoke Test
# ═══════════════════════════════════════════════════════════════════════
#
# Assumes a running FalconDB instance.
# Set PGHOST / PGPORT / PGUSER / PGDATABASE / PGPASSWORD as needed.
#
# Usage:
#   ./scripts/cli_smoke.sh
#
# Exit codes:
#   0 — all smoke tests passed
#   1 — one or more tests failed
# ═══════════════════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FSQL="${ROOT_DIR}/target/release/fsql"

PASS=0
FAIL=0

# ── Build fsql if not present ──────────────────────────────────────────
if [ ! -x "$FSQL" ]; then
    echo "Building fsql..."
    cargo build --release -p falcon_cli --quiet
fi

# ── Helpers ────────────────────────────────────────────────────────────
run_test() {
    local name="$1"
    local cmd="$2"
    if eval "$cmd" > /dev/null 2>&1; then
        echo "  ✅ PASS: $name"
        PASS=$((PASS + 1))
    else
        echo "  ❌ FAIL: $name"
        FAIL=$((FAIL + 1))
    fi
}

run_test_output() {
    local name="$1"
    local cmd="$2"
    local expected="$3"
    local out
    out=$(eval "$cmd" 2>&1)
    if echo "$out" | grep -qF "$expected"; then
        echo "  ✅ PASS: $name"
        PASS=$((PASS + 1))
    else
        echo "  ❌ FAIL: $name (expected: '$expected', got: '$out')"
        FAIL=$((FAIL + 1))
    fi
}

# ── Create smoke SQL file ──────────────────────────────────────────────
SMOKE_SQL=$(mktemp /tmp/fsql_smoke_XXXXXX.sql)
cat > "$SMOKE_SQL" <<'EOF'
CREATE TABLE IF NOT EXISTS _fsql_smoke (id INT PRIMARY KEY, v TEXT);
INSERT INTO _fsql_smoke VALUES (1, 'hello') ON CONFLICT (id) DO NOTHING;
INSERT INTO _fsql_smoke VALUES (2, 'world') ON CONFLICT (id) DO NOTHING;
SELECT * FROM _fsql_smoke ORDER BY id;
DROP TABLE _fsql_smoke;
EOF

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  fsql Smoke Test"
echo "  $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
echo "════════════════════════════════════════════════════════════════"

# ── Gate 1: Basic connectivity ─────────────────────────────────────────
run_test "Basic connectivity (SELECT 1)" \
    "$FSQL -c 'SELECT 1'"

# ── Gate 2: -c output contains expected value ──────────────────────────
run_test_output "-c output correctness" \
    "$FSQL -c 'SELECT 42 AS answer'" \
    "42"

# ── Gate 3: -f file execution ──────────────────────────────────────────
run_test "-f file execution" \
    "$FSQL -f '$SMOKE_SQL' -v ON_ERROR_STOP=1"

# ── Gate 4: --tuples-only ─────────────────────────────────────────────
run_test_output "--tuples-only suppresses headers" \
    "$FSQL -t -c 'SELECT 1 AS id, 2 AS val'" \
    "1|2"

# ── Gate 5: --csv output ───────────────────────────────────────────────
run_test_output "--csv output has header" \
    "$FSQL --csv -c 'SELECT 1 AS id, 2 AS val'" \
    "id,val"

# ── Gate 6: --json output ─────────────────────────────────────────────
run_test_output "--json output is JSON array" \
    "$FSQL --json -c 'SELECT 1 AS id'" \
    '"id"'

# ── Gate 7: Meta-command \dt via pipe ─────────────────────────────────
run_test "Meta-command \\dt via pipe" \
    "printf '\\\\dt\n\\\\q\n' | $FSQL"

# ── Gate 8: Meta-command \conninfo via pipe ────────────────────────────
run_test_output "Meta-command \\conninfo" \
    "printf '\\\\conninfo\n\\\\q\n' | $FSQL" \
    "connected to database"

# ── Gate 9: Non-zero exit on SQL error ────────────────────────────────
if "$FSQL" -c 'SELECT * FROM _nonexistent_table_xyz' > /dev/null 2>&1; then
    echo "  ❌ FAIL: Non-zero exit on SQL error (expected failure, got success)"
    FAIL=$((FAIL + 1))
else
    echo "  ✅ PASS: Non-zero exit on SQL error"
    PASS=$((PASS + 1))
fi

# ── Gate 10: ON_ERROR_STOP=1 stops on first error ─────────────────────
ERROR_SQL=$(mktemp /tmp/fsql_error_XXXXXX.sql)
cat > "$ERROR_SQL" <<'EOF'
SELECT 1;
SELECT * FROM _nonexistent_xyz;
SELECT 2;
EOF

if "$FSQL" -f "$ERROR_SQL" -v ON_ERROR_STOP=1 > /dev/null 2>&1; then
    echo "  ❌ FAIL: ON_ERROR_STOP=1 should exit non-zero on error"
    FAIL=$((FAIL + 1))
else
    echo "  ✅ PASS: ON_ERROR_STOP=1 exits on first error"
    PASS=$((PASS + 1))
fi

# ── Gate 11 (v0.2): History file created after -c run ─────────────────
# Run a command that triggers history (REPL only writes history, -c does not,
# so we pipe a command through REPL to trigger history save)
HISTORY_FILE="${HOME}/.fsql_history"
rm -f "$HISTORY_FILE"
printf "SELECT 1;\n\\q\n" | "$FSQL" > /dev/null 2>&1
if [ -f "$HISTORY_FILE" ]; then
    echo "  ✅ PASS: History file created at ~/.fsql_history"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: History file not created at ~/.fsql_history"
    FAIL=$((FAIL + 1))
fi

# ── Gate 12 (v0.2): Timing output contains "Time:" ────────────────────
run_test_output "\\timing toggle produces Time: output" \
    "printf '\\\\timing\nSELECT 1;\n\\\\q\n' | $FSQL" \
    "Time:"

# ── Gate 13 (v0.2): Pager safety — non-TTY output works without hang ──
PAGER_OUT=$(mktemp /tmp/fsql_pager_XXXXXX.txt)
if "$FSQL" -c "SELECT generate_series(1,10)" > "$PAGER_OUT" 2>&1; then
    if [ -s "$PAGER_OUT" ]; then
        echo "  ✅ PASS: Non-TTY output (pager disabled) works correctly"
        PASS=$((PASS + 1))
    else
        echo "  ❌ FAIL: Non-TTY output was empty"
        FAIL=$((FAIL + 1))
    fi
else
    echo "  ❌ FAIL: Non-TTY output exited non-zero"
    FAIL=$((FAIL + 1))
fi
rm -f "$PAGER_OUT"

# ── Gate 14 (v0.3): \export creates a CSV file ────────────────────────
EXPORT_FILE=$(mktemp /tmp/fsql_export_XXXXXX.csv)
rm -f "$EXPORT_FILE"  # ensure it doesn't exist so no OVERWRITE needed
if "$FSQL" -c "CREATE TABLE IF NOT EXISTS _fsql_smoke_t (a int); INSERT INTO _fsql_smoke_t VALUES (1),(2);" > /dev/null 2>&1; then
    if "$FSQL" -c "\\export SELECT * FROM _fsql_smoke_t TO $EXPORT_FILE" > /dev/null 2>&1; then
        if [ -f "$EXPORT_FILE" ] && [ -s "$EXPORT_FILE" ]; then
            echo "  ✅ PASS: \\export created CSV file at $EXPORT_FILE"
            PASS=$((PASS + 1))
        else
            echo "  ❌ FAIL: \\export did not create a non-empty CSV file"
            FAIL=$((FAIL + 1))
        fi
    else
        echo "  ❌ FAIL: \\export command exited non-zero"
        FAIL=$((FAIL + 1))
    fi
else
    echo "  ❌ FAIL: Setup for \\export test failed"
    FAIL=$((FAIL + 1))
fi

# ── Gate 15 (v0.3): \import reads CSV and inserts rows ────────────────
if [ -f "$EXPORT_FILE" ] && [ -s "$EXPORT_FILE" ]; then
    if "$FSQL" -c "CREATE TABLE IF NOT EXISTS _fsql_smoke_t2 (a int);" > /dev/null 2>&1; then
        if "$FSQL" -c "\\import $EXPORT_FILE INTO _fsql_smoke_t2" 2>&1 | grep -q "inserted"; then
            COUNT=$("$FSQL" --tuples-only -c "SELECT count(*) FROM _fsql_smoke_t2" 2>/dev/null | tr -d ' ')
            if [ "$COUNT" = "2" ]; then
                echo "  ✅ PASS: \\import inserted 2 rows from CSV"
                PASS=$((PASS + 1))
            else
                echo "  ❌ FAIL: \\import expected 2 rows, got '$COUNT'"
                FAIL=$((FAIL + 1))
            fi
        else
            echo "  ❌ FAIL: \\import did not print 'inserted' in output"
            FAIL=$((FAIL + 1))
        fi
    else
        echo "  ❌ FAIL: Setup for \\import test failed"
        FAIL=$((FAIL + 1))
    fi
else
    echo "  ⚠️  SKIP: \\import test skipped (export file missing)"
    FAIL=$((FAIL + 1))
fi

# ── Gate 16 (v0.3): \import bad file with ON_ERROR_STOP exits non-zero ─
if "$FSQL" -c "\\import /nonexistent_bad_file_xyz.csv INTO _fsql_smoke_t2 ON ERROR STOP" > /dev/null 2>&1; then
    echo "  ❌ FAIL: \\import bad file should exit non-zero, got success"
    FAIL=$((FAIL + 1))
else
    echo "  ✅ PASS: \\import bad file exits non-zero"
    PASS=$((PASS + 1))
fi

# ── Cleanup smoke tables ───────────────────────────────────────────────
"$FSQL" -c "DROP TABLE IF EXISTS _fsql_smoke_t; DROP TABLE IF EXISTS _fsql_smoke_t2;" > /dev/null 2>&1
rm -f "$EXPORT_FILE"

# ── Gate 17 (v0.4): \cluster exits 0 and produces output ──────────────
CLUSTER_OUT=$("$FSQL" -c "\cluster" 2>&1)
CLUSTER_EXIT=$?
if [ "$CLUSTER_EXIT" -eq 0 ] && [ -n "$CLUSTER_OUT" ]; then
    echo "  ✅ PASS: \\cluster exits 0 and produces output"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\cluster exit=$CLUSTER_EXIT output='$CLUSTER_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 18 (v0.4): \txn active exits 0 and produces output ───────────
TXN_OUT=$("$FSQL" -c "\txn active" 2>&1)
TXN_EXIT=$?
if [ "$TXN_EXIT" -eq 0 ] && [ -n "$TXN_OUT" ]; then
    echo "  ✅ PASS: \\txn active exits 0 and produces output"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\txn active exit=$TXN_EXIT output='$TXN_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 19 (v0.4): \consistency status exits 0 and produces output ───
CONS_OUT=$("$FSQL" -c "\consistency status" 2>&1)
CONS_EXIT=$?
if [ "$CONS_EXIT" -eq 0 ] && [ -n "$CONS_OUT" ]; then
    echo "  ✅ PASS: \\consistency status exits 0 and produces output"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\consistency status exit=$CONS_EXIT output='$CONS_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 20 (v0.4): \node stats exits 0 and produces output ──────────
NODE_OUT=$("$FSQL" -c "\node stats" 2>&1)
NODE_EXIT=$?
if [ "$NODE_EXIT" -eq 0 ] && [ -n "$NODE_OUT" ]; then
    echo "  ✅ PASS: \\node stats exits 0 and produces output"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\node stats exit=$NODE_EXIT output='$NODE_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 21 (v0.4): \failover status exits 0 and produces output ──────
FAIL_OUT=$("$FSQL" -c "\failover status" 2>&1)
FAIL_EXIT=$?
if [ "$FAIL_EXIT" -eq 0 ] && [ -n "$FAIL_OUT" ]; then
    echo "  ✅ PASS: \\failover status exits 0 and produces output"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\failover status exit=$FAIL_EXIT output='$FAIL_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 22 (v0.5): \node drain exits 0 in plan mode, shows PLAN output ─
NODE_DRAIN_OUT=$("$FSQL" -c "\node drain node1" 2>&1)
NODE_DRAIN_EXIT=$?
if [ "$NODE_DRAIN_EXIT" -eq 0 ] && echo "$NODE_DRAIN_OUT" | grep -qi "plan\|risk\|drain"; then
    echo "  ✅ PASS: \\node drain shows plan output (no mutation)"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\node drain exit=$NODE_DRAIN_EXIT output='$NODE_DRAIN_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 23 (v0.5): \cluster mode readonly exits 0 in plan mode ────────
CLUSTER_MODE_OUT=$("$FSQL" -c "\cluster mode readonly" 2>&1)
CLUSTER_MODE_EXIT=$?
if [ "$CLUSTER_MODE_EXIT" -eq 0 ] && echo "$CLUSTER_MODE_OUT" | grep -qi "plan\|risk\|readonly\|mode"; then
    echo "  ✅ PASS: \\cluster mode readonly shows plan output (no mutation)"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\cluster mode readonly exit=$CLUSTER_MODE_EXIT output='$CLUSTER_MODE_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 24 (v0.5): \failover simulate exits 0 and shows DRY-RUN ───────
SIMULATE_OUT=$("$FSQL" -c "\failover simulate node1" 2>&1)
SIMULATE_EXIT=$?
if [ "$SIMULATE_EXIT" -eq 0 ] && echo "$SIMULATE_OUT" | grep -qi "dry-run\|simulate\|rto\|plan"; then
    echo "  ✅ PASS: \\failover simulate shows dry-run output (no mutation)"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\failover simulate exit=$SIMULATE_EXIT output='$SIMULATE_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 25 (v0.5): \audit recent exits 0 ──────────────────────────────
AUDIT_OUT=$("$FSQL" -c "\audit recent" 2>&1)
AUDIT_EXIT=$?
if [ "$AUDIT_EXIT" -eq 0 ]; then
    echo "  ✅ PASS: \\audit recent exits 0"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\audit recent exit=$AUDIT_EXIT output='$AUDIT_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 26 (v0.6): \policy list exits 0 ───────────────────────────────
POLICY_LIST_OUT=$("$FSQL" -c "\policy list" 2>&1)
POLICY_LIST_EXIT=$?
if [ "$POLICY_LIST_EXIT" -eq 0 ]; then
    echo "  ✅ PASS: \\policy list exits 0"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\policy list exit=$POLICY_LIST_EXIT output='$POLICY_LIST_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 27 (v0.6): \policy create shows plan output (no mutation) ─────
POLICY_CREATE_OUT=$("$FSQL" -c "\policy create id=smoke-pol condition=cluster_readonly action=require_human_approval" 2>&1)
POLICY_CREATE_EXIT=$?
if [ "$POLICY_CREATE_EXIT" -eq 0 ] && echo "$POLICY_CREATE_OUT" | grep -qi "plan\|policy\|smoke-pol"; then
    echo "  ✅ PASS: \\policy create shows plan output (no mutation)"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\policy create exit=$POLICY_CREATE_EXIT output='$POLICY_CREATE_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 28 (v0.6): \policy simulate exits 0 and shows dry-run ─────────
POLICY_SIM_OUT=$("$FSQL" -c "\policy simulate smoke-pol" 2>&1)
POLICY_SIM_EXIT=$?
if [ "$POLICY_SIM_EXIT" -eq 0 ] && echo "$POLICY_SIM_OUT" | grep -qi "dry-run\|simulation\|smoke-pol\|not found"; then
    echo "  ✅ PASS: \\policy simulate exits 0 and shows dry-run output"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\policy simulate exit=$POLICY_SIM_EXIT output='$POLICY_SIM_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 29 (v0.6): \automation status exits 0 ─────────────────────────
AUTOMATION_STATUS_OUT=$("$FSQL" -c "\automation status" 2>&1)
AUTOMATION_STATUS_EXIT=$?
if [ "$AUTOMATION_STATUS_EXIT" -eq 0 ]; then
    echo "  ✅ PASS: \\automation status exits 0"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\automation status exit=$AUTOMATION_STATUS_EXIT output='$AUTOMATION_STATUS_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 30 (v0.6): \automation events exits 0 ─────────────────────────
AUTOMATION_EVENTS_OUT=$("$FSQL" -c "\automation events" 2>&1)
AUTOMATION_EVENTS_EXIT=$?
if [ "$AUTOMATION_EVENTS_EXIT" -eq 0 ]; then
    echo "  ✅ PASS: \\automation events exits 0"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: \\automation events exit=$AUTOMATION_EVENTS_EXIT output='$AUTOMATION_EVENTS_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 31 (v0.7): --mode machine \events list outputs valid JSON ──────
MACHINE_EVENTS_OUT=$("$FSQL" --mode machine -c "\events list" 2>&1)
MACHINE_EVENTS_EXIT=$?
if [ "$MACHINE_EVENTS_EXIT" -eq 0 ] && echo "$MACHINE_EVENTS_OUT" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    echo "  ✅ PASS: --mode machine \\events list outputs valid JSON"
    PASS=$((PASS + 1))
elif [ "$MACHINE_EVENTS_EXIT" -eq 0 ] && echo "$MACHINE_EVENTS_OUT" | grep -q '"fsql"'; then
    echo "  ✅ PASS: --mode machine \\events list outputs JSON envelope"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: --mode machine \\events list exit=$MACHINE_EVENTS_EXIT output='$MACHINE_EVENTS_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 32 (v0.7): --mode machine \integration list outputs valid JSON ─
MACHINE_INT_OUT=$("$FSQL" --mode machine -c "\integration list" 2>&1)
MACHINE_INT_EXIT=$?
if [ "$MACHINE_INT_EXIT" -eq 0 ] && echo "$MACHINE_INT_OUT" | grep -q '"fsql"'; then
    echo "  ✅ PASS: --mode machine \\integration list outputs JSON envelope"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: --mode machine \\integration list exit=$MACHINE_INT_EXIT output='$MACHINE_INT_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Gate 33 (v0.7): --mode machine \cluster status exits 0 ─────────────
MACHINE_CLUSTER_OUT=$("$FSQL" --mode machine -c "\cluster" 2>&1)
MACHINE_CLUSTER_EXIT=$?
if [ "$MACHINE_CLUSTER_EXIT" -eq 0 ]; then
    echo "  ✅ PASS: --mode machine \\cluster exits 0"
    PASS=$((PASS + 1))
else
    echo "  ❌ FAIL: --mode machine \\cluster exit=$MACHINE_CLUSTER_EXIT output='$MACHINE_CLUSTER_OUT'"
    FAIL=$((FAIL + 1))
fi

# ── Cleanup ────────────────────────────────────────────────────────────
rm -f "$SMOKE_SQL" "$ERROR_SQL"

# ── Summary ────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  SMOKE TEST SUMMARY"
echo "════════════════════════════════════════════════════════════════"
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
echo "════════════════════════════════════════════════════════════════"

if [ "$FAIL" -gt 0 ]; then
    echo "  ❌ CLI smoke test FAILED"
    exit 1
else
    echo "  ✅ CLI smoke test PASSED"
    exit 0
fi
