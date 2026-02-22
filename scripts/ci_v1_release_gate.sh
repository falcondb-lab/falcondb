#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# FalconDB v1.0 Release Gate — Go / No-Go
# ═══════════════════════════════════════════════════════════════════════
#
# This script runs ALL gates required for a v1.0 release tag.
# Any single failure = merge blocked / tag forbidden.
#
# Checklist mapping:
#   1.1  ACID CI          — cargo test (txn, consistency, recovery)
#   1.2  Cross-shard 2PC  — cargo test (indoubt_resolver, two_phase)
#   1.3  Fast/Slow path   — cargo test (txn state machine, fast path)
#   2.1  WAL replication   — cargo test (replication, wal)
#   2.2  Failover CI gate  — scripts/ci_failover_gate.sh (if available)
#   3.1  PG SQL whitelist  — cargo test (handler SQL tests)
#   3.2  Unsupported clear — cargo test (unsupported error tests)
#   4.1  Benchmark stable  — cargo test -p falcon_bench
#   5.1  Observability     — cargo test (SHOW commands)
#   7.0  Scope guard       — feature-off verification
#   B1-B10 Isolation module — scripts/ci_isolation_gate.sh
#
# Usage:
#   ./scripts/ci_v1_release_gate.sh
#
# Environment:
#   FALCON_SKIP_BENCH=1   — skip benchmark (for fast CI)
#   FALCON_SKIP_FAILOVER=1 — skip failover gate (requires multi-process)
# ═══════════════════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

PASS=0
FAIL=0
TOTAL=0

run_gate() {
  local name="$1"
  local cmd="$2"
  TOTAL=$((TOTAL + 1))
  echo ""
  echo "════════════════════════════════════════════════════════════════"
  echo "  GATE $TOTAL: $name"
  echo "════════════════════════════════════════════════════════════════"
  if eval "$cmd"; then
    echo "  ✅ PASS: $name"
    PASS=$((PASS + 1))
  else
    echo "  ❌ FAIL: $name"
    FAIL=$((FAIL + 1))
  fi
}

echo "═══════════════════════════════════════════════════════════════"
echo "  FalconDB v1.0 Release Gate"
echo "  $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
echo "═══════════════════════════════════════════════════════════════"

# ── §0: Workspace build ──
run_gate "Workspace build (release check)" \
  "cargo build --workspace"

# ── §1.1: ACID CI — txn kernel tests ──
run_gate "1.1 ACID: txn state machine (B1)" \
  "cargo test -p falcon_txn txn_state_machine"

run_gate "1.1 ACID: SI litmus (B3)" \
  "cargo test -p falcon_storage si_litmus"

run_gate "1.1 ACID: WAL recovery (B4)" \
  "cargo test -p falcon_storage recovery_tests"

run_gate "1.1 ACID: consistency invariants" \
  "cargo test -p falcon_common consistency"

run_gate "1.1 ACID: SQL-level ACID" \
  "cargo test -p falcon_protocol_pg test_acid"

# ── §1.2: Cross-shard 2PC ──
run_gate "1.2 2PC: in-doubt resolver (B2)" \
  "cargo test -p falcon_cluster indoubt_resolver"

# ── §1.3: Fast/Slow path ──
run_gate "1.3 Fast-path: txn path stats" \
  "cargo test -p falcon_protocol_pg test_fast_path"

# ── §2.1: WAL replication ──
run_gate "2.1 Replication: WAL + replica tests" \
  "cargo test -p falcon_cluster replication"

# ── §2.2: Failover CI gate ──
if [ "${FALCON_SKIP_FAILOVER:-0}" != "1" ] && [ -x "$SCRIPT_DIR/ci_failover_gate.sh" ]; then
  run_gate "2.2 Failover CI gate" \
    "$SCRIPT_DIR/ci_failover_gate.sh"
else
  echo ""
  echo "  ⏭  SKIP: 2.2 Failover CI gate (FALCON_SKIP_FAILOVER=1 or script not executable)"
fi

# ── §3.1: PG SQL whitelist ──
run_gate "3.1 SQL: JOIN/GROUP BY/UPSERT/RETURNING" \
  "cargo test -p falcon_protocol_pg test_sql_"

run_gate "3.1 SQL: PG protocol corner cases (B8)" \
  "cargo test -p falcon_protocol_pg test_b8"

# ── §3.2: Unsupported features → clear errors ──
run_gate "3.2 Unsupported: clear error responses" \
  "cargo test -p falcon_protocol_pg test_unsupported"

# ── §4: Benchmark stability ──
if [ "${FALCON_SKIP_BENCH:-0}" != "1" ]; then
  run_gate "4.1 Benchmark: build check" \
    "cargo test -p falcon_bench"
else
  echo ""
  echo "  ⏭  SKIP: 4.1 Benchmark (FALCON_SKIP_BENCH=1)"
fi

# ── §5: Observability ──
run_gate "5.1 Observability: SHOW falcon.txn_stats" \
  "cargo test -p falcon_protocol_pg test_show_txn_stats"

run_gate "5.1 Observability: SHOW falcon.memory" \
  "cargo test -p falcon_protocol_pg test_show_memory"

run_gate "5.1 Observability: SHOW falcon.gc_stats" \
  "cargo test -p falcon_protocol_pg test_show_gc"

# ── §5.5: Admission backpressure (B5/B6) ──
run_gate "5.5 Admission: backpressure gates" \
  "cargo test -p falcon_txn admission_backpressure"

# ── §7: Scope guard — long-txn detection (B7) ──
run_gate "7.0 B7: Long-txn detection + kill" \
  "cargo test -p falcon_txn long_txn_detection"

# ── §9: DDL concurrency (B9) ──
run_gate "9.0 B9: DDL concurrency safety" \
  "cargo test -p falcon_storage ddl_concurrency"

# ── §10: Scope guard — feature-off verification ──
run_gate "10.0 Scope guard: no columnstore in default build" \
  "cargo test -p falcon_storage -- --ignored 2>&1 | grep -c 'columnstore' | xargs test 0 -eq"

# ── §11: Full workspace test suite ──
run_gate "11.0 Full workspace tests" \
  "cargo test --workspace"

# ── Summary ──
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  v1.0 RELEASE GATE SUMMARY"
echo "════════════════════════════════════════════════════════════════"
echo "  Total gates: $TOTAL"
echo "  Passed:      $PASS"
echo "  Failed:      $FAIL"
echo "════════════════════════════════════════════════════════════════"

if [ "$FAIL" -gt 0 ]; then
  echo ""
  echo "  ❌ v1.0 RELEASE GATE FAILED — $FAIL gate(s) did not pass"
  echo "     DO NOT tag v1.0 until all gates pass."
  exit 1
else
  echo ""
  echo "  ✅ ALL v1.0 RELEASE GATES PASSED"
  echo "     Ready to tag v1.0.0"
  exit 0
fi
