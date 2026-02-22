#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# B10: Isolation Module CI Gate — Crash Matrix + Verification
# ═══════════════════════════════════════════════════════════════════════
#
# Runs the full isolation module test suite (B1–B9) and verifies
# all kernel hardening invariants hold. Exit 1 on any failure.
#
# Usage:
#   ./scripts/ci_isolation_gate.sh
#
# Environment variables (optional):
#   FALCON_TEST_THREADS  — number of test threads (default: auto)
# ═══════════════════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

THREADS="${FALCON_TEST_THREADS:-}"
THREAD_FLAG=""
if [ -n "$THREADS" ]; then
  THREAD_FLAG="-- --test-threads=$THREADS"
fi

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

# ── B1: Txn state machine transitions ──
run_gate "B1: Txn state machine" \
  "cargo test -p falcon_txn txn_state_machine $THREAD_FLAG"

# ── B2: In-doubt resolver ──
run_gate "B2: In-doubt resolver" \
  "cargo test -p falcon_cluster indoubt_resolver $THREAD_FLAG"

# ── B3: Snapshot Isolation litmus ──
run_gate "B3: SI litmus tests" \
  "cargo test -p falcon_storage si_litmus $THREAD_FLAG"

# ── B4: WAL durability + replay idempotency ──
run_gate "B4: WAL recovery" \
  "cargo test -p falcon_storage recovery_tests $THREAD_FLAG"

# ── B5/B6: Admission backpressure ──
run_gate "B5/B6: Admission control" \
  "cargo test -p falcon_txn admission_backpressure $THREAD_FLAG"

# ── B6: Memory budget admission (existing) ──
run_gate "B6: Memory budget" \
  "cargo test -p falcon_txn test_try_begin $THREAD_FLAG"

# ── B7: Long-txn detection + kill ──
run_gate "B7: Long-txn detection" \
  "cargo test -p falcon_txn long_txn_detection $THREAD_FLAG"

# ── B8: PG protocol corner cases ──
run_gate "B8: PG protocol" \
  "cargo test -p falcon_protocol_pg test_b8 $THREAD_FLAG"

# ── B9: DDL concurrency safety ──
run_gate "B9: DDL concurrency" \
  "cargo test -p falcon_storage ddl_concurrency $THREAD_FLAG"

# ── Full workspace build check ──
run_gate "Workspace build" \
  "cargo build --workspace"

# ── Summary ──
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  ISOLATION GATE SUMMARY"
echo "════════════════════════════════════════════════════════════════"
echo "  Total gates: $TOTAL"
echo "  Passed:      $PASS"
echo "  Failed:      $FAIL"
echo "════════════════════════════════════════════════════════════════"

if [ "$FAIL" -gt 0 ]; then
  echo "  ❌ ISOLATION GATE FAILED — $FAIL gate(s) did not pass"
  exit 1
else
  echo "  ✅ ALL ISOLATION GATES PASSED"
  exit 0
fi
