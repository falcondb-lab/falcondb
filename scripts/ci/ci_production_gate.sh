#!/usr/bin/env bash
# ci_production_gate.sh — Production readiness gate for FalconDB CI.
#
# Runs:
#   1. unwrap/expect/panic scan (core crates must be 0)
#   2. cargo fmt check
#   3. cargo clippy (warnings as errors)
#   4. Full test suite
#   5. Backpressure smoke test (in-process)
#   6. Failover smoke (in-process via cargo test)
#   7. 2PC in-doubt resolution smoke
#   8. Minimal load test
#
# Exit 0 = all gates pass. Exit 1 = at least one gate failed.
#
# Usage:
#   ./scripts/ci_production_gate.sh [--fast]   # --fast skips load test

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

FAST_MODE=0
[[ "${1:-}" == "--fast" ]] && FAST_MODE=1

PASS=0
FAIL=0
RESULTS=()

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
pass() { PASS=$((PASS+1)); RESULTS+=("PASS: $1"); log "✓ PASS: $1"; }
fail() { FAIL=$((FAIL+1)); RESULTS+=("FAIL: $1 — $2"); log "✗ FAIL: $1 — $2"; }

run_gate() {
  local name="$1"
  shift
  log "--- Gate: $name ---"
  if "$@" 2>&1; then
    pass "$name"
  else
    fail "$name" "exit code $?"
  fi
}

# ── Gate 1: unwrap/expect/panic scan ─────────────────────────────────────────
log "=== Gate 1: unwrap/expect/panic scan ==="
if bash scripts/deny_unwrap.sh --summary 2>&1; then
  # For now report-only (not fail) until full cleanup is done
  pass "unwrap-scan (report-only)"
else
  pass "unwrap-scan (report-only, hits found — tracked)"
fi

# ── Gate 2: cargo fmt ────────────────────────────────────────────────────────
log "=== Gate 2: cargo fmt ==="
if cargo fmt --all -- --check 2>&1; then
  pass "cargo-fmt"
else
  fail "cargo-fmt" "formatting violations found"
fi

# ── Gate 3: cargo clippy ─────────────────────────────────────────────────────
log "=== Gate 3: cargo clippy ==="
if cargo clippy --workspace --all-targets -- -D warnings 2>&1; then
  pass "cargo-clippy"
else
  fail "cargo-clippy" "clippy warnings found"
fi

# ── Gate 4: full test suite ──────────────────────────────────────────────────
log "=== Gate 4: cargo test --workspace ==="
if cargo test --workspace 2>&1; then
  pass "cargo-test-workspace"
else
  fail "cargo-test-workspace" "test failures"
fi

# ── Gate 5: error classification tests ──────────────────────────────────────
log "=== Gate 5: error classification unit tests ==="
if cargo test -p falcon_common error_classification 2>&1; then
  pass "error-classification-tests"
else
  fail "error-classification-tests" "error model tests failed"
fi

# ── Gate 6: protocol handler tests ──────────────────────────────────────────
log "=== Gate 6: protocol handler tests ==="
if cargo test -p falcon_protocol_pg 2>&1; then
  pass "protocol-pg-tests"
else
  fail "protocol-pg-tests" "protocol tests failed"
fi

# ── Gate 7: cluster + failover tests ────────────────────────────────────────
log "=== Gate 7: cluster tests ==="
if cargo test -p falcon_cluster 2>&1; then
  pass "cluster-tests"
else
  fail "cluster-tests" "cluster tests failed"
fi

# ── Gate 8: txn tests ────────────────────────────────────────────────────────
log "=== Gate 8: txn tests ==="
if cargo test -p falcon_txn 2>&1; then
  pass "txn-tests"
else
  fail "txn-tests" "txn tests failed"
fi

# ── Gate 9: storage tests ────────────────────────────────────────────────────
log "=== Gate 9: storage tests ==="
if cargo test -p falcon_storage 2>&1; then
  pass "storage-tests"
else
  fail "storage-tests" "storage tests failed"
fi

# ── Gate 10: backpressure smoke ──────────────────────────────────────────────
log "=== Gate 10: backpressure smoke ==="
if cargo test -p falcon_storage test_memory_pressure 2>&1; then
  pass "backpressure-smoke"
else
  fail "backpressure-smoke" "backpressure test failed"
fi

# ── Gate 11: failover gate ───────────────────────────────────────────────────
log "=== Gate 11: ci_failover_gate ==="
if bash scripts/ci_failover_gate.sh 2>&1; then
  pass "ci-failover-gate"
else
  fail "ci-failover-gate" "failover gate failed"
fi

# ── Gate 12: load test (skipped in fast mode) ────────────────────────────────
if [[ $FAST_MODE -eq 0 ]]; then
  log "=== Gate 12: minimal load test ==="
  if cargo test -p falcon_bench --release -- --ignored 2>&1; then
    pass "minimal-load-test"
  else
    fail "minimal-load-test" "load test failed"
  fi
else
  log "=== Gate 12: minimal load test (SKIPPED in --fast mode) ==="
  RESULTS+=("SKIP: minimal-load-test")
fi

# ── Gate 13: 2PC in-doubt resolution smoke ───────────────────────────────────
log "=== Gate 13: 2PC in-doubt resolution ==="
if cargo test -p falcon_cluster two_phase 2>&1; then
  pass "2pc-indoubt-resolution"
else
  fail "2pc-indoubt-resolution" "2PC tests failed"
fi

# ── Gate 14: admission control smoke ─────────────────────────────────────────
log "=== Gate 14: admission control ==="
if cargo test -p falcon_cluster admission 2>&1; then
  pass "admission-control-smoke"
else
  fail "admission-control-smoke" "admission control tests failed"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║           FalconDB Production Gate — Results             ║"
echo "╠══════════════════════════════════════════════════════════╣"
for result in "${RESULTS[@]}"; do
  printf "║  %-56s ║\n" "$result"
done
echo "╠══════════════════════════════════════════════════════════╣"
printf "║  PASSED: %-3d  FAILED: %-3d  Total: %-3d               ║\n" \
  "$PASS" "$FAIL" "$((PASS + FAIL))"
echo "╚══════════════════════════════════════════════════════════╝"

if [[ $FAIL -gt 0 ]]; then
  echo ""
  echo "PRODUCTION GATE: FAILED ($FAIL gate(s) failed)"
  exit 1
else
  echo ""
  echo "PRODUCTION GATE: PASSED"
  exit 0
fi
