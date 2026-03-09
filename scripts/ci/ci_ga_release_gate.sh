#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# FalconDB GA Release Gate — Go / No-Go
# ═══════════════════════════════════════════════════════════════════════
#
# This script runs ALL gates required for a GA (stable) release tag.
# Any single failure = merge blocked / tag forbidden.
#
# Gate mapping:
#   P0-1  Transaction correctness     — ga_release_gate tests
#   P0-2  WAL durability + crash      — ga_release_gate + consistency_wal tests
#   P0-3  Failover behavior           — ga_release_gate + failover_determinism tests
#   P0-4  Memory safety (OOM)         — ga_release_gate + memory_backpressure tests
#   P0-5  Start/Stop/Restart safety   — ga_release_gate tests
#   P1-1  SQL boundary                — handler SQL tests + unsupported error tests
#   P1-2  Observability               — SHOW command tests
#   PREV  Pre-existing gates          — v1.0 release gate, isolation, storage
#
# Usage:
#   ./scripts/ci_ga_release_gate.sh
#
# Environment:
#   FALCON_SKIP_FAILOVER=1  — skip failover gate (requires multi-process)
#   FALCON_SKIP_PERF=1      — skip performance gate (for fast CI)
#   FALCON_SKIP_PREV=1      — skip pre-existing v1.0 gates
# ═══════════════════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

PASS=0
FAIL=0
TOTAL=0
FAILED_GATES=""

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
    FAILED_GATES="$FAILED_GATES\n  - $name"
  fi
}

echo "═══════════════════════════════════════════════════════════════"
echo "  FalconDB GA Release Gate"
echo "  $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
echo "═══════════════════════════════════════════════════════════════"

# ── Build gate ────────────────────────────────────────────────────────
run_gate "Workspace builds clean" \
  "cargo build --workspace 2>&1"

# ── P0-1: Transaction correctness ─────────────────────────────────────
run_gate "P0-1: Transaction correctness (GA gate tests)" \
  "cargo test --test ga_release_gate ga_p0_1 -- --test-threads=1 2>&1"

# ── P0-2: WAL durability & crash recovery ─────────────────────────────
run_gate "P0-2: WAL durability (GA gate tests)" \
  "cargo test --test ga_release_gate ga_p0_2 -- --test-threads=1 2>&1"

run_gate "P0-2: WAL consistency invariants" \
  "cargo test -p falcon_storage --test consistency_wal -- --test-threads=1 2>&1"

# ── P0-3: Failover behavior ──────────────────────────────────────────
run_gate "P0-3: Failover behavior (GA gate tests)" \
  "cargo test --test ga_release_gate ga_p0_3 -- --test-threads=1 2>&1"

if [ "${FALCON_SKIP_FAILOVER:-0}" != "1" ]; then
  run_gate "P0-3: Failover determinism matrix" \
    "cargo test -p falcon_cluster --test failover_determinism -- --test-threads=1 2>&1"
fi

# ── P0-4: Memory safety ──────────────────────────────────────────────
run_gate "P0-4: Memory OOM protection (GA gate tests)" \
  "cargo test --test ga_release_gate ga_p0_4 -- --test-threads=1 2>&1"

run_gate "P0-4: Memory backpressure integration" \
  "cargo test -p falcon_storage --test memory_backpressure -- --test-threads=1 2>&1"

# ── P0-5: Start/Stop/Restart safety ──────────────────────────────────
run_gate "P0-5: Restart safety (GA gate tests)" \
  "cargo test --test ga_release_gate ga_p0_5 -- --test-threads=1 2>&1"

# ── P1-1: SQL boundary ───────────────────────────────────────────────
run_gate "P1-1: SQL whitelist + unsupported errors" \
  "cargo test -p falcon_protocol_pg test_sql -- --test-threads=1 2>&1"

run_gate "P1-1: Unsupported features return errors" \
  "cargo test -p falcon_protocol_pg test_unsupported -- --test-threads=1 2>&1"

# ── P1-2: Observability ──────────────────────────────────────────────
run_gate "P1-2: SHOW observability commands" \
  "cargo test -p falcon_protocol_pg test_show -- --test-threads=1 2>&1"

# ── Clippy (zero warnings) ───────────────────────────────────────────
run_gate "Clippy (zero warnings)" \
  "cargo clippy --workspace -- -D warnings 2>&1"

# ── Full test suite ──────────────────────────────────────────────────
run_gate "Full workspace test suite" \
  "cargo test --workspace -- --test-threads=4 2>&1"

# ── Pre-existing gates ───────────────────────────────────────────────
if [ "${FALCON_SKIP_PREV:-0}" != "1" ]; then
  if [ -x "$SCRIPT_DIR/ci_isolation_gate.sh" ]; then
    run_gate "Isolation gate (B1-B10)" \
      "$SCRIPT_DIR/ci_isolation_gate.sh 2>&1"
  fi

  if [ -x "$SCRIPT_DIR/ci_storage_gate.sh" ]; then
    run_gate "Storage gate" \
      "$SCRIPT_DIR/ci_storage_gate.sh 2>&1"
  fi
fi

# ── Performance gate ─────────────────────────────────────────────────
if [ "${FALCON_SKIP_PERF:-0}" != "1" ]; then
  if [ -x "$SCRIPT_DIR/ci_perf_gate.sh" ]; then
    run_gate "Performance gate" \
      "$SCRIPT_DIR/ci_perf_gate.sh 2>&1"
  fi
fi

# ── Summary ──────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  GA RELEASE GATE SUMMARY"
echo "═══════════════════════════════════════════════════════════════"
echo "  Total:  $TOTAL"
echo "  Passed: $PASS"
echo "  Failed: $FAIL"

if [ "$FAIL" -gt 0 ]; then
  echo ""
  echo "  ❌ FAILED GATES:"
  echo -e "$FAILED_GATES"
  echo ""
  echo "  VERDICT: ❌ NO-GO — $FAIL gate(s) failed"
  echo "═══════════════════════════════════════════════════════════════"
  exit 1
else
  echo ""
  echo "  VERDICT: ✅ GO — All $TOTAL gates passed"
  echo "═══════════════════════════════════════════════════════════════"
  exit 0
fi
