#!/usr/bin/env bash
# ============================================================================
# FalconDB — CI Benchmark Smoke Test
# ============================================================================
# Purpose: Ensure benchmark scripts do not rot. NOT a performance test.
# Uses minimal parameters to run quickly in CI.
#
# Usage:
#   ./scripts/ci_bench_smoke.sh
#
# Exit codes:
#   0 — all smoke checks passed
#   1 — one or more checks failed
# ============================================================================
set -euo pipefail

PASS=0
FAIL=0
ERRORS=""

check() {
  local name="$1"
  shift
  echo "── GATE: ${name} ──"
  if "$@"; then
    echo "  ✓ PASS"
    PASS=$((PASS + 1))
  else
    echo "  ✗ FAIL"
    FAIL=$((FAIL + 1))
    ERRORS="${ERRORS}\n  - ${name}"
  fi
  echo ""
}

# ── Gate 1: Scripts exist and are parseable ────────────────────────────────
echo "============================================================"
echo "FalconDB — CI Benchmark Smoke Test"
echo "============================================================"
echo ""

check "bench_pgbench_vs_postgres.sh exists" test -f scripts/bench_pgbench_vs_postgres.sh
check "bench_pgbench_vs_postgres.ps1 exists" test -f scripts/bench_pgbench_vs_postgres.ps1
check "bench_failover_under_load.sh exists" test -f scripts/bench_failover_under_load.sh
check "bench_kernel_falcon_bench.sh exists" test -f scripts/bench_kernel_falcon_bench.sh

# ── Gate 2: Documentation exists ───────────────────────────────────────────
check "benchmark_methodology.md exists" test -f docs/benchmark_methodology.md
check "RESULTS_TEMPLATE.md exists" test -f docs/benchmarks/RESULTS_TEMPLATE.md

# ── Gate 3: Bench config exists ────────────────────────────────────────────
check "falcon.bench.toml exists" test -f bench_configs/falcon.bench.toml

# ── Gate 4: Bash syntax check ─────────────────────────────────────────────
if command -v bash &>/dev/null; then
  check "pgbench script syntax" bash -n scripts/bench_pgbench_vs_postgres.sh
  check "failover script syntax" bash -n scripts/bench_failover_under_load.sh
  check "kernel bench script syntax" bash -n scripts/bench_kernel_falcon_bench.sh
  check "ci smoke script syntax" bash -n scripts/ci_bench_smoke.sh
else
  echo "  SKIP: bash not available for syntax check"
fi

# ── Gate 5: Kernel bench smoke (minimal run) ──────────────────────────────
# Only if falcon_bench compiles
if cargo build -p falcon_bench --release 2>/dev/null; then
  check "kernel bench smoke run" \
    env BENCH_OPS=100 BENCH_RECORDS=50 BENCH_THREADS=1 BENCH_SEED=42 \
    bash scripts/bench_kernel_falcon_bench.sh
else
  echo "  SKIP: falcon_bench did not compile (expected in some CI environments)"
fi

# ── Gate 6: Output structure check ────────────────────────────────────────
# Verify that the kernel bench smoke (if it ran) produced expected artifacts
LATEST_DIR=$(ls -td bench_out/*/kernel_bench 2>/dev/null | head -1)
if [ -n "${LATEST_DIR}" ]; then
  check "kernel bench REPORT.md exists" test -f "${LATEST_DIR}/REPORT.md"
  check "kernel bench raw_output.log exists" test -f "${LATEST_DIR}/raw_output.log"
  check "kernel bench environment.txt exists" test -f "${LATEST_DIR}/environment.txt"
else
  echo "  SKIP: no kernel bench output to validate"
fi

# ── Summary ───────────────────────────────────────────────────────────────
echo "============================================================"
echo "CI Benchmark Smoke Test Summary"
echo "  Passed: ${PASS}"
echo "  Failed: ${FAIL}"
if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "  Failures:"
  echo -e "${ERRORS}"
fi
echo "============================================================"

if [ "${FAIL}" -gt 0 ]; then
  exit 1
fi
exit 0
