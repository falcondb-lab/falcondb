#!/usr/bin/env bash
# ============================================================================
# FalconDB — Kernel Benchmark (falcon_bench)
# ============================================================================
# Standardized internal kernel benchmarking using the falcon_bench harness.
# This measures raw storage engine + transaction manager performance,
# NOT SQL-layer throughput. See docs/benchmark_methodology.md for details.
#
# Usage:
#   ./scripts/bench_kernel_falcon_bench.sh
#   BENCH_OPS=50000 BENCH_THREADS=8 ./scripts/bench_kernel_falcon_bench.sh
#
# Environment variables:
#   BENCH_OPS          — total operations (default: 10000)
#   BENCH_READ_PCT     — read percentage 0-100 (default: 50)
#   BENCH_LOCAL_PCT    — local txn percentage (default: 80)
#   BENCH_SHARDS       — logical shards (default: 4)
#   BENCH_RECORDS      — pre-loaded rows (default: 1000)
#   BENCH_THREADS      — concurrency (default: 1)
#   BENCH_SEED         — random seed for reproducibility (default: 42)
#   BENCH_ISOLATION    — rc | si (default: rc)
#   BENCH_EXPORT       — text | csv | json (default: json)
#   BENCH_COMPARE      — set to 1 for fast-path ON vs OFF comparison
#   BENCH_SCALEOUT     — set to 1 for scatter/gather scale-out benchmark
#   BENCH_TPCB         — set to 1 for TPC-B style benchmark
# ============================================================================
set -euo pipefail

# ── Defaults ────────────────────────────────────────────────────────────────
BENCH_OPS="${BENCH_OPS:-10000}"
BENCH_READ_PCT="${BENCH_READ_PCT:-50}"
BENCH_LOCAL_PCT="${BENCH_LOCAL_PCT:-80}"
BENCH_SHARDS="${BENCH_SHARDS:-4}"
BENCH_RECORDS="${BENCH_RECORDS:-1000}"
BENCH_THREADS="${BENCH_THREADS:-1}"
BENCH_SEED="${BENCH_SEED:-42}"
BENCH_ISOLATION="${BENCH_ISOLATION:-rc}"
BENCH_EXPORT="${BENCH_EXPORT:-json}"
BENCH_COMPARE="${BENCH_COMPARE:-0}"
BENCH_SCALEOUT="${BENCH_SCALEOUT:-0}"
BENCH_TPCB="${BENCH_TPCB:-0}"

TS=$(date -u +"%Y%m%dT%H%M%SZ")
OUT_DIR="bench_out/${TS}/kernel_bench"

# ── Output directory ───────────────────────────────────────────────────────
mkdir -p "${OUT_DIR}"

echo "============================================================"
echo "FalconDB — Kernel Benchmark (falcon_bench)"
echo "============================================================"
echo "Timestamp : ${TS}"
echo "Output    : ${OUT_DIR}/"
echo "Ops       : ${BENCH_OPS}  Read%: ${BENCH_READ_PCT}  Local%: ${BENCH_LOCAL_PCT}"
echo "Shards    : ${BENCH_SHARDS}  Records: ${BENCH_RECORDS}  Threads: ${BENCH_THREADS}"
echo "Isolation : ${BENCH_ISOLATION}  Seed: ${BENCH_SEED}"
echo "============================================================"

# ── Environment snapshot ──────────────────────────────────────────────────
{
  echo "=== Kernel Bench Environment ==="
  echo "Timestamp: ${TS}"
  echo "Git commit: $(git rev-parse HEAD 2>/dev/null || echo 'unknown')"
  uname -a 2>/dev/null || echo "uname unavailable"
  if command -v lscpu &>/dev/null; then
    lscpu 2>/dev/null | head -10
  fi
} > "${OUT_DIR}/environment.txt"

# ── Build extra flags ─────────────────────────────────────────────────────
EXTRA_FLAGS=""
if [ "${BENCH_COMPARE}" = "1" ]; then
  EXTRA_FLAGS="${EXTRA_FLAGS} --compare"
fi
if [ "${BENCH_SCALEOUT}" = "1" ]; then
  EXTRA_FLAGS="${EXTRA_FLAGS} --scaleout"
fi
if [ "${BENCH_TPCB}" = "1" ]; then
  EXTRA_FLAGS="${EXTRA_FLAGS} --tpcb"
fi

# ── Run benchmark ─────────────────────────────────────────────────────────
BENCH_CMD="cargo run -p falcon_bench --release -- \
  --ops ${BENCH_OPS} \
  --read-pct ${BENCH_READ_PCT} \
  --local-pct ${BENCH_LOCAL_PCT} \
  --shards ${BENCH_SHARDS} \
  --record-count ${BENCH_RECORDS} \
  --threads ${BENCH_THREADS} \
  --seed ${BENCH_SEED} \
  --isolation ${BENCH_ISOLATION} \
  --export ${BENCH_EXPORT} \
  ${EXTRA_FLAGS}"

echo ""
echo "CMD: ${BENCH_CMD}"
echo "${BENCH_CMD}" > "${OUT_DIR}/command.txt"
echo ""

# Run and capture output
eval "${BENCH_CMD}" 2>&1 | tee "${OUT_DIR}/raw_output.log"
BENCH_EXIT=$?

# ── Parse JSON output if available ────────────────────────────────────────
if [ "${BENCH_EXPORT}" = "json" ]; then
  # Extract JSON from output (last JSON block)
  # falcon_bench prints JSON to stdout when --export json
  if grep -q '{' "${OUT_DIR}/raw_output.log" 2>/dev/null; then
    # Extract from first { to last }
    sed -n '/{/,/}/p' "${OUT_DIR}/raw_output.log" > "${OUT_DIR}/summary_kernel.json" 2>/dev/null || true
    echo ""
    echo "  JSON summary: ${OUT_DIR}/summary_kernel.json"
  fi
fi

# ── Generate summary ──────────────────────────────────────────────────────
{
  echo "# Kernel Benchmark Summary"
  echo ""
  echo "**Date**: ${TS}"
  echo "**Git Commit**: $(git rev-parse HEAD 2>/dev/null || echo 'unknown')"
  echo ""
  echo "## Parameters"
  echo ""
  echo "| Parameter   | Value            |"
  echo "|------------|------------------|"
  echo "| Operations | ${BENCH_OPS}      |"
  echo "| Read %     | ${BENCH_READ_PCT} |"
  echo "| Local %    | ${BENCH_LOCAL_PCT} |"
  echo "| Shards     | ${BENCH_SHARDS}   |"
  echo "| Records    | ${BENCH_RECORDS}  |"
  echo "| Threads    | ${BENCH_THREADS}  |"
  echo "| Seed       | ${BENCH_SEED}     |"
  echo "| Isolation  | ${BENCH_ISOLATION} |"
  echo ""
  echo "## Command"
  echo ""
  echo '```bash'
  echo "${BENCH_CMD}"
  echo '```'
  echo ""
  echo "## Raw Output"
  echo ""
  echo '```'
  cat "${OUT_DIR}/raw_output.log"
  echo '```'
  echo ""
  echo "## Notes"
  echo ""
  echo "- This is an **internal kernel benchmark**, not a SQL-layer benchmark."
  echo "- It measures raw StorageEngine + TxnManager throughput and latency."
  echo "- Not comparable to pgbench results. See docs/benchmark_methodology.md."
  echo "- Exit code: ${BENCH_EXIT}"
} > "${OUT_DIR}/REPORT.md"

echo ""
echo "============================================================"
echo "DONE — Results in: ${OUT_DIR}/"
echo "  REPORT.md            — Summary report"
echo "  raw_output.log       — Full benchmark output"
echo "  summary_kernel.json  — Parsed metrics (if --export json)"
echo "  environment.txt      — System snapshot"
echo "============================================================"

exit "${BENCH_EXIT}"
