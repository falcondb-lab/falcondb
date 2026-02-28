#!/usr/bin/env bash
# ============================================================================
# FalconDB — CI Performance Regression Gate
# ============================================================================
# Runs the YCSB-style kernel benchmark (falcon_bench) and checks results
# against a committed baseline. Fails CI if TPS or P99 latency regresses
# beyond the configured thresholds.
#
# Design:
#   1. Build falcon_bench in release mode.
#   2. Run the benchmark with a fixed seed for reproducibility.
#   3. If bench/perf_baseline.json exists, use --baseline-check to gate.
#   4. Always write bench/perf_current.json for artifact upload.
#   5. Print a human-readable summary table.
#
# Environment variables (all optional):
#   PERF_OPS            — operations per run        (default: 5000)
#   PERF_READ_PCT       — read percentage 0-100     (default: 50)
#   PERF_LOCAL_PCT      — local txn percentage      (default: 80)
#   PERF_SHARDS         — logical shard count       (default: 4)
#   PERF_RECORDS        — pre-loaded row count      (default: 1000)
#   PERF_THREADS        — concurrency level         (default: 1)
#   PERF_SEED           — deterministic RNG seed    (default: 42)
#   PERF_ISOLATION      — rc | si                   (default: rc)
#   PERF_TPS_THRESHOLD  — TPS regression %          (default: 15)
#   PERF_P99_THRESHOLD  — P99 regression %          (default: 25)
#   PERF_BASELINE_PATH  — path to baseline JSON     (default: bench/perf_baseline.json)
#   PERF_SKIP_GATE      — set to 1 to skip check    (default: 0)
#
# Exit codes:
#   0 — benchmark passed (or gate skipped)
#   1 — performance regression detected or benchmark failed
# ============================================================================
set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
PERF_OPS="${PERF_OPS:-5000}"
PERF_READ_PCT="${PERF_READ_PCT:-50}"
PERF_LOCAL_PCT="${PERF_LOCAL_PCT:-80}"
PERF_SHARDS="${PERF_SHARDS:-4}"
PERF_RECORDS="${PERF_RECORDS:-1000}"
PERF_THREADS="${PERF_THREADS:-1}"
PERF_SEED="${PERF_SEED:-42}"
PERF_ISOLATION="${PERF_ISOLATION:-rc}"
PERF_TPS_THRESHOLD="${PERF_TPS_THRESHOLD:-15}"
PERF_P99_THRESHOLD="${PERF_P99_THRESHOLD:-25}"
PERF_BASELINE_PATH="${PERF_BASELINE_PATH:-bench/perf_baseline.json}"
PERF_SKIP_GATE="${PERF_SKIP_GATE:-0}"

CURRENT_PATH="bench/perf_current.json"
TS=$(date -u +"%Y%m%dT%H%M%SZ")

mkdir -p bench

echo "============================================================"
echo "FalconDB — CI Performance Regression Gate"
echo "============================================================"
echo "Timestamp  : ${TS}"
echo "Git commit : $(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"
echo "Ops        : ${PERF_OPS}  Read%: ${PERF_READ_PCT}  Local%: ${PERF_LOCAL_PCT}"
echo "Shards     : ${PERF_SHARDS}  Records: ${PERF_RECORDS}  Threads: ${PERF_THREADS}"
echo "Seed       : ${PERF_SEED}  Isolation: ${PERF_ISOLATION}"
echo "TPS gate   : -${PERF_TPS_THRESHOLD}%   P99 gate: +${PERF_P99_THRESHOLD}%"
echo "Baseline   : ${PERF_BASELINE_PATH}"
echo "============================================================"
echo ""

# ── Step 1: Build ────────────────────────────────────────────────────────────
echo ">> Building falcon_bench (release)..."
cargo build -p falcon_bench --release
echo "   Build OK"
echo ""

# ── Step 2: Run benchmark ─────────────────────────────────────────────────────
echo ">> Running YCSB benchmark (seed=${PERF_SEED}, ops=${PERF_OPS})..."
echo ""

BENCH_ARGS=(
    --ops        "${PERF_OPS}"
    --read-pct   "${PERF_READ_PCT}"
    --local-pct  "${PERF_LOCAL_PCT}"
    --shards     "${PERF_SHARDS}"
    --record-count "${PERF_RECORDS}"
    --threads    "${PERF_THREADS}"
    --seed       "${PERF_SEED}"
    --isolation  "${PERF_ISOLATION}"
    --export     json
)

# Capture raw output; also tee to terminal
RAW_OUTPUT=$(cargo run -p falcon_bench --release -- "${BENCH_ARGS[@]}" 2>&1)
echo "${RAW_OUTPUT}"
echo ""

# Extract the JSON block (first complete {...} object) from output
JSON_BLOCK=$(echo "${RAW_OUTPUT}" | python3 -c "
import sys, json, re
text = sys.stdin.read()
# Find first JSON object in output
m = re.search(r'\{[\s\S]*\}', text)
if not m:
    sys.exit(1)
try:
    obj = json.loads(m.group(0))
    print(json.dumps(obj))
except Exception:
    sys.exit(1)
" 2>/dev/null) || {
    echo "ERROR: Could not parse JSON output from falcon_bench."
    echo "       Make sure --export json produces valid JSON."
    exit 1
}

echo "${JSON_BLOCK}" > "${CURRENT_PATH}"
echo ">> Current results written to: ${CURRENT_PATH}"
echo ""

# ── Step 3: Print summary table ───────────────────────────────────────────────
TPS=$(echo "${JSON_BLOCK}"   | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['tps']:.1f}\")")
P50=$(echo "${JSON_BLOCK}"   | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['latency']['all']['p50_us'])")
P95=$(echo "${JSON_BLOCK}"   | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['latency']['all']['p95_us'])")
P99=$(echo "${JSON_BLOCK}"   | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['latency']['all']['p99_us'])")
COMMITTED=$(echo "${JSON_BLOCK}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['committed'])")
ABORTED=$(echo "${JSON_BLOCK}"   | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['aborted'])")

echo "┌─────────────────────────────────────────┐"
echo "│  Benchmark Results (current run)        │"
echo "├──────────────────┬──────────────────────┤"
printf "│  %-16s│  %-20s│\n" "Metric"       "Value"
echo "├──────────────────┼──────────────────────┤"
printf "│  %-16s│  %-20s│\n" "TPS"          "${TPS}"
printf "│  %-16s│  %-20s│\n" "P50 latency"  "${P50} µs"
printf "│  %-16s│  %-20s│\n" "P95 latency"  "${P95} µs"
printf "│  %-16s│  %-20s│\n" "P99 latency"  "${P99} µs"
printf "│  %-16s│  %-20s│\n" "Committed"    "${COMMITTED}"
printf "│  %-16s│  %-20s│\n" "Aborted"      "${ABORTED}"
echo "└──────────────────┴──────────────────────┘"
echo ""

# ── Step 4: Regression gate ───────────────────────────────────────────────────
if [ "${PERF_SKIP_GATE}" = "1" ]; then
    echo ">> Gate skipped (PERF_SKIP_GATE=1)"
    exit 0
fi

if [ ! -f "${PERF_BASELINE_PATH}" ]; then
    echo ">> No baseline found at '${PERF_BASELINE_PATH}'."
    echo "   Saving current run as new baseline."
    cp "${CURRENT_PATH}" "${PERF_BASELINE_PATH}"
    echo "   Baseline saved. Future runs will be compared against this."
    exit 0
fi

echo ">> Checking against baseline: ${PERF_BASELINE_PATH}"
echo ""

REGRESSION_FOUND=0

# Use python3 for float arithmetic (available on all GitHub-hosted runners)
RESULT=$(python3 - <<PYEOF
import json, sys

with open("${PERF_BASELINE_PATH}") as f:
    base = json.load(f)
with open("${CURRENT_PATH}") as f:
    curr = json.load(f)

tps_threshold = ${PERF_TPS_THRESHOLD}
p99_threshold = ${PERF_P99_THRESHOLD}

base_tps  = float(base["tps"])
curr_tps  = float(curr["tps"])
base_p99  = int(base["latency"]["all"]["p99_us"])
curr_p99  = int(curr["latency"]["all"]["p99_us"])

tps_floor = base_tps * (1.0 - tps_threshold / 100.0)
p99_ceil  = base_p99 * (1.0 + p99_threshold / 100.0)

lines = []
failed = False

tps_delta_pct = (curr_tps - base_tps) / base_tps * 100.0
p99_delta_pct = (curr_p99 - base_p99) / base_p99 * 100.0 if base_p99 > 0 else 0.0

lines.append(f"  TPS   : current={curr_tps:.1f}  baseline={base_tps:.1f}  delta={tps_delta_pct:+.1f}%  floor={tps_floor:.1f}  threshold=-{tps_threshold}%")
lines.append(f"  P99   : current={curr_p99}µs   baseline={base_p99}µs   delta={p99_delta_pct:+.1f}%  ceil={p99_ceil:.0f}µs   threshold=+{p99_threshold}%")

if curr_tps < tps_floor:
    lines.append(f"  FAIL  TPS regression: {curr_tps:.1f} < floor {tps_floor:.1f} (dropped {abs(tps_delta_pct):.1f}%)")
    failed = True
else:
    lines.append(f"  OK    TPS within threshold")

if curr_p99 > p99_ceil:
    lines.append(f"  FAIL  P99 regression: {curr_p99}µs > ceil {p99_ceil:.0f}µs (rose {p99_delta_pct:.1f}%)")
    failed = True
else:
    lines.append(f"  OK    P99 within threshold")

print("\n".join(lines))
sys.exit(1 if failed else 0)
PYEOF
)
REGRESSION_FOUND=$?

echo "${RESULT}"
echo ""

if [ "${REGRESSION_FOUND}" -ne 0 ]; then
    echo "============================================================"
    echo "PERF REGRESSION GATE: FAILED"
    echo ""
    echo "Performance has regressed beyond the allowed thresholds."
    echo "To update the baseline after an intentional change, run:"
    echo ""
    echo "  PERF_SKIP_GATE=1 ./scripts/ci_perf_regression.sh"
    echo "  cp bench/perf_current.json bench/perf_baseline.json"
    echo "  git add bench/perf_baseline.json && git commit -m 'perf: update baseline'"
    echo "============================================================"
    exit 1
fi

echo "============================================================"
echo "PERF REGRESSION GATE: PASSED"
echo "============================================================"
exit 0
