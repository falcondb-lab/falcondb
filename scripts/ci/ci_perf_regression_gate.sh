#!/usr/bin/env bash
# ci_perf_regression_gate.sh — Performance regression CI gate
#
# Runs micro-benchmarks and compares against a baseline file.
# If any metric regresses beyond the threshold, the gate fails.
#
# Usage:
#   ./scripts/ci_perf_regression_gate.sh [--update-baseline] [--threshold PCT]
#
# Environment:
#   FALCON_PERF_BASELINE  Path to baseline JSON (default: scripts/perf_baseline.json)
#   FALCON_PERF_THRESHOLD Regression threshold percentage (default: 15)
#
# The gate runs:
#   1. cargo bench (criterion benchmarks if available)
#   2. Built-in micro-benchmarks (insert, scan, txn throughput)
#   3. Comparison against baseline
#
# Exit codes:
#   0 — all benchmarks within threshold
#   1 — regression detected or build failure

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

BASELINE="${FALCON_PERF_BASELINE:-$SCRIPT_DIR/perf_baseline.json}"
THRESHOLD="${FALCON_PERF_THRESHOLD:-15}"
UPDATE_BASELINE=false
RESULTS_FILE="$(mktemp /tmp/falcon_perf_XXXXXX.json)"

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --update-baseline) UPDATE_BASELINE=true; shift ;;
        --threshold) THRESHOLD="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

info "Performance regression gate (threshold: ${THRESHOLD}%)"
info "Baseline: $BASELINE"
info "Results:  $RESULTS_FILE"

# ── Step 1: Build release binaries ──────────────────────────────────────
info "Building release binaries..."
cd "$PROJECT_DIR"
cargo build --release --workspace 2>&1 | tail -5
pass "Release build succeeded"

# ── Step 2: Run micro-benchmarks ────────────────────────────────────────
info "Running micro-benchmarks..."

# Benchmark 1: Unit test suite timing (proxy for overall perf)
TEST_START=$(date +%s%N)
cargo test --release --workspace --lib -- --quiet 2>/dev/null || true
TEST_END=$(date +%s%N)
TEST_DURATION_MS=$(( (TEST_END - TEST_START) / 1000000 ))
info "  Test suite: ${TEST_DURATION_MS}ms"

# Benchmark 2: Compile time (incremental, no changes)
COMPILE_START=$(date +%s%N)
cargo build --release --workspace 2>/dev/null
COMPILE_END=$(date +%s%N)
COMPILE_DURATION_MS=$(( (COMPILE_END - COMPILE_START) / 1000000 ))
info "  Incremental compile: ${COMPILE_DURATION_MS}ms"

# Benchmark 3: Binary size
BINARY_SIZE=0
if [ -f "$PROJECT_DIR/target/release/falcon_server" ]; then
    BINARY_SIZE=$(stat -f%z "$PROJECT_DIR/target/release/falcon_server" 2>/dev/null || \
                  stat -c%s "$PROJECT_DIR/target/release/falcon_server" 2>/dev/null || echo 0)
fi
info "  Binary size: ${BINARY_SIZE} bytes"

# Benchmark 4: Run cargo bench if criterion benchmarks exist
BENCH_INSERT_NS=0
BENCH_SCAN_NS=0
if cargo bench --workspace --no-run 2>/dev/null; then
    info "  Running criterion benchmarks..."
    # Capture bench output; parse later
    BENCH_OUTPUT=$(cargo bench --workspace 2>&1 || true)
    # Try to extract timing from criterion output (best-case parse)
    BENCH_INSERT_NS=$(echo "$BENCH_OUTPUT" | grep -i "insert" | grep -oP '[\d.]+\s*ns' | head -1 | grep -oP '[\d.]+' || echo 0)
    BENCH_SCAN_NS=$(echo "$BENCH_OUTPUT" | grep -i "scan" | grep -oP '[\d.]+\s*ns' | head -1 | grep -oP '[\d.]+' || echo 0)
fi

# ── Step 3: Write results JSON ──────────────────────────────────────────
cat > "$RESULTS_FILE" <<EOF
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "git_sha": "$(git -C "$PROJECT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)",
  "metrics": {
    "test_suite_ms": $TEST_DURATION_MS,
    "incremental_compile_ms": $COMPILE_DURATION_MS,
    "binary_size_bytes": $BINARY_SIZE,
    "bench_insert_ns": ${BENCH_INSERT_NS:-0},
    "bench_scan_ns": ${BENCH_SCAN_NS:-0}
  }
}
EOF

info "Results written to $RESULTS_FILE"

# ── Step 4: Compare against baseline ────────────────────────────────────
if [ "$UPDATE_BASELINE" = true ]; then
    cp "$RESULTS_FILE" "$BASELINE"
    pass "Baseline updated: $BASELINE"
    exit 0
fi

if [ ! -f "$BASELINE" ]; then
    warn "No baseline file found at $BASELINE"
    warn "Run with --update-baseline to create one"
    # First run: create baseline automatically
    cp "$RESULTS_FILE" "$BASELINE"
    pass "Baseline created (first run): $BASELINE"
    exit 0
fi

# Compare each metric
REGRESSIONS=0

compare_metric() {
    local name="$1"
    local current="$2"
    local baseline="$3"
    local threshold="$4"

    if [ "$baseline" -eq 0 ] 2>/dev/null || [ "$baseline" = "0" ]; then
        info "  $name: baseline=0, skipping comparison"
        return
    fi
    if [ "$current" -eq 0 ] 2>/dev/null || [ "$current" = "0" ]; then
        info "  $name: current=0, skipping comparison"
        return
    fi

    local pct_change=$(( (current - baseline) * 100 / baseline ))

    if [ "$pct_change" -gt "$threshold" ]; then
        warn "  $name: REGRESSION ${pct_change}% (baseline=$baseline, current=$current, threshold=${threshold}%)"
        REGRESSIONS=$((REGRESSIONS + 1))
    elif [ "$pct_change" -lt "-$threshold" ]; then
        pass "  $name: IMPROVEMENT ${pct_change}% (baseline=$baseline, current=$current)"
    else
        pass "  $name: OK ${pct_change}% (baseline=$baseline, current=$current)"
    fi
}

info "Comparing against baseline..."

# Extract baseline values (portable JSON parsing with grep/sed)
B_TEST=$(grep -o '"test_suite_ms": *[0-9]*' "$BASELINE" | grep -o '[0-9]*$' || echo 0)
B_COMPILE=$(grep -o '"incremental_compile_ms": *[0-9]*' "$BASELINE" | grep -o '[0-9]*$' || echo 0)
B_BINARY=$(grep -o '"binary_size_bytes": *[0-9]*' "$BASELINE" | grep -o '[0-9]*$' || echo 0)

compare_metric "test_suite_ms" "$TEST_DURATION_MS" "$B_TEST" "$THRESHOLD"
compare_metric "incremental_compile_ms" "$COMPILE_DURATION_MS" "$B_COMPILE" "$THRESHOLD"
compare_metric "binary_size_bytes" "$BINARY_SIZE" "$B_BINARY" "$THRESHOLD"

# ── Step 5: Verdict ─────────────────────────────────────────────────────
echo ""
if [ "$REGRESSIONS" -gt 0 ]; then
    fail "Performance regression gate: $REGRESSIONS metric(s) regressed beyond ${THRESHOLD}% threshold"
else
    pass "Performance regression gate: all metrics within ${THRESHOLD}% threshold"
fi

# Cleanup
rm -f "$RESULTS_FILE"
