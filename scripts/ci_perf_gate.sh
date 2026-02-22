#!/usr/bin/env bash
# ============================================================================
# FalconDB — CI Performance Gate (P6-1)
# ============================================================================
# Runs falcon_bench workloads and checks performance invariants:
#   Gate 1: YCSB baseline regression (TPS floor + P99 ceiling)
#   Gate 2: Fast-path vs slow-path invariant (fast P99 <= slow P99)
#   Gate 3: Scale-out monotonicity (TPS non-decreasing with shards)
#   Gate 4: Failover data integrity
#   Gate 5: Long-run stability (short 30s smoke)
#
# Usage:
#   ./scripts/ci_perf_gate.sh                    # Run all gates
#   ./scripts/ci_perf_gate.sh --save-baseline    # Save new baseline
#   ./scripts/ci_perf_gate.sh --fast             # Gates 1-4 only (no long-run)
#
# Environment:
#   FALCON_PERF_OPS        Operations per benchmark (default: 5000)
#   FALCON_PERF_RECORDS    Record count (default: 500)
#   FALCON_PERF_SEED       Random seed (default: 42)
#   FALCON_PERF_TPS_PCT    TPS regression threshold % (default: 10)
#   FALCON_PERF_P99_PCT    P99 regression threshold % (default: 20)
#
# Exit codes:
#   0 — all gates passed
#   1 — regression or invariant violation detected
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

OPS="${FALCON_PERF_OPS:-5000}"
RECORDS="${FALCON_PERF_RECORDS:-500}"
SEED="${FALCON_PERF_SEED:-42}"
TPS_PCT="${FALCON_PERF_TPS_PCT:-10}"
P99_PCT="${FALCON_PERF_P99_PCT:-20}"
BASELINE_PATH="$SCRIPT_DIR/falcon_perf_baseline.json"
SAVE_BASELINE=false
FAST_MODE=false

PASS=0
FAIL=0
WARN=0
FAIL_NAMES=()

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --save-baseline) SAVE_BASELINE=true; shift ;;
        --fast) FAST_MODE=true; shift ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

gate_pass() { echo -e "${GREEN}[PASS]${NC} $1"; PASS=$((PASS + 1)); }
gate_fail() { echo -e "${RED}[FAIL]${NC} $1"; FAIL=$((FAIL + 1)); FAIL_NAMES+=("$1"); }
gate_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; WARN=$((WARN + 1)); }
gate_info() { echo -e "${CYAN}[INFO]${NC} $1"; }

echo "============================================================"
echo -e " ${BOLD}FalconDB CI Performance Gate${NC}"
echo " $(date -Iseconds 2>/dev/null || date)"
echo " ops=$OPS records=$RECORDS seed=$SEED"
echo " tps_threshold=-${TPS_PCT}% p99_threshold=+${P99_PCT}%"
echo "============================================================"

cd "$PROJECT_DIR"

# ── Build ──────────────────────────────────────────────────────────────
gate_info "Building falcon_bench (release)..."
if ! cargo build --release -p falcon_bench 2>&1 | tail -3; then
    gate_fail "Build failed"
    exit 1
fi
gate_pass "Release build"
BENCH="./target/release/falcon_bench"

# ═══════════════════════════════════════════════════════════════════════
# Gate 1: YCSB Baseline Regression
# ═══════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}── Gate 1: YCSB Baseline Regression ──${NC}"

if [ "$SAVE_BASELINE" = true ]; then
    gate_info "Saving new baseline..."
    $BENCH --ops "$OPS" --record-count "$RECORDS" --seed "$SEED" \
           --shards 4 --local-pct 80 --read-pct 50 \
           --baseline-save --baseline-path "$BASELINE_PATH" \
           --export text
    gate_pass "Baseline saved to $BASELINE_PATH"
else
    if [ -f "$BASELINE_PATH" ]; then
        gate_info "Checking against baseline..."
        OUTPUT=$($BENCH --ops "$OPS" --record-count "$RECORDS" --seed "$SEED" \
                        --shards 4 --local-pct 80 --read-pct 50 \
                        --baseline-check --baseline-path "$BASELINE_PATH" \
                        --tps-threshold-pct "$TPS_PCT" --p99-threshold-pct "$P99_PCT" \
                        --export text 2>&1)
        echo "$OUTPUT"
        if echo "$OUTPUT" | grep -q "PERF REGRESSION\|INVARIANT VIOLATION"; then
            gate_fail "YCSB baseline regression"
        else
            gate_pass "YCSB baseline check"
        fi
    else
        gate_warn "No baseline file — creating initial baseline"
        $BENCH --ops "$OPS" --record-count "$RECORDS" --seed "$SEED" \
               --shards 4 --local-pct 80 --read-pct 50 \
               --baseline-save --baseline-path "$BASELINE_PATH" \
               --export text
        gate_pass "Initial baseline created"
    fi
fi

# ═══════════════════════════════════════════════════════════════════════
# Gate 2: Fast-Path vs Slow-Path Invariant
# ═══════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}── Gate 2: Fast-Path vs Slow-Path ──${NC}"

OUTPUT=$($BENCH --ops "$OPS" --record-count "$RECORDS" --seed "$SEED" \
                --shards 4 --local-pct 80 --compare --export text 2>&1)
echo "$OUTPUT"
if echo "$OUTPUT" | grep -q "INVARIANT VIOLATION"; then
    gate_fail "Fast-path vs slow-path invariant"
else
    gate_pass "Fast-path P99 <= slow-path P99"
fi

# ═══════════════════════════════════════════════════════════════════════
# Gate 3: Scale-Out Monotonicity
# ═══════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}── Gate 3: Scale-Out TPS Monotonicity ──${NC}"

OUTPUT=$($BENCH --ops "$OPS" --record-count "$RECORDS" --seed "$SEED" \
                --scaleout --export text 2>&1)
echo "$OUTPUT"
if echo "$OUTPUT" | grep -q "SCALE-OUT WARNING"; then
    gate_warn "Scale-out TPS not strictly monotonic (sub-linear allowed)"
else
    gate_pass "Scale-out TPS monotonically non-decreasing"
fi

# ═══════════════════════════════════════════════════════════════════════
# Gate 4: Failover Data Integrity
# ═══════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}── Gate 4: Failover Data Integrity ──${NC}"

OUTPUT=$($BENCH --ops "$OPS" --record-count "$RECORDS" --seed "$SEED" \
                --failover --export text 2>&1)
echo "$OUTPUT"
if echo "$OUTPUT" | grep -q "Data intact:.*true"; then
    gate_pass "Failover data integrity"
else
    gate_fail "Failover data integrity — data loss detected"
fi

# ═══════════════════════════════════════════════════════════════════════
# Gate 5: Long-Run Stability Smoke (30s)
# ═══════════════════════════════════════════════════════════════════════
if [ "$FAST_MODE" = false ]; then
    echo ""
    echo -e "${BOLD}── Gate 5: Long-Run Stability (30s smoke) ──${NC}"

    OUTPUT=$($BENCH --long-run --duration 30s --sample-interval-secs 5 \
                    --record-count "$RECORDS" --seed "$SEED" \
                    --shards 4 --export text 2>&1)
    EXIT_CODE=$?
    echo "$OUTPUT"
    if [ "$EXIT_CODE" -ne 0 ]; then
        gate_fail "Long-run stability (TPS variance too high)"
    else
        gate_pass "Long-run stability (30s smoke)"
    fi
fi

# ═══════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════
echo ""
echo "============================================================"
echo -e " ${BOLD}Passed${NC}: $PASS"
echo -e " ${BOLD}Failed${NC}: $FAIL"
echo -e " ${BOLD}Warnings${NC}: $WARN"
echo "============================================================"

if [ ${#FAIL_NAMES[@]} -gt 0 ]; then
    echo -e "${RED}Failed gates:${NC}"
    for name in "${FAIL_NAMES[@]}"; do
        echo -e "  ${RED}✗${NC} $name"
    done
fi

echo ""
if [ "$FAIL" -gt 0 ]; then
    echo -e "${BOLD}${RED}PERF GATE: FAILED${NC}"
    exit 1
else
    if [ "$WARN" -gt 0 ]; then
        echo -e "${BOLD}${YELLOW}PERF GATE: PASSED with warnings${NC}"
    else
        echo -e "${BOLD}${GREEN}PERF GATE: PASSED${NC}"
    fi
    exit 0
fi
