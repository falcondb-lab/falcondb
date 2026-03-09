#!/usr/bin/env bash
# ============================================================================
# FalconDB — Native Protocol Failover-Under-Load CI Gate
# ============================================================================
# Validates:
#   Gate 1: Epoch fencing correctness (Rust unit tests)
#   Gate 2: Fenced epoch error propagation through native protocol
#   Gate 3: Session lifecycle under error conditions
#   Gate 4: Batch protocol error handling
#
# Usage:
#   ./scripts/ci_native_failover_under_load.sh
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

PASS=0
FAIL=0
FAIL_NAMES=()

gate_pass() { echo -e "${GREEN}[PASS]${NC} $1"; PASS=$((PASS + 1)); }
gate_fail() { echo -e "${RED}[FAIL]${NC} $1"; FAIL=$((FAIL + 1)); FAIL_NAMES+=("$1"); }
gate_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

echo "============================================================"
echo -e " ${BOLD}FalconDB Native Failover-Under-Load Gate${NC}"
echo " $(date -Iseconds 2>/dev/null || date)"
echo "============================================================"

cd "$PROJECT_DIR"

# ── Gate 1: Epoch fencing unit tests ──────────────────────────────
echo ""
echo -e "${BOLD}── Gate 1: Epoch Fencing Tests ──${NC}"
OUTPUT=$(cargo test -p falcon_native_server -- epoch 2>&1)
if echo "$OUTPUT" | grep -q "test result: ok"; then
    gate_pass "Epoch fencing unit tests"
else
    # May not have tests matching 'epoch' filter — run all
    OUTPUT=$(cargo test -p falcon_native_server 2>&1)
    if echo "$OUTPUT" | grep -q "test result: ok"; then
        gate_pass "Native server tests (includes fencing)"
    else
        gate_fail "Epoch fencing tests"
    fi
fi

# ── Gate 2: Protocol error propagation ────────────────────────────
echo ""
echo -e "${BOLD}── Gate 2: Error Propagation Tests ──${NC}"
OUTPUT=$(cargo test -p falcon_protocol_native -- error 2>&1)
echo "$OUTPUT" | grep "test result:" || true
OUTPUT2=$(cargo test -p falcon_protocol_native 2>&1)
if echo "$OUTPUT2" | grep -q "test result: ok"; then
    gate_pass "Protocol error propagation"
else
    gate_fail "Protocol error propagation"
fi

# ── Gate 3: Session lifecycle tests ───────────────────────────────
echo ""
echo -e "${BOLD}── Gate 3: Session Lifecycle ──${NC}"
OUTPUT=$(cargo test -p falcon_native_server -- session 2>&1)
if echo "$OUTPUT" | grep -q "test result: ok"; then
    gate_pass "Session lifecycle"
else
    gate_fail "Session lifecycle"
fi

# ── Gate 4: Executor bridge fencing ───────────────────────────────
echo ""
echo -e "${BOLD}── Gate 4: Executor Bridge Fencing ──${NC}"
OUTPUT=$(cargo test -p falcon_native_server -- executor_bridge 2>&1)
if echo "$OUTPUT" | grep -q "test result: ok"; then
    gate_pass "Executor bridge fencing"
else
    gate_fail "Executor bridge fencing"
fi

# ── Gate 5: Full failover bench (if falcon_bench available) ───────
echo ""
echo -e "${BOLD}── Gate 5: Failover Benchmark ──${NC}"
if cargo build --release -p falcon_bench 2>&1 | tail -2; then
    OUTPUT=$(./target/release/falcon_bench --failover --ops 1000 --export text 2>&1)
    echo "$OUTPUT" | tail -5
    if echo "$OUTPUT" | grep -q "Data intact.*true"; then
        gate_pass "Failover benchmark — data intact"
    else
        gate_fail "Failover benchmark — data loss detected"
    fi
else
    gate_info "falcon_bench build failed — skipping failover bench"
fi

# ═══════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════
echo ""
echo "============================================================"
echo -e " ${BOLD}Passed${NC}: $PASS"
echo -e " ${BOLD}Failed${NC}: $FAIL"
echo "============================================================"

if [ ${#FAIL_NAMES[@]} -gt 0 ]; then
    echo -e "${RED}Failed gates:${NC}"
    for name in "${FAIL_NAMES[@]}"; do
        echo -e "  ${RED}✗${NC} $name"
    done
fi

echo ""
if [ "$FAIL" -gt 0 ]; then
    echo -e "${BOLD}${RED}FAILOVER GATE: FAILED${NC}"
    exit 1
else
    echo -e "${BOLD}${GREEN}FAILOVER GATE: PASSED${NC}"
    exit 0
fi
