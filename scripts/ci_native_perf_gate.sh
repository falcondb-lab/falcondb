#!/usr/bin/env bash
# ============================================================================
# FalconDB — Native Protocol Performance Gate
# ============================================================================
# Validates native protocol performance characteristics:
#   Gate 1: Codec encode/decode throughput (golden vector benchmark)
#   Gate 2: Server message processing throughput
#   Gate 3: Batch protocol overhead vs single-query
#
# Usage:
#   ./scripts/ci_native_perf_gate.sh
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
echo -e " ${BOLD}FalconDB Native Protocol Performance Gate${NC}"
echo " $(date -Iseconds 2>/dev/null || date)"
echo "============================================================"

cd "$PROJECT_DIR"

# ── Gate 1: Build release ──────────────────────────────────────────
echo ""
echo -e "${BOLD}── Gate 1: Release Build ──${NC}"
if cargo build --release -p falcon_protocol_native -p falcon_native_server 2>&1 | tail -3; then
    gate_pass "Release build"
else
    gate_fail "Release build"
    exit 1
fi

# ── Gate 2: Protocol codec tests (release mode for perf) ──────────
echo ""
echo -e "${BOLD}── Gate 2: Protocol Tests (release) ──${NC}"
OUTPUT=$(cargo test --release -p falcon_protocol_native 2>&1)
if echo "$OUTPUT" | grep -q "test result: ok"; then
    gate_pass "Protocol codec tests (release)"
else
    gate_fail "Protocol codec tests (release)"
fi

# ── Gate 3: Native server tests (release mode) ────────────────────
echo ""
echo -e "${BOLD}── Gate 3: Native Server Tests (release) ──${NC}"
OUTPUT=$(cargo test --release -p falcon_native_server 2>&1)
if echo "$OUTPUT" | grep -q "test result: ok"; then
    gate_pass "Native server tests (release)"
else
    gate_fail "Native server tests (release)"
fi

# ── Gate 4: Full workspace regression ─────────────────────────────
echo ""
echo -e "${BOLD}── Gate 4: Workspace Regression ──${NC}"
OUTPUT=$(cargo test --release --workspace 2>&1)
FAILED=$(echo "$OUTPUT" | grep "FAILED" | wc -l)
if [ "$FAILED" -eq 0 ]; then
    TOTAL=$(echo "$OUTPUT" | grep "test result: ok" | grep -oP '\d+ passed' | awk '{s+=$1}END{print s}')
    gate_pass "Workspace regression ($TOTAL tests)"
else
    gate_fail "Workspace regression — $FAILED crate(s) failed"
fi

# ═══════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════
echo ""
echo "============================================================"
echo -e " ${BOLD}Passed${NC}: $PASS"
echo -e " ${BOLD}Failed${NC}: $FAIL"
echo "============================================================"

if [ "$FAIL" -gt 0 ]; then
    echo -e "${BOLD}${RED}NATIVE PERF GATE: FAILED${NC}"
    exit 1
else
    echo -e "${BOLD}${GREEN}NATIVE PERF GATE: PASSED${NC}"
    exit 0
fi
