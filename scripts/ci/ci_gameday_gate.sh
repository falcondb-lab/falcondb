#!/usr/bin/env bash
# ci_gameday_gate.sh — Chaos/GameDay CI gate for FalconDB production hardening.
#
# Validates all hardened subsystems via targeted test suites:
#   Gate 1: SST read-path hardening (CRC32, parse errors)
#   Gate 2: RBAC privilege enforcement
#   Gate 3: WAL group-commit decoupling
#   Gate 4: Cross-shard slow-path rate limiting
#   Gate 5: In-doubt txn convergence admin interface
#   Gate 6: Compaction / foreground resource isolation
#   Gate 7: Global memory governor (4-tier backpressure)
#   Gate 8: MVCC read view + GC safepoint unification
#   Gate 9: Consistency commit point observability
#   Gate 10: Full test suite (no regressions)
#
# Usage:
#   ./scripts/ci_gameday_gate.sh [gate1..gate10|all|fast]
#
# "fast" runs gates 1-9 (unit tests only, no full suite).
# "all" runs all 10 gates including the full test suite.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

pass() { echo -e "${GREEN}[PASS]${NC} $1"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail() { echo -e "${RED}[FAIL]${NC} $1"; FAIL_COUNT=$((FAIL_COUNT + 1)); }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }
gate_header() { echo -e "\n${CYAN}── Gate $1: $2 ──${NC}"; }

run_test() {
    local label="$1"
    local cmd="$2"

    if eval "$cmd" > /dev/null 2>&1; then
        pass "$label"
    else
        fail "$label"
    fi
}

cd "$PROJECT_DIR"

# ── Gate 1: SST read-path hardening ─────────────────────────────────────
gate1() {
    gate_header 1 "SST read-path hardening (CRC32, parse errors)"
    run_test "SST hardened read tests" \
        "cargo test -p falcon_storage --lib -- lsm::sst::tests 2>&1"
}

# ── Gate 2: RBAC privilege enforcement ───────────────────────────────────
gate2() {
    gate_header 2 "RBAC privilege enforcement + audit tracing"
    run_test "Executor RBAC tests" \
        "cargo test -p falcon_executor --lib -- tests 2>&1"
}

# ── Gate 3: WAL group-commit decoupling ──────────────────────────────────
gate3() {
    gate_header 3 "WAL append/fsync decoupling (GroupCommitSyncer)"
    run_test "Group commit tests" \
        "cargo test -p falcon_storage --lib -- group_commit::tests 2>&1"
}

# ── Gate 4: Cross-shard slow-path rate limiting ──────────────────────────
gate4() {
    gate_header 4 "Cross-shard slow-path rate limiting"
    run_test "PerShardSlowPathLimiter + LatencyHistogram tests" \
        "cargo test -p falcon_cluster --lib -- cross_shard::tests 2>&1"
}

# ── Gate 5: In-doubt txn convergence admin ───────────────────────────────
gate5() {
    gate_header 5 "In-doubt txn convergence admin interface"
    run_test "InDoubtResolver + admin interface tests" \
        "cargo test -p falcon_cluster --lib -- indoubt_resolver::tests 2>&1"
}

# ── Gate 6: Compaction / foreground resource isolation ───────────────────
gate6() {
    gate_header 6 "Compaction / foreground resource isolation"
    run_test "ResourceIsolator (IoScheduler + CpuScheduler) tests" \
        "cargo test -p falcon_storage --lib -- resource_isolation::tests 2>&1"
}

# ── Gate 7: Global memory governor ───────────────────────────────────────
gate7() {
    gate_header 7 "Global memory governor (4-tier backpressure)"
    run_test "Memory tracker + GlobalMemoryGovernor tests" \
        "cargo test -p falcon_storage --lib -- memory::tests 2>&1"
}

# ── Gate 8: MVCC read view + GC safepoint unification ───────────────────
gate8() {
    gate_header 8 "MVCC read view + GC safepoint unification"
    run_test "ReadViewManager + compute_safepoint tests" \
        "cargo test -p falcon_storage --lib -- gc::tests 2>&1"
}

# ── Gate 9: Consistency commit point observability ───────────────────────
gate9() {
    gate_header 9 "Consistency commit point observability"
    run_test "CommitPointTracker + consistency tests" \
        "cargo test -p falcon_common --lib -- consistency::tests 2>&1"
}

# ── Gate 10: Full test suite (regression gate) ──────────────────────────
gate10() {
    gate_header 10 "Full workspace test suite (regression gate)"
    info "Running cargo test --workspace ..."
    if cargo test --workspace 2>&1 | tail -5; then
        pass "Full test suite passed"
    else
        fail "Full test suite had failures"
    fi
}

# ── Summary ─────────────────────────────────────────────────────────────
summary() {
    echo ""
    echo "═══════════════════════════════════════════════"
    echo "  FalconDB GameDay CI Gate Summary"
    echo "═══════════════════════════════════════════════"
    echo -e "  ${GREEN}Passed${NC}: $PASS_COUNT"
    echo -e "  ${RED}Failed${NC}: $FAIL_COUNT"
    echo "═══════════════════════════════════════════════"

    if [ "$FAIL_COUNT" -gt 0 ]; then
        echo -e "${RED}GameDay gate FAILED${NC}"
        exit 1
    else
        echo -e "${GREEN}GameDay gate PASSED${NC}"
        exit 0
    fi
}

# ── Dispatch ─────────────────────────────────────────────────────────────
case "${1:-all}" in
    gate1)  gate1;  summary ;;
    gate2)  gate2;  summary ;;
    gate3)  gate3;  summary ;;
    gate4)  gate4;  summary ;;
    gate5)  gate5;  summary ;;
    gate6)  gate6;  summary ;;
    gate7)  gate7;  summary ;;
    gate8)  gate8;  summary ;;
    gate9)  gate9;  summary ;;
    gate10) gate10; summary ;;
    fast)
        echo "═══════════════════════════════════════════════"
        echo "  FalconDB GameDay CI Gates (fast — unit tests)"
        echo "═══════════════════════════════════════════════"
        gate1; gate2; gate3; gate4; gate5
        gate6; gate7; gate8; gate9
        summary
        ;;
    all)
        echo "═══════════════════════════════════════════════"
        echo "  FalconDB GameDay CI Gates (all)"
        echo "═══════════════════════════════════════════════"
        gate1; gate2; gate3; gate4; gate5
        gate6; gate7; gate8; gate9; gate10
        summary
        ;;
    *)
        echo "Usage: $0 [gate1..gate10|fast|all]"
        echo ""
        echo "Gates:"
        echo "  gate1  — SST read-path hardening"
        echo "  gate2  — RBAC privilege enforcement"
        echo "  gate3  — WAL group-commit decoupling"
        echo "  gate4  — Cross-shard slow-path limiting"
        echo "  gate5  — In-doubt txn convergence admin"
        echo "  gate6  — Compaction resource isolation"
        echo "  gate7  — Global memory governor"
        echo "  gate8  — MVCC read view + GC safepoint"
        echo "  gate9  — Commit point observability"
        echo "  gate10 — Full test suite (regression)"
        echo "  fast   — Gates 1-9 (unit tests only)"
        echo "  all    — All 10 gates"
        exit 1
        ;;
esac
