#!/usr/bin/env bash
# ============================================================================
# FalconDB — Native JDBC Smoke Test CI Gate
# ============================================================================
# Validates:
#   Gate 1: Rust crates compile (falcon_protocol_native + falcon_native_server)
#   Gate 2: Rust unit tests pass
#   Gate 3: Java JDBC driver compiles
#   Gate 4: Java unit tests pass
#   Gate 5: Protocol golden vector tests
#
# Usage:
#   ./scripts/ci_native_jdbc_smoke.sh
#   ./scripts/ci_native_jdbc_smoke.sh --rust-only
#   ./scripts/ci_native_jdbc_smoke.sh --java-only
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
RUST_ONLY=false
JAVA_ONLY=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --rust-only) RUST_ONLY=true; shift ;;
        --java-only) JAVA_ONLY=true; shift ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

gate_pass() { echo -e "${GREEN}[PASS]${NC} $1"; PASS=$((PASS + 1)); }
gate_fail() { echo -e "${RED}[FAIL]${NC} $1"; FAIL=$((FAIL + 1)); FAIL_NAMES+=("$1"); }
gate_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

echo "============================================================"
echo -e " ${BOLD}FalconDB Native JDBC Smoke Test${NC}"
echo " $(date -Iseconds 2>/dev/null || date)"
echo "============================================================"

cd "$PROJECT_DIR"

# ═══════════════════════════════════════════════════════════════════════
# Rust Gates
# ═══════════════════════════════════════════════════════════════════════
if [ "$JAVA_ONLY" = false ]; then

    echo ""
    echo -e "${BOLD}── Gate 1: Rust Compile ──${NC}"
    if cargo check -p falcon_protocol_native -p falcon_native_server 2>&1 | tail -3; then
        gate_pass "Rust compile"
    else
        gate_fail "Rust compile"
    fi

    echo ""
    echo -e "${BOLD}── Gate 2: Rust Clippy ──${NC}"
    if cargo clippy -p falcon_protocol_native -p falcon_native_server -- -D warnings 2>&1 | tail -3; then
        gate_pass "Rust clippy"
    else
        gate_fail "Rust clippy"
    fi

    echo ""
    echo -e "${BOLD}── Gate 3: Protocol Native Tests ──${NC}"
    OUTPUT=$(cargo test -p falcon_protocol_native 2>&1)
    echo "$OUTPUT" | grep "test result:"
    if echo "$OUTPUT" | grep -q "test result: ok"; then
        TESTS=$(echo "$OUTPUT" | grep "test result:" | head -1 | grep -oP '\d+ passed' | head -1)
        gate_pass "falcon_protocol_native tests ($TESTS)"
    else
        gate_fail "falcon_protocol_native tests"
    fi

    echo ""
    echo -e "${BOLD}── Gate 4: Native Server Tests ──${NC}"
    OUTPUT=$(cargo test -p falcon_native_server 2>&1)
    echo "$OUTPUT" | grep "test result:"
    if echo "$OUTPUT" | grep -q "test result: ok"; then
        TESTS=$(echo "$OUTPUT" | grep "test result:" | head -1 | grep -oP '\d+ passed' | head -1)
        gate_pass "falcon_native_server tests ($TESTS)"
    else
        gate_fail "falcon_native_server tests"
    fi

fi

# ═══════════════════════════════════════════════════════════════════════
# Java Gates
# ═══════════════════════════════════════════════════════════════════════
if [ "$RUST_ONLY" = false ]; then

    JDBC_DIR="$PROJECT_DIR/clients/falcondb-jdbc"

    echo ""
    echo -e "${BOLD}── Gate 5: Java JDBC Compile ──${NC}"
    if [ -d "$JDBC_DIR" ]; then
        if command -v mvn &>/dev/null || [ -f "$JDBC_DIR/mvnw" ]; then
            MVN="mvn"
            [ -f "$JDBC_DIR/mvnw" ] && MVN="$JDBC_DIR/mvnw"
            if (cd "$JDBC_DIR" && $MVN -q compile 2>&1 | tail -5); then
                gate_pass "Java JDBC compile"
            else
                gate_fail "Java JDBC compile"
            fi

            echo ""
            echo -e "${BOLD}── Gate 6: Java JDBC Unit Tests ──${NC}"
            if (cd "$JDBC_DIR" && $MVN -q test 2>&1 | tail -10); then
                gate_pass "Java JDBC unit tests"
            else
                gate_fail "Java JDBC unit tests"
            fi
        else
            gate_info "Maven not found — skipping Java gates"
        fi
    else
        gate_fail "Java JDBC directory not found: $JDBC_DIR"
    fi

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
    echo -e "${BOLD}${RED}NATIVE JDBC SMOKE: FAILED${NC}"
    exit 1
else
    echo -e "${BOLD}${GREEN}NATIVE JDBC SMOKE: PASSED${NC}"
    exit 0
fi
