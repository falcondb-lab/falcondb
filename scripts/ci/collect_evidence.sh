#!/usr/bin/env bash
# ============================================================================
# FalconDB — P0-4: Evidence Pack Collection Script
# ============================================================================
#
# Collects all P0 evidence artifacts into evidence/ directory, tagged with
# the current version from workspace Cargo.toml.
#
# Usage:
#   chmod +x scripts/collect_evidence.sh
#   ./scripts/collect_evidence.sh              # collect all
#   ./scripts/collect_evidence.sh --verify     # verify completeness only
# ============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EVIDENCE_DIR="$REPO_ROOT/evidence"
TIMESTAMP=$(date +%Y%m%dT%H%M%S)

# Extract version (Single Source of Truth)
VERSION=$(grep -m1 '^version' "$REPO_ROOT/Cargo.toml" | sed 's/.*"\(.*\)".*/\1/')
GIT_HASH=$(git -C "$REPO_ROOT" rev-parse --short=8 HEAD 2>/dev/null || echo "unknown")

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

VERIFY_ONLY=false
if [ "${1:-}" = "--verify" ]; then
    VERIFY_ONLY=true
fi

echo "================================================================="
echo " FalconDB — P0 Evidence Pack Collection"
echo " Version:   $VERSION"
echo " Git:       $GIT_HASH"
echo " Timestamp: $TIMESTAMP"
echo "================================================================="
echo ""

ERRORS=0

# ═══════════════════════════════════════════════════════════════════════════
# E1: Versioning Evidence
# ═══════════════════════════════════════════════════════════════════════════
echo -e "${CYAN}[E1]${NC} Versioning Evidence..."
V_DIR="$EVIDENCE_DIR/versioning"
mkdir -p "$V_DIR"

if ! $VERIFY_ONLY; then
    # Run version consistency check
    if [ -f "$REPO_ROOT/scripts/ci_version_check.sh" ]; then
        chmod +x "$REPO_ROOT/scripts/ci_version_check.sh"
        "$REPO_ROOT/scripts/ci_version_check.sh" > "$V_DIR/version_check_${TIMESTAMP}.txt" 2>&1 || true
        echo "  Collected: version consistency check"
    fi

    # Capture binary version (if built)
    FALCON_BIN="$REPO_ROOT/target/release/falcon"
    if [ ! -f "$FALCON_BIN" ]; then
        FALCON_BIN="$REPO_ROOT/target/debug/falcon"
    fi
    if [ -f "$FALCON_BIN" ]; then
        "$FALCON_BIN" version > "$V_DIR/binary_version_${TIMESTAMP}.txt" 2>&1
        echo "  Collected: binary version output"
    fi

    # Capture Cargo.toml version
    echo "workspace_version=$VERSION" > "$V_DIR/cargo_version_${TIMESTAMP}.txt"
    echo "git_hash=$GIT_HASH" >> "$V_DIR/cargo_version_${TIMESTAMP}.txt"
    echo "  Collected: Cargo.toml version"

    # Capture dist/VERSION
    if [ -f "$REPO_ROOT/dist/VERSION" ]; then
        cp "$REPO_ROOT/dist/VERSION" "$V_DIR/dist_VERSION_${TIMESTAMP}.txt"
        echo "  Collected: dist/VERSION"
    fi
fi

# Verify
for expected in "docs/versioning.md" "scripts/ci_version_check.sh" "scripts/extract_version.ps1" ".github/workflows/release.yml"; do
    if [ -f "$REPO_ROOT/$expected" ]; then
        echo -e "  ${GREEN}✓${NC} $expected exists"
    else
        echo -e "  ${RED}✗${NC} $expected MISSING"
        ERRORS=$((ERRORS + 1))
    fi
done

# ═══════════════════════════════════════════════════════════════════════════
# E2: Failover Determinism Evidence
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${CYAN}[E2]${NC} Failover Determinism Evidence..."
F_DIR="$EVIDENCE_DIR/failover"
mkdir -p "$F_DIR"

if ! $VERIFY_ONLY; then
    # Run failover matrix
    echo "  Running failover determinism tests..."
    cargo test -p falcon_cluster --test failover_determinism \
        -- failover_full_matrix_summary --nocapture \
        > "$F_DIR/matrix_results_${TIMESTAMP}.txt" 2>&1 || {
        echo "  WARN: Failover tests had failures"
    }
    echo "  Collected: failover matrix results"

    # Generate summary JSON
    PASS=$(grep -c "test.*ok" "$F_DIR/matrix_results_${TIMESTAMP}.txt" 2>/dev/null || echo "0")
    FAIL=$(grep -c "test.*FAILED" "$F_DIR/matrix_results_${TIMESTAMP}.txt" 2>/dev/null || echo "0")
    cat > "$F_DIR/summary_${TIMESTAMP}.json" <<EOF
{
  "experiment": "failover_determinism_matrix",
  "version": "$VERSION",
  "git_hash": "$GIT_HASH",
  "timestamp": "$TIMESTAMP",
  "tests_passed": $PASS,
  "tests_failed": $FAIL
}
EOF
    echo "  Collected: summary JSON"
fi

# Verify
for expected in "docs/failover_determinism_report.md" "scripts/run_failover_matrix.sh" "crates/falcon_cluster/tests/failover_determinism.rs"; do
    if [ -f "$REPO_ROOT/$expected" ]; then
        echo -e "  ${GREEN}✓${NC} $expected exists"
    else
        echo -e "  ${RED}✗${NC} $expected MISSING"
        ERRORS=$((ERRORS + 1))
    fi
done

# ═══════════════════════════════════════════════════════════════════════════
# E3: Benchmark Evidence
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${CYAN}[E3]${NC} Benchmark Evidence..."
B_DIR="$EVIDENCE_DIR/benchmarks"
mkdir -p "$B_DIR"

if ! $VERIFY_ONLY; then
    # Copy benchmark configs and scripts
    cp "$REPO_ROOT/benchmarks/README.md" "$B_DIR/README_${TIMESTAMP}.md" 2>/dev/null || true
    cp "$REPO_ROOT/benchmarks/RESULTS.md" "$B_DIR/RESULTS_${TIMESTAMP}.md" 2>/dev/null || true

    # Copy any existing raw results
    if [ -d "$REPO_ROOT/benchmarks/results" ]; then
        for f in "$REPO_ROOT"/benchmarks/results/*.txt; do
            [ -f "$f" ] && cp "$f" "$B_DIR/" 2>/dev/null || true
        done
    fi
    echo "  Collected: benchmark configs and results"
fi

# Verify
for expected in "benchmarks/README.md" "benchmarks/RESULTS.md" "benchmarks/scripts/run_all.sh" "benchmarks/workloads/setup.sql"; do
    if [ -f "$REPO_ROOT/$expected" ]; then
        echo -e "  ${GREEN}✓${NC} $expected exists"
    else
        echo -e "  ${RED}✗${NC} $expected MISSING"
        ERRORS=$((ERRORS + 1))
    fi
done

# ═══════════════════════════════════════════════════════════════════════════
# E4: CI Reports
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${CYAN}[E4]${NC} CI Reports..."
C_DIR="$EVIDENCE_DIR/ci_reports"
mkdir -p "$C_DIR"

if ! $VERIFY_ONLY; then
    # Run cargo test and capture output
    echo "  Running workspace tests..."
    cargo test --workspace > "$C_DIR/test_results_${TIMESTAMP}.txt" 2>&1 || {
        echo "  WARN: Some tests failed"
    }
    echo "  Collected: workspace test results"

    # Capture test count
    TOTAL_PASS=$(grep -oP '\d+ passed' "$C_DIR/test_results_${TIMESTAMP}.txt" | tail -1 || echo "0 passed")
    TOTAL_FAIL=$(grep -oP '\d+ failed' "$C_DIR/test_results_${TIMESTAMP}.txt" | tail -1 || echo "0 failed")
    cat > "$C_DIR/summary_${TIMESTAMP}.json" <<EOF
{
  "version": "$VERSION",
  "git_hash": "$GIT_HASH",
  "timestamp": "$TIMESTAMP",
  "tests_passed": "$TOTAL_PASS",
  "tests_failed": "$TOTAL_FAIL"
}
EOF
    echo "  Collected: test summary"
fi

# Verify CI workflow exists
for expected in ".github/workflows/ci.yml" ".github/workflows/release.yml"; do
    if [ -f "$REPO_ROOT/$expected" ]; then
        echo -e "  ${GREEN}✓${NC} $expected exists"
    else
        echo -e "  ${RED}✗${NC} $expected MISSING"
        ERRORS=$((ERRORS + 1))
    fi
done

# ═══════════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "================================================================="
if [ "$ERRORS" -eq 0 ]; then
    echo -e "  ${GREEN}${BOLD}PASS${NC}: All P0 evidence artifacts present"
else
    echo -e "  ${RED}${BOLD}FAIL${NC}: $ERRORS missing artifact(s)"
fi
echo "  Evidence directory: $EVIDENCE_DIR/"
echo "  Version: $VERSION ($GIT_HASH)"
echo "================================================================="

exit $ERRORS
