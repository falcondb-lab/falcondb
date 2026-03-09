#!/usr/bin/env bash
# ============================================================================
# FalconDB — P0-2: Failover Determinism Matrix Runner
# ============================================================================
#
# Runs the full 3×3 failover matrix (fault_type × load_type) and produces
# structured evidence output.
#
# Usage:
#   chmod +x scripts/run_failover_matrix.sh
#   ./scripts/run_failover_matrix.sh                # full matrix
#   ./scripts/run_failover_matrix.sh --ci-nightly    # reduced matrix for CI
#
# Output:
#   evidence/failover/matrix_results_<timestamp>.txt
#   evidence/failover/summary.json
# ============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TIMESTAMP=$(date +%Y%m%dT%H%M%S)
EVIDENCE_DIR="$REPO_ROOT/evidence/failover"
RESULTS_FILE="$EVIDENCE_DIR/matrix_results_${TIMESTAMP}.txt"
SUMMARY_FILE="$EVIDENCE_DIR/summary.json"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

CI_NIGHTLY=false
if [ "${1:-}" = "--ci-nightly" ]; then
    CI_NIGHTLY=true
fi

mkdir -p "$EVIDENCE_DIR"

echo "================================================================="
echo " FalconDB — Failover Determinism Matrix"
echo " Timestamp: $TIMESTAMP"
echo " Mode: $(if $CI_NIGHTLY; then echo 'CI Nightly (reduced)'; else echo 'Full'; fi)"
echo " Output: $RESULTS_FILE"
echo "================================================================="
echo ""

# ── Step 1: Build ──
echo -e "${CYAN}[1/3]${NC} Building test binary..."
cargo test -p falcon_cluster --test failover_determinism --no-run 2>&1 | tail -3
echo ""

# ── Step 2: Run matrix ──
echo -e "${CYAN}[2/3]${NC} Running failover matrix..."

PASS=0
FAIL=0

if $CI_NIGHTLY; then
    # CI nightly: run only the summary test (covers all 9 combos internally)
    TESTS="failover_full_matrix_summary"
else
    # Full: run all individual tests + summary
    TESTS="failover_matrix"
fi

if cargo test -p falcon_cluster --test failover_determinism -- $TESTS --nocapture 2>&1 | tee "$RESULTS_FILE"; then
    PASS=1
    echo ""
    echo -e "${GREEN}[PASS]${NC} Failover matrix completed"
else
    FAIL=1
    echo ""
    echo -e "${RED}[FAIL]${NC} Failover matrix had failures"
fi

# ── Step 3: Generate summary JSON ──
echo -e "${CYAN}[3/3]${NC} Generating evidence summary..."

# Extract version
VERSION=$(grep -m1 '^version' "$REPO_ROOT/Cargo.toml" | sed 's/.*"\(.*\)".*/\1/')
GIT_HASH=$(git -C "$REPO_ROOT" rev-parse --short=8 HEAD 2>/dev/null || echo "unknown")

cat > "$SUMMARY_FILE" <<EOF
{
  "experiment": "failover_determinism_matrix",
  "version": "$VERSION",
  "git_hash": "$GIT_HASH",
  "timestamp": "$TIMESTAMP",
  "mode": "$(if $CI_NIGHTLY; then echo 'ci-nightly'; else echo 'full'; fi)",
  "fault_types": ["LeaderCrash", "NetworkPartition", "WalStall"],
  "load_types": ["ReadHeavy", "WriteHeavy", "Mixed"],
  "matrix_size": 9,
  "tests_passed": $PASS,
  "tests_failed": $FAIL,
  "results_file": "matrix_results_${TIMESTAMP}.txt",
  "conclusion": "$(if [ $FAIL -eq 0 ]; then echo 'ALL_CONSISTENT'; else echo 'INCONSISTENCY_DETECTED'; fi)"
}
EOF

echo "  Summary: $SUMMARY_FILE"
echo ""

# ── Final Report ──
echo "================================================================="
if [ $FAIL -eq 0 ]; then
    echo -e "  ${GREEN}${BOLD}PASS${NC}: Failover determinism verified"
    echo "  All transaction outcomes are classifiable and consistent"
    echo "  Evidence: $EVIDENCE_DIR/"
else
    echo -e "  ${RED}${BOLD}FAIL${NC}: Inconsistency detected"
    echo "  Review: $RESULTS_FILE"
fi
echo "================================================================="

exit $FAIL
