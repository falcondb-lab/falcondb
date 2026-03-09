#!/usr/bin/env bash
# ============================================================================
# FalconDB — P1 Evidence Pack Collection Script
# ============================================================================
#
# Collects P1 evidence artifacts into evidence/ directory.
#
# Usage:
#   chmod +x scripts/collect_evidence_p1.sh
#   ./scripts/collect_evidence_p1.sh              # collect all P1 evidence
#   ./scripts/collect_evidence_p1.sh --verify     # verify completeness only
# ============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EVIDENCE_DIR="$REPO_ROOT/evidence"
TIMESTAMP=$(date +%Y%m%dT%H%M%S)

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
echo " FalconDB — P1 Evidence Pack Collection"
echo " Version:   $VERSION ($GIT_HASH)"
echo " Timestamp: $TIMESTAMP"
echo "================================================================="
echo ""

ERRORS=0

# ═══════════════════════════════════════════════════════════════════════════
# E5: Memory Backpressure
# ═══════════════════════════════════════════════════════════════════════════
echo -e "${CYAN}[E5]${NC} Memory Backpressure Evidence..."
M_DIR="$EVIDENCE_DIR/memory"
mkdir -p "$M_DIR"

if ! $VERIFY_ONLY; then
    echo "  Running memory backpressure tests..."
    cargo test -p falcon_storage --test memory_backpressure -- --nocapture \
        > "$M_DIR/backpressure_test_${TIMESTAMP}.txt" 2>&1 || {
        echo "  WARN: Some tests failed"
    }
    PASS=$(grep -c "test.*ok" "$M_DIR/backpressure_test_${TIMESTAMP}.txt" 2>/dev/null || echo "0")
    FAIL=$(grep -c "FAILED" "$M_DIR/backpressure_test_${TIMESTAMP}.txt" 2>/dev/null || echo "0")
    cat > "$M_DIR/governor_stats_${TIMESTAMP}.json" <<EOF
{
  "version": "$VERSION",
  "git_hash": "$GIT_HASH",
  "timestamp": "$TIMESTAMP",
  "tests_passed": $PASS,
  "tests_failed": $FAIL,
  "consumers_tracked": 7,
  "governor_tiers": ["None", "Soft", "Hard", "Emergency"]
}
EOF
    echo "  Collected: backpressure tests ($PASS passed, $FAIL failed)"
fi

for expected in "docs/memory_backpressure.md" "crates/falcon_storage/tests/memory_backpressure.rs"; do
    if [ -f "$REPO_ROOT/$expected" ]; then
        echo -e "  ${GREEN}✓${NC} $expected"
    else
        echo -e "  ${RED}✗${NC} $expected MISSING"
        ERRORS=$((ERRORS + 1))
    fi
done

# ═══════════════════════════════════════════════════════════════════════════
# E6: Stability
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${CYAN}[E6]${NC} Long-Run Stability Evidence..."
S_DIR="$EVIDENCE_DIR/stability"
mkdir -p "$S_DIR"

for expected in "scripts/run_stability_test.sh" "evidence/stability/72h_report_template.md" "evidence/stability/7d_report_template.md"; do
    if [ -f "$REPO_ROOT/$expected" ]; then
        echo -e "  ${GREEN}✓${NC} $expected"
    else
        echo -e "  ${RED}✗${NC} $expected MISSING"
        ERRORS=$((ERRORS + 1))
    fi
done

# ═══════════════════════════════════════════════════════════════════════════
# E7: Replication Integrity
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${CYAN}[E7]${NC} Replication Integrity Evidence..."
R_DIR="$EVIDENCE_DIR/replication"
mkdir -p "$R_DIR"

if ! $VERIFY_ONLY; then
    echo "  Running replication integrity tests..."
    cargo test -p falcon_cluster --test replication_integrity -- --nocapture \
        > "$R_DIR/integrity_test_${TIMESTAMP}.txt" 2>&1 || {
        echo "  WARN: Some tests failed"
    }
    PASS=$(grep -c "test.*ok" "$R_DIR/integrity_test_${TIMESTAMP}.txt" 2>/dev/null || echo "0")
    FAIL=$(grep -c "FAILED" "$R_DIR/integrity_test_${TIMESTAMP}.txt" 2>/dev/null || echo "0")
    cat > "$R_DIR/drift_detection_${TIMESTAMP}.json" <<EOF
{
  "version": "$VERSION",
  "git_hash": "$GIT_HASH",
  "timestamp": "$TIMESTAMP",
  "tests_passed": $PASS,
  "tests_failed": $FAIL,
  "checks": ["row_count", "checksum", "lsn_alignment"]
}
EOF
    echo "  Collected: integrity tests ($PASS passed, $FAIL failed)"
fi

for expected in "docs/replication_integrity.md" "crates/falcon_cluster/tests/replication_integrity.rs"; do
    if [ -f "$REPO_ROOT/$expected" ]; then
        echo -e "  ${GREEN}✓${NC} $expected"
    else
        echo -e "  ${RED}✗${NC} $expected MISSING"
        ERRORS=$((ERRORS + 1))
    fi
done

# ═══════════════════════════════════════════════════════════════════════════
# E8: Operability
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${CYAN}[E8]${NC} Operability Evidence..."
O_DIR="$EVIDENCE_DIR/operability"
mkdir -p "$O_DIR"

if ! $VERIFY_ONLY; then
    # Extract metrics catalog from observability crate
    grep -oP 'falcon_[a-z_]+' "$REPO_ROOT/crates/falcon_observability/src/lib.rs" \
        | sort -u > "$O_DIR/metrics_catalog_${TIMESTAMP}.txt" 2>/dev/null || true
    METRIC_COUNT=$(wc -l < "$O_DIR/metrics_catalog_${TIMESTAMP}.txt" 2>/dev/null || echo "0")
    echo "  Collected: $METRIC_COUNT unique Prometheus metrics"
fi

for expected in "docs/operability_baseline.md"; do
    if [ -f "$REPO_ROOT/$expected" ]; then
        echo -e "  ${GREEN}✓${NC} $expected"
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
    echo -e "  ${GREEN}${BOLD}PASS${NC}: All P1 evidence artifacts present"
else
    echo -e "  ${RED}${BOLD}FAIL${NC}: $ERRORS missing artifact(s)"
fi
echo "  Evidence directory: $EVIDENCE_DIR/"
echo "  Version: $VERSION ($GIT_HASH)"
echo "================================================================="

exit $ERRORS
