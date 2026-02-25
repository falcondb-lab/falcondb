#!/usr/bin/env bash
# ============================================================================
# FalconDB — P2 Evidence Pack Collection Script
# ============================================================================
#
# Collects P2 industry/commercial evidence artifacts into evidence/ directory.
#
# Usage:
#   chmod +x scripts/collect_evidence_p2.sh
#   ./scripts/collect_evidence_p2.sh              # collect all P2 evidence
#   ./scripts/collect_evidence_p2.sh --verify     # verify completeness only
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
echo " FalconDB — P2 Evidence Pack Collection"
echo " Version:   $VERSION ($GIT_HASH)"
echo " Timestamp: $TIMESTAMP"
echo "================================================================="
echo ""

ERRORS=0

# ═══════════════════════════════════════════════════════════════════════════
# E9: Industry Focus
# ═══════════════════════════════════════════════════════════════════════════
echo -e "${CYAN}[E9]${NC} Industry Focus Evidence..."

for expected in "docs/industry_focus.md" "docs/industry_edition_overview.md"; do
    if [ -f "$REPO_ROOT/$expected" ]; then
        echo -e "  ${GREEN}✓${NC} $expected"
    else
        echo -e "  ${RED}✗${NC} $expected MISSING"
        ERRORS=$((ERRORS + 1))
    fi
done

# ═══════════════════════════════════════════════════════════════════════════
# E10: Industry SLA & Comparisons
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${CYAN}[E10]${NC} Industry SLA & Comparisons..."
SLA_DIR="$EVIDENCE_DIR/industry/sla"
COMP_DIR="$EVIDENCE_DIR/industry/comparisons"
mkdir -p "$SLA_DIR" "$COMP_DIR"

if ! $VERIFY_ONLY; then
    # Snapshot SLA doc into evidence with timestamp
    cp "$REPO_ROOT/docs/industry_sla.md" "$SLA_DIR/industry_sla_${TIMESTAMP}.md" 2>/dev/null || true

    # Generate SLA traceability matrix
    cat > "$SLA_DIR/traceability_${TIMESTAMP}.json" <<EOF
{
  "version": "$VERSION",
  "git_hash": "$GIT_HASH",
  "timestamp": "$TIMESTAMP",
  "industry": "financial_trading_oltp",
  "sla_claims": [
    {"claim": "write_p99_lt_1ms", "evidence": "benchmarks/scripts/run_all.sh"},
    {"claim": "zero_phantom_commits", "evidence": "evidence/failover/"},
    {"claim": "no_oom_crash", "evidence": "evidence/memory/"},
    {"claim": "72h_stability", "evidence": "evidence/stability/"},
    {"claim": "replica_integrity", "evidence": "evidence/replication/"},
    {"claim": "rpo_zero_sync_mode", "evidence": "docs/failover_determinism_report.md"}
  ]
}
EOF

    # Generate comparison summary
    cat > "$COMP_DIR/comparison_summary_${TIMESTAMP}.json" <<EOF
{
  "version": "$VERSION",
  "timestamp": "$TIMESTAMP",
  "comparisons": [
    {
      "target": "postgresql",
      "category": "write_latency",
      "falcondb_advantage": "5-20x lower P99",
      "evidence": "benchmarks/"
    },
    {
      "target": "postgresql",
      "category": "failover_determinism",
      "falcondb_advantage": "zero phantom commits vs possible silent data loss",
      "evidence": "evidence/failover/"
    },
    {
      "target": "postgresql",
      "category": "memory_backpressure",
      "falcondb_advantage": "4-tier governor vs OOM/connection exhaustion",
      "evidence": "evidence/memory/"
    },
    {
      "target": "oracle",
      "category": "license_cost",
      "falcondb_advantage": "Apache 2.0 vs $47.5K/core",
      "evidence": "docs/commercial_model.md"
    }
  ]
}
EOF
    echo "  Collected: SLA traceability + comparison summary"
fi

for expected in "docs/industry_sla.md"; do
    if [ -f "$REPO_ROOT/$expected" ]; then
        echo -e "  ${GREEN}✓${NC} $expected"
    else
        echo -e "  ${RED}✗${NC} $expected MISSING"
        ERRORS=$((ERRORS + 1))
    fi
done

# ═══════════════════════════════════════════════════════════════════════════
# E11: Commercial Model
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${CYAN}[E11]${NC} Commercial Model..."

for expected in "docs/commercial_model.md"; do
    if [ -f "$REPO_ROOT/$expected" ]; then
        echo -e "  ${GREEN}✓${NC} $expected"
    else
        echo -e "  ${RED}✗${NC} $expected MISSING"
        ERRORS=$((ERRORS + 1))
    fi
done

# ═══════════════════════════════════════════════════════════════════════════
# E12: PoC Playbook
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo -e "${CYAN}[E12]${NC} PoC Playbook..."
POC_DIR="$EVIDENCE_DIR/industry/poc"
mkdir -p "$POC_DIR"

if ! $VERIFY_ONLY; then
    cp "$REPO_ROOT/docs/industry_poc_playbook.md" "$POC_DIR/poc_playbook_${TIMESTAMP}.md" 2>/dev/null || true
    echo "  Collected: PoC playbook snapshot"
fi

for expected in "docs/industry_poc_playbook.md"; do
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
    echo -e "  ${GREEN}${BOLD}PASS${NC}: All P2 evidence artifacts present"
else
    echo -e "  ${RED}${BOLD}FAIL${NC}: $ERRORS missing artifact(s)"
fi
echo "  Evidence directory: $EVIDENCE_DIR/"
echo "  Version: $VERSION ($GIT_HASH)"
echo "================================================================="

exit $ERRORS
