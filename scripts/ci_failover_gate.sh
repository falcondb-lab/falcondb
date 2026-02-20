#!/usr/bin/env bash
# ============================================================================
# FalconDB — CI Failover & Replication Gate (P0 + P1)
# ============================================================================
# Two-tier gate:
#   P0 (must pass):  replication lifecycle, promote fencing, basic failover
#   P1 (warn only):  fault injection, checkpoint, read-only, SHOW commands
#
# Exit code 0 = P0 gates all passed (P1 warnings allowed)
# Exit code 1 = at least one P0 gate failed
#
# Usage:
#   chmod +x scripts/ci_failover_gate.sh && ./scripts/ci_failover_gate.sh
# ============================================================================
set -uo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

P0_PASSED=0; P0_FAILED=0; P0_FAIL_NAMES=()
P1_PASSED=0; P1_FAILED=0; P1_FAIL_NAMES=()
LOG_DIR=$(mktemp -d /tmp/falcon_ci_gate_XXXX)

run_gate() {
  local tier="$1" name="$2"; shift 2
  local log="$LOG_DIR/$(echo "$name" | tr ' /:' '_').log"
  local cmd_str="$*"

  echo ""
  echo -e "${CYAN}[$tier]${NC} ${BOLD}$name${NC}"
  echo -e "  cmd: $cmd_str"

  if "$@" > "$log" 2>&1; then
    # Show summary line from cargo test output
    grep "^test result:" "$log" | tail -1
    echo -e "${GREEN}[$tier PASS]${NC} $name"
    if [ "$tier" = "P0" ]; then ((P0_PASSED++)); else ((P1_PASSED++)); fi
  else
    echo -e "${RED}[$tier FAIL]${NC} $name"
    echo -e "  ${YELLOW}--- Last 200 lines of output ---${NC}"
    tail -200 "$log"
    echo -e "  ${YELLOW}--- End of output (full log: $log) ---${NC}"
    if [ "$tier" = "P0" ]; then
      ((P0_FAILED++)); P0_FAIL_NAMES+=("$name")
    else
      ((P1_FAILED++)); P1_FAIL_NAMES+=("$name")
    fi
  fi
}

echo "============================================================"
echo " FalconDB CI Failover & Replication Gate"
echo " $(date -Iseconds)"
echo " Log directory: $LOG_DIR"
echo "============================================================"

# ═══════════════════════════════════════════════════════════════════
# P0 GATES — Must pass (merge-blocking)
# ═══════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}═══ P0 GATES (must pass) ═══${NC}"

run_gate P0 "Replication: WAL shipping + catch-up" \
  cargo test -p falcon_cluster -- replication --no-fail-fast

run_gate P0 "Failover: promote fencing protocol" \
  cargo test -p falcon_cluster -- promote_fencing --no-fail-fast

run_gate P0 "Lifecycle: M1 full lifecycle" \
  cargo test -p falcon_cluster -- m1_full_lifecycle --no-fail-fast

run_gate P0 "gRPC: proto roundtrip + transport" \
  cargo test -p falcon_cluster -- grpc --no-fail-fast

run_gate P0 "WAL: observer + replication log" \
  cargo test -p falcon_storage -- wal_observer --no-fail-fast

# ═══════════════════════════════════════════════════════════════════
# P1 GATES — Warning only (non-blocking, creates issues)
# ═══════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}═══ P1 GATES (warn on failure) ═══${NC}"

run_gate P1 "Checkpoint: streaming + recovery" \
  cargo test -p falcon_cluster -- checkpoint --no-fail-fast

run_gate P1 "Fault injection: chaos scenarios" \
  cargo test -p falcon_cluster -- fault_injection --no-fail-fast

run_gate P1 "Read-only: replica write rejection" \
  cargo test -p falcon_server -- read_only --no-fail-fast

run_gate P1 "SHOW: falcon.wal_stats + node_role" \
  cargo test -p falcon_protocol_pg -- show_falcon --no-fail-fast

run_gate P1 "SHOW: falcon.gc_stats + txn_stats" \
  cargo test -p falcon_protocol_pg -- show_falcon --no-fail-fast

# ═══════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════
echo ""
echo "============================================================"
echo -e " ${BOLD}P0${NC}: $P0_PASSED passed, $P0_FAILED failed"
echo -e " ${BOLD}P1${NC}: $P1_PASSED passed, $P1_FAILED failed (non-blocking)"
echo "============================================================"

if [ ${#P0_FAIL_NAMES[@]} -gt 0 ]; then
  echo -e "${RED}P0 failures (merge-blocking):${NC}"
  for name in "${P0_FAIL_NAMES[@]}"; do
    echo -e "  ${RED}✗${NC} $name"
  done
fi

if [ ${#P1_FAIL_NAMES[@]} -gt 0 ]; then
  echo -e "${YELLOW}P1 warnings (non-blocking):${NC}"
  for name in "${P1_FAIL_NAMES[@]}"; do
    echo -e "  ${YELLOW}⚠${NC} $name"
  done
fi

echo ""
echo "Full logs: $LOG_DIR/"
echo ""

if [ "$P0_FAILED" -gt 0 ]; then
  echo -e "${BOLD}${RED}FAILOVER GATE: FAILED (P0 failures)${NC}"
  exit 1
else
  if [ "$P1_FAILED" -gt 0 ]; then
    echo -e "${BOLD}${YELLOW}FAILOVER GATE: PASSED with warnings (P1 failures)${NC}"
  else
    echo -e "${BOLD}${GREEN}FAILOVER GATE: PASSED${NC}"
  fi
  exit 0
fi
