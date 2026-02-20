#!/usr/bin/env bash
# ============================================================================
# FalconDB — CI Failover Gate
# ============================================================================
# Runs failover and replication tests that MUST pass before merge.
# Exit code 0 = gate passed, non-zero = gate failed.
#
# Usage (CI):
#   chmod +x scripts/ci_failover_gate.sh
#   ./scripts/ci_failover_gate.sh
#
# Usage (GitHub Actions):
#   - name: Failover gate
#     run: ./scripts/ci_failover_gate.sh
# ============================================================================
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

FAILED=0
PASSED=0

run_gate() {
  local name="$1"; shift
  echo -e "${CYAN}[gate]${NC} $name"
  if "$@" 2>&1 | tail -3; then
    echo -e "${GREEN}[pass]${NC} $name"
    ((PASSED++))
  else
    echo -e "${RED}[FAIL]${NC} $name"
    ((FAILED++))
  fi
}

echo "============================================"
echo " FalconDB CI Failover & Replication Gate"
echo "============================================"
echo ""

# ── 1. Replication core tests ──────────────────────────────────────────────
run_gate "Replication: WAL shipping + catch-up" \
  cargo test -p falcon_cluster -- replication --no-fail-fast

# ── 2. Promote / fencing tests ─────────────────────────────────────────────
run_gate "Failover: promote fencing protocol" \
  cargo test -p falcon_cluster -- promote_fencing --no-fail-fast

# ── 3. Full lifecycle (create → replicate → fence → promote → write) ──────
run_gate "Lifecycle: M1 full lifecycle" \
  cargo test -p falcon_cluster -- m1_full_lifecycle --no-fail-fast

# ── 4. gRPC transport roundtrip ────────────────────────────────────────────
run_gate "gRPC: proto roundtrip + transport" \
  cargo test -p falcon_cluster -- grpc --no-fail-fast

# ── 5. Checkpoint streaming ────────────────────────────────────────────────
run_gate "Checkpoint: streaming + recovery" \
  cargo test -p falcon_cluster -- checkpoint --no-fail-fast

# ── 6. Fault injection (if tests exist) ────────────────────────────────────
run_gate "Fault injection: chaos scenarios" \
  cargo test -p falcon_cluster -- fault_injection --no-fail-fast

# ── 7. WAL observer + replication log ──────────────────────────────────────
run_gate "WAL observer: replication log append" \
  cargo test -p falcon_storage -- wal_observer --no-fail-fast

# ── 8. Read-only enforcement on replicas ───────────────────────────────────
run_gate "Read-only: replica write rejection" \
  cargo test -p falcon_server -- read_only --no-fail-fast

# ── 9. SHOW replication commands ───────────────────────────────────────────
run_gate "SHOW: falcon.wal_stats + node_role" \
  cargo test -p falcon_protocol_pg -- show_falcon --no-fail-fast

# ── Summary ────────────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo " Results: $PASSED passed, $FAILED failed"
echo "============================================"

if [ "$FAILED" -gt 0 ]; then
  echo -e "${RED}FAILOVER GATE FAILED${NC}"
  exit 1
else
  echo -e "${GREEN}FAILOVER GATE PASSED${NC}"
  exit 0
fi
