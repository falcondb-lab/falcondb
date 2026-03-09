#!/usr/bin/env bash
# ============================================================================
# FalconDB DCG PoC — One-Command Demo
# ============================================================================
#
# This script runs the entire Deterministic Commit Guarantee demo end-to-end:
#
#   1. Start a 2-node FalconDB cluster (primary + replica)
#   2. Write orders to the primary (each individually committed)
#   3. Kill the primary with SIGKILL (simulating hardware failure)
#   4. Promote the replica to primary
#   5. Verify every committed order survived
#   6. Print PASS/FAIL verdict
#   7. Cleanup
#
# Usage:
#   ./run_demo.sh
#   ORDER_COUNT=5000 ./run_demo.sh
#
# Requirements:
#   - FalconDB binary (cargo build -p falcon_server --release)
#   - psql (PostgreSQL client)
#
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export FALCON_BIN="${FALCON_BIN:-target/release/falcon_server}"
export ORDER_COUNT="${ORDER_COUNT:-1000}"
export REPL_WAIT_SEC="${REPL_WAIT_SEC:-5}"

RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
banner() { echo -e "\n${BLUE}${BOLD}═══  $1  ═══${NC}\n"; }

# Cleanup on exit
trap "${SCRIPT_DIR}/scripts/cleanup.sh 2>/dev/null || true" EXIT

banner "FalconDB — Deterministic Commit Guarantee (DCG) PoC"
echo ""
echo "  This demo proves that FalconDB never loses a committed transaction,"
echo "  even if the primary server crashes."
echo ""

# ── Step 1: Start cluster ─────────────────────────────────────────────────
banner "Step 1/6: Starting 2-node cluster"
bash "${SCRIPT_DIR}/scripts/start_cluster.sh"

# ── Step 2: Run workload ──────────────────────────────────────────────────
banner "Step 2/6: Writing ${ORDER_COUNT} orders to primary"
bash "${SCRIPT_DIR}/scripts/run_workload.sh"

# ── Step 3: Wait for replication ──────────────────────────────────────────
banner "Step 3/6: Waiting ${REPL_WAIT_SEC}s for replication"
sleep "${REPL_WAIT_SEC}"

# ── Step 4: Kill primary ─────────────────────────────────────────────────
banner "Step 4/6: KILLING the primary (hard crash)"
bash "${SCRIPT_DIR}/scripts/kill_primary.sh"

# Brief pause for replica to detect failure
sleep 2

# ── Step 5: Promote replica ──────────────────────────────────────────────
banner "Step 5/6: Promoting replica to primary"
bash "${SCRIPT_DIR}/scripts/promote_replica.sh"

# ── Step 6: Verify ────────────────────────────────────────────────────────
banner "Step 6/6: VERIFICATION"
bash "${SCRIPT_DIR}/scripts/verify_results.sh"
VERIFY_EXIT=$?

echo ""
if [ "${VERIFY_EXIT}" -eq 0 ]; then
  echo -e "  ${GREEN}${BOLD}Demo complete. The Deterministic Commit Guarantee held.${NC}"
else
  echo -e "  ${RED}${BOLD}Demo complete. Verification FAILED.${NC}"
fi
echo ""
echo "  Output files: ${SCRIPT_DIR}/output/"
echo ""

exit "${VERIFY_EXIT}"
