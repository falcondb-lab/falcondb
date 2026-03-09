#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #3 — Failover Under Load: One-Command Demo
# ============================================================================
#
# Demonstrates that FalconDB remains correct and predictable even when
# the primary crashes during sustained write traffic.
#
#   1. Start 2-node cluster (primary + replica)
#   2. Verify replication is caught up
#   3. Start sustained write load (background)
#   4. Wait for load to ramp up
#   5. Kill primary with SIGKILL (during active writes)
#   6. Promote replica
#   7. Wait for writer to reconnect and finish
#   8. Measure downtime window
#   9. Verify all acknowledged commits survived
#
# Usage:
#   ./run_demo.sh
#   MARKER_COUNT=100000 LOAD_RAMP_SEC=10 ./run_demo.sh
#
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/output"

export FALCON_BIN="${FALCON_BIN:-target/release/falcon_server}"
export MARKER_COUNT="${MARKER_COUNT:-50000}"
export LOAD_RAMP_SEC="${LOAD_RAMP_SEC:-5}"

BLUE='\033[0;34m'; GREEN='\033[0;32m'; RED='\033[0;31m'; BOLD='\033[1m'; NC='\033[0m'
banner() { echo -e "\n${BLUE}${BOLD}═══  $1  ═══${NC}\n"; }

# Cleanup on exit
trap "bash ${SCRIPT_DIR}/scripts/cleanup.sh 2>/dev/null || true" EXIT

banner "FalconDB — Failover Under Load (Correctness + Availability)"
echo ""
echo "  This demo proves that FalconDB does not lose acknowledged commits"
echo "  even when the primary crashes during peak write traffic."
echo ""
echo "  Markers to write: ${MARKER_COUNT}"
echo "  Load ramp time:   ${LOAD_RAMP_SEC}s (before crash)"
echo ""

# ── Step 1: Start cluster ─────────────────────────────────────────────────
banner "Step 1/8: Starting 2-node cluster"
bash "${SCRIPT_DIR}/scripts/start_cluster.sh"

# ── Step 2: Wait for cluster readiness ────────────────────────────────────
banner "Step 2/8: Verifying replication"
bash "${SCRIPT_DIR}/scripts/wait_cluster_ready.sh"

# ── Step 3: Start write load ──────────────────────────────────────────────
banner "Step 3/8: Starting sustained write load"
bash "${SCRIPT_DIR}/scripts/start_load.sh"

# ── Step 4: Let load ramp up ──────────────────────────────────────────────
banner "Step 4/8: Letting load ramp up (${LOAD_RAMP_SEC}s)"
echo "  The writer is actively committing markers to the primary..."
for i in $(seq 1 "${LOAD_RAMP_SEC}"); do
  COUNT=$(wc -l < "${OUTPUT_DIR}/committed_markers.log" 2>/dev/null | tr -d ' ')
  echo "  [${i}/${LOAD_RAMP_SEC}s] ${COUNT} markers committed"
  sleep 1
done

# ── Step 5: Kill primary (during active writes) ──────────────────────────
banner "Step 5/8: KILLING the primary (DURING ACTIVE WRITES)"
bash "${SCRIPT_DIR}/scripts/kill_primary.sh"

sleep 2

# ── Step 6: Promote replica ──────────────────────────────────────────────
banner "Step 6/8: Promoting replica to primary"
bash "${SCRIPT_DIR}/scripts/promote_replica.sh"

# ── Step 7: Wait for writer to finish ─────────────────────────────────────
banner "Step 7/8: Waiting for writer to reconnect and complete"
WRITER_PID_FILE="${OUTPUT_DIR}/writer.pid"
if [ -f "${WRITER_PID_FILE}" ]; then
  WRITER_PID=$(cat "${WRITER_PID_FILE}" | tr -d '[:space:]')
  MAX_WAIT=120
  for i in $(seq 1 "${MAX_WAIT}"); do
    if ! kill -0 "${WRITER_PID}" 2>/dev/null; then
      FINAL_COUNT=$(wc -l < "${OUTPUT_DIR}/committed_markers.log" 2>/dev/null | tr -d ' ')
      echo -e "  \033[0;32m✓\033[0m Writer finished (${FINAL_COUNT} markers committed)"
      break
    fi
    if [ $((i % 10)) -eq 0 ]; then
      COUNT=$(wc -l < "${OUTPUT_DIR}/committed_markers.log" 2>/dev/null | tr -d ' ')
      echo "  [${i}/${MAX_WAIT}s] Writer still running (${COUNT} markers)"
    fi
    sleep 1
  done
  # Force stop if still running
  if kill -0 "${WRITER_PID}" 2>/dev/null; then
    touch "${OUTPUT_DIR}/stop_writer"
    sleep 3
    kill "${WRITER_PID}" 2>/dev/null || true
  fi
fi

# ── Step 8: Monitor downtime + Verify ─────────────────────────────────────
banner "Step 8/8: Measuring downtime and verifying integrity"

bash "${SCRIPT_DIR}/scripts/monitor_downtime.sh"
echo ""
bash "${SCRIPT_DIR}/scripts/verify_integrity.sh"
VERIFY_EXIT=$?

echo ""
if [ "${VERIFY_EXIT}" -eq 0 ]; then
  echo -e "  ${GREEN}${BOLD}Demo complete. FalconDB remained correct through the crash.${NC}"
else
  echo -e "  ${RED}${BOLD}Demo complete. Verification FAILED.${NC}"
fi
echo ""
echo "  Output files: ${OUTPUT_DIR}/"
echo ""

exit "${VERIFY_EXIT}"
