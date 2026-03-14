#!/usr/bin/env bash
# ============================================================================
# FalconDB MES — Kill Primary Node (Simulate Crash)
# ============================================================================
# Simulates a hard crash of the FalconDB primary node using kill -9.
# This is NOT a graceful shutdown — it's the worst-case scenario.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"
PID_FILE="${OUTPUT_DIR}/falcon.pid"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

banner "模拟主节点宕机 (Kill Primary — Hard Crash)"

echo -e "  ${RED}${BOLD}╔═══════════════════════════════════════════════╗${NC}"
echo -e "  ${RED}${BOLD}║  KILLING FalconDB primary with signal 9       ║${NC}"
echo -e "  ${RED}${BOLD}║  This simulates a sudden machine failure.     ║${NC}"
echo -e "  ${RED}${BOLD}║  No graceful shutdown. No cleanup.            ║${NC}"
echo -e "  ${RED}${BOLD}╚═══════════════════════════════════════════════╝${NC}"
echo ""

if [ ! -f "${PID_FILE}" ]; then
  fail "PID file not found: ${PID_FILE}"
  fail "Is FalconDB running? Start it with start_cluster.sh"
  exit 1
fi

FALCON_PID=$(cat "${PID_FILE}" | tr -d '[:space:]')

if ! kill -0 "${FALCON_PID}" 2>/dev/null; then
  info "FalconDB (pid ${FALCON_PID}) already dead"
else
  KILL_TS=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")
  info "Sending SIGKILL to pid ${FALCON_PID}..."
  kill -9 "${FALCON_PID}" 2>/dev/null || true
  sleep 1

  if kill -0 "${FALCON_PID}" 2>/dev/null; then
    fail "Process still alive after kill -9!"
    exit 1
  fi

  ok "FalconDB primary KILLED at ${KILL_TS}"
  echo "${KILL_TS} | KILL_PRIMARY | pid=${FALCON_PID}" >> "${OUTPUT_DIR}/event_timeline.txt"
fi

rm -f "${PID_FILE}"

# Verify the API is down
if curl -sf "http://127.0.0.1:8000/api/health" &>/dev/null; then
  info "Backend API still responds (will fail on next DB query)"
fi

echo ""
echo -e "  ${RED}主节点已宕机 (Primary is DOWN)${NC}"
echo ""
echo "  The database process has been killed with SIGKILL."
echo "  This simulates a power failure or hardware crash."
echo ""
echo "  Next steps:"
echo "    1. ./scripts/promote_replica.sh    — Restart FalconDB (recover from WAL)"
echo "    2. ./scripts/verify_business_state.sh  — Verify all business facts survived"
echo ""
