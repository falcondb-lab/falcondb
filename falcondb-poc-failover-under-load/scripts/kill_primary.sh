#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #3 — Failover Under Load: Kill Primary (Hard Crash)
# ============================================================================
# SIGKILL the primary while the writer is actively committing.
# This is the worst-case scenario: crash at peak traffic.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BOLD='\033[1m'; NC='\033[0m'

PID_FILE="${OUTPUT_DIR}/primary.pid"

if [ ! -f "${PID_FILE}" ]; then
  echo -e "  ${RED}✗${NC} No primary PID file found."
  exit 1
fi

PRIMARY_PID=$(cat "${PID_FILE}" | tr -d '[:space:]')

if ! kill -0 "${PRIMARY_PID}" 2>/dev/null; then
  echo -e "  ${YELLOW}→${NC} Primary (pid ${PRIMARY_PID}) is not running."
  exit 0
fi

# Record how many markers have been committed so far
PRE_KILL_COUNT=$(wc -l < "${OUTPUT_DIR}/committed_markers.log" 2>/dev/null | tr -d ' ')

KILL_TS=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")

echo ""
echo -e "  ${RED}${BOLD}>>> kill -9 ${PRIMARY_PID} <<<${NC}"
echo ""
echo -e "  ${YELLOW}→${NC} The primary is being killed DURING ACTIVE WRITES."
echo -e "  ${YELLOW}→${NC} Markers committed before crash: ${PRE_KILL_COUNT}"
echo -e "  ${YELLOW}→${NC} No graceful shutdown. No final flush. No signal handler."
echo ""

kill -9 "${PRIMARY_PID}" 2>/dev/null || true
wait "${PRIMARY_PID}" 2>/dev/null || true

echo -e "  ${GREEN}✓${NC} Primary killed at ${KILL_TS}"
echo "${KILL_TS}" > "${OUTPUT_DIR}/kill_timestamp.txt"
echo "${PRE_KILL_COUNT}" > "${OUTPUT_DIR}/pre_kill_marker_count.txt"
echo ""
