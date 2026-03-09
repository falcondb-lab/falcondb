#!/usr/bin/env bash
# ============================================================================
# FalconDB DCG PoC — Kill Primary (Hard Crash)
# ============================================================================
# Sends SIGKILL to the primary node. This is NOT a graceful shutdown.
# The process is terminated immediately by the OS — no signal handler,
# no final flush, no cleanup. Equivalent to a power failure.
#
# Linux:   kill -9 <pid>
# Windows: use kill_primary.ps1 (taskkill /F)
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BOLD='\033[1m'; NC='\033[0m'

PID_FILE="${OUTPUT_DIR}/primary.pid"

if [ ! -f "${PID_FILE}" ]; then
  echo -e "  ${RED}✗${NC} No primary PID file found at ${PID_FILE}"
  echo "  Run scripts/start_cluster.sh first."
  exit 1
fi

PRIMARY_PID=$(cat "${PID_FILE}" | tr -d '[:space:]')

if ! kill -0 "${PRIMARY_PID}" 2>/dev/null; then
  echo -e "  ${YELLOW}→${NC} Primary (pid ${PRIMARY_PID}) is not running."
  exit 0
fi

KILL_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo ""
echo -e "  ${RED}${BOLD}>>> kill -9 ${PRIMARY_PID} <<<${NC}"
echo ""
echo -e "  ${YELLOW}→${NC} This simulates a sudden hardware failure or power loss."
echo -e "  ${YELLOW}→${NC} The primary has NO chance to flush buffers or send final messages."
echo ""

kill -9 "${PRIMARY_PID}" 2>/dev/null || true
wait "${PRIMARY_PID}" 2>/dev/null || true

echo -e "  ${GREEN}✓${NC} Primary killed at ${KILL_TS}"
echo "${KILL_TS}" > "${OUTPUT_DIR}/kill_timestamp.txt"
echo ""
