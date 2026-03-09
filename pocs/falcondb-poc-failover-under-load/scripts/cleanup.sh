#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #3 — Failover Under Load: Cleanup
# ============================================================================
# Usage:
#   ./scripts/cleanup.sh          # Stop processes only
#   ./scripts/cleanup.sh --all    # Stop + delete data + output
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

# Stop writer
WRITER_PID_FILE="${OUTPUT_DIR}/writer.pid"
if [ -f "${WRITER_PID_FILE}" ]; then
  WPID=$(cat "${WRITER_PID_FILE}" | tr -d '[:space:]')
  if kill -0 "${WPID}" 2>/dev/null; then
    info "Stopping writer (pid ${WPID})..."
    # Signal graceful stop first
    touch "${OUTPUT_DIR}/stop_writer" 2>/dev/null || true
    sleep 2
    kill "${WPID}" 2>/dev/null || true
    sleep 1
    kill -9 "${WPID}" 2>/dev/null || true
    ok "Writer stopped"
  else
    info "Writer (pid ${WPID}) already stopped"
  fi
  rm -f "${WRITER_PID_FILE}"
fi

# Stop FalconDB nodes
for role in primary replica; do
  PID_FILE="${OUTPUT_DIR}/${role}.pid"
  if [ -f "${PID_FILE}" ]; then
    PID=$(cat "${PID_FILE}" | tr -d '[:space:]')
    if kill -0 "${PID}" 2>/dev/null; then
      info "Stopping ${role} (pid ${PID})..."
      kill "${PID}" 2>/dev/null || true
      sleep 1
      kill -9 "${PID}" 2>/dev/null || true
      ok "${role} stopped"
    else
      info "${role} (pid ${PID}) already stopped"
    fi
    rm -f "${PID_FILE}"
  fi
done

if [ "${1:-}" = "--all" ]; then
  info "Removing data directories..."
  rm -rf ./poc_data_primary ./poc_data_replica
  ok "Data directories removed"

  info "Removing output files..."
  rm -f "${OUTPUT_DIR}"/*.log "${OUTPUT_DIR}"/*.txt "${OUTPUT_DIR}"/*.json
  rm -f "${OUTPUT_DIR}"/*.pid "${OUTPUT_DIR}/stop_writer"
  ok "Output files removed"
fi

ok "Cleanup complete"
