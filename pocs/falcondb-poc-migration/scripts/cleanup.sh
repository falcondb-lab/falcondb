#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #5 — Migration: Cleanup
# ============================================================================
# Stops FalconDB and optionally PostgreSQL. Removes data directories.
# Usage:
#   ./scripts/cleanup.sh          # Stop processes only
#   ./scripts/cleanup.sh --all    # Also remove data + output
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

CLEAN_ALL=false
[ "${1:-}" = "--all" ] && CLEAN_ALL=true

# Stop FalconDB
PID_FILE="${OUTPUT_DIR}/falcon.pid"
if [ -f "${PID_FILE}" ]; then
  FPID=$(cat "${PID_FILE}" | tr -d '[:space:]')
  if kill -0 "${FPID}" 2>/dev/null; then
    kill "${FPID}" 2>/dev/null || kill -9 "${FPID}" 2>/dev/null || true
    ok "Stopped FalconDB (pid ${FPID})"
  else
    info "FalconDB (pid ${FPID}) already stopped"
  fi
  rm -f "${PID_FILE}"
fi

# Stop PostgreSQL
PG_DATA="${POC_ROOT}/pg_data"
if [ -d "${PG_DATA}" ]; then
  info "Stopping PostgreSQL..."
  pg_ctl -D "${PG_DATA}" stop 2>/dev/null || info "PostgreSQL already stopped"
fi

if [ "${CLEAN_ALL}" = "true" ]; then
  info "Removing FalconDB data..."
  rm -rf ./mig_data_falcon
  ok "FalconDB data removed"

  info "Removing PostgreSQL data..."
  rm -rf "${PG_DATA}"
  ok "PostgreSQL data removed"

  info "Removing output files..."
  rm -f "${OUTPUT_DIR}"/*.log "${OUTPUT_DIR}"/*.txt "${OUTPUT_DIR}"/*.json "${OUTPUT_DIR}"/*.sql
  ok "Output files removed"
fi

echo ""
ok "Cleanup complete"
