#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #7 — Backup & PITR: Cleanup
# ============================================================================
# Usage:
#   ./scripts/cleanup.sh          # Stop processes only
#   ./scripts/cleanup.sh --all    # Also remove data, backups, output
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

if [ "${CLEAN_ALL}" = "true" ]; then
  info "Removing data directory..."
  rm -rf "${POC_ROOT}/pitr_data"
  ok "Data removed"

  info "Removing backups..."
  rm -rf "${POC_ROOT}/backups"
  ok "Backups removed"

  info "Removing WAL archive..."
  rm -rf "${POC_ROOT}/wal_archive"
  ok "WAL archive removed"

  info "Removing output files..."
  rm -f "${OUTPUT_DIR}"/*.log "${OUTPUT_DIR}"/*.txt "${OUTPUT_DIR}"/*.json "${OUTPUT_DIR}"/*.csv
  ok "Output files removed"
fi

echo ""
ok "Cleanup complete"
