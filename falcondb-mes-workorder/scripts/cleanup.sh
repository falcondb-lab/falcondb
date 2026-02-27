#!/usr/bin/env bash
# ============================================================================
# FalconDB MES — Cleanup
# ============================================================================
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

# Stop backend
if [ -f "${OUTPUT_DIR}/backend.pid" ]; then
  PID=$(cat "${OUTPUT_DIR}/backend.pid" | tr -d '[:space:]')
  kill "${PID}" 2>/dev/null || true
  rm -f "${OUTPUT_DIR}/backend.pid"
  ok "Backend stopped"
fi

# Stop FalconDB
if [ -f "${OUTPUT_DIR}/falcon.pid" ]; then
  PID=$(cat "${OUTPUT_DIR}/falcon.pid" | tr -d '[:space:]')
  kill "${PID}" 2>/dev/null || kill -9 "${PID}" 2>/dev/null || true
  rm -f "${OUTPUT_DIR}/falcon.pid"
  ok "FalconDB stopped"
fi

if [ "${CLEAN_ALL}" = "true" ]; then
  info "Removing data..."
  rm -rf "${POC_ROOT}/mes_data"
  ok "Data removed"

  info "Removing output files..."
  rm -f "${OUTPUT_DIR}"/*.log "${OUTPUT_DIR}"/*.txt "${OUTPUT_DIR}"/*.json "${OUTPUT_DIR}"/*.csv
  ok "Output files removed"
fi

echo ""
ok "Cleanup complete"
