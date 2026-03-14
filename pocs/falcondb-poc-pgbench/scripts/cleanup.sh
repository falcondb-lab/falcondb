#!/usr/bin/env bash
# ============================================================================
# FalconDB pgbench PoC — Cleanup
# ============================================================================
# Stops FalconDB and (if managed) PostgreSQL. Optionally removes data.
#
# Usage:
#   ./scripts/cleanup.sh          # Stop processes only
#   ./scripts/cleanup.sh --all    # Stop + delete data + results
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESULTS_DIR="${POC_ROOT}/results"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

# ── Stop FalconDB ─────────────────────────────────────────────────────────
FALCON_PID_FILE="${RESULTS_DIR}/raw/falcon/falcon.pid"
if [ -f "${FALCON_PID_FILE}" ]; then
  FPID=$(cat "${FALCON_PID_FILE}" | tr -d '[:space:]')
  if kill -0 "${FPID}" 2>/dev/null; then
    info "Stopping FalconDB (pid ${FPID})..."
    kill "${FPID}" 2>/dev/null || true
    sleep 1
    kill -9 "${FPID}" 2>/dev/null || true
    ok "FalconDB stopped"
  else
    info "FalconDB (pid ${FPID}) already stopped"
  fi
  rm -f "${FALCON_PID_FILE}"
fi

# ── Stop PostgreSQL (only if we started it) ────────────────────────────────
PG_MODE_FILE="${RESULTS_DIR}/raw/postgres/start_mode.txt"
PG_DATADIR="${PG_DATADIR:-./pg_bench_data}"

if [ -f "${PG_MODE_FILE}" ] && [ "$(cat "${PG_MODE_FILE}")" = "managed" ]; then
  if [ -d "${PG_DATADIR}" ]; then
    info "Stopping managed PostgreSQL..."
    pg_ctl -D "${PG_DATADIR}" stop -m fast 2>/dev/null || true
    ok "PostgreSQL stopped"
  fi
fi

# ── Optionally remove data ────────────────────────────────────────────────
if [ "${1:-}" = "--all" ]; then
  info "Removing data directories..."
  rm -rf ./falcon_bench_data "${PG_DATADIR}"
  ok "Data directories removed"

  info "Removing results..."
  rm -rf "${RESULTS_DIR}/raw/falcon" "${RESULTS_DIR}/raw/postgres"
  rm -f "${RESULTS_DIR}/parsed/"*.json
  rm -f "${RESULTS_DIR}/report.md"
  ok "Results removed"
fi

ok "Cleanup complete"
