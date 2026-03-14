#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #4 — Observability: Cleanup
# ============================================================================
# Stops FalconDB nodes and monitoring stack.
# Usage:
#   ./scripts/cleanup.sh          # Stop processes only
#   ./scripts/cleanup.sh --all    # Also remove data + output + Docker volumes
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

# Stop FalconDB nodes
for node in node1 node2; do
  PID_FILE="${OUTPUT_DIR}/${node}.pid"
  if [ -f "${PID_FILE}" ]; then
    PID=$(cat "${PID_FILE}" | tr -d '[:space:]')
    if kill -0 "${PID}" 2>/dev/null; then
      kill "${PID}" 2>/dev/null || kill -9 "${PID}" 2>/dev/null || true
      ok "Stopped ${node} (pid ${PID})"
    else
      info "${node} (pid ${PID}) already stopped"
    fi
    rm -f "${PID_FILE}"
  fi
done

# Stop monitoring stack
info "Stopping monitoring containers..."
docker compose -f "${POC_ROOT}/docker/docker-compose.yml" down 2>/dev/null || \
  docker-compose -f "${POC_ROOT}/docker/docker-compose.yml" down 2>/dev/null || \
  info "Docker Compose not available or containers already stopped"

if [ "${CLEAN_ALL}" = "true" ]; then
  info "Removing data directories..."
  rm -rf ./obs_data_node1 ./obs_data_node2
  ok "Data directories removed"

  info "Removing output files..."
  rm -f "${OUTPUT_DIR}"/*.log "${OUTPUT_DIR}"/*.pid "${OUTPUT_DIR}"/*.txt "${OUTPUT_DIR}"/*.json
  rm -f "${OUTPUT_DIR}/metrics_before_failover.txt" "${OUTPUT_DIR}/metrics_after_failover.txt"
  ok "Output files removed"

  info "Removing Docker volumes..."
  docker compose -f "${POC_ROOT}/docker/docker-compose.yml" down -v 2>/dev/null || true
  ok "Docker volumes removed"
fi

echo ""
ok "Cleanup complete"
