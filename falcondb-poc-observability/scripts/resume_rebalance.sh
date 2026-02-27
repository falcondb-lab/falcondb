#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #4 — Observability: Resume Rebalance
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"
TIMELINE="${OUTPUT_DIR}/event_timeline.txt"

HOST="127.0.0.1"
ADMIN_PORT="${ADMIN_PORT:-8080}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

info "Resuming rebalancer on ${HOST}:${ADMIN_PORT}..."

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  "http://${HOST}:${ADMIN_PORT}/rebalance/resume" 2>/dev/null || echo "000")

if [ "${HTTP_CODE}" = "200" ] || [ "${HTTP_CODE}" = "202" ]; then
  ok "Rebalancer resumed (HTTP ${HTTP_CODE})"
  echo "${TS} | REBALANCE_RESUME | status=ok http=${HTTP_CODE}" >> "${TIMELINE}"
else
  info "Resume API returned HTTP ${HTTP_CODE} (endpoint may not be implemented yet)"
  echo "${TS} | REBALANCE_RESUME | status=attempted http=${HTTP_CODE}" >> "${TIMELINE}"
fi

info "Watch the dashboard: falcon_rebalancer_paused should return to 0"
info "The rebalancer picks up where it left off."
