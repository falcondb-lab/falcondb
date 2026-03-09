#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #4 — Observability: Trigger Rebalance
# ============================================================================
# Forces a shard rebalance cycle via the admin API.
# Watch the Grafana dashboard: rebalancer metrics should react immediately.
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

info "Triggering rebalance on ${HOST}:${ADMIN_PORT}..."

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  "http://${HOST}:${ADMIN_PORT}/rebalance" 2>/dev/null || echo "000")

if [ "${HTTP_CODE}" = "200" ] || [ "${HTTP_CODE}" = "202" ]; then
  ok "Rebalance triggered (HTTP ${HTTP_CODE})"
  echo "${TS} | REBALANCE_TRIGGER | status=ok http=${HTTP_CODE}" >> "${TIMELINE}"
else
  info "Rebalance API returned HTTP ${HTTP_CODE} (endpoint may not be implemented yet)"
  echo "${TS} | REBALANCE_TRIGGER | status=attempted http=${HTTP_CODE}" >> "${TIMELINE}"
fi

info "Watch the dashboard: falcon_rebalancer_running should change to 1"
info "Watch: falcon_rebalancer_rows_migrated_total, falcon_rebalancer_imbalance_ratio"

# Snapshot current rebalancer metrics
SNAPSHOT=$(curl -sf "http://${HOST}:${ADMIN_PORT}/metrics" 2>/dev/null | \
  grep -E "^falcon_rebalancer_" || echo "(metrics not yet available)")
echo ""
echo "  Current rebalancer metrics:"
echo "${SNAPSHOT}" | sed 's/^/    /'
echo ""
