#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #4 — Observability: Generate OLTP Activity
# ============================================================================
# Generates light, continuous INSERT/UPDATE traffic so metrics have data.
# This is NOT a stress test — just enough to move the dashboard.
# Runs until stopped (Ctrl+C or kill).
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"
TIMELINE="${OUTPUT_DIR}/event_timeline.txt"

HOST="127.0.0.1"
PORT="${FALCON_PORT:-5433}"
DB="${FALCON_DB:-falcon}"
USER="${FALCON_USER:-falcon}"
INTERVAL="${ACTIVITY_INTERVAL_MS:-200}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

mkdir -p "${OUTPUT_DIR}"

echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") | ACTIVITY_START | interval=${INTERVAL}ms" >> "${TIMELINE}"
info "Generating activity on ${HOST}:${PORT}/${DB} (every ${INTERVAL}ms)"
info "Press Ctrl+C to stop"

COUNTER=0
EVENT_TYPES=("order" "payment" "shipment" "refund" "login" "logout" "search" "view")

cleanup() {
  echo ""
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") | ACTIVITY_STOP | total_events=${COUNTER}" >> "${TIMELINE}"
  ok "Stopped after ${COUNTER} events"
}
trap cleanup EXIT

while true; do
  COUNTER=$((COUNTER + 1))
  TYPE="${EVENT_TYPES[$((RANDOM % ${#EVENT_TYPES[@]}))]}"

  # INSERT a new event
  psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" -q -c \
    "INSERT INTO demo_events (event_type, payload) VALUES ('${TYPE}', 'event_${COUNTER}');" \
    2>/dev/null || true

  # Every 10th event: UPDATE a random recent row
  if [ $((COUNTER % 10)) -eq 0 ]; then
    psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" -q -c \
      "UPDATE demo_events SET payload = 'updated_${COUNTER}' WHERE id = (SELECT id FROM demo_events ORDER BY RANDOM() LIMIT 1);" \
      2>/dev/null || true
  fi

  # Every 20th event: SELECT (read traffic)
  if [ $((COUNTER % 20)) -eq 0 ]; then
    psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DB}" -q -c \
      "SELECT COUNT(*) FROM demo_events WHERE event_type = '${TYPE}';" \
      2>/dev/null || true
  fi

  # Progress every 100 events
  if [ $((COUNTER % 100)) -eq 0 ]; then
    info "[${COUNTER}] events generated"
  fi

  sleep "$(echo "scale=3; ${INTERVAL}/1000" | bc 2>/dev/null || echo "0.2")"
done
