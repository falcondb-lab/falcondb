#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #4 — Observability: Simulate Failover
# ============================================================================
# Kills the primary and promotes the replica.
# Watch the Grafana dashboard react in real time:
#   - falcon_replication_leader_changes increments
#   - falcon_replication_promote_count increments
#   - falcon_replication_lag_us resets
#   - falcon_cluster_health_status may briefly go to degraded
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"
TIMELINE="${OUTPUT_DIR}/event_timeline.txt"

HOST="127.0.0.1"
REPLICA_PORT=5434
DB="falcon"
USER="falcon"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BOLD='\033[1m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

PID_FILE="${OUTPUT_DIR}/node1.pid"

if [ ! -f "${PID_FILE}" ]; then
  fail "No node1 PID file found."
  exit 1
fi

PRIMARY_PID=$(cat "${PID_FILE}" | tr -d '[:space:]')

if ! kill -0 "${PRIMARY_PID}" 2>/dev/null; then
  info "Primary (pid ${PRIMARY_PID}) is not running."
  exit 0
fi

# Snapshot metrics before kill
info "Snapshotting metrics before failover..."
curl -sf "http://${HOST}:8080/metrics" > "${OUTPUT_DIR}/metrics_before_failover.txt" 2>/dev/null || true

KILL_TS=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")

echo ""
echo -e "  ${RED}${BOLD}>>> kill -9 ${PRIMARY_PID} <<<${NC}"
echo ""
info "Watch the Grafana dashboard — metrics will react immediately."
echo ""

kill -9 "${PRIMARY_PID}" 2>/dev/null || true
wait "${PRIMARY_PID}" 2>/dev/null || true

ok "Primary killed at ${KILL_TS}"
echo "${KILL_TS} | FAILOVER_KILL | pid=${PRIMARY_PID}" >> "${TIMELINE}"

sleep 2

# Promote replica
info "Promoting replica..."
curl -s -X POST "http://${HOST}:8081/promote" 2>/dev/null || true
sleep 2

PROMOTE_OK=false
for attempt in $(seq 1 15); do
  if psql -h "${HOST}" -p "${REPLICA_PORT}" -U "${USER}" -d "${DB}" \
    -c "SELECT 1;" &>/dev/null; then
    PROMOTE_OK=true
    ok "Replica promoted and accepting queries (${attempt}s)"
    break
  fi
  sleep 1
done

PROMOTE_TS=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "${PROMOTE_TS} | FAILOVER_PROMOTE | success=${PROMOTE_OK}" >> "${TIMELINE}"

# Snapshot metrics after promotion
info "Snapshotting metrics after failover..."
curl -sf "http://${HOST}:8081/metrics" > "${OUTPUT_DIR}/metrics_after_failover.txt" 2>/dev/null || true

echo ""
info "What to look for on the dashboard:"
echo "  • falcon_replication_leader_changes incremented"
echo "  • falcon_replication_promote_count incremented"
echo "  • falcon_cluster_health_status may have briefly changed"
echo "  • falcon_replication_lag_us reset or changed"
echo ""
