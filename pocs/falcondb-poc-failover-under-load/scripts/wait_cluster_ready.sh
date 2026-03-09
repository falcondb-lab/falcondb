#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #3 — Wait for Cluster Ready (Replication Caught Up)
# ============================================================================
# Waits until both nodes are healthy and replication is fully caught up.
# This ensures the cluster is in a stable state before starting load.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${POC_ROOT}/output"

HOST="127.0.0.1"
PRIMARY_PORT=5433
REPLICA_PORT=5434
DB="falcon"
USER="falcon"
MAX_WAIT="${CLUSTER_READY_TIMEOUT:-30}"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }
info() { echo -e "  ${YELLOW}→${NC} $1"; }

info "Waiting for cluster readiness (max ${MAX_WAIT}s)..."

# Check primary is writable
PRIMARY_OK=false
for i in $(seq 1 "${MAX_WAIT}"); do
  if psql -h "${HOST}" -p "${PRIMARY_PORT}" -U "${USER}" -d "${DB}" \
    -c "SELECT 1;" &>/dev/null; then
    PRIMARY_OK=true
    ok "Primary is accepting queries"
    break
  fi
  sleep 1
done
if [ "${PRIMARY_OK}" = "false" ]; then
  fail "Primary not ready within ${MAX_WAIT}s"
  exit 1
fi

# Check replica is connected
REPLICA_OK=false
for i in $(seq 1 "${MAX_WAIT}"); do
  if psql -h "${HOST}" -p "${REPLICA_PORT}" -U "${USER}" -d "${DB}" \
    -c "SELECT 1;" &>/dev/null; then
    REPLICA_OK=true
    ok "Replica is accepting queries"
    break
  fi
  sleep 1
done
if [ "${REPLICA_OK}" = "false" ]; then
  fail "Replica not ready within ${MAX_WAIT}s"
  exit 1
fi

# Write a canary row to primary and check it appears on replica
info "Checking replication (canary write)..."
psql -h "${HOST}" -p "${PRIMARY_PORT}" -U "${USER}" -d "${DB}" \
  -c "CREATE TABLE IF NOT EXISTS _canary (id INT PRIMARY KEY); INSERT INTO _canary VALUES (1) ON CONFLICT DO NOTHING;" \
  &>/dev/null || true

CANARY_OK=false
for i in $(seq 1 15); do
  RESULT=$(psql -h "${HOST}" -p "${REPLICA_PORT}" -U "${USER}" -d "${DB}" \
    -t -A -c "SELECT id FROM _canary WHERE id = 1;" 2>/dev/null || echo "")
  if [ "${RESULT}" = "1" ]; then
    CANARY_OK=true
    ok "Replication confirmed (canary replicated in ${i}s)"
    break
  fi
  sleep 1
done

# Cleanup canary
psql -h "${HOST}" -p "${PRIMARY_PORT}" -U "${USER}" -d "${DB}" \
  -c "DROP TABLE IF EXISTS _canary;" &>/dev/null || true

if [ "${CANARY_OK}" = "false" ]; then
  fail "Replication not caught up within 15s"
  exit 1
fi

ok "Cluster is ready for load"
