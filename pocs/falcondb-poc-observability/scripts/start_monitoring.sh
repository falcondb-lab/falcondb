#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #4 — Observability: Start Monitoring Stack
# ============================================================================
# Starts Prometheus + Grafana via Docker Compose.
# Requires: docker, docker compose
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

banner "Starting Monitoring Stack (Prometheus + Grafana)"

# Check Docker
if ! command -v docker &>/dev/null; then
  fail "Docker not found. Install Docker to use the monitoring stack."
  echo ""
  echo "  Alternative: run Prometheus and Grafana manually."
  echo "  Prometheus config: ${POC_ROOT}/prom/prometheus.yml"
  echo "  Grafana dashboard: ${POC_ROOT}/dashboards/falcondb_overview.json"
  exit 1
fi
ok "Docker available"

# Start compose
info "Starting containers..."
docker compose -f "${POC_ROOT}/docker/docker-compose.yml" up -d 2>/dev/null || \
  docker-compose -f "${POC_ROOT}/docker/docker-compose.yml" up -d

# Wait for Prometheus
info "Waiting for Prometheus..."
for i in $(seq 1 20); do
  if curl -sf "http://127.0.0.1:9090/-/ready" > /dev/null 2>&1; then
    ok "Prometheus ready: http://127.0.0.1:9090"
    break
  fi
  sleep 1
done

# Wait for Grafana
info "Waiting for Grafana..."
for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:3000/api/health" > /dev/null 2>&1; then
    ok "Grafana ready: http://127.0.0.1:3000 (admin/admin)"
    break
  fi
  sleep 1
done

echo ""
echo "  ┌────────────────────────────────────────────────────┐"
echo "  │  Monitoring Stack                                   │"
echo "  │  Prometheus: http://127.0.0.1:9090                  │"
echo "  │  Grafana:    http://127.0.0.1:3000 (admin/admin)    │"
echo "  │                                                     │"
echo "  │  Dashboard: 'FalconDB Cluster Overview'             │"
echo "  │  → auto-provisioned, auto-refreshing every 5s       │"
echo "  └────────────────────────────────────────────────────┘"
echo ""
