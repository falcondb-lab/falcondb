#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #5 — Migration: Switch Application to FalconDB
# ============================================================================
# Changes ONLY the connection config — no code changes.
# This script copies app.conf.falcon over the active config.
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
APP_DIR="${POC_ROOT}/app"
OUTPUT_DIR="${POC_ROOT}/output"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

banner "Switching Application to FalconDB"

info "Before (PostgreSQL config):"
echo ""
cat "${APP_DIR}/app.conf.postgres" | sed 's/^/    /'
echo ""

info "After (FalconDB config):"
echo ""
cat "${APP_DIR}/app.conf.falcon" | sed 's/^/    /'
echo ""

info "Changes:"
echo "    host:     127.0.0.1  →  127.0.0.1  (same)"
echo "    port:     5432       →  5433"
echo "    user:     postgres   →  falcon"
echo "    dbname:   shop_demo  →  shop_demo  (same)"
echo "    password: (none)     →  (none)"
echo ""
echo "    Code changes: NONE"
echo "    SQL changes:  NONE"
echo "    Driver:       psycopg2 (unchanged)"
echo ""

ok "To run against FalconDB, use: --config app.conf.falcon"
ok "To run against PostgreSQL, use: --config app.conf.postgres"
echo ""
info "The application is the same binary / script. Only the config file changes."
echo ""

# Log the switch
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") | APP_SWITCH | target=falcon port=5433" >> "${OUTPUT_DIR}/event_timeline.txt" 2>/dev/null || true
