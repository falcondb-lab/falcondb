#!/usr/bin/env bash
# ============================================================================
# FalconDB PoC #5 — Migration: Smoke Test (PostgreSQL vs FalconDB)
# ============================================================================
# Runs the same workflows against both databases, compares results.
# Output: output/smoke_test_results.txt
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
APP_DIR="${POC_ROOT}/app"
OUTPUT_DIR="${POC_ROOT}/output"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }
info()   { echo -e "  ${YELLOW}→${NC} $1"; }
banner() { echo -e "\n${BLUE}${BOLD}$1${NC}\n"; }

RESULTS="${OUTPUT_DIR}/smoke_test_results.txt"
PG_JSON="${OUTPUT_DIR}/smoke_pg.json"
FALCON_JSON="${OUTPUT_DIR}/smoke_falcon.json"

banner "Smoke Test: Same App, Two Databases"

# Check Python + psycopg2
if ! python3 -c "import psycopg2" 2>/dev/null; then
  if ! python -c "import psycopg2" 2>/dev/null; then
    fail "psycopg2 not installed. Run: pip install psycopg2-binary"
    exit 1
  fi
  PYTHON_CMD="python"
else
  PYTHON_CMD="python3"
fi
ok "Python + psycopg2 available"

# ── Run against PostgreSQL ───────────────────────────────────────────────────
info "Running workflows against PostgreSQL..."
if ${PYTHON_CMD} "${APP_DIR}/demo_app.py" --config "${APP_DIR}/app.conf.postgres" --workflow all --json \
  > "${PG_JSON}" 2>"${OUTPUT_DIR}/smoke_pg_err.log"; then
  ok "PostgreSQL run complete"
else
  fail "PostgreSQL run failed — check ${OUTPUT_DIR}/smoke_pg_err.log"
fi

# ── Run against FalconDB ────────────────────────────────────────────────────
info "Running workflows against FalconDB..."
if ${PYTHON_CMD} "${APP_DIR}/demo_app.py" --config "${APP_DIR}/app.conf.falcon" --workflow all --json \
  > "${FALCON_JSON}" 2>"${OUTPUT_DIR}/smoke_falcon_err.log"; then
  ok "FalconDB run complete"
else
  fail "FalconDB run failed — check ${OUTPUT_DIR}/smoke_falcon_err.log"
fi

# ── Compare results ─────────────────────────────────────────────────────────
banner "Results Comparison"

# Build comparison table
${PYTHON_CMD} -c "
import json, sys

try:
    with open('${PG_JSON}') as f:
        pg = json.load(f)
    with open('${FALCON_JSON}') as f:
        fc = json.load(f)
except Exception as e:
    print(f'  ERROR: Cannot parse results: {e}', file=sys.stderr)
    sys.exit(1)

pg_results = {r['workflow']: r for r in pg.get('results', [])}
fc_results = {r['workflow']: r for r in fc.get('results', [])}

workflows = list(dict.fromkeys(list(pg_results.keys()) + list(fc_results.keys())))

header = f'  {\"Workflow\":<22} {\"PostgreSQL\":<14} {\"FalconDB\":<14} Match'
sep    = '  ' + '-' * 64
print(header)
print(sep)

all_pass = True
for wf in workflows:
    pg_r = pg_results.get(wf, {}).get('result', 'N/A')
    fc_r = fc_results.get(wf, {}).get('result', 'N/A')
    match = 'YES' if pg_r == fc_r else 'NO'
    if match == 'NO':
        all_pass = False
    print(f'  {wf:<22} {pg_r:<14} {fc_r:<14} {match}')

print(sep)
if all_pass:
    print('  All workflows match.')
else:
    print('  MISMATCH detected.')
    sys.exit(1)
" | tee "${RESULTS}"

echo "" >> "${RESULTS}"
echo "PostgreSQL JSON: ${PG_JSON}" >> "${RESULTS}"
echo "FalconDB JSON:   ${FALCON_JSON}" >> "${RESULTS}"
echo "Date: $(date -u +"%Y-%m-%d %H:%M:%S UTC")" >> "${RESULTS}"

echo ""
ok "Results saved to ${RESULTS}"
echo ""
