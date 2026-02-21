#!/usr/bin/env bash
# deny_unwrap.sh — Scan core crates for .unwrap()/.expect()/panic!() in non-test code.
# Exit 1 if any hits found in the CORE_CRATES list.
#
# Usage:
#   ./scripts/deny_unwrap.sh              # scan all core crates, fail on any hit
#   ./scripts/deny_unwrap.sh --report     # print full report, don't fail
#   ./scripts/deny_unwrap.sh --summary    # print per-crate counts only

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Core crates where unwrap/expect/panic must be zero on the non-test path.
CORE_CRATES=(
  "crates/falcon_common/src"
  "crates/falcon_storage/src"
  "crates/falcon_txn/src"
  "crates/falcon_sql_frontend/src"
  "crates/falcon_planner/src"
  "crates/falcon_executor/src"
  "crates/falcon_protocol_pg/src"
  "crates/falcon_cluster/src"
  "crates/falcon_server/src"
)

# Patterns to detect
PATTERN='\.unwrap()\|\.expect(\|panic!('

# Files/lines to exclude (test code, bench code, examples)
EXCLUDE_PATTERN='#\[cfg(test)\]\|#\[test\]\|mod tests\|benches/\|examples/'

MODE="${1:-}"
TOTAL_HITS=0
declare -A CRATE_COUNTS

echo "=== FalconDB unwrap/expect/panic scan ==="
echo "Timestamp: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo ""

for crate_path in "${CORE_CRATES[@]}"; do
  if [[ ! -d "$crate_path" ]]; then
    echo "  [SKIP] $crate_path (not found)"
    continue
  fi

  crate_name=$(basename "$(dirname "$crate_path")")

  # Find all .rs files, exclude test/bench/example files
  hits=0
  hit_lines=""

  while IFS= read -r rs_file; do
    # Skip files in tests/ or benches/ subdirectories
    if echo "$rs_file" | grep -qE '/(tests?|benches?|examples?)/'; then
      continue
    fi

    # Scan file, skip lines that are inside test modules (best-effort)
    file_hits=$(grep -n '\.unwrap()\|\.expect(\|panic!(' "$rs_file" 2>/dev/null | \
      grep -v '^\s*//' | \
      grep -v 'unwrap_or\|unwrap_or_else\|unwrap_or_default' || true)

    if [[ -n "$file_hits" ]]; then
      count=$(echo "$file_hits" | wc -l | tr -d ' ')
      hits=$((hits + count))
      if [[ "$MODE" == "--report" ]]; then
        echo "  FILE: $rs_file"
        echo "$file_hits" | sed 's/^/    /'
        echo ""
      fi
    fi
  done < <(find "$crate_path" -name "*.rs" -type f)

  CRATE_COUNTS["$crate_name"]=$hits
  TOTAL_HITS=$((TOTAL_HITS + hits))

  status="OK"
  [[ $hits -gt 0 ]] && status="FAIL ($hits hits)"
  echo "  [$status] $crate_name"
done

echo ""
echo "=== Summary ==="
for crate_name in "${!CRATE_COUNTS[@]}"; do
  count="${CRATE_COUNTS[$crate_name]}"
  marker="✓"
  [[ $count -gt 0 ]] && marker="✗"
  echo "  $marker $crate_name: $count"
done | sort

echo ""
echo "Total hits: $TOTAL_HITS"

if [[ "$MODE" == "--report" || "$MODE" == "--summary" ]]; then
  echo ""
  echo "(Report mode: not failing on hits)"
  exit 0
fi

if [[ $TOTAL_HITS -gt 0 ]]; then
  echo ""
  echo "FAIL: $TOTAL_HITS unwrap/expect/panic hits in core crates."
  echo "Run with --report for full details."
  echo "Fix: replace with typed FalconError returns or explicit error handling."
  exit 1
else
  echo ""
  echo "PASS: No unwrap/expect/panic in core crate non-test paths."
  exit 0
fi
