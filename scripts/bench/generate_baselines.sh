#!/usr/bin/env bash
# generate_baselines.sh — Run Criterion benchmarks and extract baseline values
# for the CI regression gate (benchmark.yml).
#
# Usage:
#   ./scripts/generate_baselines.sh
#
# This will:
#   1. Run all Criterion benchmarks with --output-format bencher
#   2. Parse the output for "test <name> ... bench: <N> ns/iter"
#   3. Write each <N> to bench/baselines/<name>.txt
#
# Run this on the main branch after a clean build to establish reference values.

set -euo pipefail

BASELINES_DIR="bench/baselines"
mkdir -p "$BASELINES_DIR"

CRATES=("falcon_storage" "falcon_common" "falcon_sql_frontend")
TOTAL=0

for crate in "${CRATES[@]}"; do
  echo "==> Running benchmarks for $crate ..."
  OUTPUT=$(cargo bench -p "$crate" -- --output-format bencher 2>&1 || true)

  while IFS= read -r line; do
    if [[ "$line" =~ ^test\ (.+)\ \.\.\.\ bench:\ ([0-9,]+)\ ns/iter ]]; then
      NAME="${BASH_REMATCH[1]}"
      NS="${BASH_REMATCH[2]//,/}"
      # Sanitize name for filename: replace spaces and slashes with underscores
      SAFE_NAME=$(echo "$NAME" | tr ' /' '__')
      echo "$NS" > "$BASELINES_DIR/${SAFE_NAME}.txt"
      echo "    $NAME = ${NS} ns/iter"
      TOTAL=$((TOTAL + 1))
    fi
  done <<< "$OUTPUT"
done

echo ""
echo "==> Done. Wrote $TOTAL baselines to $BASELINES_DIR/"
echo "    Commit these files to lock in the regression gate."
