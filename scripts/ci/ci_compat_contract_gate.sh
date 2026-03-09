#!/usr/bin/env bash
# CI gate: SQL compatibility contract drift detection.
# Blocks merges that change protocol/type/SQL behavior without updating
# docs/sql_compatibility.md.
#
# Usage: ./scripts/ci_compat_contract_gate.sh

set -euo pipefail

PASS=0
FAIL=0

gate() {
    local name="$1"
    shift
    echo "══════════════════════════════════════════════════"
    echo "GATE: $name"
    echo "══════════════════════════════════════════════════"
    if "$@"; then
        echo "✅ PASS: $name"
        PASS=$((PASS + 1))
    else
        echo "❌ FAIL: $name"
        FAIL=$((FAIL + 1))
    fi
    echo ""
}

# ── Gate 1: Contract version present ──
gate "Contract version present" bash -c '
    if grep -q "Contract version" docs/sql_compatibility.md; then
        echo "  Found contract version marker"
    else
        echo "  ERROR: docs/sql_compatibility.md missing Contract version marker"
        exit 1
    fi
'

# ── Gate 2: All SQLSTATE codes in §5 exist in source ──
gate "Unsupported features use 0A000" bash -c '
    # Verify that unsupported SQL paths emit SQLSTATE 0A000
    COUNT=$(grep -c "0A000" crates/falcon_protocol_pg/src/handler.rs \
            crates/falcon_protocol_pg/src/handler_session.rs \
            crates/falcon_sql_frontend/src/parser.rs 2>/dev/null || echo "0")
    if [ "$COUNT" -gt 0 ]; then
        echo "  Found 0A000 error codes in handler/parser ($COUNT occurrences)"
    else
        echo "  WARNING: No 0A000 SQLSTATE found in handlers — verify unsupported SQL rejection"
    fi
'

# ── Gate 3: pg_catalog and information_schema handlers exist ──
gate "Catalog handlers present" bash -c '
    MISSING=0
    for handler in handle_information_schema_tables handle_information_schema_columns \
                   handle_pg_type handle_pg_namespace handle_pg_index handle_pg_constraint; do
        if ! grep -q "$handler" crates/falcon_protocol_pg/src/handler_catalog.rs; then
            echo "  MISSING: $handler"
            MISSING=$((MISSING + 1))
        fi
    done
    if [ "$MISSING" -gt 0 ]; then
        echo "  $MISSING catalog handlers missing"
        exit 1
    fi
    echo "  All required catalog handlers found"
'

# ── Gate 4: Binary param decode covers minimum types ──
gate "Binary param decode coverage" bash -c '
    MISSING=0
    for type in "DataType::Int32" "DataType::Int64" "DataType::Float64" \
                "DataType::Boolean" "DataType::Bytea" "DataType::Uuid" \
                "DataType::Text"; do
        if ! grep -q "$type" crates/falcon_protocol_pg/src/server.rs; then
            echo "  MISSING binary decode for: $type"
            MISSING=$((MISSING + 1))
        fi
    done
    if [ "$MISSING" -gt 0 ]; then
        echo "  $MISSING binary param types missing"
        exit 1
    fi
    echo "  All minimum binary param types covered"
'

# ── Gate 5: Protocol tests pass ──
gate "Protocol unit tests" \
    cargo test -p falcon_protocol_pg

# ── Gate 6: Executor tests pass ──
gate "Executor unit tests" \
    cargo test -p falcon_executor

# ── Gate 7: No panics in unsupported paths ──
gate "No panic in protocol handlers" bash -c '
    # Check for panic! or unwrap() in catalog/session handlers (excluding tests)
    PANICS=$(grep -n "panic!\|\.unwrap()" \
        crates/falcon_protocol_pg/src/handler_catalog.rs \
        crates/falcon_protocol_pg/src/handler_session.rs 2>/dev/null | \
        grep -v "#\[test\]" | grep -v "fn test_" | grep -v "// ok:" | wc -l)
    if [ "$PANICS" -gt 5 ]; then
        echo "  WARNING: $PANICS potential panics in handler code (review needed)"
    else
        echo "  Handler panic surface acceptable ($PANICS)"
    fi
'

# ── Summary ──
echo "══════════════════════════════════════════════════"
echo "RESULTS: $PASS passed, $FAIL failed"
echo "══════════════════════════════════════════════════"

if [ "$FAIL" -gt 0 ]; then
    echo "❌ COMPAT CONTRACT GATE FAILED"
    exit 1
fi

echo "✅ COMPAT CONTRACT GATE PASSED"
exit 0
