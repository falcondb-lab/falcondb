#!/usr/bin/env bash
# ============================================================================
# FalconDB — CI Version Consistency Gate
# ============================================================================
#
# Validates that ALL version references in the repo match the single source
# of truth: [workspace.package] version in the root Cargo.toml.
#
# Exit 0 = all consistent, Exit 1 = mismatch found.
# ============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CARGO_TOML="$REPO_ROOT/Cargo.toml"

# ── Extract authoritative version ──
AUTH_VERSION=$(grep -m1 '^version' "$CARGO_TOML" | sed 's/.*"\(.*\)".*/\1/')
if [ -z "$AUTH_VERSION" ]; then
    echo "FAIL: Could not extract version from $CARGO_TOML"
    exit 1
fi
echo "Authoritative version: $AUTH_VERSION"
echo ""

ERRORS=0

# ── Check 1: All crate Cargo.toml use version.workspace = true ──
echo "[1/5] Checking crate Cargo.toml files..."
for toml in "$REPO_ROOT"/crates/*/Cargo.toml; do
    crate_name=$(basename "$(dirname "$toml")")
    # Check if it uses workspace version
    if grep -q 'version\.workspace\s*=\s*true' "$toml" 2>/dev/null || \
       grep -q 'version.workspace = true' "$toml" 2>/dev/null; then
        echo "  OK: $crate_name (workspace)"
    else
        # Check if it has a hardcoded version that matches
        crate_ver=$(grep -m1 '^version' "$toml" | sed 's/.*"\(.*\)".*/\1/' || true)
        if [ "$crate_ver" = "$AUTH_VERSION" ]; then
            echo "  WARN: $crate_name has hardcoded version=$crate_ver (matches, but prefer workspace)"
        elif [ -n "$crate_ver" ]; then
            echo "  FAIL: $crate_name has version=$crate_ver (expected $AUTH_VERSION)"
            ERRORS=$((ERRORS + 1))
        fi
    fi
done
echo ""

# ── Check 2: README badge version ──
echo "[2/5] Checking README version badge..."
README="$REPO_ROOT/README.md"
if [ -f "$README" ]; then
    # Check for hardcoded version strings (excluding comments and badge)
    BADGE_VER=$(grep -oP 'badge/version-\K[^-]+' "$README" || true)
    if [ "$BADGE_VER" = "$AUTH_VERSION" ]; then
        echo "  OK: README badge matches ($BADGE_VER)"
    elif [ -n "$BADGE_VER" ]; then
        echo "  FAIL: README badge version=$BADGE_VER (expected $AUTH_VERSION)"
        ERRORS=$((ERRORS + 1))
    else
        echo "  SKIP: No version badge found"
    fi
fi
echo ""

# ── Check 3: dist/VERSION ──
echo "[3/5] Checking dist/VERSION..."
VERSION_FILE="$REPO_ROOT/dist/VERSION"
if [ -f "$VERSION_FILE" ]; then
    FILE_VER=$(head -1 "$VERSION_FILE" | sed 's/FalconDB v//')
    if [ "$FILE_VER" = "$AUTH_VERSION" ]; then
        echo "  OK: dist/VERSION matches ($FILE_VER)"
    else
        echo "  FAIL: dist/VERSION has '$FILE_VER' (expected $AUTH_VERSION)"
        ERRORS=$((ERRORS + 1))
    fi
else
    echo "  SKIP: dist/VERSION not found"
fi
echo ""

# ── Check 4: Binary version (if available) ──
echo "[4/5] Checking binary version..."
FALCON_BIN="$REPO_ROOT/target/release/falcon"
if [ ! -f "$FALCON_BIN" ]; then
    FALCON_BIN="$REPO_ROOT/target/debug/falcon"
fi
if [ -f "$FALCON_BIN" ]; then
    BIN_VER=$("$FALCON_BIN" version 2>&1 | head -1 | sed 's/FalconDB v//')
    if [ "$BIN_VER" = "$AUTH_VERSION" ]; then
        echo "  OK: Binary version matches ($BIN_VER)"
    else
        echo "  FAIL: Binary version='$BIN_VER' (expected $AUTH_VERSION)"
        ERRORS=$((ERRORS + 1))
    fi
else
    echo "  SKIP: No binary found (not built yet)"
fi
echo ""

# ── Check 5: No stale hardcoded version strings in docs ──
echo "[5/5] Scanning for stale hardcoded versions in docs..."
# Look for patterns like "v1.0.3" or "v1.0.0-rc.1" that don't match current
STALE_HITS=$(grep -rn 'FalconDB v[0-9]' "$REPO_ROOT/docs/" 2>/dev/null | grep -v "$AUTH_VERSION" | grep -v "CHANGELOG" | head -10 || true)
if [ -n "$STALE_HITS" ]; then
    echo "  WARN: Found potentially stale version references:"
    echo "$STALE_HITS" | while read -r line; do echo "    $line"; done
else
    echo "  OK: No stale version references found"
fi
echo ""

# ── Summary ──
echo "================================================================="
if [ "$ERRORS" -eq 0 ]; then
    echo "  PASS: All version references consistent ($AUTH_VERSION)"
    echo "================================================================="
    exit 0
else
    echo "  FAIL: $ERRORS version mismatch(es) found"
    echo "  Authoritative source: $CARGO_TOML"
    echo "================================================================="
    exit 1
fi
