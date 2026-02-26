#!/usr/bin/env bash
# ============================================================================
# FalconDB — RPM package (.rpm) builder
# ============================================================================
#
# Builds: falcondb-<version>-1.x86_64.rpm
#
# Usage:
#   ./scripts/build_rpm.sh              # Release build
#   ./scripts/build_rpm.sh --debug      # Debug build (faster compile)
#
# Output:
#   target/dist/falcondb-<version>-1.x86_64.rpm
#
# Requirements:
#   - Rust toolchain (rustup, cargo)
#   - rpmbuild (rpm-build package on RHEL/Fedora/CentOS)
#   - Linux x86_64

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse version from Cargo.toml
VERSION=$(grep '^version' "$PROJECT_ROOT/Cargo.toml" | head -1 | sed 's/.*"\(.*\)".*/\1/')
ARCH="x86_64"
PROFILE="release"
CARGO_FLAGS="--release"

if [[ "${1:-}" == "--debug" ]]; then
    PROFILE="debug"
    CARGO_FLAGS=""
    echo "==> Building DEBUG .rpm package"
else
    echo "==> Building RELEASE .rpm package"
fi

RPM_NAME="falcondb-${VERSION}-1.${ARCH}"

echo "==> Version: $VERSION"
echo "==> Output:  target/dist/$RPM_NAME.rpm"

# ── Build ───────────────────────────────────────────────────────────────────

echo ""
echo "==> Compiling falcon_server and falcon_cli..."
cd "$PROJECT_ROOT"
cargo build $CARGO_FLAGS -p falcon_server -p falcon_cli

# ── Prepare rpmbuild tree ──────────────────────────────────────────────────

echo ""
echo "==> Preparing rpmbuild tree..."
RPM_BUILD="$PROJECT_ROOT/target/dist/rpmbuild"
rm -rf "$RPM_BUILD"
mkdir -p "$RPM_BUILD"/{SPECS,SOURCES,BUILD,RPMS,SRPMS}

# Copy binaries to SOURCES
cp "target/$PROFILE/falcon_server" "$RPM_BUILD/SOURCES/falcon"
cp "target/$PROFILE/falcon_cli"    "$RPM_BUILD/SOURCES/falcon-cli"
chmod 0755 "$RPM_BUILD/SOURCES/falcon" "$RPM_BUILD/SOURCES/falcon-cli"

# Strip binaries in release mode
if [[ "$PROFILE" == "release" ]]; then
    if command -v strip &>/dev/null; then
        echo "==> Stripping binaries..."
        strip "$RPM_BUILD/SOURCES/falcon"
        strip "$RPM_BUILD/SOURCES/falcon-cli"
    fi
fi

# Config file
cp "$PROJECT_ROOT/docker/falcon.toml" "$RPM_BUILD/SOURCES/falcon.toml"
sed -i 's|data_dir = "data"|data_dir = "/var/lib/falcondb"|' "$RPM_BUILD/SOURCES/falcon.toml"

# systemd unit
cp "$PROJECT_ROOT/packaging/systemd/falcondb.service" "$RPM_BUILD/SOURCES/"

# Spec file
cp "$PROJECT_ROOT/packaging/rpm/falcondb.spec" "$RPM_BUILD/SPECS/"

# ── Build RPM ──────────────────────────────────────────────────────────────

echo ""
echo "==> Building .rpm package..."
rpmbuild --define "_topdir $RPM_BUILD" \
         --define "_version $VERSION" \
         -bb "$RPM_BUILD/SPECS/falcondb.spec"

# Copy result to target/dist
RPM_FILE=$(find "$RPM_BUILD/RPMS" -name "*.rpm" | head -1)
if [ -n "$RPM_FILE" ]; then
    cp "$RPM_FILE" "$PROJECT_ROOT/target/dist/$RPM_NAME.rpm"
    RPM_SIZE=$(du -h "$PROJECT_ROOT/target/dist/$RPM_NAME.rpm" | cut -f1)
    echo ""
    echo "==> Build complete!"
    echo "    Package: target/dist/$RPM_NAME.rpm ($RPM_SIZE)"
    echo ""
    echo "    Install with:"
    echo "      sudo rpm -i $RPM_NAME.rpm"
    echo "      sudo systemctl start falcondb"
else
    echo "ERROR: RPM file not found in $RPM_BUILD/RPMS"
    exit 1
fi
