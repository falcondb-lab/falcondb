#!/usr/bin/env bash
# ============================================================================
# FalconDB — Debian package (.deb) builder
# ============================================================================
#
# Builds: falcondb_<version>_amd64.deb
#
# Usage:
#   ./scripts/build_deb.sh              # Release build
#   ./scripts/build_deb.sh --debug      # Debug build (faster compile)
#
# Output:
#   target/dist/falcondb_<version>_amd64.deb
#
# Requirements:
#   - Rust toolchain (rustup, cargo)
#   - dpkg-deb (part of dpkg on Debian/Ubuntu)
#   - Linux x86_64

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse version from Cargo.toml
VERSION=$(grep '^version' "$PROJECT_ROOT/Cargo.toml" | head -1 | sed 's/.*"\(.*\)".*/\1/')
ARCH="amd64"
PROFILE="release"
CARGO_FLAGS="--release"

if [[ "${1:-}" == "--debug" ]]; then
    PROFILE="debug"
    CARGO_FLAGS=""
    echo "==> Building DEBUG .deb package"
else
    echo "==> Building RELEASE .deb package"
fi

DEB_NAME="falcondb_${VERSION}_${ARCH}"
DEB_ROOT="$PROJECT_ROOT/target/dist/deb/$DEB_NAME"

echo "==> Version: $VERSION"
echo "==> Output:  target/dist/$DEB_NAME.deb"

# ── Build ───────────────────────────────────────────────────────────────────

echo ""
echo "==> Compiling falcon_server and falcon_cli..."
cd "$PROJECT_ROOT"
cargo build $CARGO_FLAGS -p falcon_server -p falcon_cli

# ── Assemble package tree ───────────────────────────────────────────────────

echo ""
echo "==> Assembling Debian package tree..."
rm -rf "$DEB_ROOT"
mkdir -p "$DEB_ROOT"/{DEBIAN,usr/bin,etc/falcondb,usr/lib/systemd/system,var/lib/falcondb,var/log/falcondb}

# Binaries
cp "target/$PROFILE/falcon_server" "$DEB_ROOT/usr/bin/falcon"
cp "target/$PROFILE/falcon_cli"    "$DEB_ROOT/usr/bin/falcon-cli"
chmod 0755 "$DEB_ROOT/usr/bin/falcon" "$DEB_ROOT/usr/bin/falcon-cli"

# Strip binaries in release mode
if [[ "$PROFILE" == "release" ]]; then
    if command -v strip &>/dev/null; then
        echo "==> Stripping binaries..."
        strip "$DEB_ROOT/usr/bin/falcon"
        strip "$DEB_ROOT/usr/bin/falcon-cli"
    fi
fi

# Config (use docker/falcon.toml as base, adjust paths for system install)
cp "$PROJECT_ROOT/docker/falcon.toml" "$DEB_ROOT/etc/falcondb/falcon.toml"
sed -i 's|data_dir = "/var/lib/falcondb"|data_dir = "/var/lib/falcondb"|' "$DEB_ROOT/etc/falcondb/falcon.toml"
sed -i 's|data_dir = "data"|data_dir = "/var/lib/falcondb"|' "$DEB_ROOT/etc/falcondb/falcon.toml"
chmod 0640 "$DEB_ROOT/etc/falcondb/falcon.toml"

# systemd unit
cp "$PROJECT_ROOT/packaging/systemd/falcondb.service" "$DEB_ROOT/usr/lib/systemd/system/"
chmod 0644 "$DEB_ROOT/usr/lib/systemd/system/falcondb.service"

# DEBIAN control files
sed "s/\${VERSION}/$VERSION/" "$PROJECT_ROOT/packaging/debian/control" > "$DEB_ROOT/DEBIAN/control"

# Calculate installed size (in KB)
INSTALLED_SIZE=$(du -sk "$DEB_ROOT" | cut -f1)
echo "Installed-Size: $INSTALLED_SIZE" >> "$DEB_ROOT/DEBIAN/control"

cp "$PROJECT_ROOT/packaging/debian/postinst"  "$DEB_ROOT/DEBIAN/"
cp "$PROJECT_ROOT/packaging/debian/prerm"     "$DEB_ROOT/DEBIAN/"
cp "$PROJECT_ROOT/packaging/debian/conffiles" "$DEB_ROOT/DEBIAN/"
chmod 0755 "$DEB_ROOT/DEBIAN/postinst" "$DEB_ROOT/DEBIAN/prerm"
chmod 0644 "$DEB_ROOT/DEBIAN/conffiles"

# ── Build .deb ──────────────────────────────────────────────────────────────

echo ""
echo "==> Building .deb package..."
cd "$PROJECT_ROOT/target/dist"
dpkg-deb --build "deb/$DEB_NAME" "$DEB_NAME.deb"

DEB_SIZE=$(du -h "$DEB_NAME.deb" | cut -f1)
echo ""
echo "==> Build complete!"
echo "    Package: target/dist/$DEB_NAME.deb ($DEB_SIZE)"
echo ""
echo "    Install with:"
echo "      sudo dpkg -i $DEB_NAME.deb"
echo "      sudo systemctl start falcondb"
