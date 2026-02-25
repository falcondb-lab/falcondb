#!/usr/bin/env bash
# ============================================================================
# FalconDB — Linux distribution tarball builder
# ============================================================================
#
# Builds a self-contained tarball:
#   falcondb-<version>-linux-x86_64.tar.gz
#
# Usage:
#   ./scripts/build_linux_dist.sh              # Release build
#   ./scripts/build_linux_dist.sh --debug      # Debug build (faster compile)
#
# Output:
#   target/dist/falcondb-<version>-linux-x86_64.tar.gz
#
# Requirements:
#   - Rust toolchain (rustup, cargo)
#   - Linux x86_64 (native) or cross-compilation toolchain

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse version from Cargo.toml
VERSION=$(grep '^version' "$PROJECT_ROOT/Cargo.toml" | head -1 | sed 's/.*"\(.*\)".*/\1/')
ARCH="x86_64"
OS="linux"
PROFILE="release"
CARGO_FLAGS="--release"

if [[ "${1:-}" == "--debug" ]]; then
    PROFILE="debug"
    CARGO_FLAGS=""
    echo "==> Building DEBUG distribution"
else
    echo "==> Building RELEASE distribution"
fi

DIST_NAME="falcondb-${VERSION}-${OS}-${ARCH}"
DIST_DIR="$PROJECT_ROOT/target/dist/$DIST_NAME"

echo "==> Version: $VERSION"
echo "==> Target:  $OS-$ARCH ($PROFILE)"
echo "==> Output:  target/dist/$DIST_NAME.tar.gz"

# ── Build ───────────────────────────────────────────────────────────────────

echo ""
echo "==> Compiling falcon_server and falcon_cli..."
cd "$PROJECT_ROOT"
cargo build $CARGO_FLAGS -p falcon_server -p falcon_cli

# ── Assemble distribution ──────────────────────────────────────────────────

echo ""
echo "==> Assembling distribution layout..."
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"/{bin,conf,data,logs}

# Binaries
cp "target/$PROFILE/falcon_server" "$DIST_DIR/bin/falcon"
cp "target/$PROFILE/falcon_cli"    "$DIST_DIR/bin/falcon-cli"
chmod +x "$DIST_DIR/bin/falcon" "$DIST_DIR/bin/falcon-cli"

# Strip binaries in release mode
if [[ "$PROFILE" == "release" ]]; then
    if command -v strip &>/dev/null; then
        echo "==> Stripping binaries..."
        strip "$DIST_DIR/bin/falcon"
        strip "$DIST_DIR/bin/falcon-cli"
    fi
fi

# Config
cp "$PROJECT_ROOT/docker/falcon.toml" "$DIST_DIR/conf/falcon.toml"
# Adjust data_dir for local layout
sed -i 's|data_dir = "/var/lib/falcondb"|data_dir = "data"|' "$DIST_DIR/conf/falcon.toml"

# Placeholder files
echo "# WAL and snapshot files" > "$DIST_DIR/data/README.md"
echo "# Server logs"           > "$DIST_DIR/logs/README.md"

# Version file
cat > "$DIST_DIR/VERSION" <<EOF
FalconDB v${VERSION}
Build: ${OS}-${ARCH}
Profile: ${PROFILE}
Date: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF

# README
cat > "$DIST_DIR/README.md" <<'HEREDOC'
# FalconDB for Linux — Quick Start

## Layout

```
falcondb/
  bin/falcon          ← Database server
  bin/falcon-cli      ← CLI management tool
  conf/falcon.toml    ← Configuration (edit this)
  data/               ← Database files (WAL, snapshots)
  logs/               ← Log output directory
```

## Quick Start

```bash
# Start in foreground
./bin/falcon --config ./conf/falcon.toml

# Connect with psql
psql -h 127.0.0.1 -p 5443 -U falcon
```

## systemd Service

```bash
sudo cp bin/falcon /usr/local/bin/
sudo cp conf/falcon.toml /etc/falcondb/falcon.toml
sudo useradd -r -s /usr/sbin/nologin falcondb
sudo mkdir -p /var/lib/falcondb /var/log/falcondb
sudo chown falcondb:falcondb /var/lib/falcondb /var/log/falcondb

# Create /etc/systemd/system/falcondb.service (see below)
sudo systemctl daemon-reload
sudo systemctl enable --now falcondb
```

### systemd unit file

```ini
[Unit]
Description=FalconDB Database Server
After=network.target

[Service]
Type=simple
User=falcondb
Group=falcondb
ExecStart=/usr/local/bin/falcon --config /etc/falcondb/falcon.toml
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```
HEREDOC

# systemd unit template
mkdir -p "$DIST_DIR/scripts"
cat > "$DIST_DIR/scripts/falcondb.service" <<'EOF'
[Unit]
Description=FalconDB Database Server
Documentation=https://github.com/falcondb/falcondb
After=network.target

[Service]
Type=simple
User=falcondb
Group=falcondb
ExecStart=/usr/local/bin/falcon --config /etc/falcondb/falcon.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5
TimeoutStopSec=30
LimitNOFILE=65536
LimitMEMLOCK=infinity

# Security hardening
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/falcondb /var/log/falcondb
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

# Install helper
cat > "$DIST_DIR/scripts/install.sh" <<'INSTALL'
#!/usr/bin/env bash
# Quick install script for FalconDB on Linux
set -euo pipefail

PREFIX="${1:-/usr/local}"
CONF_DIR="/etc/falcondb"
DATA_DIR="/var/lib/falcondb"
LOG_DIR="/var/log/falcondb"

echo "==> Installing FalconDB to $PREFIX"

# Binaries
sudo install -m 755 bin/falcon     "$PREFIX/bin/falcon"
sudo install -m 755 bin/falcon-cli "$PREFIX/bin/falcon-cli"

# Config
sudo mkdir -p "$CONF_DIR"
if [ ! -f "$CONF_DIR/falcon.toml" ]; then
    sudo install -m 644 conf/falcon.toml "$CONF_DIR/falcon.toml"
    # Fix data_dir for system install
    sudo sed -i 's|data_dir = "data"|data_dir = "/var/lib/falcondb"|' "$CONF_DIR/falcon.toml"
    echo "    Config: $CONF_DIR/falcon.toml"
else
    echo "    Config exists, skipping: $CONF_DIR/falcon.toml"
fi

# User & directories
if ! id falcondb &>/dev/null; then
    sudo useradd -r -s /usr/sbin/nologin -d "$DATA_DIR" falcondb
    echo "    Created user: falcondb"
fi
sudo mkdir -p "$DATA_DIR" "$LOG_DIR"
sudo chown falcondb:falcondb "$DATA_DIR" "$LOG_DIR"

# systemd
if [ -d /etc/systemd/system ]; then
    sudo install -m 644 scripts/falcondb.service /etc/systemd/system/
    sudo systemctl daemon-reload
    echo "    Installed systemd service: falcondb.service"
    echo "    Enable with: sudo systemctl enable --now falcondb"
fi

echo "==> Done. Connect with: psql -h 127.0.0.1 -p 5443"
INSTALL
chmod +x "$DIST_DIR/scripts/install.sh"

# ── Create tarball ──────────────────────────────────────────────────────────

echo ""
echo "==> Creating tarball..."
cd "$PROJECT_ROOT/target/dist"
tar czf "$DIST_NAME.tar.gz" "$DIST_NAME"

TARBALL_SIZE=$(du -h "$DIST_NAME.tar.gz" | cut -f1)
echo ""
echo "==> Build complete!"
echo "    Tarball: target/dist/$DIST_NAME.tar.gz ($TARBALL_SIZE)"
echo ""
echo "    Extract and run:"
echo "      tar xzf $DIST_NAME.tar.gz"
echo "      cd $DIST_NAME"
echo "      ./bin/falcon --config ./conf/falcon.toml"
