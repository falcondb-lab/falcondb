# FalconDB — Linux Installation Guide

## Package Formats

| Format | Distro | Command |
|--------|--------|---------|
| `.deb` | Ubuntu 20.04+, Debian 11+ | `sudo dpkg -i falcondb_<ver>_amd64.deb` |
| `.rpm` | RHEL 8+, Rocky 8+, Fedora 38+ | `sudo rpm -i falcondb-<ver>-1.x86_64.rpm` |
| `.tar.gz` | Any Linux x86_64 | Extract + run `scripts/install.sh` |

## Prerequisites

- Linux x86_64 (kernel 4.18+)
- glibc 2.17+
- No additional runtime dependencies

## 1. Install via .deb (Recommended for Ubuntu/Debian)

```bash
sudo dpkg -i falcondb_1.2.0_amd64.deb
```

This automatically:
- Installs `/usr/bin/falcon` and `/usr/bin/falcon-cli`
- Creates config at `/etc/falcondb/falcon.toml`
- Creates system user `falcondb`
- Creates data dir `/var/lib/falcondb` and log dir `/var/log/falcondb`
- Installs systemd service `falcondb.service`

Start the service:

```bash
sudo systemctl start falcondb
sudo systemctl status falcondb
```

## 2. Install via .rpm (RHEL/Rocky/Fedora)

```bash
sudo rpm -i falcondb-1.2.0-1.x86_64.rpm
```

Same layout as .deb. Start:

```bash
sudo systemctl start falcondb
sudo systemctl status falcondb
```

## 3. Install from Tarball

```bash
tar xzf falcondb-1.2.0-linux-x86_64.tar.gz
cd falcondb-1.2.0-linux-x86_64

# Option A: System install (recommended for production)
sudo ./scripts/install.sh

# Option B: Run in-place (development/testing)
./bin/falcon --config ./conf/falcon.toml
```

## Filesystem Layout (System Install)

```
/usr/bin/falcon                       # Server binary
/usr/bin/falcon-cli                   # CLI tool
/etc/falcondb/falcon.toml             # Configuration (preserved on upgrade)
/var/lib/falcondb/                    # Data directory (WAL, snapshots)
/var/log/falcondb/                    # Log files
/usr/lib/systemd/system/falcondb.service  # systemd unit
```

## systemd Service Management

```bash
# Start / stop / restart
sudo systemctl start falcondb
sudo systemctl stop falcondb
sudo systemctl restart falcondb

# Enable on boot
sudo systemctl enable falcondb

# View logs
sudo journalctl -u falcondb -f

# Check status
sudo systemctl status falcondb
```

## Configuration

Edit `/etc/falcondb/falcon.toml`:

```toml
[server]
pg_listen_addr = "0.0.0.0:5443"    # PostgreSQL wire protocol
admin_listen_addr = "0.0.0.0:8080" # Health/admin HTTP

[storage]
data_dir = "/var/lib/falcondb"

[server.auth]
method = "trust"    # Change to "scram-sha-256" for production
```

After editing, restart:

```bash
sudo systemctl restart falcondb
```

## Connect

```bash
psql -h 127.0.0.1 -p 5443 -U falcon
```

## Uninstall

### Debian/Ubuntu
```bash
sudo systemctl stop falcondb
sudo dpkg -r falcondb
# To also remove config: sudo dpkg --purge falcondb
```

### RHEL/Rocky/Fedora
```bash
sudo systemctl stop falcondb
sudo rpm -e falcondb
```

**Note:** Data in `/var/lib/falcondb` is preserved on uninstall. Remove manually if no longer needed.

## Building Packages from Source

```bash
# Debian package
./scripts/build_deb.sh

# RPM package
./scripts/build_rpm.sh

# Tarball
./scripts/build_linux_dist.sh
```

## Security Hardening

The systemd unit includes:
- `ProtectSystem=strict` — read-only filesystem except data/log dirs
- `ProtectHome=true` — no access to /home
- `NoNewPrivileges=true` — prevent privilege escalation
- `PrivateTmp=true` — isolated /tmp
- `PrivateDevices=true` — no device access
- `LimitNOFILE=65536` — sufficient file descriptors
- `OOMScoreAdjust=-900` — prefer keeping FalconDB alive

For production, also configure:
- TLS (`[server.tls]` in falcon.toml)
- Authentication (`[server.auth]` method = "scram-sha-256")
- Firewall rules for ports 5443, 8080, 50051
