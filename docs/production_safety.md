# FalconDB — Production Safety Mode

> **Purpose**: Prevent common misconfiguration mistakes that silently void FalconDB's
> Deterministic Commit Guarantee (DCG) or expose the system to operational risk.

---

## Quick Start

Add this to your `falcon.toml` to enable enforcement:

```toml
[production_safety]
enforce = true
```

When `enforce = true`, FalconDB **refuses to start** if any CRITICAL safety check fails.
When `enforce = false` (default), violations are logged as warnings but startup proceeds.

---

## Safety Checks

| ID | Check | Severity | What It Catches |
|----|-------|----------|-----------------|
| **PS-1** | WAL is enabled | CRITICAL | Running without WAL means zero crash safety — the DCG is void |
| **PS-2** | WAL sync_mode is `fsync` or `fdatasync` | CRITICAL | `sync_mode = "none"` means committed data may be lost on crash |
| **PS-3** | Authentication is not `trust` | CRITICAL | `trust` mode lets anyone connect without a password |
| **PS-4** | Memory hard limit is configured | WARN | Without limits, unbounded growth may trigger OOM kill |
| **PS-5** | Statement timeout is configured | WARN | Without timeout, runaway queries can hold locks indefinitely |
| **PS-6** | TLS is configured | WARN | Without TLS, all client traffic (including passwords) is plaintext |
| **PS-7** | Shutdown drain timeout > 0 | WARN | Zero drain timeout kills active connections immediately on shutdown |

---

## Severity Levels

- **CRITICAL**: Directly undermines the DCG or creates an unacceptable security gap.
  When `enforce = true`, any CRITICAL violation blocks startup.
- **WARN**: Represents operational risk but does not void the DCG.
  Always logged, never blocks startup.

---

## Example: Minimal Production Configuration

```toml
config_version = 4

[production_safety]
enforce = true

[server]
pg_listen_addr = "0.0.0.0:5433"
admin_listen_addr = "0.0.0.0:8080"
node_id = 1
max_connections = 512
statement_timeout_ms = 30000        # PS-5: 30 second timeout
shutdown_drain_timeout_secs = 30    # PS-7: graceful drain

[server.auth]
method = "scram-sha-256"            # PS-3: real authentication
password = "${FALCON_AUTH_PASSWORD}" # from environment variable
username = "falcon"

[server.tls]
cert_path = "/etc/falcondb/tls/server.crt"  # PS-6: TLS enabled
key_path  = "/etc/falcondb/tls/server.key"

[storage]
wal_enabled = true                  # PS-1: WAL enabled
data_dir = "/var/lib/falcondb/data"

[wal]
sync_mode = "fsync"                 # PS-2: durable fsync
group_commit = true
flush_interval_us = 1000
segment_size_bytes = 67108864

[memory]
shard_soft_limit_bytes = 4294967296  # 4 GB soft limit
shard_hard_limit_bytes = 6442450944  # 6 GB hard limit (PS-4)
```

---

## Example: Development Configuration (safety off)

```toml
# Development mode — safety is off, convenience is maximized
# DO NOT use this in production!

[production_safety]
enforce = false

[server]
pg_listen_addr = "127.0.0.1:5433"
admin_listen_addr = "127.0.0.1:8080"
node_id = 1
max_connections = 128

[server.auth]
method = "trust"                    # PS-3 violation — acceptable for dev

[storage]
wal_enabled = true
data_dir = "./falcon_data"

[wal]
sync_mode = "fdatasync"
group_commit = true
flush_interval_us = 1000
segment_size_bytes = 67108864
```

---

## How It Works

1. **Config loaded** — FalconDB reads `falcon.toml` and applies CLI overrides.
2. **Safety validation** — `validate_production_safety()` runs all checks against the
   final merged config.
3. **Violations logged** — Each violation is emitted as a structured log entry with its
   ID and severity.
4. **Enforce gate** — If `enforce = true` and any CRITICAL violations exist, startup
   is aborted with an error listing the violations.

### Code Reference

| Component | File |
|-----------|------|
| Config struct | `crates/falcon_common/src/config.rs` — `ProductionSafetyConfig` |
| Validation logic | `crates/falcon_common/src/config.rs` — `validate_production_safety()` |
| Startup integration | `crates/falcon_server/src/main.rs` — `run_server_inner()` |

---

## Roadmap

Future safety checks under consideration:

| ID | Check | Severity | Status |
|----|-------|----------|--------|
| PS-8 | Replication configured for Primary role | WARN | Planned |
| PS-9 | Backup schedule configured | WARN | Planned |
| PS-10 | Monitoring endpoint accessible | WARN | Planned |

---

## FAQ

**Q: Does this change any runtime behavior?**
A: No. Production safety mode only runs at startup. It validates configuration and either
   proceeds or aborts. It does not change how FalconDB processes queries or commits.

**Q: Can I use `--no-wal` with `enforce = true`?**
A: No. The `--no-wal` CLI flag sets `storage.wal_enabled = false`, which triggers PS-1
   (CRITICAL). If you need in-memory mode for testing, set `enforce = false`.

**Q: What happens if I have WARN violations but no CRITICAL ones?**
A: FalconDB starts normally. Warnings are logged but never block startup, regardless of
   the `enforce` setting.
