# FalconDB Configuration Management Guide

## Overview
Configuration is the #1 source of enterprise incidents. FalconDB v1.1.1
ensures every config change is versioned, checksummed, staged, and rollbackable.

## Architecture

```
┌─────────────────────────────────┐
│     Controller (Single Source)   │
│  ┌───────────────────────────┐  │
│  │  ConfigRollbackManager    │  │
│  │  - versioned entries      │  │
│  │  - checksum per entry     │  │
│  │  - rollout state machine  │  │
│  │  - history per key        │  │
│  └───────────────────────────┘  │
└──────────────┬──────────────────┘
               │ push (versioned)
    ┌──────────┼──────────┐
    ▼          ▼          ▼
 DataNode   Gateway   Compactor
```

## Config Entry Properties

| Property | Description |
|----------|-------------|
| `version` | Monotonically increasing u64 |
| `key` | Config key name |
| `value` | Config value string |
| `checksum` | djb2 hash of key+value |
| `applied_at` | Unix epoch seconds |
| `applied_by` | Actor who made the change |
| `rollout_state` | Staged/Canary/RollingOut/Applied/RolledBack |

## Staged Rollout Workflow

### 1. Stage
```rust
let version = mgr.stage_change("max_connections", "200", "admin");
// State: STAGED — not yet active
```

### 2. Canary
```rust
mgr.advance_rollout("max_connections", version, RolloutState::Canary);
// Applied to canary nodes only — monitor for 15min
```

### 3. Rolling Out
```rust
mgr.advance_rollout("max_connections", version, RolloutState::RollingOut);
// Gradually applied to all nodes
```

### 4. Applied
```rust
mgr.advance_rollout("max_connections", version, RolloutState::Applied);
// Fully active cluster-wide
```

### 5. Rollback (if needed)
```rust
mgr.rollback("max_connections"); // reverts to previous version
// or:
mgr.rollback_to_version("max_connections", target_version);
```

## Checksum Verification

Every config entry carries a checksum. Use `verify_checksum(key)` to detect
tampering or corruption. Mismatches are counted in `ConfigRollbackMetrics::checksum_mismatches`.

## Operational Commands

```
falconctl config set <key> <value>          # stage + apply
falconctl config stage <key> <value>        # stage only
falconctl config rollout <key> <version>    # advance rollout
falconctl config rollback <key>             # rollback to previous
falconctl config history <key>              # show version history
falconctl config verify                     # verify all checksums
```

## Metrics

| Metric | Description |
|--------|-------------|
| `changes_applied` | Total config changes applied |
| `rollbacks_executed` | Total rollbacks |
| `checksum_mismatches` | Detected corruptions |
| `staged_count` | Staged but not yet applied |
