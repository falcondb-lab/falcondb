# FalconDB Cost & Capacity Guide

## Overview
v1.1.1 gives enterprises full visibility into where resources are spent
and automated guidance on when/how to scale.

## Cost Visibility

### Per-Table Metrics
| Metric | Description |
|--------|-------------|
| `hot_memory_bytes` | Active MemTable memory |
| `cold_memory_bytes` | Compressed cold storage |
| `wal_io_bytes` | WAL write IO since last reset |
| `replication_bytes` | Replication traffic sent |
| `row_count` | Total rows |
| `compression_ratio` | Uncompressed / compressed |
| `compression_savings_bytes` | Bytes saved by compression |

### Per-Shard Metrics
| Metric | Description |
|--------|-------------|
| `hot_memory_bytes` | Shard hot memory |
| `cold_memory_bytes` | Shard cold storage |
| `wal_io_bytes` | Shard WAL IO |
| `replication_bytes` | Shard replication traffic |
| `table_count` | Tables in shard |
| `total_rows` | Rows in shard |

### Cluster Summary
- Total hot/cold memory, WAL IO, replication traffic
- Average compression ratio, total compression savings
- Breakdown by table and shard

### Endpoints
- `GET /admin/v2/cost` — cluster cost summary
- `falconctl cost report` — CLI cost report

## Capacity Guard v2

### Pressure Detection
| Pressure | Trigger | Severity |
|----------|---------|----------|
| Memory Approaching | Usage > 75% | Warning |
| Memory Approaching | Usage > 90% | Urgent |
| WAL Growth Too Fast | Fills in < 24h | Warning |
| WAL Growth Too Fast | Fills in < 6h | Urgent |
| Cold Segment Bloat | Compression ratio > 0.8 | Advisory |
| Shard Imbalance | Load skew > threshold | Warning |
| Connection Saturation | Usage > 80% | Warning |

### Recommendations
| Type | Description |
|------|-------------|
| `ScaleOut` | Add N nodes (with reason) |
| `IncreaseCompression` | Switch compression profile |
| `WalGc` | Adjust WAL GC threshold |
| `ColdCompaction` | Trigger cold recompaction |
| `ShardRebalance` | Identify overloaded nodes |
| `ConnectionPoolResize` | Adjust pool size |

### Configuration
```rust
guard.set_threshold(PressureType::MemoryApproaching, 0.75, 0.90);
guard.set_threshold(PressureType::WalGrowthTooFast, 0.60, 0.85);
```

## Change Impact Preview

Before any operational change, preview its impact:

```
Change: compression balanced → aggressive
Impact:
  CPU:     +5.0%
  Memory:  -100MB (cold)
  Storage: -500MB
  p99:     +200us
  Guardrail: OK
  Risk:    LOW
```

### Supported Changes
- Increase/decrease compression
- Add/remove nodes
- Change replication factor
- Resize connection pool
- Adjust WAL GC
- Enable cold compaction

### Risk Levels
| Risk | Description |
|------|-------------|
| LOW | Minimal impact, guardrails satisfied |
| MEDIUM | Some impact, monitor recommended |
| HIGH | Significant impact, guardrails may breach |
