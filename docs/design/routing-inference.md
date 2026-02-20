# Routing Inference Design

## Overview
Cedar's planner infers transaction routing from the physical plan to determine
whether a query can use the fast-path (single-shard) or must use the slow-path
(cross-shard 2PC).

## Key Components

### TxnRoutingHint (cedar_planner)
- `involved_shards: Vec<ShardId>` — shards touched by the plan
- `single_shard_proven: bool` — true if provably single-shard
- `inference_reason: String` — human-readable explanation

### Shard Key Inference
- Equality predicates on shard key columns → single-shard routing
- IN-list predicates → equality set extraction for multi-value routing
- Volatile functions (RANDOM, NOW, etc.) are rejected from shard key inference

### Parameter Type Inference
- `$N` parameter types inferred from schema context (e.g., `WHERE id = $1`)
- Used for prepared statement optimization

## Flow
1. SQL → Binder → BoundStatement
2. Planner → PhysicalPlan → `routing_hint()`
3. TxnRoutingHint → TxnClassification → commit path selection
