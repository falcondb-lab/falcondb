# GC Safety Model — Two-Phase Mark/Sweep

## Core Invariant

**A segment may be deleted if and only if it is unreachable from all live references.**

## Reachability Definition

A segment `S` is **reachable** if ANY of these hold:

1. `S ∈ manifest.segments` — currently in the active manifest
2. `S ∈ snapshot_pinned` — pinned by an active snapshot
3. `∃ node_id : S ∈ catchup_anchors[node_id]` — needed by a follower catch-up
4. `S ∈ streaming_segments` — currently being streamed to a follower

**Contrapositive**: `S` is GC-eligible only when NONE of the above hold.

## Safety Proof

### Theorem: No live data is lost by GC

**Proof sketch:**

1. The mark phase evaluates every segment against all four reachability conditions
2. Only segments failing ALL conditions are placed in `eligible`
3. The sweep phase:
   a. First removes segments from the manifest (making them unreachable via condition 1)
   b. Then re-checks reachability (conditions 2-4) before physical deletion
   c. Refuses stale plans (epoch mismatch prevents TOCTOU race)
4. Therefore, a segment is only deleted when it is provably unreachable ∎

### Epoch Fencing

The GC plan records `plan_epoch`. Sweep refuses if `manifest.epoch != plan_epoch`.
This prevents:
- Concurrent manifest mutations between mark and sweep
- Applying an outdated plan after a compaction or replication event

## Two-Phase Protocol

### Phase 1: Mark (Non-Destructive)

```
Input:  ManifestSsot (manifest + pins + anchors + follower LSNs)
Output: GcPlan { eligible, deferred, kept, freeable_bytes }
```

Per-segment evaluation:

```
for each segment in manifest.segments:
    if segment ∈ streaming_segments → DEFERRED (streaming)
    if segment ∈ snapshot_pinned → DEFERRED (snapshot pin)
    if ∃ anchor containing segment → DEFERRED (catch-up)
    if not sealed → KEEP
    
    match segment.kind:
        WAL:      eligible if end_lsn ≤ min_follower_durable_lsn
        Cold:     eligible if removed from manifest (replaced by compaction)
        Snapshot: eligible if snapshot_id < current_cutpoint.snapshot_id
    
    otherwise → KEEP
```

### Phase 2: Sweep (Destructive)

```
Input:  GcPlan + ManifestSsot + SegmentStore
Output: (deleted_count, freed_bytes)

1. GUARD: plan_epoch == manifest.epoch (reject stale)
2. GUARD: sweep_delay_ready (configurable delay window)
3. Remove eligible segments from manifest
4. For each eligible segment:
   a. Re-check: is_segment_reachable(seg_id)
   b. If still reachable (e.g., newly pinned) → skip
   c. Otherwise → store.delete_segment(seg_id)
5. Return (count, bytes)
```

## GC Eligibility by Segment Kind

### WAL Segments

```
GC eligible when: end_lsn ≤ min(follower_durable_lsn for all followers)
```

This guarantees all followers have durably received all records in this segment.

### Cold Segments

```
GC eligible when: segment removed from manifest by compactor
```

The compactor creates a new cold segment covering the same key range,
adds it to manifest, then the old segment is removed from manifest.
GC deletes the old segment after it's no longer in the manifest.

### Snapshot Segments

```
GC eligible when: snapshot_id < current_snapshot_cutpoint.snapshot_id
```

Old snapshot chunks are superseded by newer snapshots.

## Protection Mechanisms

### Streaming Protection

```rust
gc.mark_streaming(segment_id);   // before starting stream
// ... stream segment ...
gc.unmark_streaming(segment_id); // after stream complete
```

While marked, the segment is DEFERRED in mark phase regardless of other conditions.

### Snapshot Pinning

```rust
ssot.pin_snapshot(snapshot_id, &segment_ids);   // when snapshot is active
ssot.unpin_snapshot(&segment_ids);               // when snapshot expires
```

Pinned segments are DEFERRED in mark phase.

### Catch-Up Anchors

```rust
ssot.register_catchup_anchor(node_id, segment_set);  // follower starts catch-up
ssot.remove_catchup_anchor(node_id);                  // follower caught up
```

Anchored segments are DEFERRED in mark phase.

## CLI Interface

```bash
# Dry-run: see what would be GC'd
falconctl gc plan

# Execute GC
falconctl gc apply

# Cancel a pending plan (no-op if sweep hasn't run)
falconctl gc rollback

# Show GC metrics
falconctl gc status
```

## Metrics

| Metric | Description |
|--------|-------------|
| `gc_mark_runs` | Number of mark phase executions |
| `gc_sweep_runs` | Number of sweep phase executions |
| `gc_segments_marked` | Segments identified as eligible |
| `gc_segments_swept` | Segments actually deleted |
| `gc_bytes_freed` | Total bytes freed by GC |
| `gc_deferred_total` | Segments deferred (streaming/pin/anchor) |
| `gc_plans_generated` | GC plans created |
| `gc_plans_applied` | GC plans executed |
| `gc_plans_rolled_back` | GC plans cancelled |

## Failure Modes

| Failure | Behavior |
|---------|----------|
| Sweep during manifest mutation | Plan epoch mismatch → refused |
| Segment pinned between mark and sweep | Re-check catches it → skipped |
| Crash during sweep | Partial deletion is safe — segments already removed from manifest |
| Network partition during stream | Streaming protection prevents GC of in-flight segments |

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `gc_sweep_delay_ms` | 5000 | Delay between mark and sweep |
| `gc_interval_ms` | 60000 | Automatic GC interval |
| `gc_min_durable_lsn_update_ms` | 1000 | How often to refresh follower LSNs |
