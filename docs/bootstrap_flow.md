# Bootstrap Flow — Empty Disk to Serviceable Node

## Definition

**Bootstrap** = acquire a Manifest + the corresponding Segment set + replay WAL tail → node is serviceable.

There is **one path** for all scenarios. No special code paths.

## Entry Points

| Scenario | Constructor | Starts At |
|----------|-----------|-----------|
| New node (empty disk) | `BootstrapCoordinator::new_empty_disk()` | `INIT` |
| Crash recovery | `BootstrapCoordinator::from_local_manifest(m)` | `COMPUTE_MISSING` |
| Snapshot restore | `receive_manifest(snapshot_manifest)` | `COMPUTE_MISSING` |
| Catch-up after partition | `from_local_manifest(m)` | `COMPUTE_MISSING` |

## Phase State Machine

```
INIT ──► FETCH_MANIFEST ──► COMPUTE_MISSING ──► STREAM_SEGMENTS
                                                       │
                                              ┌────────┘
                                              ▼
                                        VERIFY_SEGMENTS ──► REPLAY_TAIL ──► READY
                                              │
                                              ▼
                                           FAILED
```

## Phase Details

### Phase 1: INIT → FETCH_MANIFEST

- Node has no local state
- Connect to leader (address provided externally)
- Request current manifest

### Phase 2: FETCH_MANIFEST → COMPUTE_MISSING

- `receive_manifest(manifest)` stores the leader's manifest
- Transitions to COMPUTE_MISSING

### Phase 3: COMPUTE_MISSING → STREAM_SEGMENTS

- `compute_missing(local_store)` compares manifest segments vs local store
- Missing segments = `manifest.all_segment_ids() - local_store.list_segments()`
- If nothing missing → skip to VERIFY_SEGMENTS
- Otherwise → STREAM_SEGMENTS

### Phase 4: STREAM_SEGMENTS → VERIFY_SEGMENTS

For each missing segment:
1. Leader streams segment body via Segment Streaming Protocol
2. Follower creates segment in local store
3. Follower writes body at offset 0
4. Follower seals segment
5. `mark_segment_fetched(seg_id, bytes)`

Stream ordering (**required**):
1. **DICT_SEGMENTs first** (dictionaries must be loaded before any dict-compressed data segment)
2. Cold segments (enables read queries sooner)
3. Sealed WAL segments
4. Snapshot segments (if any)

> **Since v1.2**: Dictionary segments must be fetched and loaded into
> `falcon_segment_codec::DictionaryRegistry` before any cold/snapshot segment
> that references a `dictionary_id`. If a DICT_SEGMENT is missing, all segments
> referencing that dictionary are **not loadable** (hard fail).

When all missing segments are fetched → VERIFY_SEGMENTS

### Phase 5: VERIFY_SEGMENTS → REPLAY_TAIL

- `verify_all(store)` checks every manifest segment exists and passes checksum
- If any fail → FAILED with error details
- If all pass → REPLAY_TAIL

### Phase 6: REPLAY_TAIL → READY

- Replay the active WAL segment tail (if any) for records after the last sealed segment
- Apply pending transactions to in-memory state
- `mark_ready()`

### Phase 7: READY

- Node is serviceable
- Enter normal replication mode (tail streaming from leader)

## Interrupt & Resume

Bootstrap supports interruption at STREAM_SEGMENTS phase:

```
1. Node tracks: which segments already fetched
2. On reconnect: compute_missing() only returns truly missing segments
3. Already-transferred segments are not re-sent
```

## Progress Tracking

```rust
boot.progress()           // 0.0 to 1.0
boot.segments_fetched     // count
boot.total_segments_needed // total
boot.bytes_fetched        // total bytes transferred
boot.phase                // current phase
```

## Error Handling

| Error | Recovery |
|-------|----------|
| Leader unreachable | Retry with backoff |
| Segment CRC fail | Re-request segment |
| Manifest stale | Re-fetch manifest from new leader |
| Disk full | Pause + alert |
| Partial segment | Discard + re-fetch |

## Metrics

| Metric | Description |
|--------|-------------|
| `bootstrap_started` | Bootstrap attempts |
| `bootstrap_completed` | Successful bootstraps |
| `bootstrap_failed` | Failed bootstraps |
| `segments_streamed` | Segments transferred |
| `bytes_streamed` | Total bytes transferred |

## CLI

```
falconctl node bootstrap --leader host:port
falconctl node bootstrap --snapshot <snapshot_id>
falconctl node status    # shows bootstrap progress if in progress
```
