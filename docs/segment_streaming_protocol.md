# Segment Streaming Protocol — Wire Format & Semantics

## Overview

All data movement in FalconDB — replication, snapshot transfer, cold store sync,
bootstrap, and rebalance — uses one unified streaming protocol operating on segments.

## Wire Messages

### StreamBegin

Sent once at the start of a streaming session.

```
Field               Type    Description
─────────────────   ──────  ─────────────────────────────────────
manifest_epoch      u64     Leader's current manifest epoch
stream_id           u64     Unique stream identifier
protocol_version    u8      Negotiated protocol version
```

### StreamSegmentHeader

Sent once per segment before its chunks.

```
Field               Type    Description
─────────────────   ──────  ─────────────────────────────────────
segment_id          u64     Segment being streamed
kind                u8      0=WAL, 1=Cold, 2=Snapshot
size                u64     Total segment body size
codec               u8      Compression codec
checksum            u32     Header checksum
```

### StreamChunk

One or more chunks per segment. Each chunk is independently verifiable.

```
Field               Type    Description
─────────────────   ──────  ─────────────────────────────────────
segment_id          u64     Which segment this chunk belongs to
chunk_index         u32     Monotonically increasing per segment
data                [u8]    Chunk payload
crc                 u32     CRC32 of data (djb2 hash)
is_last             bool    Whether this is the final chunk
```

### StreamEnd

Sent once to close the streaming session.

```
Field                   Type    Description
─────────────────────   ──────  ─────────────────────────
stream_id               u64     Matches StreamBegin
segments_transferred    u32     Total segments sent
bytes_transferred       u64     Total bytes sent
```

## Backpressure

The receiver controls flow:

1. Receiver maintains a **max inflight chunks** window
2. Each received chunk sends an implicit ACK (chunk_index)
3. Sender pauses when inflight = max
4. Receiver can explicitly send a **pause** signal (e.g., disk full, memory pressure)
5. Sender resumes on **resume** signal

## Resume (断点续传)

When a stream is interrupted (network, failover, timeout):

1. Receiver records a `StreamResumePoint`:
   ```
   { stream_id, last_segment_id, last_chunk_index }
   ```
2. On reconnect, receiver sends the resume point in the handshake
3. Leader skips already-transferred segments and resumes from `last_chunk_index + 1`
4. If the resume point is stale (segment GC'd), leader falls back to full re-stream

### Resume Safety

- Segments are immutable once sealed → partial transfers are safe to resume
- Active WAL segment tail may have advanced → leader sends new chunks from last offset
- Resume metric: `segment_stream_resume_total`

## Cancel

Either side can cancel an active stream:

- Receiver sends cancel with reason
- Sender stops immediately, does not send StreamEnd
- Both sides clean up resources

## Version Negotiation

Integrated with the replication handshake:

1. Follower reports `protocol_version` in `ReplicationHandshake`
2. Leader uses that version (or rejects if incompatible)
3. `StreamBegin.protocol_version` confirms the negotiated version

Current versions:
| Version | Description |
|---------|-------------|
| 0 | Reserved (legacy) |
| 1 | Unified segment streaming (current) |

## Error Handling

| Error | Detection | Recovery |
|-------|-----------|----------|
| CRC mismatch | `StreamChunk.verify()` fails | Re-request chunk |
| Truncated chunk | Incomplete data | Wait for retransmit |
| Missing segment | Segment not found on leader | Error to follower |
| Stale resume | Resume segment was GC'd | Full re-stream |
| Leader change | Epoch mismatch | Re-handshake with new leader |
| Disk full | Write fails on receiver | Pause + alert |

### Error Flow

```
1. Detect error (CRC fail, truncation, etc.)
2. Receiver sends error with { stream_id, segment_id, chunk_index, error_code }
3. Leader decides: retry chunk / restart segment / abort stream
4. If abort: both sides clean up, follower re-handshakes
```

## Segment Transfer Ordering

1. **Sealed WAL segments** — oldest first (bulk catch-up)
2. **Cold segments** — required by manifest
3. **Snapshot segments** — if snapshot restore
4. **Active WAL tail** — last, as continuous tail stream

This ordering ensures the follower can begin serving reads as early as possible
(cold data + committed WAL = queryable state).

## Metrics

| Metric | Description |
|--------|-------------|
| `stream_bytes_total` | Total bytes transferred |
| `stream_segments_total` | Total segments transferred |
| `stream_resume_total` | Resume operations |
| `stream_crc_fail_total` | CRC verification failures |
| `stream_cancel_total` | Cancelled streams |
| `stream_active_count` | Currently active streams |

## Relationship to Delta-LSN Encoding

Within a WAL segment, individual records can be further compressed using
delta-LSN encoding (varint offsets). This is an optimization layer **inside**
the segment payload, orthogonal to the segment streaming protocol itself.

The streaming protocol transfers raw segment bytes; delta-LSN encoding operates
on the logical record structure within those bytes.
