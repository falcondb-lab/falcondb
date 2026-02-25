# Replication Delta-LSN Encoding — Design Document

## Core Principle

> **Send segment_id once, then delta offsets via varints.**

## Why Delta Encoding?

In a WAL segment, record offsets are monotonically increasing. The delta
between consecutive records is typically small (128–4096 bytes). Encoding
these deltas as varints instead of full 8-byte offsets saves significant
bandwidth on metadata bytes.

| Encoding | Bytes per offset | Typical delta range | Bytes per delta |
|----------|-----------------|---------------------|-----------------|
| Full LSN (v0) | 8 | N/A | 8 |
| Delta varint (v1) | N/A | 64–4096 | 1–2 |

**Target: ≥30% reduction in metadata/header bytes.**

## Wire Format

### Stream Header (sent once per segment stream)

```
[version: u8][segment_id: u64][base_offset: u64][base_lsn: u64]
```
Total: 25 bytes.

The segment_id and base_offset establish the reference point. All subsequent
record frames carry only delta offsets from the previous record.

### Record Frame (v1: delta encoding)

```
[delta_offset: varint][record_len: varint][payload: N bytes][crc32: 4 bytes]
```

- `delta_offset`: LEB128 varint — offset increment from previous record
- `record_len`: LEB128 varint — payload length
- `payload`: raw record bytes
- `crc32`: integrity check on payload

### Record Frame (v0: legacy full LSN)

```
[offset: u64][record_len: u32][payload: N bytes][crc32: u32]
```

Fixed 16-byte header overhead per record.

## Varint (LEB128) Encoding

| Value range | Encoded bytes |
|-------------|--------------|
| 0–127 | 1 |
| 128–16383 | 2 |
| 16384–2097151 | 3 |
| > 2M | 4+ |

Typical WAL record deltas (128–4096 bytes) encode in 1–2 bytes.

## Protocol Version Negotiation

### Handshake

1. Follower sends supported versions: `[v0, v1]`
2. Leader selects highest common version
3. Stream begins with selected version in header

### Fallback

If no common version exists, replication fails with `VersionMismatch`.
Operators must upgrade the follower.

| Version | Description | Status |
|---------|-------------|--------|
| v0 | Full LSN per record | Legacy fallback |
| v1 | Delta LSN + varint | Default |

## Tail Streaming

Delta encoding applies equally to tail streaming:
- Header provides `base_offset` (follower's current position)
- Subsequent records use delta from previous

## Error Handling

| Error | Recovery Action |
|-------|----------------|
| CRC mismatch | Retry from last good frame |
| Truncated frame | Retry from last good frame |
| Invalid varint | Discard segment tail, rollback |
| Version mismatch | Abort (fatal) |
| Segment boundary error | Discard and rollback |

On any frame-level error:
1. Follower discards the current segment tail
2. Falls back to last sealed segment
3. Re-handshakes with leader
4. Continues delta streaming from new base

## Bandwidth Savings Analysis

For a typical workload with 128-byte average record payload:

```
v0 per-record overhead: 8 (offset) + 4 (len) + 4 (crc) = 16 bytes
v1 per-record overhead: 1-2 (delta) + 1-2 (len) + 4 (crc) = 6-8 bytes

Metadata savings: (16 - 7) / 16 ≈ 56% on metadata bytes
Overall savings (with payload): depends on payload size
  - 128B payload: (16-7)/(16+128) ≈ 6%
  - Small payloads see larger relative savings on wire
```

The ≥30% target applies to **metadata bytes only** (excluding payload).

## Metrics

| Metric | Description |
|--------|-------------|
| `replication_protocol_version` | Negotiated version (0 or 1) |
| `replication_bytes_sent_total` | Total encoded bytes sent |
| `replication_bytes_baseline_total` | What v0 would have sent |
| `replication_bytes_saved_ratio` | `1 - (actual/baseline)` |
| `replication_crc_fail_total` | CRC verification failures |
| `replication_frames_encoded_total` | Frames encoded |
| `replication_frames_decoded_total` | Frames decoded |

## Relationship to CSN/LSN Decoupling

Delta-LSN encoding operates purely on the **LSN layer** (physical WAL positions).
CSN (commit sequence numbers) are carried inside record payloads and are
independent of the framing format.

This means:
- Replication streams WAL data efficiently via delta LSN
- Commit records inside the WAL carry CSN for visibility reconstruction
- Followers rebuild CSN state from decoded WAL commit records
- The two concerns (transport efficiency vs. visibility semantics) are fully decoupled
