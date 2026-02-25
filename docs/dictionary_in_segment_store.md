# Dictionary in Segment Store

## Overview

Zstd dictionaries are stored as **DICT_SEGMENTs** — regular segments in SegmentStore,
tracked by Manifest via `dictionary_id → segment_id` mapping.

## Dictionary Identity

| Field | Type | Description |
|-------|------|-------------|
| `dictionary_id` | u64 | Unique monotonic ID |
| `segment_id` | u64 | SegmentStore segment holding dict bytes |
| `table_id` | u64 | Table this dictionary applies to |
| `schema_version` | u64 | Schema version it was trained on |
| `checksum` | u32 | djb2 CRC of dictionary bytes |

## Lifecycle

### 1. Training (external)

Dictionary training happens **outside** `falcon_segment_codec`.
The crate only loads and uses pre-trained dictionaries.

```
External trainer:
  samples = collect recent cold segment blocks for table T
  dict_bytes = zstd_safe::train_from_buffer(&mut buf, &concat, &sizes)
```

### 2. Storage as DICT_SEGMENT

```
dict_bytes → write to SegmentStore as segment
segment_id → record in Manifest
```

### 3. Loading into DictionaryRegistry

```rust
let handle = DictionaryHandle::new(dict_id, seg_id, table_id, schema_ver, dict_bytes);
assert!(handle.verify()); // checksum
registry.load(handle);
```

### 4. Usage in Compression

```rust
let codec = registry.create_codec_with_dict(dict_id, ZstdCodecConfig::default())?;
let block = CompressedBlock::compress(&codec, &row_data)?;
```

### 5. Replication

DICT_SEGMENTs replicate via the same Segment Streaming protocol.

**Bootstrap order:**
1. Fetch Manifest
2. Fetch DICT_SEGMENTs first
3. Fetch data segments that reference those dictionaries
4. Verify checksums
5. Replay WAL tail

## Constraints

- Dictionary **missing** → segment **not loadable** (hard fail)
- Dictionary version must **strictly match** what the segment header records
- Dictionary is **immutable** once created
- A table has at most **one latest** dictionary; old ones kept for existing segments

## Manifest Integration

Segment headers record:
```
codec_id = Zstd
codec_level = 3
dictionary_id = 42        // 0 = no dictionary
dictionary_checksum = 0xCAFE
```

Segment is **self-describing**: all info needed to decompress is in the header + dictionary.

## DictionaryRegistry API

```rust
// Load pre-trained dictionary
registry.load(handle: DictionaryHandle)

// Lookup
registry.get(dictionary_id) -> Option<DictionaryHandle>
registry.get_for_table(table_id) -> Option<DictionaryHandle>
registry.list_ids() -> Vec<u64>

// Create codec with dictionary pre-loaded
registry.create_codec_with_dict(dict_id, config) -> Result<ZstdBlockCodec, CodecError>
```

## Metrics

| Metric | Description |
|--------|-------------|
| `loaded` | Dictionaries loaded into registry |
| `lookups` | Total lookup calls |
| `hits` | Successful lookups |
| `misses` | Failed lookups |
| `bytes_total` | Total dictionary bytes |
| `hit_rate()` | hits / (hits + misses) |
