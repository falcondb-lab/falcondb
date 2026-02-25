# Dictionary Lifecycle

## Overview

A Zstd dictionary is a **statistical summary** of data patterns for a table/schema.
It must be versioned, replicated, and recovered alongside data segments.

## Dictionary Identity

| Field | Description |
|-------|-------------|
| `dictionary_id` | Unique monotonic ID |
| `segment_id` | SegmentStore segment holding dict bytes |
| `table_id` | Table this dictionary applies to |
| `schema_version` | Schema version it was trained on |
| `checksum` | djb2 CRC of dictionary bytes |
| `sample_count` | Number of samples used in training |

## Lifecycle Phases

### 1. Training

```
Source: recent N cold segments (sampled, not full scan)
Method: zstd::dict::from_samples(samples, max_dict_size)
Output: DictionaryEntry { data, checksum, table_id, schema_version }
```

Training is a **background task** — never on the OLTP path.

### 2. Registration

```rust
dict_store.register(entry);
// dictionaries[dict_id] = entry
// table_dict[table_id] = dict_id
```

### 3. Storage as Segment

```rust
dict_store.store_as_segment(&mut entry, &segment_store);
// Creates a sealed segment containing the dictionary bytes
// entry.segment_id is set to the new segment ID
```

### 4. Usage in Compression

New cold segments use the **latest** dictionary for their table.
Old segments keep their original dictionary — never recompressed automatically.

### 5. Replication

Dictionary segments use the same Segment Streaming protocol. Bootstrap order:
1. Fetch Manifest
2. Fetch dictionary segments first
3. Fetch data segments that depend on those dictionaries
4. Verify checksums, replay WAL tail

### 6. Versioning

New dictionary trained -> new dictionary_id. Old segments unchanged unless GC triggers recompression.

### 7. Recovery

Load Manifest -> load dictionary segments -> register in DictionaryStore -> resume.

## Metrics

| Metric | Description |
|--------|-------------|
| `dictionaries_created` | Total trained |
| `dictionary_bytes_total` | Total dict bytes |
| `dict_hit_rate` | Lookup hit rate |
| `training_runs` | Training invocations |

## CLI

```bash
falconctl dictionary ls
falconctl dictionary stats
falconctl dictionary train --table=users
```
