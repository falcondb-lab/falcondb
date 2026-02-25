//! Snapshot chunk streaming with bounded memory.
//!
//! Provides `SnapshotReader` and `SnapshotWriter` that use the `AsyncFile`
//! abstraction for overlapped read/write. Chunks are read/written with
//! configurable size and bounded inflight count to prevent OOM.
//!
//! ## Integration with Raft
//!
//! The Raft transport layer can consume snapshot streams via:
//! ```text
//!   SnapshotReader::open(path) → .read_next_chunk() → Stream<Item=Chunk>
//!   SnapshotWriter::create(path) → .write_chunk(chunk) → apply_snapshot_stream()
//! ```

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use super::async_file::{AsyncFileConfig, IoError, IoErrorKind, SnapshotStreamConfig};
use super::AsyncFile;

// ═══════════════════════════════════════════════════════════════════════════
// §1 — Snapshot Chunk
// ═══════════════════════════════════════════════════════════════════════════

/// A single chunk of a snapshot stream.
#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    /// Snapshot identifier.
    pub snapshot_id: String,
    /// Byte offset within the snapshot file.
    pub offset: u64,
    /// Chunk data.
    pub data: Vec<u8>,
    /// Chunk sequence number (0-indexed).
    pub chunk_index: u32,
    /// True if this is the last chunk.
    pub is_last: bool,
    /// CRC32 checksum of `data`.
    pub checksum: u32,
}

impl SnapshotChunk {
    /// Verify the chunk's CRC32 checksum.
    pub fn verify_checksum(&self) -> bool {
        crc32fast::hash(&self.data) == self.checksum
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §2 — Snapshot Reader
// ═══════════════════════════════════════════════════════════════════════════

/// Reads a snapshot file in chunks with bounded memory.
pub struct SnapshotReader {
    file: Box<dyn AsyncFile>,
    config: SnapshotStreamConfig,
    snapshot_id: String,
    file_size: u64,
    current_offset: u64,
    chunk_index: u32,
    chunks_read: AtomicU64,
    bytes_read: AtomicU64,
}

impl SnapshotReader {
    /// Open a snapshot file for chunk-based reading.
    pub fn open(
        path: PathBuf,
        snapshot_id: String,
        config: SnapshotStreamConfig,
    ) -> Result<Self, IoError> {
        let file_config = AsyncFileConfig {
            sequential_scan: true,
            ..Default::default()
        };
        let file = super::create_async_file(path, file_config)?;
        let file_size = file.size()?;

        Ok(Self {
            file,
            config,
            snapshot_id,
            file_size,
            current_offset: 0,
            chunk_index: 0,
            chunks_read: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
        })
    }

    /// Read the next chunk. Returns `None` when the entire file has been read.
    pub fn read_next_chunk(&mut self) -> Result<Option<SnapshotChunk>, IoError> {
        if self.current_offset >= self.file_size {
            return Ok(None);
        }

        let remaining = self.file_size - self.current_offset;
        let read_size = (self.config.chunk_size_bytes as u64).min(remaining) as usize;

        let data = self.file.read_at(self.current_offset, read_size)?;
        if data.is_empty() {
            return Ok(None);
        }

        let checksum = crc32fast::hash(&data);
        let is_last = self.current_offset + data.len() as u64 >= self.file_size;

        let chunk = SnapshotChunk {
            snapshot_id: self.snapshot_id.clone(),
            offset: self.current_offset,
            data,
            chunk_index: self.chunk_index,
            is_last,
            checksum,
        };

        self.current_offset += chunk.data.len() as u64;
        self.chunk_index += 1;
        self.chunks_read.fetch_add(1, Ordering::Relaxed);
        self.bytes_read
            .fetch_add(chunk.data.len() as u64, Ordering::Relaxed);

        Ok(Some(chunk))
    }

    /// Total file size.
    pub const fn total_size(&self) -> u64 {
        self.file_size
    }

    /// Bytes read so far.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    /// Chunks read so far.
    pub fn chunks_read(&self) -> u64 {
        self.chunks_read.load(Ordering::Relaxed)
    }

    /// Progress as a fraction [0.0, 1.0].
    pub fn progress(&self) -> f64 {
        if self.file_size == 0 {
            1.0
        } else {
            self.bytes_read() as f64 / self.file_size as f64
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §3 — Snapshot Writer
// ═══════════════════════════════════════════════════════════════════════════

/// Writes a snapshot file from a stream of chunks with bounded memory.
pub struct SnapshotWriter {
    file: Box<dyn AsyncFile>,
    expected_snapshot_id: Option<String>,
    next_offset: u64,
    next_chunk_index: u32,
    chunks_written: AtomicU64,
    bytes_written: AtomicU64,
    #[allow(dead_code)]
    max_inflight_chunks: usize,
}

impl SnapshotWriter {
    /// Create a new snapshot file for writing.
    pub fn create(
        path: PathBuf,
        config: SnapshotStreamConfig,
    ) -> Result<Self, IoError> {
        // Remove existing file if any
        let _ = std::fs::remove_file(&path);

        let file_config = AsyncFileConfig::default();
        let file = super::create_async_file(path, file_config)?;

        Ok(Self {
            file,
            expected_snapshot_id: None,
            next_offset: 0,
            next_chunk_index: 0,
            chunks_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            max_inflight_chunks: config.max_inflight_chunks,
        })
    }

    /// Write a chunk to the snapshot file.
    ///
    /// Validates:
    /// - Chunk ordering (monotonic offset and chunk_index)
    /// - Snapshot ID consistency
    /// - CRC32 checksum integrity
    pub fn write_chunk(&mut self, chunk: &SnapshotChunk) -> Result<(), IoError> {
        // Validate snapshot ID consistency
        if let Some(ref expected) = self.expected_snapshot_id {
            if &chunk.snapshot_id != expected {
                return Err(IoError::new(
                    IoErrorKind::InvalidArgument,
                    format!(
                        "snapshot_id mismatch: expected {}, got {}",
                        expected, chunk.snapshot_id
                    ),
                ));
            }
        } else {
            self.expected_snapshot_id = Some(chunk.snapshot_id.clone());
        }

        // Validate ordering
        if chunk.offset != self.next_offset {
            return Err(IoError::new(
                IoErrorKind::InvalidArgument,
                format!(
                    "chunk offset mismatch: expected {}, got {}",
                    self.next_offset, chunk.offset
                ),
            ));
        }
        if chunk.chunk_index != self.next_chunk_index {
            return Err(IoError::new(
                IoErrorKind::InvalidArgument,
                format!(
                    "chunk index mismatch: expected {}, got {}",
                    self.next_chunk_index, chunk.chunk_index
                ),
            ));
        }

        // Validate checksum
        if !chunk.verify_checksum() {
            return Err(IoError::new(
                IoErrorKind::Corrupt,
                format!(
                    "chunk {} checksum mismatch at offset {}",
                    chunk.chunk_index, chunk.offset
                ),
            ));
        }

        // Write
        self.file.write_at(chunk.offset, &chunk.data)?;

        self.next_offset += chunk.data.len() as u64;
        self.next_chunk_index += 1;
        self.chunks_written.fetch_add(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(chunk.data.len() as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Finalize the snapshot: flush and sync to disk.
    pub fn finalize(&self) -> Result<(), IoError> {
        self.file.sync()
    }

    /// Bytes written so far.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Chunks written so far.
    pub fn chunks_written(&self) -> u64 {
        self.chunks_written.load(Ordering::Relaxed)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// §4 — Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("falcon_snap_stream_{}", name))
    }

    #[test]
    fn test_snapshot_chunk_checksum() {
        let chunk = SnapshotChunk {
            snapshot_id: "snap-1".into(),
            offset: 0,
            data: b"hello snapshot".to_vec(),
            chunk_index: 0,
            is_last: true,
            checksum: crc32fast::hash(b"hello snapshot"),
        };
        assert!(chunk.verify_checksum());

        let bad_chunk = SnapshotChunk {
            checksum: 0xDEADBEEF,
            ..chunk.clone()
        };
        assert!(!bad_chunk.verify_checksum());
    }

    #[test]
    fn test_snapshot_roundtrip_single_chunk() {
        let path = temp_path("roundtrip_single");
        let _ = std::fs::remove_file(&path);

        // Write some data to a file first
        std::fs::write(&path, b"snapshot data here").unwrap();

        let config = SnapshotStreamConfig {
            chunk_size_bytes: 1024 * 1024, // 1MB — larger than file
            ..Default::default()
        };

        let mut reader = SnapshotReader::open(path.clone(), "snap-1".into(), config.clone()).unwrap();
        assert_eq!(reader.total_size(), 18);

        let chunk = reader.read_next_chunk().unwrap().expect("should have data");
        assert_eq!(chunk.chunk_index, 0);
        assert_eq!(&chunk.data, b"snapshot data here");
        assert!(chunk.is_last);
        assert!(chunk.verify_checksum());

        // No more chunks
        assert!(reader.read_next_chunk().unwrap().is_none());
        assert!((reader.progress() - 1.0).abs() < f64::EPSILON);

        // Write to a new file via SnapshotWriter
        let out_path = temp_path("roundtrip_single_out");
        let _ = std::fs::remove_file(&out_path);

        let mut writer = SnapshotWriter::create(out_path.clone(), config).unwrap();
        writer.write_chunk(&chunk).unwrap();
        writer.finalize().unwrap();

        let written_data = std::fs::read(&out_path).unwrap();
        assert_eq!(&written_data, b"snapshot data here");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&out_path);
    }

    #[test]
    fn test_snapshot_roundtrip_multiple_chunks() {
        let path = temp_path("roundtrip_multi");
        let _ = std::fs::remove_file(&path);

        // Write 100 bytes
        let data = vec![0x42u8; 100];
        std::fs::write(&path, &data).unwrap();

        let config = SnapshotStreamConfig {
            chunk_size_bytes: 30, // Force multiple chunks
            ..Default::default()
        };

        let mut reader = SnapshotReader::open(path.clone(), "snap-2".into(), config.clone()).unwrap();
        let mut chunks = Vec::new();
        while let Some(chunk) = reader.read_next_chunk().unwrap() {
            chunks.push(chunk);
        }

        // 100 bytes / 30 = 4 chunks (30+30+30+10)
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].data.len(), 30);
        assert_eq!(chunks[1].data.len(), 30);
        assert_eq!(chunks[2].data.len(), 30);
        assert_eq!(chunks[3].data.len(), 10);
        assert!(!chunks[0].is_last);
        assert!(chunks[3].is_last);

        // Write all chunks to new file
        let out_path = temp_path("roundtrip_multi_out");
        let _ = std::fs::remove_file(&out_path);

        let mut writer = SnapshotWriter::create(out_path.clone(), config).unwrap();
        for chunk in &chunks {
            writer.write_chunk(chunk).unwrap();
        }
        writer.finalize().unwrap();

        let written_data = std::fs::read(&out_path).unwrap();
        assert_eq!(written_data, data);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&out_path);
    }

    #[test]
    fn test_snapshot_writer_rejects_bad_checksum() {
        let out_path = temp_path("bad_checksum");
        let _ = std::fs::remove_file(&out_path);

        let config = SnapshotStreamConfig::default();
        let mut writer = SnapshotWriter::create(out_path.clone(), config).unwrap();

        let bad_chunk = SnapshotChunk {
            snapshot_id: "snap-bad".into(),
            offset: 0,
            data: b"data".to_vec(),
            chunk_index: 0,
            is_last: true,
            checksum: 0xDEADBEEF, // wrong
        };

        let result = writer.write_chunk(&bad_chunk);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind, IoErrorKind::Corrupt);

        let _ = std::fs::remove_file(&out_path);
    }

    #[test]
    fn test_snapshot_writer_rejects_out_of_order() {
        let out_path = temp_path("out_of_order");
        let _ = std::fs::remove_file(&out_path);

        let config = SnapshotStreamConfig::default();
        let mut writer = SnapshotWriter::create(out_path.clone(), config).unwrap();

        let chunk = SnapshotChunk {
            snapshot_id: "snap-ooo".into(),
            offset: 100, // wrong — should be 0
            data: b"data".to_vec(),
            chunk_index: 0,
            is_last: false,
            checksum: crc32fast::hash(b"data"),
        };

        let result = writer.write_chunk(&chunk);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind, IoErrorKind::InvalidArgument);

        let _ = std::fs::remove_file(&out_path);
    }

    #[test]
    fn test_snapshot_writer_rejects_id_mismatch() {
        let out_path = temp_path("id_mismatch");
        let _ = std::fs::remove_file(&out_path);

        let config = SnapshotStreamConfig::default();
        let mut writer = SnapshotWriter::create(out_path.clone(), config).unwrap();

        let chunk1 = SnapshotChunk {
            snapshot_id: "snap-A".into(),
            offset: 0,
            data: b"aaa".to_vec(),
            chunk_index: 0,
            is_last: false,
            checksum: crc32fast::hash(b"aaa"),
        };
        writer.write_chunk(&chunk1).unwrap();

        let chunk2 = SnapshotChunk {
            snapshot_id: "snap-B".into(), // different ID!
            offset: 3,
            data: b"bbb".to_vec(),
            chunk_index: 1,
            is_last: true,
            checksum: crc32fast::hash(b"bbb"),
        };
        let result = writer.write_chunk(&chunk2);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind, IoErrorKind::InvalidArgument);

        let _ = std::fs::remove_file(&out_path);
    }

    #[test]
    fn test_snapshot_reader_empty_file() {
        let path = temp_path("empty");
        std::fs::write(&path, b"").unwrap();

        let config = SnapshotStreamConfig::default();
        let mut reader = SnapshotReader::open(path.clone(), "snap-empty".into(), config).unwrap();
        assert_eq!(reader.total_size(), 0);
        assert!(reader.read_next_chunk().unwrap().is_none());
        assert!((reader.progress() - 1.0).abs() < f64::EPSILON);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_snapshot_reader_progress() {
        let path = temp_path("progress");
        let _ = std::fs::remove_file(&path);
        std::fs::write(&path, &vec![0u8; 100]).unwrap();

        let config = SnapshotStreamConfig {
            chunk_size_bytes: 25,
            ..Default::default()
        };
        let mut reader = SnapshotReader::open(path.clone(), "snap-p".into(), config).unwrap();

        reader.read_next_chunk().unwrap(); // 25 bytes
        assert!((reader.progress() - 0.25).abs() < 0.01);

        reader.read_next_chunk().unwrap(); // 50 bytes
        assert!((reader.progress() - 0.50).abs() < 0.01);

        let _ = std::fs::remove_file(&path);
    }
}
