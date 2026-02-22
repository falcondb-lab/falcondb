//! Sorted String Table (SST) file format.
//!
//! Layout:
//! ```text
//!   [DataBlock 0] [DataBlock 1] ... [DataBlock N]
//!   [IndexBlock]       — maps last_key_per_block → block_offset
//!   [BloomFilter]      — serialized bloom filter bytes
//!   [Footer]           — fixed 48-byte trailer
//! ```
//!
//! DataBlock layout:
//! ```text
//!   [num_entries: u32]
//!   [entry 0] [entry 1] ...
//!   [restart_offsets: u32 * restart_count]
//!   [restart_count: u32]
//! ```
//!
//! Entry layout:
//! ```text
//!   [key_len: u32] [value_len: u32] [key: bytes] [value: bytes]
//! ```

use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::bloom::BloomFilter;

// ── SstReadError — graded error type for SST read paths ──────────────────────

/// Graded error type for SST / Block / Index / Footer parse failures.
/// Every variant carries structured context for diagnostics.
#[derive(Debug)]
pub enum SstReadError {
    /// Data on disk does not match its checksum — file is corrupt.
    Corruption {
        sst_path: String,
        block_offset: u64,
        block_size: u32,
        expected_crc: u32,
        actual_crc: u32,
        detail: String,
    },
    /// A structure is shorter than its declared length — truncated file.
    Truncated {
        sst_path: String,
        block_offset: u64,
        expected_len: usize,
        actual_len: usize,
        detail: String,
    },
    /// Underlying I/O error (permissions, disk failure, etc.).
    Io {
        sst_path: String,
        source: io::Error,
        detail: String,
    },
    /// Codec / format error (bad magic, unknown version, malformed encoding).
    Codec {
        sst_path: String,
        detail: String,
    },
}

impl fmt::Display for SstReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Corruption { sst_path, block_offset, block_size, expected_crc, actual_crc, detail } => {
                write!(f, "SST corruption [{}]: block_offset={} block_size={} expected_crc={:#010x} actual_crc={:#010x}: {}",
                    sst_path, block_offset, block_size, expected_crc, actual_crc, detail)
            }
            Self::Truncated { sst_path, block_offset, expected_len, actual_len, detail } => {
                write!(f, "SST truncated [{}]: block_offset={} expected_len={} actual_len={}: {}",
                    sst_path, block_offset, expected_len, actual_len, detail)
            }
            Self::Io { sst_path, source, detail } => {
                write!(f, "SST I/O error [{}]: {}: {}", sst_path, detail, source)
            }
            Self::Codec { sst_path, detail } => {
                write!(f, "SST codec error [{}]: {}", sst_path, detail)
            }
        }
    }
}

impl std::error::Error for SstReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<SstReadError> for io::Error {
    fn from(e: SstReadError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, e.to_string())
    }
}

/// Helper: convert a byte slice to a fixed-size array, returning an io::Error on failure.
fn slice_to_array<const N: usize>(slice: &[u8]) -> io::Result<[u8; N]> {
    slice.try_into().map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "SST: unexpected end of data")
    })
}

/// Compute CRC32 checksum for a data block.
fn block_crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Magic bytes for SST file identification.
const SST_MAGIC: &[u8; 4] = b"FSST";

/// SST format version.
const SST_FORMAT_VERSION: u32 = 1;

/// Target data block size (before compression). 4 KB default.
const TARGET_BLOCK_SIZE: usize = 4096;

/// Footer size: magic(4) + version(4) + index_offset(8) + index_len(8) +
///              bloom_offset(8) + bloom_len(8) + entry_count(8) + footer_crc(4) = 52 bytes.
///
/// NOTE: v1 files (48-byte footer, no CRC) are still readable for backward compatibility.
const FOOTER_SIZE: usize = 52;

/// Legacy footer size (v1, no CRC).
const FOOTER_SIZE_V1: usize = 48;

/// Global SST ID counter for unique block cache keys.
static NEXT_SST_ID: AtomicU64 = AtomicU64::new(1);

/// Metadata about an SST file (kept in memory by the LSM engine).
#[derive(Debug, Clone)]
pub struct SstMeta {
    /// Unique ID for this SST (used as block cache key).
    pub id: u64,
    /// File path on disk.
    pub path: PathBuf,
    /// LSM level (0 = freshly flushed, 1..N = compacted).
    pub level: u32,
    /// Smallest key in the SST.
    pub min_key: Vec<u8>,
    /// Largest key in the SST.
    pub max_key: Vec<u8>,
    /// Total number of key-value entries.
    pub entry_count: u64,
    /// File size in bytes.
    pub file_size: u64,
    /// Sequence number (for ordering L0 files).
    pub seq: u64,
}

impl SstMeta {
    /// Check if a key might be within this SST's key range.
    pub fn may_contain_key(&self, key: &[u8]) -> bool {
        key >= self.min_key.as_slice() && key <= self.max_key.as_slice()
    }
}

/// A single key-value entry in an SST.
#[derive(Debug, Clone)]
pub struct SstEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

// ── SST Writer ──────────────────────────────────────────────────────────────

/// Writes a new SST file from sorted key-value pairs.
pub struct SstWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    bloom: BloomFilter,
    /// Index entries: (last_key_in_block, block_offset, block_len).
    index: Vec<(Vec<u8>, u64, u32)>,
    /// Current data block buffer.
    block_buf: Vec<u8>,
    block_entry_count: u32,
    /// Current write offset in the file.
    offset: u64,
    /// Total entries written.
    entry_count: u64,
    /// First key in the SST (for SstMeta.min_key).
    first_key: Option<Vec<u8>>,
    /// Last key written (for SstMeta.max_key).
    last_key: Option<Vec<u8>>,
    /// Target block size.
    target_block_size: usize,
}

impl SstWriter {
    /// Create a new SST writer at the given path.
    /// `expected_entries` is used to size the bloom filter.
    pub fn new(path: &Path, expected_entries: usize) -> io::Result<Self> {
        let file = File::create(path)?;
        let writer = BufWriter::with_capacity(64 * 1024, file);
        let bloom = BloomFilter::new(expected_entries.max(1), 0.01);

        Ok(Self {
            writer,
            path: path.to_path_buf(),
            bloom,
            index: Vec::new(),
            block_buf: Vec::with_capacity(TARGET_BLOCK_SIZE),
            block_entry_count: 0,
            offset: 0,
            entry_count: 0,
            first_key: None,
            last_key: None,
            target_block_size: TARGET_BLOCK_SIZE,
        })
    }

    /// Add a key-value pair. Keys MUST be added in sorted order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        // Track first/last key
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.last_key = Some(key.to_vec());

        // Add to bloom filter
        self.bloom.insert(key);

        // Encode entry into block buffer
        self.block_buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.block_buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.block_buf.extend_from_slice(key);
        self.block_buf.extend_from_slice(value);
        self.block_entry_count += 1;
        self.entry_count += 1;

        // Flush block if it exceeds target size
        if self.block_buf.len() >= self.target_block_size {
            self.flush_block()?;
        }

        Ok(())
    }

    /// Finish writing the SST file. Returns metadata about the written file.
    pub fn finish(mut self, level: u32, seq: u64) -> io::Result<SstMeta> {
        // Flush any remaining data in the block buffer
        if !self.block_buf.is_empty() {
            self.flush_block()?;
        }

        // Write index block
        let index_offset = self.offset;
        let index_data = self.encode_index();
        self.writer.write_all(&index_data)?;
        self.offset += index_data.len() as u64;
        let index_len = index_data.len() as u64;

        // Write bloom filter
        let bloom_offset = self.offset;
        let bloom_data = self.bloom.to_bytes();
        self.writer.write_all(&bloom_data)?;
        self.offset += bloom_data.len() as u64;
        let bloom_len = bloom_data.len() as u64;

        // Write footer (v2 with CRC)
        let mut footer = [0u8; FOOTER_SIZE];
        footer[0..4].copy_from_slice(SST_MAGIC);
        footer[4..8].copy_from_slice(&SST_FORMAT_VERSION.to_le_bytes());
        footer[8..16].copy_from_slice(&index_offset.to_le_bytes());
        footer[16..24].copy_from_slice(&index_len.to_le_bytes());
        footer[24..32].copy_from_slice(&bloom_offset.to_le_bytes());
        footer[32..40].copy_from_slice(&bloom_len.to_le_bytes());
        footer[40..48].copy_from_slice(&self.entry_count.to_le_bytes());
        // CRC32 over the first 48 bytes of the footer
        let footer_crc = block_crc32(&footer[0..48]);
        footer[48..52].copy_from_slice(&footer_crc.to_le_bytes());
        self.writer.write_all(&footer)?;
        self.writer.flush()?;

        let file_size = self.offset + FOOTER_SIZE as u64;
        let sst_id = NEXT_SST_ID.fetch_add(1, Ordering::Relaxed);

        Ok(SstMeta {
            id: sst_id,
            path: self.path,
            level,
            min_key: self.first_key.unwrap_or_default(),
            max_key: self.last_key.unwrap_or_default(),
            entry_count: self.entry_count,
            file_size,
            seq,
        })
    }

    fn flush_block(&mut self) -> io::Result<()> {
        if self.block_buf.is_empty() {
            return Ok(());
        }

        let block_offset = self.offset;

        // Write block header: entry count
        let header = self.block_entry_count.to_le_bytes();
        self.writer.write_all(&header)?;
        self.offset += 4;

        // Write block data
        self.writer.write_all(&self.block_buf)?;
        self.offset += self.block_buf.len() as u64;

        // Write block CRC32 (over header + data)
        let mut crc_input = Vec::with_capacity(4 + self.block_buf.len());
        crc_input.extend_from_slice(&header);
        crc_input.extend_from_slice(&self.block_buf);
        let crc = block_crc32(&crc_input);
        self.writer.write_all(&crc.to_le_bytes())?;
        self.offset += 4;

        let block_len = 4 + self.block_buf.len() as u32 + 4; // header + data + crc

        // Record index entry (last key in this block)
        let last_key = self.last_key.clone().unwrap_or_default();
        self.index.push((last_key, block_offset, block_len));

        // Reset block buffer
        self.block_buf.clear();
        self.block_entry_count = 0;

        Ok(())
    }

    fn encode_index(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.index.len() as u32).to_le_bytes());
        for (key, offset, len) in &self.index {
            buf.extend_from_slice(&(*offset).to_le_bytes());
            buf.extend_from_slice(&(*len).to_le_bytes());
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
        }
        buf
    }
}

// ── SST Reader ──────────────────────────────────────────────────────────────

/// Index entry parsed from an SST file.
#[derive(Debug, Clone)]
struct IndexEntry {
    last_key: Vec<u8>,
    block_offset: u64,
    block_len: u32,
}

/// Reads an SST file for point lookups and range scans.
#[derive(Debug)]
pub struct SstReader {
    path: PathBuf,
    meta: SstMeta,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
}

impl SstReader {
    /// Open an SST file and read its index + bloom filter into memory.
    /// Returns `SstReadError` variants for all failure modes (never panics).
    pub fn open(path: &Path, sst_id: u64) -> io::Result<Self> {
        let sst_path = path.display().to_string();
        let file_len = fs::metadata(path).map_err(|e| SstReadError::Io {
            sst_path: sst_path.clone(), source: e, detail: "stat file".into(),
        })?;
        let file_len = file_len.len();

        // Accept both v2 (52-byte) and legacy v1 (48-byte) footers
        if file_len < FOOTER_SIZE_V1 as u64 {
            return Err(SstReadError::Truncated {
                sst_path: sst_path.clone(), block_offset: 0,
                expected_len: FOOTER_SIZE_V1, actual_len: file_len as usize,
                detail: "file too small for footer".into(),
            }.into());
        }

        let mut file = BufReader::new(File::open(path).map_err(|e| SstReadError::Io {
            sst_path: sst_path.clone(), source: e, detail: "open file".into(),
        })?);

        // Try v2 footer first (52 bytes), fall back to v1 (48 bytes)
        let (footer_bytes, footer_version) = if file_len >= FOOTER_SIZE as u64 {
            file.seek(SeekFrom::End(-(FOOTER_SIZE as i64))).map_err(|e| SstReadError::Io {
                sst_path: sst_path.clone(), source: e, detail: "seek to v2 footer".into(),
            })?;
            let mut buf = [0u8; FOOTER_SIZE];
            file.read_exact(&mut buf).map_err(|e| SstReadError::Io {
                sst_path: sst_path.clone(), source: e, detail: "read v2 footer".into(),
            })?;
            // Check if this is a valid v2 footer (magic at expected offset)
            if &buf[0..4] == SST_MAGIC {
                // Validate footer CRC
                let stored_crc = u32::from_le_bytes(slice_to_array(&buf[48..52])?);
                let computed_crc = block_crc32(&buf[0..48]);
                if stored_crc != computed_crc {
                    return Err(SstReadError::Corruption {
                        sst_path, block_offset: file_len - FOOTER_SIZE as u64,
                        block_size: FOOTER_SIZE as u32,
                        expected_crc: stored_crc, actual_crc: computed_crc,
                        detail: "footer checksum mismatch".into(),
                    }.into());
                }
                (buf, 2u8)
            } else {
                // Try v1 (48-byte footer at end)
                file.seek(SeekFrom::End(-(FOOTER_SIZE_V1 as i64))).map_err(|e| SstReadError::Io {
                    sst_path: sst_path.clone(), source: e, detail: "seek to v1 footer".into(),
                })?;
                let mut v1buf = [0u8; FOOTER_SIZE];
                file.read_exact(&mut v1buf[..FOOTER_SIZE_V1]).map_err(|e| SstReadError::Io {
                    sst_path: sst_path.clone(), source: e, detail: "read v1 footer".into(),
                })?;
                (v1buf, 1u8)
            }
        } else {
            // Only v1 footer fits
            file.seek(SeekFrom::End(-(FOOTER_SIZE_V1 as i64))).map_err(|e| SstReadError::Io {
                sst_path: sst_path.clone(), source: e, detail: "seek to v1 footer".into(),
            })?;
            let mut buf = [0u8; FOOTER_SIZE];
            file.read_exact(&mut buf[..FOOTER_SIZE_V1]).map_err(|e| SstReadError::Io {
                sst_path: sst_path.clone(), source: e, detail: "read v1 footer".into(),
            })?;
            (buf, 1u8)
        };

        // Validate magic
        if &footer_bytes[0..4] != SST_MAGIC {
            return Err(SstReadError::Codec {
                sst_path, detail: format!("invalid magic: {:?}", &footer_bytes[0..4]),
            }.into());
        }

        let version = u32::from_le_bytes(slice_to_array(&footer_bytes[4..8])?);
        if version > SST_FORMAT_VERSION {
            return Err(SstReadError::Codec {
                sst_path, detail: format!("unsupported SST version: {} (max: {})", version, SST_FORMAT_VERSION),
            }.into());
        }

        let index_offset = u64::from_le_bytes(slice_to_array(&footer_bytes[8..16])?);
        let index_len = u64::from_le_bytes(slice_to_array(&footer_bytes[16..24])?);
        let bloom_offset = u64::from_le_bytes(slice_to_array(&footer_bytes[24..32])?);
        let bloom_len = u64::from_le_bytes(slice_to_array(&footer_bytes[32..40])?);
        let entry_count = u64::from_le_bytes(slice_to_array(&footer_bytes[40..48])?);

        // Validate offsets are within file bounds
        let data_end = if footer_version == 2 { file_len - FOOTER_SIZE as u64 } else { file_len - FOOTER_SIZE_V1 as u64 };
        if index_offset + index_len > data_end {
            return Err(SstReadError::Truncated {
                sst_path: sst_path.clone(), block_offset: index_offset,
                expected_len: index_len as usize, actual_len: (data_end.saturating_sub(index_offset)) as usize,
                detail: "index block extends beyond file".into(),
            }.into());
        }
        if bloom_offset + bloom_len > data_end {
            return Err(SstReadError::Truncated {
                sst_path: sst_path.clone(), block_offset: bloom_offset,
                expected_len: bloom_len as usize, actual_len: (data_end.saturating_sub(bloom_offset)) as usize,
                detail: "bloom block extends beyond file".into(),
            }.into());
        }

        // Read index block
        file.seek(SeekFrom::Start(index_offset)).map_err(|e| SstReadError::Io {
            sst_path: sst_path.clone(), source: e, detail: "seek to index".into(),
        })?;
        let mut index_buf = vec![0u8; index_len as usize];
        file.read_exact(&mut index_buf).map_err(|e| SstReadError::Io {
            sst_path: sst_path.clone(), source: e, detail: "read index block".into(),
        })?;
        let index = Self::parse_index(&index_buf).map_err(|e| SstReadError::Codec {
            sst_path: sst_path.clone(), detail: format!("index parse: {}", e),
        })?;

        // Read bloom filter
        file.seek(SeekFrom::Start(bloom_offset)).map_err(|e| SstReadError::Io {
            sst_path: sst_path.clone(), source: e, detail: "seek to bloom".into(),
        })?;
        let mut bloom_buf = vec![0u8; bloom_len as usize];
        file.read_exact(&mut bloom_buf).map_err(|e| SstReadError::Io {
            sst_path: sst_path.clone(), source: e, detail: "read bloom filter".into(),
        })?;
        let bloom = BloomFilter::from_bytes(&bloom_buf)
            .ok_or_else(|| SstReadError::Codec {
                sst_path: sst_path.clone(), detail: "invalid bloom filter encoding".into(),
            })?;

        // Derive min/max key from index
        let min_key = if let Some(first_block) = index.first() {
            Self::read_first_key_from_block(path, first_block.block_offset)?
        } else {
            Vec::new()
        };
        let max_key = index.last().map(|e| e.last_key.clone()).unwrap_or_default();

        let meta = SstMeta {
            id: sst_id,
            path: path.to_path_buf(),
            level: 0,
            min_key,
            max_key,
            entry_count,
            file_size: file_len,
            seq: 0,
        };

        Ok(Self { path: path.to_path_buf(), meta, index, bloom })
    }

    /// Point lookup: returns the value for the given key, or None.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        // Bloom filter check
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }

        // Binary search the index to find the candidate block
        let block_idx = self.index.partition_point(|e| e.last_key.as_slice() < key);
        if block_idx >= self.index.len() {
            return Ok(None);
        }

        let entry = &self.index[block_idx];
        let block_data = self.read_block(entry.block_offset, entry.block_len)?;
        Self::search_block(&block_data, key)
    }

    /// Scan all entries in sorted order.
    pub fn scan(&self) -> io::Result<Vec<SstEntry>> {
        let mut results = Vec::new();
        for entry in &self.index {
            let block_data = self.read_block(entry.block_offset, entry.block_len)?;
            let entries = Self::decode_block(&block_data)?;
            results.extend(entries);
        }
        Ok(results)
    }

    /// Get the SST metadata.
    pub fn meta(&self) -> &SstMeta {
        &self.meta
    }

    fn read_block(&self, offset: u64, len: u32) -> io::Result<Vec<u8>> {
        let sst_path = self.path.display().to_string();
        let mut file = BufReader::new(File::open(&self.path).map_err(|e| SstReadError::Io {
            sst_path: sst_path.clone(), source: e, detail: "open for block read".into(),
        })?);
        file.seek(SeekFrom::Start(offset)).map_err(|e| SstReadError::Io {
            sst_path: sst_path.clone(), source: e,
            detail: format!("seek to block offset={}", offset),
        })?;
        let mut buf = vec![0u8; len as usize];
        file.read_exact(&mut buf).map_err(|e| SstReadError::Truncated {
            sst_path: sst_path.clone(), block_offset: offset,
            expected_len: len as usize, actual_len: 0,
            detail: format!("block read_exact: {}", e),
        })?;

        // Verify block CRC if present (v2 format: last 4 bytes are CRC)
        if len >= 8 {
            let data_len = (len - 4) as usize;
            let stored_crc = u32::from_le_bytes(
                slice_to_array(&buf[data_len..data_len + 4])?,
            );
            let computed_crc = block_crc32(&buf[..data_len]);
            if stored_crc != computed_crc {
                // Could be a v1 block (no CRC) — only error if the CRC looks intentional.
                // Heuristic: if stored_crc is 0, it's likely v1 (no CRC appended).
                if stored_crc != 0 {
                    return Err(SstReadError::Corruption {
                        sst_path, block_offset: offset, block_size: len,
                        expected_crc: stored_crc, actual_crc: computed_crc,
                        detail: "block checksum mismatch".into(),
                    }.into());
                }
            }
        }

        Ok(buf)
    }

    fn search_block(block_data: &[u8], target_key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        if block_data.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                format!("SST block too short for header: {} bytes", block_data.len())));
        }
        let num_entries = u32::from_le_bytes(slice_to_array(&block_data[0..4])?) as usize;
        let mut pos = 4;

        for i in 0..num_entries {
            if pos + 8 > block_data.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                    format!("SST block truncated at entry {}/{}: pos={} len={}", i, num_entries, pos, block_data.len())));
            }
            let key_len = u32::from_le_bytes(slice_to_array(&block_data[pos..pos + 4])?) as usize;
            let val_len = u32::from_le_bytes(slice_to_array(&block_data[pos + 4..pos + 8])?) as usize;
            pos += 8;

            if pos + key_len + val_len > block_data.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                    format!("SST block entry {}/{} overflow: key_len={} val_len={} pos={} block_len={}",
                        i, num_entries, key_len, val_len, pos, block_data.len())));
            }
            let key = &block_data[pos..pos + key_len];
            let value = &block_data[pos + key_len..pos + key_len + val_len];
            pos += key_len + val_len;

            if key == target_key {
                return Ok(Some(value.to_vec()));
            }
            if key > target_key {
                return Ok(None);
            }
        }
        Ok(None)
    }

    fn decode_block(block_data: &[u8]) -> io::Result<Vec<SstEntry>> {
        let mut entries = Vec::new();
        if block_data.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                format!("SST block too short for header: {} bytes", block_data.len())));
        }
        let num_entries = u32::from_le_bytes(slice_to_array(&block_data[0..4])?) as usize;
        let mut pos = 4;

        for i in 0..num_entries {
            if pos + 8 > block_data.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                    format!("SST decode_block truncated at entry {}/{}: pos={} len={}", i, num_entries, pos, block_data.len())));
            }
            let key_len = u32::from_le_bytes(slice_to_array(&block_data[pos..pos + 4])?) as usize;
            let val_len = u32::from_le_bytes(slice_to_array(&block_data[pos + 4..pos + 8])?) as usize;
            pos += 8;

            if pos + key_len + val_len > block_data.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                    format!("SST decode_block entry {}/{} overflow: key_len={} val_len={} pos={} block_len={}",
                        i, num_entries, key_len, val_len, pos, block_data.len())));
            }
            let key = block_data[pos..pos + key_len].to_vec();
            let value = block_data[pos + key_len..pos + key_len + val_len].to_vec();
            pos += key_len + val_len;

            entries.push(SstEntry { key, value });
        }
        Ok(entries)
    }

    fn parse_index(data: &[u8]) -> io::Result<Vec<IndexEntry>> {
        if data.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                format!("SST index too short: {} bytes", data.len())));
        }
        let count = u32::from_le_bytes(slice_to_array(&data[0..4])?) as usize;
        let mut pos = 4;
        let mut entries = Vec::with_capacity(count);

        for i in 0..count {
            if pos + 16 > data.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                    format!("SST index truncated at entry {}/{}: pos={} len={}", i, count, pos, data.len())));
            }
            let block_offset = u64::from_le_bytes(slice_to_array(&data[pos..pos + 8])?);
            let block_len = u32::from_le_bytes(slice_to_array(&data[pos + 8..pos + 12])?);
            let key_len = u32::from_le_bytes(slice_to_array(&data[pos + 12..pos + 16])?) as usize;
            pos += 16;

            if pos + key_len > data.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                    format!("SST index entry {}/{} key overflow: key_len={} pos={} len={}", i, count, key_len, pos, data.len())));
            }
            let last_key = data[pos..pos + key_len].to_vec();
            pos += key_len;

            entries.push(IndexEntry { last_key, block_offset, block_len });
        }
        Ok(entries)
    }

    fn read_first_key_from_block(path: &Path, block_offset: u64) -> io::Result<Vec<u8>> {
        let mut file = BufReader::new(File::open(path)?);
        file.seek(SeekFrom::Start(block_offset))?;

        let mut header = [0u8; 4];
        file.read_exact(&mut header)?;
        let num_entries = u32::from_le_bytes(header);
        if num_entries == 0 {
            return Ok(Vec::new());
        }

        let mut lens = [0u8; 8];
        file.read_exact(&mut lens)?;
        let key_len = u32::from_le_bytes(slice_to_array(&lens[0..4])?) as usize;

        let mut key = vec![0u8; key_len];
        file.read_exact(&mut key)?;
        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_test_sst(dir: &Path, entries: &[(&[u8], &[u8])]) -> SstMeta {
        let path = dir.join("test.sst");
        let mut writer = SstWriter::new(&path, entries.len()).unwrap();
        for (k, v) in entries {
            writer.add(k, v).unwrap();
        }
        writer.finish(0, 1).unwrap()
    }

    #[test]
    fn test_sst_write_read_basic() {
        let dir = TempDir::new().unwrap();
        let entries: Vec<(&[u8], &[u8])> = vec![
            (b"aaa", b"val_a"),
            (b"bbb", b"val_b"),
            (b"ccc", b"val_c"),
        ];
        let meta = write_test_sst(dir.path(), &entries);

        assert_eq!(meta.entry_count, 3);
        assert_eq!(meta.min_key, b"aaa");
        assert_eq!(meta.max_key, b"ccc");
        assert_eq!(meta.level, 0);

        let reader = SstReader::open(&meta.path, meta.id).unwrap();
        assert_eq!(reader.get(b"aaa").unwrap(), Some(b"val_a".to_vec()));
        assert_eq!(reader.get(b"bbb").unwrap(), Some(b"val_b".to_vec()));
        assert_eq!(reader.get(b"ccc").unwrap(), Some(b"val_c".to_vec()));
        assert_eq!(reader.get(b"ddd").unwrap(), None);
        assert_eq!(reader.get(b"000").unwrap(), None);
    }

    #[test]
    fn test_sst_scan() {
        let dir = TempDir::new().unwrap();
        let entries: Vec<(&[u8], &[u8])> = vec![
            (b"k1", b"v1"),
            (b"k2", b"v2"),
            (b"k3", b"v3"),
        ];
        let meta = write_test_sst(dir.path(), &entries);
        let reader = SstReader::open(&meta.path, meta.id).unwrap();
        let scanned = reader.scan().unwrap();
        assert_eq!(scanned.len(), 3);
        assert_eq!(scanned[0].key, b"k1");
        assert_eq!(scanned[2].value, b"v3");
    }

    #[test]
    fn test_sst_many_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("big.sst");
        let n = 10_000;
        let mut writer = SstWriter::new(&path, n).unwrap();

        for i in 0..n {
            let key = format!("key_{:08}", i);
            let val = format!("val_{:08}", i);
            writer.add(key.as_bytes(), val.as_bytes()).unwrap();
        }
        let meta = writer.finish(0, 1).unwrap();
        assert_eq!(meta.entry_count, n as u64);

        let reader = SstReader::open(&path, meta.id).unwrap();

        // Point lookups
        assert_eq!(
            reader.get(b"key_00000000").unwrap(),
            Some(b"val_00000000".to_vec())
        );
        assert_eq!(
            reader.get(b"key_00005000").unwrap(),
            Some(b"val_00005000".to_vec())
        );
        assert_eq!(
            reader.get(b"key_00009999").unwrap(),
            Some(b"val_00009999".to_vec())
        );
        assert_eq!(reader.get(b"key_99999999").unwrap(), None);
    }

    #[test]
    fn test_sst_bloom_filter_rejects() {
        let dir = TempDir::new().unwrap();
        let entries: Vec<(&[u8], &[u8])> = vec![
            (b"alpha", b"1"),
            (b"beta", b"2"),
        ];
        let meta = write_test_sst(dir.path(), &entries);
        let reader = SstReader::open(&meta.path, meta.id).unwrap();

        // "gamma" is not in the bloom filter — should return None without reading blocks
        assert_eq!(reader.get(b"gamma").unwrap(), None);
    }

    #[test]
    fn test_sst_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("empty.sst");
        let writer = SstWriter::new(&path, 0).unwrap();
        let meta = writer.finish(0, 1).unwrap();
        assert_eq!(meta.entry_count, 0);

        let reader = SstReader::open(&path, meta.id).unwrap();
        assert_eq!(reader.get(b"anything").unwrap(), None);
        assert!(reader.scan().unwrap().is_empty());
    }

    #[test]
    fn test_sst_corrupt_footer_detected() {
        let dir = TempDir::new().unwrap();
        let entries: Vec<(&[u8], &[u8])> = vec![(b"k", b"v")];
        let meta = write_test_sst(dir.path(), &entries);

        // Corrupt the last byte of the footer (CRC field)
        let data = fs::read(&meta.path).unwrap();
        let mut corrupted = data.clone();
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xFF;
        fs::write(&meta.path, &corrupted).unwrap();

        let result = SstReader::open(&meta.path, meta.id);
        assert!(result.is_err(), "should detect footer corruption");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("checksum") || msg.contains("corruption") || msg.contains("Corruption"),
            "error should mention checksum: {}", msg);
    }

    #[test]
    fn test_sst_truncated_file_detected() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("trunc.sst");
        // Write a file that's too short to be a valid SST
        fs::write(&path, b"too short").unwrap();

        let result = SstReader::open(&path, 99);
        assert!(result.is_err(), "should detect truncated file");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("truncated") || msg.contains("Truncated") || msg.contains("too small"),
            "error should mention truncation: {}", msg);
    }

    #[test]
    fn test_sst_bad_magic_detected() {
        let dir = TempDir::new().unwrap();
        let entries: Vec<(&[u8], &[u8])> = vec![(b"k", b"v")];
        let meta = write_test_sst(dir.path(), &entries);

        // Overwrite magic bytes
        let mut data = fs::read(&meta.path).unwrap();
        let footer_start = data.len() - FOOTER_SIZE;
        data[footer_start..footer_start + 4].copy_from_slice(b"XXXX");
        // Also fix the CRC so it doesn't trigger CRC error first
        // (actually it will fail on magic check in v1 fallback path)
        fs::write(&meta.path, &data).unwrap();

        let result = SstReader::open(&meta.path, meta.id);
        assert!(result.is_err(), "should detect bad magic");
    }

    #[test]
    fn test_sst_corrupt_data_block_detected() {
        let dir = TempDir::new().unwrap();
        let entries: Vec<(&[u8], &[u8])> = vec![
            (b"aaa", b"val_a"),
            (b"bbb", b"val_b"),
        ];
        let meta = write_test_sst(dir.path(), &entries);

        // Corrupt a byte in the middle of the data block area (offset 10)
        let mut data = fs::read(&meta.path).unwrap();
        if data.len() > 10 {
            data[10] ^= 0xFF;
        }
        fs::write(&meta.path, &data).unwrap();

        let reader = SstReader::open(&meta.path, meta.id);
        // Either open fails (footer/index corruption) or get() fails (block corruption)
        if let Ok(r) = reader {
            let result = r.get(b"aaa");
            // Corrupted block should either return error or wrong data
            // (we can't guarantee which byte was corrupted)
            assert!(result.is_err() || result.unwrap() != Some(b"val_a".to_vec()),
                "corrupted block should be detected or data should differ");
        }
    }

    #[test]
    fn test_sst_read_error_display() {
        let err = SstReadError::Corruption {
            sst_path: "/tmp/test.sst".into(),
            block_offset: 1024,
            block_size: 4096,
            expected_crc: 0xDEADBEEF,
            actual_crc: 0xCAFEBABE,
            detail: "test corruption".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("/tmp/test.sst"));
        assert!(msg.contains("1024"));
        assert!(msg.contains("4096"));
        assert!(msg.contains("0xdeadbeef"));
        assert!(msg.contains("0xcafebabe"));

        let err2 = SstReadError::Truncated {
            sst_path: "/data/shard1.sst".into(),
            block_offset: 0,
            expected_len: 52,
            actual_len: 10,
            detail: "file too small".into(),
        };
        let msg2 = err2.to_string();
        assert!(msg2.contains("truncated"));
        assert!(msg2.contains("52"));
        assert!(msg2.contains("10"));

        let err3 = SstReadError::Codec {
            sst_path: "/data/bad.sst".into(),
            detail: "invalid magic".into(),
        };
        let msg3 = err3.to_string();
        assert!(msg3.contains("codec"));
        assert!(msg3.contains("invalid magic"));
    }
}
