//! External merge sort for ORDER BY with disk spill support.
//!
//! When the number of rows exceeds `memory_rows_threshold`, sorted runs are
//! written to temporary files on disk. After all rows are consumed, the runs
//! are merged using a k-way merge (binary heap) to produce the final sorted
//! output.
//!
//! If the data fits in memory, no disk I/O occurs — the sort is a plain
//! in-memory `Vec::sort_by`.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use falcon_common::config::SpillConfig;
use falcon_common::datum::{Datum, OwnedRow};
use falcon_common::error::FalconError;
use falcon_sql_frontend::types::BoundOrderBy;

/// Comparator closure type for ordering rows.
type RowCmp = Box<dyn Fn(&OwnedRow, &OwnedRow) -> Ordering + Send>;

/// Build a comparator from `BoundOrderBy` specs.
fn make_comparator(order_by: &[BoundOrderBy]) -> RowCmp {
    let specs: Vec<(usize, bool)> = order_by.iter().map(|ob| (ob.column_idx, ob.asc)).collect();
    Box::new(move |a: &OwnedRow, b: &OwnedRow| {
        for &(idx, asc) in &specs {
            let av = a.get(idx).unwrap_or(&Datum::Null);
            let bv = b.get(idx).unwrap_or(&Datum::Null);
            let cmp = if asc { av.cmp(bv) } else { bv.cmp(av) };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    })
}

/// An external merge sorter that spills to disk when necessary.
pub struct ExternalSorter {
    /// Max rows to hold in memory before flushing a sorted run to disk.
    threshold: usize,
    /// Directory for temporary spill files.
    temp_dir: PathBuf,
    /// K-way merge fan-in.
    fan_in: usize,
}

impl ExternalSorter {
    /// Create a new sorter from config. Returns `None` if spill is disabled
    /// (threshold == 0), signaling the caller to use plain in-memory sort.
    pub fn from_config(config: &SpillConfig) -> Option<Self> {
        if config.memory_rows_threshold == 0 {
            return None;
        }
        let temp_dir = if config.temp_dir.is_empty() {
            std::env::temp_dir().join("falcon_spill")
        } else {
            PathBuf::from(&config.temp_dir)
        };
        Some(Self {
            threshold: config.memory_rows_threshold,
            temp_dir,
            fan_in: config.merge_fan_in.max(2),
        })
    }

    /// Sort `rows` in place according to `order_by`, spilling to disk if needed.
    /// This is the main entry point — it decides whether to spill or sort in-memory.
    pub fn sort(
        &self,
        rows: &mut Vec<OwnedRow>,
        order_by: &[BoundOrderBy],
    ) -> Result<(), FalconError> {
        if order_by.is_empty() {
            return Ok(());
        }

        // If data fits in memory, just sort in place
        if rows.len() <= self.threshold {
            let cmp = make_comparator(order_by);
            rows.sort_by(|a, b| cmp(a, b));
            return Ok(());
        }

        // External merge sort: split into sorted runs on disk, then merge
        let run_dir = self.create_run_dir()?;
        let result = self.external_merge_sort(rows, order_by, &run_dir);
        // Always clean up temp files
        let _ = fs::remove_dir_all(&run_dir);
        result
    }

    /// Perform the full external merge sort.
    fn external_merge_sort(
        &self,
        rows: &mut Vec<OwnedRow>,
        order_by: &[BoundOrderBy],
        run_dir: &Path,
    ) -> Result<(), FalconError> {
        let cmp = make_comparator(order_by);

        // Phase 1: Create sorted runs
        let mut run_paths = Vec::new();
        let chunks: Vec<Vec<OwnedRow>> = std::mem::take(rows)
            .chunks(self.threshold)
            .map(|c| c.to_vec())
            .collect();

        for (i, mut chunk) in chunks.into_iter().enumerate() {
            chunk.sort_by(|a, b| cmp(a, b));
            let path = run_dir.join(format!("run_{:06}.bin", i));
            write_run(&path, &chunk)?;
            run_paths.push(path);
        }

        // Phase 2: Multi-pass merge until we have a single run
        let mut merge_gen = 0u32;
        while run_paths.len() > 1 {
            let mut next_paths = Vec::new();
            for (group_idx, group) in run_paths.chunks(self.fan_in).enumerate() {
                if group.len() == 1 {
                    next_paths.push(group[0].clone());
                } else {
                    let merged_path = run_dir.join(format!("merge_g{}_i{}.bin", merge_gen, group_idx));
                    merge_runs(group, &merged_path, order_by)?;
                    // Remove consumed input runs
                    for p in group {
                        let _ = fs::remove_file(p);
                    }
                    next_paths.push(merged_path);
                }
            }
            run_paths = next_paths;
            merge_gen += 1;
        }

        // Phase 3: Read final sorted run back into memory
        if let Some(final_path) = run_paths.first() {
            *rows = read_run(final_path)?;
        }

        Ok(())
    }

    /// Create a unique subdirectory for this sort's run files.
    fn create_run_dir(&self) -> Result<PathBuf, FalconError> {
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir = self.temp_dir.join(format!("sort_{}", id));
        fs::create_dir_all(&dir).map_err(|e| {
            FalconError::Internal(format!("Failed to create spill directory {:?}: {}", dir, e))
        })?;
        Ok(dir)
    }
}

// ── Serialization helpers ──

/// Write a sorted run of rows to a binary file.
/// Format: [row_count: u64] then for each row [byte_len: u32][bincode bytes].
fn write_run(path: &Path, rows: &[OwnedRow]) -> Result<(), FalconError> {
    let file = File::create(path).map_err(|e| {
        FalconError::Internal(format!("Failed to create spill file {:?}: {}", path, e))
    })?;
    let mut w = BufWriter::new(file);

    w.write_all(&(rows.len() as u64).to_le_bytes()).map_err(io_err)?;
    for row in rows {
        let bytes = bincode::serialize(row).map_err(|e| {
            FalconError::Internal(format!("Spill serialization error: {}", e))
        })?;
        w.write_all(&(bytes.len() as u32).to_le_bytes()).map_err(io_err)?;
        w.write_all(&bytes).map_err(io_err)?;
    }
    w.flush().map_err(io_err)?;
    Ok(())
}

/// Read an entire sorted run from a binary file.
fn read_run(path: &Path) -> Result<Vec<OwnedRow>, FalconError> {
    let file = File::open(path).map_err(|e| {
        FalconError::Internal(format!("Failed to open spill file {:?}: {}", path, e))
    })?;
    let mut r = BufReader::new(file);
    let count = read_u64(&mut r)?;
    let mut rows = Vec::with_capacity(count as usize);
    for _ in 0..count {
        rows.push(read_one_row(&mut r)?);
    }
    Ok(rows)
}

/// A streaming reader for a sorted run file (reads one row at a time).
struct RunReader {
    reader: BufReader<File>,
    remaining: u64,
}

impl RunReader {
    fn open(path: &Path) -> Result<Self, FalconError> {
        let file = File::open(path).map_err(|e| {
            FalconError::Internal(format!("Failed to open spill file {:?}: {}", path, e))
        })?;
        let mut reader = BufReader::new(file);
        let remaining = read_u64(&mut reader)?;
        Ok(Self { reader, remaining })
    }

    fn next_row(&mut self) -> Result<Option<OwnedRow>, FalconError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        Ok(Some(read_one_row(&mut self.reader)?))
    }
}

/// K-way merge of sorted run files into a single output file.
fn merge_runs(
    input_paths: &[PathBuf],
    output_path: &Path,
    order_by: &[BoundOrderBy],
) -> Result<(), FalconError> {
    let cmp = make_comparator(order_by);

    // Open all input run readers
    let mut readers: Vec<RunReader> = input_paths
        .iter()
        .map(|p| RunReader::open(p))
        .collect::<Result<_, _>>()?;

    // Count total rows for output header
    let total: u64 = readers.iter().map(|r| r.remaining).sum();

    // Seed the min-heap with first row from each reader
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
    for (idx, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = reader.next_row()? {
            heap.push(HeapEntry {
                row,
                reader_idx: idx,
                cmp: &cmp,
            });
        }
    }

    // Open output
    let file = File::create(output_path).map_err(|e| {
        FalconError::Internal(format!("Failed to create merge file {:?}: {}", output_path, e))
    })?;
    let mut w = BufWriter::new(file);
    w.write_all(&total.to_le_bytes()).map_err(io_err)?;

    while let Some(entry) = heap.pop() {
        // Write the smallest row
        let bytes = bincode::serialize(&entry.row).map_err(|e| {
            FalconError::Internal(format!("Spill serialization error: {}", e))
        })?;
        w.write_all(&(bytes.len() as u32).to_le_bytes()).map_err(io_err)?;
        w.write_all(&bytes).map_err(io_err)?;

        // Advance that reader
        let ridx = entry.reader_idx;
        if let Some(next_row) = readers[ridx].next_row()? {
            heap.push(HeapEntry {
                row: next_row,
                reader_idx: ridx,
                cmp: &cmp,
            });
        }
    }
    w.flush().map_err(io_err)?;
    Ok(())
}

// ── Heap entry for k-way merge ──

struct HeapEntry<'a> {
    row: OwnedRow,
    reader_idx: usize,
    cmp: &'a RowCmp,
}

impl<'a> PartialEq for HeapEntry<'a> {
    fn eq(&self, other: &Self) -> bool {
        (self.cmp)(&self.row, &other.row) == Ordering::Equal
    }
}

impl<'a> Eq for HeapEntry<'a> {}

impl<'a> PartialOrd for HeapEntry<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for HeapEntry<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; reverse for min-heap behavior
        (self.cmp_fn())(&other.row, &self.row)
    }
}

impl<'a> HeapEntry<'a> {
    fn cmp_fn(&self) -> &RowCmp {
        self.cmp
    }
}

// ── I/O helpers ──

fn read_u64(r: &mut impl Read) -> Result<u64, FalconError> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf).map_err(io_err)?;
    Ok(u64::from_le_bytes(buf))
}

fn read_one_row(r: &mut impl Read) -> Result<OwnedRow, FalconError> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).map_err(io_err)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut data = vec![0u8; len];
    r.read_exact(&mut data).map_err(io_err)?;
    bincode::deserialize(&data).map_err(|e| {
        FalconError::Internal(format!("Spill deserialization error: {}", e))
    })
}

fn io_err(e: std::io::Error) -> FalconError {
    FalconError::Internal(format!("Spill I/O error: {}", e))
}

// ── Convenience function for executor integration ──

/// Sort rows using external merge sort if a sorter is configured,
/// otherwise fall back to plain in-memory sort.
pub fn sort_rows(
    rows: &mut Vec<OwnedRow>,
    order_by: &[BoundOrderBy],
    sorter: Option<&ExternalSorter>,
) -> Result<(), FalconError> {
    if order_by.is_empty() {
        return Ok(());
    }
    if let Some(s) = sorter {
        s.sort(rows, order_by)
    } else {
        // Pure in-memory sort (default path)
        let cmp = make_comparator(order_by);
        rows.sort_by(|a, b| cmp(a, b));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_common::config::SpillConfig;
    use falcon_common::datum::{Datum, OwnedRow};
    use falcon_sql_frontend::types::BoundOrderBy;

    fn make_row(val: i32) -> OwnedRow {
        OwnedRow::new(vec![Datum::Int32(val)])
    }

    fn make_rows(vals: &[i32]) -> Vec<OwnedRow> {
        vals.iter().map(|&v| make_row(v)).collect()
    }

    fn asc_order() -> Vec<BoundOrderBy> {
        vec![BoundOrderBy { column_idx: 0, asc: true }]
    }

    fn desc_order() -> Vec<BoundOrderBy> {
        vec![BoundOrderBy { column_idx: 0, asc: false }]
    }

    fn extract_vals(rows: &[OwnedRow]) -> Vec<i32> {
        rows.iter()
            .map(|r| match &r.values[0] {
                Datum::Int32(v) => *v,
                _ => panic!("expected Int32"),
            })
            .collect()
    }

    #[test]
    fn test_in_memory_sort_asc() {
        let mut rows = make_rows(&[5, 3, 1, 4, 2]);
        sort_rows(&mut rows, &asc_order(), None).unwrap();
        assert_eq!(extract_vals(&rows), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_in_memory_sort_desc() {
        let mut rows = make_rows(&[5, 3, 1, 4, 2]);
        sort_rows(&mut rows, &desc_order(), None).unwrap();
        assert_eq!(extract_vals(&rows), vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_external_sort_small_threshold() {
        // Threshold of 3 rows forces spill with 10 rows
        let config = SpillConfig {
            memory_rows_threshold: 3,
            temp_dir: String::new(),
            merge_fan_in: 2,
        };
        let sorter = ExternalSorter::from_config(&config).unwrap();
        let mut rows = make_rows(&[10, 7, 3, 8, 1, 6, 9, 2, 5, 4]);
        sorter.sort(&mut rows, &asc_order()).unwrap();
        assert_eq!(extract_vals(&rows), vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_external_sort_desc() {
        let config = SpillConfig {
            memory_rows_threshold: 4,
            temp_dir: String::new(),
            merge_fan_in: 2,
        };
        let sorter = ExternalSorter::from_config(&config).unwrap();
        let mut rows = make_rows(&[1, 5, 3, 7, 2, 8, 4, 6]);
        sorter.sort(&mut rows, &desc_order()).unwrap();
        assert_eq!(extract_vals(&rows), vec![8, 7, 6, 5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_external_sort_exact_threshold() {
        // Exactly at threshold — should sort in memory (no spill)
        let config = SpillConfig {
            memory_rows_threshold: 5,
            temp_dir: String::new(),
            merge_fan_in: 4,
        };
        let sorter = ExternalSorter::from_config(&config).unwrap();
        let mut rows = make_rows(&[5, 3, 1, 4, 2]);
        sorter.sort(&mut rows, &asc_order()).unwrap();
        assert_eq!(extract_vals(&rows), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_disabled_returns_none() {
        let config = SpillConfig {
            memory_rows_threshold: 0,
            temp_dir: String::new(),
            merge_fan_in: 16,
        };
        assert!(ExternalSorter::from_config(&config).is_none());
    }

    #[test]
    fn test_sort_rows_convenience() {
        let config = SpillConfig {
            memory_rows_threshold: 2,
            temp_dir: String::new(),
            merge_fan_in: 2,
        };
        let sorter = ExternalSorter::from_config(&config).unwrap();
        let mut rows = make_rows(&[3, 1, 2]);
        sort_rows(&mut rows, &asc_order(), Some(&sorter)).unwrap();
        assert_eq!(extract_vals(&rows), vec![1, 2, 3]);
    }

    #[test]
    fn test_write_read_run_roundtrip() {
        let rows = make_rows(&[42, 7, 99]);
        let dir = std::env::temp_dir().join("falcon_test_run");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("test_roundtrip.bin");
        write_run(&path, &rows).unwrap();
        let loaded = read_run(&path).unwrap();
        assert_eq!(extract_vals(&loaded), vec![42, 7, 99]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_multi_column_sort() {
        let mut rows = vec![
            OwnedRow::new(vec![Datum::Int32(1), Datum::Text("b".into())]),
            OwnedRow::new(vec![Datum::Int32(1), Datum::Text("a".into())]),
            OwnedRow::new(vec![Datum::Int32(2), Datum::Text("a".into())]),
        ];
        let order = vec![
            BoundOrderBy { column_idx: 0, asc: true },
            BoundOrderBy { column_idx: 1, asc: true },
        ];
        sort_rows(&mut rows, &order, None).unwrap();
        assert_eq!(rows[0].values[1], Datum::Text("a".into()));
        assert_eq!(rows[1].values[1], Datum::Text("b".into()));
        assert_eq!(rows[2].values[0], Datum::Int32(2));
    }
}
