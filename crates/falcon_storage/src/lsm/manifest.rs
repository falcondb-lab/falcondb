use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use super::sst::SstMeta;

/// Persistent record of SST additions and removals.
/// Append-only log; compacted by rewriting on recovery.
///
/// Format: one JSON line per event.
#[derive(Debug)]
pub struct Manifest {
    path: PathBuf,
    log: File,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum ManifestRecord {
    Add {
        level: u32,
        path: String,
        min_key: Vec<u8>,
        max_key: Vec<u8>,
        entry_count: u64,
        file_size: u64,
        seq: u64,
    },
    Remove {
        path: String,
    },
}

impl Manifest {
    pub fn open(dir: &Path) -> io::Result<Self> {
        let path = dir.join("MANIFEST");
        let log = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self { path, log })
    }

    pub fn record_add(&mut self, meta: &SstMeta) -> io::Result<()> {
        let rec = ManifestRecord::Add {
            level: meta.level,
            path: meta.path.display().to_string(),
            min_key: meta.min_key.clone(),
            max_key: meta.max_key.clone(),
            entry_count: meta.entry_count,
            file_size: meta.file_size,
            seq: meta.seq,
        };
        self.append(&rec)
    }

    pub fn record_remove(&mut self, meta: &SstMeta) -> io::Result<()> {
        let rec = ManifestRecord::Remove {
            path: meta.path.display().to_string(),
        };
        self.append(&rec)
    }

    /// Replay manifest log to reconstruct level state.
    /// Returns levels[0..max_levels] each containing their SST metadata.
    pub fn replay(dir: &Path, max_levels: usize) -> io::Result<Vec<Vec<SstMeta>>> {
        let path = dir.join("MANIFEST");
        let mut levels: Vec<Vec<SstMeta>> = (0..max_levels).map(|_| Vec::new()).collect();

        if !path.exists() {
            return Ok(levels);
        }

        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let mut next_id = 1u64;

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let rec: ManifestRecord = match serde_json::from_str(&line) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("manifest: skipping corrupt line: {}", e);
                    continue;
                }
            };
            match rec {
                ManifestRecord::Add {
                    level,
                    path,
                    min_key,
                    max_key,
                    entry_count,
                    file_size,
                    seq,
                } => {
                    let sst_path = PathBuf::from(&path);
                    if !sst_path.exists() {
                        continue; // SST was deleted externally
                    }
                    let lvl = level as usize;
                    if lvl < levels.len() {
                        levels[lvl].push(SstMeta {
                            id: next_id,
                            path: sst_path,
                            level,
                            min_key,
                            max_key,
                            entry_count,
                            file_size,
                            seq,
                        });
                        next_id += 1;
                    }
                }
                ManifestRecord::Remove { path } => {
                    let sst_path = PathBuf::from(&path);
                    for lvl in &mut levels {
                        lvl.retain(|m| m.path != sst_path);
                    }
                }
            }
        }

        // Sort: L0 by seq desc, L1+ by min_key asc
        if !levels.is_empty() {
            levels[0].sort_by(|a, b| b.seq.cmp(&a.seq));
        }
        for lvl in levels.iter_mut().skip(1) {
            lvl.sort_by(|a, b| a.min_key.cmp(&b.min_key));
        }

        Ok(levels)
    }

    /// Compact manifest: rewrite from current level state.
    pub fn compact(&mut self, levels: &[Vec<SstMeta>]) -> io::Result<()> {
        let tmp = self.path.with_extension("tmp");
        {
            let mut f = File::create(&tmp)?;
            for (lvl_idx, lvl) in levels.iter().enumerate() {
                for meta in lvl {
                    let rec = ManifestRecord::Add {
                        level: lvl_idx as u32,
                        path: meta.path.display().to_string(),
                        min_key: meta.min_key.clone(),
                        max_key: meta.max_key.clone(),
                        entry_count: meta.entry_count,
                        file_size: meta.file_size,
                        seq: meta.seq,
                    };
                    let line = serde_json::to_string(&rec).map_err(io::Error::other)?;
                    writeln!(f, "{}", line)?;
                }
            }
            f.flush()?;
        }
        fs::rename(&tmp, &self.path)?;
        // Reopen for append
        self.log = OpenOptions::new().append(true).open(&self.path)?;
        Ok(())
    }

    fn append(&mut self, rec: &ManifestRecord) -> io::Result<()> {
        let line = serde_json::to_string(rec).map_err(io::Error::other)?;
        writeln!(self.log, "{}", line)?;
        self.log.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn dummy_meta(dir: &Path, level: u32, seq: u64, min: &[u8], max: &[u8]) -> SstMeta {
        let fname = format!("sst_L{}_{:06}.sst", level, seq);
        let path = dir.join(&fname);
        fs::write(&path, b"dummy").unwrap();
        SstMeta {
            id: seq,
            path,
            level,
            min_key: min.to_vec(),
            max_key: max.to_vec(),
            entry_count: 10,
            file_size: 100,
            seq,
        }
    }

    #[test]
    fn test_manifest_add_replay() {
        let dir = TempDir::new().unwrap();
        let m1 = dummy_meta(dir.path(), 0, 1, b"aaa", b"zzz");
        let m2 = dummy_meta(dir.path(), 1, 2, b"aaa", b"mmm");
        {
            let mut mf = Manifest::open(dir.path()).unwrap();
            mf.record_add(&m1).unwrap();
            mf.record_add(&m2).unwrap();
        }
        let levels = Manifest::replay(dir.path(), 7).unwrap();
        assert_eq!(levels[0].len(), 1);
        assert_eq!(levels[1].len(), 1);
    }

    #[test]
    fn test_manifest_remove() {
        let dir = TempDir::new().unwrap();
        let m1 = dummy_meta(dir.path(), 0, 1, b"a", b"z");
        {
            let mut mf = Manifest::open(dir.path()).unwrap();
            mf.record_add(&m1).unwrap();
            mf.record_remove(&m1).unwrap();
        }
        let levels = Manifest::replay(dir.path(), 7).unwrap();
        assert_eq!(levels[0].len(), 0);
    }

    #[test]
    fn test_manifest_compact() {
        let dir = TempDir::new().unwrap();
        let m1 = dummy_meta(dir.path(), 0, 1, b"a", b"z");
        let m2 = dummy_meta(dir.path(), 1, 2, b"a", b"m");
        let levels = vec![vec![m1], vec![m2]];
        {
            let mut mf = Manifest::open(dir.path()).unwrap();
            mf.compact(&levels).unwrap();
        }
        let replayed = Manifest::replay(dir.path(), 7).unwrap();
        assert_eq!(replayed[0].len(), 1);
        assert_eq!(replayed[1].len(), 1);
    }
}
