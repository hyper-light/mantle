//! What a node keeps in its directory (docs/design/node.md §1.6), beside its log: its run, the
//! count the node-pair liveness stream numbers its heartbeats by (`hyper_liveness::Settings::run`),
//! raised at every start before the stream's first heartbeat and kept as a record written whole
//! (hyper-block `record`: a temporary name, the platform's full flush, a rename, the directory's
//! flush, and a CRC-32C checked on read). A restarted node's heartbeats are numbered on from its
//! last run's, so its peers count one restart, not two (hyper-raft `docs/timing.md` §2.8); a node's
//! random identity has no order and cannot be the run.

use std::path::{Path, PathBuf};

/// The run record's file in the node's directory.
const RUN: &str = "run";
/// The run record's bytes: one count.
const RUN_BYTES: usize = 8;

/// Why the node's records could not be read or kept.
#[derive(Debug, thiserror::Error)]
pub enum LayoutError {
    #[error("the node's run record: {0}")]
    Record(String),
    #[error("the node's run record holds {0} bytes, not one count")]
    Malformed(usize),
    #[error("the node's run count cannot rise past {0}")]
    Exhausted(u64),
}

/// The node's directory.
#[derive(Debug, Clone)]
pub struct Layout {
    dir: PathBuf,
}

impl Layout {
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Raises the node's run and keeps it before returning it: the first run is 1.
    pub fn raise_run(&self) -> Result<u64, LayoutError> {
        let path = self.dir.join(RUN);
        let last = match hyper_block::record::read(&path, RUN_BYTES)
            .map_err(|e| LayoutError::Record(e.to_string()))?
        {
            None => 0,
            Some(bytes) => {
                let count: [u8; RUN_BYTES] = bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| LayoutError::Malformed(bytes.len()))?;
                u64::from_le_bytes(count)
            }
        };
        let run = last.checked_add(1).ok_or(LayoutError::Exhausted(last))?;
        hyper_block::record::write(&path, &run.to_le_bytes())
            .map_err(|e| LayoutError::Record(e.to_string()))?;
        Ok(run)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_start_raises_the_run_and_keeps_it() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        assert_eq!(layout.raise_run().unwrap(), 1);
        assert_eq!(layout.raise_run().unwrap(), 2);
        assert_eq!(Layout::new(dir.path()).raise_run().unwrap(), 3);
    }

    #[test]
    fn a_damaged_run_record_is_refused_not_restarted() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout.raise_run().unwrap();
        let path = dir.path().join(RUN);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        assert!(layout.raise_run().is_err());
    }
}
