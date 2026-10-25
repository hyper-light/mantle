//! What a node keeps in its directory (docs/design/node.md §1.6), beside its log: its run, the
//! count the node-pair liveness stream numbers its heartbeats by (`hyper_liveness::Settings::run`),
//! raised at every start before the stream's first heartbeat and kept as a record written whole
//! (hyper-block `record`: a temporary name, the platform's full flush, a rename, the directory's
//! flush, and a CRC-32C checked on read). A restarted node's heartbeats are numbered on from its
//! last run's, so its peers count one restart, not two (hyper-raft `docs/timing.md` §2.8); a node's
//! random identity has no order and cannot be the run. And the node's orderly stops, kept the same
//! way once each stop is done: the run that stopped, the node's time run then and how many of its
//! runs have stopped in order. Runs are numbered from one, so the runs before a start that did not
//! stop in order, its crashes, are its last run less its stops (docs/design/node.md §2.4, item 7).

use std::path::{Path, PathBuf};

/// The run record's file in the node's directory.
const RUN: &str = "run";
/// The run record's bytes: one count.
const RUN_BYTES: usize = 8;
/// The stop record's file in the node's directory.
const STOPPED: &str = "stopped";
/// The stop record's bytes: the run that stopped, the node's time run then in nanoseconds, and its
/// orderly stops.
const STOPPED_BYTES: usize = 24;

/// The node's last orderly stop: the run that stopped, the node's time run over all its runs when it
/// did, and how many of its runs had then stopped in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stopped {
    pub run: u64,
    pub exposure_ns: u64,
    pub stops: u64,
}

/// Why the node's records could not be read or kept.
#[derive(Debug, thiserror::Error)]
pub enum LayoutError {
    #[error("the node's run record: {0}")]
    Record(String),
    #[error("the node's run record holds {0} bytes, not one count")]
    Malformed(usize),
    #[error("the node's stop record holds {0} bytes, not a run, a time and a count")]
    MalformedStop(usize),
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

    /// The node's last run, none before its first.
    pub fn run(&self) -> Result<Option<u64>, LayoutError> {
        let path = self.dir.join(RUN);
        match hyper_block::record::read(&path, RUN_BYTES)
            .map_err(|e| LayoutError::Record(e.to_string()))?
        {
            None => Ok(None),
            Some(bytes) => {
                let count: [u8; RUN_BYTES] = bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| LayoutError::Malformed(bytes.len()))?;
                Ok(Some(u64::from_le_bytes(count)))
            }
        }
    }

    /// Raises the node's run and keeps it before returning it: the first run is 1.
    pub fn raise_run(&self) -> Result<u64, LayoutError> {
        let last = self.run()?.unwrap_or(0);
        let run = last.checked_add(1).ok_or(LayoutError::Exhausted(last))?;
        hyper_block::record::write(&self.dir.join(RUN), &run.to_le_bytes())
            .map_err(|e| LayoutError::Record(e.to_string()))?;
        Ok(run)
    }

    /// The run that last ended in an orderly stop, none before the first did.
    pub fn stopped(&self) -> Result<Option<Stopped>, LayoutError> {
        let path = self.dir.join(STOPPED);
        let Some(bytes) = hyper_block::record::read(&path, STOPPED_BYTES)
            .map_err(|e| LayoutError::Record(e.to_string()))?
        else {
            return Ok(None);
        };
        let words: [u8; STOPPED_BYTES] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| LayoutError::MalformedStop(bytes.len()))?;
        let (words, _) = words.as_chunks::<8>();
        let mut words = words.iter().map(|word| u64::from_le_bytes(*word));
        let mut next = || {
            words
                .next()
                .ok_or(LayoutError::MalformedStop(STOPPED_BYTES))
        };
        Ok(Some(Stopped {
            run: next()?,
            exposure_ns: next()?,
            stops: next()?,
        }))
    }

    /// Keeps `stopped` as the node's last orderly stop, once the stop is done.
    pub fn keep_stopped(&self, stopped: Stopped) -> Result<(), LayoutError> {
        let mut bytes = [0u8; STOPPED_BYTES];
        let words = [stopped.run, stopped.exposure_ns, stopped.stops];
        let (slots, _) = bytes.as_chunks_mut::<8>();
        for (slot, word) in slots.iter_mut().zip(words) {
            *slot = word.to_le_bytes();
        }
        hyper_block::record::write(&self.dir.join(STOPPED), &bytes)
            .map_err(|e| LayoutError::Record(e.to_string()))
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

    #[test]
    fn the_run_last_stopped_in_order_is_kept_with_the_time_run_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        assert_eq!(layout.run().unwrap(), None);
        assert_eq!(layout.stopped().unwrap(), None);
        let run = layout.raise_run().unwrap();
        assert_eq!(layout.run().unwrap(), Some(run));
        let stopped = Stopped {
            run,
            exposure_ns: u64::MAX,
            stops: 1,
        };
        layout.keep_stopped(stopped).unwrap();
        assert_eq!(Layout::new(dir.path()).stopped().unwrap(), Some(stopped));
        // A later stop replaces it.
        let later = Stopped {
            run: layout.raise_run().unwrap(),
            exposure_ns: 7,
            stops: u64::MAX,
        };
        layout.keep_stopped(later).unwrap();
        assert_eq!(layout.stopped().unwrap(), Some(later));
    }

    #[test]
    fn a_damaged_stop_record_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        layout
            .keep_stopped(Stopped {
                run: 1,
                exposure_ns: 2,
                stops: 1,
            })
            .unwrap();
        let path = dir.path().join(STOPPED);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        assert!(layout.stopped().is_err());
    }
}
