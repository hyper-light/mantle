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

use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

use mantle_meta::record::FINGERPRINT;

/// The node's log's file in its directory (`node::open_log`).
pub(crate) const LOG: &str = "log";
/// The run record's file in the node's directory.
pub(crate) const RUN: &str = "run";
/// The run record's bytes: one count.
const RUN_BYTES: usize = 8;
/// The readiness lock's file: held exclusively by the node from the moment the cell's root range
/// enlisted it in its run until its process ends, which releases the lock however it ends, for a
/// supervisor's readiness check (`mantle status`; research/78 [13], [14], [15]).
pub(crate) const LIVE: &str = "live";
/// The stop record's file in the node's directory.
pub(crate) const STOPPED: &str = "stopped";
/// The stop record's bytes: the run that stopped, the node's time run then in nanoseconds, and its
/// orderly stops.
const STOPPED_BYTES: usize = 24;
/// The marker of a node the cell wrote off (docs/design/membership.md §5.1 step 1): the fingerprint
/// of the certificate it was refused under, kept as the other records are. A start that finds it
/// serves nothing (step 2).
const REJOIN: &str = "rejoin";

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
    #[error("the node's readiness lock: {0}")]
    Live(std::io::Error),
    #[error("the node's rejoin marker holds {0} bytes, not one certificate's fingerprint")]
    MalformedRejoin(usize),
    #[error("the node's rejoin marker: {0}")]
    Unretire(String),
}

/// The node's readiness lock, held: the cell's root range enlisted the node in this run, and the
/// node lives. It is given up as the process ends, in order or not.
pub struct Live {
    _file: File,
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

    /// Takes the readiness lock, once the cell's root range enlisted the node in this run: none
    /// while a readiness check holds it shared for the moment it looks ([`Layout::ready`]), so the
    /// node takes it at a later turn. Only the node's process takes it, under the directory's own
    /// lock, which refuses a second process.
    pub fn take_live(&self) -> Result<Option<Live>, LayoutError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.dir.join(LIVE))
            .map_err(LayoutError::Live)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Live { _file: file })),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Error(e)) => Err(LayoutError::Live(e)),
        }
    }

    /// Whether the node is ready to serve: a living process of it holds the readiness lock, which
    /// it takes once the cell's root range enlisted it in its current run. Asked shared and without
    /// waiting, the lock is refused while the node holds it; granted, no process of the node is
    /// enlisted, and it is given up at once. A process that ended holds nothing: its records say
    /// nothing of whether one serves now, and a successor recovering its stores is not ready until
    /// it enlists.
    pub fn ready(&self) -> Result<bool, LayoutError> {
        let file = match File::open(self.dir.join(LIVE)) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(LayoutError::Live(e)),
        };
        match file.try_lock_shared() {
            Ok(()) => Ok(false),
            Err(TryLockError::WouldBlock) => Ok(true),
            Err(TryLockError::Error(e)) => Err(LayoutError::Live(e)),
        }
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

    /// Keeps the marker that the cell wrote the node off under the certificate whose fingerprint is
    /// `fingerprint`: whole and durable when this returns (membership.md §5.1 step 1).
    pub fn retire(&self, fingerprint: &[u8; FINGERPRINT]) -> Result<(), LayoutError> {
        hyper_block::record::write(&self.dir.join(REJOIN), fingerprint)
            .map_err(|e| LayoutError::Record(e.to_string()))
    }

    /// The fingerprint the rejoin marker holds, none where the cell has not written the node off.
    pub fn retired(&self) -> Result<Option<[u8; FINGERPRINT]>, LayoutError> {
        let path = self.dir.join(REJOIN);
        let Some(bytes) = hyper_block::record::read(&path, FINGERPRINT)
            .map_err(|e| LayoutError::Record(e.to_string()))?
        else {
            return Ok(None);
        };
        let fingerprint: [u8; FINGERPRINT] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| LayoutError::MalformedRejoin(bytes.len()))?;
        Ok(Some(fingerprint))
    }

    /// Removes the rejoin marker, once the node took its new identity (membership.md §5.1 step 5):
    /// gone durably when this returns, its directory flushed (CLAUDE.md §6). One removed already is
    /// removed again as nothing, and its directory flushed again, which a crash after the removal
    /// and before the flush needs.
    pub fn unretire(&self) -> Result<(), LayoutError> {
        let path = self.dir.join(REJOIN);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(LayoutError::Unretire(e.to_string())),
        }
        hyper_block::file::sync_dir(&self.dir).map_err(|e| LayoutError::Unretire(e.to_string()))
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
    fn a_node_is_ready_while_its_readiness_lock_is_held_and_not_once_it_is_given_up() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        assert!(
            !layout.ready().unwrap(),
            "no process of the node has enlisted"
        );
        let live = layout.take_live().unwrap().expect("no check holds it");
        assert!(layout.ready().unwrap(), "held by the enlisted node");
        // Asked as a second process would ask, it is refused while held.
        assert!(layout.take_live().unwrap().is_none());
        // Given up as the process ends, in order or not: the file stays and says nothing.
        drop(live);
        assert!(dir.path().join(LIVE).exists());
        assert!(!layout.ready().unwrap(), "a node whose process ended");
    }

    #[test]
    fn a_take_while_a_check_looks_is_refused_and_granted_once_it_has_looked() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        drop(layout.take_live().unwrap());
        // A readiness check's shared hold, for the moment it looks.
        let looking = File::open(dir.path().join(LIVE)).unwrap();
        looking.try_lock_shared().unwrap();
        assert!(layout.take_live().unwrap().is_none());
        drop(looking);
        assert!(layout.take_live().unwrap().is_some());
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

    /// The rejoin marker is written whole, survives into the next start's reading, and a damaged one
    /// is refused as corrupt rather than read as no marker (membership.md §5.1 step 1).
    #[test]
    fn the_rejoin_marker_is_kept_whole_and_read_back_at_the_next_start() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        assert_eq!(layout.retired().unwrap(), None);
        let fingerprint = [7u8; FINGERPRINT];
        layout.retire(&fingerprint).unwrap();
        // The temporary name is gone: the marker is in place under its own.
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, vec![std::ffi::OsString::from(REJOIN)]);
        assert_eq!(
            Layout::new(dir.path()).retired().unwrap(),
            Some(fingerprint)
        );
        let path = dir.path().join(REJOIN);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        assert!(layout.retired().is_err());
    }
}
