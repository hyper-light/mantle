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
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
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
/// of the certificate it was refused under, then where each member the node knew as it was written
/// off listens, kept as the other records are. A start that finds it serves nothing (step 2), and
/// its ask dials those members (step 3).
const REJOIN: &str = "rejoin";
/// A member's QUIC address in its rejoin marker entry, a field of fixed width: the address's
/// family (4 or 6), its 16 bytes (an IPv4 address in the first 4, the rest zero), its port and its
/// IPv6 scope.
const FIELD_BYTES: usize = 1 + 16 + 2 + 4;
/// A member's entry in the rejoin marker: its id, then its address's field.
const MEMBER_BYTES: usize = 8 + FIELD_BYTES;
const IPV4: u8 = 4;
const IPV6: u8 = 6;

/// What the rejoin marker holds: the fingerprint of the certificate the cell wrote the node off
/// under, and where each member the node knew as it was written off listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retired {
    pub fingerprint: [u8; FINGERPRINT],
    pub members: Vec<(u64, SocketAddr)>,
}

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
    #[error(
        "the node's rejoin marker holds {0} bytes, not one certificate's fingerprint and whole members"
    )]
    MalformedRejoin(usize),
    #[error("the node's rejoin marker's bound of {0} members is past what a length holds")]
    RejoinBound(u32),
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
    /// `fingerprint`, with the first `most` of `members`, where each member the node knows listens
    /// (`most` the cell's bound on nodes): whole and durable when this returns (membership.md §5.1
    /// step 1).
    pub fn retire(
        &self,
        fingerprint: &[u8; FINGERPRINT],
        members: &[(u64, SocketAddr)],
        most: u32,
    ) -> Result<(), LayoutError> {
        let first = usize::try_from(most).map_err(|_| LayoutError::RejoinBound(most))?;
        let kept = members.get(..first).unwrap_or(members);
        let bytes = rejoin_bytes(kept.len()).ok_or(LayoutError::RejoinBound(most))?;
        let mut marker = Vec::with_capacity(bytes);
        marker.extend_from_slice(fingerprint);
        for (id, at) in kept {
            marker.extend_from_slice(&id.to_le_bytes());
            marker.extend_from_slice(&address_field(at));
        }
        hyper_block::record::write(&self.dir.join(REJOIN), &marker)
            .map_err(|e| LayoutError::Record(e.to_string()))
    }

    /// What the rejoin marker holds, of at most `most` members (the cell's bound on nodes), none
    /// where the cell has not written the node off. One holding more, or bytes no whole member's
    /// entry ends, is malformed.
    pub fn retired(&self, most: u32) -> Result<Option<Retired>, LayoutError> {
        let path = self.dir.join(REJOIN);
        let limit = usize::try_from(most)
            .ok()
            .and_then(rejoin_bytes)
            .ok_or(LayoutError::RejoinBound(most))?;
        let Some(bytes) = hyper_block::record::read(&path, limit)
            .map_err(|e| LayoutError::Record(e.to_string()))?
        else {
            return Ok(None);
        };
        let malformed = || LayoutError::MalformedRejoin(bytes.len());
        let (fingerprint, rest) = bytes
            .split_first_chunk::<FINGERPRINT>()
            .ok_or_else(malformed)?;
        let (entries, left) = rest.as_chunks::<MEMBER_BYTES>();
        if !left.is_empty() {
            return Err(malformed());
        }
        let mut members = Vec::with_capacity(entries.len());
        for entry in entries {
            let (id, field) = entry.split_first_chunk::<8>().ok_or_else(malformed)?;
            let at = field_address(field).ok_or_else(malformed)?;
            members.push((u64::from_le_bytes(*id), at));
        }
        Ok(Some(Retired {
            fingerprint: *fingerprint,
            members,
        }))
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

/// The bytes of a rejoin marker of `members` members, its fingerprint and each member's entry; none
/// past what a length holds.
fn rejoin_bytes(members: usize) -> Option<usize> {
    members.checked_mul(MEMBER_BYTES)?.checked_add(FINGERPRINT)
}

/// `at` in a marker entry's fixed field: its family, its 16 address bytes, its port and its scope.
fn address_field(at: &SocketAddr) -> Vec<u8> {
    let (family, octets, scope) = match at {
        SocketAddr::V4(v4) => {
            let [a, b, c, d] = v4.ip().octets();
            (IPV4, [a, b, c, d, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 0u32)
        }
        SocketAddr::V6(v6) => (IPV6, v6.ip().octets(), v6.scope_id()),
    };
    let mut field = Vec::with_capacity(FIELD_BYTES);
    field.push(family);
    field.extend_from_slice(&octets);
    field.extend_from_slice(&at.port().to_le_bytes());
    field.extend_from_slice(&scope.to_le_bytes());
    field
}

/// The address a marker entry's field holds, none where its family is neither IPv4's nor IPv6's.
fn field_address(field: &[u8]) -> Option<SocketAddr> {
    let (family, rest) = field.split_first()?;
    let (octets, rest) = rest.split_first_chunk::<16>()?;
    let (port, rest) = rest.split_first_chunk::<2>()?;
    let (scope, rest) = rest.split_first_chunk::<4>()?;
    if !rest.is_empty() {
        return None;
    }
    let port = u16::from_le_bytes(*port);
    match *family {
        IPV4 => {
            let (v4, zero) = octets.split_first_chunk::<4>()?;
            if zero.iter().any(|b| *b != 0) {
                return None;
            }
            Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(*v4)), port))
        }
        IPV6 => Some(SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::from(*octets),
            port,
            0,
            u32::from_le_bytes(*scope),
        ))),
        _ => None,
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

    /// The rejoin marker is written whole, survives into the next start's reading with its
    /// fingerprint and each member's address, IPv4 and IPv6 with its scope, and a damaged one is
    /// refused as corrupt rather than read as no marker (membership.md §5.1 step 1).
    #[test]
    fn the_rejoin_marker_is_kept_whole_and_read_back_at_the_next_start() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        assert_eq!(layout.retired(3).unwrap(), None);
        let fingerprint = [7u8; FINGERPRINT];
        let members: Vec<(u64, SocketAddr)> = vec![
            (11, "127.0.0.1:7000".parse().unwrap()),
            (12, "[fe80::1%3]:7001".parse().unwrap()),
            (13, "[2001:db8::2]:7002".parse().unwrap()),
        ];
        layout.retire(&fingerprint, &members, 3).unwrap();
        // The temporary name is gone: the marker is in place under its own.
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, vec![std::ffi::OsString::from(REJOIN)]);
        assert_eq!(
            Layout::new(dir.path()).retired(3).unwrap(),
            Some(Retired {
                fingerprint,
                members: members.clone(),
            })
        );
        // A marker of more members than the bound read with is refused, not cut short.
        assert!(layout.retired(2).is_err());
        let path = dir.path().join(REJOIN);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        assert!(layout.retired(3).is_err());
    }

    /// A marker keeps the first `most` members it is given, and one of none holds the fingerprint
    /// alone; bytes that end no whole member, or an entry of no address family, are malformed.
    #[test]
    fn the_rejoin_marker_keeps_at_most_its_bound_and_refuses_partial_members() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path());
        let fingerprint = [9u8; FINGERPRINT];
        let members: Vec<(u64, SocketAddr)> = (0..5u16)
            .map(|n| (u64::from(n), SocketAddr::from(([10, 0, 0, 1], 7000 + n))))
            .collect();
        layout.retire(&fingerprint, &members, 2).unwrap();
        assert_eq!(
            layout.retired(2).unwrap().unwrap().members,
            members[..2].to_vec()
        );
        layout.retire(&fingerprint, &[], 0).unwrap();
        assert_eq!(
            layout.retired(0).unwrap(),
            Some(Retired {
                fingerprint,
                members: Vec::new(),
            })
        );
        let path = dir.path().join(REJOIN);
        let mut partial = fingerprint.to_vec();
        partial.extend_from_slice(&[0u8; MEMBER_BYTES - 1]);
        hyper_block::record::write(&path, &partial).unwrap();
        assert!(matches!(
            layout.retired(1),
            Err(LayoutError::MalformedRejoin(_))
        ));
        let mut familyless = fingerprint.to_vec();
        familyless.extend_from_slice(&[0u8; MEMBER_BYTES]);
        hyper_block::record::write(&path, &familyless).unwrap();
        assert!(matches!(
            layout.retired(1),
            Err(LayoutError::MalformedRejoin(_))
        ));
    }
}
