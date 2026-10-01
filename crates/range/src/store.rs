//! The group's view of the device's log, as focal-raft reads what is durable
//! (docs/design/replica.md §3; 07 §1.2). The log keeps each entry's index and term; its bytes
//! are the entry's type, context and data.

use std::cell::Cell;
use std::sync::Arc;

use focal_raft::proto::{ConfState, Entry, EntryType, HardState, Snapshot};
use focal_raft::{InitialState, Storage, StorageError};
use hyper_block::block::BlockFile;
use hyper_log::{Fetched, Log, LogError};

pub struct LogStore<F: BlockFile + 'static> {
    pub log: Arc<Log<F>>,
    pub group: u128,
    /// The configuration of the state the engine opened at.
    pub conf: ConfState,
    /// The snapshot the replica last prepared, at an index at or past the log's start, for
    /// a member that needs entries the log no longer holds.
    pub snapshot: Option<Snapshot>,
    /// The reservation entries are fetched into and decoded from, kept between reads so a
    /// read allocates only the entries it gives out. It is kept only after a read of no more
    /// than a segment's bytes, the most one frame's entries take, as hyper-log keeps its own
    /// (`Log::entries`), so it holds less than two segments' room; a larger read, a catch-up's,
    /// takes its reservation with it.
    fetched: Cell<Option<Fetched>>,
    /// The group's bounds as the log last gave them, kept while none of the group's writes is
    /// out. The core asks for them on most of its calls, and each ask of the log is a round
    /// trip to its owner thread (hyper-log). Only the group's own updates move them, all of
    /// which this member writes, so between its writes the log answers the same. While one is
    /// out (`writes`), every ask goes to the log, as a write's records reach readers before
    /// its answer.
    bounds: Cell<Option<Bounds>>,
    /// The group's writes submitted and not yet answered.
    writes: Cell<u64>,
}

/// Where a group's log starts, its last entry, and that entry's term once asked.
#[derive(Debug, Clone, Copy)]
struct Bounds {
    start: u64,
    last: u64,
    last_term: Option<u64>,
}

impl<F: BlockFile + 'static> LogStore<F> {
    pub fn new(log: Arc<Log<F>>, group: u128, conf: ConfState) -> Self {
        Self {
            log,
            group,
            conf,
            snapshot: None,
            fetched: Cell::new(None),
            bounds: Cell::new(None),
            writes: Cell::new(0),
        }
    }

    /// A write of the group is about to be submitted: the bounds may move.
    pub fn writing(&self) {
        self.writes.set(self.writes.get().saturating_add(1));
        self.bounds.set(None);
    }

    /// A write of the group was answered, or refused before the log took it.
    pub fn written(&self) {
        self.writes.set(self.writes.get().saturating_sub(1));
        self.bounds.set(None);
    }

    /// Where the group's log starts and its last entry: 0 and 0 for a group it holds nothing
    /// of.
    fn bounds(&self) -> Result<Bounds, StorageError> {
        if let Some(bounds) = self.bounds.get() {
            return Ok(bounds);
        }
        let view = self.log.view(self.group).map_err(|e| storage(&e))?;
        let (start, last) = view.map_or((0, 0), |v| (v.start.index, v.last));
        let bounds = Bounds {
            start,
            last,
            last_term: None,
        };
        if self.writes.get() == 0 {
            self.bounds.set(Some(bounds));
        }
        Ok(bounds)
    }

    /// The term of `index` as the log gives it: the core asks most often for the last entry's,
    /// which is kept with the bounds.
    fn term_of(&self, index: u64) -> Result<u64, StorageError> {
        let kept = self.bounds.get().filter(|b| b.last == index);
        if let Some(term) = kept.and_then(|b| b.last_term) {
            return Ok(term);
        }
        let term = match self.log.term(self.group, index) {
            Ok(term) => term,
            // A group the log holds nothing of starts after index 0, of term 0.
            Err(LogError::Unavailable { .. }) if index == 0 => 0,
            Err(e) => return Err(storage(&e)),
        };
        if let Some(bounds) = kept {
            self.bounds.set(Some(Bounds {
                last_term: Some(term),
                ..bounds
            }));
        }
        Ok(term)
    }

    /// The entries of `[low, high)` the log gives for `max_bytes`, in the kept reservation,
    /// which the caller keeps again once it has read them.
    fn fetch(&self, low: u64, high: u64, max_bytes: u64) -> Result<Fetched, StorageError> {
        let into = self.fetched.take().unwrap_or_default();
        self.log
            .fetch(self.group, low, high, max_bytes, into)
            .map_err(|e| storage(&e))
    }

    /// Keeps `fetched` for the next read if it held no more than a segment's bytes.
    fn keep(&self, fetched: Fetched) {
        let held = fetched
            .iter()
            .map(|(_, bytes)| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            .fold(0, u64::saturating_add);
        if held <= self.log.config().segment_bytes {
            self.fetched.set(Some(fetched));
        }
    }
}

/// An entry's bytes in the log.
/// Bytes an entry's encoding adds to its data and context: its kind and the context's
/// length.
pub const ENTRY_OVERHEAD: usize = 5;

pub fn encode_entry(e: &Entry) -> Option<Vec<u8>> {
    let kind = e.entry_type.byte();
    let context_len = u32::try_from(e.context.len()).ok()?;
    let mut out = Vec::with_capacity(e.context.len().checked_add(e.data.len())?.checked_add(5)?);
    out.push(kind);
    out.extend_from_slice(&context_len.to_le_bytes());
    out.extend_from_slice(&e.context);
    out.extend_from_slice(&e.data);
    Some(out)
}

/// The entry at `index` of `term` whose bytes are `bytes`.
pub fn decode_entry(index: u64, term: u64, bytes: &[u8]) -> Option<Entry> {
    let (&kind, rest) = bytes.split_first()?;
    let entry_type = EntryType::from_byte(kind)?;
    let (len, rest) = rest.split_at_checked(4)?;
    let len = usize::try_from(u32::from_le_bytes(len.try_into().ok()?)).ok()?;
    let (context, data) = rest.split_at_checked(len)?;
    Some(Entry {
        entry_type,
        term,
        index,
        data: data.to_vec(),
        context: context.to_vec(),
    })
}

pub fn hard_to_proto(h: hyper_log::HardState) -> HardState {
    HardState {
        term: h.term,
        vote: h.vote,
        commit: h.commit,
    }
}

pub fn hard_from_proto(h: &HardState) -> hyper_log::HardState {
    hyper_log::HardState {
        term: h.term,
        vote: h.vote,
        commit: h.commit,
    }
}

/// Whether an entry of `page`, the entries from `next` on, satisfies `predicate`; `next` moves
/// past each entry looked at. A page of none is `Unavailable`: the log holds less than asked.
fn page_has(
    page: &Fetched,
    next: &mut u64,
    predicate: &mut dyn FnMut(&Entry) -> bool,
) -> Result<bool, StorageError> {
    if page.is_empty() {
        return Err(StorageError::Unavailable);
    }
    for (term, bytes) in page.iter() {
        let entry = decode_entry(*next, term, bytes)
            .ok_or(StorageError::Other("an entry does not decode"))?;
        if predicate(&entry) {
            return Ok(true);
        }
        *next = next
            .checked_add(1)
            .ok_or(StorageError::Other("an index past u64"))?;
    }
    Ok(false)
}

fn storage(e: &LogError) -> StorageError {
    match e {
        LogError::Compacted { .. } => StorageError::Compacted,
        LogError::Unavailable { .. } => StorageError::Unavailable,
        _ => StorageError::Other("the log failed"),
    }
}

impl<F: BlockFile + 'static> Storage for LogStore<F> {
    fn initial_state(&self) -> Result<InitialState, StorageError> {
        let view = self.log.view(self.group).map_err(|e| storage(&e))?;
        let (hard_state, proposals) = match view {
            None => (HardState::default(), Vec::new()),
            Some(v) => {
                let proposals = v
                    .proposals
                    .iter()
                    .map(|p| decode_entry(p.index, p.term, &p.bytes))
                    .collect::<Option<Vec<_>>>()
                    .ok_or(StorageError::Other("a proposal does not decode"))?;
                (
                    v.hard_state.map(hard_to_proto).unwrap_or_default(),
                    proposals,
                )
            }
        };
        Ok(InitialState {
            hard_state,
            configuration: self.conf.clone(),
            proposals,
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        if low >= high {
            return Ok(());
        }
        let fetched = self.fetch(low, high, max_bytes)?;
        let decoded = (low..)
            .zip(fetched.iter())
            .try_for_each(|(index, (term, bytes))| {
                into.push(
                    decode_entry(index, term, bytes)
                        .ok_or(StorageError::Other("an entry does not decode"))?,
                );
                Ok(())
            });
        self.keep(fetched);
        decoded
    }

    /// Walks `[low, high)` a page at a time, a page being one log segment's bytes, the most one
    /// read of the log returns; it stops at the first entry `predicate` accepts, so no more is
    /// read than the answer needs and no page outgrows a segment.
    fn any_entry(
        &self,
        low: u64,
        high: u64,
        predicate: &mut dyn FnMut(&Entry) -> bool,
    ) -> Result<bool, StorageError> {
        let page_bytes = self.log.config().segment_bytes;
        let mut next = low;
        while next < high {
            let fetched = self.fetch(next, high, page_bytes)?;
            let found = page_has(&fetched, &mut next, predicate);
            self.keep(fetched);
            if found? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn term(&self, index: u64) -> Result<u64, StorageError> {
        self.term_of(index)
    }

    fn first_index(&self) -> Result<u64, StorageError> {
        self.bounds()?
            .start
            .checked_add(1)
            .ok_or(StorageError::Other("an index past u64"))
    }

    fn last_index(&self) -> Result<u64, StorageError> {
        Ok(self.bounds()?.last)
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> Result<Snapshot, StorageError> {
        match &self.snapshot {
            Some(s) if focal_raft::proto::snapshot_index(s) >= request_index => Ok(s.clone()),
            _ => Err(StorageError::SnapshotTemporarilyUnavailable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper_block::buf::Alignment;
    use hyper_block::sim::SimFile;
    use hyper_log::{Config, Entries, Update, Waits};

    fn log() -> Arc<Log<SimFile>> {
        let file = SimFile::new(
            Alignment::new(4096).unwrap(),
            Alignment::new(512).unwrap(),
            1,
        )
        .unwrap();
        let config = Config {
            segment_bytes: 16 * 4096,
            max_segments: 8,
            max_groups: 4,
            group_entries: 1 << 10,
            group_bytes: 1 << 20,
            group_cache: 1 << 16,
            queue_submissions: 16,
            waits: Waits::Never,
        };
        Arc::new(Log::create(file, config, 1).unwrap())
    }

    fn entries(first: u64, terms: &[u64]) -> Update {
        Update {
            entries: Some(Entries {
                first,
                entries: terms
                    .iter()
                    .map(|&term| hyper_log::Entry {
                        term,
                        bytes: vec![0; 5],
                    })
                    .collect(),
            }),
            ..Update::default()
        }
    }

    /// The store keeps the group's bounds and last term between the group's writes, and asks
    /// the log while one is out. A write the store is not told of, as no write of the member's
    /// is, shows what it kept.
    #[test]
    fn bounds_are_kept_between_the_groups_writes_and_asked_while_one_is_out() {
        let log = log();
        let store = LogStore::new(Arc::clone(&log), 7, ConfState::default());
        assert_eq!(
            (store.first_index().unwrap(), store.last_index().unwrap()),
            (1, 0)
        );
        assert_eq!(store.term(0).unwrap(), 0);
        log.write(7, entries(1, &[1, 1])).unwrap();
        assert_eq!(store.last_index().unwrap(), 0, "kept from before the write");
        store.writing();
        log.write(7, entries(3, &[2])).unwrap();
        assert_eq!(
            (store.last_index().unwrap(), store.term(3).unwrap()),
            (3, 2)
        );
        assert_eq!(store.last_index().unwrap(), 3, "asked while a write is out");
        store.written();
        assert_eq!(
            (store.last_index().unwrap(), store.term(3).unwrap()),
            (3, 2)
        );
        assert_eq!(store.term(1).unwrap(), 1);
        store.writing();
        log.write(7, entries(3, &[3, 3])).unwrap();
        store.written();
        assert_eq!(
            (store.last_index().unwrap(), store.term(4).unwrap()),
            (4, 3)
        );
        assert_eq!(store.term(3).unwrap(), 3);
    }

    #[test]
    fn entries_round_trip_through_their_log_bytes() {
        let e = Entry {
            entry_type: EntryType::EntryConfChangeV2,
            term: 3,
            index: 9,
            data: b"data".to_vec(),
            context: b"ctx".to_vec(),
        };
        let bytes = encode_entry(&e).unwrap();
        assert_eq!(decode_entry(9, 3, &bytes), Some(e));
        assert_eq!(decode_entry(9, 3, &[7, 0, 0, 0, 0]), None);
        assert_eq!(decode_entry(9, 3, &[0, 9, 0, 0, 0]), None);
    }
}
