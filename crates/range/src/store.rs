//! The group's view of the device's log, as focal-raft reads what is durable
//! (docs/design/replica.md §3; 07 §1.2). The log keeps each entry's index and term; its bytes
//! are the entry's type, context and data.

use std::sync::Arc;

use focal_raft::proto::{ConfState, Entry, EntryType, HardState, Snapshot};
use focal_raft::{InitialState, Storage, StorageError};
use mantle_disk::block::BlockFile;
use mantle_log::{Log, LogError};

pub struct LogStore<F: BlockFile + 'static> {
    pub log: Arc<Log<F>>,
    pub group: u128,
    /// The configuration of the state the engine opened at.
    pub conf: ConfState,
    /// The snapshot the replica last prepared, at an index at or past the log's start, for
    /// a member that needs entries the log no longer holds.
    pub snapshot: Option<Snapshot>,
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

pub fn hard_to_proto(h: mantle_log::HardState) -> HardState {
    HardState {
        term: h.term,
        vote: h.vote,
        commit: h.commit,
    }
}

pub fn hard_from_proto(h: &HardState) -> mantle_log::HardState {
    mantle_log::HardState {
        term: h.term,
        vote: h.vote,
        commit: h.commit,
    }
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
        let entries = self
            .log
            .entries(self.group, low, high, max_bytes)
            .map_err(|e| storage(&e))?;
        for (index, e) in (low..).zip(entries) {
            into.push(
                decode_entry(index, e.term, &e.bytes)
                    .ok_or(StorageError::Other("an entry does not decode"))?,
            );
        }
        Ok(())
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
            let entries = self
                .log
                .entries(self.group, next, high, page_bytes)
                .map_err(|e| storage(&e))?;
            if entries.is_empty() {
                return Err(StorageError::Unavailable);
            }
            for e in entries {
                let entry = decode_entry(next, e.term, &e.bytes)
                    .ok_or(StorageError::Other("an entry does not decode"))?;
                if predicate(&entry) {
                    return Ok(true);
                }
                next = next
                    .checked_add(1)
                    .ok_or(StorageError::Other("an index past u64"))?;
            }
        }
        Ok(false)
    }

    fn term(&self, index: u64) -> Result<u64, StorageError> {
        match self.log.term(self.group, index) {
            Ok(term) => Ok(term),
            // A group the log holds nothing of starts after index 0, of term 0.
            Err(LogError::Unavailable { .. }) if index == 0 => Ok(0),
            Err(e) => Err(storage(&e)),
        }
    }

    fn first_index(&self) -> Result<u64, StorageError> {
        let view = self.log.view(self.group).map_err(|e| storage(&e))?;
        let start = view.map_or(0, |v| v.start.index);
        start
            .checked_add(1)
            .ok_or(StorageError::Other("an index past u64"))
    }

    fn last_index(&self) -> Result<u64, StorageError> {
        let view = self.log.view(self.group).map_err(|e| storage(&e))?;
        Ok(view.map_or(0, |v| v.last))
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
