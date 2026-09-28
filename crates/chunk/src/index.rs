//! The in-memory index from chunk key to the fragments that hold its bytes, and the table of
//! segment states. Haystack keeps its needle index in memory so a read costs one disk
//! operation (Beaver et al., OSDI 2010, §3.4); this index does the same, and its size is
//! bounded by the volume's chunk budget.

use std::collections::HashMap;

use crate::frame::{PutRecord, SegmentState};
use crate::key::ChunkKey;
use crate::record::FLAG_FINAL;

/// Where one contiguous piece of a chunk's bytes lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fragment {
    pub segment: u32,
    pub incarnation: u32,
    /// Byte offset of the data record within its segment.
    pub offset: u32,
    /// Bytes of the whole data record.
    pub record_len: u32,
    pub chunk_offset: u64,
    pub payload_len: u32,
    pub payload_crc: u32,
    pub sequence: u64,
}

impl Fragment {
    pub fn end(&self) -> u64 {
        self.chunk_offset
            .saturating_add(u64::from(self.payload_len))
    }

    pub fn from_put(p: &PutRecord) -> Self {
        Self {
            segment: p.segment,
            incarnation: p.incarnation,
            offset: p.offset,
            record_len: p.record_len,
            chunk_offset: p.chunk_offset,
            payload_len: p.payload_len,
            payload_crc: p.payload_crc,
            sequence: p.sequence,
        }
    }
}

/// A chunk: its fragments in chunk order, contiguous from offset zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub fragments: Vec<Fragment>,
    pub sealed: bool,
    /// When the last fragment was written.
    pub time_ns: u64,
}

impl Entry {
    /// The chunk's length so far.
    pub fn len(&self) -> u64 {
        self.fragments.last().map_or(0, Fragment::end)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The fragment starting at `chunk_offset`, if one does.
    pub fn fragment_at(&self, chunk_offset: u64) -> Option<&Fragment> {
        self.fragments
            .binary_search_by_key(&chunk_offset, |f| f.chunk_offset)
            .ok()
            .and_then(|i| self.fragments.get(i))
    }

    /// The fragments overlapping `[start, end)`.
    pub fn covering(&self, start: u64, end: u64) -> impl Iterator<Item = &Fragment> {
        self.fragments
            .iter()
            .filter(move |f| f.chunk_offset < end && f.end() > start)
    }
}

/// Why an index update was refused. The writer validates every request before it writes,
/// so these arise only from a log whose frames contradict each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inconsistent {
    /// A fragment that does not start where the chunk currently ends.
    NotContiguous { expected: u64, got: u64 },
    /// A fragment for a chunk already sealed.
    Sealed,
    /// More fragments than one chunk may have.
    TooManyFragments,
}

#[derive(Debug, Default)]
pub struct Index {
    map: HashMap<ChunkKey, Entry>,
}

impl Index {
    pub fn get(&self, key: &ChunkKey) -> Option<&Entry> {
        self.map.get(key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ChunkKey, &Entry)> {
        self.map.iter()
    }

    /// Adds a fragment written for `key`. A fragment identical to one already present (the
    /// same record replayed twice) changes nothing.
    pub fn insert(
        &mut self,
        key: ChunkKey,
        put: &PutRecord,
        max_fragments: usize,
    ) -> Result<(), Inconsistent> {
        let fragment = Fragment::from_put(put);
        let sealed = put.flags & FLAG_FINAL != 0;
        let entry = self.map.entry(key).or_insert_with(|| Entry {
            fragments: Vec::with_capacity(1),
            sealed: false,
            time_ns: put.time_ns,
        });
        if let Some(existing) = entry.fragment_at(fragment.chunk_offset)
            && existing.payload_len == fragment.payload_len
            && existing.payload_crc == fragment.payload_crc
        {
            // The same bytes at a newer location: a relocated copy supersedes the old one.
            if fragment.sequence >= existing.sequence
                && let Ok(i) = entry
                    .fragments
                    .binary_search_by_key(&fragment.chunk_offset, |f| f.chunk_offset)
                && let Some(slot) = entry.fragments.get_mut(i)
            {
                *slot = fragment;
            }
            entry.sealed |= sealed;
            return Ok(());
        }
        if entry.sealed {
            return Err(Inconsistent::Sealed);
        }
        if entry.len() != fragment.chunk_offset {
            return Err(Inconsistent::NotContiguous {
                expected: entry.len(),
                got: fragment.chunk_offset,
            });
        }
        if entry.fragments.len() >= max_fragments {
            return Err(Inconsistent::TooManyFragments);
        }
        // A fragment may start where an empty one did only if that one ended the chunk,
        // which the sealed check above already refused.
        if entry.fragments.last().is_some_and(|f| f.payload_len == 0) {
            return Err(Inconsistent::NotContiguous {
                expected: entry.len(),
                got: fragment.chunk_offset,
            });
        }
        entry.fragments.push(fragment);
        entry.sealed = sealed;
        entry.time_ns = put.time_ns;
        Ok(())
    }

    pub fn remove(&mut self, key: &ChunkKey) -> Option<Entry> {
        self.map.remove(key)
    }

    /// Removes `key`'s last fragment if it is the one at `chunk_offset` written with
    /// `sequence`, unsealing the chunk; removes the chunk if nothing is left. Returns the
    /// fragment removed.
    pub fn pop_fragment(
        &mut self,
        key: &ChunkKey,
        chunk_offset: u64,
        sequence: u64,
    ) -> Option<Fragment> {
        let entry = self.map.get_mut(key)?;
        let last = entry.fragments.last()?;
        if last.chunk_offset != chunk_offset || last.sequence != sequence {
            return None;
        }
        let popped = entry.fragments.pop();
        entry.sealed = false;
        if entry.fragments.is_empty() {
            self.map.remove(key);
        }
        popped
    }
}

/// One segment's state as the writer tracks it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentInfo {
    pub state: SegmentState,
    pub incarnation: u32,
    /// Bytes of the segment written in this incarnation, a multiple of the block size.
    pub write_pos: u32,
    /// Bytes of records in this segment that some chunk still references.
    pub live: u64,
    /// When the youngest live record was written, for cleaning by age.
    pub youngest_ns: u64,
}

impl SegmentInfo {
    pub const FREE: Self = Self {
        state: SegmentState::Free,
        incarnation: 0,
        write_pos: 0,
        live: 0,
        youngest_ns: 0,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(offset: u64, len: u32, seq: u64, flags: u8) -> PutRecord {
        PutRecord {
            key: ChunkKey {
                block: 1,
                epoch: 0,
                index: 0,
            },
            segment: 0,
            incarnation: 1,
            offset: 4096,
            record_len: len + 100,
            chunk_offset: offset,
            payload_len: len,
            payload_crc: offset as u32 ^ len,
            sequence: seq,
            time_ns: seq,
            flags,
        }
    }

    #[test]
    fn appends_must_be_contiguous_and_stop_at_seal() {
        let key = ChunkKey {
            block: 1,
            epoch: 0,
            index: 0,
        };
        let mut index = Index::default();
        index.insert(key, &put(0, 10, 1, 0), 8).unwrap();
        index.insert(key, &put(10, 5, 2, 0), 8).unwrap();
        assert_eq!(
            index.insert(key, &put(20, 5, 3, 0), 8),
            Err(Inconsistent::NotContiguous {
                expected: 15,
                got: 20
            })
        );
        index.insert(key, &put(15, 5, 3, FLAG_FINAL), 8).unwrap();
        assert_eq!(
            index.insert(key, &put(20, 1, 4, 0), 8),
            Err(Inconsistent::Sealed)
        );
        let e = index.get(&key).unwrap();
        assert_eq!(e.len(), 20);
        assert!(e.sealed);
        assert_eq!(e.covering(12, 16).count(), 2);
    }

    #[test]
    fn replaying_a_fragment_twice_changes_nothing() {
        let key = ChunkKey {
            block: 1,
            epoch: 0,
            index: 0,
        };
        let mut index = Index::default();
        index.insert(key, &put(0, 10, 1, FLAG_FINAL), 8).unwrap();
        index.insert(key, &put(0, 10, 1, FLAG_FINAL), 8).unwrap();
        assert_eq!(index.get(&key).unwrap().fragments.len(), 1);
    }

    #[test]
    fn popping_the_last_fragment_unseals_and_empties() {
        let key = ChunkKey {
            block: 1,
            epoch: 0,
            index: 0,
        };
        let mut index = Index::default();
        index.insert(key, &put(0, 10, 1, 0), 8).unwrap();
        index.insert(key, &put(10, 5, 2, FLAG_FINAL), 8).unwrap();
        assert!(
            index.pop_fragment(&key, 0, 1).is_none(),
            "only the last fragment pops"
        );
        assert!(index.pop_fragment(&key, 10, 2).is_some());
        assert!(!index.get(&key).unwrap().sealed);
        assert!(index.pop_fragment(&key, 0, 1).is_some());
        assert!(index.get(&key).is_none());
    }

    #[test]
    fn a_chunk_has_a_bounded_number_of_fragments() {
        let key = ChunkKey {
            block: 1,
            epoch: 0,
            index: 0,
        };
        let mut index = Index::default();
        index.insert(key, &put(0, 1, 1, 0), 2).unwrap();
        index.insert(key, &put(1, 1, 2, 0), 2).unwrap();
        assert_eq!(
            index.insert(key, &put(2, 1, 3, 0), 2),
            Err(Inconsistent::TooManyFragments)
        );
    }
}
