//! The log's bytes on disk (docs/design/raft-log.md §2): segment headers, frames, records.
//!
//! Numbers are little-endian. A segment's first block is its header; frames follow it,
//! each starting on a block boundary and padded to the block. A frame's CRC-32C covers its
//! header and payload. Each entry and proposal also carries a CRC-32C of its group, index,
//! term and bytes, so one entry read back alone is verified, and verified as the one asked
//! for.

use mantle_codec::{Reader, Writer};

pub const SEGMENT_MAGIC: [u8; 4] = *b"MNLS";
pub const FRAME_MAGIC: [u8; 4] = *b"MNLF";
pub const FORMAT: u8 = 1;

/// Bytes of a segment header before its padding.
pub const SEGMENT_HEADER_LEN: usize = 52;
/// Bytes of a frame header; the payload follows.
pub const FRAME_HEADER_LEN: usize = 68;
pub const FRAME_HEADER_BYTES: u64 = 68;
/// Bytes of an entry before its payload: term, length, CRC.
pub const ENTRY_HEADER_LEN: usize = 16;
pub const ENTRY_HEADER_BYTES: u64 = 16;

const ENTRIES: u8 = 1;
const RELOCATED: u8 = 2;
const HARD_STATE: u8 = 3;
const START: u8 = 4;
const PROPOSAL: u8 = 5;
const REMOVED: u8 = 6;

/// A segment's header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub log: u128,
    pub incarnation: u64,
    /// Random, drawn when the segment opens, and carried by each of its frames. An opening
    /// whose header never became durable may leave a frame behind with an incarnation a
    /// later opening takes again; the nonce keeps that frame from ever reading as the later
    /// segment's.
    pub nonce: u64,
    pub segment_bytes: u64,
}

impl SegmentHeader {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(SEGMENT_HEADER_LEN);
        w.bytes(&SEGMENT_MAGIC);
        w.u8(FORMAT);
        w.zeros(3);
        w.u128(self.log);
        w.u64(self.incarnation);
        w.u64(self.nonce);
        w.u64(self.segment_bytes);
        let crc = mantle_crc::crc32c(w.as_slice());
        w.u32(crc);
        w.into_vec()
    }

    /// The header at the start of `block`, if it is one and its checksum holds.
    pub fn decode(block: &[u8]) -> Option<Self> {
        let bytes = block.get(..SEGMENT_HEADER_LEN)?;
        let (body, crc) = bytes.split_at(SEGMENT_HEADER_LEN.checked_sub(4)?);
        if mantle_crc::crc32c(body) != u32::from_le_bytes(crc.try_into().ok()?) {
            return None;
        }
        let mut r = Reader::new(body);
        if r.take(4)? != SEGMENT_MAGIC || r.u8()? != FORMAT {
            return None;
        }
        r.take(3)?;
        Some(Self {
            log: r.u128()?,
            incarnation: r.u64()?,
            nonce: r.u64()?,
            segment_bytes: r.u64()?,
        })
    }
}

/// A frame's header: one group-commit batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub log: u128,
    /// The incarnation and nonce of the segment the frame is in.
    pub incarnation: u64,
    pub nonce: u64,
    /// Consecutive over the log's life: one a batch.
    pub sequence: u64,
    /// The incarnation of the oldest live segment when the frame was written.
    pub tail: u64,
    pub payload_len: u32,
    pub records: u32,
    pub crc: u32,
}

impl FrameHeader {
    /// The frame of `payload`, its header's CRC computed over both.
    pub fn frame(
        log: u128,
        incarnation: u64,
        nonce: u64,
        sequence: u64,
        tail: u64,
        records: u32,
        payload: &[u8],
    ) -> Option<Vec<u8>> {
        let payload_len = u32::try_from(payload.len()).ok()?;
        let mut w = Writer::with_capacity(FRAME_HEADER_LEN.checked_add(payload.len())?);
        w.bytes(&FRAME_MAGIC);
        w.u8(FORMAT);
        w.zeros(3);
        w.u128(log);
        w.u64(incarnation);
        w.u64(nonce);
        w.u64(sequence);
        w.u64(tail);
        w.u32(payload_len);
        w.u32(records);
        let mut crc = mantle_crc::Crc32c::new();
        crc.update(w.as_slice());
        crc.update(payload);
        w.u32(crc.finish());
        w.bytes(payload);
        Some(w.into_vec())
    }

    /// The header at the start of `block`, if it has the frame magic and format. Its CRC is
    /// checked against the payload by [`FrameHeader::verifies`].
    pub fn decode(block: &[u8]) -> Option<Self> {
        let mut r = Reader::new(block.get(..FRAME_HEADER_LEN)?);
        if r.take(4)? != FRAME_MAGIC || r.u8()? != FORMAT {
            return None;
        }
        r.take(3)?;
        Some(Self {
            log: r.u128()?,
            incarnation: r.u64()?,
            nonce: r.u64()?,
            sequence: r.u64()?,
            tail: r.u64()?,
            payload_len: r.u32()?,
            records: r.u32()?,
            crc: r.u32()?,
        })
    }

    /// Bytes of the frame, header and payload, before padding.
    pub fn frame_len(&self) -> Option<usize> {
        FRAME_HEADER_LEN.checked_add(usize::try_from(self.payload_len).ok()?)
    }

    /// Whether the CRC holds over `frame`, the header and payload as read.
    pub fn verifies(&self, frame: &[u8]) -> bool {
        let Some(len) = self.frame_len() else {
            return false;
        };
        let (Some(head), Some(payload)) = (
            frame.get(..FRAME_HEADER_LEN.saturating_sub(4)),
            frame.get(FRAME_HEADER_LEN..len),
        ) else {
            return false;
        };
        let mut crc = mantle_crc::Crc32c::new();
        crc.update(head);
        crc.update(payload);
        crc.finish() == self.crc
    }
}

/// A Raft hard state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
}

/// Where a group's log starts: after `index`, whose term is `term`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Start {
    pub index: u64,
    pub term: u64,
}

/// A record, borrowing its bytes, as the writer lays it into a frame.
#[derive(Debug, Clone, Copy)]
pub enum Record<'a> {
    /// Entries from `first` on, replacing any the group holds at or after it.
    Entries {
        group: u128,
        first: u64,
        entries: &'a [(u64, &'a [u8])],
    },
    /// Copies of entries the group holds, replacing none past them.
    Relocated {
        group: u128,
        first: u64,
        entries: &'a [(u64, &'a [u8])],
    },
    HardState {
        group: u128,
        state: HardState,
    },
    Start {
        group: u128,
        start: Start,
    },
    Proposal {
        group: u128,
        index: u64,
        term: u64,
        bytes: &'a [u8],
    },
    Removed {
        group: u128,
    },
}

/// Where each piece of a record went in the payload: its entries' or proposal's encoded
/// starts, or the record's own start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placed {
    /// Each entry's index and the payload offset of its encoded start.
    Entries(Vec<(u64, usize)>),
    /// The payload offset of the record.
    Record(usize),
}

/// The CRC-32C an entry or proposal carries: of its group, index, term and bytes, so a
/// read verifies both the bytes and that they are the ones asked for.
pub fn entry_crc(group: u128, index: u64, term: u64, bytes: &[u8]) -> u32 {
    let mut crc = mantle_crc::Crc32c::new();
    crc.update(&group.to_le_bytes());
    crc.update(&index.to_le_bytes());
    crc.update(&term.to_le_bytes());
    crc.update(bytes);
    crc.finish()
}

/// Appends `record` to `payload` and says where its pieces went. `None` when a length does
/// not fit its field.
pub fn put(payload: &mut Writer, record: &Record<'_>) -> Option<Placed> {
    let at = payload.len();
    match *record {
        Record::Entries {
            group,
            first,
            entries,
        }
        | Record::Relocated {
            group,
            first,
            entries,
        } => {
            let kind = if matches!(record, Record::Entries { .. }) {
                ENTRIES
            } else {
                RELOCATED
            };
            payload.u8(kind);
            payload.u128(group);
            payload.u64(first);
            payload.u32(u32::try_from(entries.len()).ok()?);
            let mut placed = Vec::with_capacity(entries.len());
            for (i, &(term, bytes)) in (0u64..).zip(entries) {
                let index = first.checked_add(i)?;
                placed.push((index, payload.len()));
                payload.u64(term);
                payload.u32(u32::try_from(bytes.len()).ok()?);
                payload.u32(entry_crc(group, index, term, bytes));
                payload.bytes(bytes);
            }
            Some(Placed::Entries(placed))
        }
        Record::HardState { group, state } => {
            payload.u8(HARD_STATE);
            payload.u128(group);
            payload.u64(state.term);
            payload.u64(state.vote);
            payload.u64(state.commit);
            Some(Placed::Record(at))
        }
        Record::Start { group, start } => {
            payload.u8(START);
            payload.u128(group);
            payload.u64(start.index);
            payload.u64(start.term);
            Some(Placed::Record(at))
        }
        Record::Proposal {
            group,
            index,
            term,
            bytes,
        } => {
            payload.u8(PROPOSAL);
            payload.u128(group);
            let placed = payload.len();
            payload.u64(index);
            payload.u64(term);
            payload.u32(u32::try_from(bytes.len()).ok()?);
            payload.u32(entry_crc(group, index, term, bytes));
            payload.bytes(bytes);
            Some(Placed::Record(placed))
        }
        Record::Removed { group } => {
            payload.u8(REMOVED);
            payload.u128(group);
            Some(Placed::Record(at))
        }
    }
}

/// Bytes `record` takes in a payload.
pub fn encoded_len(record: &Record<'_>) -> Option<usize> {
    let body = match *record {
        Record::Entries { entries, .. } | Record::Relocated { entries, .. } => {
            entries.iter().try_fold(12usize, |sum, (_, bytes)| {
                sum.checked_add(ENTRY_HEADER_LEN)?.checked_add(bytes.len())
            })?
        }
        Record::HardState { .. } => 24,
        Record::Start { .. } => 16,
        Record::Proposal { bytes, .. } => 24usize.checked_add(bytes.len())?,
        Record::Removed { .. } => 0,
    };
    body.checked_add(17)
}

/// An entry or proposal as decoded: where it starts in the payload, and its fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub at: usize,
    pub index: u64,
    pub term: u64,
    pub crc: u32,
    pub bytes: Vec<u8>,
}

/// A record as decoded from a verified frame's payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owned {
    Entries {
        group: u128,
        first: u64,
        entries: Vec<Decoded>,
    },
    Relocated {
        group: u128,
        first: u64,
        entries: Vec<Decoded>,
    },
    HardState {
        at: usize,
        group: u128,
        state: HardState,
    },
    Start {
        at: usize,
        group: u128,
        start: Start,
    },
    Proposal {
        group: u128,
        proposal: Decoded,
    },
    Removed {
        group: u128,
    },
}

/// The records of a verified frame's payload, in order; `None` if any does not decode or
/// an entry's CRC fails.
pub fn records(payload: &[u8], count: u32) -> Option<Vec<Owned>> {
    let mut r = Reader::new(payload);
    let mut out = Vec::new();
    for _ in 0..count {
        let at = r.position();
        let kind = r.u8()?;
        let group = r.u128()?;
        let record = match kind {
            ENTRIES | RELOCATED => {
                let first = r.u64()?;
                let n = r.u32()?;
                // Each entry takes its header at least: a count the payload cannot hold is
                // corrupt before anything is allocated for it.
                if usize::try_from(n).ok()? > r.remaining() / ENTRY_HEADER_LEN {
                    return None;
                }
                let mut entries = Vec::with_capacity(usize::try_from(n).ok()?);
                for i in 0..u64::from(n) {
                    let index = first.checked_add(i)?;
                    let at = r.position();
                    let term = r.u64()?;
                    let (crc, bytes) = take_bytes(&mut r)?;
                    if entry_crc(group, index, term, bytes) != crc {
                        return None;
                    }
                    entries.push(Decoded {
                        at,
                        index,
                        term,
                        crc,
                        bytes: bytes.to_vec(),
                    });
                }
                if kind == ENTRIES {
                    Owned::Entries {
                        group,
                        first,
                        entries,
                    }
                } else {
                    Owned::Relocated {
                        group,
                        first,
                        entries,
                    }
                }
            }
            HARD_STATE => Owned::HardState {
                at,
                group,
                state: HardState {
                    term: r.u64()?,
                    vote: r.u64()?,
                    commit: r.u64()?,
                },
            },
            START => Owned::Start {
                at,
                group,
                start: Start {
                    index: r.u64()?,
                    term: r.u64()?,
                },
            },
            PROPOSAL => {
                let at = r.position();
                let index = r.u64()?;
                let term = r.u64()?;
                let (crc, bytes) = take_bytes(&mut r)?;
                if entry_crc(group, index, term, bytes) != crc {
                    return None;
                }
                Owned::Proposal {
                    group,
                    proposal: Decoded {
                        at,
                        index,
                        term,
                        crc,
                        bytes: bytes.to_vec(),
                    },
                }
            }
            REMOVED => Owned::Removed { group },
            _ => return None,
        };
        out.push(record);
    }
    if r.remaining() != 0 {
        return None;
    }
    Some(out)
}

/// An entry's length, CRC and bytes.
fn take_bytes<'a>(r: &mut Reader<'a>) -> Option<(u32, &'a [u8])> {
    let len = usize::try_from(r.u32()?).ok()?;
    let crc = r.u32()?;
    Some((crc, r.take(len)?))
}

/// An entry read back alone from `bytes`, which start at its encoded start: its term and
/// payload, if its CRC holds for `group` and `index`.
pub fn entry_at(bytes: &[u8], group: u128, index: u64) -> Option<(u64, Vec<u8>)> {
    let mut r = Reader::new(bytes);
    let term = r.u64()?;
    let (crc, payload) = take_bytes(&mut r)?;
    (entry_crc(group, index, term, payload) == crc).then(|| (term, payload.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn headers_round_trip_and_refuse_damage() {
        let h = SegmentHeader {
            log: 7,
            incarnation: 9,
            nonce: 0xfeed,
            segment_bytes: 1 << 26,
        };
        let bytes = h.encode();
        assert_eq!(bytes.len(), SEGMENT_HEADER_LEN);
        assert_eq!(SegmentHeader::decode(&bytes), Some(h));
        for i in 0..bytes.len() * 8 {
            let mut bad = bytes.clone();
            bad[i / 8] ^= 1 << (i % 8);
            assert_eq!(SegmentHeader::decode(&bad), None);
        }
        let frame = FrameHeader::frame(7, 9, 0xfeed, 11, 3, 0, b"payload").unwrap();
        let header = FrameHeader::decode(&frame).unwrap();
        assert_eq!(
            (header.sequence, header.tail, header.payload_len),
            (11, 3, 7)
        );
        assert!(header.verifies(&frame));
        let mut bad = frame.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(!header.verifies(&bad));
    }

    fn record() -> impl Strategy<Value = (u8, u128, u64, Vec<(u64, Vec<u8>)>)> {
        (
            0u8..6,
            any::<u128>(),
            0u64..u64::MAX / 2,
            prop::collection::vec(
                (any::<u64>(), prop::collection::vec(any::<u8>(), 0..40)),
                0..5,
            ),
        )
    }

    proptest! {
        /// Records decode to what was put, and every entry is found where `put` placed it.
        #[test]
        fn records_round_trip(input in prop::collection::vec(record(), 0..6)) {
            let mut payload = Writer::default();
            let mut placed = Vec::new();
            let borrowed: Vec<Vec<(u64, &[u8])>> = input
                .iter()
                .map(|(_, _, _, e)| e.iter().map(|(t, b)| (*t, b.as_slice())).collect())
                .collect();
            for ((kind, group, first, entries), refs) in input.iter().zip(&borrowed) {
                let (group, first) = (*group, *first);
                let bytes = entries.first().map_or(&[][..], |e| e.1.as_slice());
                let record = match kind {
                    0 => Record::Entries { group, first, entries: refs },
                    1 => Record::Relocated { group, first, entries: refs },
                    2 => Record::HardState { group, state: HardState { term: first, vote: 1, commit: 2 } },
                    3 => Record::Start { group, start: Start { index: first, term: 4 } },
                    4 => Record::Proposal { group, index: first, term: 5, bytes },
                    _ => Record::Removed { group },
                };
                let before = payload.len();
                placed.push(put(&mut payload, &record).unwrap());
                prop_assert_eq!(payload.len() - before, encoded_len(&record).unwrap());
            }
            let bytes = payload.into_vec();
            let decoded = records(&bytes, u32::try_from(input.len()).unwrap()).unwrap();
            prop_assert_eq!(decoded.len(), input.len());
            for (record, place) in decoded.iter().zip(&placed) {
                let (group, entries) = match record {
                    Owned::Entries { group, entries, .. } | Owned::Relocated { group, entries, .. } => {
                        (*group, entries)
                    }
                    _ => continue,
                };
                let Placed::Entries(at) = place else {
                    panic!("entries placed as a record")
                };
                for (entry, (index, offset)) in entries.iter().zip(at.iter().copied()) {
                    prop_assert_eq!(entry.at, offset);
                    let alone = entry_at(&bytes[offset..], group, index);
                    prop_assert_eq!(alone, Some((entry.term, entry.bytes.clone())));
                    prop_assert_eq!(entry_at(&bytes[offset..], group, index + 1), None);
                }
            }
        }
    }
}
