//! Creating a log and opening one (docs/design/raft-log.md §6).

use std::collections::HashMap;

use mantle_disk::block::BlockFile;
use mantle_disk::buf::{AlignedBuf, Alignment};

use crate::format::{self, FRAME_HEADER_BYTES, FrameHeader, Owned, SegmentHeader};
use crate::state::{self, Live, Place, Replayed, Slot};
use crate::{Config, Head, LogError, Recovery, Segments, State};

/// What a frame's position holds.
pub(crate) enum Found {
    /// A verified frame: its header, its bytes and its padded length.
    Frame(FrameHeader, AlignedBuf, u64),
    /// No frame of this segment: its frames end before here.
    End,
    /// A frame of this segment whose checksum fails, or that runs past the segment or file.
    Invalid,
}

/// Which segment a frame must name to be one of its frames.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Segment {
    pub log: u128,
    pub incarnation: u64,
    pub nonce: u64,
}

/// The frame of `segment` at `offset`, reading no further than `end`, the segment's end.
pub(crate) fn frame_at<F: BlockFile>(
    file: &F,
    segment: Segment,
    align: Alignment,
    offset: u64,
    end: u64,
) -> Result<Found, LogError> {
    let block = u64::try_from(align.get()).map_err(|_| LogError::Config("block"))?;
    let len = file.len()?;
    let Some(first_end) = offset.checked_add(block) else {
        return Ok(Found::End);
    };
    if first_end > end || first_end > len {
        return Ok(Found::End);
    }
    let mut head = AlignedBuf::zeroed(align.get(), align).map_err(|e| LogError::Disk(e.into()))?;
    head.set_len(align.get())
        .map_err(|e| LogError::Disk(e.into()))?;
    file.read_exact_at(head.as_mut_slice(), offset)?;
    let Some(header) = FrameHeader::decode(head.as_slice()) else {
        return Ok(Found::End);
    };
    if header.log != segment.log
        || header.incarnation != segment.incarnation
        || header.nonce != segment.nonce
    {
        return Ok(Found::End);
    }
    let Some(frame_len) = header.frame_len().and_then(|l| u64::try_from(l).ok()) else {
        return Ok(Found::Invalid);
    };
    let Some(padded) = align.up_u64(frame_len) else {
        return Ok(Found::Invalid);
    };
    let fits = offset
        .checked_add(padded)
        .is_some_and(|e| e <= end && e <= len);
    if !fits {
        return Ok(Found::Invalid);
    }
    let size = usize::try_from(padded).map_err(|_| LogError::Damaged("a frame past usize"))?;
    let mut bytes = AlignedBuf::zeroed(size, align).map_err(|e| LogError::Disk(e.into()))?;
    bytes.set_len(size).map_err(|e| LogError::Disk(e.into()))?;
    file.read_exact_at(bytes.as_mut_slice(), offset)?;
    if !header.verifies(bytes.as_slice()) {
        return Ok(Found::Invalid);
    }
    Ok(Found::Frame(header, bytes, padded))
}

/// Whether `segment` holds a valid frame after the block at `from`, up to `end`, with a
/// sequence past `after`.
fn later_frame<F: BlockFile>(
    file: &F,
    segment: Segment,
    align: Alignment,
    from: u64,
    end: u64,
    after: Option<u64>,
) -> Result<bool, LogError> {
    let block = block_of(align)?;
    let mut offset = from;
    loop {
        offset = match offset.checked_add(block) {
            Some(next) if next < end => next,
            _ => return Ok(false),
        };
        if let Found::Frame(header, ..) = frame_at(file, segment, align, offset, end)?
            && after.is_none_or(|seq| header.sequence > seq)
        {
            return Ok(true);
        }
    }
}

fn block_of(align: Alignment) -> Result<u64, LogError> {
    u64::try_from(align.get()).map_err(|_| LogError::Config("block"))
}

pub(crate) fn check(config: &Config, align: Alignment) -> Result<(), LogError> {
    let block = block_of(align)?;
    let blocks = config.segment_bytes.checked_div(block).unwrap_or(0);
    if !align.is_aligned_u64(config.segment_bytes) || blocks < 4 {
        return Err(LogError::Config(
            "a segment is a multiple of the alignment, four blocks at least",
        ));
    }
    if u64::from(config.max_segments) < 3 {
        return Err(LogError::Config(
            "three segments at least: the head, its successor and one to reclaim into",
        ));
    }
    if config.max_groups == 0 || config.queue_submissions == 0 {
        return Err(LogError::Config(
            "room for one group and one submission at least",
        ));
    }
    Ok(())
}

/// Writes segment 0, incarnation 1: its header and an empty first frame.
pub(crate) fn create<F: BlockFile>(file: &F, config: &Config, id: u128) -> Result<State, LogError> {
    let align = file.alignment();
    check(config, align)?;
    if !file.is_empty()? {
        return Err(LogError::Foreign("a new log needs an empty file"));
    }
    let block = align.get();
    let nonce = random_nonce()?;
    let header = SegmentHeader {
        log: id,
        incarnation: 1,
        nonce,
        segment_bytes: config.segment_bytes,
    };
    let frame = FrameHeader::frame(id, 1, nonce, 0, 1, 0, &[]).ok_or(LogError::Config("frame"))?;
    let total = block
        .checked_add(frame.len())
        .ok_or(LogError::Config("frame"))?;
    let mut buf = AlignedBuf::zeroed(total, align).map_err(|e| LogError::Disk(e.into()))?;
    buf.extend_from_slice(&header.encode())
        .map_err(|e| LogError::Disk(e.into()))?;
    buf.extend_zeros(block.saturating_sub(buf.len()))
        .map_err(|e| LogError::Disk(e.into()))?;
    buf.extend_from_slice(&frame)
        .map_err(|e| LogError::Disk(e.into()))?;
    let bytes = buf.padded().map_err(|e| LogError::Disk(e.into()))?;
    let written = u64::try_from(bytes.len()).map_err(|_| LogError::Config("frame"))?;
    file.write_all_at(bytes, 0)?;
    file.sync_data()?;
    Ok(State {
        groups: HashMap::new(),
        live: Live::with_slots(1),
        segments: Segments {
            incarnation: vec![1],
            nonce: vec![nonce],
            live: [0].into(),
            free: Default::default(),
        },
        head: Head {
            slot: 0,
            incarnation: 1,
            nonce,
            offset: written,
        },
        next_sequence: 1,
        next_incarnation: 2,
        durable: 0,
        durable_tail: 1,
    })
}

pub(crate) fn random_nonce() -> Result<u64, LogError> {
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).map_err(|_| LogError::Config("no randomness for a nonce"))?;
    Ok(u64::from_le_bytes(bytes))
}

/// Called with each verified frame of a segment: its offset, header, bytes and padded length.
type Visit<'a> = dyn FnMut(u64, &FrameHeader, &[u8], u64) -> Result<(), LogError> + 'a;

/// The last valid frame: its segment's incarnation, its offset, sequence and tail, and the
/// offset after it.
#[derive(Debug, Clone, Copy)]
struct Last {
    incarnation: u64,
    offset: u64,
    sequence: u64,
    tail: u64,
    after: u64,
}

pub(crate) fn open<F: BlockFile>(
    file: &F,
    config: &Config,
    id: u128,
) -> Result<(State, Recovery), LogError> {
    let align = file.alignment();
    check(config, align)?;
    let block = block_of(align)?;
    let len = file.len()?;
    if len == 0 {
        return Err(LogError::Foreign("the file is empty"));
    }
    let segment = config.segment_bytes;
    let slots = len.div_ceil(segment);
    let slots = u32::try_from(slots)
        .ok()
        .filter(|&s| s <= config.max_segments)
        .ok_or(LogError::Foreign("more segments than the log's quota"))?;
    let start_of = |slot: u32| u64::from(slot).checked_mul(segment);

    // 1. Every segment header.
    let mut incarnation = vec![0u64; usize::try_from(slots).unwrap_or(0)];
    let mut nonce = vec![0u64; usize::try_from(slots).unwrap_or(0)];
    let mut by_incarnation: HashMap<u64, u32> = HashMap::new();
    let mut header_block =
        AlignedBuf::zeroed(align.get(), align).map_err(|e| LogError::Disk(e.into()))?;
    header_block
        .set_len(align.get())
        .map_err(|e| LogError::Disk(e.into()))?;
    for slot in 0..slots {
        let at = start_of(slot).ok_or(LogError::Damaged("an offset past u64"))?;
        if at.checked_add(block).is_none_or(|end| end > len) {
            continue;
        }
        file.read_exact_at(header_block.as_mut_slice(), at)?;
        let Some(h) = SegmentHeader::decode(header_block.as_slice()) else {
            continue;
        };
        if h.log != id || h.segment_bytes != segment || h.incarnation == 0 {
            continue;
        }
        if by_incarnation.insert(h.incarnation, slot).is_some() {
            return Err(LogError::Damaged("two segments share an incarnation"));
        }
        let i = usize::try_from(slot).unwrap_or(usize::MAX);
        if let (Some(e), Some(n)) = (incarnation.get_mut(i), nonce.get_mut(i)) {
            *e = h.incarnation;
            *n = h.nonce;
        }
    }
    let highest = by_incarnation
        .keys()
        .copied()
        .max()
        .ok_or(LogError::Foreign("no segment of this log"))?;

    let segment_of = |slot: u32, inc: u64| Segment {
        log: id,
        incarnation: inc,
        nonce: nonce
            .get(usize::try_from(slot).unwrap_or(usize::MAX))
            .copied()
            .unwrap_or(0),
    };

    // 2. The last valid frame, in the highest segment that holds one.
    let frames_of = |inc: u64, visit: &mut Visit<'_>| -> Result<(u64, bool), LogError> {
        let slot = *by_incarnation
            .get(&inc)
            .ok_or(LogError::Damaged("a live segment is missing"))?;
        let which = segment_of(slot, inc);
        let begin = start_of(slot).ok_or(LogError::Damaged("an offset past u64"))?;
        let end = begin
            .checked_add(segment)
            .ok_or(LogError::Damaged("an offset past u64"))?;
        let mut offset = begin
            .checked_add(block)
            .ok_or(LogError::Damaged("an offset past u64"))?;
        loop {
            match frame_at(file, which, align, offset, end)? {
                Found::Frame(header, bytes, padded) => {
                    visit(offset, &header, bytes.as_slice(), padded)?;
                    offset = offset
                        .checked_add(padded)
                        .ok_or(LogError::Damaged("an offset past u64"))?;
                }
                Found::End => return Ok((offset, false)),
                Found::Invalid => return Ok((offset, true)),
            }
        }
    };
    let mut last: Option<Last> = None;
    let mut inc = highest;
    while last.is_none() {
        let mut found: Option<Last> = None;
        let (stop, invalid) = frames_of(inc, &mut |offset, header, _, padded| {
            found = Some(Last {
                incarnation: inc,
                offset,
                sequence: header.sequence,
                tail: header.tail,
                after: offset.saturating_add(padded),
            });
            Ok(())
        })?;
        if invalid {
            // A frame is written only after the one before it is flushed, so a valid frame
            // past an invalid one proves the invalid one was acknowledged (06 §A3).
            let slot = *by_incarnation
                .get(&inc)
                .ok_or(LogError::Damaged("a live segment is missing"))?;
            let end = start_of(slot)
                .and_then(|s| s.checked_add(segment))
                .ok_or(LogError::Damaged("an offset past u64"))?;
            let after = found.map(|f| f.sequence);
            if later_frame(file, segment_of(slot, inc), align, stop, end, after)? {
                return Err(LogError::Damaged("an acknowledged frame does not verify"));
            }
        }
        last = found;
        if last.is_none() {
            inc = inc
                .checked_sub(1)
                .filter(|i| by_incarnation.contains_key(i))
                .ok_or(LogError::Damaged("no segment holds a valid frame"))?;
        }
    }
    let last = last.ok_or(LogError::Damaged("no segment holds a valid frame"))?;
    if last.tail > last.incarnation {
        return Err(LogError::Damaged(
            "a frame names a tail after its own segment",
        ));
    }

    // 3. Replay the live segments, the tail to the highest, in sequence order.
    let mut replayed: HashMap<u128, Replayed> = HashMap::new();
    let mut expected: Option<u64> = None;
    let mut frames = 0u64;
    for inc in last.tail..=highest {
        let slot = *by_incarnation
            .get(&inc)
            .ok_or(LogError::Damaged("a live segment is missing"))?;
        let (end, invalid) = frames_of(inc, &mut |offset, header, bytes, _| {
            if expected.is_some_and(|e| e != header.sequence) {
                return Err(LogError::Damaged("a frame is missing"));
            }
            expected = header.sequence.checked_add(1);
            frames = frames.saturating_add(1);
            let payload = bytes
                .get(format::FRAME_HEADER_LEN..header.frame_len().unwrap_or(0))
                .ok_or(LogError::Damaged("a frame shorter than its header says"))?;
            let records = format::records(payload, header.records)
                .ok_or(LogError::Damaged("a verified frame does not decode"))?;
            let base = offset
                .checked_add(FRAME_HEADER_BYTES)
                .ok_or(LogError::Damaged("an offset past u64"))?;
            replay(&mut replayed, slot, base, records)
        })?;
        if invalid {
            // Damage unless it lies after the last valid frame: then it is the torn tail.
            let before_last =
                inc < last.incarnation || (inc == last.incarnation && end < last.offset);
            if before_last {
                return Err(LogError::Damaged("an acknowledged frame does not verify"));
            }
        }
    }
    if expected != last.sequence.checked_add(1) {
        return Err(LogError::Damaged("a frame is missing"));
    }

    // 4. The groups as they run; those with a record missing recover from their peers.
    let mut groups = HashMap::new();
    let mut damaged = Vec::new();
    for (group, r) in replayed {
        match r.finish() {
            Some(g) => {
                groups.insert(group, g);
            }
            None => damaged.push(group),
        }
    }
    damaged.sort_unstable();
    let mut live = Live::with_slots(incarnation.len());
    for g in groups.values() {
        for (place, bytes) in g.pieces() {
            live.add(place, bytes);
        }
    }

    // 5. The block where the next frame goes is erased. A frame there was never acknowledged
    // but may be partly durable, its header whole while its last sectors, or the file's
    // end, are not; left alone, a later crash could complete it with the zeros of a newer
    // frame's padding and bring it back. Erased, it can neither return nor be taken for
    // damage once frames follow it elsewhere.
    let head_slot = *by_incarnation
        .get(&highest)
        .ok_or(LogError::Damaged("a live segment is missing"))?;
    let head_offset = if highest == last.incarnation {
        last.after
    } else {
        start_of(head_slot)
            .and_then(|s| s.checked_add(block))
            .ok_or(LogError::Damaged("an offset past u64"))?
    };
    let head_end = start_of(head_slot)
        .and_then(|s| s.checked_add(segment))
        .ok_or(LogError::Damaged("an offset past u64"))?;
    if head_offset
        .checked_add(block)
        .is_some_and(|end| end <= head_end)
    {
        let mut zeros =
            AlignedBuf::zeroed(align.get(), align).map_err(|e| LogError::Disk(e.into()))?;
        zeros
            .set_len(align.get())
            .map_err(|e| LogError::Disk(e.into()))?;
        file.write_all_at(zeros.as_slice(), head_offset)?;
        file.sync_data()?;
    }

    let head_nonce = segment_of(head_slot, highest).nonce;
    let live_segments = (last.tail..=highest)
        .filter_map(|inc| by_incarnation.get(&inc).copied())
        .collect();
    let free = (0..slots)
        .filter(|slot| {
            let inc = incarnation
                .get(usize::try_from(*slot).unwrap_or(usize::MAX))
                .copied()
                .unwrap_or(0);
            inc < last.tail
        })
        .map(|slot| (slot, 0))
        .collect();
    let state = State {
        groups,
        live,
        segments: Segments {
            incarnation,
            nonce,
            live: live_segments,
            free,
        },
        head: Head {
            slot: head_slot,
            incarnation: highest,
            nonce: head_nonce,
            offset: head_offset,
        },
        next_sequence: last
            .sequence
            .checked_add(1)
            .ok_or(LogError::Damaged("sequences past u64"))?,
        next_incarnation: highest
            .checked_add(1)
            .ok_or(LogError::Damaged("incarnations past u64"))?,
        durable: last.sequence,
        durable_tail: last.tail,
    };
    Ok((state, Recovery { frames, damaged }))
}

/// Applies one frame's records, in order, to the groups being rebuilt. `base` is the file
/// offset of the frame's payload in segment `slot`.
fn replay(
    groups: &mut HashMap<u128, Replayed>,
    slot: u32,
    base: u64,
    records: Vec<Owned>,
) -> Result<(), LogError> {
    let place = |at: usize| -> Result<Place, LogError> {
        Ok(Place {
            slot,
            offset: u64::try_from(at)
                .ok()
                .and_then(|at| base.checked_add(at))
                .ok_or(LogError::Damaged("an offset past u64"))?,
        })
    };
    for record in records {
        match record {
            Owned::Entries {
                group,
                first,
                entries,
            } => {
                let g = groups.entry(group).or_default();
                g.truncate(first);
                let held = u64::try_from(entries.len()).unwrap_or(u64::MAX);
                let last = first
                    .checked_add(held)
                    .and_then(|end| end.checked_sub(1))
                    .ok_or(LogError::Damaged("an index past u64"))?;
                insert(g, entries, &place)?;
                g.reach(last);
            }
            Owned::Relocated { group, entries, .. } => {
                insert(groups.entry(group).or_default(), entries, &place)?;
            }
            Owned::HardState { at, group, state } => {
                groups.entry(group).or_default().hard = Some((state, place(at)?));
            }
            Owned::Start { at, group, start } => {
                let g = groups.entry(group).or_default();
                g.start = start;
                g.start_at = Some(place(at)?);
                let last = g.last.max(start.index);
                g.reach(last);
            }
            Owned::Proposal { group, proposal } => {
                groups.entry(group).or_default().proposals.insert(
                    proposal.index,
                    state::Proposal {
                        term: proposal.term,
                        place: place(proposal.at)?,
                        bytes: proposal.bytes.into(),
                    },
                );
            }
            Owned::Removed { group } => {
                groups.remove(&group);
            }
        }
    }
    Ok(())
}

fn insert(
    g: &mut Replayed,
    entries: Vec<format::Decoded>,
    place: &impl Fn(usize) -> Result<Place, LogError>,
) -> Result<(), LogError> {
    for e in entries {
        let len =
            u32::try_from(e.bytes.len()).map_err(|_| LogError::Damaged("an entry past u32"))?;
        g.entries.insert(
            e.index,
            Slot {
                term: e.term,
                place: place(e.at)?,
                len,
                cached: None,
            },
        );
    }
    Ok(())
}
