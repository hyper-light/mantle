//! Creating a log and opening one (docs/design/raft-log.md §6).

use std::collections::HashMap;

use mantle_disk::block::BlockFile;
use mantle_disk::buf::{AlignedBuf, Alignment, MAX_BUFFER};

use crate::format::{self, FRAME_HEADER_BYTES, FrameHeader, Owned, SegmentHeader};
use crate::state::{self, Live, Place, Replayed, Slot};
use crate::{Config, Head, LogError, Recovery, Segments, State};

/// What a frame's position holds.
pub(crate) enum Found<'a> {
    /// A verified frame: its header, its bytes and its padded length.
    Frame(FrameHeader, &'a [u8], u64),
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

/// Reads a log file's frames through a window of one segment's bytes, the most any frame
/// takes. A frame the window holds is verified there, and the window moves to the start of one
/// it does not hold, which then lies wholly within it. A walk of a segment, or a search of its
/// blocks, then reads it in one read, or two where the walk began part way; each frame took a
/// read of its first block, another of the whole frame and a look at the file's length, and a
/// search read the segment a block at a time (audit P07).
pub(crate) struct Reader<'a, F> {
    file: &'a F,
    align: Alignment,
    /// The file's length when the reader was made: nothing writes the file while a reader
    /// walks it.
    len: u64,
    window: AlignedBuf,
    /// The file offset of the window's first byte, and the bytes it holds.
    start: u64,
    held: u64,
}

impl<'a, F: BlockFile> Reader<'a, F> {
    /// A reader of `file` whose window holds `segment_bytes`.
    pub fn new(file: &'a F, align: Alignment, segment_bytes: u64) -> Result<Self, LogError> {
        let size = usize::try_from(segment_bytes).map_err(|_| LogError::Config("segment"))?;
        Ok(Self {
            file,
            align,
            len: file.len()?,
            window: AlignedBuf::zeroed(size, align).map_err(|e| LogError::Disk(e.into()))?,
            start: 0,
            held: 0,
        })
    }

    /// File bytes `[at, at + n)`, `n` at most the window's size, moving the window to `at` when
    /// it does not hold them. `None` when they run past the file.
    fn bytes(&mut self, at: u64, n: u64) -> Result<Option<&[u8]>, LogError> {
        let Some(end) = at.checked_add(n).filter(|&e| e <= self.len) else {
            return Ok(None);
        };
        if at < self.start || end > self.start.saturating_add(self.held) {
            let capacity = u64::try_from(self.window.capacity()).unwrap_or(0);
            if n > capacity {
                return Ok(None);
            }
            // Whole blocks: the file's end may fall within one, past the last frame.
            let want = self
                .align
                .down_u64(capacity.min(self.len.saturating_sub(at)));
            self.held = 0;
            self.window
                .set_len(usize::try_from(want).unwrap_or(0))
                .map_err(|e| LogError::Disk(e.into()))?;
            self.file.read_exact_at(self.window.as_mut_slice(), at)?;
            self.start = at;
            self.held = want;
        }
        let from = usize::try_from(at.saturating_sub(self.start)).unwrap_or(usize::MAX);
        let to = usize::try_from(end.saturating_sub(self.start)).unwrap_or(usize::MAX);
        Ok(self.window.as_slice().get(from..to))
    }

    /// The header in the block at `offset`, if one decodes there, not yet verified.
    fn header(&mut self, offset: u64) -> Result<Option<FrameHeader>, LogError> {
        let block = block_of(self.align)?;
        Ok(self.bytes(offset, block)?.and_then(FrameHeader::decode))
    }

    /// The frame of `segment` at `offset`, reading no further than `end`, the segment's end.
    pub fn frame_at(
        &mut self,
        segment: Segment,
        offset: u64,
        end: u64,
    ) -> Result<Found<'_>, LogError> {
        let block = block_of(self.align)?;
        let Some(first_end) = offset.checked_add(block) else {
            return Ok(Found::End);
        };
        if first_end > end || first_end > self.len {
            return Ok(Found::End);
        }
        let Some(header) = self.header(offset)? else {
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
        let Some(padded) = self.align.up_u64(frame_len) else {
            return Ok(Found::Invalid);
        };
        let fits = offset
            .checked_add(padded)
            .is_some_and(|e| e <= end && e <= self.len);
        if !fits {
            return Ok(Found::Invalid);
        }
        match self.bytes(offset, padded)? {
            Some(bytes) if header.verifies(bytes) => Ok(Found::Frame(header, bytes, padded)),
            _ => Ok(Found::Invalid),
        }
    }

    /// Whether `segment` holds a valid frame after the block at `from`, up to `end`, with a
    /// sequence past `after`.
    fn later_frame(
        &mut self,
        segment: Segment,
        from: u64,
        end: u64,
        after: Option<u64>,
    ) -> Result<bool, LogError> {
        let block = block_of(self.align)?;
        let mut offset = from;
        loop {
            offset = match offset.checked_add(block) {
                Some(next) if next < end => next,
                _ => return Ok(false),
            };
            if let Found::Frame(header, ..) = self.frame_at(segment, offset, end)?
                && after.is_none_or(|seq| header.sequence > seq)
            {
                return Ok(true);
            }
        }
    }

    /// Whether a valid frame of log `log` from a segment newer than incarnation `above` lies
    /// in the blocks after the one at `first`, up to `end`, whatever segment header the slot
    /// holds.
    fn newer_frame(
        &mut self,
        log: u128,
        first: u64,
        end: u64,
        above: u64,
    ) -> Result<bool, LogError> {
        let block = block_of(self.align)?;
        let mut offset = first;
        loop {
            offset = match offset.checked_add(block) {
                Some(next)
                    if next < end && next.checked_add(block).is_some_and(|e| e <= self.len) =>
                {
                    next
                }
                _ => return Ok(false),
            };
            let Some(header) = self.header(offset)? else {
                continue;
            };
            if header.log != log || header.incarnation <= above {
                continue;
            }
            let segment = Segment {
                log,
                incarnation: header.incarnation,
                nonce: header.nonce,
            };
            if let Found::Frame(..) = self.frame_at(segment, offset, end)? {
                return Ok(true);
            }
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
    // Recovery reads a segment through one window, and a frame may fill a segment.
    if config.segment_bytes > u64::try_from(MAX_BUFFER).unwrap_or(u64::MAX) {
        return Err(LogError::Config(
            "a segment fits one I/O buffer (mantle_disk::buf::MAX_BUFFER)",
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
    let slot = persist_slot(config, align)?;
    if slot
        .checked_mul(2)
        .is_none_or(|area| area > config.segment_bytes)
    {
        return Err(LogError::Config(
            "a segment holds two persist records of the log's groups",
        ));
    }
    Ok(())
}

/// Bytes the file keeps before its first segment slot for persist records: one segment's
/// length, so that every frame lies at least that far from them (docs/design/raft-log.md §2).
pub(crate) fn persist_area(config: &Config) -> u64 {
    config.segment_bytes
}

/// Bytes of one persist slot: a record of the most groups a frame can carry, padded to the
/// block. The record of the frame of sequence `s` goes in slot `s mod 2`, so the record of
/// the frame before it survives a torn write of this one.
pub(crate) fn persist_slot(config: &Config, align: Alignment) -> Result<u64, LogError> {
    format::persist_len(config.max_groups)
        .and_then(|len| u64::try_from(len).ok())
        .and_then(|len| align.up_u64(len))
        .ok_or(LogError::Config("persist records past u64"))
}

/// The file offset of the persist slot of the frame of `sequence`.
pub(crate) fn persist_at(slot: u64, sequence: u64) -> u64 {
    if sequence.is_multiple_of(2) { 0 } else { slot }
}

/// A group's state to write back as it was after a frame that no longer reads, which its
/// persist record describes, before the log serves anyone (docs/design/raft-log.md §6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Restore {
    pub group: u128,
    pub update: crate::Update,
    pub uncertain: Option<format::Start>,
    /// The group is marked damaged; the update is empty.
    pub damaged: bool,
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
    let at = persist_area(config);
    file.write_all_at(bytes, at)?;
    file.sync_data()?;
    Ok(State {
        groups: HashMap::new(),
        damaged: HashMap::new(),
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
            offset: at.checked_add(written).ok_or(LogError::Config("frame"))?,
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
) -> Result<(State, Recovery, Vec<Restore>), LogError> {
    let align = file.alignment();
    check(config, align)?;
    let block = block_of(align)?;
    let len = file.len()?;
    if len == 0 {
        return Err(LogError::Foreign("the file is empty"));
    }
    let segment = config.segment_bytes;
    let area = persist_area(config);
    let slots = len
        .checked_sub(area)
        .filter(|&rest| rest > 0)
        .ok_or(LogError::Foreign("no segment past the persist area"))?
        .div_ceil(segment);
    let slots = u32::try_from(slots)
        .ok()
        .filter(|&s| s <= config.max_segments)
        .ok_or(LogError::Foreign("more segments than the log's quota"))?;
    let start_of = |slot: u32| {
        u64::from(slot)
            .checked_mul(segment)
            .and_then(|s| s.checked_add(area))
    };

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
    let mut reader = Reader::new(file, align, segment)?;
    let frames_of = |reader: &mut Reader<'_, F>,
                     inc: u64,
                     visit: &mut Visit<'_>|
     -> Result<(u64, bool), LogError> {
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
            match reader.frame_at(which, offset, end)? {
                Found::Frame(header, bytes, padded) => {
                    visit(offset, &header, bytes, padded)?;
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
        let (stop, _) = frames_of(&mut reader, inc, &mut |offset, header, _, padded| {
            found = Some(Last {
                incarnation: inc,
                offset,
                sequence: header.sequence,
                tail: header.tail,
                after: offset.saturating_add(padded),
            });
            Ok(())
        })?;
        // A frame is written only after the one before it is flushed, so a valid frame past
        // the point where the frames stop proves the frame there was acknowledged, whatever
        // part of it no longer reads: its checksum, or its magic, format or identity
        // (06 §A3). Only with none past it is the stop the log's end.
        let slot = *by_incarnation
            .get(&inc)
            .ok_or(LogError::Damaged("a live segment is missing"))?;
        let end = start_of(slot)
            .and_then(|s| s.checked_add(segment))
            .ok_or(LogError::Damaged("an offset past u64"))?;
        let after = found.map(|f| f.sequence);
        if reader.later_frame(segment_of(slot, inc), stop, end, after)? {
            return Err(LogError::Damaged("an acknowledged frame does not verify"));
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
    // A slot whose header no longer reads as this log's may hold a newer segment whose header
    // was damaged. A segment's header is written with its first frame and flushed with it, so
    // a valid frame of this log past that first frame, in a segment newer than any whose
    // header reads, proves the header was durable and is now damaged. The first frame alone
    // is an opening that may never have been flushed, the torn tail's case.
    for slot in 0..slots {
        let i = usize::try_from(slot).map_err(|_| LogError::Damaged("a slot past usize"))?;
        if incarnation.get(i).is_some_and(|&inc| inc != 0) {
            continue;
        }
        let begin = start_of(slot).ok_or(LogError::Damaged("an offset past u64"))?;
        let end = begin
            .checked_add(segment)
            .ok_or(LogError::Damaged("an offset past u64"))?;
        let first = begin
            .checked_add(block)
            .ok_or(LogError::Damaged("an offset past u64"))?;
        if reader.newer_frame(id, first, end, highest)? {
            return Err(LogError::Damaged(
                "a segment's header does not read, but its frames do",
            ));
        }
    }
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
        let (end, invalid) = frames_of(&mut reader, inc, &mut |offset, header, bytes, _| {
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

    // 4. The groups as they run; those with a record missing recover from their peers, and
    // so do those an earlier recovery fenced as damaged.
    let mut groups = HashMap::new();
    let mut damaged = Vec::new();
    let mut fenced = HashMap::new();
    for (group, r) in replayed {
        if let Some(at) = r.damaged {
            fenced.insert(group, at);
            continue;
        }
        match r.finish() {
            Some(g) => {
                groups.insert(group, g);
            }
            None => damaged.push(group),
        }
    }
    // 4b. A frame after the last valid one may have been flushed and damaged since: its
    // persist record, written in the same flush, says what it held (AGL+18 §3.3.3).
    let restores = restores(
        file,
        config,
        id,
        last.sequence,
        &groups,
        &fenced,
        &mut damaged,
    )?;
    damaged.sort_unstable();
    damaged.dedup();
    // A group newly found damaged is fenced by a record written before the log serves
    // anyone, so that the fence outlives what showed the damage: the frame is overwritten
    // by the next, and a record missing may be swept away (audit S16).
    let mut restores = restores;
    restores.extend(damaged.iter().map(|&group| Restore {
        group,
        update: crate::Update::default(),
        uncertain: None,
        damaged: true,
    }));
    let mut live = Live::with_slots(incarnation.len());
    for g in groups.values() {
        for (place, bytes) in g.pieces() {
            live.add(place, bytes);
        }
    }
    for &place in fenced.values() {
        live.add(place, state::DAMAGED_BYTES);
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
    let restored = restores
        .iter()
        .filter(|r| !r.damaged)
        .map(|r| r.group)
        .collect();
    let state = State {
        damaged: damaged
            .iter()
            .map(|&group| (group, None))
            .chain(fenced.iter().map(|(&group, &at)| (group, Some(at))))
            .collect(),
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
    Ok((
        state,
        Recovery {
            frames,
            damaged: {
                let mut all: Vec<u128> = damaged.into_iter().chain(fenced.into_keys()).collect();
                all.sort_unstable();
                all
            },
            restored,
        },
        restores,
    ))
}

/// What the frame after the last valid one held, from its persist record, as the updates that
/// restore it. A persist record is written in its frame's flush, so it may survive a frame
/// torn by a crash as well as one damaged since its flush; what tells the two apart is a
/// confirmation that the frame was flushed: the next frame's record, which is written only
/// after that flush, or the one the writer writes when no frame follows at once. A frame's
/// updates are answered only once it is confirmed.
///
/// A confirmed frame was acknowledged, and damaged since (AGL+18 §3.3.3). Each of its groups
/// gets back the start and hard state the frame left, its entries cut back to where the
/// frame's began, and, where the frame wrote entries, the mark that the log may lack them; a
/// group whose frame held proposals, which a persist record does not carry, is damaged and
/// recovers from its peers. An unconfirmed frame was never acknowledged, and is the torn tail,
/// but for its term and vote, which are kept, since raising a term or keeping a vote is always
/// safe. Its commit is not kept, since it may name an entry only the frame held. A fence the
/// frame put on a damaged group is kept, confirmed or not.
fn restores<F: BlockFile>(
    file: &F,
    config: &Config,
    id: u128,
    last: u64,
    groups: &HashMap<u128, state::Group>,
    fenced: &HashMap<u128, Place>,
    damaged: &mut Vec<u128>,
) -> Result<Vec<Restore>, LogError> {
    let Some(sequence) = last.checked_add(1) else {
        return Ok(Vec::new());
    };
    let align = file.alignment();
    let slot = persist_slot(config, align)?;
    let size = usize::try_from(slot).map_err(|_| LogError::Config("a persist slot"))?;
    let mut records = Vec::with_capacity(2);
    for at in [0, slot] {
        let mut bytes = AlignedBuf::zeroed(size, align).map_err(|e| LogError::Disk(e.into()))?;
        bytes.set_len(size).map_err(|e| LogError::Disk(e.into()))?;
        file.read_exact_at(bytes.as_mut_slice(), at)?;
        if let Some(p) = format::Persist::decode(bytes.as_slice()).filter(|p| p.log == id) {
            records.push((at, p));
        }
    }
    let confirmed = records.iter().any(|(_, p)| p.confirms >= sequence);
    let Some(record) = records
        .into_iter()
        .find(|(at, p)| *at == persist_at(slot, sequence) && p.sequence == sequence)
        .map(|(_, p)| p)
    else {
        return Ok(Vec::new());
    };
    let empty = state::Group::default();
    let mut out = Vec::new();
    for p in record.groups {
        if damaged.contains(&p.group) || (fenced.contains_key(&p.group) && !p.removed) {
            continue;
        }
        // A fence the frame wrote is kept whether or not the frame was confirmed: marking a
        // group damaged is always safe, and the frame it fenced may be gone.
        if p.damaged {
            damaged.push(p.group);
            continue;
        }
        let g = groups.get(&p.group).unwrap_or(&empty);
        let current = g.hard.map(|(h, _)| h);
        if !confirmed {
            // The torn tail, keeping only a later term or a vote given.
            let Some(h) = p.hard_state else {
                continue;
            };
            let later =
                current.is_none_or(|c| h.term > c.term || (h.term == c.term && c.vote == 0));
            if later {
                out.push(Restore {
                    group: p.group,
                    update: crate::Update {
                        hard_state: Some(format::HardState {
                            commit: current.map_or(0, |c| c.commit),
                            ..h
                        }),
                        ..crate::Update::default()
                    },
                    uncertain: None,
                    damaged: false,
                });
            }
            continue;
        }
        if p.proposals {
            damaged.push(p.group);
            continue;
        }
        if p.removed {
            out.push(Restore {
                group: p.group,
                update: crate::Update {
                    remove: true,
                    ..crate::Update::default()
                },
                uncertain: None,
                damaged: false,
            });
            continue;
        }
        let mut update = crate::Update::default();
        let mut last_index = g.last().ok_or(LogError::Damaged("an index past u64"))?;
        if let Some(start) = p.start.filter(|s| s.index >= g.start.index) {
            update.start = Some(start);
            last_index = last_index.max(start.index);
        }
        let mut uncertain = g.uncertain.map(|(mark, _)| mark);
        if let Some(w) = p.entries {
            let from = w.first.max(
                update
                    .start
                    .map_or(g.start.index, |s| s.index)
                    .saturating_add(1),
            );
            update.entries = Some(crate::Entries {
                first: from,
                entries: Vec::new(),
            });
            last_index = last_index.min(from.saturating_sub(1));
            if w.count > 0 {
                let mark = format::Start {
                    index: w
                        .first
                        .checked_add(w.count)
                        .and_then(|end| end.checked_sub(1))
                        .ok_or(LogError::Damaged("an index past u64"))?,
                    term: w.term,
                };
                uncertain = Some(merge(uncertain, mark));
            }
        }
        if let Some(mark) = p.uncertain {
            uncertain = Some(merge(uncertain, mark));
        }
        let newer = p
            .hard_state
            .filter(|h| current.is_none_or(|c| h.term >= c.term));
        // A commit past the entries the log still holds would name entries it lacks; the
        // leader tells the replica its commit again.
        let hard = newer.or(current).map(|h| format::HardState {
            commit: h.commit.min(last_index),
            ..h
        });
        if hard != current {
            update.hard_state = hard;
        }
        if update == crate::Update::default() && uncertain == g.uncertain.map(|(m, _)| m) {
            continue;
        }
        out.push(Restore {
            group: p.group,
            update,
            uncertain,
            damaged: false,
        });
    }
    Ok(out)
}

/// A mark covering both marks.
fn merge(mark: Option<format::Start>, other: format::Start) -> format::Start {
    match mark {
        Some(m) => format::Start {
            index: m.index.max(other.index),
            term: m.term.max(other.term),
        },
        None => other,
    }
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
            Owned::Uncertain { at, group, mark } => {
                groups.entry(group).or_default().uncertain = Some((mark, place(at)?));
            }
            Owned::Damaged { at, group } => {
                *groups.entry(group).or_default() = Replayed {
                    damaged: Some(place(at)?),
                    ..Replayed::default()
                };
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
