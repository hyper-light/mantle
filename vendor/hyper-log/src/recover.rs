//! Creating a log and opening one (docs/design/raft-log.md §6).

use std::collections::HashMap;

use hyper_block::block::BlockFile;
use hyper_block::buf::{AlignedBuf, Alignment, MAX_BUFFER};

use hyper_seal::log::FrameMac;

use crate::format::{self, FRAME_HEADER_BYTES, FrameHeader, MAC_LEN, Owned, SegmentHeader};
use crate::seal::Sealer;
use crate::state::{self, Live, Place, Replayed, Slot};
use crate::state::{Head, Segments, State};
use crate::{Answers, AtFlush, Config, Laid, LogError, Recovery, Tail};

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
    pub(crate) log: u128,
    pub(crate) incarnation: u64,
    pub(crate) nonce: u64,
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
    /// A sealed log's framing MAC: every frame it reads is checked against it once its CRC holds.
    mac: Option<FrameMac>,
}

impl<'a, F: BlockFile> Reader<'a, F> {
    /// A reader of `file` whose window holds `segment_bytes`, checking a sealed log's frames
    /// against `mac`.
    pub(crate) fn new(
        file: &'a F,
        align: Alignment,
        segment_bytes: u64,
        mac: Option<FrameMac>,
    ) -> Result<Self, LogError> {
        let size = usize::try_from(segment_bytes).map_err(|_| LogError::Config("segment"))?;
        Ok(Self {
            file,
            align,
            len: file.len()?,
            window: AlignedBuf::zeroed(size, align).map_err(|e| LogError::Disk(e.into()))?,
            start: 0,
            held: 0,
            mac,
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
    pub(crate) fn frame_at(
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
        if !self.ours(&header)? {
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
        let mac = self.mac.clone();
        match self.bytes(offset, padded)? {
            Some(bytes) if header.verifies(bytes) => {
                if let Some(mac) = &mac {
                    check_frame_mac(mac, &header, bytes)?;
                }
                Ok(Found::Frame(header, bytes, padded))
            }
            _ => Ok(Found::Invalid),
        }
    }

    /// Whether a frame of this log's ID is of its kind. A sealed log writes no frame of format 3:
    /// one of its own there was put there by someone else. An unsealed log reads a sealed frame as
    /// no frame of its own.
    fn ours(&self, header: &FrameHeader) -> Result<bool, LogError> {
        match (self.mac.is_some(), header.sealed) {
            (true, false) => Err(LogError::Tampered("an unsealed frame in a sealed log")),
            (false, true) => Ok(false),
            _ => Ok(true),
        }
    }

    /// The frames of `segment` after the block at `from`, up to `end`, with a sequence past
    /// `after`, the last valid frame's, where each valid one is a frame of the window of the frame
    /// after that one, the gap: laid while the gap may still have been in flight, past it by no
    /// more than its own overlap, so it proves nothing of it (mantle docs/design/raft-log.md
    /// §3.3). Any other valid one was laid once the gap was applied, so the gap was durable and is
    /// damaged, and so is any valid one where `after` is `None` or `windows` is false: no window
    /// lies there. A window holds one frame of each sequence, of the 255 an overlap reaches at
    /// most; a second of one is damage too. Each is given by its offset and whether it verified:
    /// one of the segment's that does not proves nothing, but is erased with the window, as step
    /// 6's block is. They are at most the segment's blocks.
    fn past_stop(
        &mut self,
        segment: Segment,
        from: u64,
        end: u64,
        after: Option<u64>,
        windows: bool,
    ) -> Result<Vec<(u64, bool)>, LogError> {
        let block = block_of(self.align)?;
        let gap = after.and_then(|seq| seq.checked_add(1)).filter(|_| windows);
        let mut window: Vec<(u64, u64, bool)> = Vec::new();
        let mut offset = from;
        loop {
            offset = match offset.checked_add(block) {
                Some(next) if next < end => next,
                _ => return Ok(window.into_iter().map(|(at, _, ok)| (at, ok)).collect()),
            };
            let header = match self.frame_at(segment, offset, end)? {
                Found::Frame(header, ..) => header,
                Found::End => continue,
                Found::Invalid => {
                    if gap.is_some()
                        && let Some(header) = self.header(offset)?
                        && after.is_some_and(|seq| header.sequence > seq)
                    {
                        window.push((offset, header.sequence, false));
                    }
                    continue;
                }
            };
            if after.is_some_and(|seq| header.sequence <= seq) {
                continue;
            }
            let within = gap.is_some_and(|gap| {
                header
                    .sequence
                    .checked_sub(gap)
                    .is_some_and(|past| past > 0 && past <= u64::from(header.overlap))
            });
            let seen = window
                .iter()
                .any(|&(_, seq, ok)| ok && seq == header.sequence);
            if !within || seen {
                return Err(LogError::Damaged("an acknowledged frame does not verify"));
            }
            window.push((offset, header.sequence, true));
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

/// Whether a persist record read whole is its log's: in a sealed log, sealed and its MAC holding
/// (checked once its CRC held, so a mismatch is tampering); in an unsealed one, unsealed.
fn check_record(
    record: &format::Persist,
    bytes: &[u8],
    sealer: Option<&Sealer>,
) -> Result<(), LogError> {
    match (sealer, record.sealed) {
        (Some(sealer), true) => {
            let len = record
                .encoded_len()
                .ok_or(LogError::Tampered("a persist record's length"))?;
            let end = len
                .checked_add(MAC_LEN)
                .ok_or(LogError::Tampered("a persist record's length"))?;
            let (Some(covered), Some(mac)) = (bytes.get(..len), bytes.get(len..end)) else {
                return Err(LogError::Tampered("a persist record cut short of its MAC"));
            };
            sealer.verify(covered, mac)
        }
        (Some(_), false) => Err(LogError::Tampered(
            "an unsealed persist record in a sealed log",
        )),
        (None, true) => Err(LogError::Foreign(
            "a sealed persist record in an unsealed log",
        )),
        (None, false) => Ok(()),
    }
}

/// Whether a sealed frame's MAC holds over its header and payload: checked once its CRC holds, so
/// a mismatch is tampering, never a torn write.
fn check_frame_mac(mac: &FrameMac, header: &FrameHeader, bytes: &[u8]) -> Result<(), LogError> {
    let at = header
        .mac_at()
        .ok_or(LogError::Tampered("a frame's length"))?;
    let end = at
        .checked_add(MAC_LEN)
        .ok_or(LogError::Tampered("a frame's length"))?;
    let (Some(covered), Some(tag)) = (bytes.get(..at), bytes.get(at..end)) else {
        return Err(LogError::Tampered("a frame cut short of its MAC"));
    };
    let expected = crate::seal::frame_mac(mac, covered, header.records)?;
    crate::seal::same_mac(&expected, tag)
}

fn block_of(align: Alignment) -> Result<u64, LogError> {
    u64::try_from(align.get()).map_err(|_| LogError::Config("block"))
}

pub(crate) fn check(config: &Config, align: Alignment, sealed: bool) -> Result<(), LogError> {
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
            "a segment fits one I/O buffer (hyper_block::buf::MAX_BUFFER)",
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
    let slot = persist_slot(config, align, sealed)?;
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

/// Bytes of one persist slot: a record of the most groups a frame can carry, and a sealed log's
/// MAC after it, padded to the block. The record of the frame of sequence `s` goes in slot
/// `s mod 2`, so the record of the frame before it survives a torn write of this one.
pub(crate) fn persist_slot(
    config: &Config,
    align: Alignment,
    sealed: bool,
) -> Result<u64, LogError> {
    format::persist_len(config.max_groups)
        .and_then(|len| len.checked_add(if sealed { MAC_LEN } else { 0 }))
        .and_then(|len| u64::try_from(len).ok())
        .and_then(|len| align.up_u64(len))
        .ok_or(LogError::Config("persist records past u64"))
}

/// Where a log's persist records lie in its persist area, and how they are laid (mantle
/// docs/design/durability.md §7.7): whole (format 3, 4 sealed) in two slots from the area's start,
/// or in pieces of the device's atomic write unit (format 5, 6 sealed) in two slots past those, or
/// at the area's end where a re-lay put them, the area's start holding the mark of pieces
/// ([`format::pieces_mark`]). The record of the frame of sequence `s` goes in slot `s mod 2`, so
/// the record of the frame before it survives a torn write of this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Area {
    /// The two slots' offsets, the even sequences' first.
    slots: (u64, u64),
    /// Bytes of each slot.
    len: u64,
    /// The pieces the records are laid in; `None` where they are whole.
    pieces: Option<format::Pieces>,
    /// The file's block the slots are laid by.
    block: Alignment,
    /// Whether the slots in pieces lie at the area's end.
    at_end: bool,
    /// Whether the log may hold frames answered at their own flush, as its mark says
    /// ([`format::Mark::at_flush`]).
    at_flush: bool,
}

impl Area {
    /// Whole records: slots at 0 and at one slot's length.
    pub(crate) fn whole(config: &Config, align: Alignment, sealed: bool) -> Result<Self, LogError> {
        let len = persist_slot(config, align, sealed)?;
        Ok(Self {
            slots: (0, len),
            len,
            pieces: None,
            block: align,
            at_end: false,
            at_flush: false,
        })
    }

    /// Records in pieces of `unit`, a sealed log's each with its MAC. Each slot starts at an offset
    /// the larger of the unit and the file's block divides, so that it is both a place for whole
    /// pieces and a write the file takes, and its length is a multiple of that: a piece stays one
    /// unit while the write meets a direct file's alignment where that is the larger. The first
    /// slot lies past the two whole ones, so a log moves to pieces without overwriting the records
    /// it carries ([`relay`]); or, `at_end`, the second ends where the last whole larger block of
    /// the area does, so a re-lay from slots past the whole ones writes clear of them, and the
    /// reverse. `None` where the persist area does not hold both; a unit that holds no row is
    /// refused.
    pub(crate) fn in_pieces(
        config: &Config,
        align: Alignment,
        unit: Alignment,
        sealed: bool,
        at_end: bool,
    ) -> Result<Option<Self>, LogError> {
        let past = || LogError::Config("persist records past u64");
        let whole = persist_slot(config, align, sealed)?;
        let pieces = format::Pieces::new(unit, sealed).ok_or(LogError::Config(
            "the device's atomic write unit holds no persist row",
        ))?;
        let block = unit.max(align);
        let len = pieces
            .len(config.max_groups)
            .and_then(|len| u64::try_from(len).ok())
            .and_then(|len| block.up_u64(len))
            .ok_or_else(past)?;
        let after_whole = whole
            .checked_mul(2)
            .and_then(|end| block.up_u64(end))
            .ok_or_else(past)?;
        let both = len.checked_mul(2).ok_or_else(past)?;
        let first = if at_end {
            match block.down_u64(persist_area(config)).checked_sub(both) {
                Some(first) => first,
                None => return Ok(None),
            }
        } else {
            after_whole
        };
        let second = first.checked_add(len).ok_or_else(past)?;
        let fits = first >= after_whole
            && second
                .checked_add(len)
                .is_some_and(|end| end <= persist_area(config));
        Ok(fits.then_some(Self {
            slots: (first, second),
            len,
            pieces: Some(pieces),
            block: align,
            at_end,
            at_flush: false,
        }))
    }

    /// The area a log's records are laid in, as its persist area's mark says, or whole without one,
    /// the file's block being `align`: in pieces of the mark's unit, by the block it records, or
    /// by `align` where it records none.
    pub(crate) fn of(
        config: &Config,
        align: Alignment,
        sealed: bool,
        mark: Option<format::Mark>,
    ) -> Result<Self, LogError> {
        match mark {
            None => Self::whole(config, align, sealed),
            Some(mark) => Self::in_pieces(
                config,
                mark.block.unwrap_or(align),
                mark.unit,
                sealed,
                mark.at_end,
            )?
            .map(|area| area.answered_at_flush(mark.at_flush))
            .ok_or(LogError::Config(
                "the persist area does not hold its records in pieces",
            )),
        }
    }

    /// The area, its mark saying the log may hold frames answered at their flush where
    /// `at_flush`: a re-lay keeps what the mark it lays from says.
    fn answered_at_flush(self, at_flush: bool) -> Self {
        Self { at_flush, ..self }
    }

    /// The mark of this area's pieces; `None` where its records are whole.
    pub(crate) fn mark(&self) -> Option<format::Mark> {
        self.pieces.map(|p| format::Mark {
            unit: p.unit(),
            block: Some(self.block),
            at_end: self.at_end,
            at_flush: self.at_flush,
        })
    }

    /// The pieces the records are laid in; `None` where they are whole.
    pub(crate) fn pieces(&self) -> Option<format::Pieces> {
        self.pieces
    }

    /// Whether the area's mark says the log may hold frames answered at their flush.
    pub(crate) fn at_flush(&self) -> bool {
        self.at_flush
    }

    /// Whether a file whose block is `block` writes this area's slots as they lie: each slot's
    /// offset and length a multiple of the block, as the writer pads a record to it.
    fn takes(&self, block: Alignment) -> bool {
        [self.slots.0, self.slots.1, self.len]
            .into_iter()
            .all(|n| block.is_aligned_u64(n))
    }

    /// The bytes from the first slot's start to the second's end.
    fn span(&self) -> Option<(u64, u64)> {
        Some((self.slots.0, self.slots.1.checked_add(self.len)?))
    }

    /// The file offset of the slot of the frame of `sequence`.
    pub(crate) fn at(&self, sequence: u64) -> u64 {
        if sequence.is_multiple_of(2) {
            self.slots.0
        } else {
            self.slots.1
        }
    }

    /// The slot other than the one at `at`.
    fn other(&self, at: u64) -> u64 {
        if at == self.slots.0 {
            self.slots.1
        } else {
            self.slots.0
        }
    }

    /// The record's bytes as this area lays them: whole, a sealed log's MAC after it, or in
    /// pieces, a sealed log's each with its MAC.
    fn encode(
        &self,
        record: &format::Persist,
        sealer: Option<&Sealer>,
    ) -> Result<Vec<u8>, LogError> {
        let too_large = LogError::TooLarge(record.groups.len());
        if let Some(pieces) = self.pieces {
            let mut bytes = pieces.encode(record).ok_or(too_large)?;
            if let Some(sealer) = sealer {
                seal_pieces(&mut bytes, pieces.unit(), sealer)?;
            }
            return Ok(bytes);
        }
        let mut bytes = record.encode().ok_or(too_large)?;
        if let Some(sealer) = sealer {
            let mac = sealer.mac(&bytes)?;
            bytes.extend_from_slice(&mac);
        }
        Ok(bytes)
    }

    /// The record log `id`'s slot at `at` holds, if one reads whole: a torn record in pieces is
    /// none, as a whole record that fails its checksum is; a piece damaged where the newest
    /// record's go is [`LogError::Damaged`]. In a sealed log each piece's MAC is checked once its
    /// CRC holds, so a forged or altered piece is [`LogError::Tampered`]. The slot is read through
    /// the file's transfer alignment, which its offset need not meet when the area is one being
    /// moved from.
    fn read<F: BlockFile>(
        &self,
        file: &F,
        at: u64,
        id: u128,
        sealer: Option<&Sealer>,
    ) -> Result<Option<format::Persist>, LogError> {
        let slot = || LogError::Config("a persist slot");
        let transfer = file.alignment();
        let from = transfer.down_u64(at);
        let to = at
            .checked_add(self.len)
            .and_then(|end| transfer.up_u64(end))
            .ok_or_else(slot)?;
        let size = to
            .checked_sub(from)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(slot)?;
        let skip = usize::try_from(at.checked_sub(from).ok_or_else(slot)?).map_err(|_| slot())?;
        let len = usize::try_from(self.len).map_err(|_| slot())?;
        let mut bytes =
            AlignedBuf::zeroed(size, file.layout_block()).map_err(|e| LogError::Disk(e.into()))?;
        bytes.set_len(size).map_err(|e| LogError::Disk(e.into()))?;
        file.read_exact_at(bytes.as_mut_slice(), from)?;
        let bytes = bytes
            .as_slice()
            .get(skip..skip.checked_add(len).ok_or_else(slot)?)
            .ok_or_else(slot)?;
        let record = match self.pieces {
            None => format::Persist::decode(bytes),
            Some(pieces) => {
                let verify = |covered: &[u8], mac: &[u8]| match sealer {
                    Some(sealer) => sealer.verify(covered, mac),
                    None => Ok(()),
                };
                match pieces.decode_verified(bytes, verify)? {
                    format::Pieced::Whole(record) => Some(record),
                    format::Pieced::Empty | format::Pieced::Torn(_) => None,
                }
            }
        };
        let Some(record) = record.filter(|p| p.log == id) else {
            return Ok(None);
        };
        if self.pieces.is_some() {
            if record.sealed && sealer.is_none() {
                return Err(LogError::Foreign(
                    "a sealed persist record in an unsealed log",
                ));
            }
        } else {
            check_record(&record, bytes, sealer)?;
        }
        Ok(Some(record))
    }

    /// Writes `record` at `at`, the start of one of the area's slots: a whole record padded to the
    /// file's block, a record in pieces with zeros to the slot's end. The open writes this way the
    /// copy of a lost frame's record, which takes that frame's sequence, into the other slot, which
    /// may hold the pieces of a later frame's record that never became durable: left past the
    /// copy's pieces, those would read as the slot's newest record, torn, and hide the copy. The
    /// writer's records need no such room: each names a later frame or confirmation than any the
    /// slot holds, so it reads as the newest wherever older pieces lie past it.
    pub(crate) fn write<F: BlockFile>(
        &self,
        file: &F,
        record: &format::Persist,
        at: u64,
        sealer: Option<&Sealer>,
    ) -> Result<(), LogError> {
        let bytes = self.encode(record, sealer)?;
        let len = match self.pieces {
            Some(_) => usize::try_from(self.len).map_err(|_| LogError::Config("a persist slot"))?,
            None => bytes.len(),
        };
        let mut buf = AlignedBuf::zeroed(len.max(bytes.len()), file.layout_block())
            .map_err(|e| LogError::Disk(e.into()))?;
        buf.extend_from_slice(&bytes)
            .map_err(|e| LogError::Disk(e.into()))?;
        buf.extend_zeros(len.saturating_sub(bytes.len()))
            .map_err(|e| LogError::Disk(e.into()))?;
        let padded = buf.padded().map_err(|e| LogError::Disk(e.into()))?;
        file.write_all_at(padded, at)?;
        Ok(())
    }
}

/// Writes each piece's MAC of a sealed log's record in pieces of `unit`, `bytes` as
/// [`format::Pieces::encode`] laid them: over the piece through its CRC, in the room after it
/// (format 6).
pub(crate) fn seal_pieces(
    bytes: &mut [u8],
    unit: Alignment,
    sealer: &Sealer,
) -> Result<(), LogError> {
    for piece in bytes.chunks_exact_mut(unit.get()) {
        let at = format::piece_mac_at(piece).ok_or(LogError::Config("a sealed piece's MAC"))?;
        let mac = sealer.mac(
            piece
                .get(..at)
                .ok_or(LogError::Config("a sealed piece's MAC"))?,
        )?;
        crate::seal::put_at(
            at.checked_add(MAC_LEN)
                .and_then(|end| piece.get_mut(at..end)),
            &mac,
        )?;
    }
    Ok(())
}

/// How log `id`'s persist area is laid in pieces, from the mark at its start; `None` where the
/// area holds whole records.
fn marked<F: BlockFile>(
    file: &F,
    id: u128,
    sealed: bool,
) -> Result<Option<format::Mark>, LogError> {
    let align = file.layout_block();
    let mut block = AlignedBuf::zeroed(align.get(), align).map_err(|e| LogError::Disk(e.into()))?;
    block
        .set_len(align.get())
        .map_err(|e| LogError::Disk(e.into()))?;
    file.read_exact_at(block.as_mut_slice(), 0)?;
    Ok(format::pieces_marked(block.as_slice(), id, sealed))
}

/// Writes the mark that log `id`'s persist area is laid as `area` is: one block of the file's at
/// the area's start, where the first whole slot began, which every area in pieces lies past.
fn write_mark<F: BlockFile>(file: &F, id: u128, sealed: bool, area: &Area) -> Result<(), LogError> {
    let mark = area
        .mark()
        .and_then(|mark| format::pieces_mark(id, sealed, mark))
        .ok_or(LogError::Config("the persist area's mark"))?;
    let align = file.layout_block();
    let mut block = AlignedBuf::zeroed(align.get(), align).map_err(|e| LogError::Disk(e.into()))?;
    block
        .extend_from_slice(&mark)
        .map_err(|e| LogError::Disk(e.into()))?;
    let padded = block.padded().map_err(|e| LogError::Disk(e.into()))?;
    file.write_all_at(padded, 0)?;
    Ok(())
}

/// Lays log `id`'s persist records again, from `from` into `to`, in pieces (durability.md §7.7):
/// a log of whole records moving to pieces, or one in pieces whose unit the device no longer
/// writes atomically, or whose slots the file's block no longer takes. Each slot of `to` is written
/// whole, zeros and the pieces of the record the same slot of `from` holds, so that it holds only
/// zeros and pieces, as their decoder assumes; then one flush; then the mark, then a flush. `to`
/// lies clear of `from` and of the mark, so a crash before the mark is durable leaves `from` as it
/// was, which the next open reads and lays again; after it, `to` carries the same records in the
/// same slots. The mark lies within the device's unit, which a crash leaves old or new. `tears`:
/// `from`'s pieces are larger than the device's unit, so a crash may have torn one: a slot whose
/// piece fails its checksum reads as a torn record, none, rather than damage.
fn relay<F: BlockFile>(
    file: &F,
    (from, to): (&Area, &Area),
    (id, sealer): (u128, Option<&Sealer>),
    tears: bool,
) -> Result<(), LogError> {
    let clear = match (from.span(), to.span()) {
        (Some(a), Some(b)) => a.1 <= b.0 || b.1 <= a.0,
        _ => false,
    };
    if !clear {
        return Err(LogError::Config(
            "the persist area holds no slots in pieces clear of those it lays from",
        ));
    }
    let size = usize::try_from(to.len).map_err(|_| LogError::Config("a persist slot"))?;
    let align = file.layout_block();
    for (old, new) in [(from.slots.0, to.slots.0), (from.slots.1, to.slots.1)] {
        let mut buf = AlignedBuf::zeroed(size, align).map_err(|e| LogError::Disk(e.into()))?;
        let record = match from.read(file, old, id, sealer) {
            Err(LogError::Damaged(_)) if tears => None,
            read => read?,
        };
        if let Some(record) = record {
            buf.extend_from_slice(&to.encode(&record, sealer)?)
                .map_err(|e| LogError::Disk(e.into()))?;
        }
        buf.set_len(size).map_err(|e| LogError::Disk(e.into()))?;
        file.write_all_at(buf.as_slice(), new)?;
    }
    file.sync_data()?;
    write_mark(file, id, sealer.is_some(), to)?;
    file.sync_data()?;
    Ok(())
}

/// The area log `id`'s persist records are laid in at its open, laid again first where they must
/// be (durability.md §7.7): an area of whole records moves to pieces of the device's unit where
/// the persist area holds them; an area in pieces stays as its mark records it, by the block it
/// records whatever the file's is now, unless the device's unit is smaller than its pieces, which
/// a crash could then tear, or the file's block does not take its slots: then it is laid again in
/// the device's unit and the file's block, at the other end of the area. A mark from before the
/// block was recorded is written again with the file's block, by which its slots lie.
fn laid_area<F: BlockFile>(
    file: &F,
    config: &Config,
    (id, sealer): (u128, Option<&Sealer>),
    device: Alignment,
) -> Result<Area, LogError> {
    let align = file.layout_block();
    let sealed = sealer.is_some();
    let Some(mark) = marked(file, id, sealed)? else {
        let whole = Area::whole(config, align, sealed)?;
        return match Area::in_pieces(config, align, device, sealed, false)? {
            Some(pieces) => {
                relay(file, (&whole, &pieces), (id, sealer), false)?;
                Ok(pieces)
            }
            None => Ok(whole),
        };
    };
    let area = Area::of(config, align, sealed, Some(mark))?;
    let tears = mark.unit > device;
    if !tears && area.takes(align) {
        if mark.block.is_none() {
            write_mark(file, id, sealed, &area)?;
            file.sync_data()?;
        }
        return Ok(area);
    }
    let to = Area::in_pieces(config, align, device, sealed, !mark.at_end)?
        .map(|to| to.answered_at_flush(mark.at_flush))
        .ok_or(LogError::Config(
            "the persist area does not hold its records laid again",
        ))?;
    relay(file, (&area, &to), (id, sealer), tears)?;
    Ok(to)
}

/// A lost frame's persist record as copied into the other slot before its restore is written:
/// the slot's offset and the record (docs/design/raft-log.md §6).
type RecordCopy = (u64, format::Persist);

/// A group's state to write back as it was after a frame that no longer reads, which its
/// persist record describes, before the log serves anyone (docs/design/raft-log.md §6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Restore {
    pub(crate) group: u128,
    pub(crate) update: crate::Update,
    pub(crate) uncertain: Option<format::Start>,
    /// The group is marked damaged; the update is empty.
    pub(crate) damaged: bool,
}

/// Writes segment 0, incarnation 1: its header and an empty first frame. A sealed log's header
/// carries its first session's key frame, and both end with their MACs.
pub(crate) fn create<F: BlockFile>(
    file: &F,
    config: &Config,
    id: u128,
    mut sealer: Option<&mut Sealer>,
    at_flush: AtFlush,
) -> Result<(State, Laid), LogError> {
    let align = file.layout_block();
    check(config, align, sealer.is_some())?;
    if !file.is_empty()? {
        return Err(LogError::Foreign("a new log needs an empty file"));
    }
    let device = file.atomic_unit()?;
    // The device's write cache beside its atomic write unit: with the records' laying, how the
    // log answers its frames (`Answers`, durability.md §7.5).
    let cache = file.write_cache()?;
    // A new log lays its records in pieces where its persist area holds them: the file is empty,
    // so the slots read as zeros, and the mark is written under the log's first flush.
    let pieces = Area::in_pieces(config, align, device.unit(), sealer.is_some(), false)?;
    // A log that answers frames at their flush says so in its mark, durable under the log's first
    // flush, before any such frame (durability.md §7.7, step 4).
    let reaches = file.writes_reach_device();
    let answers = Answers::of(pieces.and_then(|p| p.mark()), cache, at_flush, reaches);
    let pieces = pieces.map(|area| area.answered_at_flush(answers.at_flush()));
    let block = align.get();
    let nonce = random_nonce()?;
    let at = persist_area(config);
    let first_frame = at
        .checked_add(u64::try_from(block).map_err(|_| LogError::Config("block"))?)
        .ok_or(LogError::Config("frame"))?;
    let key = match sealer.as_deref_mut() {
        Some(sealer) => Some(sealer.begin(1, first_frame)?),
        None => None,
    };
    let header = SegmentHeader {
        log: id,
        incarnation: 1,
        nonce,
        segment_bytes: config.segment_bytes,
        key,
    };
    let frame = format::Frame {
        log: id,
        incarnation: 1,
        nonce,
        sequence: 0,
        tail: 1,
        records: 0,
        sealed: sealer.is_some(),
    };
    let mut frame = frame.header(&[]).ok_or(LogError::Config("frame"))?.to_vec();
    let mut header = header.encode();
    if let Some(sealer) = sealer.as_deref() {
        let mac = sealer.mac(&header)?;
        header.extend_from_slice(&mac);
        let mac = sealer.mac_frame(&frame, 0)?;
        frame.extend_from_slice(&mac);
    }
    if header.len() > block {
        return Err(LogError::Config("a segment header past its block"));
    }
    let total = block
        .checked_add(frame.len())
        .ok_or(LogError::Config("frame"))?;
    let mut buf = AlignedBuf::zeroed(total, align).map_err(|e| LogError::Disk(e.into()))?;
    buf.extend_from_slice(&header)
        .map_err(|e| LogError::Disk(e.into()))?;
    buf.extend_zeros(block.saturating_sub(buf.len()))
        .map_err(|e| LogError::Disk(e.into()))?;
    buf.extend_from_slice(&frame)
        .map_err(|e| LogError::Disk(e.into()))?;
    let bytes = buf.padded().map_err(|e| LogError::Disk(e.into()))?;
    let written = u64::try_from(bytes.len()).map_err(|_| LogError::Config("frame"))?;
    // The first slot is written whole with zeros before its header and frame, under the same flush,
    // as every slot the file grows by is (`device::Frame::zero`): its later frames are overwrites.
    if file.fills_new_space() {
        let slot =
            usize::try_from(config.segment_bytes).map_err(|_| LogError::Config("segment"))?;
        let mut zeros = AlignedBuf::zeroed(slot, align).map_err(|e| LogError::Disk(e.into()))?;
        zeros.set_len(slot).map_err(|e| LogError::Disk(e.into()))?;
        file.write_all_at(zeros.as_slice(), at)?;
    }
    file.write_all_at(bytes, at)?;
    if let Some(pieces) = &pieces {
        write_mark(file, id, sealer.is_some(), pieces)?;
    }
    file.sync_data()?;
    let laid = Laid::of(device, cache, pieces.as_ref().and_then(Area::mark), answers);
    let state = State {
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
        ceiling: u32::MAX,
    };
    Ok((state, laid))
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

/// The file as an open finds it: its block, its length, the persist area before its segment
/// slots, and how many slots it has.
#[derive(Debug, Clone, Copy)]
struct Shape {
    id: u128,
    block: u64,
    len: u64,
    segment: u64,
    area: u64,
    slots: u32,
}

impl Shape {
    fn of<F: BlockFile>(file: &F, config: &Config, id: u128) -> Result<Self, LogError> {
        let block = block_of(file.layout_block())?;
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
        Ok(Self {
            id,
            block,
            len,
            segment,
            area,
            slots,
        })
    }

    /// Where slot `slot` begins.
    fn start_of(&self, slot: u32) -> Result<u64, LogError> {
        u64::from(slot)
            .checked_mul(self.segment)
            .and_then(|s| s.checked_add(self.area))
            .ok_or(LogError::Damaged("an offset past u64"))
    }

    /// Where slot `slot` ends.
    fn end_of(&self, slot: u32) -> Result<u64, LogError> {
        self.start_of(slot)?
            .checked_add(self.segment)
            .ok_or(LogError::Damaged("an offset past u64"))
    }

    /// Where slot `slot`'s first frame goes, after its header block.
    fn first_frame(&self, slot: u32) -> Result<u64, LogError> {
        self.start_of(slot)?
            .checked_add(self.block)
            .ok_or(LogError::Damaged("an offset past u64"))
    }
}

/// Every segment header that reads as this log's: each slot's incarnation, 0 where none reads,
/// and nonce, which slot holds each incarnation, and the highest.
struct Headers {
    incarnation: Vec<u64>,
    nonce: Vec<u64>,
    by_incarnation: HashMap<u64, u32>,
    highest: u64,
}

impl Headers {
    /// The slot of incarnation `inc`, which must be live.
    fn slot(&self, inc: u64) -> Result<u32, LogError> {
        self.by_incarnation
            .get(&inc)
            .copied()
            .ok_or(LogError::Damaged("a live segment is missing"))
    }

    /// Which segment the frames of incarnation `inc` in `slot` name.
    fn segment(&self, id: u128, slot: u32, inc: u64) -> Segment {
        Segment {
            log: id,
            incarnation: inc,
            nonce: self
                .nonce
                .get(usize::try_from(slot).unwrap_or(usize::MAX))
                .copied()
                .unwrap_or(0),
        }
    }

    /// Whether slot `slot`'s header read as this log's.
    fn reads(&self, slot: u32) -> bool {
        usize::try_from(slot)
            .ok()
            .and_then(|i| self.incarnation.get(i))
            .is_some_and(|&inc| inc != 0)
    }
}

/// Step 1: every segment header. In a sealed log each header's MAC is checked once its CRC holds,
/// and the session it opened is known from its first frame on.
fn headers<F: BlockFile>(
    file: &F,
    shape: &Shape,
    mut sealer: Option<&mut Sealer>,
) -> Result<Headers, LogError> {
    let align = file.layout_block();
    let count = usize::try_from(shape.slots).unwrap_or(0);
    let mut heads = Headers {
        incarnation: vec![0u64; count],
        nonce: vec![0u64; count],
        by_incarnation: HashMap::new(),
        highest: 0,
    };
    let mut block = AlignedBuf::zeroed(align.get(), align).map_err(|e| LogError::Disk(e.into()))?;
    block
        .set_len(align.get())
        .map_err(|e| LogError::Disk(e.into()))?;
    // This log's headers of the other kind than the open's: sealed ones read without keys, or
    // unsealed ones read with them. Counted, never taken, so the open's refusal can name why.
    let mut other_kind = 0u64;
    for slot in 0..shape.slots {
        let at = shape.start_of(slot)?;
        if at
            .checked_add(shape.block)
            .is_none_or(|end| end > shape.len)
        {
            continue;
        }
        file.read_exact_at(block.as_mut_slice(), at)?;
        let Some(h) = SegmentHeader::decode(block.as_slice()) else {
            continue;
        };
        if h.log != shape.id || h.segment_bytes != shape.segment || h.incarnation == 0 {
            continue;
        }
        match (sealer.as_deref_mut(), h.key) {
            (Some(sealer), Some(key)) => {
                let end = format::SEALED_SEGMENT_HEADER_LEN.saturating_add(MAC_LEN);
                let (Some(covered), Some(mac)) = (
                    block.as_slice().get(..format::SEALED_SEGMENT_HEADER_LEN),
                    block.as_slice().get(format::SEALED_SEGMENT_HEADER_LEN..end),
                ) else {
                    return Err(LogError::Tampered("a segment header cut short of its MAC"));
                };
                sealer.verify(covered, mac)?;
                sealer.found(h.incarnation, shape.first_frame(slot)?, &key)?;
            }
            (Some(_), None) | (None, Some(_)) => {
                other_kind = other_kind.saturating_add(1);
                continue;
            }
            (None, None) => {}
        }
        if heads.by_incarnation.insert(h.incarnation, slot).is_some() {
            return Err(LogError::Damaged("two segments share an incarnation"));
        }
        let i = usize::try_from(slot).unwrap_or(usize::MAX);
        if let (Some(e), Some(n)) = (heads.incarnation.get_mut(i), heads.nonce.get_mut(i)) {
            *e = h.incarnation;
            *n = h.nonce;
        }
    }
    // A log is sealed or not from its creation: headers of both kinds are bytes someone changed;
    // headers of the other kind alone are a log opened with the wrong expectation of keys.
    if other_kind > 0 {
        return Err(match (heads.by_incarnation.is_empty(), sealer.is_some()) {
            (false, true) => LogError::Tampered("an unsealed segment header in a sealed log"),
            (false, false) => LogError::Tampered("a sealed segment header in an unsealed log"),
            (true, true) => LogError::UnsealedWithKeys,
            (true, false) => LogError::SealedWithoutKeys,
        });
    }
    heads.highest = heads
        .by_incarnation
        .keys()
        .copied()
        .max()
        .ok_or(LogError::Foreign("no segment of this log"))?;
    Ok(heads)
}

/// Walks the frames of the segment of incarnation `inc`, visiting each that verifies: where
/// they stop, and whether what stops them is a frame of the segment that does not verify.
fn frames_of<F: BlockFile>(
    reader: &mut Reader<'_, F>,
    shape: &Shape,
    heads: &Headers,
    inc: u64,
    visit: &mut Visit<'_>,
) -> Result<(u64, bool), LogError> {
    let slot = heads.slot(inc)?;
    let which = heads.segment(shape.id, slot, inc);
    let end = shape.end_of(slot)?;
    let mut offset = shape.first_frame(slot)?;
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
}

/// 2. The last valid frame, in the highest segment that holds one.
///
/// Also the frames of the window past it, which are never taken, each by its offset and whether
/// it verified (mantle docs/design/raft-log.md §3.3).
fn last_frame<F: BlockFile>(
    reader: &mut Reader<'_, F>,
    shape: &Shape,
    heads: &Headers,
) -> Result<(Last, Vec<(u64, bool)>), LogError> {
    let mut inc = heads.highest;
    loop {
        let mut found: Option<Last> = None;
        let (stop, _) = frames_of(
            reader,
            shape,
            heads,
            inc,
            &mut |offset, header, _, padded| {
                found = Some(Last {
                    incarnation: inc,
                    offset,
                    sequence: header.sequence,
                    tail: header.tail,
                    after: offset.saturating_add(padded),
                });
                Ok(())
            },
        )?;
        // A frame is laid only once every frame through its overlap before it is applied, so a
        // valid frame past the point where the frames stop, by more than its overlap, proves
        // the frame there was acknowledged, whatever part of it no longer reads: its checksum,
        // or its magic, format or identity (06 §A3). One within its overlap, in the frame's
        // window, proves nothing of it (raft-log.md §3.3). A window lies only in the newest
        // segment: a segment's opening is laid once every frame before it is applied. Only with
        // none past it, or only its window, is the stop the log's end.
        let slot = heads.slot(inc)?;
        let end = shape.end_of(slot)?;
        let after = found.map(|f| f.sequence);
        let segment = heads.segment(shape.id, slot, inc);
        let window = reader.past_stop(segment, stop, end, after, inc == heads.highest)?;
        if let Some(last) = found {
            return Ok((last, window));
        }
        inc = inc
            .checked_sub(1)
            .filter(|i| heads.by_incarnation.contains_key(i))
            .ok_or(LogError::Damaged("no segment holds a valid frame"))?;
    }
}

/// A slot whose header no longer reads as this log's may hold a newer segment whose header
/// was damaged. A segment's header is written with its first frame and flushed with it, so a
/// valid frame of this log past that first frame, in a segment newer than any whose header
/// reads, proves the header was durable and is now damaged. The first frame alone is an
/// opening that may never have been flushed, the torn tail's case.
fn lost_headers<F: BlockFile>(
    reader: &mut Reader<'_, F>,
    shape: &Shape,
    heads: &Headers,
) -> Result<(), LogError> {
    for slot in 0..shape.slots {
        if heads.reads(slot) {
            continue;
        }
        let first = shape.first_frame(slot)?;
        if reader.newer_frame(shape.id, first, shape.end_of(slot)?, heads.highest)? {
            return Err(LogError::Damaged(
                "a segment's header does not read, but its frames do",
            ));
        }
    }
    Ok(())
}

/// What replaying the live segments rebuilt: each group, and the frames replayed.
struct Replay {
    groups: HashMap<u128, Replayed>,
    frames: u64,
}

/// 3. Replays the live segments, the tail to the highest, in sequence order.
fn replay_live<F: BlockFile>(
    reader: &mut Reader<'_, F>,
    shape: &Shape,
    heads: &Headers,
    last: &Last,
    mut sealer: Option<&mut Sealer>,
) -> Result<Replay, LogError> {
    let mut replay = Replay {
        groups: HashMap::new(),
        frames: 0,
    };
    let mut expected: Option<u64> = None;
    for inc in last.tail..=heads.highest {
        let slot = heads.slot(inc)?;
        let mut held = 0u64;
        let (end, invalid) = frames_of(
            reader,
            shape,
            heads,
            inc,
            &mut |offset, header, bytes, _padded| {
                if expected.is_some_and(|e| e != header.sequence) {
                    return Err(LogError::Damaged("a frame is missing"));
                }
                expected = header.sequence.checked_add(1);
                held = held.saturating_add(1);
                let at = Frame {
                    slot,
                    incarnation: inc,
                    offset,
                };
                replay_frame(&mut replay, at, header, bytes, sealer.as_deref_mut())
            },
        )?;
        // A segment's header is flushed with its first frame, so a live segment before the
        // last frame's holds a frame at least. With none, its frames were written over or
        // damaged where they begin: the torn opening of a later frame in its reused slot, where
        // the last frame, which freed it, no longer reads. Sequences are checked only from the
        // first frame replayed, so without this the segment's pieces vanished unreported
        // (mantle docs/design/raft-log.md §6, step 4).
        if held == 0 && inc < last.incarnation {
            return Err(LogError::Damaged("a live segment holds no frame"));
        }
        // Damage unless it lies after the last valid frame: then it is the torn tail.
        let before_last = inc < last.incarnation || (inc == last.incarnation && end < last.offset);
        if invalid && before_last {
            return Err(LogError::Damaged("an acknowledged frame does not verify"));
        }
    }
    if expected != last.sequence.checked_add(1) {
        return Err(LogError::Damaged("a frame is missing"));
    }
    Ok(replay)
}

/// Where a frame being replayed lies: its segment's slot and incarnation, and its offset.
#[derive(Debug, Clone, Copy)]
struct Frame {
    slot: u32,
    incarnation: u64,
    offset: u64,
}

/// Replays one verified frame. In a sealed log its key records are known first, in order, then
/// its proposals are opened, which the state keeps, and its entries' lengths are their
/// plaintexts': the log reads an entry's bytes back when asked, and opens them then.
fn replay_frame(
    replay: &mut Replay,
    at: Frame,
    header: &FrameHeader,
    bytes: &[u8],
    sealer: Option<&mut Sealer>,
) -> Result<(), LogError> {
    replay.frames = replay.frames.saturating_add(1);
    let payload = bytes
        .get(format::FRAME_HEADER_LEN..header.mac_at().unwrap_or(0))
        .ok_or(LogError::Damaged("a frame shorter than its header says"))?;
    let mut records = format::records(payload, header.records)
        .ok_or(LogError::Damaged("a verified frame does not decode"))?;
    let base = at
        .offset
        .checked_add(FRAME_HEADER_BYTES)
        .ok_or(LogError::Damaged("an offset past u64"))?;
    let tag = match sealer {
        Some(sealer) => {
            open_records(sealer, at.incarnation, base, &mut records)?;
            format::TAG_LEN
        }
        None => 0,
    };
    replay_records(&mut replay.groups, at.slot, base, records, tag)
}

/// A sealed frame's records made plain where the state keeps them: each key record's session known
/// from its offset on, then each proposal opened.
fn open_records(
    sealer: &mut Sealer,
    incarnation: u64,
    base: u64,
    records: &mut [Owned],
) -> Result<(), LogError> {
    let offset = |at: usize, past: usize| {
        u64::try_from(at.checked_add(past)?)
            .ok()
            .and_then(|at| base.checked_add(at))
    };
    for record in records.iter_mut() {
        match record {
            Owned::Key { at, frame } => {
                let from = offset(*at, 0).ok_or(LogError::Damaged("an offset past u64"))?;
                sealer.found(incarnation, from, frame)?;
            }
            Owned::Proposal { group, proposal } => {
                let from = offset(proposal.at, format::PROPOSAL_FIELDS_LEN)
                    .ok_or(LogError::Damaged("an offset past u64"))?;
                proposal.bytes = sealer.open(
                    incarnation,
                    from,
                    *group,
                    proposal.index,
                    proposal.term,
                    &proposal.bytes,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// The groups replay rebuilt, as they run: those whose records run unbroken, those with a
/// record missing, which recover from their peers, and those an earlier recovery fenced as
/// damaged, with where the fence is.
type Finished = (HashMap<u128, state::Group>, Vec<u128>, HashMap<u128, Place>);

/// 4. The groups as they run.
fn finish(replayed: HashMap<u128, Replayed>) -> Finished {
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
    (groups, damaged, fenced)
}

/// Each segment slot's live pieces and bytes, and the bytes its frames take.
fn live_of(
    slots: usize,
    groups: &HashMap<u128, state::Group>,
    fenced: &HashMap<u128, Place>,
) -> Live {
    let mut live = Live::with_slots(slots);
    for g in groups.values() {
        for (place, bytes) in g.pieces() {
            live.add(place, bytes);
        }
    }
    for &place in fenced.values() {
        live.add(place, state::DAMAGED_BYTES);
    }
    live
}

/// Step 5: the block where the next frame goes is erased; returns its offset. A frame there was never
/// acknowledged but may be partly durable, its header whole while its last sectors, or the
/// file's end, are not; left alone, a later crash could complete it with the zeros of a newer
/// frame's padding and bring it back. Erased, it can neither return nor be taken for damage once
/// frames follow it elsewhere.
///
/// The restore's frame takes the lost frame's sequence, so its persist record goes in the lost
/// frame's slot, before its flush, and may tear there while the frame does not become durable.
/// The lost frame's record, and whether it was confirmed, are copied to the other slot first
/// and flushed with the erasure (`copy`, when there is a restore): what restores the lost frame
/// then survives until the restore itself is durable. The other slot holds nothing recovery
/// still needs: the record of the frame before, which reads whole, or of the frame after, which
/// never became durable and so was never answered, and whose confirmation of the lost frame the
/// copy carries.
fn erase<F: BlockFile>(
    file: &F,
    (shape, area): (&Shape, &Area),
    heads: &Headers,
    (last, window): (&Last, &[(u64, bool)]),
    copy: Option<RecordCopy>,
    sealer: Option<&Sealer>,
) -> Result<u64, LogError> {
    let mut flush = false;
    if let Some((at, record)) = copy {
        area.write(file, &record, at, sealer)?;
        flush = true;
    }
    let head_slot = heads.slot(heads.highest)?;
    let head_offset = if heads.highest == last.incarnation {
        last.after
    } else {
        shape.first_frame(head_slot)?
    };
    // Where the head is a newer segment than the last frame's, an opening whose frame never
    // became durable, the block after the last frame is erased as well. A frame there was
    // flushed before that opening was written, and no longer reads: the lost frame. Left in
    // the tail, a sweep of that segment took it for a live frame damaged, and the restore's
    // own frame fenced the log (mantle docs/design/raft-log.md §6, step 6).
    let head_end = shape.end_of(head_slot)?;
    let mut erasures = vec![(head_offset, head_end)];
    if heads.highest != last.incarnation {
        erasures.push((last.after, shape.end_of(heads.slot(last.incarnation)?)?));
    }
    // A frame of the window past the last frame, in the head segment, keeps its segment's
    // identity and a checksum that holds: left whole, the walk takes it once a later frame ends
    // where it begins, and one torn may be completed by a later write torn over it. Its first
    // block erased, it never reads again (raft-log.md §3.3).
    erasures.extend(window.iter().map(|&(at, _)| (at, head_end)));
    let align = file.layout_block();
    let mut zeros = AlignedBuf::zeroed(align.get(), align).map_err(|e| LogError::Disk(e.into()))?;
    zeros
        .set_len(align.get())
        .map_err(|e| LogError::Disk(e.into()))?;
    for (at, end) in erasures {
        if at.checked_add(shape.block).is_some_and(|stop| stop <= end) {
            file.write_all_at(zeros.as_slice(), at)?;
            flush = true;
        }
    }
    if flush {
        file.sync_data()?;
    }
    Ok(head_offset)
}

pub(crate) fn open<F: BlockFile>(
    file: &F,
    config: &Config,
    id: u128,
    mut sealer: Option<&mut Sealer>,
    at_flush: AtFlush,
) -> Result<(State, Recovery, Vec<Restore>), LogError> {
    let align = file.layout_block();
    let sealed = sealer.is_some();
    check(config, align, sealed)?;
    let device = file.atomic_unit()?;
    // The device's write cache beside its atomic write unit: with the records' laying, how the
    // log answers its frames (`Answers`, durability.md §7.5).
    let cache = file.write_cache()?;
    let shape = Shape::of(file, config, id)?;
    let heads = headers(file, &shape, sealer.as_deref_mut())?;
    let mac = sealer.as_deref().map(Sealer::frame_mac);
    let mut reader = Reader::new(file, file.layout_block(), shape.segment, mac)?;
    let (last, window) = last_frame(&mut reader, &shape, &heads)?;
    lost_headers(&mut reader, &shape, &heads)?;
    if last.tail > last.incarnation {
        return Err(LogError::Damaged(
            "a frame names a tail after its own segment",
        ));
    }
    let replay = replay_live(&mut reader, &shape, &heads, &last, sealer.as_deref_mut())?;
    let (groups, mut damaged, fenced) = finish(replay.groups);
    // 4a. The persist area as its mark says it is laid, or laid again first: a log of whole
    // records moves to pieces of the device's unit where its area holds them, carrying its
    // records (durability.md §7.7).
    let area = laid_area(file, config, (id, sealer.as_deref()), device.unit())?;
    // A frame of the window past the last frame proves nothing of the frame after the last, but
    // a record that confirms that frame proves it was applied: durable, and damaged since. A
    // record of it or of a frame past it that says it was answered at its own flush, which a
    // log of depth past one never does, leaves nothing to tell a crash from damage by, so it is
    // damage too (raft-log.md §3.3). The area read is the one laid above, which carried every
    // record where it laid the area again.
    if window.iter().any(|&(_, verified)| verified) {
        let gap = last
            .sequence
            .checked_add(1)
            .ok_or(LogError::Damaged("sequences past u64"))?;
        let records = records(file, &area, id, sealer.as_deref())?;
        if records
            .iter()
            .any(|(_, p)| p.confirms >= gap || (p.sequence >= gap && p.at_flush))
        {
            return Err(LogError::Damaged(
                "a frame that may have been answered does not verify",
            ));
        }
    }
    // A log that answers frames at their flush sets its mark's bit, and flushes it, before the
    // first such frame, the restore included: a binary from before then refuses the log rather
    // than cut such a frame as the torn tail (durability.md §7.7, step 4). A mark that has it
    // keeps it, whatever the log answers now.
    let answers = Answers::of(area.mark(), cache, at_flush, file.writes_reach_device());
    let area = if answers.at_flush() && !area.at_flush {
        let marked = area.answered_at_flush(true);
        write_mark(file, id, sealed, &marked)?;
        file.sync_data()?;
        marked
    } else {
        area
    };
    // 4b. A frame after the last valid one may have been flushed and damaged since: its
    // persist record, written in the same flush, says what it held (AGL+18 §3.3.3).
    let (mut restores, copy, tail) = restores(
        file,
        &area,
        id,
        last.sequence,
        (&groups, &fenced),
        &mut damaged,
        sealer.as_deref(),
    )?;
    damaged.sort_unstable();
    damaged.dedup();
    // A group newly found damaged is fenced by a record written before the log serves
    // anyone, so that the fence outlives what showed the damage: the frame is overwritten
    // by the next, and a record missing may be swept away (mantle audit S16).
    restores.extend(damaged.iter().map(|&group| Restore {
        group,
        update: crate::Update::default(),
        uncertain: None,
        damaged: true,
    }));
    let live = live_of(heads.incarnation.len(), &groups, &fenced);
    let copy = copy.filter(|_| !restores.is_empty());
    let head_offset = erase(
        file,
        (&shape, &area),
        &heads,
        (&last, &window),
        copy,
        sealer.as_deref(),
    )?;
    let opened = Opened {
        groups,
        damaged,
        fenced,
        live,
        head_offset,
        frames: replay.frames,
    };
    let laid = Laid::of(device, cache, area.mark(), answers);
    let (state, recovery) =
        opened.into_state(heads, &last, shape.slots, &restores, (tail, laid))?;
    Ok((state, recovery, restores))
}

/// What an open found, before it is the log's state.
struct Opened {
    groups: HashMap<u128, state::Group>,
    damaged: Vec<u128>,
    fenced: HashMap<u128, Place>,
    live: Live,
    head_offset: u64,
    frames: u64,
}

impl Opened {
    /// The log's state, its head after the last frame, and what the open reports.
    fn into_state(
        self,
        heads: Headers,
        last: &Last,
        slots: u32,
        restores: &[Restore],
        (tail, laid): (Tail, Laid),
    ) -> Result<(State, Recovery), LogError> {
        let highest = heads.highest;
        let head_slot = heads.slot(highest)?;
        let head_nonce = heads.segment(0, head_slot, highest).nonce;
        let live_segments = (last.tail..=highest)
            .filter_map(|inc| heads.by_incarnation.get(&inc).copied())
            .collect();
        let free = (0..slots)
            .filter(|slot| {
                let inc = heads
                    .incarnation
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
            damaged: self
                .damaged
                .iter()
                .map(|&group| (group, None))
                .chain(self.fenced.iter().map(|(&group, &at)| (group, Some(at))))
                .collect(),
            groups: self.groups,
            live: self.live,
            segments: Segments {
                incarnation: heads.incarnation,
                nonce: heads.nonce,
                live: live_segments,
                free,
            },
            head: Head {
                slot: head_slot,
                incarnation: highest,
                nonce: head_nonce,
                offset: self.head_offset,
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
            ceiling: u32::MAX,
        };
        let mut all: Vec<u128> = self
            .damaged
            .into_iter()
            .chain(self.fenced.into_keys())
            .collect();
        all.sort_unstable();
        let recovery = Recovery {
            frames: self.frames,
            damaged: all,
            restored,
            tail,
            laid,
        };
        Ok((state, recovery))
    }
}

/// What the frame after the last valid one held, from its persist record, as the updates that
/// restore it. A persist record is written in its frame's flush, so it may survive a frame
/// torn by a crash as well as one damaged since its flush; what tells the two apart is a
/// confirmation that the frame was flushed: the next frame's record, which is written only
/// after that flush, or the one the writer writes when no frame follows at once. A frame's
/// updates are answered once it is confirmed, or at its own flush where its record says so
/// (mantle docs/design/durability.md §7.1).
///
/// A confirmed frame was acknowledged, and damaged since (AGL+18 §3.3.3). So may an unconfirmed
/// one whose record says it was answered at its flush have been: torn by a crash or damaged since,
/// which nothing on the device tells apart, it is restored as a confirmed one is, and its group
/// decides from the mark (§7.2, §7.3). The record of either kind says so: the frame's own, and the
/// copy an open restoring the frame made, so that a restore torn after its record landed is
/// restored again the same way. Each of its groups
/// gets back the start and hard state the frame left, its entries cut back to where the
/// frame's began, and, where the frame wrote entries, the mark that the log may lack them; a
/// group whose frame held proposals, which a persist record does not carry, is damaged and
/// recovers from its peers. An unconfirmed frame answered once confirmed was never acknowledged,
/// and is the torn tail, but for its term and vote, which are kept, since raising a term or keeping a vote is always
/// safe. Its commit is not kept, since it may name an entry only the frame held. A fence the
/// frame put on a damaged group is kept, confirmed or not.
///
/// The frame's record is the one in its own slot or, where that one no longer reads, the copy
/// in the other slot that an open restoring the frame made before the restore's frame took the
/// lost frame's sequence, and so its slot (§6). Where the restore's frame tore after its record
/// landed, that record is the one read: it holds the restore, and restoring from it leaves
/// each group as restoring from the lost frame's record does. Also returns the copy to make
/// before a restore is written, when the record read is in its own slot: where the copy goes,
/// and the record saying whether the frame was confirmed; and what the records left of the
/// frame, the open's report of it.
fn restores<F: BlockFile>(
    file: &F,
    area: &Area,
    id: u128,
    last: u64,
    (groups, fenced): (&HashMap<u128, state::Group>, &HashMap<u128, Place>),
    damaged: &mut Vec<u128>,
    sealer: Option<&Sealer>,
) -> Result<(Vec<Restore>, Option<RecordCopy>, Tail), LogError> {
    let Some(sequence) = last.checked_add(1) else {
        return Ok((Vec::new(), None, Tail::Torn));
    };
    let records = records(file, area, id, sealer)?;
    let confirmed = records.iter().any(|(_, p)| p.confirms >= sequence);
    // Only the frame's own records say how it was answered: the other slot's record of the frame
    // before says nothing of this one, so a crash between two frames never marks the earlier
    // (durability.md §7.6).
    let at_flush = records
        .iter()
        .any(|(_, p)| p.sequence == sequence && p.at_flush);
    let own = area.at(sequence);
    let other = area.other(own);
    let chosen = [own, other].into_iter().find_map(|from| {
        records
            .iter()
            .find(|(at, p)| *at == from && p.sequence == sequence)
            .map(|(_, p)| (from, p.clone()))
    });
    let Some((from, record)) = chosen else {
        return Ok((Vec::new(), None, Tail::Torn));
    };
    let tail = if confirmed {
        Tail::Lost { sequence }
    } else {
        Tail::Undetermined { sequence, at_flush }
    };
    // Answered, by its confirmation or at its flush: restored and marked, not cut.
    let answered = confirmed || at_flush;
    let copy = (from == own).then(|| {
        (
            other,
            format::Persist {
                confirms: if confirmed { sequence } else { record.confirms },
                ..record.clone()
            },
        )
    });
    let empty = state::Group::default();
    let mut out = Vec::new();
    for p in record.groups {
        if damaged.contains(&p.group) || (fenced.contains_key(&p.group) && !p.removed) {
            continue;
        }
        // A fence the frame wrote is kept whether or not the frame was answered: marking a
        // group damaged is always safe, and the frame it fenced may be gone.
        if p.damaged || (answered && p.proposals) {
            damaged.push(p.group);
            continue;
        }
        let g = groups.get(&p.group).unwrap_or(&empty);
        let restore = if answered {
            restore_confirmed(&p, g)?
        } else {
            restore_torn(&p, g)
        };
        out.extend(restore);
    }
    Ok((out, copy, tail))
}

/// The records that read in the area's slots, each with its slot's offset.
fn records<F: BlockFile>(
    file: &F,
    area: &Area,
    id: u128,
    sealer: Option<&Sealer>,
) -> Result<Vec<(u64, format::Persist)>, LogError> {
    let mut records = Vec::with_capacity(2);
    for at in [area.at(0), area.at(1)] {
        if let Some(p) = area.read(file, at, id, sealer)? {
            records.push((at, p));
        }
    }
    Ok(records)
}

/// An unconfirmed frame's group: the torn tail, keeping only a later term or a vote given, at
/// the group's own commit.
fn restore_torn(p: &format::Persisted, g: &state::Group) -> Option<Restore> {
    let h = p.hard_state?;
    let current = g.hard.map(|(h, _)| h);
    let later = current.is_none_or(|c| h.term > c.term || (h.term == c.term && c.vote == 0));
    later.then(|| Restore {
        group: p.group,
        update: crate::Update {
            hard_state: Some(format::HardState {
                commit: current.map_or(0, |c| c.commit),
                // Nor its promise: none was answered for on it, unconfirmed (hyper-raft's R-8).
                held: current.and_then(|c| c.held),
                ..h
            }),
            ..crate::Update::default()
        },
        uncertain: None,
        damaged: false,
    })
}

/// An answered frame's group, confirmed or answered at its flush, the frame damaged or torn since
/// it was acknowledged: its removal, or the
/// start and hard state the frame left, its entries cut back to where the frame's began, and,
/// where the frame wrote entries, the mark that the log may lack them. `None` where that
/// changes nothing.
fn restore_confirmed(p: &format::Persisted, g: &state::Group) -> Result<Option<Restore>, LogError> {
    if p.removed {
        return Ok(Some(Restore {
            group: p.group,
            update: crate::Update {
                remove: true,
                ..crate::Update::default()
            },
            uncertain: None,
            damaged: false,
        }));
    }
    let current = g.hard.map(|(h, _)| h);
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
        uncertain = lacking(uncertain, w)?;
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
        return Ok(None);
    }
    Ok(Some(Restore {
        group: p.group,
        update,
        uncertain,
        damaged: false,
    }))
}

/// The mark `mark` widened to the entries `w` a lost frame wrote, if it wrote any.
fn lacking(
    mark: Option<format::Start>,
    w: format::Written,
) -> Result<Option<format::Start>, LogError> {
    if w.count == 0 {
        return Ok(mark);
    }
    let lost = format::Start {
        index: w
            .first
            .checked_add(w.count)
            .and_then(|end| end.checked_sub(1))
            .ok_or(LogError::Damaged("an index past u64"))?,
        term: w.term,
    };
    Ok(Some(merge(mark, lost)))
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
fn replay_records(
    groups: &mut HashMap<u128, Replayed>,
    slot: u32,
    base: u64,
    records: Vec<Owned>,
    tag: usize,
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
                insert(g, entries, &place, tag)?;
                g.reach(last);
            }
            Owned::Relocated { group, entries, .. } => {
                insert(groups.entry(group).or_default(), entries, &place, tag)?;
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
            // A sealed log's session key: what opens the records after it, not a group's piece.
            Owned::Key { .. } => {}
            Owned::Proposal { group, proposal } => {
                groups.entry(group).or_default().proposals.insert(
                    proposal.index,
                    state::Proposal {
                        term: proposal.term,
                        place: place(proposal.at)?,
                        bytes: proposal.bytes,
                    },
                );
            }
            Owned::Removed { group } => {
                groups.remove(&group);
            }
            Owned::Uncertain { at, group, mark } => {
                groups.entry(group).or_default().uncertain = Some((mark, place(at)?));
            }
            Owned::Released { at, group, through } => {
                groups
                    .entry(group)
                    .or_default()
                    .release(through, place(at)?);
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
    tag: usize,
) -> Result<(), LogError> {
    for e in entries {
        let plain = e
            .bytes
            .len()
            .checked_sub(tag)
            .ok_or(LogError::Damaged("a sealed entry shorter than its tag"))?;
        let len = u32::try_from(plain).map_err(|_| LogError::Damaged("an entry past u32"))?;
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

#[cfg(test)]
mod tests {
    use hyper_block::block::BlockFile;
    use hyper_block::buf::Alignment;
    use hyper_block::sim::{Crash, SimFile};

    use super::{Area, check_record, laid_area, marked, persist_area, persist_slot, write_mark};
    use crate::format::{self, MAC_LEN, Persist, Persisted, Written};
    use crate::seal::Sealer;
    use crate::{Answers, AtFlush, Config, LogError, Waits};

    const ID: u128 = 0x7069_6563_6573;

    fn config(segment_bytes: u64, max_groups: usize) -> Config {
        Config {
            segment_bytes,
            max_segments: 8,
            max_groups,
            group_entries: 1 << 16,
            group_bytes: 1 << 24,
            group_cache: 1 << 10,
            queue_submissions: 256,
            waits: Waits::Never,
        }
    }

    fn align(bytes: usize) -> Alignment {
        Alignment::new(bytes).unwrap()
    }

    fn sealer() -> Sealer {
        let _ = hyper_seal::lock_keys(256);
        let sealing = crate::seal::Sealing {
            parent: hyper_seal::keys::WrappingKey::new(
                hyper_seal::keys::KeyId([3; 16]),
                0,
                hyper_seal::Secret32::from_bytes(&[3; 32]).unwrap(),
            ),
            auth: hyper_seal::Secret32::from_bytes(&[4; 32]).unwrap(),
        };
        Sealer::new(sealing, ID).unwrap()
    }

    /// A simulated device of `block`-byte transfers and `sector`-byte atomic writes, holding
    /// `image`, or the zeros of `cfg`'s persist area, flushed.
    fn device(
        block: usize,
        sector: usize,
        seed: u64,
        image: Option<&[u8]>,
        cfg: &Config,
    ) -> SimFile {
        let file = SimFile::new(align(block), align(sector), seed).unwrap();
        let zeros = vec![0u8; usize::try_from(persist_area(cfg)).unwrap()];
        let bytes = image.unwrap_or(&zeros);
        let mut buf = hyper_block::buf::AlignedBuf::zeroed(bytes.len(), align(block)).unwrap();
        buf.extend_from_slice(bytes).unwrap();
        file.write_all_at(buf.padded().unwrap(), 0).unwrap();
        file.sync_data().unwrap();
        file
    }

    fn record(sequence: u64, groups: u64) -> Persist {
        Persist {
            log: ID,
            sequence,
            confirms: sequence - 1,
            groups: (0..groups)
                .map(|g| Persisted {
                    group: u128::from(g) + 1,
                    entries: Some(Written {
                        first: sequence,
                        count: 1,
                        term: sequence,
                    }),
                    ..Persisted::default()
                })
                .collect(),
            sealed: false,
            at_flush: false,
        }
    }

    /// Each slot in pieces starts at an offset the larger of the unit and the file's block
    /// divides, and its length is a multiple of it, so a write meets a direct file's alignment
    /// where that is the larger (512 against 4,096) while each piece stays one unit; the first
    /// lies past the two whole slots, which keep their offsets.
    #[test]
    fn the_slots_in_pieces_meet_the_unit_and_the_files_alignment() {
        let cfg = config(1 << 20, 100);
        for (unit, block) in [
            (512, 4096),
            (4096, 512),
            (4096, 4096),
            (512, 512),
            (16384, 4096),
        ] {
            let (unit, block) = (align(unit), align(block));
            let whole = Area::whole(&cfg, block, false).unwrap();
            let slot = persist_slot(&cfg, block, false).unwrap();
            assert_eq!((whole.at(0), whole.at(1), whole.mark()), (0, slot, None));
            let area = Area::in_pieces(&cfg, block, unit, false, false)
                .unwrap()
                .unwrap();
            let larger = unit.max(block);
            let pieces = area.pieces().unwrap();
            assert_eq!(pieces.unit(), unit, "a piece is one unit");
            for at in [area.at(0), area.at(1)] {
                assert!(larger.is_aligned_u64(at), "{at} at {unit:?} {block:?}");
                assert!(at >= 2 * slot, "past the whole slots");
            }
            assert!(larger.is_aligned_u64(area.len));
            assert!(area.len >= pieces.len(100).unwrap() as u64);
            assert_eq!(area.at(1), area.at(0) + area.len);
        }
        // An area too small for the slots in pieces keeps whole records; a unit that holds no
        // row is refused.
        let small = config(4 * 4096, 80);
        assert_eq!(
            Area::in_pieces(&small, align(4096), align(4096), false, false).unwrap(),
            None
        );
        assert!(matches!(
            Area::in_pieces(&cfg, align(4096), align(128), false, false),
            Err(LogError::Config(_))
        ));
    }

    /// A binary from before format 5 reads the mark at the area's start as a whole persist
    /// record of the other kind, of the log's ID, and refuses the log there by its format check,
    /// rather than read the pieces after it as no record; a sealed log's mark is an unsealed
    /// record, which such a binary refuses as tampering. A binary of format 5 from before the
    /// block was recorded read a mark only of sequence and confirmation 0, and refuses one that
    /// records the block the same way; its own marks read here, their block unrecorded.
    #[test]
    fn a_binary_before_pieces_refuses_a_log_in_pieces() {
        let mark = format::Mark {
            unit: align(4096),
            block: Some(align(512)),
            at_end: true,
            at_flush: false,
        };
        for sealed in [false, true] {
            let bytes = format::pieces_mark(ID, sealed, mark).unwrap();
            assert_eq!(format::pieces_marked(&bytes, ID, sealed), Some(mark));
            assert_eq!(format::pieces_marked(&bytes, ID + 1, sealed), None);
            assert_eq!(format::pieces_marked(&bytes, ID, !sealed), None);
            let read = Persist::decode(&bytes).expect("a whole record to a binary before pieces");
            assert_eq!((read.log, read.sealed, read.groups.len()), (ID, !sealed, 0));
            assert_eq!((read.sequence, read.confirms), (1, 512));
            if sealed {
                let refused = check_record(&read, &bytes, Some(&sealer()));
                assert!(matches!(refused, Err(LogError::Tampered(_))), "{refused:?}");
            } else {
                let refused = check_record(&read, &bytes, None);
                assert!(matches!(refused, Err(LogError::Foreign(_))), "{refused:?}");
            }
        }
        // The mark of a binary of format 5 from before the block was recorded: its slots lie
        // past the whole ones, by the file's block.
        let mut old = format::pieces_mark(ID, false, mark).unwrap();
        old[24..40].fill(0);
        let crc = crate::codec::crc32c(&old[..44]);
        old[44..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            format::pieces_marked(&old, ID, false),
            Some(format::Mark {
                block: None,
                at_end: false,
                ..mark
            })
        );
        // A whole record is no mark.
        let whole = record(2, 3).encode().unwrap();
        assert_eq!(format::pieces_marked(&whole, ID, false), None);
    }

    /// The open's copy of a lost frame's record goes into the other slot, which may hold the pieces
    /// of a later frame's longer record that never became durable: the copy fills the slot, so
    /// none of those pieces is left to read as the slot's newest record and hide it.
    #[test]
    fn a_copy_over_a_later_frames_longer_record_reads() {
        let cfg = config(16 * 4096, 64);
        let file = SimFile::new(align(4096), align(512), 7).unwrap();
        let area = Area::in_pieces(&cfg, file.layout_block(), align(512), false, false)
            .unwrap()
            .unwrap();
        let mut zeros =
            hyper_block::buf::AlignedBuf::zeroed(16 * 4096, file.layout_block()).unwrap();
        zeros.set_len(16 * 4096).unwrap();
        file.write_all_at(zeros.as_slice(), 0).unwrap();
        let later = record(3, 40);
        let copy = Persist {
            confirms: 2,
            ..record(2, 1)
        };
        area.write(&file, &later, area.at(3), None).unwrap();
        area.write(&file, &copy, area.at(3), None).unwrap();
        file.sync_data().unwrap();
        assert_eq!(area.read(&file, area.at(3), ID, None).unwrap(), Some(copy));
    }

    /// Power fails while a record in pieces of the simulated device's sector overwrites the one
    /// before in its slot: the crash keeps each unflushed sector or not, so the slot tears only at
    /// unit boundaries. Every outcome reads: the old record, the new one, or none, a torn record
    /// being no record (the torn tail, `Tail::Torn`), and never damage.
    #[test]
    fn a_crash_tears_pieces_at_unit_boundaries_and_a_torn_record_is_none() {
        let cfg = config(16 * 4096, 64);
        let mut seen = [0u32; 3];
        for seed in 0..64 {
            let file = SimFile::new(align(4096), align(512), seed).unwrap();
            let unit = file.atomic_unit().unwrap();
            assert!(unit.stated());
            let area = Area::in_pieces(&cfg, file.layout_block(), unit.unit(), false, false)
                .unwrap()
                .unwrap();
            let (old, new) = (record(2, 20), record(4, 20));
            assert!(area.pieces().unwrap().count(20).unwrap() > 1);
            // The file holds its persist area, as a log's does.
            let zeros = vec![0u8; usize::try_from(cfg.segment_bytes).unwrap()];
            let mut buf =
                hyper_block::buf::AlignedBuf::zeroed(zeros.len(), file.layout_block()).unwrap();
            buf.extend_from_slice(&zeros).unwrap();
            file.write_all_at(buf.as_slice(), 0).unwrap();
            area.write(&file, &old, area.at(2), None).unwrap();
            file.sync_data().unwrap();
            area.write(&file, &new, area.at(4), None).unwrap();
            file.crash(Crash::Random).unwrap();
            match area.read(&file, area.at(4), ID, None).unwrap() {
                Some(r) if r == old => seen[0] += 1,
                Some(r) if r == new => seen[1] += 1,
                Some(r) => panic!("seed {seed}: a record never written: {r:?}"),
                None => seen[2] += 1,
            }
        }
        assert!(seen.iter().all(|&n| n > 0), "old, new, torn: {seen:?}");
    }

    /// A sealed log's record in pieces (format 6) carries a MAC in each piece and reads back
    /// whole. A piece changed with its CRC recomputed, so that only its MAC tells, is tampering,
    /// as is an unsealed piece in its place; one changed without its CRC is damage, as in an
    /// unsealed log; and the pieces read under another log's key are tampering.
    #[test]
    fn a_sealed_record_in_pieces_carries_a_mac_in_each_and_a_forged_piece_is_tampering() {
        let cfg = config(16 * 4096, 64);
        let sealer = sealer();
        let file = device(4096, 512, 11, None, &cfg);
        let area = Area::in_pieces(&cfg, align(4096), align(512), true, false)
            .unwrap()
            .unwrap();
        let sealed = Persist {
            sealed: true,
            ..record(2, 9)
        };
        assert!(area.pieces().unwrap().count(9).unwrap() > 1);
        area.write(&file, &sealed, area.at(2), Some(&sealer))
            .unwrap();
        file.sync_data().unwrap();
        assert_eq!(
            area.read(&file, area.at(2), ID, Some(&sealer)).unwrap(),
            Some(sealed.clone())
        );
        let image = file.durable_image().unwrap();
        let at = usize::try_from(area.at(2)).unwrap();
        let mac_at = format::piece_mac_at(&image[at..]).unwrap();
        assert!(
            image[at + mac_at..at + mac_at + MAC_LEN]
                .iter()
                .any(|&b| b != 0)
        );
        // The second piece's first row's group, changed, its CRC recomputed.
        let second = at + 512;
        let covered = format::piece_mac_at(&image[second..]).unwrap() - 4;
        let mut forged = image.clone();
        forged[second + format::PIECE_HEADER_LEN] ^= 1;
        let crc = crate::codec::crc32c(&forged[second..second + covered]);
        forged[second + covered..second + covered + 4].copy_from_slice(&crc.to_le_bytes());
        let file = device(4096, 512, 12, Some(&forged), &cfg);
        assert!(matches!(
            area.read(&file, area.at(2), ID, Some(&sealer)),
            Err(LogError::Tampered(_))
        ));
        // Changed without its CRC: damage, never a torn write, since a piece is written whole.
        let mut damaged = image.clone();
        damaged[second + format::PIECE_HEADER_LEN] ^= 1;
        let file = device(4096, 512, 13, Some(&damaged), &cfg);
        assert!(matches!(
            area.read(&file, area.at(2), ID, Some(&sealer)),
            Err(LogError::Damaged(_))
        ));
        // An unsealed record's piece in a sealed log's slot.
        let unsealed = format::Pieces::new(align(512), false)
            .unwrap()
            .encode(&record(2, 1))
            .unwrap();
        let mut planted = image.clone();
        planted[second..second + 512].copy_from_slice(&unsealed);
        let file = device(4096, 512, 14, Some(&planted), &cfg);
        assert!(matches!(
            area.read(&file, area.at(2), ID, Some(&sealer)),
            Err(LogError::Tampered(_))
        ));
        // Another key's MACs.
        let other = {
            let _ = hyper_seal::lock_keys(256);
            let sealing = crate::seal::Sealing {
                parent: hyper_seal::keys::WrappingKey::new(
                    hyper_seal::keys::KeyId([3; 16]),
                    0,
                    hyper_seal::Secret32::from_bytes(&[3; 32]).unwrap(),
                ),
                auth: hyper_seal::Secret32::from_bytes(&[5; 32]).unwrap(),
            };
            Sealer::new(sealing, ID).unwrap()
        };
        let file = device(4096, 512, 15, Some(&image), &cfg);
        assert!(matches!(
            area.read(&file, area.at(2), ID, Some(&other)),
            Err(LogError::Tampered(_))
        ));
    }

    /// The area of `cfg` in pieces of 4,096 bytes past the whole slots, by a 4,096-byte block, its
    /// mark and two records written and flushed on `file`; the records.
    fn laid_in_4096(file: &SimFile, cfg: &Config, sealer: Option<&Sealer>) -> (Persist, Persist) {
        let sealed = sealer.is_some();
        let area = Area::in_pieces(cfg, align(4096), align(4096), sealed, false)
            .unwrap()
            .unwrap();
        let (even, odd) = (
            Persist {
                sealed,
                ..record(2, 50)
            },
            Persist {
                sealed,
                ..record(3, 7)
            },
        );
        area.write(file, &even, area.at(2), sealer).unwrap();
        area.write(file, &odd, area.at(3), sealer).unwrap();
        file.sync_data().unwrap();
        write_mark(file, ID, sealed, &area).unwrap();
        file.sync_data().unwrap();
        (even, odd)
    }

    /// Power fails at each write and flush of an open's re-lay of a persist area in pieces of
    /// 4,096 bytes on a device whose atomic write unit is 512, each unflushed sector kept or not:
    /// after the crash the mark names the old area or the new, and the area it names holds both
    /// records; the next open lays the area in 512-byte pieces at the area's end, both records
    /// in their slots. Sealed or not.
    #[test]
    fn a_relay_across_a_crash_at_each_of_its_steps_keeps_the_old_records_or_the_new() {
        let cfg = config(16 * 4096, 64);
        for sealed in [false, true] {
            let sealer = sealed.then(sealer);
            let sealer = sealer.as_ref();
            let mut seen = [0u32; 2];
            // The re-lay's two slot writes, its flush, the mark and its flush; then one step
            // past, where the re-lay is whole.
            for ops in 0..=5 {
                for seed in 0..4 {
                    let file = device(4096, 512, ops * 8 + seed, None, &cfg);
                    let (even, odd) = laid_in_4096(&file, &cfg, sealer);
                    file.inject(hyper_block::sim::Fault::PowerCut { ops })
                        .unwrap();
                    let laid = laid_area(&file, &cfg, (ID, sealer), align(512));
                    assert_eq!(laid.is_ok(), ops == 5, "ops {ops}: {laid:?}");
                    file.crash(Crash::Random).unwrap();
                    file.clear_faults().unwrap();
                    let mark = marked(&file, ID, sealed).unwrap().unwrap();
                    seen[usize::from(mark.unit == align(512))] += 1;
                    let area = Area::of(&cfg, align(4096), sealed, Some(mark)).unwrap();
                    for (sequence, record) in [(2, &even), (3, &odd)] {
                        assert_eq!(
                            area.read(&file, area.at(sequence), ID, sealer)
                                .unwrap()
                                .as_ref(),
                            Some(record),
                            "ops {ops} seed {seed}: {mark:?}"
                        );
                    }
                    let area = laid_area(&file, &cfg, (ID, sealer), align(512)).unwrap();
                    assert_eq!(
                        area.mark().map(|m| (m.unit, m.at_end)),
                        Some((align(512), true))
                    );
                    for (sequence, record) in [(2, &even), (3, &odd)] {
                        assert_eq!(
                            area.read(&file, area.at(sequence), ID, sealer)
                                .unwrap()
                                .as_ref(),
                            Some(record)
                        );
                    }
                }
            }
            assert!(seen.iter().all(|&n| n > 0), "old, new: {seen:?}");
        }
    }

    /// A piece of 4,096 bytes torn by a crash on a device whose unit is 512 fails its checksum,
    /// which on a device that writes it whole would be damage: the re-lay reads its slot as a torn
    /// record, none, and keeps the other slot's.
    #[test]
    fn a_relay_reads_a_piece_torn_within_the_larger_unit_as_a_torn_record() {
        let cfg = config(16 * 4096, 64);
        let mut torn = 0;
        for seed in 0..32 {
            let file = device(4096, 512, seed, None, &cfg);
            let (_, odd) = laid_in_4096(&file, &cfg, None);
            let area = Area::in_pieces(&cfg, align(4096), align(4096), false, false)
                .unwrap()
                .unwrap();
            area.write(&file, &record(4, 50), area.at(4), None).unwrap();
            file.crash(Crash::Random).unwrap();
            if matches!(
                area.read(&file, area.at(4), ID, None),
                Err(LogError::Damaged(_))
            ) {
                torn += 1;
                let area = laid_area(&file, &cfg, (ID, None), align(512)).unwrap();
                assert_eq!(area.read(&file, area.at(4), ID, None).unwrap(), None);
                assert_eq!(area.read(&file, area.at(3), ID, None).unwrap(), Some(odd));
            }
        }
        assert!(torn > 0, "no crash tore a piece");
    }

    /// The slots lie by the block the mark records. Laid by a 4,096-byte block, they read on a
    /// file whose block is 512, which takes them where they lie; laid by 512, on a file whose
    /// block is 4,096, which does not take them, the open lays them again by 4,096 at the area's
    /// end. A mark from before the block was recorded is written again with the file's block.
    #[test]
    fn an_alignment_change_between_runs_reads_by_the_recorded_alignment() {
        let cfg = config(16 * 4096, 64);
        let file = device(4096, 512, 21, None, &cfg);
        let area = Area::in_pieces(&cfg, align(4096), align(512), false, false)
            .unwrap()
            .unwrap();
        let (even, odd) = (record(2, 50), record(3, 7));
        area.write(&file, &even, area.at(2), None).unwrap();
        area.write(&file, &odd, area.at(3), None).unwrap();
        write_mark(&file, ID, false, &area).unwrap();
        file.sync_data().unwrap();
        let image = file.durable_image().unwrap();
        // The same bytes on a file of 512-byte blocks: by its own block the slots would lie
        // elsewhere.
        let by_512 = Area::in_pieces(&cfg, align(512), align(512), false, false)
            .unwrap()
            .unwrap();
        assert_ne!(by_512.at(2), area.at(2));
        let small = device(512, 512, 22, Some(&image), &cfg);
        let found = laid_area(&small, &cfg, (ID, None), align(512)).unwrap();
        assert_eq!(found, area);
        assert_eq!(
            found.read(&small, found.at(2), ID, None).unwrap(),
            Some(even.clone())
        );
        assert_eq!(
            found.read(&small, found.at(3), ID, None).unwrap(),
            Some(odd.clone())
        );
        // Laid by 512, opened on a file of 4,096-byte blocks.
        let small = device(512, 512, 23, None, &cfg);
        by_512.write(&small, &even, by_512.at(2), None).unwrap();
        by_512.write(&small, &odd, by_512.at(3), None).unwrap();
        write_mark(&small, ID, false, &by_512).unwrap();
        small.sync_data().unwrap();
        assert!(!by_512.takes(align(4096)));
        let large = device(4096, 512, 24, Some(&small.durable_image().unwrap()), &cfg);
        let found = laid_area(&large, &cfg, (ID, None), align(512)).unwrap();
        assert_eq!(
            found.mark(),
            Some(format::Mark {
                unit: align(512),
                block: Some(align(4096)),
                at_end: true,
                at_flush: false,
            })
        );
        assert_eq!(
            found.read(&large, found.at(2), ID, None).unwrap(),
            Some(even.clone())
        );
        assert_eq!(
            found.read(&large, found.at(3), ID, None).unwrap(),
            Some(odd.clone())
        );
        // A mark from before the block was recorded: its slots by the file's block, the mark
        // written again with it.
        let mut old = image.clone();
        old[24..40].fill(0);
        let crc = crate::codec::crc32c(&old[..44]);
        old[44..48].copy_from_slice(&crc.to_le_bytes());
        let file = device(4096, 512, 25, Some(&old), &cfg);
        assert_eq!(marked(&file, ID, false).unwrap().unwrap().block, None);
        let found = laid_area(&file, &cfg, (ID, None), align(512)).unwrap();
        assert_eq!(found, area);
        assert_eq!(marked(&file, ID, false).unwrap(), area.mark());
        assert_eq!(
            found.read(&file, found.at(2), ID, None).unwrap(),
            Some(even)
        );
    }
    /// A log that may hold frames answered at their flush says so in its mark, which a binary
    /// from before (hyper-raft f3dae3b6 and earlier, whose mark's sequence was only 0 or 1) reads
    /// as no mark: it then reads the area's start as a whole record of the other kind and refuses
    /// the log, as a binary before pieces does, rather than cut such a frame as the torn tail. This
    /// binary reads the mark, and a re-lay keeps it.
    #[test]
    fn a_binary_before_answers_at_the_flush_refuses_a_log_that_may_hold_them() {
        for (sealed, at_end) in [(false, false), (false, true), (true, false), (true, true)] {
            let mark = format::Mark {
                unit: align(4096),
                block: Some(align(512)),
                at_end,
                at_flush: true,
            };
            let bytes = format::pieces_mark(ID, sealed, mark).unwrap();
            assert_eq!(format::pieces_marked(&bytes, ID, sealed), Some(mark));
            let laid = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
            assert!(laid > 1, "a binary before reads sequence {laid} as a mark");
            let read = Persist::decode(&bytes).expect("a whole record to a binary before");
            assert_eq!((read.log, read.sealed, read.groups.len()), (ID, !sealed, 0));
            let refused = if sealed {
                check_record(&read, &bytes, Some(&sealer()))
            } else {
                check_record(&read, &bytes, None)
            };
            match (sealed, refused) {
                (true, Err(LogError::Tampered(_))) | (false, Err(LogError::Foreign(_))) => {}
                (_, refused) => panic!("sealed {sealed}: {refused:?}"),
            }
        }
        // A mark of no recorded block names no frame answered at its flush: such a mark is
        // written only with its block.
        let mut none = format::pieces_mark(
            ID,
            false,
            format::Mark {
                unit: align(512),
                block: Some(align(512)),
                at_end: false,
                at_flush: true,
            },
        )
        .unwrap();
        none[32..40].fill(0);
        let crc = crate::codec::crc32c(&none[..44]);
        none[44..48].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(format::pieces_marked(&none, ID, false), None);
        // Laid by 512 and marked, opened on a file of 4,096-byte blocks, which lays the slots
        // again at the area's end: the new mark keeps what the old one said.
        let cfg = config(16 * 4096, 64);
        let small = device(512, 512, 31, None, &cfg);
        let by_512 = Area::in_pieces(&cfg, align(512), align(512), false, false)
            .unwrap()
            .unwrap()
            .answered_at_flush(true);
        let odd = record(3, 7);
        by_512.write(&small, &odd, by_512.at(3), None).unwrap();
        write_mark(&small, ID, false, &by_512).unwrap();
        small.sync_data().unwrap();
        assert_eq!(marked(&small, ID, false).unwrap(), by_512.mark());
        let large = device(4096, 512, 32, Some(&small.durable_image().unwrap()), &cfg);
        let found = laid_area(&large, &cfg, (ID, None), align(512)).unwrap();
        let kept = format::Mark {
            unit: align(512),
            block: Some(align(4096)),
            at_end: true,
            at_flush: true,
        };
        assert_eq!(found.mark(), Some(kept));
        assert_eq!(marked(&large, ID, false).unwrap(), Some(kept));
        assert_eq!(
            found.read(&large, found.at(3), ID, None).unwrap(),
            Some(odd)
        );
    }

    /// The mark the writer itself sets, where the owner's decisions let its log answer frames at
    /// their flush (durability.md §7.7, step 4b), refuses a binary from before (hyper-raft f3dae3b6
    /// and earlier) as the mark of step 4a does: at create, under the log's first flush, and at an
    /// open of a log created before the decision, before its first frame. Under today's decisions
    /// neither sets it.
    #[test]
    fn a_binary_before_answers_at_the_flush_refuses_a_log_the_writer_marked() {
        let trusted = AtFlush {
            write_back: crate::WriteBackFrames::Confirmed,
            stated: crate::StatedCache::Trusted,
        };
        let cfg = config(16 * 4096, 64);
        let keeping = |seed| SimFile::keeping(align(4096), align(512), seed).unwrap();
        let refused_before = |file: &SimFile| {
            file.crash(Crash::LoseAll).unwrap();
            let image = file.durable_image().unwrap();
            let bytes = &image[..4096];
            let mark = format::pieces_marked(bytes, ID, false).expect("the mark");
            assert!(mark.at_flush);
            let laid = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
            assert!(laid > 1, "a binary before reads sequence {laid} as a mark");
            let read = Persist::decode(bytes).expect("a whole record to a binary before");
            assert!(matches!(
                check_record(&read, bytes, None),
                Err(LogError::Foreign(_))
            ));
        };
        let created = keeping(41);
        let (_, laid) = super::create(&created, &cfg, ID, None, trusted).unwrap();
        assert_eq!(laid.answers, Answers::Kept);
        refused_before(&created);
        let unmarked = keeping(42);
        let (_, laid) = super::create(&unmarked, &cfg, ID, None, AtFlush::DECIDED).unwrap();
        assert_eq!(laid.answers, Answers::Unmeasured);
        assert_eq!(laid.pieces.map(|m| m.at_flush), Some(false));
        let (_, recovery, _) = super::open(&unmarked, &cfg, ID, None, AtFlush::DECIDED).unwrap();
        assert_eq!(recovery.laid.pieces.map(|m| m.at_flush), Some(false));
        let (_, recovery, _) = super::open(&unmarked, &cfg, ID, None, trusted).unwrap();
        assert_eq!(recovery.laid.pieces.map(|m| m.at_flush), Some(true));
        refused_before(&unmarked);
    }
}
