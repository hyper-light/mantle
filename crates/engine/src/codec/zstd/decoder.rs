//! The Zstandard decoder (RFC 8878 §3): frames and skippable frames, fed in pieces of any size
//! and drained into an output of any size, with dictionaries (§5).
//!
//! Input is gathered one unit at a time (a frame header, a block header, a block, a checksum)
//! into a buffer that never holds more than one block (§3.1.1.2.4: at most 128 KiB); a block is
//! decoded into the window, which keeps the frame's `Window_Size` of history for matches and the
//! bytes not yet handed out. A frame whose window passes the decoder's bound is refused before
//! anything is allocated for it.

use super::Corrupt;
use super::huffman::Huffman;
use super::sequences::{self, Sequence, Tables};
use crate::error::Error;
use crate::util::xxhash::Xxh64;

/// A Zstandard frame's magic number (§3.1.1).
const FRAME_MAGIC: u32 = 0xFD2F_B528;
/// The skippable frames' magic numbers, `0x184D2A50` to `0x184D2A5F` (§3.1.2).
const SKIPPABLE_MAGIC: u32 = 0x184D_2A50;
const SKIPPABLE_MASK: u32 = 0xFFFF_FFF0;
/// A formatted dictionary's magic number (§5).
const DICTIONARY_MAGIC: u32 = 0xEC30_A437;
/// The largest block, decoded or compressed (§3.1.1.2.4: the smaller of `Window_Size` and
/// 128 KB).
const BLOCK_MAX: usize = 128 * 1024;
/// A block header's length (§3.1.1.2).
const BLOCK_HEADER: usize = 3;
/// The smallest raw-content dictionary (§5: "they must be at least 8 bytes").
const RAW_DICTIONARY_MIN: usize = 8;
/// The repeat offsets a frame starts with when no dictionary gives them (§3.1.1.5).
const START_OFFSETS: [u32; 3] = [1, 4, 8];
/// The window's smallest exponent base (§3.1.1.1.2: `windowLog = 10 + Exponent`).
const WINDOW_LOG_BASE: u32 = 10;

/// A dictionary (§5): raw content, or the format `zstd --train` writes, with the entropy tables
/// and repeat offsets a frame starts from.
#[derive(Clone, Debug)]
pub struct Dictionary {
    id: u32,
    content: Vec<u8>,
    huffman: Option<Huffman>,
    tables: Tables,
    offsets: [u32; 3],
}

impl Dictionary {
    /// Parses `bytes`: a formatted dictionary when it starts with the dictionary magic number,
    /// raw content otherwise.
    pub fn new(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::parse(bytes)?)
    }

    fn parse(bytes: &[u8]) -> Result<Self, Corrupt> {
        if read_u32(bytes, 0) != Some(DICTIONARY_MAGIC) {
            if bytes.len() < RAW_DICTIONARY_MIN {
                return Err(Corrupt::Dictionary);
            }
            return Ok(Self {
                id: 0,
                content: bytes.to_vec(),
                huffman: None,
                tables: Tables::default(),
                offsets: START_OFFSETS,
            });
        }
        let id = read_u32(bytes, 4).ok_or(Corrupt::Dictionary)?;
        if id == 0 {
            return Err(Corrupt::Dictionary);
        }
        let mut at = 8usize;
        let (huffman, used) = Huffman::read(bytes.get(at..).ok_or(Corrupt::Dictionary)?)?;
        at = at.checked_add(used).ok_or(Corrupt::Dictionary)?;
        // Offsets, match lengths, literals lengths, in that order (§5), each a description.
        let mut tables = Tables::default();
        at = sequences::dictionary_tables(bytes, at, &mut tables)?;
        let mut offsets = [0u32; 3];
        for slot in &mut offsets {
            *slot = read_u32(bytes, at).ok_or(Corrupt::Dictionary)?;
            at = at.checked_add(4).ok_or(Corrupt::Dictionary)?;
        }
        let content = bytes.get(at..).ok_or(Corrupt::Dictionary)?.to_vec();
        // "Each repeat offset must have a value less than the dictionary size."
        let size = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
        if offsets.iter().any(|&o| o == 0 || o >= size) {
            return Err(Corrupt::Dictionary);
        }
        Ok(Self {
            id,
            content,
            huffman: Some(huffman),
            tables,
            offsets,
        })
    }

    /// The dictionary's ID; 0 for raw content.
    pub fn id(&self) -> u32 {
        self.id
    }
}

/// What one [`Decoder::decompress`] call did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Progress {
    /// Input bytes taken.
    pub consumed: usize,
    /// Output bytes written.
    pub produced: usize,
    /// Whether the last frame begun has ended and every byte of it was handed out.
    pub frame_done: bool,
}

/// Where the decoder is in its input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    /// Gathering a magic number.
    Magic,
    /// Gathering the frame header, whose length its first byte gives.
    Header,
    /// Gathering a skippable frame's size.
    SkipSize,
    /// Skipping a skippable frame's remaining user data.
    Skip { remaining: u64 },
    /// Gathering a block header.
    BlockHeader,
    /// Gathering a block's content.
    Block { last: bool, kind: u8, size: usize },
    /// Gathering the content checksum.
    Checksum,
}

/// A frame's header (§3.1.1.1) as decoding needs it.
#[derive(Clone, Copy, Debug)]
struct Frame {
    window: usize,
    content_size: Option<u64>,
    checksum: bool,
    block_max: usize,
}

/// A streaming Zstandard decoder.
#[derive(Debug)]
pub struct Decoder {
    max_window: usize,
    dictionary: Option<Dictionary>,
    stage: Stage,
    unit: Vec<u8>,
    frame: Option<Frame>,
    /// Decoded bytes: history for matches, then bytes not yet handed out from `emitted`.
    window: Vec<u8>,
    emitted: usize,
    /// Where this frame's bytes begin in the window: before it, a previous frame's bytes not yet
    /// handed out, which no match may reach (frames are independent, §3.1).
    frame_start: usize,
    /// Bytes this frame has decoded, in blocks before the current one.
    decoded: u64,
    hasher: Xxh64,
    huffman: Option<Huffman>,
    tables: Tables,
    offsets: [u32; 3],
    literals: Vec<u8>,
    sequences: Vec<Sequence>,
}

impl Decoder {
    /// A decoder that refuses frames whose window passes `max_window` bytes, with an optional
    /// dictionary for every frame it reads.
    pub fn new(max_window: usize, dictionary: Option<Dictionary>) -> Self {
        Self {
            max_window,
            dictionary,
            stage: Stage::Magic,
            unit: Vec::new(),
            frame: None,
            window: Vec::new(),
            emitted: 0,
            frame_start: 0,
            decoded: 0,
            hasher: Xxh64::new(0),
            huffman: None,
            tables: Tables::default(),
            offsets: START_OFFSETS,
            literals: Vec::new(),
            sequences: Vec::new(),
        }
    }

    /// Ends any frame in progress, so the next input starts a new frame; buffers are kept.
    pub fn reset(&mut self) {
        self.stage = Stage::Magic;
        self.unit.clear();
        self.frame = None;
        self.window.clear();
        self.emitted = 0;
        self.frame_start = 0;
    }

    /// Takes what it can of `input` and writes what it can into `output`.
    pub fn decompress(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
        let mut consumed = 0usize;
        let mut produced = 0usize;
        loop {
            produced = produced
                .checked_add(self.drain(output.get_mut(produced..).unwrap_or_default()))
                .ok_or(Corrupt::Truncated)?;
            if produced == output.len() && self.pending() > 0 {
                break;
            }
            let rest = input.get(consumed..).unwrap_or_default();
            if let Stage::Skip { remaining } = self.stage {
                let take = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(rest.len());
                consumed = consumed.checked_add(take).ok_or(Corrupt::Truncated)?;
                let left = remaining.saturating_sub(u64::try_from(take).unwrap_or(u64::MAX));
                self.stage = if left == 0 {
                    Stage::Magic
                } else {
                    Stage::Skip { remaining: left }
                };
                if left > 0 {
                    break;
                }
                continue;
            }
            let need = self.need();
            if self.unit.len() < need {
                let take = need.saturating_sub(self.unit.len()).min(rest.len());
                self.unit
                    .extend_from_slice(rest.get(..take).unwrap_or_default());
                consumed = consumed.checked_add(take).ok_or(Corrupt::Truncated)?;
                // Still short: the input ran out, or the unit's first bytes said it is longer.
                if self.unit.len() < self.need() {
                    if take == 0 {
                        break;
                    }
                    continue;
                }
            }
            self.step()?;
            self.unit.clear();
        }
        Ok(Progress {
            consumed,
            produced,
            frame_done: self.frame.is_none() && self.pending() == 0 && self.stage == Stage::Magic,
        })
    }

    /// Bytes decoded and not yet handed out.
    fn pending(&self) -> usize {
        self.window.len().saturating_sub(self.emitted)
    }

    /// Hands out what `output` holds of the pending bytes, then lets go of history the window
    /// no longer needs.
    fn drain(&mut self, output: &mut [u8]) -> usize {
        let pending = self.window.get(self.emitted..).unwrap_or_default();
        let n = pending.len().min(output.len());
        if let (Some(dst), Some(src)) = (output.get_mut(..n), pending.get(..n)) {
            dst.copy_from_slice(src);
        }
        self.emitted = self.emitted.saturating_add(n);
        // Keep the frame's window of history behind the emitted point; move the rest out once it
        // is as long again, so the move costs a constant per byte.
        let keep = self.frame.map_or(0, |f| f.window);
        let behind = self.emitted.saturating_sub(keep);
        if behind > keep.max(BLOCK_MAX) {
            self.window.drain(..behind);
            self.emitted = self.emitted.saturating_sub(behind);
            self.frame_start = self.frame_start.saturating_sub(behind);
        }
        n
    }

    /// The bytes the current stage gathers.
    fn need(&self) -> usize {
        match self.stage {
            Stage::Magic | Stage::SkipSize | Stage::Checksum => 4,
            Stage::Header => self.unit.first().map_or(1, |&d| header_length(d)),
            Stage::Skip { .. } => 0,
            Stage::BlockHeader => BLOCK_HEADER,
            Stage::Block { kind, size, .. } => {
                // An RLE block's content is its one byte (§3.1.1.2.2).
                if kind == 1 { 1 } else { size }
            }
        }
    }

    /// Acts on a complete unit.
    fn step(&mut self) -> Result<(), Error> {
        match self.stage {
            Stage::Magic => {
                let magic = read_u32(&self.unit, 0).ok_or(Corrupt::Truncated)?;
                if magic == FRAME_MAGIC {
                    self.stage = Stage::Header;
                } else if magic & SKIPPABLE_MASK == SKIPPABLE_MAGIC {
                    self.stage = Stage::SkipSize;
                } else {
                    return Err(Corrupt::Magic.into());
                }
            }
            Stage::SkipSize => {
                let size = read_u32(&self.unit, 0).ok_or(Corrupt::Truncated)?;
                self.stage = Stage::Skip {
                    remaining: u64::from(size),
                };
            }
            Stage::Skip { .. } => {}
            Stage::Header => self.begin_frame()?,
            Stage::BlockHeader => {
                let raw = u32::from(*self.unit.first().ok_or(Corrupt::Truncated)?)
                    | u32::from(*self.unit.get(1).ok_or(Corrupt::Truncated)?) << 8
                    | u32::from(*self.unit.get(2).ok_or(Corrupt::Truncated)?) << 16;
                let last = raw & 1 == 1;
                let kind = u8::try_from((raw >> 1) & 0b11).map_err(|_| Corrupt::Block)?;
                let size = usize::try_from(raw >> 3).map_err(|_| Corrupt::Block)?;
                let frame = self.frame.ok_or(Corrupt::Block)?;
                if kind == 3 || size > frame.block_max {
                    return Err(Corrupt::Block.into());
                }
                self.stage = Stage::Block { last, kind, size };
                // An empty raw or compressed block has no content to gather.
                if kind != 1 && size == 0 {
                    self.unit.clear();
                    self.step()?;
                }
            }
            Stage::Block { last, kind, size } => {
                let start = self.window.len();
                match kind {
                    0 => self.window.extend_from_slice(&self.unit),
                    1 => {
                        let byte = *self.unit.first().ok_or(Corrupt::Truncated)?;
                        self.window
                            .resize(start.checked_add(size).ok_or(Corrupt::Block)?, byte);
                    }
                    _ => {
                        let unit = std::mem::take(&mut self.unit);
                        let decoded = self.compressed_block(&unit);
                        self.unit = unit;
                        decoded?;
                    }
                }
                let block = self.window.get(start..).unwrap_or_default();
                let frame = self.frame.ok_or(Corrupt::Block)?;
                if block.len() > frame.block_max {
                    return Err(Corrupt::Block.into());
                }
                self.hasher.update(block);
                self.decoded = self
                    .decoded
                    .checked_add(u64::try_from(block.len()).unwrap_or(u64::MAX))
                    .ok_or(Corrupt::ContentSize)?;
                if last {
                    self.end_blocks(frame)?;
                } else {
                    self.stage = Stage::BlockHeader;
                }
            }
            Stage::Checksum => {
                let stored = read_u32(&self.unit, 0).ok_or(Corrupt::Truncated)?;
                // The low 4 bytes of XXH64 of the content, seed 0 (§3.1.1).
                if u64::from(stored) != self.hasher.digest() & 0xFFFF_FFFF {
                    return Err(Corrupt::Checksum.into());
                }
                self.end_frame();
            }
        }
        Ok(())
    }

    /// Parses the frame header in `unit` and readies the frame's state.
    fn begin_frame(&mut self) -> Result<(), Error> {
        let header = &self.unit;
        let descriptor = *header.first().ok_or(Corrupt::Truncated)?;
        if descriptor & 0b0000_1000 != 0 {
            return Err(Corrupt::Header.into());
        }
        let fcs_flag = descriptor >> 6;
        let single_segment = descriptor & 0b0010_0000 != 0;
        let checksum = descriptor & 0b0000_0100 != 0;
        let did_size = match descriptor & 0b11 {
            0 => 0,
            1 => 1,
            2 => 2,
            _ => 4,
        };
        let mut at = 1usize;
        let window_descriptor = if single_segment {
            None
        } else {
            let byte = *header.get(at).ok_or(Corrupt::Truncated)?;
            at = at.saturating_add(1);
            Some(byte)
        };
        let dictionary_id = read_le(header, at, did_size).ok_or(Corrupt::Truncated)?;
        at = at.saturating_add(did_size);
        let fcs_size = fcs_length(fcs_flag, single_segment);
        let content_size = if fcs_size == 0 {
            None
        } else {
            let raw = read_le(header, at, fcs_size).ok_or(Corrupt::Truncated)?;
            // A 2-byte size is stored less 256 (§3.1.1.1.4).
            Some(if fcs_size == 2 {
                raw.checked_add(256).ok_or(Corrupt::Header)?
            } else {
                raw
            })
        };
        let window = match window_descriptor {
            Some(byte) => window_size(byte)?,
            None => content_size.ok_or(Corrupt::Header)?,
        };
        let limit = u64::try_from(self.max_window).unwrap_or(u64::MAX);
        if window > limit {
            return Err(Error::LimitExceeded {
                what: "ZSTD frame window",
                limit,
            });
        }
        let window = usize::try_from(window).map_err(|_| Corrupt::Header)?;
        // A frame naming a dictionary needs that one (§3.1.1.1.3).
        if dictionary_id != 0
            && self.dictionary.as_ref().map(Dictionary::id)
                != Some(u32::try_from(dictionary_id).unwrap_or(u32::MAX))
        {
            return Err(Corrupt::Header.into());
        }
        self.frame = Some(Frame {
            window,
            content_size,
            checksum,
            block_max: window.min(BLOCK_MAX),
        });
        // What was handed out of the previous frame goes; what was not stays before this one.
        self.window.drain(..self.emitted);
        self.emitted = 0;
        self.frame_start = self.window.len();
        self.decoded = 0;
        self.hasher = Xxh64::new(0);
        match &self.dictionary {
            Some(d) => {
                self.huffman = d.huffman.clone();
                self.tables = d.tables.clone();
                self.offsets = d.offsets;
            }
            None => {
                self.huffman = None;
                self.tables = Tables::default();
                self.offsets = START_OFFSETS;
            }
        }
        self.stage = Stage::BlockHeader;
        Ok(())
    }

    /// The last block was decoded: check the content size, then the checksum if there is one.
    fn end_blocks(&mut self, frame: Frame) -> Result<(), Error> {
        if frame.content_size.is_some_and(|size| size != self.decoded) {
            return Err(Corrupt::ContentSize.into());
        }
        if frame.checksum {
            self.stage = Stage::Checksum;
        } else {
            self.end_frame();
        }
        Ok(())
    }

    fn end_frame(&mut self) {
        self.frame = None;
        self.stage = Stage::Magic;
    }

    /// Decodes a compressed block (§3.1.1.3) onto the window.
    fn compressed_block(&mut self, block: &[u8]) -> Result<(), Error> {
        let used = self.literals_section(block)?;
        let section = block.get(used..).ok_or(Corrupt::Literals)?;
        sequences::decode(section, &mut self.tables, &mut self.sequences)?;
        self.execute()?;
        Ok(())
    }

    /// Decodes the literals section into `literals`, returning the bytes it took
    /// (§3.1.1.3.1).
    fn literals_section(&mut self, block: &[u8]) -> Result<usize, Corrupt> {
        let byte0 = *block.first().ok_or(Corrupt::Literals)?;
        let kind = byte0 & 0b11;
        let format = (byte0 >> 2) & 0b11;
        let b = |i: usize| -> Result<usize, Corrupt> {
            block
                .get(i)
                .map(|&x| usize::from(x))
                .ok_or(Corrupt::Literals)
        };
        self.literals.clear();
        if kind <= 1 {
            // Raw or RLE: one size, of 5, 12 or 20 bits.
            let (regenerated, header): (usize, usize) = match format {
                0 | 2 => (usize::from(byte0 >> 3), 1),
                // The fields are disjoint bits (§3.1.1.3.1.1), so they are OR-ed.
                1 => ((usize::from(byte0) >> 4) | (b(1)? << 4), 2),
                _ => ((usize::from(byte0) >> 4) | (b(1)? << 4) | (b(2)? << 12), 3),
            };
            if regenerated > BLOCK_MAX {
                return Err(Corrupt::Literals);
            }
            if kind == 0 {
                let end = header.checked_add(regenerated).ok_or(Corrupt::Literals)?;
                self.literals
                    .extend_from_slice(block.get(header..end).ok_or(Corrupt::Literals)?);
                return Ok(end);
            }
            let byte = *block.get(header).ok_or(Corrupt::Literals)?;
            self.literals.resize(regenerated, byte);
            return Ok(header.saturating_add(1));
        }
        // Compressed or treeless: two sizes of 10, 14 or 18 bits, and the stream count.
        let (four, regenerated, compressed, header): (bool, usize, usize, usize) = match format {
            0 | 1 => {
                let v = (usize::from(byte0) >> 4) | (b(1)? << 4) | (b(2)? << 12);
                (format == 1, v & 0x3FF, v >> 10, 3)
            }
            2 => {
                let v = (usize::from(byte0) >> 4) | (b(1)? << 4) | (b(2)? << 12) | (b(3)? << 20);
                (true, v & 0x3FFF, v >> 14, 4)
            }
            _ => {
                let v = (usize::from(byte0) >> 4)
                    | (b(1)? << 4)
                    | (b(2)? << 12)
                    | (b(3)? << 20)
                    | (b(4)? << 28);
                (true, v & 0x3_FFFF, v >> 18, 5)
            }
        };
        if regenerated > BLOCK_MAX {
            return Err(Corrupt::Literals);
        }
        let end = header.checked_add(compressed).ok_or(Corrupt::Literals)?;
        let mut body = block.get(header..end).ok_or(Corrupt::Literals)?;
        if kind == 2 {
            let (tree, used) = Huffman::read(body)?;
            self.huffman = Some(tree);
            body = body.get(used..).ok_or(Corrupt::Literals)?;
        }
        let tree = self.huffman.as_ref().ok_or(Corrupt::Literals)?;
        self.literals.resize(regenerated, 0);
        tree.literals(body, four, &mut self.literals)?;
        Ok(end)
    }

    /// Executes the block's sequences onto the window, then appends the literals left
    /// (§3.1.1.4, §3.1.1.5).
    fn execute(&mut self) -> Result<(), Corrupt> {
        let frame = self.frame.ok_or(Corrupt::Execution)?;
        let block_start = self.window.len();
        let mut lit = 0usize;
        for seq in &self.sequences {
            let ll = usize::try_from(seq.literals).map_err(|_| Corrupt::Execution)?;
            let lit_end = lit.checked_add(ll).ok_or(Corrupt::Execution)?;
            self.window
                .extend_from_slice(self.literals.get(lit..lit_end).ok_or(Corrupt::Execution)?);
            lit = lit_end;
            let offset = resolve_offset(&mut self.offsets, seq.offset_value, seq.literals)?;
            let offset = usize::try_from(offset).map_err(|_| Corrupt::Execution)?;
            let length = usize::try_from(seq.match_length).map_err(|_| Corrupt::Execution)?;
            // This frame's bytes so far, earlier blocks' and this one's (§5's "decoded from this
            // frame").
            let so_far = self
                .decoded
                .checked_add(
                    u64::try_from(self.window.len().saturating_sub(block_start))
                        .unwrap_or(u64::MAX),
                )
                .ok_or(Corrupt::Execution)?;
            copy_match(
                &mut self.window,
                self.frame_start,
                self.dictionary.as_ref().map(|d| d.content.as_slice()),
                frame.window,
                so_far,
                offset,
                length,
            )?;
            if self.window.len().saturating_sub(block_start) > frame.block_max {
                return Err(Corrupt::Execution);
            }
        }
        self.window
            .extend_from_slice(self.literals.get(lit..).ok_or(Corrupt::Execution)?);
        Ok(())
    }
}

/// The frame header's length after the magic number, from its descriptor (§3.1.1.1).
fn header_length(descriptor: u8) -> usize {
    let single_segment = descriptor & 0b0010_0000 != 0;
    let did = match descriptor & 0b11 {
        0 => 0,
        1 => 1,
        2 => 2,
        _ => 4,
    };
    // At most 1 + 1 + 4 + 8 bytes (§3.1.1.1).
    1usize
        .saturating_add(usize::from(!single_segment))
        .saturating_add(did)
        .saturating_add(fcs_length(descriptor >> 6, single_segment))
}

/// `FCS_Field_Size` (§3.1.1.1.1.1, Table 4).
fn fcs_length(flag: u8, single_segment: bool) -> usize {
    match flag {
        0 => usize::from(single_segment),
        1 => 2,
        2 => 4,
        _ => 8,
    }
}

/// `Window_Size` from the window descriptor (§3.1.1.1.2).
fn window_size(byte: u8) -> Result<u64, Corrupt> {
    let exponent = u32::from(byte >> 3);
    let mantissa = u64::from(byte & 0b111);
    let log = WINDOW_LOG_BASE + exponent;
    let base = 1u64.checked_shl(log).ok_or(Corrupt::Header)?;
    let add = (base / 8).checked_mul(mantissa).ok_or(Corrupt::Header)?;
    base.checked_add(add).ok_or(Corrupt::Header)
}

/// The offset a sequence's `offset_value` names, updating the repeat offsets (§3.1.1.5): above
/// 3 a new offset pushed to the front; 1 to 3 a repeat offset, shifted by one when the sequence
/// has no literals, where 3 then names the first less one byte, a new offset.
fn resolve_offset(offsets: &mut [u32; 3], value: u32, literals: u32) -> Result<u32, Corrupt> {
    let [r1, r2, r3] = *offsets;
    if value > 3 {
        let offset = value.saturating_sub(3);
        *offsets = [offset, r1, r2];
        return Ok(offset);
    }
    // `value` is 1 to 3 here.
    let index = value
        .saturating_sub(1)
        .saturating_add(u32::from(literals == 0));
    let offset = match index {
        0 => r1,
        1 => {
            *offsets = [r2, r1, r3];
            r2
        }
        2 => {
            *offsets = [r3, r1, r2];
            r3
        }
        _ => {
            let offset = r1
                .checked_sub(1)
                .filter(|&o| o > 0)
                .ok_or(Corrupt::Execution)?;
            *offsets = [offset, r1, r2];
            offset
        }
    };
    Ok(offset)
}

/// Copies a match of `length` bytes from `offset` back onto `window`, whose frame bytes begin at
/// `frame_start`. Bytes before the frame's first come from the dictionary's content, allowed
/// while the frame has decoded (`so_far`) no more than its window (§5); a match may overlap the
/// bytes it writes.
fn copy_match(
    window: &mut Vec<u8>,
    frame_start: usize,
    dictionary: Option<&[u8]>,
    window_size: usize,
    so_far: u64,
    offset: usize,
    length: usize,
) -> Result<(), Corrupt> {
    if offset == 0 {
        return Err(Corrupt::Execution);
    }
    // The frame's bytes the window holds.
    let frame_bytes = window.len().saturating_sub(frame_start);
    let mut remaining = length;
    if offset > frame_bytes {
        // Only the dictionary lies further back, and only while the frame is within its window
        // and holds every byte it decoded.
        let whole = u64::try_from(frame_bytes).unwrap_or(u64::MAX) == so_far;
        if !whole || so_far > u64::try_from(window_size).unwrap_or(u64::MAX) {
            return Err(Corrupt::Execution);
        }
        // Into the dictionary: `offset - frame_bytes` bytes back from its end.
        let dict = dictionary.ok_or(Corrupt::Execution)?;
        let back = offset.saturating_sub(frame_bytes);
        let start = dict.len().checked_sub(back).ok_or(Corrupt::Execution)?;
        let from_dict = back.min(remaining);
        window.extend_from_slice(
            dict.get(start..start.checked_add(from_dict).ok_or(Corrupt::Execution)?)
                .ok_or(Corrupt::Execution)?,
        );
        remaining = remaining.saturating_sub(from_dict);
        if remaining == 0 {
            return Ok(());
        }
        // The rest continues from the frame's first byte, `offset` back from the new end.
    }
    let start = window.len().checked_sub(offset).ok_or(Corrupt::Execution)?;
    if offset >= remaining {
        let end = start.checked_add(remaining).ok_or(Corrupt::Execution)?;
        window.extend_from_within(start..end);
    } else {
        // Overlapping: each byte may be one this copy wrote.
        for i in 0..remaining {
            let byte = *window
                .get(start.checked_add(i).ok_or(Corrupt::Execution)?)
                .ok_or(Corrupt::Execution)?;
            window.push(byte);
        }
    }
    Ok(())
}

/// A little-endian u32 at `at`.
fn read_u32(bytes: &[u8], at: usize) -> Option<u32> {
    bytes
        .get(at..)?
        .first_chunk::<4>()
        .map(|b| u32::from_le_bytes(*b))
}

/// A little-endian integer of `len` (0 to 8) bytes at `at`.
fn read_le(bytes: &[u8], at: usize, len: usize) -> Option<u64> {
    let src = bytes.get(at..at.checked_add(len)?)?;
    let mut word = [0u8; 8];
    word.get_mut(..len)?.copy_from_slice(src);
    Some(u64::from_le_bytes(word))
}
