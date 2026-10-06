//! The Zstandard encoder (RFC 8878 §3): frames a [`super::Decoder`] and any compliant decoder
//! read, built from the decoder's own tables run backwards.
//!
//! A frame is compressed whole from its input: its content size is known, so it is written as
//! one segment with the size and, when asked, the content checksum. Each block of at most 128 KB
//! is the smallest of raw, RLE and compressed; a compressed block's matches come from hash
//! chains over every byte before it in the frame, its literals are raw, RLE or Huffman-coded,
//! and its sequences' three codes each use the predefined, RLE or a block's own FSE table,
//! whichever the counts make cheapest.

use super::Corrupt;
use super::bits::Writer;
use super::decoder::{BLOCK_MAX, FRAME_MAGIC};
use super::fse::{
    CELLS_MAX, EncodeState, EncodeTable, LITERALS_LENGTH_DEFAULT, LITERALS_LENGTH_DEFAULT_LOG,
    MATCH_LENGTH_DEFAULT, MATCH_LENGTH_DEFAULT_LOG, OFFSET_DEFAULT, OFFSET_DEFAULT_LOG,
    SYMBOLS_MAX, normalize, table_log, write_distribution,
};
use super::huffman::{HuffmanEncoder, LITERALS, MAX_BITS, code_lengths};
use super::sequences::{
    LITERALS_LENGTH_CODES, LITERALS_LENGTH_MAX_LOG, MATCH_LENGTH_CODES, MATCH_LENGTH_MAX_LOG,
    OFFSET_MAX_LOG,
};
use crate::error::Error;
use crate::util::xxhash::xxh64;

/// The bytes the match finder hashes: a match it finds is at least this long, whatever shorter
/// one a level allows (the format codes matches from 3, §3.1.1.3.2.1.1).
const HASHED: usize = 4;
/// `ZSTD_HASHLOG_MIN` (zstd 1.5.7, lib/zstd.h:1268): the smallest window and hash logs the
/// reference fits parameters to.
const HASH_LOG_MIN: u32 = 6;
/// The highest offset code the predefined offset table holds (§3.1.1.3.2.2.3: N = 28).
const OFFSET_DEFAULT_MAX_CODE: u8 = 28;

/// The reference's parameters by input size and by level 0 (the base of the negative levels) to
/// 22 (zstd 1.5.7, lib/compress/clevels.h:25-129, `ZSTD_defaultCParameters`): for each, the
/// window's log `W`, the hash table's log `H`, the search log `S` (2^S chain positions tried),
/// the shortest match `L`, and whether the strategy is lazy (`ZSTD_lazy` and beyond) or greedy
/// and faster (`ZSTD_fast`, `ZSTD_dfast`, `ZSTD_greedy`). Tables 0 to 3 are for inputs above
/// 256 KB, of at most 256 KB, 128 KB and 16 KB, as `ZSTD_getCParams` chooses them.
/// A row of [`LEVELS`]: `(W, H, S, L, lazy)`.
type Params = (u32, u32, u32, usize, bool);

const LEVELS: [[Params; 23]; 4] = [
    // inputs above 256 KB
    [
        (19, 13, 1, 6, false),
        (19, 14, 1, 7, false),
        (20, 16, 1, 6, false),
        (21, 17, 1, 5, false),
        (21, 18, 1, 5, false),
        (21, 19, 3, 5, false),
        (21, 19, 3, 5, true),
        (21, 20, 4, 5, true),
        (21, 20, 4, 5, true),
        (22, 21, 4, 5, true),
        (22, 22, 5, 5, true),
        (22, 22, 6, 5, true),
        (22, 23, 6, 5, true),
        (22, 22, 4, 5, true),
        (22, 23, 5, 5, true),
        (22, 23, 6, 5, true),
        (22, 22, 5, 5, true),
        (23, 22, 5, 4, true),
        (23, 22, 6, 3, true),
        (23, 22, 7, 3, true),
        (25, 23, 7, 3, true),
        (26, 24, 7, 3, true),
        (27, 25, 9, 3, true),
    ],
    // inputs of at most 256 KB
    [
        (18, 13, 1, 5, false),
        (18, 14, 1, 6, false),
        (18, 14, 1, 5, false),
        (18, 16, 1, 4, false),
        (18, 17, 3, 5, false),
        (18, 18, 5, 5, false),
        (18, 19, 3, 5, true),
        (18, 19, 4, 4, true),
        (18, 19, 4, 4, true),
        (18, 19, 5, 4, true),
        (18, 19, 6, 4, true),
        (18, 19, 5, 4, true),
        (18, 19, 7, 4, true),
        (18, 19, 4, 4, true),
        (18, 19, 4, 3, true),
        (18, 19, 6, 3, true),
        (18, 19, 6, 3, true),
        (18, 19, 8, 3, true),
        (18, 19, 6, 3, true),
        (18, 19, 8, 3, true),
        (18, 19, 10, 3, true),
        (18, 19, 12, 3, true),
        (18, 19, 13, 3, true),
    ],
    // inputs of at most 128 KB
    [
        (17, 12, 1, 5, false),
        (17, 13, 1, 6, false),
        (17, 15, 1, 5, false),
        (17, 16, 2, 5, false),
        (17, 17, 2, 4, false),
        (17, 17, 3, 4, false),
        (17, 17, 3, 4, true),
        (17, 17, 3, 4, true),
        (17, 17, 4, 4, true),
        (17, 17, 5, 4, true),
        (17, 17, 6, 4, true),
        (17, 17, 5, 4, true),
        (17, 17, 7, 4, true),
        (17, 17, 3, 4, true),
        (17, 17, 4, 3, true),
        (17, 17, 6, 3, true),
        (17, 17, 6, 3, true),
        (17, 17, 8, 3, true),
        (17, 17, 10, 3, true),
        (17, 17, 5, 3, true),
        (17, 17, 7, 3, true),
        (17, 17, 9, 3, true),
        (17, 17, 11, 3, true),
    ],
    // inputs of at most 16 KB
    [
        (14, 13, 1, 5, false),
        (14, 15, 1, 5, false),
        (14, 15, 1, 4, false),
        (14, 15, 2, 4, false),
        (14, 14, 4, 4, false),
        (14, 14, 3, 4, true),
        (14, 14, 4, 4, true),
        (14, 14, 6, 4, true),
        (14, 14, 8, 4, true),
        (14, 14, 5, 4, true),
        (14, 14, 9, 4, true),
        (14, 14, 3, 4, true),
        (14, 14, 4, 3, true),
        (14, 14, 5, 3, true),
        (14, 15, 6, 3, true),
        (14, 15, 7, 3, true),
        (14, 15, 5, 3, true),
        (14, 15, 6, 3, true),
        (14, 15, 7, 3, true),
        (14, 15, 8, 3, true),
        (14, 15, 8, 3, true),
        (14, 15, 9, 3, true),
        (14, 15, 10, 3, true),
    ],
];

/// How hard the encoder looks for matches, a level's row of [`LEVELS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Level {
    level: i32,
    hash_log: u32,
    chain: u32,
    min_match: usize,
    lazy: bool,
}

impl Level {
    /// A level by number, as RocksDB passes one: 1 to 22, the negative levels taking the
    /// reference's base row (0), above 22 the 22nd. Its parameters are those for inputs above
    /// 256 KB until [`Level::fit`] fits them to an input.
    pub fn new(level: i32) -> Self {
        Self::from_table(level, 0, None)
    }

    fn from_table(level: i32, table: usize, src_len: Option<usize>) -> Self {
        let row = usize::try_from(level.clamp(0, 22)).unwrap_or(0);
        let (window_log, hash_log, search_log, min_match, lazy) = LEVELS
            .get(table)
            .and_then(|t| t.get(row))
            .copied()
            .unwrap_or((19, 13, 1, 6, false));
        // `ZSTD_adjustCParams_internal` (zstd 1.5.7, lib/compress/zstd_compress.c:1551-1565): a
        // known input shrinks the window to its size, at least 2^6 (`ZSTD_HASHLOG_MIN`), and the
        // hash table to twice the window.
        let hash_log = match src_len {
            Some(len) => {
                let src_log = if len < 1 << HASH_LOG_MIN {
                    HASH_LOG_MIN
                } else {
                    usize::BITS.saturating_sub(len.saturating_sub(1).leading_zeros())
                };
                hash_log.min(window_log.min(src_log).saturating_add(1))
            }
            None => hash_log,
        };
        Self {
            level,
            hash_log,
            chain: 1u32 << search_log,
            min_match: min_match.max(HASHED),
            lazy,
        }
    }

    /// The parameters the reference uses for an input of `len` bytes: `ZSTD_getCParams`'s table
    /// by size (lib/compress/zstd_compress.c:7759-7762), then its adjustment to the size.
    pub fn fit(self, len: usize) -> Self {
        let table = [256usize << 10, 128 << 10, 16 << 10]
            .iter()
            .filter(|&&bound| len <= bound)
            .count();
        Self::from_table(self.level, table, Some(len))
    }
}

/// Compresses `input` as one frame, with the content checksum when `checksum`.
pub fn compress(input: &[u8], level: Level, checksum: bool) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    Compressor::new()?.compress_into(input, level, checksum, &mut out)?;
    Ok(out)
}

/// The predefined distributions' encoding tables (§3.1.1.3.2.2), built once a compressor.
#[derive(Debug)]
struct Predefined {
    literals_length: EncodeTable,
    offset: EncodeTable,
    match_length: EncodeTable,
}

/// What compressing keeps from one frame to the next: the reference's `ZSTD_CCtx` working area,
/// which RocksDB keeps a thread's. The match finder's tables, the block's sequences and
/// literals, the predefined tables and the cost table are allocated once and reused.
#[derive(Debug)]
pub struct Compressor {
    head: Vec<u32>,
    chain: Vec<u32>,
    found: Vec<Found>,
    literals: Vec<u8>,
    block: Vec<u8>,
    codes: Vec<Codes>,
    /// The block's own encoding tables, rebuilt in place: literals length, offset, match length.
    own: [EncodeTable; 3],
    predefined: Predefined,
    /// `256·log2(n)` of a symbol's cells `n`, 0 to the largest table's size.
    log2: Vec<u16>,
}

impl Compressor {
    /// A compressor with its fixed tables built.
    pub fn new() -> Result<Self, Error> {
        let log2 = (0..=CELLS_MAX)
            .map(|n| u16::try_from(log2_256(u64::try_from(n).unwrap_or(0))).unwrap_or(u16::MAX))
            .collect();
        Ok(Self {
            head: Vec::new(),
            chain: Vec::new(),
            found: Vec::new(),
            literals: Vec::new(),
            block: Vec::new(),
            codes: Vec::new(),
            own: Default::default(),
            predefined: Predefined {
                literals_length: EncodeTable::new(
                    &LITERALS_LENGTH_DEFAULT,
                    LITERALS_LENGTH_DEFAULT_LOG,
                )?,
                offset: EncodeTable::new(&OFFSET_DEFAULT, OFFSET_DEFAULT_LOG)?,
                match_length: EncodeTable::new(&MATCH_LENGTH_DEFAULT, MATCH_LENGTH_DEFAULT_LOG)?,
            },
            log2,
        })
    }

    /// Compresses `input` as one frame onto the end of `out`, with the content checksum when
    /// `checksum`.
    pub fn compress_into(
        &mut self,
        input: &[u8],
        level: Level,
        checksum: bool,
        out: &mut Vec<u8>,
    ) -> Result<(), Error> {
        Ok(self.encode_frame(input, level, checksum, out)?)
    }

    fn encode_frame(
        &mut self,
        input: &[u8],
        level: Level,
        checksum: bool,
        out: &mut Vec<u8>,
    ) -> Result<(), Corrupt> {
        let level = level.fit(input.len());
        out.reserve((input.len() / 2).saturating_add(32));
        out.extend_from_slice(&FRAME_MAGIC.to_le_bytes());
        frame_header(out, input.len(), checksum);
        let mut matcher = Matcher::new(input, level, &mut self.head, &mut self.chain);
        let mut offsets = [1u32, 4, 8];
        let mut start = 0usize;
        loop {
            let end = start.saturating_add(BLOCK_MAX).min(input.len());
            let last = end == input.len();
            let mut work = Work {
                found: &mut self.found,
                literals: &mut self.literals,
                block: &mut self.block,
                codes: &mut self.codes,
                own: &mut self.own,
                predefined: &self.predefined,
                log2: &self.log2,
            };
            encode_block(
                out,
                input,
                start..end,
                last,
                &mut matcher,
                &mut offsets,
                &mut work,
            )?;
            start = end;
            if last {
                break;
            }
        }
        if checksum {
            let digest = xxh64(input, 0);
            out.extend_from_slice(
                &u32::try_from(digest & 0xFFFF_FFFF)
                    .unwrap_or(0)
                    .to_le_bytes(),
            );
        }
        Ok(())
    }
}

/// A block's working buffers and the fixed tables, lent by the [`Compressor`].
struct Work<'a> {
    found: &'a mut Vec<Found>,
    literals: &'a mut Vec<u8>,
    block: &'a mut Vec<u8>,
    codes: &'a mut Vec<Codes>,
    own: &'a mut [EncodeTable; 3],
    predefined: &'a Predefined,
    log2: &'a [u16],
}

/// A sequence's three codes, each with its extra bits' value and count: literals length, match
/// length, offset.
type Codes = [(u8, u32, u8); 3];

/// A single-segment frame header with the content size (§3.1.1.1).
fn frame_header(out: &mut Vec<u8>, size: usize, checksum: bool) {
    let size = u64::try_from(size).unwrap_or(u64::MAX);
    // FCS_Field_Size 1, 2, 4 or 8 bytes; 2 stores the size less 256 (§3.1.1.1.4).
    let (flag, bytes): (u8, Vec<u8>) = if size < 256 {
        (0, vec![u8::try_from(size).unwrap_or(0)])
    } else if size < 65_536 + 256 {
        (
            1,
            u16::try_from(size.saturating_sub(256))
                .unwrap_or(0)
                .to_le_bytes()
                .to_vec(),
        )
    } else if size <= u64::from(u32::MAX) {
        (2, u32::try_from(size).unwrap_or(0).to_le_bytes().to_vec())
    } else {
        (3, size.to_le_bytes().to_vec())
    };
    let descriptor = (flag << 6) | 0b0010_0000 | if checksum { 0b0000_0100 } else { 0 };
    out.push(descriptor);
    out.extend_from_slice(&bytes);
}

/// One block, `range` of `input`: compressed if that is smaller than raw, RLE when every byte is
/// one.
fn encode_block(
    out: &mut Vec<u8>,
    input: &[u8],
    range: std::ops::Range<usize>,
    last: bool,
    matcher: &mut Matcher<'_>,
    offsets: &mut [u32; 3],
    work: &mut Work<'_>,
) -> Result<(), Corrupt> {
    let (start, end) = (range.start, range.end);
    let block = input.get(start..end).ok_or(Corrupt::Block)?;
    let header = |kind: u32, size: usize| -> Result<[u8; 3], Corrupt> {
        let size = u32::try_from(size).map_err(|_| Corrupt::Block)?;
        let raw = u32::from(last) | (kind << 1) | (size << 3);
        let b = raw.to_le_bytes();
        Ok([b[0], b[1], b[2]])
    };
    if let Some(&first) = block.first()
        && block.len() > 1
        && block.iter().all(|&b| b == first)
    {
        out.extend_from_slice(&header(1, block.len())?);
        out.push(first);
        // The match finder still learns the block's positions for the blocks after it.
        matcher.skip(start, end);
        return Ok(());
    }
    let saved = *offsets;
    matcher.sequences(start, end, offsets, work.found);
    compress_block(input, start, end, work)?;
    if work.block.len() < block.len() {
        out.extend_from_slice(&header(2, work.block.len())?);
        out.extend_from_slice(work.block);
    } else {
        // Raw: a raw block does not move the repeat offsets (§3.1.1.5).
        *offsets = saved;
        out.extend_from_slice(&header(0, block.len())?);
        out.extend_from_slice(block);
    }
    Ok(())
}

/// A match the finder chose: `literals` bytes before it, then `length` bytes from `offset_value`
/// (the coded value, 1 to 3 a repeat offset, §3.1.1.5).
#[derive(Clone, Copy, Debug)]
struct Found {
    literals: u32,
    offset_value: u32,
    length: u32,
}

/// Hash chains over the frame's input: `head` the latest position of each 4-byte hash, `chain`
/// each position's previous one of the same hash.
struct Matcher<'a> {
    input: &'a [u8],
    level: Level,
    head: &'a mut [u32],
    chain: &'a mut [u32],
    /// Positions inserted so far.
    next: usize,
}

/// No position: the hash table's and chains' empty value.
const NONE: u32 = u32::MAX;

impl<'a> Matcher<'a> {
    /// A matcher over `input` in `head` and `chain`, emptied and sized for it.
    fn new(input: &'a [u8], level: Level, head: &'a mut Vec<u32>, chain: &'a mut Vec<u32>) -> Self {
        head.clear();
        head.resize(1usize << level.hash_log, NONE);
        chain.clear();
        chain.resize(input.len(), NONE);
        Self {
            input,
            level,
            head,
            chain,
            next: 0,
        }
    }

    fn hash(&self, at: usize) -> Option<usize> {
        let b = self.input.get(at..at.checked_add(4)?)?;
        let v = u32::from_le_bytes([*b.first()?, *b.get(1)?, *b.get(2)?, *b.get(3)?]);
        // Knuth's multiplicative hash (TAOCP 3, §6.4): the golden-ratio multiplier's top bits.
        let shift = 32u32.saturating_sub(self.level.hash_log);
        Some(
            usize::try_from(v.wrapping_mul(0x9E37_79B1).checked_shr(shift).unwrap_or(0))
                .unwrap_or(0),
        )
    }

    /// Inserts every position before `to`.
    fn insert_to(&mut self, to: usize) {
        while self.next < to {
            if let Some(h) = self.hash(self.next)
                && let (Some(slot), Some(link)) =
                    (self.head.get_mut(h), self.chain.get_mut(self.next))
            {
                *link = *slot;
                *slot = u32::try_from(self.next).unwrap_or(NONE);
            }
            self.next = self.next.saturating_add(1);
        }
    }

    fn skip(&mut self, _start: usize, end: usize) {
        self.insert_to(end);
    }

    /// The length the input matches itself at `a` and `b` (`b` later), up to `limit`.
    #[inline]
    fn common(&self, a: usize, b: usize, limit: usize) -> usize {
        let (Some(x), Some(y)) = (self.input.get(a..), self.input.get(b..limit)) else {
            return 0;
        };
        // Eight bytes at a time, as `ZSTD_count` (zstd 1.5.7, lib/compress/zstd_compress_internal.h)
        // counts: the lowest set bit of two little-endian words' difference lies in their first
        // differing byte.
        let (mut x, mut y, mut n) = (x, y, 0usize);
        while let (Some((xw, xr)), Some((yw, yr))) =
            (x.split_first_chunk::<8>(), y.split_first_chunk::<8>())
        {
            let d = u64::from_le_bytes(*xw) ^ u64::from_le_bytes(*yw);
            if d != 0 {
                return n.saturating_add((d.trailing_zeros() / 8) as usize);
            }
            n = n.saturating_add(8);
            x = xr;
            y = yr;
        }
        n.saturating_add(x.iter().zip(y).take_while(|(p, q)| p == q).count())
    }

    /// The longest match at `at` within the block's end `end`, trying the last offset and then the
    /// chain: its length and offset.
    fn best(&mut self, at: usize, end: usize, rep: u32) -> (usize, usize) {
        self.insert_to(at);
        let mut best = (0usize, 0usize);
        let rep = usize::try_from(rep).unwrap_or(0);
        if rep > 0 && rep <= at {
            let len = self.common(at.saturating_sub(rep), at, end);
            if len >= self.level.min_match {
                best = (len, rep);
            }
        }
        let Some(h) = self.hash(at) else {
            return best;
        };
        let mut candidate = self.head.get(h).copied().unwrap_or(NONE);
        let mut tries = self.level.chain;
        while candidate != NONE && tries > 0 {
            let c = usize::try_from(candidate).unwrap_or(0);
            let len = self.common(c, at, end);
            if len > best.0 {
                best = (len, at.saturating_sub(c));
                if at.saturating_add(len) >= end {
                    break;
                }
            }
            candidate = self.chain.get(c).copied().unwrap_or(NONE);
            tries = tries.saturating_sub(1);
        }
        if best.0 < self.level.min_match {
            (0, 0)
        } else {
            best
        }
    }

    /// The block's matches, greedy or one position lazy, the repeat offsets updated as the
    /// decoder will update them.
    fn sequences(
        &mut self,
        start: usize,
        end: usize,
        offsets: &mut [u32; 3],
        out: &mut Vec<Found>,
    ) {
        out.clear();
        let mut at = start;
        let mut anchor = start;
        while at.saturating_add(self.level.min_match) <= end {
            let (mut len, mut off) = self.best(at, end, offsets[0]);
            if len == 0 {
                at = at.saturating_add(1);
                continue;
            }
            if self.level.lazy && at.saturating_add(1).saturating_add(self.level.min_match) <= end {
                let (len2, off2) = self.best(at.saturating_add(1), end, offsets[0]);
                if len2 > len.saturating_add(1) {
                    at = at.saturating_add(1);
                    len = len2;
                    off = off2;
                }
            }
            let literals = u32::try_from(at.saturating_sub(anchor)).unwrap_or(0);
            let offset = u32::try_from(off).unwrap_or(0);
            let offset_value = code_offset(offsets, offset, literals);
            out.push(Found {
                literals,
                offset_value,
                length: u32::try_from(len).unwrap_or(0),
            });
            at = at.saturating_add(len);
            anchor = at;
        }
        self.insert_to(end);
        // The literals after the last match are the block's tail; the count is implied.
        let _ = anchor;
    }
}

/// The value that codes `offset` after `literals` literals, updating the repeat offsets as the
/// decoder does (§3.1.1.5): a repeat code where the offset is one, a new offset otherwise.
fn code_offset(offsets: &mut [u32; 3], offset: u32, literals: u32) -> u32 {
    let [r1, r2, r3] = *offsets;
    if literals > 0 {
        if offset == r1 {
            return 1;
        }
        if offset == r2 {
            *offsets = [r2, r1, r3];
            return 2;
        }
        if offset == r3 {
            *offsets = [r3, r1, r2];
            return 3;
        }
    } else {
        // With no literals the codes shift: 1 names the second, 2 the third, 3 the first less one.
        if offset == r2 {
            *offsets = [r2, r1, r3];
            return 1;
        }
        if offset == r3 {
            *offsets = [r3, r1, r2];
            return 2;
        }
        if r1 > 1 && offset == r1.saturating_sub(1) {
            *offsets = [offset, r1, r2];
            return 3;
        }
    }
    *offsets = [offset, r1, r2];
    offset.saturating_add(3)
}

/// The code of a literals length, and its extra bits' value (§3.1.1.3.2.1.1, Table 16).
fn literals_length_code(len: u32) -> (u8, u32, u8) {
    code_for(len, &LITERALS_LENGTH_CODES)
}

fn match_length_code(len: u32) -> (u8, u32, u8) {
    code_for(len, &MATCH_LENGTH_CODES)
}

/// The largest code whose baseline is at most `value`: the code, the extra bits' value, their
/// count. The baselines ascend, so a binary search finds it.
#[inline]
fn code_for(value: u32, table: &[(u32, u8)]) -> (u8, u32, u8) {
    let code = table
        .partition_point(|&(base, _)| base <= value)
        .saturating_sub(1);
    let (base, bits) = table.get(code).copied().unwrap_or((0, 0));
    (
        u8::try_from(code).unwrap_or(0),
        value.saturating_sub(base),
        bits,
    )
}

/// The offset code of an offset value: its highest bit, the rest its extra bits
/// (§3.1.1.3.2.1.1).
fn offset_code(value: u32) -> (u8, u32, u8) {
    let code = u32::BITS
        .saturating_sub(1)
        .saturating_sub(value.leading_zeros());
    let base = 1u32 << code;
    let c = u8::try_from(code).unwrap_or(0);
    (c, value.saturating_sub(base), c)
}

/// A compressed block's content into `work.block`: the literals section, then the sequences
/// section (§3.1.1.3).
fn compress_block(
    input: &[u8],
    start: usize,
    end: usize,
    work: &mut Work<'_>,
) -> Result<(), Corrupt> {
    let literals = &mut *work.literals;
    literals.clear();
    let mut at = start;
    for f in work.found.iter() {
        let to = at.saturating_add(usize::try_from(f.literals).unwrap_or(0));
        literals.extend_from_slice(input.get(at..to).ok_or(Corrupt::Block)?);
        at = to.saturating_add(usize::try_from(f.length).unwrap_or(0));
    }
    literals.extend_from_slice(input.get(at..end).ok_or(Corrupt::Block)?);
    work.block.clear();
    literals_section(literals, work.block)?;
    sequences_section(work)?;
    Ok(())
}

/// The literals section (§3.1.1.3.1) onto `out`: RLE when every literal is one, Huffman-coded
/// when that saves bytes, raw otherwise.
fn literals_section(literals: &[u8], out: &mut Vec<u8>) -> Result<(), Corrupt> {
    let n = literals.len();
    if let Some(&first) = literals.first()
        && n > 1
        && literals.iter().all(|&b| b == first)
    {
        raw_or_rle_header(1, n, out)?;
        out.push(first);
        return Ok(());
    }
    let start = out.len();
    let raw = raw_header_len(n).saturating_add(n);
    if huffman_literals(literals, out).is_ok() && out.len().saturating_sub(start) < raw {
        return Ok(());
    }
    out.truncate(start);
    raw_or_rle_header(0, n, out)?;
    out.extend_from_slice(literals);
    Ok(())
}

/// The length of a raw or RLE literals header for `size` literals (§3.1.1.3.1.1).
fn raw_header_len(size: usize) -> usize {
    if size < 32 {
        1
    } else if size < 4096 {
        2
    } else {
        3
    }
}

/// A raw or RLE literals header of the shortest size format onto `out` (§3.1.1.3.1.1).
fn raw_or_rle_header(kind: u8, size: usize, out: &mut Vec<u8>) -> Result<(), Corrupt> {
    let s = u32::try_from(size).map_err(|_| Corrupt::Literals)?;
    let kind = u32::from(kind);
    let (v, len) = match raw_header_len(size) {
        1 => (kind | (s << 3), 1),
        2 => (kind | (1 << 2) | (s << 4), 2),
        _ => (kind | (3 << 2) | (s << 4), 3),
    };
    out.extend_from_slice(v.to_le_bytes().get(..len).ok_or(Corrupt::Literals)?);
    Ok(())
}

/// Huffman-coded literals with their tree (§3.1.1.3.1.4) onto `out`: one stream below 256
/// literals, four above, as the reference chooses. The header's size format follows from the
/// literals' count alone, as the reference's `ZSTD_compressLiterals` sets it: a coded section
/// no shorter than the literals is not kept, so its size fits wherever theirs does. On an error
/// `out` may hold a partial section.
fn huffman_literals(literals: &[u8], out: &mut Vec<u8>) -> Result<(), Corrupt> {
    let mut counts = [0u32; LITERALS];
    for &b in literals {
        let c = counts.get_mut(usize::from(b)).ok_or(Corrupt::Huffman)?;
        *c = c.saturating_add(1);
    }
    let mut lengths = [0u8; LITERALS];
    code_lengths(&counts, MAX_BITS, &mut lengths);
    let table = HuffmanEncoder::new(&lengths)?;
    let r = u32::try_from(literals.len()).map_err(|_| Corrupt::Literals)?;
    let four = literals.len() >= 256;
    // Size_Format (§3.1.1.3.1.1): one stream only with 10-bit sizes.
    let (format, header): (u64, usize) = if !four {
        (0, 3)
    } else if r < 1024 {
        (1, 3)
    } else if r < 16_384 {
        (2, 4)
    } else if r < 262_144 {
        (3, 5)
    } else {
        return Err(Corrupt::Literals);
    };
    let start = out.len();
    out.extend_from_slice([0; 5].get(..header).ok_or(Corrupt::Literals)?);
    table.description(out)?;
    table.streams(literals, four, out)?;
    let c = u64::try_from(out.len().saturating_sub(start).saturating_sub(header))
        .map_err(|_| Corrupt::Literals)?;
    // Each size's bits, and where the compressed size starts after the 4 header bits.
    let (width, at) = match format {
        0 | 1 => (10, 14),
        2 => (14, 18),
        _ => (18, 22),
    };
    if c >= 1 << width {
        return Err(Corrupt::Literals);
    }
    let v = 2u64 | (format << 2) | (u64::from(r) << 4) | (c << at);
    out.get_mut(start..start.saturating_add(header))
        .ok_or(Corrupt::Literals)?
        .copy_from_slice(v.to_le_bytes().get(..header).ok_or(Corrupt::Literals)?);
    Ok(())
}

/// One symbol type's table for the block: its mode (§3.1.1.3.2.1, Table 15) and its encoding
/// table, none in RLE mode, where its one state takes no bits to start, step or flush.
struct Chosen<'t> {
    mode: u8,
    table: Option<&'t EncodeTable>,
}

/// A state over a chosen table; none in RLE mode, where nothing is written.
struct Coder<'t> {
    table: Option<&'t EncodeTable>,
    state: Option<EncodeState>,
}

impl<'t> Coder<'t> {
    fn init(chosen: &Chosen<'t>, symbol: u8) -> Result<Self, Corrupt> {
        let table = chosen.table;
        let state = table.map(|t| EncodeState::init(t, symbol)).transpose()?;
        Ok(Self { table, state })
    }

    #[inline]
    fn encode(&mut self, symbol: u8, w: &mut Writer<'_>) -> Result<(), Corrupt> {
        if let (Some(t), Some(s)) = (self.table, self.state.as_mut()) {
            s.encode(t, symbol, w)?;
        }
        Ok(())
    }

    fn flush(self, w: &mut Writer<'_>) {
        if let (Some(t), Some(s)) = (self.table, self.state) {
            s.flush(t, w);
        }
    }
}

/// The cheapest of predefined, RLE and the block's own FSE table for a code's `counts` (one per
/// code, `total` in all), by the bits each spends; its description, if any, goes onto `out`.
/// `default` is the predefined distribution and `predefined` its encoding table, `own` the table
/// rebuilt for the block's own, and `log2` the cost table.
fn choose<'t>(
    counts: &[u32],
    total: usize,
    max_log: u32,
    (default, default_log, predefined): (&[i16], u32, &'t EncodeTable),
    own: &'t mut EncodeTable,
    log2: &[u16],
    out: &mut Vec<u8>,
) -> Result<Chosen<'t>, Corrupt> {
    let max_symbol = counts
        .iter()
        .rposition(|&c| c > 0)
        .ok_or(Corrupt::Sequences)?;
    let present = counts.iter().filter(|&&c| c > 0).count();
    if present == 1 {
        // One symbol: RLE (FSE_Compressed_Mode needs two, §3.1.1.3.2.1).
        out.push(u8::try_from(max_symbol).map_err(|_| Corrupt::Sequences)?);
        return Ok(Chosen {
            mode: 1,
            table: None,
        });
    }
    let total32 = u32::try_from(total).map_err(|_| Corrupt::Sequences)?;
    let log = table_log(total, max_symbol, max_log);
    let counts = counts.get(..=max_symbol).ok_or(Corrupt::Sequences)?;
    let mut norm = [0i16; SYMBOLS_MAX];
    let norm = normalize(counts, total32, log, &mut norm)?;
    let start = out.len();
    write_distribution(norm, log, out)?;
    let description = out.len().saturating_sub(start);
    // The table's own bits and its description's, both in 256ths of a bit.
    let own_bits = cost(counts, norm, log, log2).saturating_add(
        u64::try_from(description)
            .unwrap_or(0)
            .saturating_mul(8 * 256),
    );
    // The predefined table covers the codes only if every code present has a cell in it.
    let default_fits = counts
        .iter()
        .enumerate()
        .all(|(s, &c)| c == 0 || default.get(s).is_some_and(|&p| p != 0));
    if default_fits && cost(counts, default, default_log, log2) <= own_bits {
        out.truncate(start);
        return Ok(Chosen {
            mode: 0,
            table: Some(predefined),
        });
    }
    own.set(norm, log)?;
    Ok(Chosen {
        mode: 2,
        table: Some(own),
    })
}

/// `256·log2(x)` for `x ≥ 1`, rounded down: the integer part from the bit length, eight fraction
/// bits by repeated squaring of the mantissa (each square doubles the exponent; a square of 2 or
/// more is a one bit). Integer arithmetic, so every host chooses alike.
fn log2_256(x: u64) -> u64 {
    if x == 0 {
        return 0;
    }
    let whole = u64::from(63u32.saturating_sub(x.leading_zeros()));
    // The mantissa in [1, 2) as a 1.32 fixed-point number.
    let shift = u32::try_from(whole).unwrap_or(0);
    let mut m: u128 = (u128::from(x) << 32) >> shift;
    let mut frac = 0u64;
    for _ in 0..8 {
        m = (m.saturating_mul(m)) >> 32;
        frac <<= 1;
        if m >= 2u128 << 32 {
            m >>= 1;
            frac |= 1;
        }
    }
    (whole << 8) | frac
}

/// The bits `counts` take under `norm` at `log`, in 256ths of a bit: each symbol
/// `log − log2(p)` bits a time, `p` its cells (a "less than 1" symbol one), absent from `norm` the
/// most a cost can be. `log2` holds `256·log2(p)` for every count of cells a table has.
fn cost(counts: &[u32], norm: &[i16], log: u32, log2: &[u16]) -> u64 {
    let mut total = 0u64;
    for (s, &c) in counts.iter().enumerate() {
        if c == 0 {
            continue;
        }
        let cells = match norm.get(s) {
            Some(&-1) => 1u64,
            Some(&n) if n > 0 => u64::from(n.unsigned_abs()),
            _ => return u64::MAX,
        };
        let cells = usize::try_from(cells).unwrap_or(usize::MAX);
        let each = (u64::from(log) << 8)
            .saturating_sub(u64::from(log2.get(cells).copied().unwrap_or(u16::MAX)));
        total = total.saturating_add(u64::from(c).saturating_mul(each));
    }
    total
}

/// The sequences section (§3.1.1.3.2) onto the block: the count, the modes, the
/// descriptions, and the interleaved bitstream, written last sequence first.
fn sequences_section(work: &mut Work<'_>) -> Result<(), Corrupt> {
    let found = &*work.found;
    let out = &mut *work.block;
    let n = found.len();
    match n {
        0 => {
            out.push(0);
            return Ok(());
        }
        1..=127 => out.push(u8::try_from(n).map_err(|_| Corrupt::Sequences)?),
        128..=0x7EFF => {
            out.push(u8::try_from((n >> 8) | 0x80).map_err(|_| Corrupt::Sequences)?);
            out.push(u8::try_from(n & 0xFF).map_err(|_| Corrupt::Sequences)?);
        }
        _ => {
            let rest = n.checked_sub(0x7F00).ok_or(Corrupt::Sequences)?;
            out.push(255);
            out.push(u8::try_from(rest & 0xFF).map_err(|_| Corrupt::Sequences)?);
            out.push(u8::try_from(rest >> 8).map_err(|_| Corrupt::Sequences)?);
        }
    }
    // Each sequence's codes, and each code's count, in one pass.
    let codes = &mut *work.codes;
    codes.clear();
    let mut ll_counts = [0u32; LITERALS_LENGTH_CODES.len()];
    let mut ml_counts = [0u32; MATCH_LENGTH_CODES.len()];
    let mut of_counts = [0u32; 32];
    for f in found {
        let c = [
            literals_length_code(f.literals),
            match_length_code(f.length),
            offset_code(f.offset_value),
        ];
        for (counts, &(code, _, _)) in [&mut ll_counts[..], &mut ml_counts[..], &mut of_counts[..]]
            .into_iter()
            .zip(&c)
        {
            let slot = counts
                .get_mut(usize::from(code))
                .ok_or(Corrupt::Sequences)?;
            *slot = slot.saturating_add(1);
        }
        codes.push(c);
    }
    let modes_at = out.len();
    out.push(0);
    let [ll_own, of_own, ml_own] = &mut *work.own;
    let predefined = work.predefined;
    let ll_t = choose(
        &ll_counts,
        n,
        LITERALS_LENGTH_MAX_LOG,
        (
            &LITERALS_LENGTH_DEFAULT,
            LITERALS_LENGTH_DEFAULT_LOG,
            &predefined.literals_length,
        ),
        ll_own,
        work.log2,
        out,
    )?;
    // Offset codes above the predefined table's N cannot use it; `choose` sees that as no cell.
    let of_max = of_counts.iter().rposition(|&c| c > 0).unwrap_or(0);
    let of_default: &[i16] = if of_max > usize::from(OFFSET_DEFAULT_MAX_CODE) {
        &[]
    } else {
        &OFFSET_DEFAULT
    };
    let of_t = choose(
        &of_counts,
        n,
        OFFSET_MAX_LOG,
        (of_default, OFFSET_DEFAULT_LOG, &predefined.offset),
        of_own,
        work.log2,
        out,
    )?;
    let ml_t = choose(
        &ml_counts,
        n,
        MATCH_LENGTH_MAX_LOG,
        (
            &MATCH_LENGTH_DEFAULT,
            MATCH_LENGTH_DEFAULT_LOG,
            &predefined.match_length,
        ),
        ml_own,
        work.log2,
        out,
    )?;
    *out.get_mut(modes_at).ok_or(Corrupt::Sequences)? =
        (ll_t.mode << 6) | (of_t.mode << 4) | (ml_t.mode << 2);
    // The bitstream, the reference's ZSTD_encodeSequences: the last sequence's states and bits
    // first, then each earlier one, then the states flushed, read back in the decoder's order.
    let mut w = Writer::new(out);
    let [l_ll, l_ml, l_of] = *codes.last().ok_or(Corrupt::Sequences)?;
    let mut s_ml = Coder::init(&ml_t, l_ml.0)?;
    let mut s_of = Coder::init(&of_t, l_of.0)?;
    let mut s_ll = Coder::init(&ll_t, l_ll.0)?;
    w.add(u64::from(l_ll.1), u32::from(l_ll.2));
    w.add(u64::from(l_ml.1), u32::from(l_ml.2));
    w.add(u64::from(l_of.1), u32::from(l_of.2));
    for &[c_ll, c_ml, c_of] in codes.iter().rev().skip(1) {
        s_of.encode(c_of.0, &mut w)?;
        s_ml.encode(c_ml.0, &mut w)?;
        s_ll.encode(c_ll.0, &mut w)?;
        w.add(u64::from(c_ll.1), u32::from(c_ll.2));
        w.add(u64::from(c_ml.1), u32::from(c_ml.2));
        w.add(u64::from(c_of.1), u32::from(c_of.2));
    }
    s_ml.flush(&mut w);
    s_of.flush(&mut w);
    s_ll.flush(&mut w);
    w.close();
    Ok(())
}

/// What one [`Encoder::compress`] call wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Written {
    /// Bytes written to the output.
    pub output_len: usize,
    /// Bytes of the frame still to hand out; 0 once the frame is out.
    pub remaining: usize,
}

/// A frame at a time, handed out in pieces of the caller's size: RocksDB's
/// `ZSTDStreamingCompress`, which compresses one record per frame (`ZSTD_e_end`) and calls again
/// with the same record until nothing remains [R util/compression.cc:190-231].
#[derive(Debug)]
pub struct Encoder {
    level: Level,
    checksum: bool,
    compressor: Option<Compressor>,
    frame: Vec<u8>,
    /// Bytes of `frame` handed out.
    at: usize,
    /// Whether `frame` is the current record's, still being handed out.
    active: bool,
}

impl Encoder {
    /// An encoder at `level`, each frame with the content checksum when `checksum`.
    pub fn new(level: Level, checksum: bool) -> Self {
        Self {
            level,
            checksum,
            compressor: None,
            frame: Vec::new(),
            at: 0,
            active: false,
        }
    }

    /// Continues `input`'s frame into `output`: the first call for an input compresses it, each
    /// call hands out what `output` holds. An empty input writes nothing.
    pub fn compress(&mut self, input: &[u8], output: &mut [u8]) -> Result<Written, Error> {
        if input.is_empty() {
            return Ok(Written {
                output_len: 0,
                remaining: 0,
            });
        }
        if !self.active {
            let compressor = match &mut self.compressor {
                Some(c) => c,
                None => self.compressor.insert(Compressor::new()?),
            };
            self.frame.clear();
            compressor.compress_into(input, self.level, self.checksum, &mut self.frame)?;
            self.at = 0;
            self.active = true;
        }
        let rest = self.frame.get(self.at..).unwrap_or_default();
        let n = rest.len().min(output.len());
        if let (Some(dst), Some(src)) = (output.get_mut(..n), rest.get(..n)) {
            dst.copy_from_slice(src);
        }
        self.at = self.at.saturating_add(n);
        let remaining = self.frame.len().saturating_sub(self.at);
        if remaining == 0 {
            self.active = false;
        }
        Ok(Written {
            output_len: n,
            remaining,
        })
    }

    /// Ends the frame being handed out, so the next input starts a new one.
    pub fn reset(&mut self) {
        self.active = false;
        self.at = 0;
        self.frame.clear();
    }
}
