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
    EncodeState, EncodeTable, LITERALS_LENGTH_DEFAULT, LITERALS_LENGTH_DEFAULT_LOG,
    MATCH_LENGTH_DEFAULT, MATCH_LENGTH_DEFAULT_LOG, OFFSET_DEFAULT, OFFSET_DEFAULT_LOG, normalize,
    table_log, write_distribution,
};
use super::huffman::{HuffmanEncoder, MAX_BITS, code_lengths};
use super::sequences::{
    LITERALS_LENGTH_CODES, LITERALS_LENGTH_MAX_LOG, MATCH_LENGTH_CODES, MATCH_LENGTH_MAX_LOG,
    OFFSET_MAX_LOG,
};
use crate::error::Error;
use crate::util::xxhash::xxh64;

/// The bytes the match finder hashes: a match it finds is at least this long, whatever shorter
/// one a level allows (the format codes matches from 3, §3.1.1.3.2.1.1).
const HASHED: usize = 4;
/// The highest offset code the predefined offset table holds (§3.1.1.3.2.2.3: N = 28).
const OFFSET_DEFAULT_MAX_CODE: u8 = 28;

/// The reference's parameters for inputs above 256 KB, by level 0 (the base of the negative
/// levels) to 22 (zstd 1.5.7, lib/compress/clevels.h:28-50): the hash table's log `H`, the search
/// log `S` (2^S chain positions tried), the shortest match `L`, and whether the strategy is lazy
/// (`ZSTD_lazy` and beyond) or greedy and faster (`ZSTD_fast`, `ZSTD_dfast`, `ZSTD_greedy`).
const LEVELS: [(u32, u32, usize, bool); 23] = [
    (13, 1, 6, false),
    (14, 1, 7, false),
    (16, 1, 6, false),
    (17, 1, 5, false),
    (18, 1, 5, false),
    (19, 3, 5, false),
    (19, 3, 5, true),
    (20, 4, 5, true),
    (20, 4, 5, true),
    (21, 4, 5, true),
    (22, 5, 5, true),
    (22, 6, 5, true),
    (23, 6, 5, true),
    (22, 4, 5, true),
    (23, 5, 5, true),
    (23, 6, 5, true),
    (22, 5, 5, true),
    (22, 5, 4, true),
    (22, 6, 3, true),
    (22, 7, 3, true),
    (23, 7, 3, true),
    (24, 7, 3, true),
    (25, 9, 3, true),
];

/// How hard the encoder looks for matches, a level's row of [`LEVELS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Level {
    hash_log: u32,
    chain: u32,
    min_match: usize,
    lazy: bool,
}

impl Level {
    /// A level by number, as RocksDB passes one: 1 to 22, the negative levels taking the
    /// reference's base row (0), above 22 the 22nd.
    pub fn new(level: i32) -> Self {
        let row = usize::try_from(level.clamp(0, 22)).unwrap_or(0);
        let (hash_log, search_log, min_match, lazy) =
            LEVELS.get(row).copied().unwrap_or((13, 1, 6, false));
        Self {
            hash_log,
            chain: 1u32 << search_log,
            min_match: min_match.max(HASHED),
            lazy,
        }
    }
}

/// Compresses `input` as one frame, with the content checksum when `checksum`.
pub fn compress(input: &[u8], level: Level, checksum: bool) -> Result<Vec<u8>, Error> {
    Ok(encode_frame(input, level, checksum)?)
}

fn encode_frame(input: &[u8], level: Level, checksum: bool) -> Result<Vec<u8>, Corrupt> {
    let mut out = Vec::with_capacity((input.len() / 2).saturating_add(32));
    out.extend_from_slice(&FRAME_MAGIC.to_le_bytes());
    frame_header(&mut out, input.len(), checksum);
    let mut matcher = Matcher::new(input, level);
    let mut offsets = [1u32, 4, 8];
    let mut start = 0usize;
    loop {
        let end = start.saturating_add(BLOCK_MAX).min(input.len());
        let last = end == input.len();
        let block = input.get(start..end).ok_or(Corrupt::Block)?;
        encode_block(
            &mut out,
            input,
            start,
            end,
            block,
            last,
            &mut matcher,
            &mut offsets,
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
    Ok(out)
}

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

/// One block: compressed if that is smaller than raw, RLE when every byte is one.
#[allow(clippy::too_many_arguments)]
fn encode_block(
    out: &mut Vec<u8>,
    input: &[u8],
    start: usize,
    end: usize,
    block: &[u8],
    last: bool,
    matcher: &mut Matcher<'_>,
    offsets: &mut [u32; 3],
) -> Result<(), Corrupt> {
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
    let sequences = matcher.sequences(start, end, offsets);
    let compressed = compress_block(input, start, end, &sequences)?;
    if compressed.len() < block.len() {
        out.extend_from_slice(&header(2, compressed.len())?);
        out.extend_from_slice(&compressed);
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
    head: Vec<u32>,
    chain: Vec<u32>,
    /// Positions inserted so far.
    next: usize,
}

/// No position: the hash table's and chains' empty value.
const NONE: u32 = u32::MAX;

impl<'a> Matcher<'a> {
    fn new(input: &'a [u8], level: Level) -> Self {
        Self {
            input,
            level,
            head: vec![NONE; 1usize << level.hash_log],
            chain: vec![NONE; input.len()],
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
    fn common(&self, a: usize, b: usize, limit: usize) -> usize {
        let (Some(x), Some(y)) = (self.input.get(a..), self.input.get(b..limit)) else {
            return 0;
        };
        x.iter().zip(y).take_while(|(p, q)| p == q).count()
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
    fn sequences(&mut self, start: usize, end: usize, offsets: &mut [u32; 3]) -> Vec<Found> {
        let mut out = Vec::new();
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
        out
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
/// count.
fn code_for(value: u32, table: &[(u32, u8)]) -> (u8, u32, u8) {
    let code = table
        .iter()
        .rposition(|&(base, _)| base <= value)
        .unwrap_or(0);
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

/// A compressed block's content: the literals section, then the sequences section (§3.1.1.3).
fn compress_block(
    input: &[u8],
    start: usize,
    end: usize,
    found: &[Found],
) -> Result<Vec<u8>, Corrupt> {
    let mut literals = Vec::with_capacity(end.saturating_sub(start));
    let mut at = start;
    for f in found {
        let to = at.saturating_add(usize::try_from(f.literals).unwrap_or(0));
        literals.extend_from_slice(input.get(at..to).ok_or(Corrupt::Block)?);
        at = to.saturating_add(usize::try_from(f.length).unwrap_or(0));
    }
    literals.extend_from_slice(input.get(at..end).ok_or(Corrupt::Block)?);
    let mut out = literals_section(&literals)?;
    out.extend_from_slice(&sequences_section(found)?);
    Ok(out)
}

/// The literals section (§3.1.1.3.1): RLE when every literal is one, Huffman-coded when that
/// saves bytes, raw otherwise.
fn literals_section(literals: &[u8]) -> Result<Vec<u8>, Corrupt> {
    let n = literals.len();
    if let Some(&first) = literals.first()
        && n > 1
        && literals.iter().all(|&b| b == first)
    {
        let mut out = raw_or_rle_header(1, n)?;
        out.push(first);
        return Ok(out);
    }
    let mut raw = raw_or_rle_header(0, n)?;
    raw.extend_from_slice(literals);
    if let Ok(coded) = huffman_literals(literals)
        && coded.len() < raw.len()
    {
        return Ok(coded);
    }
    Ok(raw)
}

/// A raw or RLE literals header of the shortest size format (§3.1.1.3.1.1).
fn raw_or_rle_header(kind: u8, size: usize) -> Result<Vec<u8>, Corrupt> {
    let s = u32::try_from(size).map_err(|_| Corrupt::Literals)?;
    let kind = u32::from(kind);
    Ok(if s < 32 {
        vec![u8::try_from(kind | (s << 3)).map_err(|_| Corrupt::Literals)?]
    } else if s < 4096 {
        let v = kind | (1 << 2) | (s << 4);
        v.to_le_bytes().get(..2).ok_or(Corrupt::Literals)?.to_vec()
    } else {
        let v = kind | (3 << 2) | (s << 4);
        v.to_le_bytes().get(..3).ok_or(Corrupt::Literals)?.to_vec()
    })
}

/// Huffman-coded literals with their tree (§3.1.1.3.1.4): one stream below 256 literals, four
/// above, as the reference chooses.
fn huffman_literals(literals: &[u8]) -> Result<Vec<u8>, Corrupt> {
    let mut counts = [0u32; 256];
    for &b in literals {
        let c = counts.get_mut(usize::from(b)).ok_or(Corrupt::Huffman)?;
        *c = c.saturating_add(1);
    }
    let lengths = code_lengths(&counts, MAX_BITS);
    let table = HuffmanEncoder::new(&lengths)?;
    let description = table.description()?;
    let four = literals.len() >= 256;
    let streams = table.streams(literals, four)?;
    let compressed = description.len().saturating_add(streams.len());
    let regenerated = literals.len();
    let r = u32::try_from(regenerated).map_err(|_| Corrupt::Literals)?;
    let c = u32::try_from(compressed).map_err(|_| Corrupt::Literals)?;
    // Size_Format by the larger of the two sizes (§3.1.1.3.1.1); one stream only with 10 bits.
    let mut out = if !four && r < 1024 && c < 1024 {
        let v = 2u32 | (r << 4) | (c << 14);
        v.to_le_bytes().get(..3).ok_or(Corrupt::Literals)?.to_vec()
    } else if !four {
        return Err(Corrupt::Literals);
    } else if r < 1024 && c < 1024 {
        let v = 2u32 | (1 << 2) | (r << 4) | (c << 14);
        v.to_le_bytes().get(..3).ok_or(Corrupt::Literals)?.to_vec()
    } else if r < 16_384 && c < 16_384 {
        let v = 2u32 | (2 << 2) | (r << 4) | (c << 18);
        v.to_le_bytes().get(..4).ok_or(Corrupt::Literals)?.to_vec()
    } else if r < 262_144 && c < 262_144 {
        let v = 2u64 | (3 << 2) | (u64::from(r) << 4) | (u64::from(c) << 22);
        v.to_le_bytes().get(..5).ok_or(Corrupt::Literals)?.to_vec()
    } else {
        return Err(Corrupt::Literals);
    };
    out.extend_from_slice(&description);
    out.extend_from_slice(&streams);
    Ok(out)
}

/// One symbol type's table for the block: its mode (§3.1.1.3.2.1, Table 15), its description, and
/// the encoding table.
struct Chosen {
    mode: u8,
    description: Vec<u8>,
    /// None in RLE mode: its one state takes no bits to start, step or flush.
    table: Option<EncodeTable>,
}

/// A state over a chosen table; none in RLE mode, where nothing is written.
struct Coder<'t> {
    table: Option<&'t EncodeTable>,
    state: Option<EncodeState>,
}

impl<'t> Coder<'t> {
    fn init(chosen: &'t Chosen, symbol: u8) -> Result<Self, Corrupt> {
        let table = chosen.table.as_ref();
        let state = table.map(|t| EncodeState::init(t, symbol)).transpose()?;
        Ok(Self { table, state })
    }

    fn encode(&mut self, symbol: u8, w: &mut Writer) -> Result<(), Corrupt> {
        if let (Some(t), Some(s)) = (self.table, self.state.as_mut()) {
            s.encode(t, symbol, w)?;
        }
        Ok(())
    }

    fn flush(self, w: &mut Writer) {
        if let (Some(t), Some(s)) = (self.table, self.state) {
            s.flush(t, w);
        }
    }
}

/// The cheapest of predefined, RLE and the block's own FSE table for `codes` (each below
/// `symbols`), by the bits each spends.
fn choose(
    codes: &[u8],
    symbols: usize,
    max_log: u32,
    default: &[i16],
    default_log: u32,
) -> Result<Chosen, Corrupt> {
    let mut counts = vec![0u32; symbols];
    for &c in codes {
        let slot = counts.get_mut(usize::from(c)).ok_or(Corrupt::Sequences)?;
        *slot = slot.saturating_add(1);
    }
    let max_symbol = counts
        .iter()
        .rposition(|&c| c > 0)
        .ok_or(Corrupt::Sequences)?;
    let present = counts.iter().filter(|&&c| c > 0).count();
    if present == 1 {
        let symbol = u8::try_from(max_symbol).map_err(|_| Corrupt::Sequences)?;
        // One symbol: RLE (FSE_Compressed_Mode needs two, §3.1.1.3.2.1).
        return Ok(Chosen {
            mode: 1,
            description: vec![symbol],
            table: None,
        });
    }
    let total = u32::try_from(codes.len()).map_err(|_| Corrupt::Sequences)?;
    let log = table_log(codes.len(), max_symbol, max_log);
    let counts = counts.get(..=max_symbol).ok_or(Corrupt::Sequences)?;
    let norm = normalize(counts, total, log)?;
    let description = write_distribution(&norm, log)?;
    // The table's own bits and its description's, both in 256ths of a bit.
    let own_bits = cost(counts, &norm, log).saturating_add(
        u64::try_from(description.len())
            .unwrap_or(0)
            .saturating_mul(8 * 256),
    );
    // The predefined table covers the codes only if every code present has a cell in it.
    let default_fits = counts
        .iter()
        .enumerate()
        .all(|(s, &c)| c == 0 || default.get(s).is_some_and(|&p| p != 0));
    if default_fits && cost(counts, default, default_log) <= own_bits {
        return Ok(Chosen {
            mode: 0,
            description: Vec::new(),
            table: Some(EncodeTable::new(default, default_log)?),
        });
    }
    Ok(Chosen {
        mode: 2,
        table: Some(EncodeTable::new(&norm, log)?),
        description,
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
/// most a cost can be.
fn cost(counts: &[u32], norm: &[i16], log: u32) -> u64 {
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
        let each = (u64::from(log) << 8).saturating_sub(log2_256(cells));
        total = total.saturating_add(u64::from(c).saturating_mul(each));
    }
    total
}

/// The sequences section (§3.1.1.3.2): the count, the modes, the descriptions, and the
/// interleaved bitstream, written last sequence first.
fn sequences_section(found: &[Found]) -> Result<Vec<u8>, Corrupt> {
    let n = found.len();
    let mut out = Vec::new();
    match n {
        0 => {
            out.push(0);
            return Ok(out);
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
    let ll: Vec<(u8, u32, u8)> = found
        .iter()
        .map(|f| literals_length_code(f.literals))
        .collect();
    let ml: Vec<(u8, u32, u8)> = found.iter().map(|f| match_length_code(f.length)).collect();
    let of: Vec<(u8, u32, u8)> = found.iter().map(|f| offset_code(f.offset_value)).collect();
    let ll_codes: Vec<u8> = ll.iter().map(|c| c.0).collect();
    let ml_codes: Vec<u8> = ml.iter().map(|c| c.0).collect();
    let of_codes: Vec<u8> = of.iter().map(|c| c.0).collect();
    let ll_t = choose(
        &ll_codes,
        LITERALS_LENGTH_CODES.len(),
        LITERALS_LENGTH_MAX_LOG,
        &LITERALS_LENGTH_DEFAULT,
        LITERALS_LENGTH_DEFAULT_LOG,
    )?;
    // Offset codes above the predefined table's N cannot use it; `choose` sees that as no cell.
    let of_max = of_codes.iter().copied().max().unwrap_or(0);
    let of_t = if of_max > OFFSET_DEFAULT_MAX_CODE {
        choose(
            &of_codes,
            usize::from(of_max) + 1,
            OFFSET_MAX_LOG,
            &[],
            OFFSET_DEFAULT_LOG,
        )?
    } else {
        choose(
            &of_codes,
            32,
            OFFSET_MAX_LOG,
            &OFFSET_DEFAULT,
            OFFSET_DEFAULT_LOG,
        )?
    };
    let ml_t = choose(
        &ml_codes,
        MATCH_LENGTH_CODES.len(),
        MATCH_LENGTH_MAX_LOG,
        &MATCH_LENGTH_DEFAULT,
        MATCH_LENGTH_DEFAULT_LOG,
    )?;
    out.push((ll_t.mode << 6) | (of_t.mode << 4) | (ml_t.mode << 2));
    out.extend_from_slice(&ll_t.description);
    out.extend_from_slice(&of_t.description);
    out.extend_from_slice(&ml_t.description);
    // The bitstream, the reference's ZSTD_encodeSequences: the last sequence's states and bits
    // first, then each earlier one, then the states flushed, read back in the decoder's order.
    let mut w = Writer::new();
    let last = n.saturating_sub(1);
    let get = |v: &[(u8, u32, u8)], i: usize| v.get(i).copied().ok_or(Corrupt::Sequences);
    let (l_ll, l_ml, l_of) = (get(&ll, last)?, get(&ml, last)?, get(&of, last)?);
    let mut s_ml = Coder::init(&ml_t, l_ml.0)?;
    let mut s_of = Coder::init(&of_t, l_of.0)?;
    let mut s_ll = Coder::init(&ll_t, l_ll.0)?;
    w.add(u64::from(l_ll.1), u32::from(l_ll.2));
    w.add(u64::from(l_ml.1), u32::from(l_ml.2));
    w.add(u64::from(l_of.1), u32::from(l_of.2));
    for i in (0..last).rev() {
        let (c_ll, c_ml, c_of) = (get(&ll, i)?, get(&ml, i)?, get(&of, i)?);
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
    out.extend_from_slice(&w.close());
    Ok(out)
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
            self.frame = encode_frame(input, self.level, self.checksum)?;
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
