//! Huffman-coded literals (RFC 8878 §4.2): the tree description (§4.2.1), its weights direct or
//! FSE-compressed (§4.2.1.1, §4.2.1.2), the prefix codes they give (§4.2.1.3), and the streams
//! (§4.2.2), one or four behind a jump table (§3.1.1.3.1.6).

use super::Corrupt;
use super::bits::Backward;
use super::fse::{State, Table, read_distribution};

/// The longest prefix code (§4.2.1: "limits the maximum code length to 11 bits").
pub(super) const MAX_BITS: u32 = 11;
/// The most weights a description holds: literals 0 to 254, the last one's being deduced
/// (§4.2.1.2).
const MAX_WEIGHTS: usize = 255;
/// The weights' FSE accuracy log at most (§4.2.1.2).
const WEIGHTS_MAX_LOG: u32 = 6;
/// Weights an FSE description can name: 0 to `MAX_BITS`.
const WEIGHT_SYMBOLS: usize = 12;
/// A direct description's header bytes start here (§4.2.1.1).
const DIRECT: u8 = 128;
/// The jump table's length before four streams (§3.1.1.3.1.6).
const JUMP_TABLE: usize = 6;

/// One entry of the decoding table: the literal and the bits its code takes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Entry {
    symbol: u8,
    bits: u8,
}

/// A decoding table indexed by the stream's next `max_bits` bits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Huffman {
    max_bits: u32,
    entries: Vec<Entry>,
}

impl Huffman {
    /// Reads a tree description from the front of `bytes`, returning the table and the bytes it
    /// took.
    pub(super) fn read(bytes: &[u8]) -> Result<(Self, usize), Corrupt> {
        let header = *bytes.first().ok_or(Corrupt::Huffman)?;
        let (weights, used) = if header >= DIRECT {
            direct_weights(bytes, header)?
        } else {
            fse_weights(bytes, header)?
        };
        Ok((Self::from_weights(&weights)?, used))
    }

    /// The table of a series of weights, the last literal's weight deduced by completing to a
    /// power of two (§4.2.1).
    fn from_weights(weights: &[u8]) -> Result<Self, Corrupt> {
        let mut total = 0u32;
        for &w in weights {
            if u32::from(w) > MAX_BITS {
                return Err(Corrupt::Huffman);
            }
            if w > 0 {
                let share = 1u32
                    .checked_shl(u32::from(w).checked_sub(1).ok_or(Corrupt::Huffman)?)
                    .ok_or(Corrupt::Huffman)?;
                total = total.checked_add(share).ok_or(Corrupt::Huffman)?;
            }
        }
        if total == 0 {
            return Err(Corrupt::Huffman);
        }
        // The bits of the next power of two above the total.
        let max_bits = u32::BITS.saturating_sub(total.leading_zeros());
        if max_bits > MAX_BITS {
            return Err(Corrupt::Huffman);
        }
        let rest = 1u32
            .checked_shl(max_bits)
            .and_then(|p| p.checked_sub(total))
            .ok_or(Corrupt::Huffman)?;
        if !rest.is_power_of_two() {
            return Err(Corrupt::Huffman);
        }
        let last = u8::try_from(u32::BITS.saturating_sub(rest.leading_zeros()))
            .map_err(|_| Corrupt::Huffman)?;
        let mut all = Vec::with_capacity(weights.len().saturating_add(1));
        all.extend_from_slice(weights);
        all.push(last);
        if all.len() > 256 {
            return Err(Corrupt::Huffman);
        }
        // Codes are handed out by weight, lowest first, and by literal within a weight
        // (§4.2.1.3): each weight's run of the table starts where the lower weights' end.
        let mut start = [0usize; MAX_BITS as usize + 2];
        let mut next = 0usize;
        for w in 1..=usize::try_from(max_bits).map_err(|_| Corrupt::Huffman)? {
            let count = all.iter().filter(|&&x| usize::from(x) == w).count();
            if let Some(s) = start.get_mut(w) {
                *s = next;
            }
            next = next
                .checked_add(
                    count
                        .checked_shl(u32::try_from(w.saturating_sub(1)).unwrap_or(0))
                        .ok_or(Corrupt::Huffman)?,
                )
                .ok_or(Corrupt::Huffman)?;
        }
        let size = 1usize.checked_shl(max_bits).ok_or(Corrupt::Huffman)?;
        if next != size {
            return Err(Corrupt::Huffman);
        }
        let mut entries = vec![Entry::default(); size];
        for (symbol, &w) in all.iter().enumerate() {
            if w == 0 {
                continue;
            }
            let w = usize::from(w);
            let span = 1usize
                .checked_shl(u32::try_from(w.saturating_sub(1)).map_err(|_| Corrupt::Huffman)?)
                .ok_or(Corrupt::Huffman)?;
            let at = *start.get(w).ok_or(Corrupt::Huffman)?;
            // Number_of_Bits = Max_Number_of_Bits + 1 - Weight (§4.2.1).
            let bits = max_bits
                .checked_add(1)
                .and_then(|b| b.checked_sub(u32::try_from(w).ok()?))
                .and_then(|b| u8::try_from(b).ok())
                .ok_or(Corrupt::Huffman)?;
            let end = at.checked_add(span).ok_or(Corrupt::Huffman)?;
            let symbol = u8::try_from(symbol).map_err(|_| Corrupt::Huffman)?;
            for e in entries.get_mut(at..end).ok_or(Corrupt::Huffman)? {
                *e = Entry { symbol, bits };
            }
            if let Some(s) = start.get_mut(w) {
                *s = end;
            }
        }
        Ok(Self { max_bits, entries })
    }

    /// Decodes one stream into `out`, which it fills exactly; the stream must be consumed
    /// exactly (§4.2.2).
    fn stream(&self, stream: &[u8], out: &mut [u8]) -> Result<(), Corrupt> {
        let mut bits = Backward::new(stream)?;
        for byte in out.iter_mut() {
            let index = bits.peek(self.max_bits);
            let entry = self
                .entries
                .get(usize::try_from(index).map_err(|_| Corrupt::Huffman)?)
                .ok_or(Corrupt::Huffman)?;
            *byte = entry.symbol;
            bits.skip(u32::from(entry.bits));
        }
        if !bits.finished() {
            return Err(Corrupt::Huffman);
        }
        Ok(())
    }

    /// Decodes `streams` (one stream, or four behind a jump table) into `out`, which takes
    /// exactly the regenerated size (§3.1.1.3.1.6).
    pub(super) fn literals(
        &self,
        streams: &[u8],
        four: bool,
        out: &mut [u8],
    ) -> Result<(), Corrupt> {
        if !four {
            return self.stream(streams, out);
        }
        let (jump, rest) = streams
            .split_at_checked(JUMP_TABLE)
            .ok_or(Corrupt::Huffman)?;
        let size = |i: usize| -> usize {
            let at = i.saturating_mul(2);
            let lo = jump.get(at).copied().unwrap_or(0);
            let hi = jump.get(at.saturating_add(1)).copied().unwrap_or(0);
            usize::from(u16::from_le_bytes([lo, hi]))
        };
        let (s1, s2, s3) = (size(0), size(1), size(2));
        let first3 = s1
            .checked_add(s2)
            .and_then(|x| x.checked_add(s3))
            .ok_or(Corrupt::Huffman)?;
        if first3 > rest.len() {
            return Err(Corrupt::Huffman);
        }
        let segment = out.len().div_ceil(4);
        // The first three segments are whole; the fourth takes what is left, at most 3 bytes
        // fewer, never a negative size.
        if segment.checked_mul(3).is_none_or(|three| three > out.len()) {
            return Err(Corrupt::Huffman);
        }
        let mut input = rest;
        let mut output = &mut *out;
        for len in [Some(s1), Some(s2), Some(s3), None] {
            let (stream, tail) = match len {
                Some(n) => input.split_at_checked(n).ok_or(Corrupt::Huffman)?,
                None => (input, &[][..]),
            };
            let take = if len.is_some() {
                segment.min(output.len())
            } else {
                output.len()
            };
            let (part, after) = output.split_at_mut(take);
            self.stream(stream, part)?;
            input = tail;
            output = after;
        }
        Ok(())
    }
}

/// Weights written directly, 4 bits each, two to a byte, high nibble first (§4.2.1.1).
fn direct_weights(bytes: &[u8], header: u8) -> Result<(Vec<u8>, usize), Corrupt> {
    // Number_of_Symbols = headerByte - 127 (§4.2.1.1); `header` is at least `DIRECT`.
    let count = usize::from(header.saturating_sub(DIRECT - 1));
    let len = count.div_ceil(2);
    let end = len.checked_add(1).ok_or(Corrupt::Huffman)?;
    let packed = bytes.get(1..end).ok_or(Corrupt::Huffman)?;
    let weights = packed
        .iter()
        .flat_map(|b| [b >> 4, b & 0xF])
        .take(count)
        .collect();
    Ok((weights, end))
}

/// Weights compressed by FSE, two interleaved states over one table, until a state's update
/// reads past the stream's start (§4.2.1.2).
fn fse_weights(bytes: &[u8], header: u8) -> Result<(Vec<u8>, usize), Corrupt> {
    // The FSE-compressed series is `headerByte` bytes long (§4.2.1.1).
    let end = usize::from(header).checked_add(1).ok_or(Corrupt::Huffman)?;
    let body = bytes.get(1..end).ok_or(Corrupt::Huffman)?;
    let (norm, log, used) = read_distribution(body, WEIGHT_SYMBOLS, WEIGHTS_MAX_LOG)?;
    let table = Table::from_distribution(&norm, log)?;
    let stream = body.get(used..).ok_or(Corrupt::Huffman)?;
    let mut bits = Backward::new(stream)?;
    let mut states = [
        State::init(&table, &mut bits),
        State::init(&table, &mut bits),
    ];
    let mut weights = Vec::with_capacity(MAX_WEIGHTS);
    'decode: loop {
        for i in 0..2 {
            // One weight short of the most, so the other state's last symbol still fits.
            if weights.len() >= MAX_WEIGHTS.saturating_sub(1) {
                return Err(Corrupt::Huffman);
            }
            let state = states.get_mut(i).ok_or(Corrupt::Huffman)?;
            weights.push(state.symbol(&table)?);
            state.update(&table, &mut bits)?;
            if bits.overflowed() {
                // The other state's symbol is the last.
                let other = states.get(i ^ 1).ok_or(Corrupt::Huffman)?;
                weights.push(other.symbol(&table)?);
                break 'decode;
            }
        }
    }
    Ok((weights, end))
}
