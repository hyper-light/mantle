//! Huffman-coded literals (RFC 8878 §4.2): the tree description (§4.2.1), its weights direct or
//! FSE-compressed (§4.2.1.1, §4.2.1.2), the prefix codes they give (§4.2.1.3), and the streams
//! (§4.2.2), one or four behind a jump table (§3.1.1.3.1.6).

use super::Corrupt;
use super::bits::{Backward, Writer};
use super::fse::{
    EncodeState, EncodeTable, State, Table, normalize, read_distribution, table_log,
    write_distribution,
};

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

/// Optimal code lengths of at most `limit` bits for `counts` (one per literal, 0 for absent):
/// the package-merge algorithm (Larmore and Hirschberg, "A fast algorithm for optimal
/// length-limited Huffman codes", JACM 37(3), 1990). Absent literals get 0; a single present
/// literal gets 1.
pub(super) fn code_lengths(counts: &[u32], limit: u32) -> Vec<u8> {
    let mut lengths = vec![0u8; counts.len()];
    let mut present: Vec<(u64, usize)> = counts
        .iter()
        .enumerate()
        .filter(|&(_, &c)| c > 0)
        .map(|(s, &c)| (u64::from(c), s))
        .collect();
    if present.len() == 1 {
        if let Some(&(_, s)) = present.first()
            && let Some(l) = lengths.get_mut(s)
        {
            *l = 1;
        }
        return lengths;
    }
    present.sort_unstable();
    // Each item: its weight and the leaves (symbols) it packages, as counts per leaf index.
    let leaves: Vec<(u64, Vec<usize>)> = present.iter().map(|&(w, s)| (w, vec![s])).collect();
    let mut row: Vec<(u64, Vec<usize>)> = leaves.clone();
    for _ in 1..limit {
        // Package pairs of the previous row, then merge with the leaves by weight.
        let mut packages: Vec<(u64, Vec<usize>)> = Vec::with_capacity(row.len() / 2);
        for [(wa, a), (wb, b)] in row.as_chunks::<2>().0 {
            {
                let mut both = a.clone();
                both.extend_from_slice(b);
                packages.push((wa.saturating_add(*wb), both));
            }
        }
        let mut merged = Vec::with_capacity(leaves.len().saturating_add(packages.len()));
        let (mut i, mut j) = (0, 0);
        while i < leaves.len() || j < packages.len() {
            let take_leaf = match (leaves.get(i), packages.get(j)) {
                (Some(l), Some(p)) => l.0 <= p.0,
                (Some(_), None) => true,
                _ => false,
            };
            if take_leaf {
                if let Some(l) = leaves.get(i) {
                    merged.push(l.clone());
                }
                i = i.saturating_add(1);
            } else {
                if let Some(p) = packages.get(j) {
                    merged.push(p.clone());
                }
                j = j.saturating_add(1);
            }
        }
        row = merged;
    }
    // The first 2n − 2 items of the last row: each leaf's length is how many hold it.
    let take = present.len().saturating_mul(2).saturating_sub(2);
    for (_, symbols) in row.iter().take(take) {
        for &s in symbols {
            if let Some(l) = lengths.get_mut(s) {
                *l = l.saturating_add(1);
            }
        }
    }
    lengths
}

/// An encoding table: each literal's code and its length.
#[derive(Clone, Debug)]
pub(super) struct HuffmanEncoder {
    codes: Vec<(u32, u8)>,
    /// The weights written in the tree description, the last present literal's left out.
    weights: Vec<u8>,
}

impl HuffmanEncoder {
    /// The table of `lengths` (each at most [`MAX_BITS`], at least two present): weights from the
    /// lengths (§4.2.1), codes handed out as the decoder's table lays them (§4.2.1.3).
    pub(super) fn new(lengths: &[u8]) -> Result<Self, Corrupt> {
        let max_bits = u32::from(lengths.iter().copied().max().unwrap_or(0));
        if max_bits == 0 || max_bits > MAX_BITS {
            return Err(Corrupt::Huffman);
        }
        let weight = |len: u8| -> u8 {
            if len == 0 {
                0
            } else {
                u8::try_from(max_bits.saturating_add(1).saturating_sub(u32::from(len))).unwrap_or(0)
            }
        };
        let last = lengths
            .iter()
            .rposition(|&l| l > 0)
            .ok_or(Corrupt::Huffman)?;
        let weights: Vec<u8> = lengths
            .get(..last)
            .ok_or(Corrupt::Huffman)?
            .iter()
            .map(|&l| weight(l))
            .collect();
        // The decoder's table: weight w's literals in order, each 2^(w−1) entries, the lowest
        // weight's first; a code is its first entry's index shifted down by the bits it skips.
        let mut start = [0u32; MAX_BITS as usize + 2];
        let mut next = 0u32;
        for w in 1..=max_bits {
            let count = lengths
                .iter()
                .filter(|&&l| l > 0 && u32::from(weight(l)) == w)
                .count();
            if let Some(s) = start.get_mut(usize::try_from(w).unwrap_or(0)) {
                *s = next;
            }
            next = next.saturating_add(u32::try_from(count).unwrap_or(0) << w.saturating_sub(1));
        }
        if next != 1 << max_bits {
            return Err(Corrupt::Huffman);
        }
        let mut codes = vec![(0u32, 0u8); lengths.len()];
        for (&len, code) in lengths.iter().zip(codes.iter_mut()) {
            if len == 0 {
                continue;
            }
            let w = usize::from(weight(len));
            let at = *start.get(w).ok_or(Corrupt::Huffman)?;
            *code = (at >> (max_bits.saturating_sub(u32::from(len))), len);
            if let Some(s) = start.get_mut(w) {
                *s = at.saturating_add(1 << w.saturating_sub(1));
            }
        }
        Ok(Self { codes, weights })
    }

    /// The tree description (§4.2.1.1): the weights FSE-compressed when that is shorter, written
    /// directly otherwise (possible for at most 128 weights).
    pub(super) fn description(&self) -> Result<Vec<u8>, Corrupt> {
        let direct = if self.weights.len() <= 128 {
            let mut out = vec![
                u8::try_from(self.weights.len().saturating_add(127))
                    .map_err(|_| Corrupt::Huffman)?,
            ];
            for pair in self.weights.chunks(2) {
                let hi = pair.first().copied().unwrap_or(0);
                let lo = pair.get(1).copied().unwrap_or(0);
                out.push((hi << 4) | lo);
            }
            Some(out)
        } else {
            None
        };
        let compressed = compress_weights(&self.weights).ok();
        match (direct, compressed) {
            (Some(d), Some(c)) => Ok(if c.len() < d.len() { c } else { d }),
            (Some(d), None) => Ok(d),
            (None, Some(c)) => Ok(c),
            (None, None) => Err(Corrupt::Huffman),
        }
    }

    /// One stream of `literals`, written last literal first so the decoder, reading backward,
    /// meets them in order (§4.2.2).
    pub(super) fn stream(&self, literals: &[u8]) -> Result<Vec<u8>, Corrupt> {
        let mut w = Writer::new();
        for &b in literals.iter().rev() {
            let &(code, len) = self.codes.get(usize::from(b)).ok_or(Corrupt::Huffman)?;
            if len == 0 {
                return Err(Corrupt::Huffman);
            }
            w.add(u64::from(code), u32::from(len));
        }
        Ok(w.close())
    }

    /// The streams of `literals`: one, or four behind their jump table (§3.1.1.3.1.6).
    pub(super) fn streams(&self, literals: &[u8], four: bool) -> Result<Vec<u8>, Corrupt> {
        if !four {
            return self.stream(literals);
        }
        let segment = literals.len().div_ceil(4);
        let mut parts = Vec::with_capacity(4);
        for i in 0..4usize {
            let from = segment.saturating_mul(i).min(literals.len());
            let to = if i == 3 {
                literals.len()
            } else {
                segment
                    .saturating_mul(i.saturating_add(1))
                    .min(literals.len())
            };
            parts.push(self.stream(literals.get(from..to).ok_or(Corrupt::Huffman)?)?);
        }
        let mut out = Vec::with_capacity(literals.len());
        for part in parts.iter().take(3) {
            let size = u16::try_from(part.len()).map_err(|_| Corrupt::Huffman)?;
            out.extend_from_slice(&size.to_le_bytes());
        }
        for part in &parts {
            out.extend_from_slice(part);
        }
        Ok(out)
    }
}

/// The weights compressed by FSE with two interleaved states (§4.2.1.2), the header byte its
/// length: the inverse of [`fse_weights`] (the reference's `FSE_compress_usingCTable`).
fn compress_weights(weights: &[u8]) -> Result<Vec<u8>, Corrupt> {
    if weights.len() < 2 {
        return Err(Corrupt::Huffman);
    }
    let mut counts = [0u32; WEIGHT_SYMBOLS];
    for &w in weights {
        let c = counts.get_mut(usize::from(w)).ok_or(Corrupt::Huffman)?;
        *c = c.saturating_add(1);
    }
    let max_symbol = counts
        .iter()
        .rposition(|&c| c > 0)
        .ok_or(Corrupt::Huffman)?;
    let counts = counts.get(..=max_symbol).ok_or(Corrupt::Huffman)?;
    if counts.iter().filter(|&&c| c > 0).count() < 2 {
        return Err(Corrupt::Huffman);
    }
    let total = u32::try_from(weights.len()).map_err(|_| Corrupt::Huffman)?;
    let log = table_log(weights.len(), max_symbol, WEIGHTS_MAX_LOG);
    let norm = normalize(counts, total, log)?;
    let table = EncodeTable::new(&norm, log)?;
    let mut w = Writer::new();
    // From the end: the symbols at even indices are the first state's, odd the second's; the
    // first state is flushed last, so the decoder reads it first.
    let n = weights.len();
    let at = |i: usize| -> Result<u8, Corrupt> { weights.get(i).copied().ok_or(Corrupt::Huffman) };
    let (mut s1, mut s2);
    let mut i;
    // `n` is at least 2 (checked above); the indices below stay within it.
    let back = |k: usize| -> Result<u8, Corrupt> { at(n.checked_sub(k).ok_or(Corrupt::Huffman)?) };
    if n % 2 == 1 {
        s1 = EncodeState::init(&table, back(1)?)?;
        s2 = EncodeState::init(&table, back(2)?)?;
        s1.encode(&table, back(3)?, &mut w)?;
        i = n.saturating_sub(3);
    } else {
        s2 = EncodeState::init(&table, back(1)?)?;
        s1 = EncodeState::init(&table, back(2)?)?;
        i = n.saturating_sub(2);
    }
    while i > 0 {
        s2.encode(&table, at(i.saturating_sub(1))?, &mut w)?;
        s1.encode(&table, at(i.saturating_sub(2))?, &mut w)?;
        i = i.saturating_sub(2);
    }
    s2.flush(&table, &mut w);
    s1.flush(&table, &mut w);
    let mut out = vec![0u8];
    out.extend_from_slice(&write_distribution(&norm, log)?);
    out.extend_from_slice(&w.close());
    let len = out.len().saturating_sub(1);
    *out.first_mut().ok_or(Corrupt::Huffman)? = u8::try_from(len)
        .ok()
        .filter(|&l| l < DIRECT)
        .ok_or(Corrupt::Huffman)?;
    Ok(out)
}
