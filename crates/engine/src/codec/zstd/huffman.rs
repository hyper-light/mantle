//! Huffman-coded literals (RFC 8878 §4.2): the tree description (§4.2.1), its weights direct or
//! FSE-compressed (§4.2.1.1, §4.2.1.2), the prefix codes they give (§4.2.1.3), and the streams
//! (§4.2.2), one or four behind a jump table (§3.1.1.3.1.6).

use super::Corrupt;
use super::bits::{Backward, Writer};
use super::fse::{
    EncodeState, EncodeTable, SYMBOLS_MAX, State, Table, normalize, read_distribution, table_log,
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

/// A decoding table indexed by the stream's next `max_bits` bits; empty until a description is
/// read. Reading one reuses the table's allocation, as cloning into one does.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Huffman {
    max_bits: u32,
    entries: Vec<Entry>,
    /// The weights' FSE table, kept for its allocation.
    weights: Table,
}

impl Clone for Huffman {
    fn clone(&self) -> Self {
        Self {
            max_bits: self.max_bits,
            entries: self.entries.clone(),
            weights: Table::default(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.max_bits = source.max_bits;
        self.entries.clone_from(&source.entries);
    }
}

impl Huffman {
    /// Forgets the table, keeping its allocation.
    pub(super) fn unset(&mut self) {
        self.entries.clear();
    }

    /// Whether a description was read: treeless literals need one (§3.1.1.3.1.1).
    pub(super) fn is_set(&self) -> bool {
        !self.entries.is_empty()
    }

    /// Reads a tree description from the front of `bytes` into this table, returning the bytes
    /// it took. A description that fails leaves the table empty.
    pub(super) fn read(&mut self, bytes: &[u8]) -> Result<usize, Corrupt> {
        let read = self.read_description(bytes);
        if read.is_err() {
            self.unset();
        }
        read
    }

    fn read_description(&mut self, bytes: &[u8]) -> Result<usize, Corrupt> {
        let header = *bytes.first().ok_or(Corrupt::Huffman)?;
        let mut weights = [0u8; MAX_WEIGHTS];
        let (count, used) = if header >= DIRECT {
            direct_weights(bytes, header, &mut weights)?
        } else {
            fse_weights(bytes, header, &mut self.weights, &mut weights)?
        };
        self.set_weights(weights.get(..count).ok_or(Corrupt::Huffman)?)?;
        Ok(used)
    }

    /// Makes this the table of a series of weights, the last literal's weight deduced by
    /// completing to a power of two (§4.2.1).
    fn set_weights(&mut self, weights: &[u8]) -> Result<(), Corrupt> {
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
        if weights.len() >= 256 {
            return Err(Corrupt::Huffman);
        }
        // Codes are handed out by weight, lowest first, and by literal within a weight
        // (§4.2.1.3): each weight's run of the table starts where the lower weights' end.
        let mut counts = [0usize; MAX_BITS as usize + 2];
        for &w in weights.iter().chain(std::iter::once(&last)) {
            let c = counts.get_mut(usize::from(w)).ok_or(Corrupt::Huffman)?;
            *c = c.checked_add(1).ok_or(Corrupt::Huffman)?;
        }
        let mut start = [0usize; MAX_BITS as usize + 2];
        let mut next = 0usize;
        for w in 1..=usize::try_from(max_bits).map_err(|_| Corrupt::Huffman)? {
            let count = *counts.get(w).ok_or(Corrupt::Huffman)?;
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
        // Every entry is written below (the runs sum to the size), so a table of the same size
        // is not cleared first.
        if self.entries.len() != size {
            self.entries.resize(size, Entry::default());
        }
        let mut symbol = 0u8;
        for &w in weights.iter().chain(std::iter::once(&last)) {
            if w > 0 {
                let w = usize::from(w);
                let span = 1usize << w.saturating_sub(1);
                let at = start.get_mut(w).ok_or(Corrupt::Huffman)?;
                let end = at.checked_add(span).ok_or(Corrupt::Huffman)?;
                // Number_of_Bits = Max_Number_of_Bits + 1 - Weight (§4.2.1); weights are at
                // most max_bits, as the total's power of two bounds each.
                let bits = u8::try_from(
                    max_bits
                        .checked_add(1)
                        .and_then(|b| b.checked_sub(u32::try_from(w).ok()?))
                        .ok_or(Corrupt::Huffman)?,
                )
                .map_err(|_| Corrupt::Huffman)?;
                self.entries
                    .get_mut(*at..end)
                    .ok_or(Corrupt::Huffman)?
                    .fill(Entry { symbol, bits });
                *at = end;
            }
            symbol = symbol.wrapping_add(1);
        }
        self.max_bits = max_bits;
        Ok(())
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

/// Weights written directly, 4 bits each, two to a byte, high nibble first (§4.2.1.1), into
/// `out`; returns how many and the bytes taken.
fn direct_weights(
    bytes: &[u8],
    header: u8,
    out: &mut [u8; MAX_WEIGHTS],
) -> Result<(usize, usize), Corrupt> {
    // Number_of_Symbols = headerByte - 127 (§4.2.1.1); `header` is at least `DIRECT`, so at most
    // 128 symbols.
    let count = usize::from(header.saturating_sub(DIRECT - 1));
    let len = count.div_ceil(2);
    let end = len.checked_add(1).ok_or(Corrupt::Huffman)?;
    let packed = bytes.get(1..end).ok_or(Corrupt::Huffman)?;
    for (w, slot) in packed
        .iter()
        .flat_map(|b| [b >> 4, b & 0xF])
        .take(count)
        .zip(out.iter_mut())
    {
        *slot = w;
    }
    Ok((count, end))
}

/// Weights compressed by FSE, two interleaved states over one table, until a state's update
/// reads past the stream's start (§4.2.1.2), into `out` with `table` rebuilt for them; returns
/// how many and the bytes taken.
fn fse_weights(
    bytes: &[u8],
    header: u8,
    table: &mut Table,
    out: &mut [u8; MAX_WEIGHTS],
) -> Result<(usize, usize), Corrupt> {
    // The FSE-compressed series is `headerByte` bytes long (§4.2.1.1).
    let end = usize::from(header).checked_add(1).ok_or(Corrupt::Huffman)?;
    let body = bytes.get(1..end).ok_or(Corrupt::Huffman)?;
    let mut norm = [0i16; SYMBOLS_MAX];
    let (symbols, log, used) = read_distribution(body, WEIGHT_SYMBOLS, WEIGHTS_MAX_LOG, &mut norm)?;
    table.set_distribution(norm.get(..symbols).ok_or(Corrupt::Huffman)?, log)?;
    let stream = body.get(used..).ok_or(Corrupt::Huffman)?;
    let mut bits = Backward::new(stream)?;
    let mut states = [State::init(table, &mut bits), State::init(table, &mut bits)];
    let mut count = 0usize;
    let mut push = |count: &mut usize, w: u8| -> Result<(), Corrupt> {
        *out.get_mut(*count).ok_or(Corrupt::Huffman)? = w;
        *count = count.checked_add(1).ok_or(Corrupt::Huffman)?;
        Ok(())
    };
    'decode: loop {
        for i in 0..2 {
            // One weight short of the most, so the other state's last symbol still fits.
            if count >= MAX_WEIGHTS.saturating_sub(1) {
                return Err(Corrupt::Huffman);
            }
            let state = states.get_mut(i).ok_or(Corrupt::Huffman)?;
            push(&mut count, state.symbol(table))?;
            state.update(table, &mut bits);
            if bits.overflowed() {
                // The other state's symbol is the last.
                let other = states.get(i ^ 1).ok_or(Corrupt::Huffman)?;
                push(&mut count, other.symbol(table))?;
                break 'decode;
            }
        }
    }
    Ok((count, end))
}

/// The literals a Huffman table codes: every byte value (§4.2.1).
pub(super) const LITERALS: usize = 256;
/// Items a package-merge row holds at most: every leaf and a package of each pair before.
const ROW_MAX: usize = 2 * LITERALS;

/// Optimal code lengths of at most `limit` bits (at most [`MAX_BITS`]) for `counts` (one per
/// literal, 0 for absent), into `lengths`: the package-merge algorithm (Larmore and Hirschberg,
/// "A fast algorithm for optimal length-limited Huffman codes", JACM 37(3), 1990). Absent
/// literals get 0; a single present literal gets 1.
///
/// No row's items are kept as lists of the leaves they package. Each row keeps only its merged
/// order, an item a leaf's literal or a package; the items chosen in a row are always a prefix of
/// it, its packages choosing twice as many of the row before. So each leaf's length is the
/// number of rows whose chosen prefix holds it, counted from the last row back.
pub(super) fn code_lengths(counts: &[u32], limit: u32, lengths: &mut [u8; LITERALS]) {
    lengths.fill(0);
    // The present literals by count, then by literal, as the rows merge them.
    let mut leaves = [(0u64, 0u16); LITERALS];
    let mut n = 0usize;
    for (s, &c) in counts.iter().enumerate().take(LITERALS) {
        if c > 0
            && let Some(leaf) = leaves.get_mut(n)
        {
            *leaf = (u64::from(c), u16::try_from(s).unwrap_or(0));
            n = n.saturating_add(1);
        }
    }
    let Some(leaves) = leaves.get_mut(..n) else {
        return;
    };
    if let [(_, only)] = leaves {
        if let Some(l) = lengths.get_mut(usize::from(*only)) {
            *l = 1;
        }
        return;
    }
    leaves.sort_unstable();
    let leaves = &*leaves;
    /// A row item that is a package, not a leaf.
    const PACKAGE: u16 = u16::MAX;
    let rows = usize::try_from(limit.clamp(1, MAX_BITS)).unwrap_or(1);
    let mut order = [[0u16; ROW_MAX]; MAX_BITS as usize];
    let mut sizes = [0usize; MAX_BITS as usize];
    // Each row's weights, in two buffers that take turns as the row and the one before.
    let mut weights = [[0u64; ROW_MAX]; 2];
    // The first row: the leaves.
    if let (Some(first), Some(w0)) = (order.first_mut(), weights.first_mut()) {
        for ((o, slot), &(w, s)) in first.iter_mut().zip(w0.iter_mut()).zip(leaves) {
            *o = s;
            *slot = w;
        }
    }
    if let Some(first) = sizes.first_mut() {
        *first = n;
    }
    for r in 1..rows {
        let previous = sizes.get(r.wrapping_sub(1)).copied().unwrap_or(0);
        let [even, odd] = &mut weights;
        let (before, current) = if r % 2 == 1 {
            (&*even, odd)
        } else {
            (&*odd, even)
        };
        let Some(row) = order.get_mut(r) else {
            break;
        };
        // Package pairs of the previous row, then merge them with the leaves by weight, a leaf
        // before a package of equal weight.
        let packages = previous >> 1;
        let (mut i, mut j, mut k) = (0usize, 0usize, 0usize);
        loop {
            let package = (j < packages)
                .then(|| {
                    let at = j.wrapping_mul(2);
                    before
                        .get(at)
                        .zip(before.get(at.wrapping_add(1)))
                        .map(|(a, b)| a.saturating_add(*b))
                })
                .flatten();
            let (w, item) = match (leaves.get(i), package) {
                (Some(&(lw, _)), Some(pw)) if lw > pw => (pw, PACKAGE),
                (Some(&(lw, ls)), _) => (lw, ls),
                (None, Some(pw)) => (pw, PACKAGE),
                (None, None) => break,
            };
            if item == PACKAGE {
                j = j.wrapping_add(1);
            } else {
                i = i.wrapping_add(1);
            }
            if let (Some(o), Some(slot)) = (row.get_mut(k), current.get_mut(k)) {
                *o = item;
                *slot = w;
            }
            k = k.wrapping_add(1);
        }
        if let Some(size) = sizes.get_mut(r) {
            *size = k;
        }
    }
    // The first 2n − 2 items of the last row are chosen; back through the rows, each leaf chosen
    // gains a bit and each package chosen chooses two items of the row before.
    let mut chosen = n.saturating_mul(2).saturating_sub(2);
    for r in (0..rows).rev() {
        let row = order.get(r).map_or(&[][..], <[u16; ROW_MAX]>::as_slice);
        let mut packages = 0usize;
        for &item in row.iter().take(chosen) {
            if item == PACKAGE {
                packages = packages.saturating_add(1);
            } else if let Some(l) = lengths.get_mut(usize::from(item)) {
                *l = l.saturating_add(1);
            }
        }
        chosen = packages.saturating_mul(2);
    }
}

/// An encoding table: each literal's code and its length.
#[derive(Clone, Debug)]
pub(super) struct HuffmanEncoder {
    codes: [(u32, u8); LITERALS],
    /// The weights written in the tree description, the last present literal's left out: the
    /// first `weights_len`.
    weights: [u8; LITERALS],
    weights_len: usize,
}

impl HuffmanEncoder {
    /// The table of `lengths` (each at most [`MAX_BITS`], at least two present): weights from the
    /// lengths (§4.2.1), codes handed out as the decoder's table lays them (§4.2.1.3).
    pub(super) fn new(lengths: &[u8; LITERALS]) -> Result<Self, Corrupt> {
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
        let mut weights = [0u8; LITERALS];
        let mut per_weight = [0u32; MAX_BITS as usize + 2];
        for (i, &l) in lengths.iter().enumerate() {
            let w = weight(l);
            if i < last
                && let Some(slot) = weights.get_mut(i)
            {
                *slot = w;
            }
            if l > 0
                && let Some(c) = per_weight.get_mut(usize::from(w))
            {
                *c = c.saturating_add(1);
            }
        }
        // The decoder's table: weight w's literals in order, each 2^(w−1) entries, the lowest
        // weight's first; a code is its first entry's index shifted down by the bits it skips.
        let mut start = [0u32; MAX_BITS as usize + 2];
        let mut next = 0u32;
        for w in 1..=max_bits {
            let wi = usize::try_from(w).unwrap_or(0);
            let count = per_weight.get(wi).copied().unwrap_or(0);
            if let Some(s) = start.get_mut(wi) {
                *s = next;
            }
            next = next.saturating_add(count << w.saturating_sub(1));
        }
        if next != 1 << max_bits {
            return Err(Corrupt::Huffman);
        }
        let mut codes = [(0u32, 0u8); LITERALS];
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
        Ok(Self {
            codes,
            weights,
            weights_len: last,
        })
    }

    /// The tree description (§4.2.1.1) onto `out`: the weights FSE-compressed when that is
    /// shorter, written directly otherwise (possible for at most 128 weights).
    pub(super) fn description(&self, out: &mut Vec<u8>) -> Result<(), Corrupt> {
        let weights = self
            .weights
            .get(..self.weights_len)
            .ok_or(Corrupt::Huffman)?;
        let start = out.len();
        let direct_len = if weights.len() <= 128 {
            Some(weights.len().div_ceil(2).saturating_add(1))
        } else {
            None
        };
        if compress_weights(weights, out).is_ok()
            && direct_len.is_none_or(|d| out.len().saturating_sub(start) < d)
        {
            return Ok(());
        }
        out.truncate(start);
        if direct_len.is_none() {
            return Err(Corrupt::Huffman);
        }
        out.push(u8::try_from(weights.len().saturating_add(127)).map_err(|_| Corrupt::Huffman)?);
        for pair in weights.chunks(2) {
            let hi = pair.first().copied().unwrap_or(0);
            let lo = pair.get(1).copied().unwrap_or(0);
            out.push((hi << 4) | lo);
        }
        Ok(())
    }

    /// One stream of `literals` onto `out`, written last literal first so the decoder, reading
    /// backward, meets them in order (§4.2.2).
    pub(super) fn stream(&self, literals: &[u8], out: &mut Vec<u8>) -> Result<(), Corrupt> {
        let mut w = Writer::new(out);
        for &b in literals.iter().rev() {
            let &(code, len) = self.codes.get(usize::from(b)).ok_or(Corrupt::Huffman)?;
            if len == 0 {
                return Err(Corrupt::Huffman);
            }
            w.add(u64::from(code), u32::from(len));
        }
        w.close();
        Ok(())
    }

    /// The streams of `literals` onto `out`: one, or four behind their jump table
    /// (§3.1.1.3.1.6).
    pub(super) fn streams(
        &self,
        literals: &[u8],
        four: bool,
        out: &mut Vec<u8>,
    ) -> Result<(), Corrupt> {
        if !four {
            return self.stream(literals, out);
        }
        let segment = literals.len().div_ceil(4);
        let jump = out.len();
        out.extend_from_slice(&[0; JUMP_TABLE]);
        for i in 0..4usize {
            let from = segment.saturating_mul(i).min(literals.len());
            let to = if i == 3 {
                literals.len()
            } else {
                segment
                    .saturating_mul(i.saturating_add(1))
                    .min(literals.len())
            };
            let begin = out.len();
            self.stream(literals.get(from..to).ok_or(Corrupt::Huffman)?, out)?;
            if i < 3 {
                let size =
                    u16::try_from(out.len().saturating_sub(begin)).map_err(|_| Corrupt::Huffman)?;
                let at = jump.saturating_add(i.saturating_mul(2));
                out.get_mut(at..at.saturating_add(2))
                    .ok_or(Corrupt::Huffman)?
                    .copy_from_slice(&size.to_le_bytes());
            }
        }
        Ok(())
    }
}

/// The weights compressed by FSE with two interleaved states (§4.2.1.2) onto `out`, the header
/// byte its length: the inverse of [`fse_weights`] (the reference's
/// `FSE_compress_usingCTable`). On an error `out` may hold a partial description.
fn compress_weights(weights: &[u8], out: &mut Vec<u8>) -> Result<(), Corrupt> {
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
    let mut norm = [0i16; SYMBOLS_MAX];
    let norm = normalize(counts, total, log, &mut norm)?;
    let table = EncodeTable::new(norm, log)?;
    let header = out.len();
    out.push(0);
    write_distribution(norm, log, out)?;
    let mut w = Writer::new(out);
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
    w.close();
    let len = out.len().saturating_sub(header).saturating_sub(1);
    *out.get_mut(header).ok_or(Corrupt::Huffman)? = u8::try_from(len)
        .ok()
        .filter(|&l| l < DIRECT)
        .ok_or(Corrupt::Huffman)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Package-merge kept as the algorithm states it, each item the list of the leaves it
    /// packages: the oracle the array form must equal.
    fn by_lists(counts: &[u32], limit: u32) -> Vec<u8> {
        let mut lengths = vec![0u8; counts.len()];
        let mut present: Vec<(u64, usize)> = counts
            .iter()
            .enumerate()
            .filter(|&(_, &c)| c > 0)
            .map(|(s, &c)| (u64::from(c), s))
            .collect();
        if present.len() == 1 {
            lengths[present[0].1] = 1;
            return lengths;
        }
        present.sort_unstable();
        let leaves: Vec<(u64, Vec<usize>)> = present.iter().map(|&(w, s)| (w, vec![s])).collect();
        let mut row = leaves.clone();
        for _ in 1..limit {
            let packages: Vec<(u64, Vec<usize>)> = row
                .as_chunks::<2>()
                .0
                .iter()
                .map(|[a, b]| (a.0 + b.0, [a.1.clone(), b.1.clone()].concat()))
                .collect();
            let mut merged = Vec::new();
            let (mut i, mut j) = (0, 0);
            while i < leaves.len() || j < packages.len() {
                if j == packages.len() || (i < leaves.len() && leaves[i].0 <= packages[j].0) {
                    merged.push(leaves[i].clone());
                    i += 1;
                } else {
                    merged.push(packages[j].clone());
                    j += 1;
                }
            }
            row = merged;
        }
        for (_, symbols) in row.iter().take(2 * present.len() - 2) {
            for &s in symbols {
                lengths[s] += 1;
            }
        }
        lengths
    }

    proptest! {
        #[test]
        fn the_array_form_equals_the_lists(
            counts in proptest::collection::vec(prop_oneof![Just(0u32), 1u32..4, 1u32..100_000], 1..=256),
            limit in 1u32..=MAX_BITS,
        ) {
            let present = counts.iter().filter(|&&c| c > 0).count();
            // A code of `limit` bits holds at most 2^limit literals.
            prop_assume!(present > 0 && present <= 1 << limit);
            let mut lengths = [0u8; LITERALS];
            code_lengths(&counts, limit, &mut lengths);
            let oracle = by_lists(&counts, limit);
            prop_assert_eq!(&lengths[..counts.len()], &oracle[..]);
            prop_assert!(lengths[counts.len()..].iter().all(|&l| l == 0));
        }
    }
}
