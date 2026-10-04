//! FSE (RFC 8878 §4.1): reading a table description (§4.1.1), building the decoding table from
//! the normalized distribution, and the predefined distributions of the sequence codes
//! (§3.1.1.3.2.2).

use super::Corrupt;
use super::bits::{Backward, Forward};

/// The smallest accuracy log a description states: its low 4 bits plus 5 (§4.1.1).
const MIN_ACCURACY_LOG: u32 = 5;

/// One cell of a decoding table: the symbol it decodes and how to reach the next state, the
/// next state being `baseline + read(bits)` (§4.1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Cell {
    pub(super) symbol: u8,
    pub(super) bits: u8,
    pub(super) baseline: u16,
}

/// A decoding table of `1 << accuracy_log` cells.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Table {
    pub(super) accuracy_log: u32,
    pub(super) cells: Vec<Cell>,
}

impl Table {
    /// The one-symbol table of RLE_Mode (§3.1.1.3.2.1): every state decodes `symbol` and stays.
    pub(super) fn rle(symbol: u8) -> Self {
        Self {
            accuracy_log: 0,
            cells: vec![Cell {
                symbol,
                bits: 0,
                baseline: 0,
            }],
        }
    }

    /// The decoding table of a normalized distribution (§4.1.1): "less than 1" symbols take one
    /// cell each from the end, the rest are spread by the fixed step, and each symbol's states,
    /// in natural order, get their bits and baselines.
    pub(super) fn from_distribution(norm: &[i16], accuracy_log: u32) -> Result<Self, Corrupt> {
        let size = 1usize
            .checked_shl(accuracy_log)
            .ok_or(Corrupt::Distribution)?;
        let mut cells = vec![Cell::default(); size];
        // The next state value each symbol hands out, starting at its count.
        let mut next = vec![0u32; norm.len()];
        let mut high = size.checked_sub(1).ok_or(Corrupt::Distribution)?;
        for (s, (&count, n)) in norm.iter().zip(next.iter_mut()).enumerate() {
            let symbol = u8::try_from(s).map_err(|_| Corrupt::Distribution)?;
            if count == -1 {
                let cell = cells.get_mut(high).ok_or(Corrupt::Distribution)?;
                cell.symbol = symbol;
                high = high.checked_sub(1).ok_or(Corrupt::Distribution)?;
                *n = 1;
            } else {
                *n = u32::try_from(count).map_err(|_| Corrupt::Distribution)?;
            }
        }
        // §4.1.1: the spread's step, `(tableSize >> 1) + (tableSize >> 3) + 3`; `size` is at most
        // 2^9, so the sum is far from overflowing.
        let step = (size >> 1).wrapping_add(size >> 3).wrapping_add(3);
        let mask = size.wrapping_sub(1);
        let mut position = 0usize;
        for (s, &count) in norm.iter().enumerate() {
            let symbol = u8::try_from(s).map_err(|_| Corrupt::Distribution)?;
            for _ in 0..count.max(0) {
                let cell = cells.get_mut(position).ok_or(Corrupt::Distribution)?;
                cell.symbol = symbol;
                position = position.wrapping_add(step) & mask;
                // Skip the cells the "less than 1" symbols hold.
                while position > high {
                    position = position.wrapping_add(step) & mask;
                }
            }
        }
        // Every cell is reached once by the spread only if the counts sum to the table: §4.1.1's
        // reader checks that, so a position not back at 0 means a description it did not check.
        if position != 0 {
            return Err(Corrupt::Distribution);
        }
        for cell in &mut cells {
            let n = next
                .get_mut(usize::from(cell.symbol))
                .ok_or(Corrupt::Distribution)?;
            let state = *n;
            *n = state.checked_add(1).ok_or(Corrupt::Distribution)?;
            // Bits to reach the next power of two from this state, so lower states read one more.
            let bits = accuracy_log
                .checked_sub(
                    31u32
                        .checked_sub(state.leading_zeros())
                        .ok_or(Corrupt::Distribution)?,
                )
                .ok_or(Corrupt::Distribution)?;
            let baseline = (state << bits)
                .checked_sub(u32::try_from(size).map_err(|_| Corrupt::Distribution)?)
                .ok_or(Corrupt::Distribution)?;
            cell.bits = u8::try_from(bits).map_err(|_| Corrupt::Distribution)?;
            cell.baseline = u16::try_from(baseline).map_err(|_| Corrupt::Distribution)?;
        }
        Ok(Self {
            accuracy_log,
            cells,
        })
    }

    /// The cell of `state`.
    pub(super) fn cell(&self, state: u32) -> Result<Cell, Corrupt> {
        self.cells
            .get(usize::try_from(state).map_err(|_| Corrupt::Bitstream)?)
            .copied()
            .ok_or(Corrupt::Bitstream)
    }
}

/// A decoder's state over one table: its first value read, and each next one from the cell's
/// baseline and bits (§4.1).
#[derive(Clone, Copy, Debug)]
pub(super) struct State {
    pub(super) value: u32,
}

impl State {
    pub(super) fn init(table: &Table, bits: &mut Backward<'_>) -> Self {
        Self {
            value: bits.read(table.accuracy_log),
        }
    }

    pub(super) fn symbol(self, table: &Table) -> Result<u8, Corrupt> {
        Ok(table.cell(self.value)?.symbol)
    }

    pub(super) fn update(&mut self, table: &Table, bits: &mut Backward<'_>) -> Result<(), Corrupt> {
        let cell = table.cell(self.value)?;
        self.value = u32::from(cell.baseline)
            .checked_add(bits.read(u32::from(cell.bits)))
            .ok_or(Corrupt::Bitstream)?;
        Ok(())
    }
}

/// Reads a table description (§4.1.1) of at most `max_symbols` symbols and accuracy log
/// `max_log`, returning the normalized distribution, its accuracy log and the bytes it took.
pub(super) fn read_distribution(
    bytes: &[u8],
    max_symbols: usize,
    max_log: u32,
) -> Result<(Vec<i16>, u32, usize), Corrupt> {
    let mut bits = Forward::new(bytes);
    let accuracy_log = bits
        .peek(4)
        .checked_add(MIN_ACCURACY_LOG)
        .ok_or(Corrupt::Distribution)?;
    bits.skip(4)?;
    if accuracy_log > max_log {
        return Err(Corrupt::Distribution);
    }
    let size = 1i32 << accuracy_log;
    // §4.1.1: values range to "remaining probabilities + 1".
    let mut remaining = size.checked_add(1).ok_or(Corrupt::Distribution)?;
    let mut threshold = size;
    let mut width = accuracy_log.checked_add(1).ok_or(Corrupt::Distribution)?;
    let mut norm: Vec<i16> = Vec::with_capacity(max_symbols);
    while remaining > 1 {
        if norm.len() >= max_symbols {
            return Err(Corrupt::Distribution);
        }
        // Each 2-bit flag after a zero repeats that many more zeros; 3 continues.
        if norm.last() == Some(&0) {
            loop {
                let repeat = bits.peek(2);
                bits.skip(2)?;
                for _ in 0..repeat {
                    if norm.len() >= max_symbols {
                        return Err(Corrupt::Distribution);
                    }
                    norm.push(0);
                }
                if repeat != 3 {
                    break;
                }
            }
            if norm.len() >= max_symbols {
                return Err(Corrupt::Distribution);
            }
        }
        // Table 20: the low values take one bit less.
        let max = threshold
            .checked_mul(2)
            .and_then(|t| t.checked_sub(1))
            .and_then(|t| t.checked_sub(remaining))
            .ok_or(Corrupt::Distribution)?;
        let narrow = width.checked_sub(1).ok_or(Corrupt::Distribution)?;
        let low = i32::try_from(bits.peek(narrow)).map_err(|_| Corrupt::Distribution)?;
        let value = if low < max {
            bits.skip(narrow)?;
            low
        } else {
            let full = i32::try_from(bits.peek(width)).map_err(|_| Corrupt::Distribution)?;
            bits.skip(width)?;
            if full >= threshold {
                full.checked_sub(max).ok_or(Corrupt::Distribution)?
            } else {
                full
            }
        };
        let probability = value.checked_sub(1).ok_or(Corrupt::Distribution)?;
        // "Less than 1" counts as one point.
        remaining = remaining
            .checked_sub(probability.abs())
            .ok_or(Corrupt::Distribution)?;
        norm.push(i16::try_from(probability).map_err(|_| Corrupt::Distribution)?);
        while remaining < threshold {
            width = width.checked_sub(1).ok_or(Corrupt::Distribution)?;
            threshold >>= 1;
        }
    }
    if remaining != 1 {
        return Err(Corrupt::Distribution);
    }
    // At least two symbols carry probability (§4.1.1).
    if norm.iter().filter(|&&p| p != 0).count() < 2 {
        return Err(Corrupt::Distribution);
    }
    Ok((norm, accuracy_log, bits.bytes_used()))
}

/// The predefined distribution of literals length codes, accuracy log 6 (§3.1.1.3.2.2.1).
pub(super) const LITERALS_LENGTH_DEFAULT: [i16; 36] = [
    4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, 1,
    -1, -1, -1, -1,
];
pub(super) const LITERALS_LENGTH_DEFAULT_LOG: u32 = 6;
/// The predefined distribution of match length codes, accuracy log 6 (§3.1.1.3.2.2.2).
pub(super) const MATCH_LENGTH_DEFAULT: [i16; 53] = [
    1, 4, 3, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1, -1, -1,
];
pub(super) const MATCH_LENGTH_DEFAULT_LOG: u32 = 6;
/// The predefined distribution of offset codes, accuracy log 5 (§3.1.1.3.2.2.3).
pub(super) const OFFSET_DEFAULT: [i16; 29] = [
    1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1,
];
pub(super) const OFFSET_DEFAULT_LOG: u32 = 5;
