//! FSE (RFC 8878 §4.1): reading a table description (§4.1.1), building the decoding table from
//! the normalized distribution, and the predefined distributions of the sequence codes
//! (§3.1.1.3.2.2).

use super::Corrupt;
use super::bits::{Backward, Forward, Writer};

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

/// The accuracy log for `total` symbols drawn from `max_symbol + 1` values, at most `max_log`:
/// large enough to give every present symbol a cell and resolve the distribution, small enough
/// that the description does not outweigh what it saves; the reference's choice
/// (`FSE_optimalTableLog`: the source's bit length less 2, raised to the symbols' bit length
/// plus 2, within 5 and `max_log`).
pub(super) fn table_log(total: usize, max_symbol: usize, max_log: u32) -> u32 {
    let bits = |x: usize| usize::BITS.saturating_sub(x.leading_zeros());
    let from_source = bits(total.saturating_sub(1)).saturating_sub(2);
    let floor = bits(total)
        .saturating_add(1)
        .min(bits(max_symbol).saturating_add(2));
    from_source
        .min(max_log)
        .max(floor)
        .clamp(MIN_ACCURACY_LOG, max_log)
}

/// Scales `counts` (each symbol's occurrences, `total` in all) to a distribution summing to
/// `1 << log` in which every present symbol keeps at least one cell: each its rounded share,
/// the rounding's error then taken from or given to the largest. Symbols too rare for a full
/// cell keep one (the format's "less than 1", -1, is an encoding choice this does not make).
pub(super) fn normalize(counts: &[u32], total: u32, log: u32) -> Result<Vec<i16>, Corrupt> {
    let size = 1i64.checked_shl(log).ok_or(Corrupt::Distribution)?;
    let total = i64::from(total);
    if total == 0 {
        return Err(Corrupt::Distribution);
    }
    let mut norm = Vec::with_capacity(counts.len());
    let mut sum = 0i64;
    let mut largest = 0usize;
    for (s, &c) in counts.iter().enumerate() {
        let c = i64::from(c);
        let share = if c == 0 {
            0
        } else {
            // Rounded, never below one cell.
            (c.saturating_mul(size).saturating_add(total / 2))
                .checked_div(total)
                .unwrap_or(0)
                .max(1)
        };
        if c > i64::from(*counts.get(largest).unwrap_or(&0)) {
            largest = s;
        }
        sum = sum.saturating_add(share);
        norm.push(share);
    }
    let fix = size.saturating_sub(sum);
    let slot = norm.get_mut(largest).ok_or(Corrupt::Distribution)?;
    *slot = slot.saturating_add(fix);
    if *slot < 1 {
        // The present symbols need more cells than the table has: the caller's log is too small.
        return Err(Corrupt::Distribution);
    }
    norm.into_iter()
        .map(|v| i16::try_from(v).map_err(|_| Corrupt::Distribution))
        .collect()
}

/// Writes a distribution's table description (§4.1.1), the inverse of [`read_distribution`]:
/// the accuracy log less 5 in 4 bits, then each symbol's probability plus one in the reader's
/// variable width, zero runs after a zero as 2-bit repeat flags.
pub(super) fn write_distribution(norm: &[i16], log: u32) -> Result<Vec<u8>, Corrupt> {
    let mut w = Writer::new();
    w.add(
        u64::from(
            log.checked_sub(MIN_ACCURACY_LOG)
                .ok_or(Corrupt::Distribution)?,
        ),
        4,
    );
    let size = 1i32.checked_shl(log).ok_or(Corrupt::Distribution)?;
    let mut remaining = size.checked_add(1).ok_or(Corrupt::Distribution)?;
    let mut threshold = size;
    let mut width = log.checked_add(1).ok_or(Corrupt::Distribution)?;
    let mut s = 0usize;
    let mut previous_zero = false;
    while remaining > 1 && s < norm.len() {
        if previous_zero {
            let start = s;
            while norm.get(s) == Some(&0) {
                s = s.saturating_add(1);
            }
            let mut run = s.saturating_sub(start);
            while run >= 3 {
                w.add(3, 2);
                run = run.saturating_sub(3);
            }
            w.add(u64::try_from(run).unwrap_or(0), 2);
        }
        let count = i32::from(*norm.get(s).ok_or(Corrupt::Distribution)?);
        s = s.saturating_add(1);
        let max = threshold
            .checked_mul(2)
            .and_then(|t| t.checked_sub(1))
            .and_then(|t| t.checked_sub(remaining))
            .ok_or(Corrupt::Distribution)?;
        remaining = remaining
            .checked_sub(count.abs())
            .ok_or(Corrupt::Distribution)?;
        // The value read is the probability plus one; values at or above the threshold are the
        // ones Table 20 moves up by `max`.
        let mut value = count.checked_add(1).ok_or(Corrupt::Distribution)?;
        if value >= threshold {
            value = value.checked_add(max).ok_or(Corrupt::Distribution)?;
        }
        let bits = if value < max {
            width.saturating_sub(1)
        } else {
            width
        };
        w.add(
            u64::try_from(value).map_err(|_| Corrupt::Distribution)?,
            bits,
        );
        previous_zero = count == 0;
        if remaining < 1 {
            return Err(Corrupt::Distribution);
        }
        while remaining < threshold {
            width = width.checked_sub(1).ok_or(Corrupt::Distribution)?;
            threshold >>= 1;
        }
    }
    if remaining != 1 {
        return Err(Corrupt::Distribution);
    }
    Ok(w.finish())
}

/// How one symbol is encoded from any state: the reference's `symbolTT`, the bits a state of it
/// emits found from `delta_bits`, the next state from `delta_state` (`FSE_buildCTable`).
#[derive(Clone, Copy, Debug, Default)]
struct SymbolTransform {
    delta_bits: u32,
    delta_state: i32,
}

/// An encoding table, the decoder's [`Table`] run backwards.
#[derive(Clone, Debug)]
pub(super) struct EncodeTable {
    pub(super) accuracy_log: u32,
    /// The next state, indexed by a symbol's range of states.
    states: Vec<u16>,
    symbols: Vec<SymbolTransform>,
}

impl EncodeTable {
    /// The encoding table of `norm` at `log`, its cells spread exactly as the decoder's
    /// (`FSE_buildCTable`).
    pub(super) fn new(norm: &[i16], log: u32) -> Result<Self, Corrupt> {
        let size = 1usize.checked_shl(log).ok_or(Corrupt::Distribution)?;
        let decode = Table::from_distribution(norm, log)?;
        // Each symbol's states, in the order the spread placed them, numbered from the table size.
        let mut cumul = Vec::with_capacity(norm.len().saturating_add(1));
        let mut running = 0usize;
        for &n in norm {
            cumul.push(running);
            running = running.saturating_add(if n == -1 {
                1
            } else {
                usize::try_from(n.max(0)).unwrap_or(0)
            });
        }
        let mut states = vec![0u16; size];
        let mut next = cumul.clone();
        for (u, cell) in decode.cells.iter().enumerate() {
            let at = next
                .get_mut(usize::from(cell.symbol))
                .ok_or(Corrupt::Distribution)?;
            *states.get_mut(*at).ok_or(Corrupt::Distribution)? =
                u16::try_from(size.saturating_add(u)).map_err(|_| Corrupt::Distribution)?;
            *at = at.saturating_add(1);
        }
        let mut symbols = vec![SymbolTransform::default(); norm.len()];
        let mut total = 0i32;
        for (t, &n) in symbols.iter_mut().zip(norm) {
            let size32 = i64::try_from(size).unwrap_or(0);
            match n {
                0 => {
                    // Never encoded; the reference's value keeps its maths defined.
                    t.delta_bits = u32::try_from(
                        (i64::from(log).saturating_add(1) << 16).saturating_sub(size32),
                    )
                    .unwrap_or(0);
                }
                -1 | 1 => {
                    t.delta_bits =
                        u32::try_from((i64::from(log) << 16).saturating_sub(size32)).unwrap_or(0);
                    t.delta_state = total.saturating_sub(1);
                    total = total.saturating_add(1);
                }
                _ => {
                    let n32 = u32::try_from(n).map_err(|_| Corrupt::Distribution)?;
                    let high = u32::BITS
                        .saturating_sub(1)
                        .saturating_sub((n32.saturating_sub(1)).leading_zeros());
                    let max_bits_out = log.saturating_sub(high);
                    let min_state_plus = i64::from(n32) << max_bits_out;
                    t.delta_bits = u32::try_from(
                        (i64::from(max_bits_out) << 16).saturating_sub(min_state_plus),
                    )
                    .unwrap_or(0);
                    t.delta_state = total.saturating_sub(i32::from(n));
                    total = total.saturating_add(i32::from(n));
                }
            }
        }
        Ok(Self {
            accuracy_log: log,
            states,
            symbols,
        })
    }

    fn transform(&self, symbol: u8) -> Result<SymbolTransform, Corrupt> {
        self.symbols
            .get(usize::from(symbol))
            .copied()
            .ok_or(Corrupt::Distribution)
    }

    fn state_at(&self, index: i64) -> Result<u32, Corrupt> {
        let i = usize::try_from(index).map_err(|_| Corrupt::Distribution)?;
        Ok(u32::from(*self.states.get(i).ok_or(Corrupt::Distribution)?))
    }
}

/// An encoder's state over one table (`FSE_CState_t`): its value in `[size, 2·size)`.
#[derive(Clone, Copy, Debug)]
pub(super) struct EncodeState {
    value: u32,
}

impl EncodeState {
    /// The state after the stream's last symbol, written first (`FSE_initCState2`).
    pub(super) fn init(table: &EncodeTable, symbol: u8) -> Result<Self, Corrupt> {
        let t = table.transform(symbol)?;
        let bits_out = t.delta_bits.saturating_add(1 << 15) >> 16;
        let value = (i64::from(bits_out) << 16).saturating_sub(i64::from(t.delta_bits));
        let index = (value >> bits_out).saturating_add(i64::from(t.delta_state));
        Ok(Self {
            value: table.state_at(index)?,
        })
    }

    /// Encodes `symbol`, writing the bits the decoder reads to return here (`FSE_encodeSymbol`).
    pub(super) fn encode(
        &mut self,
        table: &EncodeTable,
        symbol: u8,
        w: &mut Writer,
    ) -> Result<(), Corrupt> {
        let t = table.transform(symbol)?;
        let bits_out = self.value.saturating_add(t.delta_bits) >> 16;
        w.add(u64::from(self.value), bits_out);
        let index = i64::from(self.value >> bits_out).saturating_add(i64::from(t.delta_state));
        self.value = table.state_at(index)?;
        Ok(())
    }

    /// Writes the final state, which the decoder reads first (`FSE_flushCState`).
    pub(super) fn flush(self, table: &EncodeTable, w: &mut Writer) {
        w.add(u64::from(self.value), table.accuracy_log);
    }
}
