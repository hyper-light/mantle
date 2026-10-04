//! The sequences section (RFC 8878 §3.1.1.3.2): its header, each symbol type's table by mode,
//! the codes' baselines and extra bits (§3.1.1.3.2.1.1), and decoding the interleaved bitstream
//! (§3.1.1.3.2.1.2).

use super::Corrupt;
use super::bits::Backward;
use super::fse::{
    LITERALS_LENGTH_DEFAULT, LITERALS_LENGTH_DEFAULT_LOG, MATCH_LENGTH_DEFAULT,
    MATCH_LENGTH_DEFAULT_LOG, OFFSET_DEFAULT, OFFSET_DEFAULT_LOG, State, Table, read_distribution,
};

/// Literals length codes 0 to 35 (§3.1.1.3.2.1.1, Table 16): baseline and extra bits.
pub(super) const LITERALS_LENGTH_CODES: [(u32, u8); 36] = [
    (0, 0),
    (1, 0),
    (2, 0),
    (3, 0),
    (4, 0),
    (5, 0),
    (6, 0),
    (7, 0),
    (8, 0),
    (9, 0),
    (10, 0),
    (11, 0),
    (12, 0),
    (13, 0),
    (14, 0),
    (15, 0),
    (16, 1),
    (18, 1),
    (20, 1),
    (22, 1),
    (24, 2),
    (28, 2),
    (32, 3),
    (40, 3),
    (48, 4),
    (64, 6),
    (128, 7),
    (256, 8),
    (512, 9),
    (1024, 10),
    (2048, 11),
    (4096, 12),
    (8192, 13),
    (16384, 14),
    (32768, 15),
    (65536, 16),
];
/// Match length codes 0 to 52 (§3.1.1.3.2.1.1, Table 17): baseline and extra bits.
pub(super) const MATCH_LENGTH_CODES: [(u32, u8); 53] = [
    (3, 0),
    (4, 0),
    (5, 0),
    (6, 0),
    (7, 0),
    (8, 0),
    (9, 0),
    (10, 0),
    (11, 0),
    (12, 0),
    (13, 0),
    (14, 0),
    (15, 0),
    (16, 0),
    (17, 0),
    (18, 0),
    (19, 0),
    (20, 0),
    (21, 0),
    (22, 0),
    (23, 0),
    (24, 0),
    (25, 0),
    (26, 0),
    (27, 0),
    (28, 0),
    (29, 0),
    (30, 0),
    (31, 0),
    (32, 0),
    (33, 0),
    (34, 0),
    (35, 1),
    (37, 1),
    (39, 1),
    (41, 1),
    (43, 2),
    (47, 2),
    (51, 3),
    (59, 3),
    (67, 4),
    (83, 4),
    (99, 5),
    (131, 7),
    (259, 8),
    (515, 9),
    (1027, 10),
    (2051, 11),
    (4099, 12),
    (8195, 13),
    (16387, 14),
    (32771, 15),
    (65539, 16),
];
/// The largest offset code the decoder supports, the reference decoder's N (§3.1.1.3.2.1.1).
const MAX_OFFSET_CODE: usize = 31;
/// The largest accuracy logs a sequences table may state (§3.1.1.3.2.1, FSE_Compressed_Mode).
pub(super) const LITERALS_LENGTH_MAX_LOG: u32 = 9;
pub(super) const MATCH_LENGTH_MAX_LOG: u32 = 9;
pub(super) const OFFSET_MAX_LOG: u32 = 8;

/// One sequence: literals to copy, then a match (§3.1.1.4). `offset_value` is the raw value,
/// 1 to 3 naming repeat offsets (§3.1.1.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Sequence {
    pub(super) literals: u32,
    pub(super) offset_value: u32,
    pub(super) match_length: u32,
}

/// The three tables a block's sequences are decoded with, kept for Repeat_Mode.
#[derive(Clone, Debug, Default)]
pub(super) struct Tables {
    pub(super) literals_length: Option<Table>,
    pub(super) offset: Option<Table>,
    pub(super) match_length: Option<Table>,
}

/// Which symbol type a table is for.
#[derive(Clone, Copy)]
enum Kind {
    LiteralsLength,
    Offset,
    MatchLength,
}

impl Kind {
    fn max_symbols(self) -> usize {
        match self {
            Self::LiteralsLength => LITERALS_LENGTH_CODES.len(),
            Self::Offset => MAX_OFFSET_CODE + 1,
            Self::MatchLength => MATCH_LENGTH_CODES.len(),
        }
    }

    fn max_log(self) -> u32 {
        match self {
            Self::LiteralsLength => LITERALS_LENGTH_MAX_LOG,
            Self::Offset => OFFSET_MAX_LOG,
            Self::MatchLength => MATCH_LENGTH_MAX_LOG,
        }
    }

    fn predefined(self) -> Result<Table, Corrupt> {
        match self {
            Self::LiteralsLength => {
                Table::from_distribution(&LITERALS_LENGTH_DEFAULT, LITERALS_LENGTH_DEFAULT_LOG)
            }
            Self::Offset => Table::from_distribution(&OFFSET_DEFAULT, OFFSET_DEFAULT_LOG),
            Self::MatchLength => {
                Table::from_distribution(&MATCH_LENGTH_DEFAULT, MATCH_LENGTH_DEFAULT_LOG)
            }
        }
    }
}

/// The table one mode gives (§3.1.1.3.2.1, Table 15), reading its description from `bytes`;
/// returns the table and the bytes it took.
fn table_for(
    kind: Kind,
    mode: u8,
    bytes: &[u8],
    previous: Option<&Table>,
) -> Result<(Table, usize), Corrupt> {
    match mode {
        0 => Ok((kind.predefined()?, 0)),
        1 => {
            let symbol = *bytes.first().ok_or(Corrupt::Sequences)?;
            if usize::from(symbol) >= kind.max_symbols() {
                return Err(Corrupt::Sequences);
            }
            Ok((Table::rle(symbol), 1))
        }
        2 => {
            let (norm, log, used) = read_distribution(bytes, kind.max_symbols(), kind.max_log())?;
            Ok((Table::from_distribution(&norm, log)?, used))
        }
        _ => Ok((previous.cloned().ok_or(Corrupt::Sequences)?, 0)),
    }
}

/// Reads the sequences section `section` into `out` (cleared first), updating `tables` for the
/// next block. A section with no sequences leaves the tables as they were (§3.1.1.3.2.1).
pub(super) fn decode(
    section: &[u8],
    tables: &mut Tables,
    out: &mut Vec<Sequence>,
) -> Result<(), Corrupt> {
    out.clear();
    let byte0 = *section.first().ok_or(Corrupt::Sequences)?;
    let (count, mut at) = match byte0 {
        0 => {
            return if section.len() == 1 {
                Ok(())
            } else {
                Err(Corrupt::Sequences)
            };
        }
        1..=127 => (usize::from(byte0), 1),
        128..=254 => {
            let byte1 = *section.get(1).ok_or(Corrupt::Sequences)?;
            // ((byte0 - 128) << 8) + byte1 (§3.1.1.3.2.1): at most 0x7EFF.
            ((usize::from(byte0 & 0x7F) << 8) | usize::from(byte1), 2)
        }
        255 => {
            let byte1 = *section.get(1).ok_or(Corrupt::Sequences)?;
            let byte2 = *section.get(2).ok_or(Corrupt::Sequences)?;
            // byte1 + (byte2 << 8) + 0x7F00: at most 0x17EFF.
            (
                (usize::from(byte1) | (usize::from(byte2) << 8)).saturating_add(0x7F00),
                3,
            )
        }
    };
    let modes = *section.get(at).ok_or(Corrupt::Sequences)?;
    at = at.checked_add(1).ok_or(Corrupt::Sequences)?;
    if modes & 0b11 != 0 {
        return Err(Corrupt::Sequences);
    }
    let mut next = |kind: Kind, mode: u8, previous: Option<&Table>| -> Result<Table, Corrupt> {
        let (table, used) = table_for(
            kind,
            mode,
            section.get(at..).ok_or(Corrupt::Sequences)?,
            previous,
        )?;
        at = at.checked_add(used).ok_or(Corrupt::Sequences)?;
        Ok(table)
    };
    let ll = next(
        Kind::LiteralsLength,
        modes >> 6,
        tables.literals_length.as_ref(),
    )?;
    let of = next(Kind::Offset, (modes >> 4) & 0b11, tables.offset.as_ref())?;
    let ml = next(
        Kind::MatchLength,
        (modes >> 2) & 0b11,
        tables.match_length.as_ref(),
    )?;
    let stream = section.get(at..).ok_or(Corrupt::Sequences)?;
    let mut bits = Backward::new(stream)?;
    let mut ll_state = State::init(&ll, &mut bits);
    let mut of_state = State::init(&of, &mut bits);
    let mut ml_state = State::init(&ml, &mut bits);
    out.reserve_exact(count);
    for i in 0..count {
        let of_code = usize::from(of_state.symbol(&of)?);
        let ml_code = usize::from(ml_state.symbol(&ml)?);
        let ll_code = usize::from(ll_state.symbol(&ll)?);
        if of_code > MAX_OFFSET_CODE {
            return Err(Corrupt::Sequences);
        }
        // Offset bits first, then match length, then literals length (§3.1.1.3.2.1.2).
        let of_code32 = u32::try_from(of_code).map_err(|_| Corrupt::Sequences)?;
        let offset_value = (1u32 << of_code32)
            .checked_add(bits.read(of_code32))
            .ok_or(Corrupt::Sequences)?;
        let (ml_base, ml_bits) = *MATCH_LENGTH_CODES.get(ml_code).ok_or(Corrupt::Sequences)?;
        let match_length = ml_base
            .checked_add(bits.read(u32::from(ml_bits)))
            .ok_or(Corrupt::Sequences)?;
        let (ll_base, ll_bits) = *LITERALS_LENGTH_CODES
            .get(ll_code)
            .ok_or(Corrupt::Sequences)?;
        let literals = ll_base
            .checked_add(bits.read(u32::from(ll_bits)))
            .ok_or(Corrupt::Sequences)?;
        out.push(Sequence {
            literals,
            offset_value,
            match_length,
        });
        // States update for every sequence but the last: literals length, match length, offset.
        if i.saturating_add(1) < count {
            ll_state.update(&ll, &mut bits)?;
            ml_state.update(&ml, &mut bits)?;
            of_state.update(&of, &mut bits)?;
        }
    }
    if !bits.finished() {
        return Err(Corrupt::Sequences);
    }
    tables.literals_length = Some(ll);
    tables.offset = Some(of);
    tables.match_length = Some(ml);
    Ok(())
}

/// Reads a formatted dictionary's three FSE tables from `bytes` at `at`: offsets, match lengths,
/// literals lengths, in that order (§5), each a table description; returns where they end.
pub(super) fn dictionary_tables(
    bytes: &[u8],
    at: usize,
    tables: &mut Tables,
) -> Result<usize, Corrupt> {
    let mut at = at;
    for kind in [Kind::Offset, Kind::MatchLength, Kind::LiteralsLength] {
        let (norm, log, used) = read_distribution(
            bytes.get(at..).ok_or(Corrupt::Dictionary)?,
            kind.max_symbols(),
            kind.max_log(),
        )?;
        let table = Table::from_distribution(&norm, log)?;
        match kind {
            Kind::Offset => tables.offset = Some(table),
            Kind::MatchLength => tables.match_length = Some(table),
            Kind::LiteralsLength => tables.literals_length = Some(table),
        }
        at = at.checked_add(used).ok_or(Corrupt::Dictionary)?;
    }
    Ok(at)
}
