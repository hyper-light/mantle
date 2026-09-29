//! The File layer's state machine (docs/design/metadata.md §1–§2): a file is written once,
//! whole, as the list of extents that hold its bytes, and removed whole by the collector.

use crate::engine::{Rows, Write};
use crate::error::MetaError;
use crate::key;
use crate::record::{Extent, FileHeader};

/// Extents one file may have: a completed upload's parts, at most 10,000 (05 §4.1). A file
/// written by one PUT, at most 5 GB (05 §4.1), stays within it while its blocks hold at least
/// 5 GiB / 10,000 bytes, about 525 KiB.
pub const MAX_EXTENTS: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Writes a file of `extents`, in order from its first byte.
    Write {
        file: u128,
        extents: Vec<Extent>,
    },
    Delete {
        file: u128,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Written,
    Deleted,
    /// A file of that ID holds other extents. IDs are random 128-bit numbers, so a caller
    /// that reuses one has a bug to report.
    Conflict,
    /// No extents, more than `MAX_EXTENTS`, an empty extent, or a length past `u64`.
    Invalid,
}

/// Applies `command` as log entry `index`.
pub fn apply<E: Rows>(engine: &mut E, index: u64, command: &Command) -> Result<Outcome, MetaError> {
    let (outcome, writes) = match command {
        Command::Write { file, extents } => write(engine, *file, extents)?,
        Command::Delete { file } => (Outcome::Deleted, remove(engine, *file)?),
    };
    engine.apply(index, &writes)?;
    Ok(outcome)
}

fn write<E: Rows>(
    engine: &E,
    file: u128,
    extents: &[Extent],
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let invalid = Ok((Outcome::Invalid, Vec::new()));
    if extents.is_empty() || extents.len() > MAX_EXTENTS {
        return invalid;
    }
    let mut writes = Vec::with_capacity(extents.len());
    let mut end = 0u64;
    for extent in extents {
        match end.checked_add(extent.length) {
            Some(next) if extent.length > 0 => end = next,
            _ => return invalid,
        }
        writes.push(Write::Put(key::file_extent(file, end), extent.encode()));
    }
    let Ok(count) = u32::try_from(extents.len()) else {
        return invalid;
    };
    let header = FileHeader {
        length: end,
        extents: count,
    };
    if let Some(existing) = self::header(engine, file)? {
        // Written already: a retry of the same file changes nothing.
        let stored = self::extents(engine, file, 0, MAX_EXTENTS)?;
        let same = existing == header && stored.iter().map(|(_, e)| e).eq(extents);
        let outcome = if same {
            Outcome::Written
        } else {
            Outcome::Conflict
        };
        return Ok((outcome, Vec::new()));
    }
    writes.push(Write::Put(key::file_header(file), header.encode()));
    Ok((Outcome::Written, writes))
}

/// Writes that remove every row of `file`: its header and at most `MAX_EXTENTS` extents.
fn remove<E: Rows>(engine: &E, file: u128) -> Result<Vec<Write>, MetaError> {
    let (mut from, to) = key::id_rows(file);
    let mut writes = Vec::new();
    while let Some((k, _)) = engine.next(&from, &to)? {
        if writes.len() > MAX_EXTENTS {
            return Err(MetaError::Corrupt);
        }
        from.clone_from(&k);
        from.push(0);
        writes.push(Write::Delete(k));
    }
    Ok(writes)
}

/// A file's header.
pub fn header<E: Rows>(engine: &E, file: u128) -> Result<Option<FileHeader>, MetaError> {
    Ok(engine
        .get(&key::file_header(file))?
        .map(|b| FileHeader::decode(&b))
        .transpose()?)
}

/// A file's extents from the one holding byte `offset`, at most `max` of them, in order, each
/// with the offset of its first byte.
pub fn extents<E: Rows>(
    engine: &E,
    file: u128,
    offset: u64,
    max: usize,
) -> Result<Vec<(u64, Extent)>, MetaError> {
    // The extent holding `offset` is the first to end past it.
    let Some(past) = offset.checked_add(1) else {
        return Ok(Vec::new());
    };
    let mut from = key::file_extent(file, past);
    let (_, to) = key::id_rows(file);
    let mut out = Vec::new();
    while out.len() < max {
        let Some((k, v)) = engine.next(&from, &to)? else {
            break;
        };
        let end = key::extent_end(&k).ok_or(MetaError::Corrupt)?;
        let extent = Extent::decode(&v)?;
        let start = end.checked_sub(extent.length).ok_or(MetaError::Corrupt)?;
        out.push((start, extent));
        from = k;
        from.push(0);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Model;
    use crate::record::Target;

    fn extent(length: u64, block: u128) -> Extent {
        Extent {
            length,
            target: Target::Block(block),
        }
    }

    #[test]
    fn a_file_is_written_once_and_read_from_any_offset() {
        let mut m = Model::default();
        let parts = vec![extent(10, 1), extent(5, 2), extent(20, 3)];
        let placed = vec![(0, parts[0]), (10, parts[1]), (15, parts[2])];
        let write = Command::Write {
            file: 7,
            extents: parts.clone(),
        };
        assert_eq!(apply(&mut m, 1, &write).unwrap(), Outcome::Written);
        assert_eq!(header(&m, 7).unwrap().unwrap().length, 35);
        assert_eq!(extents(&m, 7, 0, 10).unwrap(), placed);
        assert_eq!(extents(&m, 7, 9, 1).unwrap(), placed[..1]);
        assert_eq!(extents(&m, 7, 10, 10).unwrap(), placed[1..]);
        assert_eq!(extents(&m, 7, 34, 10).unwrap(), placed[2..]);
        assert!(extents(&m, 7, 35, 10).unwrap().is_empty());
        assert!(extents(&m, 7, u64::MAX, 10).unwrap().is_empty());
        // The same file again changes nothing; another under its ID is a conflict.
        assert_eq!(apply(&mut m, 2, &write).unwrap(), Outcome::Written);
        let other = Command::Write {
            file: 7,
            extents: vec![extent(35, 9)],
        };
        assert_eq!(apply(&mut m, 3, &other).unwrap(), Outcome::Conflict);
        let delete = Command::Delete { file: 7 };
        assert_eq!(apply(&mut m, 4, &delete).unwrap(), Outcome::Deleted);
        assert_eq!(header(&m, 7).unwrap(), None);
        assert_eq!(
            m.next(&[], &[u8::MAX]).unwrap(),
            None,
            "a deleted file leaves no rows"
        );
        assert_eq!(apply(&mut m, 5, &delete).unwrap(), Outcome::Deleted);
    }

    #[test]
    fn invalid_files_are_refused() {
        let mut m = Model::default();
        let mut run = |i, extents| apply(&mut m, i, &Command::Write { file: 1, extents }).unwrap();
        assert_eq!(run(1, vec![]), Outcome::Invalid);
        assert_eq!(run(2, vec![extent(0, 1)]), Outcome::Invalid);
        assert_eq!(
            run(3, vec![extent(u64::MAX, 1), extent(1, 2)]),
            Outcome::Invalid
        );
        assert_eq!(
            run(4, vec![extent(1, 1); MAX_EXTENTS + 1]),
            Outcome::Invalid
        );
        assert_eq!(run(5, vec![extent(1, 1); MAX_EXTENTS]), Outcome::Written);
        let count = header(&m, 1).unwrap().unwrap().extents;
        assert_eq!(usize::try_from(count).unwrap(), MAX_EXTENTS);
        assert_eq!(
            apply(&mut m, 6, &Command::Delete { file: 1 }).unwrap(),
            Outcome::Deleted
        );
        assert_eq!(m.next(&[], &[u8::MAX]).unwrap(), None);
    }
}
