//! The File layer's state machine (docs/design/metadata.md §1–§2): a file is written once,
//! whole, as the list of extents that hold its bytes, and removed whole by the collector.
//!
//! A file is written for one Name-range write, which must take it by a deadline. Until the
//! sweep has settled whether it did, the file waits in the range's queue of unsettled files,
//! so a file whose gateway stopped before handing it over is found without a scan. In turn a
//! file names blocks made for it alone, which it must name by theirs, and the sweep of the
//! Block ranges asks here whether it did (docs/design/metadata.md §2).

use std::collections::BTreeSet;

use crate::clock;
use crate::engine::{Rows, Write};
use crate::error::MetaError;
use crate::key;
use crate::record::{Extent, FileHeader, Referrer, Target, Verdict};

/// Extents one file may have: a completed upload's parts, at most 10,000 (05 §4.1). A file
/// written by one PUT, at most 5 GB (05 §4.1), stays within it while its blocks hold at least
/// 5 GiB / 10,000 bytes, about 525 KiB.
pub const MAX_EXTENTS: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Writes a file of `extents`, in order from its first byte, for `referrer`'s write, which
    /// must hand it over within `handover_ns` of the range's time now. The blocks it names were
    /// made for it, and the soonest of their deadlines is `blocks_deadline_ns`: past it, the
    /// sweep may have taken one apart, and the write is refused.
    Write {
        file: u128,
        extents: Vec<Extent>,
        referrer: Referrer,
        handover_ns: u64,
        blocks_deadline_ns: u64,
        at_ns: u64,
    },
    Delete {
        file: u128,
    },
    /// The sweep settled these files' handovers: the Name range took each, or released it.
    Settle {
        files: Vec<u128>,
    },
    /// The sweep of a Block range asks whether `file` names each of these blocks, made for it,
    /// each with its deadline (docs/design/metadata.md §2).
    CheckBlocks {
        file: u128,
        blocks: Vec<(u128, u64)>,
        at_ns: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The file is written, now or by the same request before: the Name range takes it until
    /// `deadline_ns`.
    Written {
        deadline_ns: u64,
    },
    Deleted,
    Settled,
    /// A file of that ID holds other extents. IDs are random 128-bit numbers, so a caller
    /// that reuses one has a bug to report.
    Conflict,
    /// No extents, more than `MAX_EXTENTS`, an empty extent, or a length or deadline past
    /// `u64`.
    Invalid,
    /// The range's time has passed a block's deadline: the gateway makes the blocks again.
    Expired,
    /// Each block asked about: named by the file (`Held`); not, and never to be, since the file
    /// names other extents or the block's deadline has passed with no file written
    /// (`Released`); or not yet (`Young`).
    BlocksChecked(Vec<Verdict>),
}

/// A file whose handover the sweep has yet to settle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsettled {
    pub file: u128,
    pub deadline_ns: u64,
    pub referrer: Referrer,
}

/// Applies `command` as log entry `index`.
pub fn apply<E: Rows>(engine: &mut E, index: u64, command: &Command) -> Result<Outcome, MetaError> {
    let (outcome, writes) = match command {
        Command::Write {
            file,
            extents,
            referrer,
            handover_ns,
            blocks_deadline_ns,
            at_ns,
        } => {
            let intent = Intent {
                referrer,
                handover_ns: *handover_ns,
                blocks_deadline_ns: *blocks_deadline_ns,
                at_ns: *at_ns,
            };
            write(engine, *file, extents, &intent)?
        }
        Command::Delete { file } => (Outcome::Deleted, remove(engine, *file)?),
        Command::Settle { files } => (Outcome::Settled, settle(engine, files)?),
        Command::CheckBlocks {
            file,
            blocks,
            at_ns,
        } => check_blocks(engine, *file, blocks, *at_ns)?,
    };
    engine.apply(index, &writes)?;
    Ok(outcome)
}

/// What a file is written for, beside its extents.
struct Intent<'a> {
    referrer: &'a Referrer,
    handover_ns: u64,
    blocks_deadline_ns: u64,
    at_ns: u64,
}

fn write<E: Rows>(
    engine: &E,
    file: u128,
    extents: &[Extent],
    intent: &Intent<'_>,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let (referrer, handover_ns, at_ns) = (intent.referrer, intent.handover_ns, intent.at_ns);
    let invalid = Ok((Outcome::Invalid, Vec::new()));
    if extents.is_empty() || extents.len() > MAX_EXTENTS {
        return invalid;
    }
    let mut writes = Vec::with_capacity(extents.len().saturating_add(3));
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
    if let Some(existing) = self::header(engine, file)? {
        // Written already: a retry of the same file changes nothing.
        let stored = self::extents(engine, file, 0, MAX_EXTENTS)?;
        let same = existing.length == end
            && existing.extents == count
            && existing.referrer == *referrer
            && stored.iter().map(|(_, e)| e).eq(extents);
        let outcome = if same {
            Outcome::Written {
                deadline_ns: existing.deadline_ns,
            }
        } else {
            Outcome::Conflict
        };
        return Ok((outcome, Vec::new()));
    }
    let (made_ns, clock) = clock::tick(engine, at_ns)?;
    if made_ns > intent.blocks_deadline_ns {
        return Ok((Outcome::Expired, Vec::new()));
    }
    let Some(deadline_ns) = made_ns.checked_add(handover_ns) else {
        return invalid;
    };
    let header = FileHeader {
        length: end,
        extents: count,
        made_ns,
        deadline_ns,
        referrer: referrer.clone(),
    };
    writes.push(Write::Put(key::file_header(file), header.encode()?));
    writes.push(Write::Put(
        key::unsettled(deadline_ns, file),
        referrer.encode()?,
    ));
    writes.push(clock);
    Ok((Outcome::Written { deadline_ns }, writes))
}

/// Writes that remove every row of `file`: its header, at most `MAX_EXTENTS` extents, and its
/// place in the unsettled queue.
fn remove<E: Rows>(engine: &E, file: u128) -> Result<Vec<Write>, MetaError> {
    let mut writes = Vec::new();
    if let Some(h) = header(engine, file)? {
        writes.push(Write::Delete(key::unsettled(h.deadline_ns, file)));
    }
    let (mut from, to) = key::id_rows(file);
    while let Some((k, _)) = engine.next(&from, &to)? {
        if writes.len() > MAX_EXTENTS.saturating_add(1) {
            return Err(MetaError::Corrupt);
        }
        from.clone_from(&k);
        from.push(0);
        writes.push(Write::Delete(k));
    }
    Ok(writes)
}

/// Writes that take `files` out of the unsettled queue; a file already out, or removed,
/// needs none.
fn settle<E: Rows>(engine: &E, files: &[u128]) -> Result<Vec<Write>, MetaError> {
    let mut writes = Vec::with_capacity(files.len());
    for &file in files {
        if let Some(h) = header(engine, file)? {
            writes.push(Write::Delete(key::unsettled(h.deadline_ns, file)));
        }
    }
    Ok(writes)
}

/// Whether `file` names each of `blocks`, each with its deadline. A file is written once, whole,
/// so one written names what it will ever name; one not written by the range's time past a
/// block's deadline never will, since its write would be refused. The check records that time
/// as a write's, so every later write reads a time at least as late.
fn check_blocks<E: Rows>(
    engine: &E,
    file: u128,
    blocks: &[(u128, u64)],
    at_ns: u64,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let written = header(engine, file)?.is_some();
    let named: BTreeSet<u128> = if written {
        extents(engine, file, 0, MAX_EXTENTS)?
            .into_iter()
            .filter_map(|(_, e)| match e.target {
                Target::Block(block) => Some(block),
                Target::File(_) => None,
            })
            .collect()
    } else {
        BTreeSet::new()
    };
    let (now, clock) = clock::tick(engine, at_ns)?;
    let verdicts = blocks
        .iter()
        .map(|&(block, deadline_ns)| {
            if named.contains(&block) {
                Verdict::Held
            } else if written || now > deadline_ns {
                Verdict::Released
            } else {
                Verdict::Young
            }
        })
        .collect();
    Ok((Outcome::BlocksChecked(verdicts), vec![clock]))
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

/// The files whose handover deadline passed before `before_ns` and the sweep has yet to
/// settle, soonest deadline first, at most `max`: the sweep's work.
pub fn unsettled<E: Rows>(
    engine: &E,
    before_ns: u64,
    max: usize,
) -> Result<Vec<Unsettled>, MetaError> {
    let (mut from, to) = key::unsettled_before(before_ns);
    let mut out = Vec::new();
    while out.len() < max {
        let Some((k, v)) = engine.next(&from, &to)? else {
            break;
        };
        let (deadline_ns, file) = key::decode_unsettled(&k).ok_or(MetaError::Corrupt)?;
        out.push(Unsettled {
            file,
            deadline_ns,
            referrer: Referrer::decode(&v)?,
        });
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

    fn referrer(key: &str) -> Referrer {
        Referrer {
            bucket: "b".into(),
            incarnation: 1,
            key: key.into(),
        }
    }

    fn write(file: u128, extents: Vec<Extent>, at_ns: u64) -> Command {
        Command::Write {
            file,
            extents,
            referrer: referrer("k"),
            handover_ns: 100,
            at_ns,
            blocks_deadline_ns: u64::MAX,
        }
    }

    #[test]
    fn a_file_is_written_once_and_read_from_any_offset() {
        let mut m = Model::default();
        let parts = vec![extent(10, 1), extent(5, 2), extent(20, 3)];
        let placed = vec![(0, parts[0]), (10, parts[1]), (15, parts[2])];
        let written = Outcome::Written { deadline_ns: 1_100 };
        assert_eq!(
            apply(&mut m, 1, &write(7, parts.clone(), 1_000)).unwrap(),
            written
        );
        let h = header(&m, 7).unwrap().unwrap();
        assert_eq!((h.length, h.made_ns, h.deadline_ns), (35, 1_000, 1_100));
        assert_eq!(h.referrer, referrer("k"));
        assert_eq!(extents(&m, 7, 0, 10).unwrap(), placed);
        assert_eq!(extents(&m, 7, 9, 1).unwrap(), placed[..1]);
        assert_eq!(extents(&m, 7, 10, 10).unwrap(), placed[1..]);
        assert_eq!(extents(&m, 7, 34, 10).unwrap(), placed[2..]);
        assert!(extents(&m, 7, 35, 10).unwrap().is_empty());
        assert!(extents(&m, 7, u64::MAX, 10).unwrap().is_empty());
        // The same file again changes nothing, and answers the first deadline; another
        // under its ID is a conflict.
        assert_eq!(apply(&mut m, 2, &write(7, parts, 5_000)).unwrap(), written);
        assert_eq!(
            apply(&mut m, 3, &write(7, vec![extent(35, 9)], 5_000)).unwrap(),
            Outcome::Conflict
        );
        let delete = Command::Delete { file: 7 };
        assert_eq!(apply(&mut m, 4, &delete).unwrap(), Outcome::Deleted);
        assert_eq!(header(&m, 7).unwrap(), None);
        let rest = m.next(&[key::DATA], &[u8::MAX]).unwrap();
        assert_eq!(rest, None, "a deleted file leaves no rows");
        assert!(unsettled(&m, u64::MAX, 10).unwrap().is_empty());
        assert_eq!(apply(&mut m, 5, &delete).unwrap(), Outcome::Deleted);
    }

    /// Every file waits in the unsettled queue from its write until the sweep settles it, and
    /// comes due once its deadline has passed.
    #[test]
    fn a_written_file_waits_unsettled_until_the_sweep_settles_it() {
        let mut m = Model::default();
        for (i, (file, at)) in [(2u128, 100u64), (3, 200), (1, 300)]
            .into_iter()
            .enumerate()
        {
            let i = u64::try_from(i).unwrap() + 1;
            apply(&mut m, i, &write(file, vec![extent(1, file)], at)).unwrap();
        }
        let due = |m: &Model, before| -> Vec<u128> {
            unsettled(m, before, 10)
                .unwrap()
                .into_iter()
                .map(|u| u.file)
                .collect()
        };
        // Deadlines 200, 300 and 400 in order of their passing.
        assert_eq!(due(&m, 250), [2]);
        assert_eq!(due(&m, u64::MAX), [2, 3, 1]);
        assert_eq!(
            unsettled(&m, u64::MAX, 10).unwrap()[0].referrer,
            referrer("k")
        );
        let settle = Command::Settle {
            files: vec![2, 3, 99],
        };
        assert_eq!(apply(&mut m, 4, &settle).unwrap(), Outcome::Settled);
        assert_eq!(due(&m, u64::MAX), [1]);
        // Settling again, or a removed file, changes nothing.
        assert_eq!(apply(&mut m, 5, &settle).unwrap(), Outcome::Settled);
        // A file's clock is the range's: a proposal behind it takes the next instant.
        assert_eq!(
            apply(&mut m, 6, &write(4, vec![extent(1, 4)], 50)).unwrap(),
            Outcome::Written { deadline_ns: 401 }
        );
    }

    #[test]
    fn invalid_files_are_refused() {
        let mut m = Model::default();
        let mut run = |i, extents| apply(&mut m, i, &write(1, extents, 10)).unwrap();
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
        assert_eq!(
            run(5, vec![extent(1, 1); MAX_EXTENTS]),
            Outcome::Written { deadline_ns: 110 }
        );
        let count = header(&m, 1).unwrap().unwrap().extents;
        assert_eq!(usize::try_from(count).unwrap(), MAX_EXTENTS);
        let far = Command::Write {
            file: 2,
            extents: vec![extent(1, 1)],
            referrer: referrer("k"),
            handover_ns: u64::MAX,
            at_ns: 10,
            blocks_deadline_ns: u64::MAX,
        };
        assert_eq!(apply(&mut m, 6, &far).unwrap(), Outcome::Invalid);
        assert_eq!(
            apply(&mut m, 7, &Command::Delete { file: 1 }).unwrap(),
            Outcome::Deleted
        );
        assert_eq!(m.next(&[key::DATA], &[u8::MAX]).unwrap(), None);
        assert!(unsettled(&m, u64::MAX, 10).unwrap().is_empty());
    }
}
