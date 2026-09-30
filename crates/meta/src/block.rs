//! The Block layer's state machine (docs/design/metadata.md §1): where each chunk of a block
//! lives, and a reverse row for each chunk, keyed by volume first, so repair lists a range's
//! blocks on a failed volume with one scan, as Tectonic's repair works per Block shard and
//! per disk through its reverse index (01 §1.5).
//!
//! A block is written for one file, whose write must name it by a deadline. The gateway renews
//! the deadline while the body the file holds still streams in, and until the sweep has settled
//! whether the file named the block, the block waits in the range's queue of unsettled blocks,
//! so a block whose gateway stopped before writing the file is found without a scan
//! (docs/design/metadata.md §2). The sweep releases a block only at the deadline it asked the
//! File range about: a renewal since keeps the block, and a released block is renewed no more.

use mantle_chunk::ChunkKey;

use crate::clock;
use crate::engine::{Rows, Write};
use crate::error::{MetaError, reserved};
use crate::key;
use crate::record::{BlockHeader, BlockOrigin, ChunkPlace, Reverse};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Records a block and where each of its chunks was written, in index order, for `file`,
    /// whose write must name it within `handover_ns` of the range's time now.
    Write {
        block: u128,
        header: BlockHeader,
        chunks: Vec<ChunkPlace>,
        file: u128,
        handover_ns: u64,
        at_ns: u64,
    },
    /// Holds a block for its file's write until `handover_ns` past the range's time now, or
    /// its deadline if later: the gateway renews the blocks it wrote while the body they hold
    /// still streams in.
    Renew {
        block: u128,
        file: u128,
        handover_ns: u64,
        at_ns: u64,
    },
    /// Releases a block the File range found no file will name, if its deadline is still
    /// `deadline_ns`, the one that range judged: a renewal since keeps the block.
    Release {
        block: u128,
        deadline_ns: u64,
    },
    /// Records that `chunk` lives on volume `to` instead of `from`: repair or rebalancing
    /// wrote it there.
    Move {
        chunk: ChunkKey,
        from: u128,
        to: u128,
    },
    Delete {
        block: u128,
    },
    /// The sweep found these blocks named by the files they were made for.
    Settle {
        blocks: Vec<u128>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The block is recorded, now or by the same request before, and its file's write names
    /// it until `deadline_ns`, which a renewal may have moved. A block its file named keeps the
    /// deadline it was settled at.
    Written {
        deadline_ns: u64,
    },
    /// The block is released: no file will name it, and the sweep takes it apart.
    Released,
    /// The sweep released the block: its file's write can no longer name it.
    Expired,
    Moved,
    Deleted,
    Settled,
    /// The block holds other chunks, the chunk is not on `from`, or `to` holds another of
    /// the block's chunks: the caller reads the block again.
    Conflict,
    /// Chunks other than one of the block per data and parity chunk, in index order, of one
    /// epoch, each on a volume of its own; a code with no data chunks; a chunk index past the
    /// block's; or a deadline past `u64`.
    Invalid,
    NoSuchBlock,
}

/// A block whose handover the sweep has yet to settle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unsettled {
    pub block: u128,
    pub deadline_ns: u64,
    /// The file it was made for.
    pub file: u128,
}

/// Applies `command` as log entry `index`.
pub fn apply<E: Rows>(engine: &mut E, index: u64, command: &Command) -> Result<Outcome, MetaError> {
    let (outcome, writes) = match command {
        Command::Write {
            block,
            header,
            chunks,
            file,
            handover_ns,
            at_ns,
        } => write(engine, *block, header, chunks, *file, *handover_ns, *at_ns)?,
        Command::Renew {
            block,
            file,
            handover_ns,
            at_ns,
        } => renew(engine, *block, *file, *handover_ns, *at_ns)?,
        Command::Release { block, deadline_ns } => release(engine, *block, *deadline_ns)?,
        Command::Move { chunk, from, to } => relocate(engine, chunk, *from, *to)?,
        Command::Delete { block } => delete(engine, *block)?,
        Command::Settle { blocks } => (Outcome::Settled, settle(engine, blocks)?),
    };
    engine.apply(index, &writes)?;
    Ok(outcome)
}

fn write<E: Rows>(
    engine: &E,
    block: u128,
    header: &BlockHeader,
    chunks: &[ChunkPlace],
    file: u128,
    handover_ns: u64,
    at_ns: u64,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    if !valid(block, header, chunks) {
        return Ok((Outcome::Invalid, Vec::new()));
    }
    if let Some((existing, places)) = read(engine, block)? {
        let origin = origin(engine, block)?.ok_or(MetaError::Corrupt)?;
        let outcome = if existing != *header || places != chunks || origin.file != file {
            Outcome::Conflict
        } else if origin.released {
            Outcome::Expired
        } else {
            Outcome::Written {
                deadline_ns: origin.deadline_ns,
            }
        };
        return Ok((outcome, Vec::new()));
    }
    let (made_ns, clock) = clock::tick(engine, at_ns)?;
    let Some(deadline_ns) = made_ns.checked_add(handover_ns) else {
        return Ok((Outcome::Invalid, Vec::new()));
    };
    let origin = BlockOrigin {
        file,
        made_ns,
        deadline_ns,
        released: false,
    };
    let mut writes = vec![
        clock,
        Write::Put(key::block_header(block), header.encode()),
        Write::Put(key::block_origin(block), origin.encode()),
        Write::Put(key::unsettled(deadline_ns, block), origin.encode()),
    ];
    for place in chunks {
        let index = place.key.index;
        writes.push(Write::Put(key::block_chunk(block, index), place.encode()));
        writes.push(Write::Put(
            key::reverse(place.volume, block),
            Reverse { index }.encode(),
        ));
    }
    Ok((Outcome::Written { deadline_ns }, writes))
}

/// Whether `chunks` are `block`'s, one per data and parity chunk of `header` in index order,
/// of one epoch, each on its own volume: two chunks on one volume would be lost together.
fn valid(block: u128, header: &BlockHeader, chunks: &[ChunkPlace]) -> bool {
    let Some(first) = chunks.first() else {
        return false;
    };
    let ours = chunks.iter().enumerate().all(|(i, place)| {
        u16::try_from(i).is_ok_and(|index| {
            place.key
                == ChunkKey {
                    block,
                    epoch: first.key.epoch,
                    index,
                }
        })
    });
    let mut volumes: Vec<u128> = chunks.iter().map(|place| place.volume).collect();
    volumes.sort_unstable();
    volumes.dedup();
    header.data > 0
        && width(header).map(usize::from) == Some(chunks.len())
        && ours
        && volumes.len() == chunks.len()
}

fn renew<E: Rows>(
    engine: &E,
    block: u128,
    file: u128,
    handover_ns: u64,
    at_ns: u64,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(origin) = origin(engine, block)? else {
        return Ok((Outcome::NoSuchBlock, Vec::new()));
    };
    if origin.file != file {
        return Ok((Outcome::Conflict, Vec::new()));
    }
    if origin.released {
        return Ok((Outcome::Expired, Vec::new()));
    }
    let queued = key::unsettled(origin.deadline_ns, block);
    let (now, clock) = clock::tick(engine, at_ns)?;
    let Some(until) = now.checked_add(handover_ns) else {
        return Ok((Outcome::Invalid, Vec::new()));
    };
    // A block the sweep settled is named by its file, and no deadline holds it any longer.
    if until <= origin.deadline_ns || engine.get(&queued)?.is_none() {
        let held = Outcome::Written {
            deadline_ns: origin.deadline_ns,
        };
        return Ok((held, vec![clock]));
    }
    let renewed = BlockOrigin {
        deadline_ns: until,
        ..origin
    };
    let writes = vec![
        clock,
        Write::Delete(queued),
        Write::Put(key::block_origin(block), renewed.encode()),
        Write::Put(key::unsettled(until, block), renewed.encode()),
    ];
    Ok((Outcome::Written { deadline_ns: until }, writes))
}

fn release<E: Rows>(
    engine: &E,
    block: u128,
    deadline_ns: u64,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(origin) = origin(engine, block)? else {
        return Ok((Outcome::NoSuchBlock, Vec::new()));
    };
    if origin.released {
        return Ok((Outcome::Released, Vec::new()));
    }
    let queued = key::unsettled(origin.deadline_ns, block);
    // Renewed since the File range judged it, or settled, its file naming it: kept.
    if origin.deadline_ns != deadline_ns || engine.get(&queued)?.is_none() {
        let held = Outcome::Written {
            deadline_ns: origin.deadline_ns,
        };
        return Ok((held, Vec::new()));
    }
    let released = BlockOrigin {
        released: true,
        ..origin
    };
    let writes = vec![
        Write::Put(key::block_origin(block), released.encode()),
        Write::Put(queued, released.encode()),
    ];
    Ok((Outcome::Released, writes))
}

fn relocate<E: Rows>(
    engine: &E,
    chunk: &ChunkKey,
    from: u128,
    to: u128,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some((_, places)) = read(engine, chunk.block)? else {
        return Ok((Outcome::NoSuchBlock, Vec::new()));
    };
    // A released block's chunks are being deleted: repair has nothing to keep.
    if origin(engine, chunk.block)?.is_some_and(|o| o.released) {
        return Ok((Outcome::Expired, Vec::new()));
    }
    let Some(place) = places.get(usize::from(chunk.index)) else {
        return Ok((Outcome::Invalid, Vec::new()));
    };
    if place.key != *chunk {
        // The block was written again at another epoch.
        return Ok((Outcome::Conflict, Vec::new()));
    }
    if place.volume == to {
        // A retried move finds the chunk where it moved it.
        return Ok((Outcome::Moved, Vec::new()));
    }
    if place.volume != from || places.iter().any(|p| p.volume == to) {
        return Ok((Outcome::Conflict, Vec::new()));
    }
    let moved = ChunkPlace {
        volume: to,
        key: *chunk,
    };
    let reverse = Reverse { index: chunk.index };
    let writes = vec![
        Write::Put(key::block_chunk(chunk.block, chunk.index), moved.encode()),
        Write::Delete(key::reverse(from, chunk.block)),
        Write::Put(key::reverse(to, chunk.block), reverse.encode()),
    ];
    Ok((Outcome::Moved, writes))
}

fn delete<E: Rows>(engine: &E, block: u128) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some((_, places)) = read(engine, block)? else {
        return Ok((Outcome::Deleted, Vec::new()));
    };
    let mut writes = vec![
        Write::Delete(key::block_header(block)),
        Write::Delete(key::block_origin(block)),
    ];
    if let Some(o) = origin(engine, block)? {
        writes.push(Write::Delete(key::unsettled(o.deadline_ns, block)));
    }
    for place in &places {
        writes.push(Write::Delete(key::block_chunk(block, place.key.index)));
        writes.push(Write::Delete(key::reverse(place.volume, block)));
    }
    Ok((Outcome::Deleted, writes))
}

/// Writes that take `blocks` out of the unsettled queue; a block already out, or removed,
/// needs none. A released block stays queued until its rows go, so a sweep that stops while
/// taking it apart finds it again.
fn settle<E: Rows>(engine: &E, blocks: &[u128]) -> Result<Vec<Write>, MetaError> {
    let mut writes = reserved(blocks.len())?;
    for &block in blocks {
        if let Some(o) = origin(engine, block)?
            && !o.released
        {
            writes.push(Write::Delete(key::unsettled(o.deadline_ns, block)));
        }
    }
    Ok(writes)
}

/// Where a block came from.
pub fn origin<E: Rows>(engine: &E, block: u128) -> Result<Option<BlockOrigin>, MetaError> {
    Ok(engine
        .get(&key::block_origin(block))?
        .map(|b| BlockOrigin::decode(&b))
        .transpose()?)
}

/// The blocks whose handover deadline passed before `before_ns` and the sweep has yet to
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
        let (deadline_ns, block) = key::decode_unsettled(&k).ok_or(MetaError::Corrupt)?;
        out.push(Unsettled {
            block,
            deadline_ns,
            file: BlockOrigin::decode(&v)?.file,
        });
        from = k;
        from.push(0);
    }
    Ok(out)
}

/// Chunks a block of `header`'s code has: its data and parity chunks.
fn width(header: &BlockHeader) -> Option<u16> {
    u16::from(header.data).checked_add(u16::from(header.parity))
}

/// A block's header and its chunks' places, in index order.
pub fn read<E: Rows>(
    engine: &E,
    block: u128,
) -> Result<Option<(BlockHeader, Vec<ChunkPlace>)>, MetaError> {
    let Some(bytes) = engine.get(&key::block_header(block))? else {
        return Ok(None);
    };
    let header = BlockHeader::decode(&bytes)?;
    let width = width(&header).ok_or(MetaError::Corrupt)?;
    let mut places = reserved(usize::from(width))?;
    for index in 0..width {
        let bytes = engine
            .get(&key::block_chunk(block, index))?
            .ok_or(MetaError::Corrupt)?;
        let place = ChunkPlace::decode(&bytes)?;
        if place.key.block != block || place.key.index != index {
            return Err(MetaError::Corrupt);
        }
        places.push(place);
    }
    Ok(Some((header, places)))
}

/// The range's blocks with a chunk on `volume`, after block `after`, at most `max` of them in
/// block order, each with the index of its chunk there.
pub fn on_volume<E: Rows>(
    engine: &E,
    volume: u128,
    after: Option<u128>,
    max: usize,
) -> Result<Vec<(u128, u16)>, MetaError> {
    let (first, to) = key::reverse_rows(volume);
    let mut from = match after {
        Some(block) => {
            let mut k = key::reverse(volume, block);
            k.push(0);
            k
        }
        None => first,
    };
    let mut out = Vec::new();
    while out.len() < max {
        let Some((k, v)) = engine.next(&from, &to)? else {
            break;
        };
        let (_, block) = key::decode_reverse(&k).ok_or(MetaError::Corrupt)?;
        out.push((block, Reverse::decode(&v)?.index));
        from = k;
        from.push(0);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Model;
    use proptest::prelude::*;

    fn header(data: u8, parity: u8) -> BlockHeader {
        BlockHeader {
            length: 3 << 20,
            data,
            parity,
            chunk_len: 3 << 19,
            crc32c: 1,
        }
    }

    /// Chunks of `block` at `epoch`, chunk `i` on `volumes[i]`.
    fn chunks(block: u128, epoch: u32, volumes: &[u128]) -> Vec<ChunkPlace> {
        (0u16..)
            .zip(volumes)
            .map(|(index, &volume)| ChunkPlace {
                volume,
                key: ChunkKey {
                    block,
                    epoch,
                    index,
                },
            })
            .collect()
    }

    fn chunk(block: u128, index: u16) -> ChunkKey {
        ChunkKey {
            block,
            epoch: 1,
            index,
        }
    }

    #[test]
    fn moves_keep_the_reverse_rows_beside_the_chunks() {
        let mut m = Model::default();
        let placed = chunks(5, 1, &[10, 11, 12]);
        let write = Command::Write {
            block: 5,
            header: header(2, 1),
            chunks: placed.clone(),
            at_ns: 0,
            file: 1,
            handover_ns: u64::MAX / 2,
        };
        let written = apply(&mut m, 1, &write).unwrap();
        assert!(matches!(written, Outcome::Written { .. }));
        // The same block again changes nothing, and answers the first deadline.
        assert_eq!(apply(&mut m, 2, &write).unwrap(), written);
        let again = Command::Write {
            block: 5,
            header: header(2, 1),
            chunks: chunks(5, 1, &[10, 11, 13]),
            at_ns: 0,
            file: 1,
            handover_ns: u64::MAX / 2,
        };
        assert_eq!(apply(&mut m, 3, &again).unwrap(), Outcome::Conflict);
        assert_eq!(read(&m, 5).unwrap(), Some((header(2, 1), placed)));
        assert_eq!(on_volume(&m, 10, None, 10).unwrap(), [(5, 0)]);
        assert_eq!(on_volume(&m, 12, None, 10).unwrap(), [(5, 2)]);

        let mv = |index, from, to| Command::Move {
            chunk: chunk(5, index),
            from,
            to,
        };
        assert_eq!(apply(&mut m, 4, &mv(0, 10, 13)).unwrap(), Outcome::Moved);
        assert!(on_volume(&m, 10, None, 10).unwrap().is_empty());
        assert_eq!(on_volume(&m, 13, None, 10).unwrap(), [(5, 0)]);
        // A retry answers as before; a move from where the chunk is not, or onto a volume
        // holding another of the block's chunks, conflicts.
        assert_eq!(apply(&mut m, 5, &mv(0, 10, 13)).unwrap(), Outcome::Moved);
        assert_eq!(apply(&mut m, 6, &mv(1, 10, 14)).unwrap(), Outcome::Conflict);
        assert_eq!(apply(&mut m, 7, &mv(1, 11, 12)).unwrap(), Outcome::Conflict);
        let stale = Command::Move {
            chunk: ChunkKey {
                epoch: 0,
                ..chunk(5, 1)
            },
            from: 11,
            to: 14,
        };
        assert_eq!(apply(&mut m, 8, &stale).unwrap(), Outcome::Conflict);
        assert_eq!(apply(&mut m, 9, &mv(3, 11, 14)).unwrap(), Outcome::Invalid);
        let missing = Command::Move {
            chunk: chunk(6, 0),
            from: 1,
            to: 2,
        };
        assert_eq!(apply(&mut m, 10, &missing).unwrap(), Outcome::NoSuchBlock);

        let delete = Command::Delete { block: 5 };
        assert_eq!(apply(&mut m, 11, &delete).unwrap(), Outcome::Deleted);
        assert_eq!(read(&m, 5).unwrap(), None);
        assert_eq!(
            m.next(&[key::DATA], &[u8::MAX]).unwrap(),
            None,
            "a deleted block leaves no rows"
        );
        assert!(unsettled(&m, u64::MAX, 10).unwrap().is_empty());
        assert_eq!(apply(&mut m, 12, &delete).unwrap(), Outcome::Deleted);
    }

    #[test]
    fn a_block_is_one_chunk_per_data_and_parity_chunk_each_on_its_own_volume() {
        let mut m = Model::default();
        let bad = [
            (header(2, 1), chunks(1, 1, &[1, 2])),
            (header(0, 1), chunks(1, 1, &[1])),
            (header(1, 1), chunks(1, 1, &[1, 1])),
            (header(1, 1), chunks(2, 1, &[1, 2])),
            (
                header(1, 1),
                vec![chunks(1, 1, &[1, 2])[1], chunks(1, 1, &[1, 2])[0]],
            ),
            (
                header(1, 1),
                vec![chunks(1, 1, &[1])[0], chunks(1, 2, &[3, 2])[1]],
            ),
        ];
        for (i, (header, chunks)) in (1..).zip(bad) {
            let write = Command::Write {
                block: 1,
                header,
                chunks,
                at_ns: 0,
                file: 1,
                handover_ns: u64::MAX / 2,
            };
            assert_eq!(
                apply(&mut m, i, &write).unwrap(),
                Outcome::Invalid,
                "{write:?}"
            );
        }
        assert_eq!(m.next(&[], &[u8::MAX]).unwrap(), None);
    }

    #[test]
    fn a_volume_lists_its_blocks_in_pages() {
        let mut m = Model::default();
        for block in 1..=5u128 {
            let write = Command::Write {
                block,
                header: header(1, 1),
                chunks: chunks(block, 1, &[7, 100 + block]),
                at_ns: 0,
                file: 1,
                handover_ns: u64::MAX / 2,
            };
            let index = u64::try_from(block).unwrap();
            assert!(matches!(
                apply(&mut m, index, &write).unwrap(),
                Outcome::Written { .. }
            ));
        }
        let first = on_volume(&m, 7, None, 2).unwrap();
        assert_eq!(first, [(1, 0), (2, 0)]);
        let rest = on_volume(&m, 7, Some(2), 10).unwrap();
        assert_eq!(rest, [(3, 0), (4, 0), (5, 0)]);
        assert_eq!(on_volume(&m, 103, None, 10).unwrap(), [(3, 1)]);
        assert!(on_volume(&m, 8, None, 10).unwrap().is_empty());
    }

    /// A renewal holds a block for another handover from the range's time. The sweep's
    /// release at a deadline renewed since keeps the block, and a released block renews no
    /// more: its write is refused and repair leaves it, while it waits in the queue until its
    /// rows go.
    #[test]
    fn a_block_is_held_by_renewals_until_released_at_the_deadline_judged() {
        let mut m = Model::default();
        let write = Command::Write {
            block: 5,
            header: header(1, 1),
            chunks: chunks(5, 1, &[1, 2]),
            file: 9,
            handover_ns: 100,
            at_ns: 1_000,
        };
        let held = |deadline_ns| Outcome::Written { deadline_ns };
        assert_eq!(apply(&mut m, 1, &write).unwrap(), held(1_100));
        let renew = |file, handover_ns, at_ns| Command::Renew {
            block: 5,
            file,
            handover_ns,
            at_ns,
        };
        assert_eq!(
            apply(&mut m, 2, &renew(9, 100, 1_050)).unwrap(),
            held(1_150)
        );
        let queued = |m: &Model| -> Vec<u64> {
            unsettled(m, u64::MAX, 10)
                .unwrap()
                .iter()
                .map(|u| u.deadline_ns)
                .collect()
        };
        assert_eq!(queued(&m), [1_150]);
        // A renewal never brings the deadline nearer.
        assert_eq!(apply(&mut m, 3, &renew(9, 10, 1_060)).unwrap(), held(1_150));
        assert_eq!(
            apply(&mut m, 4, &renew(8, 100, 1_070)).unwrap(),
            Outcome::Conflict
        );
        let other = Command::Renew {
            block: 6,
            file: 9,
            handover_ns: 100,
            at_ns: 1_080,
        };
        assert_eq!(apply(&mut m, 5, &other).unwrap(), Outcome::NoSuchBlock);
        // The File range judged the first deadline; the renewal since keeps the block.
        let release = |deadline_ns| Command::Release {
            block: 5,
            deadline_ns,
        };
        assert_eq!(apply(&mut m, 6, &release(1_100)).unwrap(), held(1_150));
        assert_eq!(
            apply(&mut m, 7, &release(1_150)).unwrap(),
            Outcome::Released
        );
        assert_eq!(
            apply(&mut m, 8, &release(1_150)).unwrap(),
            Outcome::Released
        );
        assert_eq!(
            apply(&mut m, 9, &renew(9, 100, 1_200)).unwrap(),
            Outcome::Expired
        );
        assert_eq!(apply(&mut m, 10, &write).unwrap(), Outcome::Expired);
        let repair = Command::Move {
            chunk: chunk(5, 0),
            from: 1,
            to: 3,
        };
        assert_eq!(apply(&mut m, 11, &repair).unwrap(), Outcome::Expired);
        assert_eq!(queued(&m), [1_150]);
        let delete = Command::Delete { block: 5 };
        assert_eq!(apply(&mut m, 12, &delete).unwrap(), Outcome::Deleted);
        assert!(queued(&m).is_empty());
        assert_eq!(m.next(&[key::DATA], &[u8::MAX]).unwrap(), None);
    }

    /// A block its file named, which the sweep settled, is held by the file: renewing it and
    /// releasing it change nothing.
    #[test]
    fn a_settled_block_is_neither_renewed_nor_released() {
        let mut m = Model::default();
        let write = Command::Write {
            block: 5,
            header: header(1, 1),
            chunks: chunks(5, 1, &[1, 2]),
            file: 9,
            handover_ns: 100,
            at_ns: 1_000,
        };
        apply(&mut m, 1, &write).unwrap();
        let settle = Command::Settle { blocks: vec![5] };
        assert_eq!(apply(&mut m, 2, &settle).unwrap(), Outcome::Settled);
        let renew = Command::Renew {
            block: 5,
            file: 9,
            handover_ns: 100,
            at_ns: 1_050,
        };
        let held = Outcome::Written { deadline_ns: 1_100 };
        assert_eq!(apply(&mut m, 3, &renew).unwrap(), held);
        let release = Command::Release {
            block: 5,
            deadline_ns: 1_100,
        };
        assert_eq!(apply(&mut m, 4, &release).unwrap(), held);
        assert!(unsettled(&m, u64::MAX, 10).unwrap().is_empty());
        assert!(!origin(&m, 5).unwrap().unwrap().released);
    }

    fn command() -> impl Strategy<Value = Command> {
        let volumes = prop::collection::vec(0u128..6, 1..4);
        prop_oneof![
            (0u128..3, 0u32..2, volumes, 1u64..40).prop_map(
                |(block, epoch, volumes, handover_ns)| {
                    let parity = u8::try_from(volumes.len() - 1).unwrap();
                    Command::Write {
                        block,
                        header: header(1, parity),
                        chunks: chunks(block, epoch, &volumes),
                        at_ns: 0,
                        file: 1,
                        handover_ns,
                    }
                }
            ),
            (0u128..3, 1u128..3, 0u64..40).prop_map(|(block, file, handover_ns)| {
                Command::Renew {
                    block,
                    file,
                    handover_ns,
                    at_ns: 0,
                }
            }),
            (0u128..3, 0u64..80)
                .prop_map(|(block, deadline_ns)| Command::Release { block, deadline_ns }),
            prop::collection::vec(0u128..3, 0..3).prop_map(|blocks| Command::Settle { blocks }),
            (0u128..3, 0u32..2, 0u16..3, 0u128..6, 0u128..6).prop_map(
                |(block, epoch, index, from, to)| Command::Move {
                    chunk: ChunkKey {
                        block,
                        epoch,
                        index,
                    },
                    from,
                    to,
                }
            ),
            (0u128..3).prop_map(|block| Command::Delete { block }),
        ]
    }

    proptest! {
        /// After any commands, each chunk row has exactly one reverse row, under its volume
        /// and naming its index, and no other reverse row exists. A block waits in the queue
        /// once, at its deadline, until the sweep settles it or its rows go; its deadline never
        /// comes nearer, and once released it stays released.
        #[test]
        fn every_chunk_has_one_reverse_row(commands in prop::collection::vec(command(), 1..40)) {
            let mut m = Model::default();
            let mut before: Vec<Option<BlockOrigin>> = vec![None; 3];
            for (index, command) in (1..).zip(&commands) {
                apply(&mut m, index, command).unwrap();
                let queue = unsettled(&m, u64::MAX, 100).unwrap();
                for block in 0..3u128 {
                    let now = origin(&m, block).unwrap();
                    let queued: Vec<u64> = queue
                        .iter()
                        .filter(|u| u.block == block)
                        .map(|u| u.deadline_ns)
                        .collect();
                    match now {
                        None => prop_assert!(queued.is_empty()),
                        Some(o) => {
                            prop_assert!(queued.is_empty() || queued == [o.deadline_ns]);
                            prop_assert!(!o.released || queued == [o.deadline_ns]);
                        }
                    }
                    let slot = usize::try_from(block).unwrap();
                    if let (Some(b), Some(n)) = (before[slot], now) {
                        prop_assert!(n.deadline_ns >= b.deadline_ns);
                        prop_assert!(n.released || !b.released);
                    }
                    before[slot] = now;
                }
                let mut forward = Vec::new();
                for block in 0..3 {
                    if let Some((_, places)) = read(&m, block).unwrap() {
                        let mut volumes: Vec<_> = places.iter().map(|p| p.volume).collect();
                        volumes.sort_unstable();
                        volumes.dedup();
                        prop_assert_eq!(volumes.len(), places.len());
                        forward.extend(places.iter().map(|p| (p.volume, block, p.key.index)));
                    }
                }
                let mut reverse = Vec::new();
                for volume in 0..6 {
                    for (block, index) in on_volume(&m, volume, None, 10).unwrap() {
                        reverse.push((volume, block, index));
                    }
                }
                forward.sort_unstable();
                prop_assert_eq!(forward, reverse);
            }
        }
    }
}
