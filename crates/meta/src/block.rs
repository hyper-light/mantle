//! The Block layer's state machine (docs/design/metadata.md §1): where each chunk of a block
//! lives, and a reverse row for each chunk, keyed by volume first, so repair lists a range's
//! blocks on a failed volume with one scan, as Tectonic's repair works per Block shard and
//! per disk through its reverse index (01 §1.5).

use mantle_chunk::ChunkKey;

use crate::engine::{Engine, Write};
use crate::error::MetaError;
use crate::key;
use crate::record::{BlockHeader, ChunkPlace, Reverse};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Records a block and where each of its chunks was written, in index order.
    Write {
        block: u128,
        header: BlockHeader,
        chunks: Vec<ChunkPlace>,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Written,
    Moved,
    Deleted,
    /// The block holds other chunks, the chunk is not on `from`, or `to` holds another of
    /// the block's chunks: the caller reads the block again.
    Conflict,
    /// Chunks other than one of the block per data and parity chunk, in index order, of one
    /// epoch, each on a volume of its own; a code with no data chunks; or a chunk index past
    /// the block's.
    Invalid,
    NoSuchBlock,
}

/// Applies `command` as log entry `index`.
pub fn apply<E: Engine>(
    engine: &mut E,
    index: u64,
    command: &Command,
) -> Result<Outcome, MetaError> {
    let (outcome, writes) = match command {
        Command::Write {
            block,
            header,
            chunks,
        } => write(engine, *block, header, chunks)?,
        Command::Move { chunk, from, to } => relocate(engine, chunk, *from, *to)?,
        Command::Delete { block } => delete(engine, *block)?,
    };
    engine.apply(index, &writes)?;
    Ok(outcome)
}

fn write<E: Engine>(
    engine: &E,
    block: u128,
    header: &BlockHeader,
    chunks: &[ChunkPlace],
) -> Result<(Outcome, Vec<Write>), MetaError> {
    if !valid(block, header, chunks) {
        return Ok((Outcome::Invalid, Vec::new()));
    }
    if let Some((existing, places)) = read(engine, block)? {
        let outcome = if existing == *header && places == chunks {
            Outcome::Written
        } else {
            Outcome::Conflict
        };
        return Ok((outcome, Vec::new()));
    }
    let mut writes = vec![Write::Put(key::block_header(block), header.encode())];
    for place in chunks {
        let index = place.key.index;
        writes.push(Write::Put(key::block_chunk(block, index), place.encode()));
        writes.push(Write::Put(
            key::reverse(place.volume, block),
            Reverse { index }.encode(),
        ));
    }
    Ok((Outcome::Written, writes))
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

fn relocate<E: Engine>(
    engine: &E,
    chunk: &ChunkKey,
    from: u128,
    to: u128,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some((_, places)) = read(engine, chunk.block)? else {
        return Ok((Outcome::NoSuchBlock, Vec::new()));
    };
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

fn delete<E: Engine>(engine: &E, block: u128) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some((_, places)) = read(engine, block)? else {
        return Ok((Outcome::Deleted, Vec::new()));
    };
    let mut writes = vec![Write::Delete(key::block_header(block))];
    for place in &places {
        writes.push(Write::Delete(key::block_chunk(block, place.key.index)));
        writes.push(Write::Delete(key::reverse(place.volume, block)));
    }
    Ok((Outcome::Deleted, writes))
}

/// Chunks a block of `header`'s code has: its data and parity chunks.
fn width(header: &BlockHeader) -> Option<u16> {
    u16::from(header.data).checked_add(u16::from(header.parity))
}

/// A block's header and its chunks' places, in index order.
pub fn read<E: Engine>(
    engine: &E,
    block: u128,
) -> Result<Option<(BlockHeader, Vec<ChunkPlace>)>, MetaError> {
    let Some(bytes) = engine.get(&key::block_header(block))? else {
        return Ok(None);
    };
    let header = BlockHeader::decode(&bytes)?;
    let width = width(&header).ok_or(MetaError::Corrupt)?;
    let mut places = Vec::with_capacity(usize::from(width));
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
pub fn on_volume<E: Engine>(
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
        };
        assert_eq!(apply(&mut m, 1, &write).unwrap(), Outcome::Written);
        assert_eq!(apply(&mut m, 2, &write).unwrap(), Outcome::Written);
        let again = Command::Write {
            block: 5,
            header: header(2, 1),
            chunks: chunks(5, 1, &[10, 11, 13]),
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
            m.next(&[], &[u8::MAX]).unwrap(),
            None,
            "a deleted block leaves no rows"
        );
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
            };
            let index = u64::try_from(block).unwrap();
            assert_eq!(apply(&mut m, index, &write).unwrap(), Outcome::Written);
        }
        let first = on_volume(&m, 7, None, 2).unwrap();
        assert_eq!(first, [(1, 0), (2, 0)]);
        let rest = on_volume(&m, 7, Some(2), 10).unwrap();
        assert_eq!(rest, [(3, 0), (4, 0), (5, 0)]);
        assert_eq!(on_volume(&m, 103, None, 10).unwrap(), [(3, 1)]);
        assert!(on_volume(&m, 8, None, 10).unwrap().is_empty());
    }

    fn command() -> impl Strategy<Value = Command> {
        let volumes = prop::collection::vec(0u128..6, 1..4);
        prop_oneof![
            (0u128..3, 0u32..2, volumes).prop_map(|(block, epoch, volumes)| {
                let parity = u8::try_from(volumes.len() - 1).unwrap();
                Command::Write {
                    block,
                    header: header(1, parity),
                    chunks: chunks(block, epoch, &volumes),
                }
            }),
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
        /// and naming its index, and no other reverse row exists.
        #[test]
        fn every_chunk_has_one_reverse_row(commands in prop::collection::vec(command(), 1..40)) {
            let mut m = Model::default();
            for (index, command) in (1..).zip(&commands) {
                apply(&mut m, index, command).unwrap();
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
