//! Reclaiming a file the Name layer released (docs/design/metadata.md §2): each block's chunks
//! from their volumes, then the block's rows, an adopted part's file the same way, then the
//! file's rows, and last its row in the Name range's queue.
//!
//! A [`Reclaimer`] names its next [`Request`] and moves on with the [`Answer`], doing no I/O
//! itself, as the coordinator does. Every step can be repeated: a chunk or a row already gone
//! stays gone, and a file already removed reads as one with no extents. So a collector that
//! stops part way starts again from the queue row, which goes last.

use std::collections::VecDeque;

use crate::record::{BlockHeader, ChunkPlace, Extent, Target};
use crate::{block, file, name};

/// A reclaimer's next request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// A read of at most `max` of `file`'s extents, from the one holding byte `from`
    /// ([`file::extents`]).
    Extents { file: u128, from: u64, max: usize },
    /// A read of a block's header and its chunks' places ([`block::read`]).
    Block(u128),
    /// Deleting a chunk from its volume, on the node that holds the volume.
    Chunk(ChunkPlace),
    /// A command to the Block range that holds the block.
    BlockCommand(block::Command),
    /// A command to the File range that holds the file.
    FileCommand(file::Command),
    /// Dropping the file's row from the queue of the Name range that released it, as
    /// `name::Command::Reclaim`.
    Reclaim(name::Reclaim),
}

/// What a request answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Extents(Vec<(u64, Extent)>),
    Block(Option<(BlockHeader, Vec<ChunkPlace>)>),
    /// The chunk is gone from its volume, deleted now or before.
    Chunk,
    BlockOutcome(block::Outcome),
    FileOutcome(file::Outcome),
    NameOutcome(name::Outcome),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReclaimError {
    #[error("an answer of another kind than the request")]
    Mismatch,
    #[error("an answer the reclaimer's step cannot have")]
    Unexpected,
    /// An adopted part's file names another file: a part is written as blocks.
    #[error("a part's file holds another file")]
    Nested,
}

/// A file being taken apart: where its next extents start, and those read but not yet
/// reclaimed.
#[derive(Debug, Clone)]
struct Frame {
    file: u128,
    from: u64,
    /// The last read found the file's end.
    ended: bool,
    pending: VecDeque<Extent>,
}

impl Frame {
    fn new(file: u128) -> Self {
        Self {
            file,
            from: 0,
            ended: false,
            pending: VecDeque::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    /// Reading the top file's next extents, or moving on to its next pending one.
    Next,
    ReadBlock(u128),
    /// Deleting the block's chunks, the next one first.
    Chunks(u128),
    DeleteBlock(u128),
    DeleteFile(u128),
    Reclaim,
    Done,
}

/// The reclamation of one released file.
#[derive(Debug, Clone)]
pub struct Reclaimer {
    released_ns: u64,
    root: u128,
    /// The released file, then the adopted part being taken apart: at most two.
    frames: Vec<Frame>,
    chunks: Vec<ChunkPlace>,
    /// Extents one read returns at most, which bounds what the reclaimer holds.
    budget: usize,
    phase: Phase,
}

impl Reclaimer {
    /// The reclamation of `file`, released at `released_ns` ([`name::released`]).
    pub fn new(released_ns: u64, file: u128, budget: usize) -> Self {
        Self {
            released_ns,
            root: file,
            frames: vec![Frame::new(file)],
            chunks: Vec::new(),
            budget: budget.max(1),
            phase: Phase::Next,
        }
    }

    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    /// The request to send next; `None` once the file is reclaimed.
    pub fn next(&self) -> Option<Request> {
        Some(match &self.phase {
            Phase::Next => {
                let frame = self.frames.last()?;
                Request::Extents {
                    file: frame.file,
                    from: frame.from,
                    max: self.budget,
                }
            }
            Phase::ReadBlock(block) => Request::Block(*block),
            Phase::Chunks(_) => Request::Chunk(*self.chunks.last()?),
            Phase::DeleteBlock(block) => {
                Request::BlockCommand(block::Command::Delete { block: *block })
            }
            Phase::DeleteFile(file) => Request::FileCommand(file::Command::Delete { file: *file }),
            Phase::Reclaim => Request::Reclaim(name::Reclaim {
                released_ns: self.released_ns,
                file: self.root,
            }),
            Phase::Done => return None,
        })
    }

    /// Moves on with what the request [`Reclaimer::next`] named answered.
    pub fn step(&mut self, answer: Answer) -> Result<(), ReclaimError> {
        match (&self.phase, answer) {
            (Phase::Next, Answer::Extents(extents)) => {
                let frame = self.frames.last_mut().ok_or(ReclaimError::Unexpected)?;
                match extents.last() {
                    None => frame.ended = true,
                    Some(&(start, last)) => {
                        frame.from = start
                            .checked_add(last.length)
                            .ok_or(ReclaimError::Unexpected)?;
                        frame.ended = extents.len() < self.budget;
                    }
                }
                frame
                    .pending
                    .extend(extents.into_iter().map(|(_, extent)| extent));
                self.advance()
            }
            (Phase::ReadBlock(block), Answer::Block(read)) => {
                let block = *block;
                match read {
                    // Removed before: a reclaimer that stopped after its block step.
                    None => self.advance(),
                    Some((_, places)) => {
                        self.chunks = places;
                        self.phase = if self.chunks.is_empty() {
                            Phase::DeleteBlock(block)
                        } else {
                            Phase::Chunks(block)
                        };
                        Ok(())
                    }
                }
            }
            (Phase::Chunks(block), Answer::Chunk) => {
                let block = *block;
                self.chunks.pop();
                if self.chunks.is_empty() {
                    self.phase = Phase::DeleteBlock(block);
                }
                Ok(())
            }
            (Phase::DeleteBlock(_), Answer::BlockOutcome(outcome)) => match outcome {
                block::Outcome::Deleted => self.advance(),
                _ => Err(ReclaimError::Unexpected),
            },
            (Phase::DeleteFile(_), Answer::FileOutcome(outcome)) => match outcome {
                file::Outcome::Deleted => {
                    self.frames.pop();
                    if self.frames.is_empty() {
                        self.phase = Phase::Reclaim;
                        Ok(())
                    } else {
                        self.advance()
                    }
                }
                _ => Err(ReclaimError::Unexpected),
            },
            (Phase::Reclaim, Answer::NameOutcome(outcome)) => match outcome {
                name::Outcome::Reclaimed => {
                    self.phase = Phase::Done;
                    Ok(())
                }
                _ => Err(ReclaimError::Unexpected),
            },
            _ => Err(ReclaimError::Mismatch),
        }
    }

    /// Takes the top file's next pending extent, reads its next ones, or, once it has none
    /// left, removes it.
    fn advance(&mut self) -> Result<(), ReclaimError> {
        let depth = self.frames.len();
        let frame = self.frames.last_mut().ok_or(ReclaimError::Unexpected)?;
        self.phase = match frame.pending.pop_front() {
            Some(Extent {
                target: Target::Block(block),
                ..
            }) => Phase::ReadBlock(block),
            Some(Extent {
                target: Target::File(part),
                ..
            }) => {
                if depth > 1 {
                    return Err(ReclaimError::Nested);
                }
                self.frames.push(Frame::new(part));
                Phase::Next
            }
            None if frame.ended => Phase::DeleteFile(frame.file),
            None => Phase::Next,
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Model;
    use crate::name::{Command, Delete, Named, Outcome, Put};
    use crate::record::{Version, Versioning};
    use mantle_chunk::ChunkKey;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    /// A cell's worth of ranges on model engines, and the chunks its volumes hold.
    struct Cell {
        name: Model,
        files: Model,
        blocks: Model,
        chunks: BTreeSet<(u128, ChunkKey)>,
        index: u64,
    }

    impl Cell {
        fn new() -> Self {
            let mut cell = Self {
                name: Model::default(),
                files: Model::default(),
                blocks: Model::default(),
                chunks: BTreeSet::new(),
                index: 0,
            };
            cell.run_name(&Command::Gate(name::GateChange {
                bucket: "b".into(),
                incarnation: 1,
                attempt: 1,
                from: None,
                to: Some(crate::record::GateState::Open),
            }));
            cell
        }

        fn tick(&mut self) -> u64 {
            self.index += 1;
            self.index
        }

        fn run_name(&mut self, command: &Command) -> Outcome {
            let index = self.tick();
            name::apply(&mut self.name, index, command).unwrap()
        }

        /// A block of `width` chunks on volumes of their own, recorded as a PUT's would be.
        fn block(&mut self, block: u128, width: u8) {
            let chunks: Vec<ChunkPlace> = (0..width)
                .map(|index| ChunkPlace {
                    volume: u128::from(index) + 1,
                    key: ChunkKey {
                        block,
                        epoch: 1,
                        index: u16::from(index),
                    },
                })
                .collect();
            for place in &chunks {
                self.chunks.insert((place.volume, place.key));
            }
            let index = self.tick();
            let outcome = block::apply(
                &mut self.blocks,
                index,
                &block::Command::Write {
                    block,
                    header: BlockHeader {
                        length: 1,
                        data: width,
                        parity: 0,
                        chunk_len: 1,
                        crc32c: 0,
                    },
                    chunks,
                },
            )
            .unwrap();
            assert_eq!(outcome, block::Outcome::Written);
        }

        fn file(&mut self, file: u128, targets: &[Target]) {
            let extents = targets
                .iter()
                .map(|&target| Extent { length: 1, target })
                .collect();
            let index = self.tick();
            let outcome = file::apply(
                &mut self.files,
                index,
                &file::Command::Write { file, extents },
            )
            .unwrap();
            assert_eq!(outcome, file::Outcome::Written);
        }

        /// Sends `request` where it goes and returns its answer.
        fn answer(&mut self, request: Request) -> Answer {
            match request {
                Request::Extents { file, from, max } => {
                    Answer::Extents(file::extents(&self.files, file, from, max).unwrap())
                }
                Request::Block(block) => Answer::Block(block::read(&self.blocks, block).unwrap()),
                Request::Chunk(place) => {
                    self.chunks.remove(&(place.volume, place.key));
                    Answer::Chunk
                }
                Request::BlockCommand(command) => {
                    let index = self.tick();
                    Answer::BlockOutcome(block::apply(&mut self.blocks, index, &command).unwrap())
                }
                Request::FileCommand(command) => {
                    let index = self.tick();
                    Answer::FileOutcome(file::apply(&mut self.files, index, &command).unwrap())
                }
                Request::Reclaim(reclaim) => {
                    Answer::NameOutcome(self.run_name(&Command::Reclaim(reclaim)))
                }
            }
        }

        /// Runs `reclaimer` for at most `steps` requests; whether it finished.
        fn drive(&mut self, reclaimer: &mut Reclaimer, steps: usize) -> bool {
            for _ in 0..steps {
                let Some(request) = reclaimer.next() else {
                    return true;
                };
                let answer = self.answer(request);
                reclaimer.step(answer).unwrap();
            }
            reclaimer.is_done()
        }

        fn rows(engine: &Model) -> usize {
            let mut count = 0;
            let mut from = vec![crate::key::DATA];
            let to = vec![crate::key::REVERSE, 0xFF];
            while let Some((k, _)) = crate::engine::Rows::next(engine, &from, &to).unwrap() {
                count += 1;
                from = k;
                from.push(0);
            }
            count
        }
    }

    fn object(file: u128) -> Version {
        Version {
            marker: false,
            null: false,
            modified_ns: 0,
            etag: "e".into(),
            size: 1,
            checksum: None,
            file: Some(file),
            owner: "o".into(),
            headers: Vec::new(),
            retention: None,
            legal_hold: None,
        }
    }

    /// A completed upload's file of two part files, each of blocks, and a plain object beside
    /// it: removing the upload's version releases its file, and reclaiming it removes its
    /// parts, their blocks and chunks, and the queue row, and nothing of the other object.
    #[test]
    fn a_released_upload_is_reclaimed_to_its_chunks() {
        let mut cell = Cell::new();
        cell.block(11, 3);
        cell.block(12, 2);
        cell.block(21, 3);
        cell.file(100, &[Target::Block(11), Target::Block(12)]);
        cell.file(200, &[Target::Block(21)]);
        cell.file(300, &[Target::File(100), Target::File(200)]);
        cell.block(41, 2);
        cell.file(400, &[Target::Block(41)]);
        for (key, file) in [("upload", 300), ("other", 400)] {
            let outcome = cell.run_name(&Command::Put(Put {
                bucket: "b".into(),
                incarnation: 1,
                key: key.into(),
                versioning: Versioning::Unversioned,
                preconditions: name::Preconditions::default(),
                at_ns: 1,
                ordered_ns: None,
                version: object(file),
                default: None,
            }));
            assert!(matches!(outcome, Outcome::Put { .. }));
        }
        cell.run_name(&Command::Delete(Delete {
            bucket: "b".into(),
            incarnation: 1,
            key: "upload".into(),
            versioning: Versioning::Unversioned,
            named: Some(Named::Null),
            if_match: None,
            at_ns: 2,
            bypass: false,
        }));
        let queue = name::released(&cell.name, u64::MAX, 10).unwrap();
        assert_eq!(queue.len(), 1);
        let (released_ns, file) = queue[0];
        assert_eq!(file, 300);
        let mut reclaimer = Reclaimer::new(released_ns, file, 1);
        assert!(cell.drive(&mut reclaimer, 1_000));
        assert_eq!(name::released(&cell.name, u64::MAX, 10).unwrap(), []);
        for gone in [100, 200, 300] {
            assert_eq!(file::header(&cell.files, gone).unwrap(), None);
        }
        for gone in [11, 12, 21] {
            assert_eq!(block::read(&cell.blocks, gone).unwrap(), None);
        }
        // Only the other object's chunks, block and file remain.
        assert_eq!(cell.chunks.len(), 2);
        assert!(cell.chunks.iter().all(|(_, key)| key.block == 41));
        assert!(block::read(&cell.blocks, 41).unwrap().is_some());
        assert!(file::header(&cell.files, 400).unwrap().is_some());
        assert!(Cell::rows(&cell.blocks) > 0);
    }

    #[test]
    fn answers_out_of_turn_are_refused() {
        let mut reclaimer = Reclaimer::new(1, 5, 4);
        assert_eq!(reclaimer.step(Answer::Chunk), Err(ReclaimError::Mismatch));
        reclaimer
            .step(Answer::Extents(vec![(
                0,
                Extent {
                    length: 1,
                    target: Target::File(6),
                },
            )]))
            .unwrap();
        // The adopted part holds a file: parts are written as blocks.
        assert_eq!(
            reclaimer.step(Answer::Extents(vec![(
                0,
                Extent {
                    length: 1,
                    target: Target::File(7),
                },
            )])),
            Err(ReclaimError::Nested)
        );
    }

    proptest! {
        /// Stopped after any number of steps and started again from the queue row, as many
        /// times as it takes, the reclaimer ends where one uninterrupted run does: every
        /// row, block and chunk of the released file gone, and nothing else.
        #[test]
        fn a_reclaimer_stopped_anywhere_resumes_from_the_queue(
            parts in proptest::collection::vec(proptest::collection::vec(1u8..4, 1..4), 1..4),
            budget in 1usize..4,
            stops in proptest::collection::vec(0usize..12, 0..6),
        ) {
            let mut cell = Cell::new();
            let mut next = 10u128;
            let mut part_files = Vec::new();
            for widths in &parts {
                let mut targets = Vec::new();
                for &width in widths {
                    next += 1;
                    cell.block(next, width);
                    targets.push(Target::Block(next));
                }
                next += 1;
                cell.file(next, &targets);
                part_files.push(Target::File(next));
            }
            next += 1;
            let root = next;
            cell.file(root, &part_files);
            cell.run_name(&Command::Put(Put {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                versioning: Versioning::Unversioned,
                preconditions: name::Preconditions::default(),
                at_ns: 1,
                ordered_ns: None,
                version: object(root),
                default: None,
            }));
            cell.run_name(&Command::Delete(Delete {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                versioning: Versioning::Unversioned,
                named: None,
                if_match: None,
                at_ns: 2,
                bypass: false,
            }));
            for &stop in &stops {
                let [(released_ns, file)] = name::released(&cell.name, u64::MAX, 2).unwrap()[..] else {
                    break;
                };
                let mut reclaimer = Reclaimer::new(released_ns, file, budget);
                cell.drive(&mut reclaimer, stop);
            }
            if let [(released_ns, file)] = name::released(&cell.name, u64::MAX, 2).unwrap()[..] {
                let mut reclaimer = Reclaimer::new(released_ns, file, budget);
                prop_assert!(cell.drive(&mut reclaimer, 10_000));
            }
            prop_assert_eq!(name::released(&cell.name, u64::MAX, 2).unwrap(), vec![]);
            prop_assert!(cell.chunks.is_empty());
            prop_assert_eq!(Cell::rows(&cell.files), 0);
            prop_assert_eq!(Cell::rows(&cell.blocks), 0);
        }
    }
}
