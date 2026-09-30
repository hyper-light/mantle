//! Reclaiming a file the Name layer released (docs/design/metadata.md §2): each block's chunks
//! from their volumes, then the block's rows; each part file it names given back to the Name
//! range, which releases it only if this file adopted it; then the file's rows, then its mark,
//! wherever its key now is, and last its row in the Name range's queue.
//!
//! A [`Reclaimer`] names its next [`Request`] and moves on with the [`Answer`], doing no I/O
//! itself, as the coordinator does. Every step can be repeated: a chunk or a row already gone
//! stays gone, a part given back twice is released once, and a file already removed reads as
//! one with no extents. So a collector that stops part way starts again from the queue row,
//! which goes last.

use std::collections::VecDeque;

use crate::record::{BlockHeader, ChunkPlace, Extent, Holder, Target};
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
    /// Giving back a part file the released file names, as `name::Command::Disown`, at the
    /// Name range that holds its key now. A part only its adopter gives back: a composite that
    /// lost its completion, or was written again for a retry, names parts it never adopted,
    /// which the upload or another composite holds (audit B02).
    Disown(name::Disown),
    /// Removing the file's mark, as `name::Command::Unmark`, at the Name range that holds its
    /// key now.
    Unmark(name::Unmark),
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    /// Reading the file's next extents, or moving on to its next pending one.
    Next,
    ReadBlock(u128),
    /// Deleting the block's chunks, the next one first.
    Chunks(u128),
    DeleteBlock(u128),
    Disown(u128),
    DeleteFile,
    Unmark,
    Reclaim,
    Done,
}

/// The reclamation of one released file.
#[derive(Debug, Clone)]
pub struct Reclaimer {
    released_ns: u64,
    file: u128,
    holder: Holder,
    /// Where the file's next extents start, and whether the last read found its end.
    from: u64,
    ended: bool,
    /// Extents read but not yet reclaimed.
    pending: VecDeque<Extent>,
    chunks: Vec<ChunkPlace>,
    /// Extents one read returns at most, which bounds what the reclaimer holds.
    budget: usize,
    phase: Phase,
}

impl Reclaimer {
    /// The reclamation of a released file ([`name::released`]).
    pub fn new(released: name::Released, budget: usize) -> Self {
        Self {
            released_ns: released.released_ns,
            file: released.file,
            holder: released.holder,
            from: 0,
            ended: false,
            pending: VecDeque::new(),
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
            Phase::Next => Request::Extents {
                file: self.file,
                from: self.from,
                max: self.budget,
            },
            Phase::ReadBlock(block) => Request::Block(*block),
            Phase::Chunks(_) => Request::Chunk(*self.chunks.last()?),
            Phase::DeleteBlock(block) => {
                Request::BlockCommand(block::Command::Delete { block: *block })
            }
            Phase::Disown(part) => Request::Disown(name::Disown {
                bucket: self.holder.bucket.clone(),
                key: self.holder.key.clone(),
                file: *part,
                owner: self.file,
                // The entry that carries a command gives it its time.
                at_ns: 0,
            }),
            Phase::DeleteFile => Request::FileCommand(file::Command::Delete { file: self.file }),
            Phase::Unmark => Request::Unmark(name::Unmark {
                files: vec![name::Marked {
                    bucket: self.holder.bucket.clone(),
                    key: self.holder.key.clone(),
                    file: self.file,
                }],
            }),
            Phase::Reclaim => Request::Reclaim(name::Reclaim {
                released_ns: self.released_ns,
                file: self.file,
            }),
            Phase::Done => return None,
        })
    }

    /// Moves on with what the request [`Reclaimer::next`] named answered.
    pub fn step(&mut self, answer: Answer) -> Result<(), ReclaimError> {
        match (&self.phase, answer) {
            (Phase::Next, Answer::Extents(extents)) => {
                match extents.last() {
                    None => self.ended = true,
                    Some(&(start, last)) => {
                        self.from = start
                            .checked_add(last.length)
                            .ok_or(ReclaimError::Unexpected)?;
                        self.ended = extents.len() < self.budget;
                    }
                }
                self.pending
                    .extend(extents.into_iter().map(|(_, extent)| extent));
                self.advance();
                Ok(())
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
                    }
                }
                Ok(())
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
                block::Outcome::Deleted => {
                    self.advance();
                    Ok(())
                }
                _ => Err(ReclaimError::Unexpected),
            },
            (Phase::Disown(_), Answer::NameOutcome(outcome)) => match outcome {
                // Released for its own reclaiming, or held by another: either way done here.
                name::Outcome::Disowned { .. } => {
                    self.advance();
                    Ok(())
                }
                _ => Err(ReclaimError::Unexpected),
            },
            (Phase::DeleteFile, Answer::FileOutcome(outcome)) => match outcome {
                file::Outcome::Deleted => {
                    self.phase = Phase::Unmark;
                    Ok(())
                }
                _ => Err(ReclaimError::Unexpected),
            },
            (Phase::Unmark, Answer::NameOutcome(outcome)) => match outcome {
                name::Outcome::Unmarked => {
                    self.phase = Phase::Reclaim;
                    Ok(())
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

    /// Takes the file's next pending extent, reads its next ones, or, once it has none left,
    /// removes it.
    fn advance(&mut self) {
        self.phase = match self.pending.pop_front() {
            Some(Extent {
                target: Target::Block(block),
                ..
            }) => Phase::ReadBlock(block),
            Some(Extent {
                target: Target::File(part),
                ..
            }) => Phase::Disown(part),
            None if self.ended => Phase::DeleteFile,
            None => Phase::Next,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{Engine, Model};
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
            let mut first = Model::default();
            first.install(0, name::first(1).unwrap()).unwrap();
            let mut cell = Self {
                name: first,
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
                generation: 1,
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
                    at_ns: 0,
                    file: 1,
                    handover_ns: u64::MAX / 2,
                },
            )
            .unwrap();
            assert!(matches!(outcome, block::Outcome::Written { .. }));
        }

        fn file(&mut self, file: u128, targets: &[Target]) {
            let extents = targets
                .iter()
                .map(|&target| Extent { length: 1, target })
                .collect();
            let index = self.tick();
            let write = file::Command::Write {
                file,
                extents,
                referrer: crate::record::Referrer {
                    bucket: "b".into(),
                    incarnation: 1,
                    key: "k".into(),
                },
                key: None,
                handover_ns: 1_000,
                at_ns: index,
                blocks_deadline_ns: u64::MAX,
            };
            let outcome = file::apply(&mut self.files, index, &write).unwrap();
            assert!(matches!(outcome, file::Outcome::Written { .. }));
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
                Request::Disown(disown) => {
                    Answer::NameOutcome(self.run_name(&Command::Disown(disown)))
                }
                Request::Unmark(unmark) => {
                    Answer::NameOutcome(self.run_name(&Command::Unmark(unmark)))
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
            listing: None,
        }
    }

    /// A completed upload's file of two part files, each of blocks, and a plain object beside
    /// it: removing the upload's version releases its file, and reclaiming it gives back its
    /// parts, reclaimed in turn to their blocks and chunks, and nothing of the other object.
    #[test]
    fn a_released_upload_is_reclaimed_to_its_chunks() {
        let mut cell = Cell::new();
        let upload = upload(&mut cell, 300);
        assert!(matches!(
            complete(&mut cell, &upload, 300, None),
            Outcome::Put { .. }
        ));
        cell.block(41, 2);
        cell.file(400, &[Target::Block(41)]);
        let outcome = cell.run_name(&Command::Put(Put {
            bucket: "b".into(),
            incarnation: 1,
            key: "other".into(),
            versioning: Versioning::Unversioned,
            preconditions: name::Preconditions::default(),
            at_ns: 4,
            ordered_ns: None,
            version: object(400),
            default: None,
            deadline_ns: u64::MAX,
        }));
        assert!(matches!(outcome, Outcome::Put { .. }));
        cell.run_name(&Command::Delete(Delete {
            bucket: "b".into(),
            incarnation: 1,
            key: "k".into(),
            versioning: Versioning::Unversioned,
            named: Some(Named::Null),
            if_match: None,
            at_ns: 5,
            bypass: false,
            owner: "o".into(),
        }));
        let queue = name::released(&cell.name, u64::MAX, 10).unwrap();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].file, 300);
        reclaim_all(&mut cell);
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
        // No mark is left on a file reclaimed.
        let (mut from, to) = crate::key::marks_span("b");
        while let Some((k, _)) = crate::engine::Rows::next(&cell.name, &from, &to).unwrap() {
            let (_, _, file) = crate::key::decode_mark(&k).unwrap();
            assert_eq!(file, 400, "a mark left on file {file}");
            from = k;
            from.push(0);
        }
    }

    /// An upload of two parts, each a file of blocks: part 1 of blocks 11 and 12, part 2 of
    /// block 21, and a composite `composite` of both written for its completion.
    fn upload(cell: &mut Cell, composite: u128) -> String {
        cell.block(11, 3);
        cell.block(12, 2);
        cell.block(21, 3);
        cell.file(100, &[Target::Block(11), Target::Block(12)]);
        cell.file(200, &[Target::Block(21)]);
        cell.file(composite, &[Target::File(100), Target::File(200)]);
        let created = cell.run_name(&Command::CreateUpload(name::CreateUpload {
            bucket: "b".into(),
            incarnation: 1,
            key: "k".into(),
            at_ns: 1,
            upload: crate::record::Upload {
                initiated_ns: 0,
                owner: "o".into(),
                headers: Vec::new(),
                checksum: None,
                retention: None,
                legal_hold: None,
            },
        }));
        let Outcome::Created { upload } = created else {
            panic!("{created:?}")
        };
        for (number, file, size) in [(1, 100, name::MIN_PART), (2, 200, 1)] {
            let outcome = cell.run_name(&Command::PutPart(name::PutPart {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                upload: upload.clone(),
                number,
                part: crate::record::Part {
                    etag: format!("p{number}"),
                    size,
                    checksum: None,
                    file,
                    modified_ns: 0,
                },
                at_ns: 2,
                deadline_ns: u64::MAX,
            }));
            assert_eq!(outcome, Outcome::PartWritten);
        }
        upload
    }

    fn complete(cell: &mut Cell, upload: &str, file: u128, if_match: Option<&str>) -> Outcome {
        cell.run_name(&Command::Complete(name::Complete {
            bucket: "b".into(),
            incarnation: 1,
            key: "k".into(),
            upload: upload.into(),
            versioning: Versioning::Unversioned,
            preconditions: name::Preconditions {
                if_match: if_match.map(|t| name::Match::Tags(vec![t.into()])),
                if_none_match: None,
            },
            at_ns: 3,
            parts: vec![
                name::Listed {
                    number: 1,
                    etag: "p1".into(),
                    file: 100,
                },
                name::Listed {
                    number: 2,
                    etag: "p2".into(),
                    file: 200,
                },
            ],
            etag: "m-2".into(),
            size: name::MIN_PART + 1,
            checksum: None,
            file: Some(file),
            default: None,
            deadline_ns: u64::MAX,
            listing: [0; crate::record::LISTING],
        }))
    }

    /// Reclaims every file the Name range has released, each to its end.
    fn reclaim_all(cell: &mut Cell) {
        for _ in 0..10 {
            let queue = name::released(&cell.name, u64::MAX, 10).unwrap();
            if queue.is_empty() {
                return;
            }
            for released in queue {
                let mut reclaimer = Reclaimer::new(released, 2);
                assert!(cell.drive(&mut reclaimer, 1_000));
            }
        }
        panic!("the queue never emptied");
    }

    /// The part files, their blocks and their chunks are all still held.
    fn parts_held(cell: &Cell) -> bool {
        [100, 200]
            .iter()
            .all(|&f| file::header(&cell.files, f).unwrap().is_some())
            && [11, 12, 21]
                .iter()
                .all(|&b| block::read(&cell.blocks, b).unwrap().is_some())
            && cell.chunks.len() == 8
    }

    /// A completion refused, here by its precondition, releases the composite written for
    /// it; reclaiming that takes nothing of the parts, which the upload still holds, and
    /// which an abort then releases (audit B02).
    #[test]
    fn a_refused_completion_reclaims_none_of_its_parts() {
        let mut cell = Cell::new();
        let upload = upload(&mut cell, 300);
        assert_eq!(
            complete(&mut cell, &upload, 300, Some("nope")),
            Outcome::NoSuchKey
        );
        reclaim_all(&mut cell);
        assert_eq!(file::header(&cell.files, 300).unwrap(), None);
        assert!(parts_held(&cell), "the upload's parts were taken apart");
        assert_eq!(
            name::parts(&cell.name, "b", "k", &upload, 0, 10)
                .unwrap()
                .len(),
            2
        );
        let aborted = cell.run_name(&Command::Abort(name::Abort {
            bucket: "b".into(),
            incarnation: 1,
            key: "k".into(),
            upload: upload.clone(),
            at_ns: 4,
        }));
        assert_eq!(aborted, Outcome::Aborted);
        reclaim_all(&mut cell);
        assert!(cell.chunks.is_empty());
    }

    /// A completion retried after it committed, with a composite made again, releases the
    /// second composite; reclaiming it takes nothing of the parts, which the first holds. The
    /// object's removal then releases the first, and its parts with it (audit B02).
    #[test]
    fn a_retried_completion_reclaims_none_of_the_committed_parts() {
        let mut cell = Cell::new();
        let upload = upload(&mut cell, 300);
        assert!(matches!(
            complete(&mut cell, &upload, 300, None),
            Outcome::Put { .. }
        ));
        cell.file(301, &[Target::File(100), Target::File(200)]);
        assert!(matches!(
            complete(&mut cell, &upload, 301, None),
            Outcome::Completed { .. }
        ));
        reclaim_all(&mut cell);
        assert_eq!(file::header(&cell.files, 301).unwrap(), None);
        assert!(file::header(&cell.files, 300).unwrap().is_some());
        assert!(
            parts_held(&cell),
            "the committed object's parts were taken apart"
        );
        cell.run_name(&Command::Delete(Delete {
            bucket: "b".into(),
            incarnation: 1,
            key: "k".into(),
            versioning: Versioning::Unversioned,
            named: Some(Named::Null),
            if_match: None,
            at_ns: 5,
            bypass: false,
            owner: "o".into(),
        }));
        reclaim_all(&mut cell);
        assert!(cell.chunks.is_empty());
        for gone in [100, 200, 300] {
            assert_eq!(file::header(&cell.files, gone).unwrap(), None);
        }
    }

    #[test]
    fn answers_out_of_turn_are_refused() {
        let mut reclaimer = Reclaimer::new(
            name::Released {
                released_ns: 1,
                file: 5,
                holder: crate::record::Holder {
                    bucket: "b".into(),
                    key: "k".into(),
                },
            },
            4,
        );
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
        // A part file is given back to the Name range, named for the file that holds it.
        assert_eq!(
            reclaimer.next(),
            Some(Request::Disown(name::Disown {
                bucket: "b".into(),
                key: "k".into(),
                file: 6,
                owner: 5,
                at_ns: 0,
            }))
        );
        assert_eq!(
            reclaimer.step(Answer::NameOutcome(Outcome::Reclaimed)),
            Err(ReclaimError::Unexpected)
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
            stops in proptest::collection::vec(0usize..12, 0..12),
        ) {
            let mut cell = Cell::new();
            let created = cell.run_name(&Command::CreateUpload(name::CreateUpload {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                at_ns: 1,
                upload: crate::record::Upload {
                    initiated_ns: 0,
                    owner: "o".into(),
                    headers: Vec::new(),
                    checksum: None,
                    retention: None,
                    legal_hold: None,
                },
            }));
            let Outcome::Created { upload } = created else {
                panic!("{created:?}")
            };
            let mut next = 10u128;
            let mut listed = Vec::new();
            let mut part_files = Vec::new();
            for (i, widths) in parts.iter().enumerate() {
                let mut targets = Vec::new();
                for &width in widths {
                    next += 1;
                    cell.block(next, width);
                    targets.push(Target::Block(next));
                }
                next += 1;
                cell.file(next, &targets);
                part_files.push(Target::File(next));
                let number = u16::try_from(i + 1).unwrap();
                let outcome = cell.run_name(&Command::PutPart(name::PutPart {
                    bucket: "b".into(),
                    incarnation: 1,
                    key: "k".into(),
                    upload: upload.clone(),
                    number,
                    part: crate::record::Part {
                        etag: format!("p{number}"),
                        size: name::MIN_PART,
                        checksum: None,
                        file: next,
                        modified_ns: 0,
                    },
                    at_ns: 2,
                    deadline_ns: u64::MAX,
                }));
                prop_assert_eq!(outcome, Outcome::PartWritten);
                listed.push(name::Listed {
                    number,
                    etag: format!("p{number}"),
                    file: next,
                });
            }
            next += 1;
            let root = next;
            cell.file(root, &part_files);
            let completed = cell.run_name(&Command::Complete(name::Complete {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                upload,
                versioning: Versioning::Unversioned,
                preconditions: name::Preconditions::default(),
                at_ns: 3,
                etag: format!("m-{}", listed.len()),
                size: name::MIN_PART * listed.len() as u64,
                parts: listed,
                checksum: None,
                file: Some(root),
                default: None,
                deadline_ns: u64::MAX,
                listing: [0; crate::record::LISTING],
            }));
            let committed = matches!(completed, Outcome::Put { .. });
            prop_assert!(committed, "{:?}", completed);
            cell.run_name(&Command::Delete(Delete {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                versioning: Versioning::Unversioned,
                named: None,
                if_match: None,
                at_ns: 4,
                bypass: false,
                owner: "o".into(),
            }));
            // Each stop takes the oldest file released and drops its reclaimer after that
            // many steps; the next starts again from the queue.
            for &stop in &stops {
                let mut queue = name::released(&cell.name, u64::MAX, 1).unwrap();
                if queue.is_empty() {
                    break;
                }
                let mut reclaimer = Reclaimer::new(queue.remove(0), budget);
                cell.drive(&mut reclaimer, stop);
            }
            reclaim_all(&mut cell);
            prop_assert_eq!(name::released(&cell.name, u64::MAX, 2).unwrap(), vec![]);
            prop_assert!(cell.chunks.is_empty());
            prop_assert_eq!(Cell::rows(&cell.files), 0);
            prop_assert_eq!(Cell::rows(&cell.blocks), 0);
        }
    }
}
