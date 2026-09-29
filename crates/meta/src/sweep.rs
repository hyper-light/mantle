//! The sweep for files a gateway made and never handed over (docs/design/metadata.md §2).
//!
//! Every file waits in its File range's queue of unsettled files from its write until the
//! sweep settles it, as HDFS allocates a block before it is written and RocksDB registers a
//! job's outputs while it runs (docs/research/22 §3.1, §7.3). Once a file's handover deadline
//! has passed, the sweep asks the Name range its key routes to whether it took the file. The
//! range answers from its own mark, or, finding none, releases the file into its queue there
//! and then: its time only moves forward, so a handover that comes after finds the deadline
//! passed and is refused, and the check and the handover are ordered by the range's log, as
//! Giza's no-op and a stalled put contend for one Paxos slot (22 §7.4). The sweep then settles
//! the file in the File range. It leaves the range's mark: a mark removed on settling would
//! read, to another sweep that read the same file and asks after, as a file never handed over,
//! and that sweep would release a file a version references (audit B01). A mark goes only with
//! the file, when the collector reclaims it.
//!
//! A [`Sweep`] names its next [`Request`] and moves on with the [`Answer`], doing no I/O, as
//! the coordinator and the reclaimer do. Every step can be repeated.
//!
//! One layer down, a [`BlockSweep`] does the same for a Block range's blocks, made each for one
//! file. A file is written once, whole, so the File range answers from the file itself: a file
//! written names what it ever will, and one not written once the block's deadline has passed at
//! the File range's time never will, its write being refused past that deadline. The gateway
//! renews its blocks' deadlines while the body they hold streams in, so the sweep first
//! releases the block in the Block range at the deadline the File range judged: a renewal
//! since keeps it, and a block released is renewed no more. A released block is taken apart
//! there and then, chunks first: no reference ever reached it, so there is no deletion by
//! mistake for a grace period to undo.

use std::collections::{BTreeMap, VecDeque};

use crate::block;
use crate::file::Unsettled;
use crate::record::{BlockHeader, ChunkPlace, Verdict};

/// A sweep's next request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// A read of at most `max` of the File range's unsettled files whose deadline has passed at
    /// its time ([`crate::file::unsettled`]).
    Due { max: usize },
    /// `name::Command::Check` of each file, sent to the Name range its referrer's key falls
    /// in, with the verdicts answered in the order asked.
    Check(Vec<Unsettled>),
    /// `file::Command::Settle` for these files.
    Settle(Vec<u128>),
}

/// What a request answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Due(Vec<Unsettled>),
    Checked(Vec<Verdict>),
    Settled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SweepError {
    #[error("an answer of another kind than the request")]
    Mismatch,
    #[error("a verdict for each file asked about, and no other")]
    Count,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    Read,
    Check(Vec<Unsettled>),
    Settle(Vec<u128>),
    Done,
}

/// One pass over a File range's unsettled files, a page at a time. A pass ends when a page is
/// empty, or holds only files whose Name range's time has not yet passed their deadline; the
/// driver runs another once the soonest of those comes due.
#[derive(Debug, Clone)]
pub struct Sweep {
    /// Files one page holds, which bounds each request's entry.
    page: usize,
    phase: Phase,
}

impl Sweep {
    pub fn new(page: usize) -> Self {
        Self {
            page: page.max(1),
            phase: Phase::Read,
        }
    }

    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    /// The request to send next; `None` once the pass is done.
    pub fn next(&self) -> Option<Request> {
        Some(match &self.phase {
            Phase::Read => Request::Due { max: self.page },
            Phase::Check(files) => Request::Check(files.clone()),
            Phase::Settle(decided) => Request::Settle(decided.clone()),
            Phase::Done => return None,
        })
    }

    /// Takes the answer to the request [`next`](Self::next) named, and moves on. An answer that
    /// does not fit the request is refused, and the sweep stays where it was.
    pub fn answer(&mut self, answer: Answer) -> Result<(), SweepError> {
        let next = match (&self.phase, answer) {
            (Phase::Read, Answer::Due(files)) if files.is_empty() => Phase::Done,
            (Phase::Read, Answer::Due(files)) => Phase::Check(files),
            (Phase::Check(files), Answer::Checked(verdicts)) => {
                if verdicts.len() != files.len() {
                    return Err(SweepError::Count);
                }
                let decided: Vec<u128> = files
                    .iter()
                    .zip(&verdicts)
                    .filter(|(_, v)| **v != Verdict::Young)
                    .map(|(u, _)| u.file)
                    .collect();
                if decided.is_empty() {
                    Phase::Done
                } else {
                    Phase::Settle(decided)
                }
            }
            (Phase::Settle(_), Answer::Settled) => Phase::Read,
            _ => return Err(SweepError::Mismatch),
        };
        self.phase = next;
        Ok(())
    }
}

/// A block sweep's next request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockRequest {
    /// A read of at most `max` of the Block range's unsettled blocks whose deadline has passed
    /// at its time ([`crate::block::unsettled`]).
    Due { max: usize },
    /// `file::Command::CheckBlocks` for blocks made for `file`, sent to the File range that
    /// holds it, with the verdicts answered in the order asked.
    Check {
        file: u128,
        blocks: Vec<(u128, u64)>,
    },
    /// `block::Command::Settle` for blocks their files name.
    Settle(Vec<u128>),
    /// `block::Command::Release` of a block no file will name, at the deadline the File range
    /// judged it by.
    Release { block: u128, deadline_ns: u64 },
    /// A read of a released block, for its chunks ([`crate::block::read`]).
    Block(u128),
    /// Deleting a chunk from its volume, on the node that holds the volume.
    Chunk(ChunkPlace),
    /// `block::Command::Delete`: the block's rows, and its place in the queue, last.
    Delete(u128),
}

/// What a block sweep's request answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockAnswer {
    Due(Vec<block::Unsettled>),
    Checked(Vec<Verdict>),
    Settled,
    /// Whether the block is released: not if its writer renewed it since the sweep read it,
    /// or it is gone.
    Released(bool),
    Block(Option<(BlockHeader, Vec<ChunkPlace>)>),
    /// The chunk is gone from its volume, deleted now or before.
    Chunk,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BlockPhase {
    Read,
    Check {
        /// Blocks of each file still to ask about.
        groups: VecDeque<(u128, Vec<(u128, u64)>)>,
        held: Vec<u128>,
        orphans: VecDeque<(u128, u64)>,
    },
    Settle {
        held: Vec<u128>,
        orphans: VecDeque<(u128, u64)>,
    },
    /// Taking the front orphan apart, with the deadline the File range judged it by.
    Reclaim {
        orphans: VecDeque<(u128, u64)>,
        take: Take,
    },
    Done,
}

/// How far taking an orphan apart has got.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Take {
    Release,
    /// Released: its chunks are read next.
    Read,
    /// Its chunks still to delete, then its rows.
    Chunks(VecDeque<ChunkPlace>),
}

/// One pass over a Block range's unsettled blocks, a page at a time, ending as a [`Sweep`]'s
/// does.
#[derive(Debug, Clone)]
pub struct BlockSweep {
    page: usize,
    phase: BlockPhase,
}

impl BlockSweep {
    pub fn new(page: usize) -> Self {
        Self {
            page: page.max(1),
            phase: BlockPhase::Read,
        }
    }

    pub fn is_done(&self) -> bool {
        self.phase == BlockPhase::Done
    }

    /// The request to send next; `None` once the pass is done.
    pub fn next(&self) -> Option<BlockRequest> {
        Some(match &self.phase {
            BlockPhase::Read => BlockRequest::Due { max: self.page },
            BlockPhase::Check { groups, .. } => {
                let (file, blocks) = groups.front()?;
                BlockRequest::Check {
                    file: *file,
                    blocks: blocks.clone(),
                }
            }
            BlockPhase::Settle { held, .. } => BlockRequest::Settle(held.clone()),
            BlockPhase::Reclaim { orphans, take } => {
                let &(block, deadline_ns) = orphans.front()?;
                match take {
                    Take::Release => BlockRequest::Release { block, deadline_ns },
                    Take::Read => BlockRequest::Block(block),
                    Take::Chunks(left) => match left.front() {
                        Some(place) => BlockRequest::Chunk(*place),
                        None => BlockRequest::Delete(block),
                    },
                }
            }
            BlockPhase::Done => return None,
        })
    }

    /// Takes the answer to the request [`next`](Self::next) named, and moves on. An answer that
    /// does not fit the request is refused, and the sweep stays where it was.
    pub fn answer(&mut self, answer: BlockAnswer) -> Result<(), SweepError> {
        let phase = std::mem::replace(&mut self.phase, BlockPhase::Done);
        self.phase = match (phase, answer) {
            (BlockPhase::Read, BlockAnswer::Due(due)) if due.is_empty() => BlockPhase::Done,
            (BlockPhase::Read, BlockAnswer::Due(due)) => {
                let mut by_file: BTreeMap<u128, Vec<(u128, u64)>> = BTreeMap::new();
                for u in due {
                    by_file
                        .entry(u.file)
                        .or_default()
                        .push((u.block, u.deadline_ns));
                }
                BlockPhase::Check {
                    groups: by_file.into_iter().collect(),
                    held: Vec::new(),
                    orphans: VecDeque::new(),
                }
            }
            (
                BlockPhase::Check {
                    mut groups,
                    mut held,
                    mut orphans,
                },
                BlockAnswer::Checked(verdicts),
            ) => {
                // An answer that does not fit leaves the request where it was, to be asked
                // again.
                let Some((file, blocks)) = groups.pop_front() else {
                    self.phase = BlockPhase::Check {
                        groups,
                        held,
                        orphans,
                    };
                    return Err(SweepError::Mismatch);
                };
                if verdicts.len() != blocks.len() {
                    groups.push_front((file, blocks));
                    self.phase = BlockPhase::Check {
                        groups,
                        held,
                        orphans,
                    };
                    return Err(SweepError::Count);
                }
                for (&(b, deadline_ns), v) in blocks.iter().zip(&verdicts) {
                    match v {
                        Verdict::Held => held.push(b),
                        Verdict::Released => orphans.push_back((b, deadline_ns)),
                        Verdict::Young => {}
                    }
                }
                if !groups.is_empty() {
                    BlockPhase::Check {
                        groups,
                        held,
                        orphans,
                    }
                } else if !held.is_empty() {
                    BlockPhase::Settle { held, orphans }
                } else if !orphans.is_empty() {
                    BlockPhase::Reclaim {
                        orphans,
                        take: Take::Release,
                    }
                } else {
                    // Every block of the page is young: the pass ends here.
                    BlockPhase::Done
                }
            }
            (BlockPhase::Settle { orphans, .. }, BlockAnswer::Settled) => next_orphan(orphans),
            (
                BlockPhase::Reclaim {
                    mut orphans,
                    take: Take::Release,
                },
                BlockAnswer::Released(released),
            ) => {
                if released {
                    BlockPhase::Reclaim {
                        orphans,
                        take: Take::Read,
                    }
                } else {
                    orphans.pop_front();
                    next_orphan(orphans)
                }
            }
            (
                BlockPhase::Reclaim {
                    mut orphans,
                    take: Take::Read,
                },
                BlockAnswer::Block(found),
            ) => match found {
                Some((_, places)) => BlockPhase::Reclaim {
                    orphans,
                    take: Take::Chunks(places.into()),
                },
                // Gone already: a pass that stopped part way deleted it.
                None => {
                    orphans.pop_front();
                    next_orphan(orphans)
                }
            },
            (
                BlockPhase::Reclaim {
                    orphans,
                    take: Take::Chunks(mut left),
                },
                BlockAnswer::Chunk,
            ) if !left.is_empty() => {
                left.pop_front();
                BlockPhase::Reclaim {
                    orphans,
                    take: Take::Chunks(left),
                }
            }
            (
                BlockPhase::Reclaim {
                    mut orphans,
                    take: Take::Chunks(left),
                },
                BlockAnswer::Deleted,
            ) if left.is_empty() => {
                orphans.pop_front();
                next_orphan(orphans)
            }
            (phase, _) => {
                self.phase = phase;
                return Err(SweepError::Mismatch);
            }
        };
        Ok(())
    }
}

/// The next orphan to take apart, or the next page once none is left.
fn next_orphan(orphans: VecDeque<(u128, u64)>) -> BlockPhase {
    if orphans.is_empty() {
        BlockPhase::Read
    } else {
        BlockPhase::Reclaim {
            orphans,
            take: Take::Release,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Referrer;

    fn unsettled(file: u128) -> Unsettled {
        Unsettled {
            file,
            deadline_ns: 10,
            referrer: Referrer {
                bucket: "b".into(),
                incarnation: 1,
                key: format!("k{file}"),
            },
        }
    }

    #[test]
    fn a_page_is_checked_then_settled() {
        let mut s = Sweep::new(2);
        assert_eq!(s.next(), Some(Request::Due { max: 2 }));
        s.answer(Answer::Due(vec![unsettled(1), unsettled(2)]))
            .unwrap();
        assert_eq!(
            s.next(),
            Some(Request::Check(vec![unsettled(1), unsettled(2)]))
        );
        assert_eq!(
            s.answer(Answer::Checked(vec![Verdict::Held])),
            Err(SweepError::Count)
        );
        s.answer(Answer::Checked(vec![Verdict::Young, Verdict::Released]))
            .unwrap();
        // Only the file decided is settled; the young one waits.
        assert_eq!(s.next(), Some(Request::Settle(vec![2])));
        assert_eq!(
            s.answer(Answer::Checked(Vec::new())),
            Err(SweepError::Mismatch)
        );
        s.answer(Answer::Settled).unwrap();
        // The next page holds only the young file: the pass ends there.
        assert_eq!(s.next(), Some(Request::Due { max: 2 }));
        s.answer(Answer::Due(vec![unsettled(1)])).unwrap();
        s.answer(Answer::Checked(vec![Verdict::Young])).unwrap();
        assert!(s.is_done());
        assert_eq!(s.next(), None);
        let mut empty = Sweep::new(0);
        assert_eq!(empty.next(), Some(Request::Due { max: 1 }));
        empty.answer(Answer::Due(Vec::new())).unwrap();
        assert!(empty.is_done());
    }

    fn due(block: u128, file: u128) -> block::Unsettled {
        block::Unsettled {
            block,
            deadline_ns: 10,
            file,
        }
    }

    fn place(block: u128, volume: u128) -> ChunkPlace {
        ChunkPlace {
            volume,
            key: mantle_chunk::ChunkKey {
                block,
                epoch: 1,
                index: 0,
            },
        }
    }

    /// A page's blocks are asked about file by file; those named are settled, and those no
    /// file will name are taken apart, chunks first and their rows last.
    #[test]
    fn blocks_are_checked_by_file_then_settled_or_taken_apart() {
        let mut s = BlockSweep::new(8);
        s.answer(BlockAnswer::Due(vec![due(1, 7), due(2, 8), due(3, 7)]))
            .unwrap();
        assert_eq!(
            s.next(),
            Some(BlockRequest::Check {
                file: 7,
                blocks: vec![(1, 10), (3, 10)]
            })
        );
        s.answer(BlockAnswer::Checked(vec![Verdict::Held, Verdict::Released]))
            .unwrap();
        assert_eq!(
            s.next(),
            Some(BlockRequest::Check {
                file: 8,
                blocks: vec![(2, 10)]
            })
        );
        s.answer(BlockAnswer::Checked(vec![Verdict::Young]))
            .unwrap();
        assert_eq!(s.next(), Some(BlockRequest::Settle(vec![1])));
        s.answer(BlockAnswer::Settled).unwrap();
        assert_eq!(
            s.next(),
            Some(BlockRequest::Release {
                block: 3,
                deadline_ns: 10
            })
        );
        s.answer(BlockAnswer::Released(true)).unwrap();
        assert_eq!(s.next(), Some(BlockRequest::Block(3)));
        let header = BlockHeader {
            length: 1,
            data: 1,
            parity: 1,
            chunk_len: 1,
            crc32c: 0,
        };
        s.answer(BlockAnswer::Block(Some((
            header,
            vec![place(3, 20), place(3, 21)],
        ))))
        .unwrap();
        assert_eq!(s.next(), Some(BlockRequest::Chunk(place(3, 20))));
        assert_eq!(s.answer(BlockAnswer::Deleted), Err(SweepError::Mismatch));
        s.answer(BlockAnswer::Chunk).unwrap();
        s.answer(BlockAnswer::Chunk).unwrap();
        assert_eq!(s.next(), Some(BlockRequest::Delete(3)));
        s.answer(BlockAnswer::Deleted).unwrap();
        // The next page holds only the young block: the pass ends.
        assert_eq!(s.next(), Some(BlockRequest::Due { max: 8 }));
        s.answer(BlockAnswer::Due(vec![due(2, 8)])).unwrap();
        s.answer(BlockAnswer::Checked(vec![Verdict::Young]))
            .unwrap();
        assert!(s.is_done());
        // An orphan a stopped pass already deleted reads as gone.
        let mut again = BlockSweep::new(8);
        again.answer(BlockAnswer::Due(vec![due(3, 7)])).unwrap();
        again
            .answer(BlockAnswer::Checked(vec![Verdict::Released]))
            .unwrap();
        again.answer(BlockAnswer::Released(true)).unwrap();
        assert_eq!(again.next(), Some(BlockRequest::Block(3)));
        again.answer(BlockAnswer::Block(None)).unwrap();
        assert_eq!(again.next(), Some(BlockRequest::Due { max: 8 }));
        // A verdict for too few blocks, or an answer of another kind, is refused, and the
        // sweep asks the same again.
        let mut refused = BlockSweep::new(8);
        refused
            .answer(BlockAnswer::Due(vec![due(3, 7), due(4, 7)]))
            .unwrap();
        let asked = refused.next();
        assert_eq!(
            refused.answer(BlockAnswer::Checked(vec![Verdict::Released])),
            Err(SweepError::Count)
        );
        assert_eq!(
            refused.answer(BlockAnswer::Settled),
            Err(SweepError::Mismatch)
        );
        assert_eq!(refused.next(), asked);
        refused
            .answer(BlockAnswer::Checked(vec![Verdict::Held, Verdict::Held]))
            .unwrap();
        assert_eq!(refused.next(), Some(BlockRequest::Settle(vec![3, 4])));
        // An orphan its writer renewed after the sweep read it is kept, and the pass goes on.
        let mut renewed = BlockSweep::new(8);
        renewed
            .answer(BlockAnswer::Due(vec![due(3, 7), due(4, 7)]))
            .unwrap();
        renewed
            .answer(BlockAnswer::Checked(vec![
                Verdict::Released,
                Verdict::Released,
            ]))
            .unwrap();
        assert_eq!(
            renewed.answer(BlockAnswer::Chunk),
            Err(SweepError::Mismatch)
        );
        renewed.answer(BlockAnswer::Released(false)).unwrap();
        assert_eq!(
            renewed.next(),
            Some(BlockRequest::Release {
                block: 4,
                deadline_ns: 10
            })
        );
    }
}
