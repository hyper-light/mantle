#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

//! What a gateway made and never handed over (docs/design/metadata.md §2), found by the sweeps
//! (`mantle_meta::sweep`) across a Block range, a File range and two Name ranges, under
//! schedules proptest chooses. A gateway writes a block's chunk to a volume and the block, then
//! the file that names the block, then hands the file to its key's Name range; it may stop for
//! good at any stage, or come late to the next. Deletes release files, both sweeps run a step
//! at a time and stop for good between any two steps, and leaders whose clocks run behind
//! propose entries at times earlier than entries already applied.
//!
//! After every step, no file a version references is released, every file written is
//! referenced, released or unsettled, and every block a written file names still has its rows
//! and its chunk. Once the faults stop and every deadline has passed, the sweeps leave nothing
//! unsettled in either layer; every file is referenced or released and not both; every block
//! left is one a file names, and every chunk left one a block holds; and a mark is left only by
//! a sweep that stopped between settling a file and unmarking it, on a file referenced or
//! released, which its release or reclaiming removes.

use std::collections::BTreeSet;

use mantle_chunk::ChunkKey;
use mantle_meta::block;
use mantle_meta::engine::{Model, Rows};
use mantle_meta::file::{self, Unsettled};
use mantle_meta::key::{self, NameRow};
use mantle_meta::name::{self, Check, GateChange, Preconditions, Put, Unmark, Verdict};
use mantle_meta::record::{
    BlockHeader, ChunkPlace, Extent, GateState, Referrer, Target, Version, Versioning,
};
use mantle_meta::sweep::{Answer, BlockAnswer, BlockRequest, BlockSweep, Request, Sweep};
use proptest::prelude::*;

const BUCKET: &str = "b";
/// Keys writers use: the first two in Name range 0, the others in range 1.
const KEYS: [&str; 4] = ["a", "f", "m", "t"];
/// How long a gateway has to take its next step, in the simulation's clock, which moves 10 a
/// step: five steps.
const HANDOVER: u64 = 50;
const PAGE: usize = 2;

fn range_of(key: &str) -> usize {
    usize::from(key >= "m")
}

#[derive(Debug, Clone)]
enum Action {
    /// A gateway writes a chunk and its block, for a file of a key.
    Write {
        key: usize,
    },
    /// A gateway takes its next step: writes its file, or hands it over.
    Advance(usize),
    /// A gateway stops for good where it is.
    Crash(usize),
    /// A delete of the key's null version or current version.
    Delete {
        key: usize,
        versioned: bool,
    },
    /// One step of the file sweep, starting one if none runs.
    Sweep,
    SweepStops,
    /// One step of the block sweep, starting one if none runs.
    BlockSweep,
    BlockSweepStops,
    /// Time passes: a gateway's deadline goes by.
    Wait,
    /// A new leader, whose clock is `lag` behind, proposes from now on.
    Leader {
        lag: u64,
    },
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        3 => (0usize..4).prop_map(|key| Action::Write { key }),
        4 => (0usize..4).prop_map(Action::Advance),
        1 => (0usize..4).prop_map(Action::Crash),
        1 => (0usize..4, any::<bool>()).prop_map(|(key, versioned)| Action::Delete { key, versioned }),
        3 => Just(Action::Sweep),
        1 => Just(Action::SweepStops),
        3 => Just(Action::BlockSweep),
        1 => Just(Action::BlockSweepStops),
        1 => Just(Action::Wait),
        2 => (0u64..=20).prop_map(|steps| Action::Leader { lag: steps * 10 }),
    ]
}

/// Where a gateway is in writing one object.
enum Stage {
    /// Its block is written, with its deadline; its file is not.
    Block { block: u128, deadline_ns: u64 },
    /// Its file is written, with its deadline; it is not handed over.
    File { deadline_ns: u64 },
}

struct Gateway {
    key: &'static str,
    file: u128,
    versioning: Versioning,
    stage: Stage,
}

struct World {
    blocks: Model,
    block_index: u64,
    files: Model,
    file_index: u64,
    names: [Model; 2],
    name_index: [u64; 2],
    /// Chunks on volumes, as (volume, block, epoch, index).
    volumes: BTreeSet<(u128, u128, u32, u16)>,
    clock: u64,
    /// How far behind the true time the current leader proposes.
    lag: u64,
    next_id: u128,
    gateways: Vec<Gateway>,
    sweep: Option<Sweep>,
    block_sweep: Option<BlockSweep>,
    written: BTreeSet<u128>,
}

impl World {
    fn new() -> Self {
        let mut w = Self {
            blocks: Model::default(),
            block_index: 0,
            files: Model::default(),
            file_index: 0,
            names: [Model::default(), Model::default()],
            name_index: [0, 0],
            volumes: BTreeSet::new(),
            clock: 1_000,
            lag: 0,
            next_id: 1,
            gateways: Vec::new(),
            sweep: None,
            block_sweep: None,
            written: BTreeSet::new(),
        };
        for range in 0..2 {
            let open = name::Command::Gate(GateChange {
                bucket: BUCKET.into(),
                incarnation: 1,
                attempt: 1,
                from: None,
                to: Some(GateState::Open),
            });
            assert_eq!(w.name(range, open), name::Outcome::GateMoved);
        }
        w
    }

    fn name(&mut self, range: usize, command: name::Command) -> name::Outcome {
        self.name_index[range] += 1;
        name::apply(&mut self.names[range], self.name_index[range], &command).unwrap()
    }

    fn file(&mut self, command: file::Command) -> file::Outcome {
        self.file_index += 1;
        file::apply(&mut self.files, self.file_index, &command).unwrap()
    }

    fn block(&mut self, command: block::Command) -> block::Outcome {
        self.block_index += 1;
        block::apply(&mut self.blocks, self.block_index, &command).unwrap()
    }

    /// The time the current leader proposes an entry at.
    fn proposed(&self) -> u64 {
        self.clock - self.lag
    }

    fn id(&mut self) -> u128 {
        self.next_id += 1;
        self.next_id
    }

    fn act(&mut self, action: &Action) {
        self.clock += 10;
        match *action {
            Action::Write { key } => {
                let (file, block) = (self.id(), self.id());
                let place = ChunkPlace {
                    volume: block % 3,
                    key: ChunkKey {
                        block,
                        epoch: 1,
                        index: 0,
                    },
                };
                self.volumes.insert(slot(&place));
                let outcome = self.block(block::Command::Write {
                    block,
                    header: BlockHeader {
                        length: 1,
                        data: 1,
                        parity: 0,
                        chunk_len: 1,
                        crc32c: 0,
                    },
                    chunks: vec![place],
                    file,
                    handover_ns: HANDOVER,
                    at_ns: self.proposed(),
                });
                let block::Outcome::Written { deadline_ns } = outcome else {
                    panic!("{outcome:?}");
                };
                let versioning = if file.is_multiple_of(2) {
                    Versioning::Enabled
                } else {
                    Versioning::Unversioned
                };
                self.gateways.push(Gateway {
                    key: KEYS[key],
                    file,
                    versioning,
                    stage: Stage::Block { block, deadline_ns },
                });
            }
            Action::Advance(i) => {
                if !self.gateways.is_empty() {
                    let g = self.gateways.swap_remove(i % self.gateways.len());
                    self.advance(g);
                }
            }
            Action::Crash(i) => {
                if !self.gateways.is_empty() {
                    self.gateways.swap_remove(i % self.gateways.len());
                }
            }
            Action::Delete { key, versioned } => {
                let key = KEYS[key];
                let delete = name::Command::Delete(name::Delete {
                    bucket: BUCKET.into(),
                    incarnation: 1,
                    key: key.into(),
                    versioning: if versioned {
                        Versioning::Suspended
                    } else {
                        Versioning::Unversioned
                    },
                    named: None,
                    if_match: None,
                    at_ns: self.proposed(),
                    bypass: false,
                });
                self.name(range_of(key), delete);
            }
            Action::Sweep => self.sweep_step(),
            Action::SweepStops => self.sweep = None,
            Action::BlockSweep => self.block_sweep_step(),
            Action::BlockSweepStops => self.block_sweep = None,
            Action::Wait => self.clock += HANDOVER,
            Action::Leader { lag } => self.lag = lag,
        }
    }

    /// The gateway's next step: its file, or its handover.
    fn advance(&mut self, mut g: Gateway) {
        match g.stage {
            Stage::Block { block, deadline_ns } => {
                // The write takes the range's next instant.
                let late = self.proposed().max(last_instant(&self.files)) > deadline_ns;
                let outcome = self.file(file::Command::Write {
                    file: g.file,
                    extents: vec![Extent {
                        length: 1,
                        target: Target::Block(block),
                    }],
                    referrer: Referrer {
                        bucket: BUCKET.into(),
                        incarnation: 1,
                        key: g.key.into(),
                    },
                    handover_ns: HANDOVER,
                    blocks_deadline_ns: deadline_ns,
                    at_ns: self.proposed(),
                });
                match outcome {
                    file::Outcome::Written { deadline_ns } => {
                        assert!(!late, "a file named a block past its deadline");
                        self.written.insert(g.file);
                        g.stage = Stage::File { deadline_ns };
                        self.gateways.push(g);
                    }
                    // The gateway would make the block again; here it gives up.
                    file::Outcome::Expired => assert!(late, "a file in time was refused"),
                    other => panic!("{other:?}"),
                }
            }
            Stage::File { deadline_ns } => self.handover(&g, deadline_ns),
        }
    }

    fn handover(&mut self, g: &Gateway, deadline_ns: u64) {
        let put = name::Command::Put(Put {
            bucket: BUCKET.into(),
            incarnation: 1,
            key: g.key.into(),
            versioning: g.versioning,
            preconditions: Preconditions::default(),
            at_ns: self.proposed(),
            ordered_ns: None,
            version: Version {
                marker: false,
                null: false,
                modified_ns: 0,
                etag: "e".into(),
                size: 1,
                checksum: None,
                file: Some(g.file),
                owner: "o".into(),
                headers: Vec::new(),
                retention: None,
                legal_hold: None,
            },
            default: None,
            deadline_ns,
        });
        let range = range_of(g.key);
        let late =
            mantle_meta::clock::now(&self.names[range], self.proposed()).unwrap() > deadline_ns;
        match self.name(range, put) {
            name::Outcome::Put { .. } => assert!(!late, "a late handover was taken"),
            name::Outcome::Expired => assert!(late, "a handover in time was refused"),
            other => panic!("{other:?}"),
        }
    }

    fn sweep_step(&mut self) {
        let sweep = self.sweep.get_or_insert_with(|| Sweep::new(PAGE));
        let Some(request) = sweep.next() else {
            self.sweep = None;
            return;
        };
        let answer = self.serve(request);
        let sweep = self.sweep.as_mut().unwrap();
        sweep.answer(answer).unwrap();
        if sweep.is_done() {
            self.sweep = None;
        }
    }

    fn block_sweep_step(&mut self) {
        let sweep = self
            .block_sweep
            .get_or_insert_with(|| BlockSweep::new(PAGE));
        let Some(request) = sweep.next() else {
            self.block_sweep = None;
            return;
        };
        let answer = self.serve_blocks(request);
        let sweep = self.block_sweep.as_mut().unwrap();
        sweep.answer(answer).unwrap();
        if sweep.is_done() {
            self.block_sweep = None;
        }
    }

    /// What the File and Name ranges answer a file sweep's request.
    fn serve(&mut self, request: Request) -> Answer {
        match request {
            Request::Due { max } => {
                let now = mantle_meta::clock::now(&self.files, self.proposed()).unwrap();
                Answer::Due(file::unsettled(&self.files, now, max).unwrap())
            }
            Request::Check(files) => {
                let mut verdicts = vec![Verdict::Young; files.len()];
                for range in 0..2 {
                    let mine: Vec<(usize, &Unsettled)> = files
                        .iter()
                        .enumerate()
                        .filter(|(_, u)| range_of(&u.referrer.key) == range)
                        .collect();
                    if mine.is_empty() {
                        continue;
                    }
                    let check = name::Command::Check(Check {
                        files: mine.iter().map(|(_, u)| (u.file, u.deadline_ns)).collect(),
                        at_ns: self.proposed(),
                    });
                    let name::Outcome::Checked(answered) = self.name(range, check) else {
                        panic!("a check answered otherwise");
                    };
                    for ((i, _), v) in mine.iter().zip(answered) {
                        verdicts[*i] = v;
                    }
                }
                Answer::Checked(verdicts)
            }
            Request::Settle(files) => {
                assert_eq!(
                    self.file(file::Command::Settle { files }),
                    file::Outcome::Settled
                );
                Answer::Settled
            }
            Request::Unmark(files) => {
                for range in 0..2 {
                    let mine: Vec<u128> = files
                        .iter()
                        .filter(|(r, _)| range_of(&r.key) == range)
                        .map(|(_, f)| *f)
                        .collect();
                    if !mine.is_empty() {
                        let unmark = name::Command::Unmark(Unmark { files: mine });
                        assert_eq!(self.name(range, unmark), name::Outcome::Unmarked);
                    }
                }
                Answer::Unmarked
            }
        }
    }

    /// What the Block and File ranges and the volumes answer a block sweep's request.
    fn serve_blocks(&mut self, request: BlockRequest) -> BlockAnswer {
        match request {
            BlockRequest::Due { max } => {
                let now = mantle_meta::clock::now(&self.blocks, self.proposed()).unwrap();
                BlockAnswer::Due(block::unsettled(&self.blocks, now, max).unwrap())
            }
            BlockRequest::Check { file, blocks } => {
                let check = file::Command::CheckBlocks {
                    file,
                    blocks,
                    at_ns: self.proposed(),
                };
                let file::Outcome::BlocksChecked(verdicts) = self.file(check) else {
                    panic!("a check answered otherwise");
                };
                BlockAnswer::Checked(verdicts)
            }
            BlockRequest::Settle(blocks) => {
                assert_eq!(
                    self.block(block::Command::Settle { blocks }),
                    block::Outcome::Settled
                );
                BlockAnswer::Settled
            }
            BlockRequest::Block(b) => BlockAnswer::Block(block::read(&self.blocks, b).unwrap()),
            BlockRequest::Chunk(place) => {
                self.volumes.remove(&slot(&place));
                BlockAnswer::Chunk
            }
            BlockRequest::Delete(b) => {
                assert_eq!(
                    self.block(block::Command::Delete { block: b }),
                    block::Outcome::Deleted
                );
                BlockAnswer::Deleted
            }
        }
    }

    /// Files a version references, in every Name range.
    fn referenced(&self) -> BTreeSet<u128> {
        let mut out = BTreeSet::new();
        for range in &self.names {
            let (mut from, to) = key::bucket_span(BUCKET);
            while let Some((k, v)) = range.next(&from, &to).unwrap() {
                if let Some((_, _, NameRow::Version(_))) = key::decode_name(&k) {
                    out.extend(Version::decode(&v).unwrap().file);
                }
                from = k;
                from.push(0);
            }
        }
        out
    }

    fn released(&self) -> BTreeSet<u128> {
        self.names
            .iter()
            .flat_map(|r| name::released(r, u64::MAX, usize::MAX).unwrap())
            .map(|(_, file)| file)
            .collect()
    }

    fn unsettled(&self) -> BTreeSet<u128> {
        file::unsettled(&self.files, u64::MAX, usize::MAX)
            .unwrap()
            .into_iter()
            .map(|u| u.file)
            .collect()
    }

    fn unsettled_blocks(&self) -> Vec<block::Unsettled> {
        block::unsettled(&self.blocks, u64::MAX, usize::MAX).unwrap()
    }

    /// Blocks the written files name.
    fn named(&self) -> BTreeSet<u128> {
        self.written
            .iter()
            .flat_map(|&f| file::extents(&self.files, f, 0, 16).unwrap())
            .filter_map(|(_, e)| match e.target {
                Target::Block(b) => Some(b),
                Target::File(_) => None,
            })
            .collect()
    }

    /// Blocks with rows in the Block range.
    fn recorded(&self) -> BTreeSet<u128> {
        (0..=self.next_id)
            .filter(|&b| block::read(&self.blocks, b).unwrap().is_some())
            .collect()
    }

    fn marked(&self) -> BTreeSet<u128> {
        self.written
            .iter()
            .copied()
            .filter(|&f| {
                self.names
                    .iter()
                    .any(|r| r.get(&key::handed(f)).unwrap().is_some())
            })
            .collect()
    }

    fn check(&self) {
        let (referenced, released, unsettled) =
            (self.referenced(), self.released(), self.unsettled());
        let lost: Vec<_> = referenced.intersection(&released).collect();
        assert!(lost.is_empty(), "files referenced and released: {lost:?}");
        for f in &self.written {
            assert!(
                referenced.contains(f) || released.contains(f) || unsettled.contains(f),
                "file {f} is nowhere"
            );
        }
        let recorded = self.recorded();
        for b in self.named() {
            assert!(recorded.contains(&b), "block {b} of a file was taken apart");
            assert!(
                self.volumes.iter().any(|&(_, block, _, _)| block == b),
                "the chunk of block {b} of a file was deleted"
            );
        }
    }

    /// The faults stop: every gateway still in flight takes its steps, every deadline passes,
    /// and both sweeps run until nothing is unsettled.
    fn finish(&mut self) {
        self.lag = 0;
        while let Some(g) = self.gateways.pop() {
            self.clock += 10;
            self.advance(g);
            self.check();
        }
        self.clock += 10 * HANDOVER;
        for _ in 0..10_000 {
            let quiet = self.unsettled().is_empty()
                && self.sweep.is_none()
                && self.unsettled_blocks().is_empty()
                && self.block_sweep.is_none();
            if quiet {
                break;
            }
            self.clock += 10;
            self.sweep_step();
            self.block_sweep_step();
            self.check();
        }
        assert!(self.unsettled().is_empty(), "files left unsettled");
        assert!(self.unsettled_blocks().is_empty(), "blocks left unsettled");
        let (referenced, released) = (self.referenced(), self.released());
        for f in &self.written {
            assert!(
                referenced.contains(f) != released.contains(f),
                "file {f}: referenced {}, released {}",
                referenced.contains(f),
                released.contains(f)
            );
        }
        let stray: Vec<u128> = self
            .marked()
            .into_iter()
            .filter(|f| !referenced.contains(f) && !released.contains(f))
            .collect();
        assert!(
            stray.is_empty(),
            "marks on files neither referenced nor released: {stray:?}"
        );
        let (named, recorded) = (self.named(), self.recorded());
        let leaked: Vec<_> = recorded.difference(&named).collect();
        assert!(leaked.is_empty(), "blocks no file names: {leaked:?}");
        let chunks: Vec<_> = self
            .volumes
            .iter()
            .filter(|(_, block, _, _)| !recorded.contains(block))
            .collect();
        assert!(chunks.is_empty(), "chunks of no block: {chunks:?}");
    }
}

/// Where a chunk is, as a volume holds it.
fn slot(p: &ChunkPlace) -> (u128, u128, u32, u16) {
    (p.volume, p.key.block, p.key.epoch, p.key.index)
}

/// The last instant a range's clock assigned: a write takes the one after it.
fn last_instant(rows: &Model) -> u64 {
    mantle_meta::clock::now(rows, 0).unwrap() + 1
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn what_a_gateway_never_handed_over_is_released_and_nothing_handed_over_is(
        actions in prop::collection::vec(action(), 1..120),
    ) {
        let mut world = World::new();
        for action in &actions {
            world.act(action);
            world.check();
        }
        world.finish();
    }
}

/// The cases the sweeps exist for, spelled out: a gateway writes a block and its file and
/// stops; another writes only a block and stops. Once their deadlines pass, the file is
/// released and the lone block taken apart, and a gateway that comes a step late is refused.
#[test]
fn what_stopped_gateways_left_is_released_and_late_steps_refused() {
    let mut w = World::new();
    w.act(&Action::Write { key: 0 });
    w.act(&Action::Advance(0));
    w.act(&Action::Write { key: 2 });
    let late = w.gateways.pop().unwrap();
    w.act(&Action::Crash(0));
    // Before the deadlines the sweeps find nothing due.
    w.act(&Action::Sweep);
    w.act(&Action::BlockSweep);
    assert!(w.sweep.is_none() && w.block_sweep.is_none());
    w.act(&Action::Wait);
    w.act(&Action::Wait);
    for _ in 0..12 {
        w.act(&Action::Sweep);
        w.act(&Action::BlockSweep);
        w.check();
    }
    assert_eq!(w.released(), BTreeSet::from([2]));
    assert!(w.unsettled().is_empty() && w.unsettled_blocks().is_empty());
    // The lone block's chunk and rows are gone; the released file's block stays for the
    // reclaimer.
    assert_eq!(w.recorded(), BTreeSet::from([3]));
    // The gateway that wrote only a block comes to write its file, past the block's deadline.
    w.advance(late);
    w.check();
    w.finish();
}
