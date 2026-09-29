#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

//! What a gateway made and never handed over (docs/design/metadata.md §2), found by the sweeps
//! (`mantle_meta::sweep`) across a Block range, a File range and Name ranges that split while
//! it happens, under schedules proptest chooses. A gateway writes a block's chunk to a volume
//! and the block, then the file that names the block, then hands the file to its key's Name
//! range; it may stop for good at any stage, or come late to the next. Deletes release files,
//! both sweeps run a step at a time and stop for good between any two steps, leaders whose
//! clocks run behind propose entries at times earlier than entries already applied, and Name
//! ranges split, their marks going with their keys, while the gateways and the sweep route by
//! descriptors they learn of late.
//!
//! After every step, no file a version references is released, every file written is
//! referenced, released or unsettled, and every block a written file names still has its rows
//! and its chunk. Once the faults stop and every deadline has passed, the sweeps leave nothing
//! unsettled in either layer; every file is referenced or released and not both; every block
//! left is one a file names, and every chunk left one a block holds; and a mark is left only by
//! a sweep that stopped between settling a file and unmarking it, on a file referenced or
//! released, which its release or reclaiming removes.

use std::collections::{BTreeMap, BTreeSet};

use mantle_chunk::ChunkKey;
use mantle_meta::block;
use mantle_meta::engine::{Engine, Model, Rows};
use mantle_meta::file::{self, Unsettled};
use mantle_meta::key::{self, NameRow};
use mantle_meta::name::{
    self, Check, Checked, GateChange, Marked, Preconditions, Put, Split, Unmark, Verdict,
};
use mantle_meta::record::{
    BlockHeader, ChunkPlace, Descriptor, Extent, GateState, Lineage, Referrer, Target, Version,
    Versioning,
};
use mantle_meta::sweep::{Answer, BlockAnswer, BlockRequest, BlockSweep, Request, Sweep};
use proptest::prelude::*;

const BUCKET: &str = "b";
/// Keys writers use.
const KEYS: [&str; 4] = ["a", "f", "m", "t"];
/// Where Name ranges may split: at and between the keys, and at the bucket's edges.
const CUTS: [(&str, &str); 7] = [
    (BUCKET, ""),
    (BUCKET, "c"),
    (BUCKET, "f"),
    (BUCKET, "m"),
    (BUCKET, "p"),
    (BUCKET, "t"),
    ("c", ""),
];
/// How long a gateway has to take its next step, in the simulation's clock, which moves 10 a
/// step: five steps.
const HANDOVER: u64 = 50;
const PAGE: usize = 2;
/// Times a command is sent before its sender's descriptors lead it to the range that holds
/// its key: each refusal names a newer descriptor, and the directory, current here, ends it.
const ROUTES: usize = 8;

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
    /// A Name range splits at one of the cuts, if the cut falls inside it.
    Split {
        range: usize,
        cut: usize,
    },
    /// The gateways and the sweep take the directory's descriptors.
    Refresh,
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
        1 => (0usize..8, 0..CUTS.len()).prop_map(|(range, cut)| Action::Split { range, cut }),
        1 => Just(Action::Refresh),
    ]
}

/// A Name range: its rows, and the last entry its log applied.
struct Range {
    engine: Model,
    index: u64,
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
    /// Every Name range, by ID.
    names: BTreeMap<u64, Range>,
    /// The ID the next split gives its child.
    next_range: u64,
    /// The descriptors the gateways and the sweep route by.
    cache: Vec<Descriptor>,
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
    /// A cell whose first Name range opened the bucket's gate and split at "m", the gateways
    /// knowing both halves.
    fn new() -> Self {
        let mut first = Model::default();
        first.install(0, name::first(1).unwrap()).unwrap();
        let mut w = Self {
            blocks: Model::default(),
            block_index: 0,
            files: Model::default(),
            file_index: 0,
            names: BTreeMap::from([(
                1,
                Range {
                    engine: first,
                    index: 0,
                },
            )]),
            next_range: 2,
            cache: Vec::new(),
            volumes: BTreeSet::new(),
            clock: 1_000,
            lag: 0,
            next_id: 1,
            gateways: Vec::new(),
            sweep: None,
            block_sweep: None,
            written: BTreeSet::new(),
        };
        let open = name::Command::Gate(GateChange {
            bucket: BUCKET.into(),
            incarnation: 1,
            attempt: 1,
            from: None,
            to: Some(GateState::Open),
            generation: 1,
        });
        assert_eq!(w.name(1, open), name::Outcome::GateMoved);
        w.split(1, key::route(BUCKET, "m"));
        w.act(&Action::Refresh);
        w
    }

    fn name(&mut self, id: u64, command: name::Command) -> name::Outcome {
        let range = self.names.get_mut(&id).unwrap();
        range.index += 1;
        name::apply(&mut range.engine, range.index, &command).unwrap()
    }

    fn lineage(&self, id: u64) -> Lineage {
        name::lineage(&self.names[&id].engine).unwrap()
    }

    /// Every Name range's descriptor now: what the directory holds, current here.
    fn directory(&self) -> Vec<Descriptor> {
        self.names.keys().map(|&id| self.lineage(id).now).collect()
    }

    /// Splits range `id` at `at` as its replicas would: the child starts from the rows the
    /// split names, taken from the parent as they stand, and then the parent applies the split.
    fn split(&mut self, id: u64, at: Vec<u8>) {
        let parent = &self.names[&id].engine;
        let s = Split {
            generation: name::lineage(parent).unwrap().now.generation,
            at,
            child: self.next_range,
        };
        let Some(child) = name::child(parent, &s).unwrap() else {
            return;
        };
        let mut rows = child.rows;
        for (k, v) in parent.image().unwrap() {
            if child.spans.iter().any(|(from, to)| *from <= k && k < *to) {
                rows.push((k, v));
            }
        }
        let mut engine = Model::default();
        engine.install(0, rows).unwrap();
        let outcome = self.name(id, name::Command::Split(s));
        assert!(matches!(outcome, name::Outcome::Split(_)), "{outcome:?}");
        self.names
            .insert(self.next_range, Range { engine, index: 0 });
        self.next_range += 1;
    }

    /// The range the senders' descriptors route `key` to, taking the directory's when they
    /// hold none for it.
    fn route(&mut self, key: &str) -> u64 {
        let route = key::route(BUCKET, key);
        let held = |cache: &[Descriptor]| {
            cache
                .iter()
                .filter(|d| d.holds(&route))
                .max_by_key(|d| d.generation)
                .map(|d| d.id)
        };
        if let Some(id) = held(&self.cache) {
            return id;
        }
        self.cache = self.directory();
        held(&self.cache).unwrap()
    }

    /// The senders learn where a span went from a range's lineage.
    fn learn(&mut self, id: u64, lineage: &Lineage) {
        self.cache.retain(|d| d.id != id);
        for d in [Some(&lineage.now), lineage.child.as_ref()]
            .into_iter()
            .flatten()
        {
            if !self
                .cache
                .iter()
                .any(|c| c.id == d.id && c.generation >= d.generation)
            {
                self.cache.retain(|c| c.id != d.id);
                self.cache.push(d.clone());
            }
        }
    }

    /// Sends `command` for `key` where the senders' descriptors route it until the range that
    /// holds the key answers.
    fn send(&mut self, key: &str, command: &name::Command) -> name::Outcome {
        for _ in 0..ROUTES {
            let id = self.route(key);
            match self.name(id, command.clone()) {
                name::Outcome::Moved(lineage) => self.learn(id, &lineage),
                outcome => return outcome,
            }
        }
        panic!("{key} was never routed to its range");
    }

    /// The range that holds `key` now.
    fn holder(&self, key: &str) -> &Model {
        let route = key::route(BUCKET, key);
        let (_, range) = self
            .names
            .iter()
            .find(|(id, _)| self.lineage(**id).now.holds(&route))
            .unwrap();
        &range.engine
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
                self.send(key, &delete);
            }
            Action::Sweep => self.sweep_step(),
            Action::SweepStops => self.sweep = None,
            Action::BlockSweep => self.block_sweep_step(),
            Action::BlockSweepStops => self.block_sweep = None,
            Action::Wait => self.clock += HANDOVER,
            Action::Leader { lag } => self.lag = lag,
            Action::Split { range, cut } => {
                let ids: Vec<u64> = self.names.keys().copied().collect();
                let id = ids[range % ids.len()];
                let (bucket, key) = CUTS[cut];
                self.split(id, key::route(bucket, key));
            }
            Action::Refresh => self.cache = self.directory(),
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
        let late =
            mantle_meta::clock::now(self.holder(g.key), self.proposed()).unwrap() > deadline_ns;
        match self.send(g.key, &put) {
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
                let mut verdicts: Vec<Option<Verdict>> = vec![None; files.len()];
                // Each round sends one command to each range the descriptors route a file
                // still unanswered to; a range that moved on answers where its span went.
                for _ in 0..ROUTES {
                    let mut groups: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
                    for (i, u) in files.iter().enumerate() {
                        if verdicts[i].is_none() {
                            let id = self.route(&u.referrer.key);
                            groups.entry(id).or_default().push(i);
                        }
                    }
                    if groups.is_empty() {
                        break;
                    }
                    for (id, mine) in groups {
                        let check = name::Command::Check(Check {
                            files: mine.iter().map(|&i| checked(&files[i])).collect(),
                            at_ns: self.proposed(),
                        });
                        match self.name(id, check) {
                            name::Outcome::Checked(answered) => {
                                for (&i, v) in mine.iter().zip(answered) {
                                    verdicts[i] = Some(v);
                                }
                            }
                            name::Outcome::Moved(lineage) => self.learn(id, &lineage),
                            other => panic!("a check answered {other:?}"),
                        }
                    }
                }
                Answer::Checked(verdicts.into_iter().map(Option::unwrap).collect())
            }
            Request::Settle(files) => {
                assert_eq!(
                    self.file(file::Command::Settle { files }),
                    file::Outcome::Settled
                );
                Answer::Settled
            }
            Request::Unmark(files) => {
                for (referrer, file) in files {
                    let unmark = name::Command::Unmark(Unmark {
                        files: vec![Marked {
                            bucket: referrer.bucket.clone(),
                            key: referrer.key.clone(),
                            file,
                        }],
                    });
                    assert_eq!(self.send(&referrer.key, &unmark), name::Outcome::Unmarked);
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
        for range in self.names.values() {
            let (mut from, to) = key::bucket_span(BUCKET);
            while let Some((k, v)) = range.engine.next(&from, &to).unwrap() {
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
            .values()
            .flat_map(|r| name::released(&r.engine, u64::MAX, usize::MAX).unwrap())
            .map(|r| r.file)
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

    /// Files marked in any Name range, under whatever key.
    fn marked(&self) -> BTreeSet<u128> {
        let mut out = BTreeSet::new();
        for range in self.names.values() {
            let (mut from, to) = key::marks_span(BUCKET);
            while let Some((k, _)) = range.engine.next(&from, &to).unwrap() {
                out.insert(key::decode_mark(&k).unwrap().2);
                from = k;
                from.push(0);
            }
        }
        out
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

/// A file the sweep asks a Name range about.
fn checked(u: &Unsettled) -> Checked {
    Checked {
        bucket: u.referrer.bucket.clone(),
        key: u.referrer.key.clone(),
        file: u.file,
        deadline_ns: u.deadline_ns,
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
