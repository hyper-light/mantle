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
//! and the block, renews the block's deadline while its body would still stream in, then
//! writes the file that names the block, then hands the file to its key's Name range; it may
//! stop for good at any stage, or come late to the next. Deletes release files,
//! both sweeps run a step at a time and stop for good between any two steps, leaders whose
//! clocks run behind propose entries at times earlier than entries already applied, and Name
//! ranges split and merge, their marks and queues going with their keys, while the gateways
//! and the sweep route by descriptors they learn of late and wait out a merge in flight.
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
use mantle_meta::merge::{self, Merger};
use mantle_meta::name::{
    self, Check, Checked, GateChange, Marked, Preconditions, Put, Split, Unmark, Verdict,
};
use mantle_meta::record::{
    BlockHeader, ChunkPlace, Descriptor, Extent, GateState, Lineage, Referrer, Standing, Target,
    Version, Versioning,
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
/// its key: each refusal names a newer descriptor, and the directory, current here, ends it,
/// unless a merge in flight holds the key, when the sender tries again later.
const ROUTES: usize = 8;
/// Rows a merge resumed after its driver stopped may take.
const MAX_ROWS: u64 = 64;

#[derive(Debug, Clone)]
enum Action {
    /// A gateway writes a chunk and its block, for a file of a key.
    Write {
        key: usize,
    },
    /// A gateway takes its next step: writes its file, or hands it over.
    Advance(usize),
    /// A gateway renews its block's deadline, if its file is not yet written.
    Renew(usize),
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
    /// A driver begins merging a range into the one just below it, taking at most `rows`
    /// rows.
    Merge {
        range: usize,
        rows: u64,
    },
    /// One step of one merge in flight.
    MergeStep(usize),
    /// A merge's driver gives it up before its decision.
    MergeAbandon(usize),
    /// A merge's driver stops for good; the request it was sending arrives later.
    MergeStop(usize),
    /// A stopped driver's request arrives, late.
    Late(usize),
    /// Every merge a range's lineage shows in flight is resumed.
    MergeResume,
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        3 => (0usize..4).prop_map(|key| Action::Write { key }),
        4 => (0usize..4).prop_map(Action::Advance),
        2 => (0usize..4).prop_map(Action::Renew),
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
        1 => (0usize..8, 0u64..16).prop_map(|(range, rows)| Action::Merge { range, rows }),
        3 => (0usize..4).prop_map(Action::MergeStep),
        1 => (0usize..4).prop_map(Action::MergeAbandon),
        1 => (0usize..4).prop_map(Action::MergeStop),
        1 => (0usize..4).prop_map(Action::Late),
        1 => Just(Action::MergeResume),
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
    /// Merges in flight.
    mergers: Vec<Merger>,
    /// Requests of stopped merge drivers, not yet arrived.
    late: Vec<merge::Request>,
    /// The merges a lower range took, by its ID and the generation the merge named.
    taken: BTreeSet<(u64, u64)>,
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
            mergers: Vec::new(),
            late: Vec::new(),
            taken: BTreeSet::new(),
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

    /// The descriptor of every Name range that has not ended: what the directory holds,
    /// current here.
    fn directory(&self) -> Vec<Descriptor> {
        self.names
            .keys()
            .map(|&id| self.lineage(id))
            .filter(|l| l.standing != Standing::Ended)
            .map(|l| l.now)
            .collect()
    }

    /// The ranges that own their spans: every serving range, and every frozen one whose merge
    /// was not taken. A range that ended, or whose merge was taken, keeps rows its lower range
    /// took and now owns.
    fn owners(&self) -> Vec<(u64, Lineage)> {
        self.names
            .keys()
            .map(|&id| (id, self.lineage(id)))
            .filter(|(_, l)| match (l.standing, &l.into) {
                (Standing::Serving, _) => true,
                (Standing::Frozen, Some(into)) => !self.taken.contains(&(into.id, into.generation)),
                _ => false,
            })
            .collect()
    }

    /// The rows of every range that owns its span.
    fn owned(&self) -> Vec<&Model> {
        self.owners()
            .into_iter()
            .map(|(id, _)| &self.names[&id].engine)
            .collect()
    }

    /// What the Name ranges answer a merge driver's request.
    fn serve_merge(&mut self, request: merge::Request) -> merge::Answer {
        match request {
            merge::Request::Name { range, command } => {
                merge::Answer::Name(self.name(range, *command))
            }
            merge::Request::Merge {
                range,
                from,
                command,
            } => {
                let upper = self.names[&from].engine.clone();
                let lower = self.names.get_mut(&range).unwrap();
                lower.index += 1;
                let outcome =
                    name::merge(&mut lower.engine, lower.index, &command, &upper).unwrap();
                if let name::Outcome::Merged(_) = outcome {
                    self.taken.insert((range, command.generation));
                }
                merge::Answer::Name(outcome)
            }
            merge::Request::Lineage { range } => merge::Answer::Lineage(self.lineage(range)),
        }
    }

    /// One step of the merge at `i`.
    fn merge_step(&mut self, i: usize) {
        if let Some(request) = self.mergers[i].next() {
            let answer = self.serve_merge(request);
            self.mergers[i].answer(answer).unwrap();
        }
        if self.mergers[i].is_done() {
            self.mergers.swap_remove(i);
        }
    }

    /// A driver for every merge a range's lineage shows in flight.
    fn resume_merges(&mut self) {
        let ids: Vec<u64> = self.names.keys().copied().collect();
        for id in ids {
            if let Ok(m) = Merger::resume(&self.lineage(id), MAX_ROWS) {
                self.mergers.push(m);
            }
        }
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
        // An ended range holds nothing: only where its span went is learnt.
        let own = if lineage.standing == Standing::Ended {
            None
        } else {
            Some(&lineage.now)
        };
        for d in [own, lineage.child.as_ref(), lineage.into.as_ref()]
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
    /// holds the key answers; `None` while a merge in flight holds the key.
    fn send(&mut self, key: &str, command: &name::Command) -> Option<name::Outcome> {
        let mut next = None;
        for _ in 0..ROUTES {
            let id = next.take().unwrap_or_else(|| self.route(key));
            match self.name(id, command.clone()) {
                name::Outcome::Moved(lineage) => {
                    // A frozen range's span is the range below's once the merge is taken, so
                    // that range is asked next.
                    if lineage.standing == Standing::Frozen {
                        next = lineage.into.as_ref().map(|d| d.id);
                    }
                    self.learn(id, &lineage);
                }
                outcome => return Some(outcome),
            }
        }
        let route = key::route(BUCKET, key);
        let frozen = self.names.keys().any(|&id| {
            let l = self.lineage(id);
            l.standing == Standing::Frozen && l.now.holds(&route)
        });
        assert!(frozen, "{key} was never routed");
        None
    }

    /// The range that owns `key` now.
    fn holder(&self, key: &str) -> &Model {
        let route = key::route(BUCKET, key);
        let (id, _) = self
            .owners()
            .into_iter()
            .find(|(_, l)| l.now.holds(&route))
            .unwrap();
        &self.names[&id].engine
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
            Action::Renew(i) => {
                if !self.gateways.is_empty() {
                    self.renew(i % self.gateways.len());
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
            Action::Merge { range, rows } => {
                let directory = self.directory();
                let upper = directory[range % directory.len()].clone();
                if let Some(lower) = directory.iter().find(|d| d.hi.as_ref() == Some(&upper.lo)) {
                    self.mergers.push(Merger::new(lower.clone(), upper, rows));
                }
            }
            Action::MergeStep(i) => {
                if !self.mergers.is_empty() {
                    self.merge_step(i % self.mergers.len());
                }
            }
            Action::MergeAbandon(i) => {
                if !self.mergers.is_empty() {
                    let i = i % self.mergers.len();
                    if let Some(request) = self.mergers[i].abandon() {
                        let answer = self.serve_merge(request);
                        self.mergers[i].answer(answer).unwrap();
                    }
                }
            }
            Action::MergeStop(i) => {
                if !self.mergers.is_empty() {
                    let stopped = self.mergers.swap_remove(i % self.mergers.len());
                    self.late.extend(stopped.next());
                }
            }
            Action::Late(i) => {
                if !self.late.is_empty() {
                    let request = self.late.swap_remove(i % self.late.len());
                    self.serve_merge(request);
                }
            }
            Action::MergeResume => self.resume_merges(),
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
                    key: None,
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
            Stage::File { deadline_ns } => {
                if !self.handover(&g, deadline_ns) {
                    self.gateways.push(g);
                }
            }
        }
    }

    /// Gateway `i` renews its block, which holds it for another handover unless the sweep
    /// released it, or has taken it apart; a gateway whose block was released gives up, as it
    /// would make the block again.
    fn renew(&mut self, i: usize) {
        let Stage::Block { block, .. } = self.gateways[i].stage else {
            return;
        };
        let renew = block::Command::Renew {
            block,
            file: self.gateways[i].file,
            handover_ns: HANDOVER,
            at_ns: self.proposed(),
        };
        match self.block(renew) {
            block::Outcome::Written { deadline_ns } => {
                self.gateways[i].stage = Stage::Block { block, deadline_ns };
            }
            block::Outcome::Expired | block::Outcome::NoSuchBlock => {
                self.gateways.swap_remove(i);
            }
            other => panic!("{other:?}"),
        }
    }

    /// Hands the file over; false while a merge in flight holds its key, so the gateway tries
    /// again later.
    fn handover(&mut self, g: &Gateway, deadline_ns: u64) -> bool {
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
            None => return false,
            Some(name::Outcome::Put { .. }) => assert!(!late, "a late handover was taken"),
            Some(name::Outcome::Expired) => assert!(late, "a handover in time was refused"),
            Some(other) => panic!("{other:?}"),
        }
        true
    }

    fn sweep_step(&mut self) {
        let sweep = self.sweep.get_or_insert_with(|| Sweep::new(PAGE));
        let Some(request) = sweep.next() else {
            self.sweep = None;
            return;
        };
        // A key a merge in flight holds leaves the step to be tried again.
        let Some(answer) = self.serve(request) else {
            return;
        };
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

    /// What the File and Name ranges answer a file sweep's request; `None` while a merge in
    /// flight holds one of its keys.
    fn serve(&mut self, request: Request) -> Option<Answer> {
        Some(match request {
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
                if verdicts.iter().any(Option::is_none) {
                    return None;
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
                    let outcome = self.send(&referrer.key, &unmark)?;
                    assert_eq!(outcome, name::Outcome::Unmarked);
                }
                Answer::Unmarked
            }
        })
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
            BlockRequest::Release { block, deadline_ns } => {
                match self.block(block::Command::Release { block, deadline_ns }) {
                    block::Outcome::Released => BlockAnswer::Released(true),
                    block::Outcome::Written { .. } | block::Outcome::NoSuchBlock => {
                        BlockAnswer::Released(false)
                    }
                    other => panic!("{other:?}"),
                }
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
        for range in self.owned() {
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
        self.owned()
            .into_iter()
            .flat_map(|r| name::released(r, u64::MAX, usize::MAX).unwrap())
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
        for range in self.owned() {
            let (mut from, to) = key::marks_span(BUCKET);
            while let Some((k, _)) = range.next(&from, &to).unwrap() {
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
        // Late requests arrive, and every merge in flight is carried to its end, each in turn,
        // since one may wait on another's end.
        while let Some(request) = self.late.pop() {
            self.serve_merge(request);
            self.check();
        }
        self.resume_merges();
        for turn in 0..10_000 {
            if self.mergers.is_empty() {
                break;
            }
            self.merge_step(turn % self.mergers.len());
            self.check();
        }
        assert!(self.mergers.is_empty(), "a merge never finished");
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

/// A gateway renews its block while the sweep judges it. A renewal that lands after the File
/// range judged the block, and before the sweep releases it, keeps the block, and the gateway's
/// file names it in time; a renewal after the release is refused, and that gateway gives up.
#[test]
fn a_renewal_keeps_a_block_unless_the_sweep_released_it_first() {
    let judged = |w: &mut World| {
        w.act(&Action::Wait);
        w.act(&Action::Wait);
        w.act(&Action::BlockSweep);
        w.act(&Action::BlockSweep);
        assert!(matches!(
            w.block_sweep.as_ref().unwrap().next(),
            Some(BlockRequest::Release { .. })
        ));
    };

    let mut w = World::new();
    w.act(&Action::Write { key: 0 });
    judged(&mut w);
    w.act(&Action::Renew(0));
    w.act(&Action::BlockSweep);
    assert_eq!(w.recorded().len(), 1, "the renewed block was released");
    w.act(&Action::Advance(0));
    assert_eq!(w.written.len(), 1, "the renewed block's file was refused");
    w.check();
    w.finish();

    let mut w = World::new();
    w.act(&Action::Write { key: 0 });
    judged(&mut w);
    w.act(&Action::BlockSweep);
    w.act(&Action::Renew(0));
    assert!(w.gateways.is_empty(), "a released block was renewed");
    w.check();
    w.finish();
    assert!(w.recorded().is_empty());
}
