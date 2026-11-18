#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

//! What a gateway made and never handed over (docs/design/metadata.md §2), found by the sweeps
//! (`mantle_meta::sweep`) and taken apart by the collector (`mantle_meta::reclaim`), across a
//! Block range, a File range and Name ranges that split and merge while it happens, under
//! schedules proptest chooses. A gateway writes a block's chunk to a volume and the block,
//! renews the block's deadline while its body would still stream in, then writes the file that
//! names the block, then hands the file to its key's Name range; or writes the block and the file
//! at once, either arriving first, and hands the file over only once the block is held at least
//! to the file's write (docs/design/gateway.md §2.2); it may stop for good at any stage, or come
//! late to the next. Uploaders write parts and complete uploads with composite
//! files of them, have completions refused, retry completions that committed with a composite
//! made again, and abort. Deletes release files. Two file sweeps run at once, each request
//! delivered at some later step, and a stopped sweep's request arrives after it; the block
//! sweep and the collector stop between any two steps; leaders whose clocks run behind propose
//! entries at times earlier than entries already applied; and Name ranges split and merge,
//! their marks and queues going with their keys, while everyone routes by descriptors learned
//! late and waits out a merge in flight.
//!
//! After every step, no file a version, a part or a held composite names is released, and
//! each such file keeps its rows, its blocks' rows and their chunks. Once the faults stop and
//! every deadline has passed, the sweeps leave nothing unsettled in either layer, the
//! collector leaves nothing released, every file is held or reclaimed and not both, every
//! block left is one a file names, every chunk left one a block holds, and every mark is on a
//! file held or released.
//!
//! Clients of two principals draw operations putting or deleting keys, creating uploads,
//! putting their parts and completing them, each with a horizon; one may carry the identity
//! another principal's operation carries, and a client may carry one of its own part's or
//! completion's identities to another part or upload of the key, as a native client carries its
//! upload's on every run (docs/design/gateway.md §2.1). Any number of gateways attempt each
//! operation at any step, each put or part attempt with a block and a file of its own and each
//! completion attempt with a composite file and an ETag of its own, and an attempt handed over
//! is delivered again as a copy at any later step; the collector forgets the rows whose horizon
//! passed. After every step the operation record's five obligations hold: at most one effect
//! per identity within its horizon, every attempt within it answered from the row with the
//! first's answer, so a retried completion is never rebuilt, no attempt within the horizon
//! changing its key's rows, every attempt's file released once within its deadline, and the
//! rows unchanged by a split or a merge, each with its key. With the row, the gateway's scope by
//! principal, by part number or by upload, the collector's horizon check, or a completion's row
//! removed, a schedule breaks one (`each_piece_of_the_operation_record_removed_is_caught`).

use std::collections::{BTreeMap, BTreeSet};

use mantle_chunk::ChunkKey;
use mantle_meta::block;
use mantle_meta::engine::{Engine, Model, Rows, Write};
use mantle_meta::file::{self, Unsettled};
use mantle_meta::key::{self, NameRow};
use mantle_meta::merge::{self, Merger};
use mantle_meta::name::{
    self, Abort, Check, Checked, Complete, CreateUpload, Forget, GateChange, Listed, Match,
    Mutation, Operated, Operation, Preconditions, Put, PutPart, Split, Verdict,
};
use mantle_meta::reclaim::{self, Reclaimer};
use mantle_meta::record::{
    BlockHeader, ChunkPlace, Descriptor, Extent, GateState, Lineage, Made, Part, Referrer,
    Standing, Target, Upload, Version, Versioning,
};
use mantle_meta::sweep::{Answer, BlockAnswer, BlockRequest, BlockSweep, Request, Sweep};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;

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
/// Schedules the simulation runs, and the steps each takes at most. A step draws at most one
/// operation, upload or delivered attempt, so an action naming one of them by an index below
/// `STEPS` can name each a schedule holds.
const CASES: u32 = 2_000;
const STEPS: usize = 120;
/// Principals clients act as: two, the fewest under which one can carry another's identity.
const PRINCIPALS: u128 = 2;
/// Horizons clients declare: as long as a gateway has for its next step, so attempts come both
/// within and past it and the collector forgets rows between them; and the clock's last instant,
/// which no schedule outlives (`name::Operation`).
const HORIZONS: [u64; 2] = [HANDOVER, u64::MAX];

/// The simulation whole, or with one piece of the operation record removed, which it must catch
/// (docs/design/metadata.md §2, "Operations").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Variant {
    Whole,
    /// The range keeps no row of what an operation made: each is removed as it commits.
    NoRow,
    /// The gateway hands over the client's identity unscoped by its principal.
    NoScope,
    /// The collector forgets every row it reads, its horizon passed or not.
    NoHorizon,
    /// The gateway hands over a part's identity unscoped by its part number.
    NoPartNumber,
    /// The gateway hands over a part's or a completion's identity unscoped by its upload.
    NoUpload,
    /// A completion keeps no row: a retry is rebuilt from the version it finds, as a write
    /// carrying no operation is (`name::complete`).
    Rebuilt,
    /// A gateway that writes its block and its file at once hands the file over without
    /// checking that the block's deadline is no earlier than the file's write
    /// (docs/design/gateway.md §2.2).
    Unbounded,
}

#[derive(Debug, Clone)]
enum Action {
    /// A gateway writes a chunk and its block, for a file of a key.
    Write {
        key: usize,
    },
    /// A gateway writes its block and its file at once, as a PUT does once its chunks are
    /// durable (docs/design/gateway.md §2.2): the one named first arrives now, the other at the
    /// gateway's next step, and the file hands over at the step after only if the block's
    /// deadline is no earlier than the file's write.
    WriteAtOnce {
        key: usize,
        file_first: bool,
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
    /// One step of file sweep 0 or 1, starting one if none runs: a step sends the sweep's
    /// next request, or delivers the answer to the one it sent, so the other sweep's steps
    /// and every other action fall between the two.
    Sweep(usize),
    /// A sweep stops for good; the request it sent arrives later.
    SweepStops(usize),
    /// A stopped sweep's request arrives, late.
    LateSweep(usize),
    /// One step of the collector, taking a Name range's oldest released file if it has none.
    Collect,
    /// The collector stops; the next starts again from the queue.
    CollectorStops,
    /// An uploader creates an upload for a key.
    Upload {
        key: usize,
    },
    /// An uploader writes its upload's next part: a block, the part's file, and the part.
    UploadPart(usize),
    /// An uploader completes its upload with a composite file of its parts, the completion
    /// refused by its precondition if `refuse`.
    UploadComplete {
        uploader: usize,
        refuse: bool,
    },
    /// An uploader whose completion committed retries it with a composite made again.
    UploadRetry(usize),
    /// An uploader aborts its upload.
    UploadAbort(usize),
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
    /// A client of a principal draws an operation putting a key, with a horizon.
    Operate {
        key: usize,
        principal: usize,
        horizon: usize,
    },
    /// A client of a principal draws an operation deleting a key, with a horizon.
    OperateDelete {
        key: usize,
        principal: usize,
        horizon: usize,
    },
    /// A client of a principal draws an operation creating an upload of a key.
    OperateUpload {
        key: usize,
        principal: usize,
        horizon: usize,
    },
    /// A client of a principal draws an operation putting part `number` of an upload: a native
    /// client's carries its upload's identity, as every run of it does (docs/design/gateway.md
    /// §2.1), and a stock client's one it draws.
    OperatePart {
        uploader: usize,
        number: u16,
        principal: usize,
        horizon: usize,
        native: bool,
    },
    /// A client of a principal draws an operation completing an upload.
    OperateComplete {
        uploader: usize,
        principal: usize,
        horizon: usize,
    },
    /// A client of the other principal carries the identity an operation carries, on its key
    /// and to its write: forged, or drawn the same.
    Forge {
        of: usize,
        horizon: usize,
    },
    /// A client carries the identity one of its part or completion operations carries to the
    /// same write of another upload of the key.
    Reuse {
        of: usize,
        uploader: usize,
    },
    /// A gateway starts an attempt of an operation: for a put or a part, a block and a file of
    /// its own, handed over as the operation's at its later steps; for a completion, a
    /// composite file of its own, delivered at once; a delete or an upload, delivered at once.
    Attempt(usize),
    /// A gateway attempting an operation takes its next step.
    AdvanceAttempt(usize),
    /// An attempt handed over is delivered again, a copy.
    Copy(usize),
    /// The collector forgets the operations each Name range holds whose horizon has passed.
    Forget,
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        3 => (0usize..4).prop_map(|key| Action::Write { key }),
        4 => (0usize..4).prop_map(Action::Advance),
        2 => (0usize..4).prop_map(Action::Renew),
        1 => (0usize..4).prop_map(Action::Crash),
        1 => (0usize..4, any::<bool>()).prop_map(|(key, versioned)| Action::Delete { key, versioned }),
        3 => (0usize..2).prop_map(Action::Sweep),
        1 => (0usize..2).prop_map(Action::SweepStops),
        1 => (0usize..4).prop_map(Action::LateSweep),
        2 => Just(Action::Collect),
        1 => Just(Action::CollectorStops),
        1 => (0usize..4).prop_map(|key| Action::Upload { key }),
        2 => (0usize..4).prop_map(Action::UploadPart),
        1 => (0usize..4, any::<bool>())
            .prop_map(|(uploader, refuse)| Action::UploadComplete { uploader, refuse }),
        1 => (0usize..4).prop_map(Action::UploadRetry),
        1 => (0usize..4).prop_map(Action::UploadAbort),
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
        1 => (0usize..4, 0usize..2, 0..HORIZONS.len()).prop_map(|(key, principal, horizon)| {
            Action::Operate { key, principal, horizon }
        }),
        1 => (0usize..4, 0usize..2, 0..HORIZONS.len()).prop_map(|(key, principal, horizon)| {
            Action::OperateDelete { key, principal, horizon }
        }),
        1 => (0usize..4, 0usize..2, 0..HORIZONS.len()).prop_map(|(key, principal, horizon)| {
            Action::OperateUpload { key, principal, horizon }
        }),
        1 => (0..STEPS, 1u16..=3, 0usize..2, 0..HORIZONS.len(), any::<bool>()).prop_map(
            |(uploader, number, principal, horizon, native)| Action::OperatePart {
                uploader,
                number,
                principal,
                horizon,
                native,
            }
        ),
        1 => (0..STEPS, 0usize..2, 0..HORIZONS.len()).prop_map(|(uploader, principal, horizon)| {
            Action::OperateComplete { uploader, principal, horizon }
        }),
        1 => (0..STEPS, 0..HORIZONS.len()).prop_map(|(of, horizon)| Action::Forge { of, horizon }),
        1 => (0..STEPS, 0..STEPS).prop_map(|(of, uploader)| Action::Reuse { of, uploader }),
        3 => (0..STEPS).prop_map(Action::Attempt),
        4 => (0..STEPS).prop_map(Action::AdvanceAttempt),
        1 => (0..STEPS).prop_map(Action::Copy),
        2 => Just(Action::Forget),
    ]
}

/// The actions of `action`, and a gateway writing its block and its file at once, as often as
/// `Write` is drawn there. Kept apart from `action`, whose schedules each piece of the operation
/// record removed is caught within (`each_piece_of_the_operation_record_removed_is_caught`).
fn any_action() -> impl Strategy<Value = Action> {
    prop_oneof![
        19 => action(),
        1 => (0usize..4, any::<bool>())
            .prop_map(|(key, file_first)| Action::WriteAtOnce { key, file_first }),
    ]
}

/// A client's operation: the principal authentication established, the identity the client
/// drew, the key it writes and what it writes there, the versioning it writes under, its
/// horizon, and its last effect.
struct Op {
    principal: u128,
    raw: u128,
    key: &'static str,
    kind: Kind,
    versioning: Versioning,
    horizon_ns: u64,
    effect: Option<Effect>,
}

/// What an operation writes, which with its principal scopes its identity
/// (docs/design/gateway.md §2.1).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Kind {
    Put,
    Delete,
    /// CreateMultipartUpload.
    Upload,
    /// UploadPart of part `number` to `upload`.
    Part {
        upload: String,
        number: u16,
    },
    /// CompleteMultipartUpload of `upload`.
    Complete {
        upload: String,
    },
}

/// What an operation's delivery made: the answer it got, and the range's time until which its
/// row answers.
struct Effect {
    answer: String,
    expiry_ns: u64,
}

/// An attempt of operation `op` handed over: its file if it carries one, the file's deadline,
/// and the command, which a copy carries again.
#[derive(Clone)]
struct Delivered {
    op: usize,
    file: Option<u128>,
    deadline_ns: u64,
    command: name::Command,
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
    /// Its block's write and its file's are out at once, each with its deadline once it has
    /// arrived.
    AtOnce {
        block: u128,
        block_deadline_ns: Option<u64>,
        file_deadline_ns: Option<u64>,
    },
}

struct Gateway {
    key: &'static str,
    file: u128,
    versioning: Versioning,
    stage: Stage,
    /// The operation it attempts, if its write carries one.
    op: Option<usize>,
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
    sweeps: [Option<Sweep>; 2],
    /// Each sweep's request sent and not yet answered.
    sent: [Option<Request>; 2],
    /// Requests of stopped sweeps, not yet arrived.
    late_sweeps: Vec<Request>,
    block_sweep: Option<BlockSweep>,
    collector: Option<Collecting>,
    uploaders: Vec<Uploader>,
    written: BTreeSet<u128>,
    /// Files the collector took apart.
    reclaimed: BTreeSet<u128>,
    variant: Variant,
    ops: Vec<Op>,
    /// The identity the next operation's client draws.
    next_operation: u128,
    /// Attempts handed over, which copies deliver again.
    delivered: Vec<Delivered>,
    /// Files whose attempt's delivery made a version.
    made: BTreeSet<u128>,
    /// Files of attempts answered from their operation's row, with their deadlines.
    answered: BTreeMap<u128, u64>,
    /// The obligations broken, as each was.
    broken: Vec<String>,
}

/// The collector's work: a file released by Name range `range`.
struct Collecting {
    range: u64,
    file: u128,
    reclaimer: Reclaimer,
}

/// A multipart upload in progress: the principal and identity its client created it with,
/// which a native client carries on its every run; its parts, by number, as a completion
/// lists them; and the composite file its completion committed, if it did.
struct Uploader {
    principal: u128,
    raw: u128,
    key: &'static str,
    upload: String,
    parts: BTreeMap<u16, Listed>,
    completed: Option<u128>,
}

impl World {
    fn new() -> Self {
        Self::with(Variant::Whole)
    }

    /// A cell whose first Name range opened the bucket's gate and split at "m", the gateways
    /// knowing both halves.
    fn with(variant: Variant) -> Self {
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
            sweeps: [None, None],
            sent: [None, None],
            late_sweeps: Vec::new(),
            block_sweep: None,
            collector: None,
            uploaders: Vec::new(),
            written: BTreeSet::new(),
            reclaimed: BTreeSet::new(),
            variant,
            ops: Vec::new(),
            next_operation: 0,
            delivered: Vec::new(),
            made: BTreeSet::new(),
            answered: BTreeMap::new(),
            broken: Vec::new(),
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

    fn lineage(&mut self, id: u64) -> Lineage {
        name::lineage(&mut self.names.get_mut(&id).unwrap().engine).unwrap()
    }

    /// The descriptor of every Name range that has not ended: what the directory holds,
    /// current here.
    fn directory(&mut self) -> Vec<Descriptor> {
        let ids: Vec<u64> = self.names.keys().copied().collect();
        let mut out = Vec::new();
        for id in ids {
            let l = self.lineage(id);
            if l.standing != Standing::Ended {
                out.push(l.now);
            }
        }
        out
    }

    /// The ranges that own their spans: every serving range, and every frozen one whose merge
    /// was not taken. A range that ended, or whose merge was taken, keeps rows its lower range
    /// took and now owns.
    fn owners(&mut self) -> Vec<(u64, Lineage)> {
        let ids: Vec<u64> = self.names.keys().copied().collect();
        let mut owners = Vec::new();
        for id in ids {
            let l = self.lineage(id);
            let owns = match (l.standing, &l.into) {
                (Standing::Serving, _) => true,
                (Standing::Frozen, Some(into)) => !self.taken.contains(&(into.id, into.generation)),
                _ => false,
            };
            if owns {
                owners.push((id, l));
            }
        }
        owners
    }

    /// The rows of every range that owns its span.
    fn owned(&mut self) -> Vec<&mut Model> {
        let owners: BTreeSet<u64> = self.owners().into_iter().map(|(id, _)| id).collect();
        self.names
            .iter_mut()
            .filter(|(id, _)| owners.contains(id))
            .map(|(_, r)| &mut r.engine)
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
                let mut upper = self.names[&from].engine.clone();
                let lower = self.names.get_mut(&range).unwrap();
                lower.index += 1;
                let outcome =
                    name::merge(&mut lower.engine, lower.index, &command, &mut upper).unwrap();
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
            if let Ok(m) = Merger::resume(&self.lineage(id), MAX_ROWS, u64::MAX) {
                self.mergers.push(m);
            }
        }
    }

    /// Splits range `id` at `at` as its replicas would: the child starts from the rows the
    /// split names, taken from the parent as they stand, and then the parent applies the split.
    fn split(&mut self, id: u64, at: Vec<u8>) {
        let parent = &mut self.names.get_mut(&id).unwrap().engine;
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
        let ids: Vec<u64> = self.names.keys().copied().collect();
        let frozen = ids.into_iter().any(|id| {
            let l = self.lineage(id);
            l.standing == Standing::Frozen && l.now.holds(&route)
        });
        assert!(frozen, "{key} was never routed");
        None
    }

    /// The range that owns `key` now.
    fn holder(&mut self, key: &str) -> &mut Model {
        let id = self.holder_id(key);
        &mut self.names.get_mut(&id).unwrap().engine
    }

    fn holder_id(&mut self, key: &str) -> u64 {
        let route = key::route(BUCKET, key);
        let (id, _) = self
            .owners()
            .into_iter()
            .find(|(_, l)| l.now.holds(&route))
            .unwrap();
        id
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
        // A split or a merge's step carries every operation row with its key (obligation 5).
        let carries = matches!(
            action,
            Action::Split { .. } | Action::MergeStep(_) | Action::MergeAbandon(_) | Action::Late(_)
        );
        let rows = carries.then(|| self.operation_rows());
        self.step(action);
        if let Some(rows) = rows {
            self.carried(&rows, &format!("{action:?}"));
        }
    }

    /// Obligation 5: a split or a merge's step left the owners' operation rows as they were,
    /// each with its key.
    fn carried(&mut self, rows: &BTreeSet<(Vec<u8>, Vec<u8>)>, step: &str) {
        if self.operation_rows() != *rows {
            self.broken.push(format!(
                "5, carried with its key: {step} changed the operation rows"
            ));
        }
    }

    fn step(&mut self, action: &Action) {
        match *action {
            Action::Write { key } => self.start(KEYS[key], None),
            Action::WriteAtOnce { key, file_first } => self.start_at_once(KEYS[key], file_first),
            Action::Operate {
                key,
                principal,
                horizon,
            } => self.operate(KEYS[key], principal, horizon, Kind::Put),
            Action::OperateDelete {
                key,
                principal,
                horizon,
            } => self.operate(KEYS[key], principal, horizon, Kind::Delete),
            Action::OperateUpload {
                key,
                principal,
                horizon,
            } => self.operate(KEYS[key], principal, horizon, Kind::Upload),
            Action::OperatePart {
                uploader,
                number,
                principal,
                horizon,
                native,
            } => {
                if !self.uploaders.is_empty() {
                    let u = &self.uploaders[uploader % self.uploaders.len()];
                    let (key, upload) = (u.key, u.upload.clone());
                    let kind = Kind::Part { upload, number };
                    if native {
                        let run = Op {
                            principal: u.principal,
                            raw: u.raw,
                            key,
                            kind,
                            versioning: versioning(self.ops.len()),
                            horizon_ns: HORIZONS[horizon],
                            effect: None,
                        };
                        self.draw(run);
                    } else {
                        self.operate(key, principal, horizon, kind);
                    }
                }
            }
            Action::OperateComplete {
                uploader,
                principal,
                horizon,
            } => {
                if !self.uploaders.is_empty() {
                    let u = &self.uploaders[uploader % self.uploaders.len()];
                    let (key, upload) = (u.key, u.upload.clone());
                    self.operate(key, principal, horizon, Kind::Complete { upload });
                }
            }
            Action::Forge { of, horizon } => {
                if !self.ops.is_empty() {
                    let of = &self.ops[of % self.ops.len()];
                    let forged = Op {
                        principal: (of.principal + 1) % PRINCIPALS,
                        raw: of.raw,
                        key: of.key,
                        kind: of.kind.clone(),
                        versioning: versioning(self.ops.len()),
                        horizon_ns: HORIZONS[horizon],
                        effect: None,
                    };
                    self.draw(forged);
                }
            }
            Action::Reuse { of, uploader } => {
                // Each part or completion operation, with the upload it writes to.
                let carried: Vec<(&Op, &String)> = self
                    .ops
                    .iter()
                    .filter_map(|o| match &o.kind {
                        Kind::Part { upload, .. } | Kind::Complete { upload } => Some((o, upload)),
                        _ => None,
                    })
                    .collect();
                if !carried.is_empty() {
                    let (of, from) = carried[of % carried.len()];
                    let on: Vec<&Uploader> = self
                        .uploaders
                        .iter()
                        .filter(|u| u.key == of.key && &u.upload != from)
                        .collect();
                    if !on.is_empty() {
                        let upload = on[uploader % on.len()].upload.clone();
                        let kind = match of.kind {
                            Kind::Part { number, .. } => Kind::Part { upload, number },
                            _ => Kind::Complete { upload },
                        };
                        let reused = Op {
                            principal: of.principal,
                            raw: of.raw,
                            key: of.key,
                            kind,
                            versioning: versioning(self.ops.len()),
                            horizon_ns: of.horizon_ns,
                            effect: None,
                        };
                        self.draw(reused);
                    }
                }
            }
            Action::Attempt(i) => {
                if !self.ops.is_empty() {
                    let op = i % self.ops.len();
                    match self.ops[op].kind {
                        Kind::Put | Kind::Part { .. } => self.start(self.ops[op].key, Some(op)),
                        Kind::Delete | Kind::Upload => self.attempt_at_once(op),
                        Kind::Complete { .. } => self.attempt_complete(op),
                    }
                }
            }
            Action::AdvanceAttempt(i) => {
                let attempts: Vec<usize> = (0..self.gateways.len())
                    .filter(|&j| self.gateways[j].op.is_some())
                    .collect();
                if !attempts.is_empty() {
                    let g = self.gateways.swap_remove(attempts[i % attempts.len()]);
                    self.advance(g);
                }
            }
            Action::Copy(i) => {
                if !self.delivered.is_empty() {
                    let copy = self.delivered[i % self.delivered.len()].clone();
                    self.deliver(&copy);
                }
            }
            Action::Forget => {
                for (id, _) in self.owners() {
                    self.forget(id);
                }
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
                    owner: "o".into(),
                });
                self.send(key, &delete);
            }
            Action::Sweep(i) => self.sweep_step(i),
            Action::SweepStops(i) => {
                self.sweeps[i] = None;
                if let Some(request) = self.sent[i].take() {
                    self.late_sweeps.push(request);
                }
            }
            Action::LateSweep(j) => {
                if !self.late_sweeps.is_empty() {
                    let request = self.late_sweeps.swap_remove(j % self.late_sweeps.len());
                    if self.serve(request.clone()).is_none() {
                        self.late_sweeps.push(request);
                    }
                }
            }
            Action::Collect => self.collect_step(),
            Action::CollectorStops => self.collector = None,
            Action::Upload { key } => self.create_upload(KEYS[key]),
            Action::UploadPart(i) => {
                if !self.uploaders.is_empty() {
                    self.upload_part(i % self.uploaders.len());
                }
            }
            Action::UploadComplete { uploader, refuse } => {
                if !self.uploaders.is_empty() {
                    self.complete(uploader % self.uploaders.len(), refuse, false);
                }
            }
            Action::UploadRetry(i) => {
                if !self.uploaders.is_empty() {
                    self.complete(i % self.uploaders.len(), false, true);
                }
            }
            Action::UploadAbort(i) => {
                if !self.uploaders.is_empty() {
                    let u = self.uploaders.swap_remove(i % self.uploaders.len());
                    let abort = name::Command::Abort(Abort {
                        bucket: BUCKET.into(),
                        incarnation: 1,
                        key: u.key.into(),
                        upload: u.upload.clone(),
                        at_ns: self.proposed(),
                    });
                    match self.send(u.key, &abort) {
                        Some(name::Outcome::Aborted | name::Outcome::NoSuchUpload) => {}
                        // A merge in flight holds the key: the uploader tries again later.
                        None => self.uploaders.push(u),
                        Some(other) => panic!("abort: {other:?}"),
                    }
                }
            }
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
                    self.mergers
                        .push(Merger::new(lower.clone(), upper, rows, u64::MAX));
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

    /// A client of `principal` draws an operation writing `kind` to `key`, its identity afresh.
    fn operate(&mut self, key: &'static str, principal: usize, horizon: usize, kind: Kind) {
        self.next_operation += 1;
        let op = Op {
            principal: principal as u128,
            raw: self.next_operation,
            key,
            kind,
            versioning: versioning(self.ops.len()),
            horizon_ns: HORIZONS[horizon],
            effect: None,
        };
        self.draw(op);
    }

    /// Takes a client's operation, unless its principal already carries its identity to the
    /// same write of the same key: that is one operation, which every attempt carries.
    fn draw(&mut self, op: Op) {
        let same = self.ops.iter().any(|o| {
            (o.principal, o.raw, o.key, &o.kind) == (op.principal, op.raw, op.key, &op.kind)
        });
        if !same {
            self.ops.push(op);
        }
    }

    /// An attempt of delete or upload operation `op`, delivered now: it carries no file.
    fn attempt_at_once(&mut self, op: usize) {
        let o = &self.ops[op];
        let mutation = match o.kind {
            Kind::Delete => Mutation::Delete(name::Delete {
                bucket: BUCKET.into(),
                incarnation: 1,
                key: o.key.into(),
                versioning: o.versioning,
                named: None,
                if_match: None,
                at_ns: self.proposed(),
                bypass: false,
                owner: "o".into(),
            }),
            _ => Mutation::CreateUpload(CreateUpload {
                bucket: BUCKET.into(),
                incarnation: 1,
                key: o.key.into(),
                at_ns: self.proposed(),
                upload: asked(),
            }),
        };
        let operation = Operation {
            identity: self.scoped(op),
            horizon_ns: o.horizon_ns,
        };
        let d = Delivered {
            op,
            file: None,
            deadline_ns: u64::MAX,
            command: name::Command::Operated(Box::new(Operated {
                operation,
                mutation,
            })),
        };
        if self.deliver(&d).is_some() {
            self.delivered.push(d);
        }
    }

    /// An attempt of completion operation `op`: a composite file of the parts its upload holds as
    /// its client last learnt them, and an ETag naming the attempt, delivered now. A retry
    /// answered from the row gets the first's ETag; one rebuilt from what it carries would not.
    fn attempt_complete(&mut self, op: usize) {
        let Kind::Complete { upload } = &self.ops[op].kind else {
            return;
        };
        let key = self.ops[op].key;
        let Some(u) = self
            .uploaders
            .iter()
            .find(|u| u.key == key && &u.upload == upload)
        else {
            return;
        };
        if u.parts.is_empty() {
            return;
        }
        let (upload, parts) = (
            u.upload.clone(),
            u.parts.values().cloned().collect::<Vec<_>>(),
        );
        let composite = self.id();
        let deadline_ns = self.write_file(key, composite, composed(&parts));
        let base = completion(key, upload, parts, composite, deadline_ns, self.proposed());
        let complete = Complete {
            versioning: self.ops[op].versioning,
            etag: format!("m{composite}-{}", base.parts.len()),
            ..base
        };
        let operation = Operation {
            identity: self.scoped(op),
            horizon_ns: self.ops[op].horizon_ns,
        };
        let d = Delivered {
            op,
            file: Some(composite),
            deadline_ns,
            command: name::Command::Operated(Box::new(Operated {
                operation,
                mutation: Mutation::Complete(complete),
            })),
        };
        if self.deliver(&d).is_some() {
            self.delivered.push(d);
        }
    }

    /// A gateway writes a chunk and its block, for a file of `key`, as an attempt of operation
    /// `op` if it carries one.
    fn start(&mut self, key: &'static str, op: Option<usize>) {
        let (file, block) = (self.id(), self.id());
        let deadline_ns = self.write_block(file, block);
        let versioning = match op {
            Some(op) => self.ops[op].versioning,
            None => versioning(usize::try_from(file % 2).unwrap()),
        };
        self.gateways.push(Gateway {
            key,
            file,
            versioning,
            stage: Stage::Block { block, deadline_ns },
            op,
        });
    }

    /// A gateway that writes its block and its file at once, the one named first now.
    fn start_at_once(&mut self, key: &'static str, file_first: bool) {
        let (file, block) = (self.id(), self.id());
        let mut g = Gateway {
            key,
            file,
            versioning: versioning(usize::try_from(file % 2).unwrap()),
            stage: Stage::AtOnce {
                block,
                block_deadline_ns: None,
                file_deadline_ns: None,
            },
            op: None,
        };
        if file_first {
            self.write_file_at_once(&mut g);
        } else {
            self.write_block_at_once(&mut g);
        }
        self.gateways.push(g);
    }

    fn write_block_at_once(&mut self, g: &mut Gateway) {
        if let Stage::AtOnce {
            block,
            block_deadline_ns: ref mut deadline @ None,
            ..
        } = g.stage
        {
            *deadline = Some(self.write_block(g.file, block));
        }
    }

    /// The file's write, bounded by no block's deadline, since its block's write is out beside
    /// it: the gateway checks the block's once both have answered.
    fn write_file_at_once(&mut self, g: &mut Gateway) {
        if let Stage::AtOnce {
            block,
            file_deadline_ns: ref mut deadline @ None,
            ..
        } = g.stage
        {
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
                blocks_deadline_ns: u64::MAX,
                at_ns: self.proposed(),
            });
            let file::Outcome::Written { deadline_ns } = outcome else {
                panic!("{outcome:?}");
            };
            self.written.insert(g.file);
            *deadline = Some(deadline_ns);
        }
    }

    /// Writes a chunk of `block` and records the block for `file`, answering its deadline. The
    /// chunk goes with its block: a chunk no block names is reconciled per volume, which this
    /// simulation does not model.
    fn write_block(&mut self, file: u128, block: u128) -> u64 {
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
        deadline_ns
    }

    /// The identity a gateway hands over for operation `op`: the client's, scoped to the
    /// principal authentication established, the kind of write, and for a part its upload and
    /// part number, for a completion its upload (docs/design/gateway.md §2.1). Each piece takes
    /// a field of its own, so two identities agree only where every piece does, as a 128-bit
    /// hash of them does but for chance; a piece removed leaves its field empty. The key takes
    /// none: the range keeps each row under its object key (`key::operation`), so an upload's
    /// identity is scoped by its key there, and no gateway can remove that.
    fn scoped(&self, op: usize) -> u128 {
        let o = &self.ops[op];
        let (kind, upload, number) = match &o.kind {
            Kind::Put => (0, None, 0),
            Kind::Delete => (1, None, 0),
            Kind::Upload => (2, None, 0),
            Kind::Part { upload, number } => (3, Some(upload), *number),
            Kind::Complete { upload } => (4, Some(upload), 0),
        };
        let principal = match self.variant {
            Variant::NoScope => 0,
            _ => o.principal,
        };
        let number = match self.variant {
            Variant::NoPartNumber => 0,
            _ => number,
        };
        // An upload's ID is its initiation time (`key::version_id`).
        let upload = match (upload, self.variant) {
            (Some(upload), v) if v != Variant::NoUpload => key::parse_version_id(upload).unwrap(),
            _ => 0,
        };
        o.raw << 96 | principal << 88 | kind << 80 | u128::from(number) << 64 | u128::from(upload)
    }

    /// Delivers an attempt of an operation and judges the obligations its outcome bears on
    /// (docs/design/gateway.md §2.1); `None` while a merge in flight holds its key.
    fn deliver(&mut self, d: &Delivered) -> Option<name::Outcome> {
        let key = self.ops[d.op].key;
        let proposed = self.proposed();
        let before_ns = mantle_meta::clock::now(self.holder(key), proposed).unwrap();
        let within = self.ops[d.op]
            .effect
            .as_ref()
            .is_some_and(|e| before_ns <= e.expiry_ns);
        let rows = self.key_rows(key);
        let outcome = self.send(key, &d.command)?;
        let name::Command::Operated(operated) = &d.command else {
            panic!("an attempt carries its operation");
        };
        // What a delivery that went ahead answered, as an answer from its row says it.
        let effect = match (&outcome, &operated.mutation) {
            (name::Outcome::Put { version }, Mutation::Put(p)) => {
                Some(format!("{version} {}", p.version.etag))
            }
            (name::Outcome::Put { version }, Mutation::Complete(c)) => {
                Some(format!("{version} {}", c.etag))
            }
            (name::Outcome::Deleted { marker, version }, _) => {
                Some(format!("{marker} {version:?}"))
            }
            (name::Outcome::Created { upload }, _) => Some(upload.clone()),
            (name::Outcome::PartWritten, Mutation::PutPart(p)) => {
                Some(format!("{} {}", p.part.file, p.part.etag))
            }
            _ => None,
        };
        if let Some(answer) = effect {
            self.follow(d.op, &outcome, &operated.mutation);
            let now = mantle_meta::clock::now(self.holder(key), 0).unwrap();
            if let Some(e) = &self.ops[d.op].effect
                && now <= e.expiry_ns
            {
                self.broken.push(format!(
                    "1, one effect: operation {} took a second effect at {now}, within its \
                     first's horizon, to {}",
                    d.op, e.expiry_ns
                ));
            }
            let op = &mut self.ops[d.op];
            op.effect = Some(Effect {
                answer,
                expiry_ns: now.saturating_add(op.horizon_ns),
            });
            self.made.extend(d.file);
            let removed = match self.variant {
                Variant::NoRow => true,
                Variant::Rebuilt => matches!(operated.mutation, Mutation::Complete(_)),
                _ => false,
            };
            if removed {
                let (id, identity) = (self.holder_id(key), self.scoped(d.op));
                let range = self.names.get_mut(&id).unwrap();
                range.index += 1;
                let gone = Write::Delete(key::operation(BUCKET, key, identity));
                range.engine.apply(range.index, vec![gone]).unwrap();
            }
        }
        // Within its first effect's horizon an attempt finds the row and is answered from it:
        // a completion retried is answered so, never rebuilt from what it carries.
        if within && !matches!(outcome, name::Outcome::Answered(_)) {
            self.broken.push(format!(
                "2, one answer: an attempt of operation {} within its first's horizon was \
                 answered {outcome:?}, not from its row",
                d.op
            ));
        }
        if let name::Outcome::Answered(made) = &outcome {
            let answer = match made {
                Made::Version {
                    order, null, etag, ..
                } => format!("{} {etag}", name::shown_id(*null, *order)),
                Made::Deleted { marker, version } => format!("{marker} {version:?}"),
                Made::Upload { initiated_ns } => key::version_id(*initiated_ns),
                Made::Part { file, etag, .. } => format!("{file} {etag}"),
            };
            let first = self.ops[d.op].effect.as_ref().map(|e| e.answer.clone());
            if first.as_ref() != Some(&answer) {
                self.broken.push(format!(
                    "2, one answer: an attempt of operation {} was answered {answer}, its first \
                     delivery {first:?}",
                    d.op
                ));
            }
            // A copy of a delivery that made a version leaves its file to the version.
            if let Some(file) = d.file
                && !self.made.contains(&file)
                && !self.answered.contains_key(&file)
            {
                if self.queued(file).is_empty() {
                    self.broken.push(format!(
                        "4, released once: attempt file {file} answered from its row was not \
                         released"
                    ));
                }
                self.answered.insert(file, d.deadline_ns);
            }
        }
        if within && self.key_rows(key) != rows {
            self.broken.push(format!(
                "3, no stale replacement: an attempt of operation {} within its horizon changed \
                 {key}'s rows",
                d.op
            ));
        }
        Some(outcome)
    }

    /// What a client learns from operation `op`'s effect: an upload to put parts to, a part its
    /// upload holds, which a completion then lists, or its upload completed.
    fn follow(&mut self, op: usize, outcome: &name::Outcome, mutation: &Mutation) {
        let key = self.ops[op].key;
        match (outcome, mutation) {
            (name::Outcome::Created { upload }, _) => self.uploaders.push(Uploader {
                principal: self.ops[op].principal,
                raw: self.ops[op].raw,
                key,
                upload: upload.clone(),
                parts: BTreeMap::new(),
                completed: None,
            }),
            (name::Outcome::PartWritten, Mutation::PutPart(p)) => {
                if let Some(u) = self
                    .uploaders
                    .iter_mut()
                    .find(|u| u.key == key && u.upload == p.upload)
                {
                    let listed = Listed {
                        number: p.number,
                        etag: p.part.etag.clone(),
                        file: p.part.file,
                    };
                    u.parts.insert(p.number, listed);
                }
            }
            (name::Outcome::Put { .. }, Mutation::Complete(c)) => {
                if let Some(u) = self
                    .uploaders
                    .iter_mut()
                    .find(|u| u.key == key && u.upload == c.upload)
                {
                    u.completed = c.file;
                }
            }
            _ => {}
        }
    }

    /// The collector forgets, a page at a time, the operations Name range `id` holds whose
    /// horizon has passed at its time; with the horizon check removed, every one it reads.
    fn forget(&mut self, id: u64) {
        if self.variant == Variant::NoHorizon {
            let r = self.names.get_mut(&id).unwrap();
            let (mut from, to) = key::operations_all();
            let mut gone = Vec::new();
            while let Some((k, _)) = r.engine.next(&from, &to).unwrap() {
                from = k.clone();
                from.push(0);
                gone.push(Write::Delete(k));
            }
            r.index += 1;
            r.engine.apply(r.index, gone).unwrap();
            return;
        }
        // The range holds at most a row per operation, which pages of `PAGE` rows pass in fewer
        // pages than there are operations, and one more.
        let mut from = Vec::new();
        for _ in 0..=self.ops.len() {
            let forget = name::Command::Forget(Forget {
                from,
                budget: u32::try_from(PAGE).unwrap(),
                at_ns: self.proposed(),
            });
            match self.name(id, forget) {
                name::Outcome::Forgot { next: Some(next) } => from = next,
                // The last page; or a frozen range, which forgets nothing.
                _ => return,
            }
        }
    }

    /// The operation rows of every range that owns its span.
    fn operation_rows(&mut self) -> BTreeSet<(Vec<u8>, Vec<u8>)> {
        let mut out = BTreeSet::new();
        for range in self.owned() {
            let (mut from, to) = key::operations_all();
            while let Some((k, v)) = range.next(&from, &to).unwrap() {
                from = k.clone();
                from.push(0);
                out.insert((k, v));
            }
        }
        out
    }

    /// The rows of `key`'s versions, uploads and parts in the range that owns it.
    fn key_rows(&mut self, key: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        let range = self.holder(key);
        let (mut from, to) = key::bucket_span(BUCKET);
        let mut out = Vec::new();
        while let Some((k, v)) = range.next(&from, &to).unwrap() {
            from = k.clone();
            from.push(0);
            if key::decode_name(&k).is_some_and(|(_, of, _)| of == key) {
                out.push((k, v));
            }
        }
        out
    }

    /// The entries of the owners' queues of released files that release `file`.
    fn queued(&mut self, file: u128) -> Vec<name::Released> {
        self.owned()
            .into_iter()
            .flat_map(|r| name::released(r, u64::MAX, usize::MAX).unwrap())
            .filter(|q| q.file == file)
            .collect()
    }

    /// The gateway's next step: its file, or its handover.
    fn advance(&mut self, mut g: Gateway) {
        match g.stage {
            Stage::Block { block, deadline_ns } => {
                // The write takes the range's next instant.
                let late = self.proposed().max(last_instant(&mut self.files)) > deadline_ns;
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
            Stage::AtOnce {
                block_deadline_ns: Some(block_ns),
                file_deadline_ns: Some(file_ns),
                ..
            } => {
                // The File range wrote the file at its deadline less the handover. A block held
                // to an earlier time may have been released before the file was written, and the
                // gateway gives up, leaving both to the sweeps (`put::Put`'s check).
                if block_ns < file_ns - HANDOVER && self.variant != Variant::Unbounded {
                    return;
                }
                g.stage = Stage::File {
                    deadline_ns: file_ns,
                };
                if !self.handover(&g, file_ns) {
                    self.gateways.push(g);
                }
            }
            Stage::AtOnce {
                block_deadline_ns: None,
                ..
            } => {
                self.write_block_at_once(&mut g);
                self.gateways.push(g);
            }
            Stage::AtOnce {
                file_deadline_ns: None,
                ..
            } => {
                self.write_file_at_once(&mut g);
                self.gateways.push(g);
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

    /// Hands the file over, as its operation's attempt if it carries one; false while a merge in
    /// flight holds its key, so the gateway tries again later.
    fn handover(&mut self, g: &Gateway, deadline_ns: u64) -> bool {
        let put = Put {
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
                // Each attempt's own, so an answer names the attempt that made it.
                etag: if g.op.is_some() {
                    format!("e{}", g.file)
                } else {
                    "e".into()
                },
                size: 1,
                checksum: None,
                file: Some(g.file),
                owner: "o".into(),
                headers: Vec::new(),
                retention: None,
                legal_hold: None,
                listing: None,
            },
            default: None,
            id: 0,
            deadline_ns,
        };
        let proposed = self.proposed();
        let late = mantle_meta::clock::now(self.holder(g.key), proposed).unwrap() > deadline_ns;
        let outcome = match g.op {
            None => self.send(g.key, &name::Command::Put(put)),
            Some(op) => {
                let operation = Operation {
                    identity: self.scoped(op),
                    horizon_ns: self.ops[op].horizon_ns,
                };
                let mutation = match &self.ops[op].kind {
                    Kind::Part { upload, number } => Mutation::PutPart(PutPart {
                        bucket: BUCKET.into(),
                        incarnation: 1,
                        key: g.key.into(),
                        upload: upload.clone(),
                        number: *number,
                        part: Part {
                            // Each attempt's own, so an answer names the attempt that made it.
                            etag: format!("p{}", g.file),
                            size: name::MIN_PART,
                            checksum: None,
                            file: g.file,
                            modified_ns: 0,
                            deadline_ns,
                        },
                        at_ns: self.proposed(),
                        run: None,
                    }),
                    _ => Mutation::Put(put),
                };
                let d = Delivered {
                    op,
                    file: Some(g.file),
                    deadline_ns,
                    command: name::Command::Operated(Box::new(Operated {
                        operation,
                        mutation,
                    })),
                };
                let outcome = self.deliver(&d);
                if outcome.is_some() {
                    self.delivered.push(d);
                }
                outcome
            }
        };
        match outcome {
            None => return false,
            Some(name::Outcome::Put { .. } | name::Outcome::PartWritten) => {
                assert!(!late, "a late handover was taken");
            }
            Some(name::Outcome::Expired) => assert!(late, "a handover in time was refused"),
            Some(name::Outcome::Answered(_)) if g.op.is_some() => {}
            // A part of an upload completed or aborted, its file released as a refused write's.
            Some(name::Outcome::NoSuchUpload) if g.op.is_some() => {}
            Some(other) => panic!("{other:?}"),
        }
        true
    }

    /// One step of sweep `i`: delivers the answer to the request it sent, or sends its next.
    fn sweep_step(&mut self, i: usize) {
        if let Some(request) = self.sent[i].take() {
            // A key a merge in flight holds leaves the request to be delivered again.
            let Some(answer) = self.serve(request.clone()) else {
                self.sent[i] = Some(request);
                return;
            };
            if let Some(sweep) = self.sweeps[i].as_mut() {
                sweep.answer(answer).unwrap();
                if sweep.is_done() {
                    self.sweeps[i] = None;
                }
            }
            return;
        }
        let sweep = self.sweeps[i].get_or_insert_with(|| Sweep::new(PAGE));
        match sweep.next() {
            Some(request) => self.sent[i] = Some(request),
            None => self.sweeps[i] = None,
        }
    }

    /// One step of the collector: takes the oldest file some Name range released, or moves
    /// its reclaimer on with the answer to its next request (`mantle_meta::reclaim`).
    fn collect_step(&mut self) {
        let Some(c) = self.collector.as_mut() else {
            for (id, _) in self.owners() {
                let queue =
                    name::released(&mut self.names.get_mut(&id).unwrap().engine, u64::MAX, 1)
                        .unwrap();
                if let Some(released) = queue.into_iter().next() {
                    self.collector = Some(Collecting {
                        range: id,
                        file: released.file,
                        reclaimer: Reclaimer::new(released, 2),
                    });
                    break;
                }
            }
            return;
        };
        let Some(request) = c.reclaimer.next() else {
            self.collector = None;
            return;
        };
        let range = c.range;
        let answer = match request {
            reclaim::Request::Extents { file, from, max } => {
                reclaim::Answer::Extents(file::extents(&mut self.files, file, from, max).unwrap())
            }
            reclaim::Request::Block(b) => {
                reclaim::Answer::Block(block::read(&mut self.blocks, b).unwrap())
            }
            reclaim::Request::Chunk(place) => {
                self.volumes.remove(&slot(&place));
                reclaim::Answer::Chunk
            }
            reclaim::Request::BlockCommand(c) => reclaim::Answer::BlockOutcome(self.block(c)),
            reclaim::Request::FileCommand(c) => reclaim::Answer::FileOutcome(self.file(c)),
            reclaim::Request::Disown(d) => {
                let key = d.key.clone();
                match self.send(&key, &name::Command::Disown(d)) {
                    Some(outcome) => reclaim::Answer::NameOutcome(outcome),
                    None => return,
                }
            }
            reclaim::Request::Unmark(u) => {
                let key = u.files[0].key.clone();
                match self.send(&key, &name::Command::Unmark(u)) {
                    Some(outcome) => reclaim::Answer::NameOutcome(outcome),
                    None => return,
                }
            }
            reclaim::Request::Reclaim(r) => match self.name(range, name::Command::Reclaim(r)) {
                outcome @ name::Outcome::Reclaimed => reclaim::Answer::NameOutcome(outcome),
                // The range froze or ended, its queue going with its merge: the collector
                // takes the row again from where it went.
                _ => {
                    self.collector = None;
                    return;
                }
            },
        };
        let c = self.collector.as_mut().unwrap();
        c.reclaimer.step(answer).unwrap();
        if c.reclaimer.is_done() {
            self.reclaimed.insert(c.file);
            self.collector = None;
        }
    }

    /// A client creates an upload of `key` carrying no operation. Its runs carry the one
    /// identity this client carries on every upload it creates, which only the scope by upload
    /// keeps apart (docs/design/gateway.md §2.1); a client never draws 0 otherwise.
    fn create_upload(&mut self, key: &'static str) {
        let create = name::Command::CreateUpload(CreateUpload {
            bucket: BUCKET.into(),
            incarnation: 1,
            key: key.into(),
            at_ns: self.proposed(),
            upload: asked(),
        });
        if let Some(name::Outcome::Created { upload }) = self.send(key, &create) {
            self.uploaders.push(Uploader {
                principal: 0,
                raw: 0,
                key,
                upload,
                parts: BTreeMap::new(),
                completed: None,
            });
        }
    }

    /// Writes a block of one chunk for `file`, and `file` of `extents` for `key`; the file's
    /// handover deadline.
    fn write_file(&mut self, key: &str, file: u128, extents: Vec<Extent>) -> u64 {
        let outcome = self.file(file::Command::Write {
            file,
            extents,
            referrer: Referrer {
                bucket: BUCKET.into(),
                incarnation: 1,
                key: key.into(),
            },
            key: None,
            handover_ns: HANDOVER,
            blocks_deadline_ns: u64::MAX,
            at_ns: self.proposed(),
        });
        let file::Outcome::Written { deadline_ns } = outcome else {
            panic!("{outcome:?}");
        };
        self.written.insert(file);
        deadline_ns
    }

    fn upload_part(&mut self, i: usize) {
        if self.uploaders[i].completed.is_some() || self.uploaders[i].parts.len() >= 3 {
            return;
        }
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
        assert!(matches!(outcome, block::Outcome::Written { .. }));
        let key = self.uploaders[i].key;
        let deadline_ns = self.write_file(
            key,
            file,
            vec![Extent {
                length: 1,
                target: Target::Block(block),
            }],
        );
        let number = u16::try_from(self.uploaders[i].parts.len() + 1).unwrap();
        let put = name::Command::PutPart(PutPart {
            bucket: BUCKET.into(),
            incarnation: 1,
            key: key.into(),
            upload: self.uploaders[i].upload.clone(),
            number,
            part: Part {
                etag: format!("p{number}"),
                size: name::MIN_PART,
                checksum: None,
                file,
                modified_ns: 0,
                deadline_ns,
            },
            at_ns: self.proposed(),
            run: None,
        });
        // A part refused, or held up by a merge, is released or left to the sweep.
        if let Some(name::Outcome::PartWritten) = self.send(key, &put) {
            let listed = Listed {
                number,
                etag: format!("p{number}"),
                file,
            };
            self.uploaders[i].parts.insert(number, listed);
        }
    }

    /// Completes uploader `i`'s upload with a composite file of its parts; a retry makes the
    /// composite again for a completion that committed.
    fn complete(&mut self, i: usize, refuse: bool, retry: bool) {
        let u = &self.uploaders[i];
        if u.parts.is_empty() || u.completed.is_some() != retry {
            return;
        }
        let (key, upload) = (u.key, u.upload.clone());
        let parts: Vec<Listed> = u.parts.values().cloned().collect();
        let composite = self.id();
        let deadline_ns = self.write_file(key, composite, composed(&parts));
        let complete = name::Command::Complete(Complete {
            preconditions: Preconditions {
                if_match: refuse.then(|| Match::Tags(vec!["nope".into()])),
                if_none_match: None,
            },
            ..completion(key, upload, parts, composite, deadline_ns, self.proposed())
        });
        if let Some(name::Outcome::Put { .. }) = self.send(key, &complete)
            && !retry
        {
            self.uploaders[i].completed = Some(composite);
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
                let proposed = self.proposed();
                let now = mantle_meta::clock::now(&mut self.files, proposed).unwrap();
                Answer::Due(file::unsettled(&mut self.files, now, max).unwrap())
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
        })
    }

    /// What the Block and File ranges and the volumes answer a block sweep's request.
    fn serve_blocks(&mut self, request: BlockRequest) -> BlockAnswer {
        match request {
            BlockRequest::Due { max } => {
                let proposed = self.proposed();
                let now = mantle_meta::clock::now(&mut self.blocks, proposed).unwrap();
                BlockAnswer::Due(block::unsettled(&mut self.blocks, now, max).unwrap())
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
            BlockRequest::Block(b) => BlockAnswer::Block(block::read(&mut self.blocks, b).unwrap()),
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

    /// Files the Name ranges hold: each a version or an upload's part references, and each
    /// part a referenced composite adopted.
    fn referenced(&mut self) -> BTreeSet<u128> {
        let mut out = BTreeSet::new();
        for range in self.owned() {
            let (mut from, to) = key::bucket_span(BUCKET);
            while let Some((k, v)) = range.next(&from, &to).unwrap() {
                match key::decode_name(&k) {
                    Some((_, _, NameRow::Version(_))) => {
                        out.extend(Version::decode(&v).unwrap().file);
                    }
                    Some((_, _, NameRow::Part(..))) => {
                        out.insert(Part::decode(&v).unwrap().file);
                    }
                    _ => {}
                }
                from = k;
                from.push(0);
            }
        }
        let composites: Vec<u128> = out.iter().copied().collect();
        for f in composites {
            for (_, e) in file::extents(&mut self.files, f, 0, 16).unwrap() {
                if let Target::File(part) = e.target {
                    out.insert(part);
                }
            }
        }
        out
    }

    fn released(&mut self) -> BTreeSet<u128> {
        self.owned()
            .into_iter()
            .flat_map(|r| name::released(r, u64::MAX, usize::MAX).unwrap())
            .map(|r| r.file)
            .collect()
    }

    fn unsettled(&mut self) -> BTreeSet<u128> {
        file::unsettled(&mut self.files, u64::MAX, usize::MAX)
            .unwrap()
            .into_iter()
            .map(|u| u.file)
            .collect()
    }

    fn unsettled_blocks(&mut self) -> Vec<block::Unsettled> {
        block::unsettled(&mut self.blocks, u64::MAX, usize::MAX).unwrap()
    }

    /// Blocks the written files name.
    fn named(&mut self) -> BTreeSet<u128> {
        self.written
            .iter()
            .flat_map(|&f| file::extents(&mut self.files, f, 0, 16).unwrap())
            .filter_map(|(_, e)| match e.target {
                Target::Block(b) => Some(b),
                Target::File(_) => None,
            })
            .collect()
    }

    /// Blocks with rows in the Block range.
    fn recorded(&mut self) -> BTreeSet<u128> {
        (0..=self.next_id)
            .filter(|&b| block::read(&mut self.blocks, b).unwrap().is_some())
            .collect()
    }

    /// Files marked in any Name range, under whatever key.
    fn marked(&mut self) -> BTreeSet<u128> {
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

    fn check(&mut self) {
        let (referenced, released, unsettled) =
            (self.referenced(), self.released(), self.unsettled());
        let lost: Vec<_> = referenced.intersection(&released).collect();
        assert!(lost.is_empty(), "files referenced and released: {lost:?}");
        // A released composite's parts stay its own until the collector gives them back.
        let giving_back: BTreeSet<u128> = released
            .iter()
            .flat_map(|&f| file::extents(&mut self.files, f, 0, 16).unwrap())
            .filter_map(|(_, e)| match e.target {
                Target::File(part) => Some(part),
                Target::Block(_) => None,
            })
            .collect();
        for f in &self.written {
            assert!(
                referenced.contains(f)
                    || released.contains(f)
                    || unsettled.contains(f)
                    || self.reclaimed.contains(f)
                    || giving_back.contains(f),
                "file {f} is nowhere"
            );
        }
        // Everything a held file names resolves to bytes: its rows, its blocks' rows and their
        // chunks (audits B01, B02).
        let chunked: BTreeSet<u128> = self.volumes.iter().map(|&(_, b, _, _)| b).collect();
        for &f in &referenced {
            assert!(
                file::header(&mut self.files, f).unwrap().is_some(),
                "file {f}, held, was taken apart"
            );
            for (_, e) in file::extents(&mut self.files, f, 0, 16).unwrap() {
                if let Target::Block(b) = e.target {
                    assert!(
                        block::read(&mut self.blocks, b).unwrap().is_some(),
                        "block {b} of held file {f} was taken apart"
                    );
                    assert!(
                        chunked.contains(&b),
                        "the chunk of block {b} of held file {f} was deleted"
                    );
                }
            }
        }
        // A file an attempt answered from its row made is released once while its deadline
        // stands. Past it the range releases a file unmarked, and a copy may release it again,
        // which the reclaimer finds gone (`name::apply`).
        let answered: Vec<(u128, u64)> = self.answered.iter().map(|(&f, &d)| (f, d)).collect();
        for (f, deadline_ns) in answered {
            let within = self
                .queued(f)
                .iter()
                .filter(|q| q.released_ns <= deadline_ns)
                .count();
            if within > 1 {
                self.broken.push(format!(
                    "4, released once: attempt file {f} is released {within} times within its \
                     deadline"
                ));
            }
        }
        if self.variant == Variant::Whole {
            assert!(
                self.broken.is_empty(),
                "obligations broken: {:?}",
                self.broken
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
            let rows = self.operation_rows();
            self.serve_merge(request);
            self.carried(&rows, "a late merge request");
            self.check();
        }
        self.resume_merges();
        for turn in 0..10_000 {
            if self.mergers.is_empty() {
                break;
            }
            let rows = self.operation_rows();
            self.merge_step(turn % self.mergers.len());
            self.carried(&rows, "a resumed merge's step");
            self.check();
        }
        assert!(self.mergers.is_empty(), "a merge never finished");
        while let Some(g) = self.gateways.pop() {
            self.clock += 10;
            self.advance(g);
            self.check();
        }
        // Stopped sweeps' requests arrive, late.
        while let Some(request) = self.late_sweeps.pop() {
            self.serve(request).unwrap();
            self.check();
        }
        self.clock += 10 * HANDOVER;
        for _ in 0..10_000 {
            let quiet = self.unsettled().is_empty()
                && self.sweeps.iter().all(Option::is_none)
                && self.sent.iter().all(Option::is_none)
                && self.unsettled_blocks().is_empty()
                && self.block_sweep.is_none();
            if quiet {
                break;
            }
            self.clock += 10;
            // A sweep in flight goes on; a new one starts only while files wait.
            for i in 0..2 {
                let busy = self.sweeps[i].is_some() || self.sent[i].is_some();
                if busy || !self.unsettled().is_empty() {
                    self.sweep_step(i);
                }
            }
            if self.block_sweep.is_some() || !self.unsettled_blocks().is_empty() {
                self.block_sweep_step();
            }
            self.check();
        }
        // The collector takes every released file apart, each checked once it is done: the
        // steps between were checked as they came during the schedule.
        for _ in 0..100_000 {
            if self.collector.is_none() && self.released().is_empty() {
                break;
            }
            let before = self.reclaimed.len();
            self.collect_step();
            if self.reclaimed.len() != before {
                self.check();
            }
        }
        assert!(self.unsettled().is_empty(), "files left unsettled");
        assert!(self.unsettled_blocks().is_empty(), "blocks left unsettled");
        assert!(self.released().is_empty(), "files left released");
        let referenced = self.referenced();
        for f in &self.written {
            assert!(
                referenced.contains(f) != self.reclaimed.contains(f),
                "file {f}: referenced {}, reclaimed {}",
                referenced.contains(f),
                self.reclaimed.contains(f)
            );
        }
        let released = self.released();
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
fn last_instant(rows: &mut Model) -> u64 {
    mantle_meta::clock::now(rows, 0).unwrap() + 1
}

/// An upload as CreateMultipartUpload asks for it: its initiation time is the commit's.
fn asked() -> Upload {
    Upload {
        initiated_ns: 0,
        owner: "o".into(),
        headers: Vec::new(),
        checksum: None,
        retention: None,
        legal_hold: None,
        runs: None,
    }
}

/// The extents of a composite file of `parts`, in their order.
fn composed(parts: &[Listed]) -> Vec<Extent> {
    parts
        .iter()
        .map(|p| Extent {
            length: 1,
            target: Target::File(p.file),
        })
        .collect()
}

/// The completion of `upload` of `key` listing `parts`, combined into file `composite`: its
/// ETag names their number, as the range checks (`name::complete`).
fn completion(
    key: &str,
    upload: String,
    parts: Vec<Listed>,
    composite: u128,
    deadline_ns: u64,
    at_ns: u64,
) -> Complete {
    Complete {
        bucket: BUCKET.into(),
        incarnation: 1,
        key: key.into(),
        upload,
        versioning: Versioning::Unversioned,
        preconditions: Preconditions::default(),
        at_ns,
        etag: format!("m-{}", parts.len()),
        size: name::MIN_PART * parts.len() as u64,
        parts,
        checksum: None,
        file: Some(composite),
        default: None,
        id: 0,
        deadline_ns,
        listing: [0; mantle_meta::record::LISTING],
        native: false,
    }
}

/// The versioning the `n`th writer puts under: versions kept and replaced, alternately.
fn versioning(n: usize) -> Versioning {
    if n.is_multiple_of(2) {
        Versioning::Enabled
    } else {
        Versioning::Unversioned
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    #[test]
    fn what_a_gateway_never_handed_over_is_released_and_nothing_handed_over_is(
        actions in prop::collection::vec(any_action(), 1..STEPS),
    ) {
        let mut world = World::new();
        for action in &actions {
            world.act(action);
            world.check();
        }
        world.finish();
    }
}

/// With each piece of the operation record removed in turn, some schedule breaks one of its
/// obligations. The schedules are drawn by a runner of fixed seed, so every run judges the same
/// ones, and from the simulation's own budget: a variant it could not break within that budget,
/// it could not catch.
#[test]
fn each_piece_of_the_operation_record_removed_is_caught() {
    for variant in [
        Variant::NoRow,
        Variant::NoScope,
        Variant::NoHorizon,
        Variant::NoPartNumber,
        Variant::NoUpload,
        Variant::Rebuilt,
    ] {
        let mut runner = TestRunner::deterministic();
        let schedules = prop::collection::vec(action(), 1..STEPS);
        let caught = (0..CASES).find_map(|case| {
            let actions = schedules.new_tree(&mut runner).unwrap().current();
            let mut world = World::with(variant);
            for action in &actions {
                world.act(action);
                world.check();
                if let Some(broken) = world.broken.first() {
                    return Some((case, broken.clone()));
                }
            }
            None
        });
        let Some((case, broken)) = caught else {
            panic!("{variant:?}: no schedule of {CASES} broke an obligation");
        };
        println!("{variant:?}: schedule {case} broke {broken}");
    }
}

/// An upload, a native client's part and a completion, each retried through another gateway
/// within its horizon and the completion's attempt copied, spelled out: every retry is answered
/// from its row with the first's upload, part file and ETag, or version and ETag, though each
/// part and completion attempt carries its own file and ETag. With the completion's row
/// removed, its retry is rebuilt and the simulation says so.
#[test]
fn a_retried_upload_part_and_completion_are_answered_from_their_rows() {
    let schedule = [
        Action::OperateUpload {
            key: 0,
            principal: 0,
            horizon: 1,
        },
        Action::Attempt(0),
        Action::Attempt(0),
        Action::OperatePart {
            uploader: 0,
            number: 1,
            principal: 1,
            horizon: 1,
            native: true,
        },
        Action::Attempt(1),
        Action::AdvanceAttempt(0),
        Action::AdvanceAttempt(0),
        Action::Attempt(1),
        Action::AdvanceAttempt(0),
        Action::AdvanceAttempt(0),
        Action::OperateComplete {
            uploader: 0,
            principal: 1,
            horizon: 1,
        },
        Action::Attempt(2),
        Action::Attempt(2),
        Action::Copy(5),
    ];
    let mut w = World::new();
    for action in &schedule {
        w.act(action);
        w.check();
    }
    assert_eq!(w.uploaders.len(), 1, "a retried upload made a second");
    let u = &w.uploaders[0];
    assert_eq!(u.parts.len(), 1);
    assert!(u.completed.is_some());
    // The part's and the completion's retries, each answered from its row, released their own
    // files; the upload's retry carried none, and the copy's file is the retry's.
    assert_eq!(w.answered.len(), 2, "{:?}", w.answered);
    let first = |w: &World, op: usize| w.ops[op].effect.as_ref().unwrap().answer.clone();
    assert_eq!(first(&w, 0), u.upload);
    assert!(first(&w, 2).ends_with(&format!("m{}-1", u.completed.unwrap())));
    w.finish();

    let mut w = World::with(Variant::Rebuilt);
    for action in &schedule {
        w.act(action);
        w.check();
    }
    assert!(
        w.broken
            .first()
            .is_some_and(|b| b.contains("operation 2 within its first's horizon")),
        "{:?}",
        w.broken
    );
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
    // Before the deadlines the sweeps find nothing due: a request sent, and its answer.
    w.act(&Action::BlockSweep);
    w.act(&Action::Sweep(0));
    w.act(&Action::Sweep(0));
    assert!(w.sweeps[0].is_none() && w.block_sweep.is_none());
    w.act(&Action::Wait);
    w.act(&Action::Wait);
    for _ in 0..24 {
        w.act(&Action::Sweep(0));
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

/// A gateway writes its block and its file at once, and the file arrives only after the block
/// sweep, the File range finding no file past the block's deadline, released the block. The
/// gateway finds the block held to an earlier time than the file's write and gives up, and the
/// sweeps collect both (docs/design/gateway.md §2.2).
fn released_before_its_file(variant: Variant) {
    let mut w = World::with(variant);
    w.act(&Action::WriteAtOnce {
        key: 0,
        file_first: false,
    });
    w.act(&Action::Wait);
    w.act(&Action::Wait);
    for _ in 0..6 {
        w.act(&Action::BlockSweep);
    }
    w.act(&Action::Advance(0));
    let file = *w.written.first().expect("the file's write was refused");
    w.act(&Action::Advance(0));
    w.check();
    assert!(w.gateways.is_empty());
    assert!(!w.referenced().contains(&file), "the file was handed over");
    w.finish();
}

#[test]
fn a_file_written_after_its_block_was_released_is_never_handed_over() {
    released_before_its_file(Variant::Whole);
}

/// Without the gateway's check of the block's deadline against the file's write, the file is
/// handed over and a version names a block the sweep took apart.
#[test]
#[should_panic(expected = "of held file")]
fn without_the_check_a_version_names_a_released_block() {
    released_before_its_file(Variant::Unbounded);
}

/// Two sweeps read the same page. The first finds the file taken, settles it and is done;
/// the second, delayed, asks after that. The file a version references stays held
/// (audit B01).
#[test]
fn a_delayed_sweep_never_releases_a_file_another_settled() {
    let mut w = World::new();
    w.act(&Action::Write { key: 0 });
    w.act(&Action::Advance(0));
    w.act(&Action::Advance(0));
    assert_eq!(w.referenced().len(), 1);
    w.act(&Action::Wait);
    w.act(&Action::Wait);
    let mut first = Sweep::new(PAGE);
    let mut second = Sweep::new(PAGE);
    for sweep in [&mut first, &mut second] {
        let read = sweep.next().unwrap();
        let answer = w.serve(read).unwrap();
        sweep.answer(answer).unwrap();
    }
    while let Some(request) = first.next() {
        let answer = w.serve(request).unwrap();
        first.answer(answer).unwrap();
        if first.is_done() {
            break;
        }
    }
    while let Some(request) = second.next() {
        let answer = w.serve(request).unwrap();
        second.answer(answer).unwrap();
        w.check();
        if second.is_done() {
            break;
        }
    }
    w.check();
    w.finish();
}

/// A gateway writes its file before its block (CI run 44286448798, as proptest shrank it): file
/// 8 names block 9, whose write lands only once the file sweep has released the file and the
/// collector, reclaiming it, has found the block unrecorded and passed it. The block sweep asks
/// about the block before the collector deletes the file. Had the file still named the block,
/// the sweep would have settled it as the file's, and with the file's rows gone nothing would
/// have reclaimed it; the collector released the file to the block sweep before reading its
/// blocks, so the sweep releases the block past its deadline (docs/design/gateway.md §2.2).
#[test]
fn a_block_landing_after_the_collector_passed_it_is_released() {
    let schedule = [
        Action::Sweep(0),
        Action::Write { key: 0 },
        Action::WriteAtOnce {
            key: 0,
            file_first: false,
        },
        Action::Write { key: 0 },
        Action::WriteAtOnce {
            key: 0,
            file_first: true,
        },
        Action::Split { range: 0, cut: 0 },
        Action::Write { key: 0 },
        Action::Split { range: 0, cut: 0 },
        Action::Split { range: 0, cut: 0 },
        Action::LateSweep(0),
        Action::Sweep(0),
        Action::Sweep(0),
        Action::Write { key: 0 },
        Action::Split { range: 0, cut: 0 },
        Action::Sweep(0),
        Action::CollectorStops,
        Action::Collect,
        Action::Collect,
        Action::Collect,
    ];
    let mut w = World::new();
    for action in &schedule {
        w.act(action);
        w.check();
    }
    assert_eq!(w.released(), BTreeSet::from([8]));
    // The collector goes on until it has passed every block of the file, its next step the
    // file's rows: a step or two, the steps each block takes at most.
    let passed = |w: &World| {
        w.collector.as_ref().is_some_and(|c| {
            c.file == 8
                && matches!(
                    c.reclaimer.next(),
                    Some(reclaim::Request::FileCommand(file::Command::Delete { .. }))
                )
        })
    };
    for _ in 0..2 {
        if !passed(&w) {
            w.act(&Action::Collect);
            w.check();
        }
    }
    assert!(passed(&w), "the collector never passed file 8's blocks");
    assert!(
        !w.recorded().contains(&9),
        "block 9 landed before the collector passed it"
    );
    // The gateway's block write lands, and the sweeps and the collector run to their end.
    w.finish();
}
