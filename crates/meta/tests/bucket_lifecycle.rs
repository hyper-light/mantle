#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

//! Creating and deleting one bucket across a Bucket range and Name ranges that split and merge
//! while it happens (docs/design/metadata.md §2–§3), driven by the coordinator the gateway
//! runs (`mantle_meta::coordinator`), under schedules proptest chooses: coordinators that stall
//! and are taken over between any two of their reads and commands, collectors that resume
//! deletes left behind, whether or not the collector's schedule calls for it, writers whose
//! view of the bucket is stale, splits the directory and the writers learn of late, and merges
//! whose drivers (`mantle_meta::merge`) stop, are resumed from either range, give up, and
//! whose last command arrives late, as in docs/models/RangeSplit.tla. After every step, the
//! ranges that own their spans divide the keys between them and hold rows only of their own,
//! no write a Name range acknowledged is lost to a delete, an active bucket's every range has
//! its gate open, and a forgotten bucket leaves no gate behind. Once the schedule's faults
//! stop, every merge in flight ends, the directory catches up, every coordinator runs and the
//! collector acts on its schedule, and every create and delete comes to an end: the bucket
//! active, or its name forgotten.

use std::collections::{BTreeMap, BTreeSet};

use mantle_meta::bucket::{self, Create};
use mantle_meta::collector::{Schedule, Takeover};
use mantle_meta::coordinator::{Answer, Bounds, Coordinator, Request, Settled};
use mantle_meta::engine::{Engine, Model};
use mantle_meta::key::{self, NameRow};
use mantle_meta::merge::{self, Merger};
use mantle_meta::name::{self, Delete, Preconditions, Put, Split};
use mantle_meta::record::{
    BucketState, Descriptor, GateState, Lineage, Standing, Version, Versioning,
};
use proptest::prelude::*;

const BUCKET: &str = "b";
/// The keys writers use.
const KEYS: [&str; 4] = ["a", "f", "m", "t"];
/// Where ranges may split: the bucket's first routing key, between and at its keys, and the
/// next bucket's first, so some ranges hold none of the bucket's keys.
const CUTS: [(&str, &str); 7] = [
    (BUCKET, ""),
    (BUCKET, "c"),
    (BUCKET, "f"),
    (BUCKET, "m"),
    (BUCKET, "p"),
    (BUCKET, "t"),
    ("c", ""),
];

#[derive(Debug, Clone)]
enum Action {
    /// A CreateBucket request.
    Create,
    /// A DeleteBucket request.
    Delete,
    /// The collector abandoning a create left unfinished.
    Abandon,
    /// The collector resuming a deleted bucket's cleanup.
    Collect,
    /// The collector acting on its schedule: taking over every attempt that has gone its
    /// patience without progress.
    Collector,
    /// One step of one coordinator in flight.
    Step(usize),
    /// A coordinator's gateway stops for good, leaving its attempt where it was.
    Crash(usize),
    /// A writer puts or removes a key under one of the incarnations it has seen, routed by
    /// the descriptors it holds.
    Put {
        key: usize,
        view: usize,
    },
    Remove {
        key: usize,
        view: usize,
    },
    /// A writer reads the bucket's row and learns its incarnation while it is active.
    Look,
    /// A range splits at one of the cuts, if the cut falls inside it.
    Split {
        range: usize,
        cut: usize,
    },
    /// The directory learns every range's descriptor.
    Publish,
    /// The writers take the directory's descriptors.
    Refresh,
    /// A driver begins merging a range into the one just below it, as the directory
    /// describes them, taking at most `rows` rows: few enough that some merges are refused.
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
        1 => Just(Action::Create),
        1 => Just(Action::Delete),
        1 => Just(Action::Abandon),
        1 => Just(Action::Collect),
        1 => Just(Action::Collector),
        8 => (0usize..4).prop_map(Action::Step),
        1 => (0usize..4).prop_map(Action::Crash),
        3 => (0usize..4, 0usize..3).prop_map(|(key, view)| Action::Put { key, view }),
        2 => (0usize..4, 0usize..3).prop_map(|(key, view)| Action::Remove { key, view }),
        2 => Just(Action::Look),
        1 => (0usize..8, 0..CUTS.len()).prop_map(|(range, cut)| Action::Split { range, cut }),
        1 => Just(Action::Publish),
        1 => Just(Action::Refresh),
        1 => (0usize..8, 0u64..8).prop_map(|(range, rows)| Action::Merge { range, rows }),
        4 => (0usize..4).prop_map(Action::MergeStep),
        1 => (0usize..4).prop_map(Action::MergeAbandon),
        1 => (0usize..4).prop_map(Action::MergeStop),
        1 => (0usize..4).prop_map(Action::Late),
        1 => Just(Action::MergeResume),
    ]
}

/// Rows a merge resumed after its driver stopped may take.
const MAX_ROWS: u64 = 64;

/// Rows a coordinator's read or collection passes or removes at a time: one, so every paused
/// read and partial collection is exercised; and ranges the cell holds, more than the
/// simulation's splits make.
const BOUNDS: Bounds = Bounds {
    budget: 1,
    ranges: 64,
};

/// The collector's waits, in the simulation's clock, which moves 10 a step: an attempt whose
/// coordinator has not stepped for ten steps is taken over.
const SCHEDULE: Schedule = Schedule {
    grace_ns: 1_000,
    patience_ns: 100,
};

/// A Name range: its rows, and the last entry its log applied.
struct Range {
    engine: Model,
    index: u64,
}

struct World {
    buckets: Model,
    bucket_index: u64,
    /// Every Name range, by ID.
    names: BTreeMap<u64, Range>,
    /// The ID the next split gives its child.
    next_id: u64,
    /// The descriptors the directory holds, as of its last publication.
    directory: Vec<Descriptor>,
    /// The descriptors the writers route by.
    cache: Vec<Descriptor>,
    /// Merges in flight.
    mergers: Vec<Merger>,
    /// Requests of stopped merge drivers, not yet arrived.
    late: Vec<merge::Request>,
    /// The merges a lower range took, by its ID and the generation the merge named.
    taken: BTreeSet<(u64, u64)>,
    clock: u64,
    coordinators: Vec<Coordinator>,
    /// Incarnations writers have seen active, oldest first.
    views: Vec<u64>,
    /// Keys whose last acknowledged write was a put, and the incarnation it was made under.
    acked: BTreeMap<&'static str, u64>,
    /// What each finished attempt told its request, in the order they finished.
    settled: Vec<Option<Settled>>,
}

impl World {
    /// A cell whose first Name range split at "m" before anything else happened, the
    /// directory and the writers knowing both halves.
    fn new() -> Self {
        let mut first = Model::default();
        first.install(0, name::first(1).unwrap()).unwrap();
        let mut w = Self {
            buckets: Model::default(),
            bucket_index: 0,
            names: BTreeMap::from([(
                1,
                Range {
                    engine: first,
                    index: 0,
                },
            )]),
            next_id: 2,
            directory: Vec::new(),
            cache: Vec::new(),
            mergers: Vec::new(),
            late: Vec::new(),
            taken: BTreeSet::new(),
            clock: 0,
            coordinators: Vec::new(),
            views: Vec::new(),
            acked: BTreeMap::new(),
            settled: Vec::new(),
        };
        w.split(1, key::route(BUCKET, "m"));
        w.act(&Action::Publish);
        w.act(&Action::Refresh);
        w
    }

    fn bucket(&mut self, command: bucket::Command) -> bucket::Outcome {
        self.bucket_index += 1;
        bucket::apply(&mut self.buckets, self.bucket_index, &command).unwrap()
    }

    fn name(&mut self, id: u64, command: name::Command) -> name::Outcome {
        let range = self.names.get_mut(&id).unwrap();
        range.index += 1;
        name::apply(&mut range.engine, range.index, &command).unwrap()
    }

    fn row(&self) -> Option<mantle_meta::record::Bucket> {
        bucket::read(&self.buckets, BUCKET).unwrap()
    }

    fn lineage(&self, id: u64) -> Lineage {
        name::lineage(&self.names[&id].engine).unwrap()
    }

    /// Splits range `id` at `at` as its replicas would: the child starts from the rows the
    /// split names, taken from the parent as they stand, and then the parent applies the split.
    fn split(&mut self, id: u64, at: Vec<u8>) {
        let parent = &self.names[&id].engine;
        let s = Split {
            generation: name::lineage(parent).unwrap().now.generation,
            at,
            child: self.next_id,
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
        self.names.insert(self.next_id, Range { engine, index: 0 });
        self.next_id += 1;
    }

    /// The attempt `outcome` started, if it started one; any other outcome answers the request
    /// with an error, and nothing is left in flight.
    fn start(&mut self, outcome: bucket::Outcome) {
        if let Some(c) = Coordinator::start(BUCKET, &outcome, &self.directory, BOUNDS).unwrap() {
            self.coordinators.push(c);
        }
    }

    /// What a Bucket or Name range, or the directory, answers a coordinator's request.
    fn serve(&mut self, request: Request) -> Answer {
        match request {
            Request::Bucket(command) => Answer::Bucket(self.bucket(command)),
            Request::Name { range, command } => Answer::Name(self.name(range, *command)),
            Request::Gate { range, generation } => Answer::Gate(
                name::read_gate(&self.names[&range].engine, BUCKET, generation).unwrap(),
            ),
            Request::Probe {
                range,
                generation,
                from,
                budget,
            } => Answer::Probe(
                name::probe(
                    &self.names[&range].engine,
                    BUCKET,
                    generation,
                    from.as_deref(),
                    budget,
                )
                .unwrap(),
            ),
            Request::Directory => Answer::Directory(self.directory.clone()),
        }
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

    /// The range the writers route `key` by, if their descriptors hold it.
    fn route(&self, key: &str) -> Option<u64> {
        let route = key::route(BUCKET, key);
        self.cache
            .iter()
            .filter(|d| d.holds(&route))
            .max_by_key(|d| d.generation)
            .map(|d| d.id)
    }

    /// A writer's command for `key`: `None` if its descriptors hold no range for the key, and
    /// otherwise the answer of the range they route it to, the writer learning where the span
    /// went if that range has moved on.
    fn write(&mut self, key: &str, command: name::Command) -> Option<name::Outcome> {
        let Some(id) = self.route(key) else {
            self.cache.clone_from(&self.directory);
            return None;
        };
        let outcome = self.name(id, command);
        if let name::Outcome::Moved(lineage) = &outcome {
            self.cache.retain(|d| d.id != id);
            // An ended range holds nothing: only where its span went is learnt.
            let own = if lineage.standing == Standing::Ended {
                None
            } else {
                Some(&lineage.now)
            };
            let learnt = [own, lineage.child.as_ref(), lineage.into.as_ref()];
            for d in learnt.into_iter().flatten() {
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
        Some(outcome)
    }

    fn act(&mut self, action: &Action) {
        self.clock += 10;
        match action {
            Action::Create => {
                let outcome = self.bucket(bucket::Command::Create(Create {
                    bucket: BUCKET.into(),
                    owner: "o".into(),
                    location: String::new(),
                    at_ns: self.clock,
                    quota: 10,
                    lock: false,
                }));
                self.start(outcome);
            }
            Action::Delete => {
                let outcome = self.bucket(bucket::Command::BeginDelete {
                    bucket: BUCKET.into(),
                    at_ns: self.clock,
                });
                self.start(outcome);
            }
            Action::Abandon => {
                let outcome = self.bucket(bucket::Command::Abandon {
                    bucket: BUCKET.into(),
                    at_ns: self.clock,
                });
                self.start(outcome);
            }
            Action::Collect => {
                if let Some(row) = self.row()
                    && let Some(c) =
                        Coordinator::resume(BUCKET, &row, &self.directory, BOUNDS).unwrap()
                {
                    self.coordinators.push(c);
                }
            }
            Action::Collector => {
                for (name, row) in bucket::attempts(&self.buckets, None, 16).unwrap() {
                    match SCHEDULE.takeover(&row, self.clock) {
                        None => {}
                        Some(Takeover::Abandon) => {
                            let outcome = self.bucket(bucket::Command::Abandon {
                                bucket: name,
                                at_ns: self.clock,
                            });
                            self.start(outcome);
                        }
                        Some(Takeover::Delete) => {
                            let outcome = self.bucket(bucket::Command::BeginDelete {
                                bucket: name,
                                at_ns: self.clock,
                            });
                            self.start(outcome);
                        }
                        Some(Takeover::Resume) => {
                            if let Some(c) =
                                Coordinator::resume(&name, &row, &self.directory, BOUNDS).unwrap()
                            {
                                self.coordinators.push(c);
                            }
                        }
                    }
                }
            }
            Action::Step(i) => {
                if !self.coordinators.is_empty() {
                    let i = i % self.coordinators.len();
                    // A coordinator that steps is alive, and its driver says so.
                    let progress = self.coordinators[i].progress(self.clock);
                    self.bucket(progress);
                    if let Some(request) = self.coordinators[i].next() {
                        let answer = self.serve(request);
                        self.coordinators[i].answer(answer).unwrap();
                    }
                    if self.coordinators[i].is_done() {
                        let done = self.coordinators.swap_remove(i);
                        self.settled.push(done.settled());
                    }
                }
            }
            Action::Crash(i) => {
                if !self.coordinators.is_empty() {
                    let i = i % self.coordinators.len();
                    self.coordinators.swap_remove(i);
                    self.settled.push(None);
                }
            }
            Action::Put { key, view } => {
                if let Some(&incarnation) = self.views.get(view % self.views.len().max(1)) {
                    let key = KEYS[*key];
                    let put = name::Command::Put(Put {
                        bucket: BUCKET.into(),
                        incarnation,
                        key: key.into(),
                        versioning: Versioning::Unversioned,
                        preconditions: Preconditions::default(),
                        at_ns: self.clock,
                        ordered_ns: None,
                        version: object(),
                        default: None,
                        deadline_ns: u64::MAX,
                    });
                    if let Some(name::Outcome::Put { .. }) = self.write(key, put) {
                        self.acked.insert(key, incarnation);
                    }
                }
            }
            Action::Remove { key, view } => {
                if let Some(&incarnation) = self.views.get(view % self.views.len().max(1)) {
                    let key = KEYS[*key];
                    let delete = name::Command::Delete(Delete {
                        bucket: BUCKET.into(),
                        incarnation,
                        key: key.into(),
                        versioning: Versioning::Unversioned,
                        named: None,
                        if_match: None,
                        at_ns: self.clock,
                        bypass: false,
                        owner: "o".into(),
                    });
                    if let Some(name::Outcome::Deleted { .. }) = self.write(key, delete) {
                        self.acked.remove(key);
                    }
                }
            }
            Action::Look => {
                if let Some(row) = self.row()
                    && row.state == BucketState::Active
                    && self.views.last() != Some(&row.created_ns)
                {
                    self.views.push(row.created_ns);
                }
            }
            Action::Split { range, cut } => {
                let ids: Vec<u64> = self.names.keys().copied().collect();
                let id = ids[range % ids.len()];
                let (bucket, key) = CUTS[*cut];
                self.split(id, key::route(bucket, key));
            }
            Action::Publish => {
                self.directory = self.names.keys().map(|&id| self.lineage(id).now).collect();
            }
            Action::Refresh => self.cache.clone_from(&self.directory),
            Action::Merge { range, rows } => {
                if !self.directory.is_empty() {
                    let upper = self.directory[range % self.directory.len()].clone();
                    if let Some(lower) = self
                        .directory
                        .iter()
                        .find(|d| d.hi.as_ref() == Some(&upper.lo))
                    {
                        self.mergers
                            .push(Merger::new(lower.clone(), upper, *rows, u64::MAX));
                    }
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

    /// A driver for every merge a range's lineage shows in flight.
    fn resume_merges(&mut self) {
        let ids: Vec<u64> = self.names.keys().copied().collect();
        for id in ids {
            if let Ok(m) = Merger::resume(&self.lineage(id), MAX_ROWS, u64::MAX) {
                self.mergers.push(m);
            }
        }
    }

    /// Takes `n` steps of the coordinator at `i`, checking the world after each.
    fn steps(&mut self, i: usize, n: usize) {
        for _ in 0..n {
            self.act(&Action::Step(i));
            self.check();
        }
    }

    /// With the faults over, the directory catches up, every coordinator runs to its end, and
    /// the collector acts on its schedule once the attempts left behind have gone their
    /// patience: every create and delete comes to an end within a few rounds.
    fn finish(&mut self) {
        // Late requests arrive, and every merge in flight is carried to its end.
        while let Some(request) = self.late.pop() {
            self.serve_merge(request);
            self.check();
        }
        self.resume_merges();
        // Each in turn, since one may wait on another's end.
        for turn in 0..10_000 {
            if self.mergers.is_empty() {
                break;
            }
            self.merge_step(turn % self.mergers.len());
            self.check();
        }
        assert!(self.mergers.is_empty(), "a merge never finished");
        for id in self.names.keys() {
            let lineage = self.lineage(*id);
            assert!(
                lineage.standing != Standing::Frozen && lineage.taken.is_none(),
                "a merge left in flight: {lineage:?}"
            );
        }
        self.act(&Action::Publish);
        for _ in 0..16 {
            let mut budget = 10_000;
            while !self.coordinators.is_empty() && budget > 0 {
                self.act(&Action::Step(0));
                self.check();
                budget -= 1;
            }
            assert!(self.coordinators.is_empty(), "a coordinator never finished");
            if bucket::attempts(&self.buckets, None, 1).unwrap().is_empty() {
                let state = self.row().map(|r| r.state);
                assert!(
                    matches!(state, None | Some(BucketState::Active)),
                    "no attempt left, yet the bucket is {state:?}"
                );
                return;
            }
            self.clock += SCHEDULE.patience_ns;
            self.act(&Action::Collector);
            self.check();
        }
        panic!("attempts left after 16 rounds of the collector");
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

    /// The range that owns `key`, and its rows.
    fn holder(&self, key: &str) -> &Model {
        let route = key::route(BUCKET, key);
        let (id, _) = self
            .owners()
            .into_iter()
            .find(|(_, l)| l.now.holds(&route))
            .unwrap();
        &self.names[&id].engine
    }

    fn check(&self) {
        // The ranges divide the keys between them, and each holds rows, marks and gates only
        // of its own keys and buckets.
        let owners = self.owners();
        let mut spans: Vec<Descriptor> = owners.iter().map(|(_, l)| l.now.clone()).collect();
        spans.sort_by(|a, b| a.lo.cmp(&b.lo));
        assert!(spans[0].lo.is_empty(), "no range holds the first keys");
        for pair in spans.windows(2) {
            assert_eq!(pair[0].hi.as_ref(), Some(&pair[1].lo), "{spans:?}");
        }
        assert_eq!(spans.last().unwrap().hi, None);
        for (id, lineage) in &owners {
            let (now, range) = (&lineage.now, &self.names[id]);
            for (k, _) in range.engine.image().unwrap() {
                if let Some((bucket, key, _)) = key::decode_name(&k) {
                    assert!(
                        now.holds(&key::route(&bucket, &key)),
                        "{k:?} outside {now:?}"
                    );
                } else if let Some((bucket, key, _)) = key::decode_mark(&k) {
                    assert!(
                        now.holds(&key::route(&bucket, &key)),
                        "{k:?} outside {now:?}"
                    );
                } else if let Some(bucket) = key::decode_gate(&k) {
                    let (first, past) = key::bucket_routes(&bucket);
                    assert!(now.meets(&first, &past), "a gate of {bucket} in {now:?}");
                }
            }
        }
        let row = self.row();
        // Every acknowledged put is in a bucket that still exists, under the incarnation
        // that took it, and the range that holds its key holds it.
        for (key, incarnation) in &self.acked {
            let row = row
                .as_ref()
                .unwrap_or_else(|| panic!("{key} lost to a forgotten bucket"));
            assert_eq!(
                row.created_ns, *incarnation,
                "{key} lost to a re-created bucket"
            );
            assert!(
                matches!(row.state, BucketState::Active | BucketState::Deleting),
                "{key} lost to a delete: {row:?}"
            );
            let (_, version) = name::current(self.holder(key), BUCKET, key)
                .unwrap()
                .unwrap();
            assert!(!version.marker);
        }
        let (first, past) = key::bucket_routes(BUCKET);
        for (id, lineage) in &owners {
            let (now, range) = (&lineage.now, &self.names[id]);
            let gate = name::gate(&range.engine, BUCKET).unwrap();
            let versions = holds_versions(&range.engine);
            match row.as_ref().map(|r| (r.state, r.created_ns)) {
                // An active bucket takes writes to every key: every range that can hold its
                // keys has its gate open.
                Some((BucketState::Active, incarnation)) if now.meets(&first, &past) => {
                    let gate = gate.unwrap_or_else(|| panic!("no gate in {now:?}"));
                    assert_eq!(
                        (gate.incarnation, gate.state),
                        (incarnation, GateState::Open)
                    );
                }
                // A deleted bucket holds no versions, and a forgotten one no gates.
                Some((BucketState::Deleted, _)) => {
                    assert!(!versions, "a deleted bucket's version in {now:?}");
                }
                None => {
                    assert_eq!(gate, None, "a forgotten bucket's gate in {now:?}");
                    assert!(!versions, "a forgotten bucket's version in {now:?}");
                }
                _ => {}
            }
        }
    }
}

/// Whether a range's rows hold a version or delete marker of the bucket, read as they are,
/// frozen or not.
fn holds_versions(engine: &Model) -> bool {
    engine.image().unwrap().iter().any(|(k, _)| {
        matches!(
            key::decode_name(k),
            Some((bucket, _, NameRow::Null | NameRow::Version(_))) if bucket == BUCKET
        )
    })
}

fn object() -> Version {
    Version {
        marker: false,
        null: false,
        modified_ns: 0,
        etag: "e".into(),
        size: 1,
        checksum: None,
        file: Some(1),
        owner: "o".into(),
        headers: Vec::new(),
        retention: None,
        legal_hold: None,
        listing: None,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn no_acknowledged_write_is_lost_to_a_delete(actions in prop::collection::vec(action(), 1..120)) {
        let mut world = World::new();
        for action in &actions {
            world.act(action);
            world.check();
        }
        world.finish();
    }
}

/// The schedule the property is about, spelled out: a delete closes one range's gate while a
/// writer keeps writing, and a second delete takes over from the first.
#[test]
fn a_racing_put_either_stops_the_delete_or_is_refused() {
    let mut w = World::new();
    // Read and open each range's gate, then activate.
    w.act(&Action::Create);
    w.steps(0, 5);
    assert!(w.coordinators.is_empty());
    assert_eq!(w.settled, [Some(Settled::Created)]);
    w.act(&Action::Look);
    assert_eq!(w.row().unwrap().state, BucketState::Active);

    // The first delete reads and closes the first range: a put there is refused, one in the
    // second is taken.
    w.act(&Action::Delete);
    w.steps(0, 2);
    w.act(&Action::Put { key: 0, view: 0 });
    w.act(&Action::Put { key: 2, view: 0 });
    assert_eq!(w.acked.keys().copied().collect::<Vec<_>>(), ["m"]);

    // A second delete takes over, finds the put and restores the bucket: read and close each
    // range, probe each, reopen each, restore. The first delete reads the second range's
    // gate, and its close is refused: a later attempt moved the gate.
    w.act(&Action::Delete);
    w.steps(1, 9);
    assert_eq!(w.coordinators.len(), 1);
    assert_eq!(w.settled[1], Some(Settled::NotEmpty));
    w.steps(0, 2);
    assert!(w.coordinators.is_empty());
    assert_eq!(w.settled[2], Some(Settled::Superseded));
    assert_eq!(w.row().unwrap().state, BucketState::Active);
    w.act(&Action::Put { key: 0, view: 0 });
    assert_eq!(w.acked.len(), 2);

    // Emptied, the bucket is deleted: read, close and probe each range, delete, then condemn,
    // sweep and drop each range's gate, and forget the name. The writer's view is refused.
    w.act(&Action::Remove { key: 0, view: 0 });
    w.act(&Action::Remove { key: 2, view: 0 });
    w.act(&Action::Delete);
    w.steps(0, 14);
    assert!(w.coordinators.is_empty());
    assert_eq!(w.settled[3], Some(Settled::Deleted));
    assert_eq!(w.row(), None);
    w.act(&Action::Put { key: 1, view: 0 });
    assert!(w.acked.is_empty());
    w.check();
}

/// The schedule TLC finds without the generation check (docs/models/RangeSplit.tla): the
/// directory has not learned of a split, and a delete closes and reads the parent's half
/// while the child holds a version. Routed by the old generation, its read is refused; it
/// learns of the child, closes and reads both halves, and finds the version.
#[test]
fn a_delete_the_directory_has_not_told_of_a_split_still_reads_the_child() {
    let mut w = World::new();
    w.act(&Action::Create);
    w.steps(0, 5);
    w.act(&Action::Look);
    w.act(&Action::Put { key: 3, view: 0 });
    assert_eq!(w.acked.keys().copied().collect::<Vec<_>>(), ["t"]);
    // The second range splits at "p"; neither the directory nor the writers know.
    w.split(2, key::route(BUCKET, "p"));
    assert_eq!(w.directory.len(), 2);
    w.act(&Action::Delete);
    // Read and close the first range, read the second's gate: refused, it learns of the child.
    w.steps(0, 3);
    let settled = w.settled.len();
    w.steps(0, 30);
    assert!(w.coordinators.is_empty());
    assert_eq!(w.settled[settled], Some(Settled::NotEmpty));
    assert_eq!(w.row().unwrap().state, BucketState::Active);
    // The writer routes by the old descriptor, is told where "t" went, and writes it there.
    w.act(&Action::Put { key: 3, view: 0 });
    w.act(&Action::Put { key: 3, view: 0 });
    assert!(w.route("t") == Some(3));
    w.check();
}
