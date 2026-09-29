#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

//! Creating and deleting one bucket across a Bucket range and two Name ranges
//! (docs/design/metadata.md §2), under schedules proptest chooses: coordinators that stall and
//! are taken over, collectors that resume deletes left behind, and writers whose view of the
//! bucket is stale. After every step, no write a Name range acknowledged is lost to a delete,
//! and a forgotten bucket leaves no gate behind.

use std::collections::BTreeMap;

use mantle_meta::bucket::{self, Create};
use mantle_meta::engine::Model;
use mantle_meta::name::{self, Collect, Delete, GateChange, Preconditions, Probe, Put};
use mantle_meta::record::{BucketState, GateState, Version, Versioning};
use proptest::prelude::*;

const BUCKET: &str = "b";
/// The keys writers use: the first two in Name range 0, the others in range 1.
const KEYS: [&str; 4] = ["a", "f", "m", "t"];
const RANGES: usize = 2;

fn range_of(key: &str) -> usize {
    usize::from(key >= "m")
}

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
    /// One step of one coordinator in flight.
    Step(usize),
    /// A writer puts or removes a key under one of the incarnations it has seen.
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
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        1 => Just(Action::Create),
        1 => Just(Action::Delete),
        1 => Just(Action::Abandon),
        1 => Just(Action::Collect),
        8 => (0usize..4).prop_map(Action::Step),
        3 => (0usize..4, 0usize..3).prop_map(|(key, view)| Action::Put { key, view }),
        2 => (0usize..4, 0usize..3).prop_map(|(key, view)| Action::Remove { key, view }),
        2 => Just(Action::Look),
    ]
}

/// Where a coordinator is in its create or delete.
#[derive(Debug, Clone)]
enum Phase {
    Open(usize),
    Activate,
    Close(usize),
    Probe(usize, Option<Vec<u8>>),
    Reopen(usize),
    Restore,
    Finish,
    Condemn(usize),
    Sweep(usize),
    Drop(usize),
    Forget,
    Done,
}

#[derive(Debug)]
struct Coordinator {
    incarnation: u64,
    attempt: u64,
    phase: Phase,
}

#[derive(Default)]
struct World {
    buckets: Model,
    bucket_index: u64,
    names: [Model; RANGES],
    name_index: [u64; RANGES],
    clock: u64,
    coordinators: Vec<Coordinator>,
    /// Incarnations writers have seen active, oldest first.
    views: Vec<u64>,
    /// Keys whose last acknowledged write was a put, and the incarnation it was made under.
    acked: BTreeMap<&'static str, u64>,
}

impl World {
    fn bucket(&mut self, command: bucket::Command) -> bucket::Outcome {
        self.bucket_index += 1;
        bucket::apply(&mut self.buckets, self.bucket_index, &command).unwrap()
    }

    fn name(&mut self, range: usize, command: name::Command) -> name::Outcome {
        self.name_index[range] += 1;
        name::apply(&mut self.names[range], self.name_index[range], &command).unwrap()
    }

    fn gate(
        &mut self,
        c: &Coordinator,
        range: usize,
        from: Option<GateState>,
        to: Option<GateState>,
    ) -> bool {
        let change = name::Command::Gate(GateChange {
            bucket: BUCKET.into(),
            incarnation: c.incarnation,
            attempt: c.attempt,
            from,
            to,
        });
        self.name(range, change) == name::Outcome::GateMoved
    }

    fn row(&self) -> Option<mantle_meta::record::Bucket> {
        bucket::read(&self.buckets, BUCKET).unwrap()
    }

    fn start(&mut self, outcome: bucket::Outcome, phase: Phase) {
        match outcome {
            bucket::Outcome::Creating {
                incarnation,
                attempt,
            }
            | bucket::Outcome::Deleting {
                incarnation,
                attempt,
            } => self.coordinators.push(Coordinator {
                incarnation,
                attempt,
                phase,
            }),
            // The request is answered with an error; nothing is left in flight.
            _ => {}
        }
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
                }));
                self.start(outcome, Phase::Open(0));
            }
            Action::Delete => {
                let outcome = self.bucket(bucket::Command::BeginDelete {
                    bucket: BUCKET.into(),
                    at_ns: self.clock,
                });
                self.start(outcome, Phase::Close(0));
            }
            Action::Abandon => {
                let outcome = self.bucket(bucket::Command::Abandon {
                    bucket: BUCKET.into(),
                    at_ns: self.clock,
                });
                self.start(outcome, Phase::Close(0));
            }
            Action::Collect => {
                if let Some(row) = self.row()
                    && row.state == BucketState::Deleted
                {
                    self.coordinators.push(Coordinator {
                        incarnation: row.created_ns,
                        attempt: row.attempt,
                        phase: Phase::Condemn(0),
                    });
                }
            }
            Action::Step(i) => {
                if !self.coordinators.is_empty() {
                    let i = i % self.coordinators.len();
                    let mut c = std::mem::replace(
                        &mut self.coordinators[i],
                        Coordinator {
                            incarnation: 0,
                            attempt: 0,
                            phase: Phase::Done,
                        },
                    );
                    c.phase = self.step(&c);
                    if matches!(c.phase, Phase::Done) {
                        self.coordinators.swap_remove(i);
                    } else {
                        self.coordinators[i] = c;
                    }
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
                    });
                    if let name::Outcome::Put { .. } = self.name(range_of(key), put) {
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
                    });
                    if let name::Outcome::Deleted { .. } = self.name(range_of(key), delete) {
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
        }
    }

    /// Takes one step of `c` and returns its next phase: `Done` once it finishes or finds a
    /// later attempt has taken over.
    fn step(&mut self, c: &Coordinator) -> Phase {
        use GateState::{Closed, Condemned, Open};
        let next = |range: usize, more: Phase, then: Phase| {
            if range + 1 < RANGES { more } else { then }
        };
        match &c.phase {
            Phase::Open(range) => {
                let range = *range;
                let gate = name::gate(&self.names[range], BUCKET).unwrap();
                let opened = match gate {
                    // Opened by the attempt this one took over.
                    Some(g) if g.incarnation == c.incarnation && g.state == Open => true,
                    None => self.gate(c, range, None, Some(Open)),
                    Some(_) => false,
                };
                if opened {
                    next(range, Phase::Open(range + 1), Phase::Activate)
                } else {
                    Phase::Done
                }
            }
            Phase::Activate => {
                self.bucket(bucket::Command::Activate {
                    bucket: BUCKET.into(),
                    attempt: c.attempt,
                });
                Phase::Done
            }
            Phase::Close(range) => {
                let range = *range;
                let from = match name::gate(&self.names[range], BUCKET).unwrap() {
                    None => None,
                    Some(g) if g.incarnation == c.incarnation && g.state != Condemned => {
                        Some(g.state)
                    }
                    Some(_) => return Phase::Done,
                };
                if self.gate(c, range, from, Some(Closed)) {
                    next(range, Phase::Close(range + 1), Phase::Probe(0, None))
                } else {
                    Phase::Done
                }
            }
            Phase::Probe(range, from) => {
                let range = *range;
                match name::probe(&self.names[range], BUCKET, from.as_deref(), 1).unwrap() {
                    Probe::Found => Phase::Reopen(0),
                    Probe::Clear => next(range, Phase::Probe(range + 1, None), Phase::Finish),
                    Probe::Paused(at) => Phase::Probe(range, Some(at)),
                }
            }
            Phase::Reopen(range) => {
                let range = *range;
                if self.gate(c, range, Some(Closed), Some(Open)) {
                    next(range, Phase::Reopen(range + 1), Phase::Restore)
                } else {
                    Phase::Done
                }
            }
            Phase::Restore => {
                self.bucket(bucket::Command::Restore {
                    bucket: BUCKET.into(),
                    attempt: c.attempt,
                });
                Phase::Done
            }
            Phase::Finish => {
                let outcome = self.bucket(bucket::Command::Delete {
                    bucket: BUCKET.into(),
                    attempt: c.attempt,
                });
                if outcome == bucket::Outcome::Deleted {
                    Phase::Condemn(0)
                } else {
                    Phase::Done
                }
            }
            Phase::Condemn(range) => {
                let range = *range;
                if self.gate(c, range, Some(Closed), Some(Condemned)) {
                    next(range, Phase::Condemn(range + 1), Phase::Sweep(0))
                } else {
                    Phase::Done
                }
            }
            Phase::Sweep(range) => {
                let range = *range;
                let collect = name::Command::Collect(Collect {
                    bucket: BUCKET.into(),
                    incarnation: c.incarnation,
                    budget: 1,
                });
                match self.name(range, collect) {
                    name::Outcome::Collected { done: false } => Phase::Sweep(range),
                    name::Outcome::Collected { done: true } => {
                        next(range, Phase::Sweep(range + 1), Phase::Drop(0))
                    }
                    // Another collector removed the gate first.
                    name::Outcome::Conflict => Phase::Done,
                    other => panic!("collecting a condemned bucket: {other:?}"),
                }
            }
            Phase::Drop(range) => {
                let range = *range;
                if self.gate(c, range, Some(Condemned), None) {
                    next(range, Phase::Drop(range + 1), Phase::Forget)
                } else {
                    Phase::Done
                }
            }
            Phase::Forget => {
                self.bucket(bucket::Command::Forget {
                    bucket: BUCKET.into(),
                    attempt: c.attempt,
                });
                Phase::Done
            }
            Phase::Done => Phase::Done,
        }
    }

    /// Takes `n` steps of the coordinator at `i`, checking the world after each.
    fn steps(&mut self, i: usize, n: usize) {
        for _ in 0..n {
            self.act(&Action::Step(i));
            self.check();
        }
    }

    fn check(&self) {
        let row = self.row();
        // Every acknowledged put is in a bucket that still exists, under the incarnation
        // that took it, and a Name range holds it.
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
            let (_, version) = name::current(&self.names[range_of(key)], BUCKET, key)
                .unwrap()
                .unwrap();
            assert!(!version.marker);
        }
        match row.map(|r| r.state) {
            // A deleted bucket holds no versions, and a forgotten one no gates.
            Some(BucketState::Deleted) => {
                for range in &self.names {
                    assert_eq!(name::probe(range, BUCKET, None, 64).unwrap(), Probe::Clear);
                }
            }
            None => {
                for range in &self.names {
                    assert_eq!(name::gate(range, BUCKET).unwrap(), None);
                    assert_eq!(name::probe(range, BUCKET, None, 64).unwrap(), Probe::Clear);
                }
            }
            _ => {}
        }
    }
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
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn no_acknowledged_write_is_lost_to_a_delete(actions in prop::collection::vec(action(), 1..120)) {
        let mut world = World::default();
        for action in &actions {
            world.act(action);
            world.check();
        }
    }
}

/// The schedule the property is about, spelled out: a delete closes one range's gate while a
/// writer keeps writing, and a second delete takes over from the first.
#[test]
fn a_racing_put_either_stops_the_delete_or_is_refused() {
    let mut w = World::default();
    w.act(&Action::Create);
    w.steps(0, 3);
    assert!(w.coordinators.is_empty());
    w.act(&Action::Look);
    assert_eq!(w.row().unwrap().state, BucketState::Active);

    // The first delete closes range 0: a put there is refused, one in range 1 is taken.
    w.act(&Action::Delete);
    w.steps(0, 1);
    w.act(&Action::Put { key: 0, view: 0 });
    w.act(&Action::Put { key: 2, view: 0 });
    assert_eq!(w.acked.keys().copied().collect::<Vec<_>>(), ["m"]);

    // A second delete takes over, finds the put and restores the bucket: close, close, probe,
    // probe, reopen, reopen, restore. The first delete's next step is refused.
    w.act(&Action::Delete);
    w.steps(1, 7);
    assert_eq!(w.coordinators.len(), 1);
    w.steps(0, 1);
    assert!(w.coordinators.is_empty());
    assert_eq!(w.row().unwrap().state, BucketState::Active);
    w.act(&Action::Put { key: 0, view: 0 });
    assert_eq!(w.acked.len(), 2);

    // Emptied, the bucket is deleted and forgotten, and the writer's view is refused.
    w.act(&Action::Remove { key: 0, view: 0 });
    w.act(&Action::Remove { key: 2, view: 0 });
    w.act(&Action::Delete);
    w.steps(0, 12);
    assert!(w.coordinators.is_empty());
    assert_eq!(w.row(), None);
    w.act(&Action::Put { key: 1, view: 0 });
    assert!(w.acked.is_empty());
    w.check();
}
