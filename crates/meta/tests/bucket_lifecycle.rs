#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

//! Creating and deleting one bucket across a Bucket range and two Name ranges
//! (docs/design/metadata.md §2), driven by the coordinator the gateway runs
//! (`mantle_meta::coordinator`), under schedules proptest chooses: coordinators that stall and
//! are taken over between any two of their reads and commands, collectors that resume deletes
//! left behind, and writers whose view of the bucket is stale. After every step, no write a
//! Name range acknowledged is lost to a delete, and a forgotten bucket leaves no gate behind.

use std::collections::BTreeMap;

use mantle_meta::bucket::{self, Create};
use mantle_meta::coordinator::{Answer, Coordinator, Request, Settled};
use mantle_meta::engine::Model;
use mantle_meta::name::{self, Delete, Preconditions, Probe, Put};
use mantle_meta::record::{BucketState, Version, Versioning};
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

/// Rows a coordinator's read or collection passes or removes at a time: one, so every paused
/// read and partial collection is exercised.
const BUDGET: u32 = 1;

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
    /// What each finished attempt told its request, in the order they finished.
    settled: Vec<Option<Settled>>,
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

    fn row(&self) -> Option<mantle_meta::record::Bucket> {
        bucket::read(&self.buckets, BUCKET).unwrap()
    }

    /// The attempt `outcome` started, if it started one; any other outcome answers the request
    /// with an error, and nothing is left in flight.
    fn start(&mut self, outcome: bucket::Outcome) {
        if let Some(c) = Coordinator::start(BUCKET, &outcome, RANGES, BUDGET).unwrap() {
            self.coordinators.push(c);
        }
    }

    /// What a Bucket or Name range answers a coordinator's request.
    fn serve(&mut self, request: Request) -> Answer {
        match request {
            Request::Bucket(command) => Answer::Bucket(self.bucket(command)),
            Request::Name { range, command } => Answer::Name(self.name(range, *command)),
            Request::Gate { range } => {
                Answer::Gate(name::gate(&self.names[range], BUCKET).unwrap())
            }
            Request::Probe {
                range,
                from,
                budget,
            } => Answer::Probe(
                name::probe(&self.names[range], BUCKET, from.as_deref(), budget).unwrap(),
            ),
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
                    && let Some(c) = Coordinator::resume(BUCKET, &row, RANGES, BUDGET).unwrap()
                {
                    self.coordinators.push(c);
                }
            }
            Action::Step(i) => {
                if !self.coordinators.is_empty() {
                    let i = i % self.coordinators.len();
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
    // Read and open each range's gate, then activate.
    w.act(&Action::Create);
    w.steps(0, 5);
    assert!(w.coordinators.is_empty());
    assert_eq!(w.settled, [Some(Settled::Created)]);
    w.act(&Action::Look);
    assert_eq!(w.row().unwrap().state, BucketState::Active);

    // The first delete reads and closes range 0: a put there is refused, one in range 1 is
    // taken.
    w.act(&Action::Delete);
    w.steps(0, 2);
    w.act(&Action::Put { key: 0, view: 0 });
    w.act(&Action::Put { key: 2, view: 0 });
    assert_eq!(w.acked.keys().copied().collect::<Vec<_>>(), ["m"]);

    // A second delete takes over, finds the put and restores the bucket: read and close each
    // range, probe each, reopen each, restore. The first delete reads range 1's gate, and its
    // close is refused: a later attempt moved the gate.
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
