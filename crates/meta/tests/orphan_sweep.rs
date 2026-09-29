#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

//! Files a gateway made and never handed over (docs/design/metadata.md §2), found by the sweep
//! (`mantle_meta::sweep`) across a File range and two Name ranges, under schedules proptest
//! chooses: gateways that hand their files over in time, late, or never, deletes that release
//! files, and sweeps that run a step at a time and stop for good between any two steps.
//!
//! After every step, no file a version references is released, and every file written is
//! referenced, released, or still unsettled. Once the faults stop and every deadline has
//! passed, a sweep leaves nothing unsettled, every file is referenced or released and not both,
//! and a mark is left only by a sweep that stopped between settling a file and unmarking it,
//! on a file a version references or one released, which its release or its reclaiming
//! removes.

use std::collections::BTreeSet;

use mantle_meta::engine::{Model, Rows};
use mantle_meta::file::{self, Unsettled};
use mantle_meta::key::{self, NameRow};
use mantle_meta::name::{self, Check, GateChange, Preconditions, Put, Unmark, Verdict};
use mantle_meta::record::{GateState, Referrer, Version, Versioning};
use mantle_meta::sweep::{Answer, Request, Sweep};
use proptest::prelude::*;

const BUCKET: &str = "b";
/// Keys writers use: the first two in Name range 0, the others in range 1.
const KEYS: [&str; 4] = ["a", "f", "m", "t"];
/// How long a gateway has to hand its file over, in the simulation's clock, which moves 10 a
/// step: five steps.
const HANDOVER: u64 = 50;
const PAGE: usize = 2;

fn range_of(key: &str) -> usize {
    usize::from(key >= "m")
}

#[derive(Debug, Clone)]
enum Action {
    /// A gateway writes a file for a key.
    Write { key: usize },
    /// A gateway hands its file over to the key's Name range.
    Handover(usize),
    /// A gateway stops for good between writing its file and handing it over.
    Crash(usize),
    /// A delete of the key's null version or current version.
    Delete { key: usize, versioned: bool },
    /// One step of the sweep, starting one if none runs.
    Sweep,
    /// The sweep stops for good between two of its steps.
    SweepStops,
    /// Time passes: a gateway's deadline goes by.
    Wait,
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        3 => (0usize..4).prop_map(|key| Action::Write { key }),
        3 => (0usize..4).prop_map(Action::Handover),
        1 => (0usize..4).prop_map(Action::Crash),
        1 => (0usize..4, any::<bool>()).prop_map(|(key, versioned)| Action::Delete { key, versioned }),
        4 => Just(Action::Sweep),
        1 => Just(Action::SweepStops),
        1 => Just(Action::Wait),
    ]
}

/// A gateway between writing its file and handing it over.
struct Gateway {
    key: &'static str,
    file: u128,
    deadline_ns: u64,
    versioning: Versioning,
}

struct World {
    files: Model,
    file_index: u64,
    names: [Model; 2],
    name_index: [u64; 2],
    clock: u64,
    next_file: u128,
    gateways: Vec<Gateway>,
    sweep: Option<Sweep>,
    written: BTreeSet<u128>,
}

impl World {
    fn new() -> Self {
        let mut w = Self {
            files: Model::default(),
            file_index: 0,
            names: [Model::default(), Model::default()],
            name_index: [0, 0],
            clock: 1_000,
            next_file: 1,
            gateways: Vec::new(),
            sweep: None,
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

    fn act(&mut self, action: &Action) {
        self.clock += 10;
        match *action {
            Action::Write { key } => {
                let key = KEYS[key];
                let file = self.next_file;
                self.next_file += 1;
                let outcome = self.file(file::Command::Write {
                    file,
                    extents: vec![mantle_meta::record::Extent {
                        length: 1,
                        target: mantle_meta::record::Target::Block(file),
                    }],
                    referrer: Referrer {
                        bucket: BUCKET.into(),
                        incarnation: 1,
                        key: key.into(),
                    },
                    handover_ns: HANDOVER,
                    at_ns: self.clock,
                });
                let file::Outcome::Written { deadline_ns } = outcome else {
                    panic!("{outcome:?}");
                };
                self.written.insert(file);
                let versioning = if file.is_multiple_of(2) {
                    Versioning::Enabled
                } else {
                    Versioning::Unversioned
                };
                self.gateways.push(Gateway {
                    key,
                    file,
                    deadline_ns,
                    versioning,
                });
            }
            Action::Handover(i) => {
                if !self.gateways.is_empty() {
                    let g = self.gateways.swap_remove(i % self.gateways.len());
                    self.handover(&g);
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
                    at_ns: self.clock,
                    bypass: false,
                });
                self.name(range_of(key), delete);
            }
            Action::Sweep => self.sweep_step(),
            Action::SweepStops => self.sweep = None,
            Action::Wait => self.clock += HANDOVER,
        }
    }

    fn handover(&mut self, g: &Gateway) {
        let put = name::Command::Put(Put {
            bucket: BUCKET.into(),
            incarnation: 1,
            key: g.key.into(),
            versioning: g.versioning,
            preconditions: Preconditions::default(),
            at_ns: self.clock,
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
            deadline_ns: g.deadline_ns,
        });
        let outcome = self.name(range_of(g.key), put);
        let late = self.clock > g.deadline_ns;
        match outcome {
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

    /// What the File and Name ranges answer a sweep's request.
    fn serve(&mut self, request: Request) -> Answer {
        match request {
            Request::Due { max } => {
                let now = mantle_meta::clock::now(&self.files, self.clock).unwrap();
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
                        at_ns: self.clock,
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
    }

    /// The faults stop: every gateway still in flight hands its file over, every deadline
    /// passes, and the sweep runs until nothing is unsettled.
    fn finish(&mut self) {
        while let Some(g) = self.gateways.pop() {
            self.clock += 10;
            self.handover(&g);
            self.check();
        }
        self.clock += 10 * HANDOVER;
        for _ in 0..1_000 {
            if self.unsettled().is_empty() && self.sweep.is_none() {
                break;
            }
            self.sweep_step();
            self.check();
        }
        assert!(self.unsettled().is_empty(), "files left unsettled");
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
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn every_file_is_handed_over_or_released_and_none_is_both(
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

/// The case the sweep exists for, spelled out: a gateway writes a file and stops, and once its
/// deadline has passed the sweep releases the file; a gateway that hands its file over a step
/// later is refused.
#[test]
fn a_file_whose_gateway_stopped_is_released_and_a_late_handover_refused() {
    let mut w = World::new();
    w.act(&Action::Write { key: 0 });
    w.act(&Action::Write { key: 2 });
    let late = w.gateways.remove(1);
    w.act(&Action::Crash(0));
    // Before the deadline the sweep finds nothing due.
    w.act(&Action::Sweep);
    assert!(w.sweep.is_none());
    w.act(&Action::Wait);
    w.act(&Action::Wait);
    for _ in 0..8 {
        w.act(&Action::Sweep);
        w.check();
    }
    assert_eq!(w.released(), BTreeSet::from([1, 2]));
    assert!(w.unsettled().is_empty());
    w.handover(&late);
    w.check();
    w.finish();
}
