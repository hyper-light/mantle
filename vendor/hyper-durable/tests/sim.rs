//! The shell under deterministic simulation (`docs/durable.md` §12), in hyper-sim's world
//! (`docs/sim.md` S-6): groups driven by schedules drawn from the world's streams — detectors'
//! words, the world's events (a message arriving, or a member's device completing its oldest
//! write, as its strategy picks), drives, refusals for room, failed writes, proposals, entries acted
//! on at start, changes of configuration, reads, compactions and crashes — with readies ahead of
//! their persistence up to the store's depth, messages lost as the schedule draws; every output held
//! to hyper-check's oracles against the sender's durable state, and every group settling once
//! whole. A schedule runs the same twice from its seed and once from its trace.
//!
//! Then the durability windows, enumerated by fork (`hyper_check::strategy::fork`, `docs/sim.md`
//! §3.7): a schedule runs once, and after each step that did something its world is copied, the
//! member whose event it was crashed in the copy, and the copy run through the rest of the schedule
//! and settled; the copy is dropped before the schedule goes on.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    clippy::unreachable
)]

mod support;

use hyper_check::strategy::fork::each_point;
use hyper_sim::tape::Tape;
use hyper_sim::{Seeded, Source};
use support::cluster::{Cluster, Op, Reached, Shape, played, runs_the_same};

/// The liveness phase's rounds: a group that has not settled in them is reported, not waited on.
const ROUNDS: usize = 4_000;

/// `steps` of the schedule from `seed`, then the liveness phase.
fn schedule(shape: Shape, seed: u64, steps: u64) -> Cluster {
    let mut group = Cluster::scheduled(shape, Source::Seed(seed), steps, ROUNDS as u64);
    for _ in 0..steps {
        group.step();
    }
    assert!(
        group.settles(ROUNDS),
        "seed {seed}: the group did not settle: {:?}",
        group.reached()
    );
    group
}

const SHAPES: [(&str, Shape); 5] = [
    (
        "three voters, depth three, durable state machine",
        Shape {
            members: 4,
            voters: 3,
            depth: 3,
            volatile: false,
            control: false,
            ahead: false,
        },
    ),
    (
        "three voters, depth two, a replayed state machine",
        Shape {
            members: 4,
            voters: 3,
            depth: 2,
            volatile: true,
            control: false,
            ahead: false,
        },
    ),
    (
        "a control group, every entry acted on at start, depth three",
        Shape {
            members: 3,
            voters: 3,
            depth: 3,
            volatile: true,
            control: true,
            ahead: false,
        },
    ),
    (
        "three voters, depth three, a leader applying before its write is durable",
        Shape {
            members: 4,
            voters: 3,
            depth: 3,
            volatile: false,
            control: false,
            ahead: true,
        },
    ),
    (
        "five voters, depth one (slates' synchronous store)",
        Shape {
            members: 5,
            voters: 5,
            depth: 1,
            volatile: false,
            control: false,
            ahead: false,
        },
    ),
];

#[test]
fn random_schedules_keep_every_invariant_and_settle() {
    let seeds = support::count("HYPER_DURABLE_SEEDS", 128);
    let steps = support::count("HYPER_DURABLE_STEPS", 5_000);
    let first = support::count("HYPER_DURABLE_SEED", 0);
    for (name, shape) in SHAPES {
        let mut reached = Reached::default();
        for seed in first..first + seeds {
            let group = schedule(shape, seed, steps);
            reached.add(&group.reached());
            reached.acted += group.acted();
        }
        println!("{name}: {seeds} schedules of {steps} steps: {reached:?}");
        // Schedules that never committed, pipelined, crashed, held an entry behind the fence or
        // stalled prove nothing of the shell: each must be reached; how often is reported above,
        // not judged against a picked count.
        assert!(reached.committed > 0, "{name}: {reached:?}");
        assert!(
            reached.crashes > 0 && reached.fenced > 0,
            "{name}: {reached:?}"
        );
        assert!(reached.refused > 0, "{name}: {reached:?}");
        // Reads were answered: the oracle of reads answered once a group settles judged some.
        assert!(reached.reads > 0, "{name}: {reached:?}");
        assert!(
            reached.behind_fence > 0 && reached.acted > 0,
            "{name}: {reached:?}"
        );
        if shape.depth > 1 {
            assert!(reached.deep > 0, "{name}: {reached:?}");
        }
        // A leader applying ahead was seen ahead of its disk: what the shape is for.
        if shape.ahead {
            assert!(reached.ahead > 0, "{name}: {reached:?}");
        }
        // The network's bound loses fewer than the schedule does, or it, not the schedule, decides
        // what is lost (`docs/sim.md` §7).
        assert!(
            reached.lost > 0 && reached.overflowed < reached.lost,
            "{name}: {reached:?}"
        );
    }
}

/// Each shape's first schedule twice from its seed and once from its trace: the same decisions and
/// the same observations, so a failing seed replays (`docs/sim.md` §3.9).
#[test]
fn a_schedule_runs_the_same_twice_and_from_its_trace() {
    let steps = support::count("HYPER_DURABLE_STEPS", 5_000);
    for (name, shape) in SHAPES {
        let record =
            runs_the_same(shape, 0, steps, ROUNDS).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(record.steps > 0 && !record.trace.is_empty(), "{name}");
    }
}

/// Each shape's schedule played from a tape (`docs/sim.md` §18): a fresh run's tape replays it
/// exactly, and every mutation of it (a step's words redrawn, two steps of one kind exchanged, a
/// step dropped or repeated: Gulcan et al.'s feasible mutations) is a schedule that keeps every
/// oracle and settles, and whose own tape replays it. The world's schedules are mutated as
/// hyper-check mutates a harness's own.
#[test]
fn a_schedule_plays_from_its_tape_and_every_mutation_keeps_every_invariant() {
    let steps = support::count("HYPER_DURABLE_TAPE_STEPS", 800);
    let mutations = support::count("HYPER_DURABLE_MUTATIONS", 8);
    for (name, shape) in SHAPES {
        let run = |tape: Tape, filler: u64| {
            played(shape, tape, filler, steps, ROUNDS).unwrap_or_else(|e| panic!("{name}: {e}"))
        };
        let fresh = run(Tape::default(), 0);
        let tape = fresh
            .tape
            .clone()
            .expect("a played run gives its tape back");
        assert_eq!(tape.len() as u64, steps, "{name}: a step an operation");
        assert_eq!(run(tape.clone(), 1).digest, fresh.digest, "{name}");
        let mut draws = Seeded::new(17);
        let mut differed = 0u64;
        for _ in 0..mutations {
            let mutated = run(tape.mutate(&mut draws), 2);
            differed += u64::from(mutated.digest != fresh.digest);
            let taken = mutated.tape.clone().expect("its tape");
            assert_eq!(run(taken, 3).digest, mutated.digest, "{name}");
        }
        assert!(differed > 0, "{name}: the mutations ran other schedules");
    }
}

/// A crash right after every step of a schedule that did something, each in a fork of the
/// schedule's world at that step.
#[test]
fn a_crash_after_every_durability_event_loses_nothing_durable() {
    let seeds = support::count("HYPER_DURABLE_CRASH_SEEDS", 4);
    let steps = support::count("HYPER_DURABLE_CRASH_STEPS", 800);
    let mut crashes = 0u64;
    for (name, shape) in [SHAPES[0], SHAPES[2]] {
        for seed in 0..seeds {
            let mut trunk = Cluster::scheduled(shape, Source::Seed(seed), steps, ROUNDS as u64);
            let mut taken = 0u64;
            let forked = each_point(
                &mut trunk,
                steps,
                |group| {
                    taken += 1;
                    let (did, member) = group.step();
                    Some(did.then_some((member, taken)))
                },
                |mut branch, (member, at)| {
                    branch.crash(member);
                    for _ in at..steps {
                        branch.step();
                    }
                    assert!(
                        branch.settles(ROUNDS),
                        "{name}, seed {seed}, crash after step {at}: the group did not settle: {:?}",
                        branch.reached()
                    );
                },
            );
            assert!(
                trunk.settles(ROUNDS),
                "{name}, seed {seed}: the group did not settle"
            );
            let events = forked.branches.len() as u64;
            crashes += events;
            println!("{name}, seed {seed}: crashed after each of {events} events");
        }
    }
    assert!(crashes > 0, "{crashes}");
}

/// A member whose last writes were lost at rest, their persist record kept, reopens marked; once
/// its leader's heartbeat counts what it lost it says so, and the leader sends the lost entries
/// again, not a snapshot (core step R-5, `docs/durable.md` §5): its mark ends, and every member
/// applies the same history.
#[test]
fn a_member_whose_last_writes_were_lost_at_rest_is_repaired_by_entries() {
    use hyper_raft::proto::MessageType;
    let shape = SHAPES[1].1;
    let mut group = Cluster::new(shape, 1);
    assert!(group.settles(4_000));
    let ids: Vec<u64> = (1..=shape.members).collect();
    let leader = group.leader().expect("a leader");
    for at in 0..12 {
        group
            .replica(leader)
            .unwrap()
            .propose(Vec::new(), format!("entry {at}").into_bytes())
            .unwrap();
        group.round(&ids);
    }
    // What the last round sent is answered.
    for _ in 0..4 {
        group.round(&ids);
    }
    let member = (1..=shape.voters).find(|id| *id != leader).unwrap();
    let last = group.disk(leader).last();
    assert_eq!(group.disk(member).last(), last);
    let mark = group.rot(member, 6).expect("entries lost");
    let held = group.disk(member).last();
    assert_eq!(held, last - 6);
    assert_eq!(group.replica(member).unwrap().mark(), Some(mark));
    let mut resent = Vec::new();
    let mut snapshots = 0;
    for _ in 0..2_000 {
        if group.replica(member).unwrap().mark().is_none() {
            break;
        }
        for &id in &ids {
            group.act(Op::Drive(id));
            while group.act(Op::Durable(id)) {}
            group.act(Op::Drive(id));
        }
        group.deliver_all(|message| {
            if message.to == member && message.from == leader {
                match message.msg_type {
                    MessageType::MsgSnapshot => snapshots += 1,
                    MessageType::MsgAppend => {
                        resent.extend(message.entries.iter().map(|e| e.index));
                    }
                    _ => {}
                }
            }
            true
        });
    }
    assert_eq!(
        group.replica(member).unwrap().mark(),
        None,
        "the mark never ended"
    );
    assert_eq!(snapshots, 0);
    resent.sort_unstable();
    resent.dedup();
    assert!(
        (held + 1..=last).all(|index| resent.contains(&index)),
        "lost {}..={last}, sent {resent:?}",
        held + 1
    );
    assert!(group.settles(4_000));
}
