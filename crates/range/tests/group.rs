//! A range's group of three replicas on simulated devices, driven by hand.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::collections::VecDeque;
use std::sync::Arc;

use mantle_disk::buf::Alignment;
use mantle_disk::sim::SimFile;
use mantle_log::{Config as LogConfig, Log};
use mantle_meta::apply::Layer;
use mantle_meta::engine::{Model, Rows};
use mantle_meta::name::{self, GateChange, Preconditions, Put};
use mantle_meta::record::{GateState, Version, Versioning};
use mantle_meta::session::Rules;
use mantle_meta::wire::{Answer, Command, Entry, Sessioned};
use mantle_range::{ConfState, Message, Range, Replica, Settings};

const GROUP: u128 = 0x0072_616e_6765;

fn log_config() -> LogConfig {
    LogConfig {
        segment_bytes: 64 * 4096,
        max_segments: 16,
        max_groups: 16,
        group_entries: 1 << 16,
        group_bytes: 1 << 24,
        group_cache: 1 << 16,
        queue_submissions: 64,
        queue_bytes: 1 << 22,
    }
}

const SETTINGS: Settings = Settings {
    election_tick: 10,
    heartbeat_tick: 2,
    max_size_per_msg: 1 << 20,
    max_inflight_msgs: 64,
    max_uncommitted_size: 1 << 24,
    max_committed_size_per_ready: 1 << 22,
};

fn range() -> Range {
    Range {
        layer: Layer::Name,
        rules: RULES,
        boot: ConfState {
            voters: (1..=3).collect(),
            ..ConfState::default()
        },
        settings: SETTINGS,
    }
}

const RULES: Rules = Rules {
    lifetime_ns: 3_600_000_000_000,
    max_sessions: 64,
    max_answers: 16,
    expiries_per_entry: 8,
};

struct Node {
    replica: Replica<Arc<SimFile>, Model>,
}

fn node(id: u64, seed: u64) -> Node {
    let file = Arc::new(
        SimFile::new(
            Alignment::new(4096).unwrap(),
            Alignment::new(512).unwrap(),
            seed,
        )
        .unwrap(),
    );
    let log = Arc::new(Log::create(file, log_config(), 0x6c6f67 + u128::from(id)).unwrap());
    let replica = Replica::open(id, GROUP, log, Model::default(), &range(), seed).unwrap();
    Node { replica }
}

/// Drives every node until no message is left in flight, delivering in order.
fn settle(nodes: &mut [Node], answers: &mut Vec<(u64, u64, Vec<Answer>)>) {
    let mut wire: VecDeque<Message> = VecDeque::new();
    for _ in 0..10_000 {
        for n in nodes.iter_mut() {
            let out = n.replica.drive().unwrap();
            wire.extend(out.messages);
            let id = n.replica.id();
            answers.extend(out.applied.into_iter().map(|a| {
                let answers: Vec<Answer> = a.answers.into_iter().map(|(_, _, x)| x).collect();
                (id, a.index, answers)
            }));
        }
        let Some(m) = wire.pop_front() else {
            return;
        };
        let to = usize::try_from(m.to).unwrap() - 1;
        // A message the core refuses changes nothing.
        let _ = nodes[to].replica.step(m);
    }
    panic!("the group never settled");
}

fn put(key: &str) -> Command {
    Command::Name(Box::new(name::Command::Put(Put {
        bucket: "b".into(),
        incarnation: 1,
        key: key.into(),
        versioning: Versioning::Enabled,
        preconditions: Preconditions::default(),
        at_ns: 0,
        ordered_ns: None,
        version: Version {
            marker: false,
            null: false,
            modified_ns: 0,
            etag: key.into(),
            size: 1,
            checksum: None,
            file: Some(1),
            owner: "o".into(),
            headers: Vec::new(),
            retention: None,
            legal_hold: None,
        },
        default: None,
        deadline_ns: u64::MAX,
    })))
}

#[test]
fn a_group_elects_a_leader_and_applies_the_same_entries_everywhere() {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    assert!(nodes[0].replica.is_leader());
    assert!(nodes.iter().all(|n| n.replica.leader() == 1));

    let entry = |at_ns, commands| Entry { at_ns, commands };
    let register = Sessioned {
        session: 0,
        serial: 0,
        unanswered: 0,
        command: Command::Register,
    };
    nodes[0]
        .replica
        .propose(&entry(10, vec![register]))
        .unwrap();
    settle(&mut nodes, &mut answers);
    let Some((_, _, first)) = answers.iter().find(|(id, _, _)| *id == 1) else {
        panic!("{answers:?}")
    };
    let [Answer::Registered { session }] = first[..] else {
        panic!("{first:?}")
    };
    let open = Command::Name(Box::new(name::Command::Gate(GateChange {
        bucket: "b".into(),
        incarnation: 1,
        attempt: 1,
        from: None,
        to: Some(GateState::Open),
    })));
    let commands = vec![
        Sessioned {
            session,
            serial: 1,
            unanswered: 1,
            command: open,
        },
        Sessioned {
            session,
            serial: 2,
            unanswered: 1,
            command: put("k"),
        },
    ];
    nodes[0].replica.propose(&entry(20, commands)).unwrap();
    settle(&mut nodes, &mut answers);
    // Every member applied every entry, with the same answers at each index.
    let at = |id: u64| {
        answers
            .iter()
            .filter(|(n, _, _)| *n == id)
            .map(|(_, i, a)| (*i, a.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(at(1).len(), 2);
    assert_eq!(at(1), at(2));
    assert_eq!(at(1), at(3));
    for n in &nodes {
        let current = name::current(n.replica.engine(), "b", "k")
            .unwrap()
            .unwrap();
        assert_eq!(current.1.etag, "k");
    }
}

/// Every row an engine holds that its range replicates.
fn rows(m: &Model) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut at = Vec::new();
    while let Some((k, v)) = m.next(&at, &[0xFF]).unwrap() {
        at = k.clone();
        at.push(0);
        if !mantle_range::member_local(&k) {
            out.push((k, v));
        }
    }
    out
}

/// Member 3 is lost after the leader compacted its log, and member 4 takes its place: added
/// as a learner, caught up by snapshot, then swapped in by one joint change. The snapshot the
/// leader prepared at compaction does not name member 4, which refuses a snapshot that does
/// not, so the leader must prepare one that does when member 4 is added.
#[test]
fn a_member_added_after_compaction_catches_up_and_replaces_a_lost_one() {
    use mantle_range::membership::{Next, Replacement};

    let mut nodes: Vec<Node> = (1..=4).map(|id| node(id, id)).collect();
    let live = |id: u64| id != 3;
    // Delivers in order among the live members until nothing is in flight.
    let settle_live = |nodes: &mut [Node]| {
        let mut wire: VecDeque<Message> = VecDeque::new();
        for _ in 0..10_000 {
            for n in nodes.iter_mut().filter(|n| live(n.replica.id())) {
                wire.extend(n.replica.drive().unwrap().messages);
            }
            let Some(m) = wire.pop_front() else {
                return;
            };
            if live(m.to) {
                let _ = nodes[usize::try_from(m.to).unwrap() - 1].replica.step(m);
            }
        }
        panic!("the group never settled");
    };

    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes[..3], &mut answers);
    let register = Sessioned {
        session: 0,
        serial: 0,
        unanswered: 0,
        command: Command::Register,
    };
    nodes[0]
        .replica
        .propose(&Entry {
            at_ns: 10,
            commands: vec![register],
        })
        .unwrap();
    settle(&mut nodes[..3], &mut answers);
    let session = answers
        .iter()
        .find_map(|(_, _, a)| match a[..] {
            [Answer::Registered { session }] => Some(session),
            _ => None,
        })
        .unwrap();
    let open = Command::Name(Box::new(name::Command::Gate(GateChange {
        bucket: "b".into(),
        incarnation: 1,
        attempt: 1,
        from: None,
        to: Some(GateState::Open),
    })));
    let commands = vec![
        Sessioned {
            session,
            serial: 1,
            unanswered: 1,
            command: open,
        },
        Sessioned {
            session,
            serial: 2,
            unanswered: 1,
            command: put("k"),
        },
    ];
    nodes[0]
        .replica
        .propose(&Entry {
            at_ns: 20,
            commands,
        })
        .unwrap();
    settle(&mut nodes[..3], &mut answers);
    // The leader keeps nothing behind its snapshot, so a new member needs the snapshot.
    nodes[0].replica.compact(0).unwrap();

    let replacement = Replacement::new(3, 4).unwrap();
    let mut done = false;
    for _ in 0..50 {
        let leader = &mut nodes[0].replica;
        match replacement.next(
            leader.configuration(),
            leader.caught_up(4),
            leader.configuration_known(),
        ) {
            Next::Done => {
                done = true;
                break;
            }
            Next::Wait => {}
            Next::Propose(change) => leader.propose_change(&change).unwrap(),
        }
        // Heartbeats carry the leader's progress to followers that answered nothing new.
        for _ in 0..SETTINGS.heartbeat_tick {
            nodes[0].replica.tick().unwrap();
        }
        settle_live(&mut nodes);
    }
    assert!(
        done,
        "the replacement never finished: {}",
        nodes[0].replica.describe()
    );

    let want = ConfState {
        voters: vec![1, 2, 4],
        ..ConfState::default()
    };
    for id in [1, 2, 4] {
        let r = &nodes[usize::try_from(id).unwrap() - 1].replica;
        let mut conf = r.configuration().clone();
        conf.voters.sort_unstable();
        assert_eq!(conf, want, "member {id}");
        assert_eq!(
            rows(r.engine()),
            rows(nodes[0].replica.engine()),
            "member {id}"
        );
    }
}

/// A member whose engine made its rows durable past the commit its log kept reopens and
/// carries on. A member learns of commits after its entries are durable and applies them
/// without writing the commit to its log; its engine can then become durable ahead of it, as
/// a compaction makes it. Here the log is told an older commit directly, then the member
/// crashes and opens again.
#[test]
fn a_member_whose_engine_is_ahead_of_its_logs_commit_reopens() {
    use mantle_disk::sim::Crash;

    let file = |id: u64| {
        Arc::new(
            SimFile::new(
                Alignment::new(4096).unwrap(),
                Alignment::new(512).unwrap(),
                id,
            )
            .unwrap(),
        )
    };
    let files: Vec<Arc<SimFile>> = (1..=3).map(file).collect();
    let logs: Vec<Arc<Log<Arc<SimFile>>>> = files
        .iter()
        .zip(1u64..)
        .map(|(f, id)| {
            Arc::new(Log::create(Arc::clone(f), log_config(), 0x6c6f67 + u128::from(id)).unwrap())
        })
        .collect();
    let mut nodes: Vec<Node> = logs
        .iter()
        .zip(1u64..)
        .map(|(log, id)| Node {
            replica: Replica::open(id, GROUP, Arc::clone(log), Model::default(), &range(), id)
                .unwrap(),
        })
        .collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    for serial in 0..3 {
        let register = Sessioned {
            session: 0,
            serial,
            unanswered: 0,
            command: Command::Register,
        };
        nodes[0]
            .replica
            .propose(&Entry {
                at_ns: 10,
                commands: vec![register],
            })
            .unwrap();
        settle(&mut nodes, &mut answers);
    }
    let applied = nodes[1].replica.applied();
    assert!(applied > 2);

    // Member 2's engine is durable at what it applied; its log keeps an older commit.
    nodes[1].replica.compact(u64::MAX).unwrap();
    let hard = logs[1].view(GROUP).unwrap().unwrap().hard_state.unwrap();
    logs[1]
        .write_waiting(
            GROUP,
            mantle_log::Update {
                hard_state: Some(mantle_log::HardState { commit: 1, ..hard }),
                ..mantle_log::Update::default()
            },
        )
        .unwrap();

    // It crashes and opens again from what its device and engine kept.
    let old = std::mem::replace(&mut nodes[1], node(9, 9));
    let mut engine = old.replica.into_engine();
    engine.crash();
    files[1].crash(Crash::Random).unwrap();
    let (log, recovery) = Log::open(Arc::clone(&files[1]), log_config(), 0x6c6f67 + 2).unwrap();
    assert!(recovery.damaged.is_empty());
    nodes[1] = Node {
        replica: Replica::open(2, GROUP, Arc::new(log), engine, &range(), 2).unwrap(),
    };
    assert_eq!(nodes[1].replica.applied(), applied);
    // Hearing from no leader, it times out and campaigns before a leader's append could tell
    // it of the commit.
    for _ in 0..2 * SETTINGS.election_tick {
        nodes[1].replica.tick().unwrap();
    }
    let out = nodes[1].replica.drive().unwrap();
    assert!(!out.messages.is_empty());

    // It takes part again: a new entry is applied everywhere.
    let register = Sessioned {
        session: 0,
        serial: 9,
        unanswered: 0,
        command: Command::Register,
    };
    nodes[0]
        .replica
        .propose(&Entry {
            at_ns: 20,
            commands: vec![register],
        })
        .unwrap();
    settle(&mut nodes, &mut answers);
    let last = nodes[0].replica.applied();
    assert!(last > applied);
    assert!(nodes.iter().all(|n| n.replica.applied() == last));
    assert_eq!(
        rows(nodes[1].replica.engine()),
        rows(nodes[0].replica.engine())
    );
}
