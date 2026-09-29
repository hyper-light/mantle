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
use mantle_meta::engine::Model;
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
        },
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
