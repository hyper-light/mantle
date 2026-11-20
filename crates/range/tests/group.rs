//! A range's group of three replicas on simulated devices, driven by hand. Each member's log
//! answers its writes as they are submitted (`support::store`), and the shell takes each answer
//! at the member's next drive: what a drive gives out is decided by the test's calls alone.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

#[path = "support/store.rs"]
mod store;

use std::collections::VecDeque;
use std::task::Waker;
use std::time::{Duration, Instant};

use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;
use hyper_durable::Fault;
use hyper_log::{Config as LogConfig, Log, Waits};
use mantle_meta::apply::Layer;
use mantle_meta::engine::{Engine, Model, Rows};
use mantle_meta::name::{self, GateChange, Preconditions, Put};
use mantle_meta::record::{GateState, Version, Versioning};
use mantle_meta::session::Rules;
use mantle_meta::wire::{Answer, Command, ENTRY_HEAD_BYTES, Entry, Sessioned};
use mantle_range::{
    ConfState, Driven, Message, MessageType, Range, Replica, ReplicaError, Settings,
};
use store::Synchronous;

const GROUP: u128 = 0x0072_616e_6765;

fn log_config() -> LogConfig {
    LogConfig {
        segment_bytes: 64 * 4096,
        max_segments: 16,
        max_groups: 16,
        group_entries: 1 << 18,
        group_bytes: 1 << 24,
        group_cache: 1 << 16,
        queue_submissions: 64,
        waits: Waits::Measured,
    }
}

const SETTINGS: Settings = Settings {
    election_tick: 10,
    heartbeat_tick: 2,
    max_size_per_msg: 1 << 16,
    max_inflight_msgs: 16,
    max_uncommitted_size: 1 << 20,
    max_committed_size_per_ready: 1 << 22,
    max_entry_bytes: 1 << 16,
};

/// The engine of a cell's first Name range, before any entry: its lineage, holding every key.
fn first_range() -> Model {
    let mut m = Model::default();
    m.install(0, name::first(1).unwrap()).unwrap();
    m.persist().unwrap();
    m
}

fn range() -> Range {
    Range {
        layer: Layer::Name,
        rules: RULES,
        boot: ConfState {
            voters: (1..=3).collect(),
            ..ConfState::default()
        },
        settings: SETTINGS,
        members: 0,
    }
}

const RULES: Rules = Rules {
    lifetime_ns: 3_600_000_000_000,
    max_sessions: 64,
    expiries_per_entry: 8,
    // The model engine holds a row of any length.
    max_key_bytes: usize::MAX,
    max_value_bytes: usize::MAX,
};

/// A simulated device of 4 KiB blocks and 512-byte sectors whose faults replay from `seed`.
fn sim(seed: u64) -> SimFile {
    SimFile::new(
        Alignment::new(4096).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

/// The device of a log, given back once the log has answered everything it took: what a log
/// owns, the test reaches again only when the log closes.
fn close(log: Log<SimFile>) -> SimFile {
    log.close().unwrap()
}

type Member = Replica<Synchronous<SimFile>, Model>;

/// Opens member `id` of `range` over its group of `log`.
fn open(
    id: u64,
    log: &Log<SimFile>,
    engine: Model,
    range: &Range,
    seed: u64,
) -> Result<Member, ReplicaError> {
    let store = Synchronous::new(mantle_range::claim(log, GROUP, range)?, None);
    Replica::open(id, store, engine, range, seed)
}

/// What a member gave out in its drives.
type Out = mantle_range::Output;

/// Drives `r` once; with `settle`, again until none of its writes is out and nothing more is
/// due, within the rounds a group without faults takes to do anything.
fn drive(r: &mut Member, settle: bool) -> (Out, Driven) {
    let mut out = Out::default();
    let mut driven = r.drive(0, Waker::noop(), &mut out).unwrap();
    if settle {
        let mut budget = 40 * SETTINGS.election_tick;
        while driven.more || r.in_flight() > 0 {
            assert!(
                budget > 0,
                "member {} never settled: {}",
                r.id(),
                r.describe()
            );
            budget -= 1;
            driven = r.drive(0, Waker::noop(), &mut out).unwrap();
        }
    }
    (out, driven)
}

/// Each answer an applied entry made: the member, the entry's index, and its commands' answers.
type Answered = Vec<(u64, u64, Vec<Answer>)>;

fn answered(id: u64, out: &mut Out, answers: &mut Answered) {
    // An entry's answers come together, in order, within one drive.
    for a in out.answers.drain(..) {
        match answers.last_mut() {
            Some((member, index, each)) if *member == id && *index == a.index => {
                each.push(a.answer);
            }
            _ => answers.push((id, a.index, vec![a.answer])),
        }
    }
}

/// A member and the log of its device. The member holds only its group's handle on the log,
/// so it is dropped first.
struct Node {
    replica: Member,
    log: Log<SimFile>,
}

fn node(id: u64, seed: u64) -> Node {
    let log = Log::create(sim(seed), log_config(), 0x6c6f67 + u128::from(id)).unwrap();
    let replica = open(id, &log, first_range(), &range(), seed).unwrap();
    Node { replica, log }
}

/// Drives every node until no message is left in flight, delivering in order.
fn settle(nodes: &mut [Node], answers: &mut Answered) {
    let mut wire: VecDeque<Message> = VecDeque::new();
    for _ in 0..10_000 {
        for n in nodes.iter_mut() {
            let (mut out, _) = drive(&mut n.replica, true);
            wire.extend(out.messages.drain(..));
            answered(n.replica.id(), &mut out, answers);
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
            listing: None,
        },
        default: None,
        // The write carries its file, which names it.
        id: 0,
        deadline_ns: u64::MAX,
    })))
}

fn registration(serial: u64) -> Entry {
    Entry {
        at_ns: 10,
        commands: vec![Sessioned {
            session: 0,
            serial,
            unanswered: 0,
            command: Command::Register {
                owner: serial,
                run: 1,
                nth: 1,
            },
        }],
    }
}

fn open_gate() -> Command {
    Command::Name(Box::new(name::Command::Gate(GateChange {
        bucket: "b".into(),
        incarnation: 1,
        attempt: 1,
        from: None,
        to: Some(GateState::Open),
        generation: 1,
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

    nodes[0].replica.propose(&registration(0)).unwrap();
    settle(&mut nodes, &mut answers);
    let Some((_, _, first)) = answers.iter().find(|(id, _, _)| *id == 1) else {
        panic!("{answers:?}")
    };
    let [Answer::Registered { session }] = first[..] else {
        panic!("{first:?}")
    };
    let commands = vec![
        Sessioned {
            session,
            serial: 1,
            unanswered: 1,
            command: open_gate(),
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
    for n in &mut nodes {
        let current = name::current(n.replica.engine_mut(), "b", "k")
            .unwrap()
            .unwrap();
        assert_eq!(current.1.etag, "k");
    }
}

/// A command whose bytes flipped after its CRC was computed is answered `Corrupt` by every member
/// at the same index, and none writes it, the command beside it applying; sent again under its
/// serial, it applies once everywhere (docs/design/replica.md §2).
#[test]
fn a_flipped_command_is_refused_alike_on_every_member_and_applies_once_sent_again() {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    nodes[0].replica.propose(&registration(0)).unwrap();
    settle(&mut nodes, &mut answers);
    let Some((_, _, first)) = answers.iter().find(|(id, _, _)| *id == 1) else {
        panic!("{answers:?}")
    };
    let [Answer::Registered { session }] = first[..] else {
        panic!("{first:?}")
    };
    let sent = |serial: u64, command: Command| Sessioned {
        session,
        serial,
        unanswered: 1,
        command,
    };
    let entry = Entry {
        at_ns: 20,
        commands: vec![sent(1, open_gate()), sent(2, put("k"))],
    };
    let mut bytes = entry.encode().unwrap();
    // A bit of the put's own bytes: past the head, the gate's frame, its length and session fields.
    let gate = entry.commands[0].encoded_len().unwrap();
    bytes[ENTRY_HEAD_BYTES + gate + 4 + 24 + 5] ^= 0x04;
    nodes[0].replica.propose_encoded(bytes).unwrap();
    settle(&mut nodes, &mut answers);
    let at = |answers: &Answered, id: u64| {
        answers
            .iter()
            .filter(|(n, _, _)| *n == id)
            .map(|(_, i, a)| (*i, a.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(at(&answers, 1), at(&answers, 2));
    assert_eq!(at(&answers, 1), at(&answers, 3));
    let (_, last) = at(&answers, 1).pop().unwrap();
    assert_eq!(last.len(), 2);
    assert!(matches!(last[0], Answer::Name(_)), "{last:?}");
    assert_eq!(last[1], Answer::Corrupt);
    for n in &mut nodes {
        assert!(
            name::current(n.replica.engine_mut(), "b", "k")
                .unwrap()
                .is_none()
        );
    }
    nodes[0]
        .replica
        .propose(&Entry {
            at_ns: 30,
            commands: vec![sent(2, put("k"))],
        })
        .unwrap();
    settle(&mut nodes, &mut answers);
    assert_eq!(at(&answers, 1), at(&answers, 2));
    assert_eq!(at(&answers, 1), at(&answers, 3));
    for n in &mut nodes {
        let current = name::current(n.replica.engine_mut(), "b", "k")
            .unwrap()
            .unwrap();
        assert_eq!(current.1.etag, "k");
    }
}

fn of(messages: &[Message], kind: MessageType) -> Vec<&Message> {
    messages.iter().filter(|m| m.msg_type == kind).collect()
}

/// A leader's appends leave at once, in the drive that submits its own write of the entries; a
/// follower's acknowledgement only once its write is durable, at the drive that takes the log's
/// answer (hyper-raft docs/durable.md §3, I1 and I2). Meanwhile the members take messages and
/// proposals: the core gives readies ahead of their persistence (R-4), where the replica once
/// refused both. The leader commits on its followers' word before its own write's answer, and
/// the entry applies everywhere.
#[test]
fn a_leader_sends_while_its_write_is_out_and_a_follower_acknowledges_after() {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    assert!(nodes[0].replica.is_leader());
    let applied = nodes[0].replica.applied();

    // The leader's write of the entry is out: its appends go regardless.
    nodes[0].replica.propose(&registration(0)).unwrap();
    let (out, _) = drive(&mut nodes[0].replica, false);
    assert_eq!(nodes[0].replica.in_flight(), 1);
    let appends: Vec<Message> = of(&out.messages, MessageType::MsgAppend)
        .into_iter()
        .cloned()
        .collect();
    let mut to: Vec<u64> = appends.iter().map(|m| m.to).collect();
    to.sort_unstable();
    assert_eq!(to, [2, 3]);
    // And it takes another proposal while the write is out.
    nodes[0].replica.propose(&registration(1)).unwrap();

    // Follower 2 takes the append, its write out, and acknowledges nothing yet; follower 3,
    // whose answer it takes, acknowledges.
    let mut acks = Vec::new();
    for m in appends {
        let to = m.to;
        let follower = &mut nodes[usize::try_from(to).unwrap() - 1].replica;
        follower.step(m).unwrap();
        let (out, _) = drive(follower, to == 3);
        acks.extend(
            of(&out.messages, MessageType::MsgAppendResponse)
                .into_iter()
                .cloned(),
        );
    }
    assert_eq!(acks.iter().map(|m| m.from).collect::<Vec<_>>(), [3]);
    assert_eq!(nodes[1].replica.in_flight(), 1);
    // Once it takes its write's answer, follower 2 acknowledges.
    let (out, _) = drive(&mut nodes[1].replica, false);
    acks.extend(
        of(&out.messages, MessageType::MsgAppendResponse)
            .into_iter()
            .cloned(),
    );
    assert_eq!(acks.len(), 2);

    // The leader takes the acknowledgements while its own write is still out, and the entry
    // commits and applies once its drives take what is due.
    assert_eq!(nodes[0].replica.in_flight(), 1);
    for ack in acks {
        nodes[0].replica.step(ack).unwrap();
    }
    assert_eq!(nodes[0].replica.applied(), applied);
    let (out, _) = drive(&mut nodes[0].replica, true);
    assert!(nodes[0].replica.applied() > applied, "{:?}", out.messages);
    for m in out.messages {
        let _ = nodes[usize::try_from(m.to).unwrap() - 1].replica.step(m);
    }
    settle(&mut nodes, &mut answers);
    for n in &nodes {
        assert_eq!(n.replica.applied(), nodes[0].replica.applied());
    }
    assert!(nodes[0].replica.applied() >= applied + 2);
}

/// A read asked after a round of confirmation left is never confirmed by that round: its index
/// is the commit when it began, which may predate a write committed since whose answer came
/// before the read was asked (audit §5.5). The core's rounds keep this (hyper-raft `ReadRounds`):
/// a read asked after a round was sent is asked for by the next.
#[test]
fn a_read_asked_after_a_round_left_waits_for_the_next() {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    assert!(nodes[0].replica.is_leader());

    // Round one carries the first read; its heartbeats are held.
    nodes[0].replica.read_index(b"first".to_vec()).unwrap();
    let (out, _) = drive(&mut nodes[0].replica, true);
    let held: Vec<Message> = out.messages;
    assert!(
        held.iter().all(|m| m.msg_type == MessageType::MsgHeartbeat),
        "{held:?}"
    );

    // A write commits meanwhile, its appends and their answers delivered.
    nodes[0].replica.propose(&registration(0)).unwrap();
    let mut wire: VecDeque<Message> = drive(&mut nodes[0].replica, true).0.messages.into();
    let mut written = None;
    while let Some(m) = wire.pop_front() {
        if m.msg_type == MessageType::MsgHeartbeat {
            continue;
        }
        let to = usize::try_from(m.to).unwrap() - 1;
        nodes[to].replica.step(m).unwrap();
        let (out, _) = drive(&mut nodes[to].replica, true);
        if to == 0
            && let Some(a) = out.answers.last()
        {
            written = Some(a.index);
        }
        wire.extend(
            out.messages
                .into_iter()
                .filter(|m| m.msg_type != MessageType::MsgHeartbeat),
        );
    }
    let written = written.expect("the write committed at the leader");

    // A read asked now must see the write: the next round carries it.
    nodes[0].replica.read_index(b"second".to_vec()).unwrap();
    let mut wire: VecDeque<Message> = held.into();
    wire.extend(drive(&mut nodes[0].replica, true).0.messages);
    let mut confirmed = Vec::new();
    for _ in 0..1_000 {
        let Some(m) = wire.pop_front() else {
            break;
        };
        let to = usize::try_from(m.to).unwrap() - 1;
        let _ = nodes[to].replica.step(m);
        for n in nodes.iter_mut() {
            let (out, _) = drive(&mut n.replica, true);
            if n.replica.id() == 1 {
                confirmed.extend(out.reads);
            }
            wire.extend(out.messages);
        }
    }
    let at = |read: &[u8]| {
        confirmed
            .iter()
            .find(|(r, _)| r.as_slice() == read)
            .map(|(_, i)| *i)
            .unwrap_or_else(|| panic!("{:?} not confirmed", String::from_utf8_lossy(read)))
    };
    assert!(at(b"first") < written);
    assert!(
        at(b"second") >= written,
        "a read confirmed at {} before the write at {written}",
        at(b"second")
    );
}

/// Every row of an engine: every one is its range's, the configuration and the term of the last
/// entry applied among them, which every member applying the same entries writes alike.
fn rows(m: &mut Model) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut at = Vec::new();
    while let Some((k, v)) = m.next(&at, &[0xFF]).unwrap() {
        at = k.clone();
        at.push(0);
        out.push((k, v));
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
                wire.extend(drive(&mut n.replica, true).0.messages);
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
    nodes[0].replica.propose(&registration(0)).unwrap();
    settle(&mut nodes[..3], &mut answers);
    let session = answers
        .iter()
        .find_map(|(_, _, a)| match a[..] {
            [Answer::Registered { session }] => Some(session),
            _ => None,
        })
        .unwrap();
    let commands = vec![
        Sessioned {
            session,
            serial: 1,
            unanswered: 1,
            command: open_gate(),
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
    assert!(nodes[0].replica.compact(0, 0, Waker::noop()).unwrap());
    drive(&mut nodes[0].replica, true);

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
    let first = rows(nodes[0].replica.engine_mut());
    for id in [1, 2, 4] {
        let r = &mut nodes[usize::try_from(id).unwrap() - 1].replica;
        let mut conf = r.configuration().clone();
        conf.voters.sort_unstable();
        assert_eq!(conf, want, "member {id}");
        assert_eq!(rows(r.engine_mut()), first, "member {id}");
    }
}

/// A member whose engine made its rows durable past the commit its log kept reopens and
/// carries on: the shell raises the log's commit to the engine's durable index as it opens
/// (hyper-raft docs/durable.md §4.3). Here the log is told an older commit directly, once the
/// member has stopped, then the member crashes and opens again.
#[test]
fn a_member_whose_engine_is_ahead_of_its_logs_commit_reopens() {
    use hyper_block::sim::Crash;

    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    for serial in 0..3 {
        nodes[0].replica.propose(&registration(serial)).unwrap();
        settle(&mut nodes, &mut answers);
    }
    let applied = nodes[1].replica.applied();
    assert!(applied > 2);

    // Member 2's engine is durable at what it applied; its log keeps an older commit, written
    // once the member has stopped, since the log takes the group's writes from the member's
    // handle alone while it runs.
    nodes[1]
        .replica
        .compact(u64::MAX, 0, Waker::noop())
        .unwrap();
    let old = std::mem::replace(&mut nodes[1], node(9, 9));
    let mut engine = old.replica.into_engine();
    let hard = old.log.view(GROUP).unwrap().unwrap().hard_state.unwrap();
    old.log
        .write_waiting(
            GROUP,
            hyper_log::Update {
                hard_state: Some(hyper_log::HardState { commit: 1, ..hard }),
                ..hyper_log::Update::default()
            },
        )
        .unwrap();

    // It crashes and opens again from what its device and engine kept.
    engine.crash();
    let file = close(old.log);
    file.crash(Crash::Random).unwrap();
    let (log, recovery) = Log::open(file, log_config(), 0x6c6f67 + 2).unwrap();
    assert!(recovery.damaged.is_empty());
    nodes[1] = Node {
        replica: open(2, &log, engine, &range(), 2).unwrap(),
        log,
    };
    assert_eq!(nodes[1].replica.applied(), applied);
    assert!(nodes[1].replica.durable_commit() >= applied);
    // Hearing from no leader, it times out and campaigns before a leader's append could tell
    // it of the commit.
    for _ in 0..2 * SETTINGS.election_tick {
        nodes[1].replica.tick().unwrap();
    }
    let (out, _) = drive(&mut nodes[1].replica, true);
    assert!(!out.messages.is_empty());

    // It takes part again: a new entry is applied everywhere.
    nodes[0].replica.propose(&registration(9)).unwrap();
    settle(&mut nodes, &mut answers);
    let last = nodes[0].replica.applied();
    assert!(last > applied);
    assert!(nodes.iter().all(|n| n.replica.applied() == last));
    assert_eq!(
        rows(nodes[1].replica.engine_mut()),
        rows(nodes[0].replica.engine_mut())
    );
}

/// A member whose last acknowledged frame is damaged at rest reopens with the term and vote it
/// had, the entries that frame held cut and marked as possibly lacking (raft-log.md §6). It
/// judges a request for its vote against the last entry it acknowledged, not its shorter log: a
/// candidate behind that entry may lack one it helped commit, and gets no answer, while one
/// that holds it does. It campaigns on its log, without its own vote, where the others are a
/// quorum without it (core step R-7): here the two others hold the entry it lacks, so neither
/// grants. Its leader, shown a commit past its log, is told what it lost and sends it the lost
/// entries, not a snapshot (core step R-5), and its mark ends (audit S01, the last frame).
#[test]
fn a_marked_member_is_repaired_by_its_lost_entries_and_elected_only_on_its_log() {
    use hyper_block::sim::Fault as DeviceFault;

    let log_id = |id: u64| 0x6c6f67 + u128::from(id);
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    nodes[0].replica.propose(&registration(0)).unwrap();
    settle(&mut nodes, &mut answers);

    // One more entry: members 2 and 3 write it and acknowledge it, and the leader hears
    // neither answer before member 2 stops.
    nodes[0].replica.propose(&registration(1)).unwrap();
    let (out, _) = drive(&mut nodes[0].replica, true);
    for m in out.messages {
        let to = usize::try_from(m.to).unwrap() - 1;
        nodes[to].replica.step(m).unwrap();
    }
    for n in &mut nodes[1..] {
        let (out, _) = drive(&mut n.replica, true);
        assert!(out.messages.iter().any(|m| m.to == 1));
    }
    let acknowledged = nodes[1].log.view(GROUP).unwrap().unwrap();
    let index = acknowledged.last;
    let acknowledged_term = nodes[1].log.term(GROUP, index).unwrap();

    // Its last frame, the append it acknowledged, is damaged at rest: found as the valid
    // frame of member 2's log with the highest sequence.
    let image = nodes[1]
        .log
        .with_file(|f| f.durable_image())
        .unwrap()
        .unwrap();
    let last_frame = image
        .chunks(4096)
        .enumerate()
        .filter_map(|(i, block)| {
            let h = hyper_log::format::FrameHeader::decode(block)?;
            let frame = image.get(i * 4096..i * 4096 + h.frame_len()?)?;
            (h.log == log_id(2) && h.verifies(frame)).then(|| (h.sequence, i * 4096))
        })
        .max()
        .unwrap()
        .1;
    nodes[1]
        .log
        .with_file(move |f| {
            f.inject(DeviceFault::BitFlip {
                offset: last_frame as u64 + 90,
                bit: 5,
                stored: true,
            })
        })
        .unwrap()
        .unwrap();

    // It restarts from its device and its engine.
    let old = std::mem::replace(&mut nodes[1], node(9, 9));
    let engine = old.replica.into_engine();
    let file = close(old.log);
    let (log, recovery) = Log::open(file, log_config(), log_id(2)).unwrap();
    assert_eq!(recovery.restored, vec![GROUP]);
    nodes[1] = Node {
        replica: open(2, &log, engine, &range(), 2).unwrap(),
        log,
    };
    let view = nodes[1].log.view(GROUP).unwrap().unwrap();
    assert_eq!(
        view.hard_state.map(|h| (h.term, h.vote)),
        acknowledged.hard_state.map(|h| (h.term, h.vote))
    );
    assert_eq!(view.last, index - 1);
    assert_eq!(view.uncertain.map(|m| m.index), Some(index));
    assert!(nodes[1].replica.is_uncertain());

    // It campaigns on its log: both others hold the entry it lacks, and neither grants.
    nodes[1].replica.campaign().unwrap();
    let (out, _) = drive(&mut nodes[1].replica, true);
    let asked: Vec<Message> = out
        .messages
        .into_iter()
        .filter(|m| {
            matches!(
                m.msg_type,
                MessageType::MsgRequestPreVote | MessageType::MsgRequestVote
            )
        })
        .collect();
    assert_eq!(asked.len(), 2, "{asked:?}");
    for request in asked {
        assert_eq!(request.index, index - 1, "it campaigns on its log");
        let to = usize::try_from(request.to).unwrap() - 1;
        nodes[to].replica.step(request).unwrap();
        let (out, _) = drive(&mut nodes[to].replica, true);
        for answer in out.messages.into_iter().filter(|m| m.to == 2) {
            assert!(answer.reject, "a member holding more granted: {answer:?}");
            nodes[1].replica.step(answer).unwrap();
        }
    }
    drive(&mut nodes[1].replica, true);
    assert!(!nodes[1].replica.is_leader());

    // Member 3 campaigns. Its request, as if from a candidate one entry behind what member 2
    // acknowledged, is refused; as sent, holding that entry, it is granted.
    nodes[2].replica.campaign().unwrap();
    let request = drive(&mut nodes[2].replica, true)
        .0
        .messages
        .into_iter()
        .find(|m| m.to == 2)
        .unwrap();
    assert_eq!(
        (request.log_term, request.index),
        (acknowledged_term, index)
    );
    let answer = |nodes: &mut Vec<Node>| {
        drive(&mut nodes[1].replica, true)
            .0
            .messages
            .into_iter()
            .find(|m| m.to == 3)
    };
    let mut behind = request.clone();
    behind.index = index - 1;
    nodes[1].replica.step(behind).unwrap();
    assert!(
        answer(&mut nodes).is_none_or(|m| m.reject),
        "a candidate behind an entry the member acknowledged was granted"
    );
    nodes[1].replica.step(request).unwrap();
    assert!(
        answer(&mut nodes).is_some_and(|m| !m.reject),
        "a candidate holding every entry the member acknowledged was not granted"
    );

    // The group goes on, and its leader, told what member 2 lost, sends it the lost entries,
    // never a snapshot.
    for _ in 0..20 {
        for n in &mut nodes {
            for _ in 0..SETTINGS.heartbeat_tick {
                n.replica.tick().unwrap();
            }
        }
        let mut wire: VecDeque<Message> = VecDeque::new();
        for _ in 0..10_000 {
            for n in nodes.iter_mut() {
                let (mut out, _) = drive(&mut n.replica, true);
                assert!(
                    !out.messages
                        .iter()
                        .any(|m| m.msg_type == MessageType::MsgSnapshot),
                    "a snapshot sent to repair a lost entry"
                );
                wire.extend(out.messages.drain(..));
                answered(n.replica.id(), &mut out, &mut answers);
            }
            let Some(m) = wire.pop_front() else {
                break;
            };
            let _ = nodes[usize::try_from(m.to).unwrap() - 1].replica.step(m);
        }
        if !nodes[1].replica.is_uncertain() {
            break;
        }
    }
    assert!(
        !nodes[1].replica.is_uncertain(),
        "{}",
        nodes[1].replica.describe()
    );
    assert!(nodes[1].log.view(GROUP).unwrap().unwrap().last >= index);
}

/// A log that refuses a write for want of room leaves the replica waiting with it, whole: it
/// takes no message, proposal or campaign (`Stalled`), and no tick moves its timers. Once its
/// own compaction is durable, the next drive makes the refused writes again and the replica goes
/// on (audit S04; hyper-raft docs/durable.md §2.4).
#[test]
fn a_write_refused_for_room_waits_until_the_group_compacts() {
    // A ready of these settings is 512 bytes at most, 102 entries of the fewest bytes, and the
    // log holds a group to that.
    let small = Settings {
        max_size_per_msg: 256,
        max_inflight_msgs: 1,
        max_uncommitted_size: 256,
        max_entry_bytes: 256,
        ..SETTINGS
    };
    let config = LogConfig {
        group_entries: 102,
        group_bytes: 1 << 20,
        ..log_config()
    };
    let log = Log::create(sim(7), config, 0x6c6f67).unwrap();
    let alone = Range {
        boot: ConfState {
            voters: vec![1],
            ..ConfState::default()
        },
        settings: small,
        ..range()
    };
    let mut replica = open(1, &log, first_range(), &alone, 7).unwrap();
    replica.campaign().unwrap();
    assert!(drive(&mut replica, true).1.stalled.is_none());
    assert!(replica.is_leader());
    // One entry a write, none compacted, until the group would retain past its bound.
    let mut stalled = None;
    for serial in 0..200 {
        replica.propose(&registration(serial)).unwrap();
        let (_, driven) = drive(&mut replica, true);
        if driven.stalled.is_some() {
            stalled = driven.stalled;
            break;
        }
    }
    assert!(matches!(stalled, Some(Fault::Room(_))), "{stalled:?}");
    let applied = replica.applied();
    // Waiting: calls are refused, ticks move nothing, and the refused write stays refused.
    replica.tick().unwrap();
    assert!(matches!(
        replica.propose(&registration(999)),
        Err(ReplicaError::Stalled)
    ));
    assert!(drive(&mut replica, true).1.stalled.is_some());
    assert!(replica.compact(0, 0, Waker::noop()).unwrap());
    let (_, driven) = drive(&mut replica, true);
    assert!(driven.stalled.is_none(), "{:?}", driven.stalled);
    assert_eq!(replica.applied(), applied + 1);
    // And the group goes on.
    replica.propose(&registration(1_000)).unwrap();
    drive(&mut replica, true);
    assert_eq!(replica.applied(), applied + 2);
}

/// Each member's log as it stands: its frames, the flushes they and their confirmations made, and
/// the confirmations written on their own (hyper-log's `LogStats`).
fn flushed(nodes: &[Node]) -> Vec<(u64, u64, u64)> {
    nodes
        .iter()
        .map(|n| {
            let stats = n.log.stats(None).unwrap();
            (stats.frames, stats.flushes, stats.confirmations)
        })
        .collect()
}

/// The span of the failure guard on a log's fill ahead ([`filled_ahead`]), the native worker
/// fixtures' (engine's `ranges_room.rs`): a passing run never goes this long without a piece of
/// the fill written, and elapsed time never counts as the fact.
const WAIT: Duration = Duration::from_secs(5);

/// Waits until every member's log has filled ahead the slot past its file's last: written it
/// with zeros a piece at a time while its device idled, and flushed the last piece (hyper-log's
/// `Owner::fill_ahead`, counted in `LogStats::fills`). It is the one write a log makes of its
/// own, from its creation on, at a time its threads choose; the frames the test that waits for it
/// writes all lie in a log's first slot, so a log fills one slot ahead and then writes nothing of
/// its own until a frame opens it. The guard is on the fill's progress, not on the wait's
/// length: a span of [`WAIT`] with neither the fact nor a piece written fails, and the pieces are
/// bounded (a slot's bytes in pieces of at least a device block), so the wait ends.
fn filled_ahead(nodes: &[Node]) {
    let pieces = log_config().segment_bytes / 4096 * nodes.len() as u64;
    let mut spans = pieces + 1;
    let mut seen = None;
    let mut deadline = Instant::now() + WAIT;
    loop {
        let stats: Vec<_> = nodes.iter().map(|n| n.log.stats(None).unwrap()).collect();
        if stats.iter().all(|s| s.fills == 1) {
            return;
        }
        let written: u64 = stats.iter().map(|s| s.fill_pieces).sum();
        if seen != Some(written) {
            assert!(
                spans > 0,
                "a fill ahead wrote more pieces than its slot holds"
            );
            spans -= 1;
            seen = Some(written);
            deadline = Instant::now() + WAIT;
        }
        assert!(
            Instant::now() < deadline,
            "no member's log wrote a piece of its fill ahead for {WAIT:?}, filled {:?}",
            stats
                .iter()
                .map(|s| (s.fills, s.fill_pieces, s.fill_failures))
                .collect::<Vec<_>>()
        );
        std::thread::yield_now();
    }
}

/// What a group's members write while it serves only reads, counted: under a burst, every entry
/// costs each member a frame and its flush, and a confirmation where no frame followed at once
/// (raft-log.md §6), a flush of its own on a device that cannot write it durable alone; once the burst is applied, reads confirmed by their rounds of heartbeats
/// make no write on any member, so no frame and no flush, as long as they go on; a compaction
/// then costs the frame of its new start and its confirmation, and its engine's checkpoint the
/// engine's own (`Stored`'s count, `its_store_flushes_only_at_a_checkpoint_and_twice_there`);
/// and after it, reads flush nothing again. The range writes its commit alone after no quiet
/// period (`QUIET`), so nothing is owed once the burst stops. Each member's log also fills the
/// slot past its file's last ahead once, its last piece flushed, whenever its threads find the
/// device idle ([`filled_ahead`]): the burst is counted once every member's has, so a fill
/// that lands late is never charged to the reads.
#[test]
fn a_group_serving_only_reads_writes_nothing_to_its_logs() {
    const BURST: u64 = 24;
    const READS: u64 = 48;
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    assert!(nodes[0].replica.is_leader());
    let elected = flushed(&nodes);
    for serial in 0..BURST {
        nodes[0].replica.propose(&registration(serial)).unwrap();
        settle(&mut nodes, &mut answers);
    }
    // Every frame and confirmation of the burst was answered durable before its member's drive
    // went on (`support::store`); the fill ahead is the log's own, and is waited for.
    filled_ahead(&nodes);
    let burst = flushed(&nodes);
    for (id, (before, after)) in elected.iter().zip(&burst).enumerate() {
        let frames = after.0 - before.0;
        let flushes = after.1 - before.1;
        let confirmations = after.2 - before.2;
        println!(
            "member {}: a burst of {BURST} entries took {frames} frames, {flushes} flushes, \
             {confirmations} of them confirmations: {:.2} flushes an entry",
            id + 1,
            flushes as f64 / BURST as f64
        );
        // Settled one by one, each entry is a frame of its own on every member, confirmed alone:
        // the simulated device writes a confirmation durable on its own, where a device that
        // cannot flushes it (`LogStats::durable_writes`).
        assert_eq!((frames, confirmations), (BURST, BURST), "member {}", id + 1);
        assert!(flushes >= frames, "member {}", id + 1);
    }
    let mut confirmed = 0;
    let mut read = |nodes: &mut Vec<Node>, from: u64| {
        for i in from..from + READS {
            nodes[0]
                .replica
                .read_index(i.to_le_bytes().to_vec())
                .unwrap();
            let mut wire: VecDeque<Message> = VecDeque::new();
            for _ in 0..1_000 {
                for n in nodes.iter_mut() {
                    let (out, _) = drive(&mut n.replica, true);
                    if n.replica.id() == 1 {
                        confirmed += out.reads.len();
                    }
                    wire.extend(out.messages);
                }
                let Some(m) = wire.pop_front() else {
                    break;
                };
                let to = usize::try_from(m.to).unwrap() - 1;
                let _ = nodes[to].replica.step(m);
            }
        }
    };
    read(&mut nodes, 0);
    assert_eq!(flushed(&nodes), burst, "reads alone wrote to a log");
    // A compaction is the one write a quiet group makes, and only when asked.
    let compacted = nodes
        .iter_mut()
        .map(|n| n.replica.compact(0, 0, Waker::noop()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(compacted, vec![true; 3]);
    settle(&mut nodes, &mut answers);
    let after = flushed(&nodes);
    for (id, (before, after)) in burst.iter().zip(&after).enumerate() {
        assert_eq!(
            (after.0 - before.0, after.2 - before.2),
            (1, 1),
            "member {}: a compaction's start is one frame, confirmed alone",
            id + 1
        );
        assert!(after.1 > before.1, "member {}: its frame flushed", id + 1);
    }
    read(&mut nodes, READS);
    assert_eq!(
        flushed(&nodes),
        after,
        "reads after a compaction wrote to a log"
    );
    assert_eq!(
        confirmed,
        usize::try_from(2 * READS).unwrap(),
        "every read confirmed"
    );
}

/// A member refuses settings its log cannot hold to: an entry of the range's largest must fit
/// one frame, and a write of the range's largest must fit the group's bounds.
#[test]
fn a_member_refuses_settings_its_log_cannot_hold() {
    let claim = |settings: Settings, config: LogConfig| {
        let log = Log::create(sim(8), config, 0x6c6f67).unwrap();
        let r = Range {
            settings,
            ..range()
        };
        mantle_range::claim(&log, GROUP, &r).map(|_| ())
    };
    assert!(claim(SETTINGS, log_config()).is_ok());
    let wide = Settings {
        max_entry_bytes: 64 * 4096,
        ..SETTINGS
    };
    assert!(matches!(
        claim(wide, log_config()),
        Err(ReplicaError::Config(_))
    ));
    let narrow = LogConfig {
        group_bytes: 1 << 16,
        ..log_config()
    };
    assert!(matches!(
        claim(SETTINGS, narrow),
        Err(ReplicaError::Config(_))
    ));
    let few = LogConfig {
        group_entries: 1 << 10,
        ..log_config()
    };
    assert!(matches!(claim(SETTINGS, few), Err(ReplicaError::Config(_))));
}

/// A snapshot's fate reported while its sender has a write out is taken, not refused: the leader
/// pauses replication to a member until it learns its snapshot's fate, and a report refused and
/// lost left it paused for good. A seed of the simulation found it, a member one snapshot behind
/// forever once faults stopped.
#[test]
fn a_snapshot_report_that_comes_while_a_write_is_out_is_taken() {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, 40 + id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    let entry = |commands| Entry {
        at_ns: 10,
        commands,
    };
    nodes[0].replica.propose(&registration(0)).unwrap();
    settle(&mut nodes, &mut answers);
    // Entries member 3 never hears of, then a compaction that leaves it a snapshot behind.
    let cut_off = |nodes: &mut [Node]| {
        let mut wire: VecDeque<Message> = VecDeque::new();
        for _ in 0..10_000 {
            for n in nodes.iter_mut().take(2) {
                wire.extend(drive(&mut n.replica, true).0.messages);
            }
            let Some(m) = wire.pop_front() else { return };
            if m.to != 3 {
                let _ = nodes[usize::try_from(m.to).unwrap() - 1].replica.step(m);
            }
        }
        panic!("never settled");
    };
    for _ in 0..3 {
        nodes[0].replica.propose(&entry(Vec::new())).unwrap();
        cut_off(&mut nodes);
    }
    assert!(nodes[0].replica.compact(0, 0, Waker::noop()).unwrap());
    drive(&mut nodes[0].replica, true);
    // The leader sends member 3 its snapshot, which is lost.
    let mut snapshot = None;
    for _ in 0..200 {
        nodes[0].replica.tick().unwrap();
        for m in drive(&mut nodes[0].replica, true).0.messages {
            if m.to == 3 && m.msg_type == MessageType::MsgSnapshot {
                snapshot = Some(m);
            } else if m.to == 3 {
                let _ = nodes[2].replica.step(m);
                for back in drive(&mut nodes[2].replica, true).0.messages {
                    let _ = nodes[0].replica.step(back);
                }
            }
        }
        if snapshot.is_some() {
            break;
        }
    }
    assert!(
        snapshot.is_some(),
        "no snapshot was sent: {}",
        nodes[0].replica.describe()
    );
    // Its loss is reported while the leader has a write out.
    nodes[0].replica.propose(&entry(Vec::new())).unwrap();
    let (out, _) = drive(&mut nodes[0].replica, false);
    assert!(
        nodes[0].replica.in_flight() > 0,
        "the leader has a write out"
    );
    nodes[0].replica.report_snapshot(3, false).unwrap();
    // Delivered in order from here, every snapshot reported as it arrives.
    let mut wire: VecDeque<Message> = out.messages.into();
    for _ in 0..10_000 {
        for n in &mut nodes {
            wire.extend(drive(&mut n.replica, true).0.messages);
        }
        let Some(m) = wire.pop_front() else { break };
        let (from, to) = (m.from, m.to);
        let is_snapshot = m.msg_type == MessageType::MsgSnapshot;
        let _ = nodes[usize::try_from(to).unwrap() - 1].replica.step(m);
        if is_snapshot {
            nodes[usize::try_from(from).unwrap() - 1]
                .replica
                .report_snapshot(to, true)
                .unwrap();
        }
        if wire.is_empty() {
            for _ in 0..SETTINGS.heartbeat_tick {
                nodes[0].replica.tick().unwrap();
            }
        }
        if nodes[2].replica.applied() == nodes[0].replica.applied() {
            break;
        }
    }
    assert_eq!(
        nodes[2].replica.applied(),
        nodes[0].replica.applied(),
        "member 3 never caught up: {}",
        nodes[0].replica.describe()
    );
}

/// A node that overlaps its members' flushes finds each member's writes still out when the
/// network next delivers to it: under steady load the leader always has one, and so, a round
/// after each append, do its followers. The core takes the messages that come then (core step
/// R-4); refused, as they once were, every acknowledgement reached the leader while it flushed
/// and was lost, and none of the load ever committed.
#[test]
fn a_leader_whose_answers_come_while_its_writes_are_out_still_commits() {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, 60 + id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    assert!(nodes[0].replica.is_leader());
    let before = nodes[0].replica.applied();

    let mut wire: Vec<Message> = Vec::new();
    let mut proposed = 0u64;
    for round in 0..200u64 {
        // Each node drives once, taking what the log answered since and leaving what it
        // submits out, and the leader proposes.
        for (i, n) in nodes.iter_mut().enumerate() {
            wire.extend(drive(&mut n.replica, false).0.messages);
            if i == 0
                && n.replica
                    .propose(&Entry {
                        at_ns: 100 + round,
                        commands: Vec::new(),
                    })
                    .is_ok()
            {
                proposed += 1;
                wire.extend(drive(&mut n.replica, false).0.messages);
            }
        }
        // What was sent arrives, and the clock ticks, while those writes are out.
        for m in std::mem::take(&mut wire) {
            let to = usize::try_from(m.to).unwrap() - 1;
            let _ = nodes[to].replica.step(m);
        }
        for n in nodes.iter_mut() {
            n.replica.tick().unwrap();
        }
    }
    let committed = nodes[0].replica.applied() - before;
    assert!(proposed >= 50, "the leader took {proposed} proposals");
    // All but the proposals of the last rounds, still in flight, commit.
    assert!(
        committed + 3 >= proposed,
        "{committed} of {proposed} proposals committed: {}",
        nodes[0].replica.describe()
    );
    assert!(nodes[0].replica.is_leader());
}

/// Ticks that come while a member's write is out move its clock at once: a leader whose every
/// tick lands while a write is out still sends its heartbeats. Dropped, as they once were, its
/// clock stood still for as long as it had load.
#[test]
fn ticks_while_a_write_is_out_still_count() {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, 70 + id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    nodes[0]
        .replica
        .propose(&Entry {
            at_ns: 10,
            commands: Vec::new(),
        })
        .unwrap();
    drive(&mut nodes[0].replica, false);
    assert!(nodes[0].replica.in_flight() > 0);
    for _ in 0..SETTINGS.heartbeat_tick {
        nodes[0].replica.tick().unwrap();
    }
    let messages = drive(&mut nodes[0].replica, true).0.messages;
    let mut to: Vec<u64> = of(&messages, MessageType::MsgHeartbeat)
        .iter()
        .map(|m| m.to)
        .collect();
    to.sort_unstable();
    assert_eq!(to, [2, 3], "{messages:?}");
}

/// A member whose writes stay out through many election timeouts, its device holding them while
/// its owner ticks and drives it, campaigns as its clock says, and every campaign supersedes the
/// vote requests of the one before that have not left (hyper-raft core). When its device goes on
/// it sends at most one campaign's requests for each write it had out and its last campaign's,
/// not one campaign's for every timeout: the replica that held the ticks once replayed 100
/// timeouts as 116 requests at once, and its bound was one timeout's worth.
#[test]
fn a_member_whose_writes_stay_out_through_many_timeouts_sends_one_campaign_a_write() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let hold: &'static AtomicBool = Box::leak(Box::new(AtomicBool::new(false)));
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, 80 + id)).collect();
    // Member 2's device holds its writes on the test's word.
    let log = Log::create(sim(82), log_config(), 0x6c6f67 + 2).unwrap();
    let store = Synchronous::new(
        mantle_range::claim(&log, GROUP, &range()).unwrap(),
        Some(hold),
    );
    nodes[1] = Node {
        replica: Replica::open(2, store, first_range(), &range(), 82).unwrap(),
        log,
    };
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    nodes[0]
        .replica
        .propose(&Entry {
            at_ns: 10,
            commands: Vec::new(),
        })
        .unwrap();
    let (out, _) = drive(&mut nodes[0].replica, true);
    hold.store(true, Ordering::SeqCst);
    for m in out.messages.into_iter().filter(|m| m.to == 2) {
        nodes[1].replica.step(m).unwrap();
    }
    // A hundred of the longest timeouts the core draws, ticked and driven, the device holding.
    let mut sent = 0;
    let mut most_out = 0;
    for _ in 0..100 * 2 * SETTINGS.election_tick {
        nodes[1].replica.tick().unwrap();
        let (out, _) = drive(&mut nodes[1].replica, false);
        sent += out.messages.len();
        most_out = most_out.max(nodes[1].replica.in_flight());
    }
    assert_eq!(sent, 0, "a follower's message left before its write");
    hold.store(false, Ordering::SeqCst);
    let (out, _) = drive(&mut nodes[1].replica, true);
    let campaigns = out
        .messages
        .iter()
        .filter(|m| {
            matches!(
                m.msg_type,
                MessageType::MsgRequestVote | MessageType::MsgRequestPreVote
            )
        })
        .count();
    // A campaign asks each of the two others: one for each write it had out, and its last.
    assert!(
        campaigns <= 2 * (most_out + 1),
        "{campaigns} requests for votes with {most_out} writes out: {:?}",
        out.messages
    );
}

/// A read no quorum confirms is never served: a leader whose heartbeats reach no one steps down
/// within an election timeout (check-quorum) and drops the reads it held, as a lost message
/// is dropped, and the read's caller, hearing nothing, asks again of whoever leads, which
/// confirms it.
#[test]
fn a_read_no_quorum_confirms_is_never_served_and_is_asked_again() {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, 90 + id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    assert!(nodes[0].replica.is_leader());
    nodes[0].replica.read_index(b"lost".to_vec()).unwrap();
    // Nothing the leader sends is delivered from here on.
    for _ in 0..2 * SETTINGS.election_tick {
        nodes[0].replica.tick().unwrap();
        let (out, _) = drive(&mut nodes[0].replica, true);
        assert!(out.reads.is_empty(), "a read confirmed with no quorum");
    }
    assert!(
        !nodes[0].replica.is_leader(),
        "a leader no quorum hears kept leading"
    );
    // Heard again, the group elects, and the read asked anew is confirmed.
    let leader = loop {
        if let Some(i) = nodes.iter().position(|n| n.replica.is_leader()) {
            break i;
        }
        for n in &mut nodes {
            n.replica.tick().unwrap();
        }
        settle(&mut nodes, &mut answers);
    };
    nodes[leader].replica.read_index(b"again".to_vec()).unwrap();
    let mut confirmed = Vec::new();
    for _ in 0..SETTINGS.election_tick {
        for _ in 0..SETTINGS.heartbeat_tick {
            nodes[leader].replica.tick().unwrap();
        }
        let mut wire: VecDeque<Message> = VecDeque::new();
        for _ in 0..1_000 {
            for n in nodes.iter_mut() {
                let (out, _) = drive(&mut n.replica, true);
                if n.replica.is_leader() {
                    confirmed.extend(out.reads);
                }
                wire.extend(out.messages);
            }
            let Some(m) = wire.pop_front() else {
                break;
            };
            let _ = nodes[usize::try_from(m.to).unwrap() - 1].replica.step(m);
        }
        if !confirmed.is_empty() {
            break;
        }
    }
    assert_eq!(
        confirmed.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>(),
        [b"again".to_vec()]
    );
}

/// Members of a group of three, each with its own device and log, found by identity: a member
/// rebuilt under a new identity keeps its device.
///
/// A device is its log's while the member runs, and the test reaches it through the log
/// (`Devices::with_file`); a stopped member's device is the test's, to damage at rest, and a
/// stand-in on a device of its own holds its slot. A member's log is its node's, which the test
/// reads.
struct Devices {
    files: Vec<Option<SimFile>>,
    nodes: Vec<Node>,
}

fn log_id(slot: usize) -> u128 {
    0x6c6f67 + u128::try_from(slot).unwrap() + 1
}

impl Devices {
    fn new(seed: u64) -> Self {
        Self::logged(seed, log_config())
    }

    /// As `new`, each member's log under `config`.
    fn logged(seed: u64, config: LogConfig) -> Self {
        let files = (0..3).map(|_| None).collect();
        let nodes = (0..3)
            .zip(1u64..)
            .map(|(slot, id)| {
                let device = sim(seed + u64::try_from(slot).unwrap());
                let log = Log::create(device, config, log_id(slot)).unwrap();
                let replica = open(id, &log, first_range(), &range(), seed + id).unwrap();
                Node { replica, log }
            })
            .collect();
        Self { files, nodes }
    }

    /// Stops the member in `slot`, as a crash does once its log and engine are durable: its
    /// engine, which the caller keeps or drops, comes back.
    fn stop(&mut self, slot: usize) -> Model {
        let old = std::mem::replace(&mut self.nodes[slot], node(9, 9));
        let engine = old.replica.into_engine();
        self.files[slot] = Some(close(old.log));
        engine
    }

    /// Runs `look` on the device in `slot`, through its log while it runs.
    fn with_file<R: Send + 'static>(
        &self,
        slot: usize,
        look: impl FnOnce(&SimFile) -> R + Send + 'static,
    ) -> R {
        match &self.files[slot] {
            Some(file) => look(file),
            None => self.nodes[slot].log.with_file(look).unwrap(),
        }
    }

    fn slot(&self, id: u64) -> Option<usize> {
        self.nodes.iter().position(|n| n.replica.id() == id)
    }

    /// Drives every member but those in `out` until no message is left in flight, delivering in
    /// order: each applied entry's answers go to `answers` by member.
    fn settle(&mut self, out: &[u64], answers: &mut Answered) {
        self.settle_checking(out, answers, |_, _| {});
    }

    /// As `settle`, showing `check` every message a member sends, with that member's log.
    fn settle_checking(
        &mut self,
        out: &[u64],
        answers: &mut Answered,
        mut check: impl FnMut(&Message, &Log<SimFile>),
    ) {
        let mut wire: VecDeque<Message> = VecDeque::new();
        for _ in 0..10_000 {
            for (n, file) in self.nodes.iter_mut().zip(&self.files) {
                let id = n.replica.id();
                if out.contains(&id) {
                    continue;
                }
                let (mut drove, _) = drive(&mut n.replica, true);
                // A running member's log; a stopped one's device is the test's.
                if file.is_none() {
                    for m in &drove.messages {
                        check(m, &n.log);
                    }
                }
                wire.extend(drove.messages.drain(..));
                answered(id, &mut drove, answers);
            }
            let Some(m) = wire.pop_front() else {
                return;
            };
            if out.contains(&m.to) {
                continue;
            }
            if let Some(slot) = self.slot(m.to) {
                // A message the core refuses changes nothing.
                let _ = self.nodes[slot].replica.step(m);
            }
        }
        panic!("the group never settled");
    }

    /// Ticks every member but those in `out` for a heartbeat, then settles.
    fn beat(&mut self, out: &[u64], answers: &mut Answered) {
        for n in &mut self.nodes {
            if !out.contains(&n.replica.id()) {
                for _ in 0..SETTINGS.heartbeat_tick {
                    n.replica.tick().unwrap();
                }
            }
        }
        self.settle(out, answers);
    }

    /// The member among those not in `out` that leads, once one does.
    fn leader(&mut self, out: &[u64], answers: &mut Answered) -> usize {
        for _ in 0..10 * SETTINGS.election_tick {
            if let Some(slot) = self
                .nodes
                .iter()
                .position(|n| !out.contains(&n.replica.id()) && n.replica.is_leader())
            {
                return slot;
            }
            self.beat(out, answers);
        }
        panic!("no leader among the members left");
    }
}

/// Every member's answers at each index, which must agree with every other's.
fn agreed(answers: &Answered) -> std::collections::BTreeMap<u64, Vec<Answer>> {
    let mut by_index = std::collections::BTreeMap::new();
    for (id, index, a) in answers {
        let first = by_index.entry(*index).or_insert_with(|| a.clone());
        assert_eq!(first, a, "member {id} applied index {index} another way");
    }
    by_index
}

/// A member whose device fails the write or flush of a `Ready`: the member is fenced. The drive
/// that takes the failure answers `Fenced`, the member sent no acknowledgement of what that write
/// held, and every later call that could acknowledge anything or move its state answers `Fenced`
/// too: ticks, messages, proposals, changes, reads, campaigns, snapshot reports, compaction and
/// drives. The two others go on without it, electing a leader if it led, and commit. A log that
/// only refused the write for room leaves the member waiting with it instead
/// (`a_write_refused_for_room_waits_until_the_group_compacts` and those after it).
fn a_durability_failure_fences(fault: &hyper_block::sim::Fault, failing: u64) {
    use mantle_range::ConfChangeV2;

    // Each log writes only frames: a log under `Waits::Measured` writes zeros ahead into the slot
    // it grows into whenever its device is idle (hyper-log's fill ahead), which the fault, one
    // write's or one flush's, may meet first, so the failure lands on the entry's frame here only
    // under `Waits::Never`, where nothing the writer does depends on when its device is idle.
    let config = LogConfig {
        waits: Waits::Never,
        ..log_config()
    };
    let mut d = Devices::logged(60 + failing, config);
    let mut answers = Vec::new();
    d.nodes[0].replica.campaign().unwrap();
    d.settle(&[], &mut answers);
    d.nodes[0].replica.propose(&registration(0)).unwrap();
    d.settle(&[], &mut answers);
    let before = d.nodes[0].replica.applied();

    let slot = d.slot(failing).unwrap();
    let fault = fault.clone();
    d.with_file(slot, move |f| f.inject(fault)).unwrap();
    d.nodes[0].replica.propose(&registration(1)).unwrap();
    let entry = before + 1;
    // Drive in order, as `settle` does, until the failing member meets its fault.
    let mut wire: VecDeque<Message> = VecDeque::new();
    let mut failure = None;
    'drive: for _ in 0..1_000 {
        for n in &mut d.nodes {
            let mut out = Out::default();
            match n.replica.drive(0, Waker::noop(), &mut out) {
                Ok(_) => {
                    if n.replica.id() == failing {
                        // An acknowledgement of the entry leaves only once it is durable.
                        assert!(
                            !out.messages.iter().any(|m| {
                                m.msg_type == MessageType::MsgAppendResponse
                                    && !m.reject
                                    && m.index >= entry
                            }),
                            "the failing member acknowledged the entry"
                        );
                    }
                    wire.extend(out.messages);
                }
                Err(e) => {
                    assert_eq!(n.replica.id(), failing, "{e}");
                    failure = Some(e);
                    break 'drive;
                }
            }
        }
        let Some(m) = wire.pop_front() else {
            continue;
        };
        if let Some(to) = d.slot(m.to) {
            let _ = d.nodes[to].replica.step(m);
        }
    }
    assert!(
        matches!(failure, Some(ReplicaError::Fenced(_))),
        "{failure:?}"
    );
    let fenced = &mut d.nodes[slot].replica;
    assert!(fenced.is_fenced());
    let heartbeat = Message {
        msg_type: MessageType::MsgHeartbeat,
        from: if failing == 1 { 2 } else { 1 },
        to: failing,
        term: fenced.term(),
        ..Message::default()
    };
    let calls: [(&str, Result<(), ReplicaError>); 9] = [
        ("tick", fenced.tick()),
        ("step", fenced.step(heartbeat)),
        ("propose", fenced.propose(&registration(2))),
        (
            "propose_change",
            fenced.propose_change(&ConfChangeV2::default()),
        ),
        ("read_index", fenced.read_index(b"r".to_vec())),
        ("campaign", fenced.campaign()),
        ("report_snapshot", fenced.report_snapshot(3, true)),
        ("compact", fenced.compact(0, 0, Waker::noop()).map(|_| ())),
        (
            "drive",
            fenced
                .drive(0, Waker::noop(), &mut Out::default())
                .map(|_| ()),
        ),
    ];
    for (call, result) in calls {
        assert!(
            matches!(result, Err(ReplicaError::Fenced(_))),
            "{call} on a fenced member: {result:?}"
        );
    }
    let applied = d.nodes[slot].replica.applied();

    // The others go on without it: one leads, and an entry proposed now commits on both.
    let out = [failing];
    let leader = d.leader(&out, &mut answers);
    d.nodes[leader].replica.propose(&registration(3)).unwrap();
    d.settle(&out, &mut answers);
    let last = d.nodes[leader].replica.applied();
    assert!(
        last > entry,
        "the group committed nothing after the failure"
    );
    for n in &d.nodes {
        if n.replica.id() != failing {
            assert_eq!(n.replica.applied(), last, "member {}", n.replica.id());
        }
    }
    assert_eq!(d.nodes[slot].replica.applied(), applied);
    agreed(&answers);
}

/// A failed flush fences the member, whether it follows or leads (audit S04).
#[test]
fn a_failed_flush_fences_the_member_and_the_group_goes_on_without_it() {
    for failing in [2, 1] {
        a_durability_failure_fences(&hyper_block::sim::Fault::SyncError, failing);
    }
}

/// A failed write fences the member, whether it follows or leads (audit S04).
#[test]
fn a_failed_write_fences_the_member_and_the_group_goes_on_without_it() {
    for failing in [2, 1] {
        a_durability_failure_fences(&hyper_block::sim::Fault::WriteError, failing);
    }
}

/// A group of one on a log that holds `config`'s groups and segments. The test keeps the log,
/// and writes other groups to it.
fn alone(config: LogConfig, settings: Settings, seed: u64) -> (Log<SimFile>, Member) {
    let log = Log::create(sim(seed), config, 0x6c6f67).unwrap();
    let range = Range {
        boot: ConfState {
            voters: vec![1],
            ..ConfState::default()
        },
        settings,
        ..range()
    };
    let replica = open(1, &log, first_range(), &range, seed).unwrap();
    (log, replica)
}

/// A log that holds all the groups it may refuses a new member's first write: the member waits
/// with it, whole, and is not fenced; once another group leaves the log and its owner says room
/// may have been freed (`Replica::resume`), the next drive makes the write again and the member
/// goes on (audit S04).
#[test]
fn a_write_refused_for_want_of_a_group_waits_until_one_leaves() {
    let config = LogConfig {
        max_groups: 2,
        ..log_config()
    };
    let (log, mut replica) = alone(config, SETTINGS, 11);
    for group in [7, 8] {
        log.write(
            group,
            hyper_log::Update {
                hard_state: Some(hyper_log::HardState {
                    term: 1,
                    vote: 0,
                    commit: 0,
                    held: None,
                }),
                ..hyper_log::Update::default()
            },
        )
        .unwrap();
    }
    replica.campaign().unwrap();
    let (_, driven) = drive(&mut replica, true);
    assert!(matches!(driven.stalled, Some(Fault::Room(_))), "{driven:?}");
    assert!(!replica.is_fenced());
    assert!(matches!(
        replica.propose(&registration(0)),
        Err(ReplicaError::Stalled)
    ));
    // Still waiting while nothing has left.
    assert!(drive(&mut replica, true).1.stalled.is_some());
    log.write(
        7,
        hyper_log::Update {
            remove: true,
            ..hyper_log::Update::default()
        },
    )
    .unwrap();
    replica.resume();
    let (_, driven) = drive(&mut replica, true);
    assert!(driven.stalled.is_none(), "{:?}", driven.stalled);
    assert!(replica.is_leader());
    let applied = replica.applied();
    replica.propose(&registration(0)).unwrap();
    drive(&mut replica, true);
    assert_eq!(replica.applied(), applied + 1);
}

/// A member whose group's records on its log are damaged at rest, found when it restarts, is
/// quarantined and rebuilt from its peers (audit S01c; raft-log.md §6, replica.md §6). Its last
/// frame held a fast-track proposal, which a persist record does not carry, so the log fences
/// the group and serves it to no one. The member does not open under its identity: it may have
/// voted in terms its log no longer shows. The group goes on without it. The node rebuilds it
/// under a new identity on the same device: the group's records are removed, a new member opens
/// there, and the leader runs the replacement, adding it as a learner, catching it up, and
/// swapping it for the damaged one. After, the group is whole, every member holds every entry
/// any member applied with the same answers, and the same rows, and it goes on committing.
#[test]
fn a_member_whose_group_was_damaged_at_rest_is_rebuilt_from_its_peers() {
    use mantle_range::membership::Next;

    let mut d = Devices::new(70);
    let mut answers = Vec::new();
    d.nodes[0].replica.campaign().unwrap();
    d.settle(&[], &mut answers);
    d.nodes[0].replica.propose(&registration(0)).unwrap();
    d.settle(&[], &mut answers);
    let agreed_now = agreed(&answers);
    let session = agreed_now
        .values()
        .find_map(|a| match a[..] {
            [Answer::Registered { session }] => Some(session),
            _ => None,
        })
        .unwrap();
    let write = |serial: u64, command: Command| Entry {
        at_ns: 20,
        commands: vec![Sessioned {
            session,
            serial,
            unanswered: serial,
            command,
        }],
    };
    d.nodes[0].replica.propose(&write(1, open_gate())).unwrap();
    d.settle(&[], &mut answers);
    for (serial, key) in [(2, "k1"), (3, "k2")] {
        d.nodes[0]
            .replica
            .propose(&write(serial, put(key)))
            .unwrap();
        d.settle(&[], &mut answers);
    }

    // Member 3 stops. Its last frame, as a member on the fast track leaves one, holds a
    // proposal of its own; then that frame is damaged on the medium.
    drop(d.stop(2));
    let file = d.files[2].take().unwrap();
    let file = {
        let (log, _) = Log::open(file, log_config(), log_id(2)).unwrap();
        let view = log.view(GROUP).unwrap().unwrap();
        let term = view.hard_state.unwrap().term;
        log.write(
            GROUP,
            hyper_log::Update {
                proposals: vec![hyper_log::Proposal {
                    index: view.last + 1,
                    term,
                    bytes: b"fast".to_vec(),
                }],
                ..hyper_log::Update::default()
            },
        )
        .unwrap();
        log.close().unwrap()
    };
    damage_last_frame(&file, log_id(2));

    // It restarts: the log reports its group damaged, and the member does not open.
    let (log, recovery) = Log::open(file, log_config(), log_id(2)).unwrap();
    assert_eq!(recovery.damaged, vec![GROUP]);
    assert!(matches!(
        open(3, &log, first_range(), &range(), 3),
        Err(ReplicaError::Damaged)
    ));

    // The group goes on without it.
    let out = [3, 9];
    d.nodes[0].replica.propose(&write(4, put("k3"))).unwrap();
    d.settle(&out, &mut answers);

    // The node rebuilds it as member 4, and the leader runs the replacement.
    mantle_range::remove(&log, GROUP).unwrap();
    let store = Synchronous::new(mantle_range::claim(&log, GROUP, &range()).unwrap(), None);
    let (rebuilt, replacement) = Replica::rebuild(3, 4, store, first_range(), &range(), 4).unwrap();
    assert_eq!(rebuilt.id(), 4);
    d.nodes[2] = Node {
        replica: rebuilt,
        log,
    };
    let mut done = false;
    for _ in 0..50 {
        let leader = d.leader(&[3], &mut answers);
        let l = &mut d.nodes[leader].replica;
        match replacement.next(l.configuration(), l.caught_up(4), l.configuration_known()) {
            Next::Done => {
                done = true;
                break;
            }
            Next::Wait => {}
            Next::Propose(change) => l.propose_change(&change).unwrap(),
        }
        d.beat(&[3], &mut answers);
    }
    assert!(
        done,
        "the rebuild never finished: {}",
        d.nodes[0].replica.describe()
    );

    // The group is whole again, and goes on.
    let leader = d.leader(&[3], &mut answers);
    d.nodes[leader]
        .replica
        .propose(&write(5, put("k4")))
        .unwrap();
    d.settle(&[3], &mut answers);
    let want = ConfState {
        voters: vec![1, 2, 4],
        ..ConfState::default()
    };
    let by_index = agreed(&answers);
    let last = *by_index.keys().max().unwrap();
    let rows_of_leader = rows(d.nodes[leader].replica.engine_mut());
    for n in &mut d.nodes {
        let r = &mut n.replica;
        let mut conf = r.configuration().clone();
        conf.voters.sort_unstable();
        assert_eq!(conf, want, "member {}", r.id());
        assert!(r.applied() >= last, "member {}", r.id());
        assert_eq!(rows(r.engine_mut()), rows_of_leader, "member {}", r.id());
        for key in ["k1", "k2", "k3", "k4"] {
            let current = name::current(r.engine_mut(), "b", key).unwrap().unwrap();
            assert_eq!(current.1.etag, key, "member {}", r.id());
        }
    }
    // Every entry acknowledged before the damage is held with the answers it had then.
    for (index, a) in agreed_now {
        assert_eq!(by_index.get(&index), Some(&a), "index {index}");
    }
}

/// Flips a bit of the last frame of log `id` on `file`'s medium, as damage at rest does: a fault
/// written to the medium, so no clearing of faults heals it.
fn damage_last_frame(file: &SimFile, id: u128) {
    use hyper_block::block::BlockFile;
    use hyper_block::buf::AlignedBuf;

    let image = file.durable_image().unwrap();
    let last_frame = image
        .chunks(4096)
        .enumerate()
        .filter_map(|(i, block)| {
            let h = hyper_log::format::FrameHeader::decode(block)?;
            let frame = image.get(i * 4096..i * 4096 + h.frame_len()?)?;
            (h.log == id && h.verifies(frame)).then(|| (h.sequence, i * 4096))
        })
        .max()
        .unwrap()
        .1;
    let mut block = AlignedBuf::zeroed(4096, Alignment::new(4096).unwrap()).unwrap();
    block.set_len(4096).unwrap();
    file.read_exact_at(block.as_mut_slice(), last_frame as u64)
        .unwrap();
    block.as_mut_slice()[90] ^= 1 << 5;
    file.write_all_at(block.as_slice(), last_frame as u64)
        .unwrap();
    file.sync_data().unwrap();
}

/// A sole voter's engine applies the entries its last frame holds and makes its rows durable;
/// the frame is then damaged at rest. The member keeps its identity: its engine keeps the term
/// of the last entry it applied beside its index, so the log starts at the engine's point,
/// whatever the log lost, where the replica once had to infer the term from the log and was
/// rebuilt where it could not (docs/design/replica.md §4; hyper-raft docs/durable.md §4.3). The
/// mark stays while the log lacks an entry it covers, and the member goes on.
#[test]
fn a_member_whose_engine_applied_what_its_damaged_log_lost_keeps_its_identity() {
    let (log, mut replica) = alone(log_config(), SETTINGS, 13);
    replica.campaign().unwrap();
    drive(&mut replica, true);
    assert!(replica.is_leader());
    for serial in 0..3 {
        replica.propose(&registration(serial)).unwrap();
        drive(&mut replica, true);
    }
    let applied = replica.applied();
    let term = replica.term();
    // The engine makes its rows durable through what it applied, the log's start unmoved.
    assert!(!replica.compact(u64::MAX, 0, Waker::noop()).unwrap());
    let engine = replica.into_engine();
    assert_eq!(engine.durable(), applied);
    let file = close(log);
    damage_last_frame(&file, 0x6c6f67);
    let (log, recovery) = Log::open(file, log_config(), 0x6c6f67).unwrap();
    assert_eq!(recovery.restored, vec![GROUP]);
    let view = log.view(GROUP).unwrap().unwrap();
    assert!(view.last < applied, "the lost frame held an applied entry");
    let mark = view.uncertain.expect("the lost frame is marked");

    let alone = Range {
        boot: ConfState {
            voters: vec![1],
            ..ConfState::default()
        },
        ..range()
    };
    let mut member = open(1, &log, engine, &alone, 13).unwrap();
    let view = log.view(GROUP).unwrap().unwrap();
    assert_eq!(
        view.start,
        hyper_log::Start {
            index: applied,
            term
        }
    );
    assert_eq!(view.last, applied);
    assert!(view.hard_state.is_some_and(|h| h.commit >= applied));
    assert_eq!(member.applied(), applied);
    assert_eq!(member.is_uncertain(), applied < mark.index);
    // A sole voter whose log may lack what it acknowledged waits (core step R-7): it is no
    // quorum without itself.
    if member.is_uncertain() {
        assert!(matches!(member.campaign(), Err(ReplicaError::Uncertain)));
    } else {
        member.campaign().unwrap();
        drive(&mut member, true);
        assert!(member.is_leader());
    }
}

/// A member of a group of three applies an entry its last frame held, makes its rows durable,
/// and stops; the frame is damaged at rest. It reopens in place, under its identity, its log
/// starting at the entry its engine applied, and the group repairs it from its peers: every
/// acknowledgement it sends names only entries its log holds, it takes the entries after, and
/// the group, whole, commits on. No entry any member applied is lost, and every member holds the
/// same rows (audit S01c).
#[test]
fn a_member_whose_engine_applied_what_its_damaged_log_lost_is_repaired_in_place() {
    let mut d = Devices::new(90);
    let mut answers = Vec::new();
    d.nodes[0].replica.campaign().unwrap();
    d.settle(&[], &mut answers);
    d.nodes[0].replica.propose(&registration(0)).unwrap();
    d.settle(&[], &mut answers);
    // Member 2 is cut off while the others commit an entry; then it takes the entry and the
    // commit in one append, and applies it.
    d.nodes[0].replica.propose(&registration(1)).unwrap();
    d.settle(&[2], &mut answers);
    let entry = d.nodes[0].replica.applied();
    for _ in 0..4 {
        d.beat(&[], &mut answers);
        if d.nodes[1].replica.applied() == entry {
            break;
        }
    }
    assert_eq!(d.nodes[1].replica.applied(), entry);
    d.nodes[1]
        .replica
        .compact(u64::MAX, 0, Waker::noop())
        .unwrap();
    let engine = d.stop(1);
    assert_eq!(engine.durable(), entry);
    let file = d.files[1].take().unwrap();
    damage_last_frame(&file, log_id(1));

    let (log, recovery) = Log::open(file, log_config(), log_id(1)).unwrap();
    assert_eq!(recovery.restored, vec![GROUP]);
    let view = log.view(GROUP).unwrap().unwrap();
    assert!(view.last < entry, "the lost frame held no applied entry");
    assert_eq!(view.uncertain.map(|m| m.index), Some(entry));
    let member = open(2, &log, engine, &range(), 2).unwrap();
    assert_eq!(member.applied(), entry);
    d.nodes[1] = Node {
        replica: member,
        log,
    };

    // The group goes on, and repairs it: what it acknowledges, its log holds.
    let mut acknowledged = 0;
    for serial in 2..5 {
        d.nodes[0].replica.propose(&registration(serial)).unwrap();
        d.settle_checking(&[], &mut answers, |m, log| {
            if m.from == 2 && m.msg_type == MessageType::MsgAppendResponse && !m.reject {
                let last = log.view(GROUP).unwrap().unwrap().last;
                assert!(m.index <= last, "acknowledged {} holding {last}", m.index);
                acknowledged += 1;
            }
        });
    }
    assert!(acknowledged > 0);
    let last = d.nodes[0].replica.applied();
    agreed(&answers);
    let first = rows(d.nodes[0].replica.engine_mut());
    for n in &mut d.nodes {
        assert_eq!(n.replica.applied(), last, "member {}", n.replica.id());
        let id = n.replica.id();
        assert_eq!(rows(n.replica.engine_mut()), first, "member {id}");
    }
    assert!(!d.nodes[1].replica.is_uncertain());
}

/// A log whose every segment holds live records refuses a write it has no room for: the member
/// waits with it, whole, and is not fenced; once the group holding the segments compacts and the
/// owner says so (`Replica::resume`), the member's next drives make the write again and it goes
/// on (audit S04). The log is the smallest its configuration takes, segments of four blocks, so
/// it fills within a few frames. Every entry is a quarter of what one frame holds, as the log
/// says (`Log::entry_room`), so several share a segment and compacting them frees one; a log
/// refused `Full` does not yet recover room for a frame much larger than that (reported to the
/// log, docs/design/replica.md §7).
#[test]
fn a_write_refused_for_a_full_log_waits_until_another_group_compacts() {
    let config = LogConfig {
        segment_bytes: 4 * 4096,
        max_segments: 4,
        ..log_config()
    };
    // The test keeps the log, and writes another group to it.
    let log = Log::create(sim(12), config, 0x6c6f67).unwrap();
    let room = log.entry_room().unwrap();
    let quarter = room / 4;
    // An entry's encoding adds its kind and its context's length to its data.
    let largest = u64::try_from(room).unwrap() - 5;
    let settings = Settings {
        max_size_per_msg: largest,
        max_inflight_msgs: 1,
        max_uncommitted_size: largest,
        max_entry_bytes: largest,
        ..SETTINGS
    };
    let alone = Range {
        boot: ConfState {
            voters: vec![1],
            ..ConfState::default()
        },
        settings,
        ..range()
    };
    let mut replica = open(1, &log, first_range(), &alone, 12).unwrap();
    replica.campaign().unwrap();
    let (_, driven) = drive(&mut replica, true);
    assert!(driven.stalled.is_none(), "{:?}", driven.stalled);
    assert!(replica.is_leader());
    // Another group's entries fill every segment with live records.
    let mut last = 0;
    loop {
        let written = log.write(
            7,
            hyper_log::Update {
                entries: Some(hyper_log::Entries {
                    first: last + 1,
                    entries: vec![hyper_log::Entry {
                        term: 1,
                        bytes: vec![7u8; quarter],
                    }],
                }),
                ..hyper_log::Update::default()
            },
        );
        match written {
            Ok(()) => last += 1,
            Err(hyper_log::LogError::Full) => break,
            Err(e) => panic!("{e}"),
        }
        assert!(
            last <= 4 * u64::from(config.max_segments),
            "the log never filled"
        );
    }
    // An entry as large as the other group's: registrations until one more would pass it.
    let mut many = Entry {
        at_ns: 10,
        commands: Vec::new(),
    };
    for serial in 0.. {
        many.commands.push(Sessioned {
            session: 0,
            serial,
            unanswered: 0,
            command: Command::Register {
                owner: serial,
                run: 1,
                nth: 1,
            },
        });
        if many.encode().unwrap().len() > quarter {
            many.commands.pop();
            break;
        }
    }
    replica.propose(&many).unwrap();
    let applied = replica.applied();
    let (_, driven) = drive(&mut replica, true);
    assert!(matches!(driven.stalled, Some(Fault::Room(_))), "{driven:?}");
    assert!(!replica.is_fenced());
    assert!(matches!(
        replica.propose(&registration(0)),
        Err(ReplicaError::Stalled)
    ));
    // The other group compacts past everything it holds, and the owner says room was freed.
    log.write(
        7,
        hyper_log::Update {
            start: Some(hyper_log::Start {
                index: last,
                term: 1,
            }),
            ..hyper_log::Update::default()
        },
    )
    .unwrap();
    replica.resume();
    // The next drives make the waiting write and apply the entry, which a member of one commits
    // at once.
    let (_, driven) = drive(&mut replica, true);
    assert!(driven.stalled.is_none(), "{:?}", driven.stalled);
    assert_eq!(replica.applied(), applied + 1);
}

/// A group under heavy writes, whose members' logs refuse them for the group's retained bound
/// again and again while one member is cut off and then caught up by a snapshot, fences no
/// member. Each refusal is room the member waits out until its own compaction frees it; the
/// writes it then makes again, the compactions that race them and the install that replaces the
/// cut member's log are each taken by the log. A node under a closed ladder of 1-byte PUTs at
/// 2,048 clients ended on a fence whose only words were "the log refused a write the group cannot
/// take" (perf-10k, 2026-11-19); a fence here fails with the log's own reason. Do: three members
/// whose logs retain a ready of their settings and no more (as
/// `a_write_refused_for_room_waits_until_the_group_compacts`), 60 rounds of three bursts of 8
/// proposals, each driven as a write of its own and delivered, a member compacting all it
/// applied only when stalled, member 3 cut off for rounds 15 to 35. Expect: refusals for room
/// and a snapshot sent, no member fenced, and member 3 applying what the leader applied.
#[test]
fn a_group_stalled_for_room_under_heavy_writes_and_an_install_fences_no_member() {
    let small = Settings {
        max_size_per_msg: 256,
        max_inflight_msgs: 1,
        max_uncommitted_size: 256,
        max_entry_bytes: 256,
        ..SETTINGS
    };
    let config = LogConfig {
        group_entries: 102,
        group_bytes: 1 << 20,
        ..log_config()
    };
    let bounded = Range {
        settings: small,
        ..range()
    };
    let (mut stalls, mut installs) = (0u64, 0u64);
    for seed in 0..6u64 {
        let mut nodes: Vec<Node> = (1..=3u64)
            .map(|id| {
                let device = sim(300 + 3 * seed + id);
                let log = Log::create(device, config, 0x6c6f67 + u128::from(id)).unwrap();
                let replica = open(id, &log, first_range(), &bounded, 300 + seed + id).unwrap();
                Node { replica, log }
            })
            .collect();
        nodes[0].replica.campaign().unwrap();
        let mut serial = 0;
        let mut wire: VecDeque<Message> = VecDeque::new();
        for round in 0..60u64 {
            let cut = (15..35).contains(&round);
            let live = |id: u64| !(cut && id == 3);
            for _ in 0..3 {
                for n in nodes.iter_mut().filter(|n| live(n.replica.id())) {
                    if !n.replica.is_leader() {
                        continue;
                    }
                    for _ in 0..8 {
                        serial += 1;
                        // Refused while stalled or past the uncommitted bound: a fence fails the
                        // drive below.
                        let _ = n.replica.propose(&registration(serial));
                        wire.extend(drive(&mut n.replica, false).0.messages);
                    }
                }
                for _ in 0..10_000 {
                    for n in nodes.iter_mut().filter(|n| live(n.replica.id())) {
                        if n.replica.is_stalled() {
                            stalls += 1;
                            n.replica.compact(0, 0, Waker::noop()).unwrap();
                        }
                        wire.extend(drive(&mut n.replica, true).0.messages);
                    }
                    let Some(m) = wire.pop_front() else { break };
                    if !live(m.to) {
                        continue;
                    }
                    let (from, to) = (m.from, m.to);
                    let is_snapshot = m.msg_type == MessageType::MsgSnapshot;
                    // A message the core refuses, or a stalled member, changes nothing.
                    let _ = nodes[usize::try_from(to).unwrap() - 1].replica.step(m);
                    if is_snapshot {
                        installs += 1;
                        let _ = nodes[usize::try_from(from).unwrap() - 1]
                            .replica
                            .report_snapshot(to, true);
                    }
                }
            }
            for n in nodes.iter_mut().filter(|n| live(n.replica.id())) {
                n.replica.tick().unwrap();
            }
        }
        for n in &nodes {
            assert!(
                !n.replica.is_fenced(),
                "seed {seed}: member {} fenced: {}",
                n.replica.id(),
                n.replica.describe()
            );
        }
        let leader = nodes
            .iter()
            .position(|n| n.replica.is_leader())
            .unwrap_or_else(|| panic!("seed {seed}: no leader"));
        assert_eq!(
            nodes[2].replica.applied(),
            nodes[leader].replica.applied(),
            "seed {seed}: member 3 never caught up: {}",
            nodes[leader].replica.describe()
        );
    }
    assert!(stalls > 0, "no write was refused for room");
    assert!(installs > 0, "no member was caught up by a snapshot");
}

/// A snapshot a member was sent: its index, its term and the voters it names.
type Sent = (u64, u64, Vec<u64>);

/// Drives every node but `away` until no message is left in flight, delivering in order and
/// dropping what goes to `away`, and records each snapshot sent.
fn exchange(nodes: &mut [Node], away: Option<u64>, sent: &mut Vec<Sent>) {
    let mut wire: VecDeque<Message> = VecDeque::new();
    for _ in 0..10_000 {
        for n in nodes.iter_mut().filter(|n| Some(n.replica.id()) != away) {
            wire.extend(drive(&mut n.replica, true).0.messages);
        }
        let Some(m) = wire.pop_front() else {
            return;
        };
        if let Some(meta) = m.snapshot.as_ref().and_then(|s| s.metadata.as_ref()) {
            let mut voters = meta.conf_state.as_ref().unwrap().voters.clone();
            voters.sort_unstable();
            sent.push((meta.index, meta.term, voters));
        }
        if Some(m.to) != away {
            let _ = nodes[usize::try_from(m.to).unwrap() - 1].replica.step(m);
        }
    }
    panic!("the group never settled");
}

/// A group of three led by member 1, with `entries` entries applied by every member.
fn led(entries: u64) -> Vec<Node> {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, id)).collect();
    nodes[0].replica.campaign().unwrap();
    let mut sent = Vec::new();
    exchange(&mut nodes, None, &mut sent);
    for serial in 0..entries {
        nodes[0].replica.propose(&registration(serial)).unwrap();
        exchange(&mut nodes, None, &mut sent);
    }
    assert!(
        sent.is_empty(),
        "a snapshot went to a member holding the log"
    );
    nodes
}

/// The rows of `m` but the configuration's, which an install writes and a member that never
/// applied a change does not hold: the configuration itself is compared on its own.
fn rows_applied(m: &mut Model) -> Vec<(Vec<u8>, Vec<u8>)> {
    let configuration = [
        mantle_meta::key::LOCAL,
        mantle_meta::key::marker::CONFIGURATION,
    ];
    let mut out = rows(m);
    out.retain(|(k, _)| k[..] != configuration);
    out
}

/// Heartbeats from member 1 until `member` applied what it did, within `rounds` heartbeats.
fn beat_until_caught_up(nodes: &mut [Node], member: u64, rounds: u32, sent: &mut Vec<Sent>) {
    let at = usize::try_from(member).unwrap() - 1;
    for _ in 0..rounds {
        if nodes[at].replica.applied() == nodes[0].replica.applied() {
            return;
        }
        for _ in 0..SETTINGS.heartbeat_tick {
            nodes[0].replica.tick().unwrap();
        }
        exchange(nodes, None, sent);
    }
    panic!(
        "member {member} never caught up: {}",
        nodes[0].replica.describe()
    );
}

/// A compaction makes no image: with every member holding what the leader applied, none is
/// behind the new start, the core asks for no snapshot, and no member reads its engine's rows
/// for one, through heartbeats after as well (docs/design/replica.md §4). Imaged at compaction,
/// as it was, every compaction read and encoded every row on the node's shard.
#[test]
fn a_compaction_with_no_member_behind_makes_no_image() {
    let mut nodes = led(4);
    let mut sent = Vec::new();
    assert!(nodes[0].replica.compact(0, 0, Waker::noop()).unwrap());
    for n in &mut nodes[1..] {
        n.replica.compact(0, 0, Waker::noop()).unwrap();
    }
    exchange(&mut nodes, None, &mut sent);
    for _ in 0..3 {
        for _ in 0..SETTINGS.heartbeat_tick {
            nodes[0].replica.tick().unwrap();
        }
        exchange(&mut nodes, None, &mut sent);
    }
    assert!(sent.is_empty(), "a snapshot went out: {sent:?}");
    for n in &nodes {
        assert_eq!(n.replica.images_made(), 0, "member {}", n.replica.id());
    }
}

/// A member away while the leader compacts past what it holds is behind the log's start: when
/// it answers again, the core asks for a snapshot, is told none is ready, and the leader makes
/// the image then, once, for the core to send at its next try; the member installs it and holds
/// the leader's rows.
#[test]
fn a_member_behind_the_start_catches_up_by_an_image_made_when_asked() {
    let mut nodes = led(1);
    let mut sent = Vec::new();
    for serial in 1..4 {
        nodes[0].replica.propose(&registration(serial)).unwrap();
        exchange(&mut nodes, Some(3), &mut sent);
    }
    assert!(nodes[0].replica.compact(0, 0, Waker::noop()).unwrap());
    exchange(&mut nodes, Some(3), &mut sent);
    assert_eq!(nodes[0].replica.images_made(), 0, "the compaction made one");
    assert!(nodes[2].replica.applied() < nodes[0].replica.applied());

    beat_until_caught_up(&mut nodes, 3, 20, &mut sent);
    assert_eq!(nodes[0].replica.images_made(), 1);
    assert_eq!(sent.len(), 1, "{sent:?}");
    let leader = rows_applied(nodes[0].replica.engine_mut());
    assert_eq!(rows_applied(nodes[2].replica.engine_mut()), leader);
    let mut voters = nodes[2].replica.configuration().voters.clone();
    voters.sort_unstable();
    assert_eq!(voters, vec![1, 2, 3]);
}

/// The image is of the point the leader applied when the core asked for it, with the
/// configuration held there, not of the compaction's start: entries applied after the
/// compaction and before the member answered are in it, and the member continues from its
/// point.
#[test]
fn an_image_made_for_a_member_is_of_the_point_applied_when_asked() {
    let mut nodes = led(1);
    let mut sent = Vec::new();
    nodes[0].replica.propose(&registration(1)).unwrap();
    exchange(&mut nodes, Some(3), &mut sent);
    assert!(nodes[0].replica.compact(0, 0, Waker::noop()).unwrap());
    exchange(&mut nodes, Some(3), &mut sent);
    let start = nodes[0].replica.applied();
    for serial in 2..5 {
        nodes[0].replica.propose(&registration(serial)).unwrap();
        exchange(&mut nodes, Some(3), &mut sent);
    }
    let asked = nodes[0].replica.applied();
    assert!(asked > start);

    beat_until_caught_up(&mut nodes, 3, 20, &mut sent);
    let term = nodes[0].replica.term();
    assert_eq!(sent, vec![(asked, term, vec![1, 2, 3])]);
    assert_eq!(nodes[0].replica.images_made(), 1);
    let leader = rows_applied(nodes[0].replica.engine_mut());
    assert_eq!(rows_applied(nodes[2].replica.engine_mut()), leader);
}

/// Steps each of `messages` into the member it is for.
fn deliver(nodes: &mut [Node], messages: Vec<Message>) {
    for m in messages {
        let to = usize::try_from(m.to).unwrap() - 1;
        // A message the core refuses changes nothing.
        let _ = nodes[to].replica.step(m);
    }
}

/// A member behind the log's start is sent its snapshot by the leader's drive that makes the
/// image, with no tick or heartbeat between the core's request and the send: the member refuses
/// an append it cannot take, the core asks for a snapshot as it takes the refusal and is told
/// none is ready, and the leader's next drive makes the image and has the core send it, as
/// raft-rs's `on_entries_fetched` sends what storage had said was temporarily unavailable
/// (docs/design/replica.md §4). Left to the core's next try, it went at the member's next answer
/// to a heartbeat, and a member no heartbeat reached was never sent it.
#[test]
fn a_member_behind_the_start_is_sent_its_snapshot_by_the_drive_that_makes_it() {
    let mut nodes = led(1);
    let mut sent = Vec::new();
    for serial in 1..4 {
        nodes[0].replica.propose(&registration(serial)).unwrap();
        exchange(&mut nodes, Some(3), &mut sent);
    }
    assert!(nodes[0].replica.compact(0, 0, Waker::noop()).unwrap());
    exchange(&mut nodes, Some(3), &mut sent);

    // The leader's next append reaches member 3, which lacks the entries before it.
    nodes[0].replica.propose(&registration(4)).unwrap();
    let (out, _) = drive(&mut nodes[0].replica, true);
    let (to_3, others): (Vec<Message>, Vec<Message>) =
        out.messages.into_iter().partition(|m| m.to == 3);
    deliver(&mut nodes, others);
    assert!(
        !of(&to_3, MessageType::MsgAppend).is_empty(),
        "no append for member 3: {to_3:?}"
    );
    deliver(&mut nodes, to_3);
    let (out, _) = drive(&mut nodes[2].replica, true);
    let refusal = out
        .messages
        .into_iter()
        .find(|m| m.to == 1 && m.msg_type == MessageType::MsgAppendResponse && m.reject)
        .expect("member 3 refuses the append");
    // Taking the refusal, the core asks for a snapshot, and none is held.
    nodes[0].replica.step(refusal).unwrap();
    assert_eq!(nodes[0].replica.images_made(), 0);

    // The leader's next drive, with no tick before it, makes the image and sends it.
    let (out, _) = drive(&mut nodes[0].replica, false);
    assert_eq!(nodes[0].replica.images_made(), 1);
    let snapshots = of(&out.messages, MessageType::MsgSnapshot);
    assert_eq!(snapshots.len(), 1, "{:?}", out.messages);
    assert_eq!(snapshots[0].to, 3);

    // Member 3 installs it, and once caught up holds the leader's rows: the image sent was the
    // only one made and sent.
    deliver(&mut nodes, out.messages);
    beat_until_caught_up(&mut nodes, 3, 20, &mut sent);
    assert!(sent.is_empty(), "another snapshot went out: {sent:?}");
    assert_eq!(nodes[0].replica.images_made(), 1);
    let leader = rows_applied(nodes[0].replica.engine_mut());
    assert_eq!(rows_applied(nodes[2].replica.engine_mut()), leader);
}

/// The image for a member behind the log's start is made a slice a drive, each about a drive's
/// committed page of bytes, not whole in one drive (docs/design/replica.md §4): the leader's
/// drives while it is under way each ask for another turn, none applies an entry, and an entry
/// committed meanwhile is applied only once the image is made, which is of the point applied
/// when it began. The drive that makes the last slice sends it, and the member installs it and
/// holds the leader's rows. Made whole, an image of a million rows held the node's shard, which
/// drives every range on it, for seconds.
#[test]
fn a_member_behind_the_start_catches_up_by_an_image_made_a_slice_a_drive() {
    const PAGE: u64 = 512;
    let page = usize::try_from(PAGE).unwrap();
    let sliced = Range {
        settings: Settings {
            max_committed_size_per_ready: PAGE,
            ..SETTINGS
        },
        ..range()
    };
    let mut nodes: Vec<Node> = (1..=3)
        .map(|id| {
            let log = Log::create(sim(id), log_config(), 0x6c6f67 + u128::from(id)).unwrap();
            let replica = open(id, &log, first_range(), &sliced, id).unwrap();
            Node { replica, log }
        })
        .collect();
    nodes[0].replica.campaign().unwrap();
    let mut sent = Vec::new();
    exchange(&mut nodes, None, &mut sent);
    for serial in 1..40 {
        nodes[0].replica.propose(&registration(serial)).unwrap();
        let away = if serial >= 37 { Some(3) } else { None };
        exchange(&mut nodes, away, &mut sent);
    }
    assert!(nodes[0].replica.compact(0, 0, Waker::noop()).unwrap());
    exchange(&mut nodes, Some(3), &mut sent);
    let image_bytes = {
        let rows = rows(nodes[0].replica.engine_mut());
        rows.iter()
            .map(|(k, v)| 8 + k.len() + v.len())
            .sum::<usize>()
    };
    assert!(image_bytes > 4 * page, "{image_bytes} bytes of rows");

    // Member 3 refuses the leader's next append, and the core asks for a snapshot.
    nodes[0].replica.propose(&registration(40)).unwrap();
    let (out, _) = drive(&mut nodes[0].replica, true);
    let (to_3, others): (Vec<Message>, Vec<Message>) =
        out.messages.into_iter().partition(|m| m.to == 3);
    deliver(&mut nodes, others);
    deliver(&mut nodes, to_3);
    let (out, _) = drive(&mut nodes[2].replica, true);
    let refusal = out
        .messages
        .into_iter()
        .find(|m| m.to == 1 && m.msg_type == MessageType::MsgAppendResponse && m.reject)
        .expect("member 3 refuses the append");
    exchange(&mut nodes, Some(3), &mut sent);
    nodes[0].replica.step(refusal).unwrap();
    let began_at = nodes[0].replica.applied();

    // The leader's drives make the image a slice each, applying nothing meanwhile, and the one
    // that makes the last sends it.
    let mut drives = 0;
    let mut snapshot = None;
    let mut proposed = false;
    while snapshot.is_none() {
        drives += 1;
        assert!(drives < 1_000, "the image was never made");
        let (out, driven) = drive(&mut nodes[0].replica, false);
        let made = nodes[0].replica.images_made() == 1;
        let snapshots = of(&out.messages, MessageType::MsgSnapshot);
        if made {
            assert!(!nodes[0].replica.imaging());
            assert_eq!(snapshots.len(), 1, "{:?}", out.messages);
            snapshot = Some(snapshots[0].clone());
        } else {
            assert!(
                nodes[0].replica.imaging(),
                "drive {drives}: no image under way"
            );
            assert!(driven.more, "drive {drives} asked for no turn to go on");
            assert!(snapshots.is_empty());
            assert_eq!(
                nodes[0].replica.applied(),
                began_at,
                "applied while imaging"
            );
            // An entry committed while the image is under way waits for it.
            if !proposed {
                proposed = true;
                nodes[0].replica.propose(&registration(41)).unwrap();
            }
        }
        let others = out.messages.into_iter().filter(|m| m.to == 2).collect();
        deliver(&mut nodes, others);
        let (back, _) = drive(&mut nodes[1].replica, true);
        deliver(
            &mut nodes,
            back.messages.into_iter().filter(|m| m.to == 1).collect(),
        );
    }
    let slices = drives;
    assert!(
        slices >= image_bytes / (2 * page),
        "{slices} drives made an image of {image_bytes} bytes of rows"
    );
    let snapshot = snapshot.unwrap();
    let meta = snapshot
        .snapshot
        .as_ref()
        .unwrap()
        .metadata
        .as_ref()
        .unwrap();
    assert_eq!(
        meta.index, began_at,
        "the image is of the point applied when it began"
    );
    assert!(proposed, "the image was made in one drive");

    // The entry committed meanwhile applies once the image is made, and member 3 installs the
    // image and catches up to the leader's rows.
    exchange(&mut nodes, Some(3), &mut sent);
    assert!(nodes[0].replica.applied() > began_at);
    deliver(&mut nodes, vec![snapshot]);
    beat_until_caught_up(&mut nodes, 3, 20, &mut sent);
    assert_eq!(nodes[0].replica.images_made(), 1);
    let leader = rows_applied(nodes[0].replica.engine_mut());
    assert_eq!(rows_applied(nodes[2].replica.engine_mut()), leader);
}

/// When a write's answer is given, as hyper-log's writer gives it (docs/design/raft-log.md §3), on
/// the test's clock: a frame takes what was admitted while the device was busy, its flush takes
/// `flush_ns`, and its persist record confirms the frame before it, whose writes are answered once
/// its flush ends; a frame whose flush ends with nothing admitted is confirmed by a write and
/// flush of its own. A group holds three writes at most (`room::GROUP_SUBMISSIONS`), a fourth
/// admitted once the frame before is answered, after the device looked for a frame to follow.
/// Every write is made durable on the real log as it is submitted ([`Synchronous`]); the device
/// says only when its answer is given.
struct Device {
    flush_ns: u64,
    /// How long a frame whose flush ended with nothing admitted, and answered the frame before's
    /// writes, waits for a write to follow it before it is confirmed on its own; zero confirms at
    /// once, as hyper-log's device did before its owner held such a confirmation for one measured
    /// frame's writes and flush (hyper-log `owner::write`'s `hold_confirmation`).
    wait_ns: u64,
    /// While set, the frame flushed waits until then for a write to follow it.
    waiting_until: Option<u64>,
    /// The frame or confirmation on the device: when its flush ends, its writes, and whether it is
    /// a confirmation of its own.
    on: Option<(u64, Vec<Sub>, bool)>,
    /// The frame flushed, awaiting the record that confirms it.
    awaiting: Vec<Sub>,
    /// Admitted for the next frame.
    gathering: Vec<Sub>,
    /// Waiting for their group's room.
    waiting: VecDeque<Sub>,
    frames: u64,
    confirmations: u64,
}

struct Sub {
    range: usize,
    released: std::sync::Arc<std::sync::atomic::AtomicU64>,
    waker: Waker,
}

impl Device {
    fn new(flush_ns: u64) -> Self {
        Self {
            flush_ns,
            wait_ns: 0,
            waiting_until: None,
            on: None,
            awaiting: Vec::new(),
            gathering: Vec::new(),
            waiting: VecDeque::new(),
            frames: 0,
            confirmations: 0,
        }
    }

    fn held(&self, range: usize) -> usize {
        let on = self.on.as_ref().map_or(0, |(_, subs, _)| {
            subs.iter().filter(|s| s.range == range).count()
        });
        on + [&self.awaiting, &self.gathering]
            .iter()
            .map(|subs| subs.iter().filter(|s| s.range == range).count())
            .sum::<usize>()
    }

    fn submit(&mut self, sub: Sub) {
        if self.held(sub.range) < 3 && !self.waiting.iter().any(|w| w.range == sub.range) {
            self.gathering.push(sub);
        } else {
            self.waiting.push_back(sub);
        }
    }

    /// Starts a frame of what was admitted, if the device is idle.
    fn kick(&mut self, now: u64) {
        if self.on.is_none() && !self.gathering.is_empty() {
            self.waiting_until = None;
            self.start(now);
        }
    }

    /// A frame of the writes admitted, one a group at most, the rest left for the frame after.
    fn start(&mut self, at: u64) {
        let mut taken: Vec<Sub> = Vec::new();
        let mut left = Vec::new();
        for sub in self.gathering.drain(..) {
            if taken.iter().any(|t| t.range == sub.range) {
                left.push(sub);
            } else {
                taken.push(sub);
            }
        }
        self.gathering = left;
        self.on = Some((at + self.flush_ns, taken, false));
        self.frames += 1;
    }

    /// Ends every flush due by `now`: the times of the confirmations of their own begun.
    fn advance(&mut self, now: u64) -> Vec<u64> {
        let mut began = Vec::new();
        // A wait for a write to follow that ran out: the frame is confirmed on its own.
        if let Some(until) = self.waiting_until
            && until <= now
            && self.on.is_none()
        {
            self.waiting_until = None;
            self.on = Some((until + self.flush_ns, Vec::new(), true));
            self.confirmations += 1;
            began.push(until);
        }
        while let Some((ends, _, _)) = self.on.as_ref() {
            let ends = *ends;
            if ends > now {
                break;
            }
            let (_, subs, alone) = self.on.take().unwrap();
            // Whether this flush's end answered writes: their submitters come back with their next
            // in a closed loop, which a frame flushed with nothing to follow waits for.
            let answered = !self.awaiting.is_empty();
            for s in self.awaiting.drain(..) {
                s.released.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                s.waker.wake_by_ref();
            }
            if !alone {
                self.awaiting = subs;
            }
            if !self.gathering.is_empty() {
                self.start(ends);
            } else if !self.awaiting.is_empty() && self.wait_ns > 0 && answered {
                self.waiting_until = Some(ends + self.wait_ns);
            } else if !self.awaiting.is_empty() {
                self.on = Some((ends + self.flush_ns, Vec::new(), true));
                self.confirmations += 1;
                began.push(ends);
            }
            // Room given back once the device has looked for a frame to follow.
            let mut kept = VecDeque::new();
            while let Some(w) = self.waiting.pop_front() {
                if self.held(w.range) < 3 && !kept.iter().any(|k: &Sub| k.range == w.range) {
                    self.gathering.push(w);
                } else {
                    kept.push_back(w);
                }
            }
            self.waiting = kept;
            if self.on.is_none() && !self.gathering.is_empty() {
                self.start(ends);
            }
        }
        began
    }
}

/// A group's store whose answers the modeled [`Device`] gives.
struct Paced {
    inner: Synchronous<SimFile>,
    range: usize,
    device: std::sync::Arc<std::sync::Mutex<Device>>,
    released: std::sync::Arc<std::sync::atomic::AtomicU64>,
    taken: u64,
}

impl hyper_durable::LogStore for Paced {
    type Hold = std::convert::Infallible;

    fn held(&self) -> Option<&Self::Hold> {
        None
    }

    fn release(&mut self, met: &Self::Hold) {
        match *met {}
    }

    fn depth(&self) -> usize {
        self.inner.depth()
    }

    fn view(&self) -> Result<hyper_durable::StoreView, Fault> {
        self.inner.view()
    }

    fn bounds(&self) -> Result<(hyper_durable::Point, u64), hyper_raft::StorageError> {
        self.inner.bounds()
    }

    fn term(&self, index: u64) -> Result<u64, hyper_raft::StorageError> {
        self.inner.term(index)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<mantle_range::Entry>,
    ) -> Result<(), hyper_raft::StorageError> {
        self.inner.entries(low, high, max_bytes, into)
    }

    fn visit(
        &self,
        low: u64,
        high: u64,
        page: u64,
        visit: &mut dyn FnMut(hyper_durable::EntryRef<'_>) -> bool,
    ) -> Result<(), hyper_raft::StorageError> {
        self.inner.visit(low, high, page, visit)
    }

    fn proposals(
        &self,
        into: &mut Vec<mantle_range::Entry>,
    ) -> Result<(), hyper_raft::StorageError> {
        self.inner.proposals(into)
    }

    fn released(&self) -> Result<u64, hyper_raft::StorageError> {
        self.inner.released()
    }

    fn room(&self) -> bool {
        self.inner.room()
    }

    fn submit(&mut self, write: &hyper_durable::Write<'_>, waker: &Waker) -> Result<(), Fault> {
        self.inner.submit(write, waker)?;
        self.device.lock().unwrap().submit(Sub {
            range: self.range,
            released: std::sync::Arc::clone(&self.released),
            waker: waker.clone(),
        });
        Ok(())
    }

    fn poll(&mut self) -> Option<Result<(), Fault>> {
        if self.released.load(std::sync::atomic::Ordering::SeqCst) <= self.taken {
            return None;
        }
        let answer = self.inner.poll();
        if answer.is_some() {
            self.taken += 1;
        }
        answer
    }

    fn write_now(&mut self, write: &hyper_durable::Write<'_>) -> Result<(), Fault> {
        self.inner.write_now(write)
    }

    fn keep_after(&mut self, index: u64) {
        self.inner.keep_after(index);
    }

    fn kept_bytes(&self) -> u64 {
        self.inner.kept_bytes()
    }
}

/// The waker of a node's member of a range: queues it for the node's next turn.
struct Woke {
    node: usize,
    range: usize,
    woken: std::sync::Arc<std::sync::Mutex<Vec<(usize, usize)>>>,
}

impl std::task::Wake for Woke {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.woken.lock().unwrap().push((self.node, self.range));
    }
}

/// A node of the paced cell: its member of each range, its log and its device, and its turn's
/// state as the node's task keeps it (mantle-node's `Node::turn`).
struct CellNode {
    members: Vec<Replica<Paced, Model>>,
    wakers: Vec<Waker>,
    device: std::sync::Arc<std::sync::Mutex<Device>>,
    queued: VecDeque<usize>,
    is_queued: Vec<bool>,
    wake_at: Vec<Option<u64>>,
    /// Commands come to this node for each range, waiting to be proposed.
    asked: Vec<u64>,
    busy_until: u64,
    /// When the turn begun drives.
    drive_at: Option<u64>,
    _log: Log<SimFile>,
}

impl CellNode {
    fn queue(&mut self, range: usize) {
        if !self.is_queued[range] {
            self.is_queued[range] = true;
            self.queued.push_back(range);
        }
    }
}

/// What a paced cell's run counted: each node's frames and confirmations of their own, and for
/// each confirmation what each range's member on that node was doing as the frame's flush ended.
#[derive(Default, Debug)]
struct Paced3 {
    frames: Vec<u64>,
    confirmations: Vec<u64>,
    /// For each reason, the confirmations at which at least one range of the node had it.
    reasons: std::collections::BTreeMap<(&'static str, &'static str), u64>,
    /// Confirmations by the role of the node (leader of every range, or follower of every one).
    by_role: std::collections::BTreeMap<&'static str, u64>,
}

/// Timings of the paced cell, in nanoseconds of the test's clock.
#[derive(Clone, Copy)]
struct Pace {
    flush_ns: u64,
    /// One way between nodes, and between a client and a node.
    hop_ns: u64,
    /// A turn's fixed cost and each drive's.
    turn_ns: u64,
    drive_ns: u64,
    /// Ranges, all led by node 0, and the clients of each, each with one command out at a time.
    ranges: usize,
    clients: u64,
    /// Commands answered at the leader before the run ends.
    commands: u64,
    /// Between one command to a range and its next, an open stream; zero for a closed loop of
    /// `clients`, each sending its next command once its last is answered.
    gap_ns: u64,
    /// The most a client of a closed loop works between an answer and its next command.
    think_ns: u64,
    /// A turn's work before its drive.
    pre_ns: u64,
    /// How long a frame flushed with nothing to follow waits for a write before its own
    /// confirmation ([`Device::wait_ns`]).
    wait_ns: u64,
}

/// Runs a cell of three nodes, each with one log on a device of `pace.flush_ns` flushes, hosting a
/// member of each of `pace.ranges` ranges, all led by node 0, under a steady closed loop of
/// clients; each node turns as mantle-node's task does: it takes what arrived, drives each member
/// queued once, sends what they gave at the turn's end, then proposes the commands that came.
fn paced_cell(pace: Pace) -> Paced3 {
    use std::sync::{Arc, Mutex};
    let woken: Arc<Mutex<Vec<(usize, usize)>>> = Arc::new(Mutex::new(Vec::new()));
    let config = LogConfig {
        waits: Waits::Never,
        ..log_config()
    };
    let mut nodes: Vec<CellNode> = (0..3usize)
        .map(|n| {
            let id = n as u64 + 1;
            let log = Log::create(sim(id), config, 0x6c6f67 + u128::from(id)).unwrap();
            let device = Arc::new(Mutex::new(Device {
                wait_ns: pace.wait_ns,
                ..Device::new(pace.flush_ns)
            }));
            let mut members = Vec::new();
            let mut wakers = Vec::new();
            for r in 0..pace.ranges {
                let group = GROUP + r as u128;
                let store = Paced {
                    inner: Synchronous::new(
                        mantle_range::claim(&log, group, &range()).unwrap(),
                        None,
                    ),
                    range: r,
                    device: Arc::clone(&device),
                    released: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                    taken: 0,
                };
                members.push(Replica::open(id, store, first_range(), &range(), id).unwrap());
                wakers.push(Waker::from(Arc::new(Woke {
                    node: n,
                    range: r,
                    woken: Arc::clone(&woken),
                })));
            }
            CellNode {
                members,
                wakers,
                device,
                queued: VecDeque::new(),
                is_queued: vec![false; pace.ranges],
                wake_at: vec![None; pace.ranges],
                asked: vec![0; pace.ranges],
                busy_until: 0,
                drive_at: None,
                _log: log,
            }
        })
        .collect();
    // (arrives, node, range, message) and (arrives, range) for a client's command at the leader.
    let mut wire: Vec<(u64, usize, usize, Message)> = Vec::new();
    let mut asks: Vec<(u64, usize)> = Vec::new();
    for r in 0..pace.ranges {
        nodes[0].members[r].campaign().unwrap();
        nodes[0].queue(r);
    }
    let mut counted = Paced3 {
        frames: vec![0; 3],
        confirmations: vec![0; 3],
        ..Paced3::default()
    };
    let mut started = false;
    let mut serial = 0u64;
    let mut drawn = 0x9e37_79b9_7f4a_7c15u64;
    let mut now = 0u64;
    let mut answered = 0u64;
    let mut base: Option<(Vec<u64>, Vec<u64>)> = None;
    for _ in 0..5_000_000u64 {
        // Every flush ended by now, each confirmation of its own looked at as it begins.
        for (n, node) in nodes.iter().enumerate() {
            let began = node.device.lock().unwrap().advance(now);
            for _ in began {
                if base.is_none() {
                    continue;
                }
                let role = if n == 0 { "leader" } else { "follower" };
                *counted.by_role.entry(role).or_default() += 1;
                let mut seen: Vec<&'static str> = Vec::new();
                for r in 0..pace.ranges {
                    let device = node.device.lock().unwrap();
                    let untaken = node.members[r].in_flight();
                    let submitted =
                        device.held(r) + device.waiting.iter().filter(|w| w.range == r).count();
                    let reason = if untaken >= 3 && submitted + 1 >= untaken {
                        "readies in flight at depth, every one a write"
                    } else if untaken >= 3 {
                        "readies in flight at depth, one a ready that wrote nothing"
                    } else if device.held(r) >= 3 || device.waiting.iter().any(|w| w.range == r) {
                        "group's room"
                    } else if node.is_queued[r] && node.busy_until > now {
                        "queued, the node's turn not yet reached"
                    } else if node.is_queued[r] {
                        "queued, drive not yet taken"
                    } else if node.asked[r] > 0 {
                        "commands waiting to be proposed"
                    } else if wire.iter().any(|(_, to, rr, _)| *to == n && *rr == r) {
                        "messages on the wire to it"
                    } else if n == 0 && asks.iter().any(|(_, rr)| *rr == r) {
                        "commands on the wire to it"
                    } else {
                        "nothing to write"
                    };
                    if !seen.contains(&reason) {
                        seen.push(reason);
                    }
                }
                for reason in seen {
                    *counted.reasons.entry((role, reason)).or_default() += 1;
                }
            }
        }
        for (n, r) in woken.lock().unwrap().drain(..) {
            nodes[n].queue(r);
        }
        let mut i = 0;
        while i < wire.len() {
            if wire[i].0 <= now {
                let (_, to, r, m) = wire.swap_remove(i);
                let _ = nodes[to].members[r].step(m);
                nodes[to].queue(r);
            } else {
                i += 1;
            }
        }
        let mut i = 0;
        while i < asks.len() {
            if asks[i].0 <= now {
                let (at, r) = asks.swap_remove(i);
                nodes[0].asked[r] += 1;
                // An open stream: the range's next command comes its gap after this one.
                if pace.gap_ns > 0 {
                    asks.push((at + pace.gap_ns, r));
                }
            } else {
                i += 1;
            }
        }
        for (n, node) in nodes.iter_mut().enumerate() {
            if node.busy_until > now {
                continue;
            }
            for r in 0..pace.ranges {
                if node.wake_at[r].is_some_and(|at| at <= now) {
                    node.wake_at[r] = None;
                    node.queue(r);
                }
            }
            let work = !node.queued.is_empty() || node.asked.iter().any(|a| *a > 0);
            // A turn takes what came, then spends `pre_ns` before it drives (its inbox, its
            // plane, its events); its drive comes then.
            match node.drive_at {
                Some(at) if at <= now => node.drive_at = None,
                Some(_) => continue,
                None if !work => continue,
                None if pace.pre_ns > 0 => {
                    node.drive_at = Some(now + pace.pre_ns);
                    node.busy_until = now + pace.pre_ns;
                    continue;
                }
                None => {}
            }
            let mut drives = 0u64;
            let mut sends = Vec::new();
            for _ in 0..node.queued.len() {
                let r = node.queued.pop_front().unwrap();
                node.is_queued[r] = false;
                let mut out = Out::default();
                let driven = node.members[r]
                    .drive(now, &node.wakers[r], &mut out)
                    .unwrap();
                drives += 1;
                for m in out.messages.drain(..) {
                    let to = usize::try_from(m.to).unwrap() - 1;
                    sends.push((to, r, m));
                }
                if n == 0 {
                    for _ in out.answers.drain(..) {
                        answered += 1;
                        if pace.gap_ns == 0 {
                            // A client's own work between its answer and its next command, drawn
                            // up to `think_ns` (xorshift64, Marsaglia 2003).
                            drawn ^= drawn << 13;
                            drawn ^= drawn >> 7;
                            drawn ^= drawn << 17;
                            let think = drawn % (pace.think_ns + 1);
                            asks.push((now + pace.turn_ns + 2 * pace.hop_ns + think, r));
                        }
                    }
                }
                node.wake_at[r] = driven.wake;
                if driven.more {
                    node.queue(r);
                }
            }
            let ends = now + pace.turn_ns + drives * pace.drive_ns;
            for (to, r, m) in sends {
                wire.push((ends + pace.hop_ns, to, r, m));
            }
            // The commands that came, proposed after the drives, as `Node::turn` steps its asks.
            for r in 0..pace.ranges {
                let count = std::mem::take(&mut node.asked[r]);
                if count == 0 {
                    continue;
                }
                let commands = (0..count)
                    .map(|_| {
                        serial += 1;
                        Sessioned {
                            session: 0,
                            serial,
                            unanswered: 0,
                            command: Command::Register {
                                owner: serial,
                                run: 1,
                                nth: 1,
                            },
                        }
                    })
                    .collect();
                node.members[r]
                    .propose(&Entry {
                        at_ns: 10,
                        commands,
                    })
                    .unwrap();
                node.queue(r);
            }
            node.busy_until = ends;
            node.device.lock().unwrap().kick(now);
        }
        if !started && (0..pace.ranges).all(|r| nodes[0].members[r].is_leader()) {
            started = true;
            for r in 0..pace.ranges {
                // Each client's first command at its own moment within a flush's span, so the
                // clients are not all in one phase of the pipeline.
                for c in 0..if pace.gap_ns > 0 { 1 } else { pace.clients } {
                    let spread = (c * pace.ranges as u64 + r as u64) * 2 * pace.flush_ns
                        / (pace.clients * pace.ranges as u64);
                    asks.push((now + pace.hop_ns + spread, r));
                }
            }
        }
        // The count starts once every client has had a command answered.
        if base.is_none() && answered >= pace.clients * pace.ranges as u64 {
            let d = |n: &CellNode| {
                let d = n.device.lock().unwrap();
                (d.frames, d.confirmations)
            };
            base = Some((
                nodes.iter().map(|n| d(n).0).collect(),
                nodes.iter().map(|n| d(n).1).collect(),
            ));
        }
        if answered >= pace.commands {
            let (frames, confirmations) = base.unwrap();
            for n in 0..3 {
                let d = nodes[n].device.lock().unwrap();
                counted.frames[n] = d.frames - frames[n];
                counted.confirmations[n] = d.confirmations - confirmations[n];
            }
            return counted;
        }
        // The next moment anything happens.
        let mut next = u64::MAX;
        for node in &nodes {
            let device = node.device.lock().unwrap();
            if let Some((ends, _, _)) = device.on.as_ref() {
                next = next.min(*ends);
            }
            if let Some(until) = device.waiting_until {
                next = next.min(until.max(now + 1));
            }
            drop(device);
            let work = !node.queued.is_empty() || node.asked.iter().any(|a| *a > 0);
            if work {
                next = next.min(node.busy_until.max(now + 1));
            }
            if let Some(at) = node.drive_at {
                next = next.min(at.max(now + 1));
            }
            for at in node.wake_at.iter().flatten() {
                next = next.min((*at).max(node.busy_until).max(now + 1));
            }
        }
        for (at, ..) in &wire {
            next = next.min(*at);
        }
        for (at, _) in &asks {
            next = next.min(*at);
        }
        assert!(
            next != u64::MAX,
            "the cell went idle with {answered} answered"
        );
        now = next.max(now);
    }
    panic!("the cell never answered its commands: {answered}");
}

/// The paced cell's frames and confirmations of their own, summed over its three nodes, with the
/// device's wait for the callers a flush answered (`Device::wait_ns`) none, as hyper-log's
/// device confirmed before, and one flush, as hyper-log's owner now holds a confirmation for one
/// measured frame's writes and flush (hyper-log `owner::write`'s `hold_confirmation`).
fn before_and_after(pace: Pace) -> [(u64, u64, Paced3); 2] {
    [0, pace.flush_ns].map(|wait_ns| {
        let counted = paced_cell(Pace { wait_ns, ..pace });
        (
            counted.frames.iter().sum(),
            counted.confirmations.iter().sum(),
            counted,
        )
    })
}

/// A flush of 4.5 ms (the Mac Studio's `F_FULLFSYNC`), 100 us a hop, and turns about as long as
/// a flush around their drive: 2 ms before it and 3 ms after, as the cell's 0.33 confirmations a
/// frame were reproduced (docs/design/raft-log.md §3).
const LONG_TURNS: Pace = Pace {
    flush_ns: 4_500_000,
    hop_ns: 100_000,
    turn_ns: 3_000_000,
    drive_ns: 20_000,
    ranges: 8,
    clients: 1,
    commands: 20_000,
    gap_ns: 0,
    think_ns: 0,
    pre_ns: 2_000_000,
    wait_ns: 0,
};

/// Do: an open stream, a command a range every 0.4 ms across eight ranges, on nodes whose turns
/// take about a flush around their drive. Expect: with no wait, a frame's flush often ends just
/// before the next ready of a member it answered reaches the log, and about one frame in seven
/// is confirmed on its own (0.145 measured); with the log holding a confirmation one flush for
/// the callers its flush answered, under one in fifty.
#[test]
fn an_open_stream_on_long_turns_confirms_few_frames_on_their_own_once_the_log_holds() {
    let [before, after] = before_and_after(Pace {
        gap_ns: 400_000,
        ..LONG_TURNS
    });
    assert!(
        before.1 * 10 >= before.0,
        "with no wait, {} confirmations in {} frames: {:?}",
        before.1,
        before.0,
        before.2
    );
    assert!(
        after.1 * 50 <= after.0,
        "holding, {} confirmations in {} frames: {:?}",
        after.1,
        after.0,
        after.2
    );
}

/// Do: a closed loop, 32 clients a range across eight ranges, each working up to 5 ms between
/// an answer and its next command, on nodes whose turns take about a flush around their drive.
/// Expect: with no wait, more than one frame in five is confirmed on its own (0.283 measured);
/// with the log holding a confirmation one flush for the callers its flush answered, under one
/// in fifty (0.010 measured).
#[test]
fn a_closed_loop_on_long_turns_confirms_few_frames_on_their_own_once_the_log_holds() {
    let [before, after] = before_and_after(Pace {
        clients: 32,
        think_ns: 5_000_000,
        ..LONG_TURNS
    });
    assert!(
        before.1 * 5 >= before.0,
        "with no wait, {} confirmations in {} frames: {:?}",
        before.1,
        before.0,
        before.2
    );
    assert!(
        after.1 * 50 <= after.0,
        "holding, {} confirmations in {} frames: {:?}",
        after.1,
        after.0,
        after.2
    );
}

/// Do: a closed loop whose clients send their next command the moment one is answered, 16 a
/// range across four ranges, on short turns: every client in one phase, each waiting on the
/// frame being confirmed. Expect: with no wait every other frame is confirmed on its own, as
/// nothing can follow; the hold does not make it worse, since a flush that answered no one is
/// confirmed at once (without that condition the wait cost a flush a cycle, measured: one
/// confirmation a frame).
#[test]
fn a_closed_loop_in_one_phase_is_not_held() {
    let [before, after] = before_and_after(Pace {
        turn_ns: 50_000,
        pre_ns: 0,
        ranges: 4,
        clients: 16,
        ..LONG_TURNS
    });
    assert!(
        before.1 * 5 >= before.0 * 2,
        "with no wait, {} confirmations in {} frames: {:?}",
        before.1,
        before.0,
        before.2
    );
    assert!(
        after.1 <= before.1,
        "holding, {} confirmations in {} frames, against {} in {}: {:?}",
        after.1,
        after.0,
        before.1,
        before.0,
        after.2
    );
}
