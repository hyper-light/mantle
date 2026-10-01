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
use mantle_log::{Config as LogConfig, Log, Waits};
use mantle_meta::apply::Layer;
use mantle_meta::engine::{Engine, Model, Rows};
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
    }
}

const RULES: Rules = Rules {
    lifetime_ns: 3_600_000_000_000,
    max_sessions: 64,
    max_answers: 16,
    max_answer_bytes: usize::MAX,
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
    let replica = Replica::open(id, GROUP, log, first_range(), &range(), seed).unwrap();
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
            listing: None,
        },
        default: None,
        // The write carries its file, which names it.
        id: 0,
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
        generation: 1,
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

/// A simulated file whose flushes wait while its gate is shut.
struct Gated {
    file: SimFile,
    shut: std::sync::Mutex<bool>,
    opened: std::sync::Condvar,
}

impl Gated {
    fn shut(&self, shut: bool) {
        *self.shut.lock().unwrap() = shut;
        self.opened.notify_all();
    }
}

impl mantle_disk::block::BlockFile for Gated {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }
    fn len(&self) -> Result<u64, mantle_disk::DiskError> {
        mantle_disk::block::BlockFile::len(&self.file)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), mantle_disk::DiskError> {
        self.file.read_exact_at(buf, offset)
    }
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), mantle_disk::DiskError> {
        self.file.write_all_at(buf, offset)
    }
    fn sync_data(&self) -> Result<(), mantle_disk::DiskError> {
        let mut shut = self.shut.lock().unwrap();
        while *shut {
            shut = self.opened.wait(shut).unwrap();
        }
        drop(shut);
        self.file.sync_data()
    }
}

/// A member whose log's flushes can be held, and its gate.
type GatedMember = (Replica<Arc<Gated>, Model>, Arc<Gated>);

/// A member whose log's flushes can be held.
fn gated(id: u64) -> GatedMember {
    let gate = Arc::new(Gated {
        file: SimFile::new(
            Alignment::new(4096).unwrap(),
            Alignment::new(512).unwrap(),
            id,
        )
        .unwrap(),
        shut: std::sync::Mutex::new(false),
        opened: std::sync::Condvar::new(),
    });
    let log =
        Arc::new(Log::create(Arc::clone(&gate), log_config(), 0x6c6f67 + u128::from(id)).unwrap());
    let replica = Replica::open(id, GROUP, log, first_range(), &range(), id).unwrap();
    (replica, gate)
}

/// Opens every gate when dropped, as a failing test unwinds, so no log's writer is left
/// holding a flush while its log is dropped.
struct Opens(Vec<Arc<Gated>>);

impl Drop for Opens {
    fn drop(&mut self) {
        for gate in &self.0 {
            gate.shut(false);
        }
    }
}

fn kind(m: &Message) -> Option<mantle_range::MessageType> {
    mantle_range::MessageType::from_i32(m.msg_type)
}

/// `begin` gives out a leader's appends while its own write of the entries is still being made
/// durable, so they travel during its flush; it gives out a follower's acknowledgement only once
/// the follower's write is durable, which `drive` waits for; and a member holds the messages
/// that come while its ready is outstanding, taking them once it is done, and refuses proposals
/// meanwhile (audit §5.1). The entry then commits and applies everywhere.
#[test]
fn a_leader_sends_while_it_flushes_and_a_follower_acknowledges_after() {
    let mut nodes: Vec<GatedMember> = (1..=3).map(gated).collect();
    let _opens = Opens(nodes.iter().map(|(_, g)| Arc::clone(g)).collect());
    let deliver = |nodes: &mut Vec<GatedMember>| {
        let mut wire: VecDeque<Message> = VecDeque::new();
        for _ in 0..10_000 {
            for (r, _) in nodes.iter_mut() {
                wire.extend(r.drive().unwrap().messages);
            }
            let Some(m) = wire.pop_front() else {
                return;
            };
            let _ = nodes[usize::try_from(m.to).unwrap() - 1].0.step(m);
        }
        panic!("the group never settled");
    };
    nodes[0].0.campaign().unwrap();
    deliver(&mut nodes);
    assert!(nodes[0].0.is_leader());

    // The leader's flush is held: its appends go out regardless.
    nodes[0].1.shut(true);
    let register = Sessioned {
        session: 0,
        serial: 0,
        unanswered: 0,
        command: Command::Register,
    };
    nodes[0]
        .0
        .propose(&Entry {
            at_ns: 10,
            commands: vec![register],
        })
        .unwrap();
    let out = nodes[0].0.begin().unwrap();
    assert!(out.persisting && nodes[0].0.persisting());
    let appends: Vec<Message> = out
        .messages
        .into_iter()
        .filter(|m| kind(m) == Some(mantle_range::MessageType::MsgAppend))
        .collect();
    let mut to: Vec<u64> = appends.iter().map(|m| m.to).collect();
    to.sort_unstable();
    assert_eq!(to, [2, 3]);

    // Follower 2's flush is held too: it takes the append but does not acknowledge it, while
    // follower 3, whose flush is not, acknowledges once its write is durable.
    nodes[1].1.shut(true);
    let mut acks = Vec::new();
    for m in appends {
        let to = m.to;
        let follower = &mut nodes[usize::try_from(to).unwrap() - 1].0;
        follower.step(m).unwrap();
        let out = if to == 2 {
            follower.begin().unwrap()
        } else {
            follower.drive().unwrap()
        };
        acks.extend(
            out.messages
                .into_iter()
                .filter(|m| kind(m) == Some(mantle_range::MessageType::MsgAppendResponse)),
        );
    }
    assert_eq!(acks.iter().map(|m| m.from).collect::<Vec<_>>(), [3]);
    assert!(nodes[1].0.persisting());
    assert!(matches!(
        nodes[1].0.propose(&Entry {
            at_ns: 20,
            commands: Vec::new(),
        }),
        Err(mantle_range::ReplicaError::Stalled)
    ));
    // Once its write is durable, follower 2 acknowledges.
    nodes[1].1.shut(false);
    let out = nodes[1].0.drive().unwrap();
    assert!(!out.persisting);
    acks.extend(
        out.messages
            .into_iter()
            .filter(|m| kind(m) == Some(mantle_range::MessageType::MsgAppendResponse)),
    );
    assert_eq!(acks.len(), 2);

    // The leader holds the acknowledgements while its own flush is held and takes them once
    // its ready is done, and the entry applies at every member.
    let applied = nodes[0].0.applied();
    for ack in acks {
        nodes[0].0.step(ack).unwrap();
    }
    assert_eq!(nodes[0].0.applied(), applied);
    nodes[0].1.shut(false);
    // A drive takes one ready: the one flushing, then the one the acknowledgements made.
    let mut messages = Vec::new();
    for _ in 0..2 {
        messages.extend(nodes[0].0.drive().unwrap().messages);
    }
    assert!(nodes[0].0.applied() > applied, "{messages:?}");
    for m in messages {
        let _ = nodes[usize::try_from(m.to).unwrap() - 1].0.step(m);
    }
    deliver(&mut nodes);
    for (r, _) in &nodes {
        assert_eq!(r.applied(), nodes[0].0.applied());
    }
    assert!(nodes[0].0.applied() >= 2);
}

/// A read asked while a round of confirmation is out waits for the next round, which every
/// read waiting shares: the round out has the commit index of when it began, and a write
/// committed since, whose answer came before the read was asked, must be seen (audit §5.5).
#[test]
fn a_read_asked_while_a_round_is_out_waits_for_the_next() {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    assert!(nodes[0].replica.is_leader());
    let is = |m: &Message, t: mantle_range::MessageType| kind(m) == Some(t);

    // Round one carries the first read; its heartbeats are held. The second, asked after the
    // round began, waits.
    nodes[0].replica.read_index(b"first".to_vec()).unwrap();
    nodes[0].replica.read_index(b"second".to_vec()).unwrap();
    let held: Vec<Message> = nodes[0].replica.drive().unwrap().messages;
    assert!(
        held.iter()
            .all(|m| is(m, mantle_range::MessageType::MsgHeartbeat))
    );

    // A write commits meanwhile, its appends and their answers delivered.
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
    let mut wire: VecDeque<Message> = nodes[0].replica.drive().unwrap().messages.into();
    let mut written = None;
    while let Some(m) = wire.pop_front() {
        if is(&m, mantle_range::MessageType::MsgHeartbeat) {
            continue;
        }
        let to = usize::try_from(m.to).unwrap() - 1;
        nodes[to].replica.step(m).unwrap();
        let out = nodes[to].replica.drive().unwrap();
        if to == 0
            && let Some(a) = out.applied.last()
        {
            written = Some(a.index);
        }
        wire.extend(
            out.messages
                .into_iter()
                .filter(|m| !is(m, mantle_range::MessageType::MsgHeartbeat)),
        );
    }
    let written = written.expect("the write committed at the leader");

    // A read asked now must see the write: it waits for the next round with the second.
    nodes[0].replica.read_index(b"third".to_vec()).unwrap();
    let mut wire: VecDeque<Message> = held.into();
    wire.extend(nodes[0].replica.drive().unwrap().messages);
    let mut confirmed = Vec::new();
    for _ in 0..1_000 {
        let Some(m) = wire.pop_front() else {
            break;
        };
        let to = usize::try_from(m.to).unwrap() - 1;
        let _ = nodes[to].replica.step(m);
        for n in nodes.iter_mut() {
            let out = n.replica.drive().unwrap();
            if n.replica.id() == 1 {
                confirmed.extend(out.reads);
            }
            wire.extend(out.messages);
        }
    }
    let at = |read: &[u8]| {
        confirmed
            .iter()
            .find(|(_, r)| r.as_slice() == read)
            .map(|(i, _)| *i)
            .unwrap_or_else(|| panic!("{:?} not confirmed", String::from_utf8_lossy(read)))
    };
    assert!(at(b"first") < written);
    assert_eq!(at(b"second"), at(b"third"));
    assert!(
        at(b"third") >= written,
        "a read confirmed at {} before the write at {written}",
        at(b"third")
    );
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
        generation: 1,
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
            replica: Replica::open(id, GROUP, Arc::clone(log), first_range(), &range(), id)
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

/// A member whose last acknowledged frame is damaged at rest reopens with the term and vote
/// it had, the entries that frame held cut and marked as possibly lacking. It judges a
/// request for its vote against the last entry it acknowledged, not its shorter log: a
/// candidate behind that entry may lack one it helped commit, and gets no answer, while one
/// that holds it does. It cannot lead without the entries, so it does not campaign, though
/// its election timer runs. Once the leader has sent it the entries again, it campaigns as
/// before (audit S01, the last frame; raft-log.md §6).
#[test]
fn a_member_whose_last_frame_was_damaged_stays_out_of_elections_until_it_holds_it_again() {
    use mantle_disk::sim::Fault;

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
    let log_id = |id: u64| 0x6c6f67 + u128::from(id);
    let files: Vec<Arc<SimFile>> = (1..=3).map(file).collect();
    let mut logs: Vec<Arc<Log<Arc<SimFile>>>> = files
        .iter()
        .zip(1u64..)
        .map(|(f, id)| Arc::new(Log::create(Arc::clone(f), log_config(), log_id(id)).unwrap()))
        .collect();
    let mut nodes: Vec<Node> = logs
        .iter()
        .zip(1u64..)
        .map(|(log, id)| Node {
            replica: Replica::open(id, GROUP, Arc::clone(log), first_range(), &range(), id)
                .unwrap(),
        })
        .collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    let register = |serial| Entry {
        at_ns: 10,
        commands: vec![Sessioned {
            session: 0,
            serial,
            unanswered: 0,
            command: Command::Register,
        }],
    };
    nodes[0].replica.propose(&register(0)).unwrap();
    settle(&mut nodes, &mut answers);

    // One more entry: members 2 and 3 write it and acknowledge it, and the leader hears
    // neither answer before member 2 stops.
    nodes[0].replica.propose(&register(1)).unwrap();
    let appends = nodes[0].replica.drive().unwrap().messages;
    for m in appends {
        let to = usize::try_from(m.to).unwrap() - 1;
        nodes[to].replica.step(m).unwrap();
    }
    for n in &mut nodes[1..] {
        let acks = n.replica.drive().unwrap().messages;
        assert!(acks.iter().any(|m| m.to == 1));
    }
    let acknowledged = logs[1].view(GROUP).unwrap().unwrap();
    let index = acknowledged.last;
    let acknowledged_term = logs[1].term(GROUP, index).unwrap();

    // Its last frame, the append it acknowledged, is damaged at rest: found as the valid
    // frame of member 2's log with the highest sequence.
    let image = files[1].durable_image().unwrap();
    let last_frame = image
        .chunks(4096)
        .enumerate()
        .filter_map(|(i, block)| {
            let h = mantle_log::format::FrameHeader::decode(block)?;
            let frame = image.get(i * 4096..i * 4096 + h.frame_len()?)?;
            (h.log == log_id(2) && h.verifies(frame)).then(|| (h.sequence, i * 4096))
        })
        .max()
        .unwrap()
        .1;
    files[1]
        .inject(Fault::BitFlip {
            offset: last_frame as u64 + 90,
            bit: 5,
            stored: true,
        })
        .unwrap();

    // It restarts from its device and its engine.
    let old = std::mem::replace(&mut nodes[1], node(9, 9));
    let engine = old.replica.into_engine();
    let stand_in = Arc::clone(&logs[0]);
    drop(std::mem::replace(&mut logs[1], stand_in));
    let (log, recovery) = Log::open(Arc::clone(&files[1]), log_config(), log_id(2)).unwrap();
    assert_eq!(recovery.restored, vec![GROUP]);
    logs[1] = Arc::new(log);
    nodes[1] = Node {
        replica: Replica::open(2, GROUP, Arc::clone(&logs[1]), engine, &range(), 2).unwrap(),
    };
    let view = logs[1].view(GROUP).unwrap().unwrap();
    assert_eq!(
        view.hard_state.map(|h| (h.term, h.vote)),
        acknowledged.hard_state.map(|h| (h.term, h.vote))
    );
    assert_eq!(view.last, index - 1);
    assert_eq!(view.uncertain.map(|m| m.index), Some(index));

    // It will not campaign, and its election timer, which runs, sends no request.
    assert!(matches!(
        nodes[1].replica.campaign(),
        Err(mantle_range::ReplicaError::Uncertain)
    ));
    for _ in 0..3 * SETTINGS.election_tick {
        nodes[1].replica.tick().unwrap();
    }
    assert!(nodes[1].replica.drive().unwrap().messages.is_empty());
    // Member 3 campaigns. Its request, as if from a candidate one entry behind what member
    // 2 acknowledged, gets no answer; as sent, holding that entry, it gets one.
    nodes[2].replica.campaign().unwrap();
    let request = nodes[2]
        .replica
        .drive()
        .unwrap()
        .messages
        .into_iter()
        .find(|m| m.to == 2)
        .unwrap();
    assert_eq!(
        (request.log_term, request.index),
        (acknowledged_term, index)
    );
    let mut behind = request.clone();
    behind.index = index - 1;
    nodes[1].replica.step(behind).unwrap();
    assert!(
        nodes[1].replica.drive().unwrap().messages.is_empty(),
        "a candidate behind an entry the member acknowledged was answered"
    );
    nodes[1].replica.step(request).unwrap();
    assert!(
        nodes[1]
            .replica
            .drive()
            .unwrap()
            .messages
            .iter()
            .any(|m| m.to == 3),
        "a candidate holding every entry the member acknowledged was not answered"
    );

    // The group goes on, and the leader, hearing from it, sends it the entries again.
    for _ in 0..20 {
        for n in &mut nodes {
            for _ in 0..SETTINGS.heartbeat_tick {
                n.replica.tick().unwrap();
            }
        }
        settle(&mut nodes, &mut answers);
        if !nodes[1].replica.is_uncertain().unwrap() {
            break;
        }
    }
    assert!(!nodes[1].replica.is_uncertain().unwrap());
    assert!(logs[1].view(GROUP).unwrap().unwrap().last >= index);
    nodes[1].replica.campaign().unwrap();
}

/// A log that refuses a ready for want of room keeps the ready waiting, whole, in its
/// replica, which answers `Stalled` and lets no tick move its timers meanwhile. Once the group
/// compacts, the next drive writes the same ready and the replica goes on (audit S04).
#[test]
fn a_ready_refused_for_room_waits_until_the_group_compacts() {
    let file = Arc::new(
        SimFile::new(
            Alignment::new(4096).unwrap(),
            Alignment::new(512).unwrap(),
            7,
        )
        .unwrap(),
    );
    // A ready of these settings is 512 bytes at most, 102 entries of the fewest bytes, and
    // the log holds a group to that.
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
    let log = Arc::new(Log::create(file, config, 0x6c6f67).unwrap());
    let alone = Range {
        boot: ConfState {
            voters: vec![1],
            ..ConfState::default()
        },
        settings: small,
        ..range()
    };
    let mut replica = Replica::open(1, GROUP, log, first_range(), &alone, 7).unwrap();
    replica.campaign().unwrap();
    assert!(replica.drive().unwrap().stalled.is_none());
    assert!(replica.is_leader());
    let register = |serial| Entry {
        at_ns: 10,
        commands: vec![Sessioned {
            session: 0,
            serial,
            unanswered: 0,
            command: Command::Register,
        }],
    };
    // One entry a ready, none compacted, until the group would retain past its bound.
    let mut stalled = None;
    for serial in 0..200 {
        replica.propose(&register(serial)).unwrap();
        let out = replica.drive().unwrap();
        if out.stalled.is_some() {
            stalled = out.stalled;
            break;
        }
    }
    assert!(
        matches!(stalled, Some(mantle_log::LogError::Backlog(GROUP))),
        "{stalled:?}"
    );
    let applied = replica.applied();
    // Waiting: calls are refused, ticks move nothing, and the ready is written first.
    replica.tick().unwrap();
    assert!(matches!(
        replica.propose(&register(999)),
        Err(mantle_range::ReplicaError::Stalled)
    ));
    assert!(replica.drive().unwrap().stalled.is_some());
    replica.compact(0).unwrap();
    let out = replica.drive().unwrap();
    assert!(out.stalled.is_none(), "{:?}", out.stalled);
    assert_eq!(replica.applied(), applied + 1);
    // And the group goes on.
    replica.propose(&register(1_000)).unwrap();
    replica.drive().unwrap();
    assert_eq!(replica.applied(), applied + 2);
}

/// A member refuses settings its log cannot hold to: an entry of the range's largest must
/// fit one frame, and a ready of the range's largest must fit the group's bounds.
#[test]
fn a_member_refuses_settings_its_log_cannot_hold() {
    let open = |settings: Settings, config: LogConfig| {
        let file = Arc::new(
            SimFile::new(
                Alignment::new(4096).unwrap(),
                Alignment::new(512).unwrap(),
                8,
            )
            .unwrap(),
        );
        let log = Arc::new(Log::create(file, config, 0x6c6f67).unwrap());
        let r = Range {
            settings,
            ..range()
        };
        Replica::open(1, GROUP, log, first_range(), &r, 8).map(|_| ())
    };
    assert!(open(SETTINGS, log_config()).is_ok());
    let wide = Settings {
        max_entry_bytes: 64 * 4096,
        ..SETTINGS
    };
    assert!(matches!(
        open(wide, log_config()),
        Err(mantle_range::ReplicaError::Config(_))
    ));
    let narrow = LogConfig {
        group_bytes: 1 << 16,
        ..log_config()
    };
    assert!(matches!(
        open(SETTINGS, narrow),
        Err(mantle_range::ReplicaError::Config(_))
    ));
    let few = LogConfig {
        group_entries: 1 << 10,
        ..log_config()
    };
    assert!(matches!(
        open(SETTINGS, few),
        Err(mantle_range::ReplicaError::Config(_))
    ));
}

/// A snapshot's fate reported while its sender has a ready out is kept, not refused: the
/// leader pauses replication to a member until it learns its snapshot's fate, and a report
/// refused and lost left it paused for good. A seed of the simulation found it, a member one
/// snapshot behind forever once faults stopped.
#[test]
fn a_snapshot_report_that_comes_while_a_ready_is_out_is_kept() {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, 40 + id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    let register = Sessioned {
        session: 0,
        serial: 0,
        unanswered: 0,
        command: Command::Register,
    };
    let entry = |commands| Entry {
        at_ns: 10,
        commands,
    };
    nodes[0].replica.propose(&entry(vec![register])).unwrap();
    settle(&mut nodes, &mut answers);
    // Entries member 3 never hears of, then a compaction that leaves it a snapshot behind.
    let cut_off = |nodes: &mut [Node]| {
        let mut wire: VecDeque<Message> = VecDeque::new();
        for _ in 0..10_000 {
            for n in nodes.iter_mut().take(2) {
                wire.extend(n.replica.drive().unwrap().messages);
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
    nodes[0].replica.compact(0).unwrap();
    // The leader sends member 3 its snapshot, which is lost.
    let mut snapshot = None;
    for _ in 0..200 {
        nodes[0].replica.tick().unwrap();
        for m in nodes[0].replica.drive().unwrap().messages {
            if m.to == 3 && kind(&m) == Some(mantle_range::MessageType::MsgSnapshot) {
                snapshot = Some(m);
            } else if m.to == 3 {
                let _ = nodes[2].replica.step(m);
                for back in nodes[2].replica.drive().unwrap().messages {
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
    // Its loss is reported while the leader has a ready out.
    nodes[0].replica.propose(&entry(Vec::new())).unwrap();
    let out = nodes[0].replica.begin().unwrap();
    assert!(out.persisting, "the leader holds a ready");
    nodes[0].replica.report_snapshot(3, false).unwrap();
    nodes[0].replica.wait_persisted();
    // Delivered in order from here, every snapshot reported as it arrives.
    let mut wire: VecDeque<Message> = out.messages.into();
    for _ in 0..10_000 {
        for n in &mut nodes {
            wire.extend(n.replica.drive().unwrap().messages);
        }
        let Some(m) = wire.pop_front() else { break };
        let (from, to) = (m.from, m.to);
        let is_snapshot = kind(&m) == Some(mantle_range::MessageType::MsgSnapshot);
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

/// A node that overlaps its members' flushes (`begin`) finds each member's ready still out when
/// the network next delivers to it: under steady load the leader always has one, and so, a
/// round after each append, do its followers. The messages that come then are taken once the
/// ready is done, as focal's own shell has its host retain them (docs/design/replica.md §3).
/// Refused instead, as they were, every acknowledgement reached the leader while it flushed and
/// was lost, and none of the load ever committed.
#[test]
fn a_leader_whose_answers_come_while_it_flushes_still_commits() {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, 60 + id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    assert!(nodes[0].replica.is_leader());
    let before = nodes[0].replica.applied();

    let mut wire: Vec<Message> = Vec::new();
    let mut proposed = 0u64;
    for round in 0..200u64 {
        // Each node finishes the ready whose flush it waited out, proposes when it leads and
        // takes proposals, and begins its next ready.
        for (i, n) in nodes.iter_mut().enumerate() {
            n.replica.wait_persisted();
            wire.extend(n.replica.begin().unwrap().messages);
            if i == 0
                && n.replica
                    .propose(&Entry {
                        at_ns: 100 + round,
                        commands: Vec::new(),
                    })
                    .is_ok()
            {
                proposed += 1;
                wire.extend(n.replica.begin().unwrap().messages);
            }
        }
        // What was sent arrives, and the clock ticks, while those readies flush.
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

/// Ticks that come while a ready flushes still move the member's clock once it is done: a
/// leader whose every tick lands during a flush still sends its heartbeats. Dropped, as they
/// were, its clock stood still for as long as it had load.
#[test]
fn ticks_while_a_ready_flushes_still_count() {
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
    let out = nodes[0].replica.begin().unwrap();
    assert!(out.persisting);
    for _ in 0..SETTINGS.heartbeat_tick {
        nodes[0].replica.tick().unwrap();
    }
    nodes[0].replica.wait_persisted();
    // A drive takes one ready: the one flushing, then the one its held ticks made.
    let mut messages = Vec::new();
    for _ in 0..2 {
        messages.extend(nodes[0].replica.drive().unwrap().messages);
    }
    let mut to: Vec<u64> = messages
        .iter()
        .filter(|m| kind(m) == Some(mantle_range::MessageType::MsgHeartbeat))
        .map(|m| m.to)
        .collect();
    to.sort_unstable();
    assert_eq!(to, [2, 3], "{messages:?}");
}

/// Ticks held while a ready flushes stop at the longest election timeout the core draws: the
/// core acts on a timer at most once in that many, and a follower whose device stalled for
/// many timeouts would otherwise replay them as a burst of campaigns, each with its messages.
#[test]
fn ticks_held_through_a_long_flush_replay_at_most_one_timeouts_worth() {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, 80 + id)).collect();
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
    let out = nodes[0].replica.begin().unwrap();
    for m in out.messages.into_iter().filter(|m| m.to == 2) {
        nodes[1].replica.step(m).unwrap();
    }
    assert!(nodes[1].replica.begin().unwrap().persisting);
    for _ in 0..100 * SETTINGS.election_tick {
        nodes[1].replica.tick().unwrap();
    }
    nodes[1].replica.wait_persisted();
    let out = nodes[1].replica.drive().unwrap();
    let campaigns = out
        .messages
        .iter()
        .filter(|m| {
            matches!(
                kind(m),
                Some(
                    mantle_range::MessageType::MsgRequestVote
                        | mantle_range::MessageType::MsgRequestPreVote
                )
            )
        })
        .count();
    // Two timeouts at most, each asking both other members.
    assert!(
        campaigns <= 4,
        "{campaigns} requests for votes: {:?}",
        out.messages
    );
}

/// A round of reads no quorum confirms within an election timeout, as when the leader's
/// heartbeats never arrive, is given up, and its reads go back to their callers through
/// `drive`, to be answered or asked again. Before, they were dropped with the round, and a
/// caller waited for an answer that never came.
#[test]
fn reads_of_a_round_no_quorum_confirms_go_back_to_their_callers() {
    let mut nodes: Vec<Node> = (1..=3).map(|id| node(id, 90 + id)).collect();
    let mut answers = Vec::new();
    nodes[0].replica.campaign().unwrap();
    settle(&mut nodes, &mut answers);
    assert!(nodes[0].replica.is_leader());
    nodes[0].replica.read_index(b"lost".to_vec()).unwrap();
    // Nothing the leader sends is delivered from here on.
    let mut given_back = Vec::new();
    for _ in 0..2 * SETTINGS.election_tick {
        nodes[0].replica.tick().unwrap();
        let out = nodes[0].replica.drive().unwrap();
        assert!(out.reads.is_empty(), "a read confirmed with no quorum");
        given_back.extend(out.unconfirmed);
    }
    assert_eq!(given_back, [b"lost".to_vec()]);
}

/// Members of a group of three, each with its own device and log, found by identity: a
/// member rebuilt under a new identity keeps its device.
///
/// A device is shared between the test, which injects faults into it and damages its medium,
/// and its log, whose writer thread holds it: `Log` takes its file by value, `'static`, so
/// the test cannot lend it one. A log is shared between its member, as `Replica::open` takes
/// it, and the test, which reads what it holds; a stopped member's log is `None`.
struct Devices {
    files: Vec<Arc<SimFile>>,
    logs: Vec<Option<Arc<Log<Arc<SimFile>>>>>,
    nodes: Vec<Node>,
}

fn log_id(slot: usize) -> u128 {
    0x6c6f67 + u128::try_from(slot).unwrap() + 1
}

impl Devices {
    fn new(seed: u64) -> Self {
        let files: Vec<Arc<SimFile>> = (0..3)
            .map(|i| {
                Arc::new(
                    SimFile::new(
                        Alignment::new(4096).unwrap(),
                        Alignment::new(512).unwrap(),
                        seed + i,
                    )
                    .unwrap(),
                )
            })
            .collect();
        let logs: Vec<Option<Arc<Log<Arc<SimFile>>>>> = files
            .iter()
            .enumerate()
            .map(|(slot, f)| {
                Some(Arc::new(
                    Log::create(Arc::clone(f), log_config(), log_id(slot)).unwrap(),
                ))
            })
            .collect();
        let nodes = logs
            .iter()
            .flatten()
            .zip(1u64..)
            .map(|(log, id)| Node {
                replica: Replica::open(
                    id,
                    GROUP,
                    Arc::clone(log),
                    first_range(),
                    &range(),
                    seed + id,
                )
                .unwrap(),
            })
            .collect();
        Self { files, logs, nodes }
    }

    /// Stops the member in `slot`, as a crash does once its log and engine are durable: its
    /// engine, which the caller keeps or drops, comes back.
    fn stop(&mut self, slot: usize) -> Model {
        let old = std::mem::replace(&mut self.nodes[slot], node(9, 9));
        self.logs[slot] = None;
        old.replica.into_engine()
    }

    fn slot(&self, id: u64) -> Option<usize> {
        self.nodes.iter().position(|n| n.replica.id() == id)
    }

    /// Drives every member but those in `out` until no message is left in flight, delivering
    /// in order: each applied entry's answers go to `answers` by member.
    fn settle(&mut self, out: &[u64], answers: &mut Vec<(u64, u64, Vec<Answer>)>) {
        self.settle_checking(out, answers, |_, _| {});
    }

    /// As `settle`, showing `check` every message a member sends, with that member's log.
    fn settle_checking(
        &mut self,
        out: &[u64],
        answers: &mut Vec<(u64, u64, Vec<Answer>)>,
        mut check: impl FnMut(&Message, &Log<Arc<SimFile>>),
    ) {
        let mut wire: VecDeque<Message> = VecDeque::new();
        for _ in 0..10_000 {
            for (n, log) in self.nodes.iter_mut().zip(&self.logs) {
                let id = n.replica.id();
                if out.contains(&id) {
                    continue;
                }
                let drove = n.replica.drive().unwrap();
                if let Some(log) = log {
                    for m in &drove.messages {
                        check(m, log);
                    }
                }
                wire.extend(drove.messages);
                answers.extend(drove.applied.into_iter().map(|a| {
                    (
                        id,
                        a.index,
                        a.answers.into_iter().map(|(_, _, x)| x).collect(),
                    )
                }));
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
    fn beat(&mut self, out: &[u64], answers: &mut Vec<(u64, u64, Vec<Answer>)>) {
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
    fn leader(&mut self, out: &[u64], answers: &mut Vec<(u64, u64, Vec<Answer>)>) -> usize {
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

fn registration(serial: u64) -> Entry {
    Entry {
        at_ns: 10,
        commands: vec![Sessioned {
            session: 0,
            serial,
            unanswered: 0,
            command: Command::Register,
        }],
    }
}

/// Every member's answers at each index, which must agree with every other's.
fn agreed(answers: &[(u64, u64, Vec<Answer>)]) -> std::collections::BTreeMap<u64, Vec<Answer>> {
    let mut by_index = std::collections::BTreeMap::new();
    for (id, index, a) in answers {
        let first = by_index.entry(*index).or_insert_with(|| a.clone());
        assert_eq!(first, a, "member {id} applied index {index} another way");
    }
    by_index
}

/// A member whose device fails the write or flush of a ready: the member is fenced. The call
/// that met the failure answers `Fenced`, the member sent no acknowledgement of what that
/// ready held, and every later call that could acknowledge anything or move its state
/// answers `Fenced` too: ticks, messages, proposals, changes, reads, campaigns, snapshot
/// reports, compaction and drives. The two others go on without it, electing a leader if it
/// led, and commit. A log that only refused the ready for room leaves the member waiting with
/// it instead (`a_ready_refused_for_room_waits_until_the_group_compacts` and those after it).
fn a_durability_failure_fences(fault: &mantle_disk::sim::Fault, failing: u64) {
    use mantle_range::{ConfChangeV2, MessageType, ReplicaError};

    let mut d = Devices::new(60 + failing);
    let mut answers = Vec::new();
    d.nodes[0].replica.campaign().unwrap();
    d.settle(&[], &mut answers);
    d.nodes[0].replica.propose(&registration(0)).unwrap();
    d.settle(&[], &mut answers);
    let before = d.nodes[0].replica.applied();

    let slot = d.slot(failing).unwrap();
    d.files[slot].inject(fault.clone()).unwrap();
    d.nodes[0].replica.propose(&registration(1)).unwrap();
    let entry = before + 1;
    // Drive in order, as `settle` does, until the failing member meets its fault.
    let mut wire: VecDeque<Message> = VecDeque::new();
    let mut failure = None;
    'drive: for _ in 0..1_000 {
        for n in &mut d.nodes {
            match n.replica.drive() {
                Ok(out) => {
                    if n.replica.id() == failing {
                        // An acknowledgement of the entry leaves only once it is durable.
                        assert!(
                            !out.messages.iter().any(|m| {
                                m.msg_type == MessageType::MsgAppendResponse as i32
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
            break;
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
        msg_type: MessageType::MsgHeartbeat as i32,
        from: if failing == 1 { 2 } else { 1 },
        to: failing,
        term: fenced.term(),
        ..Message::default()
    };
    let calls: [(&str, Result<(), ReplicaError>); 10] = [
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
        ("compact", fenced.compact(0)),
        ("drive", fenced.drive().map(|_| ())),
        ("begin", fenced.begin().map(|_| ())),
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
        a_durability_failure_fences(&mantle_disk::sim::Fault::SyncError, failing);
    }
}

/// A failed write fences the member, whether it follows or leads (audit S04).
#[test]
fn a_failed_write_fences_the_member_and_the_group_goes_on_without_it() {
    for failing in [2, 1] {
        a_durability_failure_fences(&mantle_disk::sim::Fault::WriteError, failing);
    }
}

/// A group of one on a log that holds `config`'s groups and segments. The log is shared with
/// the test, which writes other groups to it, as `Replica::open` takes it (`Devices`).
fn alone(config: LogConfig, settings: Settings, seed: u64) -> (Arc<Log<Arc<SimFile>>>, Member) {
    let file = Arc::new(
        SimFile::new(
            Alignment::new(4096).unwrap(),
            Alignment::new(512).unwrap(),
            seed,
        )
        .unwrap(),
    );
    let log = Arc::new(Log::create(file, config, 0x6c6f67).unwrap());
    let range = Range {
        boot: ConfState {
            voters: vec![1],
            ..ConfState::default()
        },
        settings,
        ..range()
    };
    let replica = Replica::open(1, GROUP, Arc::clone(&log), first_range(), &range, seed).unwrap();
    (log, replica)
}

type Member = Replica<Arc<SimFile>, Model>;

/// A log that holds all the groups it may refuses a new member's first ready: the ready
/// waits, whole, the member is not fenced, and once another group leaves the log the next
/// drive writes the same ready and the member goes on (audit S04).
#[test]
fn a_ready_refused_for_want_of_a_group_waits_until_one_leaves() {
    let config = LogConfig {
        max_groups: 2,
        ..log_config()
    };
    let (log, mut replica) = alone(config, SETTINGS, 11);
    for group in [7, 8] {
        log.write(
            group,
            mantle_log::Update {
                hard_state: Some(mantle_log::HardState {
                    term: 1,
                    vote: 0,
                    commit: 0,
                }),
                ..mantle_log::Update::default()
            },
        )
        .unwrap();
    }
    replica.campaign().unwrap();
    let out = replica.drive().unwrap();
    assert!(
        matches!(out.stalled, Some(mantle_log::LogError::TooManyGroups(2))),
        "{:?}",
        out.stalled
    );
    assert!(!replica.is_fenced());
    assert!(matches!(
        replica.propose(&registration(0)),
        Err(mantle_range::ReplicaError::Stalled)
    ));
    // Still waiting while nothing has left.
    assert!(replica.drive().unwrap().stalled.is_some());
    log.write(
        7,
        mantle_log::Update {
            remove: true,
            ..mantle_log::Update::default()
        },
    )
    .unwrap();
    let out = replica.drive().unwrap();
    assert!(out.stalled.is_none(), "{:?}", out.stalled);
    assert!(replica.is_leader());
    let applied = replica.applied();
    replica.propose(&registration(0)).unwrap();
    replica.drive().unwrap();
    assert_eq!(replica.applied(), applied + 1);
}

/// A member whose group's records on its log are damaged at rest, found when it restarts, is
/// quarantined and rebuilt from its peers (audit S01c; raft-log.md §6, replica.md §6). Its
/// last frame held a fast-track proposal, which a persist record does not carry, so the log
/// fences the group and serves it to no one. The member does not open under its identity:
/// it may have voted in terms its log no longer shows. The group goes on without it. The
/// node rebuilds it under a new identity on the same device: the group's records are
/// removed, a new member opens there, and the leader runs the replacement, adding it as a
/// learner, catching it up, and swapping it for the damaged one. After, the group is whole,
/// every member holds every entry any member applied with the same answers, and the same
/// rows, and it goes on committing.
#[test]
fn a_member_whose_group_was_damaged_at_rest_is_rebuilt_from_its_peers() {
    use mantle_range::ReplicaError;
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
    let open = Command::Name(Box::new(name::Command::Gate(GateChange {
        bucket: "b".into(),
        incarnation: 1,
        attempt: 1,
        from: None,
        to: Some(GateState::Open),
        generation: 1,
    })));
    let write = |serial: u64, command: Command| Entry {
        at_ns: 20,
        commands: vec![Sessioned {
            session,
            serial,
            unanswered: serial,
            command,
        }],
    };
    d.nodes[0].replica.propose(&write(1, open)).unwrap();
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
    let file = Arc::clone(&d.files[2]);
    {
        let (log, _) = Log::open(Arc::clone(&file), log_config(), log_id(2)).unwrap();
        let view = log.view(GROUP).unwrap().unwrap();
        let term = view.hard_state.unwrap().term;
        log.write(
            GROUP,
            mantle_log::Update {
                proposals: vec![mantle_log::Proposal {
                    index: view.last + 1,
                    term,
                    bytes: Arc::from(&b"fast"[..]),
                }],
                ..mantle_log::Update::default()
            },
        )
        .unwrap();
    }
    damage_last_frame(&file, log_id(2));

    // It restarts: the log reports its group damaged, and the member does not open.
    let (log, recovery) = Log::open(Arc::clone(&file), log_config(), log_id(2)).unwrap();
    assert_eq!(recovery.damaged, vec![GROUP]);
    let log = Arc::new(log);
    assert!(matches!(
        Replica::open(3, GROUP, Arc::clone(&log), first_range(), &range(), 3),
        Err(ReplicaError::Damaged)
    ));

    // The group goes on without it.
    let out = [3, 9];
    d.nodes[0].replica.propose(&write(4, put("k3"))).unwrap();
    d.settle(&out, &mut answers);

    // The node rebuilds it as member 4, and the leader runs the replacement.
    let (rebuilt, replacement) =
        Replica::rebuild(3, 4, GROUP, Arc::clone(&log), first_range(), &range(), 4).unwrap();
    assert_eq!(rebuilt.id(), 4);
    d.nodes[2] = Node { replica: rebuilt };
    d.logs[2] = Some(log);
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
    let rows_of_leader = rows(d.nodes[leader].replica.engine());
    for n in &d.nodes {
        let r = &n.replica;
        let mut conf = r.configuration().clone();
        conf.voters.sort_unstable();
        assert_eq!(conf, want, "member {}", r.id());
        assert!(r.applied() >= last, "member {}", r.id());
        assert_eq!(rows(r.engine()), rows_of_leader, "member {}", r.id());
        for key in ["k1", "k2", "k3", "k4"] {
            let current = name::current(r.engine(), "b", key).unwrap().unwrap();
            assert_eq!(current.1.etag, key, "member {}", r.id());
        }
    }
    // Every entry acknowledged before the damage is held with the answers it had then.
    for (index, a) in agreed_now {
        assert_eq!(by_index.get(&index), Some(&a), "index {index}");
    }
}

/// Flips a bit of the last frame of log `id` on `file`'s medium, as damage at rest does: a
/// fault written to the medium, so no clearing of faults heals it.
fn damage_last_frame(file: &SimFile, id: u128) {
    use mantle_disk::block::BlockFile;
    use mantle_disk::buf::AlignedBuf;

    let image = file.durable_image().unwrap();
    let last_frame = image
        .chunks(4096)
        .enumerate()
        .filter_map(|(i, block)| {
            let h = mantle_log::format::FrameHeader::decode(block)?;
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

/// A member whose engine applied entries its last frame held, a frame recovery then found
/// damaged and cut back, keeps its identity where the applied entry's term is known: its log
/// starts at the engine's index, of that term, and its mark stays. Terms never fall along a
/// log, so the term is known when the engine's index is the mark's, or when the last entry
/// left is of the mark's term; otherwise the member is `Damaged` and rebuilt
/// (docs/design/replica.md §4). Each case: the terms of entries 1 to 3, those of entries 4
/// and 5 in the last frame, the engine's index, and the start the log takes, if any.
#[test]
fn a_member_whose_engine_applied_what_its_damaged_log_lost_keeps_its_identity_when_it_can() {
    use mantle_log::{Entries, HardState, Start, Update};

    let cases: [(u64, [u64; 2], u64, Option<u64>); 5] = [
        (2, [2, 2], 4, Some(2)),
        (2, [2, 2], 5, Some(2)),
        (1, [1, 2], 5, Some(2)),
        (1, [1, 2], 4, None),
        (1, [2, 2], 4, None),
    ];
    for (case, (early, late, applied, start)) in cases.into_iter().enumerate() {
        let file = Arc::new(
            SimFile::new(
                Alignment::new(4096).unwrap(),
                Alignment::new(512).unwrap(),
                80 + case as u64,
            )
            .unwrap(),
        );
        let entry = |term: u64| mantle_log::Entry {
            term,
            bytes: Arc::from(&[0u8, 0, 0, 0, 0][..]),
        };
        let voted = HardState {
            term: 2,
            vote: 1,
            commit: 0,
        };
        {
            let log = Log::create(Arc::clone(&file), log_config(), 0x6c6f67).unwrap();
            log.write(
                GROUP,
                Update {
                    entries: Some(Entries {
                        first: 1,
                        entries: vec![entry(early); 3],
                    }),
                    hard_state: Some(HardState { commit: 3, ..voted }),
                    ..Update::default()
                },
            )
            .unwrap();
            log.write(
                GROUP,
                Update {
                    entries: Some(Entries {
                        first: 4,
                        entries: late.iter().map(|&t| entry(t)).collect(),
                    }),
                    hard_state: Some(HardState { commit: 5, ..voted }),
                    ..Update::default()
                },
            )
            .unwrap();
        }
        damage_last_frame(&file, 0x6c6f67);
        let (log, recovery) = Log::open(Arc::clone(&file), log_config(), 0x6c6f67).unwrap();
        assert_eq!(recovery.restored, vec![GROUP], "case {case}");
        let mark = Start {
            index: 5,
            term: late[1],
        };
        let view = log.view(GROUP).unwrap().unwrap();
        assert_eq!((view.last, view.uncertain), (3, Some(mark)), "case {case}");
        let log = Arc::new(log);
        // The engine made its rows durable through `applied`.
        let mut engine = first_range();
        for index in 1..=applied {
            engine.apply(index, &[]).unwrap();
        }
        engine.persist().unwrap();
        let opened = Replica::open(1, GROUP, Arc::clone(&log), engine, &range(), 1);
        match start {
            Some(term) => {
                let mut member = opened.unwrap_or_else(|e| panic!("case {case}: {e}"));
                let view = log.view(GROUP).unwrap().unwrap();
                assert_eq!(
                    view.start,
                    Start {
                        index: applied,
                        term
                    },
                    "case {case}"
                );
                assert_eq!(view.last, applied, "case {case}");
                assert_eq!(
                    view.hard_state.map(|h| (h.term, h.vote, h.commit)),
                    Some((voted.term, voted.vote, applied)),
                    "case {case}"
                );
                // The mark stays while the log lacks an entry it covers.
                assert_eq!(member.is_uncertain().unwrap(), applied < mark.index);
                assert_eq!(member.applied(), applied);
            }
            None => assert!(
                matches!(opened, Err(mantle_range::ReplicaError::Damaged)),
                "case {case}: {:?}",
                opened.err()
            ),
        }
    }
}

/// A member of a group of three applies an entry its last frame held, makes its rows durable,
/// and stops; the frame is damaged at rest. It reopens in place, under its identity, its log
/// starting at the entry its engine applied, and the group repairs it from its peers: every
/// acknowledgement it sends names only entries its log holds, it takes the entries after, and
/// the group, whole, commits on. No entry any member applied is lost, and every member holds
/// the same rows (audit S01c).
#[test]
fn a_member_whose_engine_applied_what_its_damaged_log_lost_is_repaired_in_place() {
    use mantle_range::MessageType;

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
    d.nodes[1].replica.compact(u64::MAX).unwrap();
    let engine = d.stop(1);
    assert_eq!(engine.durable(), entry);
    let file = Arc::clone(&d.files[1]);
    damage_last_frame(&file, log_id(1));

    let (log, recovery) = Log::open(Arc::clone(&file), log_config(), log_id(1)).unwrap();
    assert_eq!(recovery.restored, vec![GROUP]);
    let view = log.view(GROUP).unwrap().unwrap();
    assert!(view.last < entry, "the lost frame held no applied entry");
    assert_eq!(view.uncertain.map(|m| m.index), Some(entry));
    let log = Arc::new(log);
    let member = Replica::open(2, GROUP, Arc::clone(&log), engine, &range(), 2).unwrap();
    assert_eq!(member.applied(), entry);
    d.nodes[1] = Node { replica: member };
    d.logs[1] = Some(log);

    // The group goes on, and repairs it: what it acknowledges, its log holds.
    let mut acknowledged = 0;
    for serial in 2..5 {
        d.nodes[0].replica.propose(&registration(serial)).unwrap();
        d.settle_checking(&[], &mut answers, |m, log| {
            if m.from == 2 && m.msg_type == MessageType::MsgAppendResponse as i32 && !m.reject {
                let last = log.view(GROUP).unwrap().unwrap().last;
                assert!(m.index <= last, "acknowledged {} holding {last}", m.index);
                acknowledged += 1;
            }
        });
    }
    assert!(acknowledged > 0);
    let last = d.nodes[0].replica.applied();
    agreed(&answers);
    for n in &d.nodes {
        assert_eq!(n.replica.applied(), last, "member {}", n.replica.id());
        assert_eq!(
            rows(n.replica.engine()),
            rows(d.nodes[0].replica.engine()),
            "member {}",
            n.replica.id()
        );
    }
    assert!(!d.nodes[1].replica.is_uncertain().unwrap());
}

/// A log whose every segment holds live records refuses a ready it has no room for with
/// `Full`: the ready waits, whole, the member is not fenced, and once the group holding the
/// segments compacts, the member's next drives write the same ready and it goes on (audit
/// S04). The log is the smallest its configuration takes, segments of four blocks, so it
/// fills within a few frames. Every entry is a quarter of what one frame holds, as the log
/// says (`Log::entry_room`), so several share a segment and compacting them frees one; a log
/// refused `Full` does not yet recover room for a frame much larger than that (reported to
/// the log, docs/design/replica.md §7).
#[test]
fn a_ready_refused_for_a_full_log_waits_until_another_group_compacts() {
    let config = LogConfig {
        segment_bytes: 4 * 4096,
        max_segments: 4,
        ..log_config()
    };
    let file = Arc::new(
        SimFile::new(
            Alignment::new(4096).unwrap(),
            Alignment::new(512).unwrap(),
            12,
        )
        .unwrap(),
    );
    // Shared with the member as `Replica::open` takes it; the test writes another group.
    let log = Arc::new(Log::create(file, config, 0x6c6f67).unwrap());
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
    let mut replica = Replica::open(1, GROUP, Arc::clone(&log), first_range(), &alone, 12).unwrap();
    replica.campaign().unwrap();
    // A drive takes one ready at least: the campaign's, then its leader's empty entry.
    for _ in 0..2 {
        let out = replica.drive().unwrap();
        assert!(out.stalled.is_none(), "{:?}", out.stalled);
    }
    assert!(replica.is_leader());
    // Another group's entries fill every segment with live records.
    let mut last = 0;
    loop {
        let written = log.write(
            7,
            mantle_log::Update {
                entries: Some(mantle_log::Entries {
                    first: last + 1,
                    entries: vec![mantle_log::Entry {
                        term: 1,
                        bytes: Arc::from(vec![7u8; quarter]),
                    }],
                }),
                ..mantle_log::Update::default()
            },
        );
        match written {
            Ok(()) => last += 1,
            Err(mantle_log::LogError::Full) => break,
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
            command: Command::Register,
        });
        if many.encode().unwrap().len() > quarter {
            many.commands.pop();
            break;
        }
    }
    replica.propose(&many).unwrap();
    let applied = replica.applied();
    let out = replica.drive().unwrap();
    assert!(
        matches!(out.stalled, Some(mantle_log::LogError::Full)),
        "{:?}",
        out.stalled
    );
    assert!(!replica.is_fenced());
    assert!(matches!(
        replica.propose(&registration(0)),
        Err(mantle_range::ReplicaError::Stalled)
    ));
    // The other group compacts past everything it holds.
    log.write(
        7,
        mantle_log::Update {
            start: Some(mantle_log::Start {
                index: last,
                term: 1,
            }),
            ..mantle_log::Update::default()
        },
    )
    .unwrap();
    // The next drive writes the waiting ready, and the one after it applies the entry, which
    // a member of one commits at once.
    for drive in 0..2 {
        let out = replica.drive().unwrap();
        assert!(out.stalled.is_none(), "drive {drive}: {:?}", out.stalled);
    }
    assert_eq!(replica.applied(), applied + 1);
}
