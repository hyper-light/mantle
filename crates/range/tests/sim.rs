//! Deterministic simulation of one range's group (docs/design/replica.md §5).
//!
//! Three replicas, each with its log on a simulated device and a model engine, run over a
//! simulated network that delays, drops and partitions messages. Nodes crash, losing what
//! neither their log nor their engine had made durable, and restart from what was. A
//! gateway puts keys through whichever member leads, retrying through leader changes with
//! its session's serial numbers. A run is its seed.
//!
//! After every run:
//! - every index was applied with the same answers on every member that applied it;
//! - once faults stop, every put completes;
//! - every put the gateway was answered exists exactly once on every member, however often
//!   it was retried;
//! - every member holds the same rows.
//!
//! `MANTLE_SIM_SEEDS` sets how many runs, and `MANTLE_SIM_SEED` where they begin.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use mantle_disk::buf::Alignment;
use mantle_disk::sim::{Crash, SimFile};
use mantle_log::{Config as LogConfig, Log};
use mantle_meta::apply::Layer;
use mantle_meta::engine::{Model, Rows};
use mantle_meta::name::{self, GateChange, Preconditions, Put};
use mantle_meta::record::{GateState, Version, Versioning};
use mantle_meta::session::Rules;
use mantle_meta::wire::{Answer, Command, Entry, Sessioned};
use mantle_range::{ConfState, Message, Range, Replica, ReplicaError, Settings};

const GROUP: u128 = 0x0072_616e_6765;
const MEMBERS: u64 = 3;

fn log_config() -> LogConfig {
    LogConfig {
        segment_bytes: 16 * 4096,
        max_segments: 64,
        max_groups: 4,
        group_entries: 1 << 16,
        group_bytes: 1 << 24,
        group_cache: 1 << 12,
        queue_submissions: 16,
        queue_bytes: 1 << 20,
    }
}

const SETTINGS: Settings = Settings {
    election_tick: 10,
    heartbeat_tick: 2,
    max_size_per_msg: 1 << 16,
    max_inflight_msgs: 16,
    max_uncommitted_size: 1 << 20,
    max_committed_size_per_ready: 1 << 20,
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
    lifetime_ns: u64::MAX / 2,
    max_sessions: 64,
    max_answers: 16,
    expiries_per_entry: 8,
};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn chance(&mut self, per_mille: u64) -> bool {
        self.below(1000) < per_mille
    }
}

type Member = Replica<Arc<SimFile>, Model>;

struct Node {
    id: u64,
    file: Arc<SimFile>,
    /// `None` while the node is down; its engine is kept as the crash left it.
    replica: Option<Member>,
    engine: Option<Model>,
    /// Restarts so far, which seed its next core.
    lives: u64,
}

fn log_id(id: u64) -> u128 {
    0x6c6f_6700_0000 + u128::from(id)
}

impl Node {
    fn new(id: u64, seed: u64) -> Self {
        let file = Arc::new(
            SimFile::new(
                Alignment::new(4096).unwrap(),
                Alignment::new(512).unwrap(),
                seed ^ (id << 32),
            )
            .unwrap(),
        );
        let log = Arc::new(Log::create(Arc::clone(&file), log_config(), log_id(id)).unwrap());
        let replica = Replica::open(id, GROUP, log, Model::default(), &range(), seed ^ id).unwrap();
        Self {
            id,
            file,
            replica: Some(replica),
            engine: None,
            lives: 0,
        }
    }

    /// Loses power: the process and whatever its log and engine had not made durable.
    fn crash(&mut self) {
        let Some(replica) = self.replica.take() else {
            return;
        };
        let mut engine = replica.into_engine();
        engine.crash();
        self.engine = Some(engine);
        self.file.crash(Crash::Random).unwrap();
    }

    fn restart(&mut self, seed: u64) {
        let Some(engine) = self.engine.take() else {
            return;
        };
        self.lives += 1;
        self.file.clear_faults().unwrap();
        let (log, recovery) =
            Log::open(Arc::clone(&self.file), log_config(), log_id(self.id)).unwrap();
        assert!(recovery.damaged.is_empty(), "{recovery:?}");
        let replica = Replica::open(
            self.id,
            GROUP,
            Arc::new(log),
            engine,
            &range(),
            seed ^ self.id ^ (self.lives << 40),
        )
        .unwrap();
        self.replica = Some(replica);
    }
}

/// A put the gateway has in flight.
#[derive(Debug, Clone)]
struct Op {
    serial: u64,
    key: String,
    etag: String,
    sent_at: u64,
}

/// A gateway with one session, putting keys one at a time.
struct Gateway {
    session: Option<u64>,
    /// The nonce and time of a registration in flight.
    registering: Option<(u64, u64)>,
    next_serial: u64,
    outstanding: Option<Op>,
    todo: VecDeque<(String, String)>,
    /// Puts answered, and their answers.
    done: Vec<(String, String, Answer)>,
    nonce: u64,
}

/// Steps without an answer before the gateway sends again.
const PATIENCE: u64 = 40;

struct World {
    seed: u64,
    rng: Rng,
    nodes: Vec<Node>,
    /// Messages in flight, each with the step it arrives at.
    wire: Vec<(u64, Message)>,
    step: u64,
    blocked: HashSet<(u64, u64)>,
    /// Each index's answers, as the first member to apply it gave them.
    by_index: BTreeMap<u64, Vec<(u64, u64, Answer)>>,
    faults: bool,
    gateway: Gateway,
}

impl World {
    fn new(seed: u64, puts: usize) -> Self {
        let nodes = (1..=MEMBERS).map(|id| Node::new(id, seed)).collect();
        // The bucket's gate opens first, as the bucket's creation does
        // (docs/design/metadata.md §2); an empty key marks it.
        let todo = std::iter::once((String::new(), "gate".to_owned()))
            .chain((0..puts).map(|i| (format!("k{}", i % 5), format!("e{i}"))))
            .collect();
        Self {
            seed,
            rng: Rng(seed),
            nodes,
            wire: Vec::new(),
            step: 0,
            blocked: HashSet::new(),
            by_index: BTreeMap::new(),
            faults: true,
            gateway: Gateway {
                session: None,
                registering: None,
                next_serial: 1,
                outstanding: None,
                todo,
                done: Vec::new(),
                nonce: 0,
            },
        }
    }

    fn fail(&self, what: &str) -> ! {
        panic!("seed {} step {}: {what}", self.seed, self.step)
    }

    fn step(&mut self) {
        self.step += 1;
        if self.faults {
            self.inject();
        }
        for n in &mut self.nodes {
            if let Some(r) = n.replica.as_mut() {
                r.tick().unwrap_or_else(|e| panic!("tick: {e}"));
            }
        }
        self.deliver();
        self.drive();
        self.act();
    }

    fn inject(&mut self) {
        let i = self.rng.below(MEMBERS) as usize;
        let down = self.nodes.iter().filter(|n| n.replica.is_none()).count();
        if self.rng.chance(4) && down == 0 {
            self.nodes[i].crash();
        }
        if self.rng.chance(20) && self.nodes[i].replica.is_none() {
            let seed = self.seed;
            self.nodes[i].restart(seed);
        }
        if self.rng.chance(6) {
            // Cut one member off from the others, both ways.
            let id = self.nodes[i].id;
            for other in 1..=MEMBERS {
                if other != id {
                    self.blocked.insert((id, other));
                    self.blocked.insert((other, id));
                }
            }
        }
        if self.rng.chance(15) {
            self.blocked.clear();
        }
        if std::env::var("NO_COMPACT").is_err()
            && self.rng.chance(10)
            && let Some(r) = self.nodes[i].replica.as_mut()
        {
            let keep = self.rng.below(8);
            match r.compact(keep) {
                Ok(()) => {}
                Err(e) => self.fail(&format!("compact: {e}")),
            }
        }
    }

    fn deliver(&mut self) {
        let now = self.step;
        let (due, later): (Vec<_>, Vec<_>) = std::mem::take(&mut self.wire)
            .into_iter()
            .partition(|(at, _)| *at <= now);
        self.wire = later;
        for (_, m) in due {
            let (from, to) = (m.from, m.to);
            let snapshot = focal_raft_message_is_snapshot(&m);
            let dropped =
                self.blocked.contains(&(from, to)) || (self.faults && self.rng.chance(20));
            let arrived = !dropped
                && match self
                    .nodes
                    .iter_mut()
                    .find(|n| n.id == to)
                    .and_then(|n| n.replica.as_mut())
                {
                    Some(r) => match r.step(m) {
                        Ok(()) | Err(ReplicaError::Refused(_)) => true,
                        Err(e) => panic!("step: {e}"),
                    },
                    None => false,
                };
            // A snapshot's stream learns whether it arrived, and tells its sender.
            if snapshot
                && let Some(sender) = self
                    .nodes
                    .iter_mut()
                    .find(|n| n.id == from)
                    .and_then(|n| n.replica.as_mut())
            {
                match sender.report_snapshot(to, arrived) {
                    Ok(()) | Err(ReplicaError::Refused(_)) => {}
                    Err(e) => panic!("report: {e}"),
                }
            }
        }
    }

    fn drive(&mut self) {
        let mut sent = Vec::new();
        let mut applied = Vec::new();
        for n in &mut self.nodes {
            let Some(r) = n.replica.as_mut() else {
                continue;
            };
            let out = r
                .drive()
                .unwrap_or_else(|e| panic!("drive on {}: {e}", n.id));
            sent.extend(out.messages);
            applied.extend(out.applied);
        }
        for m in sent {
            let delay = 1 + self.rng.below(if self.faults { 6 } else { 2 });
            self.wire.push((self.step + delay, m));
        }
        for a in applied {
            match self.by_index.get(&a.index) {
                Some(first) if *first != a.answers => {
                    self.fail(&format!("index {} applied two ways", a.index));
                }
                Some(_) => {}
                None => {
                    self.by_index.insert(a.index, a.answers.clone());
                    self.hear(&a.answers);
                }
            }
        }
    }

    /// The gateway reads the answers to what it sent.
    fn hear(&mut self, answers: &[(u64, u64, Answer)]) {
        let g = &mut self.gateway;
        for (session, serial, answer) in answers {
            if let (Some((nonce, _)), Answer::Registered { session: s }) = (g.registering, answer)
                && *session == 0
                && *serial == nonce
            {
                g.session = Some(*s);
                g.registering = None;
            }
            if let Some(op) = &g.outstanding
                && Some(*session) == g.session
                && *serial == op.serial
            {
                g.done
                    .push((op.key.clone(), op.etag.clone(), answer.clone()));
                g.outstanding = None;
            }
        }
    }

    fn leader(&mut self) -> Option<&mut Member> {
        self.nodes
            .iter_mut()
            .filter_map(|n| n.replica.as_mut())
            .find(|r| r.is_leader())
    }

    fn act(&mut self) {
        let now = self.step;
        let at_ns = now * 1_000_000;
        let g = &mut self.gateway;
        let command = if g.session.is_none() {
            match g.registering {
                Some((_, sent)) if now - sent < PATIENCE => return,
                _ => {
                    g.nonce += 1;
                    g.registering = Some((g.nonce, now));
                    Sessioned {
                        session: 0,
                        serial: g.nonce,
                        unanswered: 0,
                        command: Command::Register,
                    }
                }
            }
        } else {
            let session = g.session.unwrap_or(0);
            let op = match &mut g.outstanding {
                Some(op) if now - op.sent_at < PATIENCE => return,
                Some(op) => {
                    op.sent_at = now;
                    op.clone()
                }
                None => {
                    let Some((key, etag)) = g.todo.pop_front() else {
                        return;
                    };
                    let op = Op {
                        serial: g.next_serial,
                        key,
                        etag,
                        sent_at: now,
                    };
                    g.next_serial += 1;
                    g.outstanding = Some(op.clone());
                    op
                }
            };
            let command = if op.key.is_empty() {
                open_gate()
            } else {
                put(&op.key, &op.etag)
            };
            Sessioned {
                session,
                serial: op.serial,
                unanswered: op.serial,
                command,
            }
        };
        let entry = Entry {
            at_ns,
            commands: vec![command],
        };
        if let Some(leader) = self.leader() {
            match leader.propose(&entry) {
                Ok(()) | Err(ReplicaError::Refused(_)) => {}
                Err(e) => panic!("propose: {e}"),
            }
        }
    }

    fn heal(&mut self) {
        self.faults = false;
        self.blocked.clear();
        let seed = self.seed;
        for n in &mut self.nodes {
            n.restart(seed);
        }
    }

    fn settled(&self) -> bool {
        let g = &self.gateway;
        if g.outstanding.is_some() || !g.todo.is_empty() || g.session.is_none() {
            return false;
        }
        let applied: Vec<u64> = self
            .nodes
            .iter()
            .filter_map(|n| n.replica.as_ref().map(Replica::applied))
            .collect();
        applied.len() == MEMBERS as usize && applied.windows(2).all(|w| w[0] == w[1])
    }
}

fn focal_raft_message_is_snapshot(m: &Message) -> bool {
    m.msg_type == mantle_range::MessageType::MsgSnapshot as i32
}

fn put(key: &str, etag: &str) -> Command {
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
            etag: etag.into(),
            size: 1,
            checksum: None,
            file: Some(1),
            owner: "o".into(),
            headers: Vec::new(),
        },
    })))
}

/// Every row of an engine.
fn rows(m: &Model) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut at = Vec::new();
    while let Some((k, v)) = m.next(&at, &[0xFF]).unwrap() {
        at = k.clone();
        at.push(0);
        out.push((k, v));
    }
    out
}

/// The ETags of every version of `key`, newest first.
fn versions(m: &Model, key: &str) -> Vec<String> {
    let mut out = Vec::new();
    let from = mantle_meta::key::name("b", key, &mantle_meta::key::NameRow::Version(0));
    let to = mantle_meta::key::name("b", key, &mantle_meta::key::NameRow::Upload(Vec::new()));
    let mut at = from;
    while let Some((k, v)) = m.next(&at, &to).unwrap() {
        out.push(Version::decode(&v).unwrap().etag);
        at = k;
        at.push(0);
    }
    out
}

fn run(seed: u64) {
    let mut w = World::new(seed, 30);
    for _ in 0..3_000 {
        w.step();
    }
    w.heal();
    let mut budget = 20_000;
    while !w.settled() {
        budget -= 1;
        if budget == 0 {
            let members: Vec<_> = w
                .nodes
                .iter()
                .map(|n| {
                    n.replica
                        .as_ref()
                        .map(|r| (r.id(), r.is_leader(), r.leader(), r.term(), r.applied()))
                })
                .collect();
            for n in &w.nodes {
                if let Some(r) = n.replica.as_ref() {
                    eprintln!("{}", r.describe());
                }
            }
            let g = &w.gateway;
            w.fail(&format!(
                "never settled once faults stopped: members {members:?}, session {:?}, \
                 registering {:?}, outstanding {:?}, todo {}, done {}, in flight {}",
                g.session,
                g.registering,
                g.outstanding,
                g.todo.len(),
                g.done.len(),
                w.wire.len()
            ));
        }
        w.step();
    }
    // Every answered put exists exactly once on every member, and the members agree.
    let engines: Vec<&Model> = w
        .nodes
        .iter()
        .map(|n| n.replica.as_ref().unwrap().engine())
        .collect();
    let first = rows(engines[0]);
    for e in &engines[1..] {
        if rows(e) != first {
            w.fail("members hold different rows");
        }
    }
    let mut answered: HashMap<String, Vec<String>> = HashMap::new();
    for (key, etag, answer) in &w.gateway.done {
        match answer {
            Answer::Name(name::Outcome::GateMoved) if key.is_empty() => {}
            Answer::Name(name::Outcome::Put { .. }) => {
                answered.entry(key.clone()).or_default().push(etag.clone());
            }
            other => w.fail(&format!("put {key} {etag} answered {other:?}")),
        }
    }
    for (key, etags) in &answered {
        let held = versions(engines[0], key);
        for etag in etags {
            let copies = held.iter().filter(|e| *e == etag).count();
            if copies != 1 {
                w.fail(&format!("{key} {etag} held {copies} times"));
            }
        }
    }
    assert_eq!(w.gateway.done.len(), 31, "seed {seed}");
}

#[test]
fn a_group_under_faults_applies_every_put_once_and_agrees() {
    let seeds: u64 = std::env::var("MANTLE_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(24);
    let first: u64 = std::env::var("MANTLE_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    for seed in first..first + seeds {
        run(seed);
    }
}

/// The gate a put needs, opened by the gateway's first command.
fn open_gate() -> Command {
    Command::Name(Box::new(name::Command::Gate(GateChange {
        bucket: "b".into(),
        incarnation: 1,
        attempt: 1,
        from: None,
        to: Some(GateState::Open),
    })))
}
