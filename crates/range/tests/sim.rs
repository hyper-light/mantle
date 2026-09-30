//! Deterministic simulation of one range's group (docs/design/replica.md §5).
//!
//! Three replicas, each with its log on a simulated device and a model engine, run over a
//! simulated network that delays, drops and partitions messages. Nodes crash, losing what
//! neither their log nor their engine had made durable, and restart from what was; a
//! device's write or flush fails, which fences the node's log and takes the node down until
//! it restarts from what the device kept. Once or twice a run a member is lost for good, its
//! device and all, and a member with a new identity replaces it: added as a learner, caught
//! up, then swapped in by one joint change (docs/design/replica.md §6). Three
//! gateways, each with its own session, put and get two keys at once: puts go through the
//! log, retried through leader changes with the session's serial numbers, and gets through
//! the leader's ReadIndex. A run is its seed.
//!
//! After every run:
//! - every index was applied with the same answers on every member that applied it;
//! - once faults stop, every operation completes;
//! - every put a gateway was answered exists exactly once on every member, however often it
//!   was retried;
//! - every member holds the same rows;
//! - each key's history, as the gateways saw it, is linearizable (docs/research/06 §A6.8);
//! - every member's configuration names the live members as its voters, and nothing else.
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

mod support;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use mantle_disk::buf::Alignment;
use mantle_disk::sim::{Crash, Fault, SimFile};
use mantle_log::{Config as LogConfig, Log};
use mantle_meta::apply::Layer;
use mantle_meta::engine::{Engine, Model, Rows};
use mantle_meta::name::{self, GateChange, Preconditions, Put};
use mantle_meta::record::{GateState, Version, Versioning};
use mantle_meta::session::Rules;
use mantle_meta::wire::{Answer, Command, Entry, Sessioned};
use mantle_range::membership::{Next, Replacement};
use mantle_range::{ConfState, Message, Range, Replica, ReplicaError, Settings};
use support::linear::{self, Input, Operation, Output, Register, Verdict};

const GROUP: u128 = 0x0072_616e_6765;
/// Members the group starts with, and keeps: a lost member is replaced.
const MEMBERS: u64 = 3;
/// Members each run loses for good, one after another.
const LOSSES: u64 = 2;

fn log_config() -> LogConfig {
    LogConfig {
        segment_bytes: 16 * 4096,
        max_segments: 64,
        max_groups: 4,
        group_entries: 1 << 10,
        group_bytes: 4 << 10,
        group_cache: 1 << 12,
        queue_submissions: 16,
        queue_bytes: 1 << 20,
    }
}

/// Bounds small enough that a group's retained entries meet the log's bound between
/// compactions, so readies wait for room and the node compacts to go on (audit S04): a ready
/// is 3 KiB at most, which the log's bounds on a group hold.
const SETTINGS: Settings = Settings {
    election_tick: 10,
    heartbeat_tick: 2,
    max_size_per_msg: 1 << 10,
    max_inflight_msgs: 2,
    max_uncommitted_size: 2 << 10,
    max_committed_size_per_ready: 1 << 20,
    max_entry_bytes: 1 << 10,
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
    lifetime_ns: u64::MAX / 2,
    max_sessions: 64,
    max_answers: 16,
    max_answer_bytes: usize::MAX,
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
        let replica = Replica::open(id, GROUP, log, first_range(), &range(), seed ^ id).unwrap();
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

/// What a gateway is doing.
#[derive(Debug, Clone)]
enum Doing {
    /// Waiting for its session.
    Registering {
        nonce: u64,
        sent: u64,
    },
    /// A command through the log: the gate's opening or a put.
    Writing {
        serial: u64,
        key: String,
        value: String,
        call: u64,
        sent: u64,
    },
    /// A get: its attempt, the member that confirmed it and the index to wait for, once one
    /// has.
    Reading {
        key: String,
        call: u64,
        sent: u64,
        attempt: u64,
        confirmed: Option<(u64, u64)>,
    },
    Idle,
}

struct Gateway {
    id: u64,
    session: Option<u64>,
    next_serial: u64,
    /// Operations left, the gate's opening first for the gateway that opens it.
    left: u64,
    doing: Doing,
    nonce: u64,
    ops: u64,
}

/// Steps without an answer before a gateway sends again.
const PATIENCE: u64 = 40;
const KEYS: [&str; 2] = ["k0", "k1"];
const OPS_EACH: u64 = 20;
const GATEWAYS: u64 = 3;

/// One operation as its gateway saw it, for the checker.
struct Seen {
    key: String,
    op: Operation<Input, Output>,
}

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
    gateways: Vec<Gateway>,
    /// Whether the bucket's gate has opened; the gateways but the first wait for it.
    gate_open: bool,
    history: Vec<Seen>,
    /// Puts answered, by key: their values.
    put: HashMap<String, Vec<String>>,
    /// The identity the next new member takes; none is ever reused.
    next_id: u64,
    /// The replacement under way, and the step its last change was proposed.
    replacing: Option<(Replacement, Option<u64>)>,
    /// Members lost for good so far, and the step at which the next is lost.
    lost: u64,
    lose_at: u64,
    /// Replacements finished.
    replaced: u64,
    /// Readies the log refused for want of room, each waited out by compacting.
    stalls: u64,
    /// Readies begun and left flushing past a step.
    flushing: u64,
}

impl World {
    fn new(seed: u64) -> Self {
        let mut rng = Rng(seed);
        let lose_at = 200 + rng.below(1_000);
        let nodes = (1..=MEMBERS).map(|id| Node::new(id, seed)).collect();
        let gateways = (0..GATEWAYS)
            .map(|id| Gateway {
                id,
                session: None,
                next_serial: 1,
                left: OPS_EACH + u64::from(id == 0),
                doing: Doing::Idle,
                nonce: 0,
                ops: 0,
            })
            .collect();
        Self {
            seed,
            rng,
            nodes,
            wire: Vec::new(),
            step: 0,
            blocked: HashSet::new(),
            by_index: BTreeMap::new(),
            faults: true,
            gateways,
            gate_open: false,
            history: Vec::new(),
            put: HashMap::new(),
            next_id: MEMBERS + 1,
            replacing: None,
            lost: 0,
            lose_at,
            replaced: 0,
            stalls: 0,
            flushing: 0,
        }
    }

    fn fail(&self, what: &str) -> ! {
        eprintln!(
            "members {:?}, replacing {:?}, lost {}, replaced {}",
            self.nodes
                .iter()
                .map(|n| (n.id, n.replica.is_some()))
                .collect::<Vec<_>>(),
            self.replacing,
            self.lost,
            self.replaced
        );
        for n in &self.nodes {
            if let Some(r) = n.replica.as_ref() {
                eprintln!("{} conf {:?}", r.describe(), r.configuration());
            }
        }
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
        self.replace();
        self.serve_reads();
        for g in 0..self.gateways.len() {
            self.act(g);
        }
    }

    fn inject(&mut self) {
        if self.replacing.is_none() && self.lost < LOSSES && self.step >= self.lose_at {
            self.lose();
        }
        let i = self.rng.below(self.nodes.len() as u64) as usize;
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
            for other in self.nodes.iter().map(|n| n.id).filter(|&o| o != id) {
                self.blocked.insert((id, other));
                self.blocked.insert((other, id));
            }
        }
        if self.rng.chance(15) {
            self.blocked.clear();
        }
        if self.rng.chance(2) && self.nodes[i].replica.is_some() {
            // The device fails its next write or flush, which fences the log.
            let fault = if self.rng.chance(500) {
                Fault::WriteError
            } else {
                Fault::SyncError
            };
            self.nodes[i].file.inject(fault).unwrap();
        }
        if self.rng.chance(10)
            && let Some(r) = self.nodes[i].replica.as_mut()
        {
            let keep = self.rng.below(8);
            match r.compact(keep) {
                Ok(()) => {}
                Err(ReplicaError::Log(_)) => self.nodes[i].crash(),
                Err(e) => self.fail(&format!("compact: {e}")),
            }
        }
    }

    /// Loses a member for good, its device and engine with it, and starts replacing it with a
    /// new member under an identity never used before.
    fn lose(&mut self) {
        let i = self.rng.below(self.nodes.len() as u64) as usize;
        let failed = self.nodes.remove(i).id;
        let joining = self.next_id;
        self.next_id += 1;
        self.nodes.push(Node::new(joining, self.seed));
        self.replacing = Some((Replacement::new(failed, joining).unwrap(), None));
        self.lost += 1;
        self.lose_at = self.step + 300 + self.rng.below(800);
    }

    /// Drives the replacement under way through whichever member leads: the change it asks
    /// for is proposed, and proposed again if a patience passes without it taking effect.
    fn replace(&mut self) {
        let Some((replacement, sent)) = self.replacing else {
            return;
        };
        let now = self.step;
        let Some(leader) = self.leader() else {
            return;
        };
        match replacement.next(
            leader.configuration(),
            leader.caught_up(replacement.joining()),
            leader.configuration_known(),
        ) {
            Next::Done => {
                self.replacing = None;
                self.replaced += 1;
            }
            Next::Wait => {}
            Next::Propose(change) => {
                if sent.is_none_or(|at| now - at >= PATIENCE) {
                    match leader.propose_change(&change) {
                        Ok(()) | Err(ReplicaError::Refused(_) | ReplicaError::Stalled) => {}
                        Err(e) => panic!("propose a change: {e}"),
                    }
                    self.replacing = Some((replacement, Some(now)));
                }
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
            let snapshot = m.msg_type == mantle_range::MessageType::MsgSnapshot as i32;
            let dropped =
                self.blocked.contains(&(from, to)) || (self.faults && self.rng.chance(20));
            let receiver = self.nodes.iter_mut().find(|n| n.id == to);
            let arrived = !dropped
                && match receiver.and_then(|n| n.replica.as_mut()) {
                    Some(r) => match r.step(m) {
                        Ok(()) | Err(ReplicaError::Refused(_)) => true,
                        // A member waiting for room in its log takes no message.
                        Err(ReplicaError::Stalled) => false,
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
                    Ok(()) | Err(ReplicaError::Refused(_) | ReplicaError::Stalled) => {}
                    Err(e) => panic!("report: {e}"),
                }
            }
        }
    }

    fn drive(&mut self) {
        let mut sent = Vec::new();
        let mut applied = Vec::new();
        let mut reads = Vec::new();
        let mut stopped = None;
        let mut waiting = Vec::new();
        for n in &mut self.nodes {
            let Some(r) = n.replica.as_mut() else {
                continue;
            };
            // Under faults, a member half the time takes its ready without waiting for its log
            // and finishes it at a later step, messages to it meanwhile refused, as a node
            // overlapping its members' flushes does (audit §5.1).
            let out = if self.faults && self.rng.chance(500) {
                r.begin()
            } else {
                r.drive()
            };
            let out = match out {
                Ok(out) => out,
                // A fenced log takes its node down; it restarts from what its device kept.
                Err(ReplicaError::Log(_)) => {
                    n.crash();
                    continue;
                }
                Err(e) => {
                    stopped = Some(format!("drive on {}: {e}", n.id));
                    break;
                }
            };
            if out.stalled.is_some() {
                waiting.push(n.id);
            }
            if out.persisting {
                self.flushing += 1;
            }
            sent.extend(out.messages);
            applied.extend(out.applied);
            reads.extend(out.reads.into_iter().map(|(index, ctx)| (n.id, index, ctx)));
        }
        if let Some(what) = stopped {
            self.fail(&what);
        }
        // A ready waits for room in its member's log: the member compacts, and its next drive
        // writes the ready.
        for id in waiting {
            self.stalls += 1;
            let keep = self.rng.below(8);
            let Some(n) = self.nodes.iter_mut().find(|n| n.id == id) else {
                continue;
            };
            let Some(r) = n.replica.as_mut() else {
                continue;
            };
            match r.compact(keep) {
                Ok(()) => {}
                Err(ReplicaError::Log(_)) => n.crash(),
                Err(e) => self.fail(&format!("compact on {id}: {e}")),
            }
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
        for (node, index, ctx) in reads {
            let (Some(g), Some(attempt)) = (read_u64(&ctx, 0), read_u64(&ctx, 8)) else {
                continue;
            };
            if let Some(gw) = self.gateways.get_mut(g as usize)
                && let Doing::Reading {
                    attempt: current,
                    confirmed,
                    ..
                } = &mut gw.doing
                && *current == attempt
                && confirmed.is_none()
            {
                *confirmed = Some((node, index));
            }
        }
    }

    /// Answers each confirmed get once its member has applied up to the confirmed index.
    fn serve_reads(&mut self) {
        let now = self.step;
        for g in 0..self.gateways.len() {
            let Doing::Reading {
                key,
                call,
                confirmed: Some((node, index)),
                ..
            } = self.gateways[g].doing.clone()
            else {
                continue;
            };
            let Some(r) = self
                .nodes
                .iter()
                .find(|n| n.id == node)
                .and_then(|n| n.replica.as_ref())
            else {
                continue;
            };
            if r.applied() < index {
                continue;
            }
            let seen = name::current(r.engine(), "b", &key)
                .unwrap()
                .map(|(_, v)| v.etag);
            self.history.push(Seen {
                key,
                op: Operation {
                    call,
                    ret: Some(now),
                    input: Input::Get,
                    output: Some(Output::Got(seen)),
                },
            });
            self.gateways[g].doing = Doing::Idle;
        }
    }

    /// The gateways read the answers to what they sent.
    fn hear(&mut self, answers: &[(u64, u64, Answer)]) {
        let now = self.step;
        for (session, serial, answer) in answers {
            for g in &mut self.gateways {
                match &g.doing {
                    Doing::Registering { nonce, .. } if *session == 0 && serial == nonce => {
                        if let Answer::Registered { session: s } = answer {
                            g.session = Some(*s);
                            g.doing = Doing::Idle;
                        }
                    }
                    Doing::Writing {
                        serial: sent,
                        key,
                        value,
                        call,
                        ..
                    } if Some(*session) == g.session && serial == sent => {
                        if key.is_empty() {
                            if *answer == Answer::Name(name::Outcome::GateMoved) {
                                self.gate_open = true;
                            } else {
                                panic!("the gate answered {answer:?}");
                            }
                        } else {
                            match answer {
                                Answer::Name(name::Outcome::Put { .. }) => {
                                    self.history.push(Seen {
                                        key: key.clone(),
                                        op: Operation {
                                            call: *call,
                                            ret: Some(now),
                                            input: Input::Put(value.clone()),
                                            output: Some(Output::Put),
                                        },
                                    });
                                    self.put.entry(key.clone()).or_default().push(value.clone());
                                }
                                other => panic!("put {key} {value} answered {other:?}"),
                            }
                        }
                        g.doing = Doing::Idle;
                    }
                    _ => {}
                }
            }
        }
    }

    fn leader(&mut self) -> Option<&mut Member> {
        self.nodes
            .iter_mut()
            .filter_map(|n| n.replica.as_mut())
            .find(|r| r.is_leader())
    }

    fn act(&mut self, g: usize) {
        let now = self.step;
        let at_ns = now * 1_000_000;
        let gate_open = self.gate_open;
        let rng = self.rng.next();
        let gw = &mut self.gateways[g];
        // What to send: an entry, or a read to confirm.
        let mut entry: Option<Sessioned> = None;
        let mut read: Option<Vec<u8>> = None;
        match &mut gw.doing {
            Doing::Registering { sent, .. } if now - *sent < PATIENCE => return,
            Doing::Registering { .. } => gw.doing = Doing::Idle,
            Doing::Writing { sent, .. } if now - *sent < PATIENCE => return,
            Doing::Writing {
                serial,
                key,
                value,
                sent,
                ..
            } => {
                *sent = now;
                entry = Some(write(gw.session, *serial, key, value));
            }
            Doing::Reading { sent, .. } if now - *sent < PATIENCE => return,
            Doing::Reading {
                sent,
                attempt,
                confirmed,
                ..
            } => {
                // No answer in time: confirm the read again, as the same operation.
                *sent = now;
                *attempt += 1;
                *confirmed = None;
                read = Some(context(gw.id, *attempt));
            }
            Doing::Idle => {}
        }
        if matches!(gw.doing, Doing::Idle) {
            if gw.session.is_none() {
                // Registrations are told apart by a nonce no other gateway uses.
                gw.nonce = (gw.id << 32) | ((gw.nonce & 0xFFFF_FFFF) + 1);
                gw.doing = Doing::Registering {
                    nonce: gw.nonce,
                    sent: now,
                };
                entry = Some(Sessioned {
                    session: 0,
                    serial: gw.nonce,
                    unanswered: 0,
                    command: Command::Register,
                });
            } else if gw.left > 0 && (gate_open || gw.id == 0) {
                gw.left -= 1;
                gw.ops += 1;
                let key = KEYS[(rng % KEYS.len() as u64) as usize].to_owned();
                if gw.id == 0 && !gate_open {
                    // The gate opens before any put.
                    let serial = gw.next_serial;
                    gw.next_serial += 1;
                    gw.doing = Doing::Writing {
                        serial,
                        key: String::new(),
                        value: "gate".into(),
                        call: now,
                        sent: now,
                    };
                    entry = Some(write(gw.session, serial, "", "gate"));
                } else if rng & (1 << 20) == 0 {
                    let serial = gw.next_serial;
                    gw.next_serial += 1;
                    let value = format!("g{}o{}", gw.id, gw.ops);
                    entry = Some(write(gw.session, serial, &key, &value));
                    gw.doing = Doing::Writing {
                        serial,
                        key,
                        value,
                        call: now,
                        sent: now,
                    };
                } else {
                    gw.doing = Doing::Reading {
                        key,
                        call: now,
                        sent: now,
                        attempt: 0,
                        confirmed: None,
                    };
                    read = Some(context(gw.id, 0));
                }
            }
        }
        if let Some(command) = entry {
            let entry = Entry {
                at_ns,
                commands: vec![command],
            };
            if let Some(leader) = self.leader() {
                match leader.propose(&entry) {
                    Ok(()) | Err(ReplicaError::Refused(_) | ReplicaError::Stalled) => {}
                    Err(e) => panic!("propose: {e}"),
                }
            }
        }
        if let Some(context) = read
            && let Some(leader) = self.leader()
        {
            match leader.read_index(context) {
                Ok(()) | Err(ReplicaError::Refused(_) | ReplicaError::Stalled) => {}
                Err(e) => panic!("read: {e}"),
            }
        }
    }

    fn heal(&mut self) {
        self.faults = false;
        self.blocked.clear();
        let seed = self.seed;
        for n in &mut self.nodes {
            // A fault armed on a running node would still fire after the faults stop.
            n.file.clear_faults().unwrap();
            n.restart(seed);
        }
    }

    fn settled(&self) -> bool {
        let busy = self
            .gateways
            .iter()
            .any(|g| g.left > 0 || !matches!(g.doing, Doing::Idle) || g.session.is_none());
        if busy || self.replacing.is_some() {
            return false;
        }
        let applied: Vec<u64> = self
            .nodes
            .iter()
            .filter_map(|n| n.replica.as_ref().map(Replica::applied))
            .collect();
        applied.len() == self.nodes.len()
            && applied.windows(2).all(|w| w[0] == w[1])
            && self.nodes.iter().all(|n| {
                n.replica
                    .as_ref()
                    .is_some_and(|r| final_configuration(r.configuration(), &self.live()))
            })
    }

    /// The live members, in order.
    fn live(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.nodes.iter().map(|n| n.id).collect();
        ids.sort_unstable();
        ids
    }
}

/// Whether `conf` is a settled configuration whose voters are `live` and nothing else.
fn final_configuration(conf: &ConfState, live: &[u64]) -> bool {
    let mut voters = conf.voters.clone();
    voters.sort_unstable();
    voters == live
        && conf.learners.is_empty()
        && conf.voters_outgoing.is_empty()
        && conf.learners_next.is_empty()
}

/// The context a gateway's read carries: the gateway and the read's attempt.
fn context(gateway: u64, attempt: u64) -> Vec<u8> {
    let mut ctx = gateway.to_le_bytes().to_vec();
    ctx.extend_from_slice(&attempt.to_le_bytes());
    ctx
}

fn read_u64(bytes: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

/// A gateway's write: the gate's opening for an empty key, a put otherwise.
fn write(session: Option<u64>, serial: u64, key: &str, value: &str) -> Sessioned {
    let command = if key.is_empty() {
        open_gate()
    } else {
        put(key, value)
    };
    Sessioned {
        session: session.unwrap_or(0),
        serial,
        unanswered: serial,
        command,
    }
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
            retention: None,
            legal_hold: None,
        },
        default: None,
        deadline_ns: u64::MAX,
    })))
}

/// The gate a put needs, opened by the first gateway's first command.
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

/// Every row of an engine that its range replicates.
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

/// One run: the replacements it finished.
fn run(seed: u64) -> (u64, u64, u64) {
    let mut w = World::new(seed);
    for _ in 0..3_000 {
        w.step();
    }
    w.heal();
    let mut budget = 20_000;
    while !w.settled() {
        budget -= 1;
        if budget == 0 {
            w.fail("never settled once faults stopped");
        }
        w.step();
    }
    let engines: Vec<&Model> = w
        .nodes
        .iter()
        .map(|n| n.replica.as_ref().unwrap().engine())
        .collect();
    let first = rows(engines[0]);
    if engines[1..].iter().any(|e| rows(e) != first) {
        w.fail("members hold different rows");
    }
    let live = w.live();
    if live.len() != MEMBERS as usize
        || w.nodes
            .iter()
            .any(|n| !final_configuration(n.replica.as_ref().unwrap().configuration(), &live))
    {
        w.fail("the members' configurations are not the live members");
    }
    for (key, values) in &w.put {
        let held = versions(engines[0], key);
        for value in values {
            let copies = held.iter().filter(|v| *v == value).count();
            if copies != 1 {
                w.fail(&format!("{key} {value} held {copies} times"));
            }
        }
    }
    for key in KEYS {
        let ops: Vec<_> = w
            .history
            .iter()
            .filter(|s| s.key == key)
            .map(|s| s.op.clone())
            .collect();
        match linear::check(&Register, &ops, 10_000_000) {
            Verdict::Linearizable => {}
            other => w.fail(&format!("{key}: {other:?} over {} operations", ops.len())),
        }
    }
    (w.replaced, w.stalls, w.flushing)
}

#[test]
fn a_group_under_faults_is_linearizable_and_applies_every_put_once() {
    let seeds: u64 = std::env::var("MANTLE_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(24);
    let first: u64 = std::env::var("MANTLE_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let mut replaced = 0;
    let mut stalls = 0;
    let mut flushing = 0;
    for seed in first..first + seeds {
        // A member is lost within the faults of every run, and the run settles only once its
        // replacement finishes.
        let (run_replaced, run_stalls, run_flushing) = run(seed);
        assert!(run_replaced >= 1, "seed {seed} replaced no member");
        replaced += run_replaced;
        stalls += run_stalls;
        flushing += run_flushing;
    }
    // Readies waited for room and the members went on: the path audit S04 found stranded.
    assert!(stalls > 0, "no ready ever waited for room");
    // Readies were left flushing across steps, their members refusing messages meanwhile.
    assert!(flushing > 0, "no ready was begun and left flushing");
    eprintln!(
        "{seeds} runs replaced {replaced} members lost for good; {stalls} readies waited for \
         room; {flushing} were left flushing across a step"
    );
}
