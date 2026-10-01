//! Deterministic simulation of one range's group (docs/design/replica.md §5).
//!
//! Three or five replicas, odd seeds three and even seeds five, each with its log on a
//! simulated device and a model engine, run over a simulated network that delays, drops,
//! duplicates and partitions messages; with five, two may be down at once. Nodes crash, losing what
//! neither their log nor their engine had made durable, and restart from what was; a
//! device's write or flush fails, which fences the node's log and takes the node down until
//! it restarts from what the device kept. Once or twice a run a member is lost for good, its
//! device and all, and a member with a new identity replaces it: added as a learner, caught
//! up, then swapped in by one joint change (docs/design/replica.md §6). A down member's
//! device is damaged at rest now and then, a bit of one of its log's frames flipped on the
//! medium: its last frame, which its restart restores and marks, and the member is repaired
//! in place from its peers; its last frame holding a fast-track proposal, which leaves its
//! group damaged, and the member is rebuilt under a new identity on its device; or an earlier
//! frame, which leaves its whole log damaged, and the device is replaced (docs/design/replica.md
//! §4). Three
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

use mantle_disk::block::BlockFile;
use mantle_disk::buf::{AlignedBuf, Alignment};
use mantle_disk::sim::{Crash, Fault, SimFile};
use mantle_log::{Config as LogConfig, Log, LogError, Waits};
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
        // Each member has one update out at a time, so none returns within a wait.
        waits: Waits::Never,
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

/// A range whose group starts with `members` voters, and keeps that many: a lost member is
/// replaced.
fn range(members: u64) -> Range {
    Range {
        layer: Layer::Name,
        rules: RULES,
        boot: ConfState {
            voters: (1..=members).collect(),
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
    /// The identity of its log, which a member rebuilt on the same device keeps.
    log_id: u128,
    file: Arc<SimFile>,
    /// `None` while the node is down; its engine is kept as the crash left it.
    replica: Option<Member>,
    engine: Option<Model>,
    /// Restarts so far, which seed its next core.
    lives: u64,
    /// Voters the group starts with.
    members: u64,
    /// How far its clock runs ahead of the simulation's, in steps, or behind.
    skew: i64,
    /// Its device was damaged at rest since it went down.
    latent: bool,
    /// What its last restart found damaged: the member is quarantined, down until the group
    /// rebuilds it.
    found: Option<Found>,
    /// Whether its log may lack entries it acknowledged, as last seen.
    uncertain: bool,
}

/// Damage a restart found.
enum Found {
    /// The group's records on the log, or entries its engine applied beyond the term it can
    /// know: the member is rebuilt under a new identity on the same log. The node keeps a
    /// handle beside the one `Replica::open` consumes, since open may answer `Damaged` only
    /// after taking it.
    Group(Arc<Log<Arc<SimFile>>>),
    /// The log as a whole: the device is replaced.
    Log,
}

/// What restarts found of damage at rest.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Damage {
    /// Frames damaged at rest, on members down.
    injected: u64,
    /// Restarts whose member opened marked, and marks that ended as the group repaired it.
    marked: u64,
    unmarked: u64,
    /// Members rebuilt under a new identity on their device, and devices replaced.
    rebuilt: u64,
    replaced: u64,
    /// Restarts whose engine had applied entries the damaged log lost: kept in place, the
    /// applied entry's term known, or rebuilt.
    ahead_kept: u64,
    ahead_rebuilt: u64,
}

fn log_id(id: u64) -> u128 {
    0x6c6f_6700_0000 + u128::from(id)
}

impl Node {
    fn new(id: u64, seed: u64, members: u64) -> Self {
        let file = Arc::new(
            SimFile::new(
                Alignment::new(4096).unwrap(),
                Alignment::new(512).unwrap(),
                seed ^ (id << 32),
            )
            .unwrap(),
        );
        let log = Arc::new(Log::create(Arc::clone(&file), log_config(), log_id(id)).unwrap());
        let replica =
            Replica::open(id, GROUP, log, first_range(), &range(members), seed ^ id).unwrap();
        Self {
            id,
            log_id: log_id(id),
            file,
            replica: Some(replica),
            engine: None,
            lives: 0,
            members,
            skew: 0,
            latent: false,
            found: None,
            uncertain: false,
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

    /// Restarts from what the device and engine kept. Damage the restart finds quarantines
    /// the member: it stays down until the group rebuilds it (`World::rebuild`).
    fn restart(&mut self, seed: u64, damage: &mut Damage) {
        let Some(engine) = self.engine.take() else {
            return;
        };
        let latent = std::mem::take(&mut self.latent);
        self.lives += 1;
        self.file.clear_faults().unwrap();
        let (log, recovery) = match Log::open(Arc::clone(&self.file), log_config(), self.log_id) {
            Ok(opened) => opened,
            Err(LogError::Damaged(_)) if latent => {
                self.found = Some(Found::Log);
                return;
            }
            Err(e) => panic!("member {} reopens its log: {e}", self.id),
        };
        let log = Arc::new(log);
        if !recovery.damaged.is_empty() {
            assert!(latent && recovery.damaged == [GROUP], "{recovery:?}");
            self.found = Some(Found::Group(log));
            return;
        }
        let ahead = log
            .view(GROUP)
            .unwrap()
            .is_some_and(|v| engine.applied() > v.last);
        match Replica::open(
            self.id,
            GROUP,
            Arc::clone(&log),
            engine,
            &range(self.members),
            seed ^ self.id ^ (self.lives << 40),
        ) {
            Ok(mut replica) => {
                damage.ahead_kept += u64::from(ahead);
                // A mark lasts across restarts until the group repairs the member.
                let before = self.uncertain;
                self.uncertain = replica.is_uncertain().unwrap();
                if self.uncertain && !before {
                    assert!(latent, "member {} marked with no damage", self.id);
                    damage.marked += 1;
                }
                // The log reached its mark before the member went down: a drive that wrote the
                // entries back, then met a failed flush.
                if before && !self.uncertain {
                    damage.unmarked += 1;
                }
                self.replica = Some(replica);
            }
            Err(ReplicaError::Damaged) if latent && ahead => {
                damage.ahead_rebuilt += 1;
                self.found = Some(Found::Group(log));
            }
            Err(e) => panic!("member {} reopens: {e}", self.id),
        }
    }

    /// Damages a frame of the member's log at rest, the device down: a bit flipped on the
    /// medium, which no clearing of faults heals. `kind` picks the frame: 0 its last, 1 one
    /// before it, 2 its last after it takes a fast-track proposal of its own, which a persist
    /// record does not carry.
    fn damage(&mut self, kind: u64, rng: &mut Rng) {
        self.file.clear_faults().unwrap();
        if kind == 2 {
            let (log, recovery) =
                Log::open(Arc::clone(&self.file), log_config(), self.log_id).unwrap();
            assert!(recovery.damaged.is_empty());
            if let Some(view) = log.view(GROUP).unwrap() {
                let term = view.hard_state.map_or(1, |h| h.term.max(1));
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
        }
        let image = self.file.durable_image().unwrap();
        let mut frames: Vec<(u64, usize, usize)> = image
            .chunks(4096)
            .enumerate()
            .filter_map(|(i, block)| {
                let h = mantle_log::format::FrameHeader::decode(block)?;
                let len = h.frame_len()?;
                let frame = image.get(i * 4096..i * 4096 + len)?;
                (h.log == self.log_id && h.verifies(frame)).then(|| (h.sequence, i * 4096, len))
            })
            .collect();
        frames.sort_unstable();
        let Some(&last) = frames.last() else {
            return;
        };
        let (_, at, len) = if kind == 1 && frames.len() > 1 {
            frames[rng.below(frames.len() as u64 - 1) as usize]
        } else {
            last
        };
        // Within the frame's whole blocks, which an aligned read reaches.
        let whole = (image.len() / 4096 * 4096).saturating_sub(at).min(len);
        if whole == 0 {
            return;
        }
        let offset = (at + rng.below(whole as u64) as usize) as u64;
        let block = offset / 4096 * 4096;
        let mut buf = AlignedBuf::zeroed(4096, Alignment::new(4096).unwrap()).unwrap();
        buf.set_len(4096).unwrap();
        self.file.read_exact_at(buf.as_mut_slice(), block).unwrap();
        buf.as_mut_slice()[(offset - block) as usize] ^= 1 << rng.below(8);
        self.file.write_all_at(buf.as_slice(), block).unwrap();
        self.file.sync_data().unwrap();
        self.latent = true;
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
    /// Voters the group keeps.
    members: u64,
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
    /// Steps at which two members were down at once, and messages sent twice.
    two_down: u64,
    duplicated: u64,
    /// Clocks stepped, forward or back.
    stepped: u64,
    /// Messages and ticks a member held while its ready flushed, and messages it refused
    /// for want of room to hold them.
    held: u64,
    held_ticks: u64,
    refused: u64,
    damage: Damage,
}

/// What a run exercised, and what its gateways saw at which steps.
#[derive(Debug, PartialEq, Eq)]
struct Ran {
    replaced: u64,
    stalls: u64,
    flushing: u64,
    two_down: u64,
    duplicated: u64,
    stepped: u64,
    held: u64,
    held_ticks: u64,
    refused: u64,
    damage: Damage,
    steps: u64,
    history: Vec<String>,
}

impl World {
    fn new(seed: u64, members: u64) -> Self {
        let mut rng = Rng(seed);
        let lose_at = 200 + rng.below(1_000);
        let nodes = (1..=members)
            .map(|id| Node::new(id, seed, members))
            .collect();
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
            members,
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
            next_id: members + 1,
            replacing: None,
            lost: 0,
            lose_at,
            replaced: 0,
            stalls: 0,
            flushing: 0,
            two_down: 0,
            duplicated: 0,
            stepped: 0,
            held: 0,
            held_ticks: 0,
            refused: 0,
            damage: Damage::default(),
        }
    }

    fn fail(&self, what: &str) -> ! {
        eprintln!(
            "members {:?}, replacing {:?}, lost {}, replaced {}",
            self.nodes
                .iter()
                .map(|n| (n.id, n.replica.is_some(), n.found.is_some(), n.uncertain))
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
                if r.persisting() {
                    self.held_ticks += 1;
                }
                r.tick().unwrap_or_else(|e| panic!("tick: {e}"));
            }
        }
        self.deliver();
        self.drive();
        self.rebuild();
        self.replace();
        self.serve_reads();
        for g in 0..self.gateways.len() {
            self.act(g);
        }
    }

    /// Whether a member's data may be short of what it acknowledged, or is to be rebuilt:
    /// damaged at rest and not yet restarted, marked, quarantined, or being replaced. The
    /// simulation damages or loses one member's data at a time, the most a group of three
    /// survives: two members short of an entry both acknowledged may leave its only copy lost
    /// (AGL+18 §3.2).
    fn repairing(&self) -> bool {
        self.replacing.is_some()
            || self
                .nodes
                .iter()
                .any(|n| n.latent || n.uncertain || n.found.is_some())
    }

    fn inject(&mut self) {
        if !self.repairing() && self.lost < LOSSES && self.step >= self.lose_at {
            self.lose();
        }
        let i = self.rng.below(self.nodes.len() as u64) as usize;
        // As many members down at once as leave a quorum: one of three, two of five.
        let down = self.nodes.iter().filter(|n| n.replica.is_none()).count() as u64;
        if self.rng.chance(4) && down < (self.members - 1) / 2 {
            self.nodes[i].crash();
        }
        if self.nodes.iter().filter(|n| n.replica.is_none()).count() >= 2 {
            self.two_down += 1;
        }
        if self.rng.chance(3)
            && !self.repairing()
            && self.nodes[i].replica.is_none()
            && self.nodes[i].engine.is_some()
        {
            let kind = self.rng.below(3);
            self.nodes[i].damage(kind, &mut self.rng);
            self.damage.injected += 1;
        }
        if self.rng.chance(20) && self.nodes[i].replica.is_none() {
            let seed = self.seed;
            self.nodes[i].restart(seed, &mut self.damage);
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
        if self.rng.chance(5) {
            // A member's clock steps, forward or back, by up to two seconds: a leader stamps
            // the entries it proposes with its own time (docs/design/replica.md §1).
            self.nodes[i].skew = i64::try_from(self.rng.below(4_001)).unwrap() - 2_000;
            self.stepped += 1;
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
                Err(ReplicaError::Fenced(_) | ReplicaError::Log(_)) => self.nodes[i].crash(),
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
        self.nodes.push(Node::new(joining, self.seed, self.members));
        self.replacing = Some((Replacement::new(failed, joining).unwrap(), None));
        self.lost += 1;
        self.lose_at = self.step + 300 + self.rng.below(800);
    }

    /// Rebuilds a quarantined member once no replacement is under way: under a new identity,
    /// on its device where the damage was its group's, on a new device where it was its whole
    /// log's. The group's leader then replaces the damaged member with it.
    fn rebuild(&mut self) {
        if self.replacing.is_some() {
            return;
        }
        let Some(i) = self.nodes.iter().position(|n| n.found.is_some()) else {
            return;
        };
        let failed = self.nodes[i].id;
        let joining = self.next_id;
        self.next_id += 1;
        let replacement = match self.nodes[i].found.take() {
            Some(Found::Group(log)) => {
                let (replica, replacement) = Replica::rebuild(
                    failed,
                    joining,
                    GROUP,
                    log,
                    first_range(),
                    &range(self.members),
                    self.seed ^ joining,
                )
                .unwrap_or_else(|e| self.fail(&format!("rebuild {failed}: {e}")));
                let n = &mut self.nodes[i];
                n.id = joining;
                n.replica = Some(replica);
                n.lives = 0;
                n.uncertain = false;
                self.damage.rebuilt += 1;
                replacement
            }
            Some(Found::Log) => {
                self.nodes[i] = Node::new(joining, self.seed, self.members);
                self.damage.replaced += 1;
                Replacement::new(failed, joining).unwrap()
            }
            None => return,
        };
        self.replacing = Some((replacement, None));
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
                    Some(r) => match (r.persisting(), r.step(m)) {
                        // A member whose ready flushes holds the message until it is done.
                        (true, Ok(())) => {
                            self.held += 1;
                            true
                        }
                        (_, Ok(()) | Err(ReplicaError::Refused(_))) => true,
                        // Past its bound on held messages a member refuses one, as the network
                        // may drop it.
                        (_, Err(ReplicaError::MessagesHeld { .. })) => {
                            self.refused += 1;
                            false
                        }
                        // A member waiting for room in its log takes no message.
                        (_, Err(ReplicaError::Stalled)) => false,
                        (_, Err(e)) => panic!("step: {e}"),
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
            // A member half the time takes its ready without waiting for its log and finishes
            // it at a later step, holding the ticks and messages that come meanwhile, as a node
            // overlapping its members' flushes does (audit §5.1). It does so after faults stop
            // too, so a group whose messages come while its readies flush must still finish.
            // The flush is waited for before the step ends, so what the next step finds does
            // not hang on the log's thread: every run is its seed.
            let out = if self.rng.chance(500) {
                let out = r.begin();
                r.wait_persisted();
                out
            } else {
                r.drive()
            };
            let out = match out {
                Ok(out) => out,
                // A fenced member takes its node down; it restarts from what its device kept.
                // A ready begun under faults can find its log fenced a step after they stop,
                // and its node restarts at once, as the heal would have restarted it.
                Err(ReplicaError::Fenced(_)) => {
                    n.crash();
                    if !self.faults {
                        n.restart(self.seed, &mut self.damage);
                    }
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
            // A mark ends as the group repairs the member: its log holds the entries again.
            if n.uncertain && !r.is_uncertain().unwrap() {
                n.uncertain = false;
                self.damage.unmarked += 1;
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
                Err(ReplicaError::Fenced(_) | ReplicaError::Log(_)) => {
                    n.crash();
                    if !self.faults {
                        n.restart(self.seed, &mut self.damage);
                    }
                }
                Err(e) => self.fail(&format!("compact on {id}: {e}")),
            }
        }
        for m in sent {
            let delay = 1 + self.rng.below(if self.faults { 6 } else { 2 });
            // Under faults a message now and then arrives twice, the copy at another time.
            if self.faults && self.rng.chance(50) {
                let again = 1 + self.rng.below(12);
                self.wire.push((self.step + again, m.clone()));
                self.duplicated += 1;
            }
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
        // The leader's time, which its clock's skew moves.
        let skew = self
            .nodes
            .iter()
            .find(|n| n.replica.as_ref().is_some_and(|r| r.is_leader()))
            .map_or(0, |n| n.skew);
        let at_ns = u64::try_from((i64::try_from(now).unwrap() + skew).max(0)).unwrap() * 1_000_000;
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
            n.restart(seed, &mut self.damage);
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
        // A member marked by damage is repaired once faults stop: its log holds what it
        // acknowledged again.
        applied.len() == self.nodes.len()
            && applied.windows(2).all(|w| w[0] == w[1])
            && self.nodes.iter().all(|n| !n.uncertain)
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

/// A gateway's write: the gate's opening for an empty key, a put otherwise. A put carries the
/// file its gateway wrote for it, one per session and serial, so its retries carry the same.
fn write(session: Option<u64>, serial: u64, key: &str, value: &str) -> Sessioned {
    let command = if key.is_empty() {
        open_gate()
    } else {
        put(
            key,
            value,
            (u128::from(session.unwrap_or(0)) << 64) | u128::from(serial),
        )
    };
    Sessioned {
        session: session.unwrap_or(0),
        serial,
        unanswered: serial,
        command,
    }
}

fn put(key: &str, etag: &str, file: u128) -> Command {
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
            file: Some(file),
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
fn run(seed: u64, members: u64) -> Ran {
    let mut w = World::new(seed, members);
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
    // Every member marked by damage was repaired from its peers.
    if w.damage.marked != w.damage.unmarked {
        w.fail(&format!("marks left unrepaired: {:?}", w.damage));
    }
    let live = w.live();
    if live.len() != w.members as usize
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
    Ran {
        replaced: w.replaced,
        stalls: w.stalls,
        flushing: w.flushing,
        two_down: w.two_down,
        duplicated: w.duplicated,
        stepped: w.stepped,
        held: w.held,
        held_ticks: w.held_ticks,
        refused: w.refused,
        damage: w.damage,
        steps: w.step,
        history: w
            .history
            .iter()
            .map(|s| format!("{} {:?}", s.key, s.op))
            .collect(),
    }
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
    let (mut replaced, mut stalls, mut flushing, mut two_down, mut duplicated) = (0, 0, 0, 0, 0);
    let (mut stepped, mut held, mut held_ticks, mut refused) = (0, 0, 0, 0);
    let mut damage = Damage::default();
    for seed in first..first + seeds {
        // Odd seeds run a group of three, even ones of five.
        let members = if seed % 2 == 0 { 5 } else { 3 };
        let ran = run(seed, members);
        // A member is lost within the faults of every run, and the run settles only once its
        // replacement finishes.
        assert!(ran.replaced >= 1, "seed {seed} replaced no member");
        replaced += ran.replaced;
        stalls += ran.stalls;
        flushing += ran.flushing;
        two_down += ran.two_down;
        duplicated += ran.duplicated;
        stepped += ran.stepped;
        held += ran.held;
        held_ticks += ran.held_ticks;
        refused += ran.refused;
        damage.injected += ran.damage.injected;
        damage.marked += ran.damage.marked;
        damage.unmarked += ran.damage.unmarked;
        damage.rebuilt += ran.damage.rebuilt;
        damage.replaced += ran.damage.replaced;
        damage.ahead_kept += ran.damage.ahead_kept;
        damage.ahead_rebuilt += ran.damage.ahead_rebuilt;
    }
    // Leaders' clocks stepped back and forth, and every run stayed linearizable.
    assert!(stepped > 0, "no clock was stepped");
    // Readies waited for room and the members went on: the path audit S04 found stranded.
    assert!(stalls > 0, "no ready ever waited for room");
    // Readies were left flushing across steps, their members holding the messages and ticks
    // that came meanwhile.
    assert!(flushing > 0, "no ready was begun and left flushing");
    assert!(
        held > 0 && held_ticks > 0,
        "{held} messages and {held_ticks} ticks held"
    );
    // Groups of five lost two members at once and went on, and messages arrived twice.
    assert!(
        two_down > 0 && duplicated > 0,
        "{two_down} steps two down, {duplicated} duplicated"
    );
    // Devices were damaged at rest, and each way a restart finds damage was repaired: members
    // marked and repaired in place, members rebuilt under a new identity on their device, and
    // devices replaced.
    assert!(
        damage.marked > 0 && damage.unmarked > 0 && damage.rebuilt > 0 && damage.replaced > 0,
        "{damage:?}"
    );
    eprintln!("{damage:?}");
    eprintln!(
        "{seeds} runs replaced {replaced} members lost for good; {stalls} readies waited for \
         room; {flushing} were left flushing across a step; two of five were down at {two_down} \
         steps; {duplicated} messages were sent twice; {stepped} clocks were stepped; \
         {held} messages and {held_ticks} ticks were held while a ready flushed, {refused} \
         messages refused past the bound"
    );
}

/// A run is its seed: the same seed gives the same steps, the same faults and the same
/// history, down to the step each operation was seen at.
#[test]
fn a_seed_runs_the_same_every_time() {
    for seed in [3, 4] {
        let members = if seed % 2 == 0 { 5 } else { 3 };
        let first = run(seed, members);
        let again = run(seed, members);
        assert_eq!(first, again, "seed {seed} ran differently");
    }
}
