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
//!
//! Directed runs, after the seeded ones, cut a member's power at each of its device's writes
//! and flushes in turn while its group changes configuration, for a leader, a follower, a sole
//! voter and a founder removing its only peer, and check the commit fence
//! (docs/design/replica.md §3, §5): the member reopens with any configuration it applied, and
//! the group goes on. Directed replacements cut a replacement's leader, a follower and its
//! joining member the same way, every member's power at the moment the leader says the
//! replacement is done, and one member of the final configuration is then lost for good: each
//! reopens in the final configuration, and the other two elect (docs/design/replica.md §6).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

#[path = "support/store.rs"]
mod store;
mod support;

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::task::Waker;

use hyper_block::block::BlockFile;
use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::sim::{Crash, Fault, SimFile};
use hyper_log::{Config as LogConfig, Log, LogError, Recovery, Waits};
use mantle_meta::apply::Layer;
use mantle_meta::engine::{Engine, Model, Rows};
use mantle_meta::name::{self, GateChange, Preconditions, Put};
use mantle_meta::record::{GateState, Version, Versioning};
use mantle_meta::session::Rules;
use mantle_meta::wire::{Answer, Command, Entry, Sessioned};
use mantle_range::membership::{Next, Replacement};
use mantle_range::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Driven,
    Message, Range, Replica, ReplicaError, Settings,
};
use store::Synchronous;
use support::linear::{self, Input, Operation, Output, Register, Verdict};

const GROUP: u128 = 0x0072_616e_6765;

/// An entry's commands' sessions, serials and answers, in order.
type EntryAnswers = Vec<(u64, u64, Answer)>;

/// One member's applied answers as its entries made them: each entry's index with its commands'
/// answers. An entry's answers come together within one drive.
fn by_entry(answers: Vec<mantle_range::Applied>) -> Vec<(u64, EntryAnswers)> {
    let mut entries: Vec<(u64, EntryAnswers)> = Vec::new();
    for a in answers {
        match entries.last_mut() {
            Some((index, each)) if *index == a.index => each.push((a.session, a.serial, a.answer)),
            _ => entries.push((a.index, vec![(a.session, a.serial, a.answer)])),
        }
    }
    entries
}

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

type Member = Replica<Synchronous<SimFile>, Model>;

/// What a member's drive gives out.
type Out = mantle_range::Output;

/// The simulation's step on the owner's clock, in nanoseconds: the shell reads the clock only to
/// time its writes, which nothing on ticks decides by, so a step is its millisecond.
const STEP_NS: u64 = 1_000_000;

/// Opens member `id` of `range` over its group of `log`, through the store that makes each write
/// durable as it is submitted (`support::store`).
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

/// Drives `r` once, appending what it gives out to `out`; with `settle`, again until no write is
/// out and nothing more is due. Every write is answered as it is submitted (`support::store`), so
/// what a drive gives out never hangs on the log's threads. A member settles within the rounds a
/// group without faults takes to do anything (`PATIENT_ROUNDS`): reaching them is a failure.
fn drive(r: &mut Member, now: u64, settle: bool, out: &mut Out) -> Result<Driven, ReplicaError> {
    let mut driven = r.drive(now, Waker::noop(), out)?;
    if settle {
        let mut budget = PATIENT_ROUNDS;
        while driven.more || r.in_flight() > 0 {
            assert!(
                budget > 0,
                "member {} never settled: {}",
                r.id(),
                r.describe()
            );
            budget -= 1;
            driven = r.drive(now, Waker::noop(), out)?;
        }
    }
    Ok(driven)
}

struct Node {
    id: u64,
    /// The identity of its log, which a member rebuilt on the same device keeps.
    log_id: u128,
    /// Its log while it runs, or while its member is quarantined on it. The log owns the
    /// device then, and the node reaches the device through the log (`Node::with_file`).
    log: Option<Log<SimFile>>,
    /// Its device while no log holds it: the node is down.
    file: Option<SimFile>,
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
    /// know: the member is rebuilt under a new identity on the same log, which the node keeps
    /// open (`Node::log`), since `Replica::open` may answer `Damaged` only after taking a
    /// handle.
    Group,
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
    /// Restarts whose engine had applied entries the damaged log lost, kept in place: the
    /// engine keeps the term of what it applied (docs/design/replica.md §4).
    ahead_kept: u64,
}

fn log_id(id: u64) -> u128 {
    0x6c6f_6700_0000 + u128::from(id)
}

impl Node {
    fn new(id: u64, seed: u64, members: u64) -> Self {
        let file = SimFile::new(
            Alignment::new(4096).unwrap(),
            Alignment::new(512).unwrap(),
            seed ^ (id << 32),
        )
        .unwrap();
        let log = Log::create(file, log_config(), log_id(id)).unwrap();
        let replica = open(id, &log, first_range(), &range(members), seed ^ id).unwrap();
        Self {
            id,
            log_id: log_id(id),
            log: Some(log),
            file: None,
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

    /// Runs `look` on the node's device: through its log while one holds it, between the
    /// log's own I/O, or directly while the node is down.
    fn with_file<R: Send + 'static>(&self, look: impl FnOnce(&SimFile) -> R + Send + 'static) -> R {
        match (&self.log, &self.file) {
            (Some(log), _) => log.with_file(look).unwrap(),
            (None, Some(file)) => look(file),
            (None, None) => panic!("node {} has no device", self.id),
        }
    }

    /// The device, given back by the log once it has answered everything it took.
    fn device(&mut self) -> &SimFile {
        if let Some(log) = self.log.take() {
            self.file = Some(log.close().unwrap());
        }
        self.file.as_ref().unwrap()
    }

    /// Opens the log on the node's device, which a refusal gives back.
    fn open_log(&mut self) -> Result<(Log<SimFile>, Recovery), LogError> {
        let file = self.file.take().unwrap();
        Log::try_open(file, log_config(), self.log_id).map_err(|refused| {
            self.file = refused.file;
            refused.error
        })
    }

    /// Loses power: the process and whatever its log and engine had not made durable.
    fn crash(&mut self) {
        self.crash_losing(Crash::Random);
    }

    /// Loses power, the device keeping its unflushed sectors as `crash` says.
    fn crash_losing(&mut self, crash: Crash) {
        let Some(replica) = self.replica.take() else {
            return;
        };
        let mut engine = replica.into_engine();
        engine.crash();
        self.engine = Some(engine);
        self.device().crash(crash).unwrap();
    }

    /// Restarts from what the device and engine kept. Damage the restart finds quarantines
    /// the member: it stays down until the group rebuilds it (`World::rebuild`).
    fn restart(&mut self, seed: u64, damage: &mut Damage) {
        let Some(engine) = self.engine.take() else {
            return;
        };
        let latent = std::mem::take(&mut self.latent);
        self.lives += 1;
        self.device().clear_faults().unwrap();
        let (log, recovery) = match self.open_log() {
            Ok(opened) => opened,
            Err(LogError::Damaged(_)) if latent => {
                self.found = Some(Found::Log);
                return;
            }
            Err(e) => panic!("member {} reopens its log: {e}", self.id),
        };
        let log = &*self.log.insert(log);
        if !recovery.damaged.is_empty() {
            assert!(latent && recovery.damaged == [GROUP], "{recovery:?}");
            self.found = Some(Found::Group);
            return;
        }
        let ahead = log
            .view(GROUP)
            .unwrap()
            .is_some_and(|v| engine.applied() > v.last);
        match open(
            self.id,
            log,
            engine,
            &range(self.members),
            seed ^ self.id ^ (self.lives << 40),
        ) {
            Ok(replica) => {
                damage.ahead_kept += u64::from(ahead);
                // A mark lasts across restarts until the group repairs the member.
                let before = self.uncertain;
                self.uncertain = replica.is_uncertain();
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
            Err(e) => panic!("member {} reopens: {e}", self.id),
        }
    }

    /// Damages a frame of the member's log at rest, the device down: a bit flipped on the
    /// medium, which no clearing of faults heals. `kind` picks the frame: 0 its last, 1 one
    /// before it, 2 its last after it takes a fast-track proposal of its own, which a persist
    /// record does not carry.
    fn damage(&mut self, kind: u64, rng: &mut Rng) {
        self.device().clear_faults().unwrap();
        if kind == 2 {
            let (log, recovery) = self.open_log().unwrap();
            assert!(recovery.damaged.is_empty());
            if let Some(view) = log.view(GROUP).unwrap() {
                let term = view.hard_state.map_or(1, |h| h.term.max(1));
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
            }
            self.file = Some(log.close().unwrap());
        }
        let log_id = self.log_id;
        let file = self.device();
        let image = file.durable_image().unwrap();
        let mut frames: Vec<(u64, usize, usize)> = image
            .chunks(4096)
            .enumerate()
            .filter_map(|(i, block)| {
                let h = hyper_log::format::FrameHeader::decode(block)?;
                let len = h.frame_len()?;
                let frame = image.get(i * 4096..i * 4096 + len)?;
                (h.log == log_id && h.verifies(frame)).then(|| (h.sequence, i * 4096, len))
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
        file.read_exact_at(buf.as_mut_slice(), block).unwrap();
        buf.as_mut_slice()[(offset - block) as usize] ^= 1 << rng.below(8);
        file.write_all_at(buf.as_slice(), block).unwrap();
        file.sync_data().unwrap();
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
    /// Drives that found a write waiting for room, each followed by a compaction or the owner's
    /// word that room may have been freed.
    stalls: u64,
    /// Steps at which a member's writes were left out past its drive, its readies taken ahead
    /// of their answers.
    ahead: u64,
    /// Steps at which two members were down at once, and messages sent twice.
    two_down: u64,
    duplicated: u64,
    /// Clocks stepped, forward or back.
    stepped: u64,
    /// Messages and ticks the core took while the member's writes were out.
    stepped_ahead: u64,
    ticked_ahead: u64,
    damage: Damage,
}

/// What a run exercised, and what its gateways saw at which steps.
#[derive(Debug, PartialEq, Eq)]
struct Ran {
    replaced: u64,
    stalls: u64,
    ahead: u64,
    two_down: u64,
    duplicated: u64,
    stepped: u64,
    stepped_ahead: u64,
    ticked_ahead: u64,
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
            ahead: 0,
            two_down: 0,
            duplicated: 0,
            stepped: 0,
            stepped_ahead: 0,
            ticked_ahead: 0,
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
                if r.in_flight() > 0 {
                    self.ticked_ahead += 1;
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
            self.nodes[i].with_file(move |f| f.inject(fault)).unwrap();
        }
        if self.rng.chance(10)
            && let Some(r) = self.nodes[i].replica.as_mut()
        {
            let keep = self.rng.below(8);
            match r.compact(keep, self.step * STEP_NS, Waker::noop()) {
                Ok(_) => {}
                Err(ReplicaError::Fenced(_)) => self.nodes[i].crash(),
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
            Some(Found::Group) => {
                let log = self.nodes[i].log.as_ref().unwrap();
                let rebuilt = mantle_range::remove(log, GROUP)
                    .and_then(|()| mantle_range::claim(log, GROUP, &range(self.members)))
                    .and_then(|store| {
                        Replica::rebuild(
                            failed,
                            joining,
                            Synchronous::new(store, None),
                            first_range(),
                            &range(self.members),
                            self.seed ^ joining,
                        )
                    });
                let (replica, replacement) =
                    rebuilt.unwrap_or_else(|e| self.fail(&format!("rebuild {failed}: {e}")));
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
            let snapshot = m.msg_type == mantle_range::MessageType::MsgSnapshot;
            let dropped =
                self.blocked.contains(&(from, to)) || (self.faults && self.rng.chance(20));
            let receiver = self.nodes.iter_mut().find(|n| n.id == to);
            let arrived = !dropped
                && match receiver.and_then(|n| n.replica.as_mut()) {
                    Some(r) => {
                        // The core takes a message while the member's writes are out.
                        let ahead = r.in_flight() > 0;
                        match r.step(m) {
                            Ok(()) => {
                                self.stepped_ahead += u64::from(ahead);
                                true
                            }
                            Err(ReplicaError::Refused(_)) => true,
                            // A member waiting for room in its log takes no message.
                            Err(ReplicaError::Stalled) => false,
                            Err(e) => panic!("step: {e}"),
                        }
                    }
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
        let now = self.step * STEP_NS;
        for n in &mut self.nodes {
            let Some(r) = n.replica.as_mut() else {
                continue;
            };
            // A member half the time drives once and leaves its writes out past the step, its
            // next readies taken ahead of their answers and the messages and ticks that come
            // meanwhile taken by its core, as a node overlapping its members' flushes does
            // (audit §5.1); and half the time drives until none is out. It does so after faults
            // stop too. Every write is answered as it is submitted (`support::store`), so what a
            // drive gives out never hangs on the log's threads: every run is its seed.
            let settle = !self.rng.chance(500);
            let mut out = Out::default();
            let driven = drive(r, now, settle, &mut out);
            let driven = match driven {
                Ok(driven) => driven,
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
            if driven.stalled.is_some() {
                waiting.push(n.id);
            }
            if r.in_flight() > 0 {
                self.ahead += 1;
            }
            // A mark ends as the group repairs the member: its log holds the entries again.
            if n.uncertain && !r.is_uncertain() {
                n.uncertain = false;
                self.damage.unmarked += 1;
            }
            sent.extend(out.messages);
            applied.extend(by_entry(out.answers));
            reads.extend(out.reads.into_iter().map(|(ctx, index)| (n.id, index, ctx)));
        }
        if let Some(what) = stopped {
            self.fail(&what);
        }
        // A write waits for room in its member's log: the member compacts, and once that is
        // durable its next drive makes the refused writes again; with nothing to compact, the
        // owner says room may have been freed, and the next drive tries again.
        for id in waiting {
            self.stalls += 1;
            let keep = self.rng.below(8);
            let Some(n) = self.nodes.iter_mut().find(|n| n.id == id) else {
                continue;
            };
            let Some(r) = n.replica.as_mut() else {
                continue;
            };
            match r.compact(keep, now, Waker::noop()) {
                Ok(true) => {}
                Ok(false) => r.resume(),
                Err(ReplicaError::Fenced(_)) => {
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
        for (index, answers) in applied {
            match self.by_index.get(&index) {
                Some(first) if *first != answers => {
                    self.fail(&format!("index {index} applied two ways"));
                }
                Some(_) => {}
                None => {
                    self.hear(&answers);
                    self.by_index.insert(index, answers);
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
            n.with_file(SimFile::clear_faults).unwrap();
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

/// Every row of an engine: every one is its range's, the configuration and the term of the last
/// entry applied among them, which every member applying the same entries writes alike.
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
        ahead: w.ahead,
        two_down: w.two_down,
        duplicated: w.duplicated,
        stepped: w.stepped,
        stepped_ahead: w.stepped_ahead,
        ticked_ahead: w.ticked_ahead,
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
    // By default the recorded seeds 1 to 48 (docs/design/replica.md §5), among which a restart
    // finds each kind of damage: seed 42 is the first whose member opens marked.
    let seeds: u64 = std::env::var("MANTLE_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(48);
    let first: u64 = std::env::var("MANTLE_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let (mut replaced, mut stalls, mut ahead, mut two_down, mut duplicated) = (0, 0, 0, 0, 0);
    let (mut stepped, mut stepped_ahead, mut ticked_ahead) = (0, 0, 0);
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
        ahead += ran.ahead;
        two_down += ran.two_down;
        duplicated += ran.duplicated;
        stepped += ran.stepped;
        stepped_ahead += ran.stepped_ahead;
        ticked_ahead += ran.ticked_ahead;
        damage.injected += ran.damage.injected;
        damage.marked += ran.damage.marked;
        damage.unmarked += ran.damage.unmarked;
        damage.rebuilt += ran.damage.rebuilt;
        damage.replaced += ran.damage.replaced;
        damage.ahead_kept += ran.damage.ahead_kept;
    }
    // Leaders' clocks stepped back and forth, and every run stayed linearizable.
    assert!(stepped > 0, "no clock was stepped");
    // Writes waited for room and the members went on: the path audit S04 found stranded.
    assert!(stalls > 0, "no write ever waited for room");
    // Writes were left out across steps, their members' readies taken ahead of the answers
    // and the messages and ticks that came meanwhile taken by their cores.
    assert!(ahead > 0, "no write was left out past a drive");
    assert!(
        stepped_ahead > 0 && ticked_ahead > 0,
        "{stepped_ahead} messages and {ticked_ahead} ticks taken while writes were out"
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
        "{seeds} runs replaced {replaced} members lost for good; {stalls} drives found a write \
         waiting for room; members' writes were left out past {ahead} drives; two of five were down at \
         {two_down} steps; {duplicated} messages were sent twice; {stepped} clocks were \
         stepped; {stepped_ahead} messages and {ticked_ahead} ticks were taken while writes \
         were out"
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

/// How the target takes its readies in a directed run: driving until none of its writes is out
/// (`Waiting`), or once a round, its writes left out past the round and its next readies taken
/// ahead of their answers, as a node overlapping its members' flushes does (`Overlapping`;
/// audit §5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Takes {
    Waiting,
    Overlapping,
}

/// A change of configuration a group founded by `1..=members` makes under member 1's lead,
/// and the member whose power is cut while it does.
#[derive(Debug)]
struct Case {
    members: u64,
    target: u64,
    change: ConfChangeV2,
    /// The configuration the change makes.
    made: ConfState,
    /// The member the change removes. The operator stops it for good once the leader says
    /// every voter knows the change committed (`Replica::configuration_known`), as a
    /// replacement ends (`mantle_range::membership`).
    removes: Option<u64>,
    /// A replacement the leader runs instead of one change (`membership::Replacement`): each
    /// change it asks for is proposed, again after a patience without effect, until it is done.
    replacing: Option<Replacement>,
}

/// Rounds a directed run waits for what a group with a quorum and no faults does within a few
/// election timeouts (elect a leader, commit an entry): twenty of the longest timeout the core
/// draws, `2 · election_tick`. Reaching it is the failure the run looks for, not a wait.
const PATIENT_ROUNDS: usize = 40 * SETTINGS.election_tick;

/// A group run by hand, without faults but the power cut a run arms: messages arrive in the
/// order they were sent, a round after.
struct Directed {
    seed: u64,
    nodes: Vec<Node>,
    wire: VecDeque<Message>,
    damage: Damage,
    /// The registrations each member applied: (member, nonce).
    registered: HashSet<(u64, u64)>,
    /// Rounds run, and the round the replacement's last change was proposed in.
    rounds: usize,
    proposed_at: Option<usize>,
    /// The leader said the replacement is done: the round stopped at that moment.
    done: bool,
}

impl Directed {
    fn new(case: &Case, seed: u64) -> Self {
        Self {
            seed,
            nodes: (1..=case.members)
                .map(|id| Node::new(id, seed, case.members))
                .collect(),
            wire: VecDeque::new(),
            damage: Damage::default(),
            registered: HashSet::new(),
            rounds: 0,
            proposed_at: None,
            done: false,
        }
    }

    fn replica(&mut self, id: u64) -> Option<&mut Member> {
        self.nodes
            .iter_mut()
            .find(|n| n.id == id)
            .and_then(|n| n.replica.as_mut())
    }

    fn leader(&mut self) -> Option<&mut Member> {
        self.nodes
            .iter_mut()
            .filter_map(|n| n.replica.as_mut())
            .find(|r| r.is_leader())
    }

    /// The operator: stops the member the change removed for good once the leader has applied
    /// the change and says every voter knows it committed; or runs the replacement through
    /// whichever member leads, noting the moment it is done.
    fn operate(&mut self, case: &Case) {
        if let Some(replacement) = case.replacing {
            let (round, proposed_at) = (self.rounds, self.proposed_at);
            let Some(leader) = self.leader() else {
                return;
            };
            match replacement.next(
                leader.configuration(),
                leader.caught_up(replacement.joining()),
                leader.configuration_known(),
            ) {
                Next::Done => self.done = true,
                Next::Wait => {}
                Next::Propose(change) => {
                    if proposed_at.is_none_or(|at| round - at >= PATIENCE as usize) {
                        // A leader whose power was cut on a write still out is fenced: its
                        // next drive says so, and the run restarts it.
                        match leader.propose_change(&change) {
                            Ok(())
                            | Err(
                                ReplicaError::Refused(_)
                                | ReplicaError::Stalled
                                | ReplicaError::Fenced(_),
                            ) => {}
                            Err(e) => panic!("propose a change: {e}"),
                        }
                        self.proposed_at = Some(round);
                    }
                }
            }
            return;
        }
        let Some(removed) = case.removes else {
            return;
        };
        let known = self.leader().is_some_and(|r| {
            !r.configuration().voters.contains(&removed) && r.configuration_known()
        });
        if known {
            self.nodes.retain(|n| n.id != removed);
        }
    }

    /// One round: every member ticks, takes its ready, the operator looking on after each, and
    /// the messages sent arrive. The target's configuration as it stood when its device's power
    /// was cut, once the member learns it: its log fenced.
    fn round(&mut self, case: &Case, takes: Takes) -> Option<ConfState> {
        self.rounds += 1;
        let ids: Vec<u64> = self.nodes.iter().map(|n| n.id).collect();
        for &id in &ids {
            if let Some(r) = self.replica(id)
                && !r.is_fenced()
            {
                r.tick().unwrap();
            }
        }
        let mut cut = None;
        let now = u64::try_from(self.rounds).unwrap() * STEP_NS;
        for &id in &ids {
            let settle = !(id == case.target && takes == Takes::Overlapping);
            let Some(r) = self.replica(id) else {
                continue;
            };
            if r.is_fenced() {
                continue;
            }
            let mut out = Out::default();
            match drive(r, now, settle, &mut out) {
                Ok(_) => {
                    self.wire.extend(out.messages);
                    for a in out.answers {
                        if a.session == 0 && matches!(a.answer, Answer::Registered { .. }) {
                            self.registered.insert((id, a.serial));
                        }
                    }
                }
                Err(ReplicaError::Fenced(_)) if id == case.target => {
                    cut = Some(r.configuration().clone());
                    continue;
                }
                Err(e) => panic!("drive on {id}: {e}"),
            }
            // The operator acts on what the members say, whenever it looks: here, while the
            // target's writes may still be out.
            self.operate(case);
            // The moment a replacement is done is where its run cuts every member's power:
            // nothing more is driven or delivered.
            if self.done && case.replacing.is_some() {
                return cut;
            }
        }
        let sent = std::mem::take(&mut self.wire);
        for m in sent {
            let to = m.to;
            if let Some(r) = self.replica(to) {
                match r.step(m) {
                    Ok(())
                    | Err(
                        ReplicaError::Refused(_) | ReplicaError::Stalled | ReplicaError::Fenced(_),
                    ) => {}
                    Err(e) => panic!("step on {to}: {e}"),
                }
            }
        }
        cut
    }

    /// Whether every member has applied the change, and the member it removes is stopped.
    fn changed(&self, case: &Case) -> bool {
        self.nodes.iter().all(|n| {
            n.replica
                .as_ref()
                .is_some_and(|r| same_configuration(r.configuration(), &case.made))
        }) && case
            .removes
            .is_none_or(|removed| self.nodes.iter().all(|n| n.id != removed))
    }
}

/// Whether two configurations name the same members in the same roles.
fn same_configuration(a: &ConfState, b: &ConfState) -> bool {
    let sorted = |ids: &[u64]| {
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids
    };
    sorted(&a.voters) == sorted(&b.voters)
        && sorted(&a.learners) == sorted(&b.learners)
        && sorted(&a.voters_outgoing) == sorted(&b.voters_outgoing)
        && sorted(&a.learners_next) == sorted(&b.learners_next)
}

fn simple_change(kind: ConfChangeType, node_id: u64) -> ConfChangeV2 {
    ConfChangeV2 {
        transition: ConfChangeTransition::Auto,
        changes: vec![ConfChangeSingle {
            change_type: kind,
            node_id,
        }],
        context: Vec::new(),
    }
}

fn registration(nonce: u64) -> Entry {
    Entry {
        at_ns: 0,
        commands: vec![Sessioned {
            session: 0,
            serial: nonce,
            unanswered: 0,
            command: Command::Register,
        }],
    }
}

/// One directed run of `case`: member 1 is elected, proposes the change, and the target's
/// device cuts the power at its `ops`-th write or flush after the proposal, or, for `None`,
/// once the change is made everywhere. The target restarts from what its device kept.
///
/// Then, against the commit fence (hyper-raft docs/durable.md §4.1, I5): the target, driven
/// alone, reaches the configuration it had applied when the power went from its own durable
/// state, so nothing it applied, and nothing the operator did on its word, ran ahead of what
/// it reopens with; and the group goes on: a leader is elected and an entry commits on every
/// member. Returns whether the power was cut; `false` for `Some(ops)` past the last
/// operation the run makes.
fn directed(case: &Case, takes: Takes, ops: Option<u64>, seed: u64) -> bool {
    let mut d = Directed::new(case, seed);
    d.replica(1).unwrap().campaign().unwrap();
    let mut elected = false;
    for _ in 0..PATIENT_ROUNDS {
        assert!(d.round(case, Takes::Waiting).is_none());
        if d.replica(1).is_some_and(|r| r.is_leader()) {
            elected = true;
            break;
        }
    }
    assert!(elected, "member 1 is never elected");
    if let Some(ops) = ops {
        let target = d.nodes.iter().find(|n| n.id == case.target).unwrap();
        target
            .with_file(move |f| f.inject(Fault::PowerCut { ops }))
            .unwrap();
    }
    d.replica(1).unwrap().propose_change(&case.change).unwrap();
    // Until the power is cut, or the change is made and a heartbeat's rounds more pass.
    let mut cut = None;
    let mut quiet = 0;
    for _ in 0..PATIENT_ROUNDS {
        cut = d.round(case, takes);
        if cut.is_some() {
            break;
        }
        if d.changed(case) {
            quiet += 1;
            if quiet > SETTINGS.heartbeat_tick * 2 {
                break;
            }
        }
    }
    let at_cut = match (cut, ops) {
        (Some(conf), _) => conf,
        (None, Some(_)) => {
            assert!(d.changed(case), "{case:?} {takes:?} never made its change");
            return false;
        }
        (None, None) => {
            assert!(d.changed(case), "{case:?} {takes:?} never made its change");
            d.replica(case.target).unwrap().configuration().clone()
        }
    };
    let seed = d.seed;
    let mut damage = std::mem::take(&mut d.damage);
    let node = d.nodes.iter_mut().find(|n| n.id == case.target).unwrap();
    node.crash_losing(Crash::LoseAll);
    node.restart(seed, &mut damage);
    let mut found = Vec::new();
    // Alone, the target reaches what its own durable state says is committed.
    let r = node.replica.as_mut().expect("the target reopens");
    let mut settled = false;
    for _ in 0..PATIENT_ROUNDS {
        let before = (r.applied(), r.configuration().clone());
        drive(r, 0, true, &mut Out::default()).unwrap();
        if (r.applied(), r.configuration().clone()) == before {
            settled = true;
            break;
        }
    }
    assert!(settled, "the target alone never stops applying");
    // Its durable state may hold a commit it had not applied yet, never the reverse.
    let reopened = r.configuration().clone();
    if same_configuration(&at_cut, &case.made) && !same_configuration(&reopened, &case.made) {
        found.push(format!(
            "member {} applied {at_cut:?} and acted on it, but its durable state reopens at \
             {reopened:?}",
            case.target
        ));
    }
    // The group goes on: a leader, the change made, proposed again by the leader if the cut
    // lost it, and an entry committed on every member.
    let nonce = 0x5157;
    let mut proposed = false;
    let mut live = false;
    for round in 0..PATIENT_ROUNDS {
        assert!(d.round(case, takes).is_none(), "a second cut");
        if let Some(leader) = d.leader() {
            if !proposed {
                proposed = leader.propose(&registration(nonce)).is_ok();
            }
            if round % PATIENCE as usize == 0
                && !same_configuration(leader.configuration(), &case.made)
            {
                match leader.propose_change(&case.change) {
                    Ok(()) | Err(ReplicaError::Refused(_) | ReplicaError::Stalled) => {}
                    Err(e) => panic!("propose the change again: {e}"),
                }
            }
        }
        if proposed
            && d.changed(case)
            && d.nodes
                .iter()
                .all(|n| d.registered.contains(&(n.id, nonce)))
        {
            live = true;
            break;
        }
    }
    if !live {
        let states: Vec<String> = d
            .nodes
            .iter()
            .filter_map(|n| n.replica.as_ref())
            .map(|r| format!("{} conf {:?}", r.describe(), r.configuration()))
            .collect();
        found.push(format!(
            "the group never elected a leader that committed an entry on every member: \
             {states:?}"
        ));
    }
    assert!(
        found.is_empty(),
        "{case:?}, {takes:?}, power cut at operation {ops:?} after the proposal: {found:#?}"
    );
    true
}

/// Runs `case` with the power cut at each of the target's device operations after the change
/// is proposed in turn, and once after the change is made, both ways of taking readies.
fn every_cut(case: &Case) {
    // A run makes a few dozen writes and flushes; this bounds the enumeration far past them.
    const MAX_OPS: u64 = 1 << 12;
    let seed = 0x51;
    for takes in [Takes::Waiting, Takes::Overlapping] {
        let mut ops = 0;
        while directed(case, takes, Some(ops), seed) {
            ops += 1;
            assert!(ops < MAX_OPS, "{case:?} never ran out of operations");
        }
        assert!(ops > 0, "{case:?} {takes:?} cut the power nowhere");
        directed(case, takes, None, seed);
    }
}

/// The founder of a group of two removes its only peer, which the operator stops once the
/// founder says the change is known; the founder's power is cut anywhere in the change. The
/// founder, alone, must still elect itself: a founder that applied the removal on a commit
/// its log never held reopens counting the peer, and no vote ever comes (focal F17).
#[test]
fn a_founder_that_removes_its_only_peer_elects_itself_after_a_cut_anywhere() {
    every_cut(&Case {
        members: 2,
        target: 1,
        change: simple_change(ConfChangeType::RemoveNode, 2),
        made: ConfState {
            voters: vec![1],
            ..ConfState::default()
        },
        removes: Some(2),
        replacing: None,
    });
}

/// A leader of three removes a member; the leader's power is cut anywhere in the change.
#[test]
fn a_leader_cut_inside_a_change_reopens_with_what_it_applied() {
    every_cut(&Case {
        members: 3,
        target: 1,
        change: simple_change(ConfChangeType::RemoveNode, 3),
        made: ConfState {
            voters: vec![1, 2],
            ..ConfState::default()
        },
        removes: Some(3),
        replacing: None,
    });
}

/// A follower of three applies the removal of another; its power is cut anywhere in the
/// change.
#[test]
fn a_follower_cut_inside_a_change_reopens_with_what_it_applied() {
    every_cut(&Case {
        members: 3,
        target: 2,
        change: simple_change(ConfChangeType::RemoveNode, 3),
        made: ConfState {
            voters: vec![1, 2],
            ..ConfState::default()
        },
        removes: Some(3),
        replacing: None,
    });
}

/// The sole voter of a group adds a learner, a change it commits alone; its power is cut
/// anywhere in the change.
#[test]
fn a_sole_voter_cut_inside_a_change_reopens_with_what_it_applied() {
    every_cut(&Case {
        members: 1,
        target: 1,
        change: simple_change(ConfChangeType::AddLearnerNode, 2),
        made: ConfState {
            voters: vec![1],
            learners: vec![2],
            ..ConfState::default()
        },
        removes: None,
        replacing: None,
    });
}

/// The final configuration of a replacement of member 3 by member 4 in a group of three.
fn replaced_configuration() -> ConfState {
    ConfState {
        voters: vec![1, 2, 4],
        ..ConfState::default()
    }
}

/// One directed run of a replacement under member 1's lead (docs/design/replica.md §6): member 3
/// of a group of three is lost for good, member 4 joins, and the leader runs
/// `membership::Replacement` until it says the replacement is done. The target's device cuts the
/// power at its `ops`-th write or flush from when member 4 joins, or nowhere for `None`; the
/// target restarts from what its device kept, and the replacement must still finish. Among the
/// cuts are those inside the window between each change's commit and the commit the target's
/// log states.
///
/// Then the guarantee `Replica::configuration_known` gives, at the moment the leader says it:
/// every member loses power at once, keeping only what its device made durable, and each,
/// driven alone, must reopen in the final configuration; and once one member of it is lost for
/// good (`lost`), the other two must elect a leader and commit an entry. A voter whose word the
/// leader counted for a commit its log did not state would reopen in the joint configuration,
/// whose old half has lost two of its three members, and the two left could not elect
/// (hyper-raft docs/durable.md §1, §11: D-1's first test). Returns whether the power was cut;
/// `false` for `Some(ops)` past the last operation the run makes.
fn replaced(target: u64, lost: u64, takes: Takes, ops: Option<u64>, seed: u64) -> bool {
    let case = Case {
        members: 3,
        target,
        change: ConfChangeV2::default(),
        made: replaced_configuration(),
        removes: None,
        replacing: Replacement::new(3, 4),
    };
    let mut d = Directed::new(&case, seed);
    d.replica(1).unwrap().campaign().unwrap();
    let mut elected = false;
    for _ in 0..PATIENT_ROUNDS {
        assert!(d.round(&case, Takes::Waiting).is_none());
        if d.replica(1).is_some_and(|r| r.is_leader()) {
            elected = true;
            break;
        }
    }
    assert!(elected, "member 1 is never elected");
    // Member 3 is lost for good, its device and all; member 4 joins on a new one.
    d.nodes.retain(|n| n.id != 3);
    d.nodes.push(Node::new(4, seed, case.members));
    if let Some(ops) = ops {
        let node = d.nodes.iter().find(|n| n.id == target).unwrap();
        node.with_file(move |f| f.inject(Fault::PowerCut { ops }))
            .unwrap();
    }
    // A replacement takes a few changes, each committed within a few election timeouts.
    let mut cut = false;
    for _ in 0..4 * PATIENT_ROUNDS {
        if d.round(&case, takes).is_some() {
            assert!(!cut, "a second cut");
            cut = true;
            let seed = d.seed;
            let mut damage = std::mem::take(&mut d.damage);
            let node = d.nodes.iter_mut().find(|n| n.id == target).unwrap();
            node.crash_losing(Crash::LoseAll);
            node.restart(seed, &mut damage);
            d.damage = damage;
        }
        if d.done {
            break;
        }
    }
    let states = |d: &Directed| -> Vec<String> {
        d.nodes
            .iter()
            .filter_map(|n| n.replica.as_ref())
            .map(|r| format!("{} conf {:?}", r.describe(), r.configuration()))
            .collect()
    };
    assert!(
        d.done,
        "target {target}, {takes:?}, cut at {ops:?}: the replacement never finished: {:?}",
        states(&d)
    );
    if ops.is_some() && !cut {
        return false;
    }
    // Every member loses power at the moment the leader says every voter knows: what each
    // reopens with is what its device made durable.
    d.wire.clear();
    let seed = d.seed;
    let mut damage = std::mem::take(&mut d.damage);
    let mut found = Vec::new();
    for node in &mut d.nodes {
        node.crash_losing(Crash::LoseAll);
        node.restart(seed, &mut damage);
        let r = node.replica.as_mut().expect("a member reopens");
        let mut settled = false;
        for _ in 0..PATIENT_ROUNDS {
            let before = (r.applied(), r.configuration().clone());
            drive(r, 0, true, &mut Out::default()).unwrap();
            if (r.applied(), r.configuration().clone()) == before {
                settled = true;
                break;
            }
        }
        assert!(settled, "member {} alone never stops applying", node.id);
        if !same_configuration(r.configuration(), &case.made) {
            found.push(format!(
                "member {} reopens in {:?} after the leader said every voter knew {:?}",
                node.id,
                r.configuration(),
                case.made
            ));
        }
    }
    d.damage = damage;
    // One member of the final configuration is lost for good; the other two go on.
    d.nodes.retain(|n| n.id != lost);
    let after = Case {
        replacing: None,
        ..case
    };
    let nonce = 0x4e55;
    let mut proposed = false;
    let mut live = false;
    for _ in 0..PATIENT_ROUNDS {
        assert!(d.round(&after, takes).is_none(), "a second cut");
        if !proposed && let Some(leader) = d.leader() {
            proposed = leader.propose(&registration(nonce)).is_ok();
        }
        if proposed
            && d.nodes
                .iter()
                .all(|n| d.registered.contains(&(n.id, nonce)))
        {
            live = true;
            break;
        }
    }
    if !live {
        found.push(format!(
            "with member {lost} lost, the other two never elected a leader that committed an \
             entry on both: {:?}",
            states(&d)
        ));
    }
    assert!(
        found.is_empty(),
        "target {target}, {takes:?}, power cut at operation {ops:?} from member 4's joining, \
         member {lost} lost after: {found:#?}"
    );
    true
}

/// Runs a replacement with the target's power cut at each of its device operations from member
/// 4's joining in turn, and once without a cut, both ways of taking readies. The member lost
/// for good after the replacement turns with the cut: each of the final configuration's three.
fn every_replacement_cut(target: u64) {
    // A replacement makes a few dozen writes and flushes (30 to 40 for these three targets);
    // this bounds the enumeration far past them.
    const MAX_OPS: u64 = 1 << 12;
    let seed = 0x52;
    let lost_after = [1, 2, 4];
    for takes in [Takes::Waiting, Takes::Overlapping] {
        let mut ops = 0;
        while replaced(
            target,
            lost_after[(ops % 3) as usize],
            takes,
            Some(ops),
            seed,
        ) {
            ops += 1;
            assert!(ops < MAX_OPS, "target {target} never ran out of operations");
        }
        assert!(ops > 0, "target {target} {takes:?} cut the power nowhere");
        for lost in lost_after {
            replaced(target, lost, takes, None, seed);
        }
    }
}

/// The leader running a replacement loses power anywhere in it, the window between each
/// change's commit and the commit its log states among the cuts (D-1's first test).
#[test]
fn a_replacements_leader_cut_anywhere_in_it_leaves_a_group_that_elects() {
    every_replacement_cut(1);
}

/// A follower that stays a voter through a replacement loses power anywhere in it.
#[test]
fn a_replacements_follower_cut_anywhere_in_it_leaves_a_group_that_elects() {
    every_replacement_cut(2);
}

/// The joining member loses power anywhere in the replacement that makes it a voter.
#[test]
fn a_replacements_joining_member_cut_anywhere_in_it_leaves_a_group_that_elects() {
    every_replacement_cut(4);
}
