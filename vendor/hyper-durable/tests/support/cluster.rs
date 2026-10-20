//! A group of replicas on [`SimStore`]s in hyper-sim's world (`docs/sim.md` S-6), driven one step
//! at a time by a schedule drawn from the world's streams, and held at every step to hyper-check's
//! oracles.
//!
//! Every step is one of: a word of a member's failure detectors about another (suspected, or
//! trusted again), the world's next event as its strategy picks it (a message arriving, or a
//! member's device completing its oldest write), a drive of a member, a proposal, a change of
//! configuration, a read, a compaction, a crash, a refusal or failure armed on a member's device.
//! Messages travel on the world's network (`hyper_sim::net`), whose path loses what the schedule
//! says; a member's device is hyper-sim's typed write queue, whose completion is an event of the
//! world while a write is out, or, in directed tests, a test's word ([`Devices`]). Members elect by
//! suspicion (timing step L-2) and take no ticks: the clock moves a millisecond a step, and a drive
//! wakes the member at it. A crash loses every write not durable and the state machine's applied
//! state past its durable point; the member reopens on what is left, its device's pending
//! completion with it, and the others' detectors see its new incarnation.
//!
//! The oracles (`hyper_check::oracle`), against each member's durable state `D` (its store's disk,
//! the state machine's durable point beside it) as an output leaves or a write lands
//! (`docs/durable.md` §3):
//! - **Durability**: I1 and I2 and R-6 on every message a member releases, I3 on every commit a
//!   leader counts new to the group, I4 on every entry applied, I5 on every change and every entry
//!   acted on at start, I7 on every write made durable, I8 on every member after every step.
//! - **State Machine Safety**: every member applies the same entry at an index.
//! - **Election Safety**: at most one leader a term, ever.
//! - **Log Matching**: two logs holding an entry of one term at an index agree on it and on the
//!   term before it, fed from every write made durable.
//! - **Leader Completeness**: a leader holds every entry committed in an earlier term.
//!
//! Beside them, the harness's own: across a crash, every entry acted on at start is applied again
//! from what the member reopens with, before it hears from anyone (I5), and the compaction rule's
//! count of a member's log is what a walk of it counts (R22).
//!
//! Crashing after a drive rather than inside it is the harder case for every event inside it:
//! nothing durable changes within a drive (writes become durable only at the world's or a test's
//! word), and what the drive released has left, for the others to act on.
use std::collections::BTreeMap;
use std::task::Waker;
use std::time::Duration;

use hyper_check::oracle::{
    Durability, DurableView, ElectionSafety, LeaderCompleteness, LogMatching, LogView, Released,
    Says, StateMachineSafety, Terms, Violation,
};
use hyper_durable::{
    Cause, EntryRef, Fault, Output, Point, Replica, ReplicaError, Settings, Unbounded,
};
use hyper_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Entry,
    EntryType, Message, MessageType,
};
use hyper_raft::wire::Record as _;
use hyper_raft::{Config, Configuration, StateRole};
use hyper_sim::net::{Net, NetLimits, NetStats, Path, Ticket};
use hyper_sim::tape::{Bounds, Tape};
use hyper_sim::{
    Clock, Discipline, Fifo, Limits, NodeId, Random, Record, Source, Step, Strategy, StreamId,
    Twice, World, twice,
};

use super::{ACTS, DeviceWrites, Disk, Kv, SimStore};

pub type Member = Replica<SimStore, Kv, Unbounded>;

/// What an entry states, as the oracles compare it: its term, its kind and its data. The term is
/// part of it, as the schedule this harness replaced compared a commit by its index and term: no
/// fast track restamps an entry here.
pub type Value = (u64, u8, Vec<u8>);

fn value(term: u64, entry_type: EntryType, data: &[u8]) -> Value {
    (term, entry_type as u8, data.to_vec())
}

/// How the group runs.
#[derive(Clone, Copy, Debug)]
pub struct Shape {
    pub members: u64,
    pub voters: u64,
    pub depth: usize,
    pub volatile: bool,
    pub control: bool,
    /// A leader applies its own committed entries before its own write is durable
    /// (`Config::apply_unpersisted`, `docs/durable.md` §4.2).
    pub ahead: bool,
}

/// How members' writes become durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Devices {
    /// When a test says ([`Op::Durable`]): directed tests that fix their schedule.
    Directed,
    /// As the world's events, a device's next completion pending while a write is out
    /// (`hyper_sim::storage::Writes::arm`), picked by the world's strategy like any other event.
    World,
}

/// What a crash left of a member: its disk, its state machine at its durable point, and its
/// device's queue, a completion pending in the world with it.
#[derive(Clone)]
pub struct Down {
    pub disk: Disk,
    pub kv: Kv,
    writes: DeviceWrites,
}

/// One member: its replica while up, and what a crash left while down.
#[derive(Clone)]
pub struct Node {
    pub id: u64,
    pub replica: Option<Member>,
    /// While down: what a restart opens on.
    pub down: Option<Down>,
    /// Every entry acted on at start before the last crash, to hold against what it reopens with.
    pub acted_before_crash: Vec<u64>,
    /// Whether a crash left it to reopen alone first.
    pub reopened: bool,
    /// The core's seed, drawn from the world once: an incarnation opens on the same draws.
    seed: u64,
    /// The highest index of its state machine held to the oracles since it opened.
    checked: u64,
}

/// A step of the schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// The first member's detectors suspect the second.
    Suspect(u64, u64),
    /// The first member's detectors trust the second again.
    Trust(u64, u64),
    /// The world's next event, as its strategy picks it.
    World,
    /// The world's next event, as its strategy picks it, a message it delivers lost.
    Lose,
    Drive(u64),
    /// The member's device completes its oldest write, at a test's word.
    Durable(u64),
    Propose(u64),
    Fence(u64),
    Change(u64, ConfChangeType, u64),
    Read(u64),
    Compact(u64),
    Crash(u64),
    Refuse(u64),
    Fail(u64),
    Resume(u64),
}

impl Op {
    /// The kind a played tape's step is named by: which operation it was, so a tape's mutations
    /// exchange, drop and repeat operations of one kind (`hyper_sim::tape`).
    fn kind(self) -> u8 {
        match self {
            Self::Suspect(..) => 0,
            Self::Trust(..) => 1,
            Self::World => 2,
            Self::Lose => 3,
            Self::Drive(_) => 4,
            Self::Durable(_) => 5,
            Self::Propose(_) => 6,
            Self::Fence(_) => 7,
            Self::Change(..) => 8,
            Self::Read(_) => 9,
            Self::Compact(_) => 10,
            Self::Crash(_) => 11,
            Self::Refuse(_) => 12,
            Self::Fail(_) => 13,
            Self::Resume(_) => 14,
        }
    }
}

/// An event of the group's world.
#[derive(Clone, Copy, Debug)]
pub enum Event {
    /// A message arrives.
    Arrival(Ticket),
    /// The member's device completes its oldest write out.
    Device(u64),
}

/// What the group reached, so a test proves it ran what it claims.
#[derive(Clone, Copy, Debug, Default)]
pub struct Reached {
    pub committed: u64,
    pub crashes: u64,
    pub refused: u64,
    pub stalls: u64,
    pub fenced: u64,
    pub behind_fence: u64,
    pub deep: u64,
    pub changes: u64,
    pub acted: u64,
    pub compactions: u64,
    pub events: u64,
    /// Drives after which a leader had applied past what its disk holds (§4.2).
    pub ahead: u64,
    /// Messages the schedule lost ([`Op::Lose`]).
    pub lost: u64,
    /// Messages lost to the network's capacity: fewer than those the schedule lost, or the bound
    /// is too small for the schedule (`docs/sim.md` §7).
    pub overflowed: u64,
    /// The most messages in flight at once.
    pub most_in_flight: u64,
    /// Reads a member answered: what the oracle of reads answered once a group settles judged.
    pub reads: u64,
}

impl Reached {
    /// Adds `other`'s counts.
    pub fn add(&mut self, other: &Self) {
        self.committed += other.committed;
        self.crashes += other.crashes;
        self.refused += other.refused;
        self.stalls += other.stalls;
        self.fenced += other.fenced;
        self.behind_fence += other.behind_fence;
        self.deep += other.deep;
        self.changes += other.changes;
        self.acted += other.acted;
        self.compactions += other.compactions;
        self.events += other.events;
        self.ahead += other.ahead;
        self.lost += other.lost;
        self.overflowed += other.overflowed;
        self.most_in_flight = self.most_in_flight.max(other.most_in_flight);
        self.reads += other.reads;
    }
}

/// hyper-check's oracles over one run.
#[derive(Clone)]
struct Judge {
    durable: Durability,
    machine: StateMachineSafety<Value>,
    election: ElectionSafety,
    matching: LogMatching<Value>,
    complete: LeaderCompleteness,
}

/// The drive's output buffers: scratch, so a fork's copy starts empty.
struct Scratch(Output<(u64, Vec<u8>)>);

impl Clone for Scratch {
    fn clone(&self) -> Self {
        Self(Output::default())
    }
}

#[derive(Clone)]
pub struct Cluster {
    pub shape: Shape,
    pub nodes: Vec<Node>,
    world: World<Event>,
    net: Net<Message>,
    /// The schedule's draws.
    ops: StreamId,
    /// The slow leader's draws: whether its device completes when the world picks it.
    slow: StreamId,
    devices: Devices,
    pub reached: Reached,
    pub seq: u64,
    judge: Judge,
    out: Scratch,
    /// The greatest commit any leader has counted: a commit is held to I3 when it is new to the
    /// group, since one already counted was held to it when it was.
    sent_commit: u64,
    /// The member the world's last event was for.
    last_event: Option<u64>,
    /// Whether the liveness phase runs: the network whole and every disk as fast as another's.
    settling: bool,
    /// The value an applied entry is judged by, refilled for each.
    scratch: Value,
    /// The reads a member took and has not answered, by their context: the member and the term it
    /// led when it took each. One whose member crashed or stopped leading that term is let go, as
    /// an owner tells its askers to ask the new leader.
    asked_reads: std::collections::BTreeMap<Vec<u8>, (u64, u64)>,
}

/// The most messages the network holds; past it the oldest is lost, as a network may lose any.
/// `docs/sim.md` §7's rule: the smallest capacity at which every shape loses fewer to it than to
/// the schedule over the gate's seeds (128 schedules of 5,000 steps), measured 2026-10-17: at 20
/// the five-voter shape lost 859 to the bound against 799 to the schedule, at 21 808 against 834
/// (`docs/sim.md` §17). Below 24 the world refused such runs before a lost message's arrival left
/// it with the message.
const NET: usize = 21;

/// The schedule's clock: a step is a millisecond.
const STEP_NS: u64 = 1_000_000;

/// Of a thousand words about a member that is down, how many are right; and of a thousand about
/// one that is up, how many are wrong: a detector errs both ways now and then, and the schedule is
/// safe whatever it says.
const ACCURATE: u64 = 900;

/// The steps a run takes at most: a schedule's own, and the liveness phase's rounds, each a drive,
/// the writes made durable and a drive for every member, and what the network holds delivered.
pub fn steps_bound(steps: u64, rounds: u64) -> u64 {
    steps + rounds * (4 * MEMBERS as u64 + NET as u64)
}

/// The trace a run keeps at most, in words: what a schedule's step draws, at most
/// [`WORDS_A_STEP`] a step, and each core's seed, two words a member. The liveness phase draws
/// nothing: its strategy is [`Fifo`], and it loses no message and slows no disk.
pub fn trace_bound(steps: u64, members: usize) -> usize {
    (steps * WORDS_A_STEP) as usize + 2 * members
}

/// The most words a schedule's step draws: the member and the roll, and two more at most: a
/// detector's word its peer and whether it is right; a change its target and its kind; the world's
/// next event the pick among two or more pending, and a slow leader's device whether it completes.
/// Nothing else a step does draws: the network neither delays, loses nor repeats on its own.
pub const WORDS_A_STEP: u64 = 4;

/// The timing the schedule's members elect by: a round and a span of half a second each, five
/// hundred of the schedule's millisecond steps, as the ticks before it waited ten ticks of fifty
/// steps each (a member ticked one step in fifty) and drew over ten more: a vote round, a
/// message delivered and a write made durable at the schedule's choice, takes about that many
/// steps.
pub fn timing() -> hyper_raft::Timing {
    hyper_raft::Timing {
        span: Duration::from_millis(500),
        round: Duration::from_millis(500),
        // A delay within the span and one vote round.
        election: Duration::from_millis(1_000),
    }
}

/// What the shell's tests state of a member of a group of `members` (`hyper_raft::Limits::derive`):
/// a message of a UDP datagram's 64 KiB, as the end-to-end members send theirs, far past the
/// appends of 256 bytes they send; queues of 16 MiB each, room no schedule here reaches; one
/// write out, which the shell raises to the store's depth.
pub fn limits(members: usize) -> hyper_raft::Limits {
    hyper_raft::Limits::derive(hyper_raft::Stated {
        message: 64 << 10,
        members,
        memory: 16 << 20,
        depth: 1,
    })
    .expect("the tests' statement gives their bounds")
}

/// The most members a group of these tests names: the largest shape's five.
pub const MEMBERS: usize = 5;

pub fn settings(id: u64, seed: u64) -> Settings {
    Settings {
        core: Config {
            max_size_per_msg: 256,
            max_inflight_msgs: 8,
            max_committed_size_per_ready: 512,
            check_quorum: true,
            pre_vote: true,
            seed: seed.wrapping_mul(31).wrapping_add(id),
            ..Config::new(id, limits(MEMBERS))
        },
        quiet: Duration::from_millis(50),
        elections: hyper_raft::Elections::Suspicion,
    }
}

fn shaped(id: u64, seed: u64, shape: &Shape) -> Settings {
    let mut s = settings(id, seed);
    s.core.apply_unpersisted = shape.ahead;
    s
}

fn waker() -> &'static Waker {
    Waker::noop()
}

/// The world's node of member `id`.
fn node_of(id: u64) -> NodeId {
    NodeId(u32::try_from(id - 1).expect("a member of these groups"))
}

/// A member's durable state as the oracles read it: its disk, and the index its state machine
/// holds durably, which states a commit as the log does (`docs/durable.md` §4.1: `C_d` is the
/// greater); an entry lost at rest after it was acknowledged held by its mark. Where an entry's
/// effect is what is judged (I4: applied only once durable here), the state machine holds what it
/// holds durably as well (`machine_holds`): an image it installed is durable in it before the
/// log's start moves past it.
struct View<'a> {
    disk: &'a Disk,
    machine: u64,
    machine_holds: bool,
    mark: Option<Point>,
}

impl DurableView<Value> for View<'_> {
    fn term(&self) -> u64 {
        self.disk.hard.term
    }
    fn vote(&self) -> u64 {
        self.disk.hard.vote
    }
    fn commit(&self) -> u64 {
        self.disk.hard.commit.max(self.machine)
    }
    fn start(&self) -> u64 {
        self.disk.start.index
    }
    fn last(&self) -> u64 {
        self.disk.last()
    }
    fn term_at(&self, index: u64) -> Option<u64> {
        self.disk.term(index)
    }
    fn holds(&self, index: u64, held: &Value) -> bool {
        let same = |entry: &Entry| {
            entry.term == held.0 && entry.entry_type as u8 == held.1 && entry.data == held.2
        };
        index <= self.disk.start.index
            || self.disk.entry(index).is_some_and(same)
            || self
                .disk
                .proposals
                .iter()
                .any(|entry| entry.index == index && same(entry))
            || self.mark.is_some_and(|mark| mark.index >= index)
            || (self.machine_holds && index <= self.machine)
    }
}

/// Member `id`'s durable state as the oracles read it, up or down.
fn view_of(nodes: &[Node], id: u64) -> View<'_> {
    let node = nodes.iter().find(|n| n.id == id).expect("a member");
    match (&node.replica, &node.down) {
        (Some(r), _) => View {
            disk: &r.core().store().log().disk,
            machine: r.machine().durable.applied.index,
            machine_holds: false,
            mark: r.core().store().log().mark,
        },
        (None, Some(down)) => View {
            disk: &down.disk,
            machine: down.kv.durable.applied.index,
            machine_holds: false,
            mark: None,
        },
        (None, None) => unreachable!("a member neither up nor down"),
    }
}

/// A leader's log, as Leader Completeness reads it.
struct LogOf {
    start: u64,
    entries: BTreeMap<u64, (u64, Value)>,
}

impl LogView<Value> for LogOf {
    fn start(&self) -> u64 {
        self.start
    }
    fn entry(&self, index: u64) -> Option<(u64, &Value)> {
        self.entries.get(&index).map(|(term, value)| (*term, value))
    }
}

/// `message` as it leaves its sender.
fn released(message: &Message) -> Released<'static, Value> {
    let kind = message.msg_type;
    let pre_vote = matches!(
        kind,
        MessageType::MsgRequestPreVote | MessageType::MsgRequestPreVoteResponse
    );
    let says = match kind {
        MessageType::MsgRequestVote => Says::VoteRequest {
            last_index: message.index,
            last_term: message.log_term,
        },
        MessageType::MsgRequestVoteResponse => Says::Vote {
            granted: !message.reject,
        },
        MessageType::MsgAppendResponse if !message.reject => Says::Acknowledges {
            index: message.index,
        },
        _ => Says::Nothing,
    };
    let commit = matches!(
        kind,
        MessageType::MsgAppendResponse | MessageType::MsgHeartbeatResponse
    )
    .then_some(message.commit);
    Released {
        from: message.from,
        to: message.to,
        term: (!pre_vote).then_some(message.term),
        says,
        commit,
    }
}

/// A run's oracle found a violation: the run fails, naming it as `at` says. Named only then: a
/// check that holds formats nothing.
fn judged(result: Result<(), Violation>, at: impl FnOnce() -> String) {
    if let Err(violation) = result {
        panic!("{}: {violation:?}", at());
    }
}

impl Cluster {
    /// A group drawing from `seed`, its devices completing at a test's word, for directed tests.
    pub fn new(shape: Shape, seed: u64) -> Self {
        Self::from_source(shape, Source::Seed(seed), Devices::Directed, 0, 0)
    }

    /// A group drawing from `source`, its devices completing as `devices` says, within the bounds
    /// of a schedule of `steps` and a liveness phase of `rounds`.
    pub fn from_source(
        shape: Shape,
        source: Source,
        devices: Devices,
        steps: u64,
        rounds: u64,
    ) -> Self {
        // A directed test states no schedule: its bounds are the gate's largest.
        let (steps, rounds) = if steps == 0 {
            (5_000, 4_000)
        } else {
            (steps, rounds)
        };
        let members = usize::try_from(shape.members).expect("a group of these tests");
        let mut world = World::new(
            source,
            Discipline::Free,
            Limits {
                // What the network holds, each message with its one arrival (a message the network
                // loses to its bound takes its arrival with it), and each device's completion.
                events: NET + members,
                nodes: members,
                // The schedule's two, each node's lateness and its core's seed, and each directed
                // pair's delay, loss and duplication.
                streams: 2 + 2 * members + 3 * members * members,
                steps: steps_bound(steps, rounds),
                trace_words: trace_bound(steps, members),
            },
        )
        .expect("the world's bounds");
        let ops = world.stream("schedule.ops", &[]).expect("a stream");
        let slow = world.stream("schedule.slow", &[]).expect("a stream");
        let mut net = Net::new(NetLimits {
            flows: members * members,
            links: 0,
            nats: 0,
            link_messages: 0,
            messages: NET,
            bytes: NET * ((64 << 10) + hyper_raft::wire::MESSAGE_RECORD_FIXED_BYTES),
        });
        net.set_path(Path::NONE);
        let configuration = ConfState {
            voters: (1..=shape.voters).collect(),
            ..ConfState::default()
        };
        let mut nodes = Vec::new();
        for id in 1..=shape.members {
            world.node(Clock::default()).expect("a node");
            let core = world.stream("node.seed", &[id]).expect("a stream");
            let seed = world.below(core, u64::MAX).expect("a draw");
            let mut kv = Kv::new(configuration.clone(), shape.volatile);
            kv.control = shape.control;
            let mut replica = Replica::open(
                &shaped(id, seed, &shape),
                SimStore::new(shape.depth),
                kv,
                Unbounded,
            )
            .expect("a member opens");
            replica.set_timing(timing()).expect("timing");
            nodes.push(Node {
                id,
                replica: Some(replica),
                down: None,
                acted_before_crash: Vec::new(),
                reopened: false,
                seed,
                checked: 0,
            });
        }
        // Every table holds at most an entry a step's words, and what the network holds.
        let bound = trace_bound(steps, members) + NET;
        Self {
            shape,
            nodes,
            world,
            net,
            ops,
            slow,
            devices,
            reached: Reached::default(),
            seq: 0,
            judge: Judge {
                durable: Durability::default(),
                machine: StateMachineSafety::new(Terms::Compared, bound),
                election: ElectionSafety::new(bound),
                matching: LogMatching::new(Terms::Compared, bound),
                complete: LeaderCompleteness::new(bound),
            },
            out: Scratch(Output::default()),
            sent_commit: 0,
            last_event: None,
            settling: false,
            scratch: (0, 0, Vec::new()),
            asked_reads: std::collections::BTreeMap::new(),
        }
    }

    /// A random schedule's group from `source`, its devices completing as the world's events,
    /// bounded for `steps` and a liveness phase of `rounds`.
    pub fn scheduled(shape: Shape, source: Source, steps: u64, rounds: u64) -> Self {
        Self::from_source(shape, source, Devices::World, steps, rounds)
    }

    /// One step of the schedule: whether it did something a crash after it would cut into, and
    /// the member whose event it was.
    pub fn step(&mut self) -> (bool, u64) {
        let op = self.choose(true);
        let did = self.act(op);
        // A step of a played tape closes with its operation (`Source::Tape`); otherwise nothing.
        self.world
            .end_step(op.kind())
            .expect("a step within the tape's bounds");
        (did, self.member_of(op, self.last_event))
    }

    /// The world's present: the schedule's clock.
    pub fn now(&self) -> u64 {
        self.world.now()
    }

    /// The clock moves on by `by` nanoseconds with nothing delivered.
    pub fn pass(&mut self, by: u64) {
        let to = self.world.now() + by;
        self.world
            .advance(to)
            .expect("the free discipline skips nothing");
    }

    /// Events pending in the world: messages in flight and devices' completions.
    pub fn pending(&self) -> usize {
        self.world.pending()
    }

    /// What the network did.
    pub fn net_stats(&self) -> NetStats {
        self.net.stats()
    }

    /// The trace's words so far.
    pub fn decisions(&self) -> usize {
        self.world.decisions()
    }

    /// The run's record: its digest, its trace and its steps.
    pub fn finish(self) -> Record {
        self.world.finish()
    }

    pub fn node(&mut self, id: u64) -> &mut Node {
        self.nodes
            .iter_mut()
            .find(|n| n.id == id)
            .expect("a member")
    }

    pub fn replica(&mut self, id: u64) -> Option<&mut Member> {
        self.node(id).replica.as_mut()
    }

    /// The disk of `id`, up or down.
    pub fn disk(&self, id: u64) -> &Disk {
        let node = self.nodes.iter().find(|n| n.id == id).expect("a member");
        match (&node.replica, &node.down) {
            (Some(r), _) => &r.core().store().log().disk,
            (None, Some(down)) => &down.disk,
            (None, None) => unreachable!("a member neither up nor down"),
        }
    }

    pub fn leader(&self) -> Option<u64> {
        self.nodes
            .iter()
            .filter_map(|n| n.replica.as_ref())
            .filter(|r| r.is_leader() && r.fenced().is_none())
            .max_by_key(|r| r.term())
            .map(|r| r.id())
    }

    fn draw(&mut self, bound: u64) -> u64 {
        self.world.below(self.ops, bound).expect("a draw")
    }

    /// One random step, drawn from the world's schedule stream.
    pub fn choose(&mut self, faults: bool) -> Op {
        let n = self.shape.members;
        let member = 1 + self.draw(n);
        let roll = self.draw(1000);
        let leader = self.leader().unwrap_or(member);
        match roll {
            0..100 => {
                let peer = 1 + self.draw(n);
                let down = self
                    .nodes
                    .iter()
                    .any(|n| n.id == peer && n.replica.is_none());
                let right = self.draw(1000) < ACCURATE;
                if down == right {
                    Op::Suspect(member, peer)
                } else {
                    Op::Trust(member, peer)
                }
            }
            // What the schedule this one replaced spent delivering and making writes durable:
            // the world picks among the messages in flight and the devices' completions.
            100..600 if self.world.pending() > 0 => Op::World,
            // What it spent losing a message in flight.
            600..620 if self.world.pending() > 0 && faults => Op::Lose,
            620..870 => Op::Drive(member),
            870..930 => Op::Propose(leader),
            930..940 => Op::Fence(leader),
            940..950 => {
                let target = 1 + self.draw(n);
                let kind = match self.draw(3) {
                    0 => ConfChangeType::AddNode,
                    1 => ConfChangeType::AddLearnerNode,
                    _ => ConfChangeType::RemoveNode,
                };
                Op::Change(leader, kind, target)
            }
            950..960 => Op::Read(leader),
            960..975 => Op::Compact(member),
            975..980 if faults => Op::Crash(member),
            980..990 if faults => Op::Refuse(member),
            990..994 if faults => Op::Fail(member),
            994..1000 => Op::Resume(member),
            _ => Op::Drive(member),
        }
    }

    /// Takes `op`; true when it did something a crash after it would cut into.
    pub fn act(&mut self, op: Op) -> bool {
        self.pass(STEP_NS);
        let did = match op {
            Op::Suspect(id, peer) => self.with(id, |r| r.suspect(peer)),
            Op::Trust(id, peer) => self.with(id, |r| r.trust(peer)),
            Op::World => self.step_world(&mut Random, &mut |_: &Message| true),
            Op::Lose => {
                let mut lost = 0u64;
                let did = self.step_world(&mut Random, &mut |_: &Message| {
                    lost += 1;
                    false
                });
                self.reached.lost += lost;
                did || lost > 0
            }
            Op::Drive(id) => self.drive(id),
            Op::Durable(id) => self.durable(id, false),
            Op::Propose(id) => {
                self.seq += 1;
                let data = format!("p:{}", self.seq).into_bytes();
                self.with(id, |r| r.propose(Vec::new(), data))
            }
            Op::Fence(id) => {
                self.seq += 1;
                let mut data = ACTS.to_vec();
                data.extend_from_slice(self.seq.to_string().as_bytes());
                self.with(id, |r| r.propose(Vec::new(), data))
            }
            Op::Change(id, kind, target) => self.change(id, kind, target),
            Op::Read(id) => {
                self.seq += 1;
                let context = self.seq.to_le_bytes().to_vec();
                let term = self.replica(id).map(|r| r.term());
                let taken = self.with(id, |r| r.read(context.clone()));
                if taken && let Some(term) = term {
                    self.asked_reads.insert(context, (id, term));
                }
                taken
            }
            Op::Compact(id) => self.compact(id),
            Op::Crash(id) => self.crash(id),
            Op::Refuse(id) => self.arm_fault(id, Fault::Room("the group's retained bound")),
            Op::Fail(id) => self.arm_fault(id, Fault::Failed("the device failed")),
            Op::Resume(id) => {
                let stalled = self.replica(id).is_some_and(|r| r.is_stalled());
                if let Some(r) = self.replica(id) {
                    r.resume();
                }
                stalled
            }
        };
        self.check_start();
        self.check_compactable();
        let (in_flight, _) = self.net.in_flight();
        self.reached.most_in_flight = self.reached.most_in_flight.max(in_flight as u64);
        did
    }

    /// The member an event of `op` was of: the one a crash right after it crashes.
    pub fn member_of(&self, op: Op, event: Option<u64>) -> u64 {
        match op {
            Op::Suspect(id, _)
            | Op::Trust(id, _)
            | Op::Drive(id)
            | Op::Durable(id)
            | Op::Propose(id)
            | Op::Fence(id)
            | Op::Change(id, ..)
            | Op::Read(id)
            | Op::Compact(id)
            | Op::Crash(id)
            | Op::Refuse(id)
            | Op::Fail(id)
            | Op::Resume(id) => id,
            Op::World | Op::Lose => event.unwrap_or(1),
        }
    }

    /// The world's next event as `strategy` picks it: an arrival is delivered unless `keep` refuses
    /// the message, which is then lost; a device's completion completes its oldest write.
    fn step_world(
        &mut self,
        strategy: &mut impl Strategy,
        keep: &mut impl FnMut(&Message) -> bool,
    ) -> bool {
        match self.world.next(strategy).expect("the world steps") {
            Step::Event {
                event: Event::Arrival(ticket),
                node,
            } => {
                self.last_event = Some(u64::from(node.0) + 1);
                let delivery = self
                    .net
                    .deliver(&mut self.world, ticket, Event::Arrival)
                    .expect("the network delivers");
                match delivery {
                    Some(delivery) if keep(&delivery.payload) => self.deliver(delivery.payload),
                    _ => false,
                }
            }
            Step::Event {
                event: Event::Device(id),
                ..
            } => {
                self.last_event = Some(id);
                self.fire(id)
            }
            Step::Wake { .. } | Step::Idle => false,
            Step::Spent => panic!("the run's step budget is spent: its bound is too small"),
        }
    }

    /// Calls `call` on a live member; a refusal changed nothing; a fence is the oracle's to see
    /// at the member's next drive.
    fn with(
        &mut self,
        id: u64,
        call: impl FnOnce(&mut Member) -> Result<(), ReplicaError>,
    ) -> bool {
        let Some(r) = self.replica(id) else {
            return false;
        };
        match call(r) {
            Ok(()) => true,
            Err(ReplicaError::Fenced(Cause::Unwound)) => panic!("member {id} unwound"),
            Err(ReplicaError::Fenced(Cause::Invariant(why))) => {
                panic!("member {id} broke the shell's bookkeeping: {why}")
            }
            Err(ReplicaError::Fenced(Cause::Core(e))) => {
                panic!("member {id}: the core failed: {e}")
            }
            Err(_) => false,
        }
    }

    fn change(&mut self, id: u64, kind: ConfChangeType, target: u64) -> bool {
        let voters = self
            .replica(id)
            .map_or(0, |r| r.configuration().voters.len());
        if kind == ConfChangeType::RemoveNode && voters <= 2 {
            return false;
        }
        let change = ConfChangeV2 {
            transition: ConfChangeTransition::Auto,
            changes: vec![ConfChangeSingle {
                change_type: kind,
                node_id: target,
            }],
            context: Vec::new(),
        };
        let done = self.with(id, |r| r.change(Vec::new(), &change));
        if done {
            self.reached.changes += 1;
        }
        done
    }

    /// `message` arrives at its member, if it is up.
    fn deliver(&mut self, message: Message) -> bool {
        let to = message.to;
        let before = self
            .replica(to)
            .map(|r| (r.core().raft.configuration().clone(), r.is_leader()));
        let stepped = self.with(to, |r| r.step(message));
        if let Some((before, led)) = before {
            self.check_leader_commit(to, &before, led);
        }
        stepped
    }

    /// Sends what `from` released on the world's network.
    fn send(&mut self, from: u64, message: Message) {
        let to = message.to;
        // A configuration may name a member this group never had: no node, no delivery.
        if to == 0 || to > self.shape.members {
            return;
        }
        let bytes = message.encoded_len();
        self.net
            .send(
                &mut self.world,
                (node_of(from), node_of(to)),
                message,
                bytes,
                Event::Arrival,
            )
            .expect("the network takes the message");
    }

    /// I3, as a leader's commit moves: the entry it now counts committed is durable on a
    /// majority of each half of the configuration it decided it by, the one before the step or one
    /// a change applied in it. A commit already counted by some leader was held to it then.
    fn check_leader_commit(&mut self, id: u64, before: &Configuration, led: bool) {
        let Some(r) = self
            .nodes
            .iter()
            .find(|n| n.id == id)
            .and_then(|n| n.replica.as_ref())
        else {
            return;
        };
        // A leader may commit and step down in one step: a change that removed it, applied.
        if !r.is_leader() && !led {
            return;
        }
        let commit = r.core().raft.log().committed();
        if commit <= self.sent_commit {
            return;
        }
        self.sent_commit = commit;
        self.check_commit(id, commit, before);
    }

    /// Arms `fault` on the next write `id`'s device completes, while one is out.
    fn arm_fault(&mut self, id: u64, fault: Fault) -> bool {
        let Some(r) = self.replica(id) else {
            return false;
        };
        // The store is the replica's; the harness reaches it as the device's fault injection
        // reaches a log's file.
        let store = store_of(r);
        if store.pending() == 0 || store.refuse.is_some() {
            return false;
        }
        store.refuse = Some(fault);
        true
    }

    /// `id`'s device completes its oldest write: at a test's word, or as the world's event
    /// (`fired`). I7 and Log Matching on what it left.
    fn durable(&mut self, id: u64, fired: bool) -> bool {
        let Some(r) = self.replica(id) else {
            return false;
        };
        let store = store_of(r);
        let before = store.events.refused;
        let last = store.disk.last();
        let made = if fired {
            store.fire()
        } else {
            store.make_durable()
        };
        if store.events.refused > before {
            self.reached.refused += 1;
        }
        if made {
            // I7 is of the log's own statement: the state machine's durable point is not in it.
            let view = View {
                machine: 0,
                ..view_of(&self.nodes, id)
            };
            judged(self.judge.durable.written(id, &view), || {
                format!("member {id}'s write")
            });
            self.match_log(id, last);
        }
        made
    }

    /// Log Matching on `id`'s disk past what it held through `from` before a write: the entries
    /// the write holds, each with the term before it.
    fn match_log(&mut self, id: u64, from: u64) {
        let disk = view_of(&self.nodes, id).disk;
        let first = from.min(disk.last()).max(disk.start.index) + 1;
        let written: Vec<(u64, u64, Value, u64)> = (first..=disk.last())
            .filter_map(|index| {
                let entry = disk.entry(index)?;
                let before = disk.term(index - 1).unwrap_or(0);
                Some((
                    index,
                    entry.term,
                    value(entry.term, entry.entry_type, &entry.data),
                    before,
                ))
            })
            .collect();
        for (index, term, held, before) in written {
            judged(
                self.judge.matching.holds(id, index, term, &held, before),
                || format!("member {id}'s log"),
            );
        }
    }

    /// The world took `id`'s device's completion. A leader applying ahead has the slowest disk of
    /// its group: its device completes a quarter as often as another's, the world's pick drawn
    /// again three times in four.
    fn fire(&mut self, id: u64) -> bool {
        if self.shape.ahead && !self.settling && self.leader() == Some(id) {
            let slow = self.world.below(self.slow, 4).expect("a draw");
            if slow != 0 {
                let now = self.world.now();
                self.world
                    .schedule(now, node_of(id), Event::Device(id))
                    .expect("the device's completion goes back");
                return false;
            }
        }
        let node = self.node(id);
        if node.replica.is_none() {
            // Its process is gone; the device's queue lost what was out, and the completion
            // finds nothing.
            if let Some(down) = node.down.as_mut() {
                down.writes
                    .fire(|_| unreachable!("a crashed device holds no write out"));
            }
            return false;
        }
        let made = self.durable(id, true);
        self.arm(id);
        made
    }

    /// Gives the world `id`'s device's next completion, where devices complete as the world's
    /// events and a write is out.
    fn arm(&mut self, id: u64) {
        if self.devices != Devices::World {
            return;
        }
        let Self { world, nodes, .. } = self;
        if let Some(r) = nodes
            .iter_mut()
            .find(|n| n.id == id)
            .and_then(|n| n.replica.as_mut())
        {
            store_of(r)
                .arm(world, node_of(id), || Event::Device(id))
                .expect("the world holds the device's completion");
        }
    }

    fn compact(&mut self, id: u64) -> bool {
        let now = self.world.now();
        let Some(r) = self.replica(id) else {
            return false;
        };
        let compacted = match r.compact(2, now, waker()) {
            Ok(true) => {
                self.reached.compactions += 1;
                true
            }
            Ok(false) => false,
            Err(ReplicaError::Fenced(Cause::Write(_))) => true,
            Err(e) => panic!("member {id}: compact: {e}"),
        };
        self.arm(id);
        compacted
    }

    /// Loses power: the writes not durable, and what the state machine applied past its durable
    /// point.
    pub fn crash(&mut self, id: u64) -> bool {
        self.asked_reads.retain(|_, (member, _)| *member != id);
        let node = self.node(id);
        let Some(mut replica) = node.replica.take() else {
            return false;
        };
        let store = store_of(&mut replica);
        let disk = store.disk.clone();
        let writes = store.crash();
        let kv = replica.machine().clone();
        node.acted_before_crash.extend(kv.acted.iter().copied());
        node.down = Some(Down {
            disk,
            kv: kv.crashed(),
            writes,
        });
        self.reached.crashes += 1;
        true
    }

    /// A fault at rest on `id`'s log: it loses power, and its last `lose` entries above what its
    /// state machine holds durably are gone though written and acknowledged, their persist
    /// record kept (hyper-log's mark, `docs/durable.md` §5). It reopens marked through what it
    /// held. The mark, if any.
    pub fn rot(&mut self, id: u64, lose: u64) -> Option<Point> {
        self.crash(id);
        let node = self.node(id);
        let down = node.down.as_mut()?;
        let disk = &mut down.disk;
        let last = disk.last();
        let mark = Point {
            index: last,
            term: disk.term(last).unwrap_or(0),
        };
        let floor = down.kv.durable.applied.index.max(disk.start.index);
        let cut = lose.min(last.saturating_sub(floor)) as usize;
        let keep = disk.entries.len() - cut;
        disk.entries.truncate(keep);
        disk.hard.commit = disk.hard.commit.min(disk.last());
        let marked = (cut > 0).then_some(mark);
        self.reopen(id, marked);
        marked
    }

    /// Opens a member that is down on what its crash left, and drives it alone until it applies
    /// nothing more: I5 across the crash, every entry it acted on at start applied again from its
    /// own durable state before it hears from anyone.
    pub fn restart(&mut self, id: u64) {
        self.reopen(id, None);
    }

    /// [`Cluster::restart`], its store reporting `mark`.
    fn reopen(&mut self, id: u64, mark: Option<Point>) {
        let depth = self.shape.depth;
        let shape = self.shape;
        let now = self.world.now();
        let node = self.node(id);
        let Some(Down { disk, kv, writes }) = node.down.take() else {
            return;
        };
        // What its state machine holds durably it applied before the crash, and was held to the
        // oracles then; what it applies again from its log is held to them again.
        node.checked = kv.now.applied.index;
        let mut store = SimStore::on_device(disk, depth, writes);
        store.mark = mark;
        let mut replica = Replica::open(&shaped(id, node.seed, &shape), store, kv, Unbounded)
            .unwrap_or_else(|e| panic!("member {id} does not reopen: {e}"));
        replica.set_timing(timing()).expect("timing");
        let mut out = Output::default();
        for _ in 0..64 {
            let before = replica.applied();
            out.clear();
            replica
                .drive(now, waker(), &mut out)
                .expect("a reopened member drives");
            while store_of(&mut replica).make_durable() {}
            out.clear();
            replica
                .drive(now, waker(), &mut out)
                .expect("a reopened member drives");
            if replica.applied() == before {
                break;
            }
        }
        let applied = replica.applied().index;
        let acted = std::mem::take(&mut node.acted_before_crash);
        let reopened = &replica.machine().now;
        for index in acted {
            assert!(
                index <= applied && reopened.entries.iter().any(|(i, _, _)| *i == index),
                "member {id}: acted at start on {index}, but reopened applying only through {applied}"
            );
        }
        node.replica = Some(replica);
        node.reopened = true;
        self.check_applied(id, None);
        self.arm(id);
        // The others' detectors see its new incarnation.
        let others: Vec<u64> = self
            .nodes
            .iter()
            .filter(|n| n.id != id && n.replica.is_some())
            .map(|n| n.id)
            .collect();
        for other in others {
            self.with(other, |r| r.restarted(id));
        }
    }

    /// Drives `id` once and holds what it gave out to the oracles.
    pub fn drive(&mut self, id: u64) -> bool {
        let now = self.world.now();
        let mut out = std::mem::take(&mut self.out.0);
        out.clear();
        let Some(r) = self.replica(id) else {
            self.out.0 = out;
            return false;
        };
        let events = store_of(r).events;
        let configuration = r.core().raft.configuration().clone();
        let led = r.is_leader();
        let term = r.term();
        let driven = r.drive(now, waker(), &mut out);
        let after = store_of(r).events;
        let behind = r.behind_fence().is_some();
        let deep = r.in_flight() > 1;
        let stalled = r.is_stalled();
        match driven {
            Ok(_) => {}
            Err(ReplicaError::Fenced(Cause::Write(_))) => {
                // A failed write: the owner reopens the member from what is durable.
                self.reached.fenced += 1;
                self.out.0 = out;
                self.crash(id);
                self.restart(id);
                return true;
            }
            Err(e) => panic!("member {id}: drive: {e}"),
        }
        self.reached.behind_fence += u64::from(behind);
        self.reached.deep += u64::from(deep);
        self.reached.stalls += u64::from(stalled);
        self.check_output(id, &out, &configuration, led);
        self.check_applied(id, led.then_some(term));
        self.check_leadership(id);
        for (context, _) in &out.reads {
            let asked = self.asked_reads.remove(context);
            assert!(
                asked.is_some_and(|(member, _)| member == id),
                "member {id} answered a read it was not asked: {context:?} (asked of {asked:?})"
            );
            self.reached.reads += 1;
        }
        let did = after != events || !out.messages.is_empty() || !out.answers.is_empty();
        for m in out.messages.drain(..) {
            self.send(id, m);
        }
        self.arm(id);
        self.reached.events += u64::from(did);
        self.out.0 = out;
        did
    }

    /// I1, I2 and R-6 on what `id` released, against its durable state as it is now; I3 on a
    /// commit it counted. A commit is held to the configuration the leader decided it by: the one
    /// before the drive, or one a change applied in it.
    fn check_output(
        &mut self,
        id: u64,
        out: &Output<(u64, Vec<u8>)>,
        before: &Configuration,
        led: bool,
    ) {
        for m in &out.messages {
            let view = view_of(&self.nodes, id);
            judged(self.judge.durable.released(&view, &released(m)), || {
                format!("member {id}'s {:?}", m.msg_type)
            });
        }
        self.check_leader_commit(id, before, led);
    }

    /// I3: the entry at `commit` is durable on a majority of each half of the configuration the
    /// leader decided it by.
    fn check_commit(&mut self, id: u64, commit: u64, before: &Configuration) {
        let leader = self
            .nodes
            .iter()
            .find(|n| n.id == id)
            .and_then(|n| n.replica.as_ref())
            .expect("up");
        let log = leader.core().raft.log();
        // Through a step that left it no leader, its commit is still the one it decided.
        let Ok(entries) = log.slice(commit, commit + 1, u64::MAX) else {
            return;
        };
        let Some(entry) = entries.first() else {
            return;
        };
        let held = value(entry.term, entry.entry_type, &entry.data);
        let after = leader.core().raft.configuration();
        // The leader counts by the newest configuration its log states, from the entry on
        // (`docs/raft.md` §3.4): it decided `commit` by the one its log stated through its last
        // entry then, at or past `commit`. Those are folded from the configuration it applied,
        // along the entries it has not applied (a leader's log is never cut short).
        let raft = &leader.core().raft;
        let mut stated = raft.applied_configuration().clone();
        let mut decided_by = vec![before.clone(), after.clone()];
        let applied = log.applied();
        let last = log.last_index().expect("the leader's last entry");
        if applied >= commit {
            decided_by.push(stated.clone());
        }
        let unapplied = log
            .slice(applied + 1, last + 1, u64::MAX)
            .expect("the leader's entries past its applied index");
        for entry in &unapplied {
            if let Some(plan) =
                hyper_raft::proto::Plan::of_entry(entry).expect("a change that decodes")
                && let Ok(changed) = plan.apply(&stated)
            {
                stated = changed.configuration;
            }
            if entry.index >= commit {
                decided_by.push(stated.clone());
            }
        }
        let halves: Vec<[&[u64]; 2]> = decided_by
            .iter()
            .map(|c| [c.voters(), c.outgoing()])
            .collect();
        let configurations: Vec<&[&[u64]]> = halves.iter().map(|h| &h[..]).collect();
        let views: Vec<(u64, View<'_>)> = self
            .nodes
            .iter()
            .map(|n| (n.id, view_of(&self.nodes, n.id)))
            .collect();
        let found = |voter: u64| {
            views
                .iter()
                .find(|(v, _)| *v == voter)
                .map(|(_, view)| view)
        };
        let result = self
            .judge
            .durable
            .committed(id, commit, &held, &configurations, found);
        judged(result, || format!("leader {id}'s commit {commit}"));
    }

    /// I4, I5 and State Machine Safety on what `id` applied since it was last held to them; `led`
    /// the term it led as the drive that applied them began, if it led: a leader applying ahead
    /// may apply its own entries, of that term or of the one it leads now, before its write of them
    /// is durable.
    fn check_applied(&mut self, id: u64, led: Option<u64>) {
        let Some(r) = self
            .nodes
            .iter_mut()
            .find(|n| n.id == id)
            .and_then(|n| n.replica.as_mut())
        else {
            return;
        };
        // Its room kept: the machine's list is given back emptied.
        let mut fenced = std::mem::take(&mut store_machine(r).fenced_applied);
        let Self {
            nodes,
            judge,
            world,
            reached,
            shape,
            scratch,
            ..
        } = self;
        let view = View {
            machine_holds: true,
            ..view_of(nodes, id)
        };
        for &index in &fenced {
            judged(judge.durable.fenced(id, &view, index, view.machine), || {
                format!("member {id} applied {index}")
            });
        }
        let node = nodes.iter().find(|n| n.id == id).expect("a member");
        let r = node.replica.as_ref().expect("up");
        let machine = r.machine();
        let committed = r.core().raft.log().committed();
        let leads = r.is_leader().then(|| r.term());
        let ahead = leads.is_some() && machine.now.applied.index > view.disk.last();
        for (index, term, data) in machine
            .now
            .entries
            .iter()
            .filter(|(i, ..)| *i > node.checked)
        {
            // One value refilled for every entry: what the oracles keep, they copy.
            scratch.0 = *term;
            scratch.1 = EntryType::EntryNormal as u8;
            scratch.2.clear();
            scratch.2.extend_from_slice(data);
            let own_ahead = shape.ahead && (led == Some(*term) || leads == Some(*term));
            judged(
                judge
                    .durable
                    .applied(id, &view, *index, scratch, committed, own_ahead),
                || format!("member {id} applied {index}"),
            );
            if judge.machine.at(*index).is_none() {
                reached.committed += 1;
                world.observe(*index);
                world.observe(*term);
            }
            judged(judge.machine.committed(id, *index, *term, scratch), || {
                format!("member {id} applied {index}")
            });
        }
        reached.ahead += u64::from(ahead);
        let applied = machine.now.applied.index;
        fenced.clear();
        let node = nodes.iter_mut().find(|n| n.id == id).expect("a member");
        node.checked = node.checked.max(applied);
        if let Some(r) = node.replica.as_mut() {
            store_machine(r).fenced_applied = fenced;
        }
    }

    /// Election Safety on `id` if it leads, and the first time it leads a term, Leader
    /// Completeness on its log.
    fn check_leadership(&mut self, id: u64) {
        let Some(r) = self
            .nodes
            .iter()
            .find(|n| n.id == id)
            .and_then(|n| n.replica.as_ref())
        else {
            return;
        };
        if r.core().raft.state() != StateRole::Leader {
            return;
        }
        let term = r.term();
        let raft = &r.core().raft;
        let log = raft.log();
        let committed = log.committed();
        // Its log is held to what was committed before only the first time it is seen leading the
        // term: Election Safety knows whether it has been.
        let seen = self.judge.election.leader_of(term) == Some(id);
        judged(self.judge.election.leader(id, term), || {
            format!("member {id} led term {term}")
        });
        judged(self.judge.complete.committed_in(committed, term), || {
            format!("member {id} committed in term {term}")
        });
        if seen {
            return;
        }
        let first = log.first_index().expect("the leader's first index");
        let last = log.last_index().expect("the leader's last index");
        let held = LogOf {
            start: first - 1,
            entries: log
                .slice(first, last + 1, u64::MAX)
                .expect("the leader's log")
                .iter()
                .map(|e| (e.index, (e.term, value(e.term, e.entry_type, &e.data))))
                .collect(),
        };
        let result = self
            .judge
            .complete
            .leader(id, term, &held, &self.judge.machine);
        judged(result, || format!("member {id} leading term {term}"));
    }

    /// The compaction rule's count for every live member with no start out (R22,
    /// `docs/durable.md` §6.1): what it kept as it applied, compacted and installed is what a walk
    /// of its log counts, the applied entries past the start, where the store holds them and,
    /// past them, where the core does. A start out, or an install the core holds not yet
    /// durable (one refused waits to be made again), moves the start the count is from.
    fn check_compactable(&self) {
        for node in &self.nodes {
            let Some(r) = &node.replica else {
                continue;
            };
            let store = r.core().store().log();
            if store.start_out() || r.core().raft.log().unstable().snapshot().is_some() {
                continue;
            }
            let log = store.answered();
            let applied = r.applied().index;
            let unstable = r.core().raft.log().unstable().entries();
            let tail = unstable.first().map_or(u64::MAX, |e| e.index);
            let bytes = |e: &hyper_raft::proto::Entry| EntryRef::of(e).encoded_bytes();
            let held: u64 = log
                .entries
                .iter()
                .filter(|e| e.index > log.start.index && e.index <= applied && e.index < tail)
                .map(bytes)
                .sum::<u64>()
                + unstable
                    .iter()
                    .filter(|e| e.index > log.start.index && e.index <= applied)
                    .map(bytes)
                    .sum::<u64>();
            assert_eq!(
                r.compactable_bytes(),
                held,
                "member {}: what the rule counts against a walk of the log, applied {applied}",
                node.id
            );
        }
    }

    /// I8 for every member, up or down.
    fn check_start(&mut self) {
        for at in 0..self.nodes.len() {
            let id = self.nodes[at].id;
            let view = view_of(&self.nodes, id);
            let machine = view.machine;
            judged(self.judge.durable.start(id, &view, machine), || {
                format!("member {id}'s log")
            });
        }
    }

    /// Brings every member up, the network whole and every disk as fast as another's, for the rest
    /// of the run, then runs until a leader commits an entry every member of its configuration
    /// applies: false when it never does within `rounds`.
    pub fn settles(&mut self, rounds: usize) -> bool {
        self.net.set_path(Path::NONE);
        self.settling = true;
        let ids: Vec<u64> = self.nodes.iter().map(|n| n.id).collect();
        for &id in &ids {
            if self.node(id).replica.is_none() {
                self.restart(id);
            }
            if let Some(r) = self.replica(id) {
                store_of(r).refuse = None;
                r.resume();
            }
        }
        // Every member up and the network whole: the detectors trust every member.
        for &id in &ids {
            for &peer in &ids {
                if peer != id {
                    self.with(id, |r| r.trust(peer));
                }
            }
        }
        let mut proposed: Option<Vec<u8>> = None;
        for round in 0..rounds {
            // The log has room: a refusal the schedule armed before it settled, taken by the
            // replica only now, stalls it, and the owner says room was freed, as it does once a
            // compaction or a quiet queue frees it.
            for &id in &ids {
                if let Some(r) = self.replica(id)
                    && r.is_stalled()
                {
                    r.resume();
                }
            }
            self.round(&ids);
            let Some(leader) = self.leader() else {
                continue;
            };
            if proposed.is_none() || round % 50 == 0 {
                self.seq += 1;
                let data = format!("settle:{}", self.seq).into_bytes();
                if self.with(leader, |r| r.propose(Vec::new(), data.clone())) {
                    proposed = Some(data);
                }
            }
            if let Some(data) = &proposed
                && self.applied_by_all(leader, data)
            {
                self.check_reads_answered();
                return true;
            }
        }
        false
    }

    /// Every read a member took is answered by the time its group settles, unless the member
    /// crashed or stopped leading the term it took the read in: a leader that keeps leading
    /// confirms what it took, its group whole and its writes durable.
    fn check_reads_answered(&mut self) {
        let ids: Vec<u64> = self.nodes.iter().map(|n| n.id).collect();
        let leading: Vec<(u64, u64)> = ids
            .into_iter()
            .filter_map(|id| {
                let r = self.replica(id)?;
                r.is_leader().then(|| (id, r.term()))
            })
            .collect();
        let held: Vec<(Vec<u8>, u64, u64)> = self
            .asked_reads
            .iter()
            .filter(|(_, (member, term))| leading.contains(&(*member, *term)))
            .map(|(context, (member, term))| (context.clone(), *member, *term))
            .collect();
        for (_, member, _) in &held {
            let r = self.replica(*member).expect("a leader is up");
            let (confirmed, core) = (r.reads_held(), r.core().raft.reads_held());
            eprintln!(
                "member {member}: {confirmed} reads confirmed and waiting to apply, {core} held by \
                 its core, applied {}, commit {}, last {}, term committed {}",
                r.applied().index,
                r.core().raft.log().committed(),
                r.core().raft.log().last_index().unwrap_or(0),
                r.core().raft.commit_to_current_term(),
            );
        }
        assert!(
            held.is_empty(),
            "reads a leader took in its term and still leads, unanswered once the group settled: \
             {held:?}"
        );
    }

    /// A round with no faults: every member drives, makes its writes durable and drives again,
    /// and every event pending in the world is taken in the order it came.
    pub fn round(&mut self, ids: &[u64]) {
        for &id in ids {
            self.act(Op::Drive(id));
            while self.durable(id, false) {}
            self.act(Op::Drive(id));
        }
        self.deliver_all(|_| true);
    }

    /// Takes every event pending in the world, in the order it came, a millisecond each: each
    /// message delivered unless `keep` refuses it, then lost, and each device's completion made.
    pub fn deliver_all(&mut self, mut keep: impl FnMut(&Message) -> bool) {
        while self.world.pending() > 0 {
            self.pass(STEP_NS);
            self.step_world(&mut Fifo, &mut keep);
            self.check_start();
            self.check_compactable();
        }
    }

    fn applied_by_all(&mut self, leader: u64, data: &[u8]) -> bool {
        let Some(r) = self.replica(leader) else {
            return false;
        };
        let configuration = r.configuration().clone();
        let members: Vec<u64> = configuration
            .voters
            .iter()
            .chain(&configuration.learners)
            .copied()
            .collect();
        members.iter().all(|&m| {
            self.nodes
                .iter()
                .find(|n| n.id == m)
                .and_then(|n| n.replica.as_ref())
                .is_some_and(|r| r.machine().now.entries.iter().any(|(_, _, d)| d == data))
        })
    }

    pub fn acted(&self) -> u64 {
        self.nodes
            .iter()
            .filter_map(|n| n.replica.as_ref())
            .map(|r| r.machine().acted.len() as u64)
            .sum()
    }

    /// What the run reached, the network's losses with it.
    pub fn reached(&self) -> Reached {
        let net = self.net.stats();
        Reached {
            overflowed: net.dropped_capacity,
            ..self.reached
        }
    }
}

/// The store a replica writes through, for the harness to make its writes durable, as a
/// device's controller would.
pub fn store_of(replica: &mut Member) -> &mut SimStore {
    replica.log_mut()
}

fn store_machine(replica: &mut Member) -> &mut Kv {
    replica.machine_mut()
}

/// Whether `id` leads.
pub fn leads(r: &Member) -> bool {
    r.core().raft.state() == StateRole::Leader
}

/// `steps` of the schedule from `source` and its liveness phase, as the world records them: what
/// the run-twice check compares.
fn recorded(shape: Shape, source: Source, steps: u64, rounds: usize) -> Result<Record, String> {
    let mut group = Cluster::scheduled(shape, source, steps, rounds as u64);
    for _ in 0..steps {
        group.step();
    }
    if !group.settles(rounds) {
        return Err(format!("the group did not settle: {:?}", group.reached()));
    }
    Ok(group.finish())
}

/// The bounds of a played tape of a `steps` schedule of `members`: a step an operation, its words
/// at most the trace's (`docs/sim.md` §18).
pub fn tape_bounds(steps: u64, members: usize) -> Bounds {
    Bounds {
        steps: usize::try_from(steps).expect("steps a host holds"),
        words: trace_bound(steps, members),
    }
}

/// The schedule played from `tape`, its words past a step's own from `filler`, run and settled: its
/// record, whose tape replays it (`docs/sim.md` §18).
pub fn played(
    shape: Shape,
    tape: Tape,
    filler: u64,
    steps: u64,
    rounds: usize,
) -> Result<Record, String> {
    let bounds = tape_bounds(steps, shape.members as usize);
    recorded(
        shape,
        Source::Tape {
            tape,
            filler,
            bounds,
        },
        steps,
        rounds,
    )
}

/// The schedule from `seed` run twice from its seed and once from its first run's trace, to one
/// digest (`docs/sim.md` §3.9): the first run's record.
pub fn runs_the_same(
    shape: Shape,
    seed: u64,
    steps: u64,
    rounds: usize,
) -> Result<Record, Twice<String>> {
    twice(seed, |source| recorded(shape, source, steps, rounds))
}
