//! A group of replicas on [`SimStore`]s, driven one step at a time by a seeded schedule, and the
//! oracle every step is held to.
//!
//! Every step is one of: a word of a member's failure detectors about another (suspected, or
//! trusted again), a delivery or loss of a message, a drive of a member, a write of a member made
//! durable (or refused, or failed), a proposal, a change of configuration, a read, a compaction, a
//! crash. Members elect by suspicion (timing step L-2) and take no ticks: the clock moves a
//! millisecond a step, and a drive wakes the member at it. A crash loses every write not durable
//! and the state machine's applied state past its durable point; the member reopens on what is
//! left, and the others' detectors see its new incarnation.
//!
//! The oracle, against each member's durable state `D` (its store's disk) when an output leaves
//! or the step ends (`docs/durable.md` §3):
//! - **I1**: a message carries no term `D` does not hold; a vote request or a vote leaves only
//!   with `D` holding that vote.
//! - **I2**: an acknowledgement of index `i` leaves only with `D` holding the entry at `i` with
//!   the term the member's log has there.
//! - **I3**: a commit a leader sends is durable on a majority of each half of its configuration.
//! - **I4**: every member applies the same entry at an index.
//! - **I5**: a change of configuration, and an entry acted on at start, is applied only with the
//!   durable commit (the log's stated commit, or the state machine's durable index) covering it;
//!   and across a crash, every entry acted on at start is applied again from what the member
//!   reopens with, before it hears from anyone.
//! - **I7**: once a write is durable, the commit `D` states is an entry `D` holds.
//! - **I8**: `D` never starts past the state machine's durable index.
//!
//! Crashing after a drive rather than inside it is the harder case for every event inside it:
//! nothing durable changes within a drive (writes become durable only at the schedule's steps),
//! and what the drive released has left, for the others to act on.
use std::collections::{BTreeMap, VecDeque};
use std::task::Waker;
use std::time::Duration;

use hyper_durable::{Cause, Fault, Output, Point, Replica, ReplicaError, Settings, Unbounded};
use hyper_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Message,
    MessageType,
};
use hyper_raft::{Config, Configuration, StateRole};

use super::{ACTS, Disk, Kv, Seeded, SimStore};

pub type Member = Replica<SimStore, Kv, Unbounded>;

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

/// One member: its replica while up, and what a crash left while down.
pub struct Node {
    pub id: u64,
    pub replica: Option<Member>,
    /// While down: the disk and the state machine a restart opens on.
    pub down: Option<(Disk, Kv)>,
    /// Every entry acted on at start before the last crash, to hold against what it reopens with.
    pub acted_before_crash: Vec<u64>,
    /// Whether a crash left it to reopen alone first.
    pub reopened: bool,
}

/// A step of the schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// The first member's detectors suspect the second.
    Suspect(u64, u64),
    /// The first member's detectors trust the second again.
    Trust(u64, u64),
    Deliver(usize),
    Lose(usize),
    Drive(u64),
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
}

pub struct Cluster {
    pub shape: Shape,
    pub nodes: Vec<Node>,
    pub net: VecDeque<Message>,
    /// The entry every member applied at each index: term and data.
    pub chosen: BTreeMap<u64, (u64, Vec<u8>)>,
    /// The schedule's clock, nanoseconds: a step is a millisecond.
    pub now: u64,
    pub reached: Reached,
    pub seq: u64,
    out: Output<(u64, Vec<u8>)>,
    seed: u64,
    /// The greatest commit any leader has counted: a commit is held to I3 when it is new to the
    /// group, since one already counted was held to it when it was.
    sent_commit: u64,
}

/// The most messages the network holds; past it the oldest is lost, as a network may lose any.
const NET: usize = 4096;

/// Of a thousand words about a member that is down, how many are right; and of a thousand about
/// one that is up, how many are wrong: a detector errs both ways now and then, and the schedule is
/// safe whatever it says.
const ACCURATE: u64 = 900;

/// The timing the schedule's members elect by: a round and a span of half a second each, five
/// hundred of the schedule's millisecond steps, as the ticks before it waited ten ticks of fifty
/// steps each (a member ticked one step in fifty) and drew over ten more: a vote round, a
/// message delivered and a write made durable at the schedule's choice, takes about that many
/// steps.
pub fn timing() -> hyper_raft::Timing {
    hyper_raft::Timing {
        span: Duration::from_millis(500),
        round: Duration::from_millis(500),
    }
}

pub fn settings(id: u64, seed: u64) -> Settings {
    Settings {
        core: Config {
            max_size_per_msg: 256,
            max_inflight_msgs: 8,
            max_committed_size_per_ready: 512,
            check_quorum: true,
            pre_vote: true,
            seed: seed.wrapping_mul(31).wrapping_add(id),
            ..Config::new(id)
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

impl Cluster {
    pub fn new(shape: Shape, seed: u64) -> Self {
        let configuration = ConfState {
            voters: (1..=shape.voters).collect(),
            ..ConfState::default()
        };
        let nodes = (1..=shape.members)
            .map(|id| {
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
                Node {
                    id,
                    replica: Some(replica),
                    down: None,
                    acted_before_crash: Vec::new(),
                    reopened: false,
                }
            })
            .collect();
        Self {
            shape,
            nodes,
            net: VecDeque::new(),
            chosen: BTreeMap::new(),
            now: 0,
            reached: Reached::default(),
            seq: 0,
            out: Output::default(),
            seed,
            sent_commit: 0,
        }
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
            (None, Some((disk, _))) => disk,
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

    /// One random step.
    pub fn choose(&mut self, rng: &mut Seeded, faults: bool) -> Op {
        let n = self.shape.members;
        let member = 1 + rng.below(n);
        let roll = rng.below(1000);
        let leader = self.leader().unwrap_or(member);
        match roll {
            0..100 => {
                let peer = 1 + rng.below(n);
                let down = self
                    .nodes
                    .iter()
                    .any(|n| n.id == peer && n.replica.is_none());
                let right = rng.below(1000) < ACCURATE;
                if down == right {
                    Op::Suspect(member, peer)
                } else {
                    Op::Trust(member, peer)
                }
            }
            100..400 if !self.net.is_empty() => {
                Op::Deliver(rng.below(self.net.len() as u64) as usize)
            }
            400..420 if !self.net.is_empty() && faults => {
                Op::Lose(rng.below(self.net.len() as u64) as usize)
            }
            420..670 => Op::Drive(member),
            // Where a leader applies ahead, its disk is the slowest of the group: its writes are
            // made durable a quarter as often as another member's.
            670..870 if self.shape.ahead && member == leader && rng.below(4) != 0 => {
                Op::Durable(1 + (member + rng.below(n - 1)) % n)
            }
            670..870 => Op::Durable(member),
            870..930 => Op::Propose(leader),
            930..940 => Op::Fence(leader),
            940..950 => {
                let target = 1 + rng.below(n);
                let kind = match rng.below(3) {
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
        self.now += 1_000_000;
        let did = match op {
            Op::Suspect(id, peer) => self.with(id, |r| r.suspect(peer)),
            Op::Trust(id, peer) => self.with(id, |r| r.trust(peer)),
            Op::Deliver(at) => self.deliver(at),
            Op::Lose(at) => self.net.remove(at).is_some(),
            Op::Drive(id) => self.drive(id),
            Op::Durable(id) => self.durable(id),
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
                self.with(id, |r| r.read(context))
            }
            Op::Compact(id) => self.compact(id),
            Op::Crash(id) => self.crash(id),
            Op::Refuse(id) => self.arm(id, Fault::Room("the group's retained bound")),
            Op::Fail(id) => self.arm(id, Fault::Failed("the device failed")),
            Op::Resume(id) => {
                let stalled = self.replica(id).is_some_and(|r| r.is_stalled());
                if let Some(r) = self.replica(id) {
                    r.resume();
                }
                stalled
            }
        };
        self.check_start();
        did
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

    fn deliver(&mut self, at: usize) -> bool {
        let Some(message) = self.net.remove(at) else {
            return false;
        };
        let to = message.to;
        if !self.nodes.iter().any(|n| n.id == to) {
            return false;
        }
        let before = self
            .replica(to)
            .map(|r| (r.core().raft.configuration().clone(), r.is_leader()));
        let stepped = self.with(to, |r| r.step(message));
        if let Some((before, led)) = before {
            self.check_leader_commit(to, &before, led);
        }
        stepped
    }

    /// I3, as a leader's commit moves: the entry it now counts committed is durable on a
    /// majority of each half of the configuration it decided by, the one before the step or one
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

    fn arm(&mut self, id: u64, fault: Fault) -> bool {
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

    fn durable(&mut self, id: u64) -> bool {
        let Some(r) = self.replica(id) else {
            return false;
        };
        let store = store_of(r);
        let before = store.events.refused;
        let made = store.make_durable();
        if store.events.refused > before {
            self.reached.refused += 1;
        }
        if made {
            // I7: what the disk states committed, it holds.
            let disk = self.disk(id).clone();
            assert!(
                disk.hard.commit <= disk.last(),
                "member {id}: a durable write stated commit {} past the log's last {}",
                disk.hard.commit,
                disk.last()
            );
        }
        made
    }

    fn compact(&mut self, id: u64) -> bool {
        let now = self.now;
        let Some(r) = self.replica(id) else {
            return false;
        };
        match r.compact(2, now, waker()) {
            Ok(true) => {
                self.reached.compactions += 1;
                true
            }
            Ok(false) => false,
            Err(ReplicaError::Fenced(Cause::Write(_))) => true,
            Err(e) => panic!("member {id}: compact: {e}"),
        }
    }

    /// Loses power: the writes not durable, and what the state machine applied past its durable
    /// point.
    pub fn crash(&mut self, id: u64) -> bool {
        let node = self.node(id);
        let Some(replica) = node.replica.take() else {
            return false;
        };
        let disk = replica.core().store().log().disk.clone();
        let kv = replica.machine().clone();
        node.acted_before_crash.extend(kv.acted.iter().copied());
        node.down = Some((disk, kv.crashed()));
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
        let (mut disk, kv) = node.down.take()?;
        let last = disk.last();
        let mark = Point {
            index: last,
            term: disk.term(last).unwrap_or(0),
        };
        let floor = kv.durable.applied.index.max(disk.start.index);
        let cut = lose.min(last.saturating_sub(floor)) as usize;
        let keep = disk.entries.len() - cut;
        disk.entries.truncate(keep);
        disk.hard.commit = disk.hard.commit.min(disk.last());
        node.down = Some((disk, kv));
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
        let seed = self.seed;
        let depth = self.shape.depth;
        let shape = self.shape;
        let now = self.now;
        let node = self.node(id);
        let Some((disk, kv)) = node.down.take() else {
            return;
        };
        let mut store = SimStore::from_disk(disk, depth);
        store.mark = mark;
        let mut replica = Replica::open(&shaped(id, seed, &shape), store, kv, Unbounded)
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

    /// Drives `id` once and holds what it gave out to the oracle.
    pub fn drive(&mut self, id: u64) -> bool {
        let now = self.now;
        let mut out = std::mem::take(&mut self.out);
        out.clear();
        let Some(r) = self.replica(id) else {
            self.out = out;
            return false;
        };
        let events = store_of(r).events;
        let configuration = r.core().raft.configuration().clone();
        let led = r.is_leader();
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
                self.out = out;
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
        self.check_applied(id);
        let did = after != events || !out.messages.is_empty() || !out.answers.is_empty();
        for m in out.messages.drain(..) {
            if self.net.len() >= NET {
                self.net.pop_front();
            }
            self.net.push_back(m);
        }
        self.reached.events += u64::from(did);
        self.out = out;
        did
    }

    /// I1, I2, I3 on what `id` released, against the disks as they are now. A commit is held to
    /// the configuration the leader decided it by: the one before the drive, or one a change
    /// applied in it.
    fn check_output(
        &mut self,
        id: u64,
        out: &Output<(u64, Vec<u8>)>,
        before: &Configuration,
        led: bool,
    ) {
        let disk = self.disk(id);
        let replica = self
            .nodes
            .iter()
            .find(|n| n.id == id)
            .and_then(|n| n.replica.as_ref())
            .expect("up");
        for m in &out.messages {
            check_promises(id, disk, m);
            check_acknowledgement(id, disk, replica, m);
        }
        self.check_leader_commit(id, before, led);
    }

    /// I3: the entry at `commit` is durable on a majority of each half of the configuration the
    /// leader decided it by.
    fn check_commit(&self, id: u64, commit: u64, before: &Configuration) {
        let leader = self
            .nodes
            .iter()
            .find(|n| n.id == id)
            .and_then(|n| n.replica.as_ref())
            .expect("up");
        let log = leader.core().raft.log();
        // Through a step that left it no leader, its commit is still the one it decided.
        let Ok(term) = log.term(commit) else {
            return;
        };
        let held_by = |configuration: &Configuration| {
            [configuration.voters(), configuration.outgoing()]
                .iter()
                .filter(|half| !half.is_empty())
                .all(|half| {
                    let held = half
                        .iter()
                        .filter(|v| self.disk(**v).holds(commit, term))
                        .count();
                    held * 2 > half.len()
                })
        };
        let after = leader.core().raft.configuration();
        let disks: Vec<String> = before
            .voters()
            .iter()
            .map(|v| {
                let d = self.disk(*v);
                format!(
                    "{v}: start {:?} last {} term at {commit} {:?} hard {:?}",
                    d.start,
                    d.last(),
                    d.term(commit),
                    d.hard
                )
            })
            .collect();
        assert!(
            held_by(before) || held_by(after),
            "leader {id}: sent commit {commit} of term {term} not durable on a majority of {:?} or {:?} (I3): {disks:?}",
            before.voters(),
            after.voters()
        );
    }

    /// I4 and I5 on what `id` applied since the last look.
    fn check_applied(&mut self, id: u64) {
        let disk = self.disk(id).clone();
        let Some(r) = self.replica(id) else {
            return;
        };
        let durable_commit = disk.hard.commit.max(r.machine().durable.applied.index);
        let ahead = r.is_leader() && r.machine().now.applied.index > disk.last();
        let fenced: Vec<u64> = std::mem::take(&mut store_machine(r).fenced_applied);
        for index in fenced {
            assert!(
                index <= durable_commit,
                "member {id}: applied {index}, a change or an entry acted on at start, with the \
                 durable commit at {durable_commit} (I5)"
            );
        }
        let entries: Vec<(u64, u64, Vec<u8>)> = r.machine().now.entries.clone();
        self.reached.ahead += u64::from(ahead);
        for (index, term, data) in entries {
            match self.chosen.get(&index) {
                Some((t, d)) => assert!(
                    *t == term && *d == data,
                    "member {id} applied another entry at {index} (I4)"
                ),
                None => {
                    self.chosen.insert(index, (term, data));
                    self.reached.committed += 1;
                }
            }
        }
    }

    /// I8 for every member, up or down.
    fn check_start(&self) {
        for node in &self.nodes {
            let (disk, machine) = match (&node.replica, &node.down) {
                (Some(r), _) => (
                    &r.core().store().log().disk,
                    r.machine().durable.applied.index,
                ),
                (None, Some((disk, kv))) => (disk, kv.durable.applied.index),
                _ => continue,
            };
            assert!(
                disk.start.index <= machine,
                "member {}: the log starts at {} past the state machine's {} (I8)",
                node.id,
                disk.start.index,
                machine
            );
        }
    }

    /// Brings every member up and the network whole, then runs until a leader commits an entry
    /// every member of its configuration applies: false when it never does within `rounds`.
    pub fn settles(&mut self, rounds: usize) -> bool {
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
                return true;
            }
        }
        false
    }

    /// A round with no faults: every member drives, makes its writes durable and drives again,
    /// and every message is delivered.
    pub fn round(&mut self, ids: &[u64]) {
        for &id in ids {
            self.act(Op::Drive(id));
            while self.durable(id) {}
            self.act(Op::Drive(id));
        }
        while !self.net.is_empty() {
            self.act(Op::Deliver(0));
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
}

/// I1: no message carries a term the sender's disk does not hold; a vote request or a vote
/// leaves only with the disk holding the vote. A pre-vote takes no term.
fn check_promises(id: u64, disk: &Disk, m: &Message) {
    if matches!(
        m.msg_type,
        MessageType::MsgRequestPreVote | MessageType::MsgRequestPreVoteResponse
    ) {
        return;
    }
    assert!(
        disk.hard.term >= m.term,
        "member {id}: {:?} of term {} left with term {} durable (I1)",
        m.msg_type,
        m.term,
        disk.hard.term
    );
    if disk.hard.term != m.term {
        return;
    }
    match m.msg_type {
        MessageType::MsgRequestVote => assert_eq!(
            disk.hard.vote, id,
            "member {id}: asked for votes before its own vote was durable (I1)"
        ),
        MessageType::MsgRequestVoteResponse if !m.reject => assert_eq!(
            disk.hard.vote, m.to,
            "member {id}: voted before its vote was durable (I1)"
        ),
        _ => {}
    }
}

/// I2: an acknowledgement of `i` in term `t` leaves only with the disk holding an entry at `i`
/// (or a start past it) of a term no later than `t`: what the leader of `t` sent it. The member's
/// log may since hold another entry there, from a later leader whose term is not durable yet.
fn check_acknowledgement(id: u64, disk: &Disk, _replica: &Member, m: &Message) {
    if m.msg_type != MessageType::MsgAppendResponse || m.reject || m.term != disk.hard.term {
        return;
    }
    let held = m.index <= disk.start.index || disk.term(m.index).is_some_and(|t| t <= m.term);
    assert!(
        held,
        "member {id}: acknowledged {} in term {} with its disk through {} (I2)",
        m.index,
        m.term,
        disk.last()
    );
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
