//! A group of members on a network the schedule owns: it delivers, loses,
//! repeats and holds back messages, stops and reopens members, and compacts
//! their logs. What a schedule does is decided from what the members are,
//! so two groups that are alike are scheduled alike.
use std::collections::BTreeMap;

use hyper_check::liveness::{Laws, Position, Progress, Quiet};

use hyper_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Message,
    MessageType,
};

use super::{
    Coverage, Disk, Draws, Fault, Output, ROUND_NS, Replica, SPAN_NS, Said, Settings, Step, Store,
    TICK_NS, View, members, votes,
};

/// [`Draws`] as an object, for the closures `choose` and `change` share draws through.
trait DrawsDyn {
    fn pick_u64(&mut self, from: &[u64]) -> Option<u64>;
    fn chance_dyn(&mut self, percent: u64) -> bool;
    fn below_dyn(&mut self, bound: u64) -> u64;
}

impl<D: Draws> DrawsDyn for D {
    fn pick_u64(&mut self, from: &[u64]) -> Option<u64> {
        self.pick(from)
    }
    fn chance_dyn(&mut self, percent: u64) -> bool {
        self.chance(percent)
    }
    fn below_dyn(&mut self, bound: u64) -> u64 {
        self.below(bound)
    }
}

/// The most messages the network holds; the oldest is lost for a new one.
const NETWORK: usize = 2048;

#[derive(Clone, Debug)]
pub enum Op {
    Deliver {
        at: usize,
        keep: bool,
        lose: bool,
    },
    Tick(u64),
    Propose(u64, Vec<u8>),
    /// By the fast track.
    Fast(u64, Vec<u8>),
    Change(u64, ConfChangeV2),
    Transfer(u64, u64),
    Read(u64, Vec<u8>),
    /// Several reads asked of one member before it is asked what there is
    /// to do: this core sends one round for them.
    Reads(u64, Vec<Vec<u8>>),
    Restart(u64),
    Compact(u64),
    Block(u64, u64),
    Heal,
    Priority(u64, i64),
    /// The member is told what the path to another carries before it
    /// answers.
    Window(u64, u64, u64),
    Campaign(u64),
    Unreachable(u64, u64),
    Ping(u64),
    /// A step of the member's persistence ([`Step`]).
    Persist(u64, Step),
    /// By suspicion: the first member's detectors suspect the second.
    Suspect(u64, u64),
    /// By suspicion: the first member's detectors trust the second again.
    Trust(u64, u64),
    /// A fault at rest on the member's disk, found when it opens again
    /// (`Disk::verify`): it stops, suffers it, and opens marked.
    Corrupt(u64, Fault),
}

/// By suspicion, of a hundred steps, how many are a detector's word: as
/// many as the members' changes of role want, so each has its own.
const DETECTION: u64 = 10;
/// By suspicion, of a hundred words about a peer that is down or cut off
/// from the member, how many are right: a detector is wrong now and then
/// both ways, and the schedule is safe whatever it says.
const ACCURATE: u64 = 90;

/// What one member did in an operation.
#[derive(Debug, PartialEq)]
pub struct Report {
    pub member: u64,
    pub accepted: Option<bool>,
    pub output: Output,
    pub view: View,
}

/// What a schedule may do, in parts of a hundred of its steps.
#[derive(Clone, Copy, Debug)]
pub struct Mix {
    pub changes: bool,
    /// Whether a change may remove or demote the member that leads.
    pub leader_leaves: bool,
    pub restarts: bool,
    pub compaction: bool,
    pub partitions: bool,
    pub priorities: bool,
    /// Whether the bytes a member is sent ahead of its answers change
    /// while a schedule runs. Where the cores are compared they do not:
    /// `raft-rs` has no such bound.
    pub windows: bool,
    /// Whether reads are asked several at a time. Where the cores are
    /// compared they are not: `raft-rs` sends a round for each.
    pub bursts: bool,
    /// Of a hundred proposals, how many go by the fast track.
    pub fast: u64,
    pub lose: u64,
    pub repeat: u64,
    /// Of a hundred steps, how many are a member's persistence steps
    /// ([`Step`]); none where every member persists what it takes at once.
    pub lag: u64,
    /// Of a hundred persistence steps that would make a leader's write
    /// durable, how many do: the rest are drawn again, so a leader's disk
    /// is the slowest of its group and its followers commit its entries
    /// before it holds them (`docs/durable.md` §4.2, §14.2). A hundred for a
    /// leader whose disk is like the others'.
    pub leader_durable: u64,
    /// Of ten thousand steps, how many are a fault at rest ([`Op::Corrupt`]);
    /// none where the cores are compared, `raft-rs` having no marks.
    pub corrupt: u64,
    /// The most members whose disks are marked at once. Past one, what two
    /// acknowledged may be lost to both, and the group then waits for good
    /// (`docs/durable.md` §5.2); such waits are counted, not failed.
    pub marks: usize,
}
impl Mix {
    pub fn everything() -> Self {
        Self {
            changes: true,
            leader_leaves: false,
            restarts: true,
            compaction: true,
            partitions: true,
            priorities: true,
            windows: false,
            bursts: false,
            fast: 0,
            lose: 8,
            repeat: 5,
            lag: 0,
            leader_durable: 100,
            corrupt: 0,
            marks: 1,
        }
    }
}

/// What watches a group's operations: hyper-check's judge (`tests/check.rs`).
pub trait Observer<R> {
    /// Before `op` acts.
    fn before(&mut self, _group: &Cluster<R>, _op: &Op) {}
    /// After `op` acted, with what each member did.
    fn after(&mut self, _group: &Cluster<R>, _op: &Op, _reports: &[Report]) {}
}

/// No observer.
impl<R> Observer<R> for () {}

/// A member: running, and owning its disk, or stopped, and the cluster
/// holds the disk until it opens again.
#[derive(Clone)]
pub enum Member<R> {
    Up(R),
    Down(Box<Store>),
}

#[derive(Clone)]
pub struct Cluster<R> {
    members: Vec<Member<R>>,
    /// The members the group has, in and out of its configuration: what each states as the
    /// most a configuration names.
    size: usize,
    pub net: Vec<Message>,
    pub blocked: Vec<(u64, u64)>,
    pub settings: Settings,
    pub seed: u64,
    /// What was committed at each index, by whoever committed it first.
    pub chosen: BTreeMap<u64, Said>,
    /// Who led each term.
    pub leaders: BTreeMap<u64, u64>,
    /// How often a member that led a group it was no voter of was stopped
    /// for it. `raft-rs` leads on when a change it applies leaves it none,
    /// and its group follows it and commits nothing.
    pub deposed: u64,
    /// Whether such a member is stopped. Where the cores are compared it
    /// is not: the comparison ends there.
    pub stop_who_left: bool,
    reads: u64,
    /// The defect planted in every member as it opens (`hyper_raft::Mutant`).
    pub mutant: Option<hyper_raft::Mutant>,
    /// How often each member was opened: a member opened again forgot what its owner held.
    pub incarnations: BTreeMap<u64, u64>,
    /// For each read that waits, the highest index any member had
    /// committed when it was asked: what it is answered with is no less.
    asked: BTreeMap<Vec<u8>, u64>,
    /// How many reads were answered.
    pub answered: u64,
    opened: u64,
    /// For each member that leads, the term and commit last checked
    /// against what the voters hold durably.
    checked: BTreeMap<u64, (u64, u64)>,
    /// For each member that leads, the configuration it had applied when
    /// last checked: a commit decided before a change applied in the same
    /// step was counted by it.
    counted_by: BTreeMap<u64, ConfState>,
    /// What the persistence steps of members since stopped reached.
    stopped: Coverage,
    /// Each member's clock, by suspicion: its ticks advance it.
    pub clocks: BTreeMap<u64, u64>,
    /// Faults at rest suffered.
    pub faults: u64,
    /// Schedules whose group was left waiting on a mark, as the election rule
    /// must (`Cluster::electable`).
    pub waits: u64,
    /// Marks ended: the member's log holds again what it lost.
    pub repaired: u64,
    /// The steps the ended marks lasted, all together.
    pub marked_steps: u64,
    /// For each member whose disk is marked, the step it was marked at.
    marked_since: BTreeMap<u64, u64>,
    /// Operations acted on.
    steps: u64,
    /// Snapshots the network lost at its bound, by sender and recipient, whose senders are yet
    /// to be told.
    evicted: Vec<(u64, u64)>,
    /// The most operations the liveness phase may act before the run is reported unconverged
    /// (`docs/sim.md` §4.2: a group that keeps moving without converging is ended by the run's
    /// budget, never passed); `None` for none, where a harness has not stated one.
    pub liveness_bound: Option<u64>,
}

impl<R: Replica> Cluster<R> {
    pub fn new(count: u64, voters: &[u64], settings: Settings, seed: u64) -> Self {
        let boot = ConfState {
            voters: voters.to_vec(),
            ..ConfState::default()
        };
        let mut cluster = Self {
            members: Vec::new(),
            size: count as usize,
            net: Vec::new(),
            blocked: Vec::new(),
            settings,
            seed,
            chosen: BTreeMap::new(),
            leaders: BTreeMap::new(),
            deposed: 0,
            stop_who_left: false,
            reads: 0,
            mutant: None,
            incarnations: BTreeMap::new(),
            asked: BTreeMap::new(),
            answered: 0,
            opened: 0,
            checked: BTreeMap::new(),
            counted_by: BTreeMap::new(),
            stopped: Coverage::default(),
            clocks: BTreeMap::new(),
            faults: 0,
            waits: 0,
            repaired: 0,
            marked_steps: 0,
            marked_since: BTreeMap::new(),
            steps: 0,
            evicted: Vec::new(),
            liveness_bound: None,
        };
        for id in 1..=count {
            let node = cluster.open(id, Store::new(boot.clone()));
            cluster.members.push(Member::Up(node));
        }
        cluster
    }
    fn open(&mut self, id: u64, store: Store) -> R {
        self.opened += 1;
        *self.incarnations.entry(id).or_insert(0) += 1;
        let mut node = R::open(
            id,
            store,
            &self.settings,
            self.seed
                .wrapping_mul(1_000_003)
                .wrapping_add(id * 7919 + self.opened),
            self.size,
        );
        node.plant(self.mutant);
        node
    }
    /// Plants `mutant` in every member, and in each it opens from now on.
    pub fn plant(&mut self, mutant: Option<hyper_raft::Mutant>) {
        self.mutant = mutant;
        for member in &mut self.members {
            if let Member::Up(node) = member {
                node.plant(mutant);
            }
        }
    }
    pub fn node(&mut self, id: u64) -> Option<&mut R> {
        match self.members.get_mut((id - 1) as usize)? {
            Member::Up(node) => Some(node),
            Member::Down(_) => None,
        }
    }
    pub fn peek(&self, id: u64) -> Option<&R> {
        match self.members.get((id - 1) as usize)? {
            Member::Up(node) => Some(node),
            Member::Down(_) => None,
        }
    }
    /// What the member holds durable, running or stopped.
    pub fn disk(&self, id: u64) -> &Disk {
        match &self.members[(id - 1) as usize] {
            Member::Up(node) => &node.store().0,
            Member::Down(store) => &store.0,
        }
    }
    /// The member stops: what was not durable is gone, and the cluster
    /// holds what was.
    pub fn stop(&mut self, id: u64) {
        let member = &mut self.members[(id - 1) as usize];
        if let Member::Up(node) = member {
            self.stopped.add(node.coverage());
            let store = std::mem::take(node.store_mut());
            *member = Member::Down(Box::new(store));
        }
    }
    /// The member stops, if it runs, and opens on what was durable.
    fn restart(&mut self, id: u64) {
        self.stop(id);
        let Member::Down(store) = std::mem::replace(
            &mut self.members[(id - 1) as usize],
            Member::Down(Box::default()),
        ) else {
            unreachable!("a member that was just stopped");
        };
        let node = self.open(id, *store);
        self.members[(id - 1) as usize] = Member::Up(node);
    }
    /// What every member's persistence steps reached, stopped or running.
    pub fn coverage(&self) -> Coverage {
        let mut total = self.stopped;
        for id in self.up() {
            if let Some(node) = self.peek(id) {
                let mut running = node.coverage();
                // A member that runs has lost nothing.
                running.lost = 0;
                total.add(running);
            }
        }
        total
    }
    pub fn ids(&self) -> Vec<u64> {
        (1..=self.members.len() as u64).collect()
    }
    pub fn up(&self) -> Vec<u64> {
        self.ids()
            .into_iter()
            .filter(|id| self.peek(*id).is_some())
            .collect()
    }
    pub fn leaders_now(&self) -> Vec<u64> {
        self.up()
            .into_iter()
            .filter(|id| self.peek(*id).is_some_and(|node| node.view().role == 2))
            .collect()
    }
    fn is_blocked(&self, from: u64, to: u64) -> bool {
        self.blocked.contains(&(from, to))
    }

    /// What the member did, recorded and checked against what the group
    /// holds true whatever the schedule.
    fn report(&mut self, member: u64, accepted: Option<bool>) -> Report {
        let suspicion = self.settings.suspicion;
        let now = self.clocks.get(&member).copied().unwrap_or(0);
        let node = self.node(member).expect("a member that is up");
        if suspicion {
            // After each call, the owner wakes the member at its clock.
            node.wake(now);
        }
        let output = node.drain();
        let view = node.view();
        for committed in &output.committed {
            // What an entry states, and not the term it bears: by the fast
            // track the leader that took an entry and the leader that took
            // it again at its election each gave it its own.
            let stated = |said: &Said| (said.0, said.2, said.3.clone());
            match self.chosen.get(&committed.0) {
                Some(chosen) if !self.settings.judged => assert_eq!(
                    stated(chosen),
                    stated(committed),
                    "seed {}: member {member} committed another entry at {}",
                    self.seed,
                    committed.0
                ),
                Some(_) => {}
                None => {
                    self.chosen.insert(committed.0, committed.clone());
                }
            }
        }
        for (index, context) in &output.reads {
            // A read sees what was committed before it was asked, whoever
            // leads by the time it is answered: a leader that was deposed
            // meanwhile answers nothing.
            if let Some(floor) = self.asked.remove(context) {
                assert!(
                    self.settings.judged || *index >= floor,
                    "seed {}: member {member} answered a read at {index}, asked when {floor} was committed",
                    self.seed
                );
                self.answered += 1;
            }
        }
        if view.role == 2 {
            let leader = *self.leaders.entry(view.term).or_insert(member);
            assert!(
                self.settings.judged || leader == member,
                "seed {}: two leaders of term {}",
                self.seed,
                view.term
            );
        }
        for message in &output.messages {
            if self.net.len() >= NETWORK {
                let lost = self.net.remove(0);
                if lost.msg_type == MessageType::MsgSnapshot {
                    // Its sender is told the transfer failed, as an owner whose transport
                    // dropped it is: without the word, the leader waits on the snapshot for
                    // ever (the swarm's fast seed 2,396, `docs/sim.md` §15.9).
                    self.evicted.push((lost.from, lost.to));
                }
            }
            self.net.push(message.clone());
        }
        if self.stop_who_left && view.role == 2 && !view.promotable && view.applied >= view.commit {
            // It leads a group that no longer needs it, and applied all it committed: the
            // configuration it counts by names it no voter (hyper-raft's newest in its log, once
            // committed; raft-rs's applied). Its owner stops it, and it opens as what it is.
            self.deposed += 1;
            self.restart(member);
        }
        Report {
            member,
            accepted,
            output,
            view,
        }
    }

    /// `op` acted, `observer` told before and after.
    pub fn act_observed(&mut self, observer: &mut impl Observer<R>, op: &Op) -> Vec<Report> {
        observer.before(self, op);
        let reports = self.act(op);
        observer.after(self, op, &reports);
        reports
    }

    pub fn act(&mut self, op: &Op) -> Vec<Report> {
        let mut reports = Vec::new();

        match op {
            Op::Deliver { at, keep, lose } => {
                let message = if *keep {
                    self.net[*at].clone()
                } else {
                    self.net.remove(*at)
                };
                let snapshot = message.msg_type == MessageType::MsgSnapshot;
                let (from, to) = (message.from, message.to);
                let arrives = !*lose && !self.is_blocked(from, to) && self.peek(to).is_some();
                if arrives {
                    let accepted = self.node(to).map(|node| node.step(message));
                    reports.push(self.report(to, accepted));
                }
                // Who sent a snapshot is told what became of it.
                if snapshot && !*keep && self.peek(from).is_some() {
                    if let Some(node) = self.node(from) {
                        node.snapshot_status(to, arrives);
                    }
                    reports.push(self.report(from, None));
                }
            }
            Op::Tick(id) if self.settings.suspicion => {
                // Time passes on the member's clock; it is woken at it.
                *self.clocks.entry(*id).or_insert(0) += TICK_NS;
                if self.peek(*id).is_some() {
                    reports.push(self.report(*id, None));
                }
            }
            Op::Tick(id) => {
                let accepted = self.node(*id).map(|node| node.tick());
                reports.push(self.report(*id, accepted));
            }
            Op::Suspect(id, peer) => {
                if let Some(node) = self.node(*id) {
                    node.suspect(*peer);
                    reports.push(self.report(*id, None));
                }
            }
            Op::Trust(id, peer) => {
                if let Some(node) = self.node(*id) {
                    node.trust(*peer);
                    reports.push(self.report(*id, None));
                }
            }
            Op::Propose(id, data) => {
                let accepted = self.node(*id).map(|node| node.propose(data.clone()));
                reports.push(self.report(*id, accepted));
            }
            Op::Fast(id, data) => {
                let accepted = self
                    .node(*id)
                    .map(|node| node.propose_fast(data.clone()).is_some());
                reports.push(self.report(*id, accepted));
            }
            Op::Change(id, change) => {
                let accepted = self.node(*id).map(|node| node.propose_change(change));
                reports.push(self.report(*id, accepted));
            }
            Op::Transfer(id, to) => {
                if let Some(node) = self.node(*id) {
                    node.transfer(*to);
                }
                reports.push(self.report(*id, None));
            }
            Op::Read(id, context) => {
                let floor = self.chosen.keys().next_back().copied().unwrap_or(0);
                self.asked.insert(context.clone(), floor);
                if let Some(node) = self.node(*id) {
                    node.read(context.clone());
                }
                reports.push(self.report(*id, None));
            }
            Op::Reads(id, contexts) => {
                let floor = self.chosen.keys().next_back().copied().unwrap_or(0);
                for context in contexts {
                    self.asked.insert(context.clone(), floor);
                    if let Some(node) = self.node(*id) {
                        node.read(context.clone());
                    }
                }
                reports.push(self.report(*id, None));
            }
            Op::Restart(id) => {
                // What was not durable is gone; what was is what it opens on.
                self.restart(*id);
                reports.push(self.report(*id, None));
                if self.settings.suspicion {
                    // The others' detectors see its new incarnation.
                    for other in self.up() {
                        if other != *id
                            && let Some(node) = self.node(other)
                        {
                            node.restarted(*id);
                            reports.push(self.report(other, None));
                        }
                    }
                }
            }
            Op::Compact(id) => {
                let accepted = self.node(*id).map(|node| node.compact());
                reports.push(self.report(*id, accepted));
            }
            Op::Block(from, to) => {
                if !self.is_blocked(*from, *to) {
                    self.blocked.push((*from, *to));
                }
            }
            Op::Heal => self.blocked.clear(),
            Op::Priority(id, priority) => {
                if let Some(node) = self.node(*id) {
                    node.set_priority(*priority);
                }
                reports.push(self.report(*id, None));
            }
            Op::Window(id, member, bytes) => {
                if let Some(node) = self.node(*id) {
                    node.set_window(*member, *bytes);
                }
                reports.push(self.report(*id, None));
            }
            Op::Campaign(id) => {
                let accepted = self.node(*id).map(|node| node.campaign());
                reports.push(self.report(*id, accepted));
            }
            Op::Unreachable(id, member) => {
                if let Some(node) = self.node(*id) {
                    node.unreachable(*member);
                }
                reports.push(self.report(*id, None));
            }
            Op::Ping(id) => {
                if let Some(node) = self.node(*id) {
                    node.ping();
                }
                reports.push(self.report(*id, None));
            }
            Op::Persist(id, step) => {
                let accepted = self.node(*id).map(|node| node.persist(*step));
                reports.push(self.report(*id, accepted));
            }
            Op::Corrupt(id, fault) => {
                self.stop(*id);
                if let Member::Down(store) = &mut self.members[(*id - 1) as usize] {
                    store.0.suffer(*fault);
                }
                self.faults += 1;
                reports.extend(self.act(&Op::Restart(*id)));
            }
        }
        // Each snapshot the network lost at its bound is reported failed to its sender, as a
        // delivery that loses one is; a report may lose more, at most the network's bound of them.
        for _ in 0..NETWORK {
            let Some((from, to)) = self.evicted.pop() else {
                break;
            };
            if let Some(node) = self.node(from) {
                node.snapshot_status(to, false);
                reports.push(self.report(from, None));
            }
        }
        if R::LAGGED && !self.settings.judged {
            self.check_durable();
        }
        self.steps += 1;
        for id in self.ids() {
            let marked = self.disk(id).mark().is_some();
            match self.marked_since.get(&id).copied() {
                None if marked => {
                    self.marked_since.insert(id, self.steps);
                }
                Some(since) if !marked => {
                    self.marked_since.remove(&id);
                    self.repaired += 1;
                    self.marked_steps += self.steps - since;
                }
                _ => {}
            }
        }
        reports
    }

    /// The durability oracle for what a leader decides (`docs/durable.md`
    /// §3): it counts itself only for what its own disk holds (I3), and what
    /// it commits a majority of each half of its configuration holds
    /// durably, in its log or, by the fast track, beside it.
    fn check_durable(&mut self) {
        for id in self.up() {
            let Some(led) = self.peek(id).and_then(Replica::led) else {
                continue;
            };
            if let Some(own) = &led.own {
                assert!(
                    holds(self.disk(id), own),
                    "seed {}: leader {id} counted itself for {} before its disk held it",
                    self.seed,
                    own.index
                );
            }
            let Some(committed) = &led.committed else {
                continue;
            };
            let conf = led.counted_by.clone();
            let before = self.counted_by.insert(id, conf.clone());
            let last = self.checked.insert(id, (led.term, committed.index));
            // What it knew committed when it was elected it did not decide,
            // and a commit it has not moved since was checked.
            if last.is_none_or(|(term, index)| term != led.term || index == committed.index) {
                continue;
            }
            // A member whose disk lost the entry at rest after it acknowledged it
            // was counted for what it held then: its mark says so.
            let kept = |disk: &Disk| {
                holds(disk, committed)
                    || disk
                        .mark()
                        .is_some_and(|lost| lost.index >= committed.index)
            };
            let held = |conf: &ConfState| {
                [&conf.voters, &conf.voters_outgoing].iter().all(|half| {
                    half.is_empty()
                        || half.iter().filter(|voter| kept(self.disk(**voter))).count() * 2
                            > half.len()
                })
            };
            assert!(
                held(&conf) || before.as_ref().is_some_and(held),
                "seed {}: leader {id} of term {} committed {} that no majority of {conf:?} or {before:?} holds durably",
                self.seed,
                led.term,
                committed.index
            );
        }
    }

    fn change(&self, rng: &mut impl Draws, leader: u64, mix: &Mix) -> ConfChangeV2 {
        let conf = self.disk(leader).conf.clone();
        let joint = !conf.voters_outgoing.is_empty();
        if joint && rng.chance(70) {
            // Out of the joint configuration.
            return ConfChangeV2::default();
        }
        if rng.chance(4) {
            // A change that leaves no voter is committed and never applied.
            return ConfChangeV2 {
                changes: conf
                    .voters
                    .iter()
                    .map(|voter| ConfChangeSingle {
                        change_type: ConfChangeType::RemoveNode,
                        node_id: *voter,
                    })
                    .collect(),
                ..ConfChangeV2::default()
            };
        }
        let held = members(&conf);
        let leading = self.leaders_now();
        let spared = |id: &u64| mix.leader_leaves || (*id != leader && !leading.contains(id));
        let outside: Vec<u64> = self
            .ids()
            .into_iter()
            .filter(|id| !held.contains(id))
            .collect();
        let voters: Vec<u64> = conf.voters.iter().copied().filter(spared).collect();
        let inside: Vec<u64> = held.iter().copied().filter(spared).collect();
        let one = |rng: &mut dyn DrawsDyn| -> Option<ConfChangeSingle> {
            let (kind, member) = match rng.below_dyn(6) {
                0 => (ConfChangeType::AddLearnerNode, rng.pick_u64(&outside)?),
                1 => (ConfChangeType::AddNode, rng.pick_u64(&outside)?),
                2 => (ConfChangeType::AddNode, rng.pick_u64(&conf.learners)?),
                3 => (ConfChangeType::RemoveNode, rng.pick_u64(&inside)?),
                4 => (ConfChangeType::AddLearnerNode, rng.pick_u64(&voters)?),
                _ => (ConfChangeType::RemoveNode, rng.pick_u64(&conf.learners)?),
            };
            Some(ConfChangeSingle {
                change_type: kind,
                node_id: member,
            })
        };
        let count = if rng.chance(60) { 1 } else { 1 + rng.below(3) };
        let mut changes = Vec::new();
        for _ in 0..count * 4 {
            if changes.len() as u64 == count {
                break;
            }
            if let Some(change) = one(rng) {
                changes.push(change);
            }
        }
        let transition = match rng.below(10) {
            0 => ConfChangeTransition::Implicit,
            1 => ConfChangeTransition::Explicit,
            _ => ConfChangeTransition::Auto,
        };
        ConfChangeV2 {
            transition,
            changes,
            context: vec![],
        }
    }

    /// The next step of the schedule.
    pub fn choose(&mut self, rng: &mut impl Draws, mix: &Mix) -> Op {
        let up = self.up();
        let all = self.ids();
        let leaders = self.leaders_now();
        let any = |rng: &mut dyn DrawsDyn| rng.pick_u64(&up).unwrap_or(1);
        // Where a leader is wanted and none is known, any member is asked:
        // what one that does not lead does with it is compared as well.
        let leader = |rng: &mut dyn DrawsDyn| {
            if rng.chance_dyn(90) {
                rng.pick_u64(&leaders).unwrap_or_else(|| any(rng))
            } else {
                any(rng)
            }
        };
        for _ in 0..16 {
            // Drawn only by suspicion: a schedule on ticks draws as it did.
            if self.settings.suspicion && !up.is_empty() && rng.chance(DETECTION) {
                let member = any(rng);
                let peer = rng.pick(&all).unwrap_or(1);
                if peer != member {
                    let gone = self.peek(peer).is_none() || self.is_blocked(peer, member);
                    let suspect = if gone {
                        rng.chance(ACCURATE)
                    } else {
                        !rng.chance(ACCURATE)
                    };
                    return if suspect {
                        Op::Suspect(member, peer)
                    } else {
                        Op::Trust(member, peer)
                    };
                }
            }
            // Drawn only where faults at rest are: a schedule without draws as
            // it always did. At most `Mix::marks` members' disks are marked
            // at once.
            if mix.corrupt > 0 && rng.below(10_000) < mix.corrupt {
                // A member whose disk holds entries.
                let holding: Vec<u64> = all
                    .iter()
                    .copied()
                    .filter(|id| !self.disk(*id).entries.is_empty())
                    .collect();
                let member = rng.pick(&holding).unwrap_or(1);
                let others_marked = all
                    .iter()
                    .filter(|id| **id != member && self.disk(**id).mark().is_some())
                    .count();
                let disk = self.disk(member);
                let held = disk.entries.len() as u64;
                if others_marked < mix.marks && held > 0 {
                    let fault = if rng.chance(50) {
                        Fault::Flip(disk.first_index() + rng.below(held))
                    } else {
                        Fault::Lose(1 + rng.below(held))
                    };
                    return Op::Corrupt(member, fault);
                }
            }
            // Drawn only where members persist in steps: a schedule of
            // members that do not draws as it always did.
            if mix.lag > 0 && !up.is_empty() && rng.chance(mix.lag) {
                let step = match rng.below(3) {
                    0 => Step::Take,
                    1 => Step::Durable,
                    _ => Step::Notify,
                };
                // A member with something to persist, where one has.
                let busy: Vec<u64> = up
                    .iter()
                    .copied()
                    .filter(|id| self.peek(*id).is_some_and(Replica::busy))
                    .collect();
                let member = rng.pick(&busy).unwrap_or_else(|| any(rng));
                // Drawn only where a leader's disk is slow: every other
                // schedule draws as it did.
                if mix.leader_durable < 100
                    && step == Step::Durable
                    && leaders.contains(&member)
                    && !rng.chance(mix.leader_durable)
                {
                    continue;
                }
                return Op::Persist(member, step);
            }
            let drawn = rng.below(100);
            match drawn {
                0..=49 if !self.net.is_empty() => {
                    return Op::Deliver {
                        at: rng.below(self.net.len() as u64) as usize,
                        keep: rng.chance(mix.repeat),
                        lose: rng.chance(mix.lose),
                    };
                }
                0..=74 if !up.is_empty() => return Op::Tick(any(rng)),
                75..=84 if !up.is_empty() => {
                    let size = 1 + rng.below(48) as usize;
                    let data = (0..size).map(|_| rng.next() as u8).collect();
                    if rng.chance(mix.fast) {
                        // From any member: the fast track is for those
                        // that do not lead.
                        return Op::Fast(any(rng), data);
                    }
                    return Op::Propose(leader(rng), data);
                }
                85..=87 if mix.changes && !up.is_empty() => {
                    let at = leader(rng);
                    return Op::Change(at, self.change(rng, at, mix));
                }
                88..=89 if !up.is_empty() => {
                    return Op::Transfer(leader(rng), rng.pick(&all).unwrap_or(1));
                }
                90..=91 if !up.is_empty() => {
                    if mix.bursts && rng.chance(50) {
                        let contexts = (0..2 + rng.below(7))
                            .map(|_| {
                                self.reads += 1;
                                self.reads.to_le_bytes().to_vec()
                            })
                            .collect();
                        return Op::Reads(leader(rng), contexts);
                    }
                    self.reads += 1;
                    return Op::Read(leader(rng), self.reads.to_le_bytes().to_vec());
                }
                92 if mix.restarts => return Op::Restart(rng.pick(&all).unwrap_or(1)),
                93 if mix.compaction && !up.is_empty() => return Op::Compact(any(rng)),
                94 if mix.partitions => {
                    let from = rng.pick(&all).unwrap_or(1);
                    let to = rng.pick(&all).unwrap_or(1);
                    if from != to {
                        return Op::Block(from, to);
                    }
                }
                95 if mix.partitions => return Op::Heal,
                96 if mix.windows && !up.is_empty() && rng.chance(50) => {
                    // From less than one entry to more than a schedule
                    // ever has in flight.
                    let bytes = [1, 24, 96, 512, 1 << 20][rng.below(5) as usize];
                    return Op::Window(leader(rng), rng.pick(&all).unwrap_or(1), bytes);
                }
                96 if mix.priorities && !up.is_empty() => {
                    return Op::Priority(any(rng), rng.below(4) as i64);
                }
                97 if !up.is_empty() => return Op::Campaign(any(rng)),
                98 if !up.is_empty() => {
                    return Op::Unreachable(leader(rng), rng.pick(&all).unwrap_or(1));
                }
                99 if !up.is_empty() => return Op::Ping(leader(rng)),
                _ => {}
            }
        }
        Op::Heal
    }

    /// With the network whole and every member up, the group elects and
    /// commits: a proposal is applied by every member of the configuration.
    /// The rounds of ticks and deliveries go on while any member's term,
    /// commit, applied index or last index moves (`docs/sim.md` §4.2), and
    /// fail once a quiet period passes with none moving. By suspicion, the
    /// longest draw of the members' span, the election's three rounds and a
    /// replication round, in rounds of one tick of each member's clock. On
    /// ticks, twice the longest timeout a member draws with its patience:
    /// within one, every member's timer fires and its campaign ends its lease
    /// on a leader (a campaigner knows none); within the second, every member
    /// has campaigned with no lease left. A pre-vote's answers then turn on
    /// the voters' terms, logs, marks and priorities, which a quiet group does
    /// not move, so a group in which no member won one by then wins none
    /// later. A round is a tick of each member, its messages delivered within
    /// it.
    pub fn settles(&mut self) -> bool {
        self.settles_observed(&mut ())
    }

    /// [`Cluster::settles`], with `observer` told of each operation it acts.
    pub fn settles_observed(&mut self, observer: &mut impl Observer<R>) -> bool {
        self.blocked.clear();
        for id in self.ids() {
            if self.peek(id).is_none() {
                self.act_observed(observer, &Op::Restart(id));
            }
        }
        if self.settings.suspicion {
            // Every member up and the network whole: the detectors trust
            // every peer.
            for id in self.ids() {
                for peer in self.ids() {
                    if peer != id {
                        self.act_observed(observer, &Op::Trust(id, peer));
                    }
                }
            }
        }
        // The index the proposal took, and the term of the leader that
        // took it.
        let mut proposed: Option<(u64, u64)> = None;
        // hyper-check's rule (docs/sim.md §4.2): by suspicion, the span the members draw from,
        // three vote rounds and a replication round, with no detector delay, for the schedule
        // says what each detector suspects; on ticks, the longest timeout a member draws.
        let quiet = if self.settings.suspicion {
            let laws = Laws {
                detection_ns: 0,
                span_ns: SPAN_NS,
                vote_rounds_ns: 3 * ROUND_NS,
                replication_ns: ROUND_NS,
            };
            Quiet::in_rounds(&laws, TICK_NS)
        } else {
            let patience = self
                .up()
                .into_iter()
                .filter_map(|id| self.peek(id).map(|node| node.view().patience))
                .max()
                .unwrap_or(0);
            Quiet::ticks(self.settings.election_tick as u64, patience as u64)
        }
        .expect("a quiet period within u64");
        let mut progress = Progress::new(quiet, 0);
        let began = self.steps;
        let spent = |cluster: &Self| {
            cluster
                .liveness_bound
                .is_some_and(|bound| cluster.steps - began > bound)
        };
        for round in 0u64.. {
            if spent(self) {
                println!("the liveness phase passed its bound at round {round}: unconverged");
                break;
            }
            for id in self.up() {
                let view = self.peek(id).map(|node| node.view()).expect("up");
                let at = Position {
                    term: view.term,
                    commit: view.commit,
                    applied: view.applied,
                    last: view.last_index,
                };
                progress.observe(round, id, at);
            }
            if progress.stuck(round) {
                break;
            }
            // A member of raft-rs that joined after the leader's snapshot was
            // taken is not named by it and discards it (this core takes it,
            // `docs/raft.md` §3.4): it is seeded by a snapshot taken since,
            // which is the owner's to take.
            if round % 16 == 15 {
                for leader in self.leaders_now() {
                    self.act_observed(observer, &Op::Compact(leader));
                }
            }

            while !self.net.is_empty() && !spent(self) {
                self.act_observed(
                    observer,
                    &Op::Deliver {
                        at: 0,
                        keep: false,
                        lose: false,
                    },
                );
                if R::LAGGED && self.net.is_empty() {
                    // What the members took is persisted and heard of, and
                    // what that sends is delivered in turn.
                    for id in self.up() {
                        self.act_observed(observer, &Op::Persist(id, Step::Flush));
                    }
                }
            }
            // A proposal taken by a leader that was deposed before it
            // committed may be gone with its term: it is proposed again to
            // the leader that followed, as its client would.
            let leads = self
                .leaders_now()
                .into_iter()
                .filter_map(|id| self.peek(id).map(|node| node.view().term))
                .max();
            if let (Some((_, term)), Some(now)) = (proposed, leads)
                && now > term
            {
                proposed = None;
            }
            if let Some((index, _)) = proposed {
                let leader = self.leaders_now().into_iter().next();
                let conf = leader.map(|leader| self.disk(leader).conf.clone());
                if let Some(conf) = conf
                    && members(&conf).iter().all(|member| {
                        self.peek(*member)
                            .is_some_and(|node| node.app().index >= index)
                    })
                    && members(&conf).iter().any(|member| votes(&conf, *member))
                {
                    return true;
                }
            } else if let Some(leader) = self
                .leaders_now()
                .into_iter()
                .max_by_key(|id| self.peek(*id).map_or(0, |node| node.view().term))
            {
                let reports =
                    self.act_observed(observer, &Op::Propose(leader, b"settled".to_vec()));
                if reports.iter().any(|report| report.accepted == Some(true)) {
                    proposed = self.peek(leader).map(|node| {
                        let view = node.view();
                        (view.last_index, view.term)
                    });
                }
            }
            for id in self.ids() {
                self.act_observed(observer, &Op::Tick(id));
                if R::LAGGED {
                    self.act_observed(observer, &Op::Persist(id, Step::Flush));
                }
            }
        }
        for id in self.ids() {
            let view = self.peek(id).map(|node| node.view());
            let conf = self.disk(id).conf.clone();
            let deadline = self.peek(id).map(|node| node.deadline());
            println!(
                "member {id}: deadline {deadline:?} clock {:?} proposed {proposed:?} {conf:?} mark {:?}\n  {view:?}",
                self.clocks.get(&id),
                self.disk(id).lost
            );
        }
        false
    }
}

impl<R: Replica> Cluster<R> {
    /// The members the election rule admits, read from every member's disk
    /// whatever runs (`docs/durable.md` §5.2): a voter of the configuration
    /// it applied for which a quorum of each half of that configuration
    /// answers for no more than its log holds — each voter for its log's
    /// last entry, or for its mark where its log may lack what it
    /// acknowledged — not counting the candidate itself where it is marked
    /// (R-7), nor a marked one at all in a fast group. A group with none must
    /// wait, for its logs cannot show that an entry a marked member helped
    /// commit is held elsewhere; with one, once the network is whole and
    /// every member up, it elects.
    pub fn electable(&self) -> Vec<u64> {
        let claim = |disk: &Disk| {
            let last = disk.last_index();
            let whole = (disk.term(last).unwrap_or(0), last);
            disk.mark()
                .map_or(whole, |lost| (lost.term, lost.index).max(whole))
        };
        self.ids()
            .into_iter()
            .filter(|candidate| {
                let disk = self.disk(*candidate);
                let node = self.peek(*candidate);
                let counted = node.and_then(Replica::counts_by);
                let conf = counted.as_ref().unwrap_or(&disk.conf);
                let stands = node.map_or(votes(conf, *candidate), |node| node.view().promotable);
                let marked = disk.mark().is_some();
                if !stands || (marked && self.settings.fast) {
                    return false;
                }
                let last = disk.last_index();
                let whole = (disk.term(last).unwrap_or(0), last);
                [&conf.voters, &conf.voters_outgoing].iter().all(|half| {
                    half.is_empty()
                        || half
                            .iter()
                            .filter(|voter| {
                                !(marked && **voter == *candidate)
                                    && claim(self.disk(**voter)) <= whole
                            })
                            .count()
                            * 2
                            > half.len()
                })
            })
            .collect()
    }
    /// Nothing acknowledged is lost: once the group settled, every member of
    /// the configuration holds every entry committed, at its index, as it
    /// was committed (under its snapshot, or in its log), whatever its disk
    /// suffered.
    pub fn check_kept(&self) {
        let Some(leader) = self.leaders_now().into_iter().next() else {
            return;
        };
        let conf = self.disk(leader).conf.clone();
        for member in members(&conf) {
            let disk = self.disk(member);
            for (index, said) in &self.chosen {
                if *index <= disk.snapshot_index() {
                    continue;
                }
                let held = disk
                    .entries
                    .get((*index - disk.first_index()) as usize)
                    .is_some_and(|entry| entry.entry_type == said.2 && entry.data == said.3);
                assert!(
                    held,
                    "seed {}: member {member} lost {index}, committed",
                    self.seed
                );
            }
        }
    }
}

/// Whether `disk` holds `entry` durably: in its log, beside it as a
/// proposal it approved (the fast track), or under its snapshot.
pub fn holds(disk: &Disk, entry: &hyper_raft::proto::Entry) -> bool {
    let same = |held: &hyper_raft::proto::Entry| {
        held.entry_type == entry.entry_type && held.data == entry.data
    };
    entry.index <= disk.snapshot_index()
        || (entry.index >= disk.first_index()
            && entry.index <= disk.last_index()
            && same(&disk.entries[(entry.index - disk.first_index()) as usize]))
        || disk
            .proposals
            .iter()
            .any(|held| held.index == entry.index && same(held))
}
