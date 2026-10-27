//! The shell driven by the node-pair liveness stream (timing steps L-2 and L-3, `docs/timing.md`
//! §2.8–§2.9): three nodes, each an `Owner` of one replica and a `hyper_liveness::Liveness`, on one
//! simulated clock. The network delivers each message, Raft's and the stream's, a seeded one-way
//! delay after it was sent; a write is durable a flush time after it was submitted; each node wakes
//! late by a seeded lateness, which the stream measures as its granularity. Nothing is told by
//! hand: the replicas' pairs are attached from their configurations (`Owner::pairs`), the stream's
//! changes reach them (`Owner::believe`), their timing is the election law over what the stream
//! measured (`Owner::measure`), and their durable writes are the flushes its heartbeats prove
//! (`Driven::flushed`).
//!
//! Asserted: the group elects from nothing (no detector, only measured round trips: §3, item 10);
//! once it is idle its members send no message of Raft at all, only the stream's heartbeats; once
//! every other node judges the leader's node by a configured detector, the leader's node killed,
//! every survivor's stream suspects it within the bound the suspicion states, and the survivors
//! elect and commit; started again, a new run of its stream, every survivor's stream reports the
//! restart to its core and the node catches up. A leader killed as soon as it is elected, before
//! any node's link to it has evidence of its own, is suspected by every survivor, which elect and
//! commit (`docs/timing.md` §3, item 10). Every wait is on progress (`docs/sim.md` §4.2).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    clippy::unreachable
)]

mod support;

use std::collections::{BTreeMap, VecDeque};
use std::task::Waker;
use std::time::Duration;

use hyper_durable::{Bytes, Disk, Output, Owner, Replica, ReplicaError, Unbounded};
use hyper_liveness::{
    Change, Liveness, Output as LiveOutput, Settings as LiveSettings, Write, is_liveness,
};
use hyper_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Message,
    MessageType,
};
use hyper_timing::Exposure;
use support::cluster::settings;
use support::{Kv, Seeded, SimStore};

/// One-way delay: a base of 150 µs and up to 100 µs more, seeded: a loopback path's order.
const DELAY_NS: u64 = 150_000;
const JITTER_NS: u64 = 100_000;
/// A write's flush: 2 ms, a fast SSD's full flush.
const FLUSH_NS: u64 = 2_000_000;
/// How late a node wakes past what it asked: up to 60 µs, seeded.
const LATE_NS: u64 = 60_000;
/// The simulated clock's resolution: its readings are whole nanoseconds.
const RESOLUTION: Duration = Duration::from_nanos(1);
/// The store's depth.
const DEPTH: usize = 3;
/// A millisecond.
const MS: u64 = 1_000_000;

type Member = Replica<SimStore, Kv, Bytes>;

#[derive(Default)]
struct Live {
    heartbeats: Vec<(u64, Vec<u8>)>,
    flush: bool,
    changes: Vec<Change>,
}

impl LiveOutput for Live {
    fn heartbeat(&mut self, peer: u64, message: &[u8]) {
        self.heartbeats.push((peer, message.to_vec()));
    }
    fn flush(&mut self) {
        self.flush = true;
    }
    fn change(&mut self, change: Change) {
        self.changes.push(change);
    }
}

struct Node {
    id: u64,
    owner: Owner<SimStore, Kv, Bytes>,
    handle: hyper_durable::Handle,
    liveness: Liveness,
    /// When the stream's liveness write, out, is durable.
    flushing: Option<(u64, u64)>,
    /// When the replica's writes out are durable.
    durable_at: Option<u64>,
    /// Where the node's device stalled, the stall's end: every write out on it at the stall's start
    /// or given to it before the end is durable at the end and not sooner (`World::stall`); zero,
    /// never stalled.
    held: u64,
    /// When the node is next woken.
    alarm: Option<u64>,
    alive: bool,
    /// The node's run, as its durable record holds it: one at its first start, raised at each
    /// start again (`hyper_liveness::Settings::run`).
    run: u64,
}

enum Payload {
    Raft(Message),
    Liveness(Vec<u8>),
}

struct World {
    nodes: Vec<Node>,
    /// In flight: arrival, sequence → (to, from, payload).
    flight: BTreeMap<(u64, u64), (u64, u64, Payload)>,
    sent: u64,
    now: u64,
    rng: Seeded,
    /// Raft messages sent, all told.
    raft_sent: u64,
    suspicions: Vec<(u64, hyper_liveness::Suspicion)>,
    /// Each suspicion with what its judge knew of its own durability as it suspected.
    proofs: Vec<(u64, hyper_liveness::Suspicion, OwnWait)>,
    /// Restarts the streams reported: who saw whom start again.
    restarts: Vec<(u64, u64)>,
    /// Announced stops the streams reported: who saw whom stop.
    stops: Vec<(u64, u64)>,
    /// Changes the streams reported, all told.
    changes: u64,
    seed: u64,
    /// Each node's stream and replica write to one log, a frame at a time: the stream's write given
    /// while the replica's is out waits for it and goes in the next frame, as Focal's member's
    /// liveness update waits on the part's frame (focal-bus `member.rs`, one hyper-log).
    one_log: bool,
    /// Each node judges its peers' processes apart from their disks (`Liveness::set_disk_apart`,
    /// CP-T2.2 step 2): a heartbeat leaves whatever the node's flushes are doing.
    apart: bool,
    /// Its leaders apply their own entries before their writes of them are durable
    /// (`Config::apply_unpersisted`).
    ahead: bool,
    /// The group's disk mode (`Settings::disk`): in `Disk::Apart` a leader's patience with its own
    /// disk is the law's, its budget's room filling (`Replica::disk_fill`).
    disk: Disk,
    /// Each replica's budget, bytes (`How::budget`).
    budget: u64,
    /// Each proposal refused for room (`ReplicaError::Budget`): when, and the leader that refused it.
    refused_for_room: Vec<(u64, u64)>,
    /// A timeline of what each node did from `from`, a line an event, kept while asked for.
    trace: Option<Trace>,
    /// Each Raft message a node sent that carried or depended on a term or vote its disk did not
    /// yet hold (hyper-durable's I1, `breaks_promise`): when, its sender, its kind and its term.
    broken: Vec<(u64, u64, MessageType, u64)>,
    /// Each vote a node gave, as it left: when, the voter, the candidate and the term.
    votes: Vec<(u64, u64, u64, u64)>,
    /// Each disk verdict a node's stream gave (`Change::DiskSlow`, `DiskCaughtUp`): when, the
    /// judge, the peer, and whether slow.
    disks: Vec<(u64, u64, u64, bool)>,
    /// Each change of a peer's disk mode a node's stream told: when, the judge, the peer, and
    /// whether its heartbeats turned to the other mode (`Change::ModeDiffers`) or back.
    modes: Vec<(u64, u64, u64, bool)>,
    /// Each hand-over's order as it left (`MsgTimeoutNow`): when, the leader, and the heir it named.
    timeouts: Vec<(u64, u64, u64)>,
}

/// What each node did, from `from` until `until`: its writes given and made durable, the proofs
/// its stream asked, the heartbeats it sent and took, its suspicions, its votes and its terms.
struct Trace {
    from: u64,
    until: u64,
    lines: Vec<String>,
}

impl Trace {
    fn at(&self, now: u64) -> Option<f64> {
        (now >= self.from && now <= self.until).then(|| (now - self.from) as f64 / 1e6)
    }
}

/// A judge's own wait for a flush proof as it suspected: since when, and its end if it came.
type OwnWait = Option<(u64, Option<u64>)>;

/// What a judge knew of its own durability as it suspected (hyper-liveness's `own_proof`).
fn own_wait(liveness: &Liveness) -> OwnWait {
    liveness.own_proof().waited
}

/// A Raft message held to what its sender's disk holds as it leaves (hyper-durable's I1,
/// `docs/durable.md`): one that carries or depends on a term or vote leaves only once the disk holds
/// that term and vote, as Raft's servers persist both before they answer (thesis §3.8). A pre-vote
/// asks about a term its sender doesn't take. Whether `message`, from `member` whose disk holds
/// `durable` (its term and vote), breaks it.
fn breaks_promise(durable: (u64, u64), message: &Message, member: u64) -> bool {
    let (term, vote) = durable;
    if matches!(
        message.msg_type,
        MessageType::MsgRequestPreVote | MessageType::MsgRequestPreVoteResponse
    ) {
        return false;
    }
    if message.term > term {
        return true;
    }
    let current = message.term == term;
    match message.msg_type {
        MessageType::MsgRequestVote if current => vote != member,
        MessageType::MsgRequestVoteResponse if current && !message.reject => vote != message.to,
        _ => false,
    }
}

/// A replica's settings in the world: the cluster's, a leader applying its own entries before its
/// write of them is durable where `ahead` (`Config::apply_unpersisted`, core step R-6).
fn member_settings(id: u64, seed: u64, ahead: bool, disk: Disk) -> hyper_durable::Settings {
    let mut s = settings(id, seed);
    s.core.apply_unpersisted = ahead;
    s.disk = disk;
    s
}

impl World {
    fn new(seed: u64) -> Self {
        Self::with(seed, How::default())
    }

    /// A world made as `how` says: its leaders applying their own entries before their writes of
    /// them are durable, its nodes judging their peers apart from their disks.
    fn with(seed: u64, how: How) -> Self {
        let configuration = ConfState {
            voters: vec![1, 2, 3],
            ..ConfState::default()
        };
        let nodes = (1..=3)
            .map(|id| {
                let replica: Member = Replica::open(
                    &member_settings(id, seed, how.apply_ahead, how.disk),
                    SimStore::new(DEPTH),
                    Kv::new(configuration.clone(), false),
                    Bytes::new(how.budget()),
                )
                .unwrap();
                let mut owner = Owner::new(vec![Waker::noop().clone()]);
                let handle = owner.insert(replica).map_err(|_| ()).unwrap();
                let mut liveness = Liveness::new(LiveSettings {
                    local: id,
                    run: 1,
                    max_peers: 3,
                    history: Exposure::new(),
                    resolution: RESOLUTION,
                })
                .unwrap();
                liveness.set_disk_apart(how.disk_apart);
                owner.pairs(handle, &mut liveness, false).unwrap();
                Node {
                    id,
                    owner,
                    handle,
                    liveness,
                    flushing: None,
                    durable_at: None,
                    held: 0,
                    alarm: Some(0),
                    alive: true,
                    run: 1,
                }
            })
            .collect();
        Self {
            nodes,
            flight: BTreeMap::new(),
            sent: 0,
            now: 0,
            rng: Seeded(seed),
            raft_sent: 0,
            suspicions: Vec::new(),
            proofs: Vec::new(),
            restarts: Vec::new(),
            stops: Vec::new(),
            changes: 0,
            seed,
            one_log: false,
            apart: how.disk_apart,
            ahead: how.apply_ahead,
            disk: how.disk,
            budget: how.budget(),
            refused_for_room: Vec::new(),
            trace: None,
            broken: Vec::new(),
            votes: Vec::new(),
            disks: Vec::new(),
            modes: Vec::new(),
            timeouts: Vec::new(),
        }
    }

    /// The node `id` starts again on what its store holds durable, a new run of its liveness
    /// stream (its run record raised): the process it was is gone, the node is not.
    fn restart(&mut self, id: u64) {
        let at = (id - 1) as usize;
        let node = &mut self.nodes[at];
        let mut old = node.owner.remove(node.handle).unwrap();
        let disk = old.log_mut().disk.clone();
        let machine = old.machine().crashed();
        drop(old);
        let replica: Member = Replica::open(
            &member_settings(id, self.seed, self.ahead, self.disk),
            SimStore::from_disk(disk, DEPTH),
            machine,
            Bytes::new(self.budget),
        )
        .unwrap();
        let mut owner = Owner::new(vec![Waker::noop().clone()]);
        let handle = owner.insert(replica).map_err(|_| ()).unwrap();
        let run = node.run + 1;
        let mut liveness = Liveness::new(LiveSettings {
            local: id,
            run,
            max_peers: 3,
            history: Exposure::new(),
            resolution: RESOLUTION,
        })
        .unwrap();
        liveness.set_disk_apart(self.apart);
        owner.pairs(handle, &mut liveness, false).unwrap();
        *node = Node {
            id,
            owner,
            handle,
            liveness,
            flushing: None,
            durable_at: None,
            held: 0,
            alarm: Some(self.now),
            alive: true,
            run,
        };
    }

    fn record(&mut self, at: u64, change: &Change) {
        self.changes += 1;
        match change {
            Change::Suspected(suspicion) => {
                self.suspicions.push((at, *suspicion));
                let own = own_wait(&self.nodes[(at - 1) as usize].liveness);
                self.proofs.push((at, *suspicion, own));
            }
            Change::Restarted { peer, .. } => self.restarts.push((at, *peer)),
            Change::Stopped { peer, .. } => self.stops.push((at, *peer)),
            Change::DiskSlow { peer, at_ns } => self.disks.push((*at_ns, at, *peer, true)),
            Change::DiskCaughtUp { peer, at_ns } => self.disks.push((*at_ns, at, *peer, false)),
            Change::ModeDiffers { peer, at_ns } => self.modes.push((*at_ns, at, *peer, true)),
            Change::ModeAgrees { peer, at_ns } => self.modes.push((*at_ns, at, *peer, false)),
            Change::Trusted { .. } => {}
        }
    }

    fn send(&mut self, from: u64, to: u64, payload: Payload) {
        let at = self.now + DELAY_NS + self.rng.below(JITTER_NS);
        self.sent += 1;
        self.flight.insert((at, self.sent), (to, from, payload));
    }

    /// The node `id` acts at the clock: takes what is durable, polls its stream, and drives.
    fn act(&mut self, id: u64) {
        let now = self.now;
        let at = (id - 1) as usize;
        let mut live = Live::default();
        let mut outgoing: Vec<(u64, u64, Payload)> = Vec::new();
        let one_log = self.one_log;
        let tracing = self
            .trace
            .as_ref()
            .and_then(|t| t.at(now).map(|ms| (ms, t.from)));
        let mut notes: Vec<String> = Vec::new();
        let (changes, me, durable) = {
            let node = &mut self.nodes[at];
            if !node.alive {
                return;
            }
            let before = {
                let replica = node.owner.get(node.handle).unwrap();
                (replica.term(), replica.is_leader())
            };
            if let Some((started, durable)) = node.flushing
                && durable <= now
            {
                node.flushing = None;
                node.liveness.on_durable(Write::Liveness, started, durable);
                if let Some((ms, from)) = tracing {
                    let given = (started as f64 - from as f64) / 1e6;
                    notes.push(format!(
                        "{ms:9.3} n{id}: the stream's write durable, given at {given:.3}"
                    ));
                }
            }
            if node.durable_at.is_some_and(|due| due <= now) {
                node.durable_at = None;
                let replica = node.owner.get_mut(node.handle).unwrap();
                while replica.log_mut().make_durable() {}
                node.owner.schedule(node.handle);
                if let Some((ms, _)) = tracing {
                    notes.push(format!("{ms:9.3} n{id}: the log's writes durable"));
                }
            }
            node.liveness.poll(now, &mut live);
            if let Some((ms, from)) = tracing {
                if live.flush {
                    notes.push(format!(
                        "{ms:9.3} n{id}: the stream asks a write, a heartbeat due unproved"
                    ));
                }
                for change in &live.changes {
                    notes.push(match change {
                        Change::Suspected(s) => format!(
                            "{ms:9.3} n{id}: SUSPECTS n{} (its point passed at {:.3})",
                            s.peer,
                            (s.at_ns as f64 - from as f64) / 1e6
                        ),
                        Change::Trusted { .. } => {
                            format!("{ms:9.3} n{id}: trusts n{} again", change.peer())
                        }
                        Change::Restarted { .. } => {
                            format!("{ms:9.3} n{id}: n{} restarted", change.peer())
                        }
                        Change::DiskSlow { .. } => {
                            format!("{ms:9.3} n{id}: n{}'s disk SLOW", change.peer())
                        }
                        Change::DiskCaughtUp { .. } => {
                            format!("{ms:9.3} n{id}: n{}'s disk caught up", change.peer())
                        }
                        Change::ModeDiffers { .. } => {
                            format!(
                                "{ms:9.3} n{id}: n{}'s heartbeats of the OTHER disk mode",
                                change.peer()
                            )
                        }
                        Change::ModeAgrees { .. } => {
                            format!(
                                "{ms:9.3} n{id}: n{}'s heartbeats of its disk mode again",
                                change.peer()
                            )
                        }
                        Change::Stopped { .. } => {
                            format!("{ms:9.3} n{id}: n{} announced its stop", change.peer())
                        }
                    });
                }
            }
            let held = node.held;
            if std::mem::take(&mut live.flush) && node.flushing.is_none() {
                let frame = match node.durable_at {
                    Some(out) if one_log => out + FLUSH_NS,
                    _ => now + FLUSH_NS,
                };
                node.flushing = Some((now, frame.max(held)));
            }
            for change in &live.changes {
                node.owner.believe(change);
            }
            let changes = std::mem::take(&mut live.changes);
            let me = node.id;
            node.owner.measure(&mut node.liveness);
            node.owner.schedule(node.handle);
            let mut out = Output::default();
            let mut wake = None;
            let mut flushed = Vec::new();
            for _ in 0..64 {
                let mut more = false;
                node.owner.turn(now, &mut out, |_, driven, out| {
                    let driven = driven.expect("a drive");
                    more |= driven.more;
                    wake = driven.wake;
                    flushed.extend(driven.flushed);
                    for message in out.messages.drain(..) {
                        outgoing.push((message.to, id, Payload::Raft(message)));
                    }
                });
                if !more {
                    break;
                }
            }
            for (started, durable) in flushed {
                node.liveness.on_durable(Write::Log, started, durable);
            }
            // What waits on the node's disk, for its stream's own verdict (Focal's 5a).
            node.liveness
                .on_pending(node.owner.get(node.handle).unwrap().out_since());
            let replica = node.owner.get_mut(node.handle).unwrap();
            // The term and vote the node's disk holds as this turn's messages leave: nothing
            // becomes durable within a turn (`make_durable` above, at its start).
            let hard = &replica.log().disk.hard;
            let durable = (hard.term, hard.vote);
            if replica.log_mut().pending() > 0 && node.durable_at.is_none() {
                node.durable_at = Some((now + FLUSH_NS).max(held));
                if let Some((ms, from)) = tracing
                    && held > now
                {
                    let end = (held as f64 - from as f64) / 1e6;
                    notes.push(format!(
                        "{ms:9.3} n{id}: the log's writes given, held to {end:.3}"
                    ));
                }
            }
            let after = (replica.term(), replica.is_leader());
            if let Some((ms, _)) = tracing
                && after != before
            {
                notes.push(format!(
                    "{ms:9.3} n{id}: term {} {} -> term {} {}",
                    before.0,
                    if before.1 { "leading" } else { "following" },
                    after.0,
                    if after.1 { "LEADING" } else { "following" }
                ));
            }
            node.owner
                .pairs(node.handle, &mut node.liveness, false)
                .unwrap();
            if let Some((ms, _)) = tracing
                && !live.heartbeats.is_empty()
            {
                let peers: Vec<u64> = live.heartbeats.iter().map(|(peer, _)| *peer).collect();
                notes.push(format!("{ms:9.3} n{id}: heartbeats to {peers:?}"));
            }
            for (peer, message) in live.heartbeats.drain(..) {
                outgoing.push((peer, id, Payload::Liveness(message)));
            }
            node.alarm = [
                node.liveness.wake(),
                wake,
                node.flushing.map(|(_, durable)| durable),
                node.durable_at,
            ]
            .into_iter()
            .flatten()
            .filter(|at| *at > now)
            .min();
            (changes, me, durable)
        };
        for change in &changes {
            self.record(me, change);
        }
        if let Some((ms, _)) = tracing {
            for (to, _, payload) in &outgoing {
                if let Payload::Raft(m) = payload
                    && !matches!(
                        m.msg_type,
                        MessageType::MsgAppend
                            | MessageType::MsgAppendResponse
                            | MessageType::MsgHeartbeat
                            | MessageType::MsgHeartbeatResponse
                    )
                {
                    notes.push(format!(
                        "{ms:9.3} n{id} -> n{to}: {:?} term {}{}",
                        m.msg_type,
                        m.term,
                        if m.reject { " rejected" } else { "" }
                    ));
                }
            }
        }
        if let Some(trace) = &mut self.trace {
            trace.lines.append(&mut notes);
        }
        for (to, _, payload) in &outgoing {
            let Payload::Raft(m) = payload else { continue };
            if breaks_promise(durable, m, id) {
                self.broken.push((now, id, m.msg_type, m.term));
            }
            if m.msg_type == MessageType::MsgRequestVoteResponse && !m.reject {
                self.votes.push((now, id, *to, m.term));
            }
            if m.msg_type == MessageType::MsgTimeoutNow {
                self.timeouts.push((now, id, *to));
            }
        }
        for (to, from, payload) in outgoing {
            if matches!(payload, Payload::Raft(_)) {
                self.raft_sent += 1;
            }
            self.send(from, to, payload);
        }
    }

    /// The next event: an arrival, or a node's alarm (woken late by a seeded lateness).
    fn next(&mut self) {
        let arrival = self.flight.keys().next().copied();
        let alarm = self
            .nodes
            .iter()
            .filter(|n| n.alive)
            .filter_map(|n| n.alarm.map(|at| (at, n.id)))
            .min();
        match (arrival, alarm) {
            (Some((at, seq)), alarm) if alarm.is_none_or(|(due, _)| at <= due) => {
                self.now = self.now.max(at);
                let (to, from, payload) = self.flight.remove(&(at, seq)).unwrap();
                let mut seen = Vec::new();
                let node = &mut self.nodes[(to - 1) as usize];
                if !node.alive {
                    return;
                }
                match payload {
                    Payload::Raft(message) => {
                        let replica = node.owner.get_mut(node.handle).unwrap();
                        let _ = replica.step(message);
                    }
                    Payload::Liveness(bytes) => {
                        assert!(is_liveness(&bytes));
                        let mut live = Live::default();
                        let taken = node
                            .liveness
                            .on_heartbeat(from, &bytes, self.now, &mut live);
                        if let Some(trace) = &mut self.trace
                            && let Some(ms) = trace.at(self.now)
                        {
                            let how = match &taken {
                                Ok(_) => String::new(),
                                Err(refused) => format!(" refused: {refused:?}"),
                            };
                            trace
                                .lines
                                .push(format!("{ms:9.3} n{to} <- n{from}: heartbeat{how}"));
                            for change in &live.changes {
                                if let Change::Suspected(s) = change {
                                    trace.lines.push(format!(
                                        "{ms:9.3} n{to}: SUSPECTS n{} on a heartbeat's arrival",
                                        s.peer
                                    ));
                                }
                            }
                        }
                        for change in &live.changes {
                            node.owner.believe(change);
                        }
                        seen = live.changes;
                    }
                }
                for change in &seen {
                    self.record(to, change);
                }
                self.act(to);
            }
            (_, Some((due, id))) => {
                self.now = self.now.max(due) + self.rng.below(LATE_NS);
                // An alarm is set only past the present (`act`), so its wait began before it: the
                // stream's own wait where the alarm is its wake (`Liveness::on_wait`).
                let node = &mut self.nodes[(id - 1) as usize];
                if node.liveness.wake() == Some(due) {
                    node.liveness.on_wait(due, self.now);
                }
                self.act(id);
            }
            (Some(_), None) => unreachable!("matched above"),
            (None, None) => panic!("nothing in flight and nothing due"),
        }
    }

    /// Node `id`'s device stalls from now to `end`: the writes out on it, the stream's and the
    /// replica's, and every one given to it before `end`, are durable at `end` and not sooner, as
    /// every file's fsync on a volume waits on its journal's commit.
    fn stall(&mut self, id: u64, end: u64) {
        let node = &mut self.nodes[(id - 1) as usize];
        node.held = end;
        node.flushing = node
            .flushing
            .map(|(started, durable)| (started, durable.max(end)));
        node.durable_at = node.durable_at.map(|due| due.max(end));
    }

    /// The node `id`'s device, stalled (`World::stall`), answers again at `at`: every write out on
    /// it is durable then, no later than the stall would have held it.
    fn release(&mut self, id: u64, at: u64) {
        let node = &mut self.nodes[(id - 1) as usize];
        node.held = node.held.min(at);
        node.flushing = node
            .flushing
            .map(|(started, durable)| (started, durable.min(at).max(started)));
        node.durable_at = node.durable_at.map(|due| due.min(at));
        node.alarm = Some(node.alarm.map_or(at, |alarm| alarm.min(at)));
    }

    /// The next event, if one is due by `limit`; otherwise the clock moves to `limit`.
    fn next_by(&mut self, limit: u64) {
        let arrival = self.flight.keys().next().map(|(at, _)| *at);
        let alarm = self
            .nodes
            .iter()
            .filter(|n| n.alive)
            .filter_map(|n| n.alarm)
            .min();
        match arrival.into_iter().chain(alarm).min() {
            Some(at) if at <= limit => self.next(),
            _ => self.now = self.now.max(limit),
        }
    }

    /// Runs to `until` with a publisher offering a publish every `every_ns`, each offered again
    /// until a leader takes it, as the bus's client follows a redirect (`Load`).
    fn run_loaded(&mut self, until: u64, every_ns: u64, load: &mut Load) {
        if load.next_ns == 0 {
            load.next_ns = self.now;
        }
        while self.now < until {
            while load.next_ns <= self.now {
                load.waiting.push_back(load.offered.len() as u64);
                load.offered.push(load.next_ns);
                load.applied.push(None);
                load.by_leader.push(None);
                load.durable_on.push(0);
                load.next_ns += every_ns;
            }
            while let Some(&sequence) = load.waiting.front() {
                let mut data = LOAD.to_vec();
                data.extend_from_slice(&sequence.to_le_bytes());
                if !self.propose(&data) {
                    load.refused += 1;
                    break;
                }
                load.waiting.pop_front();
            }
            self.next_by(load.next_ns.min(until));
            self.observe(load);
        }
    }

    /// What the publisher sees: each publish's first application on any node, and who leads; and
    /// what each node's disk holds of the load, so that an answer the watched node gives is known
    /// to stand on a majority's disks or not.
    fn observe(&self, load: &mut Load) {
        for (index, node) in self.nodes.iter().enumerate() {
            let disk = &self.replica(node.id).log().disk;
            if disk.entries.len() < load.durable_read[index] {
                load.durable_read[index] = 0;
            }
            for entry in disk.entries.iter().skip(load.durable_read[index]) {
                let Some(sequence) = entry
                    .data
                    .strip_prefix(LOAD)
                    .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                    .map(u64::from_le_bytes)
                else {
                    continue;
                };
                if let Some(on) = load.durable_on.get_mut(sequence as usize) {
                    *on |= 1 << index;
                }
            }
            load.durable_read[index] = disk.entries.len();
        }
        for (index, node) in self.nodes.iter().enumerate() {
            if !node.alive {
                continue;
            }
            let entries = &self.replica(node.id).machine().now.entries;
            for (_, _, data) in entries.iter().skip(load.read[index]) {
                let Some(sequence) = data
                    .strip_prefix(LOAD)
                    .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                    .map(u64::from_le_bytes)
                else {
                    continue;
                };
                let applied = &mut load.applied[sequence as usize];
                if applied.is_none() {
                    *applied = Some(self.now);
                }
                if node.id == load.watch {
                    let by_leader = &mut load.by_leader[sequence as usize];
                    if by_leader.is_none() {
                        *by_leader = Some(self.now);
                        if load.durable_on[sequence as usize].count_ones() < 2 {
                            load.off_majority += 1;
                        }
                    }
                }
            }
            load.read[index] = entries.len();
        }
        let term = self
            .nodes
            .iter()
            .filter(|n| n.alive)
            .map(|n| self.replica(n.id).term())
            .max()
            .unwrap_or(0);
        let lead = (self.leader().unwrap_or(0), term);
        if load.leads.last().is_none_or(|&(_, l, t)| (l, t) != lead) {
            load.leads.push((self.now, lead.0, lead.1));
        }
    }

    fn replica(&self, id: u64) -> &Member {
        let node = &self.nodes[(id - 1) as usize];
        node.owner.get(node.handle).unwrap()
    }

    fn leader(&self) -> Option<u64> {
        self.nodes
            .iter()
            .filter(|n| n.alive)
            .map(|n| n.id)
            .filter(|id| self.replica(*id).is_leader())
            .max_by_key(|id| self.replica(*id).term())
    }

    /// What moves: each live node's term, commit, applied index and heartbeats sent and taken.
    fn progress(&self) -> Vec<(u64, u64, u64, u64)> {
        self.nodes
            .iter()
            .filter(|n| n.alive)
            .map(|n| {
                let r = self.replica(n.id);
                let heard: u64 = [1, 2, 3]
                    .iter()
                    .filter_map(|peer| n.liveness.report(*peer))
                    .map(|report| report.taken + report.sent)
                    .sum();
                (
                    r.term(),
                    r.core().raft.log().committed(),
                    r.applied().index,
                    heard,
                )
            })
            .collect()
    }

    /// Runs until `done`, failing once nothing moves for a quiet period (`docs/sim.md` §4.2):
    /// the longest detection bound a node's detectors state and the members' election, from
    /// their own timing, or, before any is configured, a flush and the delays of a round.
    fn run_until(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let mut seen = self.progress();
        let mut moved = self.now;
        while !done(self) {
            self.next();
            let now = self.progress();
            if now != seen {
                seen = now;
                moved = self.now;
            }
            assert!(
                self.now <= moved + self.quiet(),
                "seed {}: nothing moved for a quiet period: {:?}",
                self.seed,
                self.progress()
            );
        }
    }

    /// The quiet period (`docs/sim.md` §4.2): the longest any live node trusts a peer past its
    /// heartbeat's expected arrival, or, for a pair no margin judges yet, the interval its
    /// evidence comes at, the slowest election's span and rounds, and a round of the network and a
    /// flush, by the nodes' own measurements; doubled, as a node acts on them only at its next
    /// wake.
    fn quiet(&self) -> u64 {
        let (mut freshness, mut election) = (0u64, 0u64);
        for node in self.nodes.iter().filter(|n| n.alive) {
            for peer in 1..=3u64 {
                let Some(report) = node.liveness.report(peer) else {
                    continue;
                };
                let waits = if report.judged {
                    report.freshness
                } else {
                    report.interval
                };
                if let Some(waits) = waits {
                    freshness = freshness.max(waits.as_nanos() as u64);
                }
            }
            if let Some(t) = node.owner.get(node.handle).unwrap().core().raft.timing() {
                election = election.max((t.span + t.round * 4).as_nanos() as u64);
            }
        }
        2 * (freshness + election + FLUSH_NS + 2 * (DELAY_NS + JITTER_NS))
    }

    /// Whether every other live node judges `peer` by a configured detector: the fact a crash of
    /// `peer` needs to be suspected (`docs/timing.md` §3, item 10).
    fn judged(&self, peer: u64) -> bool {
        self.nodes
            .iter()
            .filter(|n| n.alive && n.id != peer)
            .all(|n| n.liveness.report(peer).is_some_and(|r| r.configured))
    }

    fn propose(&mut self, data: &[u8]) -> bool {
        let Some(leader) = self.leader() else {
            return false;
        };
        let node = &mut self.nodes[(leader - 1) as usize];
        let proposed = node
            .owner
            .get_mut(node.handle)
            .unwrap()
            .propose(Vec::new(), data.to_vec());
        node.owner.schedule(node.handle);
        node.alarm = Some(self.now);
        if let Err(ReplicaError::Budget(_)) = proposed {
            self.refused_for_room.push((self.now, leader));
        }
        proposed.is_ok()
    }

    /// The leader proposes a change of its configuration: `kind` of `node`.
    fn change(&mut self, kind: ConfChangeType, node: u64) -> bool {
        let Some(leader) = self.leader() else {
            return false;
        };
        let change = ConfChangeV2 {
            transition: ConfChangeTransition::Auto,
            changes: vec![ConfChangeSingle {
                change_type: kind,
                node_id: node,
            }],
            context: vec![],
        };
        let at = &mut self.nodes[(leader - 1) as usize];
        let ok = at
            .owner
            .get_mut(at.handle)
            .unwrap()
            .change(Vec::new(), &change)
            .is_ok();
        at.owner.schedule(at.handle);
        at.alarm = Some(self.now);
        ok
    }

    /// Whether every live node's configuration names `node` a voter (`named`), or none does.
    fn named_everywhere(&self, node: u64, named: bool) -> bool {
        self.nodes
            .iter()
            .filter(|n| n.alive)
            .all(|n| self.replica(n.id).configuration().voters.contains(&node) == named)
    }

    fn applied_everywhere(&self, data: &[u8]) -> bool {
        self.nodes.iter().filter(|n| n.alive).all(|n| {
            self.replica(n.id)
                .machine()
                .now
                .entries
                .iter()
                .any(|(_, _, d)| d == data)
        })
    }
}

#[test]
fn the_shell_elects_sleeps_and_fails_over_on_the_liveness_stream() {
    elects_sleeps_and_fails_over(How::default());
}

/// CP-T2.2 step 2: the same in the step's group (`APART`): a real failure, the leader's node
/// killed, is suspected within the bound each suspicion states, and the survivors elect.
#[test]
fn judged_apart_the_shell_elects_sleeps_and_fails_over_on_the_liveness_stream() {
    elects_sleeps_and_fails_over(APART);
}

fn elects_sleeps_and_fails_over(how: How) {
    let mut idle = (0u64, 0u64);
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    for seed in first..first + support::count("HYPER_DURABLE_LIVENESS_SEEDS", 64) {
        let mut world = World::with(seed, how);
        // Elected from nothing: the members know no leader and draw over the span the law chose
        // from the stream's first echoed round trips.
        world.run_until(|w| w.leader().is_some());
        assert!(world.propose(b"first"));
        world.run_until(|w| w.applied_everywhere(b"first"));
        world.run_until(|w| w.leader().is_some_and(|leader| w.judged(leader)));
        // Idle: no Raft message at all while the stream's heartbeats go on, but where a detector
        // changed its mind (a mistake within its allowance, one heartbeat in each margin, §2.8):
        // a follower that suspects its leader asks for pre-votes, and the leader tells it who
        // leads.
        let (raft, changes) = (world.raft_sent, world.changes);
        let until = world.now + world.quiet();
        world.run_until(|w| w.now >= until);
        assert!(
            world.raft_sent == raft || world.changes > changes,
            "seed {seed}: an idle group sent {} messages with no detector's change",
            world.raft_sent - raft
        );
        idle.0 += world.raft_sent - raft;
        idle.1 += world.changes - changes;
        // The leader's node dies, once one leads and the others judge it (a mistake in the idle
        // time may have moved the lead).
        world.run_until(|w| w.leader().is_some_and(|leader| w.judged(leader)));
        let leader = world.leader().unwrap();
        let killed_at = world.now;
        world.nodes[(leader - 1) as usize].alive = false;
        world.suspicions.clear();
        world.run_until(|w| {
            w.leader().is_some_and(|l| l != leader)
                && w.nodes
                    .iter()
                    .filter(|n| n.alive)
                    .all(|n| n.liveness.trust(leader) == Some(hyper_timing::Trust::Suspected))
        });
        // A suspicion made after the kill states its bound; one a survivor held already (a
        // mistake just before it) stands.
        for (_, suspicion) in world
            .suspicions
            .iter()
            .filter(|(_, s)| s.peer == leader && s.at_ns >= killed_at)
        {
            if let (Some(bound), Some(last)) = (suspicion.detection, suspicion.last) {
                // The bound runs from the sender's last schedule; on one clock that is its due.
                assert!(
                    suspicion.at_ns <= last.due_ns + bound.as_nanos() as u64 + LATE_NS,
                    "seed {seed}: suspected at {} past the bound {bound:?} from {}",
                    suspicion.at_ns,
                    last.due_ns
                );
            }
        }
        assert!(world.propose(b"after"));
        world.run_until(|w| w.applied_everywhere(b"after"));
        // The killed node starts again: every survivor's stream reports the restart, which reaches
        // the survivors' cores (`Replica::restarted`), and the node catches up.
        world.restarts.clear();
        world.restart(leader);
        world.run_until(|w| {
            [1, 2, 3]
                .iter()
                .filter(|id| **id != leader)
                .all(|id| w.restarts.contains(&(*id, leader)))
        });
        world.run_until(|w| w.leader().is_some());
        assert!(world.propose(b"rejoined"));
        world.run_until(|w| w.applied_everywhere(b"rejoined"));
    }
    println!(
        "idle: {} Raft messages, all after {} detectors' changes",
        idle.0, idle.1
    );
}

/// Item 10 of `docs/timing.md` §3 in the shell: the leader's node killed as soon as the group has
/// elected it, before any survivor's link to it has evidence of its own. Each survivor has one live
/// link left, and the leader's link is judged by what its node measured of its links once that one
/// has its evidence: every survivor suspects the dead leader, each suspicion within the bound it
/// states, and the survivors elect and commit.
#[test]
fn a_leader_killed_before_its_links_have_evidence_is_replaced() {
    let mut slowest = 0u64;
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    for seed in first..first + support::count("HYPER_DURABLE_LIVENESS_SEEDS", 64) {
        let mut world = World::new(seed);
        world.run_until(|w| w.leader().is_some());
        let leader = world.leader().unwrap();
        assert!(
            world
                .nodes
                .iter()
                .filter(|n| n.id != leader)
                .all(|n| n.liveness.report(leader).is_some_and(|r| !r.configured)),
            "seed {seed}: a link to the leader had its evidence when the group elected"
        );
        let killed_at = world.now;
        world.nodes[(leader - 1) as usize].alive = false;
        world.suspicions.clear();
        world.run_until(|w| {
            w.leader().is_some_and(|l| l != leader)
                && w.nodes
                    .iter()
                    .filter(|n| n.alive)
                    .all(|n| n.liveness.trust(leader) == Some(hyper_timing::Trust::Suspected))
        });
        for (_, suspicion) in world.suspicions.iter().filter(|(_, s)| s.peer == leader) {
            if let (Some(bound), Some(last)) = (suspicion.detection, suspicion.last) {
                assert!(
                    suspicion.at_ns <= last.due_ns + bound.as_nanos() as u64 + LATE_NS,
                    "seed {seed}: suspected at {} past the bound {bound:?} from {}",
                    suspicion.at_ns,
                    last.due_ns
                );
            }
        }
        slowest = slowest.max(world.now - killed_at);
        assert!(world.propose(b"after"));
        world.run_until(|w| w.applied_everywhere(b"after"));
    }
    println!(
        "a leader killed young: replaced within {} ms of the kill at the most",
        slowest / 1_000_000
    );
}

/// A node removed from its group while suspected, started again and added back counts in the
/// group's quorum once more, with the third node then dead: the removal, restart and return end to
/// end on the liveness stream. The core forgets what it was told of a member its configuration no
/// longer names, and the replica is told what its stream believes of a peer it is given
/// (`Owner::pairs`). Before both, the leader's core still suspected the node it was given back
/// (hyper-raft's `a_member_removed_while_suspected_is_believed_anew_when_added_again` holds the
/// outage that followed); here the pair, let go with the group's last and begun anew, happened to
/// tell the core again with its first judgement of the restarted node, which a pair the two nodes
/// share through another group does not.
#[test]
fn a_node_removed_while_suspected_and_added_again_counts_in_the_quorum() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    for seed in first..first + support::count("HYPER_DURABLE_LIVENESS_SEEDS", 64) {
        let mut world = World::new(seed);
        world.run_until(|w| w.leader().is_some_and(|leader| w.judged(leader)));
        let leader = world.leader().unwrap();
        let removed = if leader == 3 { 2 } else { 3 };
        let other = 6 - leader - removed;
        // A follower dies, and the leader's stream suspects it.
        world.nodes[(removed - 1) as usize].alive = false;
        world.run_until(|w| {
            w.nodes[(leader - 1) as usize].liveness.trust(removed)
                == Some(hyper_timing::Trust::Suspected)
        });
        // It is removed, starts again, and is added back.
        world.run_until(|w| w.leader().is_some());
        assert!(world.change(ConfChangeType::RemoveNode, removed));
        world.run_until(|w| w.named_everywhere(removed, false));
        world.restart(removed);
        world.run_until(|w| w.leader().is_some());
        assert!(world.change(ConfChangeType::AddNode, removed));
        world.run_until(|w| w.named_everywhere(removed, true));
        assert!(world.propose(b"rejoined"));
        world.run_until(|w| w.applied_everywhere(b"rejoined"));
        // The other follower dies. Once every live node's stream suspects it, the leader and the
        // node added back are a quorum by their detectors, and the group leads on.
        world.nodes[(other - 1) as usize].alive = false;
        world.run_until(|w| {
            w.nodes
                .iter()
                .filter(|n| n.alive)
                .all(|n| n.liveness.trust(other) == Some(hyper_timing::Trust::Suspected))
        });
        world.run_until(|w| w.leader().is_some());
        assert!(world.propose(b"after"));
        world.run_until(|w| w.applied_everywhere(b"after"));
    }
}

/// A voter's stop announced (`Liveness::stop`), its group's other members hold it out from the
/// announcement's arrival (`Change::Stopped`, `Replica::stopped`): a follower that then wrongly
/// suspects its leader is left no quorum it trusts and asks for no votes, and the leader leads on.
/// Before, the stopped voter was trusted until its freshness point passed, the detector's margin
/// after its last heartbeat, and such a follower counted it in a quorum it trusted, asked for
/// votes and named no leader meanwhile (mantle's roll; the window
/// `docs/models/SuspicionStopUnannounced.cfg` finds). Do: a group whose links are all judged; once
/// the follower took a heartbeat of the stopping node, that node announces its stop and stops; once
/// both others took the announcement, the follower's core is told its leader is suspected, wrongly,
/// for a quiet period; the stopped node starts again. Expect: the window was open (the follower
/// would have trusted the stopped node past the wrong suspicion); every step the two name the
/// leader in its term, neither asks for votes, and a write commits; the restart is told, counted no
/// failure, and the group writes again.
#[test]
fn a_voters_announced_stop_leaves_no_window_for_a_wrong_suspicion() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    for seed in first..first + support::count("HYPER_DURABLE_LIVENESS_SEEDS", 64) {
        let mut world = World::new(seed);
        world.run_until(|w| {
            w.leader()
                .is_some_and(|leader| (1..=3).all(|peer| peer == leader || w.judged(peer)))
                && w.judged(w.leader().unwrap_or(0))
        });
        let leader = world.leader().unwrap();
        let stopping = if leader == 3 { 2 } else { 3 };
        let follower = 6 - leader - stopping;
        let at = |id: u64| (id - 1) as usize;
        // The follower takes a heartbeat of the stopping node: it holds it trusted a freshness on.
        let taken = world.nodes[at(follower)]
            .liveness
            .report(stopping)
            .map_or(0, |report| report.taken);
        world.run_until(|w| {
            w.nodes[at(follower)]
                .liveness
                .report(stopping)
                .is_some_and(|report| report.taken > taken)
        });
        let Some(hyper_timing::Trust::Trusted {
            until_ns: trusted_to,
        }) = world.nodes[at(follower)].liveness.trust(stopping)
        else {
            panic!("seed {seed}: the follower does not trust the stopping node");
        };
        let failures: Vec<u64> = [leader, follower]
            .iter()
            .map(|id| world.nodes[at(*id)].liveness.history().failures())
            .collect();
        // The stop, announced: the stream's last act, then nothing more of the node.
        {
            let now = world.now;
            let node = &mut world.nodes[at(stopping)];
            let mut live = Live::default();
            node.liveness.stop(now, &mut live);
            node.alive = false;
            for (peer, message) in live.heartbeats {
                world.send(stopping, peer, Payload::Liveness(message));
            }
        }
        world.run_until(|w| {
            [leader, follower]
                .iter()
                .all(|id| w.nodes[at(*id)].liveness.stopped(stopping))
        });
        let term = world.replica(leader).term();
        assert!(
            world.now < trusted_to,
            "seed {seed}: the announcement came at {} ns, past the point {trusted_to} ns the \
             follower trusted the stopped node to: no window to close",
            world.now
        );
        // The wrong suspicion, for a quiet period.
        {
            let node = &mut world.nodes[at(follower)];
            node.owner
                .get_mut(node.handle)
                .unwrap()
                .suspect(leader)
                .unwrap();
            node.owner.schedule(node.handle);
            node.alarm = Some(world.now);
        }
        let held = |w: &World| {
            let (l, f) = (w.replica(leader), w.replica(follower));
            assert!(
                l.is_leader() && l.term() == term,
                "seed {seed}: the leader lost term {term} at {} ns",
                w.now
            );
            assert!(
                f.core().raft.state() == hyper_raft::StateRole::Follower
                    && f.leader() == leader
                    && f.term() == term,
                "seed {seed}: the follower left its leader at {} ns: {:?}, leader {}, term {}",
                w.now,
                f.core().raft.state(),
                f.leader(),
                f.term()
            );
        };
        let until = world.now + world.quiet();
        while world.now < until {
            world.next();
            held(&world);
        }
        assert!(world.propose(b"while one is stopped"));
        world.run_until(|w| {
            held(w);
            w.applied_everywhere(b"while one is stopped")
        });
        // The suspicion withdrawn, the stopped node starts again: told, no failure, trusted.
        {
            let node = &mut world.nodes[at(follower)];
            node.owner
                .get_mut(node.handle)
                .unwrap()
                .trust(leader)
                .unwrap();
            node.owner.schedule(node.handle);
            node.alarm = Some(world.now);
        }
        world.restarts.clear();
        world.restart(stopping);
        world.run_until(|w| {
            [leader, follower]
                .iter()
                .all(|id| w.restarts.contains(&(*id, stopping)))
        });
        for (id, failures) in [leader, follower].iter().zip(failures) {
            assert_eq!(
                world.nodes[at(*id)].liveness.history().failures(),
                failures,
                "seed {seed}: node {id} counted the restart after an announced stop a failure"
            );
            assert!(!world.nodes[at(*id)].liveness.stopped(stopping));
        }
        world.run_until(|w| w.leader().is_some());
        assert!(world.propose(b"back"));
        world.run_until(|w| w.applied_everywhere(b"back"));
    }
}

/// mantle's orderly stop with the announcement first (`docs/timing.md` §2.9, "A voter's stop
/// announced"): the leader's node announces its stop, its members running on; once each other node
/// answered it (`Liveness::stop_taken`), it hands its group's lead over by a transfer, and its own
/// member learns the new leader from the transferee's first append of its term. From the moment
/// all three name the new leader until the stopped node is back, the two that remain name it in its
/// term, through a wrong suspicion of it. Do: a group whose links are all judged; the leader's node
/// announces its stop; once both answered, its replica transfers the lead to a follower, if it
/// still leads; once all three name one new leader, the stopping node stops, and the other
/// remaining member's core is told the new leader is suspected, wrongly, for a quiet period; the
/// stopped node starts again. Expect: every step from then, the two name the new leader in its
/// term; a write commits; the restart is told and counts no failure.
#[test]
fn an_orderly_stop_announced_first_hands_over_and_keeps_the_new_leader() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    for seed in first..first + support::count("HYPER_DURABLE_LIVENESS_SEEDS", 64) {
        let mut world = World::new(seed);
        world.run_until(|w| {
            w.leader()
                .is_some_and(|leader| (1..=3).all(|peer| peer == leader || w.judged(peer)))
                && w.judged(w.leader().unwrap_or(0))
        });
        let stopping = world.leader().unwrap();
        let others: Vec<u64> = (1..=3).filter(|id| *id != stopping).collect();
        let at = |id: u64| (id - 1) as usize;
        let failures: Vec<u64> = others
            .iter()
            .map(|id| world.nodes[at(*id)].liveness.history().failures())
            .collect();
        // The announcement first: the node runs on, its members leading and following as before.
        {
            let now = world.now;
            let node = &mut world.nodes[at(stopping)];
            let mut live = Live::default();
            node.liveness.stop(now, &mut live);
            for (peer, message) in live.heartbeats {
                world.send(stopping, peer, Payload::Liveness(message));
            }
        }
        // Answered within a quiet period, the nodes' own measure of how long a heartbeat and a
        // flush may take (`World::quiet`).
        let answered = |w: &World| {
            others
                .iter()
                .all(|peer| w.nodes[at(stopping)].liveness.stop_taken(*peer))
        };
        let until = world.now + world.quiet();
        while !answered(&world) && world.now < until {
            world.next();
        }
        assert!(
            answered(&world),
            "seed {seed}: the announcement was not answered within a quiet period"
        );
        for id in &others {
            assert!(
                world.nodes[at(*id)].liveness.stopped(stopping),
                "seed {seed}: node {id} answered an announcement it does not hold"
            );
        }
        // Then the hand-over, as mantle's: a transfer, where the node still leads.
        if world.replica(stopping).is_leader() {
            let node = &mut world.nodes[at(stopping)];
            let replica = node.owner.get_mut(node.handle).unwrap();
            replica.transfer(others[0]).unwrap();
            node.owner.schedule(node.handle);
            node.alarm = Some(world.now);
        }
        world.run_until(|w| {
            let leader = w.replica(others[0]).leader();
            leader != 0 && leader != stopping && (1..=3).all(|id| w.replica(id).leader() == leader)
        });
        let leader = world.replica(others[0]).leader();
        let follower = if leader == others[0] {
            others[1]
        } else {
            others[0]
        };
        let term = world.replica(leader).term();
        world.nodes[at(stopping)].alive = false;
        {
            let node = &mut world.nodes[at(follower)];
            node.owner
                .get_mut(node.handle)
                .unwrap()
                .suspect(leader)
                .unwrap();
            node.owner.schedule(node.handle);
            node.alarm = Some(world.now);
        }
        let held = |w: &World| {
            let (l, f) = (w.replica(leader), w.replica(follower));
            assert!(
                l.is_leader() && l.term() == term,
                "seed {seed}: the new leader lost term {term} at {} ns",
                w.now
            );
            assert!(
                f.core().raft.state() == hyper_raft::StateRole::Follower
                    && f.leader() == leader
                    && f.term() == term,
                "seed {seed}: the follower left the new leader at {} ns: {:?}, leader {}, term {}",
                w.now,
                f.core().raft.state(),
                f.leader(),
                f.term()
            );
        };
        let until = world.now + world.quiet();
        while world.now < until {
            world.next();
            held(&world);
        }
        assert!(world.propose(b"handed over"));
        world.run_until(|w| {
            held(w);
            w.applied_everywhere(b"handed over")
        });
        {
            let node = &mut world.nodes[at(follower)];
            node.owner
                .get_mut(node.handle)
                .unwrap()
                .trust(leader)
                .unwrap();
            node.owner.schedule(node.handle);
            node.alarm = Some(world.now);
        }
        world.restarts.clear();
        world.restart(stopping);
        world.run_until(|w| {
            others
                .iter()
                .all(|id| w.restarts.contains(&(*id, stopping)))
        });
        for (id, failures) in others.iter().zip(failures) {
            assert_eq!(
                world.nodes[at(*id)].liveness.history().failures(),
                failures,
                "seed {seed}: node {id} counted the restart after an announced stop a failure"
            );
        }
        world.run_until(|w| w.leader().is_some());
        assert!(world.propose(b"back"));
        world.run_until(|w| w.applied_everywhere(b"back"));
    }
}

/// What a publish's entry begins with: its sequence follows, eight bytes little-endian.
const LOAD: &[u8] = b"load";

/// A publisher's load on the group (Focal's comparison, doc 34: a publish every millisecond), each
/// publish offered again until a leader takes it, as the bus's client follows a redirect: when each
/// was offered and first applied by a node, and who led.
#[derive(Default)]
struct Load {
    /// Each publish's first offer, by its sequence.
    offered: Vec<u64>,
    /// When a node first applied each publish, by its sequence.
    applied: Vec<Option<u64>>,
    /// The next publish's offer.
    next_ns: u64,
    /// The publishes offered that no leader took yet, oldest first.
    waiting: VecDeque<u64>,
    /// Offers refused: no leader, or one that took none.
    refused: u64,
    /// How far each node's applied entries were read.
    read: [usize; 3],
    /// Each change of who leads and of the highest term: when, the leader (zero, none) and the term.
    leads: Vec<(u64, u64, u64)>,
    /// The node whose applications are watched (`by_leader`): the stall's leader; zero, none.
    watch: u64,
    /// When the watched node applied each publish, by its sequence: what its client was answered.
    by_leader: Vec<Option<u64>>,
    /// The nodes whose disks hold each publish, a bit a node, by its sequence.
    durable_on: Vec<u8>,
    /// How far each node's disk was read.
    durable_read: [usize; 3],
    /// Answers the watched node gave for a publish fewer than two disks held.
    off_majority: usize,
}

/// A suspicion noticed: its judge, the peer, when from the stall's start, and its judge's own wait
/// for a proof (since, end), from the stall's start.
type Noticed = (u64, u64, i64, Option<(i64, Option<i64>)>);

/// What a stall did, from its start: whether the leader before it lost the lead or a term was
/// spent, the suspicions noticed with their judges' own proofs, the longest stretch without a
/// leader, and the latencies of the publishes offered from a second before it to its end and a
/// quiet period past.
struct Stalled {
    seed: u64,
    leader: u64,
    term: u64,
    /// The stall's end on each node, from its start.
    ends_ns: [u64; 3],
    /// Terms spent from the stall's start.
    terms: u64,
    /// The leader before the stall lost the lead at some point.
    lost: bool,
    suspicions: Vec<Noticed>,
    /// The longest stretch with no leader, from the stall's start on.
    leaderless_ns: u64,
    /// Offered to first applied, nanoseconds, of the publishes offered in the window, sorted.
    latencies: Vec<u64>,
    /// Publishes offered in the window and never applied.
    unapplied: usize,
    /// The freshness, `η + α`, each node held each peer to as the stall began: the widest.
    freshness_ns: u64,
    /// Publishes offered while the longest stall lasted, those of them the stall's leader applied
    /// (answered) before it ended, and the slowest of those, offered to applied.
    in_stall: usize,
    acked_in_stall: usize,
    ack_most_ns: u64,
    /// Of the publishes offered in the stall, those the stall's leader never answered, and the
    /// slowest it did, offered to answered, whenever it answered.
    unanswered: usize,
    answer_most_ns: u64,
    /// Answers the stall's leader gave for a publish fewer than two disks held.
    off_majority: usize,
    /// The hand-overs the stall's leader's patience began.
    handed_over: u64,
    /// The stall's leader's first hand-over order (`MsgTimeoutNow`), from the stall's start.
    first_order_ns: Option<u64>,
    /// Hand-over orders any other leader gave from the stall's start.
    others_ordered: usize,
    /// Proposals refused for room from the stall's start (`ReplicaError::Budget`), and the first,
    /// from the stall's start.
    refused_for_room: usize,
    first_refusal_ns: Option<u64>,
    /// The fill the stall's leader read as the stall began (`Replica::disk_fill`).
    fill_ns: Option<u64>,
}

impl Stalled {
    /// The latency at `permille` thousandths: the least that many of the latencies are at most.
    fn quantile(&self, permille: usize) -> u64 {
        let at = (self.latencies.len() * permille).div_ceil(1_000);
        self.latencies.get(at.max(1) - 1).copied().unwrap_or(0)
    }

    fn line(&self) -> String {
        let ms = |ns: i64| ns as f64 / 1e6;
        let unsigned = |ns: u64| ns as f64 / 1e6;
        let suspicions: Vec<String> = self
            .suspicions
            .iter()
            .map(|(judge, peer, noticed, waited)| {
                let own = match waited {
                    Some((since, Some(end))) => {
                        format!("own wait {:.0}..{:.0}", ms(*since), ms(*end))
                    }
                    Some((since, None)) => format!("own wait {:.0}..", ms(*since)),
                    None => "no own wait".to_string(),
                };
                format!("{judge}->{peer} at {:.0} ms ({own})", ms(*noticed))
            })
            .collect();
        format!(
            "seed {}: leader {} term {}, ends {:?} ms, freshness {:.0} ms: {} terms spent, lead {}, \
             leaderless {:.0} ms; publishes p50 {:.1} p99 {:.1} p99.9 {:.1} max {:.1} ms, {} never \
             applied; offered in the stall {}, answered by its leader in it {}, the slowest {:.1} ms, \
             never {}, the slowest ever {:.1} ms, off a majority's disks {}, hand-overs {} (the \
             first ordered at {:?} ms, others' orders {}; the fill read at the stop {:?} ms), \
             proposals refused for room {} (the first at {:?} ms); suspicions [{}]",
            self.seed,
            self.leader,
            self.term,
            self.ends_ns.map(|e| e / MS),
            unsigned(self.freshness_ns),
            self.terms,
            if self.lost { "lost" } else { "kept" },
            unsigned(self.leaderless_ns),
            unsigned(self.quantile(500)),
            unsigned(self.quantile(990)),
            unsigned(self.quantile(999)),
            unsigned(self.latencies.last().copied().unwrap_or(0)),
            self.unapplied,
            self.in_stall,
            self.acked_in_stall,
            unsigned(self.ack_most_ns),
            self.unanswered,
            unsigned(self.answer_most_ns),
            self.off_majority,
            self.handed_over,
            self.first_order_ns.map(|ns| ns / MS),
            self.others_ordered,
            self.fill_ns.map(|ns| ns / MS),
            self.refused_for_room,
            self.first_refusal_ns.map(|ns| ns / MS),
            suspicions.join(", ")
        )
    }
}

/// A group of three under the publisher's load, each pair judged by a configured detector and two
/// seconds of the load behind it; then each node's device stalls from one instant to its own end,
/// `ends(leader)` from it (zero, no stall), and the group runs on to a quiet period past the last.
fn stalled(seed: u64, how: How, ends: impl Fn(u64) -> [u64; 3]) -> Stalled {
    let mut world = World::with(seed, how);
    world.one_log = how.one_log;
    world.run_until(|w| w.leader().is_some() && (1..=3).all(|peer| w.judged(peer)));
    let mut load = Load::default();
    let settled = world.now + 2_000 * MS;
    world.run_loaded(settled, MS, &mut load);
    let leader = world.leader().unwrap_or(0);
    let term = world.replica(leader.max(1)).term();
    let from = world.now;
    load.watch = leader;
    let ends_ns = ends(leader);
    let freshness_ns = world
        .nodes
        .iter()
        .flat_map(|n| (1..=3u64).filter_map(move |peer| n.liveness.report(peer)?.freshness))
        .max()
        .map_or(0, |f| f.as_nanos() as u64);
    world.proofs.clear();
    for id in 1..=3u64 {
        let end = ends_ns[(id - 1) as usize];
        if end > 0 {
            world.stall(id, from + end);
        }
    }
    let fill_ns = world
        .replica(leader.max(1))
        .disk_fill()
        .map(|fill| fill.as_nanos() as u64);
    let first = load
        .offered
        .partition_point(|offered| *offered < from.saturating_sub(1_000 * MS));
    let leads_from = load.leads.len();
    let until = from + ends_ns.into_iter().max().unwrap_or(0) + world.quiet();
    // One seed's timeline, asked by HYPER_DURABLE_STALL_TRACE.
    if support::count("HYPER_DURABLE_STALL_TRACE", u64::MAX) == seed {
        world.trace = Some(Trace {
            from,
            until,
            lines: Vec::new(),
        });
    }
    world.run_loaded(until, MS, &mut load);
    if let Some(trace) = world.trace.take() {
        println!(
            "-- seed {seed}'s timeline, ms from the stall's start (ends {:?} ms):",
            ends_ns.map(|e| e / MS)
        );
        for line in &trace.lines {
            println!("{line}");
        }
    }
    // Every publish offered in the window, given the time to be applied: the load goes on a quiet
    // period more.
    let offered = load.offered.len();
    let drained = world.now + world.quiet();
    world.run_loaded(drained, MS, &mut load);
    let rel = |ns: u64| i64::try_from(ns).unwrap() - i64::try_from(from).unwrap();
    let suspicions = world
        .proofs
        .iter()
        .map(|(judge, s, own)| {
            (
                *judge,
                s.peer,
                rel(s.noticed_ns),
                own.map(|(since, end)| (rel(since), end.map(rel))),
            )
        })
        .collect();
    let mut leaderless_ns = 0;
    let mut none_since: Option<u64> = None;
    let mut lost = false;
    let mut last_term = term;
    for &(at, lead, t) in &load.leads[leads_from.saturating_sub(1)..] {
        let at = at.max(from);
        last_term = last_term.max(t);
        if lead != leader {
            lost = true;
        }
        match (lead, none_since) {
            (0, None) => none_since = Some(at),
            (l, Some(since)) if l != 0 => {
                leaderless_ns = leaderless_ns.max(at - since);
                none_since = None;
            }
            _ => {}
        }
    }
    if let Some(since) = none_since {
        leaderless_ns = leaderless_ns.max(world.now - since);
    }
    let mut latencies = Vec::new();
    let mut unapplied = 0;
    for (offered_ns, applied) in load.offered[first..offered]
        .iter()
        .zip(&load.applied[first..offered])
    {
        match applied {
            Some(applied) => latencies.push(applied - offered_ns),
            None => unapplied += 1,
        }
    }
    latencies.sort_unstable();
    let stall_end = from + ends_ns.into_iter().max().unwrap_or(0);
    let (mut in_stall, mut acked_in_stall, mut ack_most_ns) = (0, 0, 0);
    let (mut unanswered, mut answer_most_ns) = (0, 0);
    for (offered_ns, answered) in load.offered.iter().zip(&load.by_leader) {
        if *offered_ns < from || *offered_ns >= stall_end {
            continue;
        }
        in_stall += 1;
        match *answered {
            Some(at) => {
                answer_most_ns = answer_most_ns.max(at - offered_ns);
                if at < stall_end {
                    acked_in_stall += 1;
                    ack_most_ns = ack_most_ns.max(at - offered_ns);
                }
            }
            None => unanswered += 1,
        }
    }
    let handed_over = world.replica(leader.max(1)).handed_over();
    let first_order_ns = world
        .timeouts
        .iter()
        .find(|(at, by, _)| *at >= from && *by == leader)
        .map(|(at, ..)| at - from);
    let others_ordered = world
        .timeouts
        .iter()
        .filter(|(at, by, _)| *at >= from && *by != leader)
        .count();
    let refusals: Vec<u64> = world
        .refused_for_room
        .iter()
        .filter(|(at, _)| *at >= from)
        .map(|(at, _)| at - from)
        .collect();
    Stalled {
        seed,
        leader,
        term,
        ends_ns,
        terms: last_term - term,
        lost,
        suspicions,
        leaderless_ns,
        latencies,
        unapplied,
        freshness_ns,
        in_stall,
        acked_in_stall,
        ack_most_ns,
        unanswered,
        answer_most_ns,
        off_majority: load.off_majority,
        handed_over,
        first_order_ns,
        others_ordered,
        refused_for_room: refusals.len(),
        first_refusal_ns: refusals.first().copied(),
        fill_ns,
    }
}

/// How a stall's world is made: each node's stream and replica on one log a frame at a time
/// (`one_log`, Focal's member), each node judging its peers' processes apart from their disks
/// (`disk_apart`, CP-T2.2 step 2), and its leaders applying ahead of their own durability
/// (`apply_ahead`).
#[derive(Clone, Copy, Default)]
struct How {
    one_log: bool,
    disk_apart: bool,
    /// The leader applies its own entries before its write of them is durable
    /// (`Config::apply_unpersisted`).
    apply_ahead: bool,
    /// The group's disk mode (`Settings::disk`): in `Disk::Apart` each leader's patience with its
    /// own disk is the law's, its budget's room filling (`Replica::disk_fill`).
    disk: Disk,
    /// Each replica's budget, bytes (`hyper_durable::Bytes`); none, one that refuses nothing the
    /// world asks of it.
    budget: Option<u64>,
}

impl How {
    /// Each replica's budget, bytes.
    fn budget(&self) -> u64 {
        self.budget.unwrap_or(u64::MAX)
    }
}

/// CP-T2.2 step 2's group: each node judging its peers' processes apart from their disks, its
/// leader applying on its followers' disks while its own lags, and handing the lead on by the
/// patience's law, at a budget of 64 MiB that the world's load (a publish a millisecond) fills in
/// minutes, past the 2 s stall the step's bar is.
const APART: How = How {
    one_log: false,
    disk_apart: true,
    apply_ahead: true,
    disk: Disk::Apart,
    budget: Some(64 << 20),
};

/// A budget the world's load, a publish a millisecond, fills in about a second: 128 KiB, the
/// group's own 15 KiB and a backlog of about a thousand entries, the core's slots doubling past the
/// rest (`survey_of_the_patience`: the fill read at the stop 868 to 998 ms, the hand-over ordered
/// 1,006 to 1,009 ms in, no proposal refused for room, 4 seeds).
const SECOND_BUDGET: u64 = 128 << 10;

/// A budget a publish every 100 ms fills in about ten seconds, the backlog's slots doubling past
/// what the group's own leaves of it.
const LIGHT_BUDGET: u64 = 24 << 10;

/// A budget for a minute of stalls in turn: 1 MiB, about eight seconds of the world's load, the
/// log uncompacted holding a minute of applied entries in it too (each entry's size counted, four
/// bytes a slot, `Replica`'s `sizes`): at 128 KiB they alone filled every room by 15 s.
const MINUTE_BUDGET: u64 = 1 << 20;

/// The slowest answer a publish may take through a stall the group rides out: about twice the
/// slowest the survey shows of a publish through one (5.3 ms, the leader judged apart).
const ANSWER_MOST_NS: u64 = 10 * MS;

/// What each shape of stall does to the group, a line a seed: doc 34's (every node's flush 584 to
/// 587 ms in the same second), the same where each node's stream and replica share one log a frame
/// at a time (Focal's member), a journal's (each node's flushes held to a later commit), the
/// leader's alone, and the leader's for 2 s (CP-T2.2 step 2's example). Run with `--ignored
/// --nocapture`; HYPER_DURABLE_STALL_SEEDS seeds from HYPER_DURABLE_LIVENESS_SEED, and
/// HYPER_DURABLE_STALL_TRACE names a seed whose timeline each shape prints.
#[test]
#[ignore = "a survey of stalls for CP-T2.2's trace, not a check"]
fn survey_of_stalls() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    type Shape = (&'static str, How, fn(u64) -> [u64; 3]);
    let own_logs = How::default();
    let one_log = How {
        one_log: true,
        ..How::default()
    };
    let apart = How {
        disk_apart: true,
        ..How::default()
    };
    let apart_ahead = How {
        disk_apart: true,
        apply_ahead: true,
        ..How::default()
    };
    let shapes: [Shape; 7] = [
        ("together, 584 to 587 ms (doc 34)", own_logs, together),
        (
            "together, one log a frame at a time (Focal's member)",
            one_log,
            together,
        ),
        ("a journal's: 585, 735 and 885 ms", own_logs, |_| {
            [585 * MS, 735 * MS, 885 * MS]
        }),
        ("the leader's alone, 585 ms", own_logs, |leader| {
            let mut ends = [0; 3];
            ends[(leader.max(1) - 1) as usize] = 585 * MS;
            ends
        }),
        ("the leader's alone, 2 s", own_logs, leaders_two_seconds),
        (
            "the leader's alone, 2 s, judged apart from its disk (CP-T2.2 step 2)",
            apart,
            leaders_two_seconds,
        ),
        (
            "the leader's alone, 2 s, judged apart, applying ahead of its own durability",
            apart_ahead,
            leaders_two_seconds,
        ),
    ];
    for (name, how, ends) in shapes {
        println!("== {name}");
        let mut lost = 0;
        for seed in first..first + seeds {
            let outcome = stalled(seed, how, ends);
            lost += u64::from(outcome.lost || outcome.terms > 0);
            println!("{}", outcome.line());
        }
        println!("-- {name}: the lead lost or a term spent in {lost} of {seeds} seeds");
    }
}

/// The stall of doc 34: every node's flush 584 to 587 ms in the same second.
fn together(_: u64) -> [u64; 3] {
    [584 * MS, 585 * MS, 587 * MS]
}

/// The leader's device alone, for 2 s (CP-T2.2 step 2's example).
fn leaders_two_seconds(leader: u64) -> [u64; 3] {
    let mut ends = [0; 3];
    ends[(leader.max(1) - 1) as usize] = 2_000 * MS;
    ends
}

/// CP-T2.2 step 1: a stall of every node's flush at once, as on the one disk of Focal's doc 34
/// (each node's log flush 584 to 587 ms in the same second), spends no term under a publish a
/// millisecond, where each node's stream and replica write logs of their own and where they share
/// one a frame at a time: no judge blames a peer for a stall its own flush proof shares
/// (`docs/timing.md` §2.8), so the leader before it leads after it, in the same term. At b5ea01ba,
/// doc 34's run, before that rule (7959df9, 1ccd38d and 2b13b15), it failed in every seed: each
/// judge suspected both peers within its freshness, and the leader, left no trusted quorum, stepped
/// down (seed 0: the suspicions 63 to 81 ms into the stall, the step-down at 76 ms, no leader for
/// 516 ms).
#[test]
fn a_flush_stall_every_node_shares_spends_no_term() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    for one_log in [false, true] {
        for seed in first..first + seeds {
            let how = How {
                one_log,
                ..How::default()
            };
            let outcome = stalled(seed, how, together);
            assert!(
                !outcome.lost && outcome.terms == 0,
                "{}{}",
                if one_log {
                    "one log, a frame at a time: "
                } else {
                    ""
                },
                outcome.line()
            );
        }
    }
}

/// CP-T2.2 step 2's: flush stalls that end apart, as a journal's later commits hold some nodes'
/// flushes past others' (585, 735 and 885 ms), spend no term. Fails in today's group, judged by the
/// flush proof: a judge whose own stall ends first holds a peer still stalled to its freshness from
/// that end, and a leader suspecting both followers steps down. The step's group, which judges each
/// process apart from its disk, passes it (`judged_apart_flush_stalls_that_end_apart_spend_no_term`).
#[test]
#[ignore = "today's group, judged by the flush proof, loses the lead; the step's group does not"]
fn flush_stalls_that_end_apart_spend_no_term() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    for seed in first..first + seeds {
        let outcome = stalled(seed, How::default(), |_| [585 * MS, 735 * MS, 885 * MS]);
        assert!(!outcome.lost && outcome.terms == 0, "{}", outcome.line());
    }
}

/// CP-T2.2 step 2's: the leader's flush stalled alone, 585 ms and 2 s, spends no term, its process
/// alive and its followers' disks a majority. Fails in today's group, judged by the flush proof: its
/// heartbeats wait on its flush proof, so both followers suspect it within their freshness and elect
/// another. The step's group passes it
/// (`judged_apart_a_2_s_stall_of_the_leaders_disk_spends_no_term_and_answers_every_publish`).
#[test]
#[ignore = "today's group, judged by the flush proof, loses the lead; the step's group does not"]
fn a_stall_of_the_leaders_flush_alone_spends_no_term() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    for stall in [585 * MS, 2_000 * MS] {
        for seed in first..first + seeds {
            let outcome = stalled(seed, How::default(), |leader| {
                let mut ends = [0; 3];
                ends[(leader.max(1) - 1) as usize] = stall;
                ends
            });
            assert!(!outcome.lost && outcome.terms == 0, "{}", outcome.line());
        }
    }
}

/// CP-T2.2 step 2: the leader's disk alone stalls for 2 s, in the step's group (`APART`): no
/// election and no term spent, and every publish offered in the stall is answered by the leader,
/// within `ANSWER_MOST_NS`, each on a majority's disks as it is answered. Today's group, judged by
/// the flush proof, spends a term in every seed (`a_stall_of_the_leaders_flush_alone_...`).
#[test]
fn judged_apart_a_2_s_stall_of_the_leaders_disk_spends_no_term_and_answers_every_publish() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    for seed in first..first + seeds {
        let outcome = stalled(seed, APART, leaders_two_seconds);
        assert!(
            !outcome.lost
                && outcome.terms == 0
                && outcome.unanswered == 0
                && outcome.answer_most_ns <= ANSWER_MOST_NS
                && outcome.off_majority == 0
                && outcome.handed_over == 0,
            "{}",
            outcome.line()
        );
    }
}

/// CP-T2.2 step 2, step 1's stall test in the step's group: every node's flush stalled at once,
/// as on doc 34's one disk, spends no term, and every publish offered in it is answered, each on a
/// majority's disks as it is answered (once two disks answer, 585 ms in).
#[test]
fn judged_apart_a_flush_stall_every_node_shares_spends_no_term() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    for seed in first..first + seeds {
        let outcome = stalled(seed, APART, together);
        assert!(
            !outcome.lost
                && outcome.terms == 0
                && outcome.unanswered == 0
                && outcome.off_majority == 0,
            "{}",
            outcome.line()
        );
    }
}

/// CP-T2.2 step 2, step 1's stalls that end apart in the step's group: every node's flush stalled
/// at once and let go 585, 735 and 885 ms in, as a journal's later commits hold some nodes' flushes
/// past others', spends no term, and every publish offered in it is answered, each on a majority's
/// disks as it is answered (from 735 ms, when a second disk answers).
#[test]
fn judged_apart_flush_stalls_that_end_apart_spend_no_term() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    for seed in first..first + seeds {
        let outcome = stalled(seed, APART, |_| [585 * MS, 735 * MS, 885 * MS]);
        assert!(
            !outcome.lost
                && outcome.terms == 0
                && outcome.unanswered == 0
                && outcome.off_majority == 0,
            "{}",
            outcome.line()
        );
    }
}

/// CP-T2.2 step 2: a 2 s stall of each node's disk in turn, the leader's among them, 500 ms apart,
/// in the step's group: no term spent, and every publish offered from the first stall to the last
/// one's end answered by the leader within `ANSWER_MOST_NS`, each on a majority's disks as it is
/// answered.
#[test]
fn judged_apart_a_stall_of_each_node_in_turn_spends_no_term() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    for seed in first..first + seeds {
        let mut world = World::with(seed, APART);
        world.run_until(|w| w.leader().is_some() && (1..=3).all(|peer| w.judged(peer)));
        let mut load = Load::default();
        let settled = world.now + 2_000 * MS;
        world.run_loaded(settled, MS, &mut load);
        let leader = world.leader().unwrap();
        let term = world.replica(leader).term();
        load.watch = leader;
        let from = world.now;
        for id in 1..=3u64 {
            let end = world.now + 2_000 * MS;
            world.stall(id, end);
            world.run_loaded(end + 500 * MS, MS, &mut load);
        }
        let until = world.now;
        let drained = until + world.quiet();
        world.run_loaded(drained, MS, &mut load);
        let terms = (1..=3u64).map(|id| world.replica(id).term()).max().unwrap() - term;
        let (mut unanswered, mut slowest) = (0, 0);
        for (offered, answered) in load.offered.iter().zip(&load.by_leader) {
            if *offered < from || *offered >= until {
                continue;
            }
            match answered {
                Some(at) => slowest = slowest.max(at - offered),
                None => unanswered += 1,
            }
        }
        assert!(
            world.leader() == Some(leader)
                && terms == 0
                && unanswered == 0
                && slowest <= ANSWER_MOST_NS
                && load.off_majority == 0,
            "seed {seed}: leader {leader} then {:?}, {terms} terms spent; {unanswered} publishes \
             never answered, the slowest {:.1} ms, {} answered off a majority's disks",
            world.leader(),
            slowest as f64 / 1e6,
            load.off_majority
        );
    }
}

/// CP-T2.2 step 2, Focal's condition 4: a member whose disk is stalled gives no vote its disk doesn't
/// hold, in the step's group, and the group elects only once it can. A follower's disk stalls for
/// 2 s and the leader is killed 100 ms in: the other follower campaigns and needs the stalled
/// member's vote, which waits on its disk, so no member leads until the stall ends, and one leads
/// once it has. No message any member sends carries a term or vote its disk doesn't hold
/// (`breaks_promise`), and a vote answered from memory is caught: the check refuses one planted.
#[test]
fn judged_apart_a_stalled_disk_gives_no_vote_until_it_is_durable() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    for seed in first..first + seeds {
        let mut world = World::with(seed, APART);
        world.run_until(|w| w.leader().is_some() && (1..=3).all(|peer| w.judged(peer)));
        let leader = world.leader().unwrap();
        let stalled = (1..=3u64).find(|id| *id != leader).unwrap();
        let other = 6 - leader - stalled;
        let end = world.now + 2_000 * MS;
        world.stall(stalled, end);
        let killed = world.now + 100 * MS;
        while world.now < killed {
            world.next_by(killed);
        }
        world.nodes[(leader - 1) as usize].alive = false;
        let term = world.replica(other).term();
        while world.now < end {
            world.next_by(end);
            assert!(
                world.leader().is_none(),
                "seed {seed}: n{} led at {} ms into the stall, before n{stalled}'s disk answered",
                world.leader().unwrap_or(0),
                (world.now + 2_000 * MS - end) / MS
            );
        }
        world.run_until(|w| w.leader().is_some());
        let early: Vec<_> = world
            .votes
            .iter()
            .filter(|(at, voter, _, t)| *voter == stalled && *at < end && *t > term)
            .collect();
        assert!(
            world.broken.is_empty() && early.is_empty(),
            "seed {seed}: messages past their senders' disks {:?}; n{stalled}'s votes before its \
             disk answered {early:?}",
            world.broken
        );
        assert!(
            world.replica(world.leader().unwrap()).term() > term,
            "seed {seed}: no new term led after the stall"
        );
    }
    // Planted: a vote answered from memory, its term or its vote not on the disk, is caught.
    let vote = Message {
        msg_type: MessageType::MsgRequestVoteResponse,
        to: 1,
        from: 2,
        term: 3,
        ..Message::default()
    };
    assert!(
        breaks_promise((2, 0), &vote, 2),
        "a vote of a term not durable"
    );
    assert!(breaks_promise((3, 0), &vote, 2), "a vote not durable");
    assert!(!breaks_promise((3, 1), &vote, 2), "a vote durable");
}

/// CP-T2.2 step 2, Focal's condition 4: liveness comes from a hand-over, not from a stalled disk's
/// vote. A follower's disk stalls for 2 s and the leader hands the lead to the other follower
/// (`Replica::transfer`): the heir leads on the old leader's vote while the stall lasts, and the
/// stalled member gives no vote its disk doesn't hold.
#[test]
fn judged_apart_a_hand_over_needs_no_vote_from_a_stalled_disk() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    for seed in first..first + seeds {
        let mut world = World::with(seed, APART);
        world.run_until(|w| w.leader().is_some() && (1..=3).all(|peer| w.judged(peer)));
        let leader = world.leader().unwrap();
        let stalled = (1..=3u64).find(|id| *id != leader).unwrap();
        let heir = 6 - leader - stalled;
        let term = world.replica(leader).term();
        let end = world.now + 2_000 * MS;
        world.stall(stalled, end);
        {
            let node = &mut world.nodes[(leader - 1) as usize];
            node.owner
                .get_mut(node.handle)
                .unwrap()
                .transfer(heir)
                .unwrap();
            node.owner.schedule(node.handle);
            node.alarm = Some(world.now);
        }
        while world.now < end && world.leader() != Some(heir) {
            world.next_by(end);
        }
        let led = world.now;
        assert!(
            world.leader() == Some(heir) && world.replica(heir).term() > term && led < end,
            "seed {seed}: n{heir} did not lead before n{stalled}'s disk answered: leader {:?}",
            world.leader()
        );
        let after = end + world.quiet();
        while world.now < after {
            world.next_by(after);
        }
        let early: Vec<_> = world
            .votes
            .iter()
            .filter(|(at, voter, _, t)| *voter == stalled && *at < end && *t > term)
            .collect();
        assert!(
            world.broken.is_empty() && early.is_empty(),
            "seed {seed}: messages past their senders' disks {:?}; n{stalled}'s votes before its \
             disk answered {early:?}",
            world.broken
        );
    }
}

/// CP-T2.2 step 2, Focal's condition 5: a leader whose disk stops hands the lead only to a voter
/// whose disk isn't slow. The leader's disk stops for good and one follower's is held for 4 s, under
/// a publish a millisecond that fills the leader's room in about a second (`SECOND_BUDGET`): the
/// follower's own heartbeats say its disk is slow (`Change::DiskSlow`) before the patience ends,
/// as it waits on a write, every hand-over the leader begins names
/// the other follower, and that one leads once a second disk answers its campaign.
#[test]
fn judged_apart_a_hand_over_names_no_heir_whose_disk_is_slow() {
    let how = How {
        budget: Some(SECOND_BUDGET),
        ..APART
    };
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    for seed in first..first + seeds {
        let mut world = World::with(seed, how);
        world.run_until(|w| w.leader().is_some() && (1..=3).all(|peer| w.judged(peer)));
        let mut load = Load::default();
        let settled = world.now + 2_000 * MS;
        world.run_loaded(settled, MS, &mut load);
        let leader = world.leader().unwrap();
        let slow = (1..=3u64).find(|id| *id != leader).unwrap();
        let heir = 6 - leader - slow;
        let from = world.now;
        world.stall(slow, from + 4_000 * MS);
        world.stall(leader, from + 60_000 * MS);
        world.run_loaded(from + 6_000 * MS, MS, &mut load);
        let named: Vec<(u64, u64)> = world
            .timeouts
            .iter()
            .filter(|(at, by, _)| *at >= from && *by == leader)
            .map(|(at, _, to)| ((at - from) / MS, *to))
            .collect();
        let judged_slow = world.disks.iter().any(|(at, judge, peer, is_slow)| {
            *judge == leader && *peer == slow && *is_slow && *at < from + 1_000 * MS
        });
        assert!(
            judged_slow
                && !named.is_empty()
                && named.iter().all(|(_, to)| *to == heir)
                && world.leader() == Some(heir),
            "seed {seed}: n{leader}'s disk stopped and n{slow}'s held; judged slow in time \
             {judged_slow}; hand-overs named (ms from the stop, heir) {named:?}; leader then {:?}",
            world.leader()
        );
    }
}

/// CP-T2.2 step 2, Focal's condition 5: with every other voter's disk slow, no hand-over begins
/// and no term is spent. The leader's disk stops for good and both followers' are held for 4 s,
/// under a publish a millisecond that fills the leader's room in about a second (`SECOND_BUDGET`):
/// while both are held the leader names no
/// heir (its patience finds none, `Replica::heirless`) and no member's term moves; once they
/// answer, it hands the lead to one of them.
#[test]
fn judged_apart_no_hand_over_while_every_other_disk_is_slow() {
    let how = How {
        budget: Some(SECOND_BUDGET),
        ..APART
    };
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    for seed in first..first + seeds {
        let mut world = World::with(seed, how);
        world.run_until(|w| w.leader().is_some() && (1..=3).all(|peer| w.judged(peer)));
        let mut load = Load::default();
        let settled = world.now + 2_000 * MS;
        world.run_loaded(settled, MS, &mut load);
        let leader = world.leader().unwrap();
        let term = world.replica(leader).term();
        let from = world.now;
        let end = from + 4_000 * MS;
        for id in (1..=3u64).filter(|id| *id != leader) {
            world.stall(id, end);
        }
        world.stall(leader, from + 60_000 * MS);
        world.run_loaded(end - 100 * MS, MS, &mut load);
        let named = |w: &World| {
            w.timeouts
                .iter()
                .filter(|(at, by, _)| *at >= from && *by == leader)
                .count()
        };
        let terms = (1..=3u64).map(|id| world.replica(id).term()).max().unwrap() - term;
        let (during, heirless) = (named(&world), world.replica(leader).heirless());
        world.run_loaded(end + 6_000 * MS, MS, &mut load);
        assert!(
            during == 0 && terms == 0 && heirless > 0 && named(&world) > 0,
            "seed {seed}: while every other disk was held, {during} hand-overs and {terms} terms, \
             the patience heirless {heirless} times; after, {} hand-overs",
            named(&world)
        );
    }
}

/// Focal's condition 6 on the disk mode: with every member's disk slow, no term is spent. Every
/// member's disk stalls from one instant to a second past the leader's patience in the step's group,
/// which the law ends as the leader's room fills (`SECOND_BUDGET`; `LIGHT_BUDGET` for the light
/// load), under a publish a millisecond and under one every 100 ms, as on one disk the members
/// share (Focal's doc 34): let go together, at doc 34's spread (1 and 3 ms apart, the leader's first
/// and last) and at a journal's (150 and 300 ms apart, the leader's first). The leader's patience ends with both
/// followers' disks judged slow, so it names no heir (`Replica::heirless`) and begins no hand-over,
/// and no member's term moves; no message leaves past its sender's disk, no publish offered in the
/// stall is answered before a majority's disks hold it, its second disk let go, and every one is
/// answered after, the backlog at the group's pace (each quiet period answering some of it). A
/// leader whose disk lets go a heartbeat or more after its followers' is condition 5's: its disk
/// alone slow past its patience, it may begin a hand-over to a follower heard caught up.
#[test]
fn judged_apart_every_disk_slow_past_the_patience_spends_no_term() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    // Each shape's ends past the stall's own, the leader's and then its followers', and its load: a
    // publish a millisecond, or one every 100 ms, whose backlog an heir catches up at once.
    let shapes: [(&str, [u64; 3], u64); 6] = [
        ("together", [0, 0, 0], MS),
        ("doc 34's spread, the leader's first", [0, MS, 3 * MS], MS),
        ("doc 34's spread, the leader's last", [3 * MS, 0, MS], MS),
        (
            "a journal's, the leader's first",
            [0, 150 * MS, 300 * MS],
            MS,
        ),
        ("together, lightly loaded", [0, 0, 0], 100 * MS),
        (
            "doc 34's spread, the leader's last, lightly loaded",
            [3 * MS, 0, MS],
            100 * MS,
        ),
    ];
    let mut failed = Vec::new();
    for (shape, spread, every_ns) in shapes {
        // Each load's budget, filled in about a second, or ten under the light load.
        let how = How {
            budget: Some(if every_ns > MS {
                LIGHT_BUDGET
            } else {
                SECOND_BUDGET
            }),
            ..APART
        };
        for seed in first..first + seeds {
            let mut world = World::with(seed, how);
            world.run_until(|w| w.leader().is_some() && (1..=3).all(|peer| w.judged(peer)));
            let mut load = Load::default();
            let settled = world.now + 2_000 * MS;
            world.run_loaded(settled, every_ns, &mut load);
            let leader = world.leader().unwrap();
            let term = world.replica(leader).term();
            let (heirless_before, handed_before) = (
                world.replica(leader).heirless(),
                world.replica(leader).handed_over(),
            );
            load.watch = leader;
            let from = world.now;
            // Every disk stalls until a second past the leader's patience, which the law ends as
            // its room fills: held with no end until it has, then let go at the shape's spread.
            let members = [leader, leader % 3 + 1, (leader + 1) % 3 + 1];
            for member in members {
                world.stall(member, u64::MAX);
            }
            let cap = from + 600_000 * MS;
            while world.now < cap
                && world.replica(leader).heirless() == heirless_before
                && world.replica(leader).handed_over() == handed_before
            {
                let step = world.now + MS;
                world.run_loaded(step, every_ns, &mut load);
            }
            let patience = world.now - from;
            let end = world.now + 1_000 * MS;
            let mut ends = [0; 3];
            for (member, after) in members.iter().zip(spread) {
                ends[(member - 1) as usize] = end + after;
                world.release(*member, end + after);
            }
            let mut sorted = ends;
            sorted.sort_unstable();
            let (majority, last) = (sorted[1], sorted[2]);
            world.run_loaded(last, every_ns, &mut load);
            // The backlog the stall held, answered at the group's pace: the load runs on a quiet
            // period at a time while each answers some of it.
            let unanswered = |load: &Load| {
                load.offered
                    .iter()
                    .zip(&load.by_leader)
                    .filter(|(offered, answered)| {
                        (from..last).contains(*offered) && answered.is_none()
                    })
                    .count()
            };
            let mut left = unanswered(&load);
            while left > 0 {
                let next = world.now + world.quiet();
                world.run_loaded(next, every_ns, &mut load);
                let now_left = unanswered(&load);
                if now_left == left {
                    break;
                }
                left = now_left;
            }
            let drained = world.now + world.quiet();
            world.run_loaded(drained, every_ns, &mut load);
            let terms = (1..=3u64).map(|id| world.replica(id).term()).max().unwrap() - term;
            let lost = load
                .leads
                .iter()
                .any(|(at, lead, _)| *at >= from && *lead != leader);
            let timeouts: Vec<(u64, u64)> = world
                .timeouts
                .iter()
                .filter(|(at, ..)| *at >= from)
                .map(|(at, _, to)| ((at - from) / MS, *to))
                .collect();
            let handed_over = world.replica(leader).handed_over() - handed_before;
            let heirless = world.replica(leader).heirless() - heirless_before;
            let judged = members[1..].iter().all(|follower| {
                world.disks.iter().any(|(at, judge, peer, slow)| {
                    *judge == leader
                        && peer == follower
                        && *slow
                        && (from..from + patience).contains(at)
                })
            });
            let (mut early, mut caught_up) = (0, 0);
            for (offered, answered) in load.offered.iter().zip(&load.by_leader) {
                if let (true, Some(at)) = ((from..last).contains(offered), answered) {
                    early += usize::from(*at < majority);
                    caught_up = caught_up.max(at.saturating_sub(majority));
                }
            }
            let line = format!(
                "{shape}, seed {seed}: leader n{leader}, the disks let go {:?} ms in (the leader's, \
                 then its followers'); {terms} terms, lead {}, hand-overs begun {handed_over} \
                 (orders to campaign, ms and heir, {timeouts:?}), the patience heirless {heirless} \
                 times, both followers judged slow within it {judged}; messages past their senders' \
                 disks {}; publishes offered in the stall answered before a majority's disks held \
                 them {early}, never {left}, the last {:.0} ms after a majority's disks answered, \
                 answered off a majority's disks {}",
                members.map(|m| (ends[(m - 1) as usize] - from) / MS),
                if lost { "lost" } else { "kept" },
                world.broken.len(),
                caught_up as f64 / 1e6,
                load.off_majority
            );
            println!("{line}");
            if terms > 0
                || lost
                || handed_over > 0
                || !timeouts.is_empty()
                || heirless == 0
                || !judged
                || !world.broken.is_empty()
                || early > 0
                || left > 0
                || load.off_majority > 0
            {
                failed.push(line);
            }
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}

/// Focal's condition 5, the fill test (kiln CP-T2.2): a leader whose disk stops for good, under a
/// publish a millisecond that fills its budget's room in about a second (`SECOND_BUDGET`), hands
/// the lead on before any proposal is refused for room, by the patience's law, and the group goes on
/// with every publish applied. One hand-over: the lead moves once, to an heir whose disk answers,
/// which orders none; the core may give up a transfer and the leader order it again (the old test's
/// seeds 3 and 6), and the heir's campaign may take a second term (it skips pre-vote on the core's
/// word, its ballot's round able to end before a vote that waited on the voter's flush: seed 1,
/// 0.2 ms), so two orders and two terms are allowed and no more. No proposal at all is refused for
/// room, before the hand-over or after: the heir's room is its own.
#[test]
fn judged_apart_a_leaders_disk_that_stops_hands_over_before_its_room_fills() {
    let how = How {
        budget: Some(SECOND_BUDGET),
        ..APART
    };
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    let outcomes: Vec<Stalled> = (first..first + seeds)
        .map(|seed| {
            stalled(seed, how, |leader| {
                let mut ends = [0; 3];
                ends[(leader.max(1) - 1) as usize] = 6_000 * MS;
                ends
            })
        })
        .collect();
    for outcome in &outcomes {
        println!("{}", outcome.line());
    }
    for outcome in &outcomes {
        assert!(
            outcome.lost
                && (1..=2).contains(&outcome.terms)
                && (1..=2).contains(&outcome.handed_over)
                && outcome.first_order_ns.is_some()
                && outcome.others_ordered == 0
                && outcome.refused_for_room == 0
                && outcome.unapplied == 0
                && outcome.off_majority == 0,
            "{}",
            outcome.line()
        );
    }
}

/// Focal's condition 5, the alternating stalls (kiln CP-T2.2): at most one forced hand-over a fill
/// time, less a hand-over, however the group's disks stall in turn; no doubling is needed for it.
/// For 60 s under a publish a millisecond (`MINUTE_BUDGET`), each leader's disk stalls as it takes
/// the lead, the one before answering again, the worst turn there is: its backlog fills its room
/// from the moment it leads. The next hand-over its patience orders comes no sooner than the fill
/// it read as it took the lead, less the hand-over's time it read then, since its own backlog has to
/// fill its room first, its disk not slow when it was named heir. Every publish is applied, none is
/// answered off a majority's disks, no proposal is refused for room, and no term is spent but by a
/// hand-over (two at most each, as in the fill test).
#[test]
fn judged_apart_disks_stalling_in_turn_hand_over_at_most_once_a_fill_time() {
    let how = How {
        budget: Some(MINUTE_BUDGET),
        ..APART
    };
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    let mut failed = Vec::new();
    for seed in first..first + seeds {
        let mut world = World::with(seed, how);
        world.run_until(|w| w.leader().is_some() && (1..=3).all(|peer| w.judged(peer)));
        let mut load = Load::default();
        let settled = world.now + 2_000 * MS;
        world.run_loaded(settled, MS, &mut load);
        let term = (1..=3u64).map(|id| world.replica(id).term()).max().unwrap();
        let from = world.now;
        let until = from + 60_000 * MS;
        // Each leader as it took the lead: when, who, and the fill and hand-over it read then.
        let mut leads: Vec<(u64, u64, Option<Duration>, Option<Duration>)> = Vec::new();
        let mut held: Option<u64> = None;
        while world.now < until {
            if let Some(leader) = world.leader()
                && held != Some(leader)
            {
                if let Some(before) = held {
                    world.release(before, world.now);
                }
                world.stall(leader, until);
                held = Some(leader);
                let replica = world.replica(leader);
                leads.push((
                    world.now,
                    leader,
                    replica.disk_fill(),
                    replica.hand_over_time(),
                ));
            }
            let step = world.now + MS;
            world.run_loaded(step, MS, &mut load);
        }
        if let Some(last) = held {
            world.release(last, world.now);
        }
        // Every publish offered in the minute, given a quiet period to be applied: the load goes on
        // through it.
        let offered = load.offered.len();
        let drained = world.now + world.quiet();
        world.run_loaded(drained, MS, &mut load);
        // Each leadership's first order, from its start.
        let mut short = Vec::new();
        let mut orders = 0u64;
        for (index, (began, leader, fill, hand_over)) in leads.iter().enumerate() {
            let ended = leads.get(index + 1).map_or(until, |next| next.0);
            let Some(ordered) = world
                .timeouts
                .iter()
                .find(|(at, by, _)| (*began..ended).contains(at) && by == leader)
                .map(|(at, ..)| at - began)
            else {
                continue;
            };
            orders += 1;
            let least = fill.zip(*hand_over).map_or(0, |(fill, hand_over)| {
                fill.saturating_sub(hand_over).as_nanos() as u64
            });
            if ordered < least {
                short.push((*leader, ordered / MS, least / MS));
            }
        }
        let terms = (1..=3u64).map(|id| world.replica(id).term()).max().unwrap() - term;
        let refused = world
            .refused_for_room
            .iter()
            .filter(|(at, _)| *at >= from)
            .count();
        let unapplied = load.applied[..offered]
            .iter()
            .filter(|applied| applied.is_none())
            .count();
        let line = format!(
            "seed {seed}: {} leaderships in 60 s, {orders} hand-overs ordered, {terms} terms; \
             each leadership (ms from the stalls' start, leader, fill and hand-over read as it \
             led, ms) {:?}; ordered sooner than the fill less a hand-over (leader, ms, least ms) \
             {short:?}; proposals refused for room {refused}, publishes never applied \
             {unapplied}, answered off a majority's disks {}",
            leads.len(),
            leads
                .iter()
                .map(|(at, leader, fill, hand_over)| (
                    (at - from) / MS,
                    *leader,
                    fill.map(|f| f.as_millis()),
                    hand_over.map(|h| h.as_millis())
                ))
                .collect::<Vec<_>>(),
            load.off_majority
        );
        println!("{line}");
        if !short.is_empty()
            || orders < 2
            || terms > 2 * orders
            || refused > 0
            || unapplied > 0
            || load.off_majority > 0
        {
            failed.push(line);
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}

/// CP-T2.2 step 2's bar: a 2 s stall of the leader's disk alone over 10,000 seeds in the step's
/// group starts no election, by the simulator's term count, and every publish offered in it is
/// answered. Run with `--ignored --nocapture`; HYPER_DURABLE_STALL_SEEDS seeds (10,000 by default)
/// from HYPER_DURABLE_LIVENESS_SEED.
#[test]
#[ignore = "CP-T2.2 step 2's 10,000 seeds, minutes long"]
fn judged_apart_ten_thousand_stalls_of_the_leaders_disk_start_no_election() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 10_000);
    let (mut elections, mut unanswered, mut off_majority, mut slowest) = (0, 0, 0, 0);
    for seed in first..first + seeds {
        let outcome = stalled(seed, APART, leaders_two_seconds);
        elections += outcome.terms;
        unanswered += outcome.unanswered;
        off_majority += outcome.off_majority;
        slowest = slowest.max(outcome.answer_most_ns);
        if outcome.terms > 0 || outcome.lost || outcome.unanswered > 0 || outcome.off_majority > 0 {
            println!("{}", outcome.line());
        }
    }
    println!(
        "seeds {first}..{}: {elections} elections, {unanswered} publishes unanswered, \
         {off_majority} answered off a majority's disks, the slowest answer {:.1} ms",
        first + seeds,
        slowest as f64 / 1e6
    );
    assert!(elections == 0 && unanswered == 0 && off_majority == 0);
}

/// Focal's condition 1's backstop (kiln CP-T2.2): a dead leader of a group whose members' disk
/// modes differ, as in a rolling restart's window, is replaced. Each node judges a peer of the
/// other mode apart from its disk, by its heartbeats' arrivals and round trips alone, so a process
/// of the other mode that dies is suspected as any is. Two shapes, 8 seeds each: the leader alone
/// in the disk mode, its followers in today's; and one follower alone in it, judging every peer by
/// the other mode's heartbeats from its own measure of them. In each, every pair comes to judge its
/// peer again with no suspicion and no term spent in two seconds of the mixed group; then the
/// leader's node dies, and a new leader is elected no later than the protocol gives: the same
/// seed's failover in one mode, plus the detection deadline the surviving judges report for the
/// dead leader at the kill (each pair's freshness, the interval its heartbeats come at and the
/// margin in force). A peer judged by its arrivals alone is suspected within that deadline, and
/// the election after is the law's, as in one mode.
#[test]
fn a_dead_leader_in_a_group_of_two_disk_modes_is_replaced() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 8);
    // The time from the leader's kill to a new leader, ms; the detection deadline the surviving
    // judges report for the dead leader at the kill, ms; and the suspicions and terms the mixed
    // group cost before the kill, the nodes `apart` names judging apart from their disks.
    // The nodes judging apart from their disks, of the leader's id.
    type Apart = fn(u64) -> Vec<u64>;
    let failover = |seed: u64, apart: Apart| -> (u64, u64, usize, u64) {
        let mut world = World::with(seed, How::default());
        world.run_until(|w| w.leader().is_some() && (1..=3).all(|peer| w.judged(peer)));
        let leader = world.leader().unwrap();
        for id in apart(leader) {
            world.nodes[(id - 1) as usize].liveness.set_disk_apart(true);
        }
        let max_term = |world: &World| (1..=3u64).map(|id| world.replica(id).term()).max().unwrap();
        let term = max_term(&world);
        let suspected = world.suspicions.len();
        world.run_until(|w| (1..=3).all(|peer| w.judged(peer)));
        let settled = world.now + 2_000 * MS;
        world.run_until(|w| w.now >= settled);
        let mixed = (world.suspicions.len() - suspected, max_term(&world) - term);
        let leader = world.leader().unwrap();
        let deadline = (1..=3u64)
            .filter(|id| *id != leader)
            .filter_map(|id| {
                world.nodes[(id - 1) as usize]
                    .liveness
                    .report(leader)?
                    .freshness
            })
            .max()
            .map_or(0, |d| d.as_millis() as u64);
        let killed_at = world.now;
        world.nodes[(leader - 1) as usize].alive = false;
        world.run_until(|w| w.leader().is_some_and(|l| l != leader));
        ((world.now - killed_at) / MS, deadline, mixed.0, mixed.1)
    };
    let one_mode: Vec<u64> = (first..first + seeds)
        .map(|seed| failover(seed, |_| Vec::new()).0)
        .collect();
    let shapes: [(&str, Apart); 2] = [
        ("the leader alone in the disk mode", |leader| vec![leader]),
        ("a follower alone in the disk mode", |leader| {
            vec![leader % 3 + 1]
        }),
    ];
    let mut failed = Vec::new();
    for (shape, apart) in shapes {
        for seed in first..first + seeds {
            let (ms, deadline, suspicions, terms) = failover(seed, apart);
            let one = one_mode[(seed - first) as usize];
            let bound = one + deadline;
            let line = format!(
                "{shape}, seed {seed}: a new leader {ms} ms after the kill (the bound {bound} ms: \
                 {one} ms in one mode, this seed, and the judges' deadline {deadline} ms); in the \
                 mixed group before it, {suspicions} suspicions and {terms} terms"
            );
            println!("{line}");
            if ms > bound || suspicions > 0 || terms > 0 {
                failed.push(line);
            }
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}

/// Focal's 5b on the disk mode (kiln CP-T2.2): a replica keeps a disk's verdict only for a member
/// its configuration names, and lets it go as the member leaves; bounded by the configuration, not by
/// the liveness peers. A verdict for a member no configuration names is kept by none; one for a
/// follower is kept, and gone once the follower's removal is applied, at every member.
#[test]
fn a_disk_verdict_is_kept_for_the_configurations_members_alone() {
    let mut world = World::with(0, APART);
    world.run_until(|w| w.leader().is_some() && (1..=3).all(|peer| w.judged(peer)));
    let leader = world.leader().unwrap();
    let removed = if leader == 3 { 2 } else { 3 };
    for id in 1..=3u64 {
        let node = &mut world.nodes[(id - 1) as usize];
        let replica = node.owner.get_mut(node.handle).unwrap();
        replica.disk_slow(9).unwrap();
        assert!(
            !replica.disk_is_slow(9),
            "n{id}: a verdict for a member no configuration names"
        );
        if id != removed {
            replica.disk_slow(removed).unwrap();
            assert!(replica.disk_is_slow(removed), "n{id}: a member's verdict");
        }
    }
    assert!(world.change(ConfChangeType::RemoveNode, removed));
    // The two that stay apply the removal; the removed one need not hear of it.
    world.run_until(|w| {
        (1..=3u64)
            .filter(|id| *id != removed)
            .all(|id| !w.replica(id).configuration().voters.contains(&removed))
    });
    for id in (1..=3u64).filter(|id| *id != removed) {
        assert!(
            !world.replica(id).disk_is_slow(removed),
            "n{id} kept the verdict of a member that left"
        );
    }
}

/// What the patience's law reads in the step's group, by budget: the leader's disk stops for good
/// under a publish a millisecond, after 2 s of the load, and the survey prints, a line a seed and
/// budget, what the leader held, the fill and hand-over times it read at the stop, when it ordered
/// its first hand-over and when a proposal was first refused for room, and the heir's fill once it
/// led. A survey, not a check: run with `--ignored --nocapture`.
#[test]
#[ignore = "a survey of the patience's law for CP-T2.2, not a check"]
fn survey_of_the_patience() {
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    let seeds = support::count("HYPER_DURABLE_STALL_SEEDS", 4);
    for budget in [64u64 << 10, 128 << 10, 256 << 10, 512 << 10] {
        for seed in first..first + seeds {
            let how = How {
                budget: Some(budget),
                ..APART
            };
            let mut world = World::with(seed, how);
            world.run_until(|w| w.leader().is_some() && (1..=3).all(|peer| w.judged(peer)));
            let mut load = Load::default();
            let settled = world.now + 2_000 * MS;
            world.run_loaded(settled, MS, &mut load);
            let leader = world.leader().unwrap();
            let from = world.now;
            world.stall(leader, from + 600_000 * MS);
            world.run_loaded(from + 20 * MS, MS, &mut load);
            let read = |w: &World, id: u64| {
                let r = w.replica(id);
                (
                    r.charged(),
                    r.disk_fill().map(|f| f.as_millis()),
                    r.hand_over_time().map(|h| h.as_micros()),
                )
            };
            let at_stop = read(&world, leader);
            world.run_loaded(from + 10_000 * MS, MS, &mut load);
            let ordered = world
                .timeouts
                .iter()
                .find(|(at, by, _)| *at >= from && *by == leader)
                .map(|(at, _, to)| ((at - from) / MS, *to));
            let refused = world
                .refused_for_room
                .iter()
                .find(|(at, _)| *at >= from)
                .map(|(at, by)| ((at - from) / MS, *by));
            let heir = world.leader().unwrap_or(0);
            println!(
                "budget {} KiB, seed {seed}: leader n{leader} at the stop (held, fill ms, hand-over \
                 us) {at_stop:?}; first hand-over (ms, heir) {ordered:?}; first refusal for room \
                 (ms, leader) {refused:?}; leader at 10 s n{heir} {:?}; hand-overs begun {}",
                budget >> 10,
                if heir > 0 {
                    Some(read(&world, heir))
                } else {
                    None
                },
                world.replica(leader).handed_over()
            );
        }
    }
}

/// Focal's condition 1: the group's disk mode is one setting. A replica opened with
/// `Settings::disk` set to `Disk::Apart` applies ahead of its own disk (`Config::apply_unpersisted`),
/// states the mode, which its owner sets the node's stream to (`Replica::disk`), and keeps the
/// patience's law, no time stated (condition 5: a hand-over's time before any is timed, zero before
/// its timing is measured); one opened with the default does none of it, and the core's own
/// `apply_unpersisted`, core step R-6 alone, is kept. A budget that refuses nothing gives no fill.
#[test]
fn the_disk_mode_is_one_setting() {
    let configuration = ConfState {
        voters: vec![1, 2, 3],
        ..ConfState::default()
    };
    let open = |disk: Disk, ahead: bool| -> Member {
        Replica::open(
            &member_settings(1, 0, ahead, disk),
            SimStore::new(DEPTH),
            Kv::new(configuration.clone(), false),
            Bytes::new(u64::MAX),
        )
        .unwrap()
    };
    let apart = open(Disk::Apart, false);
    assert!(apart.core().raft.config().apply_unpersisted);
    assert_eq!(apart.disk(), Disk::Apart);
    assert!(apart.disk().apart());
    assert_eq!(apart.hand_over_time(), Some(Duration::ZERO));
    assert_eq!(apart.disk_fill(), None, "nothing grows yet");
    let flushed = open(Disk::Flushed, false);
    assert!(!flushed.core().raft.config().apply_unpersisted);
    assert!(!flushed.disk().apart());
    assert_eq!(flushed.hand_over_time(), None);
    let unbounded: Replica<SimStore, Kv, Unbounded> = Replica::open(
        &member_settings(1, 0, false, Disk::Apart),
        SimStore::new(DEPTH),
        Kv::new(configuration.clone(), false),
        Unbounded,
    )
    .unwrap();
    assert_eq!(
        unbounded.disk_fill(),
        None,
        "an unbounded budget has no room to fill"
    );
    assert!(
        open(Disk::Flushed, true)
            .core()
            .raft
            .config()
            .apply_unpersisted
    );
}
