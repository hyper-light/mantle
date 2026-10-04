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

use std::collections::BTreeMap;
use std::task::Waker;
use std::time::Duration;

use hyper_durable::{Output, Owner, Replica, Unbounded};
use hyper_liveness::{
    Change, Liveness, Output as LiveOutput, Settings as LiveSettings, Write, is_liveness,
};
use hyper_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Message,
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

type Member = Replica<SimStore, Kv, Unbounded>;

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
    owner: Owner<SimStore, Kv, Unbounded>,
    handle: hyper_durable::Handle,
    liveness: Liveness,
    /// When the stream's liveness write, out, is durable.
    flushing: Option<(u64, u64)>,
    /// When the replica's writes out are durable.
    durable_at: Option<u64>,
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
    /// Restarts the streams reported: who saw whom start again.
    restarts: Vec<(u64, u64)>,
    /// Changes the streams reported, all told.
    changes: u64,
    seed: u64,
}

impl World {
    fn new(seed: u64) -> Self {
        let configuration = ConfState {
            voters: vec![1, 2, 3],
            ..ConfState::default()
        };
        let nodes = (1..=3)
            .map(|id| {
                let replica: Member = Replica::open(
                    &settings(id, seed),
                    SimStore::new(DEPTH),
                    Kv::new(configuration.clone(), false),
                    Unbounded,
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
                owner.pairs(handle, &mut liveness, false).unwrap();
                Node {
                    id,
                    owner,
                    handle,
                    liveness,
                    flushing: None,
                    durable_at: None,
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
            restarts: Vec::new(),
            changes: 0,
            seed,
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
            &settings(id, self.seed),
            SimStore::from_disk(disk, DEPTH),
            machine,
            Unbounded,
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
        owner.pairs(handle, &mut liveness, false).unwrap();
        *node = Node {
            id,
            owner,
            handle,
            liveness,
            flushing: None,
            durable_at: None,
            alarm: Some(self.now),
            alive: true,
            run,
        };
    }

    fn record(&mut self, at: u64, change: &Change) {
        self.changes += 1;
        match change {
            Change::Suspected(suspicion) => self.suspicions.push((at, *suspicion)),
            Change::Restarted { peer, .. } => self.restarts.push((at, *peer)),
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
        let (changes, me) = {
            let node = &mut self.nodes[at];
            if !node.alive {
                return;
            }
            if let Some((started, durable)) = node.flushing
                && durable <= now
            {
                node.flushing = None;
                node.liveness.on_durable(Write::Liveness, started, durable);
            }
            if node.durable_at.is_some_and(|due| due <= now) {
                node.durable_at = None;
                let replica = node.owner.get_mut(node.handle).unwrap();
                while replica.log_mut().make_durable() {}
                node.owner.schedule(node.handle);
            }
            node.liveness.poll(now, &mut live);
            if std::mem::take(&mut live.flush) && node.flushing.is_none() {
                node.flushing = Some((now, now + FLUSH_NS));
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
            let replica = node.owner.get_mut(node.handle).unwrap();
            if replica.log_mut().pending() > 0 && node.durable_at.is_none() {
                node.durable_at = Some(now + FLUSH_NS);
            }
            node.owner
                .pairs(node.handle, &mut node.liveness, false)
                .unwrap();
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
            (changes, me)
        };
        for change in &changes {
            self.record(me, change);
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
                        let _ = node
                            .liveness
                            .on_heartbeat(from, &bytes, self.now, &mut live);
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
        let ok = node
            .owner
            .get_mut(node.handle)
            .unwrap()
            .propose(Vec::new(), data.to_vec())
            .is_ok();
        node.owner.schedule(node.handle);
        node.alarm = Some(self.now);
        ok
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
    let mut idle = (0u64, 0u64);
    let first = support::count("HYPER_DURABLE_LIVENESS_SEED", 0);
    for seed in first..first + support::count("HYPER_DURABLE_LIVENESS_SEEDS", 64) {
        let mut world = World::new(seed);
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
