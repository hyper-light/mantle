//! Node pairs under a deterministic simulation: one clock, and a world as a host's heartbeat traces
//! measured it (`support/worlds.rs`): one-way delays, a disk per node and its flushes, owners whose
//! timers fire late, and hosts that freeze. The owners run the crate as a real one does (poll at
//! its wake, feed what arrives and what becomes durable, make the liveness write it asks for) and
//! compute the election cost from the library's own law over the round trips the streams measure;
//! the tests assert what the crate promises and derive no bound of their own.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cognitive_complexity,
    missing_docs
)]

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::time::Duration;

use hyper_liveness::{
    Change, Heartbeat, Liveness, MAX_BYTES, Output, PeerId, Refusal, Settings, Suspicion, Write,
};
use hyper_sim::Seeded;
use hyper_sim::rng::stream_seed;
use hyper_timing::{Ballot, Exposure, Trust, WINDOW_LIMIT};

const MS: u64 = 1_000_000;
const US: u64 = 1_000;

/// The seeds a test runs: its default count of them, or `HYPER_LIVENESS_SEEDS` where a soak sets
/// it, from the `HYPER_LIVENESS_SEED`-th on where that is set (a seed a failure printed is its low
/// 32 bits). Each test's are in a space of their own, its `tag` above the low 32 bits, so no two
/// tests run one seed whatever the counts.
fn seeds(tag: u64, default: u64) -> impl Iterator<Item = u64> {
    let first = from_environment("HYPER_LIVENESS_SEED", 0);
    let count = from_environment("HYPER_LIVENESS_SEEDS", default);
    (first..first.saturating_add(count)).map(move |index| (tag << 32) | index)
}

#[allow(
    clippy::disallowed_methods,
    reason = "a soak sets the seeds from the environment; the defaults are the gate's"
)]
fn from_environment(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// The world's draws: hyper-sim's SplitMix64 with its unbiased `below` (`hyper_sim::rng`), a
/// stream a source, named for it (`stream_seed`): each node's timer, disk, groups' writes and host,
/// each directed link, and a test's own. A source's draws depend on the seed and its name alone,
/// so a change that makes one node send or wake differently moves no other source's.
struct Draws {
    seed: u64,
    streams: BTreeMap<(&'static str, u64, u64), Seeded>,
}

impl Draws {
    fn new(seed: u64) -> Self {
        Self {
            seed,
            streams: BTreeMap::new(),
        }
    }

    fn stream(&mut self, label: &'static str, a: u64, b: u64) -> &mut Seeded {
        let seed = self.seed;
        self.streams
            .entry((label, a, b))
            .or_insert_with(|| Seeded::new(stream_seed(seed, label, &[a, b])))
    }

    /// Uniform in `[0, 1)`: 53 bits.
    fn unit(&mut self, label: &'static str, a: u64, b: u64) -> f64 {
        (self.stream(label, a, b).next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// A value of `table` at a probability drawn from the stream.
    fn draw(&mut self, label: &'static str, a: u64, b: u64, table: &Table) -> u64 {
        let u = self.unit(label, a, b);
        quantile(table, u)
    }
}

#[path = "support/worlds.rs"]
mod worlds;
use worlds::{Freezes, GRID, Table};

#[path = "support/record.rs"]
mod record;
use record::{Beat, Record, Traced};

/// A quantity's value at probability `u`, from its table by the inverse transform, linear between
/// the neighbouring points of the grid (`hyper-timing-trace`'s, `worlds::GRID`).
fn quantile(table: &Table, u: f64) -> u64 {
    let at = GRID
        .partition_point(|point| *point <= u)
        .saturating_sub(1)
        .min(GRID.len() - 2);
    let (low, high) = (GRID[at], GRID[at + 1]);
    let share = if high > low {
        (u - low) / (high - low)
    } else {
        0.0
    };
    let (from, to) = (table[at] as f64, table[at + 1] as f64);
    (from + share * (to - from)).round().max(0.0) as u64
}

/// How late an owner's timer fires past the deadline it was set to, as its host's measured waits
/// were (`hyper-timing-trace timer`, the socket wait an owner's timer is).
#[derive(Clone, Copy, Debug)]
enum Timer {
    /// The sweep's rows: each asked wait, nanoseconds, and its lateness's quantiles. A wait between
    /// two rows is late as both are at the same probability, interpolated in the wait; a wait past
    /// the longest row as that row, no lateness past what was measured being taken: the sweep
    /// ends at 10 ms, and extrapolating macOS's coalescing in proportion to the wait (its leeway
    /// is `min(wait >> shift, cap)`, `docs/timing.md` §2.4, the cap unmeasured) made the lateness
    /// grow with every interval a link moved to, so its correlation never fell and it never
    /// configured.
    Sweep(&'static [(u64, Table)]),
    /// A wait ends on the next clock interrupt of this period, its phase against the interrupts
    /// uniform: late by up to the period.
    Tick(u64),
}

impl Timer {
    /// The lateness of a wait of `asked` at probability `u`.
    fn late(&self, asked: u64, u: f64) -> u64 {
        match *self {
            Self::Tick(period) => (u * period as f64) as u64,
            Self::Sweep(rows) => {
                let above = rows.partition_point(|(wait, _)| *wait <= asked);
                match (above.checked_sub(1).map(|at| &rows[at]), rows.get(above)) {
                    (None, Some((_, first))) => quantile(first, u),
                    (Some((low, below)), Some((high, upper))) => {
                        let (from, to) = (quantile(below, u) as f64, quantile(upper, u) as f64);
                        let share = (asked - low) as f64 / (high - low) as f64;
                        (from + share * (to - from)).round() as u64
                    }
                    (Some((_, longest)), None) => quantile(longest, u),
                    (None, None) => 0,
                }
            }
        }
    }
}

/// How the simulated world behaves: each quantity drawn from a host's measured distribution
/// (`worlds`, the heartbeat traces of `docs/benchmarks.md`, "The simulation's worlds").
#[derive(Clone, Copy, Debug)]
struct World {
    name: &'static str,
    /// One-way delay, from the send to the receiver's kernel stamp. No world loses a message: no
    /// trace lost one of millions; a sender behind its schedule skips heartbeats, which its
    /// receiver counts lost.
    delay: &'static Table,
    /// A write of a block and the platform's full flush.
    flush: &'static Table,
    /// The owner's timer.
    timer: Timer,
    /// The host's freezes, where its trace measured them (`hyper-timing-trace freezes`), each
    /// node's replayed from a phase of its own: a frozen node's owner does nothing, its timer and
    /// what arrives and completes held until the thaw, when it takes them in order and polls once,
    /// as an owner that drains its socket before it polls does (`LinkEstimator::on_heartbeat`).
    freezes: Option<&'static Freezes>,
    /// Whether the node's groups keep its log busy: each write submitted as the last completes, so
    /// a heartbeat waits for no liveness write of its own.
    busy: bool,
}

/// macOS on the M5 Max, loopback: the traces of 2026-10-02 at load 42–81 (`worlds`).
const MACOS: World = World {
    name: "macos",
    delay: &worlds::MACOS_DELAY,
    flush: &worlds::MACOS_FLUSH,
    timer: Timer::Sweep(&worlds::MACOS_TIMER),
    freezes: Some(&worlds::MACOS_FREEZES),
    busy: false,
};

/// macOS with its groups keeping its log busy: its flushes back to back, as the trace tool's sweep
/// made them.
const BUSY: World = World {
    name: "busy",
    flush: &worlds::MACOS_BUSY_FLUSH,
    busy: true,
    ..MACOS
};

/// Linux in Docker Desktop's VM on the same machine: 1 ms ticks, `fdatasync` on a disk image
/// (`worlds`).
const LINUX: World = World {
    name: "linux",
    delay: &worlds::LINUX_DELAY,
    flush: &worlds::LINUX_FLUSH,
    timer: Timer::Sweep(&worlds::LINUX_TIMER),
    freezes: Some(&worlds::LINUX_FREEZES),
    busy: false,
};

/// A Windows host, which the trace tool cannot record (`worlds`): a timed wait ends on the next
/// 15.625 ms clock interrupt (Microsoft, `timeBeginPeriod`), and a full flush takes what
/// hyper-durable-e2e's members measured on the windows-2025 and windows-11-arm runners. Its loopback
/// delay is not measured: it takes Linux's, three orders below its timer and its flush. Nor are its
/// freezes, and it has none.
const WINDOWS: World = World {
    name: "windows",
    delay: &worlds::LINUX_DELAY,
    flush: &worlds::WINDOWS_FLUSH,
    timer: Timer::Tick(worlds::WINDOWS_TICK),
    freezes: None,
    busy: false,
};

enum Event {
    /// A message reaches `to`, received by its kernel at `stamp`.
    Arrive {
        to: usize,
        from: usize,
        bytes: Vec<u8>,
        stamp: u64,
    },
    /// A node's disk makes a write durable.
    Durable {
        node: usize,
        write: Write,
        started: u64,
    },
    /// A node's host freezes: the next freeze of its replay.
    Freeze { node: usize },
    /// A node's host thaws, unless a later freeze holds it longer.
    Thaw { node: usize },
}

struct Owner {
    id: PeerId,
    sent: Vec<(PeerId, Vec<u8>)>,
    flush: bool,
    changes: Vec<Change>,
}

impl Output for Owner {
    fn heartbeat(&mut self, peer: PeerId, message: &[u8]) {
        self.sent.push((peer, message.to_vec()));
    }
    fn flush(&mut self) {
        self.flush = true;
    }
    fn change(&mut self, change: Change) {
        self.changes.push(change);
    }
}

struct Node {
    liveness: Liveness,
    owner: Owner,
    alive: bool,
    disk_stalled: bool,
    /// The heartbeats taken when the node last charged its detectors an election.
    charged: u64,
    /// The disk's flush queue: one write at a time, as one device.
    disk_busy_until: u64,
    /// Every heartbeat sent: to whom and the message, at what time.
    log: Vec<(u64, PeerId, Heartbeat)>,
    /// Every durable completion: when.
    durable: Vec<u64>,
    suspicions: Vec<Suspicion>,
    /// What the node fed its stream and what the stream told it, in order (`record`): every
    /// suspicion in it is traced to the detector's rule, and every count the stream reports is
    /// its.
    record: Record,
    /// The peers whose restart the node's stream reported.
    restarts: Vec<PeerId>,
    /// What the owner believes of each peer from the changes it was told: suspected or not. The
    /// owner trusts a peer until told otherwise, and a restarted one is trusted (`Change`).
    believed: BTreeMap<PeerId, bool>,
    /// The owner's timer: the deadline it was set to, the stream's wake, when it fires, late by a
    /// lateness drawn once, when the deadline was set ([`Sim::arm`]), and whether its wait began
    /// before the deadline, which makes the wait's end at or past it, the timer's or what came
    /// first, a sample of `G` (`Liveness::on_wait`).
    timer: Option<(u64, u64, bool)>,
    /// The host is frozen until this time (`World::freezes`).
    frozen_until: u64,
    /// What arrived and completed while the host was frozen, in order: taken at the thaw.
    held: Vec<Event>,
    /// The node's replay of its host's freezes: the point of the trace it starts at, and the
    /// freezes replayed so far.
    phase: u64,
    replayed: u64,
}

struct Sim {
    now: u64,
    world: World,
    draws: Draws,
    nodes: Vec<Node>,
    queue: BinaryHeap<Reverse<(u64, u64)>>,
    events: BTreeMap<u64, Event>,
    next_event: u64,
    /// When each node first held each pair configured: `(node, peer)` to the time.
    configured_at: BTreeMap<(usize, PeerId), u64>,
    /// The latest any timer was set to fire past its deadline, and the longest any host froze:
    /// how late past what it waits for a poll can come in this run.
    latest_late: u64,
    longest_freeze: u64,
}

impl Sim {
    fn new(count: usize, world: World, seed: u64) -> Self {
        let nodes = (0..count)
            .map(|i| {
                let id = i as u64 + 1;
                let mut liveness = Liveness::new(Settings {
                    local: id,
                    // The node's first start: its run record held none.
                    run: 1,
                    max_peers: count,
                    history: Exposure::new(),
                })
                .unwrap();
                for peer in 1..=count as u64 {
                    if peer != id {
                        liveness.attach(peer).unwrap();
                    }
                }
                Node {
                    liveness,
                    owner: Owner {
                        id,
                        sent: Vec::new(),
                        flush: false,
                        changes: Vec::new(),
                    },
                    alive: true,
                    disk_stalled: false,
                    charged: 0,
                    disk_busy_until: 0,
                    log: Vec::new(),
                    durable: Vec::new(),
                    suspicions: Vec::new(),
                    record: {
                        let mut record = Record::default();
                        record.began();
                        record
                    },
                    restarts: Vec::new(),
                    believed: BTreeMap::new(),
                    timer: None,
                    frozen_until: 0,
                    held: Vec::new(),
                    phase: 0,
                    replayed: 0,
                }
            })
            .collect();
        let mut sim = Self {
            now: 0,
            world,
            draws: Draws::new(seed),
            nodes,
            queue: BinaryHeap::new(),
            events: BTreeMap::new(),
            next_event: 0,
            configured_at: BTreeMap::new(),
            latest_late: 0,
            longest_freeze: 0,
        };
        for node in 0..count {
            if world.busy {
                sim.submit(node, Write::Log);
            }
            if let Some(freezes) = world.freezes {
                // A uniform point of the trace: 53 bits of the node's own stream.
                let u = sim.draws.unit("freeze", node as u64, 0);
                sim.nodes[node].phase = (u * freezes.span as f64) as u64;
                sim.schedule_freeze(node);
            }
        }
        sim
    }

    /// The next freeze of `node`'s replay, when it begins and how long it lasts: the trace's
    /// freezes from the node's phase on, round the trace again past its end, so the host freezes
    /// as the trace's did, a span apart.
    fn next_freeze(&self, node: usize) -> Option<(u64, u64)> {
        let freezes = self.world.freezes?;
        let count = freezes.at.len() as u64;
        if count == 0 {
            return None;
        }
        let n = &self.nodes[node];
        let index = freezes.at.partition_point(|(onset, _)| *onset < n.phase) as u64 + n.replayed;
        let (onset, length) = freezes.at[(index % count) as usize];
        Some(((index / count) * freezes.span + onset - n.phase, length))
    }

    fn schedule_freeze(&mut self, node: usize) {
        if let Some((at, _)) = self.next_freeze(node) {
            self.schedule(at, Event::Freeze { node });
        }
    }

    fn schedule(&mut self, at: u64, event: Event) {
        let key = self.next_event;
        self.next_event += 1;
        self.queue.push(Reverse((at, key)));
        self.events.insert(key, event);
    }

    /// A write submitted on `node`'s disk now, durable after its flush.
    fn submit(&mut self, node: usize, write: Write) {
        if self.nodes[node].disk_stalled {
            return;
        }
        let start = self.now.max(self.nodes[node].disk_busy_until);
        let done = start + self.draws.draw("disk", node as u64, 0, self.world.flush);
        self.nodes[node].disk_busy_until = done;
        let started = self.now;
        self.schedule(
            done,
            Event::Durable {
                node,
                write,
                started,
            },
        );
    }

    /// Polls `node` and carries out what it asked; a frozen node polls at its thaw.
    fn poll(&mut self, node: usize) {
        if !self.nodes[node].alive || self.nodes[node].frozen_until > self.now {
            return;
        }
        self.polled(node);
        self.drain(node);
    }

    /// Polls `node`'s stream now and records the poll, with what it told and the trust it holds
    /// of each peer after.
    fn polled(&mut self, node: usize) {
        let now = self.now;
        let count = self.nodes.len() as u64;
        let n = &mut self.nodes[node];
        let told = n.owner.changes.len();
        n.liveness.poll(now, &mut n.owner);
        let id = n.owner.id;
        let trusts: Vec<(PeerId, Option<Trust>)> = (1..=count)
            .filter(|peer| *peer != id)
            .map(|peer| (peer, n.liveness.trust(peer)))
            .collect();
        n.record.polled(now, &n.owner.changes[told..], trusts);
    }

    /// Feeds `node`'s stream a heartbeat from `from`, stamped by the kernel at `stamp`, and records
    /// the call: what the stream made of it, what it told, the trust it holds of the peer after.
    fn feed(&mut self, node: usize, from: usize, bytes: &[u8], stamp: u64) -> Result<(), Refusal> {
        let n = &mut self.nodes[node];
        let peer = from as u64 + 1;
        let told = n.owner.changes.len();
        let outcome = n.liveness.on_heartbeat(peer, bytes, stamp, &mut n.owner);
        let beat = Beat::of(bytes, stamp).expect("the simulation sends heartbeats whole");
        let holds = n.liveness.trust(peer);
        n.record
            .fed(peer, beat, outcome, &n.owner.changes[told..], holds);
        outcome
    }

    /// `node` restarts: a new process with the stream `liveness`, its record begun again.
    fn restart(&mut self, node: usize, liveness: Liveness) {
        let n = &mut self.nodes[node];
        n.liveness = liveness;
        n.record.began();
        n.believed.clear();
        n.alive = true;
        n.disk_busy_until = self.now;
    }

    fn drain(&mut self, node: usize) {
        let sent = std::mem::take(&mut self.nodes[node].owner.sent);
        for (peer, bytes) in sent {
            let beat = Heartbeat::decode(&bytes).unwrap();
            self.nodes[node].log.push((self.now, peer, beat));
            self.nodes[node].record.sent(peer, beat.run, beat.seq);
            let delay = self.draws.draw("link", node as u64, peer, self.world.delay);
            self.schedule(
                self.now + delay,
                Event::Arrive {
                    to: peer as usize - 1,
                    from: node,
                    bytes,
                    stamp: self.now + delay,
                },
            );
        }
        if std::mem::take(&mut self.nodes[node].owner.flush) {
            self.submit(node, Write::Liveness);
        }
        self.arm(node);
        let changes = std::mem::take(&mut self.nodes[node].owner.changes);
        for change in changes {
            let n = &mut self.nodes[node];
            n.believed
                .insert(change.peer(), matches!(change, Change::Suspected(_)));
            match change {
                Change::Suspected(suspicion) => n.suspicions.push(suspicion),
                Change::Restarted { peer, .. } => n.restarts.push(peer),
                Change::Trusted { .. } => {}
            }
        }
    }

    /// The owner's contract (`docs/timing.md` §2.8): what each live node's owner was told of each
    /// peer is what its stream believes, after every step. A peer no margin judges is one the owner
    /// trusts.
    fn told_is_believed(&self) {
        let count = self.nodes.len() as u64;
        for node in self.nodes.iter().filter(|n| n.alive) {
            for peer in (1..=count).filter(|p| *p != node.owner.id) {
                let Some(trust) = node.liveness.trust(peer) else {
                    continue;
                };
                let told = node.believed.get(&peer).copied().unwrap_or(false);
                assert_eq!(
                    trust == Trust::Suspected,
                    told,
                    "node {} at {} ns: its stream holds peer {peer} {trust:?}, its owner was told \
                     {}",
                    node.owner.id,
                    self.now,
                    if told {
                        "suspected"
                    } else {
                        "nothing against it"
                    }
                );
            }
        }
    }

    /// Every suspicion every node's stream told, traced to the detector's rule from the nodes'
    /// records (`record::trace`); a suspicion or a passed freshness point that does not trace fails
    /// the test, each named. What the trace found.
    fn traced(&self) -> Traced {
        let records: Vec<(PeerId, &[record::Entry])> = self
            .nodes
            .iter()
            .map(|node| (node.owner.id, node.record.entries.as_slice()))
            .collect();
        let (traced, failures) = record::trace(&records);
        assert!(
            failures.is_empty(),
            "{} suspicions or passed points do not trace to the detector's rule:\n{}",
            failures.len(),
            failures.join("\n")
        );
        traced
    }

    /// Every count each node's stream reports of each peer is its record's: the suspicions told,
    /// the heartbeats taken and refused for their proof, sent, and the slots skipped between.
    fn counted(&self) {
        let count = self.nodes.len() as u64;
        for node in &self.nodes {
            for peer in (1..=count).filter(|peer| *peer != node.owner.id) {
                if let Some(report) = node.liveness.report(peer)
                    && let Some(differs) = node.record.differs(peer, &report)
                {
                    panic!("node {} at {} ns: {differs}", node.owner.id, self.now);
                }
            }
        }
    }

    /// The suspicions of the live peers among `nodes` and their allowance: reported, as the model's
    /// figures, never asserted (`docs/benchmarks.md`).
    fn allowance(&self, nodes: &[usize]) -> (u64, f64) {
        let mut totals = (0u64, 0.0f64);
        for &node in nodes {
            for &peer in nodes.iter().filter(|peer| **peer != node) {
                let report = self.nodes[node].liveness.report(peer as u64 + 1).unwrap();
                totals.0 += report.suspicions;
                totals.1 += report.allowance;
            }
        }
        totals
    }

    /// The election cost each node charges its detectors: the library's law over the round trips
    /// its streams measured and its flush, once a quorum's paths are measured. Charged again on the
    /// detectors' own doubling schedule: once the heartbeats a node has taken have doubled since
    /// it last charged them, as the estimates the law reads have renewed.
    fn elect(&mut self) {
        let count = self.nodes.len();
        for node in &mut self.nodes {
            let taken: u64 = (1..=count as u64)
                .filter_map(|peer| node.liveness.report(peer))
                .map(|report| report.taken)
                .sum();
            if taken == 0 || taken < 2 * node.charged {
                continue;
            }
            let (Some(granularity), Some(durable)) =
                (node.liveness.granularity(), node.liveness.flush_mean())
            else {
                continue;
            };
            let peers: Vec<PeerId> = (1..=count as u64).filter(|p| *p != node.owner.id).collect();
            let paths: Vec<_> = peers
                .iter()
                .filter_map(|peer| node.liveness.round_trip(*peer).copied())
                .collect();
            let Some(span) = Ballot::measure(paths.iter(), count, durable, granularity)
                .and_then(|ballot| ballot.span(granularity))
            else {
                continue;
            };
            for peer in peers {
                node.liveness.set_election(peer, span.election).unwrap();
            }
            node.charged = taken;
        }
    }

    /// `node`'s timer set to its stream's wake, as an owner sets it after every call into the
    /// stream: a deadline it already has changes nothing and draws nothing; a new one fires at
    /// the later of it and now, late by a lateness drawn once, as a timer is (hyper-sim's
    /// `World::wake`). Drawn again at every turn of the loop and anchored at the present, a due
    /// wake was pushed past the world's lateness bound by every event handled before it, which
    /// put a node's notice of a death 84 µs past its freshness point in a world whose wakes are
    /// at most 80 µs late (seed 285 of the soak, `docs/timing.md` §2.8).
    fn arm(&mut self, node: usize) {
        let asked = self.nodes[node].liveness.wake();
        if self.nodes[node].timer.map(|(deadline, ..)| deadline) == asked {
            return;
        }
        let fires = asked.map(|deadline| {
            let u = self.draws.unit("timer", node as u64, 0);
            let late = self.world.timer.late(deadline.saturating_sub(self.now), u);
            self.latest_late = self.latest_late.max(late);
            // A wait begins before its deadline only if the owner was not past it when it set
            // the timer: one set late is the owner's lateness, not its timer's.
            (
                deadline,
                deadline.max(self.now) + late,
                deadline >= self.now,
            )
        });
        self.nodes[node].timer = fires;
    }

    /// The earliest timer of a live node, held while its host is frozen: the thaw polls.
    fn next_wake(&self) -> Option<(u64, usize)> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.alive)
            .filter_map(|(i, node)| {
                node.timer
                    .map(|(_, fires, _)| (fires.max(node.frozen_until), i))
            })
            .min()
    }

    /// Runs while `keep` holds of the world, an event or a wake at a time, polling every node
    /// first; never past `until`, where one is given.
    fn run_while(&mut self, keep: impl Fn(&Self) -> bool, until: Option<u64>) {
        for node in 0..self.nodes.len() {
            self.poll(node);
        }
        while keep(self) {
            let event_at = self.queue.peek().map(|Reverse((at, _))| *at);
            let wake = self.next_wake();
            let (at, is_event) = match (event_at, wake) {
                (Some(e), Some((w, _))) if e <= w => (e, true),
                (Some(e), None) => (e, true),
                (_, Some((w, _))) => (w, false),
                (None, None) => break,
            };
            if let Some(until) = until
                && at > until
            {
                self.now = until;
                break;
            }
            self.now = at;
            if is_event {
                let Reverse((_, key)) = self.queue.pop().unwrap();
                let event = self.events.remove(&key).unwrap();
                self.handle(event);
            } else if let Some((_, node)) = wake {
                // The timer fired: its wait, begun before its deadline, is reported, and the
                // timer is set again after the poll.
                if let Some((deadline, _, true)) = self.nodes[node].timer.take() {
                    self.nodes[node].liveness.on_wait(deadline, at);
                }
                self.poll(node);
            }
            self.elect();
            self.note_configured();
            self.told_is_believed();
            self.counted();
        }
        self.traced();
    }

    /// Notes the pairs each live node has newly configured.
    fn note_configured(&mut self) {
        let count = self.nodes.len() as u64;
        for (index, node) in self.nodes.iter().enumerate().filter(|(_, n)| n.alive) {
            for peer in (1..=count).filter(|p| *p != node.owner.id) {
                if !self.configured_at.contains_key(&(index, peer))
                    && node.liveness.report(peer).is_some_and(|r| r.configured)
                {
                    self.configured_at.insert((index, peer), self.now);
                }
            }
        }
    }

    fn handle(&mut self, event: Event) {
        // A frozen host's owner takes what arrives and completes at its thaw.
        if let Event::Arrive { to: node, .. } | Event::Durable { node, .. } = event
            && self.nodes[node].frozen_until > self.now
        {
            self.nodes[node].held.push(event);
            return;
        }
        match event {
            Event::Arrive {
                to,
                from,
                bytes,
                stamp,
            } => {
                if !self.nodes[to].alive {
                    return;
                }
                self.woken(to);
                // Refusals are the crate's to make: a stale or unproven heartbeat is dropped.
                let _ = self.feed(to, from, &bytes, stamp);
                self.polled(to);
                self.drain(to);
            }
            Event::Durable {
                node,
                write,
                started,
            } => {
                if !self.nodes[node].alive || self.nodes[node].disk_stalled {
                    return;
                }
                self.woken(node);
                let now = self.now;
                let n = &mut self.nodes[node];
                n.durable.push(now);
                n.liveness.on_durable(write, started, now);
                self.polled(node);
                self.drain(node);
                // The groups keep the log busy: their next write as this one completes.
                if write == Write::Log && self.world.busy {
                    self.submit(node, Write::Log);
                }
            }
            Event::Freeze { node } => {
                let Some((_, length)) = self.next_freeze(node) else {
                    return;
                };
                self.longest_freeze = self.longest_freeze.max(length);
                let n = &mut self.nodes[node];
                n.frozen_until = n.frozen_until.max(self.now + length);
                n.replayed += 1;
                let thaw = n.frozen_until;
                self.schedule(thaw, Event::Thaw { node });
                self.schedule_freeze(node);
            }
            Event::Thaw { node } => self.thaw(node),
        }
    }

    /// `node`'s owner woken now by what came: a wait it began before the stream's wake that this
    /// ends at or past the wake is reported, whatever ended it (`Liveness::on_wait`), and the
    /// timer it was waiting on is spent. One that came before the wake ends no wait the stream
    /// counts, and the timer stays.
    fn woken(&mut self, node: usize) {
        let now = self.now;
        let n = &mut self.nodes[node];
        if let Some((deadline, _, began_before)) = n.timer
            && deadline <= now
        {
            n.timer = None;
            if began_before {
                n.liveness.on_wait(deadline, now);
            }
        }
    }

    /// `node`'s host thaws: its owner takes, in order, the datagrams that arrived (each judged at
    /// its kernel stamp, `Liveness::on_heartbeat`) and the writes that completed (reported now,
    /// when the owner learns of them), then polls once. A later freeze that holds the host longer
    /// thaws it instead.
    fn thaw(&mut self, node: usize) {
        if self.nodes[node].frozen_until > self.now || !self.nodes[node].alive {
            return;
        }
        let held = std::mem::take(&mut self.nodes[node].held);
        let now = self.now;
        // The timer, due during the freeze, is the poll's: set again after it. The wait it ended
        // is the thaw's, a frozen host's lateness, reported before what arrived is read.
        if let Some((deadline, _, true)) = self.nodes[node].timer.take()
            && deadline <= now
        {
            self.nodes[node].liveness.on_wait(deadline, now);
        }
        for event in held {
            let completed = match event {
                Event::Arrive {
                    from, bytes, stamp, ..
                } => {
                    let _ = self.feed(node, from, &bytes, stamp);
                    None
                }
                Event::Durable { write, started, .. } if !self.nodes[node].disk_stalled => {
                    let n = &mut self.nodes[node];
                    n.durable.push(now);
                    n.liveness.on_durable(write, started, now);
                    Some(write)
                }
                _ => None,
            };
            // The groups keep the log busy: their next write as they learn this one completed.
            if completed == Some(Write::Log) && self.world.busy {
                self.submit(node, Write::Log);
            }
        }
        self.poll(node);
    }

    fn configured(&self) -> bool {
        self.nodes.iter().filter(|n| n.alive).all(|n| {
            (1..=self.nodes.len() as u64)
                .filter(|p| *p != n.owner.id && self.nodes[*p as usize - 1].alive)
                .all(|p| n.liveness.report(p).is_some_and(|r| r.configured))
        })
    }

    /// The live pairs that have taken more heartbeats than any window holds without a
    /// configuration of their own: a link whose estimator has not measured what it needs in
    /// `WINDOW_LIMIT` heartbeats, the longest window the drift bound lets any link average
    /// (`hyper_timing::link`), is one whose correlation no window of it can resolve, the failure
    /// `docs/timing.md` §2.9 found.
    fn unresolved(&self) -> Vec<(PeerId, PeerId, u64)> {
        let count = self.nodes.len() as u64;
        let mut stuck = Vec::new();
        for node in self.nodes.iter().filter(|n| n.alive) {
            for peer in (1..=count).filter(|p| *p != node.owner.id) {
                if !self.nodes[peer as usize - 1].alive {
                    continue;
                }
                if let Some(report) = node.liveness.report(peer)
                    && !report.configured
                    && report.taken > WINDOW_LIMIT
                {
                    stuck.push((node.owner.id, peer, report.taken));
                }
            }
        }
        stuck
    }

    /// Whether nothing is left to happen but the hosts' freezes: no message or write in flight or
    /// held, and no timer set.
    fn quiet(&self) -> bool {
        self.events
            .values()
            .all(|event| matches!(event, Event::Freeze { .. } | Event::Thaw { .. }))
            && self
                .nodes
                .iter()
                .all(|node| node.timer.is_none() && node.held.is_empty())
    }

    /// Runs until every live pair has taken twice the heartbeats it had: as much history again as
    /// it holds, the span the doubling schedule renews a configuration over (`pair.rs`,
    /// `renewal_due`), so a run measured to here crosses a renewal of every configuration.
    fn run_until_doubled(&mut self) {
        let count = self.nodes.len() as u64;
        let pairs: Vec<(usize, PeerId, u64)> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.alive)
            .flat_map(|(index, node)| {
                (1..=count)
                    .filter(move |peer| *peer != node.owner.id)
                    .filter_map(move |peer| {
                        node.liveness
                            .report(peer)
                            .map(|report| (index, peer, report.taken))
                    })
            })
            .filter(|(_, peer, _)| self.nodes[*peer as usize - 1].alive)
            .collect();
        self.run_while(
            |sim| {
                pairs.iter().any(|(node, peer, taken)| {
                    sim.nodes[*node]
                        .liveness
                        .report(*peer)
                        .is_some_and(|report| report.taken < (2 * taken).max(taken + 1))
                })
            },
            None,
        );
    }

    /// Runs until every live node but `victim` holds it suspected.
    fn run_until_suspected(&mut self, victim: usize) {
        self.run_while(
            |sim| {
                (0..sim.nodes.len())
                    .filter(|node| *node != victim && sim.nodes[*node].alive)
                    .any(|node| {
                        sim.nodes[node].liveness.trust(victim as u64 + 1) != Some(Trust::Suspected)
                    })
            },
            None,
        );
    }

    /// Runs until every live pair is configured; a pair that takes more heartbeats than any window
    /// holds unconfigured fails it (`unresolved`).
    fn run_until_configured(&mut self) {
        self.run_while(
            |sim| {
                let stuck = sim.unresolved();
                assert!(stuck.is_empty(), "unconfigured links: {stuck:?}");
                !sim.configured()
            },
            None,
        );
    }
}

/// Live peers: every pair configures and trusts, every suspicion of a live peer is traced to the
/// detector's rule, and every count each stream reports is its record's (`Sim::traced` after every
/// run, `Sim::counted` after every step). The suspicions against the allowance the configurations
/// promised are the model's figures, reported (`docs/benchmarks.md`), never asserted: a count is
/// what the rule found, not a draw to test a bound on an expectation with.
#[test]
fn live_peers_configure_and_every_suspicion_of_them_is_traced() {
    let (mut suspicions, mut allowance) = (0u64, 0.0f64);
    let mut traced = Traced::default();
    for seed in seeds(1, 8) {
        let mut sim = Sim::new(3, MACOS, seed);
        sim.run_until_configured();
        sim.run_until_doubled();
        for node in &sim.nodes {
            for peer in (1..=3u64).filter(|p| *p != node.owner.id) {
                let report = node.liveness.report(peer).unwrap();
                assert!(report.configured, "seed {seed}");
                assert!(report.taken > 0 && report.unproven == 0, "{report:?}");
            }
        }
        let (count, allowed) = sim.allowance(&[0, 1, 2]);
        suspicions += count;
        allowance += allowed;
        traced += sim.traced();
    }
    println!("every suspicion traced: {traced}");
    println!("suspicions of live peers {suspicions}, allowance {allowance:.1}");
}

/// Every heartbeat sent carries a flush made durable after the previous heartbeat to that peer
/// was due, newer than the previous one's, and none leaves without one.
#[test]
fn no_heartbeat_leaves_without_a_newer_flush() {
    for (seed, world) in seeds(2, 1).flat_map(|seed| [MACOS, BUSY].map(|world| (seed, world))) {
        let mut sim = Sim::new(3, world, seed);
        sim.run_until_configured();
        sim.run_until_doubled();
        for node in &sim.nodes {
            let mut previous: BTreeMap<PeerId, Heartbeat> = BTreeMap::new();
            for (at, peer, beat) in &node.log {
                let due = beat.sent_ns - beat.late_ns;
                let durable = beat.sent_ns - beat.flush_age_ns;
                assert_eq!(beat.sent_ns, *at);
                assert!(
                    durable + beat.interval_ns > due,
                    "the flush came after the previous was due"
                );
                assert!(
                    node.durable.contains(&durable),
                    "a flush reported to the stream"
                );
                if let Some(before) = previous.get(peer) {
                    assert!(beat.flushes > before.flushes);
                    assert!(beat.seq > before.seq);
                }
                previous.insert(*peer, *beat);
            }
            assert!(!node.log.is_empty());
        }
    }
}

/// A killed peer is suspected by every survivor, within the bound each states from the peer's
/// last heartbeat's schedule (one clock in the simulation, so the peer's schedule is on it).
#[test]
fn a_killed_peer_is_suspected_within_the_stated_bound() {
    for seed in seeds(3, 16) {
        let mut sim = Sim::new(4, MACOS, seed);
        sim.run_until_configured();
        sim.run_until_doubled();
        let victim = 3usize;
        // Killed while every survivor trusts it: one that suspected it by a mistake just before
        // would hold that suspicion through the kill and make no new one to measure.
        let trusted = |sim: &Sim| {
            (0..3).all(|node| {
                matches!(
                    sim.nodes[node].liveness.trust(victim as u64 + 1),
                    Some(Trust::Trusted { .. })
                )
            })
        };
        sim.run_while(|sim| !trusted(sim), None);
        let killed_at = sim.now;
        sim.nodes[victim].alive = false;
        let last_sent: Vec<u64> = (0..4usize)
            .map(|node| {
                sim.nodes[victim]
                    .log
                    .iter()
                    .filter(|(_, peer, _)| *peer == node as u64 + 1)
                    .map(|(at, _, _)| *at)
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        sim.run_until_suspected(victim);
        for (node, last_sent) in last_sent.iter().enumerate().take(3) {
            // The suspicion the survivor holds: a first one after the kill can be a mistake, the
            // victim's last heartbeat still in flight past its freshness point, which that
            // heartbeat ended.
            let found = sim.nodes[node]
                .suspicions
                .iter()
                .rfind(|s| s.peer == victim as u64 + 1)
                .copied()
                .unwrap_or_else(|| panic!("seed {seed}: node {node} never suspected the victim"));
            assert!(
                found.at_ns >= killed_at,
                "seed {seed}: held from before the kill"
            );
            let last = found.last.unwrap();
            assert_eq!(
                last.sent_ns, *last_sent,
                "the last heartbeat sent is the last taken"
            );
            let bound = found
                .detection
                .expect("every heartbeat in the window was echoed");
            assert!(
                found.at_ns - last.due_ns <= bound.as_nanos() as u64,
                "seed {seed}: suspected {} ns after the last schedule, past the stated {bound:?}",
                found.at_ns - last.due_ns
            );
            assert_eq!(
                sim.nodes[node].liveness.trust(victim as u64 + 1),
                Some(Trust::Suspected)
            );
        }
    }
}

/// A node whose disk stops completing flushes stops heartbeating, and every peer suspects it
/// within the bound it states, while the node itself is still running and receiving.
#[test]
fn a_stalled_disk_is_suspected_as_a_crash_is() {
    for seed in seeds(4, 8) {
        let mut sim = Sim::new(3, BUSY, seed);
        sim.run_until_configured();
        sim.run_until_doubled();
        let stalled = 2usize;
        // Stalled while every peer trusts it: one that suspected it just before (its host frozen,
        // say) would hold that suspicion through the stall and make no new one to measure.
        sim.run_while(
            |sim| {
                (0..2).any(|node| {
                    !matches!(
                        sim.nodes[node].liveness.trust(stalled as u64 + 1),
                        Some(Trust::Trusted { .. })
                    )
                })
            },
            None,
        );
        let stalled_at = sim.now;
        sim.nodes[stalled].disk_stalled = true;
        sim.run_until_suspected(stalled);
        // And on, until the stalled node has taken a heartbeat from each peer since: it still hears.
        let heard: Vec<u64> = (1..=2u64)
            .map(|peer| sim.nodes[stalled].liveness.report(peer).unwrap().taken)
            .collect();
        sim.run_while(
            |sim| {
                (1..=2u64).any(|peer| {
                    sim.nodes[stalled].liveness.report(peer).unwrap().taken
                        <= heard[peer as usize - 1]
                })
            },
            None,
        );
        // At most the heartbeats an in-flight flush had proved left after the stall.
        let late_sends = sim.nodes[stalled]
            .log
            .iter()
            .filter(|(at, _, beat)| {
                *at > stalled_at && beat.sent_ns - beat.flush_age_ns > stalled_at
            })
            .count();
        assert_eq!(
            late_sends, 0,
            "seed {seed}: a heartbeat left on a flush made after the stall"
        );
        for node in 0..2 {
            let found = sim.nodes[node]
                .suspicions
                .iter()
                .find(|s| s.peer == stalled as u64 + 1 && s.at_ns >= stalled_at)
                .copied()
                .unwrap_or_else(|| panic!("seed {seed}: node {node} never suspected the stall"));
            let last = found.last.unwrap();
            let bound = found.detection.unwrap();
            assert!(found.at_ns - last.due_ns <= bound.as_nanos() as u64);
        }
        // The stalled node still hears its peers: it took a heartbeat from each since every peer
        // suspected it (the wait above), and its suspicions of them, live throughout, are traced
        // with every other (`Sim::traced`).
    }
}

/// The stream is the pair's, not the group's: a thousand groups shared send what one does, and a
/// pair whose last group goes sends nothing.
#[test]
fn groups_share_one_stream_and_an_unshared_pair_is_silent() {
    for seed in seeds(5, 1) {
        groups_share_one_stream(seed);
    }
}

fn groups_share_one_stream(seed: u64) {
    let sent = |groups: u32| {
        let mut sim = Sim::new(2, MACOS, seed);
        for node in &mut sim.nodes {
            let peer = 3 - node.owner.id;
            for _ in 1..groups {
                node.liveness.attach(peer).unwrap();
            }
        }
        sim.run_until_configured();
        sim.run_until_doubled();
        (sim.nodes[0].log.len(), sim.nodes[1].log.len())
    };
    assert_eq!(sent(1), sent(1_000));
    let mut sim = Sim::new(2, MACOS, seed);
    sim.run_until_configured();
    for node in &mut sim.nodes {
        let peer = 3 - node.owner.id;
        node.liveness.detach(peer).unwrap();
        assert_eq!(node.liveness.report(peer), None);
    }
    let before = sim.nodes[0].log.len();
    // Until nothing is left to happen but the hosts' freezes: what was in flight lands, and no
    // timer is set.
    sim.run_while(|sim| !sim.quiet(), None);
    assert_eq!(
        sim.nodes[0].log.len(),
        before,
        "nothing shared, nothing sent"
    );
    assert_eq!(sim.nodes[0].liveness.wake(), None);
}

/// A restarted peer is reported restarted (`Change::Restarted`, for the core's `restarted`) once
/// by each other node and is judged at once by the detector in force. The old run's last heartbeat
/// to a survivor, delivered after the new run's first (the plane keeps two epochs a peer), is
/// refused as stale and counts no second failure (the count itself is
/// `a_superseded_runs_heartbeat_is_stale_and_its_restart_counts_once`'s).
#[test]
fn a_restarted_peer_is_trusted_again_and_counted() {
    for seed in seeds(6, 1) {
        a_restarted_peer(seed);
    }
}

fn a_restarted_peer(seed: u64) {
    let mut sim = Sim::new(3, MACOS, seed);
    sim.run_until_configured();
    let victim = 2usize;
    sim.nodes[victim].alive = false;
    sim.run_until_suspected(victim);
    assert_eq!(sim.nodes[0].liveness.trust(3), Some(Trust::Suspected));
    // A new run: a new process, the same node.
    let mut liveness = Liveness::new(Settings {
        local: 3,
        // Its run record raised at the start.
        run: 2,
        max_peers: 3,
        history: Exposure::new(),
    })
    .unwrap();
    liveness.attach(1).unwrap();
    liveness.attach(2).unwrap();
    let superseded = sim.nodes[victim]
        .log
        .iter()
        .rev()
        .find(|(_, peer, beat)| *peer == 1 && beat.run == 1)
        .map(|(_, _, beat)| *beat)
        .expect("the old run sent to node 1");
    sim.restart(victim, liveness);
    sim.poll(victim);
    sim.run_while(|sim| sim.nodes[0].restarts.is_empty(), None);
    let mut bytes = [0u8; MAX_BYTES];
    let stale = superseded.encode(&mut bytes).to_vec();
    let now = sim.now;
    let before = sim.nodes[0].liveness.mtbf();
    assert_eq!(
        sim.feed(0, victim, &stale, now),
        Err(Refusal::Stale),
        "the superseded run's heartbeat"
    );
    assert_eq!(sim.nodes[0].liveness.mtbf(), before, "no second failure");
    sim.drain(0);
    // Until both others saw the new run and trust it, and through a renewal of every pair.
    sim.run_while(
        |sim| {
            (0..2).any(|node| {
                sim.nodes[node].restarts.is_empty()
                    || !matches!(
                        sim.nodes[node].liveness.trust(3),
                        Some(Trust::Trusted { .. })
                    )
            })
        },
        None,
    );
    sim.run_until_doubled();
    for node in 0..2 {
        assert_eq!(
            sim.nodes[node].restarts,
            vec![3],
            "node {node} saw the restart once"
        );
    }
    assert!(sim.nodes[0].liveness.report(3).unwrap().configured);
}

/// A heartbeat of a superseded run, delivered after the new run's first (the plane keeps two
/// epochs a peer, so the old run's last datagrams still open), is refused as stale: the peer's
/// restart is reported once and counted once in the MTBF's evidence, and the new run's next
/// heartbeat is taken as the same run's. With an unordered run (a boot nonce) it was a restart
/// back to the old run and another to the new: three restarts reported, three failures counted.
#[test]
fn a_superseded_runs_heartbeat_is_stale_and_its_restart_counts_once() {
    let mut node = Liveness::new(Settings {
        local: 1,
        run: 1,
        max_peers: 1,
        history: Exposure::new(),
    })
    .unwrap();
    node.attach(2).unwrap();
    let mut owner = Owner {
        id: 1,
        sent: Vec::new(),
        flush: false,
        changes: Vec::new(),
    };
    // The first flush gives the floor, the second proves the first heartbeat, and the wait for
    // the wake it asks measures the granularity: the node takes heartbeats from here.
    node.on_durable(Write::Liveness, 0, 100 * US);
    node.poll(200 * US, &mut owner);
    node.on_durable(Write::Liveness, 200 * US, 300 * US);
    node.poll(300 * US, &mut owner);
    let wake = node.wake().expect("the next heartbeat is due");
    node.on_wait(wake, wake + 50 * US);
    node.poll(wake + 50 * US, &mut owner);
    assert!(node.granularity().is_some());
    let beat = |run: u64, seq: u64, flushes: u64| Heartbeat {
        run,
        seq,
        interval_ns: MS,
        floor_ns: 100 * US,
        ask_ns: 0,
        sent_ns: 10 * MS + seq * MS,
        late_ns: 0,
        flushes,
        flush_age_ns: 0,
        echo: None,
    };
    let mut out = [0u8; MAX_BYTES];
    let mut feed = |node: &mut Liveness, owner: &mut Owner, beat: Heartbeat, at: u64| {
        let bytes = beat.encode(&mut out).to_vec();
        node.on_heartbeat(2, &bytes, at, owner)
    };
    assert_eq!(feed(&mut node, &mut owner, beat(5, 0, 1), 11 * MS), Ok(()));
    assert_eq!(feed(&mut node, &mut owner, beat(5, 1, 2), 12 * MS), Ok(()));
    // The peer restarts: run 6, its numbers and flushes from the start.
    assert_eq!(feed(&mut node, &mut owner, beat(6, 0, 1), 13 * MS), Ok(()));
    // Run 5's last heartbeat, delivered late.
    assert_eq!(
        feed(&mut node, &mut owner, beat(5, 2, 3), 13 * MS + 500 * US),
        Err(Refusal::Stale)
    );
    assert_eq!(feed(&mut node, &mut owner, beat(6, 1, 2), 14 * MS), Ok(()));
    let end = 20 * MS;
    node.poll(end, &mut owner);
    let restarts = owner
        .changes
        .iter()
        .filter(|change| matches!(change, Change::Restarted { .. }))
        .count();
    assert_eq!(restarts, 1, "{:?}", owner.changes);
    // The node watched its one peer from its first poll to its last; one failure in that time.
    let mut expected = Exposure::new();
    expected.on_exposure(Duration::from_nanos(end - 200 * US));
    expected.on_failure();
    assert_eq!(node.mtbf(), expected.mtbf());
}

/// A heartbeat whose proof does not hold, or that is stale, from a stranger or from this node, is
/// refused.
#[test]
fn heartbeats_without_their_proof_are_refused() {
    let mut node = Liveness::new(Settings {
        local: 1,
        run: 1,
        max_peers: 1,
        history: Exposure::new(),
    })
    .unwrap();
    node.attach(2).unwrap();
    assert_eq!(node.attach(3), Err(Refusal::TooManyPeers));
    assert_eq!(node.attach(1), Err(Refusal::FromSelf));
    let mut owner = Owner {
        id: 1,
        sent: Vec::new(),
        flush: false,
        changes: Vec::new(),
    };
    let beat = Heartbeat {
        run: 9,
        seq: 0,
        interval_ns: 10 * MS,
        floor_ns: MS,
        ask_ns: 0,
        sent_ns: 50 * MS,
        late_ns: MS,
        flushes: 4,
        flush_age_ns: 2 * MS,
        echo: None,
    };
    let mut out = [0u8; MAX_BYTES];
    let send =
        |node: &mut Liveness, owner: &mut Owner, beat: Heartbeat, out: &mut [u8; MAX_BYTES]| {
            let bytes = beat.encode(out).to_vec();
            node.on_heartbeat(2, &bytes, 60 * MS, owner)
        };
    // No granularity measured yet: the proof is checked and the echo kept, nothing estimated.
    assert_eq!(
        send(&mut node, &mut owner, beat, &mut out),
        Err(Refusal::Unmeasured)
    );
    let same = Heartbeat { seq: 1, ..beat };
    assert_eq!(
        send(&mut node, &mut owner, same, &mut out),
        Err(Refusal::Unproven),
        "no newer flush"
    );
    let old = Heartbeat {
        seq: 1,
        flushes: 5,
        flush_age_ns: 12 * MS,
        ..beat
    };
    assert_eq!(
        send(&mut node, &mut owner, old, &mut out),
        Err(Refusal::Unproven),
        "a flush from before the previous heartbeat was due"
    );
    assert_eq!(node.report(2).unwrap().unproven, 2);
    let bytes = beat.encode(&mut out).to_vec();
    assert_eq!(
        node.on_heartbeat(3, &bytes, 0, &mut owner),
        Err(Refusal::UnknownPeer)
    );
    assert_eq!(
        node.on_heartbeat(1, &bytes, 0, &mut owner),
        Err(Refusal::FromSelf)
    );
    assert_eq!(
        node.on_heartbeat(2, &bytes[..9], 0, &mut owner),
        Err(Refusal::Truncated)
    );
    assert_eq!(node.detach(3), Err(Refusal::UnknownPeer));
}

/// Problem 1 and item 10 of `docs/timing.md` (§2.9, §3): every link configures, or a crash on it
/// is suspected within the bound its suspicion states. On the measured hosts, macOS (whose freezes
/// make heartbeats at a short floor late together, too correlated for a window to measure,
/// `LinkEstimator::independent_interval`), macOS with its groups keeping its log busy, and Linux
/// in its VM:
/// - before any kill, every pair configures, none taking more heartbeats unconfigured than any
///   window holds (`Sim::unresolved`);
/// - one node is killed after a share of the heartbeats it sent before every pair had configured
///   in the same seed's run, drawn from the seed between none and twice as many: before it sent
///   any, while the links were young, and after; every survivor then suspects it, each suspicion
///   within the bound it states, from the victim's last heartbeat's schedule (one clock here), or,
///   for a victim never heard, from the start.
#[test]
fn every_link_configures_or_suspects_a_crash_within_its_bound() {
    let mut slowest = (0u64, 0u64, "");
    let mut judged_by = [0u64; 3];
    let mut longest_freeze = 0u64;
    for world in [MACOS, BUSY, LINUX] {
        let name = world.name;
        for seed in seeds(7, 32) {
            // The heartbeats the victim sends before every pair is configured, in this seed's run.
            let mut twin = Sim::new(4, world, seed);
            twin.run_until_configured();
            let victim = 3usize;
            let configured_after = twin.nodes[victim].log.len() as u64;
            for node in &twin.nodes {
                for peer in (1..=4u64).filter(|p| *p != node.owner.id) {
                    let taken = node.liveness.report(peer).unwrap().taken;
                    if taken > slowest.0 {
                        slowest = (taken, seed, name);
                    }
                }
            }
            let mut draw = Seeded::new(stream_seed(seed, "kill", &[]));
            let kill_after = draw.below(2 * configured_after + 1);
            let mut sim = Sim::new(4, world, seed);
            sim.run_while(
                |sim| (sim.nodes[victim].log.len() as u64) < kill_after,
                None,
            );
            let killed_at = sim.now;
            sim.nodes[victim].alive = false;
            let last_due: Vec<Option<u64>> = (0..3usize)
                .map(|node| {
                    sim.nodes[victim]
                        .log
                        .iter()
                        .filter(|(_, peer, _)| *peer == node as u64 + 1)
                        .map(|(_, _, beat)| beat.sent_ns - beat.late_ns)
                        .max()
                })
                .collect();
            sim.run_while(
                |sim| {
                    let stuck = sim.unresolved();
                    assert!(stuck.is_empty(), "{name} seed {seed}: {stuck:?}");
                    (0..3).any(|node| {
                        sim.nodes[node].liveness.trust(victim as u64 + 1) != Some(Trust::Suspected)
                    })
                },
                None,
            );
            longest_freeze = longest_freeze
                .max(twin.longest_freeze)
                .max(sim.longest_freeze);
            for (node, last_due) in last_due.iter().enumerate() {
                let suspicion = sim.nodes[node]
                    .suspicions
                    .iter()
                    .rfind(|s| s.peer == victim as u64 + 1)
                    .copied()
                    .unwrap_or_else(|| panic!("{name} seed {seed}: {node} holds no suspicion"));
                let bound = suspicion.detection.map(|d| d.as_nanos() as u64);
                match (suspicion.last, bound) {
                    (Some(last), Some(bound)) => {
                        // Its own configuration's or the node's evidence's margin: either states its bound,
                        // from the last heartbeat taken (a later one may have been lost).
                        assert!(Some(last.due_ns) <= *last_due, "{name} seed {seed}");
                        assert!(
                            suspicion.at_ns - last.due_ns <= bound,
                            "{name} seed {seed}: node {node} suspected {} ns past the last \
                             schedule, past its stated {bound}",
                            suspicion.at_ns - last.due_ns
                        );
                        let own = sim.nodes[node]
                            .liveness
                            .report(victim as u64 + 1)
                            .unwrap()
                            .configured;
                        judged_by[usize::from(!own)] += 1;
                    }
                    (None, Some(bound)) => {
                        // Never heard: from the start, an interval and the evidence's margin.
                        assert!(suspicion.at_ns <= bound, "{name} seed {seed}");
                        judged_by[2] += 1;
                    }
                    (Some(_), None) => {
                        // A heartbeat in the window carried no echo: a link killed in its first
                        // heartbeats, before the peer heard back. Suspected all the same.
                        assert!(suspicion.at_ns >= killed_at.min(suspicion.at_ns));
                        judged_by[1] += 1;
                    }
                    (None, None) => panic!("{name} seed {seed}: a suspicion with no bound"),
                }
            }
        }
    }
    println!(
        "most heartbeats a link took to configure: {} ({} seed {}); suspicions of the killed node \
         by its own detector {}, by the node's evidence's margin {}, never heard {}; the longest \
         freeze replayed {} ms",
        slowest.0,
        slowest.2,
        slowest.1,
        judged_by[0],
        judged_by[1],
        judged_by[2],
        longest_freeze / MS
    );
}

/// Item 10 of `docs/timing.md` §3: a peer from which no heartbeat ever comes (dead before its
/// first) is suspected by every other, once its node's pool can give a margin, within the bound
/// the suspicion states from the start: one interval at the node's own floor and the margin of the node's evidence.
#[test]
fn a_peer_never_heard_from_is_suspected() {
    for seed in seeds(8, 16) {
        let mut sim = Sim::new(4, MACOS, seed);
        sim.nodes[3].alive = false;
        sim.run_while(
            |sim| {
                let stuck = sim.unresolved();
                assert!(stuck.is_empty(), "seed {seed}: {stuck:?}");
                (0..3).any(|node| sim.nodes[node].liveness.trust(4) != Some(Trust::Suspected))
            },
            None,
        );
        for node in 0..3 {
            let suspicion = sim.nodes[node]
                .suspicions
                .iter()
                .find(|s| s.peer == 4)
                .copied()
                .unwrap();
            assert_eq!(suspicion.last, None);
            let bound = suspicion.detection.expect("a bound from the start");
            assert!(suspicion.at_ns <= bound.as_nanos() as u64, "seed {seed}");
            assert_eq!(sim.nodes[node].liveness.report(4).unwrap().taken, 0);
        }
    }
}

/// Item 10 of `docs/timing.md` §3, with three nodes: a peer that dies in its links' first
/// heartbeats, before any pair has its own evidence, leaves each survivor one live link, whose
/// configuration lengthens its interval to its best (seconds, on Windows' timer), and a pool fed at
/// that rate. Every survivor suspects it no later than the first poll once both its freshness point
/// has passed and the node holds a link's own evidence (a pair it has configured): the young link
/// is judged by the widest behaviour the node has measured (`docs/timing.md` §2.8, "Judged before
/// its own evidence"). In the four worlds, Windows' timer among them, the victim killed after a
/// share, drawn from the seed, of the heartbeats it sent before any pair configured in the same
/// seed's run.
#[test]
fn a_peer_dead_before_its_links_have_evidence_is_suspected_once_a_sibling_has_its_own() {
    for world in [MACOS, BUSY, LINUX, WINDOWS] {
        let name = world.name;
        let mut noticed = Vec::new();
        for seed in seeds(9, 32) {
            let victim = 2usize;
            let mut twin = Sim::new(3, world, seed);
            twin.run_while(|sim| sim.configured_at.is_empty(), None);
            let young = twin.nodes[victim].log.len() as u64;
            let mut draw = Seeded::new(stream_seed(seed, "kill", &[]));
            let kill_after = 1 + draw.below(young.saturating_sub(1).max(1));
            let mut sim = Sim::new(3, world, seed);
            sim.run_while(
                |sim| (sim.nodes[victim].log.len() as u64) < kill_after,
                None,
            );
            assert!(
                sim.configured_at.is_empty(),
                "{name} seed {seed}: killed after a pair configured"
            );
            let killed_at = sim.now;
            sim.nodes[victim].alive = false;
            sim.run_while(
                |sim| {
                    let stuck = sim.unresolved();
                    assert!(stuck.is_empty(), "{name} seed {seed}: {stuck:?}");
                    (0..2).any(|node| {
                        sim.nodes[node].liveness.trust(victim as u64 + 1) != Some(Trust::Suspected)
                    })
                },
                None,
            );
            // A poll comes at most a timer's lateness, or a freeze, past what it waits for: the
            // latest and the longest this run drew.
            let late = sim.latest_late + sim.longest_freeze;
            for node in 0..2usize {
                let suspicion = sim.nodes[node]
                    .suspicions
                    .iter()
                    .rfind(|s| s.peer == victim as u64 + 1)
                    .copied()
                    .unwrap_or_else(|| panic!("{name} seed {seed}: {node} holds no suspicion"));
                if let (Some(last), Some(bound)) = (suspicion.last, suspicion.detection) {
                    assert!(
                        suspicion.at_ns - last.due_ns <= bound.as_nanos() as u64,
                        "{name} seed {seed}: node {node} past its stated bound"
                    );
                }
                let sibling = (1 - node) as u64 + 1;
                // Suspected before its sibling configured: the pool had its evidence first.
                let evidence = sim
                    .configured_at
                    .get(&(node, sibling))
                    .copied()
                    .unwrap_or(u64::MAX);
                assert!(
                    suspicion.noticed_ns <= suspicion.at_ns.max(evidence).saturating_add(late),
                    "{name} seed {seed}: node {node} noticed {} ms after the kill, its freshness \
                     point {} ms and its sibling's configuration {} ms after it",
                    suspicion.noticed_ns.saturating_sub(killed_at) / MS,
                    suspicion.at_ns.saturating_sub(killed_at) / MS,
                    evidence.saturating_sub(killed_at) / MS,
                );
                // A suspicion held from before the kill (a mistake) counts as at the kill.
                noticed.push(suspicion.noticed_ns.saturating_sub(killed_at));
            }
        }
        noticed.sort_unstable();
        println!(
            "{name}: survivors noticed the victim's death {} ms after the kill at the median, {} ms \
             at the 90th percentile, {} ms at the most",
            noticed[noticed.len() / 2] / MS,
            noticed[noticed.len() * 9 / 10] / MS,
            noticed[noticed.len() - 1] / MS
        );
    }
}

/// A sender behind its schedule skips the slots it was behind for and sends the latest due; its
/// receiver takes each skipped slot as the next heartbeat's lateness, not as a heartbeat lost
/// (`docs/timing.md` §2.2). Here the sender is behind for every other slot of a 2 ms stream, and the
/// receiver's own timer is 5 ms late, past the interval: an E2E pair's shape, which the single
/// heartbeat the margin held configured with no margin at all (`α = 0`, the receiver's `G` past the
/// interval) and an unavailability past one, fed half the slots as losses. The receiver configures
/// a margin past the skipped slot, an unavailability below one, and suspects the live sender at no
/// freshness point once it has.
#[test]
fn a_sender_that_skips_slots_is_late_not_lost() {
    let mut node = Liveness::new(Settings {
        local: 1,
        run: 1,
        max_peers: 1,
        history: Exposure::new(),
    })
    .unwrap();
    node.attach(2).unwrap();
    node.set_election(2, Duration::from_millis(20)).unwrap();
    let mut owner = Owner {
        id: 1,
        sent: Vec::new(),
        flush: false,
        changes: Vec::new(),
    };
    let interval = 2 * MS;
    let late = 5 * MS;
    let mut out = [0u8; MAX_BYTES];
    let mut suspected_after = None;
    // The receiver's own stream and its wakes: each poll at its wake, 5 ms late.
    node.on_durable(Write::Liveness, 0, 100 * US);
    node.poll(200 * US, &mut owner);
    node.on_durable(Write::Liveness, 200 * US, 300 * US);
    let mut now = 300 * US;
    node.poll(now, &mut owner);
    let mut jitter = Seeded::new(stream_seed(0, "skips", &[]));
    for slot in (0..4_000u64).step_by(2) {
        let sent = 10 * MS + slot * interval;
        let arrival = sent + 100 * US + jitter.below(50 * US);
        // The owner's wakes before the heartbeat arrives, each at its wake and late.
        while let Some(wake) = node.wake().filter(|wake| *wake + late <= arrival) {
            if now < wake {
                node.on_wait(wake, wake + late);
            }
            now = now.max(wake + late);
            node.on_durable(Write::Liveness, now, now);
            node.poll(now, &mut owner);
        }
        let beat = Heartbeat {
            run: 7,
            seq: slot,
            interval_ns: interval,
            floor_ns: interval,
            ask_ns: 0,
            sent_ns: sent,
            late_ns: 0,
            flushes: slot + 1,
            flush_age_ns: 0,
            echo: None,
        };
        let bytes = beat.encode(&mut out).to_vec();
        let _ = node.on_heartbeat(2, &bytes, arrival, &mut owner);
        now = now.max(arrival);
        node.poll(now, &mut owner);
        let report = node.report(2).unwrap();
        if report.configured && suspected_after.is_none() {
            suspected_after = Some(report.suspicions);
        }
    }
    let report = node.report(2).unwrap();
    let configured = node.configuration(2).expect("the pair configured");
    assert!(
        node.granularity().unwrap() > Duration::from_nanos(interval),
        "the receiver's timer is late past the interval: {:?}",
        node.granularity()
    );
    assert!(
        configured.current.margin > Duration::from_nanos(interval),
        "a margin past the skipped slot: {configured:?}"
    );
    assert!(
        configured.current.unavailability < 1.0,
        "an unavailability below one: {configured:?}"
    );
    assert_eq!(
        Some(report.suspicions),
        suspected_after,
        "no suspicion of the live sender once configured: {report:?}"
    );
}

/// `G` is the lateness the operating system adds to the owner's waits (`docs/timing.md` §2.4). An
/// owner whose thread is held in its own write past a wake comes to that wake late, and that is its
/// write's lateness, not its timer's. Here the timer ends every wait 1 ms late and every other wake
/// finds the thread held 50 ms in a write: the owner reports the waits it began before their
/// deadlines (`Liveness::on_wait`), and `G` is the timer's 1 ms, exactly. Taken from every poll
/// past a wake, as it was, it held the writes too. The owner's stalls stay in the bound it states
/// of itself (`Liveness::latest_wake`).
#[test]
fn an_owner_held_in_its_own_write_is_no_lateness_of_its_timer() {
    let mut node = Liveness::new(Settings {
        local: 1,
        run: 1,
        max_peers: 1,
        history: Exposure::new(),
    })
    .unwrap();
    node.attach(2).unwrap();
    let mut owner = Owner {
        id: 1,
        sent: Vec::new(),
        flush: false,
        changes: Vec::new(),
    };
    let (late, held) = (MS, 50 * MS);
    node.on_durable(Write::Liveness, 0, 100 * US);
    let mut now = 200 * US;
    node.poll(now, &mut owner);
    let (mut waited, mut stalled) = (0u64, 0u64);
    for turn in 0..400u64 {
        // The flush the stream asked for, which proves the heartbeats due: made at once.
        if std::mem::take(&mut owner.flush) {
            node.on_durable(Write::Liveness, now, now);
            node.poll(now, &mut owner);
        }
        let wake = node.wake().expect("a heartbeat is always due");
        if wake > now {
            if turn % 2 == 0 {
                // It waits for the wake, and its timer ends the wait `late` past it.
                node.on_wait(wake, wake + late);
                now = wake + late;
                waited += 1;
            } else {
                // Its thread is in its own write past the wake: no wait began before it.
                now = wake + held;
                stalled += 1;
            }
        }
        // A wake already past when the owner comes to it is polled at once, no wait begun.
        node.poll(now, &mut owner);
    }
    assert!(
        waited > 100 && stalled > 100,
        "{waited} waits, {stalled} stalls"
    );
    assert_eq!(node.granularity(), Some(Duration::from_nanos(late)));
    assert!(node.latest_wake(now) >= Duration::from_nanos(held));
}

/// `G` is how late past its wakes the stream is polled while its owner waits for them
/// (`docs/timing.md` §2.4), whatever ends the wait: an owner woken past a wake by a message, before
/// its timer fired, came to the wake that late. Here the owner's timer ends a wait 1 ms past its
/// deadline, as Linux's 1 ms tick does, and a message comes 300 µs past every wake, so every wait
/// ends on the message. Reported as the waits they are, they give `G` = 300 µs exactly from the
/// first wake on, and every heartbeat of the peer's is taken. Counted only where the timer ended
/// them, as the contract said before, none counts: `G` is never measured and every heartbeat is
/// refused as unmeasured, as hyper-durable-e2e's members, asked for reports every few hundred
/// microseconds, refused theirs and never formed their group (§2.9).
#[test]
fn a_wait_a_message_ends_past_its_wake_measures_the_wake() {
    let (timer, message) = (MS, 300 * US);
    for counted in [false, true] {
        let mut node = Liveness::new(Settings {
            local: 1,
            run: 1,
            max_peers: 1,
            history: Exposure::new(),
        })
        .unwrap();
        node.attach(2).unwrap();
        let mut owner = Owner {
            id: 1,
            sent: Vec::new(),
            flush: false,
            changes: Vec::new(),
        };
        // The first flush gives the floor and proves the first heartbeat.
        node.on_durable(Write::Liveness, 0, 100 * US);
        let mut now = 200 * US;
        node.poll(now, &mut owner);
        let mut out = [0u8; MAX_BYTES];
        let mut outcomes = Vec::new();
        // Fewer heartbeats than a configuration needs (an Allan level of seven windows of eight):
        // every wake is one of the node's own heartbeats coming due.
        for seq in 0..50u64 {
            // The flush the stream asked for, made at once.
            if std::mem::take(&mut owner.flush) {
                node.on_durable(Write::Liveness, now, now);
                node.poll(now, &mut owner);
            }
            let wake = node.wake().expect("a heartbeat is always due");
            // The owner waits from now for the wake; the peer's heartbeat ends the wait past it,
            // before the timer would have.
            let came = wake + message;
            assert!(now < wake && came < wake + timer);
            if counted {
                node.on_wait(wake, came);
            }
            now = came;
            let beat = Heartbeat {
                run: 7,
                seq,
                interval_ns: MS,
                floor_ns: MS,
                ask_ns: 0,
                sent_ns: came,
                late_ns: 0,
                flushes: seq + 1,
                flush_age_ns: 0,
                echo: None,
            };
            let bytes = beat.encode(&mut out).to_vec();
            outcomes.push(node.on_heartbeat(2, &bytes, came, &mut owner));
            node.poll(now, &mut owner);
        }
        if counted {
            assert_eq!(node.granularity(), Some(Duration::from_nanos(message)));
            assert!(outcomes.iter().all(Result::is_ok), "{outcomes:?}");
        } else {
            assert_eq!(node.granularity(), None);
            assert!(
                outcomes.iter().all(|o| *o == Err(Refusal::Unmeasured)),
                "{outcomes:?}"
            );
        }
    }
}
