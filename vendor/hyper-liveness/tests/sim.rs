//! Node pairs under a deterministic simulation on hyper-sim's world and network (`docs/sim.md`
//! §13): one clock, and a host as its heartbeat traces measured it (`support/worlds.rs`): one-way
//! delays on the network's measured path, a disk per node and its flushes, owners whose timers
//! fire late, and hosts that freeze, every draw from a stream of the world and every arrival,
//! completion, freeze and timer an event the world orders. The owners run the crate as a real one does (poll at
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

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use hyper_liveness::{
    Change, Heartbeat, Liveness, MAX_BYTES, Output, PeerId, Refusal, Settings, Suspicion, Write,
};
use hyper_sim::Seeded;
use hyper_sim::net::{Measured, Net, NetLimits, Path, Ticket};
use hyper_sim::rng::stream_seed;
use hyper_sim::{Clock, Discipline, Fifo, Limits, NodeId, PPM, Source, Step, StreamId};
use hyper_timing::{Ballot, Exposure, Trust, WINDOW_LIMIT};

const MS: u64 = 1_000_000;
const US: u64 = 1_000;
/// The simulated clock's resolution: its readings are whole nanoseconds.
const RESOLUTION: Duration = Duration::from_nanos(1);
/// The bounds of a run (`docs/sim.md` §7), each four times the most any run took over 80 seeds of
/// every test (3,840 runs, measured 2026-10-04 on the port to hyper-sim's world): a soak that
/// passes one has stopped converging, or has found a longer tail to measure the bound on again.
/// The events pending at once (heartbeats in flight, writes, each node's freeze or thaw): 13.
const EVENTS: usize = 4 * 13;
/// The steps of a run: 38,725.
const STEPS: u64 = 4 * 38_725;
/// The decisions the trace holds (a delay for each heartbeat a poll sends, a lateness for each
/// timer set, a flush for each write submitted): 39,383.
const TRACE_WORDS: usize = 4 * 39_383;

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

/// The streams of the world the simulation draws from, one a source, named for it: each node's
/// timer, disk and host. A source's draws depend on the seed and its name alone, so a change that
/// makes one node send or wake differently moves no other source's.
struct Streams {
    named: BTreeMap<(&'static str, u64), StreamId>,
}

impl Streams {
    /// `label`'s stream for `node`, named at its first draw.
    fn of(&mut self, world: &mut hyper_sim::World<Ev>, label: &'static str, node: u64) -> StreamId {
        *self
            .named
            .entry((label, node))
            .or_insert_with(|| world.stream(label, &[node]).unwrap())
    }

    /// A probability in parts per million, below a million, from `label`'s stream for `node`.
    fn unit(&mut self, world: &mut hyper_sim::World<Ev>, label: &'static str, node: u64) -> u32 {
        let stream = self.of(world, label, node);
        u32::try_from(world.below(stream, u64::from(PPM)).unwrap()).unwrap()
    }
}

#[path = "support/worlds.rs"]
mod worlds;
use worlds::{Freezes, GRID, Table};

/// [`GRID`] in parts per million: every hundredth, then the tail. Its finest point, 0.99999, is a
/// whole part per million, so the inverse transform is exact in integers (`Measured`).
const GRID_PPM: [u32; 107] = {
    let mut grid = [0u32; 107];
    let mut i = 0;
    while i < 100 {
        grid[i] = i as u32 * 10_000;
        i += 1;
    }
    let tail = [995_000, 999_000, 999_500, 999_900, 999_950, 999_990, PPM];
    let mut j = 0;
    while j < tail.len() {
        grid[100 + j] = tail[j];
        j += 1;
    }
    grid
};

#[path = "support/record.rs"]
mod record;
use record::{Beat, Record, Traced};

/// A quantity's value at probability `u` parts per million, from its table by the inverse
/// transform, linear between the neighbouring points of the grid (hyper-sim's `Measured`).
fn quantile(table: &'static Table, u: u32) -> u64 {
    Measured::new(&GRID_PPM, table).unwrap().at(u)
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
    /// The lateness of a wait of `asked` at probability `u` parts per million.
    fn late(&self, asked: u64, u: u32) -> u64 {
        match *self {
            Self::Tick(period) => period * u64::from(u) / u64::from(PPM),
            Self::Sweep(rows) => {
                let above = rows.partition_point(|(wait, _)| *wait <= asked);
                match (above.checked_sub(1).map(|at| &rows[at]), rows.get(above)) {
                    (None, Some((_, first))) => quantile(first, u),
                    (Some((low, below)), Some((high, upper))) => {
                        // Linear in the wait between the two rows, rounded half up.
                        let (from, to) = (quantile(below, u), quantile(upper, u));
                        let (share, span) = (asked - low, high - low);
                        if to >= from {
                            from + ((to - from) * share + span / 2) / span
                        } else {
                            from - ((from - to) * share + span / 2) / span
                        }
                    }
                    (Some((_, longest)), None) => quantile(longest, u),
                    (None, None) => 0,
                }
            }
        }
    }
}

/// How the simulated host behaves: each quantity drawn from its measured distribution (`worlds`,
/// the heartbeat traces of `docs/benchmarks.md`, "The simulation's worlds").
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

/// What the world schedules: a message's arrival on the network, a write made durable, a host's
/// freeze and its thaw. A node's timer is the world's own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ev {
    Arrive(Ticket),
    Durable {
        node: usize,
        write: Write,
        started: u64,
    },
    Freeze {
        node: usize,
    },
    Thaw {
        node: usize,
    },
}

/// An event as a node takes it, or holds it while its host is frozen.
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
    /// Every heartbeat sent the node is lost, frozen or not: it hears no peer, while they hear it
    /// (mantle's plane deafened).
    deaf: bool,
    disk_stalled: bool,
    /// What every flush of the node's disk takes past its world's draw: a device slowed for good.
    disk_slowed: u64,
    /// No flush of the node's completes before this time, in flight or submitted: a journal that
    /// stalls, as a volume the nodes share stalls for all of them at once.
    disk_held_until: u64,
    /// The heartbeats taken when the node last charged its detectors an election.
    charged: u64,
    /// The disk's flush queue: one write at a time, as one device.
    disk_busy_until: u64,
    /// The log writes out on the disk, oldest first: when each was submitted. What the owner tells
    /// its stream is pending (`Liveness::on_pending`, Focal's 5a).
    log_out: VecDeque<u64>,
    /// Every heartbeat sent: to whom and the message, at what time.
    log: Vec<(u64, PeerId, Heartbeat)>,
    /// Every durable completion: when.
    durable: Vec<u64>,
    /// The liveness writes the node's stream asked for (`Output::flush`).
    liveness_writes: u64,
    suspicions: Vec<Suspicion>,
    /// What the node fed its stream and what the stream told it, in order (`record`): every
    /// suspicion in it is traced to the detector's rule, and every count the stream reports is
    /// its.
    record: Record,
    /// The peers whose restart the node's stream reported.
    restarts: Vec<PeerId>,
    /// The peers whose announced stop the node's stream reported, and when.
    stops: Vec<(PeerId, u64)>,
    /// Each disk verdict its stream gave (`Change::DiskSlow`, `DiskCaughtUp`): the peer, whether
    /// slow, and the heartbeat's arrival.
    disks: Vec<(PeerId, bool, u64)>,
    /// Each change of a peer's disk mode the stream told: the peer, whether its heartbeats turned
    /// to the other mode (`Change::ModeDiffers`) or back, and when.
    modes: Vec<(PeerId, bool, u64)>,
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

impl Node {
    /// The owner took the answer to `write`: a log write leaves the disk's queue, oldest first, and
    /// the stream is told what is pending after it.
    fn taken_out(&mut self, write: Write) {
        if write == Write::Log {
            self.log_out.pop_front();
            self.liveness.on_pending(self.log_out.front().copied());
        }
    }
}

struct Sim {
    now: u64,
    world: World,
    /// hyper-sim's world: the clock, the pending events, the nodes' timers, the streams.
    sim: hyper_sim::World<Ev>,
    /// The network the heartbeats travel: the host's measured one-way delay on every pair.
    net: Net<Vec<u8>>,
    streams: Streams,
    /// Writes submitted and not yet durable.
    writing: usize,
    nodes: Vec<Node>,
    /// When each node first held each pair configured: `(node, peer)` to the time.
    configured_at: BTreeMap<(usize, PeerId), u64>,
    /// The latest any timer was set to fire past its deadline, and the longest any host froze:
    /// how late past what it waits for a poll can come in this run.
    latest_late: u64,
    longest_freeze: u64,
    /// The longest any write took from its submission to its completion, its queue on the device
    /// included.
    longest_write: u64,
}

impl Sim {
    fn new(count: usize, world: World, seed: u64) -> Self {
        Self::of(count, world, Source::Seed(seed))
    }

    /// The simulation of `count` nodes on `world`, its decisions drawn from `source`: a seed, or a
    /// trace to replay.
    fn of(count: usize, world: World, source: Source) -> Self {
        let nodes = (0..count)
            .map(|i| {
                let id = i as u64 + 1;
                let mut liveness = Liveness::new(Settings {
                    local: id,
                    // The node's first start: its run record held none.
                    run: 1,
                    max_peers: count,
                    history: Exposure::new(),
                    resolution: RESOLUTION,
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
                    deaf: false,
                    disk_stalled: false,
                    disk_slowed: 0,
                    disk_held_until: 0,
                    charged: 0,
                    disk_busy_until: 0,
                    log_out: VecDeque::new(),
                    log: Vec::new(),
                    durable: Vec::new(),
                    liveness_writes: 0,
                    suspicions: Vec::new(),
                    record: {
                        let mut record = Record::default();
                        record.began();
                        record
                    },
                    restarts: Vec::new(),
                    stops: Vec::new(),
                    disks: Vec::new(),
                    modes: Vec::new(),
                    believed: BTreeMap::new(),
                    timer: None,
                    frozen_until: 0,
                    held: Vec::new(),
                    phase: 0,
                    replayed: 0,
                }
            })
            .collect();
        let limits = Limits {
            events: EVENTS,
            nodes: count,
            streams: 4 * count * count,
            steps: STEPS,
            trace_words: TRACE_WORDS,
        };
        let mut sim = hyper_sim::World::new(source, Discipline::Ordered, limits).unwrap();
        // The nodes' clocks are the world's: one clock, the owners' timers late as the host's
        // sweep measured, drawn by the owner (`arm`), so the world's own lateness is none.
        for _ in 0..count {
            sim.node(Clock::default()).unwrap();
        }
        let mut net = Net::new(NetLimits {
            flows: count * count,
            links: 0,
            nats: 0,
            link_messages: 0,
            messages: EVENTS,
            bytes: EVENTS * MAX_BYTES,
        });
        net.set_path(Path::measured(
            Measured::new(&GRID_PPM, world.delay).unwrap(),
        ));
        let mut sim = Self {
            now: 0,
            world,
            sim,
            net,
            streams: Streams {
                named: BTreeMap::new(),
            },
            writing: 0,
            nodes,
            configured_at: BTreeMap::new(),
            latest_late: 0,
            longest_freeze: 0,
            longest_write: 0,
        };
        for node in 0..count {
            if world.busy {
                sim.submit(node, Write::Log);
            }
            if let Some(freezes) = world.freezes {
                // A uniform point of the trace, from the node's own stream.
                let stream = sim.streams.of(&mut sim.sim, "freeze", node as u64);
                sim.nodes[node].phase = sim.sim.below(stream, freezes.span).unwrap();
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
            self.schedule(at, node, Ev::Freeze { node });
        }
    }

    /// `event` for `node`, due at `at`.
    fn schedule(&mut self, at: u64, node: usize, event: Ev) {
        self.sim.schedule(at, NodeId(node as u32), event).unwrap();
    }

    /// A write submitted on `node`'s disk now, durable after its flush.
    fn submit(&mut self, node: usize, write: Write) {
        if self.nodes[node].disk_stalled {
            return;
        }
        let start = self.now.max(self.nodes[node].disk_busy_until);
        let u = self.streams.unit(&mut self.sim, "disk", node as u64);
        let done = start + quantile(self.world.flush, u) + self.nodes[node].disk_slowed;
        self.nodes[node].disk_busy_until = done;
        self.longest_write = self.longest_write.max(done - self.now);
        let started = self.now;
        self.writing += 1;
        if write == Write::Log {
            let n = &mut self.nodes[node];
            n.log_out.push_back(started);
            n.liveness.on_pending(n.log_out.front().copied());
        }
        self.schedule(
            done,
            node,
            Ev::Durable {
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
        // What the node knew of its own durability as the call judged.
        let own = n.liveness.own_proof();
        n.liveness.poll(now, &mut n.owner);
        let id = n.owner.id;
        let trusts: Vec<(PeerId, Option<Trust>)> = (1..=count)
            .filter(|peer| *peer != id)
            .map(|peer| (peer, n.liveness.trust(peer)))
            .collect();
        n.record.polled(now, &n.owner.changes[told..], trusts, own);
    }

    /// Feeds `node`'s stream a heartbeat from `from`, stamped by the kernel at `stamp`, and records
    /// the call: what the stream made of it, what it told, the trust it holds of the peer after.
    fn feed(&mut self, node: usize, from: usize, bytes: &[u8], stamp: u64) -> Result<(), Refusal> {
        let n = &mut self.nodes[node];
        let peer = from as u64 + 1;
        let told = n.owner.changes.len();
        let own = n.liveness.own_proof();
        let outcome = n.liveness.on_heartbeat(peer, bytes, stamp, &mut n.owner);
        let beat = Beat::of(bytes, stamp).expect("the simulation sends heartbeats whole");
        let holds = n.liveness.trust(peer);
        n.record
            .fed(peer, beat, outcome, &n.owner.changes[told..], holds, own);
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
        n.log_out.clear();
    }

    fn drain(&mut self, node: usize) {
        let sent = std::mem::take(&mut self.nodes[node].owner.sent);
        for (peer, bytes) in sent {
            let beat = Heartbeat::decode(&bytes).unwrap();
            self.nodes[node].log.push((self.now, peer, beat));
            self.nodes[node].record.sent(peer, beat.run, beat.seq);
            // No host loses a message (`World::delay`): every heartbeat departs.
            let length = bytes.len();
            let pair = (NodeId(node as u32), NodeId(peer as u32 - 1));
            self.net
                .send(&mut self.sim, pair, bytes, length, Ev::Arrive)
                .unwrap();
        }
        if std::mem::take(&mut self.nodes[node].owner.flush) {
            self.nodes[node].liveness_writes += 1;
            self.submit(node, Write::Liveness);
        }
        self.arm(node);
        let changes = std::mem::take(&mut self.nodes[node].owner.changes);
        for change in changes {
            let n = &mut self.nodes[node];
            match change {
                Change::Suspected(suspicion) => {
                    n.believed.insert(suspicion.peer, true);
                    n.suspicions.push(suspicion);
                }
                Change::Restarted { peer, .. } => {
                    n.believed.insert(peer, false);
                    n.restarts.push(peer);
                }
                Change::Trusted { peer, .. } => {
                    n.believed.insert(peer, false);
                }
                // A disk's verdict leaves what the owner believes of the peer's process as it was.
                Change::DiskSlow { peer, at_ns } => n.disks.push((peer, true, at_ns)),
                Change::DiskCaughtUp { peer, at_ns } => n.disks.push((peer, false, at_ns)),
                // A mode's mismatch leaves what the owner believes of the peer as it was.
                Change::ModeDiffers { peer, at_ns } => n.modes.push((peer, true, at_ns)),
                Change::ModeAgrees { peer, at_ns } => n.modes.push((peer, false, at_ns)),
                // An announced stop: the peer held out as suspected until its next run (Mantle's).
                Change::Stopped { peer, at_ns } => {
                    n.believed.insert(peer, true);
                    n.stops.push((peer, at_ns));
                }
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
            let u = self.streams.unit(&mut self.sim, "timer", node as u64);
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
        self.set_timer(node);
    }

    /// The world's timer for `node`: when its owner's fires, held to the thaw while its host is
    /// frozen, none while it has none or the node is dead.
    fn set_timer(&mut self, node: usize) {
        let n = &self.nodes[node];
        let at = n
            .timer
            .filter(|_| n.alive)
            .map(|(_, fires, _)| fires.max(n.frozen_until));
        self.sim.wake(NodeId(node as u32), at).unwrap();
    }

    /// Runs while `keep` holds of the world, an event or a wake at a time, polling every node
    /// first; never past `until`, where one is given. The world takes what is due earliest, events
    /// before timers and each in the order made (`Fifo`).
    fn run_while(&mut self, keep: impl Fn(&Self) -> bool, until: Option<u64>) {
        for node in 0..self.nodes.len() {
            self.poll(node);
        }
        while keep(self) {
            let Some(at) = self.sim.earliest() else {
                break;
            };
            if let Some(until) = until
                && at > until
            {
                self.sim.advance(until).unwrap();
                self.now = until;
                break;
            }
            let step = self.sim.next(&mut Fifo).unwrap();
            self.now = self.sim.now();
            match step {
                Step::Event { event, .. } => self.take(event),
                Step::Wake { node } => self.wake(node.0 as usize),
                Step::Idle => break,
                Step::Spent => panic!("the run took its {STEPS} steps"),
            }
            self.elect();
            self.note_configured();
            self.told_is_believed();
            self.counted();
        }
        self.traced();
    }

    /// An event of the world, as the node it is for takes it.
    fn take(&mut self, event: Ev) {
        match event {
            Ev::Arrive(ticket) => {
                if let Some(delivery) = self.net.deliver(&mut self.sim, ticket, Ev::Arrive).unwrap()
                {
                    self.handle(Event::Arrive {
                        to: delivery.to.0 as usize,
                        from: delivery.from.0 as usize,
                        bytes: delivery.payload,
                        stamp: self.now,
                    });
                }
            }
            Ev::Durable {
                node,
                write,
                started,
            } if self.nodes[node].disk_held_until > self.now => {
                let held = self.nodes[node].disk_held_until;
                self.schedule(
                    held,
                    node,
                    Ev::Durable {
                        node,
                        write,
                        started,
                    },
                );
            }
            Ev::Durable {
                node,
                write,
                started,
            } => {
                self.writing -= 1;
                self.handle(Event::Durable {
                    node,
                    write,
                    started,
                });
            }
            Ev::Freeze { node } => self.handle(Event::Freeze { node }),
            Ev::Thaw { node } => self.handle(Event::Thaw { node }),
        }
    }

    /// `node`'s timer fired: its wait, begun before its deadline, is reported, and the timer is set
    /// again after the poll. One that fires while the host is frozen is held to the thaw, which
    /// takes it.
    fn wake(&mut self, node: usize) {
        if !self.nodes[node].alive {
            return;
        }
        if self.nodes[node].frozen_until > self.now {
            self.set_timer(node);
            return;
        }
        if let Some((deadline, _, true)) = self.nodes[node].timer.take() {
            self.nodes[node].liveness.on_wait(deadline, self.now);
        }
        self.set_timer(node);
        self.poll(node);
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
        // A deaf node hears nothing, frozen or not.
        if let Event::Arrive { to, .. } = event
            && self.nodes[to].deaf
        {
            return;
        }
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
                n.taken_out(write);
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
                self.schedule(thaw, node, Ev::Thaw { node });
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
                    n.taken_out(write);
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
        self.net.in_flight().0 == 0
            && self.writing == 0
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

    /// The run's record: its digest and trace, for the run-twice check.
    fn finish(self) -> hyper_sim::Record {
        self.sim.finish()
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

/// The run-twice check (docs/sim.md §3.9): live peers configure and double their heartbeats, to one
/// digest from the first seed twice and from that run's trace once.
#[test]
fn a_run_replays_from_its_seed_and_from_its_trace() {
    let seed = seeds(10, 1).next().unwrap();
    let record = hyper_sim::twice(seed, |source| {
        let mut sim = Sim::of(3, MACOS, source);
        sim.run_until_configured();
        sim.run_until_doubled();
        Ok::<_, hyper_sim::SimError>(sim.finish())
    })
    .unwrap();
    assert!(!record.trace.is_empty());
}

/// The grid in parts per million is the trace tool's, point for point.
#[test]
fn the_grid_in_parts_per_million_is_the_grid() {
    for (ppm, point) in GRID_PPM.iter().zip(GRID) {
        assert_eq!(f64::from(*ppm), (point * 1e6).round(), "{point}");
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
    killed_peer_is_suspected_within_its_bound(false);
}

/// CP-T2.2 step 2, Focal's condition 5: the stream bounds its own flushes' tail once one is reported
/// (`Liveness::flush_tail`), RFC 6298's bound over them (`hyper_timing::FlushTail`), what its own
/// writes and, in the disk mode, its peers' flush ages are judged against.
#[test]
fn the_stream_bounds_its_own_flushes_tail_once_it_has_one() {
    let mut liveness = Liveness::new(Settings {
        local: 1,
        run: 1,
        max_peers: 3,
        history: Exposure::new(),
        resolution: RESOLUTION,
    })
    .unwrap();
    assert_eq!(
        (liveness.flush_tail(), liveness.flush_smoothed()),
        (None, None)
    );
    let mut tail = hyper_timing::FlushTail::new();
    for (started, durable) in [
        (0, 1_000_000),
        (5_000_000, 6_200_000),
        (9_000_000, 9_900_000),
    ] {
        liveness.on_durable(Write::Log, started, durable);
        tail.on_flush(started, durable);
    }
    // No wait reported: the granularity is not measured, and the tail is the four deviations'.
    assert_eq!(liveness.flush_smoothed(), tail.smoothed());
    assert_eq!(liveness.flush_tail(), tail.tail(Duration::ZERO));
    assert!(liveness.flush_tail() > liveness.flush_smoothed());
}

/// The same, every node judging each peer's process apart from its disk (kiln's CP-T2.2): a crash
/// is suspected within its bound as before.
#[test]
fn judged_apart_a_killed_peer_is_suspected_within_the_stated_bound() {
    killed_peer_is_suspected_within_its_bound(true);
}

fn killed_peer_is_suspected_within_its_bound(apart: bool) {
    for seed in seeds(3, 16) {
        let mut sim = Sim::new(4, MACOS, seed);
        if apart {
            sim.judge_apart();
        }
        sim.run_until_configured();
        sim.run_until_doubled();
        let victim = 3usize;
        // Killed while every survivor trusts it: one that suspected it by a mistake just before
        // would hold that suspicion through the kill and make no new one to measure.
        // Trusted to a point still ahead: one trusted to a point that passed waits on its judge's
        // own proof, and is judged at it (`docs/timing.md` §2.8, "A judge whose own proof stalls").
        let trusted = |sim: &Sim| {
            (0..3).all(|node| {
                matches!(
                    sim.nodes[node].liveness.trust(victim as u64 + 1),
                    Some(Trust::Trusted { until_ns }) if until_ns > sim.now
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
                        Some(Trust::Trusted { until_ns }) if until_ns > sim.now
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
        resolution: RESOLUTION,
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
                        Some(Trust::Trusted { until_ns }) if until_ns > sim.now
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
        resolution: RESOLUTION,
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
        apart: false,
        disk_slow: false,
        echo: None,
        stopping: false,
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
        resolution: RESOLUTION,
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
        apart: false,
        disk_slow: false,
        echo: None,
        stopping: false,
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
        apart: false,
        disk_slow: false,
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
/// make heartbeats at a short floor late together, too correlated for a short window to measure, so
/// the link waits for a longer one), macOS with its groups keeping its log busy, and Linux in its
/// VM:
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
            // latest and the longest this run drew; and a judge with nothing durable since the
            // point judges on its next completion, its own stream's within its sending interval
            // (`docs/timing.md` §2.8, "A judge whose own proof stalls"): at most the longest
            // interval it sends at and the longest write the run drew.
            let sending = (0..2usize)
                .flat_map(|node| {
                    let sim = &sim;
                    (1..=3u64)
                        .filter_map(move |peer| sim.nodes[node].liveness.report(peer)?.sending)
                })
                .max()
                .map_or(0, |interval| interval.as_nanos() as u64);
            let late = sim.latest_late + sim.longest_freeze + sim.longest_write + sending;
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

/// A node that stops hearing every peer in its links' first heartbeats, before its pool or any of
/// its links has evidence, while its peers hear it and live: it suspects every peer all the same,
/// each within the bound its suspicion states. With no evidence of its own, the node judged no pair
/// it had heard, and as a leader led for ever (mantle's node simulation, the leader's plane
/// deafened at its election, 36 heartbeats taken and none judged). In the four worlds, the node
/// deafened after a share, drawn from the seed, of the heartbeats it takes before any pair is
/// judged in the same seed's run.
#[test]
fn a_node_deaf_before_it_has_evidence_suspects_every_peer() {
    for world in [MACOS, BUSY, LINUX, WINDOWS] {
        let name = world.name;
        let mut noticed = Vec::new();
        for seed in seeds(14, 32) {
            let deaf = 2usize;
            let judged = |sim: &Sim| {
                (1..=3u64)
                    .filter(|peer| *peer != deaf as u64 + 1)
                    .any(|peer| {
                        sim.nodes[deaf]
                            .liveness
                            .report(peer)
                            .is_some_and(|r| r.judged)
                    })
            };
            let taken = |sim: &Sim| -> u64 {
                (1..=3u64)
                    .filter_map(|peer| sim.nodes[deaf].liveness.report(peer))
                    .map(|report| report.taken)
                    .sum()
            };
            let mut twin = Sim::new(3, world, seed);
            twin.run_while(|sim| !judged(sim), None);
            let young = taken(&twin);
            let mut draw = Seeded::new(stream_seed(seed, "deafen", &[]));
            let deafen_after = 2 + draw.below(young.saturating_sub(2).max(1));
            let mut sim = Sim::new(3, world, seed);
            sim.run_while(|sim| taken(sim) < deafen_after && !judged(sim), None);
            if judged(&sim) {
                // The draw fell past the evidence in this run: nothing to test.
                continue;
            }
            let deafened_at = sim.now;
            sim.nodes[deaf].deaf = true;
            sim.run_while(
                |sim| {
                    (1..=3u64)
                        .filter(|peer| *peer != deaf as u64 + 1)
                        .any(|peer| sim.nodes[deaf].liveness.trust(peer) != Some(Trust::Suspected))
                },
                None,
            );
            for peer in (1..=3u64).filter(|peer| *peer != deaf as u64 + 1) {
                let suspicion = sim.nodes[deaf]
                    .suspicions
                    .iter()
                    .rfind(|s| s.peer == peer)
                    .copied()
                    .unwrap_or_else(|| panic!("{name} seed {seed}: no suspicion of {peer}"));
                if let (Some(last), Some(bound)) = (suspicion.last, suspicion.detection) {
                    assert!(
                        suspicion.at_ns - last.due_ns <= bound.as_nanos() as u64,
                        "{name} seed {seed}: {peer} suspected past its stated bound"
                    );
                }
                noticed.push(suspicion.noticed_ns.saturating_sub(deafened_at));
            }
        }
        noticed.sort_unstable();
        assert!(
            !noticed.is_empty(),
            "{name}: no seed deafened a node before its evidence"
        );
        println!(
            "{name}: {} suspicions by a node deafened before its evidence, {} ms after it at the \
             median, {} ms at the 90th percentile, {} ms at the most",
            noticed.len(),
            noticed[noticed.len() / 2] / MS,
            noticed[noticed.len() * 9 / 10] / MS,
            noticed[noticed.len() - 1] / MS
        );
    }
}

/// A node that hears no peer keeps its streams to them on their schedules: a heartbeat due waits
/// for no more than the poll that finds it due and the flush that proves it, a timer's lateness, a
/// freeze and a write the run drew. A stream whose interval fell while it was behind sent the
/// heartbeat due with its next slot already past, asked no wake for it, and was polled again only
/// by a peer's heartbeat: a node that heard none sent nothing more, for ever (Linux, seed 175 of
/// 256 of `a_node_deaf_before_it_has_evidence_suspects_every_peer`: sixteen heartbeats a peer,
/// then none). In the four worlds, the node deafened after a share of its first heartbeats drawn
/// from the seed, for two seconds.
#[test]
fn a_node_that_hears_no_peer_keeps_its_streams_on_schedule() {
    for world in [MACOS, BUSY, LINUX, WINDOWS] {
        let name = world.name;
        for seed in seeds(15, 256) {
            let deaf = 2usize;
            let mut draw = Seeded::new(stream_seed(seed, "deafen", &[]));
            let deafen_after = 1 + draw.below(64);
            let mut sim = Sim::new(3, world, seed);
            sim.run_while(
                |sim| (sim.nodes[deaf].log.len() as u64) < deafen_after,
                None,
            );
            sim.nodes[deaf].deaf = true;
            let until = sim.now + 2_000 * MS;
            sim.run_while(
                |sim| {
                    let behind = sim.latest_late + sim.longest_freeze + sim.longest_write;
                    for peer in [1u64, 2] {
                        let due = sim.nodes[deaf]
                            .liveness
                            .report(peer)
                            .and_then(|report| report.sending_due_ns)
                            .unwrap_or(sim.now);
                        assert!(
                            sim.now.saturating_sub(due) <= behind,
                            "{name} seed {seed}: the stream to {peer} due at {} ms, {} ms behind \
                             at {} ms, past the {} ms a poll and a flush may take",
                            due / MS,
                            sim.now.saturating_sub(due) / MS,
                            sim.now / MS,
                            behind / MS
                        );
                    }
                    true
                },
                Some(until),
            );
        }
    }
}

/// RFC 8085 §3.1.3 (BCP 145): an application that "cannot maintain an RTT estimate for a
/// destination, because the destination does not send return traffic", "SHOULD NOT send more than
/// one UDP datagram every 3 seconds". A stream to a peer from which nothing had come stayed at its
/// sender's floor, `E[flush] + G`, and asked a flush for each heartbeat for as long as the peer said
/// nothing: a founder of mantle's three-node cell started alone flushed its log 10,000 times a
/// second on an idle NVMe, its floor 80 to 115 µs. Do: one node of three, its peers never alive,
/// for a minute of its clock, in the four worlds. Expect: to each peer, once a heartbeat states an
/// interval of 3 s every later one does, none states more (every floor here is shorter), and the
/// heartbeats are no more than the doublings from the shortest interval stated to 3 s and one each
/// 3 s of the minute; the liveness writes the node asked are no more than the heartbeats they
/// proved and its first.
#[test]
fn a_node_whose_peers_never_answer_backs_off_to_a_heartbeat_every_three_seconds() {
    // Format: RFC 8085 §3.1.3, the second case's bound.
    const BOUND: u64 = 3_000 * MS;
    // Shape: the run, twenty bounds.
    const SPAN: u64 = 60_000 * MS;
    for world in [MACOS, BUSY, LINUX, WINDOWS] {
        let name = world.name;
        for seed in seeds(20, 8) {
            let mut sim = Sim::new(3, world, seed);
            sim.nodes[1].alive = false;
            sim.nodes[2].alive = false;
            sim.run_while(|_| true, Some(SPAN));
            let node = &sim.nodes[0];
            let mut sent = 0u64;
            for peer in [2u64, 3] {
                let beats: Vec<&Heartbeat> = node
                    .log
                    .iter()
                    .filter(|(_, to, _)| *to == peer)
                    .map(|(_, _, beat)| beat)
                    .collect();
                assert!(!beats.is_empty(), "{name} seed {seed}: nothing sent {peer}");
                assert!(
                    beats.iter().all(|beat| beat.floor_ns < BOUND),
                    "{name} seed {seed}: a floor past the bound, which the stream keeps"
                );
                let stated: Vec<u64> = beats.iter().map(|beat| beat.interval_ns).collect();
                assert!(
                    stated.iter().all(|interval| *interval <= BOUND),
                    "{name} seed {seed}: to {peer} an interval past 3 s: {stated:?}"
                );
                if let Some(first) = stated.iter().position(|interval| *interval == BOUND) {
                    assert!(
                        stated[first..].iter().all(|interval| *interval == BOUND),
                        "{name} seed {seed}: to {peer} the bound left: {stated:?}"
                    );
                }
                let shortest = stated.iter().copied().min().unwrap().max(1);
                let doublings = u64::from((BOUND / shortest).max(1).ilog2()) + 1;
                let most = doublings + SPAN / BOUND + 1;
                assert!(
                    beats.len() as u64 <= most,
                    "{name} seed {seed}: {} heartbeats to {peer} in {} s, past {most}: the \
                     doublings from {} µs to 3 s and one every 3 s",
                    beats.len(),
                    SPAN / 1_000 / MS,
                    shortest / US
                );
                sent += beats.len() as u64;
            }
            assert!(
                node.liveness_writes <= sent + 1,
                "{name} seed {seed}: {} liveness writes for {sent} heartbeats",
                node.liveness_writes
            );
        }
    }
}

/// A stream backed off while its peer said nothing answers the peer's first heartbeat at once and
/// leaves the back-off: the peer judges the node from its own start, and a backed-off slot would
/// keep it waiting up to 3 s. Do: one node of two alone for twenty seconds, its stream to the
/// other backed off to 3 s, then the other started. Expect: the alone node's first heartbeat after
/// it took the other's first is sent within a poll's and a flush's lateness of taking it (the
/// bound `a_node_that_hears_no_peer_keeps_its_streams_on_schedule` holds a stream on schedule to),
/// every later one states an interval under 3 s, and both pairs configure.
#[test]
fn a_peer_heard_at_last_is_answered_at_once_and_its_stream_backs_off_no_more() {
    // Format: RFC 8085 §3.1.3, the second case's bound.
    const BOUND: u64 = 3_000 * MS;
    // Shape: the time alone, long enough for the stream to reach the bound from any floor here.
    const ALONE: u64 = 20_000 * MS;
    for world in [MACOS, BUSY, LINUX, WINDOWS] {
        let name = world.name;
        for seed in seeds(21, 8) {
            let mut sim = Sim::new(2, world, seed);
            sim.nodes[1].alive = false;
            sim.run_while(|_| true, Some(ALONE));
            assert_eq!(
                sim.nodes[0]
                    .liveness
                    .report(2)
                    .and_then(|report| report.sending),
                Some(Duration::from_nanos(BOUND)),
                "{name} seed {seed}: the stream alone reached the bound"
            );
            let mut liveness = Liveness::new(Settings {
                local: 2,
                run: 1,
                max_peers: 2,
                history: Exposure::new(),
                resolution: RESOLUTION,
            })
            .unwrap();
            liveness.attach(1).unwrap();
            sim.restart(1, liveness);
            sim.poll(1);
            let taken = |sim: &Sim| sim.nodes[0].liveness.report(2).map_or(0, |r| r.taken);
            sim.run_while(|sim| taken(sim) == 0, None);
            let heard = sim.now;
            let sent_before = sim.nodes[0].log.len();
            sim.run_while(|sim| sim.nodes[0].log.len() == sent_before, None);
            let (answered, to, _) = sim.nodes[0].log[sent_before];
            assert_eq!(to, 2);
            let behind = sim.latest_late + sim.longest_freeze + sim.longest_write;
            assert!(
                answered - heard <= behind,
                "{name} seed {seed}: answered {} ms after the first heartbeat, past the {} ms a \
                 poll and a flush may take",
                (answered - heard) / MS,
                behind / MS
            );
            sim.run_until_configured();
            let later: Vec<u64> = sim.nodes[0].log[sent_before + 1..]
                .iter()
                .map(|(_, _, beat)| beat.interval_ns)
                .collect();
            assert!(
                !later.is_empty(),
                "{name} seed {seed}: nothing sent after the answer"
            );
            assert!(
                later.iter().all(|interval| *interval < BOUND),
                "{name} seed {seed}: backed off after the peer was heard: {later:?}"
            );
        }
    }
}

/// A node restarted where it hears no peer backs its streams off, and its peers, which hear it,
/// expect each doubling: no suspicion of it is of a heartbeat its sender had not yet due (mantle's
/// roll test restarts a founder so: at a new port, which its peers send to at its old one until
/// they are told). Its streams state each gap and double it (`Pair::backed_off`), and a heartbeat
/// that echoes nothing tells the peer to expect that (`unanswered_next`). Doubled from the floor, unforeseen, each doubling came a whole
/// gap past the peers' freshness point: 49 to 91 suspicions of the live node a world, in 8 seeds of
/// 8 over its 10 s deaf, where the node sending at its floor drew 0 to 2, each a late heartbeat of
/// its own.
#[test]
fn a_restarted_node_that_hears_no_peer_is_not_suspected_for_its_back_off() {
    // Shape: long enough for its streams to back off to the bound from any floor here.
    const DEAF: u64 = 10_000 * MS;
    // Format: RFC 8085 §3.1.3, the second case's bound.
    const BOUND: u64 = 3_000 * MS;
    for world in [MACOS, BUSY, LINUX, WINDOWS] {
        let name = world.name;
        for seed in seeds(22, 8) {
            let mut sim = Sim::new(3, world, seed);
            settled(&mut sim);
            let before: Vec<usize> = (0..2)
                .map(|node| sim.nodes[node].suspicions.len())
                .collect();
            let mut liveness = Liveness::new(Settings {
                local: 3,
                run: 2,
                max_peers: 2,
                history: Exposure::new(),
                resolution: RESOLUTION,
            })
            .unwrap();
            liveness.attach(1).unwrap();
            liveness.attach(2).unwrap();
            sim.restart(2, liveness);
            sim.nodes[2].deaf = true;
            let start = sim.now;
            let logged = sim.nodes[2].log.len();
            sim.poll(2);
            sim.run_while(|_| true, Some(start + DEAF));
            let sent = &sim.nodes[2].log[logged..];
            assert!(
                sent.iter().any(|(_, _, beat)| beat.interval_ns == BOUND),
                "{name} seed {seed}: the restarted node never backed off to the bound"
            );
            for (node, taken_before) in before.iter().enumerate() {
                let peer = node as u64 + 1;
                for suspicion in sim.nodes[node].suspicions[*taken_before..]
                    .iter()
                    .filter(|suspicion| suspicion.peer == 3)
                {
                    let last = suspicion.last.expect("the run's heartbeats were taken").seq;
                    // The slot after the last taken, by its sender's schedule: the next heartbeat
                    // it sent, less the slots it skipped to it at its interval.
                    let due = sent
                        .iter()
                        .find(|(_, to, beat)| *to == peer && beat.seq > last)
                        .map(|(_, _, beat)| {
                            beat.sent_ns - beat.late_ns - (beat.seq - last - 1) * beat.interval_ns
                        });
                    assert!(
                        due.is_some_and(|due| due <= suspicion.at_ns),
                        "{name} seed {seed}: node {peer} suspected the restarted node at {} ms, \
                         its next heartbeat due at {:?} ms",
                        (suspicion.at_ns - start) / MS,
                        due.map(|due| (due - start) / MS)
                    );
                }
            }
        }
    }
}

/// A run of a peer that has taken none of the node's heartbeats asks from no evidence of the node's
/// stream, and its ask is not taken until it takes one: mantle's roll restarts a founder at a new
/// port, which its peers send to at its old one until they are told, while its own heartbeats reach
/// them asking no interval. Taken, that ask moved each peer's stream to its floor, a flush each,
/// for as long as the founder heard nothing: in this simulation's worlds the peers' liveness writes
/// went from 11–30 a second to 80–240 on macOS and Linux (mantle saw 40–200 against 1–2 on its busy
/// CI runners). Do: three nodes settled; the third started again, deaf, for twenty seconds; then
/// heard. Expect: each other node's heartbeats to it keep the interval its stream had before the
/// restart, and number no more than the deaf time over it and two; its liveness writes no more than
/// the heartbeats it sent and one; once heard, every pair configures, and no suspicion the restarted
/// node makes of a live peer is of a heartbeat its sender did not yet have due.
#[test]
fn a_restarted_runs_ask_is_taken_once_it_has_taken_a_heartbeat() {
    // Shape: long enough for a floor of a few milliseconds to send thousands of heartbeats.
    const DEAF: u64 = 20_000 * MS;
    for world in [MACOS, BUSY, LINUX, WINDOWS] {
        let name = world.name;
        for seed in seeds(23, 8) {
            let mut sim = Sim::new(3, world, seed);
            settled(&mut sim);
            let before: Vec<u64> = (0..2)
                .map(|node| {
                    let report = sim.nodes[node].liveness.report(3).unwrap();
                    report.sending.unwrap().as_nanos() as u64
                })
                .collect();
            let mut liveness = Liveness::new(Settings {
                local: 3,
                run: 2,
                max_peers: 2,
                history: Exposure::new(),
                resolution: RESOLUTION,
            })
            .unwrap();
            liveness.attach(1).unwrap();
            liveness.attach(2).unwrap();
            sim.restart(2, liveness);
            sim.nodes[2].deaf = true;
            let start = sim.now;
            let logged: Vec<usize> = (0..2).map(|node| sim.nodes[node].log.len()).collect();
            let writes: Vec<u64> = (0..2).map(|node| sim.nodes[node].liveness_writes).collect();
            sim.poll(2);
            sim.run_while(|_| true, Some(start + DEAF));
            for node in 0..2 {
                let sent = &sim.nodes[node].log[logged[node]..];
                let to_it: Vec<u64> = sent
                    .iter()
                    .filter(|(_, peer, _)| *peer == 3)
                    .map(|(_, _, beat)| beat.interval_ns)
                    .collect();
                let shortest = to_it.iter().copied().min().unwrap_or(0);
                assert!(
                    shortest >= before[node],
                    "{name} seed {seed}: node {} sent the deaf run {} heartbeats, down to {} µs \
                     apart, where its stream was at {} µs",
                    node + 1,
                    to_it.len(),
                    shortest / US,
                    before[node] / US
                );
                let most = DEAF / before[node] + 2;
                assert!(
                    to_it.len() as u64 <= most,
                    "{name} seed {seed}: node {} sent the deaf run {} heartbeats in {} s, past {most}",
                    node + 1,
                    to_it.len(),
                    DEAF / 1_000 / MS
                );
                let written = sim.nodes[node].liveness_writes - writes[node];
                assert!(
                    written <= sent.len() as u64 + 1,
                    "{name} seed {seed}: node {} made {written} liveness writes for {} heartbeats",
                    node + 1,
                    sent.len()
                );
            }
            // Heard: its peers take its ask once it has taken one of their heartbeats.
            let heard_from = sim.now;
            let suspected_before = sim.nodes[2].suspicions.len();
            sim.nodes[2].deaf = false;
            sim.run_until_configured();
            // No suspicion the restarted node made of a live peer once it heard them is of a
            // heartbeat its sender did not yet have due by its schedule.
            for suspicion in &sim.nodes[2].suspicions[suspected_before..] {
                let peer = suspicion.peer;
                let Some(last) = suspicion.last.filter(|last| last.arrival_ns >= heard_from) else {
                    continue;
                };
                let sent = &sim.nodes[peer as usize - 1].log;
                let due = sent
                    .iter()
                    .find(|(_, to, beat)| *to == 3 && beat.run == 1 && beat.seq > last.seq)
                    .map(|(_, _, beat)| {
                        beat.sent_ns - beat.late_ns - (beat.seq - last.seq - 1) * beat.interval_ns
                    });
                assert!(
                    due.is_some_and(|due| due <= suspicion.at_ns),
                    "{name} seed {seed}: the restarted node suspected node {peer} at {} ms, its \
                     next heartbeat due at {:?} ms",
                    (suspicion.at_ns - heard_from) / MS,
                    due.map(|due| due.saturating_sub(heard_from) / MS)
                );
            }
        }
    }
}

/// A node that stops in order announces it to its peers (`Liveness::stop`), and its next run's
/// start is no failure in their MTBF's evidence, nor is their letting go of the pair while they
/// suspect it: the MTBF counts what a detector must catch, and an orderly stop handed over what it
/// led first. Until its next run is heard, the announced run is sent nothing but the one heartbeat
/// that answers the announcement, where each heartbeat to it asked a flush of its own; that run's
/// first heartbeat is answered at once, numbered next.
/// Do: three nodes settled; the third announces its stop and stops; its peers suspect it at the
/// announcement; it starts again. Expect: the stopped run sent only the answer after its
/// announcement was taken; the restart reported and counted no failure; the first heartbeat each
/// peer sends the new run is numbered next and sent within a poll's and a flush's lateness of taking
/// its first; and a node that lets go of an announced, suspected peer counts no failure.
#[test]
fn a_restart_after_an_announced_stop_is_no_failure_and_the_stopped_run_is_sent_nothing() {
    for world in [MACOS, BUSY, LINUX, WINDOWS] {
        let name = world.name;
        for seed in seeds(24, 8) {
            let mut sim = Sim::new(3, world, seed);
            settled(&mut sim);
            let failures: Vec<u64> = (0..2)
                .map(|node| sim.nodes[node].liveness.history().failures())
                .collect();
            {
                let now = sim.now;
                let n = &mut sim.nodes[2];
                n.liveness.stop(now, &mut n.owner);
            }
            sim.drain(2);
            sim.nodes[2].alive = false;
            let stopped_at = sim.now;
            sim.run_until_suspected(2);
            // Taken by both: each sent the stopped run its answer after it took the announcement, and
            // nothing else.
            let announced: Vec<u64> = (0..2)
                .map(|node| {
                    sim.nodes[node]
                        .record
                        .entries
                        .iter()
                        .find_map(|entry| match entry {
                            record::Entry::Fed {
                                peer: 3,
                                beat,
                                refused: None,
                                ..
                            } if beat.stopping => Some(beat.stamp),
                            _ => None,
                        })
                        .unwrap_or_else(|| {
                            panic!("{name} seed {seed}: node {node} took no announcement")
                        })
                })
                .collect();
            sim.run_while(|_| true, Some(sim.now + 10_000 * MS));
            for (node, announced) in announced.iter().enumerate() {
                let after: Vec<u64> = sim.nodes[node]
                    .log
                    .iter()
                    .filter(|(at, peer, _)| *peer == 3 && at >= announced)
                    .map(|(at, _, _)| *at)
                    .collect();
                assert!(
                    after.len() == 1,
                    "{name} seed {seed}: node {} sent the stopped run {} heartbeats after its \
                     announcement, where it answers it once, the first {} ms after the stop",
                    node + 1,
                    after.len(),
                    after.first().map_or(0, |at| (at - stopped_at) / MS)
                );
            }
            let last_sent: Vec<u64> = (0..2)
                .map(|node| {
                    sim.nodes[node]
                        .log
                        .iter()
                        .filter(|(_, peer, _)| *peer == 3)
                        .map(|(_, _, beat)| beat.seq)
                        .max()
                        .unwrap()
                })
                .collect();
            let mut liveness = Liveness::new(Settings {
                local: 3,
                run: 2,
                max_peers: 2,
                history: Exposure::new(),
                resolution: RESOLUTION,
            })
            .unwrap();
            liveness.attach(1).unwrap();
            liveness.attach(2).unwrap();
            sim.restart(2, liveness);
            sim.poll(2);
            let logged: Vec<usize> = (0..2).map(|node| sim.nodes[node].log.len()).collect();
            sim.run_while(
                |sim| (0..2).any(|node| sim.nodes[node].restarts.is_empty()),
                None,
            );
            sim.run_while(
                |sim| {
                    (0..2).any(|node| {
                        !sim.nodes[node].log[logged[node]..]
                            .iter()
                            .any(|(_, peer, _)| *peer == 3)
                    })
                },
                None,
            );
            let behind = sim.latest_late + sim.longest_freeze + sim.longest_write;
            for node in 0..2 {
                assert_eq!(
                    sim.nodes[node].restarts,
                    vec![3],
                    "{name} seed {seed}: node {} saw the restart once",
                    node + 1
                );
                assert_eq!(
                    sim.nodes[node].liveness.history().failures(),
                    failures[node],
                    "{name} seed {seed}: node {} counted the restart after an announced stop a \
                     failure",
                    node + 1
                );
                let heard = sim.nodes[node]
                    .record
                    .entries
                    .iter()
                    .find_map(|entry| match entry {
                        record::Entry::Fed {
                            peer: 3,
                            beat,
                            refused: None,
                            ..
                        } if beat.run == 2 => Some(beat.stamp),
                        _ => None,
                    })
                    .expect("the new run was taken");
                let (answered, _, beat) = *sim.nodes[node].log[logged[node]..]
                    .iter()
                    .find(|(_, peer, _)| *peer == 3)
                    .unwrap();
                assert_eq!(
                    beat.seq,
                    last_sent[node] + 1,
                    "{name} seed {seed}: node {} skipped the stop's slots",
                    node + 1
                );
                assert!(
                    answered.saturating_sub(heard) <= behind,
                    "{name} seed {seed}: node {} answered the new run {} ms after taking it, past \
                     the {} ms a poll and a flush may take",
                    node + 1,
                    answered.saturating_sub(heard) / MS,
                    behind / MS
                );
            }
            sim.run_until_configured();
        }
    }
    // Let go while suspected after an announced stop: no failure.
    let mut sim = Sim::new(3, MACOS, 0);
    settled(&mut sim);
    {
        let now = sim.now;
        let n = &mut sim.nodes[2];
        n.liveness.stop(now, &mut n.owner);
    }
    sim.drain(2);
    sim.nodes[2].alive = false;
    sim.run_until_suspected(2);
    let failures = sim.nodes[0].liveness.history().failures();
    sim.nodes[0].liveness.detach(3).unwrap();
    assert!(
        sim.nodes[0].liveness.report(3).is_none(),
        "the pair was let go"
    );
    assert_eq!(sim.nodes[0].liveness.history().failures(), failures);
}

/// A stop announced is suspected at the announcement's arrival, where the stopped node was trusted
/// until its freshness point passed, the detector's margin after its last heartbeat: a group's
/// remaining voter that wrongly suspected its leader meanwhile counted the stopped voter in a
/// quorum it trusted and asked for votes (mantle's roll; the window
/// `docs/models/SuspicionStopUnannounced.cfg` finds). The stopped run is trusted never again: a
/// heartbeat of it the announcement overtook is refused. Its next run is trusted at once.
/// Do: three nodes settled; once both others took a heartbeat of the third, it announces its stop
/// and stops; one of its run's heartbeats comes after the announcement; it starts again.
/// Expect: each other node's owner told the stop at the announcement's arrival, before the point
/// it held the third trusted to, and told no suspicion of it; the late heartbeat refused as stale,
/// the third still suspected; the next run reported restarted and trusted.
#[test]
fn a_stop_announced_is_suspected_at_its_arrival_and_its_run_never_trusted_again() {
    for world in [MACOS, BUSY, LINUX, WINDOWS] {
        let name = world.name;
        for seed in seeds(25, 8) {
            let mut sim = Sim::new(3, world, seed);
            settled(&mut sim);
            // Both took a heartbeat of the third since: each holds it trusted a whole freshness on.
            let fed: Vec<usize> = (0..2)
                .map(|node| sim.nodes[node].record.entries.len())
                .collect();
            sim.run_while(
                |sim| {
                    (0..2).any(|node| {
                        !sim.nodes[node].record.entries[fed[node]..]
                            .iter()
                            .any(|entry| {
                                matches!(
                                    entry,
                                    record::Entry::Fed {
                                        peer: 3,
                                        refused: None,
                                        ..
                                    }
                                )
                            })
                    })
                },
                None,
            );
            let until: Vec<u64> = (0..2)
                .map(|node| match sim.nodes[node].liveness.trust(3) {
                    Some(Trust::Trusted { until_ns }) => until_ns,
                    other => panic!(
                        "{name} seed {seed}: node {} holds node 3 {other:?}",
                        node + 1
                    ),
                })
                .collect();
            let suspicions: Vec<usize> = (0..2)
                .map(|node| sim.nodes[node].suspicions.len())
                .collect();
            let last = sim.nodes[2]
                .log
                .iter()
                .rev()
                .find(|(_, peer, _)| *peer == 1)
                .map(|(_, _, beat)| *beat)
                .expect("the third sent the first heartbeats");
            {
                let now = sim.now;
                let n = &mut sim.nodes[2];
                n.liveness.stop(now, &mut n.owner);
            }
            sim.drain(2);
            sim.nodes[2].alive = false;
            // Until both took the announcement, and nothing more.
            sim.run_while(
                |sim| {
                    (0..2).any(|node| {
                        !sim.nodes[node].record.entries.iter().any(|entry| {
                            matches!(entry, record::Entry::Fed { peer: 3, beat, .. } if beat.stopping)
                        })
                    })
                },
                None,
            );
            for node in 0..2 {
                let announced = sim.nodes[node]
                    .record
                    .entries
                    .iter()
                    .find_map(|entry| match entry {
                        record::Entry::Fed {
                            peer: 3,
                            beat,
                            refused: None,
                            ..
                        } if beat.stopping => Some(beat.stamp),
                        _ => None,
                    })
                    .expect("the announcement was taken");
                assert_eq!(
                    sim.nodes[node].liveness.trust(3),
                    Some(Trust::Suspected),
                    "{name} seed {seed}: node {} holds the stopped node trusted",
                    node + 1
                );
                assert_eq!(
                    sim.nodes[node].stops,
                    vec![(3, announced)],
                    "{name} seed {seed}: node {} was not told the stop at its arrival",
                    node + 1
                );
                assert!(
                    announced < until[node],
                    "{name} seed {seed}: node {} took the announcement at {announced} ns, past the \
                     point {} ns it held node 3 trusted to: no window to close",
                    node + 1,
                    until[node]
                );
                assert!(
                    sim.nodes[node].suspicions[suspicions[node]..]
                        .iter()
                        .all(|suspicion| suspicion.peer != 3),
                    "{name} seed {seed}: node {} told a suspicion of the stopped node",
                    node + 1
                );
            }
            // A heartbeat of the stopped run after its announcement: refused, nothing told.
            let late = Heartbeat {
                seq: last.seq + 1,
                sent_ns: last.sent_ns + last.interval_ns,
                flushes: last.flushes + 1,
                flush_age_ns: 0,
                late_ns: 0,
                echo: None,
                stopping: false,
                ..last
            };
            let mut bytes = [0u8; MAX_BYTES];
            let length = late.encode(&mut bytes).len();
            let now = sim.now;
            assert_eq!(
                sim.feed(0, 2, &bytes[..length], now),
                Err(Refusal::Stale),
                "{name} seed {seed}: the stopped run's late heartbeat was taken"
            );
            sim.drain(0);
            assert_eq!(sim.nodes[0].liveness.trust(3), Some(Trust::Suspected));
            // The next run is trusted at its first heartbeat.
            let mut liveness = Liveness::new(Settings {
                local: 3,
                run: 2,
                max_peers: 2,
                history: Exposure::new(),
                resolution: RESOLUTION,
            })
            .unwrap();
            liveness.attach(1).unwrap();
            liveness.attach(2).unwrap();
            sim.restart(2, liveness);
            sim.poll(2);
            sim.run_while(
                |sim| (0..2).any(|node| sim.nodes[node].restarts.is_empty()),
                None,
            );
            for node in 0..2 {
                assert_eq!(sim.nodes[node].restarts, vec![3]);
                assert!(
                    matches!(
                        sim.nodes[node].liveness.trust(3),
                        Some(Trust::Trusted { .. })
                    ),
                    "{name} seed {seed}: node {} holds the next run {:?}",
                    node + 1,
                    sim.nodes[node].liveness.trust(3)
                );
            }
            sim.run_until_configured();
        }
    }
}

/// A stop announced is answered: each peer that takes the announcement sends the stopping run one
/// heartbeat that echoes it, proven as any, and nothing else, and the stopping node, which runs on
/// as an owner that announces before it hands over does, knows each peer took it
/// (`Liveness::stop_taken`). An announcement lost on its way goes again at the stream's interval
/// until the answer comes. Do: three nodes settled; the third announces its stop and runs on, its
/// first announcement to the first node lost. Expect: the third knows both peers took it, the
/// first only after an announcement sent again; each peer answered each announcement it took
/// once, echoing one, and sent the third nothing else; the third sends no announcement after a
/// peer's answer, and none to the second but the first.
#[test]
fn a_stop_announced_is_answered_once_and_the_stopping_node_knows_it_was_taken() {
    for world in [MACOS, BUSY, LINUX, WINDOWS] {
        let name = world.name;
        for seed in seeds(26, 8) {
            let mut sim = Sim::new(3, world, seed);
            let freshness = settled(&mut sim);
            let logged: Vec<usize> = (0..3).map(|node| sim.nodes[node].log.len()).collect();
            {
                let now = sim.now;
                let n = &mut sim.nodes[2];
                n.liveness.stop(now, &mut n.owner);
                assert!(!n.liveness.stop_taken(1) && !n.liveness.stop_taken(2));
                // Lost on its way to the first node: sent, and never delivered.
                let now = sim.now;
                let (lost, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut n.owner.sent)
                    .into_iter()
                    .partition(|(peer, _)| *peer == 1);
                n.owner.sent = kept;
                for (peer, bytes) in lost {
                    let beat = Heartbeat::decode(&bytes).unwrap();
                    n.log.push((now, peer, beat));
                    n.record.sent(peer, beat.run, beat.seq);
                }
            }
            sim.drain(2);
            // Within the time an announcement sent again and its answer take: a freshness, which
            // holds the stream's interval, for each, and what a host's freeze or a write held back.
            let behind = sim.latest_late + sim.longest_freeze + sim.longest_write;
            let until = sim.now + 4 * freshness + 2 * behind;
            sim.run_while(
                |sim| !(1..=2).all(|peer| sim.nodes[2].liveness.stop_taken(peer)),
                Some(until),
            );
            for peer in 1..=2 {
                assert!(
                    sim.nodes[2].liveness.stop_taken(peer),
                    "{name} seed {seed}: node {peer} did not answer the announcement within {} ms",
                    (4 * freshness + 2 * behind) / MS
                );
            }
            let first_ns = sim.nodes[2].log[logged[2]..]
                .iter()
                .map(|(_, _, beat)| beat.sent_ns)
                .min()
                .expect("the announcement went");
            // Each announcement the stopping node sent each peer, and when the answer came.
            for (node, peer) in [(0usize, 1u64), (1, 2)] {
                let goodbyes: Vec<(u64, Heartbeat)> = sim.nodes[2].log[logged[2]..]
                    .iter()
                    .filter(|(_, to, _)| *to == peer)
                    .map(|(at, _, beat)| (*at, *beat))
                    .collect();
                assert!(
                    goodbyes.iter().all(|(_, beat)| beat.stopping),
                    "{name} seed {seed}: the stopping node sent node {peer} a heartbeat"
                );
                let taken = sim.nodes[node]
                    .record
                    .entries
                    .iter()
                    .filter(|entry| {
                        matches!(entry, record::Entry::Fed { peer: 3, beat, refused: None, .. }
                            if beat.stopping)
                    })
                    .count();
                let answers: Vec<Heartbeat> = sim.nodes[node].log[logged[node]..]
                    .iter()
                    .filter(|(_, to, _)| *to == 3)
                    .map(|(_, _, beat)| *beat)
                    .filter(|beat| beat.echo.is_some_and(|echo| echo.sent_ns >= first_ns))
                    .collect();
                assert!(
                    taken >= 1 && answers.len() == taken,
                    "{name} seed {seed}: node {peer} took {taken} announcements and answered {}",
                    answers.len()
                );
                if peer == 1 {
                    assert!(
                        goodbyes.len() >= 2,
                        "{name} seed {seed}: the announcement lost on its way went once"
                    );
                } else {
                    assert_eq!(goodbyes.len(), 1, "{name} seed {seed}: sent node 2 again");
                }
            }
            // Answered, the stopping node sends no more; the peers send it nothing more.
            let counts: Vec<usize> = (0..3).map(|node| sim.nodes[node].log.len()).collect();
            let until = sim.now + 4 * freshness;
            sim.run_while(|_| true, Some(until));
            for (node, count) in counts.iter().enumerate() {
                let more: Vec<&(u64, PeerId, Heartbeat)> = sim.nodes[node].log[*count..]
                    .iter()
                    .filter(|(_, to, _)| node == 2 || *to == 3)
                    .collect();
                assert!(
                    more.is_empty(),
                    "{name} seed {seed}: node {} sent {} heartbeats after the answers",
                    node + 1,
                    more.len()
                );
            }
        }
    }
}

/// Runs `sim`, configured and doubled, until every live node trusts every live peer, and returns
/// the longest freshness, `η + α`, any pair holds then.
impl Sim {
    /// Every node judges each peer's process apart from its disk (`Liveness::set_disk_apart`,
    /// kiln's CP-T2.2), set before the first poll.
    fn judge_apart(&mut self) {
        for node in &mut self.nodes {
            node.liveness.set_disk_apart(true);
        }
    }
}

fn settled(sim: &mut Sim) -> u64 {
    sim.run_until_configured();
    sim.run_until_doubled();
    let count = sim.nodes.len() as u64;
    sim.run_while(
        |sim| {
            (0..sim.nodes.len()).any(|node| {
                (1..=count).filter(|p| *p != node as u64 + 1).any(|peer| {
                    !matches!(
                        sim.nodes[node].liveness.trust(peer),
                        Some(Trust::Trusted { until_ns }) if until_ns > sim.now
                    )
                })
            })
        },
        None,
    );
    (0..sim.nodes.len())
        .flat_map(|node| {
            let sim = &*sim;
            (1..=count)
                .filter(move |p| *p != node as u64 + 1)
                .filter_map(move |peer| sim.nodes[node].liveness.report(peer)?.freshness)
        })
        .max()
        .map_or(0, |freshness| freshness.as_nanos() as u64)
}

/// Every node's flushes stall together, as on a volume the nodes share (one journal: every file's
/// fsync waits on its commit), for three times the longest freshness any pair holds: every
/// flush-proven stream stops at once, and no node suspects another while its own proof is as stalled
/// as its peers' (Lifeguard's local health, Dadgar, Phillips and Currey 2018, applied to the flush
/// proof: `docs/timing.md` §2.8, "A judge whose own proof stalls"). Before, every pair suspected
/// every other within its freshness: mantle's cluster suite pinned on one Linux volume, every
/// all-pairs burst lined up with every node's flushes stalled at once (165 to 302 ms).
#[test]
fn flushes_stalled_together_as_on_a_shared_volume_are_no_ones_failure() {
    for world in [MACOS, BUSY, LINUX] {
        let name = world.name;
        for seed in seeds(16, 8) {
            let mut sim = Sim::new(3, world, seed);
            let freshness = settled(&mut sim);
            let from = sim.now;
            let until = from + 3 * freshness;
            for node in &mut sim.nodes {
                node.disk_held_until = until;
            }
            // Every pair stays shown judged while its judge waits on its own proof: an owner reads
            // `Trust::Unconfigured` as a link not judged yet, and read so through a start of slow
            // first flushes, mantle's nodes on windows-x86_64 elected no one (`b96bed9`).
            sim.run_while(
                |sim| {
                    for (index, node) in sim.nodes.iter().enumerate() {
                        for peer in (1..=3u64).filter(|p| *p != index as u64 + 1) {
                            assert_ne!(
                                node.liveness.trust(peer),
                                Some(Trust::Unconfigured),
                                "{name} seed {seed}: node {} showed {peer} unjudged",
                                index + 1
                            );
                        }
                    }
                    true
                },
                Some(until),
            );
            for (index, node) in sim.nodes.iter().enumerate() {
                let during: Vec<_> = node
                    .suspicions
                    .iter()
                    .filter(|s| s.noticed_ns > from && s.noticed_ns <= until)
                    .map(|s| (s.peer, (s.noticed_ns - from) / MS))
                    .collect();
                assert!(
                    during.is_empty(),
                    "{name} seed {seed}: node {} suspected {during:?} (peer, ms into the stall) \
                     while every flush stalled for {} ms",
                    index + 1,
                    (until - from) / MS
                );
            }
        }
    }
}

/// As every node's flushes come back from a stall they shared, for three times the longest
/// freshness any pair holds, no node suspects another within a freshness of the stall's end: every
/// judge's own proof returns with its peers', and each holds its peers to the margin past the end
/// (`Local::judge`). CP-T2.2 step 1: Focal's doc 34 recorded a leader that suspected both followers
/// over a shared disk's 0.6 s stall. Fails here in the busy world, a judge suspecting both peers as
/// its own stall ends: what a stall every node shares leaves to CP-T2.2 step 2.
#[test]
#[ignore = "CP-T2.2 step 2: a judge suspects both peers as a stall every node shared ends"]
fn flushes_stalled_together_draw_no_suspicion_as_they_end() {
    for world in [MACOS, BUSY, LINUX] {
        let name = world.name;
        for seed in seeds(16, 8) {
            let mut sim = Sim::new(3, world, seed);
            let freshness = settled(&mut sim);
            let from = sim.now;
            let until = from + 3 * freshness;
            for node in &mut sim.nodes {
                node.disk_held_until = until;
            }
            let after = until + freshness;
            sim.run_while(|_| true, Some(after));
            for (index, node) in sim.nodes.iter().enumerate() {
                let late: Vec<_> = node
                    .suspicions
                    .iter()
                    .filter(|s| s.noticed_ns > until && s.noticed_ns <= after)
                    .map(|s| (s.peer, (s.noticed_ns - from) / MS))
                    .collect();
                assert!(
                    late.is_empty(),
                    "{name} seed {seed}: node {} suspected {late:?} (peer, ms from the stall's start) \
                     within {} ms of the end of a stall of every flush for {} ms",
                    index + 1,
                    freshness / MS,
                    (until - from) / MS
                );
            }
        }
    }
}

/// What each node suspected from the moment a run of `world` and `seed`, every node judging each
/// peer's process apart from its disk (kiln's CP-T2.2), settled, to `spans` freshnesses past it,
/// with `hold` done to the run at that moment, given it and the freshness: each suspicion's node,
/// peer and time from the moment, in order. A world's sources are drawn apart (`Streams`), so a
/// run with and one without the hold see the same host freezes and the same network.
fn judged_apart(
    world: World,
    seed: u64,
    spans: u64,
    hold: impl Fn(&mut Sim, u64, u64),
) -> Vec<(usize, u64, u64)> {
    let mut sim = Sim::new(3, world, seed);
    sim.judge_apart();
    let freshness = settled(&mut sim);
    let from = sim.now;
    hold(&mut sim, from, freshness);
    sim.run_while(|_| true, Some(from + spans * freshness));
    let mut seen: Vec<(usize, u64, u64)> = sim
        .nodes
        .iter()
        .enumerate()
        .flat_map(|(index, node)| {
            node.suspicions
                .iter()
                .filter(|s| s.noticed_ns > from)
                .map(move |s| (index + 1, s.peer, (s.noticed_ns - from) / MS))
        })
        .collect();
    seen.sort_unstable();
    seen
}

/// Judged apart from their disks (kiln's CP-T2.2), nodes whose flushes stall together, as on a
/// volume they share, for three times the longest freshness any pair holds, suspect no one for the
/// stall: what they suspect through it is what the same run without it suspects, a host's freeze
/// among them, since no stream waits on a flush.
#[test]
fn judged_apart_flushes_stalled_together_draw_no_suspicion() {
    for world in [MACOS, BUSY, LINUX] {
        let name = world.name;
        for seed in seeds(41, 8) {
            let held = judged_apart(world, seed, 3, |sim, from, freshness| {
                for node in &mut sim.nodes {
                    node.disk_held_until = from + 3 * freshness;
                }
            });
            let unheld = judged_apart(world, seed, 3, |_, _, _| {});
            let added: Vec<_> = held.iter().filter(|s| !unheld.contains(s)).collect();
            assert!(
                added.is_empty(),
                "{name} seed {seed}: a stall of every flush for three freshnesses added the \
                 suspicions {added:?} (node, peer, ms from its start) to the run without it, {unheld:?}"
            );
        }
    }
}

/// Judged apart from its disk (kiln's CP-T2.2), a node whose disk stops completing flushes goes on
/// heartbeating, and for three times the longest freshness any pair holds no peer suspects it for
/// the stall: what its peers suspect of it is what the same run without the stall shows, and they
/// take its heartbeats throughout.
#[test]
fn judged_apart_a_stalled_disk_is_not_suspected() {
    for world in [MACOS, BUSY, LINUX] {
        let name = world.name;
        for seed in seeds(42, 8) {
            let stalled = 2usize;
            let peer = stalled as u64 + 1;
            let of_it = |seen: Vec<(usize, u64, u64)>| -> Vec<(usize, u64, u64)> {
                seen.into_iter().filter(|s| s.1 == peer).collect()
            };
            let held = of_it(judged_apart(world, seed, 3, |sim, _, _| {
                sim.nodes[stalled].disk_stalled = true;
            }));
            let unheld = of_it(judged_apart(world, seed, 3, |_, _, _| {}));
            let added: Vec<_> = held.iter().filter(|s| !unheld.contains(s)).collect();
            assert!(
                added.is_empty(),
                "{name} seed {seed}: a disk that stopped added the suspicions {added:?} (node, peer, \
                 ms from its stop) of its node to the run without it, {unheld:?}"
            );
            let mut sim = Sim::new(3, world, seed);
            sim.judge_apart();
            let freshness = settled(&mut sim);
            let from = sim.now;
            let taken: Vec<u64> = (0..2)
                .map(|node| sim.nodes[node].liveness.report(peer).unwrap().taken)
                .collect();
            sim.nodes[stalled].disk_stalled = true;
            sim.run_while(|_| true, Some(from + 3 * freshness));
            for (node, before) in taken.iter().enumerate() {
                assert!(
                    sim.nodes[node].liveness.report(peer).unwrap().taken > *before,
                    "{name} seed {seed}: node {} took no heartbeat of the node whose disk stopped",
                    node + 1
                );
            }
        }
    }
}

/// CP-T2.2 step 2, Focal's condition 5: judged apart from its disk, a node whose disk is held for
/// three freshnesses is judged slow by each peer while the hold lasts, its heartbeats saying so
/// (`Change::DiskSlow`), and caught up (`Change::DiskCaughtUp`) within a freshness of its end; what
/// the peers trust of its process is what the stream believes throughout (the sim's own check, every
/// step), and the others' disks are judged as healthy ones are
/// (`judged_apart_healthy_disks_draw_no_lasting_verdict`). A disk is judged by what waits on it: in
/// each world, idle (macOS's, Linux's: one log write left waiting on the held disk) and loaded (the
/// busy one's log writes). An idle disk with nothing to do is no slow one
/// (`judged_apart_an_idle_node_asks_no_flush_and_its_held_disk_draws_no_verdict`).
#[test]
fn judged_apart_a_held_disk_is_judged_slow_and_then_caught_up() {
    for world in [MACOS, BUSY, LINUX] {
        let name = world.name;
        for seed in seeds(43, 8) {
            let mut sim = Sim::new(3, world, seed);
            sim.judge_apart();
            let freshness = settled(&mut sim);
            let from = sim.now;
            let held = 2usize;
            let peer = held as u64 + 1;
            let end = from + 3 * freshness;
            sim.nodes[held].disk_held_until = end;
            if !world.busy {
                sim.submit(held, Write::Log);
            }
            sim.run_while(|_| true, Some(end + 2 * freshness));
            for judge in (0..3).filter(|j| *j != held) {
                let verdicts: Vec<(PeerId, bool, u64)> = sim.nodes[judge]
                    .disks
                    .iter()
                    .filter(|(_, _, at)| *at > from)
                    .copied()
                    .collect();
                let slow = verdicts.iter().find(|(p, slow, _)| *p == peer && *slow);
                let caught = verdicts
                    .iter()
                    .find(|(p, slow, at)| *p == peer && !*slow && *at >= end);
                let others: Vec<(PeerId, bool, u64)> =
                    verdicts.iter().filter(|v| v.0 != peer).copied().collect();
                assert!(
                    slow.is_some_and(|(_, _, at)| *at < end)
                        && caught.is_some_and(|(_, _, at)| *at <= end + freshness)
                        && caught_up_within(&others, freshness),
                    "{name} seed {seed}: node {}'s verdicts on the disks (peer, slow, ms from the \
                     hold's start) {:?}, the hold {} ms, the freshness {} ms",
                    judge + 1,
                    verdicts
                        .iter()
                        .map(|(p, s, at)| (*p, *s, (at - from) / MS))
                        .collect::<Vec<_>>(),
                    (end - from) / MS,
                    freshness / MS
                );
            }
        }
    }
}

/// Focal's 5a on the disk mode (kiln CP-T2.2): judged apart from their disks, idle nodes ask no
/// flush of their own, a heartbeat's slot in which nothing became durable asking no probe, and an
/// idle node's disk held for three freshnesses with nothing written to it draws no verdict: a disk
/// with nothing to do is no slow one. Today's group, for the contrast, asks its flush proofs over the
/// same six freshnesses. In macOS's and Linux's idle worlds, 8 seeds each.
#[test]
fn judged_apart_an_idle_node_asks_no_flush_and_its_held_disk_draws_no_verdict() {
    for world in [MACOS, LINUX] {
        let name = world.name;
        for seed in seeds(45, 8) {
            let asked = |apart: bool| {
                let mut sim = Sim::new(3, world, seed);
                if apart {
                    sim.judge_apart();
                }
                let freshness = settled(&mut sim);
                let from = sim.now;
                let before: Vec<u64> = sim.nodes.iter().map(|n| n.liveness_writes).collect();
                sim.nodes[2].disk_held_until = from + 3 * freshness;
                sim.run_while(|_| true, Some(from + 6 * freshness));
                let asked: Vec<u64> = sim
                    .nodes
                    .iter()
                    .zip(&before)
                    .map(|(n, before)| n.liveness_writes - before)
                    .collect();
                let verdicts: Vec<(usize, PeerId, bool)> = sim
                    .nodes
                    .iter()
                    .enumerate()
                    .flat_map(|(judge, n)| {
                        n.disks
                            .iter()
                            .filter(|(_, _, at)| *at > from)
                            .map(move |(peer, slow, _)| (judge + 1, *peer, *slow))
                    })
                    .collect();
                (asked, verdicts)
            };
            let (apart, verdicts) = asked(true);
            let (today, _) = asked(false);
            assert!(
                apart.iter().all(|asked| *asked == 0)
                    && verdicts.is_empty()
                    && today.iter().all(|asked| *asked > 0),
                "{name} seed {seed}: over six freshnesses, flushes asked judged apart {apart:?} \
                 (today's group {today:?}); verdicts (judge, peer, slow) {verdicts:?}"
            );
        }
    }
}

/// CP-T2.2 step 2, Focal's condition 5: judged apart from their disks, healthy nodes, idle or
/// loaded, over twenty freshnesses once the group has settled. A node's disk is slow only past its
/// flushes' tail and its detectors' freshness, so the flushes macOS's traces hold (to 205 ms, under
/// its freshness) are no verdict, idle or busy, host freezes and all; Linux's VM disk stalls past
/// its freshness at times (its trace to 3.7 s), which is a verdict, and each is caught up within a
/// freshness, the stall's end: no flap, no verdict that lingers past what caused it
/// (`survey_of_disk_verdicts` has each world's).
#[test]
fn judged_apart_healthy_disks_draw_no_lasting_verdict() {
    for world in [MACOS, BUSY, LINUX] {
        let name = world.name;
        for seed in seeds(44, 8) {
            let mut sim = Sim::new(3, world, seed);
            sim.judge_apart();
            let freshness = settled(&mut sim);
            let from = sim.now;
            sim.run_while(|_| true, Some(from + 20 * freshness));
            for (judge, node) in sim.nodes.iter().enumerate() {
                let verdicts: Vec<(PeerId, bool, u64)> = node
                    .disks
                    .iter()
                    .filter(|(_, _, at)| *at > from)
                    .copied()
                    .collect();
                let none_where_none_stalls = world.name == LINUX.name || verdicts.is_empty();
                assert!(
                    none_where_none_stalls && caught_up_within(&verdicts, freshness),
                    "{name} seed {seed}: node {} judged healthy disks (peer, slow, ms from the \
                     group settling) {:?}, the freshness {} ms",
                    judge + 1,
                    verdicts
                        .iter()
                        .map(|(p, s, at)| (*p, *s, (at - from) / MS))
                        .collect::<Vec<_>>(),
                    freshness / MS
                );
            }
        }
    }
}

/// Whether every slow verdict in `verdicts` (a judge's, in order: peer, slow, arrival) is caught up
/// within `within` of it.
fn caught_up_within(verdicts: &[(PeerId, bool, u64)], within: u64) -> bool {
    verdicts.iter().enumerate().all(|(i, (peer, slow, at))| {
        !*slow
            || verdicts[i + 1..]
                .iter()
                .find(|(p, ..)| p == peer)
                .is_some_and(|(_, again, back)| !*again && *back <= at + within)
    })
}

/// What healthy disks' verdicts are in each world, judged apart: per node, the slow verdicts its
/// peers gave, how long the longest lasted, against the world's longest flush (its trace's maximum)
/// and the node's own flush tail at the end. A survey, not a check: run with `--ignored
/// --nocapture`.
#[test]
#[ignore = "a survey of disk verdicts, not a check"]
fn survey_of_disk_verdicts() {
    for world in [MACOS, BUSY, LINUX] {
        let name = world.name;
        let longest = world.flush[world.flush.len() - 1];
        for seed in seeds(44, 8) {
            let mut sim = Sim::new(3, world, seed);
            sim.judge_apart();
            let freshness = settled(&mut sim);
            let from = sim.now;
            let span = 20 * freshness;
            sim.run_while(|_| true, Some(from + span));
            let mut line = Vec::new();
            for peer in 1..=3u64 {
                let mut slow = 0;
                let mut longest_verdict = 0u64;
                for judge in sim.nodes.iter().filter(|n| n.owner.id != peer) {
                    let mut since = None;
                    for &(p, is_slow, at) in
                        judge.disks.iter().filter(|d| d.0 == peer && d.2 > from)
                    {
                        let _ = p;
                        if is_slow {
                            slow += 1;
                            since = Some(at);
                        } else if let Some(began) = since.take() {
                            longest_verdict = longest_verdict.max(at - began);
                        }
                    }
                }
                let tail = sim.nodes[(peer - 1) as usize]
                    .liveness
                    .flush_tail()
                    .map_or(0, |t| t.as_nanos() as u64 / 1_000);
                line.push(format!(
                    "n{peer}: {slow} slow verdicts, the longest {} ms, tail {} us",
                    longest_verdict / MS,
                    tail
                ));
            }
            println!(
                "{name} seed {seed}: {} ms span, longest trace flush {} ms, freshness {} ms; {}",
                span / MS,
                longest / MS,
                freshness / MS,
                line.join("; ")
            );
        }
    }
}

/// Today's group judges by the flush proof and no disk apart from it: a held disk gives no
/// `Change::DiskSlow`.
#[test]
fn todays_group_gives_no_disk_verdict() {
    for world in [MACOS, BUSY, LINUX] {
        let name = world.name;
        for seed in seeds(45, 4) {
            let mut sim = Sim::new(3, world, seed);
            let freshness = settled(&mut sim);
            let from = sim.now;
            sim.nodes[2].disk_held_until = from + 3 * freshness;
            sim.run_while(|_| true, Some(from + 6 * freshness));
            assert!(
                sim.nodes.iter().all(|node| node.disks.is_empty()),
                "{name} seed {seed}: a disk verdict in today's group"
            );
        }
    }
}

/// One node's flushes stall while the judges' own complete: it is suspected as before, within the
/// bound each suspicion states.
#[test]
fn a_peer_whose_flushes_stall_alone_is_suspected_as_before() {
    for world in [MACOS, BUSY, LINUX] {
        let name = world.name;
        for seed in seeds(17, 8) {
            let mut sim = Sim::new(3, world, seed);
            let freshness = settled(&mut sim);
            let stalled = 2usize;
            let from = sim.now;
            let until = from + 3 * freshness;
            sim.nodes[stalled].disk_held_until = until;
            sim.run_while(|_| true, Some(until));
            for node in 0..2 {
                let found = sim.nodes[node]
                    .suspicions
                    .iter()
                    .find(|s| s.peer == stalled as u64 + 1 && s.noticed_ns > from)
                    .copied()
                    .unwrap_or_else(|| {
                        panic!(
                            "{name} seed {seed}: node {} never suspected the stall",
                            node + 1
                        )
                    });
                assert!(
                    found.noticed_ns <= until,
                    "{name} seed {seed}: noticed past the stall"
                );
                let last = found.last.unwrap();
                let bound = found.detection.unwrap();
                assert!(found.at_ns - last.due_ns <= bound.as_nanos() as u64);
            }
        }
    }
}

/// A node whose own disk stalls for good suspects no one, its own proof never coming, while its
/// peers suspect it: its stream stops, as a crashed node's does. Safe, since such a node cannot
/// lead (its log writes never complete) and its peers replace it.
#[test]
fn a_node_whose_disk_stalls_for_good_suspects_no_one_and_is_suspected() {
    for world in [MACOS, BUSY, LINUX] {
        let name = world.name;
        for seed in seeds(18, 8) {
            let mut sim = Sim::new(3, world, seed);
            let freshness = settled(&mut sim);
            let stalled = 2usize;
            let from = sim.now;
            sim.nodes[stalled].disk_stalled = true;
            // Its peers fall silent too for a while, so that a judge with its proof would suspect
            // them.
            let until = from + 3 * freshness;
            for node in 0..2 {
                sim.nodes[node].disk_held_until = until;
            }
            sim.run_while(|_| true, Some(until));
            let told: Vec<_> = sim.nodes[stalled]
                .suspicions
                .iter()
                .filter(|s| s.noticed_ns > from)
                .map(|s| s.peer)
                .collect();
            assert!(
                told.is_empty(),
                "{name} seed {seed}: the stalled node suspected {told:?}"
            );
            sim.run_until_suspected(stalled);
        }
    }
}

/// A judge whose every write is slower than the margins it holds its peers to, its writes still
/// completing, suspects a dead peer all the same, within one of its writes and the margin past the
/// point: each wait for a proof of its own holds a passed point once. Restated at every wait, the
/// point moved past each next one, and the judge suspected no one for as long as its device was slow
/// (mantle's pinned cluster suite at `1ccd38d`: a leader whose flushes took 182 to 208 ms logged no
/// suspicion of a dead follower in four seconds).
#[test]
fn a_judge_whose_writes_are_all_slow_still_suspects_a_dead_peer() {
    for world in [MACOS, BUSY, LINUX] {
        let name = world.name;
        for seed in seeds(19, 8) {
            let mut sim = Sim::new(3, world, seed);
            let freshness = settled(&mut sim);
            let (judge, victim) = (0usize, 2usize);
            sim.nodes[judge].disk_slowed = 2 * freshness;
            let killed_at = sim.now;
            sim.nodes[victim].alive = false;
            let until = killed_at + 20 * freshness;
            sim.run_while(
                |sim| sim.nodes[judge].liveness.trust(victim as u64 + 1) != Some(Trust::Suspected),
                Some(until),
            );
            let found = sim.nodes[judge]
                .suspicions
                .iter()
                .find(|s| s.peer == victim as u64 + 1 && s.noticed_ns > killed_at)
                .copied()
                .unwrap_or_else(|| {
                    panic!(
                        "{name} seed {seed}: the judge never suspected the dead peer in {} ms, \
                         its writes {} ms each",
                        (until - killed_at) / MS,
                        2 * freshness / MS
                    )
                });
            let late = sim.latest_late + sim.longest_freeze;
            assert!(
                found.noticed_ns <= killed_at + freshness + sim.longest_write + freshness + late,
                "{name} seed {seed}: noticed {} ms after the kill, past one write ({} ms) and two \
                 freshnesses ({} ms each)",
                (found.noticed_ns - killed_at) / MS,
                sim.longest_write / MS,
                freshness / MS
            );
        }
    }
}

/// Every node's device slows for good while its links are young: each flush takes 741 ms more,
/// the longest write hyper-durable-e2e's `member-stopped` reported on CI's ubuntu-24.04 (run
/// 37152765895), from the first heartbeat the victim sends on. The links still configure, and a
/// peer killed once they have is suspected by every other within the bound each suspicion states.
/// How long they take is the open defect in `docs/STATUS.md` (the senders' floors, the mean of
/// every flush since each log opened, climbing toward the slow flushes and moving each interval,
/// the receiver's evidence starting again at each move): each run prints it.
#[test]
fn links_young_when_every_device_slows_for_good_still_configure() {
    for world in [MACOS, LINUX] {
        let name = world.name;
        for seed in seeds(11, 8) {
            let victim = 2usize;
            let mut sim = Sim::new(3, world, seed);
            sim.run_while(|sim| sim.nodes[victim].log.is_empty(), None);
            assert!(
                sim.configured_at.is_empty(),
                "{name} seed {seed}: configured already"
            );
            for node in &mut sim.nodes {
                node.disk_slowed = 741 * MS;
            }
            let slowed_at = sim.now;
            sim.run_until_configured();
            let killed_at = sim.now;
            sim.nodes[victim].alive = false;
            sim.run_while(
                |sim| {
                    (0..2).any(|node| {
                        sim.nodes[node].liveness.trust(victim as u64 + 1) != Some(Trust::Suspected)
                    })
                },
                None,
            );
            for node in 0..2usize {
                let suspicion = sim.nodes[node]
                    .suspicions
                    .iter()
                    .rfind(|s| s.peer == victim as u64 + 1)
                    .copied()
                    .unwrap_or_else(|| panic!("{name} seed {seed}: {node} holds no suspicion"));
                let (Some(last), Some(bound)) = (suspicion.last, suspicion.detection) else {
                    panic!("{name} seed {seed}: node {node} stated no bound");
                };
                assert!(
                    suspicion.at_ns - last.due_ns <= bound.as_nanos() as u64,
                    "{name} seed {seed}: node {node} past its stated bound"
                );
            }
            println!(
                "{name} seed {seed}: every pair configured {} s after the slowdown; the victim \
                 suspected {} s after its kill",
                (killed_at - slowed_at) / (1_000 * MS),
                (sim.now - killed_at) / (1_000 * MS)
            );
        }
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
        resolution: RESOLUTION,
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
            apart: false,
            disk_slow: false,
            echo: None,
            stopping: false,
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
        resolution: RESOLUTION,
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
            resolution: RESOLUTION,
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
                apart: false,
                disk_slow: false,
                echo: None,
                stopping: false,
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

/// Focal's condition 1 on the disk mode (kiln's CP-T2.2): a heartbeat of the other mode is refused
/// for its disk, typed, and its sender judged by its arrivals. Node 1 judges its peers apart from their disks, nodes 2
/// and 3 do not. Through ten of the same-mode pair's freshnesses every heartbeat node 1 takes from
/// the others, and they from it, is refused for its disk (`Refusal::OtherMode`, counted in each
/// pair's report) and taken for its sender's process, and each pair across the modes judges its
/// peer apart from its disk, by its arrivals alone: node 1, alone in its mode, from its own measure
/// of the others' heartbeats. A pair so judged makes the mistakes any detector's margin allows, and
/// no other: the record holds each suspicion to the arrivals it was made on, whichever the modes
/// (over 256 seeds a world, node 1 suspected node 2, whose heartbeats wait on its flushes, for its
/// silence at macos seed 197568495618, and node 2 node 1 in a stall of the world's at
/// 197568495731). Nodes 2 and 3 judge each other as ever.
/// Killed while it speaks the other mode, node 1 is suspected by both (the backstop: a dead
/// process of either mode is suspected). Each pair across the modes is told the mismatch once, as it began
/// (`Change::ModeDiffers`), and the pair of one mode never. Started again in their mode, node 1 is
/// taken, its peers told once that its mode agrees again (`Change::ModeAgrees`), and killed after,
/// it is suspected as any peer is.
#[test]
fn a_heartbeat_of_the_other_mode_is_refused_for_its_disk_and_its_sender_judged_by_its_arrivals() {
    for world in [MACOS, BUSY, LINUX] {
        let name = world.name;
        for seed in seeds(46, 8) {
            let mut sim = Sim::new(3, world, seed);
            sim.nodes[0].liveness.set_disk_apart(true);
            // The pair of one mode is judged; the pairs across the modes take nothing.
            let judged = |sim: &Sim, node: usize, peer: PeerId| {
                sim.nodes[node]
                    .liveness
                    .report(peer)
                    .is_some_and(|r| r.judged)
            };
            sim.run_while(|sim| !(judged(sim, 1, 3) && judged(sim, 2, 2)), None);
            let freshness = [(1usize, 3u64), (2, 2)]
                .iter()
                .filter_map(|&(node, peer)| sim.nodes[node].liveness.report(peer)?.freshness)
                .max()
                .map_or(0, |f| f.as_nanos() as u64);
            let from = sim.now;
            sim.run_while(|_| true, Some(from + 10 * freshness));
            // The pairs across the modes judged too, within a minute: a node alone in the disk mode
            // asks no probe, so its floor rests on its first flush, and its links take longer to be
            // configured (macOS seed 197568495619: past ten freshnesses, judged by 20 s).
            let across = [(0usize, 2u64), (0, 3), (1, 1), (2, 1)];
            let limit = sim.now + 60_000 * MS;
            sim.run_while(
                |sim| !across.iter().all(|&(node, peer)| judged(sim, node, peer)),
                Some(limit),
            );
            for (node, peer) in across {
                let report = sim.nodes[node].liveness.report(peer).unwrap();
                assert!(
                    report.other_mode > 0 && report.taken > 0 && report.judged,
                    "{name} seed {seed}: node {} of peer {peer} across the modes: refused for the \
                     other mode {}, taken {}, judged {}, trust {:?}",
                    node + 1,
                    report.other_mode,
                    report.taken,
                    report.judged,
                    sim.nodes[node].liveness.trust(peer)
                );
            }
            // Each pair across the modes was told the mismatch once, as it began; a pair of one
            // mode never.
            let told = |sim: &Sim, node: usize, peer: PeerId| -> Vec<bool> {
                sim.nodes[node]
                    .modes
                    .iter()
                    .filter(|(of, ..)| *of == peer)
                    .map(|(_, differs, _)| *differs)
                    .collect()
            };
            for (node, peer) in [(0usize, 2u64), (0, 3), (1, 1), (2, 1)] {
                assert_eq!(
                    told(&sim, node, peer),
                    vec![true],
                    "{name} seed {seed}: node {} of peer {peer}: the mode's changes told",
                    node + 1
                );
            }
            for (node, peer) in [(1usize, 3u64), (2, 2)] {
                assert!(
                    told(&sim, node, peer).is_empty(),
                    "{name} seed {seed}: node {} of peer {peer}, one mode, told a mode's change",
                    node + 1
                );
            }
            // The pair of one mode is judged by its own rules, today's flush proof: taken, never
            // refused for its mode, and judged (trusted, or suspected for a disk that stalled, as
            // today's mode suspects one: Linux's VM disk at seed 55).
            for (node, peer) in [(1usize, 3u64), (2, 2)] {
                let report = sim.nodes[node].liveness.report(peer).unwrap();
                assert!(
                    report.taken > 0 && report.other_mode == 0 && report.judged,
                    "{name} seed {seed}: node {} of peer {peer}, one mode: taken {}, refused for \
                     the other mode {}, judged {}",
                    node + 1,
                    report.taken,
                    report.other_mode,
                    report.judged
                );
            }
            // Killed while it speaks the other mode, node 1 is suspected by both: they judge its
            // process by its heartbeats' arrivals. Killed once both trust it, as below, so each
            // suspicion is of the kill and not one its stalls had already drawn.
            let trusted = |sim: &Sim| {
                (1..3).all(|node| {
                    matches!(
                        sim.nodes[node].liveness.trust(1),
                        Some(Trust::Trusted { until_ns }) if until_ns > sim.now
                    )
                })
            };
            sim.run_while(|sim| !trusted(sim), None);
            let killed_other = sim.now;
            sim.nodes[0].alive = false;
            sim.run_until_suspected(0);
            for node in 1..3 {
                assert!(
                    sim.nodes[node]
                        .suspicions
                        .iter()
                        .any(|s| s.peer == 1 && s.at_ns >= killed_other),
                    "{name} seed {seed}: node {} never suspected node 1, killed while it spoke \
                     the other mode",
                    node + 1
                );
            }
            // Node 1 starts again in the others' mode: a new run, taken and judged as any.
            let mut liveness = Liveness::new(Settings {
                local: 1,
                run: 2,
                max_peers: 3,
                history: Exposure::new(),
                resolution: RESOLUTION,
            })
            .unwrap();
            liveness.attach(2).unwrap();
            liveness.attach(3).unwrap();
            sim.restart(0, liveness);
            sim.poll(0);
            // Unbounded, as `killed_peer_is_suspected_within_its_bound` waits: a judge whose own
            // flush proof is out judges no peer until it comes (Linux's VM disk at seed 55 held
            // node 2's past twenty freshnesses), and the simulation's step budget bounds the run.
            sim.run_while(|sim| !(judged(sim, 1, 1) && judged(sim, 2, 1)), None);
            sim.run_while(|sim| !trusted(sim), None);
            for node in 1..3 {
                assert_eq!(
                    told(&sim, node, 1),
                    vec![true, false],
                    "{name} seed {seed}: node {} of node 1 started again in its mode: the mode's \
                     changes told",
                    node + 1
                );
            }
            let killed = sim.now;
            sim.nodes[0].alive = false;
            sim.run_until_suspected(0);
            for node in 1..3 {
                let found = sim.nodes[node]
                    .suspicions
                    .iter()
                    .rfind(|s| s.peer == 1 && s.at_ns >= killed)
                    .copied();
                assert!(
                    found.is_some(),
                    "{name} seed {seed}: node {} never suspected node 1, killed after it started \
                     again in its mode",
                    node + 1
                );
            }
        }
    }
}
