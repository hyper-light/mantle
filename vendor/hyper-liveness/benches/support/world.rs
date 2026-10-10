//! The benchmarks' world: `nodes` liveness instances on one clock, each pair's messages delivered
//! after a seeded delay, each node's writes durable after a seeded flush, each wake late by a
//! seeded amount, as `tests/sim.rs` drives them; every buffer sized before the counted span, so
//! what is counted is the crate's own.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};

use hyper_liveness::{Change, Liveness, MAX_BYTES, Output, PeerId, Settings, Write};
use hyper_timing::{Ballot, Exposure};

/// A microsecond in nanoseconds.
const US: u64 = 1_000;

/// A LAN's one-way delay: 80 µs and up to 60 µs more.
const DELAY: (u64, u64) = (80 * US, 60 * US);
/// A flush: 200 µs and up to 400 µs more, Linux's `fdatasync` on the traces' order (`docs/timing.md`
/// §2.6: 0.09 ms in Docker's VM; a laptop's NVMe at a few hundred microseconds).
const FLUSH: (u64, u64) = (200 * US, 400 * US);
/// A wake's lateness: Linux's 50 µs default timer slack (`PR_SET_TIMERSLACK(2const)`) and up to as
/// much again.
const LATE: (u64, u64) = (50 * US, 50 * US);

/// A xorshift stream (Marsaglia 2003).
pub(crate) struct Noise(pub(crate) u64);

impl Noise {
    pub(crate) fn below(&mut self, span: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % span.max(1)
    }
}

/// A message in flight or a write on a disk.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Event {
    Arrive { to: usize, from: usize, slot: usize },
    Durable { node: usize, started: u64 },
}

/// One node's owner: what its last call asked, in buffers sized once.
pub(crate) struct Owner {
    pub(crate) out: Vec<(PeerId, usize, [u8; MAX_BYTES])>,
    pub(crate) flush: bool,
    pub(crate) suspicions: u64,
}

impl Output for Owner {
    fn heartbeat(&mut self, peer: PeerId, message: &[u8]) {
        let mut bytes = [0u8; MAX_BYTES];
        bytes[..message.len()].copy_from_slice(message);
        // Within the capacity reserved: no allocation.
        if self.out.len() < self.out.capacity() {
            self.out.push((peer, message.len(), bytes));
        }
    }
    fn flush(&mut self) {
        self.flush = true;
    }
    fn change(&mut self, change: Change) {
        if matches!(change, Change::Suspected(_)) {
            self.suspicions += 1;
        }
    }
}

pub(crate) struct Node {
    pub(crate) liveness: Liveness,
    pub(crate) owner: Owner,
    disk_until: u64,
}

pub(crate) struct World {
    pub(crate) now: u64,
    pub(crate) noise: Noise,
    pub(crate) nodes: Vec<Node>,
    heap: BinaryHeap<Reverse<(u64, Event)>>,
    /// Messages in flight, by slot.
    flight: Vec<(usize, [u8; MAX_BYTES])>,
    free: Vec<usize>,
    /// Heartbeats sent and taken since the last read.
    pub(crate) sent: u64,
    pub(crate) taken: u64,
    pub(crate) flushes: u64,
    /// Time spent in the crate's calls since the last read.
    pub(crate) busy: Duration,
    /// The bootstrap, once [`warm`](Self::warm) has seen every pair configured: the simulated
    /// time it took, the heartbeats sent and taken in it, and the time of the crate's calls.
    pub(crate) bootstrap: Option<(Duration, u64, Duration)>,
}

impl World {
    /// `nodes` nodes, each sharing `groups` groups with every other.
    pub(crate) fn new(nodes: usize, groups: u32, seed: u64) -> Self {
        let mut world = Self {
            now: 0,
            noise: Noise(seed | 1),
            nodes: (0..nodes)
                .map(|i| {
                    let id = i as u64 + 1;
                    let mut liveness = Liveness::new(Settings {
                        local: id,
                        // Each node's first start.
                        run: 1,
                        max_peers: nodes,
                        history: Exposure::new(),
                        // The simulation's stamps are whole nanoseconds.
                        resolution: Duration::from_nanos(1),
                    })
                    .unwrap();
                    for peer in (1..=nodes as u64).filter(|p| *p != id) {
                        for _ in 0..groups {
                            liveness.attach(peer).unwrap();
                        }
                    }
                    Node {
                        liveness,
                        owner: Owner {
                            out: Vec::with_capacity(nodes),
                            flush: false,
                            suspicions: 0,
                        },
                        disk_until: 0,
                    }
                })
                .collect(),
            heap: BinaryHeap::with_capacity(1 << 16),
            flight: vec![(0, [0; MAX_BYTES]); 1 << 16],
            free: (0..1 << 16).rev().collect(),
            sent: 0,
            taken: 0,
            flushes: 0,
            busy: Duration::ZERO,
            bootstrap: None,
        };
        for node in 0..nodes {
            world.poll(node);
        }
        world
    }

    fn drain(&mut self, node: usize) {
        let mut out = std::mem::take(&mut self.nodes[node].owner.out);
        for (peer, length, bytes) in out.drain(..) {
            self.sent += 1;
            let Some(slot) = self.free.pop() else {
                continue;
            };
            self.flight[slot] = (length, bytes);
            let at = self.now + DELAY.0 + self.noise.below(DELAY.1);
            self.heap.push(Reverse((
                at,
                Event::Arrive {
                    to: peer as usize - 1,
                    from: node,
                    slot,
                },
            )));
        }
        self.nodes[node].owner.out = out;
        if std::mem::take(&mut self.nodes[node].owner.flush) {
            let start = self.now.max(self.nodes[node].disk_until);
            let done = start + FLUSH.0 + self.noise.below(FLUSH.1);
            self.nodes[node].disk_until = done;
            self.heap.push(Reverse((
                done,
                Event::Durable {
                    node,
                    started: self.now,
                },
            )));
        }
    }

    #[allow(
        clippy::disallowed_methods,
        reason = "a benchmark measures real time on the host"
    )]
    fn poll(&mut self, node: usize) {
        let now = self.now;
        let n = &mut self.nodes[node];
        let started = Instant::now();
        n.liveness.poll(now, &mut n.owner);
        self.busy += started.elapsed();
        self.drain(node);
    }

    /// The owner's wait for `node`'s wake ended late at `now`: reported, then polled, as an owner
    /// does (`Liveness::on_wait`).
    #[allow(
        clippy::disallowed_methods,
        reason = "a benchmark measures real time on the host"
    )]
    fn woke(&mut self, node: usize, deadline: u64) {
        let now = self.now;
        let n = &mut self.nodes[node];
        let started = Instant::now();
        n.liveness.on_wait(deadline, now);
        n.liveness.poll(now, &mut n.owner);
        self.busy += started.elapsed();
        self.drain(node);
    }

    /// The election cost from the library's law over the measured round trips, as `tests/sim.rs`.
    pub(crate) fn elect(&mut self) {
        let count = self.nodes.len();
        for (i, node) in self.nodes.iter_mut().enumerate() {
            let (Some(granularity), Some(durable)) =
                (node.liveness.granularity(), node.liveness.flush_mean())
            else {
                continue;
            };
            let id = i as u64 + 1;
            let span = {
                let paths = (1..=count as u64)
                    .filter(|p| *p != id)
                    .filter_map(|p| node.liveness.round_trip(p));
                let paths: Vec<_> = paths.copied().collect();
                Ballot::measure(paths.iter(), count.max(3), durable, granularity)
                    .and_then(|ballot| ballot.span(granularity))
            };
            if let Some(span) = span {
                for peer in (1..=count as u64).filter(|p| *p != id) {
                    node.liveness.set_election(peer, span.election).unwrap();
                }
            }
        }
    }

    /// Runs for `span` of simulated time.
    pub(crate) fn run(&mut self, span: Duration) {
        let until = self.now + span.as_nanos() as u64;
        loop {
            let event = self.heap.peek().map(|Reverse((at, _))| *at);
            let wake = self
                .nodes
                .iter()
                .enumerate()
                .filter_map(|(i, n)| n.liveness.wake().map(|w| (w, i)))
                .min();
            let next = match (event, wake) {
                (Some(e), Some((w, _))) if e <= w => e,
                (Some(e), None) => e,
                (_, Some((w, _))) => w,
                (None, None) => break,
            };
            if next > until {
                self.now = until;
                break;
            }
            if event == Some(next) {
                self.now = next;
                let Reverse((_, event)) = self.heap.pop().unwrap();
                self.handle(event);
            } else if let Some((w, node)) = wake {
                self.now = w + LATE.0 + self.noise.below(LATE.1);
                self.woke(node, w);
            }
        }
    }

    #[allow(
        clippy::disallowed_methods,
        reason = "a benchmark measures real time on the host"
    )]
    fn handle(&mut self, event: Event) {
        let now = self.now;
        match event {
            Event::Arrive { to, from, slot } => {
                let (length, bytes) = self.flight[slot];
                self.free.push(slot);
                let n = &mut self.nodes[to];
                let began = Instant::now();
                if n.liveness
                    .on_heartbeat(from as u64 + 1, &bytes[..length], now, &mut n.owner)
                    .is_ok()
                {
                    self.taken += 1;
                }
                n.liveness.poll(now, &mut n.owner);
                self.busy += began.elapsed();
                self.drain(to);
            }
            Event::Durable { node, started } => {
                self.flushes += 1;
                let n = &mut self.nodes[node];
                let began = Instant::now();
                n.liveness.on_durable(Write::Liveness, started, now);
                n.liveness.poll(now, &mut n.owner);
                self.busy += began.elapsed();
                self.drain(node);
            }
        }
    }

    /// Whether every pair is configured.
    pub(crate) fn configured(&self) -> bool {
        let count = self.nodes.len() as u64;
        self.nodes.iter().enumerate().all(|(i, n)| {
            (1..=count)
                .filter(|p| *p != i as u64 + 1)
                .all(|p| n.liveness.report(p).is_some_and(|r| r.configured))
        })
    }

    /// Runs until every pair is configured and then a further ten seconds, electing every 100 ms;
    /// the bootstrap is recorded apart.
    pub(crate) fn warm(&mut self) {
        while !self.configured() {
            self.elect();
            self.run(Duration::from_millis(100));
        }
        self.bootstrap = Some((
            Duration::from_nanos(self.now),
            self.sent + self.taken,
            self.busy,
        ));
        for _ in 0..100 {
            self.elect();
            self.run(Duration::from_millis(100));
        }
    }
}
