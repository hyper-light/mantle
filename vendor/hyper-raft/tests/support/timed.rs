//! A group in time on measured paths: slates' timed simulation (slates
//! `crates/cluster/tests/support/timed.rs` at `ec5e0df`; mantle note 32 R40) re-founded on this
//! core, for the measurements that need a wide-area network (note 32 R16). Members of this
//! core elect by suspicion and persist what they take at once; each directed path has a one-way
//! latency, a jitter drawn uniformly below a bound, a loss rate and a bandwidth that carries what it
//! is given in order; one clock counts virtual nanoseconds. The owner's part is the harness's: it
//! wakes each member after each call and at its deadline, sets the leader's window to each member,
//! and proposes a steady stream at the leader. No detector speaks: the member founded as leader
//! leads throughout, so what is measured is replication, not elections.
use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use hyper_raft::proto::{ConfState, Message, MessageType};
use hyper_raft::wire::Record;

use super::{New, Replica, Seeded, Settings, Store};

/// A millisecond, in the clock's nanoseconds.
pub const MS: u64 = 1_000_000;
/// A second, in the clock's nanoseconds.
pub const SECOND: u64 = 1_000 * MS;

/// Format: five Azure regions, in the order of [`ROUND_TRIPS_MS`].
pub const REGIONS: [&str; 5] = [
    "East US",
    "West Europe",
    "Japan East",
    "Southeast Asia",
    "Brazil South",
];
/// Microsoft's published P50 round trips among [`REGIONS`], in milliseconds, source row to
/// destination column ("Azure network round-trip latency statistics",
/// learn.microsoft.com/en-us/azure/networking/azure-network-latency, page dated 2026-07-30, as slates
/// read it on 2026-09-28, `crates/cluster/tests/support/azure.rs`). The page is directional.
pub const ROUND_TRIPS_MS: [[u64; 5]; 5] = [
    [0, 83, 162, 224, 117],
    [85, 0, 233, 169, 185],
    [162, 234, 0, 72, 262],
    [224, 169, 72, 0, 330],
    [118, 185, 262, 331, 0],
];

/// One directed path.
#[derive(Clone, Copy, Debug)]
pub struct Path {
    /// What every message takes on it besides its jitter and its serialization.
    pub one_way_ns: u64,
    /// The bound below which each message's jitter is drawn uniformly.
    pub jitter_ns: u64,
    /// What the path carries a second; a message waits for the path to finish the ones before it.
    pub bytes_per_second: u64,
}

impl Path {
    /// How long the path takes to carry `bytes`, rounded up.
    fn serialize_ns(&self, bytes: u64) -> u64 {
        (u128::from(bytes) * u128::from(SECOND)).div_ceil(u128::from(self.bytes_per_second.max(1)))
            as u64
    }
}

/// The paths among the first `count` of [`REGIONS`], member `i` in region `i - 1`: each direction
/// half its published round trip, with `jitter_ns` and `bytes_per_second`.
pub fn regions(count: usize, jitter_ns: u64, bytes_per_second: u64) -> Vec<Vec<Path>> {
    (0..count)
        .map(|from| {
            (0..count)
                .map(|to| Path {
                    one_way_ns: ROUND_TRIPS_MS[from][to] * MS / 2,
                    jitter_ns,
                    bytes_per_second,
                })
                .collect()
        })
        .collect()
}

/// How the leader's window to each member is set.
#[derive(Clone, Copy, Debug)]
pub enum Window {
    /// A fixed number of bytes for every member.
    Bytes(u64),
    /// What the owner derives for each member from its path: this many round trips of what the path
    /// carries in one (its bandwidth over its round trip's tail).
    RoundTrips(u64),
}

/// A run.
#[derive(Clone, Debug)]
pub struct Scenario {
    /// `paths[from - 1][to - 1]`, one row and one column a member.
    pub paths: Vec<Vec<Path>>,
    /// Messages lost, a millionth each.
    pub loss_ppm: u64,
    /// Whether each path delivers in the order sent, as one stream does; otherwise each message
    /// arrives after its own jitter, as datagrams and messages on streams of their own do (focal
    /// sends a peer's frames each on its own stream, focal 27 §11).
    pub ordered: bool,
    /// What the jitter, the loss and the members' draws are drawn from.
    pub seed: u64,
    /// The member founded as leader.
    pub leader: u64,
    /// How the members run.
    pub settings: Settings,
    /// The leader's window to each member.
    pub window: Window,
    /// The leader proposes one entry every this many nanoseconds.
    pub propose_every_ns: u64,
    /// When the stream begins, after the founding.
    pub propose_from_ns: u64,
    /// How long the stream runs.
    pub stream_ns: u64,
    /// How long the run goes on after the stream, for what was proposed to commit.
    pub settle_ns: u64,
    /// The bytes each proposal states.
    pub payload: usize,
}

/// What a run measured.
#[derive(Clone, Debug, Default)]
pub struct Outcome {
    /// Each committed proposal's latency, from its proposal to the leader's commit of it.
    pub latencies_ns: Vec<u64>,
    /// Proposals made.
    pub proposed: u64,
    /// Messages the leader sent, and their bytes as the wire format writes them.
    pub messages: u64,
    pub bytes: u64,
    /// Entries the leader's appends carried, and of those the ones a member had been sent before.
    pub entries_sent: u64,
    pub entries_resent: u64,
    /// The most bytes the leader had queued on its paths at once, beyond what they carried.
    pub queued_bytes: u64,
    /// Proposals left uncommitted when the run ended.
    pub uncommitted: u64,
}

impl Outcome {
    /// The `percent`-th percentile of the latencies, in nanoseconds.
    pub fn latency_pct(&self, percent: u64) -> u64 {
        let mut sorted = self.latencies_ns.clone();
        sorted.sort_unstable();
        if sorted.is_empty() {
            return u64::MAX;
        }
        let at = ((sorted.len() - 1) as u64 * percent / 100) as usize;
        sorted[at]
    }
    /// Commits a second over the stream.
    pub fn commits_per_second(&self, stream_ns: u64) -> u64 {
        self.latencies_ns.len() as u64 * SECOND / stream_ns.max(1)
    }
}

struct Sim<'a> {
    scenario: &'a Scenario,
    nodes: Vec<New>,
    now: u64,
    /// Messages in flight, by when they arrive and in the order sent.
    flight: BTreeMap<(u64, u64), Message>,
    sent: u64,
    /// When each directed path is done carrying what it was given.
    free_at: Vec<Vec<u64>>,
    /// When the last message on each directed path arrives: an ordered path delivers none before.
    arrives_at: Vec<Vec<u64>>,
    random: Seeded,
    /// Proposals not yet committed, by index: when each was proposed.
    pending: VecDeque<(u64, u64)>,
    /// For each member, the last index the leader has sent it.
    sent_through: Vec<u64>,
    outcome: Outcome,
}

impl Sim<'_> {
    fn node(&mut self, id: u64) -> &mut New {
        &mut self.nodes[(id - 1) as usize]
    }
    fn send(&mut self, message: Message) {
        let (from, to) = ((message.from - 1) as usize, (message.to - 1) as usize);
        let path = self.scenario.paths[from][to];
        let bytes = message.encoded_len() as u64;
        if message.from == self.scenario.leader {
            self.outcome.messages += 1;
            self.outcome.bytes += bytes;
            let queued =
                self.free_at[from][to].saturating_sub(self.now) * path.bytes_per_second / SECOND;
            self.outcome.queued_bytes = self.outcome.queued_bytes.max(queued + bytes);
            if message.msg_type == MessageType::MsgAppend {
                let through = &mut self.sent_through[to];
                for entry in &message.entries {
                    self.outcome.entries_sent += 1;
                    if entry.index <= *through {
                        self.outcome.entries_resent += 1;
                    }
                }
                if let Some(last) = message.entries.last() {
                    *through = (*through).max(last.index);
                }
            }
        }
        let leaves = self.now.max(self.free_at[from][to]) + path.serialize_ns(bytes);
        self.free_at[from][to] = leaves;
        let jitter = if path.jitter_ns == 0 {
            0
        } else {
            self.random.below(path.jitter_ns)
        };
        if self.random.below(1_000_000) < self.scenario.loss_ppm {
            return;
        }
        self.sent += 1;
        let mut arrives = leaves + path.one_way_ns + jitter;
        if self.scenario.ordered {
            arrives = arrives.max(self.arrives_at[from][to]);
            self.arrives_at[from][to] = arrives;
        }
        self.flight.insert((arrives, self.sent), message);
    }
    /// After a call on `id`, the owner wakes it at its clock and sends what it gives.
    fn settle(&mut self, id: u64) {
        let now = self.now;
        let node = self.node(id);
        node.wake(now);
        let output = node.drain();
        for message in output.messages {
            self.send(message);
        }
        if id == self.scenario.leader {
            let commit = self.node(id).view().commit;
            while let Some((index, proposed)) = self.pending.front().copied() {
                if index > commit {
                    break;
                }
                self.outcome.latencies_ns.push(now - proposed);
                self.pending.pop_front();
            }
        }
    }
    fn deadline(&self) -> Option<(u64, u64)> {
        self.nodes
            .iter()
            .filter_map(|node| node.deadline().map(|at| (at, node.id())))
            .min()
    }
    /// The next event at or before `until`: a message arriving or a member's deadline. False when
    /// there is none.
    fn next(&mut self, until: u64) -> bool {
        let arrival = self.flight.keys().next().map(|(at, _)| *at);
        let deadline = self.deadline();
        match (arrival, deadline) {
            (Some(at), deadline) if at <= until && deadline.is_none_or(|(due, _)| at <= due) => {
                let key = *self.flight.keys().next().unwrap();
                let message = self.flight.remove(&key).unwrap();
                self.now = self.now.max(at);
                let to = message.to;
                self.node(to).step(message);
                self.settle(to);
                true
            }
            (_, Some((due, id))) if due <= until => {
                self.now = self.now.max(due);
                self.settle(id);
                true
            }
            _ => false,
        }
    }
    fn run_until(&mut self, until: u64) {
        while self.next(until) {}
        self.now = self.now.max(until);
    }
}

/// The round trip's tail from `from` to `to`: both directions' latency and the most jitter each
/// may draw.
fn round_trip_tail(paths: &[Vec<Path>], from: usize, to: usize) -> u64 {
    let there = paths[from][to];
    let back = paths[to][from];
    there.one_way_ns + there.jitter_ns + back.one_way_ns + back.jitter_ns
}

/// Runs `scenario`.
pub fn run(scenario: &Scenario) -> Outcome {
    let count = scenario.paths.len();
    let boot = ConfState {
        voters: (1..=count as u64).collect(),
        ..ConfState::default()
    };
    let leader = scenario.leader;
    let at = (leader - 1) as usize;
    // Every member's timing from the leader's paths: the round its beats repair over is the
    // slowest round trip's tail; the span matters only to the founding, in which the others are
    // held from campaigning.
    let round = (0..count)
        .filter(|to| *to != at)
        .map(|to| round_trip_tail(&scenario.paths, at, to))
        .max()
        .unwrap_or(MS);
    let timing = hyper_raft::Timing {
        span: Duration::from_nanos(round),
        round: Duration::from_nanos(round),
        // A delay within the span and one vote round.
        election: Duration::from_nanos(2 * round),
    };
    let nodes: Vec<New> = (1..=count as u64)
        .map(|id| {
            let mut node = New::open(
                id,
                Store::new(boot.clone()),
                &scenario.settings,
                scenario.seed.wrapping_mul(1_000_003).wrapping_add(id),
                count,
            );
            node.raw.set_timing(timing).unwrap();
            node.raw.hold_campaigns(id != leader).unwrap();
            node
        })
        .collect();
    let mut sim = Sim {
        scenario,
        nodes,
        now: 0,
        flight: BTreeMap::new(),
        sent: 0,
        free_at: vec![vec![0; count]; count],
        arrives_at: vec![vec![0; count]; count],
        random: Seeded(scenario.seed),
        pending: VecDeque::new(),
        sent_through: vec![0; count],
        outcome: Outcome::default(),
    };
    sim.node(leader).campaign();
    sim.settle(leader);
    sim.run_until(scenario.propose_from_ns);
    assert_eq!(
        sim.node(leader).view().role,
        2,
        "seed {}: the founded member leads",
        scenario.seed
    );
    for id in 1..=count as u64 {
        if id != leader {
            sim.node(id).raw.hold_campaigns(false).unwrap();
        }
    }
    // The leader's window to each member.
    for to in 0..count {
        if to == at {
            continue;
        }
        let bytes = match scenario.window {
            Window::Bytes(bytes) => bytes,
            Window::RoundTrips(round_trips) => {
                let path = scenario.paths[at][to];
                let tail = round_trip_tail(&scenario.paths, at, to);
                let carried = (u128::from(path.bytes_per_second) * u128::from(tail)
                    / u128::from(SECOND)) as u64;
                carried * round_trips
            }
        };
        sim.node(leader).set_window(to as u64 + 1, bytes);
    }
    let end = scenario.propose_from_ns + scenario.stream_ns;
    let mut at_ns = scenario.propose_from_ns;
    let mut proposal = 0u64;
    while at_ns < end {
        sim.run_until(at_ns);
        let mut data = proposal.to_le_bytes().to_vec();
        data.resize(scenario.payload.max(8), 0);
        let node = sim.node(leader);
        if node.propose(data) {
            let index = node.view().last_index;
            sim.pending.push_back((index, at_ns));
        }
        sim.outcome.proposed += 1;
        sim.settle(leader);
        proposal += 1;
        at_ns += scenario.propose_every_ns;
    }
    sim.run_until(end + scenario.settle_ns);
    sim.outcome.uncommitted = sim.pending.len() as u64;
    sim.outcome
}
