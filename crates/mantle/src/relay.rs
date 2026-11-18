//! The path a doors bench's native clients reach the cell's nodes by under the relay condition
//! (docs/research/70, "Conditions"; [19], [24]): every datagram, either way, held for a one-way
//! delay of its own and lost as a measured intercontinental path lost them, so what the harness's
//! latencies show past that physical floor is the stack's.
//!
//! - **Delay.** A datagram's one-way delay is `ONE_WAY_NS − JITTER_NS + U[0, 2·JITTER_NS]`,
//!   500 ± 100 ms, each drawn alone, so a datagram may arrive before one sent earlier: focal-sim's
//!   `Path::reordering` at its `GEOGRAPHIC` profile (research/07; research/70 [19]).
//! - **Loss.** A two-state Gilbert–Elliott chain on each direction of each link (Gilbert, BSTJ
//!   1960): the bad state loses what is sent in it, the good state nothing; it is entered with
//!   probability `RUNS / (SENT − LOST)` a datagram and left with `RUNS / LOST`, which make the
//!   trace's loss, `LOST / SENT`, and its mean run, `LOST / RUNS`, the chain's own: Hayashibara,
//!   Défago and Katayama's three weeks between Switzerland and Japan, 60,489 heartbeats of which
//!   219 were lost, 120 alone and 99 in 27 bursts (research/70 [24]). The trace sampled the path
//!   every 30 s; its runs applied a datagram at a time are the model's step, not its measure.
//! - **Its own cost.** Each datagram's lateness past its due time is counted and said beside the
//!   phases, so the relay's cost is not charged to the stack (research/70 [12], geo_open_loop's
//!   rule).
//! - **Bounds.** A link set for each native client the harness holds at most, a link a node, each
//!   a front socket the client sends to and a back socket that sends to the node; a client past
//!   them is refused. A direction of a link holds at most a client's transport's bytes
//!   (`mantle_client::least_transport`), what its connections can have unacknowledged or unread at
//!   once; a datagram past them is dropped and counted.
//!
//! One hyper-rt shard on a thread of its own drives every link, a task each, as the node tests'
//! partitioning relay does (crates/node/tests/support/relay.rs). Each datagram a link reads or
//! sends is a charge of its turn on that shard (`hyper_rt::coop`; hyper-raft docs/runtime.md
//! §3.14), so a link whose client keeps it full yields the shard to the others at its quantum.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::pin;
use std::sync::mpsc;
use std::thread::JoinHandle;

use hyper_rt::combine::{Either, Either3, race2, race3};
use hyper_rt::futures::{now_ns, sleep};
use hyper_rt::machine::calibration::Policy;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::sync::{ChannelReceiver, Sender, channel};
use hyper_rt::udp::UdpSocket;
use mantle_disk::measure::SplitMix64;

/// The path's one-way delay at its middle: the owner's condition, 500 ms (research/70 [19]).
pub const ONE_WAY_NS: u64 = 500_000_000;

/// The spread about it either way: focal's `GEOGRAPHIC` path, 500 ± 100 ms one way (research/07;
/// research/70 [19]).
pub const JITTER_NS: u64 = 100_000_000;

/// The κ-FD trace's heartbeats sent (research/70 [24]).
pub(crate) const SENT: u64 = 60_489;

/// The trace's heartbeats lost (research/70 [24]).
pub(crate) const LOST: u64 = 219;

/// The runs the trace's losses came in: 120 alone and 27 bursts (research/70 [24]).
pub(crate) const RUNS: u64 = 147;

/// Why the relay did not carry what it was asked to.
#[derive(Debug)]
pub enum RelayError {
    Runtime(String),
    Socket(String),
    /// The relay's thread ended before it said what it was asked.
    Gone,
    /// Every one of its link sets is taken.
    Full(usize),
}

impl std::fmt::Display for RelayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(e) => write!(f, "the relay's runtime: {e}"),
            Self::Socket(e) => write!(f, "a relay socket: {e}"),
            Self::Gone => write!(
                f,
                "the relay's thread ended before it said what it was asked"
            ),
            Self::Full(sets) => write!(f, "every one of the relay's {sets} link sets is taken"),
        }
    }
}

/// One direction of a link: its chain's state, the datagrams it holds and their bytes, and what
/// it counted.
struct Way {
    bad: bool,
    held: BinaryHeap<Reverse<Held>>,
    bytes: u64,
    most: u64,
    said: Said,
}

/// A datagram held until `due_ns`, `seq` keeping the order of two due at once.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Held {
    due_ns: u64,
    seq: u64,
    to: SocketAddr,
    bytes: Vec<u8>,
}

/// What a relay counted, both directions of every link summed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Said {
    /// Datagrams taken in.
    pub taken: u64,
    /// Lost by the chain.
    pub lost: u64,
    /// Dropped past a direction's bytes.
    pub over: u64,
    /// Sent on after their delay.
    pub sent: u64,
    /// The sent datagrams' lateness past their due times: summed, and the most.
    pub late_ns: u64,
    pub latest_ns: u64,
}

impl Said {
    fn add(&mut self, other: &Self) {
        self.taken = self.taken.saturating_add(other.taken);
        self.lost = self.lost.saturating_add(other.lost);
        self.over = self.over.saturating_add(other.over);
        self.sent = self.sent.saturating_add(other.sent);
        self.late_ns = self.late_ns.saturating_add(other.late_ns);
        self.latest_ns = self.latest_ns.max(other.latest_ns);
    }
}

impl Way {
    fn new(most: u64) -> Self {
        Self {
            bad: false,
            held: BinaryHeap::new(),
            bytes: 0,
            most,
            said: Said::default(),
        }
    }

    /// Takes a datagram for `to` at `now`: lost by the chain, dropped past the direction's bytes,
    /// or held for its delay.
    fn take(&mut self, draws: &mut SplitMix64, (datagram, to): (&[u8], SocketAddr), now: u64) {
        self.said.taken = self.said.taken.saturating_add(1);
        self.bad = step(draws, self.bad);
        if self.bad {
            self.said.lost = self.said.lost.saturating_add(1);
            return;
        }
        let len = u64::try_from(datagram.len()).unwrap_or(u64::MAX);
        match self.bytes.checked_add(len) {
            Some(bytes) if bytes <= self.most => self.bytes = bytes,
            _ => {
                self.said.over = self.said.over.saturating_add(1);
                return;
            }
        }
        self.held.push(Reverse(Held {
            due_ns: now.saturating_add(delay(draws)),
            seq: self.said.taken,
            to,
            bytes: datagram.to_vec(),
        }));
    }

    /// The datagram due by `now`, if one is.
    fn due(&mut self, now: u64) -> Option<Held> {
        if self
            .held
            .peek()
            .is_none_or(|Reverse(held)| held.due_ns > now)
        {
            return None;
        }
        let Reverse(held) = self.held.pop()?;
        let len = u64::try_from(held.bytes.len()).unwrap_or(u64::MAX);
        self.bytes = self.bytes.saturating_sub(len);
        let late = now.saturating_sub(held.due_ns);
        self.said.sent = self.said.sent.saturating_add(1);
        self.said.late_ns = self.said.late_ns.saturating_add(late);
        self.said.latest_ns = self.said.latest_ns.max(late);
        Some(held)
    }

    /// When the next datagram is due, if one is held.
    fn next(&self) -> Option<u64> {
        self.held.peek().map(|Reverse(held)| held.due_ns)
    }
}

/// The chain's state for the next datagram, from `bad` for the last: entered with probability
/// `RUNS / (SENT − LOST)`, left with `RUNS / LOST`.
fn step(draws: &mut SplitMix64, bad: bool) -> bool {
    if bad {
        draws.below(LOST) >= RUNS
    } else {
        draws.below(SENT.saturating_sub(LOST)) < RUNS
    }
}

/// A datagram's one-way delay: `ONE_WAY_NS − JITTER_NS + U[0, 2·JITTER_NS]`.
fn delay(draws: &mut SplitMix64) -> u64 {
    let span = JITTER_NS.saturating_mul(2).saturating_add(1);
    ONE_WAY_NS
        .saturating_sub(JITTER_NS)
        .saturating_add(draws.below(span))
}

/// A datagram from `socket`, if one waits there: its try is a charge of the link's turn on the
/// relay's shard (`hyper_rt::coop`), as `UdpSocket::recv_from`'s is, so a client that keeps its
/// link's socket full yields the shard to the other links at the shard's quantum.
async fn arrived(socket: &UdpSocket, datagram: &mut [u8]) -> Option<(usize, SocketAddr)> {
    hyper_rt::coop::consume_budget().await;
    socket.try_recv_from(datagram).ok().flatten()
}

/// One link: datagrams from its front's sender to `to`, the node, and the node's replies back,
/// each way through its own direction. Ends when the relay does, saying what it counted.
async fn link(
    (front, back): (UdpSocket, UdpSocket),
    to: SocketAddr,
    (seed, most): (u64, u64),
    mut stop: ChannelReceiver<()>,
    said: mpsc::Sender<Said>,
) {
    let mut draws = SplitMix64::new(seed);
    let mut ways = [Way::new(most), Way::new(most)];
    let mut from: Option<SocketAddr> = None;
    let mut datagram = vec![0u8; mantle_client::PIECE];
    loop {
        let [forward, backward] = &mut ways;
        // Each datagram is held from when it was read: a link that yields its shard within a drain
        // reads the rest later.
        while let Some((len, sender)) = arrived(&front, &mut datagram).await {
            from = Some(sender);
            if let Some(bytes) = datagram.get(..len) {
                forward.take(&mut draws, (bytes, to), now_ns());
            }
        }
        while let Some((len, _)) = arrived(&back, &mut datagram).await {
            if let (Some(from), Some(bytes)) = (from, datagram.get(..len)) {
                backward.take(&mut draws, (bytes, from), now_ns());
            }
        }
        // A datagram the OS refuses (its destination gone) is lost, as the network loses one.
        let now = now_ns();
        while let Some(held) = forward.due(now) {
            let _ = back.send_to_writable(&held.bytes, held.to).await;
        }
        while let Some(held) = backward.due(now) {
            let _ = front.send_to_writable(&held.bytes, held.to).await;
        }
        let next = forward.next().into_iter().chain(backward.next()).min();
        let stopped = {
            let (front, back) = (pin!(front.readable()), pin!(back.readable()));
            match next {
                None => matches!(race3(front, back, stop.recv()).await, Either3::Third(_)),
                Some(at) => {
                    let turn = sleep(at.saturating_sub(now_ns()));
                    matches!(
                        race3(front, back, race2(stop.recv(), turn)).await,
                        Either3::Third(Either::First(_))
                    )
                }
            }
        };
        if stopped {
            let mut counted = Said::default();
            for way in &ways {
                counted.add(&way.said);
            }
            let _ = said.send(counted);
            return;
        }
    }
}

/// The relay: each link set's fronts, where a client is told each node listens, and what ends it.
pub struct Relay {
    sets: Fronts,
    taken: std::cell::Cell<usize>,
    /// A link's stop each: dropped, the link says what it counted and ends.
    stops: Vec<Sender<()>>,
    /// The root's: dropped once every link has said, the relay's thread ends.
    root: Option<Sender<()>>,
    said: mpsc::Receiver<Said>,
    links: usize,
    thread: Option<JoinHandle<Result<(), RelayError>>>,
}

/// Each link set's fronts: where a client dials each node, by its id.
type Fronts = Vec<Vec<(u64, SocketAddr)>>;

/// What the relay's thread says once its links are made: each set's fronts, or why it could not.
type Made = Result<Fronts, String>;

impl Relay {
    /// A relay to `nodes`, each `(id, address)`, with `sets` link sets, a client's each; each
    /// direction holding at most `most` bytes; its draws from `seed`; its runtime from the
    /// calibration in `dir`, as a client's is (`client::runtime_of`).
    pub fn start(
        dir: &Path,
        nodes: &[(u64, SocketAddr)],
        sets: usize,
        (most, seed): (u64, u64),
    ) -> Result<Self, RelayError> {
        let links = sets.saturating_mul(nodes.len());
        let failed = |e: hyper_rt::RtError| RelayError::Runtime(format!("{e:?}"));
        let mut stops = Vec::with_capacity(links);
        let mut stopped = Vec::with_capacity(links);
        for _ in 0..links {
            let (stop, heard) = channel::<()>(1).map_err(failed)?;
            stops.push(stop);
            stopped.push(heard);
        }
        let (root, rooted) = channel::<()>(1).map_err(failed)?;
        let (said, heard) = mpsc::channel();
        let (made, built) = mpsc::channel::<Made>();
        let dir = dir.to_path_buf();
        let nodes = nodes.to_vec();
        let thread = std::thread::Builder::new()
            .name("mantle-relay".into())
            .spawn(move || {
                let mut rt = match links_on(&dir, &nodes, sets, (most, seed), (stopped, &said)) {
                    Ok((rt, fronts)) => {
                        made.send(Ok(fronts)).map_err(|_| RelayError::Gone)?;
                        rt
                    }
                    Err(e) => {
                        let _ = made.send(Err(e.to_string()));
                        return Err(e);
                    }
                };
                drop(said);
                let mut rooted = rooted;
                // The root waits for the relay to be stopped; each link ends on its own stop.
                rt.block_on(async move { while rooted.recv().await.is_ok() {} })
                    .map_err(|e| RelayError::Runtime(format!("{e:?}")))
            })
            .map_err(|e| RelayError::Runtime(e.to_string()))?;
        let sets = built
            .recv()
            .map_err(|_| RelayError::Gone)?
            .map_err(RelayError::Runtime)?;
        Ok(Self {
            sets,
            taken: std::cell::Cell::new(0),
            stops,
            root: Some(root),
            said: heard,
            links,
            thread: Some(thread),
        })
    }

    /// The next link set's fronts, a client's to dial each node at, first `first` as the harness
    /// rotates them; refused once every set is taken.
    pub fn take(&self, first: usize) -> Result<Vec<(u64, SocketAddr)>, RelayError> {
        let taken = self.taken.get();
        let mut set = self
            .sets
            .get(taken)
            .cloned()
            .ok_or(RelayError::Full(self.sets.len()))?;
        self.taken.set(taken.saturating_add(1));
        let first = first.min(set.len());
        set.rotate_left(first);
        Ok(set)
    }

    /// Stops every link and then the relay's thread: what the links counted, summed.
    pub fn stop(mut self) -> Result<Said, RelayError> {
        self.stops.clear();
        let mut said = Said::default();
        for _ in 0..self.links {
            let counted = self.said.recv().map_err(|_| RelayError::Gone)?;
            said.add(&counted);
        }
        self.root = None;
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| RelayError::Gone)??;
        }
        Ok(said)
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.stops.clear();
        self.root = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The relay's runtime and its links, made on its own thread: `sets` sets of a link a node, each
/// link a task with its two sockets, its stop and its draws' seed; each set's fronts.
fn links_on(
    dir: &Path,
    nodes: &[(u64, SocketAddr)],
    sets: usize,
    (most, seed): (u64, u64),
    (stopped, said): (Vec<ChannelReceiver<()>>, &mpsc::Sender<Said>),
) -> Result<(LocalRuntime, Fronts), RelayError> {
    let socket = |e: hyper_rt::RtError| RelayError::Socket(format!("{e:?}"));
    let mut rt = runtime(dir, stopped.len())?;
    let mut draws = SplitMix64::new(seed);
    let mut stopped = stopped.into_iter();
    let mut fronts = Vec::with_capacity(sets);
    for _ in 0..sets {
        let mut set = Vec::with_capacity(nodes.len());
        for &(id, at) in nodes {
            let front = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).map_err(socket)?;
            let back = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).map_err(socket)?;
            set.push((id, front.local_addr().map_err(socket)?));
            let stop = stopped.next().ok_or(RelayError::Gone)?;
            rt.spawn(link(
                (front, back),
                at,
                (draws.next_u64(), most),
                stop,
                said.clone(),
            ))
            .map_err(|e| RelayError::Runtime(format!("{e:?}")))?;
        }
        fronts.push(set);
    }
    Ok((rt, fronts))
}

/// The relay's shard, as a client's runtime is made from the machine's calibration
/// (`client::runtime_of`): a task and a timer each link and the root, two sockets each link.
fn runtime(dir: &Path, links: usize) -> Result<LocalRuntime, RelayError> {
    let failed = |e: String| RelayError::Runtime(e);
    let budget = mantle_node::derive::probe_budget().map_err(|e| failed(e.to_string()))?;
    let calibration =
        mantle_node::process::calibration(dir, budget).map_err(|e| failed(e.to_string()))?;
    let constants = calibration
        .constants(&Policy {
            reserved_cores: 0,
            lateness_tolerance_ns: None,
            latency_objective_ns: None,
        })
        .map_err(|unmet| failed(format!("{unmet:?}")))?;
    let tasks = links.saturating_add(1);
    let mut config = RuntimeConfig::from_calibration(&calibration, &constants, tasks, tasks);
    config.shards = 1;
    config.pin = false;
    config.cores.clear();
    config.interests_per_shard = config
        .interests_per_shard
        .max(links.saturating_mul(2).saturating_mul(2));
    LocalRuntime::new(&config).map_err(|e| failed(format!("{e:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The chain's stationary loss and mean run are the trace's, exactly in the limit; over a
    /// million datagrams from one seed (so the count is the same each run) they fall within the
    /// trace's own: 0.36% lost in runs of about one and a half.
    #[test]
    fn the_chain_loses_as_the_trace_lost() {
        let mut draws = SplitMix64::new(24);
        let (mut bad, mut lost, mut runs) = (false, 0u64, 0u64);
        let sent = 1_000_000u64;
        for _ in 0..sent {
            let was = bad;
            bad = step(&mut draws, bad);
            if bad {
                lost += 1;
                if !was {
                    runs += 1;
                }
            }
        }
        // The seed's own counts: the test is exact, its bounds what they show of the chain.
        let rate = lost as f64 / sent as f64;
        let run = lost as f64 / runs as f64;
        assert!((rate - LOST as f64 / SENT as f64).abs() < 0.0005, "{rate}");
        assert!((run - LOST as f64 / RUNS as f64).abs() < 0.1, "{run}");
    }

    /// Every delay lies within 500 ± 100 ms, and both ends of the spread are reached.
    #[test]
    fn a_delay_is_within_its_spread() {
        let mut draws = SplitMix64::new(19);
        let (mut least, mut most) = (u64::MAX, 0);
        for _ in 0..100_000 {
            let d = delay(&mut draws);
            least = least.min(d);
            most = most.max(d);
        }
        assert!(least >= ONE_WAY_NS - JITTER_NS && most <= ONE_WAY_NS + JITTER_NS);
        assert!(least < ONE_WAY_NS - JITTER_NS + 1_000_000);
        assert!(most > ONE_WAY_NS + JITTER_NS - 1_000_000);
    }

    /// A direction holds what is due in its due order, two held together in the order they came,
    /// and drops what passes its bytes.
    #[test]
    fn a_way_sends_in_due_order_within_its_bytes() {
        let to = SocketAddr::from(([127, 0, 0, 1], 9));
        let mut way = Way::new(10);
        let mut draws = SplitMix64::new(7);
        way.held.push(Reverse(Held {
            due_ns: 30,
            seq: 1,
            to,
            bytes: vec![1; 4],
        }));
        way.held.push(Reverse(Held {
            due_ns: 20,
            seq: 2,
            to,
            bytes: vec![2; 4],
        }));
        way.bytes = 8;
        way.take(&mut draws, (&[3; 4], to), 0);
        assert!(way.said.over + way.said.lost == 1, "{:?}", way.said);
        assert!(way.due(19).is_none());
        assert_eq!(way.due(25).map(|h| h.bytes), Some(vec![2; 4]));
        assert_eq!(way.due(40).map(|h| h.bytes), Some(vec![1; 4]));
        assert_eq!(way.said.latest_ns, 10);
        assert_eq!(way.bytes, 0);
    }
}
