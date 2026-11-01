//! A relay a test partitions a cell's network with. Each node is told its peers listen at the
//! relay: for each pair, from each side, one address for QUIC and one for the plane. The relay
//! forwards each datagram to where the peer does listen, and a reply back to its sender, while the
//! pair is joined; a pair cut has its datagrams dropped both ways, QUIC's and the plane's, as a
//! partition drops them, and joined again they pass. A node's plane may be deafened: every datagram
//! its peers' planes send it is dropped, on their links and as replies on its own, while its own
//! reach them, as a host that loses what it is sent and not what it sends. A node started again
//! elsewhere is followed by the relay, so its peers reach it at the address they knew, as they
//! would a node started again on its own address.
//!
//! The relay is one hyper-rt shard on a thread of its own, a task a link (a pair's direction and
//! socket), each link two sockets: its front, which the dialing side sends to, and its back, which
//! sends to the listening side and takes its replies.
//!
//! The nodes' QUIC links may be paced as a bottleneck paces them ([`Relay::pace`]): each datagram,
//! either way, leaves once the link has sent the ones before it at the pace's rate, through a queue
//! of the pace's bytes past which a datagram is dropped, as a router's tail drop drops it. The
//! planes are never paced.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::mpsc;
use std::thread::JoinHandle;

use hyper_rt::combine::{Either, Either3, race2, race3};
use hyper_rt::futures::{now_ns, sleep};
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, interests_for};
use hyper_rt::sync::{ChannelReceiver, Sender, channel};
use hyper_rt::udp::UdpSocket;

/// The largest datagram UDP carries over IPv4: RFC 768's 65,535 bytes less its header's 8 and
/// IPv4's least header of 20. Any datagram a node sends fits.
const LARGEST: usize = 65_507;

/// What a link is told: where its listening side listens now, whether its pair is joined,
/// whether what goes to its listening side, or back to its sender, is dropped, or the pace it
/// holds both ways to.
enum Told {
    To(SocketAddr),
    Joined(bool),
    DropForward(bool),
    DropBack(bool),
    Pace(Option<Pace>),
}

/// A bottleneck's pace: `rate` bytes a second, through a queue of at most `queue` bytes.
#[derive(Clone, Copy, Debug)]
pub struct Pace {
    pub rate: u64,
    pub queue: usize,
}

/// One way of a paced link: the datagrams waiting their turn and where each goes, their bytes,
/// and when, on the shard's clock, the link is free to send the next.
struct Paced {
    waiting: VecDeque<(Vec<u8>, SocketAddr)>,
    bytes: usize,
    free_ns: u64,
}

impl Paced {
    fn new() -> Self {
        Self {
            waiting: VecDeque::new(),
            bytes: 0,
            free_ns: 0,
        }
    }

    /// Queues `datagram` for `to`, or drops it where the queue has no room for it.
    fn take(&mut self, pace: Pace, datagram: &[u8], to: SocketAddr) {
        if self.bytes + datagram.len() > pace.queue {
            return;
        }
        self.bytes += datagram.len();
        self.waiting.push_back((datagram.to_vec(), to));
    }

    /// Moves into `due` each datagram the link may send at `now`, each holding the link for its
    /// bytes at the pace's rate; when the next may go, if one waits. Unpaced, every one is due.
    fn due(
        &mut self,
        pace: Option<Pace>,
        now: u64,
        due: &mut Vec<(Vec<u8>, SocketAddr)>,
    ) -> Option<u64> {
        while !self.waiting.is_empty() {
            if let Some(pace) = pace {
                if self.free_ns > now {
                    return Some(self.free_ns);
                }
                let len = self.waiting[0].0.len() as u64;
                self.free_ns = self.free_ns.max(now) + len * 1_000_000_000 / pace.rate;
            }
            let (datagram, to) = self.waiting.pop_front().unwrap();
            self.bytes -= datagram.len();
            due.push((datagram, to));
        }
        None
    }
}

/// Orders a link holds before it takes them: the test tells a link at most where its side listens
/// and one change of what passes between two of the link's turns, and waits on the cell between
/// one change and the next.
const TOLD: usize = 2;

/// One link: datagrams from its front's sender to `to`, the listening side, and that side's
/// replies back. Ends when the relay does.
async fn link(front: UdpSocket, back: UdpSocket, mut told: ChannelReceiver<Told>) {
    let mut to: Option<SocketAddr> = None;
    let mut from: Option<SocketAddr> = None;
    let mut joined = true;
    let (mut drop_forward, mut drop_back) = (false, false);
    let mut pace: Option<Pace> = None;
    let (mut forward, mut backward) = (Paced::new(), Paced::new());
    let mut due = Vec::new();
    let mut datagram = vec![0u8; LARGEST];
    loop {
        // What waits on either side is forwarded, or dropped while the pair is cut, through each
        // way's queue. A datagram the OS refuses (its destination gone) is lost, as the network
        // loses one.
        while let Ok(Some((len, sender))) = front.try_recv_from(&mut datagram) {
            from = Some(sender);
            if joined
                && !drop_forward
                && let Some(to) = to
            {
                match pace {
                    Some(pace) => forward.take(pace, &datagram[..len], to),
                    None => {
                        let _ = back.send_to_writable(&datagram[..len], to).await;
                    }
                }
            }
        }
        while let Ok(Some((len, _))) = back.try_recv_from(&mut datagram) {
            if joined
                && !drop_back
                && let Some(from) = from
            {
                match pace {
                    Some(pace) => backward.take(pace, &datagram[..len], from),
                    None => {
                        let _ = front.send_to_writable(&datagram[..len], from).await;
                    }
                }
            }
        }
        let now = now_ns();
        let next_forward = forward.due(pace, now, &mut due);
        for (datagram, to) in due.drain(..) {
            let _ = back.send_to_writable(&datagram, to).await;
        }
        let next_back = backward.due(pace, now, &mut due);
        for (datagram, to) in due.drain(..) {
            let _ = front.send_to_writable(&datagram, to).await;
        }
        let next = next_forward.into_iter().chain(next_back).min();
        let woke = {
            let (front, back) = (pin!(front.readable()), pin!(back.readable()));
            match next {
                None => match race3(front, back, told.recv()).await {
                    Either3::Third(heard) => Some(heard),
                    Either3::First(_) | Either3::Second(_) => None,
                },
                // A queued datagram's turn comes.
                Some(at) => {
                    let turn = sleep(at.saturating_sub(now_ns()));
                    match race3(front, back, race2(told.recv(), turn)).await {
                        Either3::Third(Either::First(heard)) => Some(heard),
                        Either3::First(_)
                        | Either3::Second(_)
                        | Either3::Third(Either::Second(_)) => None,
                    }
                }
            }
        };
        match woke {
            Some(Ok(Told::To(at))) => to = Some(at),
            Some(Ok(Told::Joined(now))) => joined = now,
            Some(Ok(Told::DropForward(now))) => drop_forward = now,
            Some(Ok(Told::DropBack(now))) => drop_back = now,
            // Unpaced again, what waits goes at once.
            Some(Ok(Told::Pace(now))) => pace = now,
            // The relay is done.
            Some(Err(_)) => return,
            None => {}
        }
    }
}

/// The relay's shard: its root, a task a link, and two sockets a link, each read and written. The
/// watchdog's budget and the wheel's tick are hyper-rt-transport's test harness's
/// (`tests/exchanges.rs`).
fn runtime(links: usize) -> LocalRuntime {
    LocalRuntime::new(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: links + 1,
        timers_per_shard: links + 1,
        interests_per_shard: interests_for(1).max(2 * 2 * links),
        ring_entries: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: usize::try_from(Facts::query().page.base).unwrap(),
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    })
    .unwrap()
}

/// The relay: each link's orders, and where each node is told each peer listens.
pub struct Relay {
    /// By (from, to): the QUIC link's orders and the plane's.
    links: BTreeMap<(u64, u64), [Sender<Told>; 2]>,
    /// By (from, to): where `from` is told `to` listens, QUIC's address and the plane's.
    fronts: BTreeMap<(u64, u64), (SocketAddr, SocketAddr)>,
    /// Dropped, the relay ends.
    stop: Option<Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Relay {
    /// A relay between `nodes`, every pair joined, forwarding nowhere until told where each node
    /// listens ([`Relay::listen`]).
    pub fn start(nodes: &[u64]) -> Self {
        let pairs: Vec<(u64, u64)> = nodes
            .iter()
            .flat_map(|from| nodes.iter().map(move |to| (*from, *to)))
            .filter(|(from, to)| from != to)
            .collect();
        let (made, taken) = mpsc::channel();
        let (stop, stopped) = channel::<()>(1).unwrap();
        let mut links = BTreeMap::new();
        let mut receivers = Vec::new();
        for pair in &pairs {
            let (quic, quic_told) = channel(TOLD).unwrap();
            let (plane, plane_told) = channel(TOLD).unwrap();
            links.insert(*pair, [quic, plane]);
            receivers.push((*pair, [quic_told, plane_told]));
        }
        let thread = std::thread::spawn(move || {
            let mut rt = runtime(2 * receivers.len());
            let mut fronts = BTreeMap::new();
            for (pair, told) in receivers {
                let mut at = Vec::new();
                for told in told {
                    let front = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
                    let back = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
                    at.push(front.local_addr().unwrap());
                    rt.spawn(link(front, back, told)).unwrap();
                }
                fronts.insert(pair, (at[0], at[1]));
            }
            made.send(fronts).unwrap();
            let mut stopped = stopped;
            // The root waits for the test to drop the relay; returning ends every link.
            rt.block_on(async move { while stopped.recv().await.is_ok() {} })
                .unwrap();
        });
        let fronts = taken.recv().unwrap();
        Self {
            links,
            fronts,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    /// Where `from` is told `to` listens: QUIC's address and the plane's.
    pub fn front(&self, from: u64, to: u64) -> (SocketAddr, SocketAddr) {
        self.fronts[&(from, to)]
    }

    /// Where node `id` listens now: every link to it forwards there.
    pub fn listen(&self, id: u64, quic: SocketAddr, plane: SocketAddr) {
        for ((_, to), [q, p]) in &self.links {
            if *to == id {
                q.try_send(Told::To(quic))
                    .map_err(|_| "a link's orders are full")
                    .unwrap();
                p.try_send(Told::To(plane))
                    .map_err(|_| "a link's orders are full")
                    .unwrap();
            }
        }
    }

    /// Drops every datagram the other nodes' planes send node `id`'s, or none: on each peer's link
    /// to it, and as the replies on its link to each peer. What it sends, and QUIC, pass as they did.
    pub fn deafen_plane(&self, id: u64, deaf: bool) {
        for ((from, to), [_, plane]) in &self.links {
            let told = if *to == id {
                Told::DropForward(deaf)
            } else if *from == id {
                Told::DropBack(deaf)
            } else {
                continue;
            };
            plane
                .try_send(told)
                .map_err(|_| "a link's orders are full")
                .unwrap();
        }
    }

    /// Paces every QUIC link, both ways, as `pace` says, or none: the planes pass as they did.
    pub fn pace(&self, pace: Option<Pace>) {
        for [quic, _] in self.links.values() {
            quic.try_send(Told::Pace(pace))
                .map_err(|_| "a link's orders are full")
                .unwrap();
        }
    }

    /// Joins `a` and `b`, both ways, or cuts them.
    pub fn join(&self, a: u64, b: u64, joined: bool) {
        for pair in [(a, b), (b, a)] {
            for link in &self.links[&pair] {
                link.try_send(Told::Joined(joined))
                    .map_err(|_| "a link's orders are full")
                    .unwrap();
            }
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.stop = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
