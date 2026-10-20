//! SWIM between real processes over the sealed datagram plane on real UDP sockets: the usage
//! mantle, focal and slates make of it.
//!
//! The supervisor (`a_killed_member_is_declared_dead_by_every_survivor_and_no_live_one_is`)
//! starts `NODES` copies of this test binary as member processes (`member_process`, selected by
//! `HYPER_SWIM_NODE`). Each runs the detector as the library configures it: it polls when the
//! detector asks, sends what the detector returns (its probes, the answers, and the chunks of its
//! view its anti-entropy exchanges ask), and prints, every period, its record since the last (its
//! probes, asks, answers and findings, [`Record`]) and a line of its view, the detection bound its
//! detector states and what it reports of each peer. The test measures nothing of its own and
//! derives no bound. The supervisor waits on facts, each for as long as the members
//! move toward it: a quiet period derived from what they state (the longest detection bound a live
//! member states, never less than RFC 6298's one-second retransmission timeout) that passes with
//! nothing moving fails the wait with every member's last line, and so does a pair that takes more
//! round trips without its own configuration than any window of its estimator holds
//! (`hyper_timing::WINDOW_LIMIT`), or a member whose output ends, its process exited, unless the
//! supervisor killed it. In two phases:
//! - every member judges every peer by a configured verdict, the pair's own or, while the pair's
//!   estimator refuses, the pool's; then it SIGKILLs one member ([`POOLED`]), which within a few
//!   hundred milliseconds of the start is judged mostly by the pools;
//! - every survivor reports it dead; then the run goes on until every surviving pair is judged by
//!   its own estimator, and the supervisor SIGKILLs another ([`OWNED`]);
//! - every survivor reports that one dead too. Each survivor holds each victim dead within the
//!   detection bound its detector stated, with the wait it measured for evidence of its own health
//!   added where its condemnation was pending on it.
//!
//! What it asserts of every suspicion and condemnation is the detector's rule, exactly, from the
//! members' own records (`docs/timing.md` §2.7). Each member records every probe it sends, with the
//! deadline its detector stated as it was sent, every ping-request, every answer and nack it hands
//! its detector, every ping it answers, and what each poll found ([`Detector::findings`]). A member
//! relays as the library asks: each ping it sends for another member carries its own deadline for
//! the target, and with no answer by then it nacks (Lifeguard's nack). Each suspicion, and each
//! condemnation made pending, is traced to its probe: sent when the finding says, with the
//! deadline it states; its period ended no earlier than that deadline nor, where relays were asked,
//! than theirs, unless every relay asked had nacked; no answer, direct or relayed, handed to the
//! detector before the end. Then to the answer that missed it, handed over late, or was lost, the
//! target's record saying whether the ping reached it. Each condemnation is traced to the pending
//! one it follows, with no answer from its target to the told probe or a later one handed over
//! since, and to the answer from another member it was made at; and every count a member reports
//! is its findings'. Theorem 7's allowance for live members, `Σβ` over every judged probe, is
//! printed beside their counts: a report, not a test, for a count is what the rule found, not a
//! draw.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::print_stdout,
    clippy::cognitive_complexity,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::io::{BufRead, BufReader, Write};
use std::net::UdpSocket;
use std::num::NonZeroUsize;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use hyper_datagram::{
    AdmitAll, ExporterSecret, LENGTH_BYTES, OVERHEAD_BYTES, Plane, PlaneLimits, Role, SECRET_BYTES,
};
use hyper_swim::HostId;
use hyper_swim::codec::{Coordinate, GossipBatch, SwimMessage, gossip_capacity};
use hyper_swim::detector::{Detector, Finding, Judge, PeerReport, PingReq, Unanswered};
use hyper_swim::membership::{Liveness, MemberState};
use hyper_timing::{Exposure, WINDOW_LIMIT};

/// Members in the test cluster: two victims, a prober, and two members it can ask to relay once both
/// are dead.
const NODES: u64 = 5;
/// The member killed once every pair is judged, mostly by the pools.
const POOLED: u64 = NODES;
/// The member killed once every surviving pair is judged by its own estimator.
const OWNED: u64 = NODES - 1;
/// The path's datagram size: QUIC's minimum, which every path carries (RFC 9000 §14.1).
const DATAGRAM: usize = 1_200;
fn secret_between(a: u64, b: u64) -> ExporterSecret {
    // Stands for the QUIC exporter both ends of a connection compute: one secret per pair.
    let (low, high) = (a.min(b), a.max(b));
    let mut bytes = [0u8; SECRET_BYTES];
    bytes[..8].copy_from_slice(&low.to_le_bytes());
    bytes[8..16].copy_from_slice(&high.to_le_bytes());
    ExporterSecret::new(bytes)
}

/// A peer's liveness as a member reports it; `F` for one its view has forgotten, once dead.
fn liveness_letter(liveness: Option<Liveness>) -> char {
    match liveness {
        Some(Liveness::Alive) => 'A',
        Some(Liveness::Suspect) => 'S',
        Some(Liveness::Dead) => 'D',
        None => 'F',
    }
}

/// One member process: runs until it is killed, or until its supervisor is gone (its output
/// closes), when it ends.
#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "real processes on the host's clock, threads and environment (CLAUDE.md §1a, end to end)"
)]
fn member_process() {
    let Ok(me) = std::env::var("HYPER_SWIM_NODE") else {
        return;
    };
    let me: u64 = me.parse().unwrap();
    let ports: Vec<u16> = std::env::var("HYPER_SWIM_PORTS")
        .unwrap()
        .split(',')
        .map(|port| port.parse().unwrap())
        .collect();
    let address = move |id: u64| format!("127.0.0.1:{}", ports[(id - 1) as usize]);
    let socket = UdpSocket::bind(address(me)).unwrap();

    let limits = PlaneLimits {
        max_peers: NODES as usize,
        epochs_per_peer: 2,
        window_limit: 1_024,
    };
    let mut plane = Plane::new(me, limits).unwrap();
    // The cluster is the placement: every member knows the others and no more.
    let members = NonZeroUsize::new(NODES as usize).unwrap();
    // std's `Instant` reads whole nanoseconds and states no coarser step.
    let mut detector = Detector::new(
        HostId(me),
        Exposure::new(),
        members,
        Duration::from_nanos(1),
        // No detection budget: every period lasts what its probe needs.
        Duration::ZERO,
    );
    for peer in (1..=NODES).filter(|peer| *peer != me) {
        let role = if me < peer {
            Role::Initiator
        } else {
            Role::Acceptor
        };
        plane
            .install_epoch(peer, 1, &secret_between(me, peer), role)
            .unwrap();
        plane.set_path(peer, DATAGRAM).unwrap();
        detector.join(HostId(peer)).unwrap();
    }

    // Ready, then wait for the supervisor's start: a member joins its peers only once they are up,
    // as a real member joins a peer once its transport connection to it exists.
    let mut stdout = std::io::stdout();
    if writeln!(stdout, "ready {me}")
        .and_then(|()| stdout.flush())
        .is_err()
    {
        return;
    }
    let mut start = String::new();
    if !matches!(std::io::stdin().read_line(&mut start), Ok(read) if read > 0) {
        return;
    }

    let mut member = Member {
        me,
        socket,
        plane,
        detector,
        address: Box::new(address),
        started: Instant::now(),
        gossip: 0,
        view_room: 0,
        buffer: vec![0u8; DATAGRAM],
        batch: Vec::new(),
        encoded: Vec::new(),
        requests: Vec::new(),
        relaying: BTreeMap::new(),
        dead_seen: BTreeMap::new(),
        tallies: BTreeMap::new(),
        records: String::new(),
    };
    member.gossip = member.gossip_room();
    member.view_room = member.view_room();
    let mut period = 0u64;
    loop {
        let began = member.step();
        member.note_deaths();
        if began {
            period += 1;
            if member.report(period, &mut stdout).is_err() {
                // The supervisor is gone: nothing reads this member any more.
                return;
            }
        }
        member.receive();
        member.note_deaths();
    }
}

/// One member's driver: its socket, plane and detector, and the probes it relays.
struct Member {
    me: u64,
    socket: UdpSocket,
    plane: Plane,
    detector: Detector,
    address: Box<dyn Fn(u64) -> String>,
    started: Instant,
    /// Gossip entries a message carries: what fits the datagram.
    gossip: usize,
    /// Members a chunk of this member's view carries: what fits the datagram beside it.
    view_room: usize,
    buffer: Vec<u8>,
    batch: Vec<(HostId, MemberState)>,
    encoded: Vec<u8>,
    requests: Vec<PingReq>,
    /// The probes this member relays, by asker and target: one an asker and target, so bounded by
    /// the membership's square.
    relaying: BTreeMap<(u64, u64), Relaying>,
    /// Each peer's death as this member last came to hold it, and whether it holds it still: how
    /// long after the peer's last answer, and the bound its detector stated then.
    dead_seen: BTreeMap<u64, Noted>,
    /// What its detector has reported of each peer, kept across the peer's being forgotten.
    tallies: BTreeMap<u64, Tally>,
    /// This member's record since its last line, in the order made ([`Record`]): written out with
    /// the line, so a count a line reports follows every finding it counts.
    records: String,
}

/// A probe a member relays for another (SWIM's indirect probe): its own nonce, who asked and with
/// which nonce, the deadline its detector stated for it, and whether it has nacked.
#[derive(Clone, Copy, Debug)]
struct Relaying {
    relayed: u64,
    asker: HostId,
    asked: u64,
    due: u64,
    nacked: bool,
}

/// A death a member came to hold: how long after the peer's last answer, the bound its detector
/// stated then (none while its probes judged nothing), and whether it holds the peer dead still.
#[derive(Clone, Copy, Debug, Default)]
struct Noted {
    after: Duration,
    within: Option<Duration>,
    held: bool,
}

/// The suspicions and condemnations a member's detector reports of a peer, with their allowances.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Counts {
    suspicions: u64,
    suspicion_allowance: f64,
    condemnations: u64,
    condemnation_allowance: f64,
}

impl Counts {
    fn of(report: Option<PeerReport>) -> Self {
        report.map_or_else(Self::default, |report| Self {
            suspicions: report.suspicions,
            suspicion_allowance: report.suspicion_allowance,
            condemnations: report.condemnations,
            condemnation_allowance: report.condemnation_allowance,
        })
    }

    fn plus(self, other: Self) -> Self {
        Self {
            suspicions: self.suspicions + other.suspicions,
            suspicion_allowance: self.suspicion_allowance + other.suspicion_allowance,
            condemnations: self.condemnations + other.condemnations,
            condemnation_allowance: self.condemnation_allowance + other.condemnation_allowance,
        }
    }

    /// Whether any count is below `other`'s: counts only grow while a report lives.
    fn below(self, other: Self) -> bool {
        self.suspicions < other.suspicions
            || self.suspicion_allowance < other.suspicion_allowance
            || self.condemnations < other.condemnations
            || self.condemnation_allowance < other.condemnation_allowance
    }
}

/// A peer's counts across its reports: a report that goes with its member, forgotten once dead
/// (a live member falsely condemned, then refuting late), or starts over, carries what it reached.
#[derive(Clone, Copy, Debug, Default)]
struct Tally {
    carried: Counts,
    last: Counts,
}

impl Tally {
    fn update(&mut self, report: Option<PeerReport>) -> Counts {
        let now = Counts::of(report);
        if now.below(self.last) {
            self.carried = self.carried.plus(self.last);
        }
        self.last = now;
        self.carried.plus(now)
    }
}

impl Member {
    /// Nanoseconds on this member's monotonic clock.
    fn now(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos()).unwrap()
    }

    /// The gossip entries that fit the datagram beside the largest message, an acknowledgement
    /// with this member's coordinate.
    fn gossip_room(&mut self) -> usize {
        let coordinate = *self.detector.coordinate();
        SwimMessage::Ack {
            from: HostId(self.me),
            nonce: u64::MAX,
            boot_nonce: self.me,
            configuration_version: 0,
            standing: None,
            promise_ns: 0,
            relay_within_ns: u64::MAX,
            gossip: GossipBatch::Entries(&[]),
            coordinate: Coordinate::Held(&coordinate),
        }
        .encode_into(&mut self.encoded);
        gossip_capacity(DATAGRAM - OVERHEAD_BYTES - LENGTH_BYTES, self.encoded.len())
    }

    /// The members a chunk of this member's view carries: what fits the datagram beside an empty
    /// one.
    fn view_room(&mut self) -> usize {
        SwimMessage::Sync {
            from: HostId(self.me),
            boot_nonce: self.me,
            digest: 0,
            pull: true,
            gossip: GossipBatch::Entries(&[]),
        }
        .encode_into(&mut self.encoded);
        gossip_capacity(DATAGRAM - OVERHEAD_BYTES - LENGTH_BYTES, self.encoded.len())
    }

    fn send(&mut self, to: u64, message: &SwimMessage<'_>) {
        message.encode_into(&mut self.encoded);
        // A refused message is a lost one; the detector measures losses.
        let _ = self.plane.queue(to, &self.encoded);
    }

    /// Polls the detector and sends what it asks, recording what the poll found, the relays it
    /// asks and the probe it sends with its stated deadline; and tells the asker of each probe it
    /// relays unanswered by its deadline so (Lifeguard's nack). Whether a period began.
    fn step(&mut self) -> bool {
        let now = self.now();
        let expired: Vec<((u64, u64), Relaying)> = self
            .relaying
            .iter()
            .filter(|(_, relaying)| !relaying.nacked && relaying.due <= now)
            .map(|(key, relaying)| (*key, *relaying))
            .collect();
        for (key, relaying) in expired {
            if let Some(held) = self.relaying.get_mut(&key) {
                held.nacked = true;
            }
            self.send(
                relaying.asker.0,
                &SwimMessage::Nack {
                    from: HostId(self.me),
                    target: HostId(key.1),
                    nonce: relaying.asked,
                    boot_nonce: self.me,
                    gossip: GossipBatch::Entries(&[]),
                },
            );
        }
        let mut requests = std::mem::take(&mut self.requests);
        let ping = self.detector.poll(now, &mut requests);
        for finding in self.detector.findings() {
            note_finding(&mut self.records, finding);
        }
        for request in &requests {
            let _ = writeln!(
                self.records,
                "r {} {} {} {now}",
                request.relay.0, request.target.0, request.nonce
            );
            self.send(
                request.relay.0,
                &SwimMessage::PingReq {
                    from: HostId(self.me),
                    target: request.target,
                    nonce: request.nonce,
                    gossip: GossipBatch::Entries(&[]),
                },
            );
        }
        self.requests = requests;
        if let Some(ping) = ping {
            let mut batch = std::mem::take(&mut self.batch);
            self.detector
                .ping_gossip_into(ping.to, self.gossip, &mut batch);
            // Told: the probe carries this member's suspicion of its target.
            let told = batch
                .iter()
                .any(|(host, state)| *host == ping.to && state.liveness == Liveness::Suspect);
            let _ = writeln!(
                self.records,
                "p {} {} {now} {} {}",
                ping.nonce,
                ping.to.0,
                ping.due_ns,
                u8::from(told)
            );
            self.send(
                ping.to.0,
                &SwimMessage::Ping {
                    from: HostId(self.me),
                    nonce: ping.nonce,
                    boot_nonce: self.me,
                    configuration_version: 0,
                    gossip: GossipBatch::Entries(&batch),
                },
            );
            self.batch = batch;
        }
        // The chunks of this member's view its anti-entropy exchanges ask: the answer it owes a
        // pull, the exchange it began.
        let mut batch = std::mem::take(&mut self.batch);
        while let Some(chunk) = self.detector.sync_into(self.view_room, &mut batch) {
            self.send(
                chunk.to.0,
                &SwimMessage::Sync {
                    from: HostId(self.me),
                    boot_nonce: self.me,
                    digest: chunk.digest,
                    pull: chunk.pull,
                    gossip: GossipBatch::Entries(&batch),
                },
            );
        }
        self.batch = batch;
        flush(&mut self.plane, &self.socket, &*self.address);
        ping.is_some()
    }

    /// When this member must run next: its detector's wake, or the deadline of a probe it relays
    /// that has not nacked.
    fn wake(&self) -> Option<u64> {
        let relayed = self
            .relaying
            .values()
            .filter(|relaying| !relaying.nacked)
            .map(|relaying| relaying.due)
            .min();
        match (self.detector.wake(), relayed) {
            (Some(own), Some(relayed)) => Some(own.min(relayed)),
            (own, relayed) => own.or(relayed),
        }
    }

    /// Waits for one datagram until this member's wake (or for one datagram, when it has none),
    /// and handles it. Only a timeout or a datagram ends the wait: any other error (Windows
    /// reports a reset on the next receive after a send to a closed port) skips it.
    fn receive(&mut self) {
        let timeout = match self.wake() {
            Some(at) => match at.checked_sub(self.now()).filter(|left| *left > 0) {
                Some(left) => Some(Duration::from_nanos(left)),
                None => return,
            },
            None => None,
        };
        // Waited for without taking it, and taken once there (`hyper_measure::wait::arrives`): a
        // datagram lost to a timed receive this detector would count as a heartbeat lost. A reset
        // the wait found is taken by a receive that does not wait, which clears it.
        if !hyper_measure::wait::arrives(&self.socket, timeout, &mut self.buffer).unwrap_or(true) {
            return;
        }
        self.socket.set_nonblocking(true).unwrap();
        let taken = self.socket.recv_from(&mut self.buffer);
        self.socket.set_nonblocking(false).unwrap();
        let Ok((length, _)) = taken else {
            return;
        };
        let stamp = self.now();
        let mut buffer = std::mem::take(&mut self.buffer);
        if let Ok(opened) = self.plane.open(&mut buffer[..length], &AdmitAll) {
            for bytes in opened.messages() {
                if let Ok(message) = SwimMessage::decode(bytes) {
                    self.handle(message, stamp);
                }
            }
        }
        self.buffer = buffer;
        flush(&mut self.plane, &self.socket, &*self.address);
    }

    fn handle(&mut self, message: SwimMessage<'_>, stamp: u64) {
        match message {
            SwimMessage::Ping {
                from,
                nonce,
                gossip,
                ..
            } => {
                let _ = writeln!(self.records, "g {} {nonce} {stamp}", from.0);
                self.detector.apply_gossip(gossip);
                let ack = self.detector.on_ping(from);
                let mut batch = std::mem::take(&mut self.batch);
                self.detector.ack_gossip_into(from, self.gossip, &mut batch);
                let coordinate = *self.detector.coordinate();
                self.send(
                    ack.to.0,
                    &SwimMessage::Ack {
                        from: HostId(self.me),
                        nonce,
                        boot_nonce: self.me,
                        configuration_version: 0,
                        standing: None,
                        promise_ns: 0,
                        relay_within_ns: ack.relay_within_ns,
                        gossip: GossipBatch::Entries(&batch),
                        coordinate: Coordinate::Held(&coordinate),
                    },
                );
                self.batch = batch;
            }
            SwimMessage::Ack {
                from,
                nonce,
                relay_within_ns,
                gossip,
                coordinate,
                ..
            } => {
                self.detector.apply_gossip(gossip);
                self.detector.learn_coordinate(from, coordinate);
                // An answer to a probe made for another member goes back to it, after a nack
                // too.
                let relayed = self
                    .relaying
                    .iter()
                    .find(|((_, target), relaying)| *target == from.0 && relaying.relayed == nonce)
                    .map(|(key, relaying)| (*key, *relaying));
                match relayed {
                    Some((key, relaying)) => {
                        self.relaying.remove(&key);
                        self.send(
                            relaying.asker.0,
                            &SwimMessage::IndirectAck {
                                from: HostId(self.me),
                                target: from,
                                nonce: relaying.asked,
                                boot_nonce: self.me,
                                gossip: GossipBatch::Entries(&[]),
                            },
                        );
                    }
                    None => {
                        let _ = writeln!(self.records, "a {} {nonce} {stamp}", from.0);
                        self.detector.on_ack(from, nonce, stamp, relay_within_ns);
                    }
                }
            }
            SwimMessage::PingReq {
                from,
                target,
                nonce,
                gossip,
            } => {
                self.detector.apply_gossip(gossip);
                let ping = self.detector.on_ping_req(from, target, stamp);
                self.relaying.insert(
                    (from.0, target.0),
                    Relaying {
                        relayed: ping.nonce,
                        asker: from,
                        asked: nonce,
                        due: ping.due_ns,
                        nacked: false,
                    },
                );
                self.send(
                    target.0,
                    &SwimMessage::Ping {
                        from: HostId(self.me),
                        nonce: ping.nonce,
                        boot_nonce: self.me,
                        configuration_version: 0,
                        gossip: GossipBatch::Entries(&[]),
                    },
                );
            }
            SwimMessage::Sync {
                from,
                digest,
                pull,
                gossip,
                ..
            } => self.detector.on_sync(from, digest, pull, gossip),
            SwimMessage::IndirectAck {
                target,
                nonce,
                gossip,
                ..
            } => {
                let _ = writeln!(self.records, "i {} {nonce} {stamp}", target.0);
                self.detector.apply_gossip(gossip);
                self.detector.on_indirect_ack(target, nonce, stamp);
            }
            SwimMessage::Nack {
                from,
                target,
                nonce,
                gossip,
                ..
            } => {
                let _ = writeln!(self.records, "n {} {} {nonce} {stamp}", from.0, target.0);
                self.detector.apply_gossip(gossip);
                self.detector.on_indirect_nack(from, target, nonce, stamp);
            }
        }
    }

    /// Notes, each time this member comes to hold a peer dead, how long after the peer's last
    /// answer that is, and the bound its detector states: to a pending condemnation, plus, where
    /// this member's own had become pending, the wait since for an answer from another member,
    /// which it measures. The last note is the death that stands: a live member falsely condemned
    /// and alive again dies afresh, and a death held before this member's probes judged anything
    /// is noted with no bound, since its own probes could not have found it.
    fn note_deaths(&mut self) {
        let now = self.now();
        for peer in (1..=NODES).filter(|peer| *peer != self.me) {
            let noted = self.dead_seen.entry(peer).or_default();
            match self
                .detector
                .membership()
                .state(HostId(peer))
                .map(|state| state.liveness)
            {
                Some(Liveness::Dead) if !noted.held => {}
                Some(Liveness::Alive | Liveness::Suspect) => {
                    noted.held = false;
                    continue;
                }
                // Held dead and noted, or forgotten once dead, which was noted then.
                _ => continue,
            }
            let report = self.detector.report(HostId(peer)).unwrap_or_default();
            let since =
                |at: Option<u64>| Duration::from_nanos(at.map_or(0, |at| now.saturating_sub(at)));
            let waited = since(report.pending_since_ns);
            let bound = self.detector.detection_bound(now);
            self.dead_seen.insert(
                peer,
                Noted {
                    after: since(report.last_answer_ns),
                    within: bound.map(|bound| bound.saturating_add(waited)),
                    held: true,
                },
            );
        }
    }

    /// This member's record since its last line, then one line: `me period detection_ns` (the
    /// detector's stated bound, 0 before it states one) then, per peer,
    /// `peer:letter:judged:own:taken:answered_ns_ago:suspicions:allowance:condemnations:
    /// allowance:dead_after_ns:dead_within_ns` (`-` for a peer that never answered, and for a
    /// death noted with no bound).
    fn report(&mut self, period: u64, out: &mut impl Write) -> std::io::Result<()> {
        let now = self.now();
        let detection = self
            .detector
            .detection_bound(now)
            .map_or(0, |bound| bound.as_nanos());
        let mut line = format!("{} {period} {detection}", self.me);
        for peer in (1..=NODES).filter(|peer| *peer != self.me) {
            let state = self.detector.membership().state(HostId(peer));
            let held = self.detector.report(HostId(peer));
            let counts = self.tallies.entry(peer).or_default().update(held);
            let report = held.unwrap_or_default();
            let noted = self.dead_seen.get(&peer).copied().unwrap_or_default();
            // Judged: an estimator's verdict times this member's probes of the peer, the pair's own
            // or, while the pair's estimator refuses, the pool's; not yet the pair's timer's.
            let judged = matches!(report.judge, Judge::Own | Judge::Pool);
            line.push_str(&format!(
                " {peer}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
                liveness_letter(state.map(|state| state.liveness)),
                u8::from(judged),
                u8::from(report.configured),
                self.detector.round_trips_taken(HostId(peer)).unwrap_or(0),
                report
                    .last_answer_ns
                    .map_or_else(|| "-".to_owned(), |at| now.saturating_sub(at).to_string()),
                counts.suspicions,
                counts.suspicion_allowance,
                counts.condemnations,
                counts.condemnation_allowance,
                noted.after.as_nanos(),
                noted
                    .within
                    .map_or_else(|| "-".to_owned(), |within| within.as_nanos().to_string()),
            ));
        }
        let mut text = std::mem::take(&mut self.records);
        text.push_str(&line);
        text.push('\n');
        out.write_all(text.as_bytes())?;
        text.clear();
        self.records = text;
        out.flush()
    }
}

/// A finding in a member's record: `S` or `P` (suspected, condemnation pending) `target nonce
/// sent_ns due_ns relays_due_ns|- ended_ns`, `C target pending_since_ns answered nonce at_ns`.
fn note_finding(records: &mut String, finding: &Finding) {
    let missed = |kind: char, missed: &Unanswered| {
        format!(
            "{kind} {} {} {} {} {} {}",
            missed.target.0,
            missed.nonce,
            missed.sent_ns,
            missed.due_ns,
            missed
                .relays_due_ns
                .map_or_else(|| "-".to_owned(), |due| due.to_string()),
            missed.ended_ns
        )
    };
    let line = match finding {
        Finding::Suspected(unanswered) => missed('S', unanswered),
        Finding::Pending(unanswered) => missed('P', unanswered),
        Finding::Condemned {
            target,
            pending_since_ns,
            answered,
            nonce,
            at_ns,
        } => format!(
            "C {} {pending_since_ns} {} {nonce} {at_ns}",
            target.0, answered.0
        ),
    };
    records.push_str(&line);
    records.push('\n');
}

/// One entry of a member's record, in the order the member made it, times on its own clock.
#[derive(Clone, Copy, Debug)]
enum Record {
    /// `p nonce to at due told`: a probe sent, the deadline its detector stated as it was sent, and
    /// whether it carried the member's suspicion of its target.
    Probe {
        nonce: u64,
        to: u64,
        at: u64,
        due: u64,
        told: bool,
    },
    /// `r relay target nonce at`: a relay asked to probe `target` for the probe `nonce`.
    Asked {
        relay: u64,
        target: u64,
        nonce: u64,
        at: u64,
    },
    /// `a from nonce at`: an answer to the member's probe `nonce`, handed to its detector.
    Answer { from: u64, nonce: u64, at: u64 },
    /// `i target nonce at`: `target`'s answer to the probe `nonce` through a relay, handed over.
    Relayed { target: u64, nonce: u64, at: u64 },
    /// `n relay target nonce at`: a relay's nack for the probe `nonce` of `target`, handed over.
    Nacked {
        relay: u64,
        target: u64,
        nonce: u64,
        at: u64,
    },
    /// `g from nonce at`: a ping from `from` answered.
    Pinged { from: u64, nonce: u64 },
    /// What a poll found.
    Found(Finding),
}

/// A record line, or `None` for a line that is not one.
fn record(line: &str) -> Option<Record> {
    let fields: Vec<&str> = line.split(' ').collect();
    let number = |at: usize| -> Option<u64> { fields.get(at)?.parse().ok() };
    let maybe = |at: usize| -> Option<Option<u64>> {
        match *fields.get(at)? {
            "-" => Some(None),
            value => value.parse().ok().map(Some),
        }
    };
    let unanswered = || -> Option<Unanswered> {
        Some(Unanswered {
            target: HostId(number(1)?),
            nonce: number(2)?,
            sent_ns: number(3)?,
            due_ns: number(4)?,
            relays_due_ns: maybe(5)?,
            ended_ns: number(6)?,
        })
    };
    Some(match *fields.first()? {
        "p" => Record::Probe {
            nonce: number(1)?,
            to: number(2)?,
            at: number(3)?,
            due: number(4)?,
            told: *fields.get(5)? == "1",
        },
        "r" => Record::Asked {
            relay: number(1)?,
            target: number(2)?,
            nonce: number(3)?,
            at: number(4)?,
        },
        "a" => Record::Answer {
            from: number(1)?,
            nonce: number(2)?,
            at: number(3)?,
        },
        "i" => Record::Relayed {
            target: number(1)?,
            nonce: number(2)?,
            at: number(3)?,
        },
        "n" => Record::Nacked {
            relay: number(1)?,
            target: number(2)?,
            nonce: number(3)?,
            at: number(4)?,
        },
        "g" => Record::Pinged {
            from: number(1)?,
            nonce: number(2)?,
        },
        "S" => Record::Found(Finding::Suspected(unanswered()?)),
        "P" => Record::Found(Finding::Pending(unanswered()?)),
        "C" => Record::Found(Finding::Condemned {
            target: HostId(number(1)?),
            pending_since_ns: number(2)?,
            answered: HostId(number(3)?),
            nonce: number(4)?,
            at_ns: number(5)?,
        }),
        _ => return None,
    })
}

fn flush(plane: &mut Plane, socket: &UdpSocket, address: &dyn Fn(u64) -> String) {
    plane.flush(|peer, datagram: Result<&[u8], hyper_datagram::Refusal>| {
        // A datagram the plane refused, or the socket did not take, is a lost one.
        if let Ok(datagram) = datagram {
            let _ = socket.send_to(datagram, address(peer));
        }
    });
}

/// Free loopback ports, one per member: bound, read and released.
fn free_ports() -> Vec<u16> {
    let sockets: Vec<UdpSocket> = (0..NODES)
        .map(|_| UdpSocket::bind("127.0.0.1:0").unwrap())
        .collect();
    sockets
        .iter()
        .map(|socket| socket.local_addr().unwrap().port())
        .collect()
}

/// The member processes, killed when the supervisor ends however it ends.
struct Members(BTreeMap<u64, Child>);

/// How `member`, whose output ended, ended. A member's output ends as its process exits; one that
/// still runs is ended here, so its status says so.
fn ended(members: &mut Members, member: u64) -> String {
    let Some(child) = members.0.get_mut(&member) else {
        return "not a member".to_owned();
    };
    let _ = child.kill();
    child
        .wait()
        .map_or_else(|error| error.to_string(), |status| status.to_string())
}

impl Drop for Members {
    fn drop(&mut self) {
        for child in self.0.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// What a member last reported of one peer.
#[derive(Clone, Copy, Debug, Default)]
struct Seen {
    letter: char,
    judged: bool,
    own: bool,
    /// Round trips the pair's own estimator has taken.
    taken: u64,
    /// How long before the line the peer last answered; `None` if it never has.
    answered: Option<Duration>,
    suspicions: u64,
    suspicion_allowance: f64,
    condemnations: u64,
    condemnation_allowance: f64,
    dead_after: Duration,
    /// The bound the member stated at the death that stands; `None` if it stated none.
    dead_within: Option<Duration>,
}

/// A member's latest line: when the supervisor read it, the member's period, the detection bound
/// its detector stated, and what it reported of each peer.
#[derive(Clone, Debug)]
struct Stated {
    at: Instant,
    period: u64,
    detection: Duration,
    peers: BTreeMap<u64, Seen>,
}

/// Every member's latest line.
type Reports = BTreeMap<u64, Stated>;

fn parse(line: &str) -> Option<(u64, Duration, BTreeMap<u64, Seen>)> {
    let mut fields = line.split(' ');
    fields.next()?.parse::<u64>().ok()?;
    let period = fields.next()?.parse().ok()?;
    let detection = Duration::from_nanos(fields.next()?.parse().ok()?);
    let mut peers = BTreeMap::new();
    for field in fields {
        let parts: Vec<&str> = field.split(':').collect();
        let [
            peer,
            letter,
            judged,
            own,
            taken,
            answered,
            s,
            sa,
            c,
            ca,
            after,
            within,
        ] = parts[..]
        else {
            return None;
        };
        let answered = match answered {
            "-" => None,
            ago => Some(Duration::from_nanos(ago.parse().ok()?)),
        };
        peers.insert(
            peer.parse().ok()?,
            Seen {
                letter: letter.chars().next()?,
                judged: judged == "1",
                own: own == "1",
                taken: taken.parse().ok()?,
                answered,
                suspicions: s.parse().ok()?,
                suspicion_allowance: sa.parse().ok()?,
                condemnations: c.parse().ok()?,
                condemnation_allowance: ca.parse().ok()?,
                dead_after: Duration::from_nanos(after.parse().ok()?),
                dead_within: match within {
                    "-" => None,
                    bound => Some(Duration::from_nanos(bound.parse().ok()?)),
                },
            },
        );
    }
    Some((period, detection, peers))
}

/// RFC 6298 §2.1 and §2.4: the retransmission timeout before any round trip is measured, and the
/// least it is ever set to after, one second: the quiet period before any member has stated a
/// bound (hyper-liveness's process test and hyper-durable-e2e's waits take it so).
const RTO: Duration = Duration::from_secs(1);

/// What the supervisor hears of a member: a line of its output, or `None` once its output ends.
type Heard = (u64, Option<String>);

/// The supervisor's view of the run: the member processes, what they report, and the members it
/// killed.
struct Supervisor {
    /// The member processes still running, killed when the supervisor ends however it ends.
    members: Members,
    heard: Receiver<Heard>,
    latest: Reports,
    /// The members killed, in order, each with every member's latest line when it was killed.
    killed: Vec<(u64, Reports)>,
    /// Each member's record, as far as the supervisor read it while the member lived.
    records: BTreeMap<u64, Vec<Record>>,
    /// Of each member and peer, the suspicions and condemnations its record has found so far.
    found: BTreeMap<(u64, u64), (u64, u64)>,
}

#[allow(
    clippy::disallowed_methods,
    reason = "the supervisor of real processes reads the host's clock (CLAUDE.md §1a, end to end)"
)]
impl Supervisor {
    fn is_killed(&self, member: u64) -> bool {
        self.killed.iter().any(|(victim, _)| *victim == member)
    }

    /// Every member not killed.
    fn alive(&self) -> impl Iterator<Item = u64> + '_ {
        (1..=NODES).filter(|member| !self.is_killed(*member))
    }

    /// The next line any member reports within `left`, folded in; nothing, past it. A member whose
    /// output ends, which it does once its process exits, fails the wait with its exit status,
    /// unless the supervisor killed it. A record line is kept; a line that reports counts its
    /// record has not found fails the wait.
    fn next(&mut self, left: Duration, what: &str) {
        let (member, line) = match self.heard.recv_timeout(left) {
            Ok(heard) => heard,
            Err(RecvTimeoutError::Timeout) => return,
            Err(RecvTimeoutError::Disconnected) => {
                panic!("{what}: every member's output ended\n{}", self.dump())
            }
        };
        if self.is_killed(member) {
            return;
        }
        let Some(line) = line else {
            let status = ended(&mut self.members, member);
            panic!("{what}: member {member} ended: {status}\n{}", self.dump());
        };
        if let Some(entry) = record(&line) {
            let counted = match entry {
                Record::Found(Finding::Suspected(missed)) => Some((missed.target.0, (1, 0))),
                Record::Found(Finding::Condemned { target, .. }) => Some((target.0, (0, 1))),
                _ => None,
            };
            if let Some((target, (suspicions, condemnations))) = counted {
                let held = self.found.entry((member, target)).or_default();
                held.0 += suspicions;
                held.1 += condemnations;
            }
            self.records.entry(member).or_default().push(entry);
            return;
        }
        if let Some((period, detection, peers)) = parse(&line) {
            for (peer, seen) in &peers {
                let (suspicions, condemnations) = self
                    .found
                    .get(&(member, *peer))
                    .copied()
                    .unwrap_or_default();
                assert!(
                    (seen.suspicions, seen.condemnations) == (suspicions, condemnations),
                    "{what}: member {member} reports {} suspicions and {} condemnations of {peer}, \
                     and its record finds {suspicions} and {condemnations}\n{}",
                    seen.suspicions,
                    seen.condemnations,
                    self.dump()
                );
            }
            let at = Instant::now();
            self.latest.insert(
                member,
                Stated {
                    at,
                    period,
                    detection,
                    peers,
                },
            );
        }
    }

    /// How long the members may go with nothing moving before a wait gives up: the longest
    /// detection bound a live member states, from a peer's last answer to its condemnation pending:
    /// two probe spacings of at most `2m − 1` periods and the told probe's period, `m` the most
    /// members its view has held besides it. Every step a wait waits on is stated within one
    /// spacing and two periods of the step before, which the bound holds for every `m ≥ 1`: a
    /// pair's next round trip, a victim's suspicion once its next probe goes unanswered, and its
    /// condemnation once the probe that told it goes unanswered too and another member answers (a
    /// member states its view as each period begins). Never less than a retransmission timeout, the
    /// wait before any member states one.
    fn quiet(&self) -> Duration {
        self.alive()
            .filter_map(|member| self.latest.get(&member))
            .map(|stated| stated.detection)
            .max()
            .unwrap_or(Duration::ZERO)
            .max(RTO)
    }

    /// What moves the members toward a wait's fact: of each pair of live members, its judgement,
    /// its own configuration and, until it has one, the round trips its estimator has taken, the
    /// evidence it gathers toward it; of each victim, each live member's view of it and its
    /// suspicions and condemnations of it. A configured pair's round trips, and how live members
    /// stand with one another, move toward no fact a wait waits on.
    fn signature(&self) -> Vec<u64> {
        let mut out = Vec::new();
        for member in self.alive() {
            let Some(stated) = self.latest.get(&member) else {
                continue;
            };
            out.push(member);
            for (peer, seen) in &stated.peers {
                out.push(*peer);
                if self.is_killed(*peer) {
                    out.extend([u64::from(seen.letter), seen.suspicions, seen.condemnations]);
                } else {
                    out.extend([
                        u64::from(seen.judged),
                        u64::from(seen.own),
                        if seen.own { 0 } else { seen.taken },
                    ]);
                }
            }
        }
        out
    }

    /// A pair of live members that has taken more round trips without its own configuration than
    /// any window of its estimator holds (`hyper_timing::WINDOW_LIMIT`): a pair whose evidence no
    /// window of its estimator resolves.
    fn unresolved(&self) -> Option<(u64, u64, u64)> {
        self.alive().find_map(|member| {
            self.latest
                .get(&member)?
                .peers
                .iter()
                .find_map(|(peer, seen)| {
                    (!self.is_killed(*peer) && !seen.own && seen.taken > WINDOW_LIMIT)
                        .then_some((member, *peer, seen.taken))
                })
        })
    }

    /// Waits until `fact` holds of what the members stated, while they move toward it. Fails with
    /// every member's last line once a quiet period passes with nothing moving, once a pair takes
    /// more round trips without its own configuration than any window holds, or once a member's
    /// output ends. A member that has stated nothing yet has no bound to wait by: it states once
    /// its process is scheduled, and the wait goes on through that, saying on stderr what it waits
    /// for. It prints how long the fact took, and the stillest stretch: the longest share of the
    /// quiet period then that passed with nothing moving.
    fn until(&mut self, what: &str, fact: impl Fn(&Self) -> bool) {
        let began = Instant::now();
        let mut seen = self.signature();
        let mut moved_at = began;
        let mut stillest = (Duration::ZERO, RTO);
        while !fact(self) {
            let quiet = self.quiet();
            let still = moved_at.elapsed();
            let Some(left) = quiet.checked_sub(still).filter(|left| !left.is_zero()) else {
                let silent: Vec<u64> = self
                    .members
                    .0
                    .keys()
                    .copied()
                    .filter(|id| !self.latest.contains_key(id))
                    .collect();
                assert!(
                    !silent.is_empty(),
                    "{what}: nothing moved for {still:?}, past the quiet period of {quiet:?}\n{}",
                    self.dump()
                );
                eprintln!("{what}: waiting for members {silent:?} to state anything");
                moved_at = Instant::now();
                continue;
            };
            self.next(left, what);
            if let Some((member, peer, taken)) = self.unresolved() {
                panic!(
                    "{what}: member {member} took {taken} round trips of {peer} without its own \
                     configuration, more than any window of its estimator holds\n{}",
                    self.dump()
                );
            }
            let now = self.signature();
            if now != seen {
                seen = now;
                let still = moved_at.elapsed();
                if still.as_secs_f64() / quiet.as_secs_f64()
                    > stillest.0.as_secs_f64() / stillest.1.as_secs_f64()
                {
                    stillest = (still, quiet);
                }
                moved_at = Instant::now();
            }
        }
        println!(
            "{what}: held after {:?}; nothing moved for at most {:?}, against a quiet period of \
             {:?}",
            began.elapsed(),
            stillest.0,
            stillest.1
        );
    }

    /// Kills `victim` with SIGKILL (TerminateProcess on Windows), keeping every member's latest
    /// line as it stood.
    fn kill(&mut self, victim: u64) {
        let mut child = self.members.0.remove(&victim).unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        self.killed.push((victim, self.latest.clone()));
    }

    /// Whether every member alive holds each victim dead, or forgotten once dead.
    fn victims_held_dead(&self) -> bool {
        self.killed.iter().all(|(victim, _)| {
            self.alive().all(|member| {
                self.latest
                    .get(&member)
                    .and_then(|stated| stated.peers.get(victim))
                    .is_some_and(|seen| matches!(seen.letter, 'D' | 'F'))
            })
        })
    }

    /// Whether `judged` holds of every pair of members alive.
    fn every_pair(&self, judged: impl Fn(&Seen) -> bool) -> bool {
        self.alive().all(|member| {
            self.alive().filter(|peer| *peer != member).all(|peer| {
                self.latest
                    .get(&member)
                    .and_then(|stated| stated.peers.get(&peer))
                    .is_some_and(&judged)
            })
        })
    }

    /// Every member's latest line, for a wait that failed.
    fn dump(&self) -> String {
        let ms = |duration: Duration| duration.as_secs_f64() * 1e3;
        let silent: Vec<u64> = self
            .members
            .0
            .keys()
            .copied()
            .filter(|id| !self.latest.contains_key(id))
            .collect();
        let killed: Vec<u64> = self.killed.iter().map(|(victim, _)| *victim).collect();
        let mut out = format!(
            "quiet period {:?}; killed {killed:?}; stated nothing yet {silent:?}",
            self.quiet()
        );
        for (member, stated) in &self.latest {
            out.push_str(&format!(
                "\n  member {member}{}: period {}, stated {:.1} ms ago, detection bound {:.3} ms",
                if self.is_killed(*member) {
                    " (killed)"
                } else {
                    ""
                },
                stated.period,
                ms(stated.at.elapsed()),
                ms(stated.detection),
            ));
            for (peer, seen) in &stated.peers {
                out.push_str(&format!(
                    "\n    peer {peer}: {} judged {} own {} taken {} last answer {} suspicions {} \
                     (allows {:.3}) condemnations {} (allows {:.3})",
                    seen.letter,
                    seen.judged,
                    seen.own,
                    seen.taken,
                    seen.answered.map_or_else(
                        || "never".to_owned(),
                        |ago| format!("{:.1} ms before the line", ms(ago))
                    ),
                    seen.suspicions,
                    seen.suspicion_allowance,
                    seen.condemnations,
                    seen.condemnation_allowance,
                ));
            }
        }
        out
    }
}

#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "real processes on the host's clock, threads and environment (CLAUDE.md §1a, end to end)"
)]
fn a_killed_member_is_declared_dead_by_every_survivor_and_no_live_one_is() {
    if std::env::var("HYPER_SWIM_NODE").is_ok() {
        return;
    }
    let ports = free_ports();
    let ports = ports
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let mut members = Members(
        (1..=NODES)
            .map(|id| {
                let child = Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "member_process",
                        "--nocapture",
                        "--test-threads=1",
                    ])
                    .env("HYPER_SWIM_NODE", id.to_string())
                    .env("HYPER_SWIM_PORTS", &ports)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap();
                (id, child)
            })
            .collect(),
    );
    let (sender, heard) = std::sync::mpsc::channel::<Heard>();
    for (id, child) in &mut members.0 {
        let id = *id;
        let stdout = child.stdout.take().unwrap();
        let sender = sender.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send((id, Some(line))).is_err() {
                    return;
                }
            }
            // The output ended: the member's process exited, or is exiting.
            let _ = sender.send((id, None));
        });
    }
    drop(sender);

    // Start every member together once all have bound their sockets. A member just started has
    // no bound yet to wait by: it is ready once its process is scheduled, and one whose output ends
    // first fails the wait.
    let mut ready = 0;
    while ready < NODES {
        let (id, line) = heard
            .recv()
            .expect("every member's output ended before it was ready");
        let Some(line) = line else {
            panic!(
                "member {id} ended before it was ready: {}",
                ended(&mut members, id)
            );
        };
        // libtest prints "test member_process ... " before the body runs, without a newline.
        if line.contains("ready ") {
            ready += 1;
        }
    }
    for child in members.0.values_mut() {
        writeln!(child.stdin.as_mut().unwrap(), "start").unwrap();
    }

    // Each phase waits on its fact, then kills its victim: every pair judged, mostly by the pools;
    // then the victim held dead and every surviving pair judged by its own estimator; then both
    // victims held dead.
    let mut supervisor = Supervisor {
        members,
        heard,
        latest: BTreeMap::new(),
        killed: Vec::new(),
        records: BTreeMap::new(),
        found: BTreeMap::new(),
    };
    supervisor.until("every member judges every peer", |run| {
        run.every_pair(|seen| seen.judged)
    });
    supervisor.kill(POOLED);
    supervisor.until(
        "every survivor holds the first victim dead",
        Supervisor::victims_held_dead,
    );
    supervisor.until(
        "every surviving pair is judged by its own estimator",
        |run| run.victims_held_dead() && run.every_pair(|seen| seen.own),
    );
    supervisor.kill(OWNED);
    supervisor.until(
        "every survivor holds both victims dead",
        Supervisor::victims_held_dead,
    );
    let Supervisor {
        members,
        latest,
        killed,
        records,
        ..
    } = supervisor;
    drop(members);

    let killed_at = |member: u64| killed.iter().position(|(victim, _)| *victim == member);
    for (index, (victim, _)) in killed.iter().enumerate() {
        // Every member alive at the kill, and its own record of the death, kept to its last line.
        for (member, stated) in &latest {
            if killed_at(*member).is_some_and(|at| at <= index) {
                continue;
            }
            let seen = stated.peers[victim];
            let Some(within) = seen.dead_within else {
                panic!(
                    "member {member} holds {victim} dead from before its probes judged anything"
                );
            };
            assert!(
                seen.dead_after <= within,
                "member {member} saw {victim} dead {:?} after its last answer, past its stated \
                 bound of {within:?}",
                seen.dead_after,
            );
        }
    }
    // Of live members: each pair's counts while its peer lived, at the peer's kill where the member
    // outlived it.
    let mut live = Counts::default();
    for (member, stated) in &latest {
        for (peer, seen) in &stated.peers {
            let seen = match killed_at(*peer) {
                Some(at) if killed_at(*member).is_none_or(|own| own > at) => {
                    killed[at].1[member].peers[peer]
                }
                _ => *seen,
            };
            live = live.plus(Counts {
                suspicions: seen.suspicions,
                suspicion_allowance: seen.suspicion_allowance,
                condemnations: seen.condemnations,
                condemnation_allowance: seen.condemnation_allowance,
            });
        }
    }
    for (index, (victim, reports)) in killed.iter().enumerate() {
        // The members alive when it was killed: it, and every member killed after it or never.
        let alive: Vec<u64> = (1..=NODES)
            .filter(|member| killed_at(*member).is_none_or(|at| at >= index))
            .collect();
        let pairs = alive.len() * (alive.len() - 1);
        let own = alive
            .iter()
            .flat_map(|member| {
                let alive = &alive;
                reports[member]
                    .peers
                    .iter()
                    .filter(move |(peer, _)| alive.contains(peer))
            })
            .filter(|(_, seen)| seen.own)
            .count();
        println!(
            "member {victim} killed with {own} of {pairs} pairs judged by their own estimator; \
             detection, from its last answer: {:?}",
            alive
                .iter()
                .filter(|member| *member != victim)
                .map(|member| {
                    let seen = latest[member].peers[victim];
                    (*member, seen.dead_after, seen.dead_within)
                })
                .collect::<Vec<_>>()
        );
    }
    let Counts {
        suspicions,
        suspicion_allowance,
        condemnations,
        condemnation_allowance,
    } = live;
    // A report, not a test: Theorem 7 bounds the expected count, and a count is what the rule
    // found.
    println!(
        "suspicions of live members {suspicions} (Theorem 7 allows {suspicion_allowance:.3}); \
         condemnations {condemnations} (allows {condemnation_allowance:.3})"
    );
    let victims: Vec<u64> = killed.iter().map(|(victim, _)| *victim).collect();
    let (traced, failures) = trace(&records, &victims);
    println!(
        "every finding traced: {} suspicions and {} condemnations made pending, each probe \
         unanswered by its stated deadline and its relays' ({} ended early, every relay having \
         nacked), its answer late in {} (the latest {:?} past its deadline), relayed late in {}, \
         lost in {}, its ping unheard by a live target in {} and by a killed one in {}; {} \
         condemnations, each after its pending one and another member's answer, with no answer \
         from its target since",
        traced.suspicions,
        traced.pending,
        traced.nacked,
        traced.late,
        traced.latest,
        traced.relayed_late,
        traced.answer_lost,
        traced.probe_lost,
        traced.target_killed,
        traced.condemnations,
    );
    assert!(
        failures.is_empty(),
        "{} findings do not trace to the detector's rule:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// What tracing every finding found: how many of each, and what became of the answers the
/// unanswered probes missed.
#[derive(Debug, Default)]
struct Traced {
    suspicions: u64,
    pending: u64,
    condemnations: u64,
    /// Answers handed to the detector after their probe's period ended.
    late: u64,
    /// The latest of them past its deadline.
    latest: Duration,
    /// Relayed answers handed over after the period ended.
    relayed_late: u64,
    /// Relay stages that ended before their timeout, every relay asked having nacked.
    nacked: u64,
    /// Pings the target's record answered whose answer the member never handed over.
    answer_lost: u64,
    /// Pings a target that ran to the end has no record of.
    probe_lost: u64,
    /// Pings a target killed has no record of.
    target_killed: u64,
}

/// Where a probe is in a member's record: its place, its target, when it was sent, the deadline
/// stated as it was sent, and whether it carried the suspicion of its target.
type Sent = (usize, u64, u64, u64, bool);

/// Of each nonce, where its answers are in a member's record: their places, who answered, when.
type Answers = BTreeMap<u64, Vec<(usize, u64, u64)>>;

/// Traces every finding in the members' records to the detector's rule, from those records
/// alone, and lists each way one fails it. A suspicion or a condemnation made pending: its probe
/// was sent when it says, with the deadline it states, as stated when sent; its period ended no
/// earlier than that deadline nor, where relays were asked (at or past it), than theirs, unless
/// every relay asked had nacked before it ended; no answer, direct or relayed, was handed to the
/// detector between the probe and the end; a pending one's probe carried the suspicion. A
/// condemnation: a pending one of its target ended when it says, before it, with no answer from
/// the target to its probe or a later one handed over since; and the probe it names, of another
/// member, was answered before it.
fn trace(records: &BTreeMap<u64, Vec<Record>>, killed: &[u64]) -> (Traced, Vec<String>) {
    // Every ping each member answered: the member, who pinged it, the ping's nonce.
    let pinged: BTreeSet<(u64, u64, u64)> = records
        .iter()
        .flat_map(|(member, made)| {
            made.iter().filter_map(move |entry| match *entry {
                Record::Pinged { from, nonce } => Some((*member, from, nonce)),
                _ => None,
            })
        })
        .collect();
    let mut traced = Traced::default();
    let mut failures = Vec::new();
    for (member, made) in records {
        let mut probes: BTreeMap<u64, Sent> = BTreeMap::new();
        let mut answers = Answers::new();
        let mut relayed = Answers::new();
        let mut asked: BTreeMap<u64, Vec<(usize, u64, u64, u64)>> = BTreeMap::new();
        let mut nacks: BTreeMap<u64, Vec<(usize, u64, u64, u64)>> = BTreeMap::new();
        for (place, entry) in made.iter().enumerate() {
            match *entry {
                Record::Probe {
                    nonce,
                    to,
                    at,
                    due,
                    told,
                } => {
                    probes.insert(nonce, (place, to, at, due, told));
                }
                Record::Answer { from, nonce, at } => {
                    answers.entry(nonce).or_default().push((place, from, at));
                }
                Record::Relayed { target, nonce, at } => {
                    relayed.entry(nonce).or_default().push((place, target, at));
                }
                Record::Asked {
                    relay,
                    target,
                    nonce,
                    at,
                } => {
                    asked
                        .entry(nonce)
                        .or_default()
                        .push((place, relay, target, at));
                }
                Record::Nacked {
                    relay,
                    target,
                    nonce,
                    at,
                } => {
                    nacks
                        .entry(nonce)
                        .or_default()
                        .push((place, relay, target, at));
                }
                Record::Pinged { .. } | Record::Found(_) => {}
            }
        }
        // The answers to `nonce` from `from` in a stretch of the record.
        let any = |answers: &Answers, nonce: u64, from: u64, within: &dyn Fn(usize) -> bool| {
            answers
                .get(&nonce)
                .into_iter()
                .flatten()
                .find(|(place, by, _)| *by == from && within(*place))
                .copied()
        };
        let mut pending: Vec<(usize, Unanswered)> = Vec::new();
        for (place, entry) in made.iter().enumerate() {
            let Record::Found(finding) = *entry else {
                continue;
            };
            let mut fail =
                |why: String| failures.push(format!("member {member}, {finding:?}: {why}"));
            match finding {
                Finding::Suspected(missed) | Finding::Pending(missed) => {
                    let target = missed.target.0;
                    match finding {
                        Finding::Suspected(_) => traced.suspicions += 1,
                        _ => {
                            traced.pending += 1;
                            pending.push((place, missed));
                        }
                    }
                    let Some(&(sent, to, at, due, told)) = probes.get(&missed.nonce) else {
                        fail("its probe is not in the member's record".to_owned());
                        continue;
                    };
                    if sent > place || to != target || at != missed.sent_ns {
                        fail(format!(
                            "its probe went to {to} at {at} ns, after it or not as it says"
                        ));
                    }
                    if due != missed.due_ns {
                        fail(format!("its probe was sent with the deadline {due:?}"));
                    }
                    if missed.ended_ns < missed.due_ns {
                        fail("its period ended before the deadline".to_owned());
                    }
                    let during = |at: usize| sent < at && at < place;
                    let asks: Vec<(usize, u64, u64, u64)> = asked
                        .get(&missed.nonce)
                        .into_iter()
                        .flatten()
                        .filter(|(at, ..)| during(*at))
                        .copied()
                        .collect();
                    if asks
                        .iter()
                        .any(|(_, relay, of, _)| *of != target || *relay == target)
                    {
                        fail(format!(
                            "a relay was asked for another target, or the target: {asks:?}"
                        ));
                    }
                    match missed.relays_due_ns {
                        None if asks.is_empty() => {}
                        Some(relays_due) if !asks.is_empty() => {
                            // Before its timeout, the relay stage ends only once every relay it
                            // asked has nacked, and no sooner than the last nack.
                            if missed.ended_ns < relays_due {
                                let nacked = |relay: u64| {
                                    nacks
                                        .get(&missed.nonce)
                                        .into_iter()
                                        .flatten()
                                        .find(|(at, by, of, _)| {
                                            *by == relay && *of == target && during(*at)
                                        })
                                        .map(|(.., at)| *at)
                                };
                                match asks
                                    .iter()
                                    .map(|(_, relay, ..)| nacked(*relay).ok_or(*relay))
                                    .collect::<Result<Vec<u64>, u64>>()
                                {
                                    Err(relay) => fail(format!(
                                        "its period ended before the relays' deadline, relay \
                                         {relay} not having nacked"
                                    )),
                                    Ok(nacked) if nacked.iter().any(|at| *at > missed.ended_ns) => {
                                        fail("its period ended before a relay's nack".to_owned());
                                    }
                                    Ok(_) => traced.nacked += 1,
                                }
                            }
                            if asks.iter().any(|(.., at)| *at < missed.due_ns) {
                                fail("relays were asked before the deadline".to_owned());
                            }
                        }
                        relays_due => fail(format!(
                            "relays asked {}, the relays' deadline {relays_due:?}",
                            asks.len()
                        )),
                    }
                    if any(&answers, missed.nonce, target, &during).is_some()
                        || any(&relayed, missed.nonce, target, &during).is_some()
                    {
                        fail(
                            "an answer was handed to the detector before its period ended"
                                .to_owned(),
                        );
                    }
                    if matches!(finding, Finding::Pending(_)) && !told {
                        fail("its probe did not carry the suspicion".to_owned());
                    }
                    // What became of the answer: handed over late, or lost.
                    let after = |at: usize| at > place;
                    if let Some((_, _, at)) = any(&answers, missed.nonce, target, &after) {
                        traced.late += 1;
                        traced.latest = traced
                            .latest
                            .max(Duration::from_nanos(at.saturating_sub(missed.due_ns)));
                    } else if any(&relayed, missed.nonce, target, &after).is_some() {
                        traced.relayed_late += 1;
                    } else if pinged.contains(&(target, *member, missed.nonce)) {
                        traced.answer_lost += 1;
                    } else if killed.contains(&target) {
                        traced.target_killed += 1;
                    } else {
                        traced.probe_lost += 1;
                    }
                }
                Finding::Condemned {
                    target,
                    pending_since_ns,
                    answered,
                    nonce,
                    at_ns,
                } => {
                    traced.condemnations += 1;
                    match pending.iter().rev().find(|(at, missed)| {
                        missed.target == target
                            && missed.ended_ns == pending_since_ns
                            && *at < place
                    }) {
                        None => {
                            fail("no condemnation of it was made pending when it says".to_owned());
                        }
                        // An answer from the target to the told probe or a later one, however
                        // late and by whatever path, takes the pending condemnation back.
                        Some(&(made, told)) => {
                            let since = |at: usize| made < at && at < place;
                            let later = |answers: &Answers| {
                                answers.range(told.nonce..).any(|(_, answers)| {
                                    answers
                                        .iter()
                                        .any(|(at, by, _)| *by == target.0 && since(*at))
                                })
                            };
                            if later(&answers) || later(&relayed) {
                                fail(
                                    "the target answered its told probe or a later one after the \
                                     condemnation was pending"
                                        .to_owned(),
                                );
                            }
                        }
                    }
                    if answered == target {
                        fail("its own answer condemned it".to_owned());
                    }
                    if at_ns < pending_since_ns {
                        fail("condemned before its condemnation was pending".to_owned());
                    }
                    match probes.get(&nonce) {
                        Some(&(sent, to, ..)) if to == answered.0 && sent < place => {
                            let between = |at: usize| sent < at && at < place;
                            if any(&answers, nonce, to, &between).is_none()
                                && any(&relayed, nonce, to, &between).is_none()
                            {
                                fail(format!(
                                    "no answer from {to} to the probe {nonce} before it"
                                ));
                            }
                        }
                        _ => fail(format!(
                            "the probe {nonce} of {} is not in the member's record before it",
                            answered.0
                        )),
                    }
                }
            }
        }
    }
    (traced, failures)
}
