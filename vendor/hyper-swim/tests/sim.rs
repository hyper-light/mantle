//! hyper-swim's first simulation (`docs/sim.md` §9, step S-2): the cluster test's members on
//! hyper-sim's world and network instead of processes and sockets.
//!
//! Five members, each with a clock of its own (an offset, a rate within RFC 5905's 15 ppm, timers
//! late by a drawn amount), send their messages encoded as on the plane, carried by the network's
//! LAN path, each arrival an event of the world. Once every member judges every peer by a verdict
//! (the pair's own or the pool's), one member is killed: its arrivals are dropped and its timer
//! disarmed. Every survivor then holds it dead within the detection bound its detector stated, with
//! the wait it measured for evidence of its own health added where its condemnation was pending on
//! it, and no member holds a live one dead. Every seed's run replays from its seed, so each check
//! is exact for the seeds named; the run-twice check refuses a run whose digests differ.
//!
//! The far-link runs (`docs/timing.md` §2.7, "A pair the pool does not fit") put some members 100 ms one way from the rest
//! ([`Shape`]), each pair joined with its handshake's round trip as slates' daemons join: a pair the
//! pooled verdict does not fit is never judged by it, is judged by its own timer until its own
//! estimator configures, and a member that dies before any far pair configured is still condemned
//! by every survivor within the bound its detector stated.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    clippy::panic_in_result_fn,
    missing_docs
)]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::time::Duration;

use hyper_sim::net::{Net, NetLimits, Path, Ticket};
use hyper_sim::{
    Clock, Discipline, Lateness, Limits, NodeId, Random, Record, SimError, Source, Step, World,
    twice,
};
use hyper_swim::HostId;
use hyper_swim::codec::{Coordinate, GossipBatch, SwimMessage, gossip_capacity};
use hyper_swim::detector::{Detector, Judge, PingReq};
use hyper_swim::membership::{Liveness, MemberState};
use hyper_timing::Exposure;

/// Members in the LAN cluster.
const NODES: u32 = 5;
/// The path's datagram size: QUIC's minimum, which every path carries (RFC 9000 §14.1).
const DATAGRAM: usize = 1_200;
/// RFC 5905's frequency tolerance, the most a member's clock runs fast or slow (§7.2).
const PHI_PPM: u64 = 15;

/// The steps a run may take: four times the most any seed took, 1,240 of seeds 0 to 15 (measured
/// 2026-10-04), so a run that takes many more has stopped converging.
const STEPS: u64 = 4 * 1_240;

/// The far side's one-way delay: slates' repro, two Docker networks 100 ms apart (slates
/// `docs/bugs/2026-10-07-a-far-member-condemns-the-near-side-by-its-pooled-deadline.md`).
const FAR_ONE_WAY_NS: u64 = 100_000_000;
/// The far path's jitter: slates' repro's 100 µs, so the far path is lossless and nearly fixed
/// and any condemnation of a live member is the detector's, not the path's.
const FAR_JITTER_NS: u64 = 100_000;
/// How long the far-link runs watch the live cluster: slates' repro's 30 s.
const WATCHED_NS: u64 = 30_000_000_000;

/// A cluster's topology: its members, which of them sit across the far path from the rest, which
/// pairs besides run over it, the steps its run may take, the detection budget its members'
/// owners state (zero for none), and whether the owners seed each detector with an hourly fleet's
/// failure history.
#[derive(Clone, Copy, Debug)]
struct Shape {
    nodes: u32,
    far: &'static [u32],
    far_pairs: &'static [(u32, u32)],
    steps: u64,
    budget: Duration,
    hourly: bool,
    cost: Duration,
}

/// A fleet whose nodes fail once an hour, as `docs/timing.md` §3 item 11 seeds one: a thousand
/// failures over a thousand hours of node time.
fn hourly_fleet() -> Exposure {
    let mut history = Exposure::new();
    history.on_exposure(Duration::from_secs(3_600 * 1_000));
    for _ in 0..1_000 {
        history.on_failure();
    }
    history
}

/// The LAN cluster every member of which is one switch from the others.
const LAN_CLUSTER: Shape = Shape {
    nodes: NODES,
    far: &[],
    far_pairs: &[],
    steps: STEPS,
    budget: Duration::ZERO,
    hourly: false,
    cost: Duration::ZERO,
};

/// slates' far-link repro: two near members, four 100 ms one way from them. Its steps: four times the
/// most any named seed took ([`FAR_STEPS_TAKEN`]).
const FAR_LINK: Shape = Shape {
    nodes: 6,
    far: &[2, 3, 4, 5],
    far_pairs: &[],
    steps: 4 * FAR_STEPS_TAKEN,
    budget: Duration::ZERO,
    hourly: false,
    cost: Duration::ZERO,
};

/// The most steps a named seed's far-link or all-far run took: 26,058, seed 43's far-link run until
/// every far pair configured (`record_far_link`, measured 2026-10-07 with the pool fed only by the
/// pairs it fits; 32,188 before, seed 12).
const FAR_STEPS_TAKEN: u64 = 26_058;

/// Two near members and one far one: every survivor of the far one's death is far from it.
const ALL_FAR: Shape = Shape {
    nodes: 3,
    far: &[2],
    far_pairs: &[],
    steps: 4 * FAR_STEPS_TAKEN,
    budget: Duration::ZERO,
    hourly: false,
    cost: Duration::ZERO,
};

impl Shape {
    /// Whether the pair `(a, b)` crosses the far path.
    fn crosses(&self, a: u32, b: u32) -> bool {
        self.far.contains(&a) != self.far.contains(&b)
            || self.far_pairs.contains(&(a, b))
            || self.far_pairs.contains(&(b, a))
    }

    /// The pair's one-way delay before jitter: the LAN's, plus the far path's where it crosses it.
    fn one_way_ns(&self, a: u32, b: u32) -> u64 {
        if self.crosses(a, b) {
            FAR_ONE_WAY_NS
        } else {
            Path::LAN.one_way_ns()
        }
    }

    /// The decisions a step makes at most: the strategy's pick of a tie, a timer's lateness, and a
    /// delay for each message a poll sends, a probe and a ping-request to each other member.
    fn decisions_per_step(&self) -> u64 {
        2 + u64::from(self.nodes)
    }

    fn limits(&self) -> Limits {
        Limits {
            events: 1_024,
            nodes: self.nodes as usize,
            streams: 64,
            steps: self.steps,
            trace_words: (self.steps * self.decisions_per_step()) as usize,
        }
    }

    fn net(&self) -> NetLimits {
        NetLimits {
            flows: (self.nodes * self.nodes) as usize,
            links: 0,
            nats: 0,
            link_messages: 0,
            messages: 1_024,
            bytes: 1_024 * DATAGRAM,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ev {
    Arrive(Ticket),
    /// A stalled member runs again ([`Sim::stall`]).
    Thaw,
}

fn host(node: NodeId) -> HostId {
    HostId(u64::from(node.0) + 1)
}

fn node_of(host: HostId) -> NodeId {
    NodeId(u32::try_from(host.0 - 1).unwrap())
}

/// A death a member came to hold: how long after the peer's last answer, and the bound its detector
/// stated then (none while its probes judged nothing).
#[derive(Clone, Copy, Debug)]
struct Noted {
    after: Duration,
    within: Option<Duration>,
    /// When the member came to hold it dead, on its own clock.
    at_ns: u64,
    /// The incarnation it died at, which names the death: only a state at a higher one, a
    /// refutation, ends it. A record forgotten and adopted again from a member the death had not
    /// reached, at the incarnation it died at or below, is the same death.
    incarnation: u64,
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

/// One member's driver, as the cluster test's: its detector and the probes it relays.
struct Member {
    detector: Detector,
    gossip: usize,
    view_room: usize,
    requests: Vec<PingReq>,
    batch: Vec<(HostId, MemberState)>,
    encoded: Vec<u8>,
    /// The probes this member relays, by asker and target: one an asker and target, so bounded by
    /// the membership's square. Keyed by target alone, a second asker's request about a target took
    /// the first's place, and the first, owed a nack, waited out its whole relay stage.
    relaying: BTreeMap<(u64, u64), Relaying>,
    /// Each peer's death as this member came to hold it.
    deaths: BTreeMap<u64, Noted>,
    /// The peers this member has judged by their timer's verdict (`mistake` 1, before the pair's own
    /// estimator configured), as polled.
    timed: std::collections::BTreeSet<u64>,
    /// Every probe this member sent, on its own clock, whether the verdict that judged it rests its
    /// period at the floor (the pair's own or the pool's), and its target.
    pings: Vec<(u64, bool, HostId)>,
    nodes: u32,
    alive: bool,
    /// While its process is stalled ([`Sim::stall`]): the datagrams that reached it meanwhile, each
    /// with its arrival on the member's clock, as a kernel receive stamp gives it.
    stalled: Option<Vec<(Vec<u8>, u64)>>,
}

struct Sim {
    shape: Shape,
    /// Member 0's clock when the run began: [`Sim::now`] counts from it.
    origin: u64,
    world: World<Ev>,
    net: Net<Vec<u8>>,
    members: Vec<Member>,
    /// Members whose next datagram the network loses: each member's first, in the run that loses
    /// every first probe.
    losing: Vec<bool>,
    /// Encoded messages to send, held so the members' borrows end first.
    outbox: Vec<(NodeId, NodeId, Vec<u8>)>,
}

impl Member {
    fn new(me: NodeId, shape: Shape) -> Self {
        let members = NonZeroUsize::new(shape.nodes as usize).unwrap();
        // The world's clocks read whole nanoseconds.
        let history = if shape.hourly {
            hourly_fleet()
        } else {
            Exposure::new()
        };
        let mut detector = Detector::new(
            host(me),
            history,
            members,
            Duration::from_nanos(1),
            shape.budget,
        );
        detector.set_condemnation_cost(shape.cost);
        for peer in (0..shape.nodes).map(NodeId).filter(|peer| *peer != me) {
            if shape.far.is_empty() && shape.far_pairs.is_empty() {
                detector.join(host(peer)).unwrap();
            } else {
                // As slates' daemons join: with the round trip the keying handshake measured.
                let round_trip = 2 * shape.one_way_ns(me.0, peer.0);
                detector
                    .join_measured(host(peer), Duration::from_nanos(round_trip))
                    .unwrap();
            }
        }
        let mut member = Self {
            detector,
            gossip: 0,
            view_room: 0,
            requests: Vec::new(),
            batch: Vec::new(),
            encoded: Vec::new(),
            relaying: BTreeMap::new(),
            deaths: BTreeMap::new(),
            timed: std::collections::BTreeSet::new(),
            pings: Vec::new(),
            nodes: shape.nodes,
            alive: true,
            stalled: None,
        };
        member.gossip = member.room(me, true);
        member.view_room = member.room(me, false);
        member
    }

    /// The gossip entries that fit the datagram beside the largest message (an acknowledgement
    /// with this member's coordinate), or the members a chunk of its view carries.
    fn room(&mut self, me: NodeId, gossip: bool) -> usize {
        let coordinate = *self.detector.coordinate();
        let message = if gossip {
            SwimMessage::Ack {
                from: host(me),
                nonce: u64::MAX,
                boot_nonce: u64::from(me.0),
                configuration_version: 0,
                standing: None,
                promise_ns: 0,
                relay_within_ns: u64::MAX,
                gossip: GossipBatch::Entries(&[]),
                coordinate: Coordinate::Held(&coordinate),
            }
        } else {
            SwimMessage::Sync {
                from: host(me),
                boot_nonce: u64::from(me.0),
                digest: 0,
                pull: true,
                gossip: GossipBatch::Entries(&[]),
            }
        };
        message.encode_into(&mut self.encoded);
        gossip_capacity(DATAGRAM, self.encoded.len())
    }

    fn queue(
        &mut self,
        outbox: &mut Vec<(NodeId, NodeId, Vec<u8>)>,
        me: NodeId,
        to: HostId,
        message: &SwimMessage<'_>,
    ) {
        message.encode_into(&mut self.encoded);
        outbox.push((me, node_of(to), self.encoded.clone()));
    }

    /// Polls the detector at `now` and queues what it asks: the relays, the probe, the view's
    /// chunks.
    fn step(&mut self, me: NodeId, now: u64, outbox: &mut Vec<(NodeId, NodeId, Vec<u8>)>) {
        // A relayed probe unanswered by its deadline: tell its asker so (Lifeguard's nack).
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
            let message = SwimMessage::Nack {
                from: host(me),
                target: HostId(key.1),
                nonce: relaying.asked,
                boot_nonce: u64::from(me.0),
                gossip: GossipBatch::Entries(&[]),
            };
            self.queue(outbox, me, relaying.asker, &message);
        }
        let mut requests = std::mem::take(&mut self.requests);
        let ping = self.detector.poll(now, &mut requests);
        for request in &requests {
            let message = SwimMessage::PingReq {
                from: host(me),
                target: request.target,
                nonce: request.nonce,
                gossip: GossipBatch::Entries(&[]),
            };
            self.queue(outbox, me, request.relay, &message);
        }
        self.requests = requests;
        let mut batch = std::mem::take(&mut self.batch);
        if let Some(ping) = ping {
            let judge = self.detector.report(ping.to).map(|report| report.judge);
            self.pings.push((
                now,
                matches!(judge, Some(Judge::Own | Judge::Pool)),
                ping.to,
            ));
            self.detector
                .ping_gossip_into(ping.to, self.gossip, &mut batch);
            let message = SwimMessage::Ping {
                from: host(me),
                nonce: ping.nonce,
                boot_nonce: u64::from(me.0),
                configuration_version: 0,
                gossip: GossipBatch::Entries(&batch),
            };
            self.queue(outbox, me, ping.to, &message);
        }
        while let Some(chunk) = self.detector.sync_into(self.view_room, &mut batch) {
            let message = SwimMessage::Sync {
                from: host(me),
                boot_nonce: u64::from(me.0),
                digest: chunk.digest,
                pull: chunk.pull,
                gossip: GossipBatch::Entries(&batch),
            };
            self.queue(outbox, me, chunk.to, &message);
        }
        self.batch = batch;
    }

    /// Hands a message that arrived at `stamp` to the detector, queuing what it answers.
    fn handle(
        &mut self,
        me: NodeId,
        message: SwimMessage<'_>,
        stamp: u64,
        outbox: &mut Vec<(NodeId, NodeId, Vec<u8>)>,
    ) {
        match message {
            SwimMessage::Ping {
                from,
                nonce,
                gossip,
                ..
            } => {
                self.detector.apply_gossip(gossip);
                let ack = self.detector.on_ping(from);
                let mut batch = std::mem::take(&mut self.batch);
                self.detector.ack_gossip_into(from, self.gossip, &mut batch);
                let coordinate = *self.detector.coordinate();
                let message = SwimMessage::Ack {
                    from: host(me),
                    nonce,
                    boot_nonce: u64::from(me.0),
                    configuration_version: 0,
                    standing: None,
                    promise_ns: 0,
                    relay_within_ns: ack.relay_within_ns,
                    gossip: GossipBatch::Entries(&batch),
                    coordinate: Coordinate::Held(&coordinate),
                };
                self.queue(outbox, me, ack.to, &message);
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
                let relayed = self
                    .relaying
                    .iter()
                    .find(|((_, target), relaying)| *target == from.0 && relaying.relayed == nonce)
                    .map(|(key, relaying)| (*key, *relaying));
                match relayed {
                    // The target's answer goes back to the asker, after a nack too.
                    Some((key, relaying)) => {
                        self.relaying.remove(&key);
                        let message = SwimMessage::IndirectAck {
                            from: host(me),
                            target: from,
                            nonce: relaying.asked,
                            boot_nonce: u64::from(me.0),
                            gossip: GossipBatch::Entries(&[]),
                        };
                        self.queue(outbox, me, relaying.asker, &message);
                    }
                    None => self.detector.on_ack(from, nonce, stamp, relay_within_ns),
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
                let message = SwimMessage::Ping {
                    from: host(me),
                    nonce: ping.nonce,
                    boot_nonce: u64::from(me.0),
                    configuration_version: 0,
                    gossip: GossipBatch::Entries(&[]),
                };
                self.queue(outbox, me, target, &message);
            }
            SwimMessage::Sync {
                from,
                digest,
                pull,
                gossip,
                ..
            } => {
                self.detector.on_sync(from, digest, pull, gossip);
            }
            SwimMessage::IndirectAck {
                target,
                nonce,
                gossip,
                ..
            } => {
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
                self.detector.apply_gossip(gossip);
                self.detector.on_indirect_nack(from, target, nonce, stamp);
            }
        }
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

    /// Notes each death this member comes to hold, as the cluster test does.
    fn note_deaths(&mut self, me: NodeId, now: u64) {
        for peer in (0..self.nodes).map(NodeId).filter(|peer| *peer != me) {
            if self
                .detector
                .report(host(peer))
                .is_some_and(|report| report.judge == Judge::Timer)
            {
                self.timed.insert(host(peer).0);
            }
            let state = self.detector.membership().state(host(peer));
            let id = host(peer).0;
            match state.map(|state| (state.liveness, state.incarnation)) {
                Some((Liveness::Dead, incarnation)) if !self.deaths.contains_key(&id) => {
                    let report = self.detector.report(host(peer)).unwrap_or_default();
                    let since = |at: Option<u64>| {
                        Duration::from_nanos(at.map_or(0, |at| now.saturating_sub(at)))
                    };
                    let waited = since(report.pending_since_ns);
                    let bound = self.detector.detection_bound(now);
                    self.deaths.insert(
                        id,
                        Noted {
                            after: since(report.last_answer_ns),
                            within: bound.map(|bound| bound.saturating_add(waited)),
                            at_ns: now,
                            incarnation,
                        },
                    );
                }
                Some((Liveness::Alive | Liveness::Suspect, incarnation))
                    if self
                        .deaths
                        .get(&id)
                        .is_some_and(|noted| incarnation > noted.incarnation) =>
                {
                    self.deaths.remove(&id);
                }
                _ => {}
            }
        }
    }
}

impl Sim {
    fn new(source: Source, lose_first: bool, shape: Shape) -> Self {
        let mut world = World::new(source, Discipline::Ordered, shape.limits()).unwrap();
        // Each member's clock: an offset, a rate within ±PHI and timers late by up to 60 µs, drawn
        // through the world from a stream of the member's own, so a run is its seed or its trace.
        for id in 0..shape.nodes {
            let clocks = world.stream("clock", &[u64::from(id)]).unwrap();
            let rate = i32::try_from(world.below(clocks, 2 * PHI_PPM + 1).unwrap()).unwrap()
                - PHI_PPM as i32;
            let clock = Clock {
                offset_ns: world.below(clocks, 1 << 40).unwrap(),
                rate_ppm: rate,
                wall_ns: 0,
                lateness: Lateness {
                    floor_ns: 1_000,
                    spread_ns: 60_000,
                },
            };
            assert_eq!(world.node(clock).unwrap(), NodeId(id));
        }
        let origin = world.monotonic(NodeId(0)).unwrap();
        let mut net = Net::new(shape.net());
        net.set_path(Path::LAN);
        for a in 0..shape.nodes {
            for b in (0..shape.nodes).filter(|b| shape.crosses(a, *b)) {
                net.set_pair_path(
                    NodeId(a),
                    NodeId(b),
                    Path::in_order(FAR_ONE_WAY_NS, FAR_JITTER_NS),
                )
                .unwrap();
            }
        }
        let members = (0..shape.nodes)
            .map(|id| Member::new(NodeId(id), shape))
            .collect();
        let mut sim = Self {
            shape,
            origin,
            world,
            net,
            members,
            losing: vec![lose_first; shape.nodes as usize],
            outbox: Vec::new(),
        };
        for id in 0..shape.nodes {
            sim.step(NodeId(id));
        }
        sim
    }

    /// Nanoseconds since the run began, on member 0's clock.
    fn now(&self) -> u64 {
        self.world.monotonic(NodeId(0)).unwrap() - self.origin
    }

    /// Polls member `node`, sends what it queued and arms its timer at its detector's wake.
    fn step(&mut self, node: NodeId) {
        let now = self.world.monotonic(node).unwrap();
        let member = &mut self.members[node.0 as usize];
        member.step(node, now, &mut self.outbox);
        member.note_deaths(node, now);
        let wake = member.wake();
        self.world.wake(node, wake).unwrap();
        self.flush();
    }

    fn flush(&mut self) {
        for (from, to, bytes) in std::mem::take(&mut self.outbox) {
            if std::mem::take(&mut self.losing[from.0 as usize]) {
                continue;
            }
            let length = bytes.len();
            self.net
                .send(&mut self.world, (from, to), bytes, length, Ev::Arrive)
                .unwrap();
        }
    }

    /// Runs until `done`, within the world's step budget.
    fn run(&mut self, what: &str, done: impl Fn(&Self) -> bool) {
        if let Err(stopped) = self.run_until(done) {
            panic!("{what}: {stopped}");
        }
    }

    /// Runs until `done`; `Err` naming why the world stopped first (nothing pending, or the step
    /// budget spent), for the record runs that must report a detector that never gets there.
    fn run_until(&mut self, done: impl Fn(&Self) -> bool) -> Result<(), String> {
        let mut strategy = Random;
        while !done(self) {
            match self
                .world
                .next(&mut strategy)
                .map_err(|refusal| refusal.to_string())?
            {
                Step::Wake { node } => {
                    let member = &self.members[node.0 as usize];
                    if member.alive && member.stalled.is_none() {
                        self.step(node);
                    }
                }
                Step::Event {
                    node,
                    event: Ev::Thaw,
                } => self.thaw(node),
                Step::Event {
                    node,
                    event: Ev::Arrive(ticket),
                } => {
                    let delivered = self
                        .net
                        .deliver(&mut self.world, ticket, Ev::Arrive)
                        .map_err(|refusal| refusal.to_string())?;
                    if let Some(delivery) = delivered
                        && self.members[node.0 as usize].alive
                    {
                        let stamp = self
                            .world
                            .monotonic(node)
                            .map_err(|refusal| refusal.to_string())?;
                        let member = &mut self.members[node.0 as usize];
                        if let Some(held) = member.stalled.as_mut() {
                            held.push((delivery.payload, stamp));
                            continue;
                        }
                        if let Ok(message) = SwimMessage::decode(&delivery.payload) {
                            member.handle(node, message, stamp, &mut self.outbox);
                        }
                        self.step(node);
                    }
                }
                Step::Idle => return Err("nothing pending".to_owned()),
                Step::Spent => return Err("the step budget is spent".to_owned()),
            }
        }
        Ok(())
    }

    /// Whether every live member judges every live peer by an estimator's verdict, the pair's own
    /// or the pool's, not the pair's timer.
    fn every_pair_judged(&self) -> bool {
        self.live().all(|(me, member)| {
            self.live()
                .filter(|(peer, _)| *peer != me)
                .all(|(peer, _)| {
                    matches!(
                        member
                            .detector
                            .report(host(peer))
                            .map(|report| report.judge),
                        Some(Judge::Own | Judge::Pool)
                    )
                })
        })
    }

    fn live(&self) -> impl Iterator<Item = (NodeId, &Member)> {
        self.members
            .iter()
            .enumerate()
            .filter(|(_, member)| member.alive)
            .map(|(id, member)| (NodeId(id as u32), member))
    }

    /// Whether every live member's pair with every live member across the far path has its own
    /// estimator's verdict.
    fn every_far_pair_configured(&self) -> bool {
        self.live().all(|(me, member)| {
            self.live()
                .filter(|(peer, _)| self.shape.crosses(me.0, peer.0))
                .all(|(peer, _)| {
                    member
                        .detector
                        .report(host(peer))
                        .is_some_and(|report| report.configured)
                })
        })
    }

    /// Stalls `node`'s process for `for_ns` of the world's time, as a host stalls a process
    /// (`docs/timing.md` §2.6): its timer does not fire and the datagrams that reach it wait, each
    /// stamped on arrival; at the thaw it handles them in order, then polls.
    fn stall(&mut self, node: NodeId, for_ns: u64) {
        self.stop(node);
        self.world.after(for_ns, node, Ev::Thaw).unwrap();
    }

    /// Stops `node`'s process, as `SIGSTOP` does, until [`Sim::thaw`]: a stall of no stated
    /// length.
    fn stop(&mut self, node: NodeId) {
        self.members[node.0 as usize].stalled = Some(Vec::new());
        self.world.wake(node, None).unwrap();
    }

    /// Runs `node`'s stalled process again: it handles what reached it meanwhile, in order, then
    /// polls. Nothing for a member not stalled.
    fn thaw(&mut self, node: NodeId) {
        let Some(held) = self.members[node.0 as usize].stalled.take() else {
            return;
        };
        for (payload, stamp) in held {
            let member = &mut self.members[node.0 as usize];
            if let Ok(message) = SwimMessage::decode(&payload) {
                member.handle(node, message, stamp, &mut self.outbox);
            }
        }
        if self.members[node.0 as usize].alive {
            self.step(node);
        }
    }

    /// Whether every live member but `subject` holds it in `liveness` (dead: as noted).
    fn held_by_the_others(&self, subject: NodeId, liveness: Liveness) -> bool {
        self.live()
            .filter(|(me, _)| *me != subject)
            .all(|(_, member)| {
                member
                    .detector
                    .membership()
                    .state(host(subject))
                    .is_some_and(|state| state.liveness == liveness)
            })
    }

    fn kill(&mut self, victim: NodeId) {
        self.members[victim.0 as usize].alive = false;
        self.world.wake(victim, None).unwrap();
    }

    /// Whether every survivor holds `victim` dead.
    fn held_dead(&self, victim: NodeId) -> bool {
        self.live()
            .all(|(_, member)| member.deaths.contains_key(&host(victim).0))
    }
}

/// One seed's run: judged, a member killed, every survivor holding it dead. The survivors' deaths
/// noted, and the digest of the run.
/// One run from `source`: judged, a member killed, every survivor holding it dead within its
/// bound and none holding a live one dead. The run's record.
fn run(source: Source) -> Result<Record, SimError> {
    let seed = match &source {
        Source::Seed(seed) => format!("seed {seed}"),
        Source::Trace(_) => "the trace".to_owned(),
        Source::Tape { .. } => "the tape".to_owned(),
    };
    let mut sim = Sim::new(source, false, LAN_CLUSTER);
    sim.run("every pair judged", Sim::every_pair_judged);
    kill_and_hold_dead_within_bounds(sim, &seed)
}

/// Kills the cluster's last member and runs until every survivor holds it dead: each within the bound
/// its detector stated, with the wait it measured for evidence of its own health added, and none
/// holding a live member dead before or after. The run's record.
fn kill_and_hold_dead_within_bounds(mut sim: Sim, seed: &str) -> Result<Record, SimError> {
    kill_and_check(&mut sim, seed);
    Ok(sim.world.finish())
}

/// [`kill_and_hold_dead_within_bounds`]'s kill and checks, the run going on after: the victim.
fn kill_and_check(sim: &mut Sim, seed: &str) -> NodeId {
    for (me, member) in sim.live() {
        assert!(
            member.deaths.is_empty(),
            "{seed}: member {me:?} holds a live member dead"
        );
    }
    let victim = NodeId(sim.shape.nodes - 1);
    sim.kill(victim);
    sim.run("the victim held dead by every survivor", |sim| {
        sim.held_dead(victim)
    });
    let mut noted = Vec::new();
    for (me, member) in sim.live() {
        for peer in member.deaths.keys() {
            assert_eq!(
                *peer,
                host(victim).0,
                "{seed}: member {me:?} holds live member {peer} dead"
            );
        }
        let death = member.deaths[&host(victim).0];
        let within = death
            .within
            .unwrap_or_else(|| panic!("{seed}: member {me:?} stated no bound"));
        assert!(
            death.after <= within,
            "{seed}: member {me:?} held the victim dead {:?} after its last answer, past its \
             stated bound {within:?}",
            death.after
        );
        assert!(
            death.after > Duration::ZERO,
            "{seed}: held dead after its last answer"
        );
        noted.push((me.0, death));
    }
    assert_eq!(noted.len(), (sim.shape.nodes - 1) as usize);
    victim
}

#[test]
fn a_killed_member_is_held_dead_by_every_survivor_within_its_bound_and_no_live_one_is() {
    // Every seed through the run-twice check (docs/sim.md §3.9): twice from its seed and once from
    // its trace, to one digest.
    for seed in 0..16 {
        twice(seed, run).unwrap_or_else(|refusal| panic!("seed {seed}: {refusal}"));
    }
}

/// slates' detection budget, its membership horizon (slates `lease::horizon_ns`, A-125): the budget
/// the first owner to state one states.
const SLATES_BUDGET_NS: u64 = 900_000_000;

/// The LAN cluster, each member's owner stating slates' detection budget.
const BUDGETED_LAN: Shape = Shape {
    budget: Duration::from_nanos(SLATES_BUDGET_NS),
    ..LAN_CLUSTER
};

/// One run from `source` of the LAN cluster under slates' detection budget: every pair judged by a
/// verdict that rests its period (the pair's own or the pool's), then one budget's time, then the
/// LAN run's kill and checks. Through the budget's time, each member's probe judged so is followed
/// by its next no sooner than the floor `D / (2(2m − 1) + 1)` on the member's own clock, `m` the
/// four peers its rounds hold.
fn budgeted(source: Source) -> Result<Record, SimError> {
    let seed = match &source {
        Source::Seed(seed) => format!("seed {seed}"),
        Source::Trace(_) => "the trace".to_owned(),
        Source::Tape { .. } => "the tape".to_owned(),
    };
    let mut sim = Sim::new(source, false, BUDGETED_LAN);
    sim.run("every pair judged at the floor", Sim::every_pair_judged);
    let budget = SLATES_BUDGET_NS;
    let floor = budget / (2 * (2 * u64::from(NODES - 1) - 1) + 1);
    let from: Vec<usize> = sim
        .members
        .iter()
        .map(|member| member.pings.len())
        .collect();
    let until = sim.now() + budget;
    sim.run("one budget's time", |sim| sim.now() >= until);
    for (me, member) in sim.live() {
        let pings = &member.pings[from[me.0 as usize]..];
        assert!(pings.len() > 1, "{seed}: member {me:?} probed");
        for pair in pings.windows(2) {
            let ((sent, rests, _), (next, _, _)) = (pair[0], pair[1]);
            assert!(
                !rests || next - sent >= floor,
                "{seed}: member {me:?} probed {} ns after a probe judged at the floor, {floor} ns",
                next - sent
            );
        }
    }
    kill_and_hold_dead_within_bounds(sim, &seed)
}

/// An owner's detection budget floors each judged period at the budget's share of one period
/// (slates' A-125, `docs/timing.md` §2.7): an idle member probes no faster than detection within
/// the budget needs, and a killed member is still held dead within every survivor's stated bound.
/// Before the floor (the budget taken and unused), a member probed a few hundred microseconds after
/// its last probe on every seed.
#[test]
fn a_member_under_a_detection_budget_probes_no_faster_than_its_floor() {
    for seed in 0..16 {
        twice(seed, budgeted).unwrap_or_else(|refusal| panic!("seed {seed}: {refusal}"));
    }
}

/// The LAN cluster, each member's owner seeding its detector with an hourly fleet's history.
const HOURLY_LAN: Shape = Shape {
    hourly: true,
    ..LAN_CLUSTER
};

/// One run from `source` of the LAN cluster seeded with an hourly fleet's history: once every pair
/// is judged by a verdict that rests (the pool's or its own), every verdict's margin is past its
/// interval, where `U` falls (an hour's MTBF against round trips of about half a millisecond); then
/// the LAN run's kill and checks.
fn hourly(source: Source) -> Result<Record, SimError> {
    let seed = match &source {
        Source::Seed(seed) => format!("seed {seed}"),
        Source::Trace(_) => "the trace".to_owned(),
        Source::Tape { .. } => "the tape".to_owned(),
    };
    let mut sim = Sim::new(source, false, HOURLY_LAN);
    sim.run("every pair judged", Sim::every_pair_judged);
    for (me, member) in sim.live() {
        for (peer, _) in sim.live().filter(|(peer, _)| *peer != me) {
            let verdict = member.detector.verdict(host(peer));
            assert!(
                verdict.is_some_and(|verdict| verdict.margin > verdict.interval),
                "{seed}: member {me:?} judges {peer:?} by {verdict:?}, its margin under its interval"
            );
        }
    }
    kill_and_hold_dead_within_bounds(sim, &seed)
}

/// A verdict's margin is searched over every margin, not only below the pair's interval: a pair's
/// probes never overlap, so the one-probe bound holds at any margin (`docs/timing.md` §2.7, "The
/// margin"). Before, the margin was held under `η − G` to keep Theorem 7's product to one factor,
/// and a member seeded with an hourly fleet judged its peers with margins under their interval on
/// every seed (slates saw that cap hold a verdict at a mistake bound of 0.713).
#[test]
fn a_verdict_s_margin_reaches_past_its_interval_where_unavailability_falls() {
    for seed in 0..16 {
        twice(seed, hourly).unwrap_or_else(|refusal| panic!("seed {seed}: {refusal}"));
    }
}

/// slates' fleet's three members on the LAN.
const THREE: Shape = Shape {
    nodes: 3,
    ..LAN_CLUSTER
};

/// How long a stall holds a member's process: the shortest correlation time the heartbeat traces
/// measured, 20 ms (`docs/timing.md` §2.6, item 6: 20 to 250 ms), past the few round trips three
/// later probes of the stalled member take.
const STALL_NS: u64 = 20_000_000;

/// One run from `source` of three members whose first two each stall once before any pool
/// configures, one after the other (slates' fleet at load 80, 2026-10-10): the member that stalls
/// neither then has every pair of its own made late once, each answer found its record reused.
/// Every member's pool configures, and then the LAN run's kill and checks.
fn stalled_early(source: Source) -> Result<Record, SimError> {
    let seed = match &source {
        Source::Seed(seed) => format!("seed {seed}"),
        Source::Trace(_) => "the trace".to_owned(),
        Source::Tape { .. } => "the tape".to_owned(),
    };
    let mut sim = Sim::new(source, false, THREE);
    let every_pair_answered = |sim: &Sim| {
        sim.live().all(|(me, member)| {
            sim.live().filter(|(peer, _)| *peer != me).all(|(peer, _)| {
                member
                    .detector
                    .report(host(peer))
                    .is_some_and(|report| report.last_answer_ns.is_some())
            })
        })
    };
    sim.run("every pair answered", every_pair_answered);
    for stalled in [NodeId(1), NodeId(2)] {
        assert!(
            sim.live()
                .all(|(_, member)| member.detector.pool().verdict.is_none()),
            "{seed}: a pool configured before the stalls"
        );
        sim.stall(stalled, STALL_NS);
        sim.run("the stall over", |sim| {
            sim.members[stalled.0 as usize].stalled.is_none()
        });
    }
    sim.run("every member's pool configured", |sim| {
        sim.live()
            .all(|(_, member)| member.detector.pool().verdict.is_some())
    });
    kill_and_hold_dead_within_bounds(sim, &seed)
}

/// A pair one stall made late feeds its member's pool again from its next answer in time
/// (`docs/timing.md` §2.7, "A pair the pool does not fit"). Before, each answer that found its
/// record reused marked its pair a misfit for good: member 0, both of whose peers stalled once,
/// fed its pool from neither again, and it never configured on any seed (slates: a pool at 5 round
/// trips and a silent peer judged by nothing for 4,000 periods).
#[test]
fn a_pool_fed_by_pairs_a_stall_made_late_configures() {
    for seed in 0..16 {
        twice(seed, stalled_early).unwrap_or_else(|refusal| panic!("seed {seed}: {refusal}"));
    }
}

/// One run from `source` of the LAN cluster in which member 1's process stops until every other
/// member has condemned it, then runs again and refutes its death: no member counts that death a
/// failure, so none prices its margins with an MTBF the false death shortened. Then the LAN run's
/// kill and checks, and on until every survivor forgets the victim, its death having stood: each
/// counts exactly that one failure.
fn stopped_and_refuted(source: Source) -> Result<Record, SimError> {
    let seed = match &source {
        Source::Seed(seed) => format!("seed {seed}"),
        Source::Trace(_) => "the trace".to_owned(),
        Source::Tape { .. } => "the tape".to_owned(),
    };
    let mut sim = Sim::new(source, false, LAN_CLUSTER);
    sim.run("every pair judged", Sim::every_pair_judged);
    let stopped = NodeId(1);
    sim.stop(stopped);
    sim.run("the stopped member condemned by every other", |sim| {
        sim.held_by_the_others(stopped, Liveness::Dead)
    });
    sim.thaw(stopped);
    sim.run("the stopped member alive again in every view", |sim| {
        sim.held_by_the_others(stopped, Liveness::Alive)
    });
    for (me, member) in sim.live() {
        assert_eq!(
            member.detector.exposure().failures(),
            0,
            "{seed}: member {me:?} counts a death that was refuted"
        );
    }
    let victim = kill_and_check(&mut sim, &seed);
    sim.run("the victim forgotten by every survivor", |sim| {
        sim.live()
            .all(|(_, member)| member.detector.membership().state(host(victim)).is_none())
    });
    for (me, member) in sim.live() {
        assert_eq!(
            member.detector.exposure().failures(),
            1,
            "{seed}: member {me:?} counts the death that stood"
        );
    }
    Ok(sim.world.finish())
}

/// One run from `source` of three members whose two peers of member 0 both die once every pair has
/// answered once, before any pool configures: member 0, its pool fed by nothing more, suspects
/// both, each by its pair's timer, and condemns neither
/// (it has no live member's answer to show its own network works).
fn every_peer_dead_early(source: Source) -> Result<Record, SimError> {
    let seed = match &source {
        Source::Seed(seed) => format!("seed {seed}"),
        Source::Trace(_) => "the trace".to_owned(),
        Source::Tape { .. } => "the tape".to_owned(),
    };
    let mut sim = Sim::new(source, false, THREE);
    sim.run("every pair answered", |sim| {
        sim.live().all(|(me, member)| {
            sim.live().filter(|(peer, _)| *peer != me).all(|(peer, _)| {
                member
                    .detector
                    .report(host(peer))
                    .is_some_and(|report| report.last_answer_ns.is_some())
            })
        })
    });
    assert!(
        sim.members[0].detector.pool().verdict.is_none(),
        "{seed}: member 0's pool configured before the deaths"
    );
    sim.kill(NodeId(1));
    sim.kill(NodeId(2));
    let suspected = |sim: &Sim, peer: u32| {
        sim.members[0]
            .detector
            .membership()
            .state(host(NodeId(peer)))
            .is_some_and(|state| state.liveness == Liveness::Suspect)
    };
    sim.run("member 0 suspecting both", |sim| {
        suspected(sim, 1) && suspected(sim, 2)
    });
    for peer in [1, 2] {
        let report = sim.members[0].detector.report(host(NodeId(peer)));
        assert!(
            report.is_some_and(|report| report.judge == Judge::Timer && report.condemnations == 0),
            "{seed}: member 0 suspects {peer} by its timer and condemns it not: {report:?}"
        );
    }
    Ok(sim.world.finish())
}

/// A member whose every peer dies before its pool configures still judges them (`docs/timing.md`
/// §2.7, "Before a pair's own estimator configures"): every probe is judged, by its pair's timer
/// where no estimator judges it. Before, nothing judged them: member 0's probes stayed measurement
/// only, and the world ran out of steps on every seed with both peers alive in its view.
#[test]
fn a_member_whose_peers_all_die_early_suspects_them() {
    for seed in 0..16 {
        twice(seed, every_peer_dead_early)
            .unwrap_or_else(|refusal| panic!("seed {seed}: {refusal}"));
    }
}

/// A false condemnation is no failure (`docs/timing.md` §2.7, "The margin"): a death counts in a
/// member's failure history once its record outlives its window unrefuted. Before, every adopted
/// death counted, the refuted ones too, so each false condemnation shortened the MTBF the margins
/// are priced with: every member that condemned the stopped member counted one failure on every
/// seed, and smaller margins buy more false condemnations.
#[test]
fn a_refuted_death_is_no_failure() {
    for seed in 0..16 {
        twice(seed, stopped_and_refuted).unwrap_or_else(|refusal| panic!("seed {seed}: {refusal}"));
    }
}

/// Every member's first datagram, its first probe, is lost: each member probes again at its
/// initial wait and every pair is judged. Waiting on its answer or another member instead, every
/// member waited on the others for ever and the world went idle (slates' daemons at a re-key).
#[test]
fn members_whose_first_probes_are_all_lost_probe_again() {
    for seed in 0..16 {
        twice(seed, lost_first_probes).unwrap_or_else(|refusal| panic!("seed {seed}: {refusal}"));
    }
}

/// One run from `source` with every member's first datagram lost, until every pair is judged.
fn lost_first_probes(source: Source) -> Result<Record, SimError> {
    let mut sim = Sim::new(source, true, LAN_CLUSTER);
    sim.run("every pair judged", Sim::every_pair_judged);
    Ok(sim.world.finish())
}

/// The far-link runs' seeds: 0 to 15, as the LAN runs', and [`STALE_POOL_SEEDS`].
const FAR_SEEDS: std::ops::Range<u64> = 0..16;

/// Seeds whose far-link run condemned a live member for the stale-pool cause: with the handshake
/// compared against the pool's verdict as held before `start` configured it, the probe that first
/// configured the pool judged a far pair by it. Found 2026-10-07 on this harness with that order
/// restored: seeds 1, 6, 25, 33, 36, 37, 43 and 52 of 0 to 63 each condemned a live near member once
/// in 30 s; 1 and 6 are in [`FAR_SEEDS`] already.
const STALE_POOL_SEEDS: &[u64] = &[25, 33, 36, 37, 43, 52];

/// Every own-probe condemnation of a live member: `(member, condemned, count)`.
fn condemned_live(sim: &Sim) -> Vec<(u32, u32, u64)> {
    let mut found = Vec::new();
    for (me, member) in sim.live() {
        for (peer, _) in sim.live().filter(|(peer, _)| *peer != me) {
            let count = member
                .detector
                .report(host(peer))
                .map_or(0, |report| report.condemnations);
            if count > 0 {
                found.push((me.0, peer.0, count));
            }
        }
    }
    found
}

/// slates' far-link repro on the simulated network: two near members, four 100 ms one way from
/// them, the path lossless with 100 µs of jitter, watched 30 s. No member condemns a live one, and
/// every far pair has left its timer's judgement for its own estimator's verdict by the end.
fn far_link(source: Source) -> Result<Record, SimError> {
    let mut sim = Sim::new(source, false, FAR_LINK);
    sim.run("30 s watched", |sim| sim.now() >= WATCHED_NS);
    no_live_member_condemned(&sim, "in 30 s");
    // Then on until every far pair's own estimator has configured: from then each is judged by its
    // own verdict, not its timer's.
    sim.run("every far pair configured", Sim::every_far_pair_configured);
    no_live_member_condemned(&sim, "once every far pair configured");
    for (me, member) in sim.live() {
        for (peer, _) in sim
            .live()
            .filter(|(peer, _)| sim.shape.crosses(me.0, peer.0))
        {
            let judge = member
                .detector
                .report(host(peer))
                .map(|report| report.judge);
            assert!(
                member.timed.contains(&host(peer).0),
                "member {me:?} judged far peer {peer:?} by its timer before its own estimator configured"
            );
            assert_eq!(
                judge,
                Some(Judge::Own),
                "member {me:?} judges configured far peer {peer:?} by its own verdict"
            );
        }
    }
    Ok(sim.world.finish())
}

/// No member has condemned a live one by its own probes, and none holds a live one dead.
fn no_live_member_condemned(sim: &Sim, when: &str) {
    let condemned = condemned_live(sim);
    assert!(
        condemned.is_empty(),
        "{when}: live members condemned (member, condemned, count): {condemned:?}"
    );
    for (me, member) in sim.live() {
        assert!(
            member.deaths.is_empty(),
            "{when}: member {me:?} holds a live member dead"
        );
    }
}

/// slates' repro (`no_live_member_is_condemned_across_a_lossless_far_link`), deterministic: every
/// named seed condemns no live member, and every far pair moves from its timer's judgement to its
/// own verdict. Each seed through the run-twice check.
#[test]
fn no_live_member_is_condemned_across_a_lossless_far_link() {
    for seed in FAR_SEEDS.chain(STALE_POOL_SEEDS.iter().copied()) {
        twice(seed, far_link).unwrap_or_else(|refusal| panic!("seed {seed}: {refusal}"));
    }
}

/// Two near members and one far one, the far one killed from the start: neither survivor ever
/// measures it, so neither pair has its own verdict, and each is judged by its timer. Every
/// survivor holds it dead, by its own probes, within the bound its detector stated, counted from
/// the kill: a member that never answered has no last answer to count from.
fn all_far_kill(source: Source) -> Result<Record, SimError> {
    let mut sim = Sim::new(source, false, ALL_FAR);
    let victim = NodeId(2);
    let killed_at: Vec<(NodeId, u64)> = sim
        .live()
        .filter(|(me, _)| *me != victim)
        .map(|(me, _)| (me, sim.world.monotonic(me).unwrap()))
        .collect();
    sim.kill(victim);
    sim.run("the far member held dead by every survivor", |sim| {
        sim.held_dead(victim)
    });
    for (me, killed) in killed_at {
        let member = &sim.members[me.0 as usize];
        let death = member.deaths[&host(victim).0];
        let within = death
            .within
            .unwrap_or_else(|| panic!("member {me:?} stated no bound"));
        let after = Duration::from_nanos(death.at_ns - killed);
        assert!(
            after <= within,
            "member {me:?} held the far member dead {after:?} after the kill, past its stated bound {within:?}"
        );
        assert!(
            member.timed.contains(&host(victim).0),
            "member {me:?} judged the never-measured far member by its timer"
        );
        assert!(
            member
                .detector
                .report(host(victim))
                .is_some_and(|report| report.condemnations > 0)
                || sim.live().any(|(other, held)| other != me
                    && held
                        .detector
                        .report(host(victim))
                        .is_some_and(|report| report.condemnations > 0)),
            "a survivor's own probes condemned the far member"
        );
    }
    let condemned = condemned_live(&sim);
    assert!(
        condemned.is_empty(),
        "live members condemned: {condemned:?}"
    );
    Ok(sim.world.finish())
}

/// Three members, the pair of members 1 and 2 over the far path, 100 ms one way: slates' fleet at
/// load 100, a member reachable only through a relay whose own way to it is slower than the asker's
/// direct way was.
const SLOW_RELAY: Shape = Shape {
    nodes: 3,
    far: &[],
    far_pairs: &[(1, 2)],
    steps: 4 * FAR_STEPS_TAKEN,
    budget: Duration::ZERO,
    hourly: false,
    cost: Duration::ZERO,
};

/// The probes of the member it reaches only through a relay that the asker sends while the run is
/// watched: ten times the two, a suspicion and its told probe, that condemned it before.
const RELAYED_PROBES: usize = 20;

/// One run from `source` of [`SLOW_RELAY`]: once every pair is judged, member 0 loses its direct
/// path to member 1 both ways, and reaches it only through member 2, over member 2's far path to it;
/// member 1 reaches member 0 only through member 2 as well. Until member 0 has probed member 1
/// [`RELAYED_PROBES`] times since, no member condemns a live one.
fn relayed_only(source: Source) -> Result<Record, SimError> {
    let seed = match &source {
        Source::Seed(seed) => format!("seed {seed}"),
        Source::Trace(_) => "the trace".to_owned(),
        Source::Tape { .. } => "the tape".to_owned(),
    };
    let mut sim = Sim::new(source, false, SLOW_RELAY);
    sim.run("every pair judged", Sim::every_pair_judged);
    let (asker, target) = (NodeId(0), NodeId(1));
    sim.net.partition(asker, target, true);
    sim.net.partition(target, asker, true);
    let from = sim.members[asker.0 as usize].pings.len();
    // Any member that comes to hold a live one dead, at any step: a death refuted and forgotten
    // later leaves no count behind.
    let condemned = |sim: &Sim| {
        sim.live().any(|(me, member)| {
            sim.live().any(|(peer, _)| {
                peer != me
                    && member
                        .detector
                        .membership()
                        .state(host(peer))
                        .is_some_and(|state| state.liveness == Liveness::Dead)
            })
        })
    };
    sim.run("the asker probed the target through the relay", |sim| {
        condemned(sim)
            || sim.members[asker.0 as usize].pings[from..]
                .iter()
                .filter(|(_, _, to)| *to == host(target))
                .count()
                >= RELAYED_PROBES
    });
    assert!(
        !condemned(&sim),
        "{seed}: a member holds a live one dead {} ms in",
        sim.now() / 1_000_000
    );
    Ok(sim.world.finish())
}

/// A member reachable only through a relay whose own leg to it is slow is never condemned: the
/// relay judges its leg by its own deadline for it, and the asker waits for its response as long
/// as the relay stated it might take (slates, 2026-10-10). When the asker timed the relay stage by
/// its own span for the target, its direct path's from before the cut, every relayed answer came
/// after the stage, and a member held a live one dead on all 16 seeds (on seed 0, member 0
/// condemned member 1 150 ms after the cut).
#[test]
fn a_member_reachable_only_through_a_slow_relay_is_never_condemned() {
    for seed in 0..16 {
        twice(seed, relayed_only).unwrap_or_else(|refusal| panic!("seed {seed}: {refusal}"));
    }
}

/// The hyper-raft review's liveness case for `swim-pair-deadline`: a member that dies before any far
/// pair configures, with every survivor far from it, is still condemned within its stated bound.
/// Before the provisional verdict, a misfit pair's probes judged nothing and it was never held dead.
#[test]
fn a_far_member_killed_before_any_far_pair_configures_is_held_dead_by_every_survivor() {
    for seed in FAR_SEEDS {
        twice(seed, all_far_kill).unwrap_or_else(|refusal| panic!("seed {seed}: {refusal}"));
    }
}

/// One far-link record run: false condemnations in [`WATCHED_NS`], then a far member killed and how
/// long until every survivor held it dead (`None` if it never was within the step budget), then the
/// steps the run took. Asserts nothing, so the same run measures a detector without the fix.
fn far_link_record(seed: u64) -> (u64, Option<Duration>, u64) {
    let mut sim = Sim::new(Source::Seed(seed), false, FAR_LINK);
    let watched = sim.run_until(|sim| sim.now() >= WATCHED_NS);
    let false_condemnations = condemned_live(&sim)
        .iter()
        .map(|(_, _, count)| *count)
        .sum::<u64>();
    let victim = NodeId(FAR_LINK.nodes - 1);
    let killed = sim.now();
    sim.kill(victim);
    let detected = watched
        .and_then(|()| sim.run_until(|sim| sim.held_dead(victim)))
        .ok()
        .map(|()| Duration::from_nanos(sim.now() - killed));
    (false_condemnations, detected, sim.world.steps())
}

/// One all-far record run: the far member killed from the start, and how long until both survivors
/// held it dead (`None` if never within the step budget), with the steps taken.
fn all_far_record(seed: u64) -> (Option<Duration>, u64) {
    let mut sim = Sim::new(Source::Seed(seed), false, ALL_FAR);
    let victim = NodeId(2);
    sim.kill(victim);
    let detected = sim
        .run_until(|sim| sim.held_dead(victim))
        .ok()
        .map(|()| Duration::from_nanos(sim.now()));
    (detected, sim.world.steps())
}

/// One far-link run until every far pair's own estimator configured: when, or `None` within the
/// step budget, and the steps taken.
fn configured_record(seed: u64) -> (Option<Duration>, u64) {
    let mut sim = Sim::new(Source::Seed(seed), false, FAR_LINK);
    let configured = sim
        .run_until(|sim| sim.now() >= WATCHED_NS && sim.every_far_pair_configured())
        .ok()
        .map(|()| Duration::from_nanos(sim.now()));
    (configured, sim.world.steps())
}

/// Prints the far-link and all-far records `docs/benchmarks.md` cites, per seed: false
/// condemnations a minute in the watched 30 s, a killed far member's detection time, the all-far
/// detection time, and the steps each run took (`FAR_STEPS_TAKEN`). Run on demand:
/// `cargo test -p hyper-swim --test sim --release -- --ignored --nocapture record_far_link`.
#[test]
#[ignore = "a record run for docs/benchmarks.md, printed, not checked"]
fn record_far_link() {
    let minutes = WATCHED_NS as f64 / 60e9;
    println!(
        "| seed | false condemnations / min | far kill detected | all-far detected | far pairs configured | steps |"
    );
    println!("|---|---|---|---|---|---|");
    for seed in FAR_SEEDS.chain(STALE_POOL_SEEDS.iter().copied()) {
        let (condemned, detected, steps) = far_link_record(seed);
        let (all_far, all_far_steps) = all_far_record(seed);
        let (configured, configured_steps) = configured_record(seed);
        println!(
            "| {seed} | {:.1} | {} | {} | {} | {} |",
            condemned as f64 / minutes,
            detected.map_or("never".to_owned(), |at| format!("{at:.2?}")),
            all_far.map_or("never".to_owned(), |at| format!("{at:.2?}")),
            configured.map_or("never".to_owned(), |at| format!("{at:.2?}")),
            steps.max(all_far_steps).max(configured_steps)
        );
    }
}

/// What one detection-budget record run measured.
struct BudgetRecord {
    /// The largest least detection bound of any member, read just before the kill.
    least: Duration,
    /// The widest margin of any member's verdict on a peer, read then.
    margin: Duration,
    /// The widest span (round trip and margin) of any such verdict.
    span: Duration,
    /// The most probes any member sent a second over the window before the kill.
    probes: f64,
    /// The slowest survivor's detection of the killed member, from its last answer.
    detected: Duration,
    /// The bound that survivor stated.
    stated: Duration,
}

/// One detection-budget record run of `shape` from `seed`: every pair judged, then the fleet's own
/// probing for two seconds or one budget, whichever is longer, read at its end as an owner reads it
/// in steady state, then a member killed. A pair's relay bound is restated only by its answers, and
/// at the floor each pair is probed at least every third of the budget (`m` of the `4m − 1`
/// periods), so a budget's time restates every bound at least three times: a shorter read takes the
/// bounds stated before the pairs' own estimators configured, each a timer's second. Its checks are
/// the kill's own: no live member held dead, each survivor within the bound it stated.
fn budget_record(shape: Shape, seed: u64) -> BudgetRecord {
    let mut sim = Sim::new(Source::Seed(seed), false, shape);
    sim.run("every pair judged", Sim::every_pair_judged);
    let from: Vec<usize> = sim
        .members
        .iter()
        .map(|member| member.pings.len())
        .collect();
    let window = nanos_of(shape.budget).max(2_000_000_000);
    let until = sim.now() + window;
    sim.run("the window", |sim| sim.now() >= until);
    let mut least = Duration::ZERO;
    let mut margin = Duration::ZERO;
    let mut span = Duration::ZERO;
    for (me, member) in sim.live() {
        let now = sim.world.monotonic(me).unwrap();
        least = least.max(member.detector.least_detection_bound(now).unwrap());
        for (peer, _) in sim.live().filter(|(peer, _)| *peer != me) {
            let verdict = member.detector.verdict(host(peer)).unwrap();
            margin = margin.max(verdict.margin);
            span = span.max(verdict.round_trip + verdict.margin);
        }
    }
    let sent = sim
        .members
        .iter()
        .zip(&from)
        .map(|(member, from)| member.pings.len() - from)
        .max()
        .unwrap();
    let victim = kill_and_check(&mut sim, &format!("seed {seed}"));
    let (detected, stated) = sim
        .live()
        .map(|(_, member)| {
            let death = member.deaths[&host(victim).0];
            (death.after, death.within.unwrap())
        })
        .max()
        .unwrap();
    BudgetRecord {
        least,
        margin,
        span,
        probes: sent as f64 / (window as f64 / 1e9),
        detected,
        stated,
    }
}

/// `duration` in whole nanoseconds, saturating.
fn nanos_of(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// The budgets the record runs: none, slates' membership horizon (`SLATES_BUDGET_NS`), and slates'
/// failover objective's placeholder, 10 s, the most its `D = F − confirmation` can be.
const RECORD_BUDGETS: [Duration; 3] = [
    Duration::ZERO,
    Duration::from_nanos(SLATES_BUDGET_NS),
    Duration::from_secs(10),
];

/// The condemnation costs the record runs: none, and a second to a thousand by decades, the range a
/// takeover's unavailability spans from one region's handoff to a fleet's rebuild.
const RECORD_COSTS: [Duration; 5] = [
    Duration::ZERO,
    Duration::from_secs(1),
    Duration::from_secs(10),
    Duration::from_secs(100),
    Duration::from_secs(1_000),
];

/// Prints the detection-budget records `docs/benchmarks.md` cites: for slates' LAN fleet of five,
/// its fleet of three and the far link, each member seeded with an hourly fleet's history, under
/// each budget `D` and condemnation cost, the worst over the seeds the budget test runs of: the
/// least detection bound the measured deadlines support, the widest verdict margin and span, the
/// probes a member sends a second, and a killed member's detection with the bound stated for it.
/// A budget is kept by every period only when it is at least the least bound. Run on demand:
/// `cargo test -p hyper-swim --test sim --release -- --ignored --nocapture record_detection_budget`.
#[test]
#[ignore = "a record run for docs/benchmarks.md, printed, not checked"]
fn record_detection_budget() {
    let lan3 = Shape {
        nodes: 3,
        ..HOURLY_LAN
    };
    let far = Shape {
        hourly: true,
        ..FAR_LINK
    };
    println!(
        "| fleet | D | cost | least bound | widest margin | widest span | probes/s | detected | stated |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for (fleet, base) in [
        ("LAN, 5", HOURLY_LAN),
        ("LAN, 3", lan3),
        ("far link, 6", far),
    ] {
        for budget in RECORD_BUDGETS {
            for cost in RECORD_COSTS {
                let shape = Shape {
                    budget,
                    cost,
                    ..base
                };
                let records: Vec<BudgetRecord> =
                    (0..16).map(|seed| budget_record(shape, seed)).collect();
                let worst =
                    |field: fn(&BudgetRecord) -> Duration| records.iter().map(field).max().unwrap();
                let probes = records.iter().map(|r| r.probes).fold(0.0, f64::max);
                println!(
                    "| {fleet} | {budget:.1?} | {cost:?} | {:.2?} | {:.1?} | {:.1?} | {probes:.1} | {:.2?} | {:.2?} |",
                    worst(|r| r.least),
                    worst(|r| r.margin),
                    worst(|r| r.span),
                    worst(|r| r.detected),
                    worst(|r| r.stated),
                );
            }
        }
    }
}
