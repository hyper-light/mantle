//! The network's contract (`docs/sim.md` §3.4): focal's path tests, carried onto the world, with
//! each draw checked against the model it is drawn by rather than against a statistical band; then
//! what the world adds: a stated capacity, duplication, and a flow's draws independent of others'.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    missing_docs
)]

use hyper_sim::net::{
    Adversary, Attack, Dropped, Fate, Link, Loss, Marking, Measured, Nat, Net, NetLimits,
    Partitions, Path, Split, Ticket,
};
use hyper_sim::{
    Clock, Discipline, Fifo, Limits, NodeId, PPM, Record, SimError, Source, Step, StreamId, World,
    twice,
};

const MILLISECOND: u64 = 1_000_000;
const SECOND: u64 = 1_000_000_000;

/// The most messages a test sends: the loss tests' 10,000, one at a time.
const MESSAGES: u64 = 10_000;
/// The most in flight at once: the replay scenario's 2,000, sent together, and room to spare.
const IN_FLIGHT: usize = 4_096;

const LIMITS: Limits = Limits {
    events: IN_FLIGHT,
    nodes: 5,
    streams: 64,
    // Each message a send, a delivery and, duplicated, a few more.
    steps: 4 * MESSAGES,
    // A message draws at most a delay and the loss channel's two.
    trace_words: (3 * MESSAGES) as usize,
};

const NET: NetLimits = NetLimits {
    flows: 64,
    links: 8,
    nats: 8,
    link_messages: IN_FLIGHT,
    messages: IN_FLIGHT,
    // The longest message a test sends is 1,400 bytes.
    bytes: IN_FLIGHT * 1_400,
};

/// How many of `messages` a test sends: all of them, or under Miri a twentieth, the same code over
/// fewer messages (as `tests/world.rs` runs fewer cases there). At full size this file took more
/// than 35 minutes in CI's Miri job, which cancelled it (0.26 s natively, 2026-10-04); the job's
/// budget of 36 minutes holds the whole crate, whose other tests took 16. Most of it was
/// `a_measured_path_draws_by_the_inverse_transform`, whose order-keeping path ties its arrivals at
/// a few instants, and the world hands a tied step every event of its tie.
fn sized(messages: u64) -> u64 {
    if cfg!(miri) { messages / 20 } else { messages }
}

/// What the harness schedules: an arrival.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ev {
    Arrive(Ticket),
}

/// A world of the nodes the tests name, 0 to 4.
fn world(seed: u64) -> World<Ev> {
    world_of(Source::Seed(seed))
}

/// The same world, its decisions drawn from `source`: a seed, or a trace to replay.
fn world_of(source: Source) -> World<Ev> {
    let mut world = World::new(source, Discipline::Ordered, LIMITS).unwrap();
    for id in 0..5 {
        assert_eq!(world.node(Clock::default()).unwrap(), NodeId(id));
    }
    world
}

fn node(id: u32) -> NodeId {
    NodeId(id)
}

fn send<P: Clone>(
    world: &mut World<Ev>,
    net: &mut Net<P>,
    from: u32,
    to: u32,
    message: P,
    bytes: usize,
) -> Fate {
    net.send(world, (node(from), node(to)), message, bytes, Ev::Arrive)
        .unwrap()
}

/// One step of the world: a delivery, if the step's arrival delivered one. `None` when idle.
fn step<P: Clone>(world: &mut World<Ev>, net: &mut Net<P>) -> Option<Option<(u64, P)>> {
    match world.next(&mut Fifo).unwrap() {
        Step::Event {
            event: Ev::Arrive(ticket),
            ..
        } => Some(
            net.deliver(world, ticket, Ev::Arrive)
                .unwrap()
                .map(|delivery| (world.now(), delivery.payload)),
        ),
        Step::Idle => None,
        other => panic!("{other:?}"),
    }
}

/// Every delivery until nothing is in flight: when and what.
fn drain<P: Clone>(world: &mut World<Ev>, net: &mut Net<P>) -> Vec<(u64, P)> {
    let mut delivered = Vec::new();
    while let Some(delivery) = step(world, net) {
        delivered.extend(delivery);
    }
    delivered
}

/// Delivers what arrives up to `at`, then moves the clock there.
fn until<P: Clone>(
    world: &mut World<Ev>,
    net: &mut Net<P>,
    at: u64,
    delivered: &mut Vec<(u64, P)>,
) {
    while world.earliest().is_some_and(|due| due <= at) {
        if let Some(delivery) = step(world, net) {
            delivered.extend(delivery);
        }
    }
    world.advance(at.max(world.now())).unwrap();
}

/// The stream the network names for `what` on the flow from `from` to `to`, in a world of its own
/// on the same seed: its draws are the network's, since a stream's draws depend on the seed and
/// the name alone.
fn reference(seed: u64, what: &'static str, from: u32, to: u32) -> (World<Ev>, StreamId) {
    let mut world = world(seed);
    let stream = world
        .stream(what, &[u64::from(from), u64::from(to)])
        .unwrap();
    (world, stream)
}

/// The Gilbert–Elliott channel by its definition, on a reference stream: whether each of `count`
/// messages is lost.
fn gilbert_elliott(seed: u64, loss: (u32, u32, u32, u32), count: usize) -> Vec<bool> {
    let (good_to_bad, bad_to_good, good_loss, bad_loss) = loss;
    let (mut world, stream) = reference(seed, "net.loss", 1, 2);
    let mut bad = false;
    (0..count)
        .map(|_| {
            bad = if bad {
                !world.chance(stream, bad_to_good).unwrap()
            } else {
                world.chance(stream, good_to_bad).unwrap()
            };
            world
                .chance(stream, if bad { bad_loss } else { good_loss })
                .unwrap()
        })
        .collect()
}

#[test]
fn the_zero_path_delivers_at_once_and_draws_nothing() {
    let mut world = world(7);
    let mut net = Net::new(NET);
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 10u64, 100),
        Fate::Arrives { at: 0 }
    );
    assert_eq!(world.decisions(), 0, "nothing drawn");
    assert_eq!(drain(&mut world, &mut net), vec![(0, 10)]);
}

#[test]
fn propagation_is_its_draw_inside_the_jitter_and_in_order() {
    let seed = 7;
    let mut world = world(seed);
    let mut net = Net::new(NET);
    net.set_path(Path::REGIONAL);
    let (mut draws, stream) = reference(seed, "net.delay", 1, 2);
    let mut expected = Vec::new();
    let mut previous = 0;
    for message in 0..sized(500) {
        world.advance(message * 1_000).unwrap();
        let fate = send(&mut world, &mut net, 1, 2, message, 100);
        // `one_way − jitter + U[0, 2·jitter]`, held behind the flow's previous arrival.
        let propagation = 60 * MILLISECOND + draws.below(stream, 40 * MILLISECOND + 1).unwrap();
        let at = (message * 1_000 + propagation).max(previous);
        previous = at;
        assert_eq!(fate, Fate::Arrives { at });
        expected.push((at, message));
    }
    assert_eq!(
        drain(&mut world, &mut net),
        expected,
        "in send order, at their draws"
    );
}

#[test]
fn a_reordering_path_lets_messages_overtake() {
    let seed = 7;
    let mut world = world(seed);
    let mut net = Net::new(NET);
    net.set_path(Path::reordering(80 * MILLISECOND, 20 * MILLISECOND));
    let (mut draws, stream) = reference(seed, "net.delay", 1, 2);
    let mut expected: Vec<(u64, u64)> = (0..200u64)
        .map(|message| {
            send(&mut world, &mut net, 1, 2, message, 100);
            (
                60 * MILLISECOND + draws.below(stream, 40 * MILLISECOND + 1).unwrap(),
                message,
            )
        })
        .collect();
    // Arrival order, ties in send order.
    expected.sort();
    let delivered = drain(&mut world, &mut net);
    assert_eq!(delivered, expected);
    assert!(
        delivered.windows(2).any(|pair| pair[0].1 > pair[1].1),
        "this seed's draws overtake"
    );
}

#[test]
fn independent_loss_is_its_process() {
    let seed = 7;
    let mut world = world(seed);
    let mut net = Net::new(NET);
    net.set_path(Path::NONE.with_loss(Loss::random(50_000)));
    let fates: Vec<bool> = (0..sized(10_000))
        .map(|message| {
            let fate = send(&mut world, &mut net, 1, 2, message, 100);
            drain(&mut world, &mut net);
            fate == Fate::Dropped(Dropped::Loss)
        })
        .collect();
    assert_eq!(
        fates,
        gilbert_elliott(
            seed,
            (0, PPM, 50_000, 50_000),
            usize::try_from(sized(10_000)).unwrap()
        )
    );
    let lost = fates.iter().filter(|lost| **lost).count() as u64;
    assert_eq!(net.stats().dropped_loss, lost);
    assert_eq!(net.stats().delivered, sized(10_000) - lost);
}

#[test]
fn bursty_loss_is_its_process_and_comes_in_runs() {
    let seed = 7;
    let mut world = world(seed);
    let mut net = Net::new(NET);
    // Bursts of ten messages on average, all lost, entered once in a hundred.
    net.set_path(Path::NONE.with_loss(Loss::bursty(10_000, 100_000, PPM)));
    let fates: Vec<bool> = (0..sized(10_000))
        .map(|message| {
            let fate = send(&mut world, &mut net, 1, 2, message, 100);
            drain(&mut world, &mut net);
            fate == Fate::Dropped(Dropped::Loss)
        })
        .collect();
    assert_eq!(
        fates,
        gilbert_elliott(
            seed,
            (10_000, 100_000, 0, PPM),
            usize::try_from(sized(10_000)).unwrap()
        )
    );
    // Outside a burst nothing is lost, so every loss is in a run that began with a burst: a run of
    // losses here is a stay in the bad state.
    let runs = fates.windows(2).filter(|pair| !pair[0] && pair[1]).count();
    assert!(runs > 0, "this seed's draws enter bursts");
}

/// The measured burst condition of `docs/research/burst-loss.md` §4: bursts of 36.8 ms on average
/// every 700 ms, everything inside them lost, 5% of the time in all.
fn measured_bursts() -> Loss {
    Loss::bursty_in_time(36_800_000, 700 * MILLISECOND, PPM).unwrap()
}

#[test]
fn a_loss_process_in_time_needs_a_burst_and_a_gap() {
    assert_eq!(
        Loss::bursty_in_time(0, MILLISECOND, PPM),
        Err(SimError::NotALossProcess)
    );
    assert_eq!(
        Loss::bursty_in_time(MILLISECOND, 0, PPM),
        Err(SimError::NotALossProcess)
    );
}

/// A message sent with a lost one shares its burst: with no time between them the chain in time
/// has not moved, so a copy sent with its original is lost with it, where independent loss draws
/// each afresh.
#[test]
fn a_message_sent_with_a_lost_one_is_lost_with_it_in_a_burst() {
    let seed = 7;
    let pairs = |loss: Loss| -> Vec<(bool, bool)> {
        let mut world = world(seed);
        let mut net = Net::new(NET);
        net.set_path(Path::NONE.with_loss(loss));
        (0..sized(2_000))
            .map(|pair| {
                world.advance(pair * SECOND).unwrap();
                let first = send(&mut world, &mut net, 1, 2, 2 * pair, 100);
                let second = send(&mut world, &mut net, 1, 2, 2 * pair + 1, 100);
                drain(&mut world, &mut net);
                let lost = |fate| fate == Fate::Dropped(Dropped::Loss);
                (lost(first), lost(second))
            })
            .collect()
    };
    let bursty = pairs(measured_bursts());
    assert!(
        bursty.iter().any(|(first, _)| *first),
        "this seed's draws find bursts"
    );
    assert!(bursty.iter().all(|(first, second)| first == second));
    let independent = pairs(Loss::random(50_000));
    assert!(
        independent.iter().any(|(first, second)| first != second),
        "independent loss parts this seed's pairs"
    );
}

/// The chain in time by its definition (`docs/research/burst-loss.md` §2), on a reference stream:
/// each message's chance of a burst from the time since the flow's previous one, then its loss in
/// the state drawn.
fn in_time(seed: u64, (burst, gap): (u64, u64), sends: &[u64]) -> Vec<bool> {
    let one: u128 = 1 << 64;
    let (mut world, stream) = reference(seed, "net.loss", 1, 2);
    let stationary = (u128::from(burst) << 64) / u128::from(gap + burst);
    let lambda = one - (one / u128::from(gap) + one / u128::from(burst));
    let times = |a: u128, b: u128| a.checked_mul(b).map_or(one, |p| p >> 64);
    let mut previous: Option<(bool, u64)> = None;
    sends
        .iter()
        .map(|&at| {
            let chance = match previous {
                None => stationary,
                Some((bad, then)) => {
                    let mut decay = one;
                    let mut square = lambda;
                    let mut exponent = at - then;
                    while exponent > 0 {
                        if exponent & 1 == 1 {
                            decay = times(decay, square);
                        }
                        exponent >>= 1;
                        if exponent > 0 {
                            square = times(square, square);
                        }
                    }
                    if bad {
                        stationary + times(one - stationary, decay)
                    } else {
                        times(stationary, one - decay)
                    }
                }
            };
            let bad = world.below(stream, 1 << 32).unwrap() < (chance >> 32) as u64;
            previous = Some((bad, at));
            world.chance(stream, if bad { PPM } else { 0 }).unwrap()
        })
        .collect()
}

#[test]
fn bursty_loss_in_time_is_its_process() {
    let seed = 7;
    let mut world = world(seed);
    let mut net = Net::new(NET);
    net.set_path(Path::NONE.with_loss(measured_bursts()));
    // Spacings from back to back to past the mean gap, cycled.
    let spacings = [
        0,
        1_000,
        MILLISECOND,
        10 * MILLISECOND,
        50 * MILLISECOND,
        SECOND,
    ];
    let mut at = 0;
    let mut sends = Vec::new();
    let fates: Vec<bool> = (0..sized(10_000))
        .map(|message| {
            at += spacings[message as usize % spacings.len()];
            world.advance(at).unwrap();
            sends.push(at);
            let fate = send(&mut world, &mut net, 1, 2, message, 100);
            drain(&mut world, &mut net);
            fate == Fate::Dropped(Dropped::Loss)
        })
        .collect();
    assert_eq!(
        fates,
        in_time(seed, (36_800_000, 700 * MILLISECOND), &sends)
    );
    assert!(
        fates.iter().any(|lost| *lost),
        "this seed's draws find bursts"
    );
}

#[test]
fn loss_state_is_per_flow() {
    let mut world = world(7);
    let mut net = Net::new(NET);
    net.set_pair_path(node(1), node(2), Path::NONE.with_loss(Loss::random(PPM)))
        .unwrap();
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 1u64, 10),
        Fate::Dropped(Dropped::Loss)
    );
    assert_eq!(
        send(&mut world, &mut net, 2, 1, 2, 10),
        Fate::Arrives { at: 0 }
    );
    assert_eq!(
        send(&mut world, &mut net, 1, 3, 3, 10),
        Fate::Arrives { at: 0 }
    );
}

#[test]
fn a_bottleneck_serializes_queues_and_drops_the_tail() {
    let mut world = world(7);
    let mut net = Net::new(NET);
    // 8,000 bits a second is 1,000 bytes a second; the queue holds two messages of 500 bytes behind
    // the one being sent.
    let link = net.add_link(Link::drop_tail(8_000, 1_500)).unwrap();
    net.set_path(Path::NONE.through(link));
    let half = 500_000_000;
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 1u64, 500),
        Fate::Arrives { at: half }
    );
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 2, 500),
        Fate::Arrives { at: 2 * half }
    );
    assert_eq!(
        send(&mut world, &mut net, 3, 2, 3, 500),
        Fate::Arrives { at: 3 * half }
    );
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 4, 500),
        Fate::Dropped(Dropped::Queue)
    );
    assert_eq!(net.stats().dropped_queue, 1);
    assert_eq!(net.stats().peak_queue_bytes, 1_000);
    // Half of the message being sent has left: still no room for a whole one, and room for what
    // has left.
    let mut delivered = Vec::new();
    until(&mut world, &mut net, half / 2, &mut delivered);
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 9, 500),
        Fate::Dropped(Dropped::Queue)
    );
    assert!(matches!(
        send(&mut world, &mut net, 1, 2, 8, 250),
        Fate::Arrives { .. }
    ));
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 7, 1),
        Fate::Dropped(Dropped::Queue)
    );
    // The backlog drains at the rate: one message later there is room.
    until(&mut world, &mut net, half, &mut delivered);
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 6, 500),
        Fate::Dropped(Dropped::Queue)
    );
    until(&mut world, &mut net, half + half / 2, &mut delivered);
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 5, 500),
        Fate::Arrives {
            at: 4 * half + half / 2
        }
    );
    assert_eq!(net.stats().dropped_queue, 4);
    delivered.extend(drain(&mut world, &mut net));
    assert_eq!(delivered.len(), 5);
}

#[test]
fn a_link_whose_capacity_changes_is_followed() {
    let mut world = world(7);
    let mut net = Net::new(NET);
    let link = net.add_link(Link::drop_tail(8_000, 10_000)).unwrap();
    net.set_path(Path::NONE.through(link));
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 1u64, 1_000),
        Fate::Arrives { at: SECOND }
    );
    net.set_link(link, Link::drop_tail(80_000, 10_000));
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 2, 1_000),
        Fate::Arrives {
            at: SECOND + SECOND / 10
        }
    );
}

#[test]
fn a_message_over_the_path_mtu_is_a_black_hole() {
    let mut world = world(7);
    let mut net = Net::new(NET);
    net.set_path(Path::LAN.with_mtu(1_200));
    assert!(matches!(
        send(&mut world, &mut net, 1, 2, 1u64, 1_200),
        Fate::Arrives { .. }
    ));
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 2, 1_201),
        Fate::Dropped(Dropped::Mtu)
    );
    assert_eq!(net.stats().dropped_mtu, 1);
}

#[test]
fn a_nat_mapping_expires_and_the_node_is_reached_again_once_it_sends() {
    let mut world = world(7);
    let mut net = Net::new(NET);
    net.set_nat(
        node(2),
        Nat {
            idle_timeout_ns: 30 * SECOND,
        },
        world.now(),
    )
    .unwrap();
    send(&mut world, &mut net, 1, 2, 1u64, 10);
    assert_eq!(drain(&mut world, &mut net), vec![(0, 1)]);
    world.advance(31 * SECOND).unwrap();
    send(&mut world, &mut net, 1, 2, 2, 10);
    assert_eq!(drain(&mut world, &mut net), vec![]);
    assert_eq!(net.stats().dropped_nat, 1);
    // The node sends: its mapping is alive again.
    send(&mut world, &mut net, 2, 1, 3, 10);
    send(&mut world, &mut net, 1, 2, 4, 10);
    let now = world.now();
    assert_eq!(drain(&mut world, &mut net), vec![(now, 3), (now, 4)]);
    // A rebinding the run chooses.
    net.rebind(node(2));
    send(&mut world, &mut net, 1, 2, 5, 10);
    assert_eq!(drain(&mut world, &mut net), vec![]);
    assert_eq!(net.stats().dropped_nat, 2);
}

#[test]
fn a_partition_cuts_at_send_and_in_flight() {
    let mut world = world(7);
    let mut net = Net::new(NET);
    net.set_path(Path::REGIONAL);
    send(&mut world, &mut net, 1, 2, 1u64, 10);
    net.partition(node(1), node(2), true);
    assert!(
        net.cut(node(1), node(2)) && !net.cut(node(2), node(1)),
        "directed"
    );
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 2, 10),
        Fate::Dropped(Dropped::Partition)
    );
    assert_eq!(drain(&mut world, &mut net), vec![]);
    assert_eq!(net.stats().dropped_partition, 2);
    net.partition(node(1), node(2), false);
    assert!(matches!(
        send(&mut world, &mut net, 1, 2, 3, 10),
        Fate::Arrives { .. }
    ));
    assert_eq!(drain(&mut world, &mut net).len(), 1);
    net.partition(node(2), node(1), true);
    net.heal();
    assert!(!net.cut(node(2), node(1)), "healed");
}

/// A run of three nodes on a reordering, bursty path: what arrives and the counters.
fn scenario(source: Source) -> Result<Record, SimError> {
    let mut world = world_of(source);
    let mut net = Net::new(NET);
    net.set_path(
        Path::reordering(80 * MILLISECOND, 20 * MILLISECOND)
            .with_loss(Loss::bursty(20_000, 200_000, 500_000)),
    );
    for message in 0..sized(2_000) {
        let from = 1 + (message % 3) as u32;
        let to = 1 + ((message + 1) % 3) as u32;
        send(&mut world, &mut net, from, to, message, 100);
    }
    let delivered = drain(&mut world, &mut net);
    // What arrived, and the counters, are in the run's digest.
    for (at, message) in delivered {
        world.observe(at ^ message.rotate_left(32));
    }
    let stats = net.stats();
    for count in [stats.delivered, stats.dropped_loss, stats.duplicated] {
        world.observe(count);
    }
    Ok(world.finish())
}

/// A run of three nodes whose messages are spaced in time on a path that loses in bursts in time.
fn scenario_in_time(source: Source) -> Result<Record, SimError> {
    let mut world = world_of(source);
    let mut net = Net::new(NET);
    net.set_path(
        Path::reordering(80 * MILLISECOND, 20 * MILLISECOND).with_loss(Loss::bursty_in_time(
            36_800_000,
            700 * MILLISECOND,
            PPM,
        )?),
    );
    let mut delivered = Vec::new();
    for message in 0..sized(2_000) {
        until(&mut world, &mut net, message * MILLISECOND, &mut delivered);
        let from = 1 + (message % 3) as u32;
        let to = 1 + ((message + 1) % 3) as u32;
        send(&mut world, &mut net, from, to, message, 100);
    }
    delivered.extend(drain(&mut world, &mut net));
    for (at, message) in delivered {
        world.observe(at ^ message.rotate_left(32));
    }
    let stats = net.stats();
    for count in [stats.delivered, stats.dropped_loss] {
        world.observe(count);
    }
    Ok(world.finish())
}

/// The run-twice check holds for loss in time: its chances are integer arithmetic on the world's
/// clock, so a run gives one digest from its seed twice and from its trace.
#[test]
fn a_run_with_bursts_in_time_replays_from_its_seed_and_from_its_trace() {
    let record = twice(11, scenario_in_time).unwrap();
    assert!(!record.trace.is_empty());
    assert_ne!(twice(12, scenario_in_time).unwrap().digest, record.digest);
}

/// The run-twice check (docs/sim.md §3.9): a run of three nodes on a reordering, bursty path gives
/// one digest from its seed twice and from its trace, and another seed another.
#[test]
fn a_run_replays_from_its_seed_and_from_its_trace() {
    let record = twice(11, scenario).unwrap();
    assert!(!record.trace.is_empty());
    assert_ne!(twice(12, scenario).unwrap().digest, record.digest);
}

#[test]
fn every_message_is_accounted_for() {
    let mut world = world(7);
    let mut net = Net::new(NET);
    let link = net.add_link(Link::drop_tail(1_000_000, 20_000)).unwrap();
    net.set_path(
        Path::REGIONAL
            .with_loss(Loss::random(30_000))
            .with_mtu(1_200)
            .through(link),
    );
    let mut delivered = Vec::new();
    for message in 0..sized(5_000) {
        until(
            &mut world,
            &mut net,
            message * 2 * MILLISECOND,
            &mut delivered,
        );
        let bytes = if message % 50 == 0 { 1_400 } else { 600 };
        send(&mut world, &mut net, 1, 2, message, bytes);
    }
    delivered.extend(drain(&mut world, &mut net));
    let stats = net.stats();
    assert!(stats.dropped_mtu > 0 && stats.dropped_loss > 0 && stats.dropped_queue > 0);
    assert_eq!(stats.delivered, delivered.len() as u64);
    assert_eq!(
        stats.sent,
        stats.delivered
            + stats.dropped_mtu
            + stats.dropped_queue
            + stats.dropped_loss
            + stats.dropped_partition
            + stats.dropped_capacity
            + stats.dropped_nat
    );
    assert_eq!(net.in_flight(), (0, 0));
}

#[test]
fn every_table_is_bounded_and_a_forgotten_node_frees_its_rows() {
    let limits = NetLimits {
        flows: 2,
        links: 1,
        nats: 1,
        link_messages: 3,
        ..NET
    };
    let mut world = world(1);
    let mut net = Net::new(limits);
    let link = net.add_link(Link::drop_tail(8, u64::MAX)).unwrap();
    assert!(net.add_link(Link::drop_tail(8, 1)).is_err());
    net.set_nat(node(1), Nat { idle_timeout_ns: 1 }, 0).unwrap();
    net.set_nat(node(1), Nat { idle_timeout_ns: 2 }, 0).unwrap();
    assert!(net.set_nat(node(2), Nat { idle_timeout_ns: 1 }, 0).is_err());
    net.set_path(Path::LAN.with_loss(Loss::random(1)).through(link));
    // The link's bytes never fill: its count of messages holds.
    assert!(matches!(
        send(&mut world, &mut net, 1, 2, 1u64, 1),
        Fate::Arrives { .. }
    ));
    assert!(matches!(
        send(&mut world, &mut net, 1, 2, 2, 1),
        Fate::Arrives { .. }
    ));
    assert!(matches!(
        send(&mut world, &mut net, 2, 1, 3, 1),
        Fate::Arrives { .. }
    ));
    assert_eq!(
        send(&mut world, &mut net, 2, 1, 4, 1),
        Fate::Dropped(Dropped::Queue)
    );
    // A third flow has no row to take.
    assert_eq!(
        send(&mut world, &mut net, 1, 3, 5, 1),
        Fate::Dropped(Dropped::Capacity)
    );
    net.set_pair_path(node(1), node(2), Path::NONE).unwrap();
    net.set_pair_path(node(2), node(1), Path::NONE).unwrap();
    assert!(net.set_pair_path(node(1), node(3), Path::NONE).is_err());
    net.forget(node(2));
    net.set_pair_path(node(1), node(3), Path::NONE).unwrap();
    assert!(matches!(
        send(&mut world, &mut net, 1, 3, 6, 1),
        Fate::Arrives { .. }
    ));
}

#[test]
fn a_step_marks_what_finds_more_than_its_threshold_and_drops_what_cannot_be_marked() {
    let mut world = world(7);
    let mut net: Net<(u64, bool)> = Net::new(NET);
    // 8 kbit/s: a message of 1,000 bytes takes a second to leave.
    let link = net
        .add_link(Link {
            rate_bits_per_second: 8_000,
            queue_bytes: 10_000,
            marking: Marking::Step {
                threshold_bytes: 2_500,
            },
        })
        .unwrap();
    net.set_path(Path::NONE.through(link));
    let mark = |sent: &mut (u64, bool)| sent.1 = true;
    // Five at once find 0, 1,000, 2,000, 3,000 and 4,000 bytes ahead.
    for message in 0..5 {
        let fate = net
            .send_ecn(
                &mut world,
                (node(1), node(2)),
                (message, false),
                1_000,
                Ev::Arrive,
                mark,
            )
            .unwrap();
        assert!(matches!(fate, Fate::Arrives { .. }), "{fate:?}");
    }
    // One that is not ECN-capable finds 5,000: dropped where it would be marked, though the queue
    // has room for it.
    assert_eq!(
        send(&mut world, &mut net, 1, 2, (5, false), 1_000),
        Fate::Dropped(Dropped::Queue)
    );
    let delivered = drain(&mut world, &mut net);
    let marked: Vec<u64> = delivered
        .iter()
        .filter(|(_, (_, marked))| *marked)
        .map(|(_, (message, _))| *message)
        .collect();
    assert_eq!(marked, vec![3, 4]);
    assert_eq!(delivered.len(), 5);
    assert_eq!(net.stats().marked, 2);
    assert_eq!(net.stats().dropped_queue, 1);
    // Below the threshold again, nothing is marked.
    net.send_ecn(
        &mut world,
        (node(1), node(2)),
        (6, false),
        1_000,
        Ev::Arrive,
        mark,
    )
    .unwrap();
    assert!(
        !drain(&mut world, &mut net)
            .iter()
            .any(|(_, (_, marked))| *marked)
    );
}

#[test]
fn codel_marks_a_standing_queue_after_an_interval_and_ever_sooner_until_it_drains() {
    let mut world = world(7);
    let mut net: Net<(u64, bool)> = Net::new(NET);
    // 1 Mbit/s: a message of 1,250 bytes takes 10 ms to leave. CoDel's defaults (RFC 8289 §4.3): a
    // target of 5 ms over 100 ms.
    let link = net
        .add_link(Link {
            rate_bits_per_second: 1_000_000,
            queue_bytes: 1_000_000,
            marking: Marking::CoDel {
                target_ns: 5 * MILLISECOND,
                interval_ns: 100 * MILLISECOND,
            },
        })
        .unwrap();
    net.set_path(Path::NONE.through(link));
    // When each message arrives, by its number.
    let mut arrivals: Vec<u64> = Vec::new();
    let mut delivered = Vec::new();
    let mut send = |world: &mut World<Ev>, net: &mut Net<(u64, bool)>| {
        let message = arrivals.len() as u64;
        let fate = net
            .send_ecn(
                world,
                (node(1), node(2)),
                (message, false),
                1_250,
                Ev::Arrive,
                |sent| {
                    sent.1 = true;
                },
            )
            .unwrap();
        match fate {
            Fate::Arrives { at } => arrivals.push(at),
            fate => panic!("{fate:?}"),
        }
    };
    // A standing queue of three messages, then one at the link's rate for a second: each waits
    // 30 ms, six times the target.
    for _ in 0..3 {
        send(&mut world, &mut net);
    }
    for step in 0..100_u64 {
        until(
            &mut world,
            &mut net,
            step * 10 * MILLISECOND,
            &mut delivered,
        );
        send(&mut world, &mut net);
    }
    // Then one every 20 ms for a second: the queue drains and stays below the target.
    for step in 0..50_u64 {
        until(
            &mut world,
            &mut net,
            SECOND + step * 20 * MILLISECOND,
            &mut delivered,
        );
        send(&mut world, &mut net);
    }
    delivered.extend(drain(&mut world, &mut net));
    assert_eq!(delivered.len(), 153);
    // A message leaves the queue 10 ms before it arrives: its own time on the link, and no
    // propagation.
    let marks: Vec<u64> = delivered
        .iter()
        .filter(|(_, (_, marked))| *marked)
        .map(|(_, (message, _))| arrivals[*message as usize] - 10 * MILLISECOND)
        .collect();
    assert_eq!(net.stats().marked, marks.len() as u64);
    assert!(marks.len() >= 5, "{marks:?}");
    // Nothing before the sojourn has stood above the target for an interval.
    assert!(marks[0] >= 100 * MILLISECOND, "{marks:?}");
    // The gaps shrink as interval/√count: the first is the interval, none is longer than it and a
    // message's time (marks fall where messages leave, every 10 ms), and the later half of the
    // standing queue holds more marks than the earlier.
    let gaps: Vec<u64> = marks.windows(2).map(|pair| pair[1] - pair[0]).collect();
    assert_eq!(gaps[0], 100 * MILLISECOND, "{gaps:?}");
    assert!(gaps.iter().all(|gap| *gap <= 110 * MILLISECOND), "{gaps:?}");
    assert!(gaps[gaps.len() - 1] < gaps[0], "{gaps:?}");
    let middle = (marks[0] + SECOND) / 2;
    let earlier = marks.iter().filter(|at| **at < middle).count();
    let later = marks
        .iter()
        .filter(|at| **at >= middle && **at < SECOND)
        .count();
    assert!(later > earlier, "{marks:?}");
    // Once the queue drained, nothing more is marked.
    assert!(
        marks.iter().all(|at| *at < 1_200 * MILLISECOND),
        "{marks:?}"
    );
}

#[test]
fn past_its_capacity_the_oldest_message_in_flight_is_lost_and_counted() {
    let limits = NetLimits {
        messages: 3,
        bytes: 250,
        ..NET
    };
    let mut world = world(7);
    let mut net = Net::new(limits);
    net.set_path(Path::in_order(SECOND, 0));
    for message in 0..3u64 {
        assert!(matches!(
            send(&mut world, &mut net, 1, 2, message, 50),
            Fate::Arrives { .. }
        ));
    }
    assert_eq!(net.in_flight(), (3, 150));
    // A fourth message: the oldest goes.
    assert!(matches!(
        send(&mut world, &mut net, 1, 2, 3, 50),
        Fate::Arrives { .. }
    ));
    assert_eq!(net.stats().dropped_capacity, 1);
    // Bytes bind too: 200 more makes room by losing the two oldest.
    assert!(matches!(
        send(&mut world, &mut net, 1, 2, 4, 200),
        Fate::Arrives { .. }
    ));
    assert_eq!(net.stats().dropped_capacity, 3);
    assert_eq!(net.in_flight(), (2, 250));
    // A message larger than the network holds is refused, and nothing else is lost for it.
    assert_eq!(
        send(&mut world, &mut net, 1, 2, 5, 251),
        Fate::Dropped(Dropped::Capacity)
    );
    assert_eq!(net.stats().dropped_capacity, 4);
    let delivered: Vec<u64> = drain(&mut world, &mut net)
        .into_iter()
        .map(|(_, m)| m)
        .collect();
    assert_eq!(
        delivered,
        vec![3, 4],
        "the lost ones' arrivals name nothing"
    );
    assert_eq!(net.in_flight(), (0, 0));
}

#[test]
fn a_duplicate_is_the_message_delivered_again_and_holds_no_more_room() {
    let limits = NetLimits { messages: 1, ..NET };
    let mut world = world(7);
    let mut net = Net::new(limits);
    net.set_path(Path::in_order(MILLISECOND, 0));
    net.set_duplicate_ppm(PPM);
    send(&mut world, &mut net, 1, 2, 9u64, 10);
    // Every delivery keeps it: it arrives again one propagation later, in its one slot.
    for round in 1..=5u64 {
        let delivery = step(&mut world, &mut net).unwrap();
        assert_eq!(delivery, Some((round * MILLISECOND, 9)));
        assert_eq!(net.in_flight(), (1, 10), "one slot");
    }
    assert_eq!(net.stats().duplicated, 5);
    // Once nothing keeps it, its next arrival is its last.
    net.set_duplicate_ppm(0);
    assert_eq!(
        step(&mut world, &mut net).unwrap(),
        Some((6 * MILLISECOND, 9))
    );
    assert_eq!(net.in_flight(), (0, 0));
    assert_eq!(step(&mut world, &mut net), None);
}

#[test]
fn a_flows_draws_do_not_depend_on_other_flows() {
    let delays = |others: bool| {
        let mut world = world(5);
        let mut net = Net::new(NET);
        net.set_path(Path::LAN.with_loss(Loss::random(100_000)));
        let mut fates = Vec::new();
        for message in 0..sized(1_000) {
            if others {
                send(&mut world, &mut net, 3, 4, message, 10);
                send(&mut world, &mut net, 2, 1, message, 10);
            }
            let sent = world.now();
            // Its delay from the send, or none: the other flows' arrivals move the clock.
            fates.push(match send(&mut world, &mut net, 1, 2, message, 10) {
                Fate::Arrives { at } => Some(at - sent),
                Fate::Dropped(_) => None,
            });
            drain(&mut world, &mut net);
        }
        fates
    };
    assert_eq!(delays(false), delays(true));
}

/// Every node the tests name.
fn nodes() -> Vec<NodeId> {
    (0..5).map(NodeId).collect()
}

/// The pairs a drawn partition cuts.
fn drawn_cuts<P: Clone>(net: &Net<P>) -> Vec<(u32, u32)> {
    let mut cuts = Vec::new();
    for from in nodes() {
        for to in nodes() {
            if net.cut(from, to) {
                cuts.push((from.0, to.0));
            }
        }
    }
    cuts
}

#[test]
fn a_drawn_partition_starts_and_heals_once_its_state_has_lasted_its_stability() {
    let plan = Partitions {
        split: Split::IsolateSingle,
        symmetric: true,
        start_ppm: PPM,
        heal_ppm: PPM,
        stable_ns: SECOND,
    };
    let mut world = world(3);
    let mut net: Net<u64> = Net::new(NET);
    assert!(
        !net.churn(&mut world, &nodes(), plan).unwrap(),
        "the absence lasts a second first"
    );
    world.advance(SECOND).unwrap();
    assert!(net.churn(&mut world, &nodes(), plan).unwrap());
    assert!(net.partitioned());
    world.advance(SECOND + SECOND / 2).unwrap();
    assert!(
        !net.churn(&mut world, &nodes(), plan).unwrap(),
        "it lasts a second"
    );
    assert!(net.partitioned());
    world.advance(2 * SECOND).unwrap();
    assert!(net.churn(&mut world, &nodes(), plan).unwrap());
    assert!(!net.partitioned());
    assert_eq!(drawn_cuts(&net), vec![]);
}

#[test]
fn each_split_cuts_its_side_from_the_rest() {
    for seed in 0..64 {
        for split in [
            Split::UniformSize,
            Split::UniformPartition,
            Split::IsolateSingle,
        ] {
            for symmetric in [true, false] {
                let plan = Partitions {
                    split,
                    symmetric,
                    start_ppm: PPM,
                    heal_ppm: 0,
                    stable_ns: 0,
                };
                let mut world = world(seed);
                let mut net: Net<u64> = Net::new(NET);
                net.churn(&mut world, &nodes(), plan).unwrap();
                let cuts = drawn_cuts(&net);
                // The side is every node a cut leaves from that nothing cuts back to it from
                // outside, symmetric or not.
                let side: Vec<u32> = nodes()
                    .iter()
                    .map(|node| node.0)
                    .filter(|node| cuts.iter().any(|(from, _)| from == node))
                    .collect();
                let rest: Vec<u32> = (0..5).filter(|node| !side.contains(node)).collect();
                let mut expected: Vec<(u32, u32)> = Vec::new();
                for inside in &side {
                    for outside in &rest {
                        expected.push((*inside, *outside));
                        if symmetric {
                            expected.push((*outside, *inside));
                        }
                    }
                }
                expected.sort();
                if symmetric && !cuts.is_empty() {
                    // Both sides send across: the side is the smaller-numbered of the two that
                    // holds node 0's opposite; check the cut set is a full bipartition.
                    let a: Vec<u32> = (0..5)
                        .filter(|node| net.cut(NodeId(0), NodeId(*node)))
                        .collect();
                    let zero_side: Vec<u32> = (0..5).filter(|node| !a.contains(node)).collect();
                    let mut bipartition = Vec::new();
                    for inside in &zero_side {
                        for outside in &a {
                            bipartition.push((*inside, *outside));
                            bipartition.push((*outside, *inside));
                        }
                    }
                    bipartition.sort();
                    assert_eq!(cuts, bipartition, "{split:?} seed {seed}");
                } else {
                    assert_eq!(
                        cuts, expected,
                        "{split:?} seed {seed} symmetric {symmetric}"
                    );
                }
                match split {
                    Split::IsolateSingle => {
                        let cut_off = (0..5)
                            .filter(|node| cuts.iter().any(|(from, _)| from == node))
                            .count();
                        let expected_cut_off = if symmetric { 5 } else { 1 };
                        assert_eq!(cut_off, expected_cut_off, "one node alone");
                        assert_eq!(cuts.len(), if symmetric { 8 } else { 4 });
                    }
                    Split::UniformSize => {
                        assert!(!cuts.is_empty(), "a side of one to four always cuts");
                        assert!(net.partitioned());
                    }
                    Split::UniformPartition => {
                        assert_eq!(net.partitioned(), !cuts.is_empty());
                    }
                }
            }
        }
    }
}

#[test]
fn a_drawn_partition_is_held_apart_from_the_tests_cuts_and_cuts_in_flight() {
    let plan = Partitions {
        split: Split::IsolateSingle,
        symmetric: true,
        start_ppm: PPM,
        heal_ppm: PPM,
        stable_ns: 0,
    };
    let mut world = world(9);
    let mut net = Net::new(NET);
    net.set_path(Path::in_order(SECOND, 0));
    // A message from every node to every other, in flight across what is drawn next.
    for from in 0..5 {
        for to in (0..5).filter(|to| *to != from) {
            send(
                &mut world,
                &mut net,
                from,
                to,
                u64::from(from * 10 + to),
                10,
            );
        }
    }
    net.partition(node(0), node(1), true);
    assert!(net.churn(&mut world, &nodes(), plan).unwrap());
    let cut_pairs = drawn_cuts(&net);
    let delivered = drain(&mut world, &mut net);
    assert_eq!(
        delivered.len() + cut_pairs.len(),
        20,
        "every cut pair's message lost in flight"
    );
    assert_eq!(net.stats().dropped_partition, cut_pairs.len() as u64);
    // Healing the test's cut leaves the drawn partition; the churn's heal leaves the test's.
    net.heal();
    assert!(net.partitioned());
    net.partition(node(0), node(1), true);
    assert!(net.churn(&mut world, &nodes(), plan).unwrap());
    assert!(!net.partitioned());
    assert!(net.cut(node(0), node(1)));
}

#[test]
fn the_adversary_replays_truncates_and_forges_what_the_network_carried() {
    let mut world = world(13);
    let mut net: Net<Vec<u8>> = Net::new(NET);
    let mut adversary = Adversary::new(8, 1 << 12);
    let originals: Vec<(u32, u32, Vec<u8>)> = (0..4u32)
        .map(|n| (n % 3, (n + 1) % 3, (0..40u8).map(|b| b ^ n as u8).collect()))
        .collect();
    for (from, to, datagram) in &originals {
        adversary.observe(node(*from), node(*to), datagram);
    }
    assert_eq!(adversary.kept(), 4);
    let mut seen = [false; 3];
    for _ in 0..200 {
        let (attack, fate) = adversary
            .attack(&mut world, &mut net, node(4), Ev::Arrive)
            .unwrap()
            .unwrap();
        assert!(
            matches!(fate, Fate::Arrives { .. }),
            "the zero path carries it"
        );
        let Some(Step::Event {
            event: Ev::Arrive(ticket),
            ..
        }) = Some(world.next(&mut Fifo).unwrap())
        else {
            panic!("an arrival");
        };
        let delivery = net
            .deliver(&mut world, ticket, Ev::Arrive)
            .unwrap()
            .unwrap();
        let original = originals
            .iter()
            .find(|(from, to, datagram)| {
                node(*to) == delivery.to
                    && match attack {
                        Attack::Replay => {
                            node(*from) == delivery.from && *datagram == delivery.payload
                        }
                        Attack::Truncate => datagram.starts_with(&delivery.payload),
                        Attack::Forge => {
                            datagram.len() == delivery.payload.len()
                                && datagram
                                    .iter()
                                    .zip(&delivery.payload)
                                    .filter(|(a, b)| a != b)
                                    .count()
                                    == 1
                        }
                    }
            })
            .unwrap_or_else(|| panic!("{attack:?} of no kept datagram: {delivery:?}"));
        match attack {
            Attack::Replay => seen[0] = true,
            Attack::Truncate => {
                assert!(
                    delivery.payload.len() < original.2.len(),
                    "strictly shorter"
                );
                assert_eq!(delivery.from, node(4), "from the adversary");
                seen[1] = true;
            }
            Attack::Forge => {
                assert_eq!(delivery.from, node(4), "from the adversary");
                seen[2] = true;
            }
        }
    }
    assert_eq!(seen, [true; 3], "this seed's draws try all three");
}

#[test]
fn the_adversary_keeps_its_bound_and_gives_up_the_oldest() {
    let mut adversary = Adversary::new(2, 10);
    adversary.observe(node(0), node(1), &[1; 4]);
    adversary.observe(node(0), node(1), &[2; 4]);
    adversary.observe(node(0), node(1), &[3; 4]);
    assert_eq!(adversary.kept(), 2, "the count binds");
    adversary.observe(node(0), node(1), &[4; 9]);
    assert_eq!(adversary.kept(), 1, "the bytes bind: both older ones go");
    adversary.observe(node(0), node(1), &[5; 11]);
    assert_eq!(
        adversary.kept(),
        1,
        "one longer than it keeps in all is not kept"
    );
    let mut world = world(1);
    let mut net: Net<Vec<u8>> = Net::new(NET);
    let mut empty = Adversary::new(0, 0);
    empty.observe(node(0), node(1), &[1]);
    assert_eq!(
        empty
            .attack(&mut world, &mut net, node(4), Ev::Arrive)
            .unwrap(),
        None
    );
}

static GRID: [u32; 3] = [0, 500_000, PPM];
static VALUES: [u64; 3] = [100, 200, 1_000];

#[test]
fn a_measured_table_that_is_no_distribution_is_refused() {
    static SHORT: [u32; 1] = [0];
    static ONE: [u64; 1] = [1];
    static NOT_FROM_ZERO: [u32; 3] = [1, 500_000, PPM];
    static NOT_TO_ALL: [u32; 3] = [0, 500_000, PPM - 1];
    static FLAT: [u32; 3] = [0, 0, PPM];
    static FALLING: [u64; 3] = [100, 50, 1_000];
    static TWO: [u64; 2] = [1, 2];
    assert!(Measured::new(&GRID, &VALUES).is_ok());
    assert_eq!(Measured::new(&SHORT, &ONE), Err(SimError::NotADistribution));
    assert_eq!(
        Measured::new(&NOT_FROM_ZERO, &VALUES),
        Err(SimError::NotADistribution)
    );
    assert_eq!(
        Measured::new(&NOT_TO_ALL, &VALUES),
        Err(SimError::NotADistribution)
    );
    assert_eq!(
        Measured::new(&FLAT, &VALUES),
        Err(SimError::NotADistribution)
    );
    assert_eq!(
        Measured::new(&GRID, &FALLING),
        Err(SimError::NotADistribution)
    );
    assert_eq!(Measured::new(&GRID, &TWO), Err(SimError::NotADistribution));
}

#[test]
fn a_measured_path_draws_by_the_inverse_transform() {
    let seed = 21;
    let mut world = world(seed);
    let mut net = Net::new(NET);
    net.set_path(Path::reordering(0, 0));
    net.set_path(Path::measured(Measured::new(&GRID, &VALUES).unwrap()));
    let (mut draws, stream) = reference(seed, "net.delay", 1, 2);
    let mut previous = 0;
    for message in 0..sized(2_000) {
        let fate = send(&mut world, &mut net, 1, 2, message, 10);
        // Linear between the grid's points, rounded half up.
        let u = draws.below(stream, u64::from(PPM)).unwrap();
        let delay = if u < 500_000 {
            100 + (100 * u + 250_000) / 500_000
        } else {
            200 + (800 * (u - 500_000) + 250_000) / 500_000
        };
        // The path keeps the flow's order.
        let at = delay.max(previous);
        previous = at;
        assert_eq!(fate, Fate::Arrives { at }, "message {message}");
    }
    let delivered = drain(&mut world, &mut net);
    assert_eq!(delivered.len() as u64, sized(2_000));
    assert!(delivered.iter().all(|(at, _)| (100..=1_000).contains(at)));
}
