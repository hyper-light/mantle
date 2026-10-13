//! The world's contract: the generator is focal's, streams are independent, a run replays from its
//! seed or its trace, the two disciplines enable what they say, node clocks keep their laws, every
//! bound refuses, and the run-twice check catches nondeterminism.
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

use std::collections::BTreeSet;

use hyper_sim::{
    Clock, Discipline, Fifo, Lateness, Limits, NodeId, Random, Record, Seeded, SimError, Source,
    Step, Trace, Twice, World, twice,
};
use proptest::prelude::*;

const LIMITS: Limits = Limits {
    events: 4_096,
    nodes: 16,
    streams: 64,
    steps: 100_000,
    // The ring's 5,000 steps draw at most four words each.
    trace_words: 1 << 15,
};

fn world(seed: u64, discipline: Discipline) -> World<u64> {
    World::new(Source::Seed(seed), discipline, LIMITS).unwrap()
}

/// focal's `Seeded`, verbatim, as the reference the generator must equal draw for draw.
struct Focal(u64);
impl Focal {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, bound: u64) -> u64 {
        let Some(excess) = u64::MAX.checked_rem(bound) else {
            return 0;
        };
        let last = if excess.checked_add(1) == Some(bound) {
            u64::MAX
        } else {
            u64::MAX.saturating_sub(excess).saturating_sub(1)
        };
        let mut draw = self.next_u64();
        for _ in 0..64 {
            if draw <= last {
                break;
            }
            draw = self.next_u64();
        }
        draw.checked_rem(bound).unwrap_or(0)
    }
}

#[test]
fn the_generator_is_focals() {
    // focal-sim's own fixture.
    assert_eq!(Seeded::new(0).next_u64(), 0xe220_a839_7b1d_cdaf);
    for seed in [0u64, 1, 3, 0xdead_beef, u64::MAX] {
        let (mut ours, mut theirs) = (Seeded::new(seed), Focal(seed));
        for bound in [
            0u64,
            1,
            2,
            3,
            7,
            100,
            1 << 32,
            (1 << 32) + 1,
            3 << 62,
            u64::MAX,
        ] {
            for _ in 0..50 {
                assert_eq!(ours.below(bound), theirs.below(bound));
            }
        }
        assert_eq!(ours.next_u64(), theirs.next_u64());
    }
}

#[test]
fn below_is_uniform_and_unbiased() {
    let mut random = Seeded::new(3);
    assert_eq!(random.below(0), 0);
    let mut counts = [0u32; 7];
    for _ in 0..70_000 {
        counts[random.below(7) as usize] += 1;
    }
    // focal's test: 10,000 each, standard deviation 92.6; the band is 5.4 of them.
    assert!(counts.iter().all(|count| (9_500..=10_500).contains(count)));
    // At 3·2⁶², a bare remainder gives [0, 2⁶²) half the draws; the exact draw a third. With
    // 30,000 draws the third's standard deviation is 0.0027 and the half is 61 of them away.
    let bound = 3u64 << 62;
    let low = (0..30_000)
        .filter(|_| random.below(bound) < (1 << 62))
        .count() as f64
        / 30_000.0;
    assert!((low - 1.0 / 3.0).abs() < 0.02, "{low}");
}

#[test]
fn a_bound_of_one_draws_and_a_bound_of_zero_does_not() {
    let (mut a, mut b) = (Seeded::new(9), Seeded::new(9));
    a.below(0);
    assert_eq!(a, Seeded::new(9));
    a.below(1);
    b.next_u64();
    assert_eq!(a, b);
}

/// The draws of each stream of a world, by name, given a schedule of which stream draws.
fn draws(seed: u64, names: &[(&'static str, u64)], order: &[usize], bound: u64) -> Vec<Vec<u64>> {
    let mut world = world(seed, Discipline::Ordered);
    let ids: Vec<_> = names
        .iter()
        .map(|(label, part)| world.stream(label, &[*part]).unwrap())
        .collect();
    let mut out = vec![Vec::new(); names.len()];
    for at in order {
        out[*at].push(world.below(ids[*at], bound).unwrap());
    }
    out
}

#[test]
fn a_new_source_leaves_every_other_streams_draws_unchanged() {
    let names = [("link", 1), ("link", 2), ("device", 1)];
    // Without the new fault kind: the three streams interleaved.
    let before = draws(7, &names, &[0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2], 1_000);
    // With a new fault kind enabled on link 1 — its own stream, named first and drawn between
    // every other draw — and link 2 drawing more often.
    let with = [("fault", 1), ("link", 1), ("link", 2), ("device", 1)];
    let after = draws(
        7,
        &with,
        &[
            0, 1, 0, 2, 2, 0, 3, 1, 0, 2, 2, 3, 0, 1, 2, 2, 3, 0, 1, 2, 3,
        ],
        1_000,
    );
    assert_eq!(before[0][..], after[1][..4]);
    assert_eq!(before[1][..], after[2][..4]);
    assert_eq!(before[2][..], after[3][..4]);
    // And a different seed draws differently.
    assert_ne!(
        draws(8, &names, &[0, 0, 0, 0], 1_000)[0],
        before[0],
        "seeds 7 and 8 drew alike"
    );
}

#[test]
fn a_name_is_one_stream() {
    let mut world = world(1, Discipline::Ordered);
    world.stream("link", &[1, 2]).unwrap();
    assert_eq!(
        world.stream("link", &[1, 2]),
        Err(SimError::DuplicateStream("link"))
    );
    // The encoding is prefix-free: these are three streams.
    world.stream("link", &[1]).unwrap();
    world.stream("link", &[1, 2, 0]).unwrap();
    world.stream("lin", &[1, 2]).unwrap();
}

/// Draws of every width from a world, then the same from its trace.
#[test]
fn a_run_replays_from_its_trace_alone() {
    let bounds = [2u64, 3, 1 << 32, (1 << 32) + 1, u64::MAX, 1, 0, 1_000_000];
    let run = |source: Source| {
        let mut world: World<u64> = World::new(source, Discipline::Ordered, LIMITS).unwrap();
        let a = world.stream("a", &[]).unwrap();
        let values: Vec<u64> = (0..100)
            .map(|i| world.below(a, bounds[i % bounds.len()]).unwrap())
            .collect();
        (values, world.finish())
    };
    let (values, record) = run(Source::Seed(42));
    // Two words for each bound past 2³², none for 0 and 1.
    let words: usize = (0..100)
        .map(|i| match bounds[i % bounds.len()] {
            0 | 1 => 0,
            b if b <= 1 << 32 => 1,
            _ => 2,
        })
        .sum();
    assert_eq!(record.trace.len(), words);
    let (replayed, again) = run(Source::Trace(record.trace.clone()));
    assert_eq!(replayed, values);
    assert_eq!(again.digest, record.digest);
}

#[test]
fn a_trace_from_another_run_is_refused() {
    let mut world: World<u64> = World::new(
        Source::Trace(Trace::from_words(vec![5, 1])),
        Discipline::Ordered,
        LIMITS,
    )
    .unwrap();
    let a = world.stream("a", &[]).unwrap();
    assert_eq!(world.below(a, 5), Err(SimError::Diverged { at: 0 }));
    let mut world: World<u64> = World::new(
        Source::Trace(Trace::from_words(vec![1])),
        Discipline::Ordered,
        LIMITS,
    )
    .unwrap();
    let a = world.stream("a", &[]).unwrap();
    assert_eq!(world.below(a, 5), Ok(1));
    assert_eq!(world.below(a, 5), Err(SimError::TraceEnded { at: 1 }));
}

/// Every step taken from a world until it idles: when, which node, and what.
fn drain<S: hyper_sim::Strategy>(world: &mut World<u64>, strategy: &mut S) -> Vec<(u64, u32, u64)> {
    let mut taken = Vec::new();
    loop {
        match world.next(strategy).unwrap() {
            Step::Event { node, event } => taken.push((world.now(), node.0, event)),
            Step::Wake { node } => taken.push((world.now(), node.0, u64::MAX)),
            Step::Idle | Step::Spent => return taken,
        }
    }
}

#[test]
fn ordered_runs_the_earliest_and_chooses_among_ties() {
    let mut firsts = BTreeSet::new();
    for seed in 0..64 {
        let mut world = world(seed, Discipline::Ordered);
        let node = world.node(Clock::default()).unwrap();
        for (at, tag) in [(30, 0), (10, 1), (20, 2), (20, 3), (20, 4), (5, 5)] {
            world.schedule(at, node, tag).unwrap();
        }
        let taken = drain(&mut world, &mut Random);
        let times: Vec<u64> = taken.iter().map(|(at, _, _)| *at).collect();
        assert_eq!(times, [5, 10, 20, 20, 20, 30]);
        firsts.insert(taken[2].2);
    }
    // Each of the three tied at 20 runs first in some seed.
    assert_eq!(firsts, BTreeSet::from([2, 3, 4]));
    // Fifo: equal times in the order they were scheduled, and no decision.
    let mut world = world(0, Discipline::Ordered);
    let node = world.node(Clock::default()).unwrap();
    for (at, tag) in [(20, 2), (20, 3), (10, 1), (20, 4)] {
        world.schedule(at, node, tag).unwrap();
    }
    let tags: Vec<u64> = drain(&mut world, &mut Fifo).iter().map(|t| t.2).collect();
    assert_eq!(tags, [1, 2, 3, 4]);
    assert!(world.finish().trace.is_empty());
}

#[test]
fn free_enables_every_pending_event_and_time_never_goes_back() {
    let mut firsts = BTreeSet::new();
    for seed in 0..64 {
        let mut world = world(seed, Discipline::Free);
        let node = world.node(Clock::default()).unwrap();
        for at in [10, 20, 30] {
            world.schedule(at, node, at).unwrap();
        }
        let taken = drain(&mut world, &mut Random);
        firsts.insert(taken[0].2);
        let clock: Vec<u64> = taken.iter().map(|t| t.0).collect();
        assert!(clock.windows(2).all(|w| w[0] <= w[1]), "{clock:?}");
        // The clock is at least each event's time when it runs.
        assert!(taken.iter().all(|(now, _, at)| now >= at));
    }
    assert_eq!(firsts, BTreeSet::from([10, 20, 30]));
}

#[test]
fn a_run_switches_discipline_and_keeps_its_events() {
    let mut world = world(5, Discipline::Free);
    let node = world.node(Clock::default()).unwrap();
    for at in [50, 10, 40, 20, 30] {
        world.schedule(at, node, at).unwrap();
    }
    let first = match world.next(&mut Random).unwrap() {
        Step::Event { event, .. } => event,
        other => panic!("{other:?}"),
    };
    world.set_discipline(Discipline::Ordered);
    assert_eq!(world.pending(), 4);
    let rest: Vec<u64> = drain(&mut world, &mut Random).iter().map(|t| t.2).collect();
    let mut expected: Vec<u64> = [10, 20, 30, 40, 50]
        .into_iter()
        .filter(|at| *at != first)
        .collect();
    expected.sort_unstable();
    assert_eq!(rest, expected);
    world.set_discipline(Discipline::Free);
    world.set_discipline(Discipline::Free);
    assert_eq!(world.pending(), 0);
}

#[test]
fn timers_fire_late_by_their_nodes_lateness_and_once() {
    let lateness = Lateness {
        floor_ns: 1_000,
        spread_ns: 500,
    };
    let mut world = world(11, Discipline::Ordered);
    let slow = world
        .node(Clock {
            lateness,
            ..Clock::default()
        })
        .unwrap();
    let exact = world.node(Clock::default()).unwrap();
    world.wake(slow, Some(10_000)).unwrap();
    let words = world.decisions();
    assert_eq!(words, 1, "the lateness drawn");
    // The same deadline again changes nothing and draws nothing.
    world.wake(slow, Some(10_000)).unwrap();
    assert_eq!(world.decisions(), words);
    world.wake(exact, Some(10_000)).unwrap();
    world.schedule(10_000, exact, 7).unwrap();
    match world.next(&mut Fifo).unwrap() {
        Step::Event { event: 7, .. } => assert_eq!(world.now(), 10_000),
        other => panic!("{other:?}"),
    }
    assert_eq!(world.next(&mut Fifo).unwrap(), Step::Wake { node: exact });
    assert_eq!(world.next(&mut Fifo).unwrap(), Step::Wake { node: slow });
    assert!((11_000..11_500).contains(&world.now()), "{}", world.now());
    assert_eq!(world.next(&mut Fifo).unwrap(), Step::Idle);
    // Disarmed, it does not fire.
    world.wake(exact, Some(20_000)).unwrap();
    world.wake(exact, None).unwrap();
    assert_eq!(world.next(&mut Fifo).unwrap(), Step::Idle);
}

#[test]
fn many_timers_fire_in_time_order() {
    let mut world = world(3, Discipline::Ordered);
    let nodes: Vec<NodeId> = (0..16)
        .map(|_| world.node(Clock::default()).unwrap())
        .collect();
    let mut stream = Seeded::new(99);
    let mut expected = Vec::new();
    for round in 0..3 {
        // Each node re-armed several times; only the last deadline counts.
        for node in &nodes {
            for _ in 0..3 {
                let at = 1_000 * (round + 1) + stream.below(100);
                world.wake(*node, Some(at)).unwrap();
            }
        }
        let fired = drain(&mut world, &mut Random);
        assert_eq!(fired.len(), 16);
        let times: Vec<u64> = fired.iter().map(|f| f.0).collect();
        assert!(times.windows(2).all(|w| w[0] <= w[1]));
        expected.push(fired);
    }
}

#[test]
fn every_bound_refuses() {
    let limits = Limits {
        events: 2,
        nodes: 1,
        streams: 3,
        steps: 2,
        trace_words: 1,
    };
    let mut world: World<u64> = World::new(Source::Seed(1), Discipline::Free, limits).unwrap();
    let node = world.node(Clock::default()).unwrap();
    assert!(matches!(
        world.node(Clock::default()),
        Err(SimError::Full { what: "nodes", .. })
    ));
    let a = world.stream("a", &[]).unwrap();
    assert!(matches!(
        world.stream("b", &[]),
        Err(SimError::Full {
            what: "streams",
            ..
        })
    ));
    world.below(a, 9).unwrap();
    assert!(matches!(
        world.below(a, 9),
        Err(SimError::Full { what: "trace", .. })
    ));
    assert_eq!(world.below(a, 1), Ok(0), "no decision, no word");
    world.schedule(5, node, 0).unwrap();
    world.schedule(6, node, 1).unwrap();
    assert!(matches!(
        world.schedule(7, node, 2),
        Err(SimError::Full { what: "events", .. })
    ));
    assert_eq!(
        world.schedule(1, NodeId(4), 0),
        Err(SimError::UnknownNode(4))
    );
    assert!(matches!(world.next(&mut Fifo).unwrap(), Step::Event { .. }));
    assert_eq!(
        world.schedule(1, node, 3),
        Err(SimError::InThePast { at: 1, now: 5 })
    );
    assert!(matches!(world.next(&mut Fifo).unwrap(), Step::Event { .. }));
    world.schedule(9, node, 3).unwrap();
    assert_eq!(world.next(&mut Fifo).unwrap(), Step::Spent);
    assert!(matches!(
        World::<u64>::new(
            Source::Seed(1),
            Discipline::Free,
            Limits {
                trace_words: usize::MAX,
                ..limits
            }
        ),
        Err(SimError::Full { what: "trace", .. })
    ));
    assert_eq!(
        World::<u64>::new(Source::Seed(1), Discipline::Free, limits)
            .unwrap()
            .node(Clock {
                rate_ppm: -1_000_000,
                ..Clock::default()
            }),
        Err(SimError::Rate(-1_000_000))
    );
}

#[test]
fn a_run_until_a_time_stops_before_it_and_jumps_to_it() {
    let mut world = world(1, Discipline::Ordered);
    let node = world.node(Clock::default()).unwrap();
    assert_eq!(world.earliest(), None);
    world.schedule(30, node, 1).unwrap();
    world.wake(node, Some(20)).unwrap();
    assert_eq!(world.earliest(), Some(20));
    assert_eq!(world.advance(25), Err(SimError::Skips { due: 20, to: 25 }));
    world.advance(20).unwrap();
    assert_eq!(world.next(&mut Fifo).unwrap(), Step::Wake { node });
    assert_eq!(world.earliest(), Some(30));
    world.set_discipline(Discipline::Free);
    assert_eq!(world.earliest(), Some(30));
    // Under the free discipline time is the schedule's to give: a jump skips nothing.
    world.advance(40).unwrap();
    assert_eq!(world.now(), 40);
    assert!(matches!(
        world.next(&mut Fifo).unwrap(),
        Step::Event { event: 1, .. }
    ));
    assert_eq!(world.now(), 40, "the clock never goes back");
}

#[test]
fn a_strategy_cannot_pick_what_is_not_there() {
    struct Past;
    impl hyper_sim::Strategy for Past {
        fn pick(
            &mut self,
            choice: &hyper_sim::Choice<'_>,
            _: &mut hyper_sim::Draw<'_>,
        ) -> Result<usize, SimError> {
            Ok(choice.len())
        }
    }
    let mut world = world(1, Discipline::Ordered);
    let node = world.node(Clock::default()).unwrap();
    world.schedule(1, node, 1).unwrap();
    world.schedule(1, node, 2).unwrap();
    assert_eq!(
        world.next(&mut Past),
        Err(SimError::Pick {
            picked: 2,
            candidates: 2
        })
    );
    // Refused, nothing was taken.
    assert_eq!(world.pending(), 2);
}

#[test]
fn instants_differ_as_the_monotonic_clock_does() {
    let mut world = world(1, Discipline::Ordered);
    let node = world
        .node(Clock {
            offset_ns: 5_000,
            rate_ppm: 100,
            wall_ns: 1_700_000_000_000_000_000,
            ..Clock::default()
        })
        .unwrap();
    let start = world.instant(node).unwrap();
    world.schedule(1_000_000_000, node, 0).unwrap();
    world.next(&mut Fifo).unwrap();
    let elapsed = world.instant(node).unwrap() - start;
    // A second of virtual time is a second and 100 µs on a clock 100 ppm fast.
    assert_eq!(elapsed.as_nanos(), 1_000_100_000);
    assert_eq!(world.monotonic(node).unwrap(), 5_000 + 1_000_100_000);
    assert_eq!(
        world.wall(node).unwrap(),
        1_700_000_000_000_000_000 + 1_000_100_000
    );
    world.step_wall(node, -2_000_000_000).unwrap();
    assert_eq!(world.wall(node).unwrap(), 1_699_999_999_000_100_000);
    assert_eq!(world.monotonic(node).unwrap(), 5_000 + 1_000_100_000);
    assert_eq!(world.step_wall(node, i64::MIN), Err(SimError::TimeOverflow));
}

proptest! {
    // No failure file: the tests touch no file system, Miri's isolation included (the CI's Miri
    // lane runs fewer cases, each the same code).
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) { 8 } else { 256 },
        failure_persistence: None,
        ..ProptestConfig::default()
    })]
    /// The first virtual time a deadline is reached is exact: the clock reads at least the
    /// deadline there and less one nanosecond before.
    #[test]
    fn a_deadline_is_reached_exactly_when_the_clock_reads_it(
        offset in 0u64..1 << 40,
        rate in -500_000i32..500_000,
        local in 0u64..1 << 50,
    ) {
        let clock = Clock { offset_ns: offset, rate_ppm: rate, ..Clock::default() };
        let at = clock.virtual_at(local).unwrap();
        prop_assert!(clock.monotonic(at).unwrap() >= local);
        if at > 0 {
            prop_assert!(clock.monotonic(at - 1).unwrap() < local);
        }
    }

    /// The monotonic clock is the stated formula, `offset + ⌊v · (10⁶ + rate) / 10⁶⌋`, computed
    /// here in 128 bits.
    #[test]
    fn the_monotonic_clock_is_its_formula(
        offset in 0u64..1 << 40,
        rate in -999_999i32..1_000_000,
        v in 0u64..1 << 52,
    ) {
        let clock = Clock { offset_ns: offset, rate_ppm: rate, ..Clock::default() };
        let exact = i128::from(offset) + i128::from(v) * (1_000_000 + i128::from(rate)) / 1_000_000;
        prop_assert_eq!(i128::from(clock.monotonic(v).unwrap()), exact);
    }

    /// The monotonic clock never goes backward.
    #[test]
    fn the_monotonic_clock_never_goes_back(
        rate in -999_999i32..1_000_000,
        a in 0u64..1 << 50,
        b in 0u64..1 << 50,
    ) {
        let clock = Clock { rate_ppm: rate, ..Clock::default() };
        let (early, late) = (a.min(b), a.max(b));
        prop_assert!(clock.monotonic(early).unwrap() <= clock.monotonic(late).unwrap());
    }

    /// Any bound's draws are below it, and replay from the trace.
    #[test]
    fn draws_replay_for_any_bounds(seed in any::<u64>(), bounds in prop::collection::vec(any::<u64>(), 1..64)) {
        let run = |source: Source| {
            let mut world: World<u64> = World::new(source, Discipline::Free, LIMITS).unwrap();
            let s = world.stream("s", &[]).unwrap();
            let values: Vec<u64> = bounds.iter().map(|b| world.below(s, *b).unwrap()).collect();
            (values, world.finish())
        };
        let (values, record) = run(Source::Seed(seed));
        for (value, bound) in values.iter().zip(&bounds) {
            prop_assert!(*value < (*bound).max(1));
        }
        let (again, replayed) = run(Source::Trace(record.trace.clone()));
        prop_assert_eq!(again, values);
        prop_assert_eq!(replayed.digest, record.digest);
    }
}

/// A token ring under faults: five nodes, each arrival sent on to a node its own stream draws,
/// after a delay its link's stream draws, sometimes lost; every node's timer re-armed to inject a
/// fresh token. Every arrival is observed.
fn ring(source: Source, discipline: Discipline, leak: u64) -> Result<Record, SimError> {
    let mut world: World<u64> = World::new(
        source,
        discipline,
        Limits {
            steps: 5_000,
            ..LIMITS
        },
    )?;
    let late = Lateness {
        floor_ns: 10,
        spread_ns: 1_000,
    };
    let nodes: Vec<NodeId> = (0..5)
        .map(|i| {
            world.node(Clock {
                offset_ns: i * 1_000_000,
                rate_ppm: (i as i32 - 2) * 50,
                lateness: late,
                ..Clock::default()
            })
        })
        .collect::<Result<_, _>>()?;
    let own: Vec<_> = (0..5u64)
        .map(|i| world.stream("node", &[i]))
        .collect::<Result<_, _>>()?;
    let mut links = Vec::new();
    for from in 0..5u64 {
        for to in 0..5u64 {
            links.push(world.stream("link", &[from, to])?);
        }
    }
    for node in &nodes {
        let at = world.monotonic(*node)? + 50_000;
        world.wake(*node, Some(at))?;
    }
    world.observe(leak);
    let mut tokens = 0u64;
    loop {
        let (node, token) = match world.next(&mut hyper_sim::Random)? {
            Step::Event { node, event } => (node, event),
            Step::Wake { node } => {
                let at = world.monotonic(node)? + 50_000;
                world.wake(node, Some(at))?;
                tokens += 1;
                (node, tokens)
            }
            Step::Idle | Step::Spent => break,
        };
        world.observe((u64::from(node.0) << 32) | token);
        let from = u64::from(node.0);
        let to = world.below(own[node.0 as usize], 5)?;
        let link = links[(from * 5 + to) as usize];
        if world.chance(link, 50_000)? {
            continue;
        }
        let delay = 1_000 + world.below(link, 20_000)?;
        if world.pending() < 64 {
            world.after(delay, nodes[to as usize], token)?;
        }
    }
    Ok(world.finish())
}

#[test]
fn a_seed_runs_the_same_twice_and_from_its_trace_under_both_disciplines() {
    for discipline in [Discipline::Ordered, Discipline::Free] {
        let record = twice(1, |source| ring(source, discipline, 0)).unwrap();
        assert_eq!(record.steps, 5_000);
        assert!(!record.trace.is_empty());
        let other = twice(2, |source| ring(source, discipline, 0)).unwrap();
        assert_ne!(record.digest, other.digest);
    }
}

#[test]
fn the_check_refuses_state_that_leaks_between_runs() {
    let mut runs = 0u64;
    let refused = twice(1, |source| {
        runs += 1;
        ring(source, Discipline::Ordered, runs)
    });
    assert!(matches!(refused, Err(Twice::Seed { .. })), "{refused:?}");
}

#[test]
fn the_check_refuses_a_draw_made_outside_the_world() {
    // A choice drawn from the seed directly, not through the world: the trace cannot replay it.
    let refused = twice(1, |source| {
        let outside = match &source {
            Source::Seed(seed) => Seeded::new(*seed).next_u64(),
            Source::Trace(_) => 0,
        };
        ring(source, Discipline::Free, outside)
    });
    assert!(matches!(refused, Err(Twice::Replay { .. })), "{refused:?}");
}
