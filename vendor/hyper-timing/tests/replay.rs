//! The estimator run as the detector over synthetic traces of the recorded shapes
//! (`docs/timing.md` §2.2, §2.6; `docs/benchmarks.md`, "Heartbeat traces"). Every suspicion is
//! traced, event by event, to the detector's own rule: the heartbeat taken next came at or past
//! the freshness point in force, by its stamp, and its lateness past the expected arrival that
//! point was set from passed the margin; and every heartbeat that came so made one suspicion. The
//! mistakes and the bound the configurations put on them, summed over the heartbeats taken, are
//! reported (`docs/benchmarks.md`), not asserted. No raw trace is kept, so each shape is a model
//! fitted to a recorded run's table, its shape at the recorded interval reported beside the run's.
//! A stall is run two ways: the sender sends its backlog at the stall's end, as the trace
//! recorder's did, and it sends only the latest heartbeat due, the rest skipped, as the node-pair
//! stream's does. The models are seeded, and a replay run twice is the same run.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::cognitive_complexity,
    missing_docs
)]

use std::time::Duration;

use hyper_timing::{Costs, Event, LinkEstimator, Refusal, Schedule, lateness_bound};

/// A delay model: a body (a floor plus an exponential), occasional hiccups, and stalls. A stall
/// starts at Poisson times, lasts a Pareto time capped at the run's longest, and delays every
/// heartbeat scheduled inside it to its end, as a sender blocked on its scheduler or its disk does.
/// Microseconds and seconds.
#[derive(Clone, Copy)]
struct Shape {
    floor: f64,
    body: f64,
    hiccup: f64,
    hiccup_mean: f64,
    stalls_per_second: f64,
    stall_least: f64,
    stall_tail: f64,
    stall_most: f64,
}

/// macOS at 100 µs (`docs/benchmarks.md`, "macOS, 100 µs"): median 51.7 µs, MAD 14.8, mean 95.7,
/// deviation 521.5, `sd / (1.4826·MAD)` 24, longest 42 ms.
const MACOS: Shape = Shape {
    floor: 33.0,
    body: 25.0,
    hiccup: 0.02,
    hiccup_mean: 400.0,
    stalls_per_second: 0.15,
    stall_least: 2_000.0,
    stall_tail: 1.0,
    stall_most: 42_000.0,
};

/// macOS with a write and `F_FULLFSYNC` before each heartbeat at 10 ms ("macOS, write and
/// F_FULLFSYNC, 10 ms"): median 5.9 ms, MAD 0.58, mean 7.0, deviation 10.8, `sd / (1.4826·MAD)` 13,
/// p99.9 189 ms, longest 246 ms.
const FLUSH: Shape = Shape {
    floor: 5_000.0,
    body: 1_200.0,
    hiccup: 0.015,
    hiccup_mean: 6_000.0,
    stalls_per_second: 0.3,
    stall_least: 10_000.0,
    stall_tail: 1.0,
    stall_most: 250_000.0,
};

/// xorshift64* (Vigna 2016): a fixed seed gives the same trace on every machine.
struct Noise(u64);

impl Noise {
    fn uniform(&mut self) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        let word = self.0.wrapping_mul(0x2545_F491_4F6C_DD1D);
        ((word >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }
    fn exponential(&mut self, mean: f64) -> f64 {
        -mean * self.uniform().ln()
    }
}

/// Arrival times, nanoseconds on the sender's clock, of `count` heartbeats every `interval_us`
/// scheduled from zero, over a FIFO link (a heartbeat never passes the one before it). With `skips`,
/// a heartbeat scheduled inside a stall is never sent unless it is the latest due at the stall's
/// end, which is sent then: `None`.
fn trace(shape: Shape, interval_us: f64, count: usize, seed: u64, skips: bool) -> Vec<Option<u64>> {
    let mut noise = Noise(seed);
    let mut next_stall = noise.exponential(1e6 / shape.stalls_per_second);
    let mut stall_end = f64::NEG_INFINITY;
    let mut last = 0u64;
    (0..count)
        .map(|i| {
            let sigma = i as f64 * interval_us;
            while next_stall <= sigma {
                let length = (shape.stall_least / noise.uniform().powf(1.0 / shape.stall_tail))
                    .min(shape.stall_most);
                stall_end = stall_end.max(next_stall + length);
                next_stall += noise.exponential(1e6 / shape.stalls_per_second);
            }
            let mut delay = shape.floor + noise.exponential(shape.body);
            if noise.uniform() < shape.hiccup {
                delay += noise.exponential(shape.hiccup_mean);
            }
            let held = (stall_end - sigma).max(0.0);
            // The latest due at the stall's end is the one whose successor is due after it.
            if skips && held > interval_us {
                return None;
            }
            delay += held;
            let arrival = ((sigma + delay) * 1e3) as u64;
            last = last.max(arrival);
            Some(last)
        })
        .collect()
}

/// `(median, MAD, mean, deviation)` of the delays, microseconds, of the heartbeats sent.
fn summary(arrivals: &[Option<u64>], interval_us: f64) -> (f64, f64, f64, f64) {
    let delays: Vec<f64> = arrivals
        .iter()
        .enumerate()
        .filter_map(|(i, a)| a.map(|a| a as f64 / 1e3 - i as f64 * interval_us))
        .collect();
    let n = delays.len() as f64;
    let mean = delays.iter().sum::<f64>() / n;
    let sd = (delays.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / (n - 1.0)).sqrt();
    let mut sorted = delays.clone();
    sorted.sort_by(f64::total_cmp);
    let median = sorted[sorted.len() / 2];
    let mut deviations: Vec<f64> = delays.iter().map(|d| (d - median).abs()).collect();
    deviations.sort_by(f64::total_cmp);
    (median, deviations[deviations.len() / 2], mean, sd)
}

#[derive(Debug, PartialEq)]
struct Replay {
    mistakes: u64,
    /// Heartbeats taken under a margin: the freshness points that could each err once.
    points: u64,
    /// The bound on each summed, at the margin in force and the arrivals as they stood: the
    /// mistakes the configuration allows.
    allowed: f64,
    configurations: u64,
}

/// Runs the estimator as the detector over `arrivals`, as a sans-io driver runs it: every deadline
/// before the next arrival is polled, then the arrival is fed, and the detector is reconfigured
/// whenever its arrivals' window has renewed. The sender never fails, so every suspicion is a
/// mistake.
fn replay(
    arrivals: &[Option<u64>],
    interval: Duration,
    granularity: Duration,
    floor: Duration,
    costs: &Costs,
) -> (Replay, LinkEstimator) {
    let mut link =
        LinkEstimator::new(interval, granularity, Some(Schedule { seq: 0, at_ns: 0 })).unwrap();
    let mut run = Replay {
        mistakes: 0,
        points: 0,
        allowed: 0.0,
        configurations: 0,
    };
    for (seq, arrival) in arrivals.iter().enumerate() {
        let Some(arrival) = *arrival else {
            continue;
        };
        // The freshness point in force while the sender is trusted, and its margin.
        let (point, margin) = (link.deadline(), link.margin());
        let mut suspected = 0;
        while let Some(deadline) = link.deadline().filter(|d| *d <= arrival) {
            if link.poll(deadline) == Some(Event::Suspected) {
                suspected += 1;
            }
        }
        // The promise the freshness point in force made: its margin, on the arrivals as they
        // stood when it was set.
        let promised = margin
            .zip(link.arrivals().ok())
            .map(|(margin, arrivals)| lateness_bound(&arrivals, margin));
        link.on_heartbeat(seq as u64, arrival).unwrap();
        // The rule, traced: a suspicion in this gap exactly when the heartbeat came at or past the
        // freshness point in force, which is exactly when its lateness reached the margin.
        let late = point.is_some_and(|until| until <= arrival);
        assert_eq!(suspected, u64::from(late), "heartbeat {seq} at {arrival}");
        if let (Some(_), Some(margin), Some(lateness)) = (point, margin, link.latest_lateness()) {
            assert_eq!(
                late,
                lateness >= i64::try_from(margin.as_nanos()).unwrap(),
                "heartbeat {seq}: lateness {lateness} ns, margin {margin:?}"
            );
        }
        run.mistakes += suspected;
        if let (Some(bound), Some(_)) = (promised, link.latest_lateness()) {
            run.points += 1;
            run.allowed += bound;
        }
        if link.reconfigure_due() {
            match link.configure(costs, granularity, floor) {
                Ok(_) => run.configurations += 1,
                Err(Refusal::TooFewHeartbeats | Refusal::CorrelationUnmeasured) => {}
                Err(refusal @ (Refusal::Unconfigurable | Refusal::Unavailable)) => {
                    panic!("{refusal:?} at {seq}")
                }
            }
        }
    }
    (run, link)
}

struct Case {
    name: &'static str,
    shape: Shape,
    /// The recorded run's interval, microseconds, and its heartbeats: the shape is checked on a
    /// run as long.
    recorded_us: f64,
    recorded: usize,
    /// The recorded run's `sd / (1.4826·MAD)`, and its mean over its median.
    heavy: f64,
    skew: f64,
    /// The detector's intervals: one inside the stalls' correlation time, where heartbeats in a
    /// margin are late together, and the one past it that Theorem 7's product needed.
    intervals: [Duration; 2],
    granularity: Duration,
    sender: Duration,
    election: Duration,
}

/// Heartbeats a replay runs: four simulated hours at 50 ms.
const REPLAYED: usize = 288_000;

fn check(case: &Case) {
    // The model has the recorded run's shape at the recorded interval.
    let recorded = trace(
        case.shape,
        case.recorded_us,
        case.recorded,
        0x9E37_79B9_7F4A_7C15,
        false,
    );
    let (median, mad, mean, sd) = summary(&recorded, case.recorded_us);
    let heavy = sd / (1.482_602_218_505_602 * mad);
    println!(
        "{}: at the recorded interval median {median:.1} µs, MAD {mad:.1}, mean {mean:.1}, sd {sd:.1}, sd/(1.4826·MAD) {heavy:.1} (recorded {}), mean/median {:.2} (recorded {})",
        case.name,
        case.heavy,
        mean / median,
        case.skew
    );
    // The detector at each interval, its stalls sent late or skipped, for nodes failing every hour
    // (tight margins, so mistakes happen) and every month.
    for interval in case.intervals {
        for skips in [false, true] {
            let interval_us = interval.as_secs_f64() * 1e6;
            let arrivals = trace(
                case.shape,
                interval_us,
                REPLAYED,
                0x2545_F491_4F6C_DD1D,
                skips,
            );
            let sent = arrivals.iter().flatten().count();
            for mtbf in [3_600u64, 30 * 86_400] {
                let costs = Costs {
                    election: case.election,
                    mtbf: Duration::from_secs(mtbf),
                };
                let (run, link) =
                    replay(&arrivals, interval, case.granularity, case.sender, &costs);
                let (again, _) = replay(&arrivals, interval, case.granularity, case.sender, &costs);
                assert_eq!(run, again, "a seeded replay is the same run");
                let estimates = link.estimates();
                println!(
                    "{} at {interval:?}, {}, MTBF {mtbf} s: {} mistakes in {} heartbeats taken of {} \
                     sent, {} allowed; {} configurations, margin {:?}, window {:?}, τ_int {:?}, \
                     arrivals {}",
                    case.name,
                    if skips {
                        "stalls skipped"
                    } else {
                        "stalls sent late"
                    },
                    run.mistakes,
                    run.points,
                    sent,
                    run.allowed,
                    run.configurations,
                    link.margin(),
                    estimates.window,
                    estimates.correlation,
                    estimates.arrivals
                );
                assert!(run.configurations > 0);
            }
        }
    }
}

#[test]
fn every_mistake_on_the_macos_shape_is_the_rules() {
    check(&Case {
        name: "macOS 100 µs",
        shape: MACOS,
        recorded_us: 100.0,
        recorded: 3_000_000,
        heavy: 24.0,
        skew: 95.7 / 51.7,
        intervals: [Duration::from_millis(5), Duration::from_millis(50)],
        granularity: Duration::from_nanos(45_400),
        sender: Duration::from_nanos(45_400),
        election: Duration::from_nanos(378_200),
    });
}

#[test]
fn every_mistake_on_the_flush_shape_is_the_rules() {
    check(&Case {
        name: "macOS F_FULLFSYNC 10 ms",
        shape: FLUSH,
        recorded_us: 10_000.0,
        recorded: 60_000,
        heavy: 13.0,
        skew: 7_002.5 / 5_889.7,
        // The model's stalls block the sender for up to 250 ms. At 200 ms two heartbeats in a
        // margin are late together, and Theorem 7's product, which multiplies their chances as
        // independent, broke its bound there (19 mistakes against 11.5 at an hour's MTBF); the
        // lateness of the arrival is one event, bounded alone.
        intervals: [Duration::from_millis(20), Duration::from_millis(200)],
        granularity: Duration::from_nanos(1_627_400),
        sender: Duration::from_nanos(4_535_800 + 1_627_400),
        election: Duration::from_nanos(5_670_000),
    });
}

/// A sender that stops is suspected within the detection bound the configurator promised, its
/// stalls before the stop sent late or skipped.
#[test]
fn a_stopped_sender_is_suspected_within_the_detection_bound() {
    let interval = Duration::from_millis(50);
    let granularity = Duration::from_nanos(45_400);
    for skips in [false, true] {
        let arrivals = trace(MACOS, 50_000.0, 20_000, 7, skips);
        let costs = Costs {
            election: Duration::from_nanos(378_200),
            mtbf: Duration::from_secs(30 * 86_400),
        };
        let (_, mut link) = replay(&arrivals, interval, granularity, granularity, &costs);
        let configured = link.configure(&costs, granularity, granularity).unwrap();
        // The sender stops right after its last heartbeat was scheduled: no heartbeat after.
        let last = arrivals.iter().rposition(Option::is_some).unwrap() as u64;
        let last_scheduled = last * 50_000_000;
        let Some(deadline) = link.deadline() else {
            // The last heartbeat came late: the sender is already suspected.
            assert_eq!(link.trust(), hyper_timing::Trust::Suspected);
            continue;
        };
        assert_eq!(link.poll(deadline), Some(Event::Suspected));
        let detected = Duration::from_nanos(deadline - last_scheduled);
        assert!(
            detected <= configured.current.detection + Duration::from_nanos(1),
            "detected after {detected:?}, bound {:?}",
            configured.current.detection
        );
    }
}
