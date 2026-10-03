//! The allocation law for the detector's estimator and the election law (`CLAUDE.md` §1a): once
//! built, a heartbeat, a poll, a read of its estimates and a configuration allocate nothing, and
//! neither do the folds, a path's sample, the ballot, the span's search, the timing, the priority,
//! the pace, the round budget or the timer.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    missing_docs
)]

use std::time::Duration;

use hyper_measure::alloc;
use hyper_timing::{
    Ballot, Costs, ElectionTimer, ElectionTiming, Exposure, Flushes, Lateness, LinkEstimator,
    PathRtt, RoundAnchors, RoundBudget, Schedule, TickPace, quorum_priority,
};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

const MS: u64 = 1_000_000;

#[test]
fn a_heartbeat_a_poll_and_a_configuration_allocate_nothing() {
    assert!(alloc::installed());
    let interval = Duration::from_millis(50);
    let granularity = Duration::from_micros(1_000);
    let costs = Costs {
        election: Duration::from_micros(400),
        mtbf: Duration::from_secs(3_600),
    };
    let mut link =
        LinkEstimator::new(interval, granularity, Some(Schedule { seq: 0, at_ns: 0 })).unwrap();
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut delay = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        // A millisecond of jitter, and one heartbeat in a thousand stalled 40 ms.
        MS + state % MS
            + if state.is_multiple_of(1_000) {
                40 * MS
            } else {
                0
            }
    };
    let mut seq = 0u64;
    // Fill the window to the drift bound before counting.
    while seq < 4_000 {
        link.on_heartbeat(seq, seq * 50 * MS + delay()).unwrap();
        seq += 1;
    }
    link.configure(&costs, granularity, granularity).unwrap();
    let mut late = Lateness::new();
    let mut flushes = Flushes::new();
    let mut fleet = Exposure::new();
    alloc::begin();
    let mut configurations = 0;
    for _ in 0..100_000 {
        let arrival = seq * 50 * MS + delay();
        if let Some(deadline) = link.deadline().filter(|d| *d <= arrival) {
            link.poll(deadline);
        }
        link.on_heartbeat(seq, arrival).unwrap();
        std::hint::black_box(link.estimates());
        if link.reconfigure_due() {
            link.configure(&costs, granularity, granularity).unwrap();
            configurations += 1;
        }
        late.on_wait(arrival, arrival + 1_000).unwrap();
        flushes.on_flush(arrival, arrival + 4 * MS).unwrap();
        fleet.on_exposure(interval);
        seq += 1;
    }
    let counts = alloc::end();
    assert!(configurations > 0, "{configurations} configurations");
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (0, 0),
        "{counts:?}"
    );
}

#[test]
fn the_election_law_allocates_nothing_once_its_paths_are_built() {
    assert!(alloc::installed());
    let granularity = Duration::from_micros(45);
    let interval = Duration::from_millis(10);
    let correlation = Duration::from_millis(50);
    let mut paths: Vec<PathRtt> = (0..4)
        .map(|_| PathRtt::new(correlation, interval).unwrap())
        .collect();
    let detector = hyper_timing::Detector {
        interval: Duration::from_millis(50),
        margin: Duration::from_micros(65_800),
        detection: Duration::from_micros(115_900),
        mistake_recurrence: Duration::from_secs(86_400),
        unavailability: 4.5e-8,
    };
    let anchors = RoundAnchors {
        heartbeat_ns: 10 * MS,
        stall_periods: 6,
        polls_per_period: 4,
        lookahead: (3, 4),
    };
    let mut timer = ElectionTimer::new();
    let g = granularity.as_nanos() as u64;
    // Every path answered once: a five-voter ballot needs two.
    for path in &mut paths {
        path.on_sample(40 * MS);
    }
    alloc::begin();
    let mut derived = 0u32;
    for index in 0..20_000u64 {
        let path = &mut paths[(index % 4) as usize];
        path.on_sample(40 * MS + (index * 7_919) % (18 * MS));
        let ballot = Ballot::measure(&paths, 5, Duration::from_millis(4), granularity).unwrap();
        let span = ballot.span(granularity).unwrap();
        let timing = ElectionTiming::derive(interval, &detector, &span, &ballot);
        std::hint::black_box(quorum_priority(paths.iter().map(Some), 5, g));
        std::hint::black_box(TickPace::derive(
            interval,
            Duration::from_secs(5),
            10,
            &timing,
        ));
        std::hint::black_box(RoundBudget::derive(
            &anchors,
            paths[0].tail_ns(g),
            u64::from(timing.base_periods) * 10 * MS,
        ));
        std::hint::black_box(timing.delay(index, 0));
        let _ = std::hint::black_box(timer.follower_period(index / 4, &timing, 1, 0));
        derived += 1;
    }
    let counts = alloc::end();
    assert_eq!(derived, 20_000);
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (0, 0),
        "{counts:?}"
    );
}

/// A link moved to a longer interval keeps its ring: the drift bound needs fewer slots, so the
/// move allocates nothing, and nor do the heartbeats after it.
#[test]
fn a_longer_interval_reuses_the_ring() {
    assert!(alloc::installed());
    let granularity = Duration::from_micros(50);
    let mut link = LinkEstimator::new(Duration::from_millis(1), granularity, None).unwrap();
    for seq in 0..100u64 {
        link.on_heartbeat(seq, seq * MS + 3_000).unwrap();
    }
    alloc::begin();
    let mut seq = 100u64;
    for step in 2..40u64 {
        link.retime(Duration::from_millis(step), None).unwrap();
        for _ in 0..10 {
            link.on_heartbeat(seq, seq * step * MS).unwrap();
            seq += 1;
        }
    }
    let counts = alloc::end();
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (0, 0),
        "{counts:?}"
    );
    assert_eq!(link.interval(), Duration::from_millis(39));
}
