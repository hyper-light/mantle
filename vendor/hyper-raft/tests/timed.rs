//! Replication across five Azure regions in time (`tests/support/timed.rs`): slates' pipelining
//! measurement (slates `crates/cluster/tests/pipelining.rs`, `docs/wip/BENCHMARKS.md`, "Pipelined
//! replication across five regions", 2026-09-29; mantle note 32 R16) run on this core. slates measured
//! that with no window its group, sending one batch a period to each follower, committed 1,029 of
//! 2,000 proposals a second at a 7,319 ms median, and with a window of one batch ahead all of them at
//! 172 ms.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cognitive_complexity,
    unreachable_pub
)]
mod support;

use support::Settings;
use support::timed::{MS, Outcome, SECOND, Scenario, Window, regions, run};

/// Shape: slates' batch budget across five regions, the bytes one append carries
/// (`raft_wire::append_batch_bytes`, a fresh session's first credit, 4,367 bytes).
const BATCH: u64 = 4_367;
/// Shape: what each path carries, ten megabits a second: well above what the stream offers any
/// one member, so that the window, not the path, decides.
const PATH_BYTES_PER_SECOND: u64 = 1_250_000;
/// Shape: the jitter added to each one-way delay, slates' few milliseconds of queueing.
const JITTER_NS: u64 = 5 * MS;
/// Shape: when the stream begins, after the founding on these paths.
const PROPOSE_FROM_NS: u64 = 2 * SECOND;
/// Shape: slates' stream entries hold a counter.
const PAYLOAD: usize = 8;
/// Shape: how long a run goes on after its stream: three of the slowest round trips among the
/// regions (Southeast Asia and Brazil South, 331 ms), time for what was proposed last to commit.
const SETTLE_NS: u64 = 3 * 331 * MS;

/// What the environment variable `name` says, or `default`.
fn count(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn scenario(seed: u64, window: Window, every_ns: u64, stream_ns: u64, loss_ppm: u64) -> Scenario {
    Scenario {
        paths: regions(5, JITTER_NS, PATH_BYTES_PER_SECOND),
        loss_ppm,
        ordered: false,
        seed,
        leader: 1,
        settings: Settings {
            max_size_per_msg: BATCH,
            // The window counts no messages of its own: its bytes bound it (mantle note 32 R16).
            max_inflight_msgs: usize::MAX,
            ..Settings::focal().by_suspicion()
        },
        window,
        propose_every_ns: every_ns,
        propose_from_ns: PROPOSE_FROM_NS,
        stream_ns,
        settle_ns: SETTLE_NS,
        payload: PAYLOAD,
    }
}

fn describe(outcome: &Outcome, stream_ns: u64) -> String {
    format!(
        "median {} ms, p99 {} ms, {} committed of {} ({}/s, {} left), {} messages, {} bytes, {} of {} entries resent, {} bytes queued at most",
        outcome.latency_pct(50) / MS,
        outcome.latency_pct(99) / MS,
        outcome.latencies_ns.len(),
        outcome.proposed,
        outcome.commits_per_second(stream_ns),
        outcome.uncommitted,
        outcome.messages,
        outcome.bytes,
        outcome.entries_resent,
        outcome.entries_sent,
        outcome.queued_bytes
    )
}

/// R16's rule against a window of one batch, at 2,000 proposals a second across five regions on
/// paths that keep order (slates' overloaded row): with one batch out at a time to each member the
/// group falls behind, and proposals are left uncommitted after the stream and three round trips
/// more; with twice what each path carries in a round trip every proposal commits. Exact for each
/// seed: the simulation is its seed.
#[test]
fn a_window_of_two_round_trips_keeps_up_where_one_batch_does_not() {
    let stream_ns = SECOND;
    // Paths that keep order, as slates' did: its drive sent appends a period apart.
    let scenario = |seed, window| Scenario {
        ordered: true,
        ..scenario(seed, window, 500_000, stream_ns, 0)
    };
    for seed in 0..2 {
        let one = run(&scenario(seed, Window::Bytes(BATCH)));
        eprintln!("seed {seed}, one batch: {}", describe(&one, stream_ns));
        assert!(
            one.uncommitted > 0,
            "seed {seed}: {}",
            describe(&one, stream_ns)
        );
        let rule = run(&scenario(seed, Window::RoundTrips(2)));
        eprintln!("seed {seed}, the rule: {}", describe(&rule, stream_ns));
        assert_eq!(
            rule.uncommitted,
            0,
            "seed {seed}: {}",
            describe(&rule, stream_ns)
        );
        assert_eq!(rule.proposed, one.proposed);
    }
}

/// R17 on paths that reorder each message by its own jitter, at 2,000 proposals a second with the
/// rule's window: where members keep what arrives ahead of a hole and the leader sends the hole
/// alone, every proposal commits and less is sent again than where members refuse it and the
/// leader probes and sends what followed again (raft-rs's rule, `Ahead::Refused`). Exact for each
/// seed: the simulation is its seed.
#[test]
fn on_paths_that_reorder_what_arrives_ahead_is_not_sent_again() {
    let stream_ns = SECOND;
    for seed in 0..2 {
        let kept = run(&scenario(
            seed,
            Window::RoundTrips(2),
            500_000,
            stream_ns,
            0,
        ));
        eprintln!("seed {seed}, kept: {}", describe(&kept, stream_ns));
        let mut refusing = scenario(seed, Window::RoundTrips(2), 500_000, stream_ns, 0);
        refusing.settings.refuse_ahead = true;
        let refused = run(&refusing);
        eprintln!("seed {seed}, refused: {}", describe(&refused, stream_ns));
        assert_eq!(
            kept.uncommitted,
            0,
            "seed {seed}: {}",
            describe(&kept, stream_ns)
        );
        assert!(
            kept.entries_resent < refused.entries_resent,
            "seed {seed}: kept {}, refused {}",
            describe(&kept, stream_ns),
            describe(&refused, stream_ns)
        );
    }
}

/// A measurement tool: every window × rate × loss × path order, printed a line a run, for
/// `docs/benchmarks.md`. `HYPER_RAFT_TIMED_SEEDS=4 HYPER_RAFT_TIMED_STREAM_S=10 cargo test -p hyper-raft
/// --release --test timed -- --ignored --exact replication_across_rates_and_loss --nocapture`.
#[test]
#[ignore = "a measurement tool, run by hand"]
fn replication_across_rates_and_loss() {
    let seeds = count("HYPER_RAFT_TIMED_SEEDS", 2);
    // A count of messages for the window as well, as focal's 128 (`Settings::focal`).
    let places = std::env::var("HYPER_RAFT_TIMED_PLACES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok());
    // raft-rs's rule for what arrives ahead of a hole (`Ahead::Refused`), to compare.
    let refuse_ahead =
        std::env::var("HYPER_RAFT_TIMED_AHEAD").is_ok_and(|value| value == "refused");
    let stream_ns = count("HYPER_RAFT_TIMED_STREAM_S", 5) * SECOND;
    for ordered in [false, true] {
        for loss_ppm in [0, 10_000] {
            for every_us in [50_000, 2_000, 1_000, 500, 250] {
                for window in [
                    Window::Bytes(BATCH),
                    Window::Bytes(4 * BATCH),
                    Window::RoundTrips(1),
                    Window::RoundTrips(2),
                ] {
                    for seed in 0..seeds {
                        let mut run_of = Scenario {
                            ordered,
                            ..scenario(seed, window, every_us * 1_000, stream_ns, loss_ppm)
                        };
                        if let Some(places) = places {
                            run_of.settings.max_inflight_msgs = places;
                        }
                        run_of.settings.refuse_ahead = refuse_ahead;
                        let outcome = run(&run_of);
                        eprintln!(
                            "ordered {ordered}, loss {loss_ppm} ppm, every {every_us} us, {window:?}, seed {seed}: {}",
                            describe(&outcome, stream_ns)
                        );
                    }
                }
            }
        }
    }
}
