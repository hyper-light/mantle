//! The idle spin is opened by client activity and bounded by its window (§4.7 "Shards poll rings while
//! any client has activity within the measured idle window"; §4.3 "Loop"; A-42).
//!
//! A shard with nothing to run spins before it parks only inside the window its last client activity
//! opened ([`hyper_rt::shard::ShardContext::note_activity`]), and only until that window's end: timers that
//! fire after the client has gone quiet do not open a fresh window each. Until 2026-09-29 the daemon set a
//! shard "active" at its first client's handoff and nothing cleared it, so every timer of an idle daemon
//! ended in a full spin: an idle solo daemon spent 1.6–1.8 % of a CPU after one client had come and gone
//! (0.07 % before any), and an idle fleet pod's shard 15–32 % of a core
//! (docs/bugs/2026-09-29-an-idle-shard-spun-for-good-once-a-client-had-connected.md).

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::string_slice,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn,
    clippy::missing_panics_doc
)]
// Test harness code: an unwrap here is a failed test, which is what it should be.

use std::sync::mpsc::channel;
use std::time::{Duration, Instant};

use hyper_rt::futures::sleep;
use hyper_rt::registry;
use hyper_rt::runtime::{Runtime, RuntimeConfig};
use hyper_rt::shard::ShardId;
use hyper_rt::shard_loop::Counters;

/// Shape: the idle window — long against the timer period, so the window holds dozens of idle moments,
/// and short enough that the test spans a few of them in well under a second.
const WINDOW_NS: u64 = 200_000_000;
/// Shape: the period of the task whose timers make the shard go idle and wake again, 40 per window.
const TICK_NS: u64 = 5_000_000;
/// Shape: a stretch with no client activity, as long as a window: 40 idle moments, each of which would
/// spin if an idle moment opened a window of its own.
const QUIET: Duration = Duration::from_nanos(WINDOW_NS);
/// Shape: how long a shard gets to answer a counters question (a step is microseconds).
const ANSWER: Duration = Duration::from_secs(5);

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: WINDOW_NS,
        wake_tracking: None,
    }
}

/// The idle spins the shard has entered: every spin ends in a hit, a miss or a timer's deadline.
fn spins(counters: &Counters) -> u64 {
    counters.spin_hits + counters.spin_misses + counters.spin_deadlines
}

/// The shard's counters, asked of the shard by a task (which is no client activity).
fn counters_of(rt: &Runtime, shard: ShardId) -> Counters {
    let (tx, rx) = channel();
    rt.spawn_on(shard, async move {
        let _ = tx.send(registry::with_current(|ctx| ctx.counters()).unwrap_or_default());
    })
    .unwrap();
    rx.recv_timeout(ANSWER).unwrap()
}

/// Waits `span` on the test thread (the stretch is the measurement; the shard's own timers run through
/// it). A channel nobody sends on is the wait, as the runtime's disallowed `sleep` is for shipped code.
fn wait(span: Duration) {
    let (_hold, rx) = channel::<()>();
    let started = Instant::now();
    while started.elapsed() < span {
        let _ = rx.recv_timeout(span.saturating_sub(started.elapsed()));
    }
}

/// A-42 by use. Do: run a periodic timer task on an idle shard; wait a stretch with no client activity;
/// note one client activity; wait out its window; wait another quiet stretch. Expect: no spin in the
/// first stretch (no activity, no window), spins inside the window the activity opened (the fast path is
/// live: the non-vacuity count), and none in the stretch after the window ended — the timers after the
/// client went quiet open no window of their own.
#[test]
fn a_shard_spins_only_inside_the_window_its_last_client_activity_opened() {
    let rt = Runtime::start(&config()).unwrap();
    let shard = rt.shard_ids()[0];
    rt.spawn_on(shard, async {
        loop {
            sleep(TICK_NS).await.unwrap();
        }
    })
    .unwrap();
    let start = counters_of(&rt, shard);
    wait(QUIET);
    let quiet = counters_of(&rt, shard);
    assert_eq!(
        spins(&quiet),
        spins(&start),
        "an idle shard with no client activity spun: {quiet:?}"
    );
    rt.spawn_on(shard, async {
        registry::with_current(|ctx| ctx.note_activity());
    })
    .unwrap();
    wait(Duration::from_nanos(WINDOW_NS).saturating_mul(2));
    let after_window = counters_of(&rt, shard);
    assert!(
        spins(&after_window) > spins(&quiet),
        "the window the activity opened never spun: {after_window:?}"
    );
    wait(QUIET);
    let later = counters_of(&rt, shard);
    assert_eq!(
        spins(&later),
        spins(&after_window),
        "the shard spun after its client's window had ended: {later:?}"
    );
    rt.shutdown().unwrap();
}

/// Opens a client-activity window on `shard` from one of its own tasks.
fn note_activity(rt: &Runtime, shard: ShardId) {
    rt.spawn_on(shard, async {
        registry::with_current(|ctx| ctx.note_activity());
    })
    .unwrap();
}

/// mantle's E2 range bench found a shard with no readiness wait spending half its time in `kevent`: the
/// spin polled the driver every turn and a busy loop every quantum, though nothing was armed. Do: open a
/// window on a shard that holds no readiness wait and let it spin the window out. Expect: it spun, and
/// polled the driver not once (`harvests` unmoved): with nothing armed the driver holds nothing to poll.
#[test]
fn a_spin_with_no_readiness_wait_polls_no_driver() {
    let rt = Runtime::start(&config()).unwrap();
    let shard = rt.shard_ids()[0];
    let before = counters_of(&rt, shard);
    note_activity(&rt, shard);
    wait(Duration::from_nanos(WINDOW_NS).saturating_mul(2));
    let after = counters_of(&rt, shard);
    assert!(
        spins(&after) > spins(&before),
        "the window never spun: {after:?}"
    );
    assert_eq!(
        after.harvests, before.harvests,
        "the shard polled a driver with nothing armed: {after:?}"
    );
    rt.shutdown().unwrap();
}

/// Do: hold a read wait on a socket nobody sends to, open a window, and let the shard spin it out.
/// Expect: the spin polled the driver (non-vacuity), and at most once a quantum: the quantum here (the
/// configured step budget, with no wake tracking) is longer than the window, so each spin polls once, at
/// its start, and the busy path at most once more in the stretch — never once a turn.
#[test]
fn a_spin_with_a_readiness_wait_polls_the_driver_once_a_quantum() {
    use hyper_rt::udp::{Ipv4Addr, SocketAddr, UdpSocket};

    let rt = Runtime::start(&config()).unwrap();
    let shard = rt.shard_ids()[0];
    let (armed, is_armed) = channel();
    rt.spawn_on(shard, async move {
        let idle = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
        let wait = idle.readable();
        let mut wait = std::pin::pin!(wait);
        // Polled once, the wait is registered; the task then holds it, unfired, for the test's span.
        std::future::poll_fn(|cx| {
            let _ = wait.as_mut().poll(cx);
            std::task::Poll::Ready(())
        })
        .await;
        armed.send(()).unwrap();
        wait.await.unwrap();
    })
    .unwrap();
    is_armed.recv_timeout(ANSWER).unwrap();
    let before = counters_of(&rt, shard);
    note_activity(&rt, shard);
    wait(Duration::from_nanos(WINDOW_NS).saturating_mul(2));
    let after = counters_of(&rt, shard);
    let spun = spins(&after) - spins(&before);
    let polled = after.harvests - before.harvests;
    assert!(spun > 0, "the window never spun: {after:?}");
    assert!(
        polled > 0,
        "a spin with a wait armed never polled the driver: {after:?}"
    );
    assert!(
        polled <= spun + 1,
        "{polled} driver polls in {spun} spins: the spin polls once a quantum, not once a turn"
    );
    rt.shutdown().unwrap();
}
