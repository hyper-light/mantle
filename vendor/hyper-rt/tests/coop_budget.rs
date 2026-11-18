//! A task's turn by time (Kiln's `kiln-rt` `tests/coop_budget.rs`, its goal 6, plan 01a1a59c2abe step 1;
//! `hyper_rt::coop`, docs/runtime.md §3.14): a task looping on always-ready work no longer holds its shard
//! past its share. Its leaves (a timer already due, a socket that always has bytes or room) charge it, and
//! past its share, with something else wanting the shard, it yields at its next await, with no yield of its
//! own written in. Alone on its shard it goes on with no round trip, a long computation yields through
//! `consume_budget`, and each poll makes at least one operation, so a poll whose own work outlasts its share
//! is never refused at every poll. A simulated shard keeps its schedules.
// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation,
    clippy::indexing_slicing
)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};

use hyper_rt::coop::consume_budget;
use hyper_rt::futures::{sleep, spawn};
use hyper_rt::machine::clock::monotonic_ns;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::tcp::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};

/// Shape: the share under test, a fixed step budget (no wake tracking), long enough that a loaded host's
/// scheduling noise is a small part of it and short enough that a test runs many shares in well under a
/// second.
const QUANTUM_NS: u64 = 2_000_000;

/// Shape: one item of the hog's work, a twentieth of the share: many items fit a share, so a hog that did
/// not yield would run many shares in one poll.
const ITEM_NS: u64 = QUANTUM_NS / 20;

/// Shape: how long the hog loops, in shares: enough for its competitor's timer to fall due many times.
const SHARES: u64 = 40;

/// Shape: the hog's work, in items: `SHARES` shares' worth. Counted, not timed: a host that takes the
/// test's thread away for longer than the whole loop stretches it, and every share's evidence is still
/// there.
const ITEMS: u64 = SHARES * (QUANTUM_NS / ITEM_NS);

/// The most of the hog's items a competitor waits behind once its timer is armed, where the hog yields at
/// its share: each item holds the shard at least `ITEM_NS` of its clock, so a share holds
/// `QUANTUM_NS / ITEM_NS` of them, and the item running as the share ends finishes first. A host's
/// deschedules only end a share sooner, in fewer items.
const ITEMS_PAST: u64 = QUANTUM_NS / ITEM_NS + 1;

/// Shape: a listener's backlog, room for the one connection a test makes.
const BACKLOG: u32 = 4;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 64,
        ring_entries: 16,
        step_budget_ns: QUANTUM_NS,
        // Shape: the wheel's tick, a twentieth of the share, so a competitor's timer falls due within a
        // share.
        timer_tick_ns: QUANTUM_NS / 20,
        batch: 16,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// Spins for `ns` on the CPU: an item of work with no await in it.
fn work(ns: u64) {
    let until = monotonic_ns() + ns;
    while monotonic_ns() < until {
        std::hint::spin_loop();
    }
}

/// A connected pair on loopback: the client's end and the server's.
async fn pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), BACKLOG).unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let server = listener.accept().await.unwrap();
    (client, server)
}

/// A leaf of the caller's own that is always ready and charges nothing: what a loop over work no runtime
/// leaf sees looks like, for the control.
struct Uncharged;

impl Future for Uncharged {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}

/// The hog's always-ready leaf.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Leaf {
    /// A timer already due.
    Timer,
    /// A socket's read of a byte its peer sent before the loop began.
    Read,
    /// A socket's write of a byte into a send buffer with room for every item's.
    Write,
    /// The caller's own, which charges nothing: the control.
    Uncharged,
}

/// Shape: room for every run a competitor can make in a hog's loop: at most one a hog's poll, and a hog's
/// poll holds at least one item.
const RUNS_KEPT: usize = ITEMS as usize;

/// What a competitor saw while the hog looped: the hog's items done so far, as the hog counts them, and for
/// each of the competitor's runs, how many of them it waited behind from arming its timer to running, each
/// run in a place of its own.
#[derive(Debug)]
struct Seen {
    items: AtomicU64,
    runs: AtomicU64,
    items_past: Vec<AtomicU64>,
}

impl Default for Seen {
    fn default() -> Self {
        Seen {
            items: AtomicU64::new(0),
            runs: AtomicU64::new(0),
            items_past: (0..RUNS_KEPT).map(|_| AtomicU64::new(0)).collect(),
        }
    }
}

impl Seen {
    /// The hog's item done: counted for the competitor.
    fn item_done(&self) {
        self.items.fetch_add(1, Ordering::Relaxed);
    }

    /// The most of the hog's items the competitor waited behind, over its runs.
    fn most_items_past(&self) -> u64 {
        let runs = usize::try_from(self.runs.load(Ordering::Relaxed))
            .unwrap()
            .min(RUNS_KEPT);
        self.items_past[..runs]
            .iter()
            .map(|p| p.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0)
    }
}

/// State a test's tasks share, for the test's life: leaked, a few words a test.
fn shared<T>(value: T) -> &'static T {
    Box::leak(Box::new(value))
}

/// A competitor: wakes on a timer every quarter share while `hog` runs, noting how many of the hog's items
/// it waited behind from arming the timer to running.
async fn compete(seen: &'static Seen, hog_done: &'static AtomicBool) {
    while !hog_done.load(Ordering::Acquire) {
        let armed = seen.items.load(Ordering::Relaxed);
        sleep(QUANTUM_NS / 4).await.unwrap();
        if hog_done.load(Ordering::Acquire) {
            break;
        }
        let run = usize::try_from(seen.runs.fetch_add(1, Ordering::Relaxed)).unwrap();
        if let Some(place) = seen.items_past.get(run) {
            place.store(
                seen.items.load(Ordering::Relaxed) - armed,
                Ordering::Relaxed,
            );
        }
    }
}

/// Runs a hog for `ITEMS` items on `leaf` beside a competitor. What the competitor saw, the hog's polls,
/// and the shard's counters.
fn hog_beside_a_competitor(leaf: Leaf) -> (&'static Seen, u64, hyper_rt::shard_loop::Counters) {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let seen = shared(Seen::default());
    let hog_polls = shared(AtomicU64::new(0));
    let (seen_in, polls_in) = (seen, hog_polls);
    rt.block_on(async move {
        let (client, server) = pair().await;
        if leaf == Leaf::Read {
            // Every item's byte, sent before the loop: each read finds one ready.
            client.write_all(&[0; ITEMS as usize]).await.unwrap();
        }
        let hog_done = shared(AtomicBool::new(false));
        spawn(compete(seen_in, hog_done)).unwrap();
        // The competitor's first poll, which arms its timer.
        hyper_rt::futures::yield_now().await;
        let mut last_task_poll = None;
        let mut byte = [0u8; 1];
        for _ in 0..ITEMS {
            match leaf {
                Leaf::Timer => sleep(0).await.unwrap(),
                Leaf::Read => assert_eq!(server.read(&mut byte).await.unwrap(), 1),
                Leaf::Write => assert_eq!(client.write(&byte).await.unwrap(), 1),
                Leaf::Uncharged => Uncharged.await,
            }
            // A new poll of this task shows as a new count of the shard's polls.
            let polls = hyper_rt::futures::shard_counters().unwrap().polls;
            if last_task_poll != Some(polls) {
                last_task_poll = Some(polls);
                polls_in.fetch_add(1, Ordering::Relaxed);
            }
            work(ITEM_NS);
            seen_in.item_done();
        }
        hog_done.store(true, Ordering::Release);
    })
    .unwrap();
    let counters = rt.counters();
    (seen, hog_polls.load(Ordering::Relaxed), counters)
}

/// The slice's acceptance, for each always-ready leaf: a task looping on it no longer holds its shard past
/// its share. Do: a hog loops on a timer already due, a socket read whose byte is there, or a socket write
/// with room (each a runtime leaf, so charged) for forty shares' worth of items, each a twentieth of a
/// share, beside a competitor whose timer falls due every quarter share. Expect: the hog yields at its
/// share, so it is polled about once a share and the competitor runs as often, never waiting behind more
/// than a share and an item of the hog's work once its timer is armed; the shard counts the yields, and no
/// long poll of the hog's (one past twice its share on the CPU, which preemptions are not). All counted in
/// the shard's own terms, items and polls, which a host's deschedules don't take away.
#[test]
fn a_task_looping_on_always_ready_work_no_longer_holds_its_shard_past_its_share() {
    for leaf in [Leaf::Timer, Leaf::Read, Leaf::Write] {
        let (seen, hog_polls, counters) = hog_beside_a_competitor(leaf);
        let runs = seen.runs.load(Ordering::Relaxed);
        let most_past = seen.most_items_past();
        eprintln!(
            "{leaf:?} hog: {hog_polls} polls; competitor {runs} runs, behind {most_past} items at most; {counters:?}"
        );
        assert!(
            hog_polls >= SHARES / 2,
            "{leaf:?}: the hog yielded at its share: {hog_polls} polls in {SHARES} shares"
        );
        // The hog yields at each share's end, and the competitor, due every quarter share, runs then: about
        // once a share.
        assert!(
            runs >= SHARES / 2,
            "{leaf:?}: the competitor ran while the hog looped: {runs} runs in {SHARES} shares"
        );
        assert!(
            most_past <= ITEMS_PAST,
            "{leaf:?}: the competitor waited behind a share and an item of the hog's work at the most: {most_past} items"
        );
        assert!(
            counters.coop_yields >= SHARES / 2,
            "{leaf:?}: the yields were the budget's: {counters:?}"
        );
        assert_eq!(
            counters.long_steps, 0,
            "{leaf:?}: no poll of the hog held the shard past twice its share on the CPU: {counters:?}"
        );
    }
}

/// The control: the same hog on a leaf of its own that charges nothing holds its shard for the whole loop,
/// in one poll, and the competitor never runs while it loops.
#[test]
fn an_uncharged_hog_holds_its_shard_for_its_whole_loop() {
    let (seen, hog_polls, counters) = hog_beside_a_competitor(Leaf::Uncharged);
    eprintln!(
        "uncharged hog: {hog_polls} polls; competitor {} runs",
        seen.runs.load(Ordering::Relaxed)
    );
    assert_eq!(
        hog_polls, 1,
        "uncharged, the hog held its shard for the whole loop"
    );
    assert_eq!(
        seen.runs.load(Ordering::Relaxed),
        0,
        "the competitor never ran while the uncharged hog looped"
    );
    assert_eq!(counters.coop_yields, 0);
}

/// Alone on its shard, a charge needs no time and no round trip: a thousand `consume_budget`s and a
/// thousand due timers are one poll, as `maybe_yield`'s are (docs/runtime.md §3.10).
#[test]
fn alone_on_its_shard_a_charged_task_is_polled_once() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let before = rt.counters().polls;
    rt.block_on(async {
        for _ in 0..1000 {
            consume_budget().await;
            sleep(0).await.unwrap();
        }
    })
    .unwrap();
    assert_eq!(rt.counters().polls - before, 1, "no want: no round trip");
    assert_eq!(rt.counters().coop_yields, 0);
}

/// A long computation yields through the one call: pieces of a twentieth of a share with `consume_budget`
/// between them let a competitor's due timer in within a share and a piece of the computation, and the
/// computation goes on to its last piece. Counted in pieces, which a host's deschedules don't take away.
#[test]
fn a_long_computation_yields_through_consume_budget() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let seen = shared(Seen::default());
    let seen_in = seen;
    rt.block_on(async move {
        let done = shared(AtomicBool::new(false));
        spawn(compete(seen_in, done)).unwrap();
        hyper_rt::futures::yield_now().await;
        for _ in 0..ITEMS {
            work(ITEM_NS);
            seen_in.item_done();
            consume_budget().await;
        }
        done.store(true, Ordering::Release);
    })
    .unwrap();
    let pieces = seen.items.load(Ordering::Relaxed);
    let runs = seen.runs.load(Ordering::Relaxed);
    let most_past = seen.most_items_past();
    eprintln!(
        "computation: {pieces} pieces; competitor {runs} runs, behind {most_past} pieces at most"
    );
    assert_eq!(pieces, ITEMS, "the computation went on to its last piece");
    assert!(runs >= SHARES / 2, "the competitor ran: {runs} runs");
    assert!(
        most_past <= ITEMS_PAST,
        "within a share and a piece of the computation: {most_past} pieces"
    );
}

/// Only the task's own waker is refused. Past its share, with a competitor's timer due, a charge under
/// another waker (an executor nested in the task's poll, or a `now_or_never`) goes on at its first poll,
/// where a refusal would only be polled again at once for as long as the task's poll lasted; the time still
/// counts, and the task yields at its own waker's charges past the poll's first.
#[test]
fn a_charge_under_another_waker_goes_on_and_the_task_yields_at_its_own() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let seen = shared(Seen::default());
    let seen_in = seen;
    let (nested_polls, spent, yielded) = rt
        .block_on(async move {
            let done = shared(AtomicBool::new(false));
            spawn(compete(seen_in, done)).unwrap();
            hyper_rt::futures::yield_now().await;
            // Past the share, the competitor's timer (a quarter share) due.
            work(3 * QUANTUM_NS);
            let mut charge = std::pin::pin!(consume_budget());
            let mut nested_polls = 0u32;
            while charge
                .as_mut()
                .poll(&mut Context::from_waker(std::task::Waker::noop()))
                .is_pending()
            {
                nested_polls += 1;
                assert!(
                    nested_polls < 1_000,
                    "a poll under another waker refused over and over"
                );
            }
            let spent = hyper_rt::coop::spent();
            let polls = hyper_rt::futures::shard_counters().unwrap().polls;
            // The poll's first charge under its own waker goes on (each poll makes one operation); the next
            // yields.
            consume_budget().await;
            consume_budget().await;
            let yielded = hyper_rt::futures::shard_counters().unwrap().polls != polls;
            done.store(true, Ordering::Release);
            (nested_polls, spent, yielded)
        })
        .unwrap();
    assert_eq!(
        nested_polls, 0,
        "ready at its first poll under another waker"
    );
    assert!(spent, "the time counted: the task has used its share");
    assert!(
        yielded,
        "the task yielded at its next charge under its own waker"
    );
}

/// No livelock: each poll makes at least one operation. A task whose every poll does twice its share of
/// its own work before its first leaf, and does it again when that leaf answers `Pending` (as futures'
/// `read_to_end` zeroes its buffer's spare room again at every poll), beside a competitor whose timer is
/// due by then: its first charge of each poll goes on, so each step takes one poll. Refused there, it redid
/// the work at every poll and got no further, spinning its shard (Kiln's proxy test did).
#[test]
fn each_poll_makes_at_least_one_operation() {
    const STEPS: u32 = 8;
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let seen = shared(Seen::default());
    let seen_in = seen;
    let polls = rt
        .block_on(async move {
            let done = shared(AtomicBool::new(false));
            spawn(compete(seen_in, done)).unwrap();
            hyper_rt::futures::yield_now().await;
            let mut polls = 0u32;
            for _ in 0..STEPS {
                std::future::poll_fn(|cx| {
                    polls += 1;
                    assert!(
                        polls <= 4 * STEPS,
                        "a step's work redone poll after poll: {polls} polls"
                    );
                    work(2 * QUANTUM_NS);
                    let mut leaf = consume_budget();
                    Pin::new(&mut leaf).poll(cx)
                })
                .await;
            }
            done.store(true, Ordering::Release);
            polls
        })
        .unwrap();
    // Each step's first poll is its task's first charge of that poll, or the second after a step ended
    // within the poll: at most two polls a step.
    assert!(polls <= 2 * STEPS, "{polls} polls for {STEPS} steps");
}

/// A simulated shard keeps a step a yield (docs/runtime.md §11): its clock does not move within a poll, so
/// a charge never ends one, and a loop of due timers is one poll.
#[test]
fn a_simulated_shard_never_yields_by_budget() {
    let mut sim = hyper_rt::sim::SimRuntime::new(&config(), 7).unwrap();
    let shard = sim.shard_ids()[0];
    let looped = shared(AtomicU64::new(0));
    let looped_in = looped;
    sim.spawn_on(shard, async move {
        for _ in 0..1000 {
            sleep(0).await.unwrap();
            consume_budget().await;
            looped_in.fetch_add(1, Ordering::Relaxed);
        }
    })
    .unwrap();
    sim.run_until_idle();
    assert_eq!(looped.load(Ordering::Relaxed), 1000);
    let counters = sim.counters(shard).unwrap();
    assert_eq!(counters.coop_yields, 0, "{counters:?}");
}

/// What a charge costs (a measurement, run on demand: `cargo test -p hyper-rt --release --test coop_budget
/// -- --ignored --nocapture`): with nothing that could want the shard (no clock read), and with a timer
/// armed far ahead (each charge reads the shard's clock and compares it).
#[test]
#[ignore = "a measurement, run on demand"]
fn what_a_charge_costs() {
    const CHARGES: u64 = 1_000_000;
    let mut rt = LocalRuntime::new(&config()).unwrap();
    for armed in [false, true] {
        let ns = rt
            .block_on(async move {
                if armed {
                    // A timer an hour out: something could want the shard, so each charge reads the clock.
                    spawn(async {
                        let _ = sleep(3_600_000_000_000).await;
                    })
                    .unwrap();
                    hyper_rt::futures::yield_now().await;
                }
                let started = monotonic_ns();
                for _ in 0..CHARGES {
                    consume_budget().await;
                }
                (monotonic_ns() - started) / CHARGES
            })
            .unwrap();
        eprintln!(
            "a charge, {}: {ns} ns",
            if armed {
                "a timer armed (the clock read)"
            } else {
                "nothing armed (no clock read)"
            }
        );
    }
}
