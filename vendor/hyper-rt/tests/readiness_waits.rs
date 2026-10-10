//! Readiness waits inside one task's combinators (mantle's final review of hyper-rt, findings 1 and 2):
//! each wait is its own, and only a fire makes it ready.
//!
//! - Finding 1: two waits of one task on one handle and direction shared the table's node (keyed by the
//!   task's word); a race's losing wait, dropped, withdrew it, and the join's wait on the same socket
//!   never woke.
//!   (The join test below fails on the old code through finding 2 first: the race's own wait reported
//!   ready. Finding 1 alone is pinned by `interests.rs`'s table test of two waits of one word.)
//! - Finding 2: a wait re-polled for any reason reported ready, so a timer that woke a biased race made
//!   the idle socket's wait win, and that wait's node was never withdrawn.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use hyper_rt::combine::{Either, join2, race2};
use hyper_rt::futures::{sleep, yield_now};
use hyper_rt::registry;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::udp::{Ipv4Addr, SocketAddr, UdpSocket};

/// Shape: the waits the shard holds at once; the leak test runs three times this many races.
const WAITS: usize = 16;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: WAITS,
        ring_entries: 64,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

/// Shape: a sleep that loses to nothing here: no datagram is sent until it has ended.
const TICK_NS: u64 = 1_000_000;
/// Shape: how long a wait that should have woken is given before the test fails rather than hangs.
const PATIENCE_NS: u64 = 10_000_000_000;

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

fn waits_held() -> usize {
    registry::with_current(|ctx| ctx.waits_held()).unwrap()
}

/// Do: one task joins a read wait on a socket with a race of a second read wait on the same socket and a
/// sleep; the sleep wins the race, then a datagram is sent to the socket. Expect: the join's read wait
/// wakes, and no wait is held after.
#[test]
fn a_race_lost_on_a_handle_leaves_the_joins_wait_on_it() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let ours = UdpSocket::bind(loopback()).unwrap();
        let peer = UdpSocket::bind(loopback()).unwrap();
        let to = ours.local_addr().unwrap();
        let joined = join2(ours.readable(), async {
            let raced = race2(ours.readable(), sleep(TICK_NS)).await;
            assert!(
                matches!(raced, Either::Second(Ok(()))),
                "nothing was sent: the sleep wins the race, got {raced:?}"
            );
            peer.send_to(b"x", to).unwrap();
        });
        match race2(joined, sleep(PATIENCE_NS)).await {
            Either::First((read, ())) => read.unwrap(),
            Either::Second(_) => {
                panic!("the join's read wait never woke: the race's loser took it")
            }
        }
        yield_now().await;
        assert_eq!(waits_held(), 0, "every wait gave its slot back");
    })
    .unwrap();
}

/// Do: race a read wait on an idle socket against a sleep, three times as often as the shard has wait
/// slots. Expect: the sleep wins every race (the timer's wake does not make the unfired wait ready), and
/// every losing wait gives its slot back, so the bound is never reached.
#[test]
fn a_wait_woken_by_a_timer_stays_unready_and_leaves_when_it_loses() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let idle = UdpSocket::bind(loopback()).unwrap();
        for round in 0..WAITS * 3 {
            let raced = race2(idle.readable(), sleep(TICK_NS)).await;
            assert!(
                matches!(raced, Either::Second(Ok(()))),
                "round {round}: the idle socket's wait won ({raced:?})"
            );
            // The loser's withdrawal is applied by the loop after this poll: one yield, then its slot is back.
            yield_now().await;
            assert_eq!(waits_held(), 0, "round {round}: the loser's slot came back");
        }
    })
    .unwrap();
}

/// Mantle's final review, second pass, finding 4. Do: on shard A, arm a read wait and move the armed
/// `Ready` to shard B, where a task waits on its own socket (its wait in the same slot and generation of
/// B's table as the moved one's in A's), and drop it there; then send B's socket a datagram. Expect: B's
/// wait wakes (a ticket of A's names nothing on B), and A's slot comes back (the drop is carried to A).
#[test]
fn a_wait_dropped_on_another_shard_ends_on_its_own() {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::mpsc::channel;
    use std::task::Poll;

    use hyper_rt::runtime::Runtime;
    use hyper_rt::sync::oneshot;

    let rt = Runtime::start(&RuntimeConfig {
        shards: 2,
        ..config()
    })
    .unwrap();
    let (a, b) = (rt.shard_ids()[0], rt.shard_ids()[1]);
    let (moved, arrives) = oneshot().unwrap();
    let (finished, done) = oneshot::<()>().unwrap();
    let (report, reports) = channel();
    let from_a = report.clone();
    rt.spawn_on(a, async move {
        let ours = UdpSocket::bind(loopback()).unwrap();
        let mut armed = ours.readable();
        std::future::poll_fn(|cx| {
            let _ = Pin::new(&mut armed).poll(cx);
            Poll::Ready(())
        })
        .await;
        assert_eq!(waits_held(), 1, "armed on A");
        let _ = moved.send(armed);
        let _ = done.await;
        yield_now().await;
        let _ = from_a.send(("A's slots held", waits_held()));
    })
    .unwrap();
    rt.spawn_on(b, async move {
        let ours = UdpSocket::bind(loopback()).unwrap();
        let peer = UdpSocket::bind(loopback()).unwrap();
        let to = ours.local_addr().unwrap();
        let joined = join2(ours.readable(), async {
            let foreign = arrives.await.unwrap();
            drop(foreign);
            yield_now().await;
            peer.send_to(b"x", to).unwrap();
        });
        let woke = matches!(
            race2(joined, sleep(PATIENCE_NS)).await,
            Either::First((Ok(()), ()))
        );
        let _ = report.send(("B's wait woke", usize::from(woke)));
        let _ = finished.send(());
    })
    .unwrap();
    let mut seen = std::collections::BTreeMap::new();
    for _ in 0..2 {
        let (what, value) = reports.recv().unwrap();
        seen.insert(what, value);
    }
    rt.shutdown().unwrap();
    assert_eq!(
        seen.get("B's wait woke"),
        Some(&1),
        "B's live wait was taken for A's"
    );
    assert_eq!(seen.get("A's slots held"), Some(&0), "A's slot came back");
    assert_eq!(hyper_rt::readiness::abandons_lost(), 0);
}

/// Mantle's final review, third pass, A. Do: shard A arms a wait, moves the armed `Ready` to shard B and
/// holds itself inside one poll; A's control channel is filled behind the hold; B drops the moved `Ready`
/// while the channel is full; A is let go. Expect: A's slot comes back and no end of a wait was lost.
/// The drop used to travel as a control message: refused by the full channel, it left the slot held for
/// the shard's life.
#[test]
fn a_wait_dropped_elsewhere_ends_on_its_shard_with_its_control_channel_full() {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::channel;
    use std::task::Poll;

    use hyper_rt::RtError;
    use hyper_rt::runtime::Runtime;
    use hyper_rt::sync::oneshot;

    /// Whether shard A may leave its hold.
    static RELEASE: AtomicBool = AtomicBool::new(false);
    /// Shape: yields the held task gives its shard to sweep and withdraw before it reports.
    const SETTLE_YIELDS: usize = 64;

    let lost_before = hyper_rt::readiness::abandons_lost();
    let rt = Runtime::start(&RuntimeConfig {
        shards: 2,
        ..config()
    })
    .unwrap();
    let (a, b) = (rt.shard_ids()[0], rt.shard_ids()[1]);
    let (moved, arrives) = oneshot().unwrap();
    let (go, dropping) = oneshot::<()>().unwrap();
    let (report, reports) = channel();
    let (holding, held) = channel();
    let (dropped, after_drop) = channel();
    rt.spawn_on(a, async move {
        let ours = UdpSocket::bind(loopback()).unwrap();
        let mut armed = ours.readable();
        std::future::poll_fn(|cx| {
            let _ = Pin::new(&mut armed).poll(cx);
            Poll::Ready(())
        })
        .await;
        let _ = moved.send(armed);
        let _ = holding.send(());
        // The hold: this poll does not return until released, so A drains nothing meanwhile.
        while !RELEASE.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        let mut left = waits_held();
        for _ in 0..SETTLE_YIELDS {
            if left == 0 {
                break;
            }
            yield_now().await;
            left = waits_held();
        }
        let _ = report.send(left);
        drop(ours);
    })
    .unwrap();
    rt.spawn_on(b, async move {
        let foreign = arrives.await.unwrap();
        let _ = dropping.await;
        drop(foreign);
        let _ = dropped.send(());
    })
    .unwrap();
    held.recv().unwrap();
    let mut queued = 0;
    loop {
        match rt.spawn_on(a, async {}) {
            Ok(()) => queued += 1,
            Err(RtError::ControlFull { .. }) => break,
            Err(e) => panic!("{e}"),
        }
    }
    assert!(queued >= 1, "A's control channel filled behind the hold");
    let _ = go.send(());
    after_drop.recv().unwrap();
    RELEASE.store(true, Ordering::Release);
    let left = reports.recv().unwrap();
    rt.shutdown().unwrap();
    assert_eq!(
        left, 0,
        "A's slot came back although its control channel was full"
    );
    assert_eq!(
        hyper_rt::readiness::abandons_lost(),
        lost_before,
        "no end of a wait was lost"
    );
}

/// Mantle's final review, fourth pass, E. Do: on a shard with thousands of wait slots, arm one wait,
/// drop it on another shard, and let the owner sweep. Expect: the owner's sweeps visited exactly one mark
/// (its summary bitmap names the marked slot), and the slot came back. The sweep used to swap every
/// slot's mark for one drop.
#[test]
fn a_sweep_visits_only_the_marked_slots() {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::mpsc::channel;
    use std::task::Poll;

    use hyper_rt::runtime::Runtime;
    use hyper_rt::sync::oneshot;

    /// Shape: wait slots a shard holds, so a pass over all of them would be unmistakable.
    const MANY: usize = 4096;
    /// Shape: yields the owner gives its loop to sweep and withdraw before it reports.
    const SETTLE_YIELDS: usize = 64;

    let rt = Runtime::start(&RuntimeConfig {
        shards: 2,
        interests_per_shard: MANY,
        ..config()
    })
    .unwrap();
    let (a, b) = (rt.shard_ids()[0], rt.shard_ids()[1]);
    let (moved, arrives) = oneshot().unwrap();
    let (finished, done) = oneshot::<()>().unwrap();
    let (report, reports) = channel();
    rt.spawn_on(a, async move {
        let ours = UdpSocket::bind(loopback()).unwrap();
        let mut armed = ours.readable();
        std::future::poll_fn(|cx| {
            let _ = Pin::new(&mut armed).poll(cx);
            Poll::Ready(())
        })
        .await;
        let _ = moved.send(armed);
        let _ = done.await;
        let mut left = waits_held();
        for _ in 0..SETTLE_YIELDS {
            if left == 0 {
                break;
            }
            yield_now().await;
            left = waits_held();
        }
        let _ = report.send(left);
        drop(ours);
    })
    .unwrap();
    rt.spawn_on(b, async move {
        drop(arrives.await.unwrap());
        let _ = finished.send(());
    })
    .unwrap();
    let left = reports.recv().unwrap();
    let counters = rt.shutdown().unwrap();
    assert_eq!(left, 0, "the dropped wait's slot came back");
    assert_eq!(
        counters[0].abandons_swept, 1,
        "one drop, one mark visited, of {MANY} slots"
    );
}
