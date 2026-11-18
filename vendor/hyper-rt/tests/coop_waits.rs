//! A leaf is charged to its task's turn where it goes on, never where it waits (Kiln's `kiln-rt`
//! `tests/coop_waits.rs` and `tests/readiness_waits.rs`' charge case, its goal 6, plan 01a1a59c2abe step 1;
//! `hyper_rt::coop`, docs/runtime.md §3.14). Each looks first, taking nothing: one that waits registers with
//! no charge, one that goes on is charged and then takes, so a refusal leaves what it would have taken.
//! Charged as it looked, a leaf that waited took its poll's first charge, which always goes on, so the leaf
//! after it was refused at every poll once the poll's own work outlasted the share, and its task was polled
//! again and again for nothing: the livelock Kiln's front door spun a shard with for hours. So a task that
//! awaits real readiness is charged nothing extra for its waits. The cases here are the leaves that charge:
//! timers, sockets and their readiness waits, datagrams, the sync primitives, joins and signals (Kiln's
//! notification, one-shot, channel, watch, permit, join and signal cases, with a granted permit, a delivered
//! signal and datagrams added).
// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use hyper_rt::coop::consume_budget;
use hyper_rt::futures::{join, sleep, spawn, yield_now};
use hyper_rt::machine::clock::monotonic_ns;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::sync::{Notify, Semaphore, channel, oneshot, watch};
use hyper_rt::tcp::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use hyper_rt::udp::UdpSocket;

/// Shape: the share under test, a fixed step budget (no wake tracking), as `coop_budget.rs` has it.
const QUANTUM_NS: u64 = 2_000_000;
/// Shape: the steps a test takes beside a leaf that waits, each in a poll of its own.
const STEPS: u32 = 8;
/// Shape: the polls past which a task counts as spun, four times its steps.
const SPUN: u32 = 4 * STEPS;
/// Shape: a listener's backlog, room for the connections a test makes.
const BACKLOG: u32 = 4;
/// Shape: a sleep far past any test's end.
const LONG_NS: u64 = 3_600_000_000_000;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: QUANTUM_NS,
        // Shape: a twentieth of the share, so a timer falls due within one.
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

/// Spins for `ns` on the CPU: a poll's own work, past its share.
fn work(ns: u64) {
    let until = monotonic_ns() + ns;
    while monotonic_ns() < until {
        std::hint::spin_loop();
    }
}

/// A connected pair on loopback, and its listener: the client's end and the server's.
async fn pair() -> (TcpListener, TcpStream, TcpStream) {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), BACKLOG).unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let server = listener.accept().await.unwrap();
    (listener, client, server)
}

/// Keeps the shard wanted, until `done`: a timer of its own falls due within every share.
fn compete() -> &'static AtomicBool {
    // Leaked: a word a test, for the test's life.
    let done: &'static AtomicBool = Box::leak(Box::new(AtomicBool::new(false)));
    let until = done;
    spawn(async move {
        while !until.load(Ordering::Acquire) && sleep(QUANTUM_NS / 4).await.is_ok() {}
    })
    .unwrap();
    done
}

/// Steps beside `wait`, a leaf that waits, on a wanted shard: each step works twice its share, polls
/// `wait`, then charges once (`consume_budget`), in a poll of its own. The polls the steps took, and
/// whether `wait` waited all along.
async fn steps_beside<W: Future + Unpin>(wait: &mut W) -> (u32, bool) {
    let done = compete();
    yield_now().await;
    let (mut polls, mut waiting) = (0u32, true);
    for _ in 0..STEPS {
        let mut step = consume_budget();
        poll_fn(|cx| {
            polls += 1;
            if polls > SPUN {
                return Poll::Ready(());
            }
            work(2 * QUANTUM_NS);
            waiting &= Pin::new(&mut *wait).poll(cx).is_pending();
            Pin::new(&mut step).poll(cx)
        })
        .await;
        yield_now().await;
    }
    done.store(true, Ordering::Release);
    (polls, waiting)
}

/// `ready`, a leaf whose operation is ready, polled on a wanted shard after its poll's own work outlasted
/// the share and its first charge went on elsewhere, then in the next poll: whether it was refused there,
/// and went on here.
async fn twice<R: Future + Unpin>(ready: &mut R) -> (bool, bool) {
    let done = compete();
    yield_now().await;
    let mut first = consume_budget();
    let refused = poll_fn(|cx| {
        work(2 * QUANTUM_NS);
        assert!(
            Pin::new(&mut first).poll(cx).is_ready(),
            "a poll's first charge goes on"
        );
        Poll::Ready(Pin::new(&mut *ready).poll(cx).is_pending())
    })
    .await;
    yield_now().await;
    let went_on = poll_fn(|cx| Poll::Ready(Pin::new(&mut *ready).poll(cx).is_ready())).await;
    done.store(true, Ordering::Release);
    (refused, went_on)
}

/// A leaf that waits, built and stepped beside by `steps`, is no charge: each step one poll, the leaf
/// waiting.
fn waits_uncharged<F: Future<Output = (u32, bool)> + 'static>(
    what: &str,
    steps: impl FnOnce() -> F,
) {
    let (polls, waiting) = LocalRuntime::new(&config())
        .unwrap()
        .block_on(steps())
        .unwrap();
    assert!(waiting, "{what}: nothing came, yet it went on");
    assert_eq!(
        polls, STEPS,
        "{what}: the waiting leaf took each poll's first charge, its steps redone poll after poll"
    );
}

/// A leaf that goes on, built and polled by `polls`, is charged: refused past its share, its operation left
/// for the next poll.
fn goes_on_charged<F: Future<Output = (bool, bool)> + 'static>(
    what: &str,
    polls: impl FnOnce() -> F,
) {
    let (refused, went_on) = LocalRuntime::new(&config())
        .unwrap()
        .block_on(polls())
        .unwrap();
    assert!(
        refused,
        "{what}: going on past its share beside other work, it wasn't charged"
    );
    assert!(went_on, "{what}: refused, it lost what it would have taken");
}

/// Do: a sleep that ends long after the test, then one due at once. Expect: the wait no charge (each step
/// one poll); the one due charged, refused past its share, then ended.
#[test]
fn a_sleep_is_charged_as_it_goes_on_never_as_it_waits() {
    waits_uncharged("a sleep", || async {
        let mut wait = sleep(LONG_NS);
        steps_beside(&mut wait).await
    });
    goes_on_charged("a sleep", || async {
        let mut ready = sleep(0);
        twice(&mut ready).await
    });
}

/// Do: a wait for a socket's readability that nothing is sent to, beside each step's charge (Kiln's
/// `readiness_waits.rs` case, on a stream). Expect: the wait no charge, each step one poll, the wait still
/// waiting: a task that awaits real readiness is charged nothing for its wait.
#[test]
fn a_readiness_wait_that_has_not_fired_is_no_charge() {
    waits_uncharged("a readiness wait", || async {
        let (_listener, _client, server) = pair().await;
        let mut wait = server.readable();
        steps_beside(&mut wait).await
    });
}

/// Do: a read that waits on a socket nothing is sent to, beside each step's charge. Expect: its try is
/// charged once, in its first poll, where it takes the step's charge's place, so that step takes a second
/// poll; its wait is no charge after, each later step one poll, and the read still waiting.
#[test]
fn a_read_that_waits_is_charged_for_its_try_alone() {
    let (polls, waiting) = LocalRuntime::new(&config())
        .unwrap()
        .block_on(async {
            let (_listener, _client, server) = pair().await;
            let mut buf = [0u8; 8];
            let mut wait = pin!(server.read(&mut buf));
            steps_beside(&mut wait).await
        })
        .unwrap();
    assert!(waiting, "nothing was sent, yet the read went on");
    assert_eq!(
        polls,
        STEPS + 1,
        "the read's try took one poll's first charge; its wait took none"
    );
}

/// Do: a socket's read of bytes already there, then a write with room, each polled past the share. Expect:
/// each charged, refused, then gone on with nothing lost.
#[test]
fn a_socket_operation_that_goes_on_is_charged_and_loses_nothing() {
    goes_on_charged("a read", || async {
        let (_listener, client, server) = pair().await;
        client.write_all(b"ready").await.unwrap();
        // The bytes are there before the test polls the read: a wait for them ends first.
        server.readable().await.unwrap();
        let mut buf = [0u8; 8];
        let mut ready = pin!(server.read(&mut buf));
        twice(&mut ready).await
    });
    goes_on_charged("a write", || async {
        let (_listener, client, _server) = pair().await;
        let mut ready = pin!(client.write(b"room"));
        twice(&mut ready).await
    });
}

/// Two datagram sockets on loopback: a sender's and a receiver's.
fn datagram_pair() -> (UdpSocket, UdpSocket) {
    let bind = || UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    (bind(), bind())
}

/// Do: a datagram receive that waits on a socket nothing is sent to, beside each step's charge. Expect: as
/// for a stream's read, its try charged once, in its first poll, so that step takes a second poll; its wait
/// no charge after, and the receive still waiting.
#[test]
fn a_datagram_receive_that_waits_is_charged_for_its_try_alone() {
    let (polls, waiting) = LocalRuntime::new(&config())
        .unwrap()
        .block_on(async {
            let (_sender, receiver) = datagram_pair();
            let mut buf = [0u8; 8];
            let mut wait = pin!(receiver.recv_from(&mut buf));
            steps_beside(&mut wait).await
        })
        .unwrap();
    assert!(waiting, "nothing was sent, yet the receive went on");
    assert_eq!(
        polls,
        STEPS + 1,
        "the receive's try took one poll's first charge; its wait took none"
    );
}

/// Do: a receive of a datagram already there, then a send with room, each polled past the share. Expect:
/// each charged, refused, then gone on with nothing lost.
#[test]
fn a_datagram_operation_that_goes_on_is_charged_and_loses_nothing() {
    goes_on_charged("a datagram's receive", || async {
        let (sender, receiver) = datagram_pair();
        let to = receiver.local_addr().unwrap();
        sender.send_to_writable(b"ready", to).await.unwrap();
        // The datagram is there before the test polls the receive: a wait for it ends first.
        receiver.readable().await.unwrap();
        let mut buf = [0u8; 8];
        let mut ready = pin!(receiver.recv_from(&mut buf));
        twice(&mut ready).await
    });
    goes_on_charged("a datagram's send", || async {
        let (sender, receiver) = datagram_pair();
        let to = receiver.local_addr().unwrap();
        let mut ready = pin!(sender.send_to_writable(b"room", to));
        twice(&mut ready).await
    });
}

/// Do: a notification waited for beside a step's charge, then one that came, polled past the share. Expect:
/// the wait no charge (each step one poll); the one that came charged, refused, then taken.
#[test]
fn a_notification_is_charged_as_it_goes_on_never_as_it_waits() {
    waits_uncharged("a notification", || async {
        let notify = Notify::new().unwrap();
        let mut wait = notify.notified();
        steps_beside(&mut wait).await
    });
    goes_on_charged("a notification", || async {
        let notify = Notify::new().unwrap();
        notify.notify_one();
        let mut ready = notify.notified();
        twice(&mut ready).await
    });
}

/// Do: a one-shot waited for, then one sent. Expect: as for a notification.
#[test]
fn a_one_shot_is_charged_as_it_goes_on_never_as_it_waits() {
    waits_uncharged("a one-shot", || async {
        let (_sender, mut wait) = oneshot::<u32>().unwrap();
        steps_beside(&mut wait).await
    });
    goes_on_charged("a one-shot", || async {
        let (sender, mut ready) = oneshot::<u32>().unwrap();
        sender.send(7).unwrap();
        twice(&mut ready).await
    });
}

/// Do: a channel's receive waited for, then one with a value to take. Expect: as for a notification.
#[test]
fn a_channel_receive_is_charged_as_it_goes_on_never_as_it_waits() {
    waits_uncharged("a channel's receive", || async {
        let (_sender, mut receiver) = channel::<u32>(1).unwrap();
        let mut wait = receiver.recv();
        steps_beside(&mut wait).await
    });
    goes_on_charged("a channel's receive", || async {
        let (sender, mut receiver) = channel::<u32>(1).unwrap();
        sender.try_send(7).unwrap();
        let mut ready = receiver.recv();
        twice(&mut ready).await
    });
}

/// Do: a channel's send waiting for room (its receiver takes nothing), then one with room. Expect: as for a
/// notification.
#[test]
fn a_channel_send_is_charged_as_it_goes_on_never_as_it_waits() {
    waits_uncharged("a channel's send", || async {
        let (sender, _receiver) = channel::<u32>(1).unwrap();
        sender.try_send(0).unwrap();
        let mut wait = sender.send(1);
        steps_beside(&mut wait).await
    });
    goes_on_charged("a channel's send", || async {
        let (sender, _receiver) = channel::<u32>(1).unwrap();
        let mut ready = sender.send(7);
        twice(&mut ready).await
    });
}

/// Do: a watched word's change waited for, then one changed. Expect: as for a notification.
#[test]
fn a_watched_change_is_charged_as_it_goes_on_never_as_it_waits() {
    waits_uncharged("a watched word", || async {
        let (_sender, mut receiver) = watch(0, 1).unwrap();
        let mut wait = receiver.changed();
        steps_beside(&mut wait).await
    });
    goes_on_charged("a watched word", || async {
        let (mut sender, mut receiver) = watch(0, 1).unwrap();
        sender.send(7);
        let mut ready = receiver.changed();
        twice(&mut ready).await
    });
}

/// Do: a semaphore's permit waited for (none free), then one free, then one granted to a wait already
/// queued. Expect: as for a notification, the grant charged as the free permit is.
#[test]
fn a_permit_is_charged_as_it_goes_on_never_as_it_waits() {
    waits_uncharged("a permit", || async {
        let semaphore = Semaphore::new(0, 1).unwrap();
        let mut wait = semaphore.acquire();
        steps_beside(&mut wait).await
    });
    goes_on_charged("a permit", || async {
        let semaphore = Semaphore::new(1, 1).unwrap();
        let mut ready = semaphore.acquire();
        twice(&mut ready).await
    });
    goes_on_charged("a granted permit", || async {
        let semaphore = Semaphore::new(1, 1).unwrap();
        let held = semaphore.try_acquire().unwrap();
        let mut ready = semaphore.acquire();
        let queued = poll_fn(|cx| Poll::Ready(Pin::new(&mut ready).poll(cx).is_pending())).await;
        assert!(queued, "the one permit held, the wait queues");
        // Its release grants the permit to the queued wait.
        drop(held);
        twice(&mut ready).await
    });
}

/// Do: a join of a task that never ends, then one of a task that ended. Expect: as for a notification.
#[test]
fn a_join_is_charged_as_it_goes_on_never_as_it_waits() {
    waits_uncharged("a join", || async {
        let id = spawn(std::future::pending::<()>()).unwrap();
        let mut wait = join(id);
        steps_beside(&mut wait).await
    });
    goes_on_charged("a join", || async {
        let id = spawn(async {}).unwrap();
        // Its one poll, in which it ends.
        yield_now().await;
        let mut ready = join(id);
        twice(&mut ready).await
    });
}

/// Do: a signal waited for (`Hangup`, never sent), then one delivered. Expect: the wait no charge, each step
/// one poll; the delivery charged, refused, then taken.
#[cfg(unix)]
#[test]
fn a_signal_is_charged_as_it_goes_on_never_as_it_waits() {
    use hyper_rt::signal::{Signal, subscribe};
    waits_uncharged("a signal", || async {
        let stream = subscribe(&[Signal::Hangup]).unwrap();
        let mut wait = stream.recv();
        steps_beside(&mut wait).await
    });
    goes_on_charged("a signal", || async {
        let stream = subscribe(&[Signal::Hangup]).unwrap();
        let mut ready = stream.recv();
        // Its wait registers first, so the delivery wakes this task, which nothing else wakes: the test
        // waits on the delivery itself.
        let waiting = poll_fn(|cx| Poll::Ready(Pin::new(&mut ready).poll(cx).is_pending())).await;
        assert!(waiting, "no signal was sent yet");
        rustix::process::kill_process(rustix::process::getpid(), rustix::process::Signal::HUP)
            .unwrap();
        let mut woken = false;
        poll_fn(|_| {
            if std::mem::replace(&mut woken, true) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        twice(&mut ready).await
    });
}
