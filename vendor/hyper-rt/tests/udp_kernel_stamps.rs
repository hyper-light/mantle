//! A stamped socket's arrival on macOS (docs/runtime.md §5.1): the kernel's receive stamp, its own
//! `mach_absolute_time()` in its own ticks, carried onto the shard's clock by its age, read on this
//! process's `mach_absolute_time`. The two count one counter for a native process and not for one
//! Rosetta 2 translates, which is shown a timebase of 1/1 and its own clock in nanoseconds while the
//! stamp stays in Apple silicon's 24 MHz ticks: read with the process's timebase alone, a stamp's age
//! came out as most of the host's uptime and every arrival fell to the shard clock's zero, so a cell of
//! mantle's translated nodes never judged a heartbeat and never elected (mantle's macos-x86_64 CI).
//! Run translated with `cargo test --target x86_64-apple-darwin` on Apple silicon.

#![cfg(target_os = "macos")]
// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::time::{Duration, Instant};

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::udp::{Arrival, Batched, Io, Ipv4Addr, Ring, Sent, SocketAddr};

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
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// Shape: how long a datagram waits queued before it is read, on the clock the stamps' age is read on: a
/// millisecond, so an age read at 3/125 of its length (the kernel's ticks taken for a translated process's
/// nanoseconds) falls short of it.
const HELD: Duration = Duration::from_millis(1);

/// The datagram each socket sends itself.
const BODY: &[u8] = b"stamp";

/// The shard's clock now.
fn now_ns() -> u64 {
    hyper_rt::registry::with_current(|context| context.now_ns()).unwrap()
}

/// The shard's clock once it reads past `after`.
fn past(after: u64) -> u64 {
    loop {
        let now = now_ns();
        if now > after {
            return now;
        }
        std::hint::spin_loop();
    }
}

/// What a datagram a socket sent itself showed: the shard's clock before the send, once it was queued,
/// and after the receive; its arrival; and how long it waited queued before the read began, on the clock
/// the stamps' age is read on (`Instant`, `CLOCK_UPTIME_RAW`, this process's `mach_absolute_time`).
struct Seen {
    before_ns: u64,
    queued_ns: u64,
    arrival: Arrival,
    after_ns: u64,
    held: Duration,
}

/// Sends `socket` a datagram from itself, waits until it is queued (so stamped: the kernel stamps a
/// datagram as UDP input appends it to the socket, `ip_savecontrol`), holds it [`HELD`], and receives it,
/// through `ring` for a socket lent its buffers.
async fn to_itself(socket: &mut Batched, ring: Option<&mut Ring>) -> Seen {
    let to = socket.local_addr().unwrap();
    // Past the shard clock's zero, where an arrival carried from an age read too long would land.
    let before_ns = past(0);
    assert!(socket.queue(to, BODY), "the outbox takes one datagram");
    while socket.send() == Sent::Blocked {
        socket.writable().await.unwrap();
    }
    while socket.socket().queued_bytes().unwrap() == 0 {
        socket.readable().await.unwrap();
    }
    let queued = Instant::now();
    let queued_ns = now_ns();
    while queued.elapsed() < HELD {
        std::hint::spin_loop();
    }
    let held = queued.elapsed();
    // The shard's clock moved past `queued_ns` too, so a stamp taken at the read shows as after it.
    past(queued_ns);
    let mut arrival = None;
    let mut take = |got: Arrival, bytes: &mut [u8]| {
        assert_eq!(bytes, BODY);
        assert!(arrival.replace(got).is_none(), "one datagram");
    };
    let taken = match ring {
        Some(ring) => socket.receive_with(ring, &mut take).unwrap(),
        None => socket.receive(&mut take).unwrap(),
    };
    assert_eq!(taken, 1, "the datagram was queued");
    Seen {
        before_ns,
        queued_ns,
        arrival: arrival.unwrap(),
        after_ns: now_ns(),
        held,
    }
}

/// The arrival is the kernel's stamp on the shard's clock: no earlier than the send and no later than the
/// receive by the shard's clock, and before the read began, by at least the time the datagram was held.
/// The age is read on the stamps' clock, so a stamp carried late by a preemption between that reading and
/// the shard's moves the arrival and not its age; a nanosecond each way is the rounding of the readings
/// to whole nanoseconds.
fn is_the_kernels_stamp(seen: &Seen) {
    let Seen {
        before_ns,
        queued_ns,
        arrival,
        after_ns,
        held,
    } = *seen;
    assert!(arrival.kernel, "the kernel stamped the datagram");
    assert!(
        before_ns <= arrival.at_ns && arrival.at_ns <= after_ns,
        "arrived at {} ns, outside the send at {before_ns} ns to the receive at {after_ns} ns \
         (queued by {queued_ns} ns)",
        arrival.at_ns
    );
    assert!(
        queued_ns < arrival.read_ns && arrival.read_ns <= after_ns,
        "read at {} ns, outside the hold's end after {queued_ns} ns to {after_ns} ns",
        arrival.read_ns
    );
    let age = arrival.read_ns - arrival.at_ns;
    assert!(
        u128::from(age) + 2 >= held.as_nanos(),
        "an age of {age} ns at the read, for a datagram held {held:?} before it"
    );
}

/// Do: on a shard, a stamped socket that holds its own receive buffer (`Batched::bind`, the plane socket's)
/// sends itself a datagram and reads it once it has waited queued. Expect: the arrival is the kernel's
/// stamp on the shard's clock, between the send and the receive and at least the hold before the read.
#[test]
fn a_stamped_sockets_arrival_is_the_kernels_stamp_on_the_shards_clock() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let seen = rt
        .block_on(async {
            let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
            let mut socket = Batched::bind(loopback, Io { batch: 1 }, true).unwrap();
            assert!(socket.stats().kernel_stamps, "the kernel stamps receives");
            to_itself(&mut socket, None).await
        })
        .unwrap();
    is_the_kernels_stamp(&seen);
}

/// Do: the same on a socket lent its receive buffers (`Batched::bind_lent` and a `Ring`, the transport
/// driver's sockets). Expect: the same.
#[test]
fn a_lent_sockets_arrival_is_the_kernels_stamp_on_the_shards_clock() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let seen = rt
        .block_on(async {
            let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
            let mut socket = Batched::bind_lent(loopback, true).unwrap();
            let mut ring = Ring::new(Io { batch: 1 }).unwrap();
            assert!(socket.stats().kernel_stamps, "the kernel stamps receives");
            to_itself(&mut socket, Some(&mut ring)).await
        })
        .unwrap();
    is_the_kernels_stamp(&seen);
}
