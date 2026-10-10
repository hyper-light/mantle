//! The batched UDP socket and IPv6 (docs/runtime.md §5.1): a batch crosses loopback over IPv4 and
//! IPv6 in as few calls as the platform allows, each datagram handed over with an arrival on the shard's
//! clock that is neither before it was sent nor after it was read; an IPv6 socket is IPv6-only and sends
//! with the don't-fragment bit; the outbox and the batch keep their bounds; on the simulation fabric a
//! datagram is stamped with the simulated clock.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cast_possible_truncation,
    clippy::missing_panics_doc,
    clippy::cognitive_complexity
)]

use std::sync::mpsc::channel;

use hyper_rt::RtError;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::sim::SimRuntime;
use hyper_rt::udp::{
    Arrival, Batched, Io, Ipv4Addr, Ipv6Addr, MAX_BATCH, Sent, SocketAddr, UdpSocket,
};

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
    }
}

/// Shape: datagrams in the batch, at most the batch so one `sendmmsg` may carry them all.
const DATAGRAMS: usize = 32;
/// Shape: each datagram's length, equal so Linux may segment them as one send and coalesce them as one
/// receive (`UDP_SEGMENT`, `UDP_GRO`), which the receive must cut back into datagrams.
const DATAGRAM_BYTES: usize = 100;

/// The shard's clock now.
fn now_ns() -> u64 {
    hyper_rt::registry::with_current(|context| context.now_ns()).unwrap()
}

/// What a received batch looked like.
struct Received {
    order: Vec<usize>,
    arrivals: Vec<Arrival>,
    before_send_ns: u64,
    read_before_ns: u64,
    stats: hyper_rt::udp::IoStats,
    sender_stats: hyper_rt::udp::IoStats,
    sender: SocketAddr,
}

/// Sends [`DATAGRAMS`] datagrams of [`DATAGRAM_BYTES`] from one batched socket to another on `loopback`,
/// and receives them all.
async fn round_trip(loopback: SocketAddr) -> Result<Received, RtError> {
    let io = Io { batch: DATAGRAMS };
    let mut sender = Batched::bind(loopback, io, false)?;
    let mut receiver = Batched::bind(loopback, io, true)?;
    let to = receiver.local_addr()?;
    for index in 0..DATAGRAMS {
        let mut bytes = vec![0u8; DATAGRAM_BYTES];
        bytes[0] = index as u8;
        assert!(sender.queue(to, &bytes), "the outbox holds a batch");
    }
    let before_send_ns = now_ns();
    while sender.send() == Sent::Blocked {
        sender.writable().await?;
    }
    let mut order = Vec::new();
    let mut arrivals = Vec::new();
    while order.len() < DATAGRAMS {
        let taken = receiver.receive(|arrival, bytes| {
            assert_eq!(bytes.len(), DATAGRAM_BYTES, "one datagram, cut back out");
            order.push(usize::from(bytes[0]));
            arrivals.push(arrival);
        })?;
        if taken == 0 {
            receiver.readable().await?;
        }
    }
    Ok(Received {
        order,
        arrivals,
        before_send_ns,
        read_before_ns: now_ns(),
        stats: receiver.stats(),
        sender_stats: sender.stats(),
        sender: sender.local_addr()?,
    })
}

/// Do: a batch over `loopback`. Expect: every datagram once, in order, from the sender; each arrival no
/// earlier than the clock read before the send and no later than the clock read after the receive, and
/// none before the one handed over before it; kernel stamps where the platform has them.
fn a_batch_crosses(loopback: SocketAddr) {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let received = rt.block_on(round_trip(loopback)).unwrap().unwrap();
    assert_eq!(
        received.order,
        (0..DATAGRAMS).collect::<Vec<_>>(),
        "every datagram once, in order"
    );
    let mut latest = 0;
    for arrival in &received.arrivals {
        assert_eq!(arrival.from, received.sender);
        assert!(
            arrival.at_ns >= received.before_send_ns,
            "an arrival is not before its send: {} < {}",
            arrival.at_ns,
            received.before_send_ns
        );
        assert!(
            arrival.at_ns <= received.read_before_ns,
            "nor after its read"
        );
        assert!(arrival.at_ns >= latest, "nor before the one before it");
        latest = arrival.at_ns;
        assert_eq!(arrival.kernel, received.stats.kernel_stamps);
    }
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        assert!(received.stats.kernel_stamps, "the kernel stamps receives");
    }
    assert_eq!(received.stats.received, DATAGRAMS as u64);
    assert!(
        received.stats.receive_calls <= DATAGRAMS as u64,
        "never more calls than datagrams"
    );
    assert_eq!(received.sender_stats.sent, DATAGRAMS as u64);
    if cfg!(target_os = "linux") {
        // The whole batch was queued before either side called: one `sendmmsg` (segmented or not) and
        // one `recvmmsg` (coalesced or not) each carry more than one datagram.
        assert!(
            received.sender_stats.send_calls < DATAGRAMS as u64,
            "sends batched: {:?}",
            received.sender_stats
        );
        assert!(
            received.stats.receive_calls < DATAGRAMS as u64,
            "receives batched: {:?}",
            received.stats
        );
    }
}

#[test]
fn a_batch_crosses_ipv4_loopback() {
    a_batch_crosses(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
}

#[test]
fn a_batch_crosses_ipv6_loopback() {
    a_batch_crosses(SocketAddr::from((Ipv6Addr::LOCALHOST, 0)));
}

/// Do: bind an IPv6 socket to the unspecified address, then an IPv4 one to the same port. Expect: both
/// bind (the IPv6 one is IPv6 only, so a dual-stack service is two sockets), and the IPv6 one sends with
/// the don't-fragment bit (`IPV6_DONTFRAG` read back on macOS, `IPV6_PMTUDISC_PROBE` on Linux).
#[test]
fn an_ipv6_socket_is_ipv6_only_and_sends_with_dont_fragment() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let v6 = UdpSocket::bind(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0))).unwrap();
        let port = v6.local_addr().unwrap().port();
        let v4 = UdpSocket::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)));
        assert!(
            v4.is_ok(),
            "an IPv4 socket binds the IPv6-only socket's port: {v4:?}"
        );
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(v6.dont_fragment().unwrap(), "the IPv6 socket sets DF");
    })
    .unwrap();
}

/// Do: a batch of two, three datagrams queued. Expect: the third refused and counted, nothing sent yet.
#[test]
fn the_outbox_refuses_past_its_bound_and_counts() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let mut socket = Batched::bind(loopback, Io { batch: 2 }, false).unwrap();
        let to = socket.local_addr().unwrap();
        assert!(socket.queue(to, b"one"));
        assert!(socket.queue(to, b"two"));
        assert!(!socket.queue(to, b"three"), "past the bound");
        assert_eq!(socket.stats().outbox_full, 1);
        assert_eq!(socket.stats().sent, 0);
        assert!(socket.pending());
    })
    .unwrap();
}

/// Do: batches of 0 and of one past [`MAX_BATCH`]. Expect: both refused as configuration.
#[test]
fn a_batch_outside_its_bounds_is_refused() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        for batch in [0, MAX_BATCH + 1] {
            assert!(matches!(
                Batched::bind(loopback, Io { batch }, false),
                Err(RtError::BadConfig { .. })
            ));
        }
    })
    .unwrap();
}

/// Do: on the simulation fabric at a set clock, a batch from one socket to another. Expect: each
/// datagram arrives stamped with the simulated clock when it was read, not by a kernel.
#[test]
fn a_simulated_batch_is_stamped_on_the_simulated_clock() {
    /// Shape: the simulated clock's reading, far from zero so a stamp of zero would show.
    const AT_NS: u64 = 7_000_000_000;
    let mut sim = SimRuntime::new(&config(), 1).unwrap();
    sim.advance(AT_NS);
    let id = sim.shard_ids()[0];
    let (done_tx, done_rx) = channel();
    sim.spawn_on(id, async move {
        let outcome = async {
            let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
            let io = Io { batch: 4 };
            let mut sender = Batched::bind(loopback, io, true)?;
            let mut receiver = Batched::bind(loopback, io, true)?;
            let to = receiver.local_addr()?;
            for byte in 0..4u8 {
                sender.queue(to, &[byte]);
            }
            while sender.send() == Sent::Blocked {
                sender.writable().await?;
            }
            let mut arrivals = Vec::new();
            while arrivals.len() < 4 {
                if receiver.receive(|arrival, _| arrivals.push(arrival))? == 0 {
                    receiver.readable().await?;
                }
            }
            Ok::<_, RtError>((arrivals, receiver.stats()))
        }
        .await;
        let _ = done_tx.send(outcome);
    })
    .unwrap();
    sim.run_until_idle();
    let (arrivals, stats) = done_rx.try_recv().unwrap().unwrap();
    assert!(!stats.kernel_stamps, "no kernel on the fabric");
    for arrival in arrivals {
        assert_eq!(
            arrival.at_ns, AT_NS,
            "stamped when read, on the simulated clock"
        );
        assert!(!arrival.kernel);
    }
}

/// Windows reports a datagram sent to a closed port as a reset on the socket's next receive
/// (`WSAECONNRESET`, the `recvfrom` reference). Do: send to a port nothing listens on, then receive.
/// Expect: the report is counted as astray and the receive takes nothing, without an error.
#[cfg(windows)]
#[test]
fn an_astray_report_is_counted_not_an_error() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let closed = UdpSocket::bind(loopback).unwrap().local_addr().unwrap();
        let mut socket = Batched::bind(loopback, Io { batch: 4 }, false).unwrap();
        socket.queue(closed, b"astray");
        assert_eq!(socket.send(), Sent::Drained);
        socket.readable().await.unwrap();
        assert_eq!(socket.receive(|_, _| {}).unwrap(), 0);
        assert!(socket.stats().astray >= 1, "{:?}", socket.stats());
    })
    .unwrap();
}
