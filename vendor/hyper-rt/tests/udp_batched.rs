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
    Arrival, Batched, Io, IoStats, Ipv4Addr, Ipv6Addr, MAX_BATCH, Queued, RECEIVE_BYTES, Ring,
    Sent, SocketAddr, UdpSocket,
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
        latency_objective_ns: None,
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

/// Do: queue the largest datagram a slot takes, one past it, and fill a slot in place past it. Expect: the
/// first queued, the others dropped and counted as oversize, and the socket holding no more than its `Io`
/// says it may (`Io::held_bound`), which is what an owner charges for it.
#[test]
fn a_socket_holds_no_more_than_its_bound() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let io = Io { batch: 4 };
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let mut socket = Batched::bind(loopback, io, false).unwrap();
        let to = socket.local_addr().unwrap();
        assert!(socket.held_bytes() <= io.held_bound());
        assert!(socket.queue(to, &vec![1u8; RECEIVE_BYTES]));
        assert!(!socket.queue(to, &vec![2u8; RECEIVE_BYTES + 1]));
        let slot = socket.slot().unwrap();
        slot.bytes.resize(RECEIVE_BYTES * 2, 3);
        slot.to = to;
        socket.commit();
        assert_eq!(socket.stats().oversize, 2);
        assert!(
            socket.held_bytes() <= io.held_bound(),
            "{} held past a bound of {}",
            socket.held_bytes(),
            io.held_bound()
        );
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
            assert!(matches!(
                Ring::new(Io { batch }),
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

/// A datagram the kernel has no route for fails its send the same way every time: macOS reports an
/// IPv6 destination on an IPv4 socket as unreachable (`EHOSTUNREACH`), which is also how a datagram
/// sent earlier is reported astray. Do: queue one to such a destination, then one to a live socket,
/// and send. Expect: the first is refused alone — its report taken once as an earlier datagram's, then
/// as its own — and the queue behind it is sent: one unroutable peer never holds a socket's other
/// datagrams (a focal connector that dialed a name's `::1` stopped sending to every peer).
#[test]
fn a_datagram_with_no_route_is_refused_alone_and_the_queue_behind_it_is_sent() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let receiver = UdpSocket::bind(loopback).unwrap();
        let to = receiver.local_addr().unwrap();
        let mut socket = Batched::bind(loopback, Io { batch: 4 }, false).unwrap();
        socket.queue(
            SocketAddr::from((Ipv6Addr::LOCALHOST, to.port())),
            b"nowhere",
        );
        socket.queue(to, b"behind");
        assert_eq!(socket.send(), Sent::Drained, "{:?}", socket.stats());
        let mut buffer = [0u8; 16];
        let (length, _) = receiver.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..length], b"behind");
        assert!(socket.stats().send_errors >= 1, "{:?}", socket.stats());
    })
    .unwrap();
}

/// The single-datagram send waits for the socket's room, never for a route: a datagram the kernel has
/// no route for is refused, its astray report taken once as an earlier datagram's and then as its own.
/// Do: send to an IPv6 destination from an IPv4 socket, waiting for writability. Expect: an error, at
/// once, where the send used to wait for a writability that never sent it.
#[test]
fn a_send_waiting_for_room_is_refused_for_a_destination_with_no_route() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
        let nowhere = SocketAddr::from((Ipv6Addr::LOCALHOST, 9));
        let sent = hyper_rt::futures::within(1_000_000_000, socket.send_to_writable(b"x", nowhere))
            .await
            .unwrap();
        assert!(matches!(sent, Some(Err(_))), "{sent:?}");
    })
    .unwrap();
}

/// `UDP_MAX_SEGMENTS` (`include/linux/udp.h`): the most datagrams one segmented send carries.
const MOST_SEGMENTS: usize = 64;

/// What a segmented round trip looked like.
struct Segmented {
    /// Each datagram's index and length, in the order they arrived.
    arrived: Vec<(usize, usize)>,
    /// What [`Batched::send_segments`] said it had done, call by call.
    done: Vec<usize>,
    sender: IoStats,
    receiver: IoStats,
}

/// `count` datagrams of `segment` bytes, the last `last` bytes, back to back as an endpoint makes a
/// batch, each beginning with its index.
fn datagrams(count: usize, segment: usize, last: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for index in 0..count {
        let length = if index + 1 == count { last } else { segment };
        let start = bytes.len();
        bytes.resize(start + length, 0xA5);
        bytes[start..start + 2].copy_from_slice(&(index as u16).to_be_bytes());
    }
    bytes
}

/// Takes `count` datagrams on `receiver` through `ring`: each one's index and length, in order.
async fn take(
    receiver: &mut Batched,
    ring: &mut Ring,
    count: usize,
) -> Result<Vec<(usize, usize)>, RtError> {
    let mut arrived = Vec::new();
    while arrived.len() < count {
        let taken = receiver.receive_with(ring, |_, datagram| {
            let index = usize::from(u16::from_be_bytes([datagram[0], datagram[1]]));
            arrived.push((index, datagram.len()));
        })?;
        if taken == 0 {
            receiver.readable().await?;
        }
    }
    Ok(arrived)
}

/// Sends [`datagrams`] from one lent socket to another on `loopback` with
/// [`Batched::send_segments`], straight from the buffer, until all are done, and takes them through
/// a ring.
async fn segmented_round_trip(
    loopback: SocketAddr,
    count: usize,
    segment: usize,
    last: usize,
) -> Result<Segmented, RtError> {
    let mut sender = Batched::bind_lent(loopback, false)?;
    let mut receiver = Batched::bind_lent(loopback, false)?;
    let mut ring = Ring::new(Io { batch: 8 })?;
    let to = receiver.local_addr()?;
    let bytes = datagrams(count, segment, last);
    // Every datagram crosses before the receiver reads one, so its buffer holds the whole burst:
    // room for each at the most a datagram may take (`RECEIVE_BYTES`), whatever the platform
    // charges a datagram beside its bytes. A default buffer is the OS's, and one that holds less
    // drops what it cannot hold: the receive below then waits for datagrams that will never come.
    let granted = receiver
        .socket()
        .set_recv_buffer_bytes(count.saturating_mul(RECEIVE_BYTES))?;
    assert!(
        granted >= bytes.len(),
        "a receive buffer of {granted} bytes for a burst of {} bytes",
        bytes.len()
    );
    let mut done = Vec::new();
    let mut sent = 0;
    while sent < count {
        let from = (sent * segment).min(bytes.len());
        let now = sender.send_segments(to, &bytes[from..], segment);
        done.push(now);
        sent += now;
        if sent < count {
            sender.writable().await?;
        }
    }
    let arrived = take(&mut receiver, &mut ring, count).await?;
    Ok(Segmented {
        arrived,
        done,
        sender: sender.stats(),
        receiver: receiver.stats(),
    })
}

/// Do: in one call, more datagrams than one segmented send carries: 150 of 100 bytes (64 a message,
/// the kernel's segment bound) and 60 of 1,200 (54 a message, its byte bound), the last of each
/// shorter, over IPv4 and IPv6 loopback, received through a ring. Expect: every datagram once, in
/// order, with its own length (the last too); on Linux, which segments, as few calls as the bounds
/// allow and no more; elsewhere a call a datagram.
#[test]
fn a_segmented_send_crosses_whole_and_in_order_from_the_callers_buffer() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    for loopback in [
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        SocketAddr::from((Ipv6Addr::LOCALHOST, 0)),
    ] {
        for (count, segment, last) in [(150, 100, 37), (60, 1_200, 700)] {
            let trip = rt
                .block_on(segmented_round_trip(loopback, count, segment, last))
                .unwrap()
                .unwrap();
            let expected: Vec<(usize, usize)> = (0..count)
                .map(|index| (index, if index + 1 == count { last } else { segment }))
                .collect();
            assert_eq!(
                trip.arrived, expected,
                "{count} of {segment} bytes over {loopback}: every one, in order, its own length"
            );
            assert_eq!(trip.done.iter().sum::<usize>(), count, "{:?}", trip.done);
            assert_eq!(trip.sender.sent, count as u64);
            assert_eq!(trip.receiver.received, count as u64);
            let per_message = (65_507 / segment).min(MOST_SEGMENTS);
            if cfg!(target_os = "linux") {
                assert!(
                    trip.sender.gso,
                    "Linux 4.18 and later segment: {:?}",
                    trip.sender
                );
                assert_eq!(
                    trip.sender.send_calls,
                    count.div_ceil(per_message) as u64,
                    "a call a message of {per_message}: {:?}",
                    trip.sender
                );
            } else {
                assert_eq!(trip.sender.send_calls, count as u64, "{:?}", trip.sender);
            }
        }
    }
}

/// Do: two datagrams queued in a socket's outbox, then a segmented send of three. Expect: the queued
/// two arrive first, then the three, each whole: a segmented send never passes what the outbox
/// holds.
#[test]
fn what_the_outbox_holds_leaves_before_a_segmented_send() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let receiver = UdpSocket::bind(loopback).unwrap();
        let to = receiver.local_addr().unwrap();
        let mut sender = Batched::bind(loopback, Io { batch: 4 }, false).unwrap();
        assert!(sender.queue(to, b"first"));
        assert!(sender.queue(to, b"second"));
        assert_eq!(sender.send_segments(to, b"aaabbbcc", 3), 3);
        assert!(!sender.pending());
        let mut buffer = [0u8; 16];
        for expected in [&b"first"[..], b"second", b"aaa", b"bbb", b"cc"] {
            let (length, _) = receiver.recv_from(&mut buffer).await.unwrap();
            assert_eq!(&buffer[..length], expected);
        }
    })
    .unwrap();
}

/// Do: on the simulation fabric, a segmented send of five while the socket takes nothing, then the
/// same once it takes again. Expect: none done and none sent the first time, all five the second,
/// arriving once each and in order: what the socket did not take is the caller's to send again.
#[test]
fn a_segmented_send_the_socket_does_not_take_is_done_by_none_and_sent_again() {
    let mut sim = SimRuntime::new(&config(), 1).unwrap();
    let id = sim.shard_ids()[0];
    let (done_tx, done_rx) = channel();
    sim.spawn_on(id, async move {
        let outcome = async {
            let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
            let mut sender = Batched::bind_lent(loopback, false)?;
            let mut receiver = Batched::bind_lent(loopback, false)?;
            let mut ring = Ring::new(Io { batch: 4 })?;
            let to = receiver.local_addr()?;
            let port = sender.local_addr()?.port();
            let bytes = datagrams(5, 10, 4);
            hyper_rt::sim::sim_udp_block_sends(port);
            let refused = sender.send_segments(to, &bytes, 10);
            let sent_while_blocked = sender.stats().sent;
            hyper_rt::sim::sim_udp_release_sends(port);
            let done = sender.send_segments(to, &bytes, 10);
            let arrived = take(&mut receiver, &mut ring, 5).await?;
            Ok::<_, RtError>((refused, sent_while_blocked, done, arrived, sender.stats()))
        }
        .await;
        let _ = done_tx.send(outcome);
    })
    .unwrap();
    sim.run_until_idle();
    let (refused, sent_while_blocked, done, arrived, stats) = done_rx.try_recv().unwrap().unwrap();
    assert_eq!(
        (refused, sent_while_blocked),
        (0, 0),
        "nothing taken, nothing done"
    );
    assert_eq!(done, 5);
    assert_eq!(
        arrived,
        vec![(0, 10), (1, 10), (2, 10), (3, 10), (4, 4)],
        "each once, in order"
    );
    assert_eq!(
        stats.send_calls, 5,
        "the fabric does not segment: {stats:?}"
    );
}

/// Do: one ring, two lent sockets each sent three datagrams, each read through the ring in turn.
/// Expect: each socket hands over its own three; neither holds a receive buffer of its own before or
/// after (the ring's buffers go back to it); a lent socket's own receive is refused, there being
/// nothing to receive into.
#[test]
fn one_ring_serves_every_lent_socket_and_none_holds_its_buffers() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let mut ring = Ring::new(Io { batch: 4 }).unwrap();
        assert!(
            ring.held_bound() >= RECEIVE_BYTES,
            "the ring holds the buffers"
        );
        let mut sender = Batched::bind_lent(loopback, false).unwrap();
        let mut first = Batched::bind_lent(loopback, false).unwrap();
        let mut second = Batched::bind_lent(loopback, false).unwrap();
        for socket in [&first, &second] {
            assert!(
                socket.held_bytes() < RECEIVE_BYTES,
                "{}",
                socket.held_bytes()
            );
        }
        let bytes = datagrams(3, 8, 8);
        for socket in [&first, &second] {
            assert_eq!(
                sender.send_segments(socket.local_addr().unwrap(), &bytes, 8),
                3
            );
        }
        for socket in [&mut first, &mut second] {
            let arrived = take(socket, &mut ring, 3).await.unwrap();
            assert_eq!(arrived, vec![(0, 8), (1, 8), (2, 8)]);
            assert!(
                socket.held_bytes() < RECEIVE_BYTES,
                "{}",
                socket.held_bytes()
            );
            assert!(matches!(
                socket.receive(|_, _| {}),
                Err(RtError::BadConfig { .. })
            ));
        }
    })
    .unwrap();
}

/// A socket whose segmented sends the kernel refuses: one sending without UDP checksums
/// (`SO_NO_CHECK`), which `udp_send_skb` refuses to segment with EINVAL, as drivers that cannot
/// segment have refused it (quinn-udp). Do: ten datagrams of equal size in one segmented send, and
/// ten queued in an outbox the kernel would segment as one message. Expect: every datagram arrives,
/// once and in order, each sent a datagram a call once the socket stops segmenting; none refused.
#[cfg(target_os = "linux")]
#[test]
fn a_socket_the_kernel_will_not_segment_for_sends_each_datagram_alone() {
    use std::os::fd::AsRawFd;

    /// A socket bound to `loopback` that sends without UDP checksums.
    #[allow(
        unsafe_code,
        reason = "SO_NO_CHECK, which the runtime has no call for, is set through libc"
    )]
    fn unchecked(loopback: SocketAddr) -> UdpSocket {
        let owned = UdpSocket::bind(loopback).unwrap().into_owned().unwrap();
        let one: libc::c_int = 1;
        // SAFETY: `owned` is an open socket, and the option's value is a live `c_int` of the
        // length passed.
        let set = unsafe {
            libc::setsockopt(
                owned.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_NO_CHECK,
                (&raw const one).cast(),
                size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        assert_eq!(set, 0, "{}", std::io::Error::last_os_error());
        UdpSocket::adopt(owned).unwrap()
    }

    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let mut receiver = Batched::bind_lent(loopback, false).unwrap();
        let mut ring = Ring::new(Io { batch: 4 }).unwrap();
        let to = receiver.local_addr().unwrap();
        let expected: Vec<(usize, usize)> = (0..10).map(|index| (index, 100)).collect();

        let mut lent = Batched::lent(unchecked(loopback), false).unwrap();
        assert!(
            lent.stats().gso,
            "the kernel offers segmentation: {:?}",
            lent.stats()
        );
        assert_eq!(lent.send_segments(to, &datagrams(10, 100, 100), 100), 10);
        let stats = lent.stats();
        assert!(!stats.gso, "the socket stopped segmenting: {stats:?}");
        assert_eq!(
            (stats.sent, stats.send_calls, stats.send_errors),
            (10, 10, 0)
        );
        assert_eq!(take(&mut receiver, &mut ring, 10).await.unwrap(), expected);

        let mut outboxed = Batched::new(unchecked(loopback), Io { batch: 16 }, false).unwrap();
        let bytes = datagrams(10, 100, 100);
        for datagram in bytes.chunks(100) {
            assert!(outboxed.queue(to, datagram));
        }
        while outboxed.send() == Sent::Blocked {
            outboxed.writable().await.unwrap();
        }
        let stats = outboxed.stats();
        assert!(!stats.gso, "the socket stopped segmenting: {stats:?}");
        assert_eq!((stats.sent, stats.send_errors), (10, 0), "{stats:?}");
        assert_eq!(take(&mut receiver, &mut ring, 10).await.unwrap(), expected);
    })
    .unwrap();
}

/// Do: on loopback with the kernel's receive stamps, a ring of one buffer (a datagram a call),
/// three datagrams sent, the shard's clock read, three more sent, then receives of what was queued
/// until all six are taken. Expect: the receive begun at that reading hands over at most one
/// datagram sent after it, the one whose call ends it; every datagram arrives once and in order;
/// the receive that finds the socket empty says so. A stamp is late, never early (`arrival`), so
/// a busy machine can end a receive sooner, never later. A socket without stamps (Windows) counts
/// the bytes queued when the receive is called instead (`FIONREAD`, every datagram on Winsock), the
/// reading aside: all six were queued by then, so the bound on what came after the reading is a
/// stamped socket's alone; the order, and the receive that finds the socket empty, hold for both.
#[test]
fn a_queued_receive_takes_what_was_queued_when_it_began() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let sender = UdpSocket::bind(loopback).unwrap();
        let mut receiver = Batched::bind_lent(loopback, true).unwrap();
        let mut ring = Ring::new(Io { batch: 1 }).unwrap();
        let to = receiver.local_addr().unwrap();
        for index in 0..3u8 {
            sender.send_to(&[index], to).unwrap();
        }
        let began = now_ns();
        for index in 3..6u8 {
            sender.send_to(&[index], to).unwrap();
        }
        let mut order = Vec::new();
        let mut first = None;
        let mut emptied = false;
        for _ in 0..64 {
            if order.len() == 6 && emptied {
                break;
            }
            let start = if first.is_none() { began } else { now_ns() };
            let mut taken = Vec::new();
            let (got, ended) = receiver
                .receive_queued(&mut ring, start, 16, |_, bytes| taken.push(bytes[0]))
                .unwrap();
            assert_eq!(got.datagrams, taken.len());
            assert_eq!(got.bytes, taken.len(), "one byte each");
            first.get_or_insert_with(|| taken.clone());
            order.extend_from_slice(&taken);
            emptied = ended == Queued::Emptied;
            if emptied && order.len() < 6 {
                receiver.readable().await.unwrap();
            }
        }
        assert_eq!(
            order,
            vec![0, 1, 2, 3, 4, 5],
            "every datagram once, in order"
        );
        let first = first.unwrap();
        if receiver.stats().kernel_stamps {
            assert!(
                first.iter().filter(|index| **index >= 3).count() <= 1,
                "the first receive took more than one datagram sent after it began: {first:?}"
            );
        }
        assert!(emptied, "the last receive found the socket empty");
    })
    .unwrap();
}

/// Do: on Linux, more datagrams sent than the receiver's buffer holds, the queue drained, then one
/// more sent and received. Expect: the drain is told no drop (the datagrams queued before the
/// overflow carry no count), and the one queued after it says the socket dropped some
/// (`SO_RXQ_OVFL`): a drop is told by the next datagram, without reading the socket's account.
#[cfg(target_os = "linux")]
#[test]
fn a_drop_is_told_by_the_next_datagram() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let sender = UdpSocket::bind(loopback).unwrap();
        let mut receiver = Batched::bind_lent(loopback, true).unwrap();
        let mut ring = Ring::new(Io { batch: 64 }).unwrap();
        let to = receiver.local_addr().unwrap();
        assert_eq!(receiver.stats().drops, Some(0), "the kernel tells drops");
        // Twice the buffer the socket reports, in datagrams of 1,200 bytes: loopback queues each
        // as it is sent, and drops what the queue cannot hold.
        let buffer = receiver.socket().recv_buffer_bytes().unwrap();
        let datagrams = buffer * 2 / 1_200 + 1;
        for _ in 0..datagrams {
            sender.send_to(&[7u8; 1_200], to).unwrap();
        }
        let mut drained = 0usize;
        loop {
            let now = now_ns();
            let (got, ended) = receiver
                .receive_queued(&mut ring, now, 1_024, |_, _| {})
                .unwrap();
            drained += got.datagrams;
            if ended == Queued::Emptied {
                break;
            }
        }
        assert!(drained < datagrams, "the queue held fewer than were sent");
        assert_eq!(
            receiver.stats().drops,
            Some(0),
            "no datagram has told a drop yet"
        );
        sender.send_to(&[8u8], to).unwrap();
        let mut told = None;
        for _ in 0..64 {
            let mut last = None;
            let (got, _) = receiver
                .receive_queued(&mut ring, now_ns(), 16, |_, bytes| last = Some(bytes[0]))
                .unwrap();
            if got.datagrams > 0 {
                told = last;
                break;
            }
            receiver.readable().await.unwrap();
        }
        assert_eq!(
            told,
            Some(8),
            "the datagram sent after the overflow arrived"
        );
        let drops = receiver.stats().drops.unwrap();
        assert!(drops > 0, "the datagram after the overflow told its drops");
        assert_eq!(
            usize::try_from(drops).unwrap(),
            datagrams - drained,
            "every datagram the queue could not hold, counted"
        );
    })
    .unwrap();
}

/// Do: on Linux, a ring grown to eight places, three datagrams queued, then one receive of what
/// was queued. Expect: one call takes the three and says the socket is empty. recvmmsg(2) with
/// `MSG_DONTWAIT` returns what is queued, so a call that fills fewer places than it offers found
/// nothing more, and the receive asks no more. Until 2026-10-16 it asked again for the `EAGAIN`
/// that said so: a system call a drain, 0.59 a message at 10,000 messages a second (strace of a bus
/// node).
#[cfg(target_os = "linux")]
#[test]
fn a_receive_that_fills_fewer_places_than_it_offers_asks_no_more() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let sender = UdpSocket::bind(loopback).unwrap();
        let mut receiver = Batched::bind_lent(loopback, true).unwrap();
        let mut ring = Ring::new(Io { batch: 8 }).unwrap();
        ring.resize(8).unwrap();
        let to = receiver.local_addr().unwrap();
        // A loopback send queues its datagram before it returns.
        for index in 0..3u8 {
            sender.send_to(&[index], to).unwrap();
        }
        let before = receiver.stats();
        let mut taken = Vec::new();
        let (got, ended) = receiver
            .receive_queued(&mut ring, now_ns(), 16, |_, bytes| taken.push(bytes[0]))
            .unwrap();
        let after = receiver.stats();
        assert_eq!(taken, vec![0, 1, 2]);
        assert_eq!(got.datagrams, 3);
        assert_eq!(ended, Queued::Emptied);
        assert_eq!(after.receive_calls - before.receive_calls, 1);
        assert_eq!(
            after.receives_empty - before.receives_empty,
            0,
            "a call that filled three of eight places asked again"
        );
    })
    .unwrap();
}

/// Queues `count` one-byte datagrams for `receiver` and takes them through `ring` in one drain,
/// begun at `began_ns`; the messages it took, how it ended, and the receive calls and the empty
/// ones it made.
#[cfg(target_os = "linux")]
fn drained(
    sender: &UdpSocket,
    receiver: &mut Batched,
    ring: &mut Ring,
    count: u8,
    began_ns: u64,
) -> (usize, Queued, u64, u64) {
    let to = receiver.local_addr().unwrap();
    // A loopback send queues its datagram before it returns.
    for index in 0..count {
        sender.send_to(&[index], to).unwrap();
    }
    let before = receiver.stats();
    let (got, ended) = receiver
        .receive_queued(ring, began_ns, 16, |_, _| {})
        .unwrap();
    let after = receiver.stats();
    (
        got.messages,
        ended,
        after.receive_calls - before.receive_calls,
        after.receives_empty - before.receives_empty,
    )
}

/// Do: on Linux, a new ring of a batch of eight (one place), five datagrams queued and taken in a
/// drain that no stamp is past, the ring resized as it then wants, four more queued and taken.
/// Expect: the first drain takes the five a call each, its sixth call finding the socket empty,
/// and the ring wants five; resized, it holds five buffers, and the second drain takes the four in
/// one call that asks no more. A ring holds what its drains needed, not its batch from the start.
#[cfg(target_os = "linux")]
#[test]
fn a_ring_grows_to_what_a_drain_took_and_the_next_takes_one_call() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let sender = UdpSocket::bind(loopback).unwrap();
        let mut receiver = Batched::bind_lent(loopback, true).unwrap();
        let io = Io { batch: 8 };
        let mut ring = Ring::new(io).unwrap();
        assert_eq!(ring.buffers(), 1);
        assert_eq!(ring.held_bytes(), Ring::bytes_for(io, 1));
        assert!(ring.held_bytes() < ring.held_bound());
        // Begun at the end of time: no kernel stamp, carried late by a busy machine, is past it.
        assert_eq!(
            drained(&sender, &mut receiver, &mut ring, 5, u64::MAX),
            (5, Queued::Emptied, 5, 1)
        );
        assert_eq!(ring.wants(), Some(5));
        ring.resize(5).unwrap();
        assert_eq!(ring.held_bytes(), Ring::bytes_for(io, 5));
        assert_eq!(
            drained(&sender, &mut receiver, &mut ring, 4, u64::MAX),
            (4, Queued::Emptied, 1, 0)
        );
        assert_eq!(ring.wants(), None, "{ring:?}");
    })
    .unwrap();
}

/// Do: on Linux, a socket without the kernel's stamps, which takes one call a drain (its
/// `FIONREAD` tells only the next datagram), seven datagrams queued, then drains of a new ring of a
/// batch of eight, resized each time it wants. Expect: each drain fills every place it offers and
/// leaves more, and the ring doubles, 1, 2, 4, to its batch; the drain after the last takes
/// nothing, and the ring wants no more.
#[cfg(target_os = "linux")]
#[test]
fn a_drain_that_leaves_more_doubles_the_ring() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let sender = UdpSocket::bind(loopback).unwrap();
        let mut receiver = Batched::bind_lent(loopback, false).unwrap();
        let mut ring = Ring::new(Io { batch: 8 }).unwrap();
        let first = drained(&sender, &mut receiver, &mut ring, 7, now_ns());
        assert_eq!(first, (1, Queued::More, 1, 0));
        for (took, wanted) in [(1, 2), (2, 4), (4, 8)] {
            assert_eq!(
                ring.wants(),
                Some(wanted),
                "after a drain of {took}: {ring:?}"
            );
            ring.resize(wanted).unwrap();
            if wanted < 8 {
                let next = drained(&sender, &mut receiver, &mut ring, 0, now_ns());
                assert_eq!(next, (wanted, Queued::More, 1, 0));
            }
        }
        let last = drained(&sender, &mut receiver, &mut ring, 0, now_ns());
        assert_eq!((last.0, last.1), (0, Queued::Emptied));
        assert_eq!(ring.wants(), None, "{ring:?}");
    })
    .unwrap();
}

/// Do: on the simulation fabric, which stamps nothing, three datagrams queued, then two receives
/// of what was queued. Expect: the first takes the three, the bytes the socket held when it began
/// (`UdpSocket::queued_bytes`), and says more may follow; the second finds the socket empty.
#[test]
fn a_socket_without_stamps_takes_the_bytes_it_held() {
    let mut sim = SimRuntime::new(&config(), 1).unwrap();
    let id = sim.shard_ids()[0];
    let (done_tx, done_rx) = channel();
    sim.spawn_on(id, async move {
        let outcome = async {
            let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
            let sender = UdpSocket::bind(loopback)?;
            let mut receiver = Batched::bind_lent(loopback, true)?;
            let mut ring = Ring::new(Io { batch: 1 })?;
            let to = receiver.local_addr()?;
            for index in 0..3u8 {
                sender.send_to(&[index, index], to)?;
            }
            receiver.readable().await?;
            let held = receiver.socket().queued_bytes()?;
            let mut taken = Vec::new();
            let first = receiver.receive_queued(&mut ring, now_ns(), 16, |_, bytes| {
                taken.push(bytes[0]);
            })?;
            let second = receiver.receive_queued(&mut ring, now_ns(), 16, |_, _| {})?;
            Ok::<_, RtError>((held, taken, first, second, receiver.stats()))
        }
        .await;
        let _ = done_tx.send(outcome);
    })
    .unwrap();
    sim.run_until_idle();
    let (held, taken, first, second, stats) = done_rx.try_recv().unwrap().unwrap();
    assert!(!stats.kernel_stamps, "no kernel on the fabric");
    assert_eq!(held, 6, "three datagrams of two bytes held");
    assert_eq!(taken, vec![0, 1, 2]);
    assert_eq!(
        (first.0.datagrams, first.0.messages, first.0.bytes),
        (3, 3, 6)
    );
    assert_eq!(first.1, Queued::More);
    assert_eq!((second.0.datagrams, second.1), (0, Queued::Emptied));
}

/// Do: one datagram of 1,000 bytes queued, its charge read, then the datagram read and the charge
/// read again. Expect: on Linux (`SO_MEMINFO`) the queue charged more than the payload, its
/// allocation's truesize, against the buffer `SO_RCVBUF` reports, a quarter of it the release
/// lag, and nothing once read; on macOS (`FIONREAD`, `sb_cc`) the payload and the address XNU
/// counts beside it, with its mbuf limit as a floor, and nothing once read; on Windows no account
/// a process may read.
#[test]
fn a_socket_tells_what_its_queue_is_charged() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let sender = UdpSocket::bind(loopback).unwrap();
        let receiver = UdpSocket::bind(loopback).unwrap();
        let to = receiver.local_addr().unwrap();
        sender.send_to(&[5u8; 1_000], to).unwrap();
        let mut buffer = [0u8; 2_000];
        let charge = loop {
            let charge = receiver.queue_charge().unwrap();
            match charge {
                Some(charge) if charge.charged == 0 => receiver.readable().await.unwrap(),
                other => break other,
            }
        };
        let (length, _) = receiver.recv_from(&mut buffer).await.unwrap();
        assert_eq!(length, 1_000);
        if cfg!(target_os = "linux") {
            let charge = charge.expect("Linux tells the queue's charge");
            assert!(
                charge.charged > 1_000,
                "the allocation, past the payload: {charge:?}"
            );
            assert_eq!(charge.buffer, receiver.recv_buffer_bytes().unwrap() as u64);
            assert_eq!(charge.release_lag, charge.buffer >> 2);
            assert_eq!(charge.message_floor, 0, "SO_MEMINFO is the whole account");
            assert_eq!(
                receiver.queue_charge().unwrap().unwrap().charged,
                0,
                "released once read"
            );
        } else if cfg!(target_os = "macos") {
            let charge = charge.expect("macOS tells the queue's byte count");
            // `sbappendaddr`: the payload and the sender's address (`sockaddr_in`, 16 bytes).
            assert_eq!(charge.charged, 1_016, "{charge:?}");
            assert_eq!(charge.buffer, receiver.recv_buffer_bytes().unwrap() as u64);
            assert_eq!(charge.release_lag, 0);
            assert_eq!(
                charge.message_floor,
                (3 * 256 + 16_384) / 8,
                "XNU's mbuf limit"
            );
            assert_eq!(
                receiver.queue_charge().unwrap().unwrap().charged,
                0,
                "released once read"
            );
        } else {
            assert_eq!(charge, None, "no account a process may read");
        }
    })
    .unwrap();
}

/// Do: on the simulation fabric, one datagram of 1,000 bytes queued, its charge read, then the
/// datagram read. Expect: the mailbox charged its payload exactly, against the fabric's buffer,
/// released at once, and nothing once read.
#[test]
fn a_simulated_socket_is_charged_its_payload() {
    let mut sim = SimRuntime::new(&config(), 1).unwrap();
    let id = sim.shard_ids()[0];
    let (done_tx, done_rx) = channel();
    sim.spawn_on(id, async move {
        let outcome = async {
            let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
            let sender = UdpSocket::bind(loopback)?;
            let receiver = UdpSocket::bind(loopback)?;
            let to = receiver.local_addr()?;
            sender.send_to(&[5u8; 1_000], to)?;
            receiver.readable().await?;
            let queued = receiver.queue_charge()?;
            let mut buffer = [0u8; 2_000];
            let _ = receiver.recv_from(&mut buffer).await?;
            Ok::<_, RtError>((queued, receiver.queue_charge()?))
        }
        .await;
        let _ = done_tx.send(outcome);
    })
    .unwrap();
    sim.run_until_idle();
    let (queued, read) = done_rx.try_recv().unwrap().unwrap();
    let queued = queued.unwrap();
    assert_eq!(queued.charged, 1_000);
    assert_eq!(queued.buffer, hyper_rt::sim::SIM_RECV_BUFFER_BYTES as u64);
    assert_eq!(queued.release_lag, 0);
    assert_eq!(read.unwrap().charged, 0);
}
