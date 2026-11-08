//! A driver whose task was held still (its process stopped, swapped out, its VM or host paused) reads
//! what its sockets took during the hold before it judges its timers: a connection's idle timer fires
//! only at a clock read before a drain that took every datagram queued by then (the driver's
//! `poll_event`). Found by mantle (docs/measurements/2026-11-06-node-test-holds.md at e718cddb): a
//! node held 4 s by SIGSTOP timed out a client's connection at its first turn after SIGCONT, while
//! what the client sent during the hold sat unread in its socket and the client's own idle timer had
//! 0.55 s left.
//!
//! Each test holds a driver by not polling it, on real sockets and the host's clock: the hold is the
//! only wall-clock wait. Every other wait is for a fact, guarded only against one that never comes.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    dead_code,
    missing_docs
)]

#[path = "../../hyper-transport/tests/common/mod.rs"]
mod common;

use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use common::*;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt_transport::{Driver, Io};
use hyper_transport::{Config, Endpoint, Event, Fixed, Limits};

type Drv = Driver<Mantle, Fixed, Book>;

/// Datagrams a system call carries: one QUIC initial window (RFC 9002 §7.2).
const IO: Io = Io { batch: 10 };
/// The failure guard of a wait for a fact, far past anything loopback needs.
const GUARD: Duration = Duration::from_secs(60);
/// The connections' idle timeout (RFC 9000 §10.1), both sides': each hold is judged against it.
/// Long enough that the quarter of it each test keeps as its margin outlasts a busy host's
/// scheduling delays.
const IDLE: Duration = Duration::from_secs(4);
/// How long a held driver is not polled: past its idle timer by a quarter of the timeout.
const HOLD: Duration = Duration::from_secs(5);
/// How far into the one-sided hold the running side sends: its own idle timer then runs from
/// here, due a quarter of the timeout after the held side is polled again.
const SPEAKS: Duration = Duration::from_secs(2);
/// The server is node 2 in the client's directory, the client node 1 in the server's.
const SERVER: u64 = 2;
const CLIENT: u64 = 1;

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

/// The harness's limits with the idle timeout [`IDLE`] and no keep-alive: nothing but the test's
/// own frames resets an idle timer.
fn held_limits() -> Limits {
    Limits {
        idle_timeout: IDLE,
        keep_alive: None,
        ..limits()
    }
}

fn endpoint(
    pki: &Pki,
    own: &(Vec<u8>, Vec<u8>),
    other: &[u8],
    peer: u64,
    listen: bool,
) -> Endpoint<Mantle, Fixed, Book> {
    let mut book = Book::default();
    book.add(other, peer, Role::Node);
    let config = Config {
        tls: hyper_transport::tls::Tls::mutual(&credentials(&pki.root, &own.0, &own.1), listen)
            .unwrap(),
        role: Role::Node,
        limits: held_limits(),
        congestion: Default::default(),
    };
    Endpoint::new(config, Fixed::new(512 << 20, 64), book, Instant::now()).unwrap()
}

/// What each side said, by the side that said it.
#[derive(Default)]
struct Seen {
    connected: u32,
    /// Each frame a side took: the side and its bytes.
    frames: Vec<(u64, Vec<u8>)>,
    /// Each connection a side saw close: the side and the peer.
    closed: Vec<(u64, u64)>,
    steps: u64,
}

/// Takes the next event of the drivers given (one held is passed as `None`), or a millisecond's
/// worth of nothing, and records it. The driver asked first alternates from one step to the next.
async fn step(mut server: Option<&mut Drv>, mut client: Option<&mut Drv>, seen: &mut Seen) {
    let mut tick = pin!(hyper_rt::futures::sleep(1_000_000));
    let server_first = seen.steps.is_multiple_of(2);
    seen.steps = seen.steps.wrapping_add(1);
    let next = poll_fn(|context| {
        for side in if server_first {
            [SERVER, CLIENT]
        } else {
            [CLIENT, SERVER]
        } {
            let driver = if side == SERVER {
                server.as_deref_mut()
            } else {
                client.as_deref_mut()
            };
            if let Some(driver) = driver
                && let Poll::Ready(event) = driver.poll_event(context)
            {
                return Poll::Ready(Some((side, event.unwrap())));
            }
        }
        if tick.as_mut().poll(context).is_ready() {
            return Poll::Ready(None);
        }
        Poll::Pending
    })
    .await;
    let Some((side, event)) = next else {
        return;
    };
    let driver = if side == SERVER { server } else { client }.unwrap();
    let mut take = |event: Event<Mantle>, driver: &mut Drv| match event {
        Event::Connected { .. } => seen.connected += 1,
        Event::Closed { peer, .. } => seen.closed.push((side, peer)),
        Event::Frame { frame, .. } => {
            seen.frames.push((side, frame.bytes().to_vec()));
            driver.endpoint().release(frame);
        }
        _ => {}
    };
    take(event, driver);
    while let Some(event) = driver.endpoint().poll_event() {
        take(event, driver);
    }
}

/// Whether the driver's endpoint has nothing due before half the idle timeout: no packet of its
/// own waits on an acknowledgement (its probe timer, RFC 9002 §6.2), and no acknowledgement of its
/// own waits to be sent (its delay timer, RFC 9000 §13.2.1). Its idle timer alone is armed, and
/// its next ack-eliciting packet restarts it (RFC 9000 §10.1).
fn quiet(driver: &mut Drv) -> bool {
    let now = driver.clock().unwrap();
    driver
        .endpoint()
        .poll_timeout()
        .is_none_or(|due| due > now + IDLE / 2)
}

/// The path MTU `driver`'s connection to `peer` has found.
fn mtu(driver: &mut Drv, peer: u64) -> Option<u16> {
    driver
        .endpoint()
        .connection_stats(peer)
        .map(|stats| stats.path.current_mtu)
}

/// Two drivers on loopback with a connection up, each side's MTU search ended at its path's MTU as
/// the kernel holds it, and each quiet ([`quiet`]). A search not ended holds a probe its window
/// does not yet afford (hyper-quic patch Q25), and the pacer's timer, re-armed for it, is never
/// quiet: each side sends bursts that fill its lane, so its window grows and the probe goes, as
/// `peer_sockets::a_connections_mtu_search_ends_at_its_paths_mtu_as_the_kernel_holds_it` does.
async fn connected(pki: &Pki, seen: &mut Seen) -> (Drv, Drv) {
    let one = pki.issue(&name(1));
    let two = pki.issue(&name(2));
    let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut server: Drv =
        Driver::bind(endpoint(pki, &two, &one.0, CLIENT, true), loopback, IO).unwrap();
    let server_addr = server.local_addr().unwrap();
    let mut client: Drv =
        Driver::bind(endpoint(pki, &one, &two.0, SERVER, false), loopback, IO).unwrap();
    let client_addr = client.local_addr().unwrap();
    let asked = hyper_rt::udp::UdpSocket::bind(loopback).unwrap();
    let to_server = asked.path_payload_to(server_addr).unwrap();
    let to_client = asked.path_payload_to(client_addr).unwrap();
    let now = client.clock().unwrap();
    client.endpoint().connect(now, SERVER, server_addr).unwrap();
    let began = Instant::now();
    while seen.connected < 2 {
        step(Some(&mut server), Some(&mut client), seen).await;
        assert!(began.elapsed() < GUARD, "the two never connected");
    }
    let frame = [1u8; 1_000];
    while to_server.is_some_and(|to| mtu(&mut client, SERVER) != Some(to))
        || to_client.is_some_and(|to| mtu(&mut server, CLIENT) != Some(to))
    {
        for (driver, peer) in [(&mut client, SERVER), (&mut server, CLIENT)] {
            for _ in 0..2_000 {
                if driver
                    .endpoint()
                    .send_frame(peer, 0, Kind::Append, &frame)
                    .is_err()
                {
                    break;
                }
            }
        }
        step(Some(&mut server), Some(&mut client), seen).await;
        assert!(began.elapsed() < GUARD, "the MTU searches never ended");
    }
    while !quiet(&mut server) || !quiet(&mut client) {
        step(Some(&mut server), Some(&mut client), seen).await;
        assert!(began.elapsed() < GUARD, "the two never settled");
    }
    assert_eq!(seen.closed, []);
    seen.frames.clear();
    (server, client)
}

/// Each side sends the other a frame and takes the other's: the connection carries both ways.
async fn both_carry(server: &mut Drv, client: &mut Drv, seen: &mut Seen, mark: u8) {
    let first = seen.frames.len();
    server
        .endpoint()
        .send_frame(CLIENT, 0, Kind::Append, &[mark; 100])
        .unwrap();
    client
        .endpoint()
        .send_frame(SERVER, 0, Kind::Append, &[mark; 100])
        .unwrap();
    let began = Instant::now();
    while seen.frames.len() < first + 2 {
        step(Some(&mut *server), Some(&mut *client), seen).await;
        assert!(
            began.elapsed() < GUARD,
            "the frames marked {mark} never arrived"
        );
        assert_eq!(seen.closed, []);
    }
    let mut got = seen.frames[first..].to_vec();
    got.sort();
    assert_eq!(got, [(CLIENT, vec![mark; 100]), (SERVER, vec![mark; 100])]);
}

/// Do: a connection up and quiet; the server held still past its idle timer, while the client,
/// running, sends it a frame partway into the hold (its own idle timer running from that send, due
/// a quarter of the timeout after the hold ends). Expect: the server, polled again, takes the
/// client's frame before it judges its idle timer, and keeps the connection; neither side sees it
/// close, and it carries frames both ways after. Before: the server's first turn timed the
/// connection out (`Event::Closed`), the frame unread in its socket.
#[test]
fn a_driver_held_past_its_idle_timeout_reads_what_came_before_it_times_out() {
    let pki = Pki::new();
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async move {
        let mut seen = Seen::default();
        let (mut server, mut client) = connected(&pki, &mut seen).await;
        let held = Instant::now();
        let mut spoke = false;
        while held.elapsed() < HOLD {
            if !spoke && held.elapsed() >= SPEAKS {
                client
                    .endpoint()
                    .send_frame(SERVER, 0, Kind::Append, &[5; 100])
                    .unwrap();
                spoke = true;
            }
            step(None, Some(&mut client), &mut seen).await;
        }
        assert_eq!(
            seen.closed,
            [],
            "the client kept the connection through the hold"
        );
        assert_eq!(seen.frames, []);
        let began = Instant::now();
        while seen.frames.is_empty() {
            step(Some(&mut server), Some(&mut client), &mut seen).await;
            assert!(began.elapsed() < GUARD, "the client's frame never arrived");
            assert_eq!(
                seen.closed,
                [],
                "the held server judged its idle timer before reading"
            );
        }
        assert_eq!(seen.frames, [(SERVER, vec![5; 100])]);
        both_carry(&mut server, &mut client, &mut seen, 6).await;
        assert!(server.endpoint().connection_stats(CLIENT).is_some());
        assert!(client.endpoint().connection_stats(SERVER).is_some());
        assert_eq!(seen.closed, []);
    })
    .unwrap();
}

/// Do: a connection up and quiet; each side sends the other a frame and both are held still, the
/// whole host paused, past their idle timers with the frames in flight both ways (each side's idle
/// timer running from its own send). Expect: each, polled again, takes the other's frame before it
/// judges its idle timer, and both keep the connection, which carries frames both ways after.
/// Before: whichever was polled first timed the connection out at its first turn.
#[test]
fn drivers_held_together_past_their_idle_timeouts_keep_the_connection() {
    let pki = Pki::new();
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async move {
        let mut seen = Seen::default();
        let (mut server, mut client) = connected(&pki, &mut seen).await;
        server
            .endpoint()
            .send_frame(CLIENT, 0, Kind::Append, &[7; 100])
            .unwrap();
        client
            .endpoint()
            .send_frame(SERVER, 0, Kind::Append, &[8; 100])
            .unwrap();
        server.flush().unwrap();
        client.flush().unwrap();
        let held = u64::try_from(HOLD.as_nanos()).unwrap();
        hyper_rt::futures::sleep(held).await.unwrap();
        let began = Instant::now();
        while seen.frames.len() < 2 {
            step(Some(&mut server), Some(&mut client), &mut seen).await;
            assert!(began.elapsed() < GUARD, "the held frames never arrived");
            assert_eq!(
                seen.closed,
                [],
                "a held driver judged its idle timer before reading"
            );
        }
        let mut got = seen.frames.clone();
        got.sort();
        assert_eq!(got, [(CLIENT, vec![7; 100]), (SERVER, vec![8; 100])]);
        both_carry(&mut server, &mut client, &mut seen, 9).await;
        assert!(server.endpoint().connection_stats(CLIENT).is_some());
        assert!(client.endpoint().connection_stats(SERVER).is_some());
        assert_eq!(seen.closed, []);
    })
    .unwrap();
}
