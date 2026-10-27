//! A driver whose port is shared gives each connection its endpoint identifies a socket of its own,
//! bound to that port and connected to the peer (the driver's module doc): the peer's datagrams then
//! arrive on it and not on the listener, the connection's windows are funded from its own socket's
//! room, and the socket goes when the connection does. Where the platform does not hand a connected
//! socket its peer's datagrams (Windows), every peer stays the listener's, as before.

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
use hyper_transport::{Config, Endpoint, Event, Fixed, Refusal};

type Drv = Driver<Mantle, Fixed, Book>;

/// Datagrams a system call carries: one QUIC initial window (RFC 9002 §7.2).
const IO: Io = Io { batch: 10 };
/// The failure guard of a wait, far past anything loopback needs.
const GUARD: Duration = Duration::from_secs(60);
/// The frames the client sends once connected, a thousand bytes each.
const FRAMES: u32 = 2_000;
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
        limits: limits(),
        congestion: Default::default(),
    };
    Endpoint::new(config, Fixed::new(512 << 20, 64), book, Instant::now()).unwrap()
}

/// What the two endpoints said.
#[derive(Default)]
struct Seen {
    connected: u32,
    frames: u32,
    closed: bool,
    /// Steps taken: which driver a step asks first alternates with it.
    steps: u64,
}

/// Takes the next event of either driver, or a tick's worth of nothing, and acts on it. The driver
/// asked first alternates from one step to the next: asked first always, one side flooding the
/// other had an event at every step, the other's were not taken, the frames it was sent held
/// unreleased, its own lane full, and its connection, sending nothing, never grew a window its MTU
/// probe fits (`a_connections_mtu_search_...`, 60 s at 1,200 bytes on Linux CI, 2026-10-22).
async fn step(server: &mut Drv, client: &mut Drv, seen: &mut Seen) {
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
                &mut *server
            } else {
                &mut *client
            };
            if let Poll::Ready(event) = driver.poll_event(context) {
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
    let driver = if side == SERVER { server } else { client };
    let mut take = |event: Event<Mantle>, driver: &mut Drv| match event {
        Event::Connected { .. } => seen.connected += 1,
        Event::Closed { peer, .. } if side == SERVER && peer == CLIENT => seen.closed = true,
        Event::Frame { frame, .. } => {
            seen.frames += 1;
            driver.endpoint().release(frame);
        }
        _ => {}
    };
    take(event, driver);
    while let Some(event) = driver.endpoint().poll_event() {
        take(event, driver);
    }
}

/// A node's port is its listener's alone: a second driver asked for the port a driver holds is
/// refused `AddrInUse`, as the plane's socket is (found by mantle: at `628111e` a second node of the
/// same user bound a running node's named port without error on Linux 6.19 and Darwin 25.4, and the
/// two split its datagrams; docs/transport.md §4w). Where the platform shares no port (Windows) the
/// listener is a plain bind, which the OS refuses a held port as well.
#[test]
fn a_second_driver_on_a_held_port_is_refused() {
    let pki = Pki::new();
    let one = pki.issue(&name(1));
    let two = pki.issue(&name(2));
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async move {
        let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let first: Drv =
            Driver::bind(endpoint(&pki, &two, &one.0, CLIENT, true), loopback, IO).unwrap();
        let held = first.local_addr().unwrap();
        let second: Result<Drv, _> =
            Driver::bind(endpoint(&pki, &one, &two.0, SERVER, true), held, IO);
        let refused = match &second {
            Err(hyper_rt::RtError::DriverRefused {
                code: Some(code), ..
            }) => Some(std::io::Error::from_raw_os_error(*code).kind()),
            _ => None,
        };
        assert_eq!(
            refused,
            Some(std::io::ErrorKind::AddrInUse),
            "a second driver on {held}: {:?}",
            second.map(|_| ())
        );
        drop(first);
    })
    .unwrap();
}

#[test]
fn each_connection_receives_on_a_socket_of_its_own_and_lets_it_go_with_the_connection() {
    let pki = Pki::new();
    let one = pki.issue(&name(1));
    let two = pki.issue(&name(2));
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let report = rt
        .block_on(async move {
            let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let mut server: Drv =
                Driver::bind(endpoint(&pki, &two, &one.0, CLIENT, true), loopback, IO).unwrap();
            let server_addr = server.local_addr().unwrap();
            let mut client: Drv =
                Driver::bind(endpoint(&pki, &one, &two.0, SERVER, false), loopback, IO).unwrap();
            let now = client.clock().unwrap();
            client.endpoint().connect(now, SERVER, server_addr).unwrap();
            let mut seen = Seen::default();
            let began = Instant::now();
            while seen.connected < 2 {
                step(&mut server, &mut client, &mut seen).await;
                assert!(began.elapsed() < GUARD, "the two never connected");
            }
            // Each driver matches its sockets to the connection at its next turn.
            let shared = usize::from(cfg!(unix));
            while server.peer_sockets() < shared || client.peer_sockets() < shared {
                step(&mut server, &mut client, &mut seen).await;
                assert!(began.elapsed() < GUARD, "no socket of its own for the peer");
            }
            assert_eq!(server.peer_sockets(), shared);
            assert_eq!(client.peer_sockets(), shared);
            assert_eq!(server.endpoint().stats().receive_rooms, shared);
            assert_eq!(client.endpoint().stats().receive_rooms, shared);

            // The client's frames reach the server by the server's socket for the client, not its
            // listener, every one of them.
            let listener_before = server.stats().received;
            let frame = [3u8; 1_000];
            let mut sent = 0;
            while seen.frames < FRAMES {
                while sent < FRAMES {
                    match client.endpoint().send_frame(SERVER, 0, Kind::Append, &frame) {
                        Ok(()) => sent += 1,
                        Err(Refusal::LaneFull) => break,
                        Err(other) => panic!("{other:?}"),
                    }
                }
                step(&mut server, &mut client, &mut seen).await;
                assert!(began.elapsed() < GUARD, "{} of {FRAMES} frames arrived", seen.frames);
            }
            if shared == 1 {
                assert_eq!(
                    server.stats().received,
                    listener_before,
                    "the peer's datagrams came by its own socket"
                );
            }
            // The client's bulk leaves its peer's socket as the endpoint made it, as many datagrams a
            // call as one segmented send carries at the path's MTU where the kernel segments: two or
            // more at Ethernet's, one at loopback's 65,507 bytes on Linux, which fill a send alone.
            let peer = client.peer_stats();
            if cfg!(target_os = "linux") {
                assert!(peer.gso, "{peer:?}");
                let mtu = client
                    .endpoint()
                    .connection_stats(SERVER)
                    .map_or(0, |stats| stats.path.current_mtu);
                let most = u64::try_from(hyper_rt::udp::segments_per_send(usize::from(mtu)).min(2)).unwrap();
                assert!(
                    peer.send_calls * most <= peer.sent,
                    "segmented sends carried fewer than {most} datagrams a call at an MTU of {mtu}: {peer:?}"
                );
            }

            // The connection goes, and its socket with it; what the socket did stays counted.
            let held = server.peer_stats();
            if shared == 1 {
                assert!(held.received > 0, "the frames came by the peer's socket: {held:?}");
            }
            let now = client.clock().unwrap();
            client.endpoint().disconnect(now, SERVER);
            while !seen.closed || server.peer_sockets() > 0 {
                step(&mut server, &mut client, &mut seen).await;
                assert!(began.elapsed() < GUARD, "the server kept the client's socket");
            }
            assert_eq!(server.endpoint().stats().receive_rooms, 0);
            let gone = server.peer_stats();
            for (what, before, after) in [
                ("received", held.received, gone.received),
                ("receive calls", held.receive_calls, gone.receive_calls),
                ("sent", held.sent, gone.sent),
                ("send calls", held.send_calls, gone.send_calls),
            ] {
                assert!(after >= before, "{what} went back from {before} to {after} as the peer went");
            }
            assert!(!gone.gro && !gone.gso, "no socket is held to have a flag: {gone:?}");
            format!(
                "{FRAMES} frames by the peer's own socket ({shared} a side), {} datagrams in {} calls; \
                 its socket let go with it",
                peer.sent, peer.send_calls
            )
        })
        .unwrap();
    eprintln!("{report}");
}

/// A driver given a socket already bound shares its port as a bound one does, where the socket was
/// bound with its port shared: a caller that holds its port from the moment it learns it and hands
/// the socket over loses no peer's queue of its own. Do: a client connects to a server driven on a
/// socket bound with its port shared (`UdpSocket::bind_shared`), then to one driven on a plain
/// socket. Expect: the first gives the client a socket of its own where the platform shares ports,
/// and Winsock, which shares none so, refuses its bind (`netsys::port_sharing`); the second never.
#[test]
fn a_driver_given_a_socket_bound_with_its_port_shared_gives_each_peer_its_own() {
    let pki = Pki::new();
    let one = pki.issue(&name(1));
    let two = pki.issue(&name(2));
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async move {
        let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
        for (socket, apart) in [
            (hyper_rt::udp::UdpSocket::bind_shared(loopback), 1),
            (hyper_rt::udp::UdpSocket::bind(loopback), 0),
        ] {
            let socket = match socket {
                Err(refused) if cfg!(windows) && apart == 1 => {
                    assert!(
                        matches!(refused, hyper_rt::RtError::BadConfig { .. }),
                        "{refused:?}"
                    );
                    continue;
                }
                socket => socket.unwrap(),
            };
            assert_eq!(socket.shares_port().unwrap(), apart == 1);
            let batched = hyper_rt::udp::Batched::lent(socket, false).unwrap();
            let mut server: Drv =
                Driver::new(endpoint(&pki, &two, &one.0, CLIENT, true), batched, IO).unwrap();
            let server_addr = server.local_addr().unwrap();
            let mut client: Drv =
                Driver::bind(endpoint(&pki, &one, &two.0, SERVER, false), loopback, IO).unwrap();
            let now = client.clock().unwrap();
            client.endpoint().connect(now, SERVER, server_addr).unwrap();
            let mut seen = Seen::default();
            let began = Instant::now();
            while seen.connected < 2 || server.peer_sockets() < apart {
                step(&mut server, &mut client, &mut seen).await;
                assert!(
                    began.elapsed() < GUARD,
                    "never connected, or no socket of its own"
                );
            }
            // Turns more, in which a socket the server should not have made would be.
            for _ in 0..8 {
                step(&mut server, &mut client, &mut seen).await;
            }
            assert_eq!(server.peer_sockets(), apart);
            assert_eq!(server.endpoint().stats().receive_rooms, apart);
        }
    })
    .unwrap();
}

/// RFC 8899 §5.1.2: a connection's MTU search ends at its path's MTU as the kernel holds it, the
/// driver telling the connection before its first probe (`Driver::tell_ceilings`): on loopback,
/// 65,507 bytes on Linux (its 65,536-byte MTU past what IPv4's length field says), macOS's UDP cap
/// below its 16,384 (9,216 by default), and nothing past it probed, each side sending what grows its
/// window so its probes are afforded. Each side's search is waited for as a fact; the guard only
/// fails a search that never ends.
#[test]
fn a_connections_mtu_search_ends_at_its_paths_mtu_as_the_kernel_holds_it() {
    let pki = Pki::new();
    let one = pki.issue(&name(1));
    let two = pki.issue(&name(2));
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let report = rt
        .block_on(async move {
            let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let mut server: Drv =
                Driver::bind(endpoint(&pki, &two, &one.0, CLIENT, true), loopback, IO).unwrap();
            let server_addr = server.local_addr().unwrap();
            let mut client: Drv =
                Driver::bind(endpoint(&pki, &one, &two.0, SERVER, false), loopback, IO).unwrap();
            let client_addr = client.local_addr().unwrap();
            let asked = hyper_rt::udp::UdpSocket::bind(loopback).unwrap();
            let to_server = asked.path_payload_to(server_addr).unwrap();
            let to_client = asked.path_payload_to(client_addr).unwrap();
            let now = client.clock().unwrap();
            client.endpoint().connect(now, SERVER, server_addr).unwrap();
            let mut seen = Seen::default();
            let began = Instant::now();
            while seen.connected < 2 {
                step(&mut server, &mut client, &mut seen).await;
                assert!(began.elapsed() < GUARD, "the two never connected");
            }
            let mtu = |driver: &mut Drv, peer| {
                driver
                    .endpoint()
                    .connection_stats(peer)
                    .map(|stats| stats.path.current_mtu)
            };
            let (Some(to_server), Some(to_client)) = (to_server, to_client) else {
                return "the kernel does not say: the search is the peer's limit's".to_owned();
            };
            // A probe waits for its connection's window (RFC 8899 §3, item 7; hyper-quic patch
            // Q25), and an idle connection's window is its first, which a probe of loopback's MTU
            // passes: each side sends bursts that fill its lane, so its window grows, and the
            // searches probe in the pauses between them.
            let frame = [7u8; 1_000];
            while mtu(&mut client, SERVER) != Some(to_server)
                || mtu(&mut server, CLIENT) != Some(to_client)
            {
                for (driver, peer) in [(&mut client, SERVER), (&mut server, CLIENT)] {
                    for _ in 0..FRAMES {
                        if driver.endpoint().send_frame(peer, 0, Kind::Append, &frame).is_err() {
                            break;
                        }
                    }
                }
                step(&mut server, &mut client, &mut seen).await;
                assert!(
                    began.elapsed() < GUARD,
                    "the searches ended at {:?} and {:?}, the paths carry {to_server} and {to_client}; \
                     the client's path {:?}; the server's {:?}",
                    mtu(&mut client, SERVER),
                    mtu(&mut server, CLIENT),
                    client.endpoint().connection_stats(SERVER).map(|stats| stats.path),
                    server.endpoint().connection_stats(CLIENT).map(|stats| stats.path)
                );
            }
            for (driver, peer) in [(&mut client, SERVER), (&mut server, CLIENT)] {
                let path = driver.endpoint().connection_stats(peer).unwrap().path;
                assert_eq!(path.lost_plpmtud_probes, 0, "no probe past the path: {path:?}");
                assert_eq!(path.black_holes_detected, 0, "{path:?}");
            }
            format!("both searches ended at the kernel's payload: {to_server} and {to_client}")
        })
        .unwrap();
    eprintln!("{report}");
}

/// Datagrams the stray sender puts in the server's peer socket at once: more than two drains take
/// when each receive call takes one ([`ONE`]) and a drain makes at most
/// [`hyper_rt_transport::TURNS`] calls, and few enough that Linux's default receive buffer holds
/// them all (about 900 bytes charged each).
const STRAY: usize = 2 * hyper_rt_transport::TURNS + 1;
/// A ring of one buffer, so a drain takes one datagram a call.
const ONE: Io = Io { batch: 1 };

/// A socket left holding datagrams after a drain is drained again without another wake: its
/// readiness is one-shot and fires when a datagram arrives, not for one already waiting, so a
/// driver that awaited it parked with them unread until something else woke it. Do: connect;
/// from the client's port (which its sockets share) put more datagrams than two drains take, none
/// of them a QUIC packet, in the server's socket for the client; poll the server as an executor
/// would, again only when it is woken. Expect: whenever a poll ends pending without having woken
/// its own task (`Driver::yields`), no socket is left holding datagrams (`Driver::backlogged`);
/// and every one is read. Neither rests on timing or on how the kernel delivers them.
#[test]
fn a_peer_socket_left_holding_datagrams_is_drained_without_another_wake() {
    if !cfg!(unix) {
        return;
    }
    let pki = Pki::new();
    let one = pki.issue(&name(1));
    let two = pki.issue(&name(2));
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async move {
        let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut server: Drv =
            Driver::bind(endpoint(&pki, &two, &one.0, CLIENT, true), loopback, ONE).unwrap();
        let server_addr = server.local_addr().unwrap();
        let mut client: Drv =
            Driver::bind(endpoint(&pki, &one, &two.0, SERVER, false), loopback, ONE).unwrap();
        let client_addr = client.local_addr().unwrap();
        let now = client.clock().unwrap();
        client.endpoint().connect(now, SERVER, server_addr).unwrap();
        let mut seen = Seen::default();
        let began = Instant::now();
        while seen.connected < 2 || server.peer_sockets() < 1 || client.peer_sockets() < 1 {
            step(&mut server, &mut client, &mut seen).await;
            assert!(
                began.elapsed() < GUARD,
                "no socket of its own for each side"
            );
        }
        let stray = hyper_rt::udp::UdpSocket::join_shared(client_addr).unwrap();
        let before = server.peer_stats().received;
        for index in 0..STRAY {
            let mut datagram = [0u8; 32];
            datagram[..8].copy_from_slice(&(index as u64).to_be_bytes());
            stray.send_to(&datagram, server_addr).unwrap();
        }
        let mut guard = pin!(hyper_rt::futures::sleep(
            u64::try_from(GUARD.as_nanos()).unwrap()
        ));
        let mut parked_holding = 0u64;
        let read = poll_fn(|context| {
            let yields = server.yields();
            loop {
                match server.poll_event(context) {
                    Poll::Ready(Ok(_)) => continue,
                    Poll::Ready(Err(error)) => panic!("{error:?}"),
                    Poll::Pending => break,
                }
            }
            let woke_itself = server.yields() > yields;
            if !woke_itself && server.backlogged() > 0 {
                parked_holding += 1;
            }
            let read = server.peer_stats().received - before;
            if read >= STRAY as u64 || parked_holding > 0 {
                return Poll::Ready(read);
            }
            assert!(
                guard.as_mut().poll(context).is_pending(),
                "{read} of {STRAY} read"
            );
            Poll::Pending
        })
        .await;
        assert_eq!(
            parked_holding, 0,
            "the driver parked with a socket holding datagrams and asked for no wake ({read} of \
             {STRAY} read)"
        );
        // The client's own datagrams still in flight when the strays went may be among those read.
        assert!(read >= STRAY as u64, "{read} of {STRAY} read");
    })
    .unwrap();
}
