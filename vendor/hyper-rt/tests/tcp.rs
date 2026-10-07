//! Async TCP through the runtime's driver (§4.6, R5): a server task accepts a connection, reads a
//! request and writes a framed reply, all awaiting the shard's driver; a client task connects, sends
//! the request and reads the reply back — the accept/read/write path the NFS loopback server runs on,
//! proven end to end on the readiness-native driver (kqueue here on macOS; epoll on Linux), with no
//! foreign runtime and both ends on the runtime's own sockets.

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

// The address types come through the runtime's re-export (`core::net`'s), so the test builds on every OS.
use hyper_rt::runtime::{Runtime, RuntimeConfig};
use hyper_rt::tcp::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};

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

/// Shape: the test makes exactly one connection, so one queued pending connection suffices.
const BACKLOG: u32 = 1;

/// The bytes the client sends and the marker the server frames its echo with, so the reply proves the
/// whole accept -> read -> write -> read round trip travelled the socket.
const REQUEST: &[u8] = b"ping";
const REPLY_PREFIX: &[u8] = b"reply:";

/// A server task accepts one connection and echoes the request back with a marker; a client task
/// connects, sends the request, and reads the framed reply — every step awaiting the driver.
#[test]
fn a_tcp_request_and_reply_travel_through_the_driver() {
    let rt = Runtime::start(&config()).unwrap();
    let id = rt.shard_ids()[0];

    // The listener is bound (and listening) before the tasks spawn, so the client can connect at once.
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0), BACKLOG).unwrap();
    let addr = listener.local_addr().unwrap();
    assert_ne!(addr.port(), 0, "the OS assigned a port");

    rt.spawn_on(id, async move {
        if let Ok(stream) = listener.accept().await {
            let mut buf = [0u8; 64];
            if let Ok(n) = stream.read(&mut buf).await {
                let _ = stream.write_all(REPLY_PREFIX).await;
                let _ = stream.write_all(&buf[..n]).await;
            }
        }
    })
    .unwrap();

    let (tx, rx) = channel();
    rt.spawn_on(id, async move {
        let outcome: Result<Vec<u8>, hyper_rt::RtError> = async {
            let stream = TcpStream::connect(addr).await?;
            stream.write_all(REQUEST).await?;
            // Read until the whole framed reply has arrived (loopback may split the two writes).
            let want = REPLY_PREFIX.len() + REQUEST.len();
            let mut got = Vec::new();
            let mut buf = [0u8; 64];
            while got.len() < want {
                let n = stream.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                got.extend_from_slice(&buf[..n]);
            }
            Ok(got)
        }
        .await;
        let _ = tx.send(outcome);
    })
    .unwrap();

    // The reply is the fact waited on; a hang is the harness's to report (finding 10c).
    match rx.recv() {
        Ok(Ok(bytes)) => {
            let mut expected = REPLY_PREFIX.to_vec();
            expected.extend_from_slice(REQUEST);
            assert_eq!(
                bytes, expected,
                "the client read back the server's framed echo"
            );
        }
        Ok(Err(e)) => panic!("the round trip failed: {e:?}"),
        Err(e) => panic!("the client task ended without a reply: {e}"),
    }
    rt.shutdown().unwrap();
}

/// A listener bound, reduced to its bare descriptor, and re-adopted serves on the SAME port — the
/// descriptor handoff a supervisor uses to keep the loopback port across a daemon restart (§4.6,
/// "One TCP loopback listener held by the anchor"). Proven by accepting a connection on the adopted
/// listener at the original port, so the port is stable across the hand-off, not re-assigned.
#[test]
fn a_listener_handed_over_by_descriptor_serves_on_the_same_port() {
    let rt = Runtime::start(&config()).unwrap();
    let id = rt.shard_ids()[0];

    // Bind and note the port, then hand the listener over as a bare descriptor and re-adopt it — the
    // supervisor-binds / daemon-adopts hand-off. The port must not change.
    let bound = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0), BACKLOG).unwrap();
    let port = bound.local_addr().unwrap().port();
    assert_ne!(port, 0, "the OS assigned a port");
    let listener = TcpListener::from_owned(bound.into_owned(), BACKLOG).unwrap();
    assert_eq!(
        listener.local_addr().unwrap().port(),
        port,
        "the adopted listener keeps the original port"
    );
    let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);

    rt.spawn_on(id, async move {
        if let Ok(stream) = listener.accept().await {
            let mut buf = [0u8; 64];
            if let Ok(n) = stream.read(&mut buf).await {
                let _ = stream.write_all(REPLY_PREFIX).await;
                let _ = stream.write_all(&buf[..n]).await;
            }
        }
    })
    .unwrap();

    let (tx, rx) = channel();
    rt.spawn_on(id, async move {
        let outcome: Result<Vec<u8>, hyper_rt::RtError> = async {
            let stream = TcpStream::connect(addr).await?;
            stream.write_all(REQUEST).await?;
            let want = REPLY_PREFIX.len() + REQUEST.len();
            let mut got = Vec::new();
            let mut buf = [0u8; 64];
            while got.len() < want {
                let n = stream.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                got.extend_from_slice(&buf[..n]);
            }
            Ok(got)
        }
        .await;
        let _ = tx.send(outcome);
    })
    .unwrap();

    // The reply is the fact waited on; a hang is the harness's to report (finding 10c).
    match rx.recv() {
        Ok(Ok(bytes)) => {
            let mut expected = REPLY_PREFIX.to_vec();
            expected.extend_from_slice(REQUEST);
            assert_eq!(
                bytes, expected,
                "the client round-tripped through the re-adopted listener"
            );
        }
        Ok(Err(e)) => panic!("the round trip failed: {e:?}"),
        Err(e) => panic!("the client task ended without a reply: {e}"),
    }
    rt.shutdown().unwrap();
}

/// Shape: an idle spin window that never ends, so the shard has no reason to park during the test and a
/// park can only be the fault (mantle's final review, second pass: a finite window rested the test's
/// premise on the exchange finishing inside it, a clock assumption).
const LONG_SPIN_NS: u64 = u64::MAX;
/// Shape: how long the exchange is given before the test fails rather than hangs. It bounds only the
/// failure: under the fault the shard spins for good and never answers. A correct shard passes on its
/// state alone.
const PATIENCE: std::time::Duration = std::time::Duration::from_secs(30);

/// §4.3: a shard spinning in its idle window sees its driver's readiness, not only its rings: a request
/// arriving on a socket while the shard spins is answered at once, not after the window ends and the
/// shard parks. (A shard spins within the window its clients' activity opened, so each socket request had
/// waited out up to one window — 1.3 ms in a container — per hop.)
#[test]
fn a_spinning_shard_answers_a_socket_request_without_waiting_out_its_window() {
    let config = RuntimeConfig {
        spin_ns: LONG_SPIN_NS,
        ..config()
    };
    let rt = Runtime::start(&config).unwrap();
    let id = rt.shard_ids()[0];
    // A client's earlier request opened the shard's idle window, as the server notes one it served.
    rt.spawn_on(id, async {
        hyper_rt::registry::with_current(|ctx| ctx.note_activity());
    })
    .unwrap();
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0), BACKLOG).unwrap();
    let addr = listener.local_addr().unwrap();
    rt.spawn_on(id, async move {
        if let Ok(stream) = listener.accept().await {
            let mut buf = [0u8; 64];
            while let Ok(n) = stream.read(&mut buf).await {
                if n == 0 || stream.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        }
    })
    .unwrap();
    let (tx, rx) = channel();
    rt.spawn_on(id, async move {
        let outcome: Result<(u64, u64), hyper_rt::RtError> = async {
            let stream = TcpStream::connect(addr).await?;
            // The first exchange settles the connection; the watched one finds the server shard spinning.
            let mut buf = [0u8; 64];
            stream.write_all(REQUEST).await?;
            stream.read(&mut buf).await?;
            let waits =
                || hyper_rt::registry::with_entry(id.0, |entry| entry.pulse.waits()).unwrap_or(0);
            let before = waits();
            stream.write_all(REQUEST).await?;
            stream.read(&mut buf).await?;
            Ok((before, waits()))
        }
        .await;
        let _ = tx.send(outcome);
    })
    .unwrap();
    let (before, after) = rx
        .recv_timeout(PATIENCE)
        .expect("the exchange completed: a spinning shard must see socket readiness")
        .expect("the exchange succeeded");
    // A shard that sees the readiness while spinning enters no driver wait: the window never ends, so any
    // wait counted here is the fault's (judged by the shard's state, not by a clock).
    assert_eq!(
        after, before,
        "the shard parked in its driver during the exchange: a spinning shard must see socket readiness"
    );
    rt.shutdown().unwrap();
}

/// Shape: request/reply rounds, each answered in two writes.
const ROUNDS: usize = 32;

/// §4.6 (the NFS loopback server's sockets): a server that answers one request in two writes — as it
/// does when two replies leave in separate batches — must not hold the second write until the client
/// acknowledges the first. With Nagle's algorithm on, the second small write waits for that ACK, and a
/// client with nothing to send delays its ACK (40 ms on Linux), so each round costs a delayed-ACK timer:
/// the 40 ms tails the hot-directory storm measured through a native Linux mount. The cause is Nagle's
/// algorithm, so the test judges it, read back from the OS on both ends, not a median round against a
/// guessed bound (mantle's review, finding 10c). Do: run request/reply rounds whose reply leaves in two
/// writes. Expect: every round answered whole, and `TCP_NODELAY` set on the accepted and the connected
/// stream.
#[test]
fn a_reply_in_two_writes_does_not_wait_for_the_peers_delayed_acknowledgement() {
    let rt = Runtime::start(&config()).unwrap();
    let id = rt.shard_ids()[0];
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0), BACKLOG).unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = channel();
    rt.spawn_on(id, async move {
        if let Ok(stream) = listener.accept().await {
            let _ = accepted_tx.send(stream.nodelay());
            let mut buf = [0u8; 64];
            while let Ok(n) = stream.read(&mut buf).await {
                if n == 0
                    || stream.write_all(REPLY_PREFIX).await.is_err()
                    || stream.write_all(&buf[..n]).await.is_err()
                {
                    break;
                }
            }
        }
    })
    .unwrap();
    let (tx, rx) = channel();
    rt.spawn_on(id, async move {
        let outcome: Result<(bool, usize), hyper_rt::RtError> = async {
            let stream = TcpStream::connect(addr).await?;
            let nodelay = stream.nodelay()?;
            let want = REPLY_PREFIX.len() + REQUEST.len();
            let mut answered = 0;
            for _ in 0..ROUNDS {
                stream.write_all(REQUEST).await?;
                let mut got = 0;
                let mut buf = [0u8; 64];
                while got < want {
                    let n = stream.read(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    got += n;
                }
                if got == want {
                    answered += 1;
                }
            }
            Ok((nodelay, answered))
        }
        .await;
        let _ = tx.send(outcome);
    })
    .unwrap();
    let (connected_nodelay, answered) = rx
        .recv()
        .expect("the rounds completed")
        .expect("the rounds succeeded");
    assert_eq!(answered, ROUNDS, "every round was answered whole");
    assert!(connected_nodelay, "the connected stream is TCP_NODELAY");
    assert!(
        accepted_rx
            .recv()
            .expect("the server accepted")
            .expect("the option reads back"),
        "the accepted stream is TCP_NODELAY"
    );
    rt.shutdown().unwrap();
}
