//! A datagram queued behind an astray report on Windows is taken without another arriving
//! (docs/runtime.md §5.1, "Astray reports"). A send to a closed port comes back as a report
//! (`WSAECONNRESET`, the `recvfrom` reference) that makes the socket readable on its own, holds its
//! place in the receive queue, and is consumed by the one receive that returns it, which moves no
//! data. The receive future reads on past it, and so does the batched receive the transport's
//! driver drains with, in the same drain. A test binary of its own, its cases one after another,
//! for the reason `udp_astray.rs` is one: no other socket of the process may be bound while a port
//! it closed is free, or the next bind can be handed that port and take the datagram meant for no
//! one (windows-11-arm, diag run 38275004157).
#![cfg(windows)]
// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::missing_panics_doc
)]

use hyper_rt::futures::{now_ns, within, yield_now};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::udp::{Batched, Io, Ipv4Addr, Queued, Ring, Sent, SocketAddr, UdpSocket};

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

/// Shape: how long a wait that should end is given before the test fails rather than hangs.
const PATIENCE_NS: u64 = 10_000_000_000;

/// Shape: the receive calls one drain may make, hyper-rt-transport's `TURNS`.
const CALLS: usize = 128;

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

/// A port no socket holds: bound and released after every socket of the case is bound, so none of
/// them can be given it.
fn closed_port() -> SocketAddr {
    UdpSocket::bind(loopback()).unwrap().local_addr().unwrap()
}

/// Waits until `socket`'s queue holds at least `bytes` of datagrams: Winsock's `FIONREAD` counts
/// the bytes of every queued datagram, and of a report alone one.
async fn queued(socket: &UdpSocket, bytes: usize) {
    let held = within(PATIENCE_NS, async {
        while socket.queued_bytes().unwrap() < bytes {
            yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(held.is_some(), "{bytes} bytes never queued");
}

/// Waits until `socket` is readable.
async fn readable(socket: &UdpSocket, what: &str) {
    let fired = within(PATIENCE_NS, socket.readable()).await.unwrap();
    assert!(fired.is_some(), "{what}: the socket never became readable");
    fired.unwrap().unwrap();
}

/// Do: a socket sends to a closed port and waits until it is readable, nothing having been sent to
/// it: the report is queued. A second socket then sends it two datagrams, which queue behind the
/// report. Then (1) the socket's receive future, and (2) for a batched socket lent its buffers, one
/// drain as hyper-rt-transport's driver drains its listener.
/// Expect: (1) the future returns the first datagram, the report counted once; (2) the one drain
/// takes both datagrams, in order, the report counted once, without any further datagram arriving
/// and without a readiness wait. A receive that stopped at the report as at an empty queue would
/// leave them there until another datagram came.
#[test]
fn a_datagram_queued_behind_a_report_is_taken_without_another_arriving() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        // (1) The receive future.
        let socket = UdpSocket::bind(loopback()).unwrap();
        let sender = UdpSocket::bind(loopback()).unwrap();
        let closed = closed_port();
        let to = socket.local_addr().unwrap();
        socket.send_to(b"astray", closed).unwrap();
        readable(&socket, "the report").await;
        sender.send_to(b"first", to).unwrap();
        sender.send_to(b"second", to).unwrap();
        queued(&socket, b"first".len() + b"second".len()).await;
        let mut buf = [0u8; 64];
        let received = within(PATIENCE_NS, socket.recv_from(&mut buf))
            .await
            .unwrap()
            .expect("the receive future stopped at the report");
        let (length, from) = received.unwrap();
        assert_eq!(&buf[..length], b"first");
        assert_eq!(from, sender.local_addr().unwrap());
        assert_eq!(socket.astray_reports(), 1, "the report counted once");

        // (2) The driver's drain.
        let mut listener = Batched::bind_lent(loopback(), true).unwrap();
        let mut ring = Ring::new(Io { batch: 8 }).unwrap();
        let sender = UdpSocket::bind(loopback()).unwrap();
        let closed = closed_port();
        let to = listener.local_addr().unwrap();
        assert!(listener.queue(closed, b"astray"));
        assert_eq!(listener.send(), Sent::Drained);
        readable(listener.socket(), "the report").await;
        sender.send_to(b"first", to).unwrap();
        sender.send_to(b"second", to).unwrap();
        queued(listener.socket(), b"first".len() + b"second".len()).await;
        let mut taken = Vec::new();
        let (got, _ended): (_, Queued) = listener
            .receive_queued(&mut ring, now_ns(), CALLS, |_, bytes| {
                taken.push(bytes.to_vec());
            })
            .unwrap();
        assert_eq!(
            taken,
            vec![b"first".to_vec(), b"second".to_vec()],
            "the drain stopped at the report: {:?}",
            listener.stats()
        );
        assert_eq!(got.datagrams, 2);
        assert_eq!(listener.stats().astray, 1, "the report counted once");
    })
    .unwrap();
}
