//! An astray report on Windows: a datagram sent to a closed port comes back as a reset on the socket's
//! next receive (`WSAECONNRESET`, the `recvfrom` reference), counted, never an error. A test binary of its
//! own, so no other test of the process binds a socket while the port it closes is free: Windows can hand
//! the next bind the port a socket just released (windows-11-arm, 2 of 300 runs of the batched socket's
//! tests, diag run 38275004157), and a datagram meant for no one would reach that socket.
#![cfg(windows)]
// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::missing_panics_doc
)]

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::udp::{Batched, Io, Ipv4Addr, Sent, SocketAddr, UdpSocket};

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

/// Do: bind the socket, then bind a second and close it, and send from the first to the second's port;
/// then receive. Expect: the report is counted as astray and the receive takes nothing, without an
/// error. The socket is bound before the port is closed, so the port cannot be its own: bound after,
/// it was given the port just closed, and the datagram reached the test's own socket (CI run
/// 38102324750: one taken where none was sent to it).
#[test]
fn an_astray_report_is_counted_not_an_error() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let mut socket = Batched::bind(loopback, Io { batch: 4 }, false).unwrap();
        let closed = UdpSocket::bind(loopback).unwrap().local_addr().unwrap();
        assert_ne!(closed, socket.local_addr().unwrap());
        socket.queue(closed, b"astray");
        assert_eq!(socket.send(), Sent::Drained);
        socket.readable().await.unwrap();
        assert_eq!(socket.receive(|_, _| {}).unwrap(), 0);
        assert!(socket.stats().astray >= 1, "{:?}", socket.stats());
    })
    .unwrap();
}
