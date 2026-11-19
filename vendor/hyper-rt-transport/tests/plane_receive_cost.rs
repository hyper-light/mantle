//! What a plane socket's receive costs: the receive calls it makes and the time a poll takes, with
//! nothing queued and with one datagram queued at a time (mantle's perf of a node under a native GET
//! load, perf-10k c1589e30: hyper-rt's Linux `receive` 9.3% of all samples, most of it for sockets
//! that held nothing, each call making a whole batch's message headers).
//!
//! A receive call is one `recvmmsg` on Linux and one `recvmsg` or `recvfrom` elsewhere, counted by
//! the socket's stats: the calls that received (`receive_calls`) and those that found nothing
//! (`receives_empty`). The time a poll takes is printed, not asserted: it is the machine's.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::future::{Future, poll_fn};
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::pin;
use std::task::Poll;

use hyper_datagram::{AdmitAll, Plane, PlaneLimits};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::udp::UdpSocket;
use hyper_rt_transport::{Io, IoStats, PlaneSocket};

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

const PLANE: PlaneLimits = PlaneLimits {
    max_peers: 4,
    epochs_per_peer: 2,
    window_limit: 1_024,
};

/// Shape: the plane socket's batch, as a node of 1,024 peers and clients makes it (mantle's
/// `bounds::io`, `MAX_BATCH`): the places a Linux receive offers the kernel.
const BATCH: usize = 1_024;
/// Shape: polls of a receive with nothing queued.
const EMPTY_POLLS: u64 = 1_000;
/// Shape: datagrams received one at a time.
const ONE_AT_A_TIME: u64 = 100;

/// The receive calls `after` made past `before`: those that received and those that found nothing.
fn calls(before: IoStats, after: IoStats) -> u64 {
    (after.receive_calls - before.receive_calls) + (after.receives_empty - before.receives_empty)
}

fn now_ns() -> u64 {
    hyper_rt::futures::now_ns()
}

/// Do: a plane socket of a batch of 1,024 is received from once (taking nothing), then its receive
/// is polled 1,000 times with nothing queued; then 100 times a datagram is sent to it and its
/// receive awaited until it takes it. Expect: the polls with nothing queued make no receive call,
/// the socket's readiness standing armed, where each poll asked the kernel before; each datagram is
/// taken with at most two calls, one where the platform tells from the call that it emptied the
/// socket (Linux), and every datagram is taken.
#[test]
fn a_receive_asks_the_kernel_only_when_the_socket_is_readable() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let mut socket = PlaneSocket::bind(loopback, Io { batch: BATCH }).unwrap();
        let mut plane = Plane::new(1, PLANE).unwrap();
        let to = socket.local_addr().unwrap();
        let sender = UdpSocket::bind(loopback).unwrap();

        // A first receive finds the socket empty, as a node's first turn does.
        assert_eq!(socket.receive_ready(&mut plane, &AdmitAll, |_, _| {}).unwrap(), 0);

        let before = socket.stats();
        let began = now_ns();
        {
            let mut receive = pin!(socket.receive(&mut plane, &AdmitAll, |_, _| {}));
            poll_fn(|context| {
                for _ in 0..EMPTY_POLLS {
                    assert!(receive.as_mut().poll(context).is_pending());
                }
                Poll::Ready(())
            })
            .await;
        }
        let empty_ns = now_ns() - began;
        let empty_calls = calls(before, socket.stats());

        let before = socket.stats();
        let began = now_ns();
        let mut taken = 0u64;
        for index in 0..ONE_AT_A_TIME {
            // A loopback send queues its datagram before it returns.
            sender.send_to(&index.to_le_bytes(), to).unwrap();
            // The plane opens nothing it holds no keys for; the datagram is taken all the same.
            socket
                .receive(&mut plane, &AdmitAll, |_, _| taken += 1)
                .await
                .unwrap();
        }
        let one_ns = now_ns() - began;
        let one_calls = calls(before, socket.stats());

        eprintln!(
            "plane receive, batch {BATCH}: {EMPTY_POLLS} polls with nothing queued made \
             {empty_calls} receive calls in {empty_ns} ns ({} ns a poll); {ONE_AT_A_TIME} datagrams \
             received one at a time made {one_calls} receive calls in {one_ns} ns ({} ns a datagram)",
            empty_ns / EMPTY_POLLS,
            one_ns / ONE_AT_A_TIME,
        );
        assert_eq!(empty_calls, 0, "a poll with nothing queued asked the kernel");
        assert_eq!(taken, ONE_AT_A_TIME, "every datagram is taken");
        let most = if cfg!(target_os = "linux") { 1 } else { 2 };
        assert!(
            one_calls <= most * ONE_AT_A_TIME,
            "{one_calls} receive calls for {ONE_AT_A_TIME} datagrams"
        );
    })
    .unwrap();
}
