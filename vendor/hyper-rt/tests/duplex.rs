//! Readiness waits on one handle in both directions, and by two waiters in one direction (mantle's review of
//! hyper-rt, finding 2). epoll keeps one registration per descriptor and its `MOD` replaced the mask and the
//! word, so a writable wait erased a readable one and the reader slept for good; kqueue's `EV_ADD` replaced a
//! second reader's word. The shard now keeps the waiters per handle and direction and arms the union.

#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc,
    clippy::cognitive_complexity
)]

use std::sync::atomic::{AtomicU32, Ordering};

use hyper_rt::readiness::{readable, writable};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::tcp::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use hyper_rt::udp::UdpSocket;

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

/// Shape: how many 1 ms sleeps a test waits for a task's wake before it fails rather than hangs.
const PATIENCE: u32 = 5_000;

async fn until(flag: &AtomicU32, value: u32) {
    for _ in 0..PATIENCE {
        if flag.load(Ordering::Acquire) >= value {
            return;
        }
        hyper_rt::futures::sleep(1_000_000).await.unwrap();
    }
    panic!("a wait never woke");
}

/// Do: one task waits to read a stream while another, its send buffer full, waits to write the same one;
/// the peer drains, then sends. Expect: the writer wakes, then the reader wakes.
#[test]
fn a_reader_and_a_writer_of_one_stream_each_wake() {
    static WROTE: AtomicU32 = AtomicU32::new(0);
    static READ: AtomicU32 = AtomicU32::new(0);
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 4).unwrap();
        let ours = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let peer = listener.accept().await.unwrap();
        // Fill our send buffer and the peer's receive buffer: the next write would block.
        let chunk = vec![7u8; 64 * 1024];
        while ours.try_write(&chunk).unwrap().is_some() {}
        let raw = ours.readiness_handle();
        hyper_rt::futures::spawn_detached(async move {
            readable(raw).await.unwrap();
            READ.store(1, Ordering::Release);
        })
        .unwrap();
        hyper_rt::futures::spawn_detached(async move {
            writable(raw).await.unwrap();
            WROTE.store(1, Ordering::Release);
        })
        .unwrap();
        // Both waits registered before the peer moves.
        hyper_rt::futures::yield_now().await;
        hyper_rt::futures::yield_now().await;
        let mut sink = vec![0u8; 256 * 1024];
        while WROTE.load(Ordering::Acquire) == 0 {
            match peer.try_read(&mut sink).unwrap() {
                Some(_) => {}
                None => hyper_rt::futures::sleep(1_000_000).await.unwrap(),
            }
        }
        until(&WROTE, 1).await;
        assert_eq!(READ.load(Ordering::Acquire), 0, "nothing to read yet");
        peer.write_all(b"news").await.unwrap();
        until(&READ, 1).await;
    })
    .unwrap();
}

/// Do: two tasks wait to read one UDP socket; one datagram arrives. Expect: both wake.
#[test]
fn two_readers_of_one_socket_both_wake() {
    static WOKEN: AtomicU32 = AtomicU32::new(0);
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
        let sender = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
        let raw = socket.readiness_handle();
        for _ in 0..2 {
            hyper_rt::futures::spawn_detached(async move {
                readable(raw).await.unwrap();
                WOKEN.fetch_add(1, Ordering::AcqRel);
            })
            .unwrap();
        }
        hyper_rt::futures::yield_now().await;
        hyper_rt::futures::yield_now().await;
        sender
            .send_to(b"one", socket.local_addr().unwrap())
            .unwrap();
        until(&WOKEN, 2).await;
    })
    .unwrap();
}
