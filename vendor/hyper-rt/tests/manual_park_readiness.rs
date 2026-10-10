//! A skipped public park must not postpone nonblocking readiness retrieval forever.
//! The caller drives LocalRuntime::step and park; the sibling leaves local work ready, so each park
//! returns without waiting in the driver. The sibling yields until the datagram is delivered, so
//! the test waits on that delivery and nothing else: a loop whose skipped parks keep renewing the
//! retrieval's age never ends, and the job's own limit is its failure. No clock decides the outcome.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{TryRecvError, channel};
use std::task::Poll;

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::udp::UdpSocket;

const PAYLOAD: &[u8] = b"manual step delivers ready data";
static DELIVERED: AtomicBool = AtomicBool::new(false);
static BUSY_STARTED: AtomicBool = AtomicBool::new(false);

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

async fn receive() -> Result<(), String> {
    let observed = std::net::UdpSocket::bind("127.0.0.1:0")
        .map_err(|error| format!("receiver bind: {error}"))?;
    observed
        .set_nonblocking(true)
        .map_err(|error| format!("nonblocking receiver: {error}"))?;
    // Both descriptors observe the same receive queue; peek does not consume the datagram.
    let socket = UdpSocket::adopt(
        observed
            .try_clone()
            .map_err(|error| format!("receiver duplicate: {error}"))?
            .into(),
    )
    .map_err(|error| format!("receiver adoption: {error}"))?;
    let peer =
        std::net::UdpSocket::bind("127.0.0.1:0").map_err(|error| format!("peer bind: {error}"))?;
    peer.set_nonblocking(true)
        .map_err(|error| format!("nonblocking peer: {error}"))?;
    let mut readiness = pin!(socket.readable());
    poll_fn(|cx| {
        Poll::Ready(match readiness.as_mut().poll(cx) {
            Poll::Pending => Ok(()),
            Poll::Ready(result) => Err(format!("empty socket must first wait: {result:?}")),
        })
    })
    .await?;
    // The sibling keeps local work ready until this task has its datagram.
    hyper_rt::futures::spawn_detached(async {
        BUSY_STARTED.store(true, Ordering::Release);
        while !DELIVERED.load(Ordering::Acquire) {
            hyper_rt::futures::yield_now().await;
        }
    })
    .map_err(|error| format!("busy task admission: {error}"))?;
    while !BUSY_STARTED.load(Ordering::Acquire) {
        hyper_rt::futures::yield_now().await;
    }
    let sent = peer
        .send_to(
            PAYLOAD,
            observed
                .local_addr()
                .map_err(|error| format!("receiver address: {error}"))?,
        )
        .map_err(|error| format!("loopback send: {error}"))?;
    if sent != PAYLOAD.len() {
        return Err("loopback send did not accept the complete datagram".to_owned());
    }
    let mut peeked = [0; PAYLOAD.len()];
    loop {
        match observed.peek_from(&mut peeked) {
            Ok((n, _)) if peeked.get(..n) == Some(PAYLOAD) => break,
            Ok(other) => return Err(format!("loopback peek changed the datagram: {other:?}")),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                hyper_rt::futures::yield_now().await;
            }
            Err(error) => return Err(format!("loopback peek: {error}")),
        }
    }
    // The datagram is in the kernel and only the sibling wakes itself now: this waiter is woken
    // by the driver's readiness or not at all.
    readiness
        .await
        .map_err(|error| format!("readiness: {error}"))?;
    let mut received = [0; PAYLOAD.len()];
    let delivered = socket
        .try_recv_from(&mut received)
        .map_err(|error| format!("ready receive: {error}"))?
        .ok_or("ready socket had no datagram")?;
    if received.get(..delivered.0) != Some(PAYLOAD) {
        return Err("the readiness delivery changed the datagram".to_owned());
    }
    DELIVERED.store(true, Ordering::Release);
    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)] // Native socket readiness opens kqueue, epoll or AFD.
fn a_park_skipped_for_ready_local_work_does_not_reset_the_readiness_age() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let (completed, received) = channel();
    rt.spawn(async move {
        let _ = completed.send(receive().await);
    })
    .expect("socket task admission");
    let outcome = loop {
        let _ = rt.step();
        match received.try_recv() {
            Ok(result) => break result,
            Err(TryRecvError::Disconnected) => break Err("socket task disappeared".to_owned()),
            Err(TryRecvError::Empty) => {}
        }
        // The sibling leaves local work ready: this park returns without a driver wait, and must
        // not count as a retrieval. Completion is checked first, since a finished root and child
        // may leave nothing pending, when an unbounded park would be valid.
        rt.park(None);
    };
    drop(rt);
    assert_eq!(outcome, Ok(()), "public socket delivery");
}
