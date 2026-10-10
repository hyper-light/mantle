//! LocalRuntime::block_on must deliver socket readiness while another task keeps yielding.
//! The sibling yields until the root has its datagram, so the test waits on that delivery and
//! nothing else: a block_on that never retrieves readiness never returns, and the job's own limit
//! is its failure. No clock decides the outcome.

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
use std::task::Poll;

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::udp::UdpSocket;

static DELIVERED: AtomicBool = AtomicBool::new(false);
static BUSY_STARTED: AtomicBool = AtomicBool::new(false);
const PAYLOAD: &[u8] = b"ready while another task yields";

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

#[test]
#[cfg_attr(miri, ignore)] // Native socket readiness opens kqueue, epoll or AFD.
fn block_on_delivers_readiness_while_another_task_keeps_yielding() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let outcome = rt.block_on(async {
        // A duplicated descriptor observes the same kernel receive queue without consuming it.
        // Adoption is the public socket-activation API already exercised by tests/udp.rs.
        let observed = std::net::UdpSocket::bind("127.0.0.1:0")
            .map_err(|error| format!("receiver bind: {error}"))?;
        observed
            .set_nonblocking(true)
            .map_err(|error| format!("nonblocking receiver: {error}"))?;
        let socket = UdpSocket::adopt(
            observed
                .try_clone()
                .map_err(|error| format!("receiver duplicate: {error}"))?
                .into(),
        )
        .map_err(|error| format!("receiver adoption: {error}"))?;
        let peer = std::net::UdpSocket::bind("127.0.0.1:0")
            .map_err(|error| format!("peer bind: {error}"))?;
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
        // The sibling keeps local work ready until the root has its datagram.
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
        let address = observed
            .local_addr()
            .map_err(|error| format!("receiver address: {error}"))?;
        let sent = peer
            .send_to(PAYLOAD, address)
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
        // The datagram is in the kernel and only the sibling wakes itself now: the root is woken
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
        Ok::<_, String>(())
    });
    drop(rt);
    assert_eq!(
        outcome.expect("block_on completed"),
        Ok(()),
        "public socket delivery"
    );
}
