//! Both busy and idle run_until_idle paths must deliver registered native readiness.
//! Exact datagram presence, not a delay or yield count, establishes that the I/O can complete.
//! The busy case's sibling yields until the root has its datagram, so that case waits on the
//! delivery and nothing else: a run_until_idle that never retrieves readiness never returns, and
//! the job's own limit is its failure. No clock decides any outcome here.

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

/// The busy case's facts: its root has the datagram, and its sibling has started. Its own
/// statics, so the cases run in parallel with no lock.
static DELIVERED: AtomicBool = AtomicBool::new(false);
static BUSY_STARTED: AtomicBool = AtomicBool::new(false);
const PAYLOAD: &[u8] = b"owned loop readiness";

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

async fn receive(busy: bool) -> Result<(), String> {
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
    if busy {
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
    // Kernel data is now present and the root no longer wakes itself: it is woken by the driver's
    // readiness or not at all.
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
    if busy {
        DELIVERED.store(true, Ordering::Release);
    }
    Ok(())
}

fn run_until_idle(busy: bool) {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let (done, received) = channel();
    rt.spawn(async move {
        let _ = done.send(receive(busy).await);
    })
    .unwrap();
    rt.run_until_idle();
    let outcome = received.try_recv();
    drop(rt);
    assert_eq!(
        outcome,
        Ok(Ok(())),
        "run_until_idle must deliver the already-present datagram before it returns"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Native socket readiness opens kqueue, epoll or AFD.
fn run_until_idle_delivers_readiness_while_another_task_keeps_yielding() {
    run_until_idle(true);
}

#[test]
#[cfg_attr(miri, ignore)] // Native socket readiness opens kqueue, epoll or AFD.
fn run_until_idle_does_not_skip_a_ready_socket_when_no_task_is_runnable() {
    run_until_idle(false);
}

#[test]
#[cfg_attr(miri, ignore)] // Native socket readiness opens kqueue, epoll or AFD.
fn idle_external_waits_return_intact_and_complete_on_a_later_run() {
    let observed = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    observed.set_nonblocking(true).unwrap();
    let address = observed.local_addr().unwrap();
    let owned = observed.try_clone().unwrap().into();
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    peer.set_nonblocking(true).unwrap();
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let (armed, pending) = channel();
    let (done, received) = channel();
    rt.spawn(async move {
        let outcome = async {
            let socket =
                UdpSocket::adopt(owned).map_err(|error| format!("receiver adoption: {error}"))?;
            let mut readiness = pin!(socket.readable());
            poll_fn(|cx| {
                Poll::Ready(match readiness.as_mut().poll(cx) {
                    Poll::Pending => Ok(()),
                    Poll::Ready(result) => Err(format!("empty socket must first wait: {result:?}")),
                })
            })
            .await?;
            let _ = armed.send(());
            readiness
                .await
                .map_err(|error| format!("readiness: {error}"))?;
            let mut bytes = [0; PAYLOAD.len()];
            let delivered = socket
                .try_recv_from(&mut bytes)
                .map_err(|error| format!("ready receive: {error}"))?
                .ok_or("ready socket had no datagram")?;
            if bytes.get(..delivered.0) != Some(PAYLOAD) {
                return Err("the readiness delivery changed the datagram".to_owned());
            }
            Ok::<_, String>(())
        }
        .await;
        let _ = done.send(outcome);
    })
    .unwrap();
    let (sender, mut receiver) = hyper_rt::sync::channel::<u32>(1).unwrap();
    let (channel_done, channel_received) = channel();
    rt.spawn(async move {
        let _ = channel_done.send(receiver.recv().await);
    })
    .unwrap();
    // Nothing is ready: the call returns with both waits still registered. One that parked in the
    // driver for them instead never returns.
    rt.run_until_idle();
    let first = (
        pending.try_recv(),
        received.try_recv(),
        channel_received.try_recv(),
    );
    if first != (Ok(()), Err(TryRecvError::Empty), Err(TryRecvError::Empty)) {
        drop(rt);
        panic!("idle external waits must remain pending without blocking: {first:?}");
    }
    assert_eq!(peer.send_to(PAYLOAD, address).unwrap(), PAYLOAD.len());
    let mut peeked = [0; PAYLOAD.len()];
    loop {
        match observed.peek_from(&mut peeked) {
            Ok((n, _)) if peeked.get(..n) == Some(PAYLOAD) => break,
            Ok(other) => panic!("loopback peek changed the datagram: {other:?}"),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::yield_now();
            }
            Err(error) => panic!("loopback peek: {error}"),
        }
    }
    sender.try_send(9).unwrap();
    rt.run_until_idle();
    let outcomes = (received.try_recv(), channel_received.try_recv());
    drop(rt);
    assert_eq!(outcomes, (Ok(Ok(())), Ok(Ok(9))));
}
