//! TCP on every OS (docs/runtime.md §5.2): IPv6 with vectored reads into the caller's buffers and
//! vectored writes; the stream options read back from the OS; the process-wide connection budget closes
//! a connection past it, counts it, and takes connections again once a slot returns; shared listeners
//! bind one port where the OS spreads connections among them.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
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

use std::io::{IoSlice, IoSliceMut};
use std::sync::mpsc::channel;

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::tcp::{
    ConnectionBudget, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpListener, TcpStream,
};

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

/// Shape: the pending connections a test listener queues; the tests make at most three.
const BACKLOG: u32 = 4;
/// Shape: how many 1 ms sleeps a test task waits for another task's answer before it fails: a step
/// takes microseconds, so a second is generous and still ends a broken run.
const ANSWER_SLEEPS: u32 = 1_000;
/// Shape: one sleep of that wait, nanoseconds.
const SLEEP_NS: u64 = 1_000_000;

/// Do: over IPv6 loopback, a client writes eight bytes from two slices in one call and reads the echo
/// into two buffers of its own in one call. Expect: the server saw the bytes in order, and the reply
/// landed split across the client's buffers.
#[test]
fn an_ipv6_stream_reads_and_writes_vectored_into_the_callers_buffers() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let listener =
            TcpListener::bind(SocketAddr::from((Ipv6Addr::LOCALHOST, 0)), BACKLOG).unwrap();
        let addr = listener.local_addr().unwrap();
        assert!(addr.is_ipv6());
        hyper_rt::futures::spawn(async move {
            let stream = listener.accept().await.unwrap();
            assert!(stream.peer_addr().unwrap().is_ipv6());
            let mut request = [0u8; 8];
            let mut got = 0;
            while got < request.len() {
                got += stream.read(&mut request[got..]).await.unwrap();
            }
            stream.write_all(&request).await.unwrap();
            stream.shutdown(Shutdown::Write).unwrap();
        })
        .unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let sent = client
            .write_vectored(&[IoSlice::new(b"abc"), IoSlice::new(b"defgh")])
            .await
            .unwrap();
        assert_eq!(sent, 8, "one call took both slices");
        let (mut head, mut tail) = ([0u8; 3], [0u8; 5]);
        let mut got = 0;
        while got < 8 {
            let n = {
                let (head_rest, tail_rest) = if got < 3 {
                    (&mut head[got..], &mut tail[..])
                } else {
                    (&mut head[3..], &mut tail[got - 3..])
                };
                let mut bufs = [IoSliceMut::new(head_rest), IoSliceMut::new(tail_rest)];
                client.read_vectored(&mut bufs).await.unwrap()
            };
            assert_ne!(n, 0, "the echo ended early");
            got += n;
        }
        assert_eq!(&head, b"abc");
        assert_eq!(&tail, b"defgh");
        let mut end = [0u8; 1];
        assert_eq!(client.read(&mut end).await.unwrap(), 0, "end of stream");
    })
    .unwrap();
}

/// Do: set the options on a connected stream and read them back from the OS. Expect: `TCP_NODELAY`
/// always; `TCP_NOTSENT_LOWAT` on Linux and macOS (macOS's value is declared in the crate, so a wrong one
/// fails here); the retransmission deadline on all three (macOS keeps whole seconds).
#[test]
fn the_stream_options_read_back() {
    /// Shape: a low-water mark, one write's worth.
    const LOWAT: u32 = 16_384;
    /// Shape: a deadline in whole seconds, so macOS's rounding keeps it exactly.
    const DEADLINE_MS: u32 = 5_000;
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let listener =
            TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), BACKLOG).unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let server = listener.accept().await.unwrap();
        for stream in [&client, &server] {
            assert!(stream.nodelay().unwrap(), "TCP_NODELAY");
            let offered = stream.set_notsent_lowat(LOWAT).unwrap();
            assert_eq!(offered, cfg!(any(target_os = "linux", target_os = "macos")));
            if offered {
                assert_eq!(stream.notsent_lowat().unwrap(), Some(LOWAT));
            }
            assert!(stream.set_user_timeout(DEADLINE_MS).unwrap());
            assert_eq!(stream.user_timeout_ms().unwrap(), Some(DEADLINE_MS));
        }
    })
    .unwrap();
}

/// Do: a budget of one; a first connection accepted, a second connected, the listener asked for the
/// next. Expect: the second is closed at once (its read ends) and counted; once the first stream drops,
/// its slot returns and a third connection is accepted under the budget.
#[test]
fn a_connection_past_the_budget_is_closed_and_counted_until_a_slot_returns() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let budget = ConnectionBudget::new(1).unwrap();
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), BACKLOG)
            .unwrap()
            .with_budget(budget.clone());
        let addr = listener.local_addr().unwrap();
        let _first_client = TcpStream::connect(addr).await.unwrap();
        let first = listener.accept().await.unwrap();
        assert_eq!(budget.open(), 1);
        let second = TcpStream::connect(addr).await.unwrap();
        let (answer, answered) = channel();
        hyper_rt::futures::spawn(async move {
            let third = listener.accept().await.map(|_| ());
            let _ = answer.send((third, listener.refused()));
        })
        .unwrap();
        let mut byte = [0u8; 1];
        let ended = second.read(&mut byte).await;
        assert!(
            matches!(ended, Ok(0) | Err(_)),
            "the refused connection ends: {ended:?}"
        );
        drop(first);
        assert_eq!(budget.open(), 0, "the slot returned with its stream");
        let _third_client = TcpStream::connect(addr).await.unwrap();
        let mut waited = 0;
        let (third, refused) = loop {
            if let Ok(answer) = answered.try_recv() {
                break answer;
            }
            assert!(
                waited < ANSWER_SLEEPS,
                "the third connection was not accepted"
            );
            waited += 1;
            hyper_rt::futures::sleep(SLEEP_NS).await.unwrap();
        };
        third.unwrap();
        assert_eq!(refused, 1, "the second connection was refused, once");
    })
    .unwrap();
}

/// Do: two shared listeners on one port, then a plain one. Expect: the shared ones both bind
/// (`SO_REUSEPORT`), and the plain one is refused the port.
#[cfg(unix)]
#[test]
fn shared_listeners_bind_one_port() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let first =
            TcpListener::bind_shared(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), BACKLOG).unwrap();
        let addr = first.local_addr().unwrap();
        let second = TcpListener::bind_shared(addr, BACKLOG);
        assert!(second.is_ok(), "a second shared listener binds: {second:?}");
        assert!(
            TcpListener::bind(addr, BACKLOG).is_err(),
            "a plain one does not"
        );
    })
    .unwrap();
}
