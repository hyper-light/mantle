//! Local stream sockets (docs/runtime.md §5.3): a server and a client on one shard over an `AF_UNIX`
//! stream, each seeing the other's identity as the kernel reports it — this process and this user — and a
//! bind over an existing path refused.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::path::PathBuf;

use hyper_rt::local::{LocalListener, LocalStream, current_user};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::tcp::Shutdown;

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

/// A socket path of this test's own, short enough for `sun_path` (104 bytes on macOS).
fn path(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("hrt-{}-{name}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

/// Do: listen, connect, exchange bytes both ways. Expect: each end's peer is this process and this user,
/// the bytes arrive, and a second bind of the same path is refused.
#[test]
fn a_local_stream_carries_bytes_and_the_kernels_identity_of_its_peer() {
    let path = path("echo");
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let server_path = path.clone();
    rt.block_on(async move {
        let listener = LocalListener::bind(&server_path, 4).unwrap();
        assert!(
            LocalListener::bind(&server_path, 4).is_err(),
            "an existing path is not bound over"
        );
        let me = current_user().unwrap();
        let (seen, saw) = std::sync::mpsc::channel();
        hyper_rt::futures::spawn_detached(async move {
            let stream = listener.accept().await.unwrap();
            let _ = seen.send(stream.peer().unwrap());
            let mut request = [0u8; 4];
            let mut got = 0;
            while got < request.len() {
                got += stream.read(&mut request[got..]).await.unwrap();
            }
            stream.write_all(&request).await.unwrap();
            stream.shutdown(Shutdown::Write).unwrap();
        })
        .unwrap();
        let client = LocalStream::connect(&server_path).await.unwrap();
        let server_side = client.peer().unwrap();
        assert_eq!(
            server_side.pid,
            Some(std::process::id()),
            "the server is this process"
        );
        assert_eq!(server_side.user, me);
        client.write_all(b"ping").await.unwrap();
        let mut reply = [0u8; 4];
        let mut got = 0;
        while got < reply.len() {
            got += client.read(&mut reply[got..]).await.unwrap();
        }
        assert_eq!(&reply, b"ping");
        let client_side = saw.try_recv().expect("the server saw its peer");
        assert_eq!(
            client_side.pid,
            Some(std::process::id()),
            "the client is this process"
        );
        assert_eq!(client_side.user, me);
    })
    .unwrap();
    let _ = std::fs::remove_file(&path);
}

/// A future whose polls that found it pending are counted into `pending`.
struct Counted<'a, F> {
    future: std::pin::Pin<Box<F>>,
    pending: &'a std::cell::Cell<u32>,
}

impl<F: std::future::Future> std::future::Future for Counted<'_, F> {
    type Output = F::Output;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<F::Output> {
        let polled = self.future.as_mut().poll(context);
        if polled.is_pending() {
            self.pending.set(self.pending.get() + 1);
        }
        polled
    }
}

fn counted<F: std::future::Future>(future: F, pending: &std::cell::Cell<u32>) -> Counted<'_, F> {
    Counted {
        future: Box::pin(future),
        pending,
    }
}

/// The byte at `at` of the stream: its offset modulo 251, a prime, so no buffer's boundary lines up
/// with the pattern's.
fn pattern(at: usize) -> u8 {
    u8::try_from(at % 251).unwrap()
}

/// The bytes the client sends: more than any OS's local stream buffers hold, so its writes wait for
/// the server to read.
const STREAMED: usize = 16 << 20;

/// Do: await an accept before any client connects, then stream 16 MiB from the client while the
/// server reads it. Expect: the accept and the server's reads each waited on the driver's readiness
/// at least once, and every byte arrived in order; on Linux and macOS the client's writes waited too,
/// their kernels holding a local stream's unread bytes to its buffers. On Windows this is AFD's
/// `IOCTL_AFD_POLL` on `AF_UNIX` sockets (docs/runtime.md §15 item 1): accept and receive readiness
/// reported, or the shard would park forever. Windows took the 16 MiB a write at a time without a
/// write ever refused (both runners, CI run 39015895080), so no send readiness was asked there.
#[test]
fn a_local_streams_accept_reads_and_writes_each_wait_on_readiness() {
    let path = path("waits");
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let server_path = path.clone();
    let (writes_seen, writes) = std::sync::mpsc::channel();
    let (accepts, reads) = rt
        .block_on(async move {
            let (accepts, reads) = (std::cell::Cell::new(0), std::cell::Cell::new(0));
            let listener = LocalListener::bind(&server_path, 4).unwrap();
            let client_path = server_path.clone();
            // The client runs only once the server's accept has found nothing pending and yielded.
            hyper_rt::futures::spawn_detached(async move {
                let client = LocalStream::connect(&client_path).await.unwrap();
                let bytes: Vec<u8> = (0..STREAMED).map(pattern).collect();
                let pending = std::cell::Cell::new(0);
                counted(client.write_all(&bytes), &pending).await.unwrap();
                client.shutdown(Shutdown::Write).unwrap();
                let _ = writes_seen.send(pending.get());
            })
            .unwrap();
            let stream = counted(listener.accept(), &accepts).await.unwrap();
            let mut buffer = vec![0u8; 64 << 10];
            let mut got = 0usize;
            loop {
                let read = counted(stream.read(&mut buffer), &reads).await.unwrap();
                if read == 0 {
                    break;
                }
                for (at, byte) in buffer[..read].iter().enumerate() {
                    assert_eq!(*byte, pattern(got + at), "byte {} out of order", got + at);
                }
                got += read;
            }
            assert_eq!(got, STREAMED, "every byte arrived");
            (accepts.get(), reads.get())
        })
        .unwrap();
    let writes = writes.try_recv().expect("the client finished");
    assert!(accepts >= 1, "the accept waited for the connect");
    assert!(reads >= 1, "the server's reads waited for bytes");
    if cfg!(not(windows)) {
        assert!(
            writes >= 1,
            "the client's writes waited for the server to read"
        );
    }
    let _ = std::fs::remove_file(&path);
}
