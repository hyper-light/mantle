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
