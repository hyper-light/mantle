//! Accepting on every shard (docs/runtime.md §5.2): `tcp::serve` serves an address on a runtime's
//! shards, every connection under the process-wide budget and counted on the shard that serves it, the
//! counts returning to zero as the connections close; connections land on more than one shard.

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

use std::collections::BTreeMap;

use hyper_rt::runtime::{LocalRuntime, Runtime, RuntimeConfig};
use hyper_rt::tcp::{ConnectionBudget, Ipv4Addr, Shutdown, SocketAddr, TcpStream, serve};

fn config(shards: u16) -> RuntimeConfig {
    RuntimeConfig {
        shards,
        tasks_per_shard: 128,
        timers_per_shard: 64,
        interests_per_shard: 256,
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

/// Shape: the shards serving.
const SHARDS: u16 = 4;
/// Shape: the connections the clients make, enough that a spread over the shards shows (all on one of
/// four by chance: 4 × 4^-32).
const CONNECTIONS: usize = 32;
/// Shape: each listener's accept queue: every connection may be pending at once.
const BACKLOG: u32 = 64;
/// Shape: how many 1 ms sleeps the test waits for the counts to return to zero after the clients close.
const SETTLE_SLEEPS: u32 = 1_000;
/// Shape: one sleep, nanoseconds.
const SLEEP_NS: u64 = 1_000_000;
/// Shape: how long a client waits for its answer before the test fails rather than hangs: a step takes
/// microseconds.
const ANSWER_NS: u64 = 5_000_000_000;

/// Do: serve on four shards a handler that answers each request with the shard serving it; 32 clients
/// connect and ask, all open at once, then close. Expect: every client answered; more than one shard
/// served; every connection counted while open (the counts total 32) and the counts back to zero after.
#[test]
fn every_shard_serves_and_counts_its_connections() {
    let rt = Runtime::start(&config(SHARDS)).unwrap();
    let budget = ConnectionBudget::new(u64::try_from(CONNECTIONS).unwrap()).unwrap();
    let serving = serve(
        &rt,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        BACKLOG,
        budget.clone(),
        |stream: TcpStream| async move {
            let mut request = [0u8; 1];
            if stream.read(&mut request).await.unwrap_or(0) == 1 {
                let shard = hyper_rt::registry::with_current(|context| context.id).unwrap();
                let _ = stream.write_all(&shard.to_le_bytes()).await;
            }
            let mut end = [0u8; 1];
            let _ = stream.read(&mut end).await;
        },
    )
    .unwrap();
    let addr = serving.local_addr();
    assert_ne!(addr.port(), 0);

    let mut clients = LocalRuntime::new(&config(1)).unwrap();
    let served = clients
        .block_on(async move {
            let mut streams = Vec::new();
            for _ in 0..CONNECTIONS {
                streams.push(TcpStream::connect(addr).await.unwrap());
            }
            let mut by_shard = BTreeMap::<u16, usize>::new();
            for stream in &streams {
                stream.write_all(b"?").await.unwrap();
                let mut answer = [0u8; 2];
                let mut got = 0;
                while got < answer.len() {
                    let n = hyper_rt::futures::within(ANSWER_NS, stream.read(&mut answer[got..]))
                        .await
                        .unwrap()
                        .expect("answered within the bound")
                        .unwrap();
                    assert_ne!(n, 0, "answered before the end");
                    got += n;
                }
                *by_shard.entry(u16::from_le_bytes(answer)).or_default() += 1;
            }
            (streams, by_shard)
        })
        .unwrap();
    let (streams, by_shard) = served;
    let open: u64 = serving.open().iter().map(|(_, open)| open).sum();
    assert_eq!(open, CONNECTIONS as u64, "every open connection is counted");
    assert_eq!(budget.open(), CONNECTIONS as u64);
    assert!(
        by_shard.len() > 1,
        "connections landed on more than one shard: {by_shard:?}"
    );
    for stream in &streams {
        stream.shutdown(Shutdown::Write).unwrap();
    }
    drop(streams);
    let settled = clients
        .block_on(async move {
            for _ in 0..SETTLE_SLEEPS {
                if budget.open() == 0 {
                    return true;
                }
                hyper_rt::futures::sleep(SLEEP_NS).await.unwrap();
            }
            false
        })
        .unwrap();
    assert!(settled, "every slot came back as its connection closed");
    assert!(serving.open().iter().all(|(_, open)| *open == 0));
    assert!(serving.ended().is_empty(), "no accept loop stopped");
    rt.shutdown().unwrap();
}
