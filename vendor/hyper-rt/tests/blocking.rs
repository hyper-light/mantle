//! The blocking pool and name resolution (docs/runtime.md §9, §5.4), in one process because the pool is
//! one per process: jobs return through a task's await; a share at its limit refuses; a job that panics
//! ends the job, not its worker; a second start is refused; `localhost` resolves to a loopback address
//! through the platform resolver; the pool stops and joins its threads.

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

use std::sync::mpsc::{SyncSender, sync_channel};

use hyper_rt::blocking::{self, Config, Share};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::{RtError, dns};

/// A job's result that, when dropped, submits a job under the share `order` and reports whether it was
/// accepted.
struct SubmitsOnDrop {
    seen: SyncSender<bool>,
}

impl Drop for SubmitsOnDrop {
    fn drop(&mut self) {
        let accepted = blocking::run("order", || ()).is_ok();
        self.seen.send(accepted).unwrap();
    }
}

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
fn the_pool_runs_refuses_survives_and_resolves() {
    blocking::start(Config {
        workers: 2,
        queue: 8,
        shares: vec![
            Share {
                name: "work",
                limit: 2,
            },
            Share {
                name: dns::SHARE,
                limit: 1,
            },
            Share {
                name: "order",
                limit: 1,
            },
        ],
    })
    .unwrap();
    assert!(
        matches!(
            blocking::start(Config {
                workers: 1,
                queue: 1,
                shares: vec![Share {
                    name: "x",
                    limit: 1
                }],
            }),
            Err(RtError::BadConfig { .. })
        ),
        "one pool per process"
    );
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        // A job's result arrives through the await.
        let value = blocking::run("work", || 6 * 7).unwrap().await.unwrap();
        assert_eq!(value, 42);

        // Two jobs held open fill the share; a third is refused before any thread is asked.
        let (release_a, hold_a) = sync_channel::<()>(1);
        let (release_b, hold_b) = sync_channel::<()>(1);
        let a = blocking::run("work", move || hold_a.recv().is_ok()).unwrap();
        let b = blocking::run("work", move || hold_b.recv().is_ok()).unwrap();
        assert!(matches!(
            blocking::run("work", || ()),
            Err(RtError::Capacity { .. })
        ));
        assert!(matches!(
            blocking::run("nothing", || ()),
            Err(RtError::BadConfig { .. })
        ));
        release_a.send(()).unwrap();
        release_b.send(()).unwrap();
        assert!(a.await.unwrap() && b.await.unwrap());

        // A job that panics ends without a result; the pool and the share live on.
        let panicked = blocking::run("work", || -> u8 { panic!("the job's own failure") }).unwrap();
        assert!(panicked.await.is_err(), "the receiver sees the sender gone");
        assert_eq!(blocking::run("work", || 1u8).unwrap().await.unwrap(), 1);

        // The share is free before the result is: a result discarded because its receiver is gone is
        // dropped on the worker, and its drop submits under the same full share. Were the slot given back
        // after the result, the drop would be refused, every time.
        let (release, hold) = sync_channel::<()>(1);
        let (seen_tx, seen) = sync_channel::<bool>(1);
        let job = blocking::run("order", move || {
            hold.recv().unwrap();
            SubmitsOnDrop { seen: seen_tx }
        })
        .unwrap();
        drop(job);
        release.send(()).unwrap();
        assert!(
            seen.recv().unwrap(),
            "the result's drop found its share still held"
        );

        // Resolution through the platform resolver, on the pool.
        let addrs = dns::resolve("localhost", 443, 8).await.unwrap();
        assert!(!addrs.is_empty(), "localhost resolves");
        assert!(
            addrs
                .iter()
                .all(|addr| addr.ip().is_loopback() && addr.port() == 443)
        );
        assert!(dns::resolve("no-such-host.invalid", 443, 8).await.is_err());
    })
    .unwrap();
    let stats = blocking::stats();
    assert_eq!(stats.panicked, 1);
    assert!(stats.refused >= 1);
    blocking::stop().unwrap();
    assert!(
        blocking::run("work", || ()).is_err(),
        "no jobs after the stop"
    );
}
