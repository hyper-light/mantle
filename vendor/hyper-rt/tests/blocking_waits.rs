//! A share at its running limit queues the jobs past it, in order, up to its waiting bound
//! (docs/runtime.md §9), in a process of its own because the pool is one per process. Before the
//! bound a job is never refused for its share being busy: it waits for one of the share's places
//! and runs as one frees, while another share's job runs beside them. Past the bound it is refused
//! `Capacity`, before any thread is asked.

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

use std::sync::mpsc::sync_channel;

use hyper_rt::RtError;
use hyper_rt::blocking::{self, Config, Share};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

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

#[test]
fn a_busy_share_queues_its_jobs_in_order_up_to_its_waiting_bound() {
    blocking::start(Config {
        workers: 2,
        shares: vec![
            Share {
                name: "files",
                limit: 1,
                waiting: 2,
            },
            Share {
                name: "other",
                limit: 1,
                waiting: 0,
            },
        ],
    })
    .unwrap();
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        // One job holds the share's one place; the order the share's jobs ran in is recorded.
        let (order_tx, order) = sync_channel::<u8>(4);
        let (release_a, hold_a) = sync_channel::<()>(1);
        let ran_a = order_tx.clone();
        let a = blocking::run("files", move || {
            hold_a.recv().unwrap();
            ran_a.send(1).unwrap();
        })
        .unwrap();
        // Two more wait for it, in order, rather than being refused.
        let (release_b, hold_b) = sync_channel::<()>(1);
        let ran_b = order_tx.clone();
        let b = blocking::run("files", move || {
            hold_b.recv().unwrap();
            ran_b.send(2).unwrap();
        })
        .unwrap();
        let ran_c = order_tx.clone();
        let c = blocking::run("files", move || ran_c.send(3).unwrap()).unwrap();
        // A fourth is past the waiting bound: refused before any thread is asked.
        assert!(matches!(
            blocking::run("files", || ()),
            Err(RtError::Capacity { .. })
        ));
        // Another share's job runs beside them on the other worker, while the share's jobs wait.
        assert_eq!(blocking::run("other", || 7u8).unwrap().await.unwrap(), 7);
        // As each place frees, the next job in line takes it.
        release_a.send(()).unwrap();
        a.await.unwrap();
        release_b.send(()).unwrap();
        b.await.unwrap();
        c.await.unwrap();
        drop(order_tx);
        let ran: Vec<u8> = order.iter().collect();
        assert_eq!(
            ran,
            vec![1, 2, 3],
            "the share's jobs ran in the order given"
        );
        // Once the line has emptied, the share takes jobs again.
        assert_eq!(blocking::run("files", || 9u8).unwrap().await.unwrap(), 9);
    })
    .unwrap();
    let stats = blocking::stats();
    assert_eq!(stats.refused, 1);
    blocking::stop().unwrap();
}
