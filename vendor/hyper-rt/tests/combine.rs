//! Combining futures within a task: a race takes the first to finish, in the order given when several are
//! ready, and drops the rest; a join waits for all; a task set hands back its tasks as they end, refuses past
//! its bound, and reports cancelled ones.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::sync::atomic::{AtomicU32, Ordering};

use hyper_rt::RtError;
use hyper_rt::combine::{Either, Either3, TaskSet, join2, join3, race2, race3};
use hyper_rt::futures::sleep;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::task::Outcome;

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

/// Shape: a short and a long sleep, far enough apart that the order is never in doubt.
const SHORT_NS: u64 = 1_000_000;
const LONG_NS: u64 = 1_000_000_000;

#[test]
fn a_race_takes_the_first_and_drops_the_rest() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let won = race2(sleep(LONG_NS), async {
            sleep(SHORT_NS).await.unwrap();
            7
        })
        .await;
        assert_eq!(won, Either::Second(7));
        // Both ready at once: the earlier wins (biased, so a simulation replays).
        assert_eq!(race2(async { 1 }, async { 2 }).await, Either::First(1));
        let third = race3(sleep(LONG_NS), sleep(LONG_NS), async { "now" }).await;
        assert!(matches!(third, Either3::Third("now")));
        assert_eq!(join2(async { 1 }, async { 2 }).await, (1, 2));
        let (a, b, c) = join3(
            async {
                sleep(SHORT_NS).await.unwrap();
                1
            },
            async { 2 },
            async { 3 },
        )
        .await;
        assert_eq!((a, b, c), (1, 2, 3));
    })
    .unwrap();
}

#[test]
fn a_task_set_hands_back_its_tasks_as_they_end() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        static ENDED: AtomicU32 = AtomicU32::new(0);
        let mut set = TaskSet::new(3).unwrap();
        for wait in [3 * SHORT_NS, SHORT_NS, 2 * SHORT_NS] {
            set.spawn(async move {
                sleep(wait).await.unwrap();
                ENDED.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
        }
        assert!(matches!(set.spawn(async {}), Err(RtError::Capacity { .. })));
        let mut seen = 0;
        while let Some((_, outcome)) = set.join_next().await {
            assert_eq!(outcome.unwrap(), Outcome::Completed);
            seen += 1;
        }
        assert_eq!((seen, ENDED.load(Ordering::Relaxed)), (3, 3));
        assert!(set.is_empty());

        let mut set = TaskSet::new(2).unwrap();
        set.spawn(async {
            sleep(LONG_NS).await.unwrap();
        })
        .unwrap();
        set.cancel_all();
        let (_, outcome) = set.join_next().await.unwrap();
        assert_eq!(outcome.unwrap(), Outcome::Cancelled);
    })
    .unwrap();
}
