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

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::Poll;

use hyper_rt::RtError;
use hyper_rt::combine::{Either, Either3, TaskSet, join2, join3, race2, race3};
use hyper_rt::futures::{sleep, within, yield_now};
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
        latency_objective_ns: None,
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

/// A future holding 16 KiB across its wait, as a request's state does.
fn heavy() -> impl Future<Output = u8> {
    let state = [7u8; 16 * 1024];
    async move {
        yield_now().await;
        state.iter().fold(0u8, |sum, byte| sum.wrapping_add(*byte))
    }
}

/// A combinator holds each future it is given once, beside a few words of its own. Written as `async fn`s
/// they held each twice (an `async fn` keeps its arguments beside the locals they move into,
/// rust-lang/rust#62958), so nesting multiplied: focal's five writes and a transfer, joined in threes, took
/// 492 KB of a 2 MiB debug test thread, and every timed call of focal's four times its own size.
#[test]
fn a_combinator_holds_each_future_once() {
    let one = size_of_val(&heavy());
    let own = 256;
    assert!(one >= 16 * 1024, "{one}");
    let sizes = [
        (size_of_val(&race2(heavy(), heavy())), 2),
        (size_of_val(&race3(heavy(), heavy(), heavy())), 3),
        (size_of_val(&join2(heavy(), heavy())), 2),
        (size_of_val(&join3(heavy(), heavy(), heavy())), 3),
        (size_of_val(&within(LONG_NS, heavy())), 1),
        (
            size_of_val(&join3(
                join3(heavy(), heavy(), heavy()),
                join2(heavy(), within(LONG_NS, heavy())),
                race2(heavy(), heavy()),
            )),
            7,
        ),
    ];
    for (at, (size, futures)) in sizes.into_iter().enumerate() {
        assert!(
            size <= futures * one + own * futures,
            "combinator {at}: {size} bytes for {futures} futures of {one}"
        );
    }
}

/// A join drops each future once it has finished, not when the last does: what a finished future still
/// holds (a timer, a reservation) is let go as soon as it is done with.
#[test]
fn a_join_lets_each_future_go_as_it_finishes() {
    static DROPPED: AtomicU32 = AtomicU32::new(0);
    /// A future that holds what it was given until it is dropped, as a hand-written one may.
    struct Holding;
    impl Drop for Holding {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
    impl Future for Holding {
        type Output = u32;
        fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<u32> {
            Poll::Ready(1)
        }
    }
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let slow = async {
            sleep(SHORT_NS).await.unwrap();
            DROPPED.load(Ordering::Relaxed)
        };
        // The finished future is gone before the slow one reads the count.
        assert_eq!(join2(Holding, slow).await, (1, 1));
    })
    .unwrap();
}

/// Polled again once it has given its output, a combinator waits rather than polling a finished future,
/// which an `async` block refuses by unwinding.
#[test]
fn a_finished_combinator_polled_again_waits() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let mut race = pin!(race2(async { 1 }, std::future::pending::<u8>()));
        let mut join = pin!(join2(async { 1 }, async { 2 }));
        let mut timed = pin!(within(LONG_NS, async { 3 }));
        std::future::poll_fn(|cx| {
            assert_eq!(race.as_mut().poll(cx), Poll::Ready(Either::First(1)));
            assert_eq!(join.as_mut().poll(cx), Poll::Ready((1, 2)));
            assert!(matches!(timed.as_mut().poll(cx), Poll::Ready(Ok(Some(3)))));
            assert_eq!(race.as_mut().poll(cx), Poll::Pending);
            assert_eq!(join.as_mut().poll(cx), Poll::Pending);
            assert!(timed.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    })
    .unwrap();
}
