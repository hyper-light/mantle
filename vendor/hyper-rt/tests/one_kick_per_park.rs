//! A parked shard is kicked once per park, however many wakes reach it while it sleeps (docs/runtime.md
//! §3.5): the first sender to find the park announced claims its kick, and every later sender finds it
//! claimed and skips the system call, its wake already in the bitmap the woken shard drains. Until
//! 2026-10-10 every sender that read the announcement kicked, so a fan-in of clients onto one parked shard
//! paid a `kevent`/`eventfd` write each: 3.2 to 4.5 kicks per park in mantle's fan-in and blocking-pool
//! panels (`benchmark-results/hyper-rt-vs-tokio-20261010/runs/base-f5d66a8-r1`), where tokio's parker pays
//! one (`scheduler/multi_thread/park.rs`, its `NOTIFIED` state).
//!
//! The shard parks in a test driver whose wait holds until the test releases it, so every wake below lands
//! while the shard is asleep: the count is exact, no interleaving decides it.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
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

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::{Receiver, Sender, SyncSender, channel, sync_channel};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use hyper_rt::interests::Readiness;
use hyper_rt::registry;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

/// Shape: the wakes that reach one parked shard, each for its own task: more than one, so a second
/// sender exists to find the kick claimed.
const WAKERS: usize = 4;
/// Shape: the parks the test drives, so each park is shown to take its own kick.
const PARKS: usize = 2;

/// A driver whose blocking wait says it has entered, then holds until the test releases it; a
/// zero-timeout harvest returns at once. It holds nothing else.
struct HeldDriver {
    epoch: Instant,
    entered: SyncSender<()>,
    release: Receiver<()>,
}

impl Driver for HeldDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::Kqueue
    }

    fn kick_handle(&self) -> Kick {
        Kick::None
    }

    fn now_ns(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn wait(&mut self, timeout: Option<u64>, _out: &mut Vec<Completion>) -> Result<(), RtError> {
        if timeout == Some(0) {
            return Ok(());
        }
        self.entered.send(()).map_err(|_| RtError::DriverLost)?;
        self.release.recv().map_err(|_| RtError::DriverLost)
    }

    fn submit_nop(&mut self, _user_data: u64) -> Result<(), RtError> {
        Ok(())
    }

    fn arm(&mut self, _raw: i32, _want: Readiness, _tag: u64) -> Result<(), RtError> {
        Ok(())
    }

    fn has_pending(&self) -> bool {
        false
    }
}

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 4,
        interests_per_shard: 4,
        ring_entries: 16,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: 16,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

/// A future pending until its waker has been handed over and the future is polled again.
struct PendOnce {
    wakers: Sender<Waker>,
    polled: bool,
}

impl Future for PendOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.polled {
            return Poll::Ready(());
        }
        self.polled = true;
        self.wakers.send(cx.waker().clone()).unwrap();
        Poll::Pending
    }
}

/// Do: park a shard with `WAKERS` tasks waiting, and while it sleeps wake every one of them from other
/// threads; release it, run the tasks; twice. Expect: every task woken ran (no wake lost) and each park
/// took exactly one kick — the other senders skipped theirs.
#[test]
fn a_parked_shard_takes_one_kick_however_many_wakes_reach_it() {
    let (entered_tx, mut entered) = sync_channel::<()>(1);
    let (release, release_rx) = channel::<()>();
    let seed: DriverSeed = Box::new(move |_kick| {
        Ok(Box::new(HeldDriver {
            epoch: Instant::now(),
            entered: entered_tx,
            release: release_rx,
        }) as Box<dyn Driver>)
    });
    let mut rt = LocalRuntime::with_driver(&config(), seed, Kick::None).unwrap();
    let shard = rt.shard_id().0;
    let skipped = || registry::with_entry(shard, |entry| entry.parking.kicks_skipped()).unwrap();
    let (done_tx, done) = channel::<usize>();
    for _ in 0..PARKS {
        let (tx, wakers) = channel::<Waker>();
        for index in 0..WAKERS {
            let wakers = tx.clone();
            let done_tx = done_tx.clone();
            rt.spawn(async move {
                PendOnce {
                    wakers,
                    polled: false,
                }
                .await;
                done_tx.send(index).unwrap();
            })
            .unwrap();
        }
        while rt.step().did_work {}
        let held: Vec<Waker> = wakers.try_iter().collect();
        assert_eq!(held.len(), WAKERS, "every task handed over its waker");
        let before = skipped();
        let entered = &mut entered;
        let release = &release;
        std::thread::scope(|scope| {
            scope.spawn(move || {
                // The shard is inside its driver's wait, announced parked, until released below.
                entered.recv().unwrap();
                std::thread::scope(|wakes| {
                    for waker in &held {
                        wakes.spawn(move || waker.wake_by_ref());
                    }
                });
                release.send(()).unwrap();
            });
            rt.park(None);
        });
        while rt.step().did_work {}
        let ran: Vec<usize> = done.try_iter().collect();
        assert_eq!(ran.len(), WAKERS, "every woken task ran: {ran:?}");
        let kicks = WAKERS as u64 - (skipped() - before);
        assert_eq!(
            kicks, 1,
            "{WAKERS} wakes reached one parked shard and it took {kicks} kicks; one ends a park"
        );
    }
}
