//! No compute job holds a shard (docs/runtime.md §3.16), in a process of its own because the pool is one per
//! process: the pool sized beside a runtime from the machine's grant ([`Config::for_runtime`]) is saturated with
//! jobs that spin until the shard says stop, and the shard's loop keeps turning all the while; every job ran on
//! a pool thread at the stated rank below the shard, read back from the OS.
//!
//! The jobs end only once a task on the shard has turned its loop a counted number of times after every pool
//! thread took a job: were any job to hold the shard, that task could never run and the test would never end.

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

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use hyper_rt::compute::{self, Config, Priority, Step};
use hyper_rt::futures::yield_now;
use hyper_rt::machine::facts::Facts;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 250_000,
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

/// The shard's loop turns this many times while every pool thread is busy.
const TURNS: usize = 1_000;

/// Jobs that have taken a pool thread, and the shard's word to stop.
static BUSY: AtomicUsize = AtomicUsize::new(0);
static STOP: AtomicBool = AtomicBool::new(false);
/// The shard's turns while the pool was saturated, and the jobs finished by the last of them.
static TURNED: AtomicUsize = AtomicUsize::new(0);
static FINISHED_BEFORE_STOP: AtomicU64 = AtomicU64::new(u64::MAX);

#[test]
fn a_saturated_pool_holds_no_shard_and_runs_below_it() {
    let runtime = config();
    let pool = Config::for_runtime(&runtime, Facts::query().cpus_at_once()).unwrap();
    assert_eq!(pool.piece_ns, runtime.step_budget_ns);
    assert_eq!(pool.queued, 64);
    let started = compute::start(pool.clone()).unwrap();
    let workers = started.workers;
    assert_eq!(workers, pool.workers);
    // More jobs than threads, so some always wait and every piece past its time gives way.
    let jobs = workers + 2;

    let mut rt = LocalRuntime::new(&runtime).unwrap();
    rt.spawn(async move {
        // Every pool thread holds a job: the shard waits for that by turning its loop.
        while BUSY.load(Ordering::Acquire) < workers {
            yield_now().await;
        }
        for _ in 0..TURNS {
            yield_now().await;
            TURNED.fetch_add(1, Ordering::Relaxed);
        }
        FINISHED_BEFORE_STOP.store(compute::stats().finished, Ordering::Relaxed);
        STOP.store(true, Ordering::Release);
    })
    .unwrap();
    let (here, ranks) = rt
        .block_on(async move {
            let receivers: Vec<_> = (0..jobs)
                .map(|_| {
                    compute::run_pieces(false, |counted, turn| {
                        if !*counted {
                            *counted = true;
                            BUSY.fetch_add(1, Ordering::AcqRel);
                        }
                        loop {
                            if STOP.load(Ordering::Acquire) {
                                return Step::Done(compute::thread_priority());
                            }
                            std::hint::black_box(0u64);
                            if turn.over() {
                                return Step::More;
                            }
                        }
                    })
                    .unwrap()
                })
                .collect();
            let mut ranks = Vec::new();
            for receiver in receivers {
                ranks.push(receiver.await.unwrap());
            }
            (compute::thread_priority(), ranks)
        })
        .unwrap();
    assert_eq!(TURNED.load(Ordering::Relaxed), TURNS);
    assert_eq!(
        FINISHED_BEFORE_STOP.load(Ordering::Relaxed),
        0,
        "no job ended while the shard turned: the pool stayed saturated"
    );
    let stats = compute::stats();
    assert_eq!(stats.finished, u64::try_from(jobs).unwrap());
    assert!(
        stats.yielded > 0,
        "pieces gave way to the jobs waiting: {stats:?}"
    );
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    {
        assert_eq!(started.lowered, workers, "{:?}", started.priority);
        for rank in ranks {
            assert!(
                rank.is_below(here) || rank_kept(here, rank),
                "a pool thread at {rank}, the shard at {here}"
            );
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    let _ = (here, ranks);
    compute::stop().unwrap();
}

/// Where the shard's thread already runs at the pool's rank or below it (a test run under `nice` or at
/// background QoS), the pool's threads keep that rank rather than rise to the shard's.
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
fn rank_kept(here: Priority, rank: Priority) -> bool {
    match (here, rank) {
        (Priority::Qos { class }, Priority::Qos { class: kept }) => {
            class != 0 && class <= compute::QOS_CLASS_BACKGROUND && kept == class
        }
        (Priority::Level { level }, Priority::Level { level: kept }) => {
            level <= -1 && kept == level
        }
        _ => false,
    }
}
