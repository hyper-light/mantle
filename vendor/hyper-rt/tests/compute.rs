//! The compute pool (src/compute.rs, docs/runtime.md §3.16; Kiln's `tests/compute.rs`, taken here), in one
//! process because the pool is one per process: a job's result returns through a task's await and through a
//! plain thread's wait; its thread runs below the thread that started it, at the stated rank read back from the
//! OS; a job that unwinds ends without a result and the pool lives on; at its bound a job is refused and handed
//! back; a job in pieces gives its thread to a job that waits and goes on after it, and goes on where none
//! waits; a second start is refused; the pool stops, and refuses jobs after.

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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::sync_channel;

use hyper_rt::RtError;
use hyper_rt::compute::{self, Config, Priority, Step};
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

/// A piece's time here: long enough that a piece is many units of the spinning job's work.
const PIECE_NS: u64 = 2_000_000;

/// Some CPU work no compiler removes.
fn churn(seed: u64) -> u64 {
    let mut x = seed | 1;
    for _ in 0..1_000 {
        x = std::hint::black_box(
            x.wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407),
        );
    }
    x
}

/// The rank a pool thread started by a thread at `parent` takes, as the platform file states it.
#[cfg(target_os = "linux")]
fn stated_rank(parent: Priority) -> Priority {
    match parent {
        Priority::Other { nice } | Priority::Batch { nice } => Priority::Batch {
            nice: (nice + 10).min(19),
        },
        other => panic!("the test's thread at {other}"),
    }
}

/// The rank a pool thread started by a thread at `parent` takes, as the platform file states it.
#[cfg(target_os = "macos")]
fn stated_rank(parent: Priority) -> Priority {
    match parent {
        Priority::Qos { class } if class == 0 || class > compute::QOS_CLASS_UTILITY => {
            Priority::Qos {
                class: compute::QOS_CLASS_UTILITY,
            }
        }
        Priority::Qos { class } if class > compute::QOS_CLASS_BACKGROUND => Priority::Qos {
            class: compute::QOS_CLASS_BACKGROUND,
        },
        kept => kept,
    }
}

/// The rank a pool thread started by a thread at `parent` takes, as the platform file states it.
#[cfg(windows)]
fn stated_rank(parent: Priority) -> Priority {
    match parent {
        Priority::Level { level } if level <= -1 => Priority::Level { level },
        _ => Priority::Level { level: -1 },
    }
}

/// The spinning job's stop, and its units of work so far.
static STOP: AtomicBool = AtomicBool::new(false);
static UNITS: AtomicU64 = AtomicU64::new(0);

#[test]
fn the_pool_runs_lowers_refuses_yields_and_stops() {
    let started = compute::start(Config {
        workers: 1,
        queued: 4,
        piece_ns: PIECE_NS,
    })
    .unwrap();
    assert_eq!(started.workers, 1);
    assert!(
        matches!(
            compute::start(Config {
                workers: 1,
                queued: 1,
                piece_ns: 1
            }),
            Err(RtError::BadConfig { .. })
        ),
        "one pool per process"
    );
    assert!(compute::running());

    // A job's result arrives through a task's await.
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let (answer, here, there) = rt
        .block_on(async {
            let answer = compute::run(|| 6 * 7).unwrap().await.unwrap();
            // The pool's thread runs below the thread that asked (a shard's, here, at the process's priority).
            let there = compute::run(compute::thread_priority)
                .unwrap()
                .await
                .unwrap();
            (answer, compute::thread_priority(), there)
        })
        .unwrap();
    assert_eq!(answer, 42);
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    {
        assert_eq!(
            there,
            stated_rank(here),
            "the pool's thread at {there}, the shard's at {here}"
        );
        assert!(
            there.is_below(here) || there == here,
            "{there} against {here}"
        );
        assert_eq!(started.lowered, 1, "{:?}", started.priority);
        assert_eq!(started.priority, Ok(there));
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    let _ = (here, there);

    // And through a plain thread's wait.
    assert_eq!(
        compute::run(|| churn(3)).unwrap().blocking_recv().unwrap(),
        churn(3)
    );

    // A job that unwinds ends without a result; the pool lives on.
    let mut unwound = compute::run(|| -> u8 { panic!("the job's own failure") }).unwrap();
    assert!(
        unwound.blocking_recv().is_err(),
        "the receiver sees the sender gone"
    );
    assert_eq!(compute::run(|| 1u8).unwrap().blocking_recv().unwrap(), 1);

    // At the bound (four admitted, one running and three waiting) a fifth is refused, and handed back.
    let (release, hold) = sync_channel::<()>(1);
    let (holding, held_now) = sync_channel::<()>(1);
    let mut held = compute::run(move || {
        holding.send(()).unwrap();
        hold.recv().is_ok()
    })
    .unwrap();
    held_now.recv().unwrap();
    let mut waiting: Vec<_> = (0..3u64)
        .map(|i| compute::run(move || churn(i)).unwrap())
        .collect();
    let refused = compute::run(|| 5u8).unwrap_err();
    assert!(
        matches!(refused.error, RtError::Capacity { bound: 4, .. }),
        "{:?}",
        refused.error
    );
    release.send(()).unwrap();
    assert!(held.blocking_recv().unwrap());
    for (i, receiver) in waiting.iter_mut().enumerate() {
        assert_eq!(
            receiver.blocking_recv().unwrap(),
            churn(u64::try_from(i).unwrap())
        );
    }
    assert_eq!(
        compute::run(refused.job).unwrap().blocking_recv().unwrap(),
        5,
        "the job handed back runs"
    );

    // A job in pieces gives the one thread to a job that waits: a job that spins until told to stop, asking
    // its turn between units of work, and a short job submitted after it, whose result comes while the long
    // one still runs. Without pieces the short job would wait for good: the long one stops only once told,
    // after the short one's result came.
    let (spinning, spins) = sync_channel::<()>(1);
    let mut long = compute::run_pieces(Some(spinning), |spinning, turn| {
        if let Some(spinning) = spinning.take() {
            spinning.send(()).unwrap();
        }
        loop {
            if STOP.load(Ordering::Acquire) {
                return Step::Done(UNITS.load(Ordering::Relaxed));
            }
            churn(UNITS.fetch_add(1, Ordering::Relaxed));
            if turn.over() {
                return Step::More;
            }
        }
    })
    .unwrap();
    spins.recv().unwrap();
    let yielded = compute::stats().yielded;
    assert_eq!(
        compute::run(|| 9u8).unwrap().blocking_recv().unwrap(),
        9,
        "the waiting job ran beside the long one"
    );
    assert!(
        compute::stats().yielded > yielded,
        "the long job gave its thread up"
    );
    assert!(!STOP.load(Ordering::Acquire), "and went on after it");

    // Where nothing waits, the long job goes on without giving its thread up: it runs on until told to stop,
    // its pieces past their time with no job waiting.
    let yielded = compute::stats().yielded;
    let pieces = compute::stats().pieces;
    let before = UNITS.load(Ordering::Relaxed);
    while UNITS.load(Ordering::Relaxed) < before + 20_000 {
        std::hint::spin_loop();
    }
    STOP.store(true, Ordering::Release);
    assert!(long.blocking_recv().unwrap() >= before + 20_000);
    assert_eq!(
        compute::stats().yielded,
        yielded,
        "no piece ended with no job waiting"
    );
    assert!(
        compute::stats().pieces <= pieces + 2,
        "{:?}",
        compute::stats()
    );

    let stats = compute::stats();
    assert_eq!(stats.panicked, 1);
    assert_eq!(stats.refused, 1);
    assert_eq!(stats.finished, stats.accepted, "{stats:?}");
    assert!(stats.longest_piece_ns > 0);

    // The pool stops: what was admitted ran; a job after is refused, handed back.
    compute::stop().unwrap();
    assert!(!compute::running());
    assert!(matches!(compute::stop(), Err(RtError::BadConfig { .. })));
    let after = compute::run(|| ()).unwrap_err();
    assert!(matches!(after.error, RtError::BadConfig { .. }));
    assert_eq!(
        compute::run_here(0u8, |n, _| {
            *n += 1;
            Step::Done(*n)
        }),
        1,
        "the consumer's fallback"
    );
}
