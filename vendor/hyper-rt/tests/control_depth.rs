//! A shard's control channel holds its admission limit (`control.rs`; docs/runtime.md §4.3): a burst of
//! spawns from another thread that the shard's arena could admit is queued whole, however long the shard
//! takes to drain it, and only a spawn past the limit is refused `ControlFull`. The channel was bounded at
//! `ring_entries` instead, which a configuration from the machine's calibration derived from the wake probe
//! (`next_pow2(wake p99 / syscall median)`): four in a one-CPU Linux container, where a benchmark's nine
//! spawns met `ControlFull` at the fifth because the shard's thread had not run between them
//! (`benchmark-results/hyper-rt-vs-tokio-20261010/runs-linux/c1-quiet-bd7c0fd/fanin/r3-hyper.txt`).

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::time::Duration;

use hyper_rt::futures::now_ns;
use hyper_rt::runtime::{Runtime, RuntimeConfig};
use hyper_rt::{Admission, RtError};

/// Shape: the shard's admission limit, sixteen times its inbound ring.
const TASKS: usize = 64;
/// Shape: the inbound ring the configuration names: four, as the wake probe derived it on the host that
/// found this.
const RING: usize = 4;
/// Shape: how long a receipt or a task's report is waited for before the test calls it lost.
const WAIT: Duration = Duration::from_secs(10);

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: TASKS,
        timers_per_shard: 1,
        interests_per_shard: 1,
        ring_entries: RING,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: RING,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

/// Do: hold the shard inside one poll (as a host with one CPU holds it while the sender runs), send a burst
/// of the admission limit's spawns from this thread, then one more, and release the shard. Expect: the
/// burst is queued whole, the one past the limit is refused `ControlFull`, and every task of the burst is
/// admitted and runs once the shard drains.
#[test]
fn a_held_shard_queues_a_burst_of_its_admission_limit_and_refuses_past_it() {
    let rt = Runtime::start(&config()).unwrap();
    let shard = rt.shard_ids()[0];
    // Leaked: the spinning task and the test share it, and the task may outlive the test's frame.
    let released: &'static AtomicBool = Box::leak(Box::new(AtomicBool::new(false)));
    let bound = u64::try_from(WAIT.as_nanos()).unwrap();
    let hold = rt
        .spawn_on_with_receipt(shard, async move {
            let end = now_ns().saturating_add(bound);
            while !released.load(Ordering::Acquire) && now_ns() < end {
                std::hint::spin_loop();
            }
        })
        .unwrap();
    assert!(
        matches!(hold.wait(WAIT), Some(Admission::Admitted(_))),
        "the hold is admitted"
    );
    let (ran, reports) = channel();
    for index in 0..TASKS {
        let ran = ran.clone();
        if let Err(refusal) = rt.spawn_on(shard, async move {
            let _ = ran.send(index);
        }) {
            panic!("spawn {index} of a burst of {TASKS} was refused: {refusal}");
        }
    }
    assert_eq!(
        rt.spawn_on(shard, async {}),
        Err(RtError::ControlFull { shard: shard.0 }),
        "a spawn past the admission limit is refused"
    );
    drop(ran);
    released.store(true, Ordering::Release);
    let mut seen = Vec::new();
    while let Ok(index) = reports.recv_timeout(WAIT) {
        seen.push(index);
    }
    assert_eq!(
        seen,
        (0..TASKS).collect::<Vec<_>>(),
        "every task of the burst ran, in order"
    );
    let counters = rt.shutdown().unwrap();
    assert_eq!(counters[0].admission_refused, 0, "{:?}", counters[0]);
}
