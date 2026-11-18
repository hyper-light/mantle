//! What held a shard (`hyper_rt::slow`): each long poll, one that held its shard past twice the step quantum
//! since it was last let go on (docs/runtime.md §3.11), is counted against its task's spawn site and label, with
//! its hold and the attribution's verdict; a poll within the threshold is not; and the table stays its fixed
//! size when more pairs of site and label run long than it holds, counting the rest unkeyed and losing none.
//! A hold here is a spin on the wall clock, so it holds its shard that long whether the host preempts it or not.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cast_possible_truncation
)]

use std::panic::Location;
use std::time::{Duration, Instant};

use hyper_rt::futures;
use hyper_rt::registry;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::slow::{self, ENTRIES, Label, SlowPoll};
use hyper_rt::{RtError, TaskId};

/// Shape: a step quantum of a millisecond, far past any wake, so only a poll's own hold passes twice it.
const QUANTUM_NS: u64 = 1_000_000;
/// Shape: a step quantum of a second: twice it is far past any hold below, or any the host makes.
const LONG_QUANTUM_NS: u64 = 1_000_000_000;
/// Shape: a hold of three quanta of [`QUANTUM_NS`]: past twice the quantum, and a 330th of [`LONG_QUANTUM_NS`].
const HOLD: Duration = Duration::from_millis(3);
/// Shape: the quick polls a task takes; any count above one tells a task's polls from a poll.
const QUICK_POLLS: u32 = 100;

static GET: Label = Label::new("GetObject");

fn config(quantum_ns: u64, tasks: usize) -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: tasks,
        timers_per_shard: 16,
        interests_per_shard: 16,
        ring_entries: 16,
        step_budget_ns: quantum_ns,
        timer_tick_ns: 100_000,
        batch: 16,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// Holds the thread `span` by the wall clock.
fn hold(span: Duration) {
    let began = Instant::now();
    while began.elapsed() < span {
        std::hint::spin_loop();
    }
}

/// Spawns `future` on the current shard from the caller's site, and says that site: the table's key for
/// its polls.
#[track_caller]
fn spawn_here(
    future: impl std::future::Future<Output = ()> + 'static,
) -> (TaskId, &'static Location<'static>) {
    (futures::spawn(future).unwrap(), Location::caller())
}

/// The shard's table, read as any thread reads it.
fn table(rt: &LocalRuntime) -> Vec<SlowPoll> {
    let holder = registry::holder_of(rt.shard_id().0).unwrap();
    slow::slow_polls(holder)
}

#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue, an eventfd or a port, which Miri does not model
fn an_overrun_is_counted_against_its_spawn_site_and_label() {
    let mut rt = LocalRuntime::new(&config(QUANTUM_NS, 16)).unwrap();
    let site = rt
        .block_on(async {
            let (task, site) = spawn_here(async {
                futures::label(Some(&GET)).unwrap();
                hold(HOLD);
            });
            futures::join(task).await.unwrap();
            site
        })
        .unwrap();
    let read = table(&rt);
    let held: Vec<&SlowPoll> = read.iter().filter(|p| p.site == Some(site)).collect();
    assert_eq!(held.len(), 1, "one entry for the task's site: {read:?}");
    let p = held[0];
    assert_eq!(p.label.map(|l| l.name), Some(GET.name), "{p:?}");
    assert!(p.label.is_some_and(|l| std::ptr::eq(l, &GET)), "{p:?}");
    assert_eq!(p.polls, 1, "its one poll: {p:?}");
    let hold_ns = u64::try_from(HOLD.as_nanos()).unwrap();
    assert!(p.held_ns >= hold_ns && p.longest_ns == p.held_ns, "{p:?}");
    assert!(p.task_polls + p.host_polls <= p.polls, "{p:?}");
    // The table counts every long poll the shard counted, by whoever's it was.
    let c = rt.counters();
    let long = c.long_steps + c.preempted_steps + c.unattributed_steps;
    assert_eq!(
        read.iter().map(|p| p.polls).sum::<u64>(),
        long,
        "{c:?} {read:?}"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue, an eventfd or a port, which Miri does not model
fn a_poll_within_twice_the_quantum_is_not_counted() {
    // The same hold that passes twice a millisecond's quantum, under a second's, beside a task's quick polls.
    let mut rt = LocalRuntime::new(&config(LONG_QUANTUM_NS, 16)).unwrap();
    let (held_at, quick_at) = rt
        .block_on(async {
            let (held, held_at) = spawn_here(async {
                futures::label(Some(&GET)).unwrap();
                hold(HOLD);
            });
            let (quick, quick_at) = spawn_here(async {
                for _ in 0..QUICK_POLLS {
                    futures::yield_now().await;
                }
            });
            futures::join(held).await.unwrap();
            futures::join(quick).await.unwrap();
            (held_at, quick_at)
        })
        .unwrap();
    let read = table(&rt);
    assert!(
        rt.counters().polls > u64::from(QUICK_POLLS),
        "{:?}",
        rt.counters()
    );
    assert!(
        read.iter()
            .all(|p| p.site != Some(held_at) && p.site != Some(quick_at)),
        "neither task held its shard past twice a second's quantum: {read:?}"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue, an eventfd or a port, which Miri does not model
fn the_table_stays_bounded_when_more_pairs_overrun_than_it_holds() {
    let extra: u64 = 3;
    let pairs = ENTRIES + extra as usize;
    // A label of its own for each task: one site, more pairs of site and label than the table holds.
    let labels: Vec<&'static Label> = (0..pairs)
        .map(|_| &*Box::leak(Box::new(Label::new("an operation"))))
        .collect();
    let mut rt = LocalRuntime::new(&config(QUANTUM_NS, 16)).unwrap();
    rt.block_on(async move {
        for label in labels {
            let (task, _) = spawn_here(async move {
                futures::label(Some(label)).unwrap();
                hold(HOLD);
            });
            futures::join(task).await.unwrap();
        }
    })
    .unwrap();
    let read = table(&rt);
    assert_eq!(
        read.len(),
        ENTRIES + 1,
        "every entry, and the unkeyed one: {read:?}"
    );
    let rest: Vec<&SlowPoll> = read
        .iter()
        .filter(|p| p.site.is_none() && p.label.is_none())
        .collect();
    assert_eq!(rest.len(), 1, "{read:?}");
    // The pairs past the table, and any of the root's polls the host held past the threshold after it filled.
    assert!(rest[0].polls >= extra, "{read:?}");
    let c = rt.counters();
    let long = c.long_steps + c.preempted_steps + c.unattributed_steps;
    assert_eq!(
        read.iter().map(|p| p.polls).sum::<u64>(),
        long,
        "no poll lost: {c:?}"
    );
}

#[test]
fn a_label_off_a_shard_is_refused() {
    assert!(matches!(
        futures::label(Some(&GET)),
        Err(RtError::NotOnShardThread)
    ));
}
