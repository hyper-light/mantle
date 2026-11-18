//! Classes of tasks on a shard (Kiln's `kiln-rt` `tests/classes.rs`, its goal 6, plan 01a1a59c2abe step 3;
//! `hyper_rt::coop::Class`): each class with tasks waiting gets an equal share of the shard's time, and the most
//! urgent goes first. A Critical task woken while Normal and Background tasks keep the shard busy runs next, not
//! behind them; no class is starved, a busy Critical one included; a poll is charged to its class at most the
//! watchdog's limit; and a shard whose tasks never take a class runs as it did.
// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation,
    clippy::indexing_slicing,
    clippy::cognitive_complexity
)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use hyper_rt::coop::{Class, set_class};
use hyper_rt::futures::{now_ns, sleep, spawn, spawn_detached_as, yield_now};
use hyper_rt::machine::clock::monotonic_ns;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

/// Shape: a fixed quantum, long against a loaded host's noise and short enough for many in a test.
const QUANTUM_NS: u64 = 1_000_000;

/// Shape: busy tasks of each class that works: a round of two classes' is eight quanta, far past one.
const BUSY: usize = 4;

/// Shape: how long the shard is kept busy, in quanta.
const QUANTA: u64 = 60;

/// `value` for the rest of the test, shared by reference with the tasks that need it.
fn shared<T>(value: T) -> &'static T {
    Box::leak(Box::new(value))
}

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 32,
        timers_per_shard: 32,
        interests_per_shard: 64,
        ring_entries: 32,
        step_budget_ns: QUANTUM_NS,
        timer_tick_ns: QUANTUM_NS / 20,
        batch: 32,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// Works a quantum at a time until `done`, yielding between, counting the quanta in `worked`.
async fn busy(done: &'static AtomicBool, worked: &'static AtomicU64) {
    while !done.load(Ordering::Acquire) {
        let until = monotonic_ns() + QUANTUM_NS;
        while monotonic_ns() < until {
            std::hint::spin_loop();
        }
        worked.fetch_add(1, Ordering::Relaxed);
        yield_now().await;
    }
}

/// Runs a sleeper of `class` beside busy Normal and Background tasks: how late it ran after each timer, sorted,
/// and the quanta the Normal and Background tasks worked.
fn sleeper_beside_busy_work(class: Class) -> (Vec<u64>, u64, u64) {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let done = shared(AtomicBool::new(false));
    let (normal, background) = (shared(AtomicU64::new(0)), shared(AtomicU64::new(0)));
    let mut late = rt
        .block_on(async move {
            for _ in 0..BUSY {
                spawn_detached_as(Class::Normal, busy(done, normal)).unwrap();
                spawn_detached_as(Class::Background, busy(done, background)).unwrap();
            }
            set_class(class).unwrap();
            let mut late = Vec::new();
            let end = monotonic_ns() + QUANTA * QUANTUM_NS;
            while monotonic_ns() < end {
                let due = now_ns() + QUANTUM_NS / 4;
                sleep(QUANTUM_NS / 4).await.unwrap();
                late.push(now_ns().saturating_sub(due));
            }
            done.store(true, Ordering::Release);
            late
        })
        .unwrap();
    late.sort_unstable();
    (
        late,
        normal.load(Ordering::Relaxed),
        background.load(Ordering::Relaxed),
    )
}

/// A woken Critical task goes ahead of the Normal and Background work that keeps its shard busy. Do: four Normal
/// and four Background tasks each work a quantum at a time; a sleeper wakes every quarter quantum. Expect: as
/// Critical, it runs within about the quantum then running at the median (a loaded host's preemptions move the
/// most, not the median); as Normal, it waits its turn behind the round of busy tasks of its share, several
/// quanta. Both lower classes keep working: neither is starved.
#[test]
fn s5a_a_woken_critical_task_goes_ahead_of_busy_normal_and_background_work() {
    let (late, normal, background) = sleeper_beside_busy_work(Class::Critical);
    let median = late[late.len() / 2];
    eprintln!(
        "critical sleeper: {} wakes, late {median} ns at the median, {} at most; normal worked {normal} quanta, \
         background {background}",
        late.len(),
        late[late.len() - 1]
    );
    assert!(
        median <= 2 * QUANTUM_NS,
        "a Critical task runs within the poll running as it woke: {median} ns at the median"
    );
    assert!(
        normal > 0 && background > 0,
        "neither lower class starved: {normal}, {background}"
    );

    let (late, _, _) = sleeper_beside_busy_work(Class::Normal);
    let normal_median = late[late.len() / 2];
    eprintln!("normal sleeper: late {normal_median} ns at the median");
    assert!(
        normal_median > median,
        "as Normal it waits behind its class's busy tasks: {normal_median} ns against {median} ns as Critical"
    );
}

/// Each class with work gets an equal share: Normal and Background tasks busy side by side work about alike,
/// where a Background task alone would have worked the whole time. Equal within a factor of two either way,
/// against a loaded host's noise.
#[test]
fn s5a_busy_classes_share_the_shard_alike() {
    let (_, normal, background) = sleeper_beside_busy_work(Class::Normal);
    eprintln!("normal worked {normal} quanta, background {background}");
    assert!(
        background * 2 >= normal && normal * 2 >= background,
        "equal shares: normal {normal}, background {background}"
    );
}

/// A busy Critical class starves no other: four Critical tasks working a quantum at a time beside four Normal and
/// four Background ones leave each lower class its share, within a factor of two of the Critical class's.
#[test]
fn s5a_a_busy_critical_class_starves_no_other() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let done = shared(AtomicBool::new(false));
    let worked: &'static [AtomicU64; 3] =
        shared([AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)]);
    rt.block_on(async move {
        for _ in 0..BUSY {
            for (at, class) in [
                (0, Class::Critical),
                (1, Class::Normal),
                (2, Class::Background),
            ] {
                spawn_detached_as(class, busy(done, &worked[at])).unwrap();
            }
        }
        sleep(QUANTA * QUANTUM_NS).await.unwrap();
        done.store(true, Ordering::Release);
    })
    .unwrap();
    let [critical, normal, background] = [0, 1, 2].map(|at| worked[at].load(Ordering::Relaxed));
    eprintln!("critical worked {critical} quanta, normal {normal}, background {background}");
    for (name, other) in [("normal", normal), ("background", background)] {
        assert!(
            other * 2 >= critical && critical * 2 >= other,
            "equal shares: critical {critical}, {name} {other}"
        );
    }
}

/// A shard whose tasks never take a class keeps one list, in arrival order: a round of yields goes in the order
/// the tasks were spawned, as before classes.
#[test]
fn s5a_without_classes_the_order_is_arrival() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let seen: &'static Vec<AtomicU64> = shared((0..12).map(|_| AtomicU64::new(u64::MAX)).collect());
    let next = shared(AtomicU64::new(0));
    rt.block_on(async move {
        for task in 0..4u64 {
            spawn(async move {
                for _ in 0..3 {
                    let at = next.fetch_add(1, Ordering::Relaxed) as usize;
                    seen[at].store(task, Ordering::Relaxed);
                    yield_now().await;
                }
            })
            .unwrap();
        }
        for _ in 0..8 {
            yield_now().await;
        }
    })
    .unwrap();
    let order: Vec<u64> = seen
        .iter()
        .map(|task| task.load(Ordering::Relaxed))
        .collect();
    assert_eq!(order, vec![0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3]);
}

/// A poll is charged to its class at most the watchdog's limit (`batch` polls each held at most twice the
/// quantum, `hyper_rt::watch`): past it the poll is a stall, reported as one, and charged in full it would leave
/// its class owing all of it, its other tasks waiting as long again while the other classes caught up. Do:
/// Background tasks keep the shard busy; a Normal task holds it in one poll for ten limits, then sleeps a quarter
/// quantum. Expect: it runs again within two limits of its timer (a limit of catch-up, where the whole wedge would
/// be ten).
#[test]
fn s5a_a_stall_is_charged_to_its_class_at_most_the_watchdogs_limit() {
    let limit_ns = 2 * QUANTUM_NS * config().batch as u64;
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let done = shared(AtomicBool::new(false));
    let background = shared(AtomicU64::new(0));
    let late = rt
        .block_on(async move {
            for _ in 0..BUSY {
                spawn_detached_as(Class::Background, busy(done, background)).unwrap();
            }
            // Their first polls, which list their class.
            yield_now().await;
            let until = monotonic_ns() + 10 * limit_ns;
            while monotonic_ns() < until {
                std::hint::spin_loop();
            }
            let due = now_ns() + QUANTUM_NS / 4;
            sleep(QUANTUM_NS / 4).await.unwrap();
            let late = now_ns().saturating_sub(due);
            done.store(true, Ordering::Release);
            late
        })
        .unwrap();
    eprintln!(
        "after a wedge of ten limits: late {late} ns; background worked {} quanta",
        background.load(Ordering::Relaxed)
    );
    assert!(
        late <= 2 * limit_ns,
        "late {late} ns against a limit of {limit_ns} ns"
    );
}
