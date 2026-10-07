//! AUD-29-08 (§4.3, R2, D-8): a shard's context and the values it keeps are reached only through a borrow
//! that proves them alive. A kept value is named by a [`Kept`] handle and lent inside a closure while its own
//! context runs on the calling thread; the thread's current shard is published only while its owner steps
//! it. Until 2026-09-30 the runtime lent `&'static` references to both, which safe code could hold past the
//! owner's drop that freed them. These run on the simulated driver, so Miri runs them too (the owner
//! teardown with held handles, stale wakers and kept values the audit asks it to see).

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::string_slice,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn,
    clippy::missing_panics_doc
)]
// Test harness code: an unwrap here is a failed test, which is what it should be.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::channel;

use hyper_rt::registry::with_current;
use hyper_rt::runtime::RuntimeConfig;
use hyper_rt::shard::Kept;
use hyper_rt::sim::SimRuntime;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 64,
        ring_entries: 16,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 16,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

/// A kept value that counts its own drop, so a test sees it dropped exactly once, with its context.
struct Counted {
    word: u64,
    drops: &'static AtomicU64,
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// Keeps a [`Counted`] on the shard of a fresh simulated runtime (its owner keeps it, between runs) and
/// returns the runtime and the handle.
fn runtime_keeping(word: u64, drops: &'static AtomicU64) -> (SimRuntime, Kept<Counted>) {
    let mut sim = SimRuntime::new(&config(), 1).unwrap();
    let shard = sim.shard_ids()[0];
    let kept = sim.keep(shard, Counted { word, drops }).unwrap();
    sim.run_until_idle();
    (sim, kept)
}

/// Resolves `kept` from a task of `sim`'s shard: what the value's word reads there.
fn resolve_in_task(sim: &mut SimRuntime, kept: Kept<Counted>) -> Option<u64> {
    let shard = sim.shard_ids()[0];
    let (tx, rx) = channel();
    sim.spawn_on(shard, async move {
        let _ = tx.send(kept.with(|counted| counted.word));
    })
    .unwrap();
    sim.run_until_idle();
    rx.recv().unwrap()
}

/// AUD-29-08: do: keep a value from a task, hold its handle, drop the runtime; expect the handle to resolve
/// in a task while the runtime lives, the value dropped exactly once with the runtime, and the same handle
/// to answer `None` afterwards — from this thread and from another — never a read of the freed value.
#[test]
fn a_kept_handle_held_past_its_runtime_answers_none() {
    static DROPS: AtomicU64 = AtomicU64::new(0);
    let (mut sim, kept) = runtime_keeping(7, &DROPS);
    assert_eq!(resolve_in_task(&mut sim, kept), Some(7));
    assert_eq!(
        kept.with(|counted| counted.word),
        None,
        "no shard runs between steps"
    );
    assert_eq!(DROPS.load(Ordering::SeqCst), 0);
    drop(sim);
    assert_eq!(
        DROPS.load(Ordering::SeqCst),
        1,
        "the value dropped with its context"
    );
    assert_eq!(kept.with(|counted| counted.word), None);
    let elsewhere = std::thread::spawn(move || kept.with(|counted| counted.word))
        .join()
        .unwrap();
    assert_eq!(elsewhere, None);
}

/// AUD-29-08: do: keep a value on one runtime, then resolve its handle in a task of another runtime — one
/// alive beside it, and one built after the first dropped (which takes the freed registry slot); expect
/// `None` both times: a handle names its context's registration, never a slot or a thread.
#[test]
fn a_kept_handle_resolves_only_on_its_own_context() {
    static FIRST: AtomicU64 = AtomicU64::new(0);
    static SECOND: AtomicU64 = AtomicU64::new(0);
    static THIRD: AtomicU64 = AtomicU64::new(0);
    let (first, kept) = runtime_keeping(11, &FIRST);
    let (mut beside, _) = runtime_keeping(12, &SECOND);
    assert_eq!(resolve_in_task(&mut beside, kept), None);
    drop(first);
    let (mut after, _) = runtime_keeping(13, &THIRD);
    assert_eq!(resolve_in_task(&mut after, kept), None);
    assert_eq!(FIRST.load(Ordering::SeqCst), 1);
}

/// AUD-29-08: do: step a runtime's task, then ask for the current shard outside any step; expect `None`.
/// Before 2026-09-30 a bare step published its context and left it published after it returned, so the
/// thread's next lend could name a context whose owner had since dropped.
#[test]
fn the_current_shard_is_published_only_while_its_owner_steps() {
    let mut sim = SimRuntime::new(&config(), 5).unwrap();
    let shard = sim.shard_ids()[0];
    let (tx, rx) = channel();
    sim.spawn_on(shard, async move {
        let _ = tx.send(with_current(|context| context.id));
    })
    .unwrap();
    sim.run_until_idle();
    assert_eq!(rx.recv().unwrap(), Some(shard.0), "inside a step");
    let _ = sim.step(shard).unwrap();
    assert_eq!(
        with_current(|context| context.id),
        None,
        "after a bare step"
    );
}

/// docs/runtime.md §3.4: kept values are kept by the shard's owner between runs and immutable while it runs,
/// so a task reaches every one of them by shared lend, one inside another, with nothing to refuse; and each
/// is dropped exactly once, with its shard.
#[test]
fn kept_values_are_lent_together_and_dropped_once_with_their_shard() {
    static DROPS: AtomicU64 = AtomicU64::new(0);
    let mut sim = SimRuntime::new(&config(), 9).unwrap();
    let shard = sim.shard_ids()[0];
    let outer = sim.keep(shard, 1_u64).unwrap();
    let counted = sim
        .keep(
            shard,
            Counted {
                word: 4,
                drops: &DROPS,
            },
        )
        .unwrap();
    let (tx, rx) = channel();
    sim.spawn_on(shard, async move {
        let nested = outer
            .with(|first| counted.with(|second| (*first, second.word)))
            .flatten();
        let _ = tx.send(nested);
    })
    .unwrap();
    sim.run_until_idle();
    assert_eq!(rx.recv().unwrap(), Some((1, 4)));
    assert_eq!(
        DROPS.load(Ordering::SeqCst),
        0,
        "kept while the shard lives"
    );
    drop(sim);
    assert_eq!(
        DROPS.load(Ordering::SeqCst),
        1,
        "dropped once, with its shard"
    );
}

/// AUD-29-08: do: inside a task of one runtime, build and run another to idle on the same thread; expect
/// the inner runtime's task to see its own shard as current, and the outer task to see its shard again once
/// the inner run returned (a nested span restores the enclosing one).
#[test]
fn a_nested_runtime_restores_the_enclosing_shard() {
    let mut outer = SimRuntime::new(&config(), 21).unwrap();
    let outer_shard = outer.shard_ids()[0];
    let (tx, rx) = channel();
    outer
        .spawn_on(outer_shard, async move {
            let mut inner = SimRuntime::new(&config(), 22).unwrap();
            let inner_shard = inner.shard_ids()[0];
            let (inner_tx, inner_rx) = channel();
            inner
                .spawn_on(inner_shard, async move {
                    let _ = inner_tx.send(with_current(|context| context.id));
                })
                .unwrap();
            inner.run_until_idle();
            let seen_inside = inner_rx.recv().unwrap();
            let _ = tx.send((
                inner_shard.0,
                seen_inside,
                with_current(|context| context.id),
            ));
        })
        .unwrap();
    outer.run_until_idle();
    let (inner_shard, seen_inside, seen_after) = rx.recv().unwrap();
    assert_eq!(seen_inside, Some(inner_shard));
    assert_eq!(seen_after, Some(outer_shard.0));
}
