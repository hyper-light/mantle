//! An owner's replicas cost no thread of their own (`docs/durable.md` §7, `CLAUDE.md`: never a
//! thread per unit): a test of its own, so the process's thread count is this test's alone.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity
)]

mod support;

use std::task::Waker;

use hyper_durable::{GroupStore, Output, Owner, Replica, Unbounded};
use hyper_raft::proto::ConfState;
use support::Kv;
use support::cluster::settings;

fn voters(ids: &[u64]) -> ConfState {
    ConfState {
        voters: ids.to_vec(),
        ..ConfState::default()
    }
}

/// An owner's replicas cost no thread of their own: one log's two threads serve one group or
/// sixty-four, the process's thread count the same (`docs/durable.md` §7).
#[test]
fn threads_do_not_grow_with_the_groups() {
    use hyper_log::{Config, Log, Waits};
    let config = Config {
        segment_bytes: 64 * 4096,
        max_segments: 64,
        max_groups: 128,
        group_entries: 1 << 12,
        group_bytes: 1 << 20,
        group_cache: 1 << 14,
        queue_submissions: 512,
        waits: Waits::Never,
    };
    let log = Log::create(support::device::sim_file(11), config, 1).unwrap();
    let (tell, woken) = std::sync::mpsc::sync_channel(4096);
    let wakers: Vec<Waker> = (0..64usize)
        .map(|slot| hyper_measure::wake::waker(slot, tell.clone()).0)
        .collect();
    let mut owner: Owner<GroupStore<_>, Kv, Unbounded> = Owner::new(wakers);
    let mut threads = Vec::new();
    let mut handles = Vec::new();
    for group in 0..64u128 {
        let store = GroupStore::claim(&log, group).unwrap();
        let mut r = Replica::open(
            &settings(1, 3),
            store,
            Kv::new(voters(&[1]), true),
            Unbounded,
        )
        .unwrap();
        r.campaign().unwrap();
        let h = owner.insert(r).map_err(|_| ()).unwrap();
        owner
            .get_mut(h)
            .unwrap()
            .propose(Vec::new(), b"x".to_vec())
            .unwrap();
        owner.schedule(h);
        handles.push(h);
        if group == 0 || group == 63 {
            threads.push(hyper_block::threads::count().unwrap());
        }
    }
    let mut out = Output::default();
    let mut applied = 0;
    for _ in 0..10_000 {
        owner.turn(now(), &mut out, |_, d, _| {
            d.unwrap();
        });
        applied = handles
            .iter()
            .filter(|&&h| {
                owner
                    .get(h)
                    .is_some_and(|r| r.machine().now.entries.len() >= 2)
            })
            .count();
        if applied == 64 {
            break;
        }
        if !owner.has_work() {
            // Every write out wakes its slot once when answered.
            let slot = woken.recv().expect("a write is answered");
            owner.woken(slot);
        }
        while let Ok(slot) = woken.try_recv() {
            owner.woken(slot);
        }
    }
    assert_eq!(applied, 64);
    threads.push(hyper_block::threads::count().unwrap());
    assert!(
        threads.windows(2).all(|w| w[0] == w[1]),
        "threads grew with the groups: {threads:?}"
    );
}

/// The owner's clock in nanoseconds, simulated: each reading a nanosecond after the one before, so
/// time only moves forward, as an owner's monotonic clock does, and every run reads the same times.
/// No test here waits on elapsed time; those that judge time state it outright.
fn now() -> u64 {
    thread_local! {
        static CLOCK: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }
    CLOCK.with(|clock| {
        let now = clock.get() + 1;
        clock.set(now);
        now
    })
}
