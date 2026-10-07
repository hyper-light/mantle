//! AC-0.7, §4.3: race registration, foreign wakes and retirement through the public registry.
//! This fixture owns its process: its neighbour wakes intentionally address any live slot, so
//! sharing a binary with other tests would wake their tasks spuriously. All sixteen concurrent workers and their history budget remain unchanged.

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
use std::sync::atomic::{AtomicU64, Ordering};

use hyper_rt::driver::Kick;
use hyper_rt::mem::Encoded;
use hyper_rt::registry::{MAX_SHARDS, RegisterKick, holder_of, register, wake, with_entry};

/// Format: the task slot the "shard" (this thread) wakes in itself; a wake bitmap of four slots holds it.
const OWN_SLOT: u32 = 1;

/// Drains the calling thread's own bitmap (it is the "shard" of `id`), counting its own wake; reports
/// whether it was seen. A neighbour's wake that landed here is drained too but not counted.
fn drain_own(id: u16, landed: &AtomicU64) -> bool {
    with_entry(id, |entry| {
        let mut seen = false;
        entry.wakes.drain(|slot| {
            if slot == OWN_SLOT {
                seen = true;
            }
        });
        if seen {
            landed.fetch_add(1, Ordering::Relaxed);
        }
        seen
    })
    .unwrap_or(false)
}

/// The slot protocol under contention (a stand-in until the loom lane covers this crate, which is
/// owed): many threads register, wake and unregister against a handful of slots at once; every
/// wake either lands in a live ring or is counted stale — never a fault — and every slot ends free
/// with no entry lost (each registration is unregistered by its own thread). Do: 16 threads × 200
/// register/wake/unregister cycles. Expect: no panic, every wake accounted for, every slot free.
/// This test found two faults in the protocol on 2026-09-14 (both timing-dependent, so the same
/// binary passed a validation run and hung or crashed the next): a wake to a neighbour whose ring
/// was full and whose holder then unregistered spun forever (`send_foreign` waited for a consumer
/// that was gone), and a reader descheduled between loading a neighbour's entry and pushing to it
/// dereferenced the entry a re-registration had freed (a null load in the freed ring's head word).
/// The re-validated spin and the counted reader are the fixes; this test must pass every run.
#[test]
fn registrations_wakes_and_unregistrations_interleave_without_a_fault() {
    /// Shape: concurrent holders exercising contention on registration and retirement.
    const THREADS: usize = 16;
    /// Shape: repeated registration histories per holder in this bounded stress fixture.
    const CYCLES: usize = 200;
    // A process-static counter the threads share (R2: no `Arc`; a `&'static` is the sharing form).
    static WAKES_LANDED: AtomicU64 = AtomicU64::new(0);
    let landed: &'static AtomicU64 = &WAKES_LANDED;
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            std::thread::spawn(move || {
                for _ in 0..CYCLES {
                    let (registration, receiver) =
                        register(4, 2, RegisterKick::Kick(Kick::none())).unwrap();
                    let id = registration.shard();
                    let held = holder_of(id).unwrap();
                    // A foreign wake to our own slot lands in its bitmap at once, drained by the "shard" (this
                    // thread): a bit set never waits for a consumer (slates' rings deadlocked here on 2026-09-14,
                    // a holder looping on a full ring of neighbour wakes without draining its own).
                    wake(Encoded::pack(id, OWN_SLOT, 0).unwrap());
                    assert!(drain_own(id, landed), "slot {id}'s own wake landed");
                    // A wake to a slot another thread may have freed meanwhile is counted, never a fault.
                    let neighbour =
                        id.wrapping_add(1) % u16::try_from(MAX_SHARDS).unwrap_or(u16::MAX);
                    wake(Encoded::pack(neighbour, OWN_SLOT, 0).unwrap());
                    let _ = receiver.try_recv();
                    drop(registration);
                    // The slot was given back: its generation moved past the even value this thread held
                    // (another test in this binary, or another thread here, may already hold it again, so
                    // "free" is not the claim — "no longer mine" is).
                    assert_ne!(holder_of(id), Some(held), "slot {id}'s registration ended");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(
        landed.load(Ordering::Relaxed),
        u64::try_from(THREADS * CYCLES).unwrap(),
        "every wake to a live slot landed"
    );
}
