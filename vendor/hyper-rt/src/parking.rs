//! Parking a shard and kicking it awake: the protocol between a shard about to wait in its driver
//! and whoever sends it a message from another thread (§4.3, "wake from another shard enqueues
//! (slot, generation) on the target's ring and kicks the driver"; §4.7 "Wake strategy": a sender
//! kicks only a parked shard, so a message to a spinning shard costs no syscall, and the saving
//! is counted). The shard announces that it is about to park, then re-checks its inboxes; a
//! sender publishes its message, then checks the announcement. Each side writes and then reads,
//! the store-buffering shape, so the two reads must be ordered against the two writes by one
//! total order or both can miss: the shard sees no message and parks, the sender sees no
//! announcement and skips the kick, and the message waits for a wake that never comes.
//!
//! **One kick a park.** The announcement is a state word, not a flag: running, parked, or parked
//! with its kick claimed. A sender that reads it parked claims the kick with one compare-and-swap
//! (parked → kicked) and only the winner makes the system call; a sender that finds it kicked skips,
//! because the claimer's kick ends the same wait and the woken shard drains every word published
//! before (the argument below covers both: the claim is a read of the announcement). Until 2026-10-10
//! every sender that read the announcement kicked, so the senders of a fan-in onto one parked shard
//! each paid the system call: 3.2 to 4.5 kicks a park in mantle's fan-in and blocking-pool panels
//! (`benchmark-results/hyper-rt-vs-tokio-20261010/runs/base-f5d66a8-r1`), where tokio's parker pays one
//! (its `NOTIFIED` state, `runtime/scheduler/multi_thread/park.rs`). The word is read before it is
//! swapped: a compare-and-swap takes its cache line exclusive even when it fails, and most wakes find
//! the shard running.
//!
//! Each side therefore fences with `SeqCst` between its write and its read. That is the C++20
//! fence rule ([B: `[atomics.order]`, the fence–fence case]: with a `SeqCst` fence after the
//! write on one thread and a `SeqCst` fence before the read on the other, whichever fence comes
//! first in the total order, the later thread's read observes the earlier thread's write), and
//! it holds whatever ordering the message's own publication used — the multi-producer ring
//! publishes with `Release`, the control flag with `SeqCst`. A `SeqCst` store and load on the
//! announcement alone were not enough: the ring's `Release` publication takes no part in the
//! announcement's total order, so the abstract model lets both reads miss, and x86 realizes it
//! by reordering the plain store of the publication past the plain load of the announcement
//! through its store buffer (a `SeqCst` load is a plain `mov` there; only a `SeqCst` store gets
//! the locked instruction). loom found the lost wake in the first executions it explored
//! (`docs/bugs/2026-09-13-parked-shard-loses-a-foreign-wake.md`); arm64 orders a store-release
//! before a later load-acquire and was never exposed.
//!
//! Under `--cfg loom` the model in this module drives exactly this code: one sender publishing
//! into the shard's multi-producer ring against one shard parking, with loom's `Notify` standing
//! in for the driver's kick (sticky like an eventfd count or an `EVFILT_USER` trigger, spurious
//! like a real `kevent` return); a lost wake is a shard blocked with no runnable thread left,
//! which loom reports as a deadlock (AC-0.7).

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering, fence};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering, fence};

/// Format: the shard is not parked (running, spinning, or between its park's wait and its withdrawal).
const RUNNING: u32 = 0;
/// Format: the shard announced its park and no sender has claimed the kick.
const PARKED: u32 = 1;
/// Format: the shard announced its park and a sender claimed its kick.
const KICKED: u32 = 2;

/// The shard's announcement that it is parked, the count of kicks the announcement saved, and what the
/// claimed kicks cost their senders when the shard learns what blocking costs (`crate::park_cost`).
#[derive(Debug)]
pub struct Parking {
    /// [`RUNNING`], [`PARKED`] or [`KICKED`].
    state: AtomicU32,
    kicks_skipped: AtomicU64,
    /// The CPU the claimed kicks cost their senders, nanoseconds, and how many were measured: the sender's
    /// half of what blocking costs, which the shard adds to its own park's (`crate::park_cost`). Counted by
    /// the claimer of a park the shard measures, read by the shard; measurement words, `Relaxed`, written once
    /// a park at most.
    kick_cpu_ns: AtomicU64,
    kick_cpu_samples: AtomicU64,
    /// Whether the claimer of a kick reads its CPU clock around it ([`Parking::measure_kicks`]). A shard that
    /// learns nothing — a simulated one, or one that cannot spin — has no clock read on either side (D-20: the
    /// simulation makes no OS call; its instruction counts are gated on that).
    measured: AtomicBool,
}

/// A shard's control-pending flag: the sender's publication marker for its control channel, so the shard
/// learns a control message waits without polling the channel every step, and so a parking shard's re-check
/// sees one ([`Parking::park_unless_pending`]). A sender publishes after pushing its message; the shard takes
/// the mark before draining, and re-arms it when a batch-limited drain may have left messages behind.
#[derive(Debug, Default)]
pub struct ControlFlag {
    pending: AtomicBool,
}

impl ControlFlag {
    /// Nothing pending.
    pub fn new() -> Self {
        Self {
            pending: AtomicBool::new(false),
        }
    }

    /// The sender's half, after its message is pushed: marks the channel pending — a read-modify-write, not a
    /// store. Consecutive RMWs form one release sequence, so the shard's acquiring [`take`](Self::take) of the
    /// latest mark synchronizes with **every** sender whose mark it absorbs, and the drain that follows sees
    /// each of their messages. A plain store from a second sender would end the first sender's release
    /// sequence (C++20 `[intro.races]`: a release sequence continues only through RMWs), leaving the first
    /// sender's message unordered before the drain: loom's first interleaving drained neither message this
    /// way — the flag cleared, both kicks skipped, the shard parked for good.
    pub fn publish(&self) {
        let _ = self.pending.swap(true, Ordering::SeqCst);
    }

    /// The shard's half, before draining: whether a message may wait, clearing the mark — one atomic
    /// read-modify-write. The read and the clear must be one step: an RMW reads the latest mark in the flag's
    /// modification order, so either it reads a concurrent sender's publication (and acquires its message,
    /// which the drain that follows then finds) or that publication lands after it and stays set for the next
    /// step. Until 2026-09-28 this was a load and then a store of `false`: the load could read an earlier
    /// sender's mark, the store then overwrote a later sender's, and the drain could miss that sender's
    /// message — a `Shutdown` stranded in the channel while the shard parked for good, since the later
    /// sender's kick had coalesced into the wake the shard already took (loom: the first interleaving
    /// explored; `docs/bugs/2026-09-28-a-cleared-control-flag-stranded-a-shutdown.md`).
    pub fn take(&self) -> bool {
        self.pending.swap(false, Ordering::AcqRel)
    }

    /// The shard re-arms the mark after a drain that stopped at its batch bound (messages may remain).
    pub fn rearm(&self) {
        self.pending.store(true, Ordering::Release);
    }

    /// The shard's park re-check: whether a message may wait.
    pub fn is_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }
}

/// What a park did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Parked {
    /// A message was already pending: the announcement was withdrawn without waiting.
    Pending,
    /// The shard waited.
    Waited,
}

impl Default for Parking {
    fn default() -> Self {
        Self::new()
    }
}

impl Parking {
    /// Not parked, nothing saved yet, kicks unmeasured.
    pub fn new() -> Self {
        Self {
            state: AtomicU32::new(RUNNING),
            kicks_skipped: AtomicU64::new(0),
            kick_cpu_ns: AtomicU64::new(0),
            kick_cpu_samples: AtomicU64::new(0),
            measured: AtomicBool::new(false),
        }
    }

    /// The claimers of this shard's kicks read their CPU clock around them from now on: a shard that learns
    /// what blocking costs says so once, as it is built, before any sender can see it parked.
    pub fn measure_kicks(&self) {
        self.measured.store(true, Ordering::Relaxed);
    }

    /// The sender's half, called after the message is published (a word in the ring, the control
    /// flag set): kicks the shard when it has announced parking and no other sender has claimed that
    /// park's kick, else counts the kick saved. The fence orders the publication before the read of the
    /// announcement (the module doc); the claim is that read's compare-and-swap, made only when the read
    /// found the park unclaimed. When the shard learns what blocking costs, the claimer reads its CPU clock
    /// around the kick.
    pub fn kick_if_parked(&self, kick: impl FnOnce()) {
        fence(Ordering::SeqCst);
        if self.state.load(Ordering::SeqCst) == PARKED
            && self
                .state
                .compare_exchange(PARKED, KICKED, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            if self.measured.load(Ordering::Relaxed) {
                let before = crate::attribution::thread_cpu_now();
                kick();
                if let (Some(before), Some(after)) = (before, crate::attribution::thread_cpu_now())
                {
                    self.kick_cpu_ns
                        .fetch_add(after.saturating_sub(before), Ordering::Relaxed);
                    self.kick_cpu_samples.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                kick();
            }
        } else {
            self.kicks_skipped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The shard's half: announces parking, then asks `pending` whether a message already waits.
    /// When one does, withdraws the announcement and returns [`Parked::Pending`] without waiting;
    /// otherwise runs `wait` (the driver's blocking wait, which a kick ends), withdraws the announcement
    /// afterwards, and returns [`Parked::Waited`]. The fence orders the announcement before the re-check.
    pub fn park_unless_pending(
        &self,
        pending: impl FnOnce() -> bool,
        wait: impl FnOnce(),
    ) -> Parked {
        self.state.store(PARKED, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if pending() {
            self.state.store(RUNNING, Ordering::SeqCst);
            return Parked::Pending;
        }
        wait();
        self.state.store(RUNNING, Ordering::SeqCst);
        Parked::Waited
    }

    /// Kicks skipped because the shard had not announced parking (the saving, counted).
    pub fn kicks_skipped(&self) -> u64 {
        self.kicks_skipped.load(Ordering::Relaxed)
    }

    /// The CPU the measured kicks cost their senders, nanoseconds, and how many were measured: running sums,
    /// which the shard differences between its reads.
    pub(crate) fn kick_cpu(&self) -> (u64, u64) {
        (
            self.kick_cpu_ns.load(Ordering::Relaxed),
            self.kick_cpu_samples.load(Ordering::Relaxed),
        )
    }

    /// Whether the shard is announcing itself parked right now — an **observer's snapshot** (a stall
    /// diagnosis reading it beside the shard's pulse, `registry::Pulse`), never part of the protocol above:
    /// a sender must go through [`kick_if_parked`](Self::kick_if_parked), whose fence is what makes the
    /// answer safe to act on. `Relaxed`: a snapshot that may be a moment stale is what an observer wants.
    pub fn parked(&self) -> bool {
        self.state.load(Ordering::Relaxed) != RUNNING
    }
}

// Two attributes rather than `all(test, loom)`: clippy's test-context rule (unwrap allowed in
// tests) recognizes only a bare `cfg(test)`, and an unwrap here is a failed model, as it should be.
#[cfg(test)]
#[cfg(loom)]
mod loom_tests {
    use std::sync::atomic::{AtomicU64, Ordering as StdOrdering};

    // loom's `Notify` is the model's stand-in for the driver's kick: test scaffolding under
    // `cfg(loom)` only (D-8 exception 3, a test harness); the two owners are this model's sender
    // and shard.
    use crate::mem::loom_bounds;
    use crate::wakes::WakeBitmap;
    use loom::sync::Notify;

    use super::*;

    /// Shape: the shard's wake bitmap in the model: two task slots, one per sender of the control model.
    const SLOTS: usize = 2;
    /// Format: the slot the sender wakes; the model checks that its wake arrives.
    const WORD: u32 = 0;

    /// Takes every wake waiting in `bitmap`: the slots woken.
    fn take_wakes(bitmap: &WakeBitmap) -> Vec<u32> {
        let mut woken = Vec::new();
        bitmap.drain(|slot| woken.push(slot));
        woken
    }

    /// Kicks delivered, across every explored interleaving.
    static KICKS: AtomicU64 = AtomicU64::new(0);
    /// Kicks skipped because the shard was not parked, across every explored interleaving.
    static SKIPS: AtomicU64 = AtomicU64::new(0);
    /// Waits the shard entered, across every explored interleaving.
    static WAITS: AtomicU64 = AtomicU64::new(0);
    /// Kicks claimed against a shard whose kicks are measured, across every explored interleaving of that
    /// model.
    static MEASURED: AtomicU64 = AtomicU64::new(0);

    /// AC-0.7 (the kick-if-parked protocol of `registry::wake` and `Shard::park`): a sender sets a
    /// task's bit in the shard's wake bitmap and kicks only if the shard announced parking; the shard
    /// announces, re-checks the bitmap, and waits only when it saw nothing. In
    /// every interleaving the shard receives the word: it either saw it before waiting, or was
    /// kicked out of its wait. A lost wake leaves the shard blocked with nothing left to run,
    /// which loom reports as a deadlock. Some interleaving kicked, some skipped the kick, and some
    /// made the shard wait, so neither half of the protocol is vacuous.
    #[test]
    fn a_word_published_while_the_shard_parks_is_never_lost() {
        loom_bounds::explore("parking: one sender against one parking shard", || {
            let bitmap: &'static WakeBitmap = Box::leak(Box::new(WakeBitmap::new(SLOTS)));
            let parking: &'static Parking = Box::leak(Box::new(Parking::new()));
            let kick: &'static Notify = Box::leak(Box::new(Notify::new()));
            let sender = loom::thread::spawn(move || {
                // `registry::wake`'s order: set the bit, then kick if the shard is parked.
                assert!(bitmap.set(WORD));
                parking.kick_if_parked(|| {
                    KICKS.fetch_add(1, StdOrdering::Relaxed);
                    kick.notify();
                });
            });
            let mut waited = false;
            // The shard's loop: drain the bitmap, and park unless a wake is pending.
            let word = loop {
                if let Some(word) = take_wakes(bitmap).first().copied() {
                    break word;
                }
                if matches!(
                    parking.park_unless_pending(|| bitmap.is_pending(), || kick.wait()),
                    Parked::Waited
                ) {
                    waited = true;
                }
            };
            assert_eq!(word, WORD);
            sender.join().unwrap();
            if waited {
                WAITS.fetch_add(1, StdOrdering::Relaxed);
            }
            SKIPS.fetch_add(parking.kicks_skipped(), StdOrdering::Relaxed);
        });
        assert!(
            KICKS.load(StdOrdering::Relaxed) > 0,
            "some interleaving kicked"
        );
        assert!(
            SKIPS.load(StdOrdering::Relaxed) > 0,
            "some interleaving skipped the kick"
        );
        assert!(
            WAITS.load(StdOrdering::Relaxed) > 0,
            "some interleaving made the shard wait"
        );
    }

    /// Kicks sent across every explored interleaving of the two-sender model.
    static CLAIMED: AtomicU64 = AtomicU64::new(0);

    /// One kick a park (the module doc): two senders each publish a word into the shard's bitmap and kick it
    /// if it is parked, while the shard drains and parks. In every interleaving both words arrive — a sender
    /// that found the park's kick claimed relied on the claimer's kick, and loom reports a word stranded
    /// behind a skipped kick as a deadlock — and no park is kicked twice: a run never sends more kicks than
    /// the parks it announced. Some interleaving kicked, so the claim's path is not vacuous.
    #[test]
    fn two_senders_never_lose_a_word_and_a_park_takes_one_kick() {
        loom_bounds::explore("parking: two senders against one parking shard", || {
            let bitmap: &'static WakeBitmap = Box::leak(Box::new(WakeBitmap::new(SLOTS)));
            let parking: &'static Parking = Box::leak(Box::new(Parking::new()));
            let kick: &'static Notify = Box::leak(Box::new(Notify::new()));
            let kicks: &'static loom::sync::atomic::AtomicU64 =
                Box::leak(Box::new(loom::sync::atomic::AtomicU64::new(0)));
            let send = move |slot: u32| {
                assert!(bitmap.set(slot));
                parking.kick_if_parked(|| {
                    kicks.fetch_add(1, Ordering::Relaxed);
                    kick.notify();
                });
            };
            let first = loom::thread::spawn(move || send(WORD));
            let second = loom::thread::spawn(move || send(WORD + 1));
            let mut seen = [false; SLOTS];
            let mut parks = 0u64;
            loop {
                for slot in take_wakes(bitmap) {
                    seen[usize::try_from(slot).unwrap()] = true;
                }
                if seen.iter().all(|word| *word) {
                    break;
                }
                parks += 1;
                let _ = parking.park_unless_pending(|| bitmap.is_pending(), || kick.wait());
            }
            first.join().unwrap();
            second.join().unwrap();
            let sent = kicks.load(Ordering::Relaxed);
            assert!(sent <= parks, "{sent} kicks for {parks} parks");
            CLAIMED.fetch_add(sent, StdOrdering::Relaxed);
        });
        assert!(
            CLAIMED.load(StdOrdering::Relaxed) > 0,
            "some interleaving kicked"
        );
    }

    /// Delivered control messages, across every explored interleaving of the control model.
    static CONTROL_WAITS: AtomicU64 = AtomicU64::new(0);

    /// AC-0.7 (the control path of `registry::send_control_to` and `shard::drain_control`): two senders each
    /// push one control message into the shard's channel, publish the [`ControlFlag`], and kick the shard if
    /// it announced parking; the shard takes the flag and drains the channel, and parks unless the flag is
    /// pending. In every interleaving both messages are drained: a message whose publication the shard's
    /// take cleared is either drained by that same drain or re-marked. A lost message leaves the shard parked
    /// with nothing left to run, which loom reports as a deadlock. Some interleaving made the shard wait.
    #[test]
    fn a_control_message_published_while_the_shard_drains_is_never_lost() {
        loom_bounds::explore(
            "parking: two control senders against one draining, parking shard",
            || {
                let channel: &'static WakeBitmap = Box::leak(Box::new(WakeBitmap::new(SLOTS)));
                let flag: &'static ControlFlag = Box::leak(Box::new(ControlFlag::new()));
                let parking: &'static Parking = Box::leak(Box::new(Parking::new()));
                let kick: &'static Notify = Box::leak(Box::new(Notify::new()));
                let send = move |word: u32| {
                    // `send_control_to`'s order: push, publish the flag, then kick if the shard is parked.
                    assert!(channel.set(word));
                    flag.publish();
                    parking.kick_if_parked(|| kick.notify());
                };
                let first = loom::thread::spawn(move || send(WORD));
                let second = loom::thread::spawn(move || send(WORD + 1));
                let mut received = 0;
                let mut waited = false;
                // The shard's loop: a step drains when the flag is taken; a step that drained loops again; an idle
                // step parks unless the flag is pending.
                while received < 2 {
                    let mut drained = 0;
                    if flag.take() {
                        drained = take_wakes(channel).len();
                    }
                    received += drained;
                    if received == 2 {
                        break;
                    }
                    if drained > 0 {
                        continue;
                    }
                    if matches!(
                        parking.park_unless_pending(|| flag.is_pending(), || kick.wait()),
                        Parked::Waited
                    ) {
                        waited = true;
                    }
                }
                first.join().unwrap();
                second.join().unwrap();
                if waited {
                    CONTROL_WAITS.fetch_add(1, StdOrdering::Relaxed);
                }
            },
        );
        assert!(
            CONTROL_WAITS.load(StdOrdering::Relaxed) > 0,
            "some interleaving made the shard wait"
        );
    }

    /// The same protocol with the shard learning what blocking costs: the claimer reads its CPU clock around
    /// the kick, a measurement outside the protocol, so the word still arrives in every interleaving. Some
    /// interleaving claimed a kick, so the measured path is not vacuous.
    #[test]
    fn a_measured_kick_never_loses_the_word() {
        loom_bounds::explore(
            "parking: one sender against one parking shard whose kicks are measured",
            || {
                let bitmap: &'static WakeBitmap = Box::leak(Box::new(WakeBitmap::new(SLOTS)));
                let parking: &'static Parking = Box::leak(Box::new(Parking::new()));
                parking.measure_kicks();
                let kick: &'static Notify = Box::leak(Box::new(Notify::new()));
                let sender = loom::thread::spawn(move || {
                    assert!(bitmap.set(WORD));
                    parking.kick_if_parked(|| {
                        MEASURED.fetch_add(1, StdOrdering::Relaxed);
                        kick.notify();
                    });
                });
                let word = loop {
                    if let Some(word) = take_wakes(bitmap).first().copied() {
                        break word;
                    }
                    let _ = parking.park_unless_pending(|| bitmap.is_pending(), || kick.wait());
                };
                assert_eq!(word, WORD);
                sender.join().unwrap();
            },
        );
        assert!(
            MEASURED.load(StdOrdering::Relaxed) > 0,
            "some interleaving claimed a kick"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parks `parking` until another thread kicks it (once it has announced the park); what the park did.
    fn kicked_park(parking: &Parking) -> Parked {
        std::thread::scope(|scope| {
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            let parked = &parking;
            let kicker = scope.spawn(move || {
                while !parked.parked() {
                    std::thread::yield_now();
                }
                parked.kick_if_parked(|| {
                    let _ = tx.send(());
                });
            });
            let parked = parking.park_unless_pending(
                || false,
                || {
                    let _ = rx.recv();
                },
            );
            let _ = kicker.join();
            parked
        })
    }

    /// A kick claimed against a shard whose kicks are measured is charged to the sender's CPU where the OS
    /// keeps a per-thread clock; an unmeasured kick is not; a park with a message already pending never waits.
    #[test]
    #[cfg_attr(miri, ignore)] // a measured kick reads the thread's CPU clock, which Miri does not model
    fn a_measured_kick_is_charged_to_its_sender() {
        let parking = Parking::new();
        assert_eq!(kicked_park(&parking), Parked::Waited);
        assert_eq!(
            parking.kick_cpu(),
            (0, 0),
            "an unmeasured kick reads no clock"
        );
        parking.measure_kicks();
        assert_eq!(parking.park_unless_pending(|| true, || {}), Parked::Pending);
        assert_eq!(kicked_park(&parking), Parked::Waited);
        let samples = if cfg!(any(target_os = "linux", target_os = "macos")) {
            1
        } else {
            0
        };
        assert_eq!(parking.kick_cpu().1, samples);
        assert_eq!(parking.kicks_skipped(), 0);
    }

    /// D-20: a shard that learns nothing — a simulated one — parks and is kicked exactly as before, and
    /// neither side reads a clock. Runs under Miri too, where a clock read would fail, so a pass there is the
    /// proof that none happened. (The simulation's instruction counts run under callgrind, where the first
    /// host clock read faulted inside rustix's vDSO setup: CI run 36189146259.)
    #[test]
    fn an_unmeasured_park_waits_and_reads_no_clock() {
        let parking = Parking::new();
        assert_eq!(parking.park_unless_pending(|| true, || {}), Parked::Pending);
        assert_eq!(kicked_park(&parking), Parked::Waited);
        assert_eq!(parking.kicks_skipped(), 0);
    }
}
