//! A task's turn on its shard, by time, with no hand-placed yields (Kiln's `kiln-rt` `coop.rs`, its goal 6,
//! plan 01a1a59c2abe step 1; ORIGIN.md; docs/runtime.md §3.14).
//!
//! A task is charged as it uses its shard. Each operation of a leaf future is one charge, made as the
//! operation goes on: a socket's read, write or accept, a readiness wait that fired, a timer due. Each charge
//! weighs the time the task has held the shard since its poll began, or since the shard last let it go on
//! (`ShardContext::checked_ns`, docs/runtime.md §3.11), against its share. Past its share, and with anything
//! else wanting the shard ([`crate::shard::ShardContext::wanted`]: a task listed, an intent, a wake or control
//! message from another thread, a timer or the driver's harvest due), the charge answers no: the leaf returns
//! `Pending`, nothing taken, with its task woken behind the loop's next reading of its inputs, and every leaf
//! the task polls before that poll ends answers the same. So the task yields at its next await, as Tokio's
//! leaves make theirs do [Tokio, "Reducing tail latencies with automatic cooperative task yielding", 2020;
//! `tokio::task::coop`]. Where nothing else wants the shard the task goes on, and its hold counts from there,
//! as [`crate::futures::maybe_yield`] lets it.
//!
//! - **The share is the step quantum** (`ShardContext::quantum_ns`): the online wake estimate while the
//!   shard tracks one, else its configured step budget. It is the quantum a task that asks `wanted` once a
//!   quantum holds a wanted shard for (docs/runtime.md §3.10), the batch's latency budget (§3.3) and the
//!   watchdog's unit (§3.13), so a task charged this way never holds a wanted shard past twice it: it makes
//!   no long poll (§3.11).
//! - **Each charge reads the shard's clock**, except where nothing could want the shard, which needs no
//!   time. A count of charges between readings would bound nothing in time: a loop whose items turned from
//!   cheap to dear would run a count's worth of dear ones past its share.
//! - **Long computations** call [`consume_budget`] between their pieces: the same charge, alone.
//! - **A simulated shard never yields by budget**: its clock does not move within a poll, and its schedules
//!   stay a step a yield as they were (docs/runtime.md §11).
//! - **Only the task's own waker is refused.** A leaf polled under another waker (an executor nested in the
//!   task's poll, `now_or_never`'s, a combinator's own) is charged, so the time counts, but always goes on: a
//!   refusal there can't make the task yield, since what polls it would only poll it again at once, for as
//!   long as the task's poll lasts, and what comes at once must still come at once. The task yields at its
//!   next leaf under its own waker.
//! - **Each poll makes at least one operation**: its first charge under its own waker goes on whatever the
//!   time, as Tokio's budget gives each poll its 128. A poll whose own work before its first leaf outlasts
//!   the share (futures' `read_to_end` zeroes its buffer's spare room again at every poll) would otherwise be
//!   refused there at every poll, redoing that work and getting no further: a livelock that spun a shard of
//!   Kiln's tests (`tests/coop_budget.rs`).
//! - **A leaf is charged where it goes on, never where it waits.** It looks first, taking nothing: one that
//!   would wait registers its task and answers `Pending` with no charge, since waiting gives the shard up
//!   already; one that goes on is charged, then takes, so a refusal leaves what it would have taken for its
//!   next poll. Charged as it looked, a leaf that waited took its poll's first charge, which always goes on,
//!   so the leaf after it was refused at every poll once the poll's own work outlasted the share; and refused
//!   itself, a leaf registered nothing and woke its task at once instead of at its event. Either way the task
//!   was polled again and again for nothing (Kiln's front door spun a shard for hours so;
//!   `tests/coop_waits.rs`). Where only the operation itself can say whether it waits, a socket's try, the
//!   try is charged and a wait it then arms is not.
//!
//! Charged here: sockets and datagrams as each tries, readiness waits, timers, and as each goes on, the sync
//! primitives (`crate::sync`), joins and signals. A readiness group's members do not charge yet, nor the
//! `try_*` calls a caller drives itself.
//!
//! Tokio's budget counts operations, 128 a poll, and reads no clock; Seastar's task quota is a time (0.5 ms
//! by default) that a timer ends, checked between tasks. Here the count is kept (`Counters::coop_charges`,
//! the operations charged) and the share is a time, read at each charge.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::registry;
use crate::shard::ShardContext;

/// Charges the polling task one operation of a leaf future: `Ready` to go on with it, or `Pending` once the
/// task has held its shard past its share while anything else wants the shard, with the waker woken, so the
/// task is polled again after the loop has read its inputs. A leaf calls it once it has looked and found that
/// it goes on, before it takes anything, so an answer of `Pending` leaves its operation undone; a leaf that
/// waits never calls it. `Ready` off a shard's thread, between polls (an owner's code) and under any waker
/// but the polling task's own (the module's notes).
pub fn poll_proceed(cx: &mut Context<'_>) -> Poll<()> {
    if registry::with_current(|here| proceeds(here, cx)).unwrap_or(true) {
        return Poll::Ready(());
    }
    cx.waker().wake_by_ref();
    Poll::Pending
}

/// [`poll_proceed`] on the shard `here` a leaf has looked up already: one lookup of the thread's shard
/// serves both.
pub(crate) fn proceed_in(here: &ShardContext, cx: &mut Context<'_>) -> Poll<()> {
    if proceeds(here, cx) {
        return Poll::Ready(());
    }
    cx.waker().wake_by_ref();
    Poll::Pending
}

/// The charge, and whether the leaf goes on: it does within the task's share, at the poll's first charge,
/// and under a waker other than the polling task's own, which no refusal would make yield.
fn proceeds(here: &ShardContext, cx: &Context<'_>) -> bool {
    let own = crate::waker::polling_task_in(here, cx.waker()).is_some();
    here.charge(own)
}

/// The yield point of a long computation: one charge with no other operation (Tokio's `consume_budget`).
/// Ready at once while the task is within its share, or while nothing else wants its shard; past its share
/// with something else waiting, the task yields once and goes on at its next poll.
pub fn consume_budget() -> ConsumeBudget {
    ConsumeBudget { _private: () }
}

/// The future of [`consume_budget`].
#[derive(Debug)]
#[must_use = "a charge does nothing unless awaited"]
pub struct ConsumeBudget {
    _private: (),
}

impl Future for ConsumeBudget {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        poll_proceed(cx)
    }
}

/// Whether the polling task has used its share on a shard something else wants: the leaves it polls before
/// its poll ends answer `Pending` ([`poll_proceed`]). `false` off a shard's thread.
pub fn spent() -> bool {
    registry::with_current(|here| here.coop_spent.get()).unwrap_or(false)
}

/// A task's class on its shard (Kiln's `kiln-rt` `coop.rs`, its goal 6, plan 01a1a59c2abe step 3; ORIGIN.md):
/// which of the run queue's lists it waits in. The shard gives each class with tasks waiting an equal share of its
/// time, and the most urgent goes first while it has run no more than the shard's quantum past the least-run class
/// waiting (`crate::queue::LocalQueue::pop_within`): Seastar's scheduling groups give components shares of a shard
/// the same way, and Google tags each request with a criticality its servers shed and serve by. Equal shares, since
/// no class's need is known to outweigh another's: urgency decides the order, the shares the time, and none is
/// starved. A spawn takes the class it is given, [`Class::Normal`] unless it names another
/// ([`crate::futures::spawn_detached_as`]): Kiln's spawns take their spawner's class, but here a node's Critical
/// task accepts connections that must stay Normal. A task that lowers its class within a poll gives way to a more
/// urgent one waiting ([`give_way`]) to go on in its own class's turn.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Class {
    /// Work that may wait without harm: catch-ups, sweeps, telemetry.
    Background = 0,
    /// The default: a server's requests.
    #[default]
    Normal = 1,
    /// What the rest waits on: a node's consensus, log and transport.
    Critical = 2,
}

/// Sets the class of the task being polled: from its next listing it waits in that class's lists, and its polls
/// count to that class's share. The class it had; `None` off a task.
pub fn set_class(class: Class) -> Option<Class> {
    registry::with_current(|here| {
        let slot = here.current_slot()?;
        Some(here.local.set_class(slot, class))
    })
    .flatten()
}

/// The class of the task being polled; `None` off a task.
pub fn class() -> Option<Class> {
    registry::with_current(|here| here.current_slot().map(|slot| here.local.class_of(slot)))
        .flatten()
}

/// Whether a task of a more urgent class than the polling task's waits on its shard, listed or woken itself and
/// waiting for its release: what a task that lowered its class gives way to ([`give_way`]). `false` off a task.
pub fn outranked() -> bool {
    registry::with_current(|here| {
        here.current_slot()
            .is_some_and(|slot| here.local.waiting_above(here.local.class_of(slot)))
    })
    .unwrap_or(false)
}

/// Yields once where a task more urgent than the polling one waits on its shard ([`outranked`]), so a task that
/// lowered its class within a poll goes on in its own class's turn rather than in the more urgent one it was
/// polled in (Kiln's connections read each request's head as Critical and give way once it is known to be of the
/// API). Ready at once where none waits: the task keeps its place ahead of its own class's queued work, as it
/// would have without the check.
pub fn give_way() -> GiveWay {
    GiveWay { yielded: false }
}

/// The future of [`give_way`].
#[derive(Debug)]
#[must_use = "a yield does nothing unless awaited"]
pub struct GiveWay {
    yielded: bool,
}

impl Future for GiveWay {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded || !outranked() {
            return Poll::Ready(());
        }
        self.yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};

    use super::{Class, class, set_class};
    use crate::futures::{spawn, spawn_detached, spawn_detached_as, yield_now};
    use crate::runtime::{LocalRuntime, RuntimeConfig};

    /// `value` for the rest of the test, shared by reference with the tasks that need it.
    fn shared<T>(value: T) -> &'static T {
        Box::leak(Box::new(value))
    }

    /// What the tasks' polls did, in order: a letter a poll (its task's), as many as fit.
    struct Trail {
        at: AtomicUsize,
        marks: Vec<AtomicU8>,
    }

    impl Trail {
        fn new(room: usize) -> &'static Self {
            shared(Self {
                at: AtomicUsize::new(0),
                marks: (0..room).map(|_| AtomicU8::new(0)).collect(),
            })
        }

        fn mark(&self, letter: u8) {
            if let Some(cell) = self.marks.get(self.at.fetch_add(1, Ordering::Relaxed)) {
                cell.store(letter, Ordering::Relaxed);
            }
        }

        fn read(&self) -> String {
            let n = self.at.load(Ordering::Relaxed).min(self.marks.len());
            self.marks[..n]
                .iter()
                .map(|m| char::from(m.load(Ordering::Relaxed)))
                .collect()
        }
    }

    /// One shard, its quantum a millisecond: far past a poll here, so the order is the classes', not the time's.
    fn config() -> RuntimeConfig {
        RuntimeConfig {
            shards: 1,
            tasks_per_shard: 32,
            timers_per_shard: 32,
            interests_per_shard: 64,
            ring_entries: 32,
            step_budget_ns: 1_000_000,
            timer_tick_ns: 50_000,
            batch: 32,
            pin: false,
            cores: Vec::new(),
            page_bytes: 4096,
            spin_ns: 0,
            wake_tracking: None,
            latency_objective_ns: None,
        }
    }

    /// Three Normal tasks (`n`) and three Background ones (`b`) keep the shard busy, each marking a poll and
    /// yielding, queued behind each other; a task spawned as `class` waits until the root wakes it (`S`, a wake
    /// from another task, as a socket's readiness or a reply is), then marks `c`. The trail of marks.
    fn woken_beside_queued_work(class: Class) -> String {
        let mut rt = LocalRuntime::new(&config()).unwrap();
        let trail = Trail::new(4096);
        let marks = trail;
        rt.block_on(async move {
            let done = shared(AtomicBool::new(false));
            for (letter, busy) in [(b'n', Class::Normal), (b'b', Class::Background)] {
                for _ in 0..3 {
                    spawn_detached_as(busy, async move {
                        while !done.load(Ordering::Acquire) {
                            marks.mark(letter);
                            yield_now().await;
                        }
                    })
                    .unwrap();
                }
            }
            let (wake, woken) = crate::sync::oneshot::<()>().unwrap();
            let ran = shared(AtomicBool::new(false));
            let (waiter_marks, waiter_ran) = (marks, ran);
            spawn_detached_as(class, async move {
                let _ = woken.await;
                waiter_marks.mark(b'c');
                waiter_ran.store(true, Ordering::Release);
            })
            .unwrap();
            // Rounds of the busy tasks, each queued behind the others, the waiter waiting.
            for _ in 0..8 {
                yield_now().await;
            }
            marks.mark(b'S');
            let _ = wake.send(());
            while !ran.load(Ordering::Acquire) {
                yield_now().await;
            }
            done.store(true, Ordering::Release);
        })
        .unwrap();
        trail.read()
    }

    /// A spawn takes the class it is given, Normal unless it names another; a task's own spawns do not take its
    /// class; and a slot a Critical task left starts Normal for its next task. Do: spawn as Critical; it spawns a
    /// task, and ends; spawn again into the freed slots. Expect: Critical, then Normal for the rest.
    #[test]
    fn s5a_a_spawn_takes_its_class_and_defaults_to_normal() {
        let mut rt = LocalRuntime::new(&config()).unwrap();
        let seen = rt
            .block_on(async move {
                let classes = shared(RefCell::new(Vec::new()));
                let outer = classes;
                spawn_detached_as(Class::Critical, async move {
                    outer.borrow_mut().push(class());
                    let inner = outer;
                    spawn_detached(async move {
                        inner.borrow_mut().push(class());
                    })
                    .unwrap();
                })
                .unwrap();
                for _ in 0..4 {
                    yield_now().await;
                }
                for _ in 0..4 {
                    let again = classes;
                    let _ = spawn(async move {
                        again.borrow_mut().push(class());
                    })
                    .unwrap();
                }
                for _ in 0..4 {
                    yield_now().await;
                }
                let root = class();
                let seen = classes.borrow().clone();
                (root, seen)
            })
            .unwrap();
        let (root, seen) = seen;
        assert_eq!(root, Some(Class::Normal), "a root starts Normal");
        assert_eq!(
            seen,
            vec![
                Some(Class::Critical),
                Some(Class::Normal),
                Some(Class::Normal),
                Some(Class::Normal),
                Some(Class::Normal),
                Some(Class::Normal),
            ]
        );
        assert_eq!(class(), None, "off a task");
    }

    /// Kiln's `g6_s3_a_task_that_lowered_its_class_gives_way_only_to_more_urgent_work`: a task that lowered its
    /// class gives way to a more urgent task waiting on its shard, and only to one: with its own class's and less
    /// urgent work waiting, it goes on at once, its place kept. Do: the root starts a Critical, a Normal and a
    /// Background task that each wait, wakes the Normal and Background ones, lowers itself to Normal and gives way
    /// (`r`); then wakes the Critical one and gives way again (`R`). Expect: not outranked the first time, outranked
    /// the second; `r` before every other mark, and the Critical task's `k` before `R`: the shard handed over.
    #[test]
    fn s5a_give_way_hands_the_shard_to_more_urgent_work_only() {
        let mut rt = LocalRuntime::new(&config()).unwrap();
        let trail = Trail::new(64);
        let marks = trail;
        let (first, second) = rt
            .block_on(async move {
                let mut wakes = Vec::new();
                for (letter, class) in [
                    (b'k', Class::Critical),
                    (b'n', Class::Normal),
                    (b'b', Class::Background),
                ] {
                    let (wake, woken) = crate::sync::oneshot::<()>().unwrap();
                    spawn_detached_as(class, async move {
                        let _ = woken.await;
                        marks.mark(letter);
                    })
                    .unwrap();
                    wakes.push(wake);
                }
                let _ = set_class(Class::Critical);
                // Their first polls, in which each waits.
                yield_now().await;
                let mut wakes = wakes.into_iter();
                let (critical, normal, background) = (
                    wakes.next().unwrap(),
                    wakes.next().unwrap(),
                    wakes.next().unwrap(),
                );
                normal.send(()).unwrap();
                background.send(()).unwrap();
                let _ = set_class(Class::Normal);
                let first = super::outranked();
                super::give_way().await;
                marks.mark(b'r');
                critical.send(()).unwrap();
                let second = super::outranked();
                super::give_way().await;
                marks.mark(b'R');
                (first, second)
            })
            .unwrap();
        let trail = trail.read();
        assert!(
            !first,
            "only its own class's and less urgent work waited: {trail}"
        );
        assert!(second, "a Critical task waited: {trail}");
        assert!(
            trail.starts_with('r'),
            "it went on at once, ahead of the Normal and Background work woken: {trail}"
        );
        let (k, big_r) = (trail.find('k').unwrap(), trail.find('R').unwrap());
        assert!(k < big_r, "it gave way to the Critical task: {trail}");
    }

    /// Kiln's `g6_s3_on_a_shard_a_woken_critical_task_overtakes_queued_normal_and_background_work`: a Critical task
    /// woken while Normal and Background tasks are queued is polled next, ahead of all of them; as Normal it waits
    /// behind the Normal tasks queued before it. Do: three busy Normal and three busy Background tasks taking turns;
    /// the root wakes a waiting task. Expect: as Critical, `c` right after `S`; as Normal, a queued `n` between them.
    #[test]
    fn s5a_a_woken_critical_task_overtakes_queued_normal_and_background_work() {
        let critical = woken_beside_queued_work(Class::Critical);
        let s = critical.find('S').unwrap();
        assert!(
            critical.matches('n').count() >= 8 && critical.matches('b').count() >= 8,
            "the busy tasks took turns: {critical}"
        );
        assert_eq!(
            critical[s..].chars().nth(1),
            Some('c'),
            "the Critical task overtook the queued work: {critical}"
        );

        let normal = woken_beside_queued_work(Class::Normal);
        let (s, c) = (normal.find('S').unwrap(), normal.find('c').unwrap());
        assert!(
            normal[s..c].contains('n'),
            "as Normal it waits behind the Normal tasks queued before it: {normal}"
        );
    }
}
