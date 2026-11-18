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
//! Sockets, readiness and timers charge here; channels, datagrams and signals do not yet.
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
