//! Public driver faults must terminate owned loops and preserve exact readiness refusals.
//! Each case waits on its own outcome: a loop that never retrieves the driver's result, and so
//! never sees the loss or the refusal, never returns, and the job's own limit is its failure. No
//! clock decides an outcome.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::task::Poll;
use std::thread;
use std::time::Instant;

use hyper_rt::RtError;
use hyper_rt::TaskId;
use hyper_rt::combine::join2;
use hyper_rt::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use hyper_rt::interests::Readiness;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::task::Outcome;

/// The one opaque handle accepted by this public driver adapter; no OS descriptor is accessed.
const HANDLE: i32 = 0;

struct State {
    stop: AtomicBool,
    started: AtomicBool,
    pending: AtomicBool,
    lost: AtomicBool,
    busy_loss: AtomicBool,
    rearm_refused: AtomicBool,
}

impl State {
    const fn new() -> Self {
        Self {
            stop: AtomicBool::new(false),
            started: AtomicBool::new(false),
            pending: AtomicBool::new(false),
            lost: AtomicBool::new(false),
            busy_loss: AtomicBool::new(false),
            rearm_refused: AtomicBool::new(false),
        }
    }
}

static LOST: State = State::new();
static PARTIAL: State = State::new();
static REARM: State = State::new();

#[derive(Clone, Copy)]
enum Fault {
    Lost,
    LostWithRead,
    Rearm,
}

struct FaultDriver {
    epoch: Instant,
    fault: Fault,
    state: &'static State,
    armed: Option<u64>,
    delivered: bool,
}

fn rearm_error() -> RtError {
    RtError::Capacity {
        what: "test remaining write readiness",
        bound: 0,
    }
}

impl Driver for FaultDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::Simulation
    }

    fn kick_handle(&self) -> Kick {
        Kick::None
    }

    fn now_ns(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn wait(&mut self, timeout: Option<u64>, out: &mut Vec<Completion>) -> Result<(), RtError> {
        let Some(tag) = self.armed else {
            // A harmless spurious return lets the threaded adapter accept control messages.
            thread::yield_now();
            return Ok(());
        };
        match self.fault {
            Fault::Lost | Fault::LostWithRead => {
                if matches!(self.fault, Fault::LostWithRead) {
                    out.push(Completion {
                        user_data: tag,
                        result: Readiness::READ.bits(),
                    });
                }
                self.state.lost.store(true, Ordering::Release);
                self.state
                    .busy_loss
                    .store(timeout == Some(0), Ordering::Release);
                Err(RtError::DriverLost)
            }
            Fault::Rearm => {
                if !self.delivered {
                    self.delivered = true;
                    out.push(Completion {
                        user_data: tag,
                        result: Readiness::READ.bits(),
                    });
                }
                Ok(())
            }
        }
    }

    fn submit_nop(&mut self, _word: u64) -> Result<(), RtError> {
        Ok(())
    }

    fn arm(&mut self, _raw: i32, _want: Readiness, tag: u64) -> Result<(), RtError> {
        if matches!(self.fault, Fault::Rearm) && self.delivered {
            self.state.rearm_refused.store(true, Ordering::Release);
            return Err(rearm_error());
        }
        self.armed = Some(tag);
        Ok(())
    }

    fn has_pending(&self) -> bool {
        self.armed.is_some()
    }
}

fn seed(fault: Fault, state: &'static State) -> DriverSeed {
    Box::new(move |_kick| {
        Ok(Box::new(FaultDriver {
            epoch: Instant::now(),
            fault,
            state,
            armed: None,
            delivered: false,
        }) as Box<dyn Driver>)
    })
}

fn config() -> RuntimeConfig {
    // The lifecycle fixture's one-millisecond quantum; no timing value drives fault order.
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 64,
        ring_entries: 16,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: 16,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

#[derive(Debug)]
enum Event {
    Busy(TaskId),
    Dropped(&'static str),
}

struct DropNotice(&'static str, Sender<Event>);

impl Drop for DropNotice {
    fn drop(&mut self) {
        let _ = self.1.send(Event::Dropped(self.0));
    }
}

/// Stops the busy sibling when the program ends, however it ends.
struct StopOnDrop(&'static State);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.stop.store(true, Ordering::Release);
    }
}

/// Awaits `future`, noting that it returned Pending at least once.
async fn observed<F: Future>(mut future: Pin<&mut F>, state: &'static State) -> F::Output {
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Ready(output) => Poll::Ready(output),
        Poll::Pending => {
            state.pending.store(true, Ordering::Release);
            Poll::Pending
        }
    })
    .await
}

type Delivered = (Result<(), RtError>, Option<Result<(), RtError>>);

async fn program(
    fault: Fault,
    state: &'static State,
    events: Sender<Event>,
) -> Result<Delivered, String> {
    let _root = DropNotice("root", events.clone());
    let _stop = StopOnDrop(state);
    let busy_drop = DropNotice("busy", events.clone());
    let busy = hyper_rt::futures::spawn(async move {
        let _drop = busy_drop;
        state.started.store(true, Ordering::Release);
        while !state.stop.load(Ordering::Acquire) {
            hyper_rt::futures::yield_now().await;
        }
    })
    .map_err(|error| format!("busy task admission: {error}"))?;
    let _ = events.send(Event::Busy(busy));
    while !state.started.load(Ordering::Acquire) {
        hyper_rt::futures::yield_now().await;
    }
    if matches!(fault, Fault::Rearm) {
        let mut both = pin!(join2(
            hyper_rt::readiness::readable(HANDLE),
            hyper_rt::readiness::writable(HANDLE),
        ));
        let (read, write) = observed(both.as_mut(), state).await;
        Ok((read, Some(write)))
    } else {
        let mut read = pin!(hyper_rt::readiness::readable(HANDLE));
        Ok((observed(read.as_mut(), state).await, None))
    }
}

fn drop_roles(events: &[Event]) -> Vec<&'static str> {
    let mut roles: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::Dropped(role) => Some(*role),
            Event::Busy(_) => None,
        })
        .collect();
    roles.sort_unstable();
    roles
}

fn lost_case(fault: Fault, state: &'static State) {
    let (events, received) = channel();
    let mut rt = LocalRuntime::with_driver(&config(), seed(fault, state), Kick::None).unwrap();
    let result = rt.block_on(program(fault, state, events));
    // Capture promised cleanup and terminal joins BEFORE Runtime Drop could hide a failure.
    let before_drop: Vec<_> = received.try_iter().collect();
    let joined = before_drop.iter().find_map(|event| match event {
        Event::Busy(id) => Some(rt.context().poll_join(*id, None)),
        Event::Dropped(_) => None,
    });
    drop(rt);
    assert!(
        state.pending.load(Ordering::Acquire),
        "readiness actually returned Pending"
    );
    assert!(
        state.lost.load(Ordering::Acquire),
        "the public driver actually returned DriverLost"
    );
    assert!(
        state.busy_loss.load(Ordering::Acquire),
        "loss occurred during a nonblocking busy harvest"
    );
    assert!(
        matches!(result, Err(RtError::ShardGone { .. })),
        "fatal busy driver loss: {result:?}"
    );
    assert_eq!(joined, Some(Poll::Ready(Ok(Outcome::Cancelled))));
    assert_eq!(drop_roles(&before_drop), ["busy", "root"]);
    // A fresh public owner still admits and completes work after the failed owner is retired.
    let mut healthy = LocalRuntime::with_driver(&config(), seed(fault, state), Kick::None).unwrap();
    assert_eq!(healthy.block_on(async { 42 }), Ok(42));
}

#[test]
fn block_on_loses_a_busy_driver_with_terminal_task_cleanup() {
    lost_case(Fault::Lost, &LOST);
}

#[test]
fn driver_loss_wins_over_a_partial_read_completion_before_the_root_finishes() {
    lost_case(Fault::LostWithRead, &PARTIAL);
}

#[test]
fn a_remaining_write_wait_receives_the_exact_rearm_refusal() {
    let (events, received) = channel();
    let mut rt =
        LocalRuntime::with_driver(&config(), seed(Fault::Rearm, &REARM), Kick::None).unwrap();
    let result = rt.block_on(program(Fault::Rearm, &REARM, events));
    let before_drop: Vec<_> = received.try_iter().collect();
    drop(rt);
    assert!(
        REARM.pending.load(Ordering::Acquire),
        "both readiness waits first returned Pending"
    );
    assert!(
        REARM.rearm_refused.load(Ordering::Acquire),
        "the public adapter refused rearming"
    );
    assert_eq!(result, Ok(Ok((Ok(()), Some(Err(rearm_error()))))));
    assert_eq!(drop_roles(&before_drop), ["busy", "root"]);
}
