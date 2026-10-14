//! A step's batch reads the shard's inputs again (wakes from other threads, timers, the driver's
//! completions) once its polls have run its latency budget since it last read them (docs/runtime.md
//! §3.3), and goes on to its count. The count is the budget over a trivial poll's cost, so polls that ran
//! longer made a step of `batch` polls last `batch` times the budget, while a wake from another thread
//! waited in the shard's bitmap for the step to end. The budget is the step quantum, or the consumer's
//! latency objective where that is tighter.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::cell::Cell;
use std::future::poll_fn;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::task::{Poll, Waker};

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

thread_local! {
    /// The test's clock: each test steps its shard on its own thread, and its polls advance it.
    static CLOCK_NS: Cell<u64> = const { Cell::new(0) };
}

/// A driver whose clock is the test's.
struct SteppedDriver;

impl Driver for SteppedDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::Simulation
    }
    fn kick_handle(&self) -> Kick {
        Kick::None
    }
    fn now_ns(&self) -> u64 {
        CLOCK_NS.with(Cell::get)
    }
    fn wait(&mut self, _timeout: Option<u64>, _out: &mut Vec<Completion>) -> Result<(), RtError> {
        Ok(())
    }
    fn submit_nop(&mut self, _word: u64) -> Result<(), RtError> {
        Ok(())
    }
    fn arm(
        &mut self,
        _raw: i32,
        _want: hyper_rt::interests::Readiness,
        _tag: u64,
    ) -> Result<(), RtError> {
        Err(RtError::BadConfig {
            what: "the batch fixture registers no native I/O",
        })
    }
    fn has_pending(&self) -> bool {
        false
    }
}

/// One shard with room for `batch` polls a step, a fixed quantum and the consumer's `objective`.
fn owner(tasks: usize, batch: usize, quantum_ns: u64, objective: Option<u64>) -> LocalRuntime {
    let config = RuntimeConfig {
        shards: 1,
        tasks_per_shard: tasks,
        timers_per_shard: tasks,
        interests_per_shard: tasks,
        ring_entries: tasks,
        step_budget_ns: quantum_ns,
        timer_tick_ns: quantum_ns,
        batch,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: objective,
    };
    let seed: DriverSeed = Box::new(|_| Ok(Box::new(SteppedDriver) as Box<dyn Driver>));
    LocalRuntime::with_driver(&config, seed, Kick::None).unwrap()
}

#[derive(Debug, PartialEq, Eq)]
enum Fact {
    Computer,
    Waiter,
}

/// A waiter, pending until its waker is woken from another thread; then a request and its reply handed
/// between two tasks of the shard, each poll spending `poll_ns` of the clock and listing the other, the
/// first poll waking the waiter from another thread.
fn spawn_handoff(rt: &mut LocalRuntime, poll_ns: u64, facts: &Sender<Fact>) {
    let (wakers, handed) = channel::<Waker>();
    let waiter = facts.clone();
    rt.spawn(async move {
        loop {
            waiter.send(Fact::Waiter).unwrap();
            let mut handed_over = false;
            poll_fn(|cx| {
                if handed_over {
                    return Poll::Ready(());
                }
                handed_over = true;
                wakers.send(cx.waker().clone()).unwrap();
                Poll::Pending
            })
            .await;
        }
    })
    .unwrap();
    let (requests, mut offered) = hyper_rt::sync::channel::<u64>(1).unwrap();
    let (replies, mut answered) = hyper_rt::sync::channel::<u64>(1).unwrap();
    let asker = facts.clone();
    rt.spawn(async move {
        let mut first = true;
        loop {
            asker.send(Fact::Computer).unwrap();
            CLOCK_NS.with(|clock| clock.set(clock.get().saturating_add(poll_ns)));
            if std::mem::take(&mut first) {
                let waker = handed.try_recv().unwrap();
                std::thread::spawn(move || waker.wake()).join().unwrap();
            }
            requests.try_send(1).unwrap();
            answered.recv().await.unwrap();
        }
    })
    .unwrap();
    let answerer = facts.clone();
    rt.spawn(async move {
        loop {
            offered.recv().await.unwrap();
            answerer.send(Fact::Computer).unwrap();
            CLOCK_NS.with(|clock| clock.set(clock.get().saturating_add(poll_ns)));
            replies.try_send(1).unwrap();
        }
    })
    .unwrap();
}

/// The polls of one step.
fn step(rt: &mut LocalRuntime, observed: &Receiver<Fact>) -> Vec<Fact> {
    assert!(rt.step().did_work);
    observed.try_iter().collect()
}

use Fact::{Computer, Waiter};

#[test]
fn a_batch_reads_its_inputs_again_once_its_polls_spend_the_quantum() {
    // Room for eight polls a step; each poll of the handoff spends the whole quantum. The asker's first
    // poll wakes the waiter from another thread; the answerer's first lists the asker, and the inputs are
    // read after it: the wake goes behind the asker's next turn, within the step.
    let mut rt = owner(3, 8, 1_000, None);
    let (facts, observed) = channel();
    spawn_handoff(&mut rt, 1_000, &facts);
    let first = step(&mut rt, &observed);
    assert_eq!(&first[..5], [Waiter, Computer, Computer, Computer, Waiter]);
    assert_eq!(first.len(), 8, "the step goes on to its count");
}

#[test]
fn polls_within_the_quantum_leave_the_inputs_to_the_next_step() {
    // Polls of a tenth of the quantum: eight run within it, so the wake waits for the next step.
    let mut rt = owner(3, 8, 1_000, None);
    let (facts, observed) = channel();
    spawn_handoff(&mut rt, 100, &facts);
    assert_eq!(
        step(&mut rt, &observed),
        [
            Waiter, Computer, Computer, Computer, Computer, Computer, Computer, Computer
        ]
    );
    assert_eq!(&step(&mut rt, &observed)[..2], [Computer, Waiter]);
}

#[test]
fn an_objective_tighter_than_the_quantum_reads_the_inputs_at_it() {
    // A quantum of ten polls, an objective of one: the inputs are read after each turn of the handoff.
    let mut rt = owner(3, 8, 10_000, Some(1_000));
    let (facts, observed) = channel();
    spawn_handoff(&mut rt, 1_000, &facts);
    let first = step(&mut rt, &observed);
    assert_eq!(&first[..5], [Waiter, Computer, Computer, Computer, Waiter]);
}

#[test]
fn polls_that_list_nothing_leave_the_inputs_to_the_steps_end() {
    // Three tasks each spend the quantum once and wait, listing nothing: a wake from another thread during
    // the first goes behind the other two whenever it is read, so the step reads it at its end, not between
    // them (a drain is a read-modify-write on lines the wakers write too).
    let mut rt = owner(4, 8, 1_000, None);
    let (facts, observed) = channel();
    let (wakers, handed) = channel::<Waker>();
    let waiter = facts.clone();
    rt.spawn(async move {
        loop {
            waiter.send(Fact::Waiter).unwrap();
            let mut handed_over = false;
            poll_fn(|cx| {
                if handed_over {
                    return Poll::Ready(());
                }
                handed_over = true;
                wakers.send(cx.waker().clone()).unwrap();
                Poll::Pending
            })
            .await;
        }
    })
    .unwrap();
    // The first of the three takes the waiter's waker and wakes it from another thread.
    let mut handed = Some(handed);
    for _ in 0..3 {
        let facts = facts.clone();
        let handed = handed.take();
        rt.spawn(async move {
            facts.send(Fact::Computer).unwrap();
            CLOCK_NS.with(|clock| clock.set(clock.get().saturating_add(1_000)));
            if let Some(waker) = handed.and_then(|handed| handed.try_recv().ok()) {
                std::thread::spawn(move || waker.wake()).join().unwrap();
            }
            std::future::pending::<()>().await;
        })
        .unwrap();
    }
    assert_eq!(
        step(&mut rt, &observed),
        [Waiter, Computer, Computer, Computer]
    );
    assert_eq!(step(&mut rt, &observed), [Waiter]);
}
