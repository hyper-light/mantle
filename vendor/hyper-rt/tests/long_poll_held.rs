//! A long poll is one that held its shard past twice the step quantum since the shard last let it go on
//! (docs/runtime.md §3.11): from its start, or from its last `ShardContext::wanted` that answered no. A
//! task whose turns run under twice the quantum, or that asks between operations and goes on only when
//! told it may, is never its own long poll however long its polls run; a task that holds a wanted shard
//! past twice the quantum is. It runs where the runtime keeps a fine thread clock to hold by (Linux, macOS:
//! `attribution::thread_account`): a poll held off the CPU by the host is the host's, never the task's.
#![cfg(any(target_os = "linux", target_os = "macos"))]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper_rt::futures::maybe_yield;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

/// Shape: the step quantum, a millisecond, far past any wake, so only a poll's own hold passes it.
const QUANTUM_NS: u64 = 1_000_000;
/// Shape: a cooperative turn on the CPU, one and a half quanta: past the quantum, under twice it.
const TURN: Duration = Duration::from_micros(1_500);
/// Shape: a hold that breaks the contract, three quanta: past twice the quantum on the CPU.
const HOLD: Duration = Duration::from_millis(3);
/// Shape: the turns a task takes; any count above one tells a poll's hold from a turn's.
const TURNS: u64 = 8;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 16,
        ring_entries: 16,
        step_budget_ns: QUANTUM_NS,
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

/// The thread's CPU time, as the runtime reads it: on macOS libc's clock restated at the kernel's rate,
/// which a process Rosetta 2 translates is not shown (`hyper_rt::machine::clock::MachTicks`).
fn thread_time() -> Duration {
    let reading = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    let read = Duration::new(
        u64::try_from(reading.tv_sec).unwrap(),
        u32::try_from(reading.tv_nsec).unwrap(),
    );
    #[cfg(target_os = "macos")]
    let read = Duration::from_nanos(
        hyper_rt::machine::clock::mach_ticks()
            .unwrap()
            .at_kernel_rate(u64::try_from(read.as_nanos()).unwrap()),
    );
    read
}

/// Runs `span` on the thread's CPU clock.
fn hold_on_cpu(span: Duration) {
    let began = thread_time();
    while thread_time().saturating_sub(began) < span {
        std::hint::spin_loop();
    }
}

/// Each poll runs [`TURN`] on the CPU and wakes itself, until its last, which is ready at once.
struct Turns {
    left: u64,
}

impl Future for Turns {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.left == 0 {
            return Poll::Ready(());
        }
        self.left -= 1;
        hold_on_cpu(TURN);
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue or an eventfd, which Miri does not model
fn a_turn_under_twice_the_quantum_is_not_its_tasks_long_poll() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.spawn(Turns { left: TURNS }).unwrap();
    while rt.step().did_work {}
    let c = rt.counters();
    assert!(c.polls > TURNS, "every turn a poll: {c:?}");
    // A turn the host held past twice the quantum is the host's; on the CPU no turn passes it.
    assert_eq!(c.long_steps, 0, "{c:?}");
}

#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue or an eventfd, which Miri does not model
fn a_task_let_go_on_is_judged_from_then() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    // A first long poll arms the attribution, unattributed for want of a window; the next step's start opens
    // one, so a long poll after it is judged, and one held on the CPU is its task's.
    rt.spawn(async { hold_on_cpu(HOLD) }).unwrap();
    while rt.step().did_work {}
    let armed = rt.counters();
    rt.spawn(async {
        for _ in 0..TURNS {
            hold_on_cpu(TURN);
            // Alone on its shard: told no each time, so it goes on, and the shard's verdict starts here.
            maybe_yield().await;
        }
    })
    .unwrap();
    while rt.step().did_work {}
    let c = rt.counters();
    assert_eq!(
        c.polls - armed.polls,
        1,
        "never wanted, never yielded: {c:?}"
    );
    assert_eq!(
        c.long_steps, armed.long_steps,
        "held under twice the quantum since each go-on: {c:?}"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue or an eventfd, which Miri does not model
fn a_task_that_holds_a_wanted_shard_past_twice_the_quantum_is_long() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        // A task listed: the shard is wanted from here, and this one holds it regardless.
        hyper_rt::futures::spawn(async {}).unwrap();
        assert!(hyper_rt::registry::with_current(|ctx| ctx.wanted()).unwrap());
        hold_on_cpu(HOLD);
    })
    .unwrap();
    let c = rt.counters();
    assert_eq!(
        c.long_steps + c.preempted_steps + c.unattributed_steps,
        1,
        "the one poll that held a wanted shard: {c:?}"
    );
}
