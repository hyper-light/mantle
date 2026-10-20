//! A shard whose every poll runs past the step quantum on the CPU judges its long polls by step: one
//! reading of the thread's account at a step's end judges all of the step's long polls
//! (`attribution::Tracker::by_step`), where every long poll was a reading of its own and every step's
//! start another. Found on mantle's ranges bench: a seek's poll ran 4–7 µs against a 2.5–3.3 µs quantum,
//! and the shard read its thread's CPU clock 727,739 times in 1,000,000 seeks, 1.6–2.9% of the shard's
//! time. The attribution stays what each poll's own window gives (`tests/wake_estimate.rs`). It runs
//! where the runtime keeps a fine thread clock to judge by (Linux, macOS: `attribution::thread_account`).
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

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

/// Shape: the step quantum, a millisecond, far past any wake, so only a poll's own hold passes it.
const QUANTUM_NS: u64 = 1_000_000;
/// Shape: what each poll runs on its own thread clock, three quanta: past twice the quantum (a long poll,
/// docs/runtime.md §3.11) whatever holds the thread off it.
const HOLD: Duration = Duration::from_millis(3);
/// Shape: the tasks a step polls, its poll budget.
const TASKS: usize = 4;
/// Shape: the polls each task holds the CPU in.
const POLLS: u64 = 8;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 16,
        ring_entries: 16,
        step_budget_ns: QUANTUM_NS,
        timer_tick_ns: 100_000,
        batch: TASKS,
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

/// Each poll runs [`HOLD`] on the thread's CPU clock and wakes itself, until its last, which is ready
/// at once.
struct HoldOnCpu {
    polls_left: u64,
}

impl Future for HoldOnCpu {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.polls_left == 0 {
            return Poll::Ready(());
        }
        self.polls_left -= 1;
        let began = thread_time();
        while thread_time().saturating_sub(began) < HOLD {
            std::hint::spin_loop();
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Every long poll is attributed once, and the task's (on the CPU past the quantum) unless a window
/// left it undecided; the readings are one a step's end, and each beyond it is paid for by a verdict a
/// window left undecided (at most three: a step's start, a poll's own end, and the step's end) or a
/// wait's end.
#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue or an eventfd, which Miri does not model
fn a_shard_of_long_polls_on_the_cpu_reads_its_thread_once_a_step() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    for _ in 0..TASKS {
        rt.spawn(HoldOnCpu { polls_left: POLLS }).unwrap();
    }
    while rt.step().did_work {}
    let c = rt.counters();
    let long_polls = TASKS as u64 * POLLS;
    assert_eq!(
        c.long_steps + c.preempted_steps + c.unattributed_steps,
        long_polls,
        "each long poll attributed once: {c:?}"
    );
    assert_eq!(
        (c.blocked_steps, c.preempted_steps),
        (0, 0),
        "a run on the CPU is never a block or the host's: {c:?}"
    );
    assert!(
        c.thread_accounts <= c.steps + c.waits + 3 * c.unattributed_steps,
        "a reading a step, and each beyond it paid for by an undecided verdict: {c:?}"
    );
    assert!(
        c.thread_accounts < long_polls,
        "fewer readings than long polls: {c:?}"
    );
}
