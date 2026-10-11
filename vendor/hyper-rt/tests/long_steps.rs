//! The attribution of a shard's long steps (§4.3; A-31;
//! docs/bugs/2026-09-25-wake-estimate-frozen-at-boot-and-preemptions-counted-as-long-steps.md). A step past
//! its bound (the step budget and one quantum) by the wall clock is attributed in a window of the thread's
//! account: its tasks' when it ran past the bound on the CPU or blocked in a call, the host's when it was
//! runnable and held off the CPU, unattributed where the platform cannot tell (the first long step opens the
//! first window). Each held poll below is a step of its own (a batch of one), so a step's attribution is its
//! poll's.

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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

/// Shape: the fixed quantum of the long-step test — a millisecond, far above a poll's own cost.
const QUANTUM_NS: u64 = 1_000_000;
/// Shape: how long a long poll holds its thread, three quanta: past a step's bound (the budget and one
/// quantum, two) by any clock.
const HOLD: Duration = Duration::from_millis(3);
/// Shape: polls each long-step history runs in one busy period.
const LONG_POLLS: u64 = 3;

fn config(step_budget_ns: u64) -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        // No idle spin: every wait below is a park.
        spin_ns: 0,
        wake_tracking: None,
    }
}

/// Steps the shard until it has nothing to do.
fn step_until_idle(rt: &mut LocalRuntime) {
    while rt.step().did_work {}
}

/// How a held poll spends its hold.
#[derive(Clone, Copy, Debug)]
enum Hold {
    /// Busy on the CPU until the thread has run [`HOLD`] of CPU time: a task's own long step.
    OnCpu,
    /// Asleep in the kernel for [`HOLD`]: a task blocked in a call.
    InCall,
    /// Yielding the CPU to a runnable competitor until [`HOLD`] has passed: a runnable thread the host
    /// keeps off its CPU, as a preemption does.
    #[cfg(target_os = "linux")]
    Runnable,
}

/// The calling thread's CPU time where the platform keeps a fine one, else the wall clock since `since`
/// (a platform with no per-thread clock attributes nothing, so the hold's exact nature does not matter).
fn thread_time(since: Instant) -> Duration {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let _ = since;
        let reading = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
        Duration::new(
            u64::try_from(reading.tv_sec).unwrap(),
            u32::try_from(reading.tv_nsec).unwrap(),
        )
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        since.elapsed()
    }
}

thread_local! {
  /// The held polls whose own thread CPU clock ran past the quantum: on a busy virtual machine a guest
  /// may count time its virtual CPU was stolen while the thread ran as the thread's CPU (the macOS CI
  /// runner, run 36289513559: a 3 ms sleep's poll counted past a 1 ms quantum), and then the rule —
  /// past the quantum on the thread clock is the task's — rightly calls that poll the task's. The
  /// shard runs on this thread, so a thread-local sees every poll.
  static CPU_PAST_QUANTUM: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Notes in [`CPU_PAST_QUANTUM`] a poll whose thread CPU, from its first to its last instruction
/// (`cpu_began` to now), ran past the quantum.
fn note_cpu(began: Instant, cpu_began: Duration) {
    if thread_time(began).saturating_sub(cpu_began) > Duration::from_nanos(QUANTUM_NS) {
        CPU_PAST_QUANTUM.with(|count| count.set(count.get() + 1));
    }
}

/// A future whose every poll holds the thread for [`HOLD`] as `hold` says, then wakes itself, so its
/// polls run back to back — one per step — in one busy period with no wait between them.
struct HoldEachPoll {
    polls_left: u64,
    hold: Hold,
}

impl Future for HoldEachPoll {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.polls_left == 0 {
            return Poll::Ready(());
        }
        self.polls_left -= 1;
        let began = Instant::now();
        let poll_cpu_began = thread_time(began);
        match self.hold {
            Hold::OnCpu => {
                let cpu_began = thread_time(began);
                while thread_time(began).saturating_sub(cpu_began) < HOLD {
                    std::hint::spin_loop();
                }
            }
            // A test may sleep (the lint's stated exception): a sleep is a call the thread blocks in.
            #[allow(clippy::disallowed_methods)]
            Hold::InCall => std::thread::sleep(HOLD),
            #[cfg(target_os = "linux")]
            Hold::Runnable => {
                while began.elapsed() < HOLD {
                    std::thread::yield_now();
                }
            }
        }
        cx.waker().wake_by_ref();
        // The whole poll's CPU, measured as late as the poll can: the shard's window closes on a reading
        // taken just after it returns.
        note_cpu(began, poll_cpu_began);
        Poll::Pending
    }
}

/// Runs one busy period of [`LONG_POLLS`] held polls, one a step, and returns the shard's counters.
fn one_busy_period(hold: Hold) -> hyper_rt::shard_loop::Counters {
    CPU_PAST_QUANTUM.with(|count| count.set(0));
    let mut rt = LocalRuntime::new(&RuntimeConfig {
        batch: 1,
        ..config(QUANTUM_NS)
    })
    .unwrap();
    rt.spawn(HoldEachPoll {
        polls_left: LONG_POLLS,
        hold,
    })
    .unwrap();
    step_until_idle(&mut rt);
    let counters = rt.counters();
    let hold_ns = u64::try_from(HOLD.as_nanos()).unwrap();
    assert!(
        counters.longest_step_ns >= hold_ns,
        "the wall clock saw each held poll: {counters:?}"
    );
    counters
}

/// A shard's long steps as (the tasks', of those blocked, the host's, unattributed).
fn attributed(counters: &hyper_rt::shard_loop::Counters) -> (u64, u64, u64, u64) {
    (
        counters.long_steps,
        counters.blocked_steps,
        counters.preempted_steps,
        counters.unattributed_steps,
    )
}

/// Whether this platform keeps a fine per-thread CPU clock the shard attributes by.
const HAS_THREAD_CLOCK: bool = cfg!(any(target_os = "linux", target_os = "macos"));

/// §4.3, A-31: a poll that ran past the quantum on the CPU is its task's. The first long poll of a busy
/// period has no window and goes unattributed (it arms the shard); every later one is judged in a window
/// its step opened. With no per-thread clock (Windows) none is attributed.
#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue or an eventfd, which Miri does not model
fn a_poll_busy_on_the_cpu_past_the_quantum_is_its_tasks() {
    let counters = one_busy_period(Hold::OnCpu);
    let expected = if HAS_THREAD_CLOCK {
        (LONG_POLLS - 1, 0, 0, 1)
    } else {
        (0, 0, 0, LONG_POLLS)
    };
    assert_eq!(attributed(&counters), expected, "{counters:?}");
}

/// §4.3, A-31: a poll that blocked in a call past the quantum is its task's — the shard stalled on it just
/// the same — where the platform counts a thread's voluntary switches (Linux). macOS cannot tell a block
/// from a preemption, so there it is unattributed, as it is everywhere with no per-thread clock. Every
/// long poll is attributed once, and never to the host (nothing held the thread off the CPU but its own
/// call). A poll whose own thread clock ran past the quantum (a guest counting stolen time as the
/// thread's) is its task's by the rule, as a long run: the test counts those polls and allows exactly
/// that many to move from blocked or unattributed to the task's long polls.
#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue or an eventfd, which Miri does not model
fn a_poll_blocked_in_a_call_past_the_quantum_is_its_tasks_where_blocks_are_counted() {
    let counters = one_busy_period(Hold::InCall);
    let clocked_long = CPU_PAST_QUANTUM.with(std::cell::Cell::get);
    let (long, blocked, preempted, unattributed) = attributed(&counters);
    assert_eq!(
        long + unattributed,
        LONG_POLLS,
        "each long poll attributed once: {counters:?}"
    );
    assert_eq!(preempted, 0, "a block is never the host's: {counters:?}");
    if cfg!(target_os = "linux") {
        // The first long poll opens the first window; every later one is the task's, blocked unless its
        // own clock ran past the quantum.
        assert_eq!((long, unattributed), (LONG_POLLS - 1, 1), "{counters:?}");
        assert!(
            blocked + clocked_long >= LONG_POLLS - 1,
            "a later poll is blocked unless its clock ran long ({clocked_long}): {counters:?}"
        );
    } else {
        assert_eq!(
            blocked, 0,
            "no platform but Linux counts blocks: {counters:?}"
        );
        assert!(
            long <= clocked_long,
            "a blocked poll is the task's only where its own clock ran past the quantum \
       ({clocked_long}): {counters:?}"
        );
    }
}

/// Restores the calling thread's CPU affinity on drop, so a failed assertion leaves the test thread as
/// it found it.
#[cfg(target_os = "linux")]
struct RestoreAffinity(rustix::thread::CpuSet);

#[cfg(target_os = "linux")]
impl Drop for RestoreAffinity {
    fn drop(&mut self) {
        let _ = rustix::thread::sched_setaffinity(None, &self.0);
    }
}

/// §4.3, A-31: a runnable poll the host keeps off its CPU past the quantum is not its task's. The shard
/// thread and a spinning competitor are pinned to one CPU, and each poll yields to it until the hold has
/// passed: Linux counts those switches involuntary, as it does a preemption, and the poll's own CPU stays
/// within the quantum. Linux only: macOS will not pin, and cannot tell this from a block anyway.
#[cfg(target_os = "linux")]
#[test]
#[cfg_attr(miri, ignore)] // the OS driver opens a kqueue or an eventfd, which Miri does not model
fn a_poll_the_host_kept_off_its_cpu_is_not_its_tasks() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let saved = rustix::thread::sched_getaffinity(None).unwrap();
    let cpu = rustix::thread::sched_getcpu();
    let mut one = rustix::thread::CpuSet::new();
    one.set(cpu);
    if let Err(refusal) = rustix::thread::sched_setaffinity(None, &one) {
        eprintln!(
            "SKIP a_poll_the_host_kept_off_its_cpu_is_not_its_tasks: pinning refused ({refusal})"
        );
        return;
    }
    let _restore = RestoreAffinity(saved);
    let running = AtomicBool::new(false);
    let stop = AtomicBool::new(false);
    let counters = std::thread::scope(|scope| {
        scope.spawn(|| {
            rustix::thread::sched_setaffinity(None, &one).unwrap();
            running.store(true, Ordering::Release);
            while !stop.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
        });
        while !running.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        let counters = one_busy_period(Hold::Runnable);
        stop.store(true, Ordering::Release);
        counters
    });
    assert_eq!(
        attributed(&counters),
        (0, 0, LONG_POLLS - 1, 1),
        "{counters:?}"
    );
}
