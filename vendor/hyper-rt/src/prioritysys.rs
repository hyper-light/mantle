//! A thread's scheduling priority, lowered and read back: the OS-interface file of the compute pool's threads
//! ([`crate::compute`], docs/runtime.md §3.16; Kiln's runtime's `prioritysys.rs`, taken here, ORIGIN.md). Each
//! `unsafe` block states its invariant.
//!
//! The pool's threads go below the shards at a rank that keeps a share of the CPU against any other thread at
//! the process's priority: requests wait on the pool's jobs, and the OS's idle rank leaves them waiting on
//! whatever else busies the process's cgroup. Kiln measured both (its `benches/compute_wait.rs`, 2026-11-04):
//! beside one thread spinning on its core, a 5 ms job took 6.1 s at the median at the idle rank (`SCHED_IDLE`, a
//! weight of 3 against 1024) and 48 ms at the rank below, which the weights' 51.5 ms derives. So the rank below
//! is the one taken here, and the idle rank is not.
//!
//! - **Linux:** the batch policy at a nice value [`INCREMENT`] above the thread's own, both set by one
//!   `sched_setattr` on the calling thread [B: sched(7); sched_setattr(2)]: a weight of 110 against 1024
//!   (sched(7), "The nice value": each step a factor of about 1.25), the thread taken as CPU-bound and
//!   disfavoured at wakeup. Where the call is refused (a kernel before 3.14 has no `sched_setattr`; a seccomp
//!   policy may refuse it), the nice value alone by `setpriority` on the thread's id. A thread may lower its own
//!   priority unprivileged, and its policy and nice value are its own on Linux (sched(7)).
//! - **macOS:** the utility QoS class, `pthread_set_qos_class_self_np` [B: Apple, Energy Efficiency Guide for Mac
//!   Apps, "Prioritize Work at the Task Level"]: the system ranks a thread's CPU, I/O and timers by its class.
//!   Kiln measured on a 14-core Mac Studio a shard at the default class beside 13 spinning threads waking with a
//!   p99 lateness of 9.6 ms beside threads at its own class and 8.9 ms beside utility; 0.54 ms beside background,
//!   but tests that waited on background threads took 30 to 160 s on that machine where they took 5 s (its
//!   `benches/compute_priority.rs`, 2026-11-04).
//! - **Windows:** `THREAD_PRIORITY_BELOW_NORMAL` by `SetThreadPriority` [B: Microsoft Learn, "Scheduling
//!   Priorities": "Background threads, particularly those that are processor intensive, can be set to
//!   THREAD_PRIORITY_BELOW_NORMAL or THREAD_PRIORITY_LOWEST, to ensure that they can be preempted when
//!   necessary"].
//!
//! Each only ever lowers: a thread already below the target (a process started under `nice`, at background QoS,
//! at a lower level) keeps what it has, as `nice` adds its increment to what a process runs at.
#![allow(unsafe_code)]

use crate::compute::{Priority, Refusal};

/// How far the pool's threads' nice value goes above the thread's own on Linux: POSIX's default for `nice`,
/// "If -n increment is not specified, an increment of 10 shall be used" [B: IEEE Std 1003.1-2017, the `nice`
/// utility], the increment Kiln's pool takes.
#[cfg(target_os = "linux")]
pub(crate) const INCREMENT: i32 = 10;

/// Format: the least favourable nice value, "+19 (low priority)" [B: setpriority(2), sched(7)]; Linux's
/// `MAX_NICE`, past which `sched_setattr` refuses a value.
#[cfg(target_os = "linux")]
const MAX_NICE: i32 = 19;

/// Lowers the calling thread's priority below the shards as its OS allows (the module's notes): what it runs at
/// now, or the call the OS refused.
pub(crate) fn lower_current_thread() -> Result<Priority, Refusal> {
    platform::lower()
}

/// The calling thread's priority as its OS reports it.
pub(crate) fn of_current_thread() -> Priority {
    platform::of_current()
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{INCREMENT, MAX_NICE};
    use crate::compute::{Priority, Refusal};

    /// The batch policy at a nice value [`INCREMENT`] above the thread's own (at most [`MAX_NICE`]), and that
    /// nice value alone where the batch policy is refused: what the thread runs at then, as the OS reports it.
    pub(super) fn lower() -> Result<Priority, Refusal> {
        let thread = rustix::thread::gettid();
        let now = rustix::process::getpriority_process(Some(thread)).unwrap_or(0);
        let nice = now.saturating_add(INCREMENT).min(MAX_NICE);
        if set(libc::SCHED_BATCH, nice) {
            return Ok(of_current());
        }
        nice_alone(nice)
    }

    /// The calling thread's policy and nice value, set together by `sched_setattr`, its slice the kernel's
    /// default: whether the kernel took them.
    fn set(policy: libc::c_int, nice: i32) -> bool {
        let (Ok(size), Ok(policy)) = (
            u32::try_from(std::mem::size_of::<libc::sched_attr>()),
            u32::try_from(policy),
        ) else {
            return false;
        };
        let attr = libc::sched_attr {
            size,
            sched_policy: policy,
            sched_flags: 0,
            sched_nice: nice,
            sched_priority: 0,
            sched_runtime: 0,
            sched_deadline: 0,
            sched_period: 0,
        };
        let pid: libc::pid_t = 0;
        let flags: libc::c_uint = 0;
        // SAFETY: `attr` is a live, initialised `sched_attr` whose `size` states its own size, which the kernel
        // reads for the call's duration and does not keep; pid 0 names the calling thread and flags 0 asks
        // nothing more (sched_setattr(2)). No memory of this process is written.
        let done = unsafe { libc::syscall(libc::SYS_sched_setattr, pid, &raw const attr, flags) };
        done == 0
    }

    /// The calling thread's real-time priority, by `sched_getattr`: `None` where the kernel doesn't say.
    fn realtime_priority() -> Option<i32> {
        let size = u32::try_from(std::mem::size_of::<libc::sched_attr>()).ok()?;
        let mut attr = libc::sched_attr {
            size,
            sched_policy: 0,
            sched_flags: 0,
            sched_nice: 0,
            sched_priority: 0,
            sched_runtime: 0,
            sched_deadline: 0,
            sched_period: 0,
        };
        let pid: libc::pid_t = 0;
        let flags: libc::c_uint = 0;
        // SAFETY: `attr` is live, writable and `size` bytes long, the most the kernel writes, for the call's
        // duration only; pid 0 names the calling thread and flags 0 asks nothing more (sched_getattr(2)).
        let done =
            unsafe { libc::syscall(libc::SYS_sched_getattr, pid, &raw mut attr, size, flags) };
        if done == 0 {
            i32::try_from(attr.sched_priority).ok()
        } else {
            None
        }
    }

    /// The nice value alone, by `setpriority` on the calling thread's id (a thread's own on Linux, sched(7)).
    fn nice_alone(nice: i32) -> Result<Priority, Refusal> {
        let thread = rustix::thread::gettid();
        match rustix::process::setpriority_process(Some(thread), nice) {
            Ok(()) => Ok(of_current()),
            Err(error) => Err(Refusal {
                call: "setpriority",
                code: Some(error.raw_os_error()),
            }),
        }
    }

    /// The calling thread's policy (`sched_getscheduler`, its reset-on-fork flag set aside) and nice value
    /// (`getpriority` on its id), or its real-time priority in a real-time class.
    pub(super) fn of_current() -> Priority {
        let pid: libc::pid_t = 0;
        // SAFETY: no pointer passes; pid 0 names the calling thread, and the call only reads its policy
        // (sched_getscheduler(2)).
        let policy = unsafe { libc::sched_getscheduler(pid) } & !libc::SCHED_RESET_ON_FORK;
        if policy == libc::SCHED_RR || policy == libc::SCHED_FIFO {
            return realtime_priority().map_or(Priority::Unknown, |priority| Priority::Realtime {
                priority,
            });
        }
        let Ok(nice) = rustix::process::getpriority_process(Some(rustix::thread::gettid())) else {
            return Priority::Unknown;
        };
        match policy {
            libc::SCHED_IDLE => Priority::Idle { nice },
            libc::SCHED_BATCH => Priority::Batch { nice },
            _ => Priority::Other { nice },
        }
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use crate::compute::{Priority, QOS_CLASS_BACKGROUND, QOS_CLASS_UTILITY, Refusal};

    /// The utility QoS class, or background for a thread at utility already, so the pool's threads are below
    /// the thread that starts them. A thread at background or below keeps its own.
    pub(super) fn lower() -> Result<Priority, Refusal> {
        let now = match of_current() {
            Priority::Qos { class } => class,
            _ => 0,
        };
        if now != 0 && now <= QOS_CLASS_BACKGROUND {
            return Ok(Priority::Qos { class: now });
        }
        let (target, class) = if now == 0 || now > QOS_CLASS_UTILITY {
            (libc::qos_class_t::QOS_CLASS_UTILITY, QOS_CLASS_UTILITY)
        } else {
            (
                libc::qos_class_t::QOS_CLASS_BACKGROUND,
                QOS_CLASS_BACKGROUND,
            )
        };
        // SAFETY: no pointer passes; the call changes only the calling thread's QoS class, to a class
        // `sys/qos.h` defines, at no relative priority (pthread/qos.h).
        let code = unsafe { libc::pthread_set_qos_class_self_np(target, 0) };
        if code == 0 {
            Ok(Priority::Qos { class })
        } else {
            Err(Refusal {
                call: "pthread_set_qos_class_self_np",
                code: Some(code),
            })
        }
    }

    /// The calling thread's QoS class, as `pthread_get_qos_class_np` reports it.
    pub(super) fn of_current() -> Priority {
        let mut class: u32 = 0;
        let mut relative: libc::c_int = 0;
        // SAFETY: the calling thread's own handle and two live, writable outputs of the sizes the call writes:
        // a `qos_class_t` is an unsigned 32-bit value (`sys/qos.h`), read here as that integer so a class
        // the binding's enum doesn't name is a value, never an invalid enum.
        let code = unsafe {
            libc::pthread_get_qos_class_np(
                libc::pthread_self(),
                (&raw mut class).cast::<libc::qos_class_t>(),
                &raw mut relative,
            )
        };
        if code == 0 {
            Priority::Qos { class }
        } else {
            Priority::Unknown
        }
    }
}

#[cfg(windows)]
mod platform {
    use crate::compute::{Priority, Refusal};
    use windows_sys::Win32::System::Threading::{
        GetCurrentThread, GetThreadPriority, SetThreadPriority, THREAD_PRIORITY_BELOW_NORMAL,
    };

    /// Format: what `GetThreadPriority` returns when it fails, `THREAD_PRIORITY_ERROR_RETURN` (MAXLONG,
    /// winbase.h).
    const ERROR_RETURN: i32 = i32::MAX;

    /// `THREAD_PRIORITY_BELOW_NORMAL` for the calling thread, unless it runs at that level or below already.
    pub(super) fn lower() -> Result<Priority, Refusal> {
        if let Priority::Level { level } = of_current()
            && level <= THREAD_PRIORITY_BELOW_NORMAL
        {
            return Ok(Priority::Level { level });
        }
        // SAFETY: the calling thread's pseudo-handle, valid for the call; no pointer passes.
        let done = unsafe { SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_BELOW_NORMAL) };
        if done != 0 {
            Ok(Priority::Level {
                level: THREAD_PRIORITY_BELOW_NORMAL,
            })
        } else {
            Err(Refusal {
                call: "SetThreadPriority",
                code: std::io::Error::last_os_error().raw_os_error(),
            })
        }
    }

    /// The calling thread's priority level, as `GetThreadPriority` reports it.
    pub(super) fn of_current() -> Priority {
        // SAFETY: the calling thread's pseudo-handle, valid for the call; no pointer passes.
        let level = unsafe { GetThreadPriority(GetCurrentThread()) };
        if level == ERROR_RETURN {
            Priority::Unknown
        } else {
            Priority::Level { level }
        }
    }
}

/// An OS this file has no call for: the thread keeps the process's priority.
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
mod platform {
    use crate::compute::{Priority, Refusal};

    pub(super) fn lower() -> Result<Priority, Refusal> {
        Err(Refusal {
            call: "no priority call on this OS",
            code: None,
        })
    }

    pub(super) fn of_current() -> Priority {
        Priority::Unknown
    }
}
