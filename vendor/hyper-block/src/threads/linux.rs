//! Linux: the process's thread count from `Threads:` in /proc/self/status (proc_pid_status(5)),
//! the pool ceiling from the smaller of /proc/sys/kernel/threads-max, which the kernel sets so
//! that thread structures fill at most an eighth of RAM, and the soft `RLIMIT_NPROC`
//! (getrlimit(2); research/26 §1.5), and a thread's CPU time from `CLOCK_THREAD_CPUTIME_ID`
//! (clock_gettime(2)). A foreground thread's time slice is set through sched_setattr(2).
#![cfg_attr(target_os = "linux", allow(unsafe_code))]

use std::io;
use std::time::Duration;

fn invalid(what: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what)
}

pub(super) fn count() -> io::Result<usize> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("Threads:"))
        .and_then(|n| n.trim().parse().ok())
        .ok_or_else(|| invalid("/proc/self/status names no thread count"))
}

pub(super) fn ceiling() -> io::Result<usize> {
    let max: usize = std::fs::read_to_string("/proc/sys/kernel/threads-max")?
        .trim()
        .parse()
        .map_err(|_| invalid("kernel.threads-max is not a count"))?;
    let nproc = rustix::process::getrlimit(rustix::process::Resource::Nproc)
        .current
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(usize::MAX);
    Ok(max.min(nproc))
}

pub(super) fn thread_cpu() -> io::Result<Duration> {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    let secs = u64::try_from(t.tv_sec).map_err(|_| invalid("a negative CPU time"))?;
    let nanos = u32::try_from(t.tv_nsec).map_err(|_| invalid("a negative CPU time"))?;
    Ok(Duration::new(secs, nanos))
}

/// `struct sched_attr` of <linux/sched/types.h>, `SCHED_ATTR_SIZE_VER1`, field for field.
#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Default)]
struct SchedAttr {
    size: u32,
    policy: u32,
    flags: u64,
    nice: i32,
    priority: u32,
    runtime: u64,
    deadline: u64,
    period: u64,
    util_min: u32,
    util_max: u32,
}

/// `SCHED_OTHER` (sched(7)): the fair class, the only one whose slice this sets.
#[cfg(target_os = "linux")]
const SCHED_OTHER: u32 = 0;

/// Cited: the shortest slice the fair scheduler takes from `sched_runtime`, which it clamps to
/// 0.1-100 ms (kernel/sched/fair.c, Linux 6.12, commit 857b158dc5e8: a slice suggestion of the
/// thread's run at a high percentile; a request's run on a shard is microseconds).
#[cfg(target_os = "linux")]
const FOREGROUND_SLICE_NS: u64 = 100_000;

/// The calling thread's fair-class slice at its shortest, its policy and nice value kept: a
/// thread in another class is left as it is. A kernel before 6.12 takes no slice from it.
#[cfg(target_os = "linux")]
pub(super) fn set_foreground() -> io::Result<bool> {
    let mut attr = SchedAttr::default();
    let size =
        u32::try_from(size_of::<SchedAttr>()).map_err(|_| invalid("a sched_attr past u32"))?;
    // SAFETY: sched_getattr(2) writes at most `size` bytes, the size of `attr`, into `attr` for the
    // calling thread (pid 0); `attr` outlives the call.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_sched_getattr,
            0,
            std::ptr::from_mut(&mut attr),
            size,
            0,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    if attr.policy != SCHED_OTHER {
        return Ok(false);
    }
    attr.size = size;
    attr.flags = 0;
    attr.runtime = FOREGROUND_SLICE_NS;
    // SAFETY: sched_setattr(2) reads `attr`, whose size field names its size, for the calling
    // thread (pid 0); `attr` outlives the call.
    let rc = unsafe { libc::syscall(libc::SYS_sched_setattr, 0, std::ptr::from_ref(&attr), 0) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(true)
}

/// Android sets no slice here.
#[cfg(target_os = "android")]
pub(super) fn set_foreground() -> io::Result<bool> {
    Ok(false)
}
