//! Linux: the process's thread count from the tasks in /proc/self/task whose kernel flags do not
//! say they are exiting (proc_pid_stat(5), `PF_EXITING`), the pool ceiling from the smaller of /proc/sys/kernel/threads-max, which the kernel sets so
//! that thread structures fill at most an eighth of RAM, and the soft `RLIMIT_NPROC`
//! (getrlimit(2); research/26 §1.5), and a thread's CPU time from `CLOCK_THREAD_CPUTIME_ID`
//! (clock_gettime(2)).

use std::io;
use std::time::Duration;

fn invalid(what: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what)
}

/// The kernel's flag on a task that has begun to exit (include/linux/sched.h `PF_EXITING`). `do_exit`
/// sets it (`exit_signals`) before it clears the thread's tid and wakes its joiner (`exit_mm`,
/// `mm_release`, the futex `pthread_join` waits on), and the thread leaves the count in
/// /proc/self/status's `Threads:` only later (`release_task`, `__exit_signal`; kernel/exit.c). So a
/// thread already joined is still in `Threads:` for a while: 81,088 of 200,000 joins on adacachy
/// (Linux 7.2.9, Ryzen 9 9950X3D2, 2026-11-11) read it there at once, and none with this flag left out.
const PF_EXITING: u64 = 0x0000_0004;

/// The threads the process runs: the tasks /proc/self/task lists whose flags, the ninth field of
/// each one's `stat` (proc_pid_stat(5)), do not carry [`PF_EXITING`]. A task gone between the listing
/// and its read is exiting, and is not counted.
pub(super) fn count() -> io::Result<usize> {
    let mut live = 0usize;
    for entry in std::fs::read_dir("/proc/self/task")? {
        let mut path = entry?.path();
        path.push("stat");
        let Ok(stat) = std::fs::read_to_string(&path) else {
            continue;
        };
        if !exiting(&stat)? {
            live = live
                .checked_add(1)
                .ok_or_else(|| invalid("more threads than a count holds"))?;
        }
    }
    Ok(live)
}

/// Whether a task's `stat` text says it is exiting. Its second field, the command name in
/// parentheses, may itself hold spaces and parentheses, so the fields are read after the last `)`:
/// state, ppid, pgrp, session, tty_nr, tpgid, then flags (proc_pid_stat(5) fields 3 to 9).
fn exiting(stat: &str) -> io::Result<bool> {
    let flags: u64 = stat
        .rsplit_once(')')
        .and_then(|(_, rest)| rest.split_whitespace().nth(6))
        .and_then(|f| f.parse().ok())
        .ok_or_else(|| invalid("a task's stat names no flags"))?;
    Ok(flags & PF_EXITING != 0)
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
