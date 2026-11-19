//! The descriptors a process may hold open at once (CLAUDE.md §2, §5): raised at its start to the
//! most the OS lets it take without privilege, then read back, which is the bound every holder of a
//! descriptor a peer, a client or a request is derived within (node.md §1.4).
//!
//! - Linux and the other Unixes: the soft `RLIMIT_NOFILE`, raised to the hard one, as "an
//!   unprivileged process may set only its soft limit to a value in the range from 0 up to the hard
//!   limit" (getrlimit(2), Linux man-pages). A refusal (a sandbox's) leaves the limit as it stands,
//!   and the read-back says it.
//! - macOS: the same, with the kernel's own ceiling `kern.maxfilesperproc` (sysctl(3)) beside it:
//!   "The real NOFILE limits enforced by the kernel is capped at MIN(RLIMIT_NOFILE,
//!   maxfilesperproc)" (XNU `bsd/kern/kern_resource.c`, `dosetrlimit`; `proc_limitgetcur_nofile`),
//!   whatever the soft limit says, and `setrlimit` "no longer accepts "rlim_cur = RLIM_INFINITY"
//!   for RLIM_NOFILE. Use "rlim_cur = min(OPEN_MAX, rlim_max)"" (getrlimit(2), macOS,
//!   COMPATIBILITY). The soft limit is raised to the smaller of the hard limit and the ceiling, and
//!   the bound read back is the smaller of the soft limit and the ceiling.
//! - Windows: nothing to raise. The C runtime's limit (`_setmaxstdio`) bounds only its own streams
//!   and low-level descriptors, which Rust's std does not use: its files are handles and its
//!   sockets Winsock's, both kernel objects, and "The per-process limit on kernel handles is 2^24"
//!   (Microsoft Learn, Kernel Objects). That is the bound, though "handles are stored in the paged
//!   pool, so the actual number of handles you can create is based on available memory" (the same
//!   page).

#[cfg(target_os = "macos")]
mod macos;

/// Why the process's descriptor limit could not be read.
#[derive(Debug, thiserror::Error)]
pub enum DescriptorError {
    /// macOS did not say its per-process ceiling (`kern.maxfilesperproc`).
    #[error("sysctl kern.maxfilesperproc: {0}")]
    Ceiling(std::io::Error),
}

/// The process's descriptor limit before its start raised it, and after, read back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Raised {
    pub was: u64,
    pub now: u64,
}

/// Raises the process's descriptor limit to the most it may take without privilege, and reads it
/// back (the module's doc): once, at the process's start, before any thread that opens a
/// descriptor runs.
pub fn raise() -> Result<Raised, DescriptorError> {
    let was = limit()?;
    lift()?;
    Ok(Raised { was, now: limit()? })
}

/// The most descriptors the process may hold open at once, as the OS holds it now (the module's
/// doc).
pub fn limit() -> Result<u64, DescriptorError> {
    #[cfg(unix)]
    {
        let soft = rustix::process::getrlimit(rustix::process::Resource::Nofile)
            .current
            .unwrap_or(u64::MAX);
        Ok(soft.min(ceiling()?))
    }
    #[cfg(windows)]
    {
        Ok(WINDOWS_HANDLES)
    }
}

/// "The per-process limit on kernel handles is 2^24" (Microsoft Learn, Kernel Objects): every file
/// and socket the process holds is one.
#[cfg(windows)]
pub const WINDOWS_HANDLES: u64 = 1 << 24;

/// The soft `RLIMIT_NOFILE` set to the hard one, at most the kernel's per-process ceiling; never
/// lowered. A refusal leaves the limit as it stands: what the process may hold is what [`limit`]
/// reads back, not what was asked.
#[cfg(unix)]
fn lift() -> Result<(), DescriptorError> {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
    let held = getrlimit(Resource::Nofile);
    let wanted = held.maximum.unwrap_or(u64::MAX).min(ceiling()?);
    if held.current.is_none_or(|soft| soft >= wanted) {
        return Ok(());
    }
    // Refused, the limit stands as it was (the read-back says it): a sandbox may forbid the call.
    let _refused = setrlimit(
        Resource::Nofile,
        Rlimit {
            current: Some(wanted),
            maximum: held.maximum,
        },
    );
    Ok(())
}

/// Windows has no limit a process raises (the module's doc).
#[cfg(windows)]
fn lift() -> Result<(), DescriptorError> {
    Ok(())
}

/// The kernel's ceiling on one process's descriptors past its rlimit: macOS's
/// `kern.maxfilesperproc`; none elsewhere, where the hard limit is the ceiling.
#[cfg(unix)]
fn ceiling() -> Result<u64, DescriptorError> {
    #[cfg(target_os = "macos")]
    {
        macos::max_files_per_proc().map_err(DescriptorError::Ceiling)
    }
    #[cfg(not(target_os = "macos"))]
    {
        Ok(u64::MAX)
    }
}

// Windows has no limit to raise or read but its constant: the tests are Unix's.
#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// A soft limit below the most the process may take is raised to it, the hard limit or, on
    /// macOS, `kern.maxfilesperproc` where that is smaller, and read back so. The test lowers its
    /// own soft limit by one first, so the raise has something to do, and leaves it raised.
    #[test]
    fn the_soft_limit_is_raised_to_the_most_the_process_may_take() {
        use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
        let held = getrlimit(Resource::Nofile);
        let most = held.maximum.unwrap_or(u64::MAX).min(ceiling().unwrap());
        assert!(most > 1, "a hard limit of {most}");
        setrlimit(
            Resource::Nofile,
            Rlimit {
                current: Some(most - 1),
                maximum: held.maximum,
            },
        )
        .unwrap();
        assert_eq!(limit().unwrap(), most - 1);
        let raised = raise().unwrap();
        assert_eq!(
            raised,
            Raised {
                was: most - 1,
                now: most
            }
        );
        assert_eq!(
            getrlimit(Resource::Nofile).current,
            Some(most),
            "the soft limit itself is set"
        );
    }

    /// macOS holds a process to `kern.maxfilesperproc` whatever its soft limit: the bound read is
    /// never past it.
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_reads_its_ceiling_and_bounds_the_limit_by_it() {
        let ceiling = macos::max_files_per_proc().unwrap();
        assert!(ceiling > 0);
        assert!(limit().unwrap() <= ceiling);
    }
}
