//! The descriptors a process holds open, read from the OS: what a test of how many a file's
//! handles take counts.
//!
//! - Linux: the entries of `/proc/self/fd`, one for each descriptor (proc(5)), less the one the
//!   listing itself holds while it reads.
//! - macOS: `proc_pidinfo(pid, PROC_PIDLISTFDS, 0, buffer, size)` (`<libproc.h>`), which writes one
//!   `struct proc_fdinfo` (`<sys/proc_info.h>`) for each descriptor and returns the bytes written.
//! - Windows: `GetProcessHandleCount` (processthreadsapi.h): every handle the process holds,
//!   files and events and threads alike.
#![allow(unsafe_code)]

use std::io;

/// The descriptors this process holds open now (on Windows, its handles).
pub fn open() -> io::Result<usize> {
    os::open()
}

#[cfg(any(target_os = "linux", target_os = "android"))]
mod os {
    use std::io;

    pub(super) fn open() -> io::Result<usize> {
        let mut entries = 0usize;
        for entry in std::fs::read_dir("/proc/self/fd")? {
            entry?;
            entries = entries.saturating_add(1);
        }
        // The listing's own descriptor is among those it lists.
        Ok(entries.saturating_sub(1))
    }
}

#[cfg(target_vendor = "apple")]
mod os {
    use std::io;

    /// `PROC_PIDLISTFDS` of `<sys/proc_info.h>`.
    const PROC_PIDLISTFDS: libc::c_int = 1;

    /// `struct proc_fdinfo` of `<sys/proc_info.h>`: its `int32_t proc_fd` and `uint32_t
    /// proc_fdtype`, two 4-byte fields, which a listing is only counted by.
    type FdInfo = [u32; 2];

    unsafe extern "C" {
        fn proc_pidinfo(
            pid: libc::c_int,
            flavor: libc::c_int,
            arg: u64,
            buffer: *mut libc::c_void,
            buffersize: libc::c_int,
        ) -> libc::c_int;
    }

    pub(super) fn open() -> io::Result<usize> {
        let invalid = || io::Error::from(io::ErrorKind::InvalidInput);
        let pid = libc::c_int::try_from(std::process::id()).map_err(|_| invalid())?;
        // SAFETY: a null buffer of no bytes asks for the size the listing needs, and the call
        // writes nothing.
        let needed = unsafe { proc_pidinfo(pid, PROC_PIDLISTFDS, 0, std::ptr::null_mut(), 0) };
        let needed = usize::try_from(needed).map_err(|_| io::Error::last_os_error())?;
        let each = std::mem::size_of::<FdInfo>();
        // Room for those open as the size was asked and as many again, opened meanwhile: a listing
        // that fills it is asked again, larger.
        let mut room = needed.checked_div(each).ok_or_else(invalid)?;
        for _ in 0..usize::BITS {
            room = room
                .checked_mul(2)
                .and_then(|n| n.checked_add(1))
                .ok_or_else(invalid)?;
            let mut infos: Vec<FdInfo> = Vec::new();
            infos
                .try_reserve_exact(room)
                .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
            infos.resize(room, [0; 2]);
            let size = room
                .checked_mul(each)
                .and_then(|n| libc::c_int::try_from(n).ok())
                .ok_or_else(invalid)?;
            // SAFETY: `infos` is a writable array of `room` proc_fdinfo records, `size` bytes,
            // live for the call, which writes at most `size` bytes and returns how many it wrote.
            let wrote =
                unsafe { proc_pidinfo(pid, PROC_PIDLISTFDS, 0, infos.as_mut_ptr().cast(), size) };
            let wrote = usize::try_from(wrote).map_err(|_| io::Error::last_os_error())?;
            let listed = wrote.checked_div(each).ok_or_else(invalid)?;
            if listed < room {
                return Ok(listed);
            }
        }
        Err(invalid())
    }
}

#[cfg(windows)]
mod os {
    use std::io;

    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessHandleCount};

    pub(super) fn open() -> io::Result<usize> {
        let mut count = 0u32;
        // SAFETY: GetCurrentProcess returns the pseudo-handle of the calling process, which needs
        // no closing; `count` is a live u32 the call writes.
        let ok = unsafe { GetProcessHandleCount(GetCurrentProcess(), &raw mut count) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        usize::try_from(count).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    windows
)))]
mod os {
    use std::io;

    pub(super) fn open() -> io::Result<usize> {
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }
}
