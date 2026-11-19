//! macOS's per-process descriptor ceiling (`crate::descriptors`): `kern.maxfilesperproc`, read by
//! `sysctlbyname` (sysctl(3)), an `int` (`CTLTYPE_INT` in XNU's `bsd/kern/kern_sysctl.c`). libSystem,
//! which every macOS process links, exports the call.
#![allow(unsafe_code)]

use std::ffi::{c_char, c_int, c_void};

unsafe extern "C" {
    fn sysctlbyname(
        name: *const c_char,
        oldp: *mut c_void,
        oldlenp: *mut usize,
        newp: *mut c_void,
        newlen: usize,
    ) -> c_int;
}

/// `kern.maxfilesperproc`: the most descriptors one process may hold, whatever its rlimit.
pub(super) fn max_files_per_proc() -> Result<u64, std::io::Error> {
    let mut value: c_int = 0;
    let mut length = size_of::<c_int>();
    // SAFETY: the name is a NUL-terminated literal; `oldp` points at a live `c_int` and `oldlenp` at
    // its size, so the kernel writes at most that many bytes there and the length it wrote back;
    // `newp` is null with a length of zero, so nothing is set.
    let called = unsafe {
        sysctlbyname(
            c"kern.maxfilesperproc".as_ptr(),
            (&raw mut value).cast::<c_void>(),
            &raw mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if called != 0 {
        return Err(std::io::Error::last_os_error());
    }
    if length != size_of::<c_int>() {
        return Err(std::io::Error::other(format!(
            "{length} bytes where an int is {}",
            size_of::<c_int>()
        )));
    }
    u64::try_from(value).map_err(|_| std::io::Error::other(format!("{value}, below zero")))
}
