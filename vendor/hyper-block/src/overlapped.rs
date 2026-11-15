//! Windows: reads and writes at explicit offsets on a handle opened for overlapped I/O, which many
//! threads issue on one handle at once (a segmented file's segments, `crate::segmented`).
//!
//! A handle opened without `FILE_FLAG_OVERLAPPED` takes its threads in turn: "If this flag is not
//! specified, then I/O operations are serialized, even if the calls to the read and write functions
//! specify an OVERLAPPED structure", while with it "the file can be used for simultaneous read and
//! write operations" (Microsoft, CreateFileW, `FILE_FLAG_OVERLAPPED`). On such a handle "the system
//! does not maintain the file pointer ... therefore the file position must be passed to the read
//! and write functions in the related offset data members of the OVERLAPPED structure"
//! (Microsoft, "Synchronous and Asynchronous I/O"), each in "a valid and unique OVERLAPPED
//! structure" (ReadFile, WriteFile). Each call here starts one transfer at its offset and waits
//! for its end with GetOverlappedResult.
//!
//! What a call waits on is an event of its own thread's, not the handle: "Each time an I/O
//! operation is completed, the operating system sets the file handle to the signaled state ... If
//! an application must perform multiple asynchronous I/O operations on a single file, it should
//! wait on the event handle in the specific OVERLAPPED structure for each I/O operation, rather
//! than on the common file handle" ("Synchronous and Asynchronous I/O"). std's positioned reads and
//! writes wait on the handle (`library/std/src/sys/pal/windows/handle.rs`, `synchronous_read`),
//! so they are not used on such a handle. ReadFile "resets the event to a nonsignaled state when it
//! begins the I/O operation", WriteFile likewise, and a thread has one transfer out at a time, so
//! one manual-reset event a thread serves every call it makes; it is closed with the thread. A
//! read that "begins at or beyond the end of the file ... fails with the error ERROR_HANDLE_EOF"
//! (ReadFile), which is the end, read as no bytes, as std reads it.
#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::windows::io::AsRawHandle;

use windows_sys::Win32::Foundation::{CloseHandle, ERROR_HANDLE_EOF, ERROR_IO_PENDING, HANDLE};
use windows_sys::Win32::Storage::FileSystem::{ReadFile, WriteFile};
use windows_sys::Win32::System::IO::{GetOverlappedResult, OVERLAPPED};
use windows_sys::Win32::System::Threading::CreateEventW;

/// A manual-reset event, made nonsignalled, owned by one thread and closed with it.
struct Event(HANDLE);

impl Event {
    fn new() -> io::Result<Self> {
        // SAFETY: no security attributes and no name (both null, as documented for an unnamed
        // event with the default security); the call returns a new handle or null and reads
        // nothing else.
        let event = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
        if event.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(event))
    }
}

impl Drop for Event {
    fn drop(&mut self) {
        // SAFETY: the handle is this event's, made by CreateEventW and closed only here, once; no
        // transfer waits on it, since its thread is ending.
        unsafe { CloseHandle(self.0) };
    }
}

std::thread_local! {
    /// This thread's event, made at its first transfer.
    static EVENT: Result<Event, io::ErrorKind> = Event::new().map_err(|e| e.kind());
}

/// An OVERLAPPED block for a transfer at `offset`, signalling `event` at its end.
fn at(offset: u64, event: HANDLE) -> io::Result<OVERLAPPED> {
    let invalid = || io::Error::from(io::ErrorKind::InvalidInput);
    let mut overlapped = OVERLAPPED::default();
    overlapped.Anonymous.Anonymous.Offset =
        u32::try_from(offset & u64::from(u32::MAX)).map_err(|_| invalid())?;
    overlapped.Anonymous.Anonymous.OffsetHigh =
        u32::try_from(offset.checked_shr(32).ok_or_else(invalid)?).map_err(|_| invalid())?;
    overlapped.hEvent = event;
    Ok(overlapped)
}

/// Whether `error` is the end of the file, which ReadFile and GetOverlappedResult report for a
/// read at or past it on a handle opened for overlapped I/O.
fn is_end(error: &io::Error) -> bool {
    error.raw_os_error() == i32::try_from(ERROR_HANDLE_EOF).ok()
}

/// Runs one transfer, started by `start` with this thread's event, to its end: the bytes moved.
fn transfer(
    file: &File,
    offset: u64,
    start: impl FnOnce(HANDLE, *mut OVERLAPPED) -> i32,
) -> io::Result<usize> {
    let event = EVENT
        .try_with(|event| {
            event
                .as_ref()
                .map(|e| e.0)
                .map_err(|kind| io::Error::from(*kind))
        })
        .map_err(|_| {
            io::Error::other("a transfer on a thread whose locals are being destroyed")
        })??;
    let mut overlapped = at(offset, event)?;
    let handle = file.as_raw_handle();
    if start(handle, &raw mut overlapped) == 0 {
        let error = io::Error::last_os_error();
        if is_end(&error) {
            return Ok(0);
        }
        if error.raw_os_error() != i32::try_from(ERROR_IO_PENDING).ok() {
            return Err(error);
        }
    }
    let mut moved = 0u32;
    // SAFETY: waits (bWait true) for the transfer just started on `handle` with `overlapped`,
    // whose event is this thread's; `overlapped`, the buffer `start` named and `moved` stay live
    // and untouched until the wait returns, after which the kernel writes none of them.
    let done = unsafe { GetOverlappedResult(handle, &raw const overlapped, &raw mut moved, 1) };
    if done == 0 {
        let error = io::Error::last_os_error();
        return if is_end(&error) { Ok(0) } else { Err(error) };
    }
    usize::try_from(moved).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))
}

/// Reads into `buf` from `offset`: the bytes read, at most `u32::MAX` a call; 0 at the file's end.
pub(crate) fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    let len = u32::try_from(buf.len()).unwrap_or(u32::MAX);
    let at = buf.as_mut_ptr();
    transfer(file, offset, |handle, overlapped| {
        // SAFETY: `handle` is `file`'s, opened for overlapped I/O, and outlives the transfer;
        // `at` names `len` writable bytes of `buf`, which the caller holds borrowed until
        // `transfer` has waited for the read to end; the count pointer is null, as documented
        // for an overlapped read, and `overlapped` is live until that wait returns.
        unsafe { ReadFile(handle, at, len, std::ptr::null_mut(), overlapped) }
    })
}

/// Writes `buf` at `offset`: the bytes written, at most `u32::MAX` a call.
pub(crate) fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    let len = u32::try_from(buf.len()).unwrap_or(u32::MAX);
    let at = buf.as_ptr();
    transfer(file, offset, |handle, overlapped| {
        // SAFETY: `handle` is `file`'s, opened for overlapped I/O, and outlives the transfer;
        // `at` names `len` bytes of `buf`, borrowed until `transfer` has waited for the write to
        // end; the count pointer is null, as documented for an overlapped write, and
        // `overlapped` is live until that wait returns.
        unsafe { WriteFile(handle, at, len, std::ptr::null_mut(), overlapped) }
    })
}
