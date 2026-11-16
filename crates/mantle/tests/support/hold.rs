//! A node's process held still and let go on, as a host that sleeps holds it: on Unix the
//! stopping and continuing signals (`SIGSTOP`, `SIGCONT`, by the system's `kill`); on Windows,
//! which has no stopping signal, each of its threads suspended and resumed (`SuspendThread`,
//! `ResumeThread`), its threads those a Toolhelp snapshot lists (`CreateToolhelp32Snapshot` with
//! `TH32CS_SNAPTHREAD`; Microsoft's Win32 API reference for each).

#![allow(unsafe_code)]

/// Holds the process `pid` still.
pub(crate) fn still(pid: u32) {
    #[cfg(unix)]
    signal(pid, "STOP");
    #[cfg(windows)]
    windows::suspend(pid);
}

/// Lets the process `pid`, held still, go on.
pub(crate) fn go_on(pid: u32) {
    #[cfg(unix)]
    signal(pid, "CONT");
    #[cfg(windows)]
    windows::resume(pid);
}

/// Sends the process `pid` the signal named `name` by the system's `kill`.
#[cfg(unix)]
fn signal(pid: u32, name: &str) {
    let sent = std::process::Command::new("kill")
        .args([format!("-{name}"), pid.to_string()])
        .status()
        .unwrap();
    assert!(sent.success(), "kill -{name} {pid}: {sent}");
}

#[cfg(windows)]
mod windows {
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::Threading::{
        OpenThread, ResumeThread, SuspendThread, THREAD_SUSPEND_RESUME,
    };

    /// The ids of the threads of the process `pid` a snapshot taken now lists.
    fn threads(pid: u32) -> Vec<u32> {
        // SAFETY: a snapshot of every thread in the system; its handle is closed below.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        assert!(
            snapshot != INVALID_HANDLE_VALUE,
            "a thread snapshot was refused"
        );
        let mut entry = THREADENTRY32 {
            dwSize: u32::try_from(std::mem::size_of::<THREADENTRY32>()).unwrap(),
            cntUsage: 0,
            th32ThreadID: 0,
            th32OwnerProcessID: 0,
            tpBasePri: 0,
            tpDeltaPri: 0,
            dwFlags: 0,
        };
        let mut found = Vec::new();
        // SAFETY: `snapshot` is the live snapshot above and `entry` a THREADENTRY32 whose
        // `dwSize` is its own size, as Thread32First and Thread32Next require.
        let mut more = unsafe { Thread32First(snapshot, &mut entry) } != 0;
        while more {
            if entry.th32OwnerProcessID == pid {
                found.push(entry.th32ThreadID);
            }
            // SAFETY: as for Thread32First.
            more = unsafe { Thread32Next(snapshot, &mut entry) } != 0;
        }
        // SAFETY: the snapshot's handle, closed once.
        unsafe { CloseHandle(snapshot) };
        found
    }

    /// Calls `act` (SuspendThread or ResumeThread) on the thread `id`: whether it took, none
    /// where the thread has ended.
    fn act(id: u32, act: unsafe extern "system" fn(*mut core::ffi::c_void) -> u32) -> Option<bool> {
        // SAFETY: opens the thread by its id for suspending and resuming alone; a thread that has
        // ended meanwhile gives no handle.
        let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, id) };
        if thread.is_null() {
            return None;
        }
        // SAFETY: `thread` is the live handle above, opened with THREAD_SUSPEND_RESUME.
        let count = unsafe { act(thread) };
        // SAFETY: the thread's handle, closed once.
        unsafe { CloseHandle(thread) };
        Some(count != u32::MAX)
    }

    /// Suspends every thread of the process `pid`. A thread it starts between a snapshot and its
    /// threads' suspension is found by the next snapshot; once every thread is suspended the
    /// process starts none, so a snapshot that lists no thread not tried yet ends it.
    pub(super) fn suspend(pid: u32) {
        let mut tried: Vec<u32> = Vec::new();
        let mut held = 0usize;
        loop {
            let fresh: Vec<u32> = threads(pid)
                .into_iter()
                .filter(|id| !tried.contains(id))
                .collect();
            if fresh.is_empty() {
                break;
            }
            for id in fresh {
                tried.push(id);
                match act(id, SuspendThread) {
                    Some(true) => held += 1,
                    Some(false) => panic!("thread {id} of process {pid} was not suspended"),
                    None => {}
                }
            }
        }
        assert!(held > 0, "no thread of process {pid} suspended");
    }

    /// Resumes every thread of the process `pid`, each suspended once by [`suspend`].
    pub(super) fn resume(pid: u32) {
        for id in threads(pid) {
            assert_ne!(
                act(id, ResumeThread),
                Some(false),
                "thread {id} of process {pid} was not resumed"
            );
        }
    }
}
