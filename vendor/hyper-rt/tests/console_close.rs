//! Windows: a console close reaches a subscriber, and the process ends when the subscriber's orderly
//! stop ends it, not as the console handler returns (docs/runtime.md §6.1; found by mantle).
//!
//! The close is a real one: the child runs on a pseudoconsole this test makes, and the test closes it
//! ("Closing a pseudoconsole will send CTRL_CLOSE_EVENT to each client application that is still
//! connected", ClosePseudoConsole, Microsoft Learn). The child is this binary again, told its part by
//! its environment; it says it has subscribed through a named event, and the close comes after.

#![cfg(windows)]
#![allow(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::ffi::{OsStr, OsString};
use std::io::{Read, Write};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::{Path, PathBuf};

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::signal::{Signal, subscribe};
use windows_sys::Win32::Foundation::{HANDLE, WAIT_OBJECT_0};
use windows_sys::Win32::System::Console::{COORD, ClosePseudoConsole, CreatePseudoConsole, HPCON};
use windows_sys::Win32::System::Pipes::CreatePipe;
use windows_sys::Win32::System::Threading::{
    CREATE_UNICODE_ENVIRONMENT, CreateEventW, CreateProcessW, DeleteProcThreadAttributeList,
    EVENT_MODIFY_STATE, EXTENDED_STARTUPINFO_PRESENT, GetExitCodeProcess,
    InitializeProcThreadAttributeList, OpenEventW, PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE,
    PROCESS_INFORMATION, STARTF_USESTDHANDLES, STARTUPINFOEXW, SetEvent, UpdateProcThreadAttribute,
    WaitForSingleObject,
};

/// The test's name, which the child is run with.
const NAME: &str = "a_console_close_waits_for_the_subscribers_orderly_stop";
/// The environment variable that makes this binary the child, naming the file its stop writes.
const STOP_FILE: &str = "HYPER_RT_CONSOLE_CLOSE_STOP";
/// The environment variable naming the event the child sets once it has subscribed.
const SUBSCRIBED: &str = "HYPER_RT_CONSOLE_CLOSE_SUBSCRIBED";
/// What the child's stop writes.
const STOPPED_IN_ORDER: &str = "stopped in order\n";
/// The child's exit code once its stop is done.
const STOPPED: u32 = 3;
/// The child's exit code when no close came within its patience.
const NO_CLOSE: u32 = 4;
/// How long either side waits for the other before the test fails rather than hangs, in milliseconds.
const PATIENCE_MS: u32 = 60_000;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

#[test]
fn a_console_close_waits_for_the_subscribers_orderly_stop() {
    if let (Some(file), Some(event)) = (std::env::var_os(STOP_FILE), std::env::var_os(SUBSCRIBED)) {
        child(PathBuf::from(file), &event);
    }
    let id = std::process::id();
    let file = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("console-close-{id}.stop"));
    let _ = std::fs::remove_file(&file);
    let event = format!("Local\\hyper-rt-console-close-{id}");
    let code = run_child_and_close_its_console(&file, &event);
    let stopped = std::fs::read_to_string(&file).unwrap_or_default();
    let _ = std::fs::remove_file(&file);
    assert_eq!(
        code, STOPPED,
        "the child ended with {code:#x}, not by its own stop (its stop's file: {stopped:?})"
    );
    assert_eq!(stopped, STOPPED_IN_ORDER);
}

/// The child: subscribes to the close, says so, and on the close stops in order (a write made
/// durable) and ends its process itself with [`STOPPED`].
#[expect(
    clippy::exit,
    reason = "the child ends its process itself, as an owner whose stop is done does; the test reads the code"
)]
fn child(file: PathBuf, subscribed: &OsStr) -> ! {
    let signals = subscribe(&[Signal::Close]).unwrap();
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let name = wide(subscribed);
    // SAFETY: a NUL-ended name; the right to set the event, not inherited.
    let event = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, name.as_ptr()) };
    assert!(!event.is_null(), "{}", std::io::Error::last_os_error());
    // SAFETY: the handle the call above returned, owned once from here.
    let event = unsafe { OwnedHandle::from_raw_handle(event) };
    // SAFETY: a live event handle with the right to set it.
    assert!(unsafe { SetEvent(event.as_raw_handle()) } != 0);
    drop(event);
    let patience_ns = u64::from(PATIENCE_MS) * 1_000_000;
    let code = rt
        .block_on(async move {
            match hyper_rt::futures::within(patience_ns, signals.recv()).await {
                Ok(Some(Ok(seen))) if seen.contains(Signal::Close) => {
                    let mut stop = std::fs::File::create(&file).unwrap();
                    stop.write_all(STOPPED_IN_ORDER.as_bytes()).unwrap();
                    stop.sync_all().unwrap();
                    STOPPED
                }
                _ => NO_CLOSE,
            }
        })
        .unwrap();
    std::process::exit(i32::try_from(code).unwrap());
}

/// Runs this binary as the child on a pseudoconsole, closes the pseudoconsole once the child has
/// subscribed, and returns the child's exit code.
fn run_child_and_close_its_console(file: &Path, event: &str) -> u32 {
    let name = wide(OsStr::new(event));
    // SAFETY: default security, manual reset, unset, a NUL-ended name.
    let subscribed = unsafe { CreateEventW(std::ptr::null(), 1, 0, name.as_ptr()) };
    assert!(!subscribed.is_null(), "{}", std::io::Error::last_os_error());
    // SAFETY: the handle the call above returned, owned once from here.
    let subscribed = unsafe { OwnedHandle::from_raw_handle(subscribed) };

    let (input_read, input_write) = pipe();
    let (output_read, output_write) = pipe();
    let mut console: HPCON = 0;
    // SAFETY: two pipe ends this test owns; the pseudoconsole takes its own references to them.
    let made = unsafe {
        CreatePseudoConsole(
            COORD { X: 80, Y: 25 },
            input_read.as_raw_handle(),
            output_write.as_raw_handle(),
            0,
            &raw mut console,
        )
    };
    assert_eq!(made, 0, "CreatePseudoConsole: {made:#x}");
    // The pseudoconsole holds its own; what it writes reaches the read end alone.
    drop(input_read);
    drop(output_write);
    // What the pseudoconsole writes is read to its end, so it never waits on a full pipe
    // (ClosePseudoConsole, Remarks). The reader is not joined: its end is the console host's, which
    // goes when its last client does.
    std::thread::spawn(move || {
        let mut output = std::fs::File::from(output_read);
        let mut buf = [0u8; 4096];
        while let Ok(1..) = output.read(&mut buf) {}
    });

    let process = spawn_on(
        console,
        &[
            (STOP_FILE, file.as_os_str()),
            (SUBSCRIBED, OsStr::new(event)),
        ],
    );
    // SAFETY: a live event handle.
    let said = unsafe { WaitForSingleObject(subscribed.as_raw_handle(), PATIENCE_MS) };
    // SAFETY: the pseudoconsole this function made, closed once.
    unsafe { ClosePseudoConsole(console) };
    assert_eq!(
        said, WAIT_OBJECT_0,
        "the child never said it had subscribed"
    );
    // SAFETY: the child's process handle, open until `process` drops.
    let ended = unsafe { WaitForSingleObject(process.as_raw_handle(), PATIENCE_MS) };
    assert_eq!(
        ended, WAIT_OBJECT_0,
        "the child had not ended {PATIENCE_MS} ms after the close"
    );
    let mut code = 0u32;
    // SAFETY: as above; `code` is a live local.
    assert!(unsafe { GetExitCodeProcess(process.as_raw_handle(), &raw mut code) } != 0);
    drop(input_write);
    code
}

/// Starts this binary as the child, attached to `console`, with `vars` added to its environment and
/// no standard handles; its process handle.
fn spawn_on(console: HPCON, vars: &[(&str, &OsStr)]) -> OwnedHandle {
    let mut size = 0usize;
    // SAFETY: the documented size query: no list, one attribute; it fails and writes the size.
    unsafe { InitializeProcThreadAttributeList(std::ptr::null_mut(), 1, 0, &raw mut size) };
    let mut list = vec![0u8; size];
    let attributes = list.as_mut_ptr().cast();
    // SAFETY: a buffer of the size the query gave, initialized for one attribute.
    assert!(unsafe { InitializeProcThreadAttributeList(attributes, 1, 0, &raw mut size) } != 0);
    // SAFETY: the pseudoconsole's handle is the attribute's value itself, as the documented sample
    // passes it; the list outlives the CreateProcessW below.
    let set = unsafe {
        UpdateProcThreadAttribute(
            attributes,
            0,
            PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE as usize,
            console as *const core::ffi::c_void,
            std::mem::size_of::<HPCON>(),
            std::ptr::null_mut(),
            std::ptr::null(),
        )
    };
    assert!(set != 0);
    // SAFETY: an all-zero STARTUPINFOEXW is a valid value; its size, flags and list are set below.
    let mut start: STARTUPINFOEXW = unsafe { std::mem::zeroed() };
    start.StartupInfo.cb = u32::try_from(std::mem::size_of::<STARTUPINFOEXW>()).unwrap();
    // No standard handles: the child takes neither this process's nor any other's.
    start.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    start.lpAttributeList = attributes;

    let exe = std::env::current_exe().unwrap();
    let mut line: Vec<u16> = format!(
        "\"{}\" --exact {NAME} --nocapture --test-threads 1",
        exe.display()
    )
    .encode_utf16()
    .chain([0])
    .collect();
    let environment = environment_with(vars);
    // SAFETY: an all-zero PROCESS_INFORMATION is a valid value for the call to fill.
    let mut info: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: a writable, NUL-ended command line; a Unicode environment block ended by two NULs;
    // the startup information and its attribute list live through the call.
    let created = unsafe {
        CreateProcessW(
            std::ptr::null(),
            line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            0,
            EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT,
            environment.as_ptr().cast(),
            std::ptr::null(),
            &raw const start.StartupInfo,
            &raw mut info,
        )
    };
    let failed = std::io::Error::last_os_error();
    // SAFETY: the list initialized above, deleted once, after the only call that reads it.
    unsafe { DeleteProcThreadAttributeList(attributes) };
    assert!(created != 0, "CreateProcessW: {failed}");
    // SAFETY: the call made both handles for this test; each is owned once from here.
    let (process, thread) = unsafe {
        (
            OwnedHandle::from_raw_handle(info.hProcess),
            OwnedHandle::from_raw_handle(info.hThread),
        )
    };
    drop(thread);
    process
}

/// This process's environment with `vars` set, as a Unicode environment block: `name=value` strings
/// sorted by name without regard to case, each NUL-ended, and one NUL more (CreateProcessW's
/// `lpEnvironment`; Changing Environment Variables, Microsoft Learn).
fn environment_with(vars: &[(&str, &OsStr)]) -> Vec<u16> {
    let mut all: Vec<(OsString, OsString)> = std::env::vars_os()
        .filter(|(name, _)| !vars.iter().any(|(set, _)| name.eq_ignore_ascii_case(set)))
        .collect();
    all.extend(
        vars.iter()
            .map(|(name, value)| (OsString::from(name), value.to_os_string())),
    );
    all.sort_by_cached_key(|(name, _)| name.to_string_lossy().to_uppercase());
    let mut block = Vec::new();
    for (name, value) in &all {
        block.extend(name.encode_wide());
        block.push(u16::from(b'='));
        block.extend(value.encode_wide());
        block.push(0);
    }
    block.push(0);
    block
}

/// `text`, NUL-ended, as Windows' wide strings are.
fn wide(text: &OsStr) -> Vec<u16> {
    text.encode_wide().chain([0]).collect()
}

/// An anonymous pipe: its read end and its write end.
fn pipe() -> (OwnedHandle, OwnedHandle) {
    let (mut read, mut write): (HANDLE, HANDLE) = (std::ptr::null_mut(), std::ptr::null_mut());
    // SAFETY: two live locals for the handles; default security; the system's default size.
    assert!(unsafe { CreatePipe(&raw mut read, &raw mut write, std::ptr::null(), 0) } != 0);
    // SAFETY: the call made both handles for this test; each is owned once from here.
    unsafe {
        (
            OwnedHandle::from_raw_handle(read),
            OwnedHandle::from_raw_handle(write),
        )
    }
}
