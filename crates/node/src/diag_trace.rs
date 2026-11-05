//! DIAGNOSTIC ONLY, never landed (branch diag-doors): a stall trace. Each request's stages through
//! both doors, the host's asks and answers, and the node loop's turns, stamped on
//! CLOCK_MONOTONIC (the clock `perf record -k CLOCK_MONOTONIC` and the harness stamp with), so a
//! slow request's time is attributed to the stage it waited in. On only where
//! `MANTLE_DIAG_TRACE` names a directory; written there at each `counts` answer
//! (`trace-<pid>-<n>.txt`).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::arithmetic_side_effects)]

use std::cell::Cell;
use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

/// Events a window holds at most; past it they are counted as dropped.
const MOST: usize = 4_000_000;

pub const ACCEPT: u16 = 1;
pub const H_HEAD: u16 = 2;
pub const H_AUTH: u16 = 3;
pub const H_SUBMIT: u16 = 4;
pub const H_READY: u16 = 5;
pub const H_BODY_SENT: u16 = 6;
pub const H_REPLY: u16 = 7;
pub const H_RESPONDED: u16 = 8;
pub const H_END: u16 = 9;
pub const N_ARRIVED: u16 = 20;
pub const N_DECODED: u16 = 21;
pub const J_TAKEN: u16 = 30;
pub const J_ASK: u16 = 31;
pub const J_DONE: u16 = 32;
pub const J_REPLY: u16 = 33;
pub const J_END: u16 = 34;
pub const J_OVER: u16 = 35;
pub const T_BEGIN: u16 = 40;
pub const T_PHASE: u16 = 41;
pub const T_END: u16 = 42;
pub const C_INTENDED: u16 = 50;
pub const C_SEND: u16 = 51;
pub const C_DONE: u16 = 52;

static ON: AtomicU8 = AtomicU8::new(0);
static NEXT: AtomicU64 = AtomicU64::new(1);
static DROPPED: AtomicU64 = AtomicU64::new(0);
static WINDOW: AtomicU64 = AtomicU64::new(0);
static LOG: Mutex<Vec<(u64, u64, u16, u64)>> = Mutex::new(Vec::new());
static KEYS: Mutex<Vec<(u64, String)>> = Mutex::new(Vec::new());

thread_local! {
    /// The client's request under way on this thread, for the key its operation names.
    static CURRENT: Cell<u64> = const { Cell::new(0) };
}

/// Whether the trace is on.
pub fn on() -> bool {
    match ON.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = std::env::var_os("MANTLE_DIAG_TRACE").is_some();
            if on && let Ok(mut log) = LOG.lock() {
                log.reserve(MOST);
            }
            ON.store(if on { 2 } else { 1 }, Ordering::Relaxed);
            on
        }
    }
}

/// CLOCK_MONOTONIC, in nanoseconds.
pub fn now() -> u64 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    (t.tv_sec as u64) * 1_000_000_000 + t.tv_nsec as u64
}

/// This thread's CPU time, in nanoseconds.
pub fn cpu() -> u64 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    (t.tv_sec as u64) * 1_000_000_000 + t.tv_nsec as u64
}

/// A fresh id; 0 where the trace is off.
pub fn id() -> u64 {
    if on() { NEXT.fetch_add(1, Ordering::Relaxed) } else { 0 }
}

pub fn ev(id: u64, stage: u16, a: u64) {
    if !on() {
        return;
    }
    let t = now();
    if let Ok(mut log) = LOG.lock() {
        if log.len() < MOST {
            log.push((t, id, stage, a));
        } else {
            DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// The key a request names.
pub fn key(id: u64, what: &str, key: &str) {
    if !on() {
        return;
    }
    if let Ok(mut keys) = KEYS.lock() {
        keys.push((id, format!("{what} {key}")));
    }
}

pub fn set_current(id: u64) {
    CURRENT.with(|c| c.set(id));
}
pub fn current() -> u64 {
    CURRENT.with(Cell::get)
}

/// The op's kind and key, for [`key`].
pub fn op_key(id: u64, op: &mantle_protocol::object::Op) {
    use mantle_protocol::object::Op;
    if !on() {
        return;
    }
    let (what, k) = match op {
        Op::Put { key, .. } => ("Put", key.as_str()),
        Op::Get { key, .. } => ("Get", key.as_str()),
        Op::Head { key, .. } => ("Head", key.as_str()),
        Op::Delete { key, .. } => ("Delete", key.as_str()),
        Op::CreateUpload { key, .. } => ("CreateUpload", key.as_str()),
        Op::UploadPart { key, .. } => ("UploadPart", key.as_str()),
        Op::Complete { key, .. } => ("Complete", key.as_str()),
        Op::Abort { key, .. } => ("Abort", key.as_str()),
        Op::ListParts { key, .. } => ("ListParts", key.as_str()),
        Op::Open { key, .. } => ("Open", key.as_str()),
        Op::Append { key, .. } => ("Append", key.as_str()),
        Op::Offset { key, .. } => ("Offset", key.as_str()),
        Op::Finish { key, .. } => ("Finish", key.as_str()),
        Op::List(_) => ("List", "-"),
        _ => ("Other", "-"),
    };
    key(id, what, k);
}

/// Writes the window's events and keys and begins the next.
pub fn dump(label: &str) {
    if !on() {
        return;
    }
    let Some(dir) = std::env::var_os("MANTLE_DIAG_TRACE") else { return };
    let events = LOG.lock().map(|mut l| std::mem::replace(&mut *l, Vec::with_capacity(MOST))).unwrap_or_default();
    let keys = KEYS.lock().map(|mut k| std::mem::take(&mut *k)).unwrap_or_default();
    let n = WINDOW.fetch_add(1, Ordering::SeqCst);
    let path = std::path::Path::new(&dir).join(format!("trace-{}-{n}.txt", std::process::id()));
    if let Ok(f) = std::fs::File::create(path) {
        let mut f = std::io::BufWriter::new(f);
        let _ = writeln!(f, "trace {n} label {label} events {} dropped {}", events.len(), DROPPED.swap(0, Ordering::SeqCst));
        for (id, k) in keys {
            let _ = writeln!(f, "k {id} {k}");
        }
        for (t, id, stage, a) in events {
            let _ = writeln!(f, "e {t} {id} {stage} {a}");
        }
        let _ = f.flush();
    }
}
