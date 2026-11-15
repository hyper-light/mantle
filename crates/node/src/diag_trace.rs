//! DIAGNOSTIC ONLY, never landed (branch diag-nfc2): a stall and admission trace. Events stamped on
//! CLOCK_MONOTONIC, shared by every process of the machine, so a harness's request and the node
//! that served it line up. On only where `MANTLE_DIAG_TRACE` names a directory; each process
//! writes its window there at each `counts` answer (a node) or phase's end (the harness), as
//! `trace-<pid>-<n>.txt`.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

/// Events a window holds at most; past it they are counted as dropped.
const MOST: usize = 8_000_000;

// Admission.
pub const LIMIT: u64 = 10;
pub const PHASE: u64 = 11;
// The host.
pub const PARK: u64 = 20;
pub const UNPARK: u64 = 21;
pub const START: u64 = 22;
pub const END: u64 = 23;
pub const REFUSE: u64 = 24;
pub const LOST: u64 = 25;
pub const BOUND_SLOW: u64 = 26;
pub const ASK: u64 = 27;
pub const ANSWER: u64 = 28;
pub const DOOR_TAKEN: u64 = 29;
pub const FAILED: u64 = 30;
pub const ENTRY_OUT: u64 = 31;
pub const ENTRY_ANSWERED: u64 = 32;
// The node's loop.
pub const TURN: u64 = 40;
pub const TURN_PART: u64 = 41;
pub const WAITED: u64 = 42;
// The log and volumes.
pub const LOG_WRITE: u64 = 60;
pub const LOG_DURABLE: u64 = 61;
pub const VOL_WRITE: u64 = 62;
pub const VOL_DONE: u64 = 63;
pub const PROPOSE: u64 = 64;
pub const APPLIED: u64 = 65;
// The harness.
pub const OP_START: u64 = 80;
pub const OP_END: u64 = 81;
pub const PAUSE: u64 = 82;

static ON: AtomicU8 = AtomicU8::new(0);
static NEXT: AtomicU64 = AtomicU64::new(1);
static DROPPED: AtomicU64 = AtomicU64::new(0);
static WINDOW: AtomicU64 = AtomicU64::new(0);
static LOG: Mutex<Vec<[u64; 5]>> = Mutex::new(Vec::new());
static NOTES: Mutex<Vec<String>> = Mutex::new(Vec::new());
/// The node loop's turns since the last window, the time they ran and the time they waited.
pub static TURNS: AtomicU64 = AtomicU64::new(0);
pub static BUSY_NS: AtomicU64 = AtomicU64::new(0);
pub static WAIT_NS: AtomicU64 = AtomicU64::new(0);

/// One turn of the node's loop, `waited` then `ran` nanoseconds.
pub fn turned(waited: u64, ran: u64) {
    if !on() {
        return;
    }
    TURNS.fetch_add(1, Ordering::Relaxed);
    BUSY_NS.fetch_add(ran, Ordering::Relaxed);
    WAIT_NS.fetch_add(waited, Ordering::Relaxed);
}

/// Whether the trace is on.
pub fn on() -> bool {
    match ON.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = std::env::var_os("MANTLE_DIAG_TRACE").is_some();
            if on && let Ok(mut log) = LOG.lock() {
                log.reserve(1 << 16);
            }
            ON.store(if on { 2 } else { 1 }, Ordering::Relaxed);
            on
        }
    }
}

/// CLOCK_MONOTONIC, in nanoseconds.
#[cfg(unix)]
pub fn now() -> u64 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    (t.tv_sec as u64) * 1_000_000_000 + t.tv_nsec as u64
}
#[cfg(not(unix))]
pub fn now() -> u64 {
    0
}

/// A fresh id; 0 where the trace is off.
pub fn id() -> u64 {
    if on() {
        NEXT.fetch_add(1, Ordering::Relaxed)
    } else {
        0
    }
}

pub fn ev(kind: u64, a: u64, b: u64, c: u64) {
    if !on() {
        return;
    }
    let t = now();
    if let Ok(mut log) = LOG.lock() {
        if log.len() < MOST {
            log.push([t, kind, a, b, c]);
        } else {
            DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// A line of free text for the window.
pub fn note(line: String) {
    if !on() {
        return;
    }
    if let Ok(mut notes) = NOTES.lock() {
        notes.push(format!("{} {line}", now()));
    }
}

fn status() -> String {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // utime and stime, fields 14 and 15, past the command's closing parenthesis.
    let cpu = stat
        .rsplit_once(')')
        .map(|(_, rest)| {
            let f: Vec<&str> = rest.split_whitespace().collect();
            format!("utime {} stime {}", f.get(11).unwrap_or(&"-"), f.get(12).unwrap_or(&"-"))
        })
        .unwrap_or_default();
    cpu + " | " + &std::fs::read_to_string("/proc/self/status")
        .map(|s| {
            s.lines()
                .filter(|l| {
                    l.starts_with("VmRSS")
                        || l.starts_with("VmSwap")
                        || l.starts_with("voluntary")
                        || l.starts_with("nonvoluntary")
                })
                .map(|l| l.split_whitespace().collect::<Vec<_>>().join(" "))
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .unwrap_or_default()
}

/// Writes the window's events and begins the next.
pub fn dump(label: &str) {
    if !on() {
        return;
    }
    let Some(dir) = std::env::var_os("MANTLE_DIAG_TRACE") else {
        return;
    };
    let events = LOG
        .lock()
        .map(|mut l| std::mem::replace(&mut *l, Vec::with_capacity(1 << 16)))
        .unwrap_or_default();
    let notes = NOTES.lock().map(|mut n| std::mem::take(&mut *n)).unwrap_or_default();
    let n = WINDOW.fetch_add(1, Ordering::SeqCst);
    let path = std::path::Path::new(&dir).join(format!("trace-{}-{n:04}.txt", std::process::id()));
    if let Ok(f) = std::fs::File::create(path) {
        let mut f = std::io::BufWriter::new(f);
        let _ = writeln!(
            f,
            "trace {n} pid {} at {} label {label} events {} dropped {} turns {} busy_ns {} wait_ns {} status {}",
            std::process::id(),
            now(),
            events.len(),
            DROPPED.swap(0, Ordering::SeqCst),
            TURNS.swap(0, Ordering::SeqCst),
            BUSY_NS.swap(0, Ordering::SeqCst),
            WAIT_NS.swap(0, Ordering::SeqCst),
            status()
        );
        for line in notes {
            let _ = writeln!(f, "n {line}");
        }
        for [t, kind, a, b, c] in events {
            let _ = writeln!(f, "e {t} {kind} {a} {b} {c}");
        }
        let _ = f.flush();
    }
}
