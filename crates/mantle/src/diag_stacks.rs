//! DIAGNOSTIC ONLY, never landed (branch diag-doors): every allocation and reallocation of the
//! process recorded with its call stack, window by window, so a phase's counts are attributed to
//! sites.
//!
//! A window runs from one switch to the next: in `mantle serve` each `counts` answer, in the
//! harness each [`mark`]. Every event outside the recorder is counted by hyper-measure's
//! `Counting` and recorded under one gate, and a switch reads and restarts `Counting`'s process
//! count under the same gate, so a window's recorded events plus the recorder's own equal what
//! `Counting` counted for it exactly; the window file states both.
//!
//! Stacks are raw return addresses (glibc `backtrace`, which allocates nothing through Rust's
//! allocator), aggregated in process by stack; the file carries the executable's mappings so the
//! addresses are symbolized offline (scripts in the ledger directory).
#![allow(unsafe_code, clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::arithmetic_side_effects)]

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use hyper_measure::alloc::{self, Counting, Counts};

pub struct Stacks;

const DEPTH: usize = 48;

unsafe extern "C" {
    fn backtrace(buffer: *mut *mut std::ffi::c_void, size: std::ffi::c_int) -> std::ffi::c_int;
}

#[derive(Default, Clone, Copy)]
struct Site {
    allocs: u64,
    reallocs: u64,
    moved: u64,
    bytes: u64,
}

#[derive(Hash, PartialEq, Eq)]
struct Key {
    /// The harness's own work (hyper-measure's set-aside flag on the allocating thread).
    aside: bool,
    stack: Vec<usize>,
}

struct Window {
    sites: HashMap<Key, Site>,
}

static ARMED: AtomicBool = AtomicBool::new(false);
/// The gate: held across `Counting`'s event and its record, and across a switch.
static GATE: Mutex<Option<Window>> = Mutex::new(None);
static R_ALLOCS: AtomicU64 = AtomicU64::new(0);
static R_REALLOCS: AtomicU64 = AtomicU64::new(0);
static R_MOVED: AtomicU64 = AtomicU64::new(0);
static R_BYTES: AtomicU64 = AtomicU64::new(0);
static WINDOW: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static IN: Cell<bool> = const { Cell::new(false) };
}

/// 0 alloc, 1 realloc in place, 2 realloc moved.
fn recorder(kind: u8, bytes: u64) {
    match kind {
        0 => R_ALLOCS.fetch_add(1, Ordering::Relaxed),
        1 => R_REALLOCS.fetch_add(1, Ordering::Relaxed),
        _ => {
            R_REALLOCS.fetch_add(1, Ordering::Relaxed);
            R_MOVED.fetch_add(1, Ordering::Relaxed)
        }
    };
    R_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Runs `event` (the counting allocator's call) and records it: `event` gives the block and the
/// kind and bytes, or none where it failed.
fn around(event: impl FnOnce() -> (*mut u8, Option<(u8, u64)>)) -> *mut u8 {
    if !ARMED.load(Ordering::Relaxed) {
        return event().0;
    }
    match IN.try_with(|c| c.replace(true)) {
        Ok(true) => {
            // The recorder's own: this thread holds the gate, or is the switching thread.
            let (b, k) = event();
            if let Some((kind, bytes)) = k {
                recorder(kind, bytes);
            }
            b
        }
        Ok(false) => {
            let mut gate = GATE.lock().unwrap_or_else(|p| p.into_inner());
            let (b, k) = event();
            if let Some((kind, bytes)) = k {
                let mut frames = [std::ptr::null_mut::<std::ffi::c_void>(); DEPTH];
                // SAFETY: the buffer holds DEPTH pointers.
                let n = unsafe { backtrace(frames.as_mut_ptr(), DEPTH as std::ffi::c_int) };
                let stack: Vec<usize> = frames[..n.max(0) as usize].iter().map(|f| *f as usize).collect();
                let aside = alloc::is_set_aside();
                if let Some(w) = gate.as_mut() {
                    let s = w.sites.entry(Key { aside, stack }).or_default();
                    match kind {
                        0 => s.allocs += 1,
                        1 => s.reallocs += 1,
                        _ => {
                            s.reallocs += 1;
                            s.moved += 1;
                        }
                    }
                    s.bytes += bytes;
                }
            }
            drop(gate);
            let _ = IN.try_with(|c| c.set(false));
            b
        }
        Err(_) => {
            // A thread past its thread-locals: counted as the recorder's, under the gate so no
            // switch falls between its count and its record.
            let _gate = GATE.lock().unwrap_or_else(|p| p.into_inner());
            let (b, k) = event();
            if let Some((kind, bytes)) = k {
                recorder(kind, bytes);
            }
            b
        }
    }
}

unsafe impl GlobalAlloc for Stacks {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        around(|| {
            // SAFETY: forwarded unchanged.
            let b = unsafe { Counting.alloc(layout) };
            (b, (!b.is_null()).then_some((0, layout.size() as u64)))
        })
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        around(|| {
            // SAFETY: forwarded unchanged.
            let b = unsafe { Counting.alloc_zeroed(layout) };
            (b, (!b.is_null()).then_some((0, layout.size() as u64)))
        })
    }
    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged.
        unsafe { Counting.dealloc(block, layout) }
    }
    unsafe fn realloc(&self, block: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        around(|| {
            // SAFETY: forwarded unchanged.
            let b = unsafe { Counting.realloc(block, layout, new_size) };
            let grown = new_size.saturating_sub(layout.size()) as u64;
            (b, (!b.is_null()).then_some((if b == block { 1 } else { 2 }, grown)))
        })
    }
}

/// Arms recording from here, the process count begun with it: window 0 runs from here.
pub fn arm() {
    if std::env::var_os("MANTLE_DIAG_STACKS").is_none() {
        return;
    }
    let _ = IN.try_with(|c| c.set(true));
    {
        let mut gate = GATE.lock().unwrap_or_else(|p| p.into_inner());
        *gate = Some(Window { sites: HashMap::new() });
        alloc::begin_process();
        ARMED.store(true, Ordering::SeqCst);
    }
    let _ = IN.try_with(|c| c.set(false));
}

/// Ends the window under way and begins the next: the counting allocator's count for the ended
/// window, which is written with its sites and labelled `label`.
pub fn switch(label: &str) -> Counts {
    let _ = IN.try_with(|c| c.set(true));
    let (counted, window, r) = {
        let mut gate = GATE.lock().unwrap_or_else(|p| p.into_inner());
        let window = gate.replace(Window { sites: HashMap::new() });
        let counted = alloc::read_process();
        alloc::begin_process();
        let r = (
            R_ALLOCS.swap(0, Ordering::SeqCst),
            R_REALLOCS.swap(0, Ordering::SeqCst),
            R_MOVED.swap(0, Ordering::SeqCst),
            R_BYTES.swap(0, Ordering::SeqCst),
        );
        (counted, window, r)
    };
    let number = WINDOW.fetch_add(1, Ordering::SeqCst);
    if let (Some(dir), Some(window)) = (std::env::var_os("MANTLE_DIAG_STACKS"), window) {
        let path = std::path::Path::new(&dir).join(format!("stacks-{}-{number}.txt", std::process::id()));
        if let Ok(f) = std::fs::File::create(path) {
            let mut f = std::io::BufWriter::new(f);
            let (mut a, mut re, mut m, mut b) = (0u64, 0u64, 0u64, 0u64);
            for s in window.sites.values() {
                a += s.allocs;
                re += s.reallocs;
                m += s.moved;
                b += s.bytes;
            }
            let _ = writeln!(
                f,
                "window {number} label {label} recorded allocs {a} reallocs {re} moved {m} bytes {b} recorder allocs {} reallocs {} moved {} bytes {} counted allocs {} reallocs {} moved {} bytes {} sites {}",
                r.0,
                r.1,
                r.2,
                r.3,
                counted.allocations,
                counted.reallocations,
                counted.moved,
                counted.bytes,
                window.sites.len()
            );
            if let Ok(maps) = std::fs::read_to_string("/proc/self/maps") {
                for line in maps.lines().filter(|l| l.contains("r-xp") || l.contains(" 00000000 ")) {
                    if line.contains("/mantle") || line.contains(".so") {
                        let _ = writeln!(f, "map {line}");
                    }
                }
            }
            for (k, s) in &window.sites {
                let _ = write!(f, "site {} {} {} {} {} :", u8::from(k.aside), s.allocs, s.reallocs, s.moved, s.bytes);
                for ip in &k.stack {
                    let _ = write!(f, " {ip:x}");
                }
                let _ = writeln!(f);
            }
            let _ = f.flush();
        }
    }
    let _ = IN.try_with(|c| c.set(false));
    counted
}
