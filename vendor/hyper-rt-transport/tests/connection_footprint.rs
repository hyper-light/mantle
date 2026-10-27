//! What a server holds for each connection it holds, as the operating system charges it, beside what
//! its Rust code asked the allocator for (ignored: a measurement, run in release with `--ignored
//! --nocapture`). `connection_memory` counts the bytes the server's thread asks Rust's allocator for;
//! that misses what a connection holds outside it: the C library's allocations (aws-lc's AEAD and
//! header-protection contexts go to `malloc`, not through Rust's allocator), the allocator's own
//! rounding and bookkeeping, and the pages either keeps. The OS's account of the process holds them
//! all (`hyper_measure::usage`: macOS's physical footprint, Linux's resident set, Windows's working
//! set), so this test's process is the server alone: its clients run in a process of their own (this
//! binary again, `footprint_clients`), so the process's footprint is the server's.
//!
//! Each row makes a server admitting `n`, has `n` clients connect and go quiet, and reports, per
//! connection, what the server thread's Rust allocations and the process's footprint grew by while
//! they connected and settled, and their ratio: what a charge made of Rust's requests alone leaves
//! out on this host. A cost learned on the host a node runs on is to be learned from the second.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::too_many_lines,
    dead_code,
    missing_docs,
    unsafe_code,
    reason = "a measurement harness: aws-lc's allocator hooks are C functions it calls"
)]

#[path = "../../hyper-transport/tests/common/mod.rs"]
mod common;
#[path = "footprint/mod.rs"]
mod footprint;

use std::alloc::{GlobalAlloc as _, Layout, System};
use std::ffi::{c_char, c_int, c_void};
use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use common::*;
use footprint::*;
use hyper_measure::alloc;
use hyper_rt::runtime::LocalRuntime;
use hyper_rt_transport::Driver;

/// The counting allocator, and a histogram of the sizes asked for while a row's histogram is on:
/// what a burst of handshakes allocates, by size, to tell a transient's makeup.
struct Sized;
/// Allocations by log2 of their size (bucket b: sizes below 2^(b+1)), and their bytes.
static SIZES: [AtomicI64; 40] = [const { AtomicI64::new(0) }; 40];
static SIZE_BYTES: [AtomicI64; 40] = [const { AtomicI64::new(0) }; 40];
static LARGEST: AtomicI64 = AtomicI64::new(0);
static HISTOGRAM: AtomicBool = AtomicBool::new(false);

fn note_size(size: usize) {
    if !HISTOGRAM.load(Ordering::Relaxed) {
        return;
    }
    let bucket = (usize::BITS - size.max(1).leading_zeros()).saturating_sub(1) as usize;
    let bucket = bucket.min(SIZES.len() - 1);
    SIZES[bucket].fetch_add(1, Ordering::Relaxed);
    SIZE_BYTES[bucket].fetch_add(size as i64, Ordering::Relaxed);
    LARGEST.fetch_max(size as i64, Ordering::Relaxed);
    // Where a block of 16 MiB or more is asked for, once a thread: capturing allocates, so the
    // thread's guard keeps the capture from noting itself.
    thread_local! {
        static CAPTURING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    if size >= 16 << 20 && !CAPTURING.with(std::cell::Cell::get) {
        CAPTURING.with(|capturing| capturing.set(true));
        eprintln!(
            "LARGE {size} B at\n{}",
            std::backtrace::Backtrace::force_capture()
        );
        CAPTURING.with(|capturing| capturing.set(false));
    }
}

// SAFETY: every call is forwarded unchanged to the counting allocator, which forwards to `System`.
unsafe impl std::alloc::GlobalAlloc for Sized {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note_size(layout.size());
        // SAFETY: as the caller's.
        unsafe { alloc::Counting.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note_size(layout.size());
        // SAFETY: as the caller's.
        unsafe { alloc::Counting.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: as the caller's.
        unsafe { alloc::Counting.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note_size(new_size);
        // SAFETY: as the caller's.
        unsafe { alloc::Counting.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Sized = Sized;

/// aws-lc's live bytes and their highest since the hooks were set, in the server's process.
static C_LIVE: AtomicI64 = AtomicI64::new(0);
static C_PEAK: AtomicI64 = AtomicI64::new(0);
/// Each of aws-lc's blocks carries its size in a prefix of this many bytes, kept aligned.
const PREFIX: usize = 16;

fn c_note(delta: i64) {
    let live = C_LIVE.fetch_add(delta, Ordering::Relaxed) + delta;
    C_PEAK.fetch_max(live, Ordering::Relaxed);
}

/// aws-lc's `malloc`, counted: the system's, the block's size before it.
unsafe extern "C" fn c_malloc(size: usize, _: *const c_char, _: c_int) -> *mut c_void {
    let Ok(layout) = Layout::from_size_align(size + PREFIX, PREFIX) else {
        return std::ptr::null_mut();
    };
    // SAFETY: the layout is non-zero (at least the prefix) and valid.
    let base = unsafe { System.alloc(layout) };
    if base.is_null() {
        return std::ptr::null_mut();
    }
    // SAFETY: the block holds the prefix, aligned for a usize.
    unsafe { base.cast::<usize>().write(size) };
    c_note(size as i64);
    // SAFETY: within the block.
    unsafe { base.add(PREFIX).cast() }
}

/// aws-lc's `free`, counted, the block wiped first as aws-lc's own `free` wipes it.
unsafe extern "C" fn c_free(ptr: *mut c_void, _: *const c_char, _: c_int) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: `ptr` came from `c_malloc` or `c_realloc`: its prefix is before it.
    let base = unsafe { ptr.cast::<u8>().sub(PREFIX) };
    // SAFETY: the prefix holds the size `c_malloc` wrote.
    let size = unsafe { base.cast::<usize>().read() };
    // SAFETY: the block is `size + PREFIX` bytes, ours to write.
    unsafe { std::ptr::write_bytes(base, 0, size + PREFIX) };
    c_note(-(size as i64));
    // SAFETY: allocated by `System` with this layout.
    unsafe {
        System.dealloc(
            base,
            Layout::from_size_align_unchecked(size + PREFIX, PREFIX),
        )
    };
}

/// aws-lc's `realloc`, counted: a new block, the old one's bytes copied, the old one freed.
unsafe extern "C" fn c_realloc(
    ptr: *mut c_void,
    size: usize,
    file: *const c_char,
    line: c_int,
) -> *mut c_void {
    // SAFETY: as `c_malloc` and `c_free`.
    unsafe {
        let new = c_malloc(size, file, line);
        if !ptr.is_null() && !new.is_null() {
            let old = ptr.cast::<u8>().sub(PREFIX).cast::<usize>().read();
            std::ptr::copy_nonoverlapping(ptr.cast::<u8>(), new.cast::<u8>(), old.min(size));
            c_free(ptr, file, line);
        }
        new
    }
}

/// aws-lc's allocations go through the counting functions from now on; set before aws-lc allocates.
fn count_aws_lc() {
    // SAFETY: called once, first in the server's process, before anything uses aws-lc.
    let set = unsafe {
        aws_lc_sys::CRYPTO_set_mem_functions(Some(c_malloc), Some(c_realloc), Some(c_free))
    };
    assert_eq!(set, 1, "aws-lc's memory functions were set already");
}

/// What one row measured: the server thread's Rust bytes and the process's footprint once the
/// server was made and once its `n` connections settled.
struct Row {
    n: usize,
    rust_made: i64,
    rust_settled: i64,
    rust_peak: i64,
    c_made: i64,
    c_settled: i64,
    c_peak: i64,
    os_made: i64,
    os_settled: i64,
    sock_made: Option<i64>,
    sock_settled: Option<i64>,
}

/// One row: a server admitting `n` measured as `n` clients, in a process of their own, connect to it
/// and go quiet.
fn row(n: usize) -> Row {
    let pki = Pki::new();
    let server = pki.issue(&name(SERVER));
    let clients: Vec<(Vec<u8>, Vec<u8>)> =
        (1..=n as u64).map(|peer| pki.issue(&name(peer))).collect();
    // The row's credentials for its clients' process, in a directory of its own, removed after.
    let dir = std::env::temp_dir().join(format!("footprint-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    write_credentials(&dir, &pki.root, &server.0, &clients);
    let peers: Vec<(Vec<u8>, u64)> = clients
        .iter()
        .enumerate()
        .map(|(index, client)| (client.0.clone(), index as u64 + 1))
        .collect();
    let root = pki.root.clone();
    // Room for every connection's socket, timer and task: each connection has a socket of its own
    // where the platform shares ports.
    let mut rt = LocalRuntime::new(&runtime(n)).unwrap();
    rt.block_on(async move {
        let peers: Vec<(&[u8], u64)> = peers
            .iter()
            .map(|(certificate, peer)| (certificate.as_slice(), *peer))
            .collect();
        let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
        alloc::begin();
        let mut driver: Drv = Driver::bind(
            endpoint(&root, &server, &peers, true, admitting(n)),
            loopback,
            IO,
        )
        .unwrap();
        let rust_made = alloc::read().live;
        HISTOGRAM.store(true, Ordering::Relaxed);
        let c_made = C_LIVE.load(Ordering::Relaxed);
        C_PEAK.store(c_made, Ordering::Relaxed);
        let os_made = footprint();
        let sock_made = cgroup_sock();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["footprint_clients", "--exact", "--ignored", "--nocapture"])
            .env("FOOTPRINT_DIR", &dir)
            .env("FOOTPRINT_N", n.to_string())
            .env("FOOTPRINT_SERVER", driver.local_addr().unwrap().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        settle(&mut driver, n, &mut |_| {}).await;
        let counts = alloc::read();
        HISTOGRAM.store(false, Ordering::Relaxed);
        let (rust_settled, rust_peak) = (counts.live, counts.peak);
        let (c_settled, c_peak) = (
            C_LIVE.load(Ordering::Relaxed),
            C_PEAK.load(Ordering::Relaxed),
        );
        let os_settled = footprint();
        let sock_settled = cgroup_sock();
        alloc::end();
        drop(child.stdin.take());
        child.wait().unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
        Row {
            n,
            rust_made,
            rust_settled,
            rust_peak,
            c_made,
            c_settled,
            c_peak,
            os_made,
            os_settled,
            sock_made,
            sock_settled,
        }
    })
    .unwrap()
}

/// Repetitions of each row, each in a process of its own.
const REPEATS: usize = 3;

#[test]
#[ignore = "a measurement: cargo test --release -p hyper-rt-transport --test connection_footprint -- --ignored --nocapture what_the_os"]
fn what_the_os_charges_a_server_for_each_connection() {
    println!("{} {}", std::env::consts::OS, std::env::consts::ARCH);
    println!(
        "| connections | Rust each | C each | Rust + C each | OS footprint each | footprint / (Rust + C) | handshake peak each (Rust + C) | cgroup socket each |"
    );
    println!("|---|---|---|---|---|---|---|---|");
    for n in [64usize, 256, 1024] {
        for _ in 0..REPEATS {
            // Each row in a process of its own: what an earlier row freed and its allocator kept
            // is not reused by this one's.
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["footprint_server", "--exact", "--ignored", "--nocapture"])
                .env("FOOTPRINT_ROW", n.to_string())
                .stderr(Stdio::inherit())
                .output()
                .unwrap();
            assert!(output.status.success(), "the row's server failed");
            let text = String::from_utf8_lossy(&output.stdout);
            let line = text
                .lines()
                .find_map(|line| line.strip_prefix("ROW "))
                .unwrap_or_else(|| panic!("no row in {text}"));
            let v: Vec<i64> = line.split(' ').map(|f| f.parse().unwrap_or(-1)).collect();
            let [
                n,
                rust_made,
                rust_settled,
                rust_peak,
                c_made,
                c_settled,
                c_peak,
                os_made,
                os_settled,
                sock_made,
                sock_settled,
            ] = v[..]
            else {
                panic!("a row of other fields: {line}");
            };
            let each = |made: i64, settled: i64| (settled - made) / n;
            let rust = each(rust_made, rust_settled);
            let c = each(c_made, c_settled);
            let os = each(os_made, os_settled);
            let peak = each(rust_made + c_made, rust_peak + c_peak);
            let sock = if sock_made < 0 || sock_settled < 0 {
                "-".to_owned()
            } else {
                format!("{} B", each(sock_made, sock_settled))
            };
            println!(
                "| {n} | {rust} B | {c} B | {} B | {os} B | {:.2} | {peak} B | {sock} |",
                rust + c,
                os as f64 / (rust + c).max(1) as f64
            );
        }
    }
}

/// One row's server, in a process of its own (run by the measurement): aws-lc's allocations counted
/// from its start, the row made, its figures printed on one line.
#[test]
#[ignore = "the server of a footprint row, run by it in a process of its own"]
fn footprint_server() {
    let Ok(n) = std::env::var("FOOTPRINT_ROW") else {
        return;
    };
    count_aws_lc();
    let row = row(n.parse().unwrap());
    // What the burst asked for, by size: the buckets holding the most bytes.
    let mut buckets: Vec<(usize, i64, i64)> = (0..SIZES.len())
        .map(|b| {
            (
                b,
                SIZES[b].load(Ordering::Relaxed),
                SIZE_BYTES[b].load(Ordering::Relaxed),
            )
        })
        .filter(|(_, count, _)| *count > 0)
        .collect();
    buckets.sort_by_key(|(_, _, bytes)| -bytes);
    for (b, count, bytes) in buckets.iter().take(6) {
        eprintln!(
            "SIZES [{}, {}) B: {count} allocations, {bytes} B",
            1u64 << b,
            1u64 << (b + 1)
        );
    }
    eprintln!("SIZES largest {} B", LARGEST.load(Ordering::Relaxed));
    println!(
        "ROW {} {} {} {} {} {} {} {} {} {} {}",
        row.n,
        row.rust_made,
        row.rust_settled,
        row.rust_peak,
        row.c_made,
        row.c_settled,
        row.c_peak,
        row.os_made,
        row.os_settled,
        row.sock_made.unwrap_or(-1),
        row.sock_settled.unwrap_or(-1)
    );
}

/// The clients of a row, in a process of their own (run by [`row`]): `FOOTPRINT_N` clients connect
/// to `FOOTPRINT_SERVER` and stay until this process's standard input closes.
#[test]
#[ignore = "the clients of a footprint row, run by it in a process of their own"]
fn footprint_clients() {
    run_clients();
}

/// What the stepped run under the platform's allocator reads: the bytes counted live (the server
/// thread's Rust bytes and aws-lc's) and their highest.
struct Counted;

impl Probe for Counted {
    fn begin(&self) {
        alloc::begin();
    }
    fn measures(&self, _driver: &mut Drv) -> Vec<f64> {
        let counts = alloc::read();
        vec![
            (counts.live + C_LIVE.load(Ordering::Relaxed)) as f64,
            (counts.peak + C_PEAK.load(Ordering::Relaxed)) as f64,
        ]
    }
    fn end(&self) {
        alloc::end();
    }
}

/// A stepped run's server, in a process of its own (run by the learning measurement).
#[test]
#[ignore = "the server of a stepped run, run by the learning measurement in a process of its own"]
fn learning_server() {
    let Ok(noisy) = std::env::var("LEARNING_NOISY") else {
        return;
    };
    count_aws_lc();
    print_samples(&stepped_run(noisy == "1", Counted));
}

/// How well each way a node could learn what a connection costs it, on the host it runs on, learns
/// it: each estimate as the samples come, step by step, against what the host charges a connection
/// (the slope of a run with nothing else churning). The ways:
///
/// - footprint on connections: what the OS charges, regressed on the connections alone;
/// - footprint on connections and other holdings: the same, with what else the node holds (here,
///   the churn, which a node's budget knows) as a second regressor;
/// - live on connections and other holdings, scaled: the bytes the program counts it holds for its
///   connections, regressed so, times the ratio of footprint to counted live the run shows (the
///   allocator's and the pages' share, which no count inside the program sees);
/// - kernel socket memory on connections, where the host charges it to the process's limit.
#[test]
#[ignore = "a measurement: cargo test --release -p hyper-rt-transport --test connection_footprint -- --ignored --nocapture how_well"]
fn how_well_each_way_learns_what_a_connection_costs() {
    println!("{} {}", std::env::consts::OS, std::env::consts::ARCH);
    let live = |s: &Sample| s.measures[0];
    let peak_live = |s: &Sample| s.measures[1];
    let clean = samples_of("learning_server", false);
    let truth = slope(&clean, |s| s.footprint);
    let truth_live = slope(&clean, live);
    let sock = clean[0].sock >= 0.0;
    let truth_sock = if sock { slope(&clean, |s| s.sock) } else { 0.0 };
    println!(
        "host charges {truth:.0} B a connection (counted live {truth_live:.0} B); kernel sockets {truth_sock:.0} B"
    );
    let noisy = samples_of("learning_server", true);
    println!(
        "| after step | n | footprint ~ n | footprint ~ n + other | live ~ n + other, scaled | live alone | live x peak ratio | kernel ~ n |"
    );
    println!("|---|---|---|---|---|---|---|---|");
    for k in 2..noisy.len() {
        let seen = &noisy[..=k];
        let e1 = slope(seen, |s| s.footprint);
        let e2 = slope_beside_churn(seen, |s| s.footprint);
        let live_each = slope_beside_churn(seen, live);
        // The allocator's and pages' share: footprint against counted live bytes, the churn's
        // variation identifying it.
        let lives: Vec<Vec<f64>> = seen.iter().map(|s| vec![live(s)]).collect();
        let ratio = least_squares(
            &lives,
            &seen.iter().map(|s| s.footprint).collect::<Vec<_>>(),
        )[1];
        let e3 = live_each * ratio;
        // The allocator's share from the high-water marks, which only grow as it takes pages it
        // never had: footprint's against counted live's.
        let peaks: Vec<Vec<f64>> = seen.iter().map(|s| vec![peak_live(s)]).collect();
        let peak_ratio = least_squares(
            &peaks,
            &seen.iter().map(|s| s.peak_footprint).collect::<Vec<_>>(),
        )[1];
        let e5 = live_each * peak_ratio;
        let e4 = if sock {
            err(slope(seen, |s| s.sock), truth_sock)
        } else {
            "-".to_owned()
        };
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {e4} |",
            k + 1,
            seen[k].n,
            err(e1, truth),
            err(e2, truth),
            err(e3, truth),
            err(live_each, truth),
            err(e5, truth)
        );
    }
}
