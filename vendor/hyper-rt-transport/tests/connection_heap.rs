//! What a server's connections cost it under mimalloc (focal's allocator), read where they are held
//! (ignored: measurements, run in release with `--ignored --nocapture`). The server's work for its
//! connections runs on a mimalloc heap of its own (`mi_heap_new`), made the thread's default while
//! that work runs and given back after (`mi_heap_set_default`): every block that work allocates,
//! aws-lc's included (its allocations routed to mimalloc through `CRYPTO_set_mem_functions`), is on
//! that heap's pages, and a block freed anywhere goes back to the page it came from. The heap's
//! pages (`mi_heap_visit_blocks`: each page's initialized blocks and the blocks in use) are then
//! what the connections hold, the allocator's rounding and its partly used pages included, and
//! nothing else is on them: the measurements of `connection_footprint`, under mimalloc, with the
//! heap's account beside them.

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
    reason = "a measurement harness: mimalloc's heap API and aws-lc's allocator hooks are C functions"
)]

#[path = "../../hyper-transport/tests/common/mod.rs"]
mod common;
#[path = "footprint/mod.rs"]
mod footprint;

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::ffi::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

use footprint::*;
use hyper_rt_transport::Driver;
use libmimalloc_sys as mi;
use mimalloc::MiMalloc;

/// mimalloc, the bytes each thread asks for counted on that thread.
struct Counted;

thread_local! {
    // Const-initialized, so no destructor: readable through the thread's teardown, never allocating.
    static LIVE: Cell<i64> = const { Cell::new(0) };
}

fn note(delta: i64) {
    LIVE.with(|live| live.set(live.get() + delta));
}

// SAFETY: every call is forwarded unchanged to mimalloc's own `GlobalAlloc`.
unsafe impl GlobalAlloc for Counted {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: as the caller's.
        let block = unsafe { MiMalloc.alloc(layout) };
        if !block.is_null() {
            note(layout.size() as i64);
        }
        block
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: as the caller's.
        let block = unsafe { MiMalloc.alloc_zeroed(layout) };
        if !block.is_null() {
            note(layout.size() as i64);
        }
        block
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: as the caller's.
        unsafe { MiMalloc.dealloc(ptr, layout) };
        note(-(layout.size() as i64));
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: as the caller's.
        let block = unsafe { MiMalloc.realloc(ptr, layout, new_size) };
        if !block.is_null() {
            note(new_size as i64 - layout.size() as i64);
        }
        block
    }
}

#[global_allocator]
static ALLOCATOR: Counted = Counted;

/// aws-lc's live bytes, usable sizes, in the server's process.
static C_LIVE: AtomicI64 = AtomicI64::new(0);

/// aws-lc's `malloc`: mimalloc's, on the thread's default heap.
unsafe extern "C" fn c_malloc(size: usize, _: *const c_char, _: c_int) -> *mut c_void {
    // SAFETY: any size may be asked of mimalloc.
    let block = unsafe { mi::mi_malloc(size) };
    if !block.is_null() {
        // SAFETY: a block mimalloc gave.
        C_LIVE.fetch_add(
            unsafe { mi::mi_usable_size(block) } as i64,
            Ordering::Relaxed,
        );
    }
    block
}

/// aws-lc's `free`: the block wiped first, as aws-lc's own `free` wipes it (its hooks are called in
/// its place), then given back to mimalloc.
unsafe extern "C" fn c_free(ptr: *mut c_void, _: *const c_char, _: c_int) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: `ptr` came from `c_malloc`: a block of mimalloc's, its usable bytes ours to write.
    unsafe {
        let size = mi::mi_usable_size(ptr);
        std::ptr::write_bytes(ptr.cast::<u8>(), 0, size);
        C_LIVE.fetch_sub(size as i64, Ordering::Relaxed);
        mi::mi_free(ptr);
    }
}

/// aws-lc's `realloc`: a new block, the old one's bytes copied, the old one wiped and freed.
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
            let old = mi::mi_usable_size(ptr);
            std::ptr::copy_nonoverlapping(ptr.cast::<u8>(), new.cast::<u8>(), old.min(size));
            c_free(ptr, file, line);
        }
        new
    }
}

/// aws-lc's allocations go to mimalloc from now on; set before aws-lc allocates.
fn route_aws_lc() {
    // SAFETY: called once, first in the server's process, before anything uses aws-lc.
    let set = unsafe {
        aws_lc_sys::CRYPTO_set_mem_functions(Some(c_malloc), Some(c_realloc), Some(c_free))
    };
    assert_eq!(set, 1, "aws-lc's memory functions were set already");
}

/// A heap's pages, summed: the bytes of each page's initialized blocks, and of its blocks in use.
#[derive(Default)]
struct Areas {
    committed: usize,
    used: usize,
}

/// Adds one page of a heap to the [`Areas`] `arg` points to.
unsafe extern "C" fn visit(
    _: *const mi::mi_heap_t,
    area: *const mi::mi_heap_area_t,
    _: *mut c_void,
    _: usize,
    arg: *mut c_void,
) -> bool {
    // SAFETY: mimalloc passes a valid area; `arg` is the `Areas` `areas` passed.
    let (area, areas) = unsafe { (&*area, &mut *arg.cast::<Areas>()) };
    areas.committed += area.committed;
    areas.used += area.used * area.block_size;
    true
}

/// The pages of `heap`, its blocks freed by other threads taken back first. On the heap's thread.
fn areas(heap: *mut mi::mi_heap_t) -> Areas {
    let mut areas = Areas::default();
    // SAFETY: `heap` is this thread's, made by `mi_heap_new`; `visit` writes the `Areas` passed.
    unsafe {
        mi::mi_heap_collect(heap, false);
        mi::mi_heap_visit_blocks(heap, false, Some(visit), (&raw mut areas).cast());
    }
    areas
}

/// What mimalloc has committed for the whole process, bytes.
fn process_commit() -> usize {
    let mut commit = 0usize;
    // SAFETY: every out-parameter but one is null, which mimalloc allows.
    unsafe {
        mi::mi_process_info(
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &raw mut commit,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
    }
    commit
}

/// The stepped run under mimalloc: the server's work for its connections on a heap of its own.
/// Measures: the bytes counted live (the thread's Rust requests and aws-lc's usable bytes), the
/// heap's initialized and in-use bytes, and mimalloc's commit for the process.
struct Heap {
    heap: Cell<*mut mi::mi_heap_t>,
    backing: Cell<*mut mi::mi_heap_t>,
}

impl Probe for Heap {
    fn begin(&self) {
        // SAFETY: mimalloc's own calls, on the thread that keeps the heap.
        unsafe {
            self.backing.set(mi::mi_heap_get_default());
            self.heap.set(mi::mi_heap_new());
        }
    }
    fn enter(&self) {
        // SAFETY: the heap is this thread's.
        unsafe { mi::mi_heap_set_default(self.heap.get()) };
    }
    fn exit(&self) {
        // SAFETY: the thread's own default heap, as it was.
        unsafe { mi::mi_heap_set_default(self.backing.get()) };
    }
    /// The endpoint observed as it wants, through what a node does: hyper-heap's account of the
    /// heap its work runs on (`held_here`), the driver adding the kernel's holdings for its sockets.
    fn turned(&self, driver: &mut Drv) {
        if driver.endpoint().wants_observation()
            && let Some(held) = hyper_heap::held_here()
        {
            driver.observe(held.in_use);
        }
    }
    fn measures(&self, driver: &mut Drv) -> Vec<f64> {
        let areas = areas(self.heap.get());
        let state = driver.endpoint().stats().state;
        vec![
            (LIVE.with(Cell::get) + C_LIVE.load(Ordering::Relaxed)) as f64,
            areas.committed as f64,
            areas.used as f64,
            process_commit() as f64,
            state.each as f64,
            state.charged as f64,
            state.observations as f64,
            driver.kernel_held().map_or(-1.0, |held| held as f64),
        ]
    }
}

/// A stepped run's server under mimalloc, in a process of its own (run by the measurement).
#[test]
#[ignore = "the server of a stepped run, run by the heap measurement in a process of its own"]
fn heap_server() {
    let Ok(noisy) = std::env::var("LEARNING_NOISY") else {
        return;
    };
    route_aws_lc();
    print_samples(&stepped_run(
        noisy == "1",
        Heap {
            heap: Cell::new(std::ptr::null_mut()),
            backing: Cell::new(std::ptr::null_mut()),
        },
    ));
}

/// The clients of a stepped run, in a process of their own.
#[test]
#[ignore = "the clients of a stepped run, run by its server in a process of their own"]
fn footprint_clients() {
    run_clients();
}

/// How well each way learns what a connection costs under mimalloc, the heap of its own among them:
/// as `connection_footprint`'s, against the clean run's slope of footprint on connections, each
/// estimate made from the noisy run's samples up to each step.
#[test]
#[ignore = "a measurement: cargo test --release -p hyper-rt-transport --test connection_heap -- --ignored --nocapture how_well"]
fn how_well_a_heap_of_its_own_learns_what_a_connection_costs() {
    println!("{} {}", std::env::consts::OS, std::env::consts::ARCH);
    let live = |s: &Sample| s.measures[0];
    let committed = |s: &Sample| s.measures[1];
    let used = |s: &Sample| s.measures[2];
    let commit = |s: &Sample| s.measures[3];
    let learned = |s: &Sample| s.measures[4];
    let charged = |s: &Sample| s.measures[5];
    let observations = |s: &Sample| s.measures[6];
    let kernel = |s: &Sample| s.measures[7];
    let clean = samples_of("heap_server", false);
    let truth = slope(&clean, |s| s.footprint);
    let sock = clean[0].sock >= 0.0;
    let truth_sock = if sock { slope(&clean, |s| s.sock) } else { 0.0 };
    println!(
        "clean: footprint {truth:.0} B a connection; heap's pages {:.0} B, in use {:.0} B; counted live {:.0} B; mimalloc's commit {:.0} B; kernel sockets {truth_sock:.0} B",
        slope(&clean, committed),
        slope(&clean, used),
        slope(&clean, live),
        slope(&clean, commit),
    );
    if kernel(&clean[0]) >= 0.0 {
        println!(
            "clean: the driver's sockets' kernel holdings {:.0} B a connection against the control group's {truth_sock:.0} B",
            slope(&clean, kernel)
        );
    }
    for sample in &clean {
        println!(
            "clean n {} footprint {:.0} heap pages {:.0} in use {:.0}; the endpoint charges {:.0} B a connection more, {:.0} B in all, after {:.0} observations",
            sample.n,
            sample.footprint,
            committed(sample),
            used(sample),
            learned(sample),
            charged(sample),
            observations(sample)
        );
    }
    let noisy = samples_of("heap_server", true);
    // What the endpoint learned is a connection's whole cost, its socket's kernel holdings among
    // it where the kernel charges them: against the footprint's slope and the control group's.
    let whole = truth + truth_sock;
    println!(
        "| after step | n | footprint ~ n | footprint ~ n + other | live ~ n + other | heap's pages ~ n | heap's pages / n | heap in use ~ n | commit ~ n + other | kernel ~ n | the endpoint's charge, against footprint and kernel |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|");
    for k in 2..noisy.len() {
        let seen = &noisy[..=k];
        let kernel = if sock {
            err(slope(seen, |s| s.sock), truth_sock)
        } else {
            "-".to_owned()
        };
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {kernel} | {} |",
            k + 1,
            seen[k].n,
            err(slope(seen, |s| s.footprint), truth),
            err(slope_beside_churn(seen, |s| s.footprint), truth),
            err(slope_beside_churn(seen, live), truth),
            err(slope(seen, committed), truth),
            err(committed(&seen[k]) / seen[k].n, truth),
            err(slope(seen, used), truth),
            err(slope_beside_churn(seen, commit), truth),
            err(learned(&seen[k]), whole),
        );
    }
}

/// What a heap of its own costs the work that runs on it: a switch onto it and back
/// (`mi_heap_set_default`, twice), and an allocation and its free on it against the same on the
/// thread's own heap (mixed sizes, 16 bytes to 4 KiB).
#[test]
#[ignore = "a measurement: cargo test --release -p hyper-rt-transport --test connection_heap -- --ignored --nocapture what_a_heap"]
fn what_a_heap_of_its_own_costs() {
    const SWITCHES: u32 = 10_000_000;
    const ALLOCATIONS: u32 = 10_000_000;
    // SAFETY: mimalloc's own calls, on this thread, with its own heaps.
    let (backing, heap) = unsafe { (mi::mi_heap_get_default(), mi::mi_heap_new()) };
    for round in 0..3 {
        let began = Instant::now();
        for _ in 0..SWITCHES {
            // SAFETY: both heaps are this thread's.
            unsafe {
                mi::mi_heap_set_default(std::hint::black_box(heap));
                mi::mi_heap_set_default(std::hint::black_box(backing));
            }
        }
        let switch = began.elapsed().as_nanos() as f64 / f64::from(SWITCHES);
        let allocate = |on: *mut mi::mi_heap_t| {
            // SAFETY: the heap is this thread's.
            unsafe { mi::mi_heap_set_default(on) };
            let mut size = 16usize;
            let began = Instant::now();
            for _ in 0..ALLOCATIONS {
                let block = std::hint::black_box(Vec::<u8>::with_capacity(size));
                drop(block);
                size = if size >= 4096 { 16 } else { size * 2 };
            }
            let each = began.elapsed().as_nanos() as f64 / f64::from(ALLOCATIONS);
            // SAFETY: the thread's own default heap, as it was.
            unsafe { mi::mi_heap_set_default(backing) };
            each
        };
        let own = allocate(heap);
        let thread = allocate(backing);
        println!(
            "round {round}: a switch onto a heap and back {switch:.1} ns; an allocation and its free {thread:.1} ns on the thread's heap, {own:.1} ns on a heap of its own"
        );
    }
}

/// The plumbing test's server address, once bound, and its end.
static SERVER_ADDRESS: std::sync::OnceLock<std::net::SocketAddr> = std::sync::OnceLock::new();
static PLUMBED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Ends the plumbing test's clients as the server's part ends, passed or failed: a server that
/// failed and left them turning held the test's scope open until its job's time ran out.
struct Plumbed;

impl Drop for Plumbed {
    fn drop(&mut self) {
        PLUMBED.store(true, Ordering::Relaxed);
    }
}

/// An endpoint whose work runs on a heap of its own learns what its connections hold there and
/// charges it (docs/transport.md §4x): observed as it wants, its first look as it is made and the
/// rest as its connections double, its connections' state is charged what the heap holds past what
/// it held with none and past the receive pool, the kernel's holdings for its sockets beside it
/// where the platform charges them, and one more connection what one held on average, never less
/// than its structures' own bytes. The clients run on a thread of their own, off the server's heap.
#[test]
fn an_endpoint_on_a_heap_of_its_own_charges_its_connections_what_they_hold() {
    const CLIENTS: usize = 16;
    let pki = common::Pki::new();
    let server = pki.issue(&common::name(SERVER));
    let clients: Vec<(Vec<u8>, Vec<u8>)> = (1..=CLIENTS as u64)
        .map(|peer| pki.issue(&common::name(peer)))
        .collect();
    let peers: Vec<(Vec<u8>, u64)> = clients
        .iter()
        .enumerate()
        .map(|(index, client)| (client.0.clone(), index as u64 + 1))
        .collect();
    let root = pki.root.clone();
    std::thread::scope(|scope| {
        let (client_root, server_certificate) = (root.clone(), server.0.clone());
        scope.spawn(move || {
            let mut rt = hyper_rt::runtime::LocalRuntime::new(&runtime(CLIENTS)).unwrap();
            rt.block_on(async move {
                let server_addr = loop {
                    if let Some(address) = SERVER_ADDRESS.get() {
                        break *address;
                    }
                    hyper_rt::futures::sleep(1_000_000).await.unwrap();
                };
                let to_server = [(server_certificate.as_slice(), SERVER)];
                for client in &clients {
                    let mut driver: Drv = Driver::bind(
                        endpoint(&client_root, client, &to_server, false, admitting(1)),
                        "127.0.0.1:0".parse().unwrap(),
                        IO,
                    )
                    .unwrap();
                    let now = driver.clock().unwrap();
                    driver.endpoint().connect(now, SERVER, server_addr).unwrap();
                    hyper_rt::futures::spawn_detached(async move {
                        while !PLUMBED.load(Ordering::Relaxed) {
                            turn(&mut driver).await;
                            while driver.endpoint().poll_event().is_some() {}
                        }
                    })
                    .unwrap();
                }
                while !PLUMBED.load(Ordering::Relaxed) {
                    hyper_rt::futures::sleep(5_000_000).await.unwrap();
                }
            })
            .unwrap();
        });
        let _plumbed = Plumbed;
        let mut rt = hyper_rt::runtime::LocalRuntime::new(&runtime(CLIENTS)).unwrap();
        rt.block_on(hyper_heap::OnHeap::new(async move {
            let peers: Vec<(&[u8], u64)> = peers
                .iter()
                .map(|(certificate, peer)| (certificate.as_slice(), *peer))
                .collect();
            let mut driver: Drv = Driver::bind(
                endpoint(&root, &server, &peers, true, admitting(CLIENTS)),
                "127.0.0.1:0".parse().unwrap(),
                IO,
            )
            .unwrap();
            let observe = |driver: &mut Drv| {
                if driver.endpoint().wants_observation() {
                    let held =
                        hyper_heap::held_here().expect("the test allocates through mimalloc");
                    driver.observe(held.in_use);
                }
            };
            observe(&mut driver);
            SERVER_ADDRESS.set(driver.local_addr().unwrap()).unwrap();
            settle(&mut driver, CLIENTS, &mut |driver| observe(driver)).await;
            let state = driver.endpoint().stats().state;
            let base = state.base.expect("observed as it was made");
            assert!(state.observations >= 2, "{state:?}");
            // Observed once more now, settled: the charge is what the heap holds past the base, the
            // pool and what the receive ring grew by (charged as the ring's, docs/transport.md
            // §4aa), the kernel's holdings beside it.
            let held = hyper_heap::held_here().unwrap().in_use;
            let kernel = driver.kernel_held().unwrap_or(0);
            let pool =
                (driver.endpoint().stats().receive_chunks * hyper_transport::RECEIVE_CHUNK) as u64;
            let ring = driver.ring();
            assert_eq!(
                ring.charged,
                ((ring.buffers - 1) * hyper_rt_transport::RECEIVE_BYTES) as u64,
                "{ring:?}"
            );
            driver.observe(held);
            let state = driver.endpoint().stats().state;
            let expected = (held + kernel - pool - ring.charged) - base;
            assert_eq!((state.charged, state.unfunded), (expected, 0), "{state:?}");
            let least = hyper_transport::state_charge::<common::Mantle>(&admitting(CLIENTS));
            assert_eq!(
                state.each,
                (expected / CLIENTS as u64).max(least),
                "{state:?}"
            );
            assert!(
                state.each > least,
                "a connection held only its structures: {state:?}"
            );
            assert_eq!(
                driver.endpoint().connection_charge(),
                hyper_transport::connection_charge::<common::Mantle>(&admitting(CLIENTS)) - least
                    + state.each
            );
        }))
        .unwrap();
    });
}
