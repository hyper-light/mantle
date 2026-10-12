//! What a file's first writes cost the other transfers to it, on any platform (docs/research/48).
//!
//! `cargo bench -p mantle-disk --bench firstwrite -- MODE OP CACHING LOAD DIR [T [G [SPAN [SECS [GROW [RATE]]]]]]`
//!
//! `T` threads (default 2) overwrite (`OP` = `write`) or read (`read`) random 128 KiB blocks of a
//! written `SPAN`-byte region (default 1 GiB) of a file F while one more thread writes `G`-byte
//! blocks (default 1 MiB): past F's end (`MODE` = `same`), into never-written blocks inside F's
//! end, F sized ahead first (`hole`), past another file's end (`other`), over a second written
//! region of F (`over`), or nothing (`none`). Every file is opened `buffered` or `direct`
//! (`CACHING`; hyper-block's `PreferDirect`: O_DIRECT on Linux, F_NOCACHE on macOS,
//! FILE_FLAG_NO_BUFFERING on Windows; the line names what the file system granted). With `LOAD` =
//! `flush`, one more thread writes 8 KiB and makes the platform's full flush in a loop on a file
//! of its own, as another writer on the device does. The first-writer writes at most `RATE` bytes
//! a second (default 0, unpaced): a store grows at its fill's pace, not the device's. A run lasts
//! `SECS` (default 3) or until the first-writer has added `GROW` bytes (default 4 GiB), whichever
//! is first, so a small disk is never filled; its files are removed at the end.
//!
//! Prints one line: the transfers made, their p50, p99, p99.9 and max in µs, the first-writer's
//! GB/s, the run's seconds, and the flushes a second with their p50 and max in µs.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::disallowed_macros
)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::file::{Caching, CachingRequest, DeviceFile};

const BLOCK: usize = 128 << 10;
const ALIGN: usize = 4096;
const LOAD_WRITE: usize = 8 << 10;
const LOAD_SPAN: u64 = 64 << 20;

fn arg<T: std::str::FromStr>(args: &[String], i: usize, default: T) -> T {
    args.get(i)
        .map_or(default, |a| a.parse().ok().expect("a number"))
}

fn buffer(len: usize, fill: u8) -> AlignedBuf {
    let mut b = AlignedBuf::zeroed(len, Alignment::new(ALIGN).unwrap()).unwrap();
    b.extend_from_slice(&vec![fill; len]).unwrap();
    b
}

fn open(path: &Path, caching: CachingRequest) -> DeviceFile {
    DeviceFile::open(path, true, caching, Alignment::new(ALIGN).unwrap()).unwrap()
}

/// Writes `[from, to)` of `file` whole, a block at a time, and flushes it.
fn write_whole(file: &DeviceFile, from: u64, to: u64) {
    let b = buffer(BLOCK, 0x11);
    let mut at = from;
    while at < to {
        file.write_all_at(b.as_slice(), at).unwrap();
        at += BLOCK as u64;
    }
    file.sync_data().unwrap();
}

fn percentiles(mut lat: Vec<u64>) -> (usize, f64, f64, f64, f64) {
    lat.sort_unstable();
    let n = lat.len();
    if n == 0 {
        return (0, 0.0, 0.0, 0.0, 0.0);
    }
    let at = |permille: usize| lat[(n * permille / 1000).min(n - 1)] as f64 / 1e3;
    (n, at(500), at(990), at(999), lat[n - 1] as f64 / 1e3)
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let usage = "MODE(none|same|hole|other|over) OP(read|write) CACHING(buffered|direct) LOAD(none|flush) DIR [T G SPAN SECS GROW RATE]";
    let mode = args.first().expect(usage).clone();
    let reading = args.get(1).expect(usage) == "read";
    let caching = match args.get(2).expect(usage).as_str() {
        "direct" => CachingRequest::PreferDirect,
        _ => CachingRequest::Buffered,
    };
    let loaded = args.get(3).expect(usage) == "flush";
    let dir = PathBuf::from(args.get(4).expect(usage));
    let threads: usize = arg(&args, 5, 2);
    let grow: usize = arg(&args, 6, 1 << 20);
    let span: u64 = arg(&args, 7, 1 << 30);
    let secs: u64 = arg(&args, 8, 3);
    let grow_max: u64 = arg(&args, 9, 4 << 30);
    let rate: u64 = arg(&args, 10, 0);
    assert!(
        matches!(mode.as_str(), "none" | "same" | "hole" | "other" | "over"),
        "{usage}"
    );
    assert!(
        span.is_multiple_of(BLOCK as u64) && grow.is_multiple_of(ALIGN),
        "{usage}"
    );

    let f_path = dir.join("firstwrite-f.bin");
    let g_path = dir.join("firstwrite-g.bin");
    let load_path = dir.join("firstwrite-load.bin");
    let f = open(&f_path, caching);
    let granted = match f.caching() {
        Caching::Direct => "direct",
        Caching::Buffered => "buffered",
    };
    write_whole(&f, 0, span);
    // Where the first-writer starts, and in which file.
    let (grower, start) = match mode.as_str() {
        "other" => (Some(open(&g_path, caching)), 0u64),
        "over" => {
            write_whole(&f, span, 2 * span);
            (Some(f.try_clone().unwrap()), span)
        }
        "hole" => {
            let sized = std::fs::OpenOptions::new()
                .write(true)
                .open(&f_path)
                .unwrap();
            sized.set_len(span + grow_max).unwrap();
            sized.sync_all().unwrap();
            (Some(f.try_clone().unwrap()), span)
        }
        "same" => (Some(f.try_clone().unwrap()), span),
        _ => (None, 0),
    };
    let over = mode == "over";
    let load = loaded.then(|| open(&load_path, CachingRequest::Buffered));

    let stop = AtomicBool::new(false);
    // The load's flushes made: the transfers measured beside it start once it has made one.
    let flushed = AtomicU64::new(0);
    // Set once the first-writer has stopped: the transfers measured beside it stop with it.
    let writer_done = AtomicBool::new(false);
    let grown = AtomicU64::new(0);
    let began = Instant::now();
    let deadline = began + Duration::from_secs(secs);
    let (lat, flushes, writer_secs) = std::thread::scope(|s| {
        let load_thread = load.as_ref().map(|lf| {
            let lf = lf.try_clone().unwrap();
            let (stop, flushed) = (&stop, &flushed);
            s.spawn(move || {
                let b = buffer(LOAD_WRITE, 0x5a);
                let mut at = 0u64;
                let mut lat = Vec::with_capacity(1 << 20);
                while !stop.load(Ordering::Relaxed) {
                    lf.write_all_at(b.as_slice(), at).unwrap();
                    at = (at + LOAD_WRITE as u64) % LOAD_SPAN;
                    let t = Instant::now();
                    lf.sync_data().unwrap();
                    flushed.fetch_add(1, Ordering::Relaxed);
                    if lat.len() < lat.capacity() {
                        lat.push(t.elapsed().as_nanos() as u64);
                    }
                }
                lat
            })
        });
        // The transfers measured beside the load start once it has made a flush.
        if load.is_some() {
            while flushed.load(Ordering::Relaxed) == 0 {
                std::thread::yield_now();
            }
        }
        let writer = grower.as_ref().map(|g| {
            let g = g.try_clone().unwrap();
            let (stop, grown, writer_done) = (&stop, &grown, &writer_done);
            s.spawn(move || {
                let b = buffer(grow, 0);
                let t = Instant::now();
                let mut at = start;
                while !stop.load(Ordering::Relaxed) {
                    g.write_all_at(b.as_slice(), at).unwrap();
                    at += grow as u64;
                    let so_far = grown.fetch_add(grow as u64, Ordering::Relaxed) + grow as u64;
                    if rate > 0 {
                        // Ahead of the pace: parked until the bytes written are due, or woken to
                        // stop.
                        let due = Duration::from_secs_f64(so_far as f64 / rate as f64);
                        while let Some(wait) = due.checked_sub(t.elapsed()) {
                            if stop.load(Ordering::Relaxed) {
                                break;
                            }
                            std::thread::park_timeout(wait);
                        }
                    }
                    if over && at + grow as u64 > start + span {
                        at = start;
                    } else if !over && at - start >= grow_max {
                        break;
                    }
                }
                writer_done.store(true, Ordering::Relaxed);
                t.elapsed().as_secs_f64()
            })
        });
        let workers: Vec<_> = (0..threads)
            .map(|i| {
                let fh = f.try_clone().unwrap();
                let (stop, writer_done) = (&stop, &writer_done);
                s.spawn(move || {
                    let mut b = buffer(BLOCK, 0x42);
                    let mut x = 0x9e37_79b9_7f4a_7c15u64 ^ (i as u64 + 1).wrapping_mul(7919);
                    let blocks = span / BLOCK as u64;
                    let mut lat = Vec::with_capacity(1 << 22);
                    while !stop.load(Ordering::Relaxed)
                        && Instant::now() < deadline
                        && !writer_done.load(Ordering::Relaxed)
                    {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        let at = (x % blocks) * BLOCK as u64;
                        let t = Instant::now();
                        if reading {
                            fh.read_exact_at(b.as_mut_slice(), at).unwrap();
                        } else {
                            fh.write_all_at(b.as_slice(), at).unwrap();
                        }
                        if lat.len() < lat.capacity() {
                            lat.push(t.elapsed().as_nanos() as u64);
                        }
                    }
                    lat
                })
            })
            .collect();
        let mut lat = Vec::new();
        for w in workers {
            lat.extend(w.join().unwrap());
        }
        stop.store(true, Ordering::Relaxed);
        if let Some(w) = &writer {
            w.thread().unpark();
        }
        let writer_secs = writer.map_or(0.0, |w| w.join().unwrap());
        let flushes = load_thread.map(|l| l.join().unwrap()).unwrap_or_default();
        (lat, flushes, writer_secs)
    });
    let secs_run = began.elapsed().as_secs_f64();
    let (n, p50, p99, p999, max) = percentiles(lat);
    let flush_count = flushes.len();
    let (_, f50, _, _, fmax) = percentiles(flushes);
    let gbps = if writer_secs > 0.0 {
        grown.load(Ordering::Relaxed) as f64 / writer_secs / 1e9
    } else {
        0.0
    };
    println!(
        "{mode} {} {granted} {} T={threads} G={grow} rate={rate} transfers {n} p50 {p50:.1} p99 {p99:.1} p99.9 {p999:.1} max {max:.1} us writer_GBps {gbps:.2} secs {secs_run:.2} flushes_per_s {:.0} flush_p50_us {f50:.1} flush_max_us {fmax:.1}",
        if reading { "read" } else { "write" },
        if loaded { "flush" } else { "none" },
        flush_count as f64 / secs_run.max(1e-9),
    );
    drop(f);
    drop(grower);
    drop(load);
    for p in [&f_path, &g_path, &load_path] {
        let _ = std::fs::remove_file(p);
    }
}
