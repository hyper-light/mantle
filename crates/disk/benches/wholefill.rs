//! How fast a new file is written whole with zeros, as a store grows a segment
//! (docs/research/48 §6), on any platform.
//!
//! `cargo bench -p mantle-disk --bench wholefill -- LOAD DIR [LEN [ROUNDS]]`
//!
//! Each round writes a new `LEN`-byte file (default 256 MiB) whole in each way below, the ways'
//! order rotated a round (default 3 rounds); every file is allocated first
//! (`DeviceFile::preallocate`) and flushed after. The ways: past the OS's cache (hyper-block's
//! `PreferDirect`: O_DIRECT on Linux, F_NOCACHE on macOS, FILE_FLAG_NO_BUFFERING on Windows; the
//! line names what the file system granted) 128 KiB a write, 1 MiB, 8 MiB or 64 MiB a write;
//! `DeviceFile::write_copies_at` from one 128 KiB buffer, past the cache (pwritev(2) on Linux and
//! macOS, a write a copy elsewhere); and through the cache 128 KiB a write. With `LOAD` = `flush`,
//! one more thread writes 8 KiB and makes the platform's full flush in a loop on a file of its own,
//! as another writer on the device does.
//!
//! Prints a line a way and round: the milliseconds the writes took, the flush after, and both.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::disallowed_macros
)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::file::{Caching, CachingRequest, DeviceFile};

const ALIGN: usize = 4096;
/// The page a fill's buffer is aligned to: the largest VM page of the six targets (16 KiB, Apple
/// silicon), which macOS's cluster layer needs to write past its cache.
const BUF_ALIGN: usize = 16 << 10;
const LOAD_WRITE: usize = 8 << 10;
const LOAD_SPAN: u64 = 64 << 20;
const COPY: usize = 128 << 10;
const WAYS: [&str; 6] = [
    "direct-128k",
    "direct-1m",
    "direct-8m",
    "direct-64m",
    "copies-128k",
    "buffered-128k",
];

fn arg<T: std::str::FromStr>(args: &[String], i: usize, default: T) -> T {
    args.get(i)
        .map_or(default, |a| a.parse().ok().expect("a number"))
}

fn zeros(len: usize) -> AlignedBuf {
    let mut b = AlignedBuf::zeroed(len, Alignment::new(BUF_ALIGN).unwrap()).unwrap();
    b.extend_from_slice(&vec![0; len]).unwrap();
    b
}

/// Writes a new file at `path` of `len` bytes whole in `way`: the writes' and the flush's
/// seconds, and the caching granted.
fn fill(way: &str, path: &Path, len: u64) -> (f64, f64, &'static str) {
    let caching = if way.starts_with("buffered") {
        CachingRequest::Buffered
    } else {
        CachingRequest::PreferDirect
    };
    let f = DeviceFile::open(path, true, caching, Alignment::new(ALIGN).unwrap()).unwrap();
    let granted = match f.caching() {
        Caching::Direct => "direct",
        Caching::Buffered => "buffered",
    };
    f.preallocate(len).unwrap();
    f.sync_data().unwrap();
    let piece: usize = match way {
        "direct-1m" => 1 << 20,
        "direct-8m" => 8 << 20,
        "direct-64m" => 64 << 20,
        _ => COPY,
    };
    let b = zeros(piece);
    let t = Instant::now();
    if way == "copies-128k" {
        f.write_copies_at(b.as_slice(), 0, len / COPY as u64)
            .unwrap();
    } else {
        let mut at = 0u64;
        while at < len {
            f.write_all_at(b.as_slice(), at).unwrap();
            at += piece as u64;
        }
    }
    let writes = t.elapsed().as_secs_f64();
    let t = Instant::now();
    f.sync_data().unwrap();
    let flush = t.elapsed().as_secs_f64();
    drop(f);
    std::fs::remove_file(path).unwrap();
    (writes, flush, granted)
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let usage = "LOAD(none|flush) DIR [LEN [ROUNDS]]";
    let loaded = args.first().expect(usage) == "flush";
    let dir = PathBuf::from(args.get(1).expect(usage));
    let len: u64 = arg(&args, 2, 256 << 20);
    let rounds: usize = arg(&args, 3, 3);
    assert!(
        len.is_multiple_of(64 << 20),
        "{usage}: LEN a multiple of 64 MiB"
    );
    let path = dir.join("wholefill.bin");
    let load_path = dir.join("wholefill-load.bin");
    let stop = AtomicBool::new(false);
    let flushed = AtomicU64::new(0);
    std::thread::scope(|s| {
        let load = loaded.then(|| {
            let lf = DeviceFile::open(
                &load_path,
                true,
                CachingRequest::Buffered,
                Alignment::new(ALIGN).unwrap(),
            )
            .unwrap();
            let (stop, flushed) = (&stop, &flushed);
            s.spawn(move || {
                let b = zeros(LOAD_WRITE);
                let mut at = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    lf.write_all_at(b.as_slice(), at).unwrap();
                    at = (at + LOAD_WRITE as u64) % LOAD_SPAN;
                    lf.sync_data().unwrap();
                    flushed.fetch_add(1, Ordering::Relaxed);
                }
            })
        });
        // Beside the load once it has made a flush.
        if load.is_some() {
            while flushed.load(Ordering::Relaxed) == 0 {
                std::thread::yield_now();
            }
        }
        for r in 0..rounds {
            for k in 0..WAYS.len() {
                let way = WAYS[(k + r) % WAYS.len()];
                let (writes, flush, granted) = fill(way, &path, len);
                println!(
                    "way {way} granted {granted} load {} round {} len_mib {} writes_ms {:.1} flush_ms {:.1} total_ms {:.1} GBps {:.2}",
                    if loaded { "flush" } else { "none" },
                    r + 1,
                    len >> 20,
                    writes * 1e3,
                    flush * 1e3,
                    (writes + flush) * 1e3,
                    len as f64 / (writes + flush) / 1e9,
                );
            }
        }
        stop.store(true, Ordering::Relaxed);
        if let Some(l) = load {
            l.join().unwrap();
        }
    });
    let _ = std::fs::remove_file(&load_path);
}
