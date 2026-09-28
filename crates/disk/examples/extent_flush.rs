//! Durable-append latency into freshly allocated space vs. overwriting written space:
//! `cargo run --release --example extent_flush -- DIR [SPAN_MIB] [BLOCK_KIB]`.
//! Answers whether steady-state appends should overwrite pre-written blocks (a metadata
//! journal commit per flush when extents are unwritten or the file grows).
#![allow(clippy::unwrap_used, clippy::disallowed_macros, clippy::expect_used)]

use std::time::Duration;

use mantle_disk::buf::Alignment;
use mantle_disk::file::{CachingRequest, DeviceFile};
use mantle_disk::measure::{Job, Pattern, run};

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = std::path::PathBuf::from(args.next().expect("DIR"));
    let span: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(64) << 20;
    let block: usize = args
        .next()
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(64)
        << 10;
    let ops = span / block as u64;
    let job = Job {
        pattern: Pattern::SequentialWrite,
        block,
        depth: 1,
        span,
        budget: Duration::from_secs(120),
        max_ops: ops,
        sync_each: true,
        seed: 1,
    };
    let report = |label: &str, file: &DeviceFile| {
        let r = run(file, &job).unwrap();
        println!(
            "{label:<34} {:>6} ops  p50 {:>9} ns  p99 {:>9} ns  {:>8.1} MiB/s",
            r.ops,
            r.latency.p50(),
            r.latency.p99(),
            r.bytes_per_sec() / 1048576.0
        );
    };
    let path = dir.join(".mantle-extent-probe");
    let _ = std::fs::remove_file(&path);
    let file = DeviceFile::open(
        &path,
        true,
        CachingRequest::PreferDirect,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    println!(
        "caching {:?}, block {} KiB, span {} MiB",
        file.caching(),
        block >> 10,
        span >> 20
    );
    file.preallocate(span).unwrap();
    file.sync_data().unwrap();
    report("preallocated, first write", &file);
    report("same region, overwrite", &file);
    drop(file);
    std::fs::remove_file(&path).unwrap();
    // Growing the file with each write: no preallocation at all.
    let file = DeviceFile::open(
        &path,
        true,
        CachingRequest::PreferDirect,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    report("extending, no preallocation", &file);
    drop(file);
    std::fs::remove_file(&path).unwrap();
}
