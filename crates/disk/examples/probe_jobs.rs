//! Prints raw job measurements for a directory: `cargo run --release --example probe_jobs -- DIR [SPAN_MIB]`.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

use std::time::Duration;

use mantle_disk::buf::Alignment;
use mantle_disk::file::{CachingRequest, DeviceFile};
use mantle_disk::measure::{Job, Pattern, run};

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = std::path::PathBuf::from(args.next().expect("DIR"));
    let span_mib: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(256);
    let span = span_mib << 20;
    for request in [CachingRequest::PreferDirect, CachingRequest::Buffered] {
        let path = dir.join(".mantle-probe");
        let _ = std::fs::remove_file(&path);
        let file = DeviceFile::open(&path, true, request, Alignment::new(4096).unwrap()).unwrap();
        file.preallocate(span).unwrap();
        println!("== {:?}", file.caching());
        let job = |pattern, block, depth, sync_each, ms, max_ops| Job {
            pattern,
            block,
            depth,
            base: 0,
            span,
            budget: Some(Duration::from_millis(ms)),
            max_ops,
            sync_each,
            seed: 42,
        };
        let r = run(
            &file,
            &job(
                Pattern::SequentialWrite,
                1 << 20,
                1,
                false,
                60_000,
                span >> 20,
            ),
        )
        .unwrap();
        file.sync_data().unwrap();
        println!(
            "fill seq-write 1MiB qd1: {:>8.1} MiB/s",
            r.bytes_per_sec() / 1048576.0
        );
        for depth in [1, 2, 4, 8, 16, 32, 64] {
            let r = run(
                &file,
                &job(Pattern::RandomRead, 4096, depth, false, 700, u64::MAX),
            )
            .unwrap();
            println!(
                "rand-read 4K  qd{depth:<3}: {:>9.0} IOPS  p50 {:>7} ns  p99 {:>8} ns",
                r.ops_per_sec(),
                r.latency.p50(),
                r.latency.p99()
            );
        }
        for depth in [1, 2, 4, 8] {
            let r = run(
                &file,
                &job(
                    Pattern::SequentialRead,
                    1 << 20,
                    depth,
                    false,
                    700,
                    u64::MAX,
                ),
            )
            .unwrap();
            println!(
                "seq-read 1MiB qd{depth:<3}: {:>8.1} MiB/s  p99 {:>8} ns",
                r.bytes_per_sec() / 1048576.0,
                r.latency.p99()
            );
        }
        for depth in [1, 2, 4, 8] {
            let r = run(
                &file,
                &job(
                    Pattern::SequentialWrite,
                    1 << 20,
                    depth,
                    false,
                    700,
                    u64::MAX,
                ),
            )
            .unwrap();
            println!(
                "seq-write 1MiB qd{depth:<3}: {:>7.1} MiB/s  p99 {:>8} ns",
                r.bytes_per_sec() / 1048576.0,
                r.latency.p99()
            );
        }
        for (block, depth) in [(4096, 1), (4096, 4), (1 << 20, 1)] {
            let r = run(
                &file,
                &job(Pattern::RandomWrite, block, depth, true, 1500, 400),
            )
            .unwrap();
            println!(
                "durable write {block:>7}B qd{depth}: {:>7.0} ops/s  p50 {:>8} ns  p99 {:>9} ns",
                r.ops_per_sec(),
                r.latency.p50(),
                r.latency.p99()
            );
        }
        drop(file);
        std::fs::remove_file(&path).unwrap();
    }
}
