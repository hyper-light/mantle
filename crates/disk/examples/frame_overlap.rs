//! What a small write to a second region costs a batch that is written and then flushed, issued
//! after the batch's data or beside it:
//! `cargo run --release --example frame_overlap -- DIR [DATA_MIB] [ROUNDS]`.
//! Answers whether the chunk store should issue its index frame alongside a batch's records
//! (docs/measurements/2026-09-28-chunk-store-benchmark.md, finding 8). The three patterns run
//! interleaved, one batch of each in turn, so drift in the device affects each alike.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::indexing_slicing
)]

use std::time::Instant;

use mantle_disk::buf::{AlignedBuf, Alignment};
use mantle_disk::file::{CachingRequest, DeviceFile};

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = std::path::PathBuf::from(args.next().expect("DIR"));
    let data_len: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(32) << 20;
    let rounds: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(64);
    let align = Alignment::new(4096).unwrap();
    let path = dir.join(".mantle-frame-overlap");
    let _ = std::fs::remove_file(&path);
    let file = DeviceFile::open(&path, true, CachingRequest::PreferDirect, align).unwrap();
    // Data cycles through 1 GiB; the frame region sits 2 GiB in, as the log sits apart from
    // the segments. Both are written once first, so every write overwrites written blocks.
    let span = 1u64 << 30;
    let frame_at = 2u64 << 30;
    file.preallocate(frame_at + (64 << 20)).unwrap();
    let mut data = AlignedBuf::zeroed(data_len, align).unwrap();
    data.set_len(data_len).unwrap();
    data.as_mut_slice().fill(0x5a);
    let mut frame = AlignedBuf::zeroed(4096, align).unwrap();
    frame.set_len(4096).unwrap();
    frame.as_mut_slice().fill(0xa5);
    let mut at = 0u64;
    while at < span {
        file.write_all_at(data.as_slice(), at).unwrap();
        at += data_len as u64;
    }
    for i in 0..16u64 {
        file.write_all_at(frame.as_slice(), frame_at + i * 4096)
            .unwrap();
    }
    file.sync_data().unwrap();
    println!(
        "caching {:?}, {} MiB batches, {rounds} of each",
        file.caching(),
        data_len >> 20
    );

    let names = [
        "data, flush",
        "data, frame after, flush",
        "data and frame at once, flush",
    ];
    let mut times: [Vec<u64>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    let mut at = 0u64;
    let mut slot = 0u64;
    for _ in 0..rounds {
        for (pattern, samples) in times.iter_mut().enumerate() {
            let data_at = at;
            let frame_pos = frame_at + (slot % 16) * 4096;
            at = (at + data_len as u64) % span;
            slot += 1;
            let started = Instant::now();
            match pattern {
                0 => file.write_all_at(data.as_slice(), data_at).unwrap(),
                1 => {
                    file.write_all_at(data.as_slice(), data_at).unwrap();
                    file.write_all_at(frame.as_slice(), frame_pos).unwrap();
                }
                _ => std::thread::scope(|scope| {
                    let side = scope.spawn(|| file.write_all_at(frame.as_slice(), frame_pos));
                    file.write_all_at(data.as_slice(), data_at).unwrap();
                    side.join().unwrap().unwrap();
                }),
            }
            file.sync_data().unwrap();
            samples.push(started.elapsed().as_nanos() as u64);
        }
    }
    for (name, samples) in names.iter().zip(times.iter_mut()) {
        samples.sort_unstable();
        let p50 = samples[samples.len() / 2];
        let total: u64 = samples.iter().sum();
        let rate = data_len as f64 * samples.len() as f64 / (total as f64 / 1e9);
        println!(
            "{name:<32} p50 {:>6.2} ms  {:>6.2} GB/s",
            p50 as f64 / 1e6,
            rate / 1e9
        );
    }
    drop(file);
    let _ = std::fs::remove_file(&path);
}
