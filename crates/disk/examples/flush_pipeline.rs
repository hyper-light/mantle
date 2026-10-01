//! Whether writing the next batch while the last one flushes raises durable throughput:
//! `cargo run --release --example flush_pipeline -- DIR [DATA_MIB] [ROUNDS]`.
//! Serial: write a batch, flush, repeat. Pipelined: a flusher thread flushes batch N while the
//! caller writes batch N+1, and the next flush starts once both have ended; flushes stay one
//! at a time, as a group-commit writer's must. Each batch is its data and a 4 KiB frame in a
//! separate region, issued together. The two patterns alternate in blocks of 16 batches.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::indexing_slicing
)]

use std::time::Instant;

use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::file::{CachingRequest, DeviceFile};

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = std::path::PathBuf::from(args.next().expect("DIR"));
    let data_len: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(32) << 20;
    let rounds: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(8);
    let align = Alignment::new(4096).unwrap();
    let path = dir.join(".mantle-flush-pipeline");
    let _ = std::fs::remove_file(&path);
    let file = DeviceFile::open(&path, true, CachingRequest::PreferDirect, align).unwrap();
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
        "caching {:?}, {} MiB batches",
        file.caching(),
        data_len >> 20
    );

    let block = 16usize;
    let mut next_at = 0u64;
    let mut slot = 0u64;
    let mut write_batch = |file: &DeviceFile| {
        let data_at = next_at;
        next_at = (next_at + data_len as u64) % span;
        let frame_pos = frame_at + (slot % 16) * 4096;
        slot += 1;
        std::thread::scope(|scope| {
            let side = scope.spawn(|| file.write_all_at(frame.as_slice(), frame_pos));
            file.write_all_at(data.as_slice(), data_at).unwrap();
            side.join().unwrap().unwrap();
        });
    };
    let (mut serial, mut piped) = (Vec::new(), Vec::new());
    for _ in 0..rounds {
        let started = Instant::now();
        for _ in 0..block {
            write_batch(&file);
            file.sync_data().unwrap();
        }
        serial.push(started.elapsed().as_secs_f64());

        let started = Instant::now();
        write_batch(&file);
        std::thread::scope(|scope| {
            let (to_flusher, flush_requests) = std::sync::mpsc::sync_channel::<()>(1);
            let (flushed, from_flusher) = std::sync::mpsc::sync_channel::<()>(1);
            let f = &file;
            scope.spawn(move || {
                while flush_requests.recv().is_ok() {
                    f.sync_data().unwrap();
                    flushed.send(()).unwrap();
                }
            });
            for i in 0..block {
                to_flusher.send(()).unwrap(); // flush batch i
                if i + 1 < block {
                    write_batch(&file); // batch i+1 while batch i flushes
                }
                from_flusher.recv().unwrap(); // batch i durable
            }
            drop(to_flusher);
        });
        piped.push(started.elapsed().as_secs_f64());
    }
    let rate = |secs: &[f64]| {
        let total: f64 = secs.iter().sum();
        (data_len * block * secs.len()) as f64 / total / 1e9
    };
    println!("serial     {:>6.2} GB/s", rate(&serial));
    println!("pipelined  {:>6.2} GB/s", rate(&piped));
    drop(file);
    let _ = std::fs::remove_file(&path);
}
