//! What a batch costs through the device's issuer against the per-region threads it replaced:
//! `cargo run --release --example issuer_batch -- DIR [DATA_KIB] [ROUNDS] [DEPTH]`.
//! A batch is one data region and a 4 KiB frame, written at once and then flushed, as the chunk
//! store writes a batch of one segment. The two patterns run interleaved, one batch of each in
//! turn, so drift in the device and the machine's load affects each alike:
//! - threads: the region on the calling thread and the frame on a thread started for it, then
//!   the flush on the calling thread, as `write_together` did;
//! - issuer: both handed to an issuer of DEPTH workers, which flushes once both complete.
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::indexing_slicing
)]

use std::time::Instant;

use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;

fn buf(len: usize, byte: u8, align: Alignment) -> AlignedBuf {
    let mut b = AlignedBuf::zeroed(len, align).unwrap();
    b.set_len(len).unwrap();
    b.as_mut_slice().fill(byte);
    b
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = std::path::PathBuf::from(args.next().expect("DIR"));
    let data_len: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(1024) << 10;
    let rounds: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(200);
    let depth: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(64);
    let align = Alignment::new(4096).unwrap();
    let path = dir.join(".mantle-issuer-batch");
    let _ = std::fs::remove_file(&path);
    let file = DeviceFile::open(&path, true, CachingRequest::PreferDirect, align).unwrap();
    // Data cycles through 256 MiB, written once first; the frame region sits apart, as the
    // log does.
    let span = 256u64 << 20;
    let frame_at = 512u64 << 20;
    file.preallocate(frame_at + (1 << 20)).unwrap();
    let data = buf(data_len, 0x5a, align);
    let frame = buf(4096, 0xa5, align);
    let mut at = 0u64;
    while at < span {
        file.write_all_at(data.as_slice(), at).unwrap();
        at += data_len as u64;
    }
    file.write_all_at(frame.as_slice(), frame_at).unwrap();
    file.sync_data().unwrap();
    let issuer = Issuer::start(&path, depth).unwrap();
    let mut attached = issuer.attach(&file).unwrap();
    let (mut data, mut frame) = (Some(data), Some(frame));
    let mut took = [Vec::new(), Vec::new()];
    let mut at = 0u64;
    for round in 0..rounds * 2 {
        let pattern = round % 2;
        let started = Instant::now();
        if pattern == 0 {
            let (d, f) = (data.as_ref().unwrap(), frame.as_ref().unwrap());
            std::thread::scope(|s| {
                let other = s.spawn(|| file.write_all_at(f.as_slice(), frame_at));
                file.write_all_at(d.as_slice(), at).unwrap();
                other.join().unwrap().unwrap();
            });
            file.sync_data().unwrap();
        } else {
            let writes = vec![
                (data.take().unwrap(), at),
                (frame.take().unwrap(), frame_at),
            ];
            let mut back = attached.write(writes, true).unwrap();
            frame = back.pop().map(|(b, _)| b);
            data = back.pop().map(|(b, _)| b);
        }
        took[pattern].push(started.elapsed().as_nanos() as u64);
        at = (at + data_len as u64) % span;
    }
    println!(
        "caching {:?}, {} KiB regions and a 4 KiB frame, {rounds} batches of each, issuer depth {}",
        file.caching(),
        data_len >> 10,
        issuer.depth()
    );
    for (name, mut t) in ["threads", "issuer"].into_iter().zip(took) {
        t.sort_unstable();
        let q = |p: f64| t[((t.len() - 1) as f64 * p) as usize] as f64 / 1e6;
        let mean = t.iter().sum::<u64>() as f64 / t.len() as f64 / 1e6;
        println!(
            "{name:<8} p50 {:.2} ms  p90 {:.2} ms  p99 {:.2} ms  mean {mean:.2} ms",
            q(0.5),
            q(0.9),
            q(0.99)
        );
    }
    drop(attached);
    drop(issuer);
    let _ = std::fs::remove_file(&path);
}
