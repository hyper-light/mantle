//! Whether reads on this device stall in rounds, and whether durable writes bring the stalls:
//! `cargo run --release --example fresh_reads -- DIR [ROUNDS] [write|unflushed|idle]`.
//! Each round spends a second on one-at-a-time 64 KiB writes, each followed by a full flush,
//! as a chunk store's single writer does (`write`), the same number of writes without the
//! flushes, spread over the second (`unflushed`), or a second idle (`idle`); then a second
//! of one-at-a-time random 64 KiB reads of what that round wrote, and a second of the same
//! reads over data written before the rounds began. Every read slower than a millisecond is
//! listed with when it ended, so bursts can be matched against the rounds' phases. Answers
//! whether the chunk benchmark's reads that alternate between full and a tenth of full rate
//! are the store's or the device's (docs/measurements/2026-09-29-chunk-store-states.md).
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::indexing_slicing,
    // The idle and unflushed phases wait on purpose; there is no worker to stall.
    clippy::disallowed_methods
)]

use std::time::{Duration, Instant};

use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_disk::measure::SplitMix64;

const BLOCK: usize = 64 << 10;
const PHASE: Duration = Duration::from_secs(1);

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = std::path::PathBuf::from(args.next().expect("DIR"));
    let rounds: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(40);
    let mode = args.next().unwrap_or_else(|| "write".to_owned());
    let write = mode != "idle";
    let flush = mode == "write";
    // Writes per second the flushed mode reached on the machine this was run on, for the
    // unflushed mode to match.
    let paced = Duration::from_secs(1) / 260;
    let align = Alignment::new(4096).unwrap();
    let path = dir.join(".mantle-fresh-reads");
    let _ = std::fs::remove_file(&path);
    let file = DeviceFile::open(&path, true, CachingRequest::PreferDirect, align).unwrap();
    // Old data in the first GiB, written once; fresh writes advance through the next 3 GiB.
    let old = 1u64 << 30;
    let total = 4u64 << 30;
    file.preallocate(total).unwrap();
    let mut big = AlignedBuf::zeroed(8 << 20, align).unwrap();
    big.set_len(8 << 20).unwrap();
    SplitMix64::new(1).fill(big.as_mut_slice());
    let mut at = 0u64;
    while at < total {
        file.write_all_at(big.as_slice(), at).unwrap();
        at += 8 << 20;
    }
    file.sync_data().unwrap();
    let mut buf = AlignedBuf::zeroed(BLOCK, align).unwrap();
    buf.set_len(BLOCK).unwrap();
    SplitMix64::new(2).fill(buf.as_mut_slice());
    println!("caching {:?}, {} rounds, {mode}", file.caching(), rounds,);

    let start = Instant::now();
    let mut rng = SplitMix64::new(3);
    let mut next = old;
    let mut slow: Vec<(f64, f64, &str)> = Vec::new();
    println!("round  writes/s  fresh reads/s  old reads/s  (phase starts, s)");
    for round in 0..rounds {
        let t_write = start.elapsed().as_secs_f64();
        let from = next;
        let mut writes = 0u64;
        let deadline = Instant::now() + PHASE;
        if write {
            while Instant::now() < deadline && next + BLOCK as u64 <= total {
                let t = Instant::now();
                file.write_all_at(buf.as_slice(), next).unwrap();
                if flush {
                    file.sync_data().unwrap();
                } else if let Some(rest) = paced.checked_sub(t.elapsed()) {
                    std::thread::sleep(rest);
                }
                next += BLOCK as u64;
                writes += 1;
            }
        } else {
            std::thread::sleep(PHASE);
            // The same span each round, written before the rounds began.
            next = from + (240 * BLOCK) as u64;
        }
        let fresh_blocks = (next - from) / BLOCK as u64;
        let t_fresh = start.elapsed().as_secs_f64();
        let fresh = reads(
            &file,
            &mut buf,
            &mut rng,
            from,
            fresh_blocks,
            start,
            "fresh",
            &mut slow,
        );
        let t_old = start.elapsed().as_secs_f64();
        let olds = reads(
            &file,
            &mut buf,
            &mut rng,
            0,
            old / BLOCK as u64,
            start,
            "old",
            &mut slow,
        );
        println!(
            "{round:>5} {writes:>9} {fresh:>14} {olds:>12}  ({t_write:.2}, {t_fresh:.2}, {t_old:.2})"
        );
        if !write {
            next = from;
        }
    }
    println!("reads slower than 1 ms (ended at s, took ms, region):");
    for (at, ms, region) in &slow {
        println!("  {at:9.3} {ms:8.2} {region}");
    }
    let _ = std::fs::remove_file(&path);
}

/// One-at-a-time random reads of `blocks` 64 KiB blocks from `base` for a phase; returns reads
/// completed and records each slower than a millisecond.
#[allow(clippy::too_many_arguments)]
fn reads<'a>(
    file: &DeviceFile,
    buf: &mut AlignedBuf,
    rng: &mut SplitMix64,
    base: u64,
    blocks: u64,
    start: Instant,
    region: &'a str,
    slow: &mut Vec<(f64, f64, &'a str)>,
) -> u64 {
    if blocks == 0 {
        return 0;
    }
    let deadline = Instant::now() + PHASE;
    let mut n = 0u64;
    while Instant::now() < deadline {
        let at = base + rng.below(blocks) * BLOCK as u64;
        let t = Instant::now();
        file.read_exact_at(buf.as_mut_slice(), at).unwrap();
        let took = t.elapsed();
        if took > Duration::from_millis(1) {
            slow.push((
                start.elapsed().as_secs_f64(),
                took.as_secs_f64() * 1e3,
                region,
            ));
        }
        n += 1;
    }
    n
}
