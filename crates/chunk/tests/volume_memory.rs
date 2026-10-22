//! What a volume holds in memory at work, counted exactly across its threads by hyper-measure's
//! counting allocator, within what `layout::most_bytes` states beside its index's
//! `index::most_bytes`: writers filling its queue with chunks two to a segment, every other one
//! deleted so its cleaner relocates the rest, a scrub and a checkpoint. On a real file, whose bytes
//! the kernel keeps, not the process.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

mod common;

use std::time::Duration;

use common::{data, issuer, key, put_retrying};
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_measure::alloc;
use mantle_chunk::index;
use mantle_chunk::layout::{self, Geometry};
use mantle_chunk::{Config, Limits, Reads, Volume};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

/// The chunks the test writes, from `WRITERS` threads at once: each half of `CHUNK`, the largest
/// a segment holds, so two share a segment.
const CHUNKS: u64 = 48;
const CHUNK: u64 = 1 << 20;
const WRITERS: u64 = 4;

#[test]
fn a_volume_at_work_holds_at_most_its_stated_bytes() {
    let align = Alignment::new(4096).unwrap();
    // As `mantle_node::derive::volume` lays one out at the floor, for chunks of `CHUNK`.
    let segment = layout::least_segment(4096, 12, CHUNK).unwrap();
    let config = Config {
        segment_size: segment,
        checksum_shift: 12,
        max_fragments: 4096,
        compact: true,
        scrub_period: Some(Duration::from_secs(7 * 24 * 3600)),
        limits: Limits {
            batch_requests: 1,
            batch_bytes: usize::try_from(segment - 4096).unwrap(),
            fragments_per_chunk: 1,
        },
        prewrite: false,
        reads: Reads::default(),
    };
    let size = 64 << 20;
    let geometry = Geometry::plan(size, align, &config).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = DeviceFile::open(
        &dir.path().join("volume"),
        true,
        CachingRequest::PreferDirect,
        align,
    )
    .unwrap();
    file.preallocate(size).unwrap();
    // What the test holds and the device's issuer are not the volume's.
    let bytes: Vec<Vec<u8>> = (0..WRITERS).map(|n| data(n, CHUNK as usize / 2)).collect();
    let issuer = issuer();
    alloc::begin_process();
    let v = Volume::format(issuer, file, size, config).unwrap();
    std::thread::scope(|s| {
        for w in 0..WRITERS {
            let (v, bytes) = (&v, &bytes[w as usize]);
            s.spawn(move || {
                for n in (w..CHUNKS).step_by(WRITERS as usize) {
                    put_retrying(v, key(n), bytes).unwrap();
                }
            });
        }
    });
    for n in (0..CHUNKS).step_by(2) {
        v.delete(key(n)).unwrap();
    }
    let cleaned = v.clean(geometry.segments).unwrap();
    v.scrub().unwrap();
    v.checkpoint().unwrap();
    v.close();
    let peak = u64::try_from(alloc::end_process().peak).unwrap();
    let most = layout::most_bytes(&config, &geometry).unwrap() + index::most_bytes(CHUNKS).unwrap();
    eprintln!(
        "peak {peak} of {most}: {:.2}; cleaned {cleaned:?}",
        peak as f64 / most as f64
    );
    assert!(peak <= most, "the volume reached {peak} bytes, past {most}");
}
