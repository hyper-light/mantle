//! The threads a node's storage starts, as `derive::threads` counts them: the process's threads
//! before and after a volume opens, as Linux reports them (proc(5), `/proc/self/status`). A
//! binary of its own, so no other test starts a thread meanwhile.
#![cfg(target_os = "linux")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::time::Duration;

use mantle_chunk::{Config, Limits, Reads};
use mantle_node::derive;
use mantle_node::storage::{Storage, VolumeSettings};

/// The process's threads now.
fn threads() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    status
        .lines()
        .find_map(|line| line.strip_prefix("Threads:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

#[test]
fn a_volume_starts_the_threads_the_derivation_counts() {
    let dir = tempfile::tempdir().unwrap();
    let depth = 2;
    let volume = VolumeSettings {
        dir: dir.path().to_path_buf(),
        size: 64 << 20,
        config: Config {
            segment_size: 4 << 20,
            checksum_shift: 12,
            max_fragments: 4096,
            compact: true,
            scrub_period: Some(Duration::from_secs(7 * 24 * 3600)),
            limits: Limits {
                batch_requests: 1,
                batch_bytes: 4 << 20,
                fragments_per_chunk: 1,
            },
            prewrite: false,
            reads: Reads::measured(depth, 1 << 20, 0),
        },
        depth,
    };
    let before = threads();
    let storage = Storage::open(&[volume]).unwrap();
    let started = threads() - before;
    // The node's own five are its process's, not its storage's.
    assert_eq!(started, derive::threads(&[depth], &[true]).unwrap() - 5);
    drop(storage);
}
