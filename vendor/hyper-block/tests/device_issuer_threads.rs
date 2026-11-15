//! A device issuer whose transfers are the kernel's keeps its whole depth in flight on one thread,
//! its own: the threads the process runs grow by one as it starts, whatever its depth, and are
//! back where they were once it stops. A pool of the same depth runs a worker for each transfer
//! in flight besides (mantle research/26 §1.4: a thread per transfer is what panicked macOS).
//! One test alone in its binary, so no other test's threads move the count.
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

use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_block::threads;

/// Shape: the page each read moves.
const PAGE: usize = 4096;

/// The depths: one, a calibrated NVMe knee of the kind CI runners measure (64), and the deepest a
/// runner's queue reported (316, `nr_requests` of an Azure virtual disk).
const DEPTHS: [usize; 3] = [1, 64, 316];

#[test]
fn a_kernel_issuer_runs_its_whole_depth_on_one_thread() {
    let dir = tempfile::tempdir().unwrap();
    let align = Alignment::new(PAGE).unwrap();
    let file = DeviceFile::open(
        &dir.path().join("volume"),
        true,
        CachingRequest::PreferDirect,
        align,
    )
    .unwrap();
    let most = *DEPTHS.iter().max().unwrap();
    file.preallocate((most * PAGE) as u64).unwrap();
    // Counted before any issuer starts and while all of them run, never across a join: Linux
    // counts a joined thread a while after its joiner wakes (kernel/exit.c, `release_task`).
    let before = threads::count().unwrap();
    let issuers: Vec<Issuer> = DEPTHS
        .iter()
        .map(|&depth| Issuer::start_device(dir.path(), depth).unwrap())
        .collect();
    assert_eq!(
        threads::count().unwrap(),
        before + DEPTHS.len(),
        "each kernel issuer runs one thread, whatever its depth ({DEPTHS:?})"
    );
    for (issuer, depth) in issuers.iter().zip(DEPTHS) {
        assert_eq!(issuer.depth(), depth);
        assert_eq!(
            Issuer::device_threads(depth),
            Some(1),
            "what a node counts of it"
        );
        let mut attached = issuer.attach(&file).unwrap();
        let reads = (0..depth)
            .map(|i| {
                let mut buf = AlignedBuf::zeroed(PAGE, align).unwrap();
                buf.set_len(PAGE).unwrap();
                (buf, (i * PAGE) as u64)
            })
            .collect();
        // Every lane holds a read at once, and still one thread an issuer runs them.
        let number = attached.submit_reads(reads).unwrap();
        let (answered, answer) = attached.answer().unwrap();
        assert_eq!(answered, number);
        assert_eq!(answer.unwrap().len(), depth);
        assert_eq!(threads::count().unwrap(), before + DEPTHS.len());
    }
}
