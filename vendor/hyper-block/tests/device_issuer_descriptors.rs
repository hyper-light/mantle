//! A device issuer whose transfers are the kernel's shares one duplicate of each file attached among
//! its lanes, all on its one thread: attaching a file opens one descriptor in the issuer and one for
//! the submitter's own way to the issuer's bell, whatever the depth, where a pool's workers, each a
//! thread, hold one each, a descriptor a transfer in flight. One test alone in its binary, so no other
//! test's descriptors move the count.
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

/// Shape: the page the file is made of.
const PAGE: usize = 4096;

/// The depths: one, a calibrated NVMe knee of the kind CI runners measure (64), and the deepest a
/// runner's queue reported (316, `nr_requests` of an Azure virtual disk).
const DEPTHS: [usize; 3] = [1, 64, 316];

/// The process's open descriptors, as Linux lists them (proc_pid_fd(5)); the listing's own is one
/// of every count, so differences are exact.
fn descriptors() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

#[test]
fn a_kernel_issuers_lanes_share_one_descriptor_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let align = Alignment::new(PAGE).unwrap();
    let file = DeviceFile::open(
        &dir.path().join("volume"),
        true,
        CachingRequest::PreferDirect,
        align,
    )
    .unwrap();
    file.preallocate(PAGE as u64).unwrap();
    // The submitter's own duplicate, kept only for a file that reads from memory.
    let reader = usize::from(hyper_block::block::BlockFile::reads_resident(&file));
    for depth in DEPTHS {
        let issuer = Issuer::start_device(dir.path(), depth).unwrap();
        let before = descriptors();
        let attached = issuer.attach(&file).unwrap();
        assert_eq!(
            descriptors(),
            before + 2 + reader,
            "an issuer of depth {depth}: its one duplicate and the submitter's way to its bell"
        );
        drop(attached);
        assert_eq!(
            descriptors(),
            before,
            "a detach gives every descriptor back"
        );
        // Dropped with its reads out, the attachment's file is retired once they have ended.
        let mut attached = issuer.attach(&file).unwrap();
        let reads = (0..depth.min(2))
            .map(|_| {
                let mut buf = AlignedBuf::zeroed(PAGE, align).unwrap();
                buf.set_len(PAGE).unwrap();
                (buf, 0)
            })
            .collect();
        attached.submit_reads(reads).unwrap();
        drop(attached);
        assert_eq!(
            descriptors(),
            before,
            "a detach deferred for its transfers gives every descriptor back"
        );
        drop(issuer);
    }
}
