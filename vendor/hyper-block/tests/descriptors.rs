//! A segmented file's descriptors as the OS counts them (`hyper_measure::descriptors`): one for its
//! base and one a segment, however many handles transfer through it and however many workers the
//! device's issuer clones it for. This binary holds one test, so no other opens or closes a
//! descriptor during the counts.
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_block::block::BlockFile;
use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::file::CachingRequest;
use hyper_block::issuer::Issuer;
use hyper_block::segmented::SegmentedFile;
use hyper_measure::descriptors;

/// The page each transfer moves: the block of the file systems the tests run on.
const PAGE: usize = 4096;

fn align() -> Alignment {
    Alignment::new(PAGE).unwrap()
}

fn page(fill: u8) -> AlignedBuf {
    let mut buf = AlignedBuf::zeroed(PAGE, align()).unwrap();
    buf.extend_from_slice(&[fill; PAGE]).unwrap();
    buf
}

/// `count` pages filled with `fill`, one at each page from `first` on.
fn pages(first: u64, count: usize, fill: u8) -> Vec<(AlignedBuf, u64)> {
    (0..count as u64)
        .map(|i| (page(fill), (first + i) * PAGE as u64))
        .collect()
}

/// The page at `offset` of `file`.
fn read(file: &SegmentedFile, offset: u64) -> u8 {
    let mut back = page(0);
    file.read_exact_at(back.as_mut_slice(), offset).unwrap();
    back.as_slice()[0]
}

/// Has each of `issuer`'s workers, and this thread, transfer once through a segmented file of its
/// own in `dir`: on Windows a thread's first transfer through a segmented file makes the event its
/// transfers wait on (`hyper_block` `overlapped`), so none is made during the counts.
fn warm(issuer: &Issuer, dir: &std::path::Path) {
    let depth = issuer.depth();
    let warm =
        SegmentedFile::create(&dir.join("warm"), CachingRequest::Buffered, align(), 2).unwrap();
    warm.add((depth * PAGE) as u64).unwrap();
    let mut attached = issuer.attach(&warm).unwrap();
    attached.write(pages(0, depth, 1), true).unwrap();
    warm.write_all_at(page(1).as_slice(), 0).unwrap();
    attached.retire_blocking().unwrap();
}

/// Through each of `handles`, a page written into each of `segments` segments, each read back
/// through `owner`.
fn write_through(owner: &SegmentedFile, handles: &[SegmentedFile], segments: usize) {
    for (h, handle) in handles.iter().enumerate() {
        for s in 0..segments as u64 {
            handle
                .write_all_at(page(h as u8).as_slice(), s * PAGE as u64)
                .unwrap();
            assert_eq!(read(owner, s * PAGE as u64), h as u8);
        }
        handle.sync_data().unwrap();
    }
}

/// Do: start the device's issuer with a worker for each core the host reports; open a segmented
/// file and add a segment of a page for each worker, one at a time; make a handle for each
/// worker, attach the file to the issuer, and through every handle and the attachment write and
/// read a page in every segment; then drop them all. Expect: the file holds one descriptor more
/// than the process held before it, and each segment one more; the handles, the issuer's clones
/// of it for each of its workers and their transfers hold none; once all are dropped, the
/// process holds what it did before the file. Each thread that transfers has transferred once
/// before the first count ([`warm`]).
#[test]
fn a_segmented_file_holds_one_descriptor_a_segment_whatever_its_handles_and_workers() {
    let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    let dir = tempfile::tempdir().unwrap();
    let issuer = Issuer::start(dir.path(), cores).unwrap();
    warm(&issuer, dir.path());
    let segments = issuer.depth();
    let before = descriptors::open().unwrap();
    let owner = SegmentedFile::create(
        &dir.path().join("store"),
        CachingRequest::Buffered,
        align(),
        segments + 1,
    )
    .unwrap();
    assert_eq!(descriptors::open().unwrap(), before + 1, "the base");
    for s in 1..=segments {
        owner.add(PAGE as u64).unwrap();
        assert_eq!(descriptors::open().unwrap(), before + 1 + s, "segment {s}");
    }
    let held = before + 1 + segments;
    let handles: Vec<SegmentedFile> = (0..segments).map(|_| owner.try_clone().unwrap()).collect();
    let mut attached = issuer.attach(&owner).unwrap();
    assert_eq!(descriptors::open().unwrap(), held, "handles and clones");
    write_through(&owner, &handles, segments);
    assert_eq!(
        attached.write(pages(0, segments, 9), true).unwrap().len(),
        segments
    );
    assert_eq!(read(&handles[0], 0), 9);
    assert_eq!(descriptors::open().unwrap(), held, "after every transfer");
    attached.retire_blocking().unwrap();
    drop(handles);
    assert_eq!(
        descriptors::open().unwrap(),
        held,
        "the owner's, until it closes"
    );
    drop(owner);
    assert_eq!(descriptors::open().unwrap(), before, "every one closed");
}
