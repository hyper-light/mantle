//! A device issuer whose transfers are the kernel's (`Issuer::start_device`, Linux's native AIO):
//! batches of writes, their flush and reads round-trip through it over a direct file and a
//! buffered one, with the errors the file's own calls give; several submitters share its lanes;
//! and a file the kernel does not hold whole is refused before any duplicate is made.
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

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::file::{Caching, CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;

/// Shape: the page each transfer moves, the block of the file systems the tests run on.
const PAGE: usize = 4096;
/// Shape: the lanes the device issuers keep, more than any batch here so none waits for room.
const DEPTH: usize = 16;
/// Shape: the pages a batch writes and reads, fewer than the lanes and more than one.
const PAGES: usize = 8;

fn align() -> Alignment {
    Alignment::new(PAGE).unwrap()
}

fn file(dir: &Path, name: &str, request: CachingRequest, len: u64) -> DeviceFile {
    let file = DeviceFile::open(&dir.join(name), true, request, align()).unwrap();
    file.preallocate(len).unwrap();
    file
}

fn page(fill: u8) -> AlignedBuf {
    let mut buf = AlignedBuf::zeroed(PAGE, align()).unwrap();
    buf.extend_from_slice(&[fill; PAGE]).unwrap();
    buf
}

fn empty() -> AlignedBuf {
    let mut buf = AlignedBuf::zeroed(PAGE, align()).unwrap();
    buf.set_len(PAGE).unwrap();
    buf
}

/// The `i`th page's fill, distinct for each page of a batch and from a fresh file's zeros.
fn fill(i: usize, seed: u8) -> u8 {
    u8::try_from(i).unwrap().wrapping_mul(7).wrapping_add(seed) | 1
}

fn writes(seed: u8, base: u64) -> Vec<(AlignedBuf, u64)> {
    (0..PAGES)
        .map(|i| (page(fill(i, seed)), base + (i * PAGE) as u64))
        .collect()
}

/// A page at `at` read through `file`'s own handle, into a buffer at its alignment.
fn read_page(file: &DeviceFile, at: u64) -> Vec<u8> {
    let mut buf = empty();
    file.read_exact_at(buf.as_mut_slice(), at).unwrap();
    buf.as_slice().to_vec()
}

fn reads(base: u64) -> Vec<(AlignedBuf, u64)> {
    (0..PAGES)
        .map(|i| (empty(), base + (i * PAGE) as u64))
        .collect()
}

/// Every buffer of a batch's answer back in its place, holding `seed`'s fills.
fn check(answer: &[(AlignedBuf, u64)], seed: u8) {
    assert_eq!(answer.len(), PAGES);
    for (i, (buf, at)) in answer.iter().enumerate() {
        assert_eq!(
            *at,
            (i * PAGE) as u64,
            "each offset back where it was given"
        );
        assert_eq!(buf.as_slice(), &[fill(i, seed); PAGE], "page {i}");
    }
}

/// Writes a batch with its flush, reads it back, and checks every byte and every buffer given
/// back in order.
fn round_trip(request: CachingRequest) -> Caching {
    let dir = tempfile::tempdir().unwrap();
    let issuer = Issuer::start_device(dir.path(), DEPTH).unwrap();
    assert_eq!(issuer.depth(), DEPTH);
    let file = file(dir.path(), "volume", request, (PAGES * PAGE * 2) as u64);
    let mut attached = issuer.attach(&file).unwrap();
    check(&attached.write(writes(3, 0), true).unwrap(), 3);
    let number = attached.submit_reads(reads(0)).unwrap();
    let (answered, answer) = attached.answer().unwrap();
    assert_eq!(answered, number);
    check(&answer.unwrap(), 3);
    // And through the file's own handle, which the issuer's duplicates share.
    let last = (PAGES - 1) as u64 * PAGE as u64;
    assert_eq!(read_page(&file, last), vec![fill(PAGES - 1, 3); PAGE]);
    file.caching()
}

/// Over a file opened for direct I/O, where the file system takes it (tmpfs does not), the
/// kernel moves the pages itself.
#[test]
fn a_direct_files_batch_round_trips_through_the_kernel() {
    round_trip(CachingRequest::PreferDirect);
}

/// Over a buffered file the kernel makes each transfer inside io_submit: the same bytes, one at a
/// time.
#[test]
fn a_buffered_files_batch_round_trips_through_the_kernel() {
    assert_eq!(round_trip(CachingRequest::Buffered), Caching::Buffered);
}

/// A read past the file's end is the short read the file's own call gives, its batch failed, the
/// issuer usable after.
#[test]
fn a_read_past_the_end_through_the_kernel_is_a_short_read() {
    let dir = tempfile::tempdir().unwrap();
    let issuer = Issuer::start_device(dir.path(), DEPTH).unwrap();
    let file = file(
        dir.path(),
        "short",
        CachingRequest::PreferDirect,
        PAGE as u64,
    );
    let mut attached = issuer.attach(&file).unwrap();
    let mut buf = AlignedBuf::zeroed(2 * PAGE, align()).unwrap();
    buf.set_len(2 * PAGE).unwrap();
    let number = attached.submit_reads(vec![(buf, 0)]).unwrap();
    let (answered, answer) = attached.answer().unwrap();
    assert_eq!(answered, number);
    match answer {
        Err(DiskError::ShortRead {
            offset, missing, ..
        }) => {
            assert_eq!(offset, 0);
            assert_eq!(missing, PAGE);
        }
        other => panic!("a read past the end: {other:?}"),
    }
    attached.write(vec![(page(9), 0)], true).unwrap();
    assert_eq!(read_page(&file, 0), vec![9u8; PAGE]);
}

/// A transfer off a direct file's alignment is refused as the file's own call refuses it,
/// before the kernel sees it.
#[test]
fn a_misaligned_transfer_is_refused_before_the_kernel_sees_it() {
    let dir = tempfile::tempdir().unwrap();
    let issuer = Issuer::start_device(dir.path(), DEPTH).unwrap();
    let file = file(
        dir.path(),
        "aligned",
        CachingRequest::PreferDirect,
        PAGE as u64,
    );
    if file.caching() != Caching::Direct {
        // A buffered file's alignment is one byte: nothing is off it.
        return;
    }
    let mut attached = issuer.attach(&file).unwrap();
    let refused = attached.write(vec![(page(1), 512)], false);
    assert!(
        matches!(refused, Err(DiskError::Misaligned { offset: 512, .. })),
        "{refused:?}"
    );
    let landed = attached.write(vec![(page(2), 0)], true);
    assert!(landed.is_ok(), "{landed:?}");
}

/// Submitters on one device share its lanes, each batch answered whole and named by its number.
#[test]
fn submitters_share_the_kernel_lanes() {
    let dir = tempfile::tempdir().unwrap();
    let issuer = Issuer::start_device_for(dir.path(), DEPTH, 4).unwrap();
    let file = file(
        dir.path(),
        "shared",
        CachingRequest::PreferDirect,
        (PAGES * PAGE * 4) as u64,
    );
    std::thread::scope(|scope| {
        for submitter in 0..2u8 {
            let mut attached = issuer.attach_deep(&file, 2).unwrap();
            scope.spawn(move || {
                let base = u64::from(submitter) * 2 * (PAGES * PAGE) as u64;
                let first = attached.submit(writes(submitter, base), false).unwrap();
                let second = attached
                    .submit(writes(submitter + 10, base + (PAGES * PAGE) as u64), true)
                    .unwrap();
                let mut answered = [attached.answer().unwrap(), attached.answer().unwrap()];
                answered.sort_by_key(|(number, _)| *number);
                assert_eq!(answered[0].0, first);
                assert_eq!(answered[1].0, second);
                for (_, answer) in answered {
                    assert_eq!(answer.unwrap().len(), PAGES);
                }
            });
        }
    });
    for submitter in 0..2u8 {
        let base = u64::from(submitter) * 2 * (PAGES * PAGE) as u64;
        for (batch, seed) in [(0u64, submitter), (1, submitter + 10)] {
            for i in 0..PAGES {
                let at = base + batch * (PAGES * PAGE) as u64 + (i * PAGE) as u64;
                assert_eq!(read_page(&file, at), vec![fill(i, seed); PAGE]);
            }
        }
    }
}

/// A batch larger than the lanes waits for room in order and is answered whole.
#[test]
fn a_batch_past_the_lanes_waits_for_room_and_lands_whole() {
    let dir = tempfile::tempdir().unwrap();
    let issuer = Issuer::start_device(dir.path(), 2).unwrap();
    let file = file(
        dir.path(),
        "deep",
        CachingRequest::PreferDirect,
        (PAGES * PAGE) as u64,
    );
    let mut attached = issuer.attach(&file).unwrap();
    attached.write(writes(5, 0), true).unwrap();
    let number = attached.submit_reads(reads(0)).unwrap();
    let (answered, answer) = attached.answer().unwrap();
    assert_eq!(answered, number);
    for (i, (buf, _)) in answer.unwrap().iter().enumerate() {
        assert_eq!(buf.as_slice(), &[fill(i, 5); PAGE]);
    }
}

/// A file with no open file of its own for the kernel to transfer (one in memory) is refused by
/// a kernel issuer, typed, before any duplicate of it is made.
#[test]
fn a_file_the_kernel_does_not_hold_is_refused_at_attach() {
    static DUPLICATED: AtomicBool = AtomicBool::new(false);
    struct Memory;
    impl BlockFile for Memory {
        fn alignment(&self) -> Alignment {
            align()
        }
        fn len(&self) -> Result<u64, DiskError> {
            Ok(0)
        }
        fn read_exact_at(&self, _: &mut [u8], _: u64) -> Result<(), DiskError> {
            Ok(())
        }
        fn write_all_at(&self, _: &[u8], _: u64) -> Result<(), DiskError> {
            Ok(())
        }
        fn sync_data(&self) -> Result<(), DiskError> {
            Ok(())
        }
        fn try_clone(&self) -> Result<Self, DiskError> {
            DUPLICATED.store(true, Ordering::SeqCst);
            Ok(Self)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let issuer = Issuer::start_device(dir.path(), DEPTH).unwrap();
    let refused = issuer.attach(&Memory);
    assert!(
        matches!(refused, Err(DiskError::Unsupported { .. })),
        "{refused:?}"
    );
    assert!(
        !DUPLICATED.load(Ordering::SeqCst),
        "no duplicate of a refused file is made"
    );
}

/// An attacher made from a kernel issuer has its own way to its bell: it moves to another
/// thread, attaches there, and its batch lands.
#[test]
fn an_attacher_of_a_kernel_issuer_attaches_from_another_thread() {
    let dir = tempfile::tempdir().unwrap();
    let issuer = Issuer::start_device(dir.path(), DEPTH).unwrap();
    let file = file(
        dir.path(),
        "attacher",
        CachingRequest::PreferDirect,
        (PAGES * PAGE) as u64,
    );
    let attacher = issuer.attacher().unwrap();
    let copy = attacher.try_clone().unwrap();
    drop(attacher);
    let duplicate = file.try_clone().unwrap();
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut attached = copy.attach_deep(&duplicate, 1).unwrap();
                attached.write(writes(7, 0), true).unwrap();
            })
            .join()
            .unwrap();
    });
    assert_eq!(read_page(&file, 0), vec![fill(0, 7); PAGE]);
}
