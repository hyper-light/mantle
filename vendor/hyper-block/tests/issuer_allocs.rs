//! What a batch's round trip through the device's issuer allocates once warm: nothing (`CLAUDE.md`
//! §1a). A submitter keeps its transfers' vectors in a pool of its own and hands each to the
//! issuer with its batch; the answer gives it back, buffers and offsets in the order given, with
//! its allocation intact, and the issuer's own bookkeeping for the batch lives in that vector.
//!
//! Counted across the process (`hyper_measure::alloc::begin_process`): the submitter's thread, the
//! issuer's thread and its workers. This binary holds one test, so nothing else runs during the
//! count.
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
use hyper_block::issuer::{Attached, Issuer};
use hyper_measure::alloc::{self, Counting};

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The page each transfer moves: the block of the file systems the tests run on.
const PAGE: usize = 4096;
/// The issuer's workers, and the transfers of each batch: a batch keeps every worker busy, the
/// fan-out of a mantle engine seek (`benches/reads.rs`).
const DEPTH: usize = 4;
/// The batches the submitter keeps out at once: two, the depth mantle's engine attaches its extent
/// writes for (hyper-block ORIGIN.md, change 7), and so the vectors its pool holds.
const BATCHES: usize = 2;

type Transfers = Vec<(AlignedBuf, u64)>;

fn align() -> Alignment {
    Alignment::new(PAGE).unwrap()
}

/// `DEPTH` page buffers filled with `fill`, at the pages from `first` on.
fn transfers(first: usize, fill: u8) -> Transfers {
    (first..first + DEPTH)
        .map(|i| {
            let mut buf = AlignedBuf::zeroed(PAGE, align()).unwrap();
            buf.extend_from_slice(&[fill; PAGE]).unwrap();
            (buf, (i * PAGE) as u64)
        })
        .collect()
}

/// One pass of every way a submitter round-trips a batch: a blocking write and flush, a write
/// submitted and answered, reads submitted and answered, a write and reads out together, and a
/// flush alone. Each vector goes out and comes back to `pool`.
fn pass(attached: &mut Attached, pool: &mut [Option<Transfers>; BATCHES]) {
    let a = pool[0].take().unwrap();
    pool[0] = Some(attached.write(a, true).unwrap());

    let a = pool[0].take().unwrap();
    let number = attached.submit(a, false).unwrap();
    let (answered, back) = attached.answer().unwrap();
    assert_eq!(answered, number);
    pool[0] = Some(back.unwrap());

    let b = pool[1].take().unwrap();
    let number = attached.submit_reads(b).unwrap();
    let (answered, back) = attached.answer().unwrap();
    assert_eq!(answered, number);
    pool[1] = Some(back.unwrap());

    let a = pool[0].take().unwrap();
    let b = pool[1].take().unwrap();
    let write = attached.submit(a, true).unwrap();
    let read = attached.submit_reads(b).unwrap();
    for _ in 0..BATCHES {
        let (answered, back) = attached.answer().unwrap();
        let at = if answered == write {
            0
        } else {
            assert_eq!(answered, read);
            1
        };
        pool[at] = Some(back.unwrap());
    }

    attached.flush().unwrap();
    assert!(attached.try_answer().unwrap().is_none());
}

/// Do: attach a real file for two batches out, warm the issuer once (one batch of four times the
/// depth, which fills its queue past anything a pass queues, then one pass), and count a pass after
/// it. Expect: zero allocations and zero reallocations across the process, and every vector back
/// with the bytes and offsets it went out with.
#[test]
fn a_warm_round_trip_allocates_nothing() {
    assert!(
        alloc::installed(),
        "the counting allocator is not installed"
    );
    let dir = tempfile::tempdir().unwrap();
    let file = DeviceFile::open(
        &dir.path().join("issuer"),
        true,
        CachingRequest::Buffered,
        align(),
    )
    .unwrap();
    let issuer = Issuer::start(dir.path(), DEPTH).unwrap();
    assert_eq!(issuer.depth(), DEPTH);
    let mut attached = issuer.attach_deep(&file, BATCHES).unwrap();
    let mut pool = [Some(transfers(0, 0x5a)), Some(transfers(DEPTH, 0xa5))];

    // The queue's high water: a batch of four times the depth is queued whole before its first
    // transfer is dispatched, more than the two batches of a pass ever hold. It leaves each pooled
    // vector's pages holding that vector's bytes, so a pass's reads read back what it wrote.
    let wide: Transfers = (0..4 * DEPTH)
        .map(|i| {
            let fill = match i / DEPTH {
                0 => 0x5a,
                1 => 0xa5,
                _ => 0x11,
            };
            let mut buf = AlignedBuf::zeroed(PAGE, align()).unwrap();
            buf.extend_from_slice(&[fill; PAGE]).unwrap();
            (buf, (i * PAGE) as u64)
        })
        .collect();
    drop(attached.write(wide, true).unwrap());
    // One pass pays std's channels' one-time state: each thread's first wait builds its context,
    // and each channel's first waiter grows its list. Every thread waits on this pass, at least
    // through its flushes.
    pass(&mut attached, &mut pool);

    alloc::begin_process();
    pass(&mut attached, &mut pool);
    let counts = alloc::end_process();
    assert_eq!(
        (counts.allocations, counts.reallocations),
        (0, 0),
        "a warm pass allocated: {counts:?}"
    );

    for (at, fill, first) in [(0, 0x5a, 0), (1, 0xa5, DEPTH)] {
        let back = pool[at].as_ref().unwrap();
        assert_eq!(back.len(), DEPTH);
        for (i, (buf, offset)) in back.iter().enumerate() {
            assert_eq!(*offset, ((first + i) * PAGE) as u64);
            assert!(buf.as_slice().iter().all(|&b| b == fill));
        }
    }
}
