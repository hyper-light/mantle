//! The process's descriptors while a shard fills on as many maintenance workers as the host's cores
//! allow, through a device issuer with a worker for each core: its store, kept as segments, holds
//! one descriptor for each segment, its base among them, whatever the workers transferring
//! through it and the issuer's duplicates of their handles (hyper-block `segmented`). Counted with
//! `hyper_measure::descriptors` after each checkpoint: on Linux `/proc/self/fd`, on macOS
//! `proc_pidinfo(PROC_PIDLISTFDS)`, on Windows the process's handle count. This binary holds one
//! test, so no other opens or closes a descriptor during the counts.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::CachingRequest;
use hyper_block::issuer::Issuer;
use hyper_block::segmented::SegmentedFile;
use hyper_measure::descriptors;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
/// A memtable of one page: the fill packs one every few dozen puts, so the file grows by many
/// segments and the workers' compactions read and write throughout.
const MEM: usize = 4096;
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
/// Segments a file may have: past what the workload makes.
const BOUND: usize = 1 << 12;
/// The fill's puts and the checkpoints among them: `store_segments.rs`'s workload, which grows its
/// file by several segments on workers.
const OPS: u64 = 12_000;
const CHECKPOINT: u64 = 1_500;

fn key(k: u64) -> Vec<u8> {
    format!("bucket-{}/obj-{k:06}", k % 5).into_bytes()
}

/// Do: open a segmented store and make a shard on a handle of it, attach it to an issuer with a
/// worker for each core and give it maintenance workers on handles of it; fill it, and after
/// each checkpoint count the process's descriptors and the file's segments. Expect: past what the
/// process held before the file, the descriptors are the segments the file held as the count
/// began, or the segments it held as the count ended and the one a growth running then holds
/// (the shard grows its file one segment at a time); never a segment's again for a worker or an
/// issuer duplicate. On Windows the count is the process's handles, which hold each thread too and
/// each thread's event for its transfers (hyper-block `overlapped`): two a thread, past the
/// segments.
#[test]
fn a_shards_store_holds_one_descriptor_a_segment_whatever_its_workers() {
    let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let issuer = Issuer::start(dir.path(), cores).unwrap();
    let before = descriptors::open().unwrap();
    let align = Alignment::new(STORE.page_size).unwrap();
    let owner = SegmentedFile::create(&path, CachingRequest::Buffered, align, BOUND).unwrap();
    let mut db = ShardDb::create(owner.try_clone().unwrap(), STORE, MEM, TRUNK).unwrap();
    db.attach(&issuer, issuer.depth()).unwrap();
    let workers = owner.try_clone().unwrap();
    db.set_workers(move || {
        workers.try_clone().map_err(|e| mantle_engine::Error::Io {
            op: "a worker's handle of the store",
            detail: e.to_string(),
        })
    })
    .unwrap();
    let mut x = 0x2545_f491_4f6c_dd1du64;
    let mut most = 0usize;
    for i in 0..OPS {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let value = format!("v{i}-{}", "x".repeat((x % 40) as usize)).into_bytes();
        db.put(&key(x % 1_500), &value).unwrap();
        if (i + 1) % CHECKPOINT != 0 {
            continue;
        }
        db.checkpoint(i).unwrap();
        let low = owner.layout().len();
        let held = descriptors::open().unwrap() - before;
        let high = owner.layout().len();
        let threads = hyper_block::threads::count().unwrap();
        let (seats, _) = db.workers().unwrap_or((0, 0));
        eprintln!(
            "ops {} segments {low}..{high} descriptors {held} workers {seats} threads {threads}",
            i + 1
        );
        let others = if cfg!(windows) { 2 * threads } else { 0 };
        assert!(
            low <= held && held <= high + 1 + others,
            "{held} descriptors for {low}..{high} segments, {seats} workers, {threads} threads"
        );
        most = most.max(high);
    }
    // The count spoke of a file that grew by segments, workers transferring through it.
    assert!(most >= 3, "the fill grew its file by {most} segments");
    db.checkpoint(OPS).unwrap();
    drop(db);
    drop(issuer);
    drop(owner);
}
