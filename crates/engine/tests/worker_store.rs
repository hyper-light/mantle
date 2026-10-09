//! A maintenance worker's store (`Store::worker`): a compaction run over its own handle on the
//! shard's file, from extents the shard granted, writes exactly the branches the shard's own
//! store would, within the grant, and gives back what it did not use.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::merge::compact_split;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::scan::ScanMerge;
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::Source;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 14,
};

fn open(path: &std::path::Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap()
}

/// A branch of `n` keys from `seed`, a deletion every seventh.
fn build(s: &mut Store<DeviceFile>, seed: u64, n: u64) -> Branch {
    let mut x = seed;
    let mut keys: Vec<u64> = (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % 50_000
        })
        .collect();
    keys.sort_unstable();
    keys.dedup();
    let mut b = Builder::new(s, Keys::Exactly(keys.len() as u64)).unwrap();
    for k in keys {
        let op = if k % 7 == 0 { Op::Delete } else { Op::Put };
        let v = if op == Op::Put {
            format!("v{seed}-{k}").into_bytes()
        } else {
            Vec::new()
        };
        b.add(s, &k.to_be_bytes(), op, &v).unwrap();
    }
    b.finish(s).unwrap()
}

/// Every entry of `parts`, read through `s` in key order.
fn entries(s: &mut Store<DeviceFile>, parts: &[(Vec<u8>, Branch)]) -> Vec<(Vec<u8>, Op, Vec<u8>)> {
    let sources: Vec<Source<'_>> = parts.iter().map(|(_, b)| Source::Branch(b)).collect();
    let mut merge = ScanMerge::new();
    merge.open(s, &sources, b"", None, false, false).unwrap();
    let mut out = Vec::new();
    while let Some((k, op, v)) = merge.entry() {
        out.push((k.to_vec(), op, v.to_vec()));
        merge.next(s).unwrap();
    }
    merge.close(s);
    out
}

#[test]
fn a_workers_compaction_writes_what_the_shards_would_within_its_grant() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut shard = Store::create(open(&path, true), CONFIG).unwrap();
    let inputs: Vec<Branch> = (1..=3u64).map(|i| build(&mut shard, i, 6_000)).collect();
    shard.drain().unwrap();
    // Leaves of at most a third of the inputs' entries: several parts, as a leaf's split.
    let per = inputs.iter().map(|b| b.count).sum::<u64>() / 3;

    // The shard's own compaction.
    let here = compact_split(&mut shard, &inputs, b"", None, true, per).unwrap();
    shard.drain().unwrap();
    let want = entries(&mut shard, &here);
    assert!(here.len() > 1 && !want.is_empty());

    // The same compaction on a worker: a grant of the inputs' extents, which bounds its output.
    let count: usize = inputs.iter().map(|b| b.extents.len()).sum();
    let grant = shard.grant(count).unwrap();
    let mut worker = Store::worker(open(&path, false), CONFIG).unwrap();
    worker.begin_job(&grant, shard.end(), shard.generation());
    let there = compact_split(&mut worker, &inputs, b"", None, true, per).unwrap();
    worker.drain().unwrap();
    let unused = worker.unused_grant();
    let end = worker.end();
    drop(worker);
    shard.grant_back(&unused).unwrap();
    shard.extend_end(end);

    // Its branches lie within the grant, and every extent of it is either written or given back.
    let written: Vec<u64> = there
        .iter()
        .flat_map(|(_, b)| b.extents.iter().copied())
        .collect();
    assert!(
        written.iter().all(|e| grant.contains(e)),
        "{written:?} {grant:?}"
    );
    let mut accounted = written.clone();
    accounted.extend(&unused);
    accounted.sort_unstable();
    let mut granted = grant.clone();
    granted.sort_unstable();
    assert_eq!(accounted, granted);
    // And the shard reads exactly the entries its own compaction wrote, part by part.
    assert_eq!(
        there
            .iter()
            .map(|(k, b)| (k.clone(), b.count))
            .collect::<Vec<_>>(),
        here.iter()
            .map(|(k, b)| (k.clone(), b.count))
            .collect::<Vec<_>>()
    );
    assert_eq!(entries(&mut shard, &there), want);
}

/// A job granted a single extent asks for more as it outgrows it, as many again as it holds
/// each time, and writes the same branches the shard would.
#[test]
fn a_job_past_its_grant_asks_for_more_and_writes_the_same() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut shard = Store::create(open(&path, true), CONFIG).unwrap();
    let inputs: Vec<Branch> = (1..=3u64).map(|i| build(&mut shard, i, 6_000)).collect();
    shard.drain().unwrap();
    let here = compact_split(&mut shard, &inputs, b"", None, false, u64::MAX).unwrap();
    shard.drain().unwrap();
    let want = entries(&mut shard, &here);

    let count: usize = inputs.iter().map(|b| b.extents.len()).sum();
    // Enough extents for any top-up the job asks, handed out as asked.
    let mut spare = shard.grant(2 * count + 2).unwrap();
    let grant = vec![spare.pop().unwrap()];
    let (tx, rx) = std::sync::mpsc::channel();
    let mut worker = Store::worker(open(&path, false), CONFIG).unwrap();
    worker.begin_job(&grant, shard.end(), shard.generation());
    let mut handed = grant.clone();
    let mut pool = spare.clone();
    worker.set_refill(mantle_engine::store::Refill(Box::new(move |n| {
        let more: Vec<u64> = pool.drain(..n.min(pool.len())).collect();
        tx.send((n, more.clone())).unwrap();
        Ok(more)
    })));
    let there = compact_split(&mut worker, &inputs, b"", None, false, u64::MAX).unwrap();
    worker.drain().unwrap();
    let asks: Vec<(usize, Vec<u64>)> = rx.try_iter().collect();
    // Each ask doubles what the job holds: 1, 2, 4, ...
    assert!(asks.len() >= 2, "{asks:?}");
    for (i, (n, _)) in asks.iter().enumerate() {
        assert_eq!(*n, 1 << i, "{asks:?}");
    }
    for (_, more) in &asks {
        handed.extend(more);
    }
    let unused = worker.unused_grant();
    let end = worker.end();
    drop(worker);
    let written: Vec<u64> = there
        .iter()
        .flat_map(|(_, b)| b.extents.iter().copied())
        .collect();
    let mut accounted = written.clone();
    accounted.extend(&unused);
    accounted.sort_unstable();
    handed.sort_unstable();
    assert_eq!(accounted, handed);
    // The extents never handed out go back with the unused.
    let left: Vec<u64> = spare.into_iter().filter(|e| !handed.contains(e)).collect();
    shard.grant_back(&left).unwrap();
    shard.grant_back(&unused).unwrap();
    shard.extend_end(end);
    assert_eq!(entries(&mut shard, &there), want);
}
