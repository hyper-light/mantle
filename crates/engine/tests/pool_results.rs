//! A worker freed while its result waits for its owner takes that owner's next job. Each
//! result names the job it answers, as `send` named it: an owner that matched results by worker
//! credited one pivot's merge to another on a busy host (the next level then dropped both
//! pivots' entries as out of their ranges), and results held for one owner come back in no
//! promised order. A packing's feed likewise takes back only its own job's buffers.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use mantle_engine::Error;
use mantle_engine::branch::{Op, filter};
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::pool::{self, Job, Owner, Pool, Spawn, Stream, Work};
use std::path::Path;
use std::sync::mpsc;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 256,
};

fn open(path: &Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .unwrap()
}

/// A store, its device's issuer, and a pool of the host's workers on the store's file.
fn pool(path: &Path) -> (Store<DeviceFile>, Issuer, Pool) {
    let store = Store::create(open(path, true), CONFIG).unwrap();
    let issuer = Issuer::start_for(path, 1, 1).unwrap();
    let worker_path = path.to_path_buf();
    let spawn: Spawn = Box::new(move |seat| {
        let file = open(&worker_path, false);
        std::thread::Builder::new()
            .name(format!("results-worker-{}", seat.id))
            .spawn(move || pool::serve(file, CONFIG, seat))
            .map_err(|error| Error::Io {
                op: "start a results worker",
                detail: error.to_string(),
            })
    });
    let mut pool = Pool::new(spawn, Pool::cores()).unwrap();
    pool.set_attach(issuer.attacher(), 1);
    (store, issuer, pool)
}

/// A feed buffer holding the one entry `number`.
fn entry(number: u8) -> Vec<u8> {
    let key = [number];
    let mut bytes = Vec::new();
    pool::encode(
        &mut bytes,
        &key,
        Op::Put,
        &[number; 100],
        mantle_engine::maplet::hash32(filter::hash(&key)),
    )
    .unwrap();
    bytes
}

/// A packing job of `entries` entries, and its feed.
fn packing(store: &mut Store<DeviceFile>, entries: u64) -> (Box<Job>, mpsc::SyncSender<Vec<u8>>) {
    let (full, received) = mpsc::sync_channel(1);
    let job = Box::new(Job {
        work: Work::Pack(Stream {
            entries,
            full: received,
        }),
        grant: store
            .grant(usize::try_from(CONFIG.extent_pages).unwrap())
            .unwrap(),
        file_end: store.end(),
        generation: store.generation(),
    });
    (job, full)
}

/// Do: send three jobs of one owner, each once the one before has finished and its result is
/// held for the owner, so the first worker, free again each time, runs all three; then take
/// the three results.
/// Expect: the three sends name three jobs; each result names one of them, each once, and
/// holds that job's entry.
#[test]
fn each_result_names_its_job_when_one_worker_runs_three_before_any_is_taken() {
    let dir = tempfile::tempdir().unwrap();
    let (mut store, issuer, mut pool) = pool(&dir.path().join("store"));
    let mut sent = Vec::new();
    for number in 1..=3u8 {
        let (job, feed) = packing(&mut store, 1);
        feed.send(entry(number)).unwrap();
        drop(feed);
        sent.push((number, pool.send(job, Owner::Pack, true).unwrap().unwrap()));
        // Its result comes back and is held for its owner: the worker is free again.
        while pool.wait_any(&mut store, std::iter::empty()).unwrap() {}
    }
    assert!(
        sent.iter().all(|(_, t)| t.worker == sent[0].1.worker),
        "one worker runs every job, each held result freeing it for the next"
    );
    for (at, (_, a)) in sent.iter().enumerate() {
        for (_, b) in &sent[at + 1..] {
            assert_ne!(a, b, "two jobs out for one owner share a name");
        }
    }
    let mut value = Vec::new();
    let mut named = Vec::new();
    for _ in &sent {
        let back = pool.take(&mut store, Owner::Pack, true).unwrap().unwrap();
        let (number, _) = sent
            .iter()
            .find(|(_, t)| *t == back.ticket)
            .expect("a result names a job sent");
        assert!(!named.contains(number), "two results name one job");
        named.push(*number);
        let output = back.result.unwrap();
        store.extend_end(output.end);
        store.grant_back(&output.unused).unwrap();
        let branch = &output.parts[0].1;
        assert_eq!(
            branch.get(&mut store, &[*number], &mut value).unwrap(),
            Some(Op::Put)
        );
        assert_eq!(value, vec![*number; 100]);
    }
    drop(pool);
    drop(issuer);
}

/// Do: a packing's worker fails on a buffer not made of whole entries while the test still holds
/// the packing's feed open; its result is held for its owner, so the same worker takes the next
/// packing, which gives a buffer back with its own feed still open. The failed packing's feed
/// asks for a buffer, then the next one's.
/// Expect: the failed packing's feed takes none; the next one's takes its own; both results
/// come back, each naming its job, the failure a typed corruption.
#[test]
fn a_failed_packings_open_feed_takes_no_buffer_of_the_job_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let (mut store, issuer, mut pool) = pool(&dir.path().join("store"));
    let (job, failed_feed) = packing(&mut store, 2);
    let failed = pool.send(job, Owner::Pack, true).unwrap().unwrap();
    failed_feed.send(vec![0xff; 3]).unwrap();
    while pool.wait_any(&mut store, std::iter::empty()).unwrap() {}
    let (job, next_feed) = packing(&mut store, 1);
    let next = pool.send(job, Owner::Pack, true).unwrap().unwrap();
    assert_eq!(
        next.worker, failed.worker,
        "the failed packing's worker, free again, takes the next one"
    );
    next_feed.send(entry(2)).unwrap();
    while pool.spent(next.worker) == 0 {
        assert!(pool.wait_any(&mut store, std::iter::empty()).unwrap());
    }
    assert!(
        pool.buffer(&mut store, failed, false).unwrap().is_none(),
        "a failed packing's feed took a buffer of the job after it"
    );
    assert!(pool.buffer(&mut store, next, false).unwrap().is_some());
    drop(failed_feed);
    drop(next_feed);
    let mut value = Vec::new();
    for _ in 0..2 {
        let back = pool.take(&mut store, Owner::Pack, true).unwrap().unwrap();
        if back.ticket == failed {
            assert!(
                matches!(back.result, Err(Error::Corruption { .. })),
                "{:?}",
                back.result
            );
            continue;
        }
        assert_eq!(back.ticket, next);
        let output = back.result.unwrap();
        store.extend_end(output.end);
        store.grant_back(&output.unused).unwrap();
        let branch = &output.parts[0].1;
        assert_eq!(
            branch.get(&mut store, &[2], &mut value).unwrap(),
            Some(Op::Put)
        );
        assert_eq!(value, vec![2; 100]);
    }
    drop(pool);
    drop(issuer);
}
