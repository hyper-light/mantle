//! A worker freed while its result waits for its owner takes that owner's next job. Each
//! result names the job it answers, as `send` named it: an owner that matched results by worker
//! credited one pivot's merge to another on a busy host (the next level then dropped both
//! pivots' entries as out of their ranges), and results held for one owner come back in no
//! promised order.
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
use std::sync::mpsc;

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 256,
};

fn open(path: &std::path::Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(CONFIG.page_size).unwrap(),
    )
    .unwrap()
}

/// A packing job of the one entry `number`, its feed already closed.
fn job(store: &mut Store<DeviceFile>, number: u8) -> Box<Job> {
    let (full, received) = mpsc::sync_channel(1);
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
    full.send(bytes).unwrap();
    drop(full);
    Box::new(Job {
        work: Work::Pack(Stream {
            entries: 1,
            full: received,
        }),
        grant: store
            .grant(usize::try_from(CONFIG.extent_pages).unwrap())
            .unwrap(),
        file_end: store.end(),
        generation: store.generation(),
    })
}

/// Do: send three jobs of one owner, each once the one before has finished and its result is
/// held for the owner, so the first worker, free again each time, runs all three; then take
/// the three results.
/// Expect: the three sends name three jobs; each result names one of them, each once, and
/// holds that job's entry.
#[test]
fn each_result_names_its_job_when_one_worker_runs_three_before_any_is_taken() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(open(&path, true), CONFIG).unwrap();
    let issuer = Issuer::start_for(&path, 1, 1).unwrap();
    let worker_path = path.clone();
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
    let mut sent = Vec::new();
    for number in 1..=3u8 {
        let job = job(&mut store, number);
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
