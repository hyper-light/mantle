//! A worker that packs from a feed sends nothing while it waits for the next buffer, and its
//! buffers come back to the shard's pool. A wait for its message once they are all back could
//! end only by the shard feeding it: such a wait is refused, typed, rather than taken.
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
use mantle_engine::trunk::pool::{self, FEED_BUFFERS, Job, Owner, Pool, Spawn, Stream, Work};
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

/// A feed buffer holding the one entry `number`.
fn buffer(number: u8) -> Vec<u8> {
    let mut bytes = Vec::new();
    let key = [number];
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

/// Do: start a packing job whose feed the test holds, send it a buffer for each of its
/// FEED_BUFFERS, and take messages until every buffer is back in the pool: the worker now waits
/// for its feed and has nothing to send. Then ask to wait for a message while its feed is open.
/// Expect: the wait is refused at once (no message could end it); with the feed closed the
/// worker finishes and its branch holds every entry.
#[test]
fn a_wait_for_a_worker_whose_buffers_are_back_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = Store::create(open(&path, true), CONFIG).unwrap();
    let issuer = Issuer::start_for(&path, 1, 1).unwrap();
    let worker_path = path.clone();
    let spawn: Spawn = Box::new(move |seat| {
        let file = open(&worker_path, false);
        std::thread::Builder::new()
            .name(format!("feed-wait-worker-{}", seat.id))
            .spawn(move || pool::serve(file, CONFIG, seat))
            .map_err(|error| Error::Io {
                op: "start a feed-wait worker",
                detail: error.to_string(),
            })
    });
    let mut pool = Pool::new(spawn, 1).unwrap();
    pool.set_attach(issuer.attacher(), 1);
    let (full, received) = mpsc::sync_channel(FEED_BUFFERS);
    let entries = u8::try_from(FEED_BUFFERS).unwrap();
    let job = Box::new(Job {
        work: Work::Pack(Stream {
            entries: u64::from(entries),
            full: received,
        }),
        grant: store
            .grant(usize::try_from(CONFIG.extent_pages).unwrap())
            .unwrap(),
        file_end: store.end(),
        generation: store.generation(),
    });
    let worker = pool.send(job, Owner::Pack, true).unwrap().unwrap();
    for number in 1..=entries {
        full.send(buffer(number)).unwrap();
    }
    while pool.spent(worker) < FEED_BUFFERS {
        assert!(pool.wait_any(&mut store, std::iter::empty()).unwrap());
    }
    assert!(matches!(
        pool.wait_any(&mut store, [worker]),
        Err(Error::InvalidArgument { .. })
    ));
    drop(full);
    let back = pool.take(&mut store, Owner::Pack, true).unwrap().unwrap();
    let output = back.result.unwrap();
    store.extend_end(output.end);
    let branch = &output.parts[0].1;
    let mut value = Vec::new();
    for number in 1..=entries {
        assert_eq!(
            branch.get(&mut store, &[number], &mut value).unwrap(),
            Some(Op::Put)
        );
        assert_eq!(value, vec![number; 100]);
    }
    drop(pool);
    drop(issuer);
}
