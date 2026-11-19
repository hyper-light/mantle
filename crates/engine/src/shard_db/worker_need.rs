//! A maintenance job whose grant falls short of what it writes asks its pool for more
//! ([`crate::trunk::pool::Message::Need`]). A store that does not grow by segments, as a node's
//! range's does not, answers from the extents it has left; near its limit it may have fewer than
//! the ask, which doubles the job's grant, though the job needs fewer. Such an ask ends with the
//! job, a compaction's or a packing's, never with the shard failed: it waits while another job
//! out may give extents back, and the last job out takes what the store has left, with its own
//! grant the room the shard running it inline would have. Only a store with no extent left refuses
//! it, as the job run inline would find the store full.
//!
//! Two test hooks make the case: the trunk's ([`Trunk::compact_most`], [`Trunk::pack_most`]) cut
//! every grant of one kind to one extent, an estimate short of what the job writes, and the pool's
//! ([`crate::trunk::pool::Pool::ask_room`]) holds aside all but [`ROOM`] of the store's free
//! extents while an ask is answered, a store that near its limit. Each case fills the store,
//! checkpoints, reopens it without workers and reads every row back.
#![cfg(test)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use super::*;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const PAGE: usize = 4096;
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
const MEM: usize = 16 * 1024;
const KEYS: u64 = 1500;
const OPS: u64 = 6000;
/// The extents an ask is answered from: a store with one left, which a job's first ask, for as
/// many as its grant of one, takes, and its second, for two, finds short.
const ROOM: u64 = 1;

/// Extents of two pages: a compaction or a packing of a memtable writes several.
fn config() -> Config {
    Config {
        page_size: PAGE,
        extent_pages: 2,
        max_extents: 1 << 12,
    }
}

fn key(k: u64) -> Vec<u8> {
    format!("bucket-{}/obj-{k:06}", k % 5).into_bytes()
}

fn open(path: &Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(PAGE).unwrap(),
    )
    .unwrap()
}

/// Which job's estimate falls short.
#[derive(Clone, Copy, Debug)]
enum Short {
    Compaction,
    Packing,
}

/// The rows of a checkpoint, and the applied index it holds.
type Durable = (u64, BTreeMap<u64, Option<Vec<u8>>>);

/// Puts and deletes, checkpointed every 1500 operations, each checkpoint's rows in `durable`: the
/// oracle, or the first error an operation met.
fn fill(
    db: &mut ShardDb<DeviceFile>,
    durable: &mut Durable,
) -> Result<BTreeMap<u64, Option<Vec<u8>>>, Error> {
    let mut oracle = BTreeMap::new();
    let mut x = 0x2545_f491_4f6c_dd1d_u64;
    for i in 0..OPS {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let k = x % KEYS;
        if x.is_multiple_of(7) {
            db.delete(&key(k))?;
            oracle.insert(k, None);
        } else {
            let v = format!("v{i}-{}", "x".repeat((x % 40) as usize)).into_bytes();
            db.put(&key(k), &v)?;
            oracle.insert(k, Some(v));
        }
        if (i + 1) % 1500 == 0 {
            db.checkpoint(i + 1)?;
            *durable = (i + 1, oracle.clone());
        }
    }
    Ok(oracle)
}

fn check(db: &mut ShardDb<DeviceFile>, oracle: &BTreeMap<u64, Option<Vec<u8>>>) {
    for k in 0..KEYS {
        let mut value = Vec::new();
        let found = db.get(&key(k), &mut value).unwrap();
        match oracle.get(&k) {
            Some(Some(v)) => {
                assert!(found, "key {k} lost");
                assert_eq!(&value, v, "key {k}");
            }
            _ => assert!(!found, "key {k} reads a value it does not hold"),
        }
    }
}

/// A shard on `workers` workers over a new store in `dir`, `short`'s grants of one extent, every
/// ask answered from `room` extents.
fn shard(dir: &Path, short: Short, workers: usize, room: u64) -> ShardDb<DeviceFile> {
    let path: PathBuf = dir.join("store");
    let mut db = ShardDb::create(open(&path, true), config(), MEM, TRUNK).unwrap();
    db.set_workers_within(
        move || {
            DeviceFile::open(
                &path,
                false,
                CachingRequest::Buffered,
                Alignment::new(PAGE).unwrap(),
            )
            .map_err(|e| Error::Io {
                op: "open a worker's file",
                detail: e.to_string(),
            })
        },
        workers,
    )
    .unwrap();
    match short {
        Short::Compaction => db.trunk.compact_most = Some(1),
        Short::Packing => db.trunk.pack_most = Some(1),
    }
    db.trunk.pool_mut().unwrap().ask_room = Some(room);
    db
}

/// The store in `dir` reopened without workers holds `durable`'s applied index and every row.
fn reopen(dir: &Path, durable: &Durable) {
    let path = dir.join("store");
    let (mut db, applied) = ShardDb::open(open(&path, false), config(), MEM, TRUNK).unwrap();
    assert_eq!(applied, durable.0);
    check(&mut db, &durable.1);
}

/// The fill on `workers` workers, `short`'s grants of one extent and every ask answered from
/// [`ROOM`] extents: it ends without a fence, its jobs having asked for more than the room held;
/// then the store, checkpointed and reopened without workers, reads every row back.
fn run(short: Short, workers: usize) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = shard(dir.path(), short, workers, ROOM);
    let mut durable = (0, BTreeMap::new());
    let filled = fill(&mut db, &mut durable);
    let (asks, short_asks) = db.trunk.pool_mut().unwrap().asks;
    let oracle = match filled {
        Ok(oracle) => oracle,
        Err(error) => panic!(
            "{short:?} on {workers} workers failed the fill after {asks} asks, {short_asks} \
             past the room: {error:?}"
        ),
    };
    assert!(
        short_asks > 0,
        "{short:?}: no ask was past the room ({asks} asks)"
    );
    db.checkpoint(OPS).unwrap();
    check(&mut db, &oracle);
    let file = match db.into_file() {
        IntoFile::Finished { file, result } => {
            result.unwrap();
            file
        }
        IntoFile::Refused { error, .. } => panic!("{error:?}"),
    };
    drop(file);
    reopen(dir.path(), &(OPS, oracle));
}

/// Fails before the fix: `Pool::accept` refused the compaction's second ask, and the failed
/// compaction fenced the shard.
#[test]
fn a_compaction_short_of_its_grant_ends_without_a_fence() {
    run(Short::Compaction, 1);
}

/// Fails before the fix: `Pool::accept` refused the packing's second ask, and the failed packing
/// fenced the shard.
#[test]
fn a_packing_short_of_its_grant_ends_without_a_fence() {
    run(Short::Packing, 1);
}

/// Two workers: an ask the room cannot answer waits while the other's job may give extents back.
/// Fails before the fix, as the cases on one worker do.
#[test]
fn short_jobs_on_two_workers_end_without_a_fence() {
    run(Short::Compaction, 2);
    run(Short::Packing, 2);
}

/// A store with no extent left refuses the ask, as the job run inline would find the store
/// full: the fill ends on the store's typed refusal, not on a wait for room that cannot come, and
/// the store reopened holds its last checkpoint's every row.
#[test]
fn an_ask_of_a_store_with_no_extent_left_is_refused() {
    for short in [Short::Compaction, Short::Packing] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = shard(dir.path(), short, 1, 0);
        let mut durable = (0, BTreeMap::new());
        let error = fill(&mut db, &mut durable).err();
        assert!(
            matches!(error, Some(Error::LimitExceeded { .. })),
            "{short:?}: {error:?}"
        );
        assert!(db.trunk.pool_mut().unwrap().asks.1 > 0, "{short:?}");
        drop(db);
        reopen(dir.path(), &durable);
    }
}
