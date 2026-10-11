//! A shard whose compactions and packings run on maintenance workers (`ShardDb::set_workers`),
//! its process killed at a point the run picks, on a real file through the device's issuer: the
//! store reopened (without workers) recovers the last checkpoint the child acknowledged or the
//! one in flight; every key and a full scan read exactly as the workload left them at that
//! checkpoint's index; the store holds exactly the extents the engine names; and the engine goes
//! on from there. A kill lands wherever the workers' jobs then are: out, half written, back but
//! not yet taken.
//!
//! The test binary re-launches itself as the child (`kill_child_entry` with
//! `MANTLE_SHARD_KILL_CHILD` set), which prints `ack <i>` after each operation and `ckpt <n>`
//! after each checkpoint returns. Both sides generate the operations from their index, so the
//! parent replays what any checkpoint holds. The kill points are counts of acknowledged
//! operations, facts the child reports, not times.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::disallowed_methods
)]

#[path = "support/into_file.rs"]
mod file_outcome;
use file_outcome::finished_file;

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 8,
    max_extents: 1 << 16,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};
/// Memtables of a few KiB: maintenance every few hundred operations, so a kill finds jobs out.
const MEM: usize = 4 * 1024;
const KEYS: u64 = 1500;
/// A checkpoint every this many operations.
const EVERY: u64 = 400;
/// The child's operations at most: past every kill point.
const OPS: u64 = 20_000;

fn key(k: u64) -> Vec<u8> {
    format!("bucket-{}/obj-{k:06}", k % 5).into_bytes()
}

/// Operation `i` (its Raft index is `i + 1`): a put or a delete of a key.
fn op(i: u64) -> (u64, Option<Vec<u8>>) {
    let mut x = i.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x2545_f491_4f6c_dd1d;
    x ^= x >> 29;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 31;
    let k = x % KEYS;
    if x.is_multiple_of(7) {
        (k, None)
    } else {
        (
            k,
            Some(format!("v{i}-{}", "x".repeat((x % 40) as usize)).into_bytes()),
        )
    }
}

/// What every key holds once the first `n` operations are applied.
fn state(n: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut s = BTreeMap::new();
    for i in 0..n {
        match op(i) {
            (k, Some(v)) => {
                s.insert(key(k), v);
            }
            (k, None) => {
                s.remove(&key(k));
            }
        }
    }
    s
}

fn open(path: &Path, create: bool) -> DeviceFile {
    DeviceFile::open(
        path,
        create,
        CachingRequest::Buffered,
        Alignment::new(4096).unwrap(),
    )
    .unwrap()
}

fn workers(db: &mut ShardDb<DeviceFile>, path: &Path) {
    let path: PathBuf = path.to_path_buf();
    db.set_workers(move || {
        DeviceFile::open(
            &path,
            false,
            CachingRequest::Buffered,
            Alignment::new(4096).unwrap(),
        )
        .map_err(|e| mantle_engine::Error::Io {
            op: "open a worker's file",
            detail: e.to_string(),
        })
    })
    .unwrap();
}

fn apply(db: &mut ShardDb<DeviceFile>, i: u64) {
    match op(i) {
        (k, Some(v)) => db.put(&key(k), &v).unwrap(),
        (k, None) => db.delete(&key(k)).unwrap(),
    }
}

#[test]
fn kill_child_entry() {
    let Ok(dir) = std::env::var("MANTLE_SHARD_KILL_CHILD") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let path = dir.join("store");
    let mut db = ShardDb::create(open(&path, true), STORE, MEM, TRUNK).unwrap();
    let issuer = Issuer::start(&dir, 2).unwrap();
    db.attach(&issuer, 2).unwrap();
    workers(&mut db, &path);
    let mut out = std::io::stdout().lock();
    for i in 0..OPS {
        apply(&mut db, i);
        writeln!(out, "ack {i}").unwrap();
        if (i + 1) % EVERY == 0 {
            db.checkpoint(i + 1).unwrap();
            writeln!(out, "ckpt {}", i + 1).unwrap();
        }
        out.flush().unwrap();
    }
}

/// Every key and a scan of all of them read exactly `want`.
fn check(db: &mut ShardDb<DeviceFile>, want: &BTreeMap<Vec<u8>, Vec<u8>>, what: &str) {
    let mut value = Vec::new();
    for k in 0..KEYS {
        let kk = key(k);
        let found = db.get(&kk, &mut value).unwrap();
        match want.get(&kk) {
            Some(v) => assert!(found && &value == v, "{what}: key {k}"),
            None => assert!(!found, "{what}: key {k} reads a value it does not hold"),
        }
    }
    let mut rows = Rows::new();
    let mut next = Vec::new();
    assert!(
        !db.scan(b"", None, usize::MAX, &mut rows, &mut next)
            .unwrap()
    );
    let got: Vec<(Vec<u8>, Vec<u8>)> = rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
    let want: Vec<(Vec<u8>, Vec<u8>)> = want.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(got, want, "{what}: scan");
}

/// The child killed once `kill_after` operations are acknowledged; the store reopened and
/// checked against the checkpoints the child reported.
fn run(kill_after: u64) {
    let dir = tempfile::tempdir().unwrap();
    let exe = std::env::current_exe().unwrap();
    let mut child = Command::new(exe)
        .args([
            "--exact",
            "kill_child_entry",
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env("MANTLE_SHARD_KILL_CHILD", dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    let (mut acked, mut ckpt) = (0u64, 0u64);
    let note = |line: &str, acked: &mut u64, ckpt: &mut u64| {
        if let Some(i) = line.strip_prefix("ack ") {
            *acked = i.trim().parse::<u64>().unwrap() + 1;
        } else if let Some(n) = line.strip_prefix("ckpt ") {
            *ckpt = n.trim().parse().unwrap();
        }
    };
    for line in lines.by_ref() {
        note(&line.unwrap(), &mut acked, &mut ckpt);
        if acked >= kill_after {
            break;
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    for line in lines.map_while(Result::ok) {
        note(&line, &mut acked, &mut ckpt);
    }
    // The checkpoint in flight, if the child had reached one: written, or not.
    let in_flight = (acked / EVERY) * EVERY;
    let path = dir.path().join("store");
    let (mut db, applied) = ShardDb::open(open(&path, false), STORE, MEM, TRUNK)
        .unwrap_or_else(|e| panic!("killed after {acked} ops: reopening failed: {e}"));
    assert!(
        applied == ckpt || applied == in_flight,
        "killed after {acked} ops (last acknowledged checkpoint {ckpt}): recovered {applied}"
    );
    let what = format!("killed after {acked} ops, recovered checkpoint {applied}");
    db.check_references()
        .unwrap_or_else(|e| panic!("{what}: references: {e}"));
    check(&mut db, &state(applied), &what);
    // The engine goes on from there, with workers again, to the next checkpoint and through it.
    workers(&mut db, &path);
    let to = applied + EVERY;
    for i in applied..to {
        apply(&mut db, i);
    }
    db.checkpoint(to).unwrap();
    check(&mut db, &state(to), &format!("{what}, then on to {to}"));
    let (file, landed) = finished_file(db.into_file());
    landed.unwrap();
    drop(file);
    let (mut db, again) = ShardDb::open(open(&path, false), STORE, MEM, TRUNK).unwrap();
    assert_eq!(again, to);
    db.check_references().unwrap();
    check(&mut db, &state(to), &format!("{what}, reopened at {to}"));
}

/// Kill points at every checkpoint's boundary and between them, across the run: a kill in each
/// stretch of a checkpoint interval, so it lands before, during and after compactions and packings
/// out on the workers. `MANTLE_SHARD_KILL_POINTS` sets how many (default: one an interval of
/// the first ten, and the boundaries).
#[test]
fn a_shard_with_workers_killed_anywhere_recovers_an_acknowledged_checkpoint() {
    if std::env::var("MANTLE_SHARD_KILL_CHILD").is_ok() {
        return;
    }
    let intervals: u64 = std::env::var("MANTLE_SHARD_KILL_POINTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    for n in 1..=intervals {
        // Just past a checkpoint, mid-interval, and just before the next.
        for at in [
            n * EVERY + 1,
            n * EVERY + EVERY / 2 + n * 7,
            (n + 1) * EVERY - 1,
        ] {
            run(at);
        }
    }
}
