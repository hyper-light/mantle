//! A writer process killed at a point drawn afresh each run, on the host's own file system
//! through the real direct-I/O file layer and the device's issuer, as deep as measured there
//! (`live::Device`): every write the child reported as acknowledged must be there after
//! reopening, and the one write in flight may or may not be. Its frame may have reached the
//! file without its record, since a batch's writes are issued together (chunk-store.md §4):
//! recovery then keeps the record and reports it damaged (§6, step 3), as the crash test
//! expects of a write a power cut tore.
//!
//! The test binary re-launches itself as the child (`kill_child_entry` with
//! `MANTLE_KILL_CHILD` set), which formats a volume in a directory the parent names and
//! prints `ack <step> <checkpoints>` after each acknowledged operation. Both sides generate
//! the same operations from the seed, so the parent can replay what was acknowledged.
//!
//! Where the kills land: the first child is killed once its volume has written its second
//! checkpoint, a checkpoint being written once the log could not otherwise hold more (§5), so
//! the log has been filled and taken up again past the first. The acknowledgements it took
//! bound the rest: each later child is killed after a count drawn from the OS's entropy
//! within them, with a seed drawn the same way. Kills go on until they have landed before the
//! first checkpoint and after it, and during a put, an append and a delete; `MANTLE_KILL_RUNS`
//! asks for more.
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

mod common;
mod live;

use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};

use common::{SIZE, config, data, key};
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use mantle_chunk::{ChunkError, ChunkKey, Volume};

const KEYS: u64 = 8;

#[derive(Clone, Debug, PartialEq, Eq)]
struct State {
    bytes: Vec<u8>,
    sealed: bool,
}

#[derive(Clone, Debug)]
enum Op {
    Put(Vec<u8>),
    Append { data: Vec<u8>, seal: bool },
    Delete,
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// The step's operation, a function of the seed, the step and the state so far.
fn op(seed: u64, step: u64, state: &HashMap<ChunkKey, Option<State>>) -> (ChunkKey, Op) {
    let mut rng = Rng(seed ^ step.wrapping_mul(0xA24B_AED4_963E_E407));
    let k = key(rng.below(KEYS));
    let bytes = data(
        seed ^ step,
        match rng.below(8) {
            0 => 1 + rng.below(100) as usize,
            1..=5 => 1 + rng.below(4000) as usize,
            _ => 1 + rng.below(60_000) as usize,
        },
    );
    let op = match state.get(&k).cloned().flatten() {
        None if rng.below(2) == 0 => Op::Put(bytes),
        None => Op::Append {
            data: bytes,
            seal: rng.below(4) == 0,
        },
        Some(s) if !s.sealed && rng.below(4) != 0 => Op::Append {
            data: bytes,
            seal: rng.below(3) == 0,
        },
        Some(_) => Op::Delete,
    };
    (k, op)
}

fn apply(state: &Option<State>, op: &Op) -> Option<State> {
    match op {
        Op::Put(bytes) => Some(State {
            bytes: bytes.clone(),
            sealed: true,
        }),
        Op::Append { data, seal } => {
            let mut s = state.clone().unwrap_or(State {
                bytes: Vec::new(),
                sealed: false,
            });
            s.bytes.extend_from_slice(data);
            s.sealed = *seal;
            Some(s)
        }
        Op::Delete => None,
    }
}

fn open_file(path: &Path, create: bool, align: Alignment) -> DeviceFile {
    let file = DeviceFile::open(path, create, CachingRequest::PreferDirect, align).unwrap();
    if create {
        file.preallocate(SIZE).unwrap();
    }
    file
}

/// A number from the OS's entropy: std's `RandomState` keys are drawn from it.
fn entropy() -> u64 {
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish()
}

/// The child: runs the workload until killed, reporting each acknowledgement with the
/// checkpoints its volume has written. The parent kills it by its second checkpoint, so it
/// stops at its third rather than outlive a parent that died.
#[test]
fn kill_child_entry() {
    let Ok(dir) = std::env::var("MANTLE_KILL_CHILD") else {
        return;
    };
    let seed: u64 = std::env::var("MANTLE_KILL_SEED").unwrap().parse().unwrap();
    let device = live::Device::of(Path::new(&dir));
    let issuer = Issuer::start(Path::new(&dir), device.depth).unwrap();
    let path = Path::new(&dir).join("volume");
    let v = Volume::format(
        &issuer,
        open_file(&path, true, device.align),
        SIZE,
        config(),
    )
    .unwrap();
    let mut state: HashMap<ChunkKey, Option<State>> = HashMap::new();
    let mut out = std::io::stdout().lock();
    writeln!(out, "ready").unwrap();
    out.flush().unwrap();
    for step in 0u64.. {
        let (k, op) = op(seed, step, &state);
        let current = state.get(&k).cloned().flatten();
        let result = match &op {
            Op::Put(bytes) => v.put(k, bytes),
            Op::Append { data, seal } => v.append(
                k,
                current.as_ref().map_or(0, |s| s.bytes.len() as u64),
                data,
                *seal,
            ),
            Op::Delete => v.delete(k),
        };
        result.unwrap();
        state.insert(k, apply(&current, &op));
        let checkpoints = v.usage().unwrap().checkpoints;
        writeln!(out, "ack {step} {checkpoints}").unwrap();
        out.flush().unwrap();
        if checkpoints > 2 {
            break;
        }
    }
}

/// When the parent kills its child: once its volume has written this many checkpoints, or
/// after this many acknowledgements.
#[derive(Clone, Copy, Debug)]
enum Kill {
    AtCheckpoints(u64),
    AfterAcks(u64),
}

/// What a kill left: the acknowledgements that took, the checkpoints the volume had written by
/// the last of them, and the operation in flight.
struct Killed {
    acked: u64,
    checkpoints: u64,
    in_flight: Op,
}

fn run(seed: u64, kill: Kill, device: &live::Device) -> Killed {
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
        .env("MANTLE_KILL_CHILD", dir.path())
        .env("MANTLE_KILL_SEED", seed.to_string())
        .env(live::DEVICE_ENV, device.handed())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    let (mut acked, mut checkpoints) = (0u64, 0u64);
    let note = |line: &str, acked: &mut u64, checkpoints: &mut u64| {
        if let Some(rest) = line.strip_prefix("ack ") {
            let mut fields = rest.split(' ').map(|f| f.trim().parse::<u64>().unwrap());
            *acked = fields.next().unwrap() + 1;
            *checkpoints = fields.next().unwrap();
        }
    };
    for line in lines.by_ref() {
        note(&line.unwrap(), &mut acked, &mut checkpoints);
        let due = match kill {
            Kill::AtCheckpoints(c) => checkpoints >= c,
            Kill::AfterAcks(n) => acked >= n,
        };
        if due {
            break;
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    // Acknowledgements the child printed before the kill landed are still in the pipe.
    for line in lines.map_while(Result::ok) {
        note(&line, &mut acked, &mut checkpoints);
    }

    // Replay what was acknowledged; the next operation may or may not have taken effect.
    let mut state: HashMap<ChunkKey, Option<State>> = HashMap::new();
    for step in 0..acked {
        let (k, op) = op(seed, step, &state);
        let current = state.get(&k).cloned().flatten();
        state.insert(k, apply(&current, &op));
    }
    let (uncertain_key, uncertain_op) = op(seed, acked, &state);

    let issuer = Issuer::start(dir.path(), device.depth).unwrap();
    let (v, report) = Volume::open(
        &issuer,
        open_file(&dir.path().join("volume"), false, device.align),
        config(),
    )
    .unwrap_or_else(|e| panic!("seed {seed}: reopening after kill failed: {e}"));
    // Recovery reports only records of the last batch: the write in flight, if any.
    assert!(
        report.damaged.iter().all(|k| *k == uncertain_key),
        "seed {seed}, killed after {acked} acks: {:?} reported damaged, only {uncertain_key} \
         was in flight",
        report.damaged
    );
    for n in 0..KEYS {
        let k = key(n);
        let acked_state = state.get(&k).cloned().flatten();
        if report.damaged.contains(&k) {
            // The write in flight is indexed without its record whole: the chunk stands as the
            // write leaves it, a read of it answers that its bytes do not verify, and the bytes
            // acknowledged before it still read back.
            let after = apply(&acked_state, &uncertain_op).unwrap();
            let s = v.stat(&k).unwrap().unwrap();
            assert_eq!(
                (s.len, s.sealed),
                (after.bytes.len() as u64, after.sealed),
                "seed {seed}, killed after {acked} acks: damaged chunk {k}"
            );
            assert!(
                matches!(v.read(&k, 0, s.len), Err(ChunkError::Corrupt { .. })),
                "seed {seed}, killed after {acked} acks: damaged chunk {k} read back"
            );
            if let Some(a) = acked_state.filter(|a| !a.bytes.is_empty()) {
                assert_eq!(
                    v.read(&k, 0, a.bytes.len() as u64).unwrap(),
                    a.bytes,
                    "seed {seed}, killed after {acked} acks: chunk {k}'s acknowledged bytes"
                );
            }
            continue;
        }
        let got = v.stat(&k).unwrap().map(|s| State {
            bytes: v
                .read(&k, 0, s.len)
                .unwrap_or_else(|e| panic!("seed {seed}: read of {k}: {e}")),
            sealed: s.sealed,
        });
        let alternative = (k == uncertain_key).then(|| apply(&acked_state, &uncertain_op));
        assert!(
            got == acked_state || alternative.as_ref().is_some_and(|a| *a == got),
            "seed {seed}, killed after {acked} acks: chunk {k} recovered {:?}, acknowledged {:?}",
            got.as_ref().map(|s| (s.bytes.len(), s.sealed)),
            acked_state.as_ref().map(|s| (s.bytes.len(), s.sealed)),
        );
    }
    Killed {
        acked,
        checkpoints,
        in_flight: uncertain_op,
    }
}

#[test]
fn a_killed_writer_loses_nothing_it_acknowledged() {
    if std::env::var("MANTLE_KILL_CHILD").is_ok() {
        return;
    }
    let here = tempfile::tempdir().unwrap();
    let device = live::Device::of(here.path());
    let more: u64 = std::env::var("MANTLE_KILL_RUNS")
        .ok()
        .and_then(|r| r.parse().ok())
        .unwrap_or(0);
    let first = run(entropy(), Kill::AtCheckpoints(2), &device);
    let span = first.acked;
    eprintln!(
        "device {device:?}: two checkpoints after {span} acknowledgements; kills land within them"
    );
    // Before the first checkpoint and after it; during a put, an append and a delete.
    let mut landed = Landed::default();
    landed.note(&first);
    let mut runs = 1u64;
    while !landed.everywhere() || runs < more {
        let kill = Kill::AfterAcks(1 + entropy() % span);
        landed.note(&run(entropy(), kill, &device));
        runs += 1;
    }
    eprintln!("{runs} kills");
}

/// Where kills have landed: before the volume's first checkpoint or after it, and with which
/// operation in flight.
#[derive(Default)]
struct Landed {
    phases: [bool; 2],
    kinds: [bool; 3],
}

impl Landed {
    fn note(&mut self, k: &Killed) {
        self.phases[usize::from(k.checkpoints > 0)] = true;
        self.kinds[match k.in_flight {
            Op::Put(_) => 0,
            Op::Append { .. } => 1,
            Op::Delete => 2,
        }] = true;
    }

    fn everywhere(&self) -> bool {
        self.phases.iter().chain(&self.kinds).all(|&l| l)
    }
}
