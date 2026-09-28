//! A writer process killed at a random point, on a real file system through the real
//! direct-I/O file layer: every write the child reported as acknowledged must be there after
//! reopening, and the one write in flight may or may not be.
//!
//! The test binary re-launches itself as the child (`kill_child_entry` with
//! `MANTLE_KILL_CHILD` set), which formats a volume in a directory the parent names and
//! prints `ack <step>` after each acknowledged operation. Both sides generate the same
//! operations from the seed, so the parent can replay what was acknowledged.
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

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};

use common::{SIZE, config, data, key};
use mantle_chunk::{ChunkKey, Volume};
use mantle_disk::buf::Alignment;
use mantle_disk::file::{CachingRequest, DeviceFile};

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

fn open_file(path: &Path, create: bool) -> DeviceFile {
    let file = DeviceFile::open(
        path,
        create,
        CachingRequest::PreferDirect,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    if create {
        file.preallocate(SIZE).unwrap();
    }
    file
}

/// The child: runs the workload until killed, reporting each acknowledgement.
#[test]
fn kill_child_entry() {
    let Ok(dir) = std::env::var("MANTLE_KILL_CHILD") else {
        return;
    };
    let seed: u64 = std::env::var("MANTLE_KILL_SEED").unwrap().parse().unwrap();
    let path = Path::new(&dir).join("volume");
    let v = Volume::format(open_file(&path, true), SIZE, config()).unwrap();
    let mut state: HashMap<ChunkKey, Option<State>> = HashMap::new();
    let mut out = std::io::stdout().lock();
    writeln!(out, "ready").unwrap();
    out.flush().unwrap();
    for step in 0..100_000u64 {
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
        writeln!(out, "ack {step}").unwrap();
        out.flush().unwrap();
    }
}

fn run(seed: u64, kill_after: u64) {
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
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    let mut acked = 0u64;
    let note = |line: &str, acked: &mut u64| {
        if let Some(step) = line.strip_prefix("ack ") {
            *acked = step.trim().parse::<u64>().unwrap() + 1;
        }
    };
    for line in lines.by_ref() {
        note(&line.unwrap(), &mut acked);
        if acked >= kill_after {
            break;
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    // Acknowledgements the child printed before the kill landed are still in the pipe.
    for line in lines.map_while(Result::ok) {
        note(&line, &mut acked);
    }

    // Replay what was acknowledged; the next operation may or may not have taken effect.
    let mut state: HashMap<ChunkKey, Option<State>> = HashMap::new();
    for step in 0..acked {
        let (k, op) = op(seed, step, &state);
        let current = state.get(&k).cloned().flatten();
        state.insert(k, apply(&current, &op));
    }
    let (uncertain_key, uncertain_op) = op(seed, acked, &state);

    let (v, _) = Volume::open(open_file(&dir.path().join("volume"), false), config())
        .unwrap_or_else(|e| panic!("seed {seed}: reopening after kill failed: {e}"));
    for n in 0..KEYS {
        let k = key(n);
        let acked_state = state.get(&k).cloned().flatten();
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
}

#[test]
fn a_killed_writer_loses_nothing_it_acknowledged() {
    if std::env::var("MANTLE_KILL_CHILD").is_ok() {
        return;
    }
    let runs: u64 = std::env::var("MANTLE_KILL_RUNS")
        .ok()
        .and_then(|r| r.parse().ok())
        .unwrap_or(8);
    for seed in 0..runs {
        let kill_after = 1 + (seed * 37 + 11) % 120;
        run(seed, kill_after);
    }
}
