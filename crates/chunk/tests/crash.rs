//! Power loss at every point of randomized workloads (docs/design/chunk-store.md §10).
//!
//! Each run writes, appends and deletes on a simulated device until power is cut after a
//! random number of device writes and flushes, then crashes the device (every sector written
//! since the last completed flush survives or not, independently) and recovers. Runs cover
//! cuts in the middle of batches, checkpoints and superblock updates.
//!
//! What must hold after recovery:
//! - recovery succeeds: a torn tail is never mistaken for damage;
//! - a chunk whose last acknowledged operation was a write reads back exactly what was
//!   acknowledged, and one whose last acknowledged operation was a delete is absent, except
//!   that each writer's single unacknowledged operation may or may not have taken effect;
//! - every read either returns verified bytes or reports the chunk absent;
//! - the recovered volume accepts writes and survives a clean reopen.
//!
//! `MANTLE_CRASH_SEEDS` raises the number of runs for a soak.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use common::{SIZE, config, data, key, sim};
use mantle_chunk::{ChunkError, ChunkKey, Volume};
use mantle_disk::sim::{Crash, Fault, SimFile};

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

fn apply(state: &Option<State>, op: &Op) -> Option<State> {
    match op {
        Op::Put(bytes) => Some(State {
            bytes: bytes.clone(),
            sealed: true,
        }),
        // Appending nothing without sealing is a no-op.
        Op::Append { data, seal: false } if data.is_empty() => state.clone(),
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

/// One writer's keys and what it knows about them.
struct Model {
    acked: HashMap<ChunkKey, Option<State>>,
    /// The operation that failed when the power went: it may or may not have taken effect.
    uncertain: Option<(ChunkKey, Op)>,
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

/// Runs one writer's operations until one fails; returns its model.
fn writer(v: &Volume<Arc<SimFile>>, id: u64, seed: u64, ops: usize) -> Model {
    let mut rng = Rng(seed ^ (id << 48));
    let keys: Vec<ChunkKey> = (0..6).map(|i| key(id * 1000 + i)).collect();
    let mut model = Model {
        acked: keys.iter().map(|k| (*k, None)).collect(),
        uncertain: None,
    };
    for step in 0..ops {
        // Now and then, clean: relocations race the writes and the power cut.
        if rng.below(12) == 0 {
            match v.clean(2) {
                Ok(_) | Err(ChunkError::Device(_) | ChunkError::Fenced | ChunkError::Closed) => {}
                Err(e) => panic!("writer {id} step {step}: cleaning failed: {e}"),
            }
        }
        let k = keys[rng.below(keys.len() as u64) as usize];
        let state = model.acked[&k].clone();
        let size = match rng.below(10) {
            0 => 0,
            1..=6 => rng.below(3000) as usize,
            _ => rng.below(40_000) as usize,
        };
        let bytes = data(seed ^ (id << 32) ^ step as u64, size);
        let op = match &state {
            None if rng.below(2) == 0 => Op::Put(bytes),
            None => Op::Append {
                data: bytes,
                seal: rng.below(4) == 0,
            },
            Some(s) if !s.sealed && rng.below(5) != 0 => Op::Append {
                data: bytes,
                seal: rng.below(3) == 0,
            },
            Some(_) => Op::Delete,
        };
        let trace = std::env::var("MANTLE_CRASH_TRACE").is_ok();
        let result = match &op {
            Op::Put(bytes) => v.put(k, bytes),
            Op::Append { data, seal } => v.append(
                k,
                state.as_ref().map_or(0, |s| s.bytes.len() as u64),
                data,
                *seal,
            ),
            Op::Delete => v.delete(k),
        };
        if trace {
            let desc = match &op {
                Op::Put(b) => format!("put {}", b.len()),
                Op::Append { data, seal } => format!(
                    "append {} at {} seal={seal}",
                    data.len(),
                    state.as_ref().map_or(0, |s| s.bytes.len())
                ),
                Op::Delete => "delete".to_string(),
            };
            eprintln!(
                "w{id} step {step}: {k} {desc} -> {:?}",
                result.as_ref().map_err(|e| e.to_string())
            );
        }
        match result {
            Ok(()) => {
                model.acked.insert(k, apply(&state, &op));
            }
            Err(ChunkError::Device(_) | ChunkError::Fenced | ChunkError::Closed) => {
                model.uncertain = Some((k, op));
                return model;
            }
            Err(ChunkError::Full) => {}
            Err(e) => panic!("writer {id} step {step}: unexpected refusal {e}"),
        }
    }
    model
}

fn read_all(v: &Volume<Arc<SimFile>>, k: &ChunkKey, seed: u64) -> Option<State> {
    let stat = v.stat(k).unwrap()?;
    let bytes = v
        .read(k, 0, stat.len)
        .unwrap_or_else(|e| panic!("seed {seed}: read of {k} after recovery failed: {e}"));
    Some(State {
        bytes,
        sealed: stat.sealed,
    })
}

fn check(v: &Volume<Arc<SimFile>>, models: &[Model], seed: u64) {
    for model in models {
        for (k, acked) in &model.acked {
            let got = read_all(v, k, seed);
            let allowed_uncertain = model
                .uncertain
                .as_ref()
                .filter(|(uk, _)| uk == k)
                .map(|(_, op)| apply(acked, op));
            let ok = got == *acked || allowed_uncertain.as_ref().is_some_and(|u| got == *u);
            assert!(
                ok,
                "seed {seed}: chunk {k}: recovered {:?}, acknowledged {:?}, uncertain op would give {:?}",
                got.as_ref().map(|s| (s.bytes.len(), s.sealed)),
                acked.as_ref().map(|s| (s.bytes.len(), s.sealed)),
                allowed_uncertain.map(|u| u.map(|s| (s.bytes.len(), s.sealed)))
            );
        }
    }
}

fn run(seed: u64, writers: u64, crash: Crash) {
    let file = sim(seed);
    let v = Volume::format(Arc::clone(&file), SIZE, config()).unwrap();
    let mut rng = Rng(seed);
    file.inject(Fault::PowerCut {
        ops: rng.below(400),
    })
    .unwrap();
    let models: Vec<Model> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..writers)
            .map(|id| {
                let v = &v;
                s.spawn(move || writer(v, id, seed, 80))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    drop(v);
    file.crash(crash).unwrap();
    file.clear_faults().unwrap();

    let (v, report) = Volume::open(Arc::clone(&file), config())
        .unwrap_or_else(|e| panic!("seed {seed}: recovery refused the volume: {e}"));
    if std::env::var("MANTLE_CRASH_TRACE").is_ok() {
        eprintln!("recovery: {report:?}");
        for model in &models {
            for (k, acked) in &model.acked {
                eprintln!(
                    "  {k}: acked {:?} stat {:?}",
                    acked.as_ref().map(|s| (s.bytes.len(), s.sealed)),
                    v.stat(k).unwrap()
                );
            }
            eprintln!(
                "  uncertain: {:?}",
                model.uncertain.as_ref().map(|(k, op)| (
                    k.to_string(),
                    match op {
                        Op::Put(b) => format!("put {}", b.len()),
                        Op::Append { data, seal } => format!("append {} seal={seal}", data.len()),
                        Op::Delete => "delete".into(),
                    }
                ))
            );
        }
    }
    check(&v, &models, seed);

    // The recovered volume keeps working, and a clean reopen changes nothing.
    let fresh = key(999_999);
    v.put(fresh, &data(seed, 5000)).unwrap();
    drop(v);
    let (v, _) = Volume::open(Arc::clone(&file), config()).unwrap();
    check(&v, &models, seed);
    assert_eq!(v.read(&fresh, 0, 5000).unwrap(), data(seed, 5000));
}

fn seeds(default: u64) -> u64 {
    std::env::var("MANTLE_CRASH_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Runs a single seed: `MANTLE_CRASH_SEED=n cargo test --test crash one_seed -- --ignored`.
#[test]
#[ignore]
fn one_seed() {
    let seed: u64 = std::env::var("MANTLE_CRASH_SEED").unwrap().parse().unwrap();
    let writers: u64 = std::env::var("MANTLE_CRASH_WRITERS")
        .ok()
        .and_then(|w| w.parse().ok())
        .unwrap_or(1);
    run(
        seed,
        writers,
        [Crash::Random, Crash::LoseAll, Crash::KeepAll][seed as usize % 3],
    );
}

#[test]
fn power_loss_with_one_writer_never_loses_an_acknowledged_write() {
    for seed in 0..seeds(150) {
        run(
            seed,
            1,
            [Crash::Random, Crash::LoseAll, Crash::KeepAll][seed as usize % 3],
        );
    }
}

#[test]
fn power_loss_with_concurrent_writers_never_loses_an_acknowledged_write() {
    for seed in 0..seeds(100) {
        run(
            10_000 + seed,
            4,
            [Crash::Random, Crash::LoseAll, Crash::KeepAll][seed as usize % 3],
        );
    }
}
