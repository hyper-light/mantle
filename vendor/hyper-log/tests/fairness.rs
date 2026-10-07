//! How the writer shares full frames among groups (docs/design/raft-log.md §3): a hot group
//! beside many cold ones, stepped a flush at a time on the simulated device, so every run of
//! a mix is the same run. Each case prints what it measured, in frames: the unit the writer
//! decides in, which no device's speed changes.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;
use hyper_log::{Class, Config, Entries, Entry, Log, LogError, Pending, Update, Waits};

mod common;
use common::held;

const BLOCK: usize = 4096;
/// Frames each mix runs for.
const FRAMES: u64 = 400;

fn config() -> Config {
    Config {
        segment_bytes: 16 * BLOCK as u64,
        max_segments: 1024,
        max_groups: 64,
        group_entries: 1 << 16,
        group_bytes: 1 << 26,
        group_cache: 1 << 10,
        queue_submissions: 256,
        waits: Waits::Never,
    }
}

/// An update of one entry at `index` whose records take `len` payload bytes.
fn sized(index: u64, len: usize) -> Update {
    let header = hyper_log::format::encoded_len(
        &hyper_log::format::Record::Entries {
            group: 0,
            first: index,
            entries: &[(1, &[])],
        },
        0,
    )
    .unwrap();
    Update {
        entries: Some(Entries {
            first: index,
            entries: vec![Entry {
                term: 1,
                bytes: vec![b'x'; len - header],
            }],
        }),
        ..Update::default()
    }
}

/// A group that submits in a closed loop: its next update once one of its `depth` is answered.
struct Source {
    group: u128,
    class: Class,
    /// Payload bytes of each update.
    len: usize,
    depth: usize,
    next: u64,
    /// Unanswered updates: the entry's index, the frame count when it was sent, its answer.
    out: Vec<(u64, u64, Pending)>,
    /// Frames from each update's sending to its frame's being written.
    waits: Vec<u64>,
}

struct Measured {
    cold_mean: f64,
    cold_p99: u64,
    cold_max: u64,
    hot_per_frame: f64,
    cold_per_frame: f64,
}

fn percentile(sorted: &[u64], percent: usize) -> u64 {
    sorted[(sorted.len() - 1) * percent / 100]
}

/// A hot source and the cold ones beside it: each one's class and share of a frame's payload
/// in percent, and how many cold ones.
struct Mix {
    name: &'static str,
    hot: (Class, usize),
    cold: (usize, Class, usize),
}

/// Runs one hot source beside `cold` cold ones and measures how long the cold ones wait.
fn run(seed: u64, mix: &Mix) -> Measured {
    let (hot, cold) = (mix.hot, mix.cold);
    let file = SimFile::new(
        Alignment::new(BLOCK).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap();
    let (device, stepped) = held(file);
    let log = Log::create(device, config(), 1).unwrap();
    let room = log.frame_room().unwrap();
    let mut sources = vec![Source {
        group: 1,
        class: hot.0,
        len: room * hot.1 / 100,
        depth: 2,
        next: 1,
        out: Vec::new(),
        waits: Vec::new(),
    }];
    for c in 0..cold.0 {
        sources.push(Source {
            group: 100 + c as u128,
            class: cold.1,
            len: room * cold.2 / 100,
            depth: 1,
            next: 1,
            out: Vec::new(),
            waits: Vec::new(),
        });
    }
    stepped.hold();
    let _released = stepped.released();
    // A first flush to hold the writer in.
    let first = log.submit(9, Update::default()).unwrap();
    stepped.held();
    for _ in 0..FRAMES {
        let frames = log.flushed().0;
        for s in &mut sources {
            // What its frames made visible counts as written at this frame.
            let last = log.view(s.group).unwrap().map_or(0, |v| v.last);
            s.out.retain(|(index, sent, _)| {
                if *index <= last {
                    s.waits.push(frames - sent);
                    false
                } else {
                    true
                }
            });
            // The hot one first: the order most against the cold ones in a queue by arrival.
            while s.out.len() < s.depth {
                match log.submit_in(s.group, s.class, sized(s.next, s.len)) {
                    Ok(p) => {
                        s.out.push((s.next, frames, p));
                        s.next += 1;
                    }
                    Err(LogError::Busy) => break,
                    Err(e) => panic!("{e}"),
                }
            }
        }
        stepped.step();
    }
    stepped.release();
    first.wait().unwrap();
    for s in &mut sources {
        for (_, _, p) in s.out.drain(..) {
            p.wait().unwrap();
        }
    }
    let mut cold_waits: Vec<u64> = sources[1..]
        .iter()
        .flat_map(|s| s.waits.iter().copied())
        .collect();
    cold_waits.sort_unstable();
    Measured {
        cold_mean: cold_waits.iter().sum::<u64>() as f64 / cold_waits.len() as f64,
        cold_p99: percentile(&cold_waits, 99),
        cold_max: *cold_waits.last().unwrap(),
        hot_per_frame: sources[0].waits.len() as f64 / FRAMES as f64,
        cold_per_frame: cold_waits.len() as f64 / FRAMES as f64,
    }
}

/// The mixes. An update sent while a frame flushes is written at the earliest two frames
/// later: the one flushing, and the next.
fn mixes() -> [Mix; 4] {
    [
        Mix {
            name: "hot 0.9, 8 cold 0.05",
            hot: (Class::Normal, 90),
            cold: (8, Class::Normal, 5),
        },
        Mix {
            name: "hot 0.6, 16 cold 0.05",
            hot: (Class::Normal, 60),
            cold: (16, Class::Normal, 5),
        },
        Mix {
            name: "hot 0.9, 4 cold 0.3",
            hot: (Class::Normal, 90),
            cold: (4, Class::Normal, 30),
        },
        Mix {
            name: "hot 0.9 background, 8 cold 0.05 latency",
            hot: (Class::Background, 90),
            cold: (8, Class::Latency, 5),
        },
    ]
}

/// Every mix runs to its end, and a cold update is written within the bound the ordering
/// gives (docs/design/raft-log.md §3): the frame flushing when it is sent, the frame that
/// first walks it, and 2⌈B/F⌉ frames once it is passed over, B the queue's bytes and F a
/// frame's.
#[test]
fn a_hot_group_beside_cold_ones() {
    let bound = {
        let file = SimFile::new(
            Alignment::new(BLOCK).unwrap(),
            Alignment::new(512).unwrap(),
            1,
        )
        .unwrap();
        let log = Log::create(file, config(), 1).unwrap();
        let room = log.frame_room().unwrap() as u64;
        2 + 2 * log.queue_bytes().div_ceil(room)
    };
    for (seed, mix) in (50..).zip(mixes()) {
        let (name, m) = (mix.name, run(seed, &mix));
        println!(
            "{name}: cold wait mean {:.3} p99 {} max {}; updates a frame hot {:.3} cold {:.3}",
            m.cold_mean, m.cold_p99, m.cold_max, m.hot_per_frame, m.cold_per_frame
        );
        assert!(
            m.cold_max <= bound,
            "{name}: waited {} past {bound}",
            m.cold_max
        );
        assert!(m.hot_per_frame > 0.0, "{name}: the hot group starved");
    }
}
