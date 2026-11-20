//! A group's writes are laid in the order submitted, and a refusal keeps that order (mantle
//! docs/design/raft-log.md §3): groups each keep up to their handle's bound of parts out, as
//! hyper-durable's store does, over a log small enough to refuse for room, `Full`, for the
//! group's bound, `Busy` at a full queue and `Behind`; on a refusal each stops sending, may
//! compact from what its handle holds, waits for every write out, and writes again from what its
//! handle holds. No write is refused `Invalid`, which a write laid past an earlier one of its
//! group the log did not take would be ("entries past the last leave a gap").
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cast_possible_truncation
)]

use std::collections::VecDeque;

use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;
use hyper_log::{Config, Entries, Entry, Log, LogError, Start, Update, Waits};

const BLOCK: usize = 4096;
const GROUPS: u128 = 6;
static COUNTS: [std::sync::atomic::AtomicU64; 4] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 4];

fn config() -> Config {
    Config {
        segment_bytes: 16 * BLOCK as u64,
        max_segments: 12,
        max_groups: 8,
        group_entries: 96,
        group_bytes: 1 << 20,
        group_cache: 1 << 12,
        queue_submissions: 6,
        waits: Waits::Measured,
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn run(seed: u64) -> Option<String> {
    let file = SimFile::new(
        Alignment::new(BLOCK).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap();
    let log = Log::create(file, config(), 7).unwrap();
    let failed = std::sync::Mutex::new(None);
    std::thread::scope(|scope| {
        for g in 1..=GROUPS {
            let log = &log;
            let failed = &failed;
            scope.spawn(move || {
                let mut rng = Rng(seed.wrapping_mul(31).wrapping_add(g as u64) | 1);
                let mut handle = log.group(g).unwrap();
                let mut next = 1u64;
                let mut queued: VecDeque<Update> = VecDeque::new();
                let mut sent: VecDeque<String> = VecDeque::new();
                let mut stalled = false;
                let mut made = 0u32;
                while made < 400 {
                    if failed.lock().unwrap().is_some() {
                        return;
                    }
                    if !stalled && queued.is_empty() && handle.outstanding() < 4 {
                        let count = 1 + rng.below(6);
                        let big = rng.below(4) == 0;
                        let entries = (0..count)
                            .map(|_| Entry {
                                term: 1,
                                bytes: vec![
                                    7u8;
                                    if big {
                                        8 * BLOCK + rng.below(6 * BLOCK as u64) as usize
                                    } else {
                                        1 + rng.below(300) as usize
                                    }
                                ],
                            })
                            .collect();
                        let update = Update {
                            entries: Some(Entries {
                                first: next,
                                entries,
                            }),
                            ..Update::default()
                        };
                        next += count;
                        made += 1;
                        queued.extend(handle.parts(update).unwrap());
                        // A compaction now and then, from what the handle holds.
                        if rng.below(5) == 0 {
                            let (start, last) = handle.bounds().unwrap();
                            if last > start.index + 3 {
                                queued.push_back(Update {
                                    start: Some(Start {
                                        index: last - 2,
                                        term: 1,
                                    }),
                                    ..Update::default()
                                });
                            }
                        }
                    }
                    while handle.has_room() {
                        let Some(part) = queued.pop_front() else {
                            break;
                        };
                        let what = format!(
                            "start {:?} entries {:?}",
                            part.start.map(|s| s.index),
                            part.entries.as_ref().map(|e| (e.first, e.entries.len()))
                        );
                        handle.submit(part).unwrap();
                        sent.push_back(what);
                    }
                    let Some(answer) = handle.wait() else {
                        if stalled {
                            // Every write out is answered: write again from what the log holds.
                            let (_, last) = handle.bounds().unwrap();
                            next = last + 1;
                            stalled = false;
                        }
                        continue;
                    };
                    let what = sent.pop_front().unwrap();
                    match answer {
                        Ok(()) => {}
                        Err(LogError::Invalid { reason, .. }) => {
                            *failed.lock().unwrap() = Some(format!(
                                "group {g}: {what} refused: {reason}; handle bounds {:?}",
                                handle.bounds().unwrap()
                            ));
                            return;
                        }
                        Err(
                            e @ (LogError::Full
                            | LogError::Backlog(_)
                            | LogError::Busy
                            | LogError::Behind(_)),
                        ) => {
                            let k = match e {
                                LogError::Full => 0,
                                LogError::Backlog(_) => 1,
                                LogError::Busy => 2,
                                _ => 3,
                            };
                            COUNTS[k].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if !stalled && rng.below(2) == 0 {
                                // As the replica compacts while stalled, before the writes
                                // behind the refusal are answered.
                                let (start, last) = handle.bounds().unwrap();
                                if last > start.index + 1 && handle.has_room() {
                                    handle
                                        .submit(Update {
                                            start: Some(Start {
                                                index: last - 1,
                                                term: 1,
                                            }),
                                            ..Update::default()
                                        })
                                        .unwrap();
                                    sent.push_back("stall compaction".into());
                                }
                            }
                            stalled = true;
                            queued.clear();
                        }
                        Err(e) => panic!("group {g}: {what}: {e}"),
                    }
                }
                while handle.wait().is_some() {}
            });
        }
    });
    failed.into_inner().unwrap()
}

#[test]
fn a_writer_that_keeps_the_refusal_rule_never_leaves_a_gap() {
    for seed in 1..40u64 {
        if let Some(f) = run(seed) {
            panic!("seed {seed}: {f}");
        }
    }
    println!(
        "full/backlog/busy/behind {:?}",
        COUNTS
            .iter()
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
            .collect::<Vec<_>>()
    );
}
