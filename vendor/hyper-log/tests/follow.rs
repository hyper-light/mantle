//! Under steady load each frame is confirmed by the next frame's persist record, and a
//! confirmation of its own is written only when no frame follows (mantle docs/design/raft-log.md
//! §3). Groups submit as a replica does (hyper-durable `Replica`; mantle docs/design/replica.md
//! §3, "Readies ahead of their persistence"): each keeps its handle's `depth` writes out, and
//! sends its next the moment one is answered. The file's flush takes a fixed time, so frames
//! gather; at the end of each frame's flush the groups are looked at, to say why a frame had no
//! follower.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cast_precision_loss
)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use hyper_block::DiskError;
use hyper_block::block::{BlockFile, Durable};
use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;
use hyper_log::{Config, Entries, Entry, Log, Update, Waits};

const ID: u128 = 0x666f_6c6c_6f77;
const BLOCK: usize = 4096;
const GROUPS: usize = 8;
/// Writes each group makes.
const WRITES: u64 = 150;

/// What each group's submitter is doing, as the file sees it at a flush's end.
#[derive(Default)]
struct Group {
    /// Writes submitted and not yet answered.
    out: AtomicUsize,
    /// 1 between hearing an answer and sending the next write; 0 otherwise.
    turning: AtomicUsize,
    /// 1 once it has sent its last write.
    done: AtomicUsize,
}

/// At one frame flush's end, how many groups were in each state.
#[derive(Default, Clone, Copy, Debug)]
struct Seen {
    /// Every one of the group's `depth` writes out: its next write waits for one of them.
    at_depth: usize,
    /// Fewer out, its submitter between an answer and its next write.
    turning: usize,
    /// Fewer out, and nothing more to send.
    done: usize,
    /// Fewer out, its submitter neither turning nor done: not yet started.
    other: usize,
}

/// A simulated file whose flush, and whose write made durable on its own, takes `flush`, as
/// `F_FULLFSYNC` does either way on macOS. At each frame's flush end it looks at the groups.
struct Timed {
    file: SimFile,
    flush: Duration,
    groups: Arc<Vec<Group>>,
    /// The handles' depth, set before the first write.
    depth: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Seen>>>,
    /// Writes since the last flush: a frame and its record before a frame's flush, one alone
    /// before a confirmation's.
    writes: AtomicUsize,
    /// Confirmations of their own written while every group was still sending.
    steady: Arc<AtomicUsize>,
}

impl Timed {
    /// Counts a confirmation of its own, if every group was still sending.
    fn confirmed(&self) {
        if self
            .groups
            .iter()
            .all(|g| g.done.load(Ordering::SeqCst) == 0)
        {
            self.steady.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl BlockFile for Timed {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        BlockFile::len(&self.file)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        std::thread::sleep(self.flush);
        if self.writes.swap(0, Ordering::SeqCst) < 2 {
            self.confirmed();
            return self.file.sync_data();
        }
        let mut seen = Seen::default();
        let depth = self.depth.load(Ordering::SeqCst);
        for g in self.groups.iter() {
            let out = g.out.load(Ordering::SeqCst);
            if out >= depth {
                seen.at_depth += 1;
            } else if g.done.load(Ordering::SeqCst) == 1 {
                seen.done += 1;
            } else if g.turning.load(Ordering::SeqCst) == 1 {
                seen.turning += 1;
            } else {
                seen.other += 1;
            }
        }
        self.seen.lock().unwrap().push(seen);
        self.file.sync_data()
    }

    fn write_durable_at(&self, buf: &[u8], offset: u64) -> Result<Durable, DiskError> {
        self.writes.store(0, Ordering::SeqCst);
        std::thread::sleep(self.flush);
        self.confirmed();
        self.file.write_all_at(buf, offset)?;
        self.file.sync_data()?;
        Ok(Durable::Written)
    }
}

fn config() -> Config {
    Config {
        segment_bytes: 256 * BLOCK as u64,
        max_segments: 64,
        max_groups: GROUPS,
        group_entries: 1 << 16,
        group_bytes: 1 << 26,
        group_cache: 1 << 10,
        queue_submissions: 256,
        waits: Waits::Measured,
    }
}

fn update(index: u64) -> Update {
    Update {
        entries: Some(Entries {
            first: index,
            entries: vec![Entry {
                term: 1,
                bytes: vec![1u8],
            }],
        }),
        ..Update::default()
    }
}

/// What a run measured.
#[derive(Debug)]
struct Measured {
    frames: u64,
    confirmations: u64,
    flushes: u64,
    /// Confirmations of their own written while every group was still sending.
    steady_confirmations: usize,
    /// Frame flush ends seen while every group was still sending.
    steady: Vec<Seen>,
}

/// `GROUPS` groups, each keeping its handle's depth of writes out and sending its next the
/// moment one is answered, `WRITES` each, on a file whose flush takes `flush`.
fn run(flush: Duration) -> Measured {
    let groups: Arc<Vec<Group>> = Arc::new((0..GROUPS).map(|_| Group::default()).collect());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let steady = Arc::new(AtomicUsize::new(0));
    let file_depth = Arc::new(AtomicUsize::new(0));
    let file = Timed {
        file: SimFile::new(
            Alignment::new(BLOCK).unwrap(),
            Alignment::new(512).unwrap(),
            7,
        )
        .unwrap(),
        flush,
        groups: Arc::clone(&groups),
        depth: Arc::clone(&file_depth),
        seen: Arc::clone(&seen),
        writes: AtomicUsize::new(0),
        steady: Arc::clone(&steady),
    };
    let log = Log::create(file, config(), ID).unwrap();
    let handles: Vec<_> = (1..=GROUPS as u128)
        .map(|g| log.group(g).unwrap())
        .collect();
    let depth = handles[0].depth();
    file_depth.store(depth, Ordering::SeqCst);
    let start = Arc::new(Barrier::new(GROUPS));
    std::thread::scope(|scope| {
        for (g, mut handle) in handles.into_iter().enumerate() {
            let groups = Arc::clone(&groups);
            let start = Arc::clone(&start);
            scope.spawn(move || {
                let me = &groups[g];
                start.wait();
                let mut next = 1u64;
                loop {
                    while handle.outstanding() < depth && next <= WRITES {
                        me.out.fetch_add(1, Ordering::SeqCst);
                        handle.submit(update(next)).unwrap();
                        next += 1;
                    }
                    me.turning.store(0, Ordering::SeqCst);
                    if next > WRITES {
                        me.done.store(1, Ordering::SeqCst);
                    }
                    let Some(answer) = handle.wait() else { break };
                    answer.unwrap();
                    me.turning.store(1, Ordering::SeqCst);
                    me.out.fetch_sub(1, Ordering::SeqCst);
                }
            });
        }
    });
    let stats = log.stats(None).unwrap();
    let ends = seen
        .lock()
        .unwrap()
        .iter()
        .copied()
        .filter(|s| s.done == 0)
        .collect();
    Measured {
        frames: stats.frames,
        confirmations: stats.confirmations,
        flushes: stats.flushes,
        steady_confirmations: steady.load(Ordering::SeqCst),
        steady: ends,
    }
}

/// Do: eight groups each keep their depth of one-entry writes out, sending the next the moment
/// one is answered, on a file whose flush takes 3 ms. Expect: while every group is sending, a
/// frame's flush ends with a group's next write admitted, so the next frame's record confirms it
/// and no confirmation of its own is written; one is written only as the load ends. With two
/// submissions a group (before 2026-11-20) every group was at its depth at each flush's end, its
/// third write waiting for its group's room, and every other frame was confirmed on its own:
/// 75 confirmations in 151 frames, 73 of them while every group was sending.
#[test]
fn under_steady_load_each_frame_is_confirmed_by_the_next() {
    let m = run(Duration::from_millis(3));
    let ends = m.steady.len().max(1) as f64;
    let sum = |f: fn(&Seen) -> usize| m.steady.iter().map(f).sum::<usize>() as f64 / ends;
    println!(
        "frames {} confirmations {} ({} while every group sent) flushes {} ({:.3} confirmations \
         a frame); at a steady frame flush's end, of {GROUPS} groups: {:.2} at depth, {:.2} \
         between an answer and the next write, {:.2} other, over {} flush ends",
        m.frames,
        m.confirmations,
        m.steady_confirmations,
        m.flushes,
        m.confirmations as f64 / m.frames.max(1) as f64,
        sum(|s| s.at_depth),
        sum(|s| s.turning),
        sum(|s| s.other),
        m.steady.len(),
    );
    // A group's submitter that the OS holds past a whole flush leaves a frame unfollowed: a few
    // such are the machine's, not the log's; one for every two frames is the log's.
    assert!(
        m.steady_confirmations * 10 <= m.steady.len(),
        "{} confirmations of their own in {} frames under steady load",
        m.steady_confirmations,
        m.steady.len()
    );
}
