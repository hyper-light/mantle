//! A fetch of entries no longer in memory is read from the file off the log's device, on a second
//! handle (`device::Fetcher`), so a write is not held behind the read. Before, the read was the
//! device's one job, and every frame behind it waited for it: a learner's catch-up of megabytes
//! behind each of the leader's writes (kiln's audit row B21, 2026-11-06).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity
)]

use std::cell::Cell;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::time::{Duration, Instant};

use hyper_block::DiskError;
use hyper_block::block::{BlockFile, Durable};
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Class, Config, Entries, Entry, Fetched, Log, Page, Update, Waits};

/// How long the test holds the read at most before it lets it go: far past any write's frame and
/// flush on a working machine, however busy. On the old code the write waits for the read whatever
/// this is, and the test fails once it passes.
const HELD_FOR: Duration = Duration::from_secs(60);

/// A file whose next read, once armed, holds until let go: the read is the handle's that reads
/// entries back, the fetcher's (`BlockFile::try_clone` moves the trap to it), or the device's on a
/// log with no fetcher.
struct Trapped {
    file: DeviceFile,
    trap: Cell<Option<Trap>>,
}

struct Trap {
    armed: Receiver<()>,
    held: SyncSender<()>,
    go: Receiver<()>,
}

impl BlockFile for Trapped {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn layout_block(&self) -> Alignment {
        self.file.layout_block()
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        let trap = self.trap.take();
        if let Some(t) = &trap
            && t.armed.try_recv().is_ok()
        {
            let _ = t.held.send(());
            let _ = t.go.recv();
        }
        self.trap.set(trap);
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }

    fn write_durable_at(&self, buf: &[u8], offset: u64) -> Result<Durable, DiskError> {
        self.file.write_durable_at(buf, offset)
    }

    fn fills_new_space(&self) -> bool {
        self.file.fills_new_space()
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            trap: Cell::new(self.trap.take()),
        })
    }
}

/// Entries of `bytes` each, from `first`, a group's catch-up of `count` of them.
fn entries(first: u8, count: usize, bytes: usize) -> Vec<Entry> {
    (0..count)
        .map(|n| Entry {
            term: 1,
            bytes: vec![first.wrapping_add(n as u8); bytes],
        })
        .collect()
}

/// Do: write 8 MiB to group 1 in 64 KiB entries, past what the log keeps in memory; hold the read
/// of a fetch of all of it from the file; while it is held, write to group 2. Expect: group 2's
/// write durable while the read is held, and the fetch, once let go, every entry as written.
#[test]
fn a_write_is_not_held_behind_a_fetch_read_from_the_file() {
    const ENTRY: usize = 64 << 10;
    const COUNT: usize = 128;
    let dir = tempfile::tempdir().unwrap();
    let (arm, armed) = sync_channel(1);
    let (held, holding) = sync_channel(1);
    let (go, let_go) = sync_channel(1);
    let file = Trapped {
        file: DeviceFile::open(
            &dir.path().join("log"),
            true,
            CachingRequest::PreferDirect,
            Alignment::new(4096).unwrap(),
        )
        .unwrap(),
        trap: Cell::new(Some(Trap {
            armed,
            held,
            go: let_go,
        })),
    };
    let config = Config {
        segment_bytes: 1 << 20,
        max_segments: 64,
        max_groups: 4,
        group_entries: 1 << 12,
        group_bytes: 1 << 26,
        group_cache: 1 << 16,
        queue_submissions: 64,
        waits: Waits::Never,
    };
    let log = Log::create(file, config, 0x0066_6574_6368).unwrap();
    let written = entries(1, COUNT, ENTRY);
    for (at, chunk) in written.chunks(4).enumerate() {
        let first = 1 + (at * 4) as u64;
        log.write(
            1,
            Update {
                entries: Some(Entries {
                    first,
                    entries: chunk.to_vec(),
                }),
                ..Update::default()
            },
        )
        .unwrap();
    }
    arm.send(()).unwrap();
    let fetching = log
        .fetch_waking(
            1,
            1,
            COUNT as u64 + 1,
            Page::payload(u64::MAX),
            Fetched::new(),
            None,
        )
        .unwrap();
    holding
        .recv_timeout(HELD_FOR)
        .expect("the fetch's read reaches the file");
    let (tell, woken) = sync_channel(1);
    let (waker, _) = hyper_measure::wake::waker(0, tell);
    let asked = Instant::now();
    let write = log
        .submit_waking(
            2,
            Class::Normal,
            Update {
                entries: Some(Entries {
                    first: 1,
                    entries: entries(9, 1, 4096),
                }),
                ..Update::default()
            },
            waker,
        )
        .unwrap();
    let durable = woken.recv_timeout(HELD_FOR).ok().map(|_| asked.elapsed());
    go.send(()).unwrap();
    write.wait().unwrap();
    let fetched = fetching.wait().unwrap();
    assert!(
        durable.is_some(),
        "group 2's write waited {HELD_FOR:?} on group 1's fetch and was not durable"
    );
    assert_eq!(fetched.len(), COUNT);
    for ((term, bytes), entry) in fetched.iter().zip(&written) {
        assert_eq!((term, bytes), (entry.term, entry.bytes.as_slice()));
    }
}
