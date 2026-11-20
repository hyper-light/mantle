//! A frame's confirmation is made durable the cheaper of the two ways its file has measured
//! (`device::Device::confirm`): a write durable on its own, or a write and a flush of the cache;
//! and it is written once the callers the frame's flush answered have had one measured wait to
//! come back, at once where the flush answered no one (`owner::write`'s `hold_confirmation`).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods
)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hyper_block::DiskError;
use hyper_block::block::{BlockFile, Durable};
use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;
use hyper_log::{Config, Entries, Entry, Log, Update, Waits};

const ID: u128 = 0x636f_6e66_6972_6d73;
const BLOCK: usize = 4096;

/// A simulated file whose writes made durable on their own (a FUA write) take `durable` and whose
/// flushes of its cache take `flush`, as a drive's do; each made durable in the simulation.
struct Timed {
    file: SimFile,
    durable: Duration,
    flush: Duration,
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
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        std::thread::sleep(self.flush);
        self.file.sync_data()
    }

    fn write_durable_at(&self, buf: &[u8], offset: u64) -> Result<Durable, DiskError> {
        std::thread::sleep(self.durable);
        self.file.write_all_at(buf, offset)?;
        self.file.sync_data()?;
        Ok(Durable::Written)
    }
}

fn config() -> Config {
    Config {
        segment_bytes: 16 * BLOCK as u64,
        max_segments: 8,
        max_groups: 4,
        group_entries: 1 << 16,
        group_bytes: 1 << 24,
        group_cache: 1 << 10,
        queue_submissions: 256,
        waits: Waits::Measured,
    }
}

/// Twelve entries written one after another, each waited for, so every frame is confirmed on its
/// own: the log's statistics after.
fn confirmations(durable: Duration, flush: Duration) -> Box<hyper_log::LogStats> {
    let file = Timed {
        file: SimFile::new(
            Alignment::new(BLOCK).unwrap(),
            Alignment::new(512).unwrap(),
            7,
        )
        .unwrap(),
        durable,
        flush,
    };
    let log = Log::create(file, config(), ID).unwrap();
    for index in 1..=12u64 {
        log.write(
            1,
            Update {
                entries: Some(Entries {
                    first: index,
                    entries: vec![Entry {
                        term: 1,
                        bytes: index.to_le_bytes().to_vec(),
                    }],
                }),
                ..Update::default()
            },
        )
        .unwrap();
    }
    assert_eq!(log.view(1).unwrap().unwrap().last, 12);
    log.stats(None).unwrap()
}

/// Do: confirm frames on a drive whose durable write costs more than a write and a flush (a
/// consumer NVMe drive's FUA write). Expect: the first confirmation is a durable write, which
/// measures it; every later one is written and flushed.
#[test]
fn a_drive_whose_durable_write_costs_more_than_a_flush_confirms_by_flushing() {
    let stats = confirmations(Duration::from_millis(20), Duration::ZERO);
    assert_eq!(stats.durable_writes, 1, "{stats:?}");
    assert!(stats.flushed_confirmations >= 1, "{stats:?}");
}

/// Do: confirm frames on a drive whose flush costs more than its durable write. Expect: every
/// confirmation is a durable write, none flushed.
#[test]
fn a_drive_whose_flush_costs_more_than_its_durable_write_confirms_durable() {
    let stats = confirmations(Duration::ZERO, Duration::from_millis(20));
    assert_eq!(stats.flushed_confirmations, 0, "{stats:?}");
    assert!(stats.durable_writes >= 1, "{stats:?}");
}

/// What a [`Clocked`] file saw, and when.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mark {
    /// A frame's flush ended: a frame and its record were written since the last flush.
    Flushed,
    /// A confirmation's write began: written durable on its own, or written alone and flushed.
    Confirming,
}

/// A simulated file whose flush, and whose write made durable on its own, take `flush`, as
/// `F_FULLFSYNC` does either way on macOS. It notes when each frame's flush ended and when each
/// confirmation's write began, and each frame's writes and flush, from the first write's start to
/// the flush's end, what the log measures as a frame's service time; and says when a frame's
/// flush begins.
struct Clocked {
    file: SimFile,
    flush: Duration,
    /// Writes since the last flush, and when the first of them began.
    writes: AtomicUsize,
    first: Mutex<Option<Instant>>,
    marks: Arc<Mutex<Vec<(Mark, Instant)>>>,
    frames: Arc<Mutex<Vec<Duration>>>,
    flushing: SyncSender<()>,
}

impl BlockFile for Clocked {
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
        if self.writes.fetch_add(1, Ordering::SeqCst) == 0 {
            *self.first.lock().unwrap() = Some(Instant::now());
        }
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        let writes = self.writes.swap(0, Ordering::SeqCst);
        let first = self.first.lock().unwrap().take().unwrap();
        if writes >= 2 {
            let _ = self.flushing.try_send(());
        }
        std::thread::sleep(self.flush);
        self.file.sync_data()?;
        let ended = Instant::now();
        let mark = if writes >= 2 {
            self.frames.lock().unwrap().push(ended - first);
            (Mark::Flushed, ended)
        } else {
            (Mark::Confirming, first)
        };
        self.marks.lock().unwrap().push(mark);
        Ok(())
    }

    fn write_durable_at(&self, buf: &[u8], offset: u64) -> Result<Durable, DiskError> {
        self.marks
            .lock()
            .unwrap()
            .push((Mark::Confirming, Instant::now()));
        std::thread::sleep(self.flush);
        self.file.write_all_at(buf, offset)?;
        self.file.sync_data()?;
        Ok(Durable::Written)
    }
}

fn entry(index: u64) -> Update {
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

/// From each confirmation's start, back to the end of the frame flush before it.
fn holds(marks: &[(Mark, Instant)]) -> Vec<Duration> {
    let mut flushed = None;
    let mut holds = Vec::new();
    for (mark, at) in marks {
        match mark {
            Mark::Flushed => flushed = Some(*at),
            Mark::Confirming => holds.push(at.duration_since(flushed.unwrap())),
        }
    }
    holds
}

/// Do: on a file whose flush takes 20 ms, write one entry alone; then one of group 1, and once
/// its frame is flushing one of group 2, and nothing after. Expect: the lone write's frame,
/// whose flush answered no one, is confirmed at once; group 2's frame, whose flush answered
/// group 1's write, is confirmed on its own with nothing more submitted, once group 1 had its
/// wait to come back, one measured frame's writes and flush (`hyper_block::commit::Anticipation`,
/// `p·S / (0 + p)` = `S`), and within that wait and the scheduling's slack.
#[test]
fn a_lone_frame_is_confirmed_within_one_wait_for_the_callers_its_flush_answered() {
    let flush = Duration::from_millis(20);
    let marks = Arc::new(Mutex::new(Vec::new()));
    let spans = Arc::new(Mutex::new(Vec::new()));
    let (flushing, frames): (SyncSender<()>, Receiver<()>) = sync_channel(8);
    let file = Clocked {
        file: SimFile::new(
            Alignment::new(BLOCK).unwrap(),
            Alignment::new(512).unwrap(),
            7,
        )
        .unwrap(),
        flush,
        writes: AtomicUsize::new(0),
        first: Mutex::new(None),
        marks: Arc::clone(&marks),
        frames: Arc::clone(&spans),
        flushing,
    };
    let log = Log::create(file, config(), ID).unwrap();
    spans.lock().unwrap().clear();
    log.write(3, entry(1)).unwrap();
    while frames.try_recv().is_ok() {}
    let first = log.submit(1, entry(1)).unwrap();
    frames.recv().unwrap();
    let second = log.submit(2, entry(1)).unwrap();
    first.wait().unwrap();
    second.wait().unwrap();
    let stats = log.stats(None).unwrap();
    assert_eq!(stats.frames, 3, "{stats:?}");
    assert_eq!(stats.confirmations, 2, "{stats:?}");
    let holds = holds(&marks.lock().unwrap());
    let spans = spans.lock().unwrap().clone();
    assert_eq!(holds.len(), 2, "{holds:?}");
    assert_eq!(spans.len(), 3, "{spans:?}");
    // The wait is the log's service time, a moving average of what each frame's writes and flush
    // took, each a little more than the file saw of it: no less than the least the file saw, and
    // no more than the most. Past the wait, the owner's timed wake and the job's hand-off run on a
    // loaded machine's scheduling, measured here up to 0.13 s late: as slack, one more service
    // time and a flush, where a wait that never ended would not come at all.
    let (least, most) = (*spans.iter().min().unwrap(), *spans.iter().max().unwrap());
    assert!(
        holds[0] < flush / 2,
        "a frame that answered no one waited: {holds:?}"
    );
    assert!(
        holds[1] >= least,
        "the confirmation did not wait for the callers the flush answered: {holds:?} {spans:?}"
    );
    assert!(
        holds[1] < most * 2 + flush,
        "the confirmation waited past one measured service time: {holds:?} {spans:?}"
    );
}
