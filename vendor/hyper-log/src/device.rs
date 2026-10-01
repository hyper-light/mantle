//! The log's device thread: the one owner of the log's file (hyper-raft CLAUDE.md §1, the
//! sans-io rule's exception). It takes jobs from the log's owner one at a time, in order, and
//! answers each into the owner's inbox: a frame written and flushed, a confirmation, a tail read
//! for a sweep, entries read back, and a caller's own look at the file.
//!
//! The owner never waits on the device: it hands a job over and goes on answering its callers,
//! and the job's completion comes back as a message like any other. Its jobs are at most one of
//! each kind at once (`owner::Owner`), so the device's queue never holds more than [`JOBS`].
//!
//! The operations of a frame are mantle's, in mantle's order: the frame, its persist record, then
//! one flush, nothing after a failed write, and a failed flush never retried (mantle
//! docs/design/raft-log.md §3).

use std::sync::mpsc::{Receiver, SyncSender};
use std::time::Instant;

use hyper_block::block::BlockFile;
use hyper_block::buf::{AlignedBuf, Alignment, Pool};

use crate::LogError;
use crate::format::{self, FRAME_HEADER_BYTES, FRAME_HEADER_LEN, Owned};
use crate::owner::Message;
use crate::recover::{Found, Reader, Segment};

/// Jobs the device's queue holds at most: one frame or confirmation, one read of entries and
/// one caller's look at the file, each of which the owner keeps to one at a time.
pub(crate) const JOBS: usize = 3;

/// A look at the file a caller runs on the device thread.
pub(crate) type Look<F> = Box<dyn FnOnce(&F) + Send>;

/// What the owner asks of the device.
pub(crate) enum Job<F> {
    /// A frame, and its persist record, then one flush.
    Frame {
        frame: AlignedBuf,
        at: u64,
        record: AlignedBuf,
        record_at: u64,
    },
    /// A confirmation: a persist record written again, then one flush.
    Confirm {
        record: AlignedBuf,
        record_at: u64,
    },
    /// The tail's frames, read through a window of a segment and decoded.
    Sweep {
        segment: Segment,
        offset: u64,
        end: u64,
    },
    /// Entries read back from the file.
    Read(Reads),
    Look(Look<F>),
}

/// What the device answers.
pub(crate) enum Completion {
    Frame {
        frame: AlignedBuf,
        record: AlignedBuf,
        result: Result<(), LogError>,
        /// The batch's service time: its writes and its flush.
        took_ns: u64,
    },
    Confirm {
        record: AlignedBuf,
        result: Result<(), LogError>,
    },
    Sweep(Result<Vec<Swept>, LogError>),
    Read(Reads),
    Looked,
}

/// One verified frame of a swept tail: the file offset of its payload and its records.
pub(crate) struct Swept {
    pub(crate) base: u64,
    pub(crate) records: Vec<Owned>,
}

/// Runs of entries to read back, each a block-aligned span of the file, and where each entry
/// found goes in the caller's reservation.
pub(crate) struct Reads {
    pub(crate) group: u128,
    pub(crate) runs: Vec<Run>,
    /// The entries of every run, a run's after the run's before it.
    pub(crate) wanted: Vec<Wanted>,
    pub(crate) into: crate::Fetched,
    /// Entries a run did not hold as asked: moved since their place was taken, or damaged.
    pub(crate) missed: Vec<Wanted>,
    pub(crate) result: Result<(), LogError>,
}

/// A block-aligned span of the file and the entries in it.
pub(crate) struct Run {
    pub(crate) begin: u64,
    pub(crate) end: u64,
    /// The segment slot the span lies in: a run never crosses into another.
    pub(crate) slot: u32,
    /// The place in the caller's reservation that the run's next entry would take: a run holds
    /// entries asked for one after another.
    pub(crate) contiguous: usize,
    /// Its entries in the reads' list: `count` of them from `first`.
    pub(crate) first: usize,
    pub(crate) count: usize,
}

impl Reads {
    /// Reads of nothing.
    pub(crate) fn none() -> Self {
        Self {
            group: 0,
            runs: Vec::new(),
            wanted: Vec::new(),
            into: crate::Fetched::default(),
            missed: Vec::new(),
            result: Ok(()),
        }
    }
}

/// An entry to read: its position among the entries asked for, its index and term, and the
/// file offset where it was written.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Wanted {
    pub(crate) at: usize,
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) offset: u64,
    pub(crate) len: u32,
}

/// The device thread: owns `file` until the owner's queue closes, then gives it back.
pub(crate) fn run<F: BlockFile>(
    file: F,
    jobs: &Receiver<Job<F>>,
    done: &SyncSender<Message<F>>,
    pool: Pool,
    segment_bytes: u64,
) -> F {
    let mut device = Device {
        file,
        pool,
        segment_bytes,
    };
    while let Ok(job) = jobs.recv() {
        let kind = Kind::of(&job);
        // The file is the caller's code: should it unwind, the job fails as a failed write or
        // read would, and the device, the file with it, goes on (mantle CLAUDE.md §1).
        let completion = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| device.job(job)))
            .unwrap_or_else(|_| kind.unwound());
        // The owner reads its inbox until it ends, and it ends only once every job it handed
        // over has come back, so the inbox takes this.
        if done.send(Message::Done(completion)).is_err() {
            break;
        }
    }
    device.file
}

/// What kind of job a job was, to answer it should the file unwind in it.
#[derive(Clone, Copy)]
enum Kind {
    Frame,
    Confirm,
    Sweep,
    Read(u128),
    Look,
}

impl Kind {
    fn of<F>(job: &Job<F>) -> Self {
        match job {
            Job::Frame { .. } => Self::Frame,
            Job::Confirm { .. } => Self::Confirm,
            Job::Sweep { .. } => Self::Sweep,
            Job::Read(reads) => Self::Read(reads.group),
            Job::Look(_) => Self::Look,
        }
    }

    /// The answer to a job whose file unwound: its write, flush or read failed.
    fn unwound(self) -> Completion {
        let failed = || {
            LogError::Disk(hyper_block::DiskError::Io {
                op: "a file operation unwound",
                path: std::path::PathBuf::new(),
                source: std::io::Error::other("the file unwound"),
            })
        };
        match self {
            Self::Frame => Completion::Frame {
                frame: AlignedBuf::empty(),
                record: AlignedBuf::empty(),
                result: Err(failed()),
                took_ns: 0,
            },
            Self::Confirm => Completion::Confirm {
                record: AlignedBuf::empty(),
                result: Err(failed()),
            },
            Self::Sweep => Completion::Sweep(Err(failed())),
            Self::Read(group) => Completion::Read(Reads {
                group,
                result: Err(failed()),
                ..Reads::none()
            }),
            Self::Look => Completion::Looked,
        }
    }
}

struct Device<F> {
    file: F,
    /// Buffers for reading entries back, kept between reads.
    pool: Pool,
    segment_bytes: u64,
}

impl<F: BlockFile> Device<F> {
    fn job(&mut self, job: Job<F>) -> Completion {
        match job {
            Job::Frame {
                frame,
                at,
                record,
                record_at,
            } => self.frame(frame, at, record, record_at),
            Job::Confirm {
                mut record,
                record_at,
            } => {
                let result = self
                    .write(&mut record, record_at)
                    .and_then(|()| self.file.sync_data().map_err(LogError::from));
                Completion::Confirm { record, result }
            }
            Job::Sweep {
                segment,
                offset,
                end,
            } => Completion::Sweep(self.sweep(segment, offset, end)),
            Job::Read(reads) => Completion::Read(self.read(reads)),
            Job::Look(look) => {
                look(&self.file);
                Completion::Looked
            }
        }
    }

    fn frame(
        &mut self,
        mut frame: AlignedBuf,
        at: u64,
        mut record: AlignedBuf,
        record_at: u64,
    ) -> Completion {
        let started = Instant::now();
        let result = self
            .write(&mut frame, at)
            .and_then(|()| self.write(&mut record, record_at))
            .and_then(|()| self.file.sync_data().map_err(LogError::from));
        Completion::Frame {
            frame,
            record,
            result,
            took_ns: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        }
    }

    /// Writes `buf`, padded with zeros to the file's alignment, at `at`.
    fn write(&self, buf: &mut AlignedBuf, at: u64) -> Result<(), LogError> {
        let bytes = buf.padded().map_err(|e| LogError::Disk(e.into()))?;
        self.file.write_all_at(bytes, at)?;
        Ok(())
    }

    /// The verified frames of `segment` from `offset` to its end `end`, read through a window of
    /// a segment: nothing writes the tail while it is swept.
    fn sweep(&self, segment: Segment, offset: u64, end: u64) -> Result<Vec<Swept>, LogError> {
        let mut reader = Reader::new(&self.file, self.file.alignment(), self.segment_bytes)?;
        let mut out = Vec::new();
        let mut offset = offset;
        loop {
            let (header, bytes, padded) = match reader.frame_at(segment, offset, end)? {
                Found::Frame(header, bytes, padded) => (header, bytes, padded),
                Found::End => break,
                Found::Invalid => {
                    return Err(LogError::Damaged("a live frame does not verify"));
                }
            };
            let body = bytes
                .get(FRAME_HEADER_LEN..header.frame_len().unwrap_or(0))
                .ok_or(LogError::Damaged("a frame shorter than its header says"))?;
            let records = format::records(body, header.records)
                .ok_or(LogError::Damaged("a verified frame does not decode"))?;
            let base = offset
                .checked_add(FRAME_HEADER_BYTES)
                .ok_or(LogError::Damaged("an offset past u64"))?;
            out.push(Swept { base, records });
            offset = offset
                .checked_add(padded)
                .ok_or(LogError::Damaged("an offset past u64"))?;
        }
        Ok(out)
    }

    /// Reads each run in one read, and copies each entry that verifies as the one asked for
    /// into the caller's reservation.
    fn read(&mut self, mut reads: Reads) -> Reads {
        let runs = std::mem::take(&mut reads.runs);
        let wanted = std::mem::take(&mut reads.wanted);
        for run in &runs {
            let entries = run
                .first
                .checked_add(run.count)
                .and_then(|end| wanted.get(run.first..end))
                .unwrap_or_default();
            if let Err(e) = self.read_run(&mut reads, run, entries) {
                reads.result = Err(e);
                break;
            }
        }
        reads.runs = runs;
        reads.wanted = wanted;
        reads
    }

    fn read_run(
        &mut self,
        reads: &mut Reads,
        run: &Run,
        entries: &[Wanted],
    ) -> Result<(), LogError> {
        let size = usize::try_from(run.end.saturating_sub(run.begin))
            .map_err(|_| LogError::Damaged("a read past usize"))?;
        let mut buf = self.pool.take(size).map_err(|e| LogError::Disk(e.into()))?;
        let read = buf
            .set_len(size)
            .map_err(|e| LogError::Disk(e.into()))
            .and_then(|()| {
                self.file
                    .read_exact_at(buf.as_mut_slice(), run.begin)
                    .map_err(LogError::from)
            });
        if read.is_ok() {
            for wanted in entries {
                take_entry(reads, buf.as_slice(), run.begin, *wanted);
            }
        }
        self.pool.give(buf);
        read
    }
}

/// Copies the entry `wanted` from `bytes`, which hold the file from `begin`, into the caller's
/// reservation if it verifies as the group's entry of that index and term; a miss otherwise.
fn take_entry(reads: &mut Reads, bytes: &[u8], begin: u64, wanted: Wanted) {
    let found = usize::try_from(wanted.offset.saturating_sub(begin))
        .ok()
        .and_then(|skip| bytes.get(skip..))
        .and_then(|at| format::entry_slice(at, reads.group, wanted.index))
        .filter(|(term, _)| *term == wanted.term);
    match found {
        Some((_, payload)) => reads.into.fill(wanted.at, payload),
        None => reads.missed.push(wanted),
    }
}

/// The block-aligned span of the file holding an entry of `len` payload bytes at `offset`.
pub(crate) fn span(align: Alignment, offset: u64, len: u32) -> Option<(u64, u64)> {
    let end = offset
        .checked_add(format::ENTRY_HEADER_BYTES)?
        .checked_add(u64::from(len))
        .and_then(|e| align.up_u64(e))?;
    Some((align.down_u64(offset), end))
}
