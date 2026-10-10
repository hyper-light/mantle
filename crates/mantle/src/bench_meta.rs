//! `mantle bench meta`: what the metadata layers' heaviest commands cost to apply, on one core,
//! over the model engine (audit P02–P05).
//!
//! - Completing a multipart upload of 1 to 10,000 parts, as the Name range applies it.
//! - An entry of 1, 16 or 256 commands from one session whose history of answers is full, as
//!   every replica applies it.
//! - A create or delete attempt learning a directory of 1 to 10,000 Name ranges and walking
//!   their coverage of the bucket, then learning of one split.
//! - The sweep's check of a page of blocks against a file of 10,000 extents, as the File range
//!   applies it.
//!
//! Each step is timed alone, its setup outside the timing, and repeated until the step's time
//! is spent: at least five runs, and the median and slowest are given. The model engine is a
//! sorted map in memory, so the times are the layers' own work; an engine on a device adds
//! its reads and writes to them.

use std::io::Write;
use std::time::{Duration, Instant};

use mantle_disk::histogram::Histogram;
use mantle_meta::apply::{Layer, apply_entry};
use mantle_meta::bucket;
use mantle_meta::coordinator::{Answer, Bounds, Coordinator, CoordinatorError};
use mantle_meta::engine::{Engine, Model};
use mantle_meta::error::MetaError;
use mantle_meta::file;
use mantle_meta::key;
use mantle_meta::name::{
    self, Complete, CreateUpload, GateChange, Listed, MIN_PART, Outcome, Preconditions, Put,
    PutPart, Routed,
};
use mantle_meta::record::{
    Descriptor, Extent, GateState, Lineage, Part, Referrer, Standing, Target, Upload, Version,
    Versioning,
};
use mantle_meta::session::Rules;
use mantle_meta::wire::{self, Command, Entry, Sessioned};

use crate::bench::nanos;
use crate::display;

#[derive(Debug)]
pub enum Error {
    Output(std::io::Error),
    Meta(MetaError),
    Coordinator(CoordinatorError),
    /// A step answered otherwise than the benchmark set it up to.
    Unexpected(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Output(e) => write!(f, "writing output: {e}"),
            Self::Meta(e) => write!(f, "applying: {e}"),
            Self::Coordinator(e) => write!(f, "coordinating: {e}"),
            Self::Unexpected(what) => write!(f, "unexpected answer: {what}"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Output(e)
    }
}

impl From<MetaError> for Error {
    fn from(e: MetaError) -> Self {
        Self::Meta(e)
    }
}

impl From<CoordinatorError> for Error {
    fn from(e: CoordinatorError) -> Self {
        Self::Coordinator(e)
    }
}

impl From<mantle_meta::engine::EngineError> for Error {
    fn from(e: mantle_meta::engine::EngineError) -> Self {
        Self::Meta(e.into())
    }
}

const BUCKET: &str = "b";

/// Runs `step`, which sets itself up and returns the time of the part to measure, until
/// `budget` is spent and at least five times; returns the times.
fn timed(
    budget: Duration,
    mut step: impl FnMut() -> Result<Duration, Error>,
) -> Result<Histogram, Error> {
    let started = Instant::now();
    let mut times = Histogram::new();
    while times.count() < 5 || started.elapsed() < budget {
        times.record(nanos(step()?));
    }
    Ok(times)
}

fn row(out: &mut impl Write, label: String, times: &Histogram) -> Result<(), Error> {
    writeln!(
        out,
        "  {:<34} {:>10} {:>10} {:>7}",
        label,
        display::nanos(times.p50()),
        display::nanos(times.max()),
        times.count()
    )?;
    Ok(())
}

pub fn meta(out: &mut impl Write, step: Duration) -> Result<(), Error> {
    writeln!(
        out,
        "metadata commands applied over the model engine, one core; the median and slowest run"
    )?;
    writeln!(
        out,
        "  {:<34} {:>10} {:>10} {:>7}",
        "", "p50", "max", "runs"
    )?;
    for parts in [1u16, 100, 1_000, 10_000] {
        let times = timed(step, || completion(parts))?;
        row(out, format!("complete an upload of {parts} parts"), &times)?;
    }
    for commands in [1usize, 16, 256] {
        let times = timed(step, || session_entry(commands))?;
        row(
            out,
            format!("entry of {commands} commands, one session"),
            &times,
        )?;
    }
    for ranges in [1usize, 10, 100, 1_000, 10_000] {
        let (learn, relearn) = coordinator(step, ranges)?;
        row(out, format!("learn a directory of {ranges} ranges"), &learn)?;
        row(out, "  then a split of the first".to_owned(), &relearn)?;
    }
    let times = timed(step, || write_file(10_000).map(|(took, _)| took))?;
    row(out, "write a file of 10,000 extents".to_owned(), &times)?;
    for page in [1usize, 64, 512] {
        let times = timed(step, || check_blocks(10_000, page))?;
        row(
            out,
            format!("check {page} blocks of a 10,000-extent file"),
            &times,
        )?;
    }
    // The block sweep's pages at the largest file a PUT makes, 646 blocks (audit B08): one
    // block of each of a page's files, and one file's blocks over pages (audit P05).
    let times = timed(step, || check_files(512, LARGEST_FILE, 1))?;
    row(
        out,
        format!("check 1 block of each of 512 {LARGEST_FILE}-block files"),
        &times,
    )?;
    let times = timed(step, || check_files(1, LARGEST_FILE, 64))?;
    row(
        out,
        format!("check a {LARGEST_FILE}-block file 64 blocks a page"),
        &times,
    )?;
    Ok(())
}

/// The most blocks one PUT's file names: 5 GiB in the smallest blocks any layout makes
/// (`mantle_gateway::layout`, `the_largest_upload_fits_a_file_under_every_layout`).
const LARGEST_FILE: usize = 646;

/// The sweep's checks of `files` files of `blocks` blocks each, `page` blocks of a file to a
/// check, over every block of every file: the time of the checks alone.
fn check_files(files: usize, blocks: usize, page: usize) -> Result<Duration, Error> {
    let mut m = Model::default();
    let per = u128::try_from(blocks).map_err(|_| Error::Unexpected("blocks".into()))?;
    let mut index = 1u64;
    for f in 1..=u128::try_from(files).map_err(|_| Error::Unexpected("files".into()))? {
        let first = f.saturating_mul(per);
        let write = file::Command::Write {
            file: f,
            extents: (first..first.saturating_add(per))
                .map(|block| Extent {
                    length: 1 << 20,
                    target: Target::Block(block),
                })
                .collect(),
            referrer: Referrer {
                bucket: BUCKET.into(),
                incarnation: 1,
                key: format!("k{f}"),
            },
            key: None,
            handover_ns: 100,
            blocks_deadline_ns: u64::MAX,
            at_ns: 10,
        };
        file::apply(&mut m, index, &write)?;
        index = index.saturating_add(1);
    }
    // Each file's blocks from its end, where a scan of its extents finds them last, a page
    // to a check, as the sweep groups a page's blocks by file; one page per file when
    // `page` is 1, as the blocks of a page from distinct files come.
    let mut checks = Vec::new();
    for f in 1..=u128::try_from(files).map_err(|_| Error::Unexpected("files".into()))? {
        let first = f.saturating_mul(per);
        let all: Vec<(u128, u64)> = (first..first.saturating_add(per))
            .rev()
            .map(|b| (b, u64::MAX))
            .collect();
        let take = if files == 1 { all.len() } else { 1 };
        for chunk in all.get(..take).unwrap_or_default().chunks(page) {
            checks.push(file::Command::CheckBlocks {
                file: f,
                blocks: chunk.to_vec(),
                at_ns: 20,
            });
        }
    }
    let started = Instant::now();
    for check in &checks {
        match file::apply(&mut m, index, check)? {
            file::Outcome::BlocksChecked(_) => {}
            other => return Err(Error::Unexpected(format!("{other:?}"))),
        }
        index = index.saturating_add(1);
    }
    Ok(started.elapsed())
}

/// A Name range holding every key, its gate for the bucket open.
fn name_range() -> Result<Model, Error> {
    let mut m = Model::default();
    m.install(0, name::first(1)?)?;
    let open = name::Command::Gate(GateChange {
        bucket: BUCKET.into(),
        incarnation: 1,
        attempt: 1,
        from: None,
        to: Some(GateState::Open),
        generation: 1,
    });
    name::apply(&mut m, 1, &open)?;
    Ok(m)
}

/// Completes an upload of `parts` parts, all but the last the least a part may be; the time
/// of the completion alone.
fn completion(parts: u16) -> Result<Duration, Error> {
    let mut m = name_range()?;
    let created = name::apply(
        &mut m,
        2,
        &name::Command::CreateUpload(CreateUpload {
            bucket: BUCKET.into(),
            incarnation: 1,
            key: "k".into(),
            at_ns: 10,
            upload: Upload {
                initiated_ns: 0,
                owner: "o".into(),
                headers: Vec::new(),
                checksum: None,
                retention: None,
                legal_hold: None,
            },
        }),
    )?;
    let Outcome::Created { upload } = created else {
        return Err(Error::Unexpected(format!("{created:?}")));
    };
    let mut index = 3u64;
    for number in 1..=parts {
        let part = name::Command::PutPart(PutPart {
            bucket: BUCKET.into(),
            incarnation: 1,
            key: "k".into(),
            upload: upload.clone(),
            number,
            part: Part {
                etag: format!("e{number}"),
                size: MIN_PART,
                checksum: None,
                file: u128::from(number),
                modified_ns: 0,
                deadline_ns: u64::MAX,
            },
            at_ns: 20,
        });
        name::apply(&mut m, index, &part)?;
        index = index.saturating_add(1);
    }
    let complete = name::Command::Complete(Complete {
        bucket: BUCKET.into(),
        incarnation: 1,
        key: "k".into(),
        upload,
        versioning: Versioning::Enabled,
        preconditions: Preconditions::default(),
        at_ns: 30,
        parts: (1..=parts)
            .map(|number| Listed {
                number,
                etag: format!("e{number}"),
                file: u128::from(number),
            })
            .collect(),
        // A multipart ETag ends in its part count, which the range checks.
        etag: format!("whole-{parts}"),
        size: MIN_PART.saturating_mul(u64::from(parts)),
        checksum: None,
        file: Some(u128::from(u16::MAX) + 1),
        default: None,
        id: 0,
        deadline_ns: u64::MAX,
        listing: [0; mantle_meta::record::LISTING],
    });
    let started = Instant::now();
    let done = name::apply(&mut m, index, &complete)?;
    let took = started.elapsed();
    match done {
        Outcome::Put { .. } => Ok(took),
        other => Err(Error::Unexpected(format!("{other:?}"))),
    }
}

/// A put of `key` from a session.
fn put(key: String) -> Command {
    Command::Name(Box::new(name::Command::Put(Put {
        bucket: BUCKET.into(),
        incarnation: 1,
        key,
        versioning: Versioning::Enabled,
        preconditions: Preconditions::default(),
        at_ns: 0,
        ordered_ns: None,
        version: Version {
            marker: false,
            null: false,
            modified_ns: 0,
            etag: "e".into(),
            size: 1,
            checksum: None,
            file: Some(1),
            owner: "o".into(),
            headers: Vec::new(),
            retention: None,
            legal_hold: None,
            listing: None,
        },
        default: None,
        id: 0,
        deadline_ns: u64::MAX,
    })))
}

/// The answers a session keeps: a history of 256, full.
const HISTORY: u64 = 256;

/// An entry of `commands` puts from one session whose kept answers are a full history; the
/// time of that entry alone.
fn session_entry(commands: usize) -> Result<Duration, Error> {
    let rules = Rules {
        lifetime_ns: u64::MAX,
        max_sessions: 16,
        max_answers: usize::try_from(HISTORY).unwrap_or(usize::MAX),
        max_answer_bytes: usize::MAX,
        expiries_per_entry: 16,
    };
    let mut m = name_range()?;
    let register = Entry {
        at_ns: 1,
        commands: vec![Sessioned {
            session: 0,
            serial: 0,
            unanswered: 0,
            command: Command::Register,
        }],
    };
    let answers = apply_entry(&mut m, 2, &register, Layer::Name, &rules)?;
    let Some(&wire::Answer::Registered { session }) = answers.first() else {
        return Err(Error::Unexpected(format!("{answers:?}")));
    };
    let sessioned = |serial: u64| Sessioned {
        session,
        serial,
        // The gateway has received every answer but the last `HISTORY`: each command's
        // acknowledgement forgets the oldest kept, and the history stays full without passing
        // the bound, past which the session would expire.
        unanswered: serial.saturating_sub(HISTORY).saturating_add(1),
        command: put(format!("k{serial}")),
    };
    let fill = Entry {
        at_ns: 2,
        commands: (1..=HISTORY).map(sessioned).collect(),
    };
    apply_entry(&mut m, 3, &fill, Layer::Name, &rules)?;
    let entry = Entry {
        at_ns: 3,
        commands: (HISTORY..).skip(1).take(commands).map(sessioned).collect(),
    };
    let started = Instant::now();
    apply_entry(&mut m, 4, &entry, Layer::Name, &rules)?;
    Ok(started.elapsed())
}

/// `ranges` descriptors dividing the bucket's keys, in an order other than their keys'.
fn directory(ranges: usize) -> Vec<Descriptor> {
    let (first, past) = key::bucket_routes(BUCKET);
    let bound = |i: usize| key::route(BUCKET, &format!("k{i:06}"));
    let mut d: Vec<Descriptor> = (0..ranges)
        .zip(1u64..)
        .map(|(i, id)| Descriptor {
            id,
            lo: if i == 0 { first.clone() } else { bound(i) },
            hi: Some(if id == u64::try_from(ranges).unwrap_or(u64::MAX) {
                past.clone()
            } else {
                bound(i.saturating_add(1))
            }),
            generation: 1,
        })
        .collect();
    // Every other range from the back: the directory's order is not the keys'.
    d.sort_by_key(|x| (x.id % 2, std::cmp::Reverse(x.id)));
    d
}

/// A create attempt learning a directory of `ranges` ranges; then, answered at its first range
/// with that range's split, learning the split: the times of each.
fn coordinator(step: Duration, ranges: usize) -> Result<(Histogram, Histogram), Error> {
    let bounds = Bounds {
        budget: 64,
        ranges: 1 << 20,
    };
    let creating = bucket::Outcome::Creating {
        incarnation: 7,
        attempt: 7,
    };
    let dir = directory(ranges);
    let learn = timed(step, || {
        let started = Instant::now();
        let c = Coordinator::start(BUCKET, &creating, &dir, bounds)?;
        let took = started.elapsed();
        c.map(|_| took)
            .ok_or_else(|| Error::Unexpected("no attempt".into()))
    })?;
    let first = dir
        .iter()
        .min_by(|a, b| a.lo.cmp(&b.lo))
        .cloned()
        .ok_or_else(|| Error::Unexpected("no ranges".into()))?;
    let relearn = timed(step, || {
        let mut c = Coordinator::start(BUCKET, &creating, &dir, bounds)?
            .ok_or_else(|| Error::Unexpected("no attempt".into()))?;
        let _ = c.next();
        let middle = key::route(BUCKET, "k000000~");
        let lineage = Lineage {
            now: Descriptor {
                hi: Some(middle.clone()),
                generation: 2,
                ..first.clone()
            },
            child: Some(Descriptor {
                id: u64::MAX,
                lo: middle,
                hi: first.hi.clone(),
                generation: 2,
            }),
            standing: Standing::Serving,
            into: None,
            taken: None,
        };
        let started = Instant::now();
        c.answer(Answer::Gate(Routed::Moved(Box::new(lineage))))?;
        Ok(started.elapsed())
    })?;
    Ok((learn, relearn))
}

/// A File range holding file 7 of `extents` extents, each of its own block, and the time the
/// write took.
fn write_file(extents: usize) -> Result<(Duration, Model), Error> {
    let mut m = Model::default();
    let write = file::Command::Write {
        file: 7,
        extents: (1u128..)
            .take(extents)
            .map(|block| Extent {
                length: 1 << 20,
                target: Target::Block(block),
            })
            .collect(),
        referrer: Referrer {
            bucket: BUCKET.into(),
            incarnation: 1,
            key: "k".into(),
        },
        key: None,
        handover_ns: 100,
        blocks_deadline_ns: u64::MAX,
        at_ns: 10,
    };
    let started = Instant::now();
    file::apply(&mut m, 1, &write)?;
    Ok((started.elapsed(), m))
}

/// The sweep's check of `page` blocks against a file of `extents` extents, each of its own
/// block; the time of the check alone.
fn check_blocks(extents: usize, page: usize) -> Result<Duration, Error> {
    let (_, mut m) = write_file(extents)?;
    let file = 7u128;
    // Blocks from the file's end, where a scan of its extents finds them last.
    let last = u128::try_from(extents).unwrap_or(0);
    let blocks = (0..last)
        .rev()
        .map(|b| (b.saturating_add(1), u64::MAX))
        .take(page)
        .collect();
    let check = file::Command::CheckBlocks {
        file,
        blocks,
        at_ns: 20,
    };
    let started = Instant::now();
    let checked = file::apply(&mut m, 2, &check)?;
    let took = started.elapsed();
    match checked {
        file::Outcome::BlocksChecked(v) if v.len() == page => Ok(took),
        other => Err(Error::Unexpected(format!("{other:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every step runs to its answer: a benchmark whose command the range now refuses is
    /// caught here, where the gates run it, not when someone next measures.
    #[test]
    fn every_step_runs() {
        let mut out = Vec::new();
        meta(&mut out, Duration::from_millis(1)).unwrap();
        assert!(!out.is_empty());
    }
}
