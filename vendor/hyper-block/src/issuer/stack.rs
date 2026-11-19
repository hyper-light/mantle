//! The stack each issuer worker runs on ([`WORKER_STACK`]), measured. Each path a worker runs is
//! run in a process of its own on a worker of one stack size, the size halved toward the least the
//! path runs on: a worker that passes the end of its stack faults on the guard page std's spawn
//! leaves beneath it, and std's handler ends the process saying the thread overflowed its stack.
//! The least size a path runs on is its high-water mark, to the step the platform maps stacks in.
//!
//! The paths: a worker started and stopped with nothing to do; a file attached and detached, its
//! slot made and its duplicate closed; a direct file's write, read and flush, the platform's full
//! flush; the errors a transfer fails with, each a typed error made on the worker (a misaligned
//! write, a read past the file's end, a write the OS refuses); a write and a flush that unwind, the
//! flush's payload unwinding again as it drops, each caught at the worker's boundary after the
//! panic hook printed it, under each `RUST_BACKTRACE` (`0`; `1`, CI's; `full`), so a backtrace is
//! taken and symbolized on the worker's stack; and a segmented file's and the simulated device's
//! write, read and flush.

use std::path::Path;
use std::process::Command;

use super::{Issuer, WORKER_STACK};
use crate::DiskError;
use crate::block::BlockFile;
use crate::buf::{AlignedBuf, Alignment};
use crate::file::{CachingRequest, DeviceFile};
use crate::segmented::SegmentedFile;
use crate::sim::SimDevice;

/// The variable that names the path and the stack a child runs: `"<path> <bytes>"`.
const PROBE: &str = "HYPER_BLOCK_WORKER_STACK_PROBE";

/// The largest stack a path is searched at: Rust's default, 2 MiB (std `thread` module, "Stack
/// size"), which every worker ran on before it had a size of its own.
const MOST: usize = 2 << 20;

/// Shape: the bytes each transfer moves, and the alignment of the files it moves them through.
const PAGE: usize = 4096;

/// Shape: the pages the device file holds.
const PAGES: u64 = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Way {
    Idle,
    Attach,
    Write,
    Read,
    Flush,
    Errors,
    /// Under `RUST_BACKTRACE` set so.
    Unwind(&'static str),
    Segmented,
    Simulated,
}

const WAYS: [Way; 11] = [
    Way::Idle,
    Way::Attach,
    Way::Write,
    Way::Read,
    Way::Flush,
    Way::Errors,
    Way::Unwind("0"),
    Way::Unwind("1"),
    Way::Unwind("full"),
    Way::Segmented,
    Way::Simulated,
];

impl Way {
    fn name(self) -> String {
        match self {
            Self::Unwind(backtrace) => format!("unwind-{backtrace}"),
            other => format!("{other:?}").to_lowercase(),
        }
    }

    /// `RUST_BACKTRACE` in its child: the way's own, else none whatever the parent's.
    fn backtrace(self) -> &'static str {
        match self {
            Self::Unwind(backtrace) => backtrace,
            _ => "0",
        }
    }
}

/// Every path's high-water mark on this platform, each the least stack its worker runs on: the
/// largest, with as much again beside it, is no more than [`WORKER_STACK`].
#[test]
fn every_path_a_worker_runs_fits_its_stack() {
    let exe = std::env::current_exe().unwrap();
    let step = step();
    let found: Vec<(Way, (usize, usize))> = std::thread::scope(|scope| {
        let searches: Vec<_> = WAYS
            .iter()
            .map(|&way| {
                let exe = &exe;
                scope.spawn(move || (way, least(exe, way, step)))
            })
            .collect();
        searches.into_iter().map(|s| s.join().unwrap()).collect()
    });
    for (way, (faults, runs)) in &found {
        eprintln!(
            "{}: runs on {runs} bytes, faults on {faults} (step {step})",
            way.name()
        );
    }
    let most = found.iter().map(|(_, (_, runs))| *runs).max().unwrap();
    assert!(
        most * 2 <= WORKER_STACK,
        "the deepest path runs on {most} bytes: WORKER_STACK {WORKER_STACK} leaves less than as much again"
    );
}

/// One path on a worker of one stack, in a process of its own: a child of
/// `every_path_a_worker_runs_fits_its_stack`, which names both in [`PROBE`].
#[test]
#[ignore = "a child process of every_path_a_worker_runs_fits_its_stack"]
#[allow(
    clippy::disallowed_methods,
    reason = "a child reads the path and the stack its parent names in its environment"
)]
fn one_path_on_one_stack() {
    let named = std::env::var(PROBE).unwrap();
    let (name, stack) = named.split_once(' ').unwrap();
    let way = WAYS.into_iter().find(|w| w.name() == name).unwrap();
    run(way, stack.parse().unwrap());
}

/// The bytes the platform maps a stack in: its page on Unix; on Windows the allocation
/// granularity a stack's reservation is rounded up to, "typically 64 KB" (Microsoft, "Thread Stack
/// Size").
fn step() -> usize {
    #[cfg(unix)]
    {
        rustix::param::page_size()
    }
    #[cfg(not(unix))]
    {
        64 << 10
    }
}

/// The most `way` faulted on and the least it runs on, in steps of `step` bytes between none and
/// [`MOST`], halving the span between the two.
fn least(exe: &Path, way: Way, step: usize) -> (usize, usize) {
    let (mut faults, mut runs) = (0, MOST / step);
    assert!(
        fits(exe, way, MOST),
        "{} overflows {MOST} bytes",
        way.name()
    );
    while runs - faults > 1 {
        let mid = faults + (runs - faults) / 2;
        if fits(exe, way, mid * step) {
            runs = mid;
        } else {
            faults = mid;
        }
    }
    (faults * step, runs * step)
}

/// Whether `way` runs on a worker of `stack` bytes: its child ends well, or overflows the stack,
/// which std's handler says; any other end fails the test with what the child said.
fn fits(exe: &Path, way: Way, stack: usize) -> bool {
    let out = Command::new(exe)
        .args([
            child().as_str(),
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(PROBE, format!("{} {stack}", way.name()))
        .env("RUST_BACKTRACE", way.backtrace())
        .output()
        .unwrap();
    if out.status.success() {
        return true;
    }
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(
        said.contains("has overflowed its stack"),
        "{} on {stack} bytes ended {}: {said}",
        way.name(),
        out.status
    );
    false
}

/// The child's name in this binary: its module's path within the crate.
fn child() -> String {
    let module = module_path!();
    let within = module.split_once("::").map_or(module, |(_, rest)| rest);
    format!("{within}::one_path_on_one_stack")
}

/// Runs `way` through an issuer of one worker of `stack` bytes.
fn run(way: Way, stack: usize) {
    let dir = tempfile::tempdir().unwrap();
    let issuer = Issuer::start_pool(dir.path(), 1, 1, stack).unwrap();
    match way {
        Way::Idle => {}
        Way::Attach => drop(issuer.attach(&device(dir.path())).unwrap()),
        Way::Write => {
            let mut attached = issuer.attach(&device(dir.path())).unwrap();
            attached.write(vec![(page(), 0)], false).unwrap();
        }
        Way::Read => {
            let file = device(dir.path());
            file.write_all_at(page().as_slice(), 0).unwrap();
            let mut attached = issuer.attach(&file).unwrap();
            attached.submit_reads(vec![(blank(), 0)]).unwrap();
            let (_, read) = attached.answer().unwrap();
            assert_eq!(read.unwrap()[0].0.as_slice(), page().as_slice());
        }
        Way::Flush => {
            let file = device(dir.path());
            file.write_all_at(page().as_slice(), 0).unwrap();
            issuer.attach(&file).unwrap().flush().unwrap();
        }
        Way::Errors => errors(&issuer, &device(dir.path())),
        Way::Unwind(_) => unwinds(&issuer, Unwinding(device(dir.path()))),
        Way::Segmented => {
            let base = dir.path().join("segmented");
            let file = SegmentedFile::create(&base, CachingRequest::Buffered, align(), 4).unwrap();
            file.add(PAGES * PAGE as u64).unwrap();
            transfers(&issuer, &file);
        }
        Way::Simulated => {
            let device = SimDevice::new(align(), Alignment::new(512).unwrap(), 7, 4).unwrap();
            transfers(&issuer, &device.handle().unwrap());
        }
    }
    drop(issuer);
}

fn align() -> Alignment {
    Alignment::new(PAGE).unwrap()
}

/// A direct file of [`PAGES`] pages, as a volume's is.
fn device(dir: &Path) -> DeviceFile {
    let file = DeviceFile::open(
        &dir.join("device"),
        true,
        CachingRequest::PreferDirect,
        align(),
    )
    .unwrap();
    file.preallocate(PAGES * PAGE as u64).unwrap();
    file
}

/// A page of ones to write.
fn page() -> AlignedBuf {
    let mut buf = AlignedBuf::zeroed(PAGE, align()).unwrap();
    buf.extend_from_slice(&[1; PAGE]).unwrap();
    buf
}

/// A page to read into.
fn blank() -> AlignedBuf {
    let mut buf = AlignedBuf::zeroed(PAGE, align()).unwrap();
    buf.set_len(PAGE).unwrap();
    buf
}

/// A page written, read back and flushed through the worker.
fn transfers<F: BlockFile + 'static>(issuer: &Issuer, file: &F) {
    let mut attached = issuer.attach(file).unwrap();
    attached.write(vec![(page(), 0)], false).unwrap();
    attached.submit_reads(vec![(blank(), 0)]).unwrap();
    let (_, read) = attached.answer().unwrap();
    assert_eq!(read.unwrap()[0].0.as_slice(), page().as_slice());
    attached.flush().unwrap();
}

/// The errors a transfer fails with on a worker, each answered to a submitter of its own: a write
/// at half a page, refused before any system call; a read of the page past the file's end; and a
/// write at an offset the OS refuses, past the largest it takes (pwrite(2): "EINVAL ... The value
/// is negative", as `off_t` reads it).
fn errors(issuer: &Issuer, file: &DeviceFile) {
    let half = u64::try_from(PAGE / 2).unwrap();
    let misaligned = issuer
        .attach(file)
        .unwrap()
        .write(vec![(page(), half)], false)
        .err();
    assert!(
        matches!(misaligned, Some(DiskError::Misaligned { .. })),
        "{misaligned:?}"
    );
    let mut attached = issuer.attach(file).unwrap();
    attached
        .submit_reads(vec![(blank(), PAGES * PAGE as u64)])
        .unwrap();
    let (_, past) = attached.answer().unwrap();
    let past = past.err();
    assert!(
        matches!(past, Some(DiskError::ShortRead { .. })),
        "{past:?}"
    );
    drop(attached);
    let last = u64::MAX - (PAGE as u64 - 1);
    let refused = issuer
        .attach(file)
        .unwrap()
        .write(vec![(page(), last)], false)
        .err();
    assert!(matches!(refused, Some(DiskError::Io { .. })), "{refused:?}");
}

/// A write and a flush that unwind on the worker, each answered as a failure.
fn unwinds(issuer: &Issuer, file: Unwinding) {
    let mut attached = issuer.attach(&file).unwrap();
    assert!(attached.write(vec![(page(), 0)], false).is_err());
    assert!(attached.flush().is_err());
}

/// A file whose write unwinds with a message made of its offset, and whose flush unwinds with a
/// payload that unwinds again as it drops: the worker's boundary takes both
/// (`unwind::contained`).
struct Unwinding(DeviceFile);

impl BlockFile for Unwinding {
    fn alignment(&self) -> Alignment {
        self.0.alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.0.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.0.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, _: &[u8], offset: u64) -> Result<(), DiskError> {
        panic!("a write at {offset} unwinds")
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        std::panic::panic_any(Unwinds)
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self(self.0.try_clone()?))
    }
}

/// A panic payload whose drop unwinds too.
struct Unwinds;

impl Drop for Unwinds {
    fn drop(&mut self) {
        panic!("a payload that unwinds as it drops");
    }
}
