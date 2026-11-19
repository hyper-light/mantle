//! Standard input and output without blocking a shard (docs/runtime.md §6.2). Readiness serves a pipe, a
//! socket or a terminal on Unix but not a regular file (epoll refuses one with `EPERM`), and on Windows a
//! console or an anonymous pipe cannot be polled at all. One mechanism serves every OS and every kind of
//! handle: **one reader thread and one writer thread for the process**, each moving bytes between the
//! handle and a task through §8's bounded channel.
//!
//! **Bounded, and allocated once**: each direction holds `depth` buffers of `chunk` bytes (the consumer's;
//! a page is the natural chunk), allocated at the start and passed back and forth — full ones one way,
//! empty ones back — so moving bytes allocates nothing. `depth × chunk` bytes each way, two threads
//! (DERIVED).

use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::error::RtError;
use crate::sync::{
    ChannelReceiver, OneshotReceiver, OneshotSender, Sender, SyncError, channel, oneshot,
};

static STDIN_TAKEN: AtomicBool = AtomicBool::new(false);
static STDOUT_TAKEN: AtomicBool = AtomicBool::new(false);

/// Starts a named stdio thread.
fn thread(name: &str, body: impl FnOnce() + Send + 'static) -> Result<(), RtError> {
    #[allow(
        clippy::disallowed_methods,
        reason = "the process's stdio threads (docs/runtime.md §6.2): one reader, one writer"
    )]
    let spawned = std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(body);
    spawned.map(|_| ()).map_err(|_| RtError::BadConfig {
        what: "the OS refused a stdio thread",
    })
}

/// A buffer pool's two ends: where empty buffers go, and where they come from.
type Pool = (Sender<Vec<u8>>, ChannelReceiver<Vec<u8>>);

/// `depth` empty buffers of `chunk` bytes' capacity, on a channel, and the channel's other end.
fn pool(depth: usize, chunk: usize) -> Result<Pool, RtError> {
    if depth == 0 || chunk == 0 {
        return Err(RtError::BadConfig {
            what: "stdio with no buffers or empty ones",
        });
    }
    let (empty_tx, empty_rx) = channel(depth)?;
    for _ in 0..depth {
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(chunk)
            .map_err(|_| RtError::Capacity {
                what: "stdio buffer bytes",
                bound: chunk,
            })?;
        empty_tx.try_send(buffer).map_err(|_| RtError::BadConfig {
            what: "the stdio buffer pool closed while filling",
        })?;
    }
    Ok((empty_tx, empty_rx))
}

/// The process's standard input, read by a task.
#[derive(Debug)]
pub struct Stdin {
    full: ChannelReceiver<Vec<u8>>,
    empty: Sender<Vec<u8>>,
    current: Option<Vec<u8>>,
    at: usize,
}

/// Takes the process's standard input: `depth` buffers of `chunk` bytes. Refused `BadConfig` when taken
/// already.
pub fn stdin(depth: usize, chunk: usize) -> Result<Stdin, RtError> {
    if STDIN_TAKEN.swap(true, Ordering::AcqRel) {
        return Err(RtError::BadConfig {
            what: "standard input taken twice",
        });
    }
    let (empty_tx, mut empty_rx) = pool(depth, chunk)?;
    let (full_tx, full_rx) = channel(depth)?;
    thread("hyper-rt-stdin", move || {
        let mut input = std::io::stdin().lock();
        // Each empty buffer comes back from the task; the end of input (or an error, which ends it too)
        // closes the full side, which the task reads as end of stream.
        while let Ok(mut buffer) = empty_rx.blocking_recv() {
            buffer.resize(buffer.capacity(), 0);
            match input.read(&mut buffer) {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    buffer.truncate(n);
                    if full_tx.blocking_send(buffer).is_err() {
                        return;
                    }
                }
            }
        }
    })?;
    Ok(Stdin {
        full: full_rx,
        empty: empty_tx,
        current: None,
        at: 0,
    })
}

impl Stdin {
    /// Reads into `buf`, awaiting input: the byte count, zero at end of input.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, RtError> {
        loop {
            if let Some(current) = &self.current {
                let rest = current.get(self.at..).unwrap_or(&[]);
                if !rest.is_empty() {
                    let n = rest.len().min(buf.len());
                    if let (Some(into), Some(from)) = (buf.get_mut(..n), rest.get(..n)) {
                        into.copy_from_slice(from);
                    }
                    self.at = self.at.saturating_add(n);
                    return Ok(n);
                }
            }
            if let Some(mut done) = self.current.take() {
                done.clear();
                // Back to the reader; its slot is the one this buffer left.
                let _ = self.empty.try_send(done);
            }
            match self.full.recv().await {
                Ok(next) => {
                    self.current = Some(next);
                    self.at = 0;
                }
                Err(_) => return Ok(0),
            }
        }
    }
}

/// The process's standard output, written by a task.
///
/// **A failure ends the writer, and says why** (mantle: vendor/UPSTREAM.md). The first write or flush the
/// OS refuses ends the writer thread: nothing handed over after it is written, so the output never goes on
/// past a hole. The thread sends what ended it, the OS's error code where the OS gave one, and every call
/// after it is refused with that, never a bare "closed": a reader gone (`EPIPE`), a full disk (`ENOSPC`)
/// and a non-blocking descriptor with no room (`EAGAIN`) each say themselves (`tests/stdout_long_run.rs`).
#[derive(Debug)]
pub struct Stdout {
    out: Sender<OutMessage>,
    empty: ChannelReceiver<Vec<u8>>,
    /// What ended the writer thread.
    ended: Ended,
}

/// What ended a stdout's writer thread, as its writer hears it.
#[derive(Debug)]
struct Ended {
    /// Sent by the thread before its ends of the channels go.
    said: OneshotReceiver<RtError>,
    /// What was said, once a call has taken it, for every call after.
    kept: Option<RtError>,
    /// The writers the channel to the thread holds waiting for room at once.
    places: usize,
}

impl Ended {
    /// What a hand-off to or from the writer thread that was refused `e` means: the thread's end where a
    /// channel closed, else the hand-off's own refusal (a poll off a shard, or every waiting place held).
    fn of<T>(&mut self, e: &SyncError<T>) -> RtError {
        match e {
            SyncError::Closed(_) => self.why(),
            SyncError::NotOnShardThread(_) => RtError::NotOnShardThread,
            SyncError::Full(_) => RtError::Capacity {
                what: "stdout's writers waiting for room",
                bound: self.places,
            },
        }
    }

    /// What the thread said, kept for every call after; where it said nothing, that it ended (its closure
    /// unwound, the one way it ends with a writer left and nothing said).
    fn why(&mut self) -> RtError {
        let why = match self.kept.take() {
            Some(why) => why,
            None => match self.said.try_recv() {
                Ok(Some(why)) => why,
                Ok(None) | Err(_) => ENDED,
            },
        };
        self.kept = Some(why.clone());
        why
    }
}

/// What the writer thread is handed.
#[derive(Debug)]
struct OutMessage(OutInner);

#[derive(Debug)]
enum OutInner {
    Bytes(Vec<u8>),
    /// Answered once everything before it reached the handle; dropped unanswered when the writer ended.
    Flush(OneshotSender<()>),
}

/// Format: a write of standard output's calls, as an error names them: with the OS's code, and with none.
const WRITE: (&str, &str) = (
    "write(stdout)",
    "write(stdout): the OS took none of the bytes",
);
/// Format: a flush of standard output's calls, as [`WRITE`].
const FLUSH: (&str, &str) = (
    "flush(stdout)",
    "flush(stdout): the OS took none of the bytes",
);

/// Takes the process's standard output: `depth` buffers of `chunk` bytes. Refused `BadConfig` when taken
/// already.
pub fn stdout(depth: usize, chunk: usize) -> Result<Stdout, RtError> {
    if STDOUT_TAKEN.swap(true, Ordering::AcqRel) {
        return Err(RtError::BadConfig {
            what: "standard output taken twice",
        });
    }
    let (empty_tx, empty_rx) = pool(depth, chunk)?;
    // One more than the buffers, for a flush queued behind a full set of them.
    let (out_tx, mut out_rx) = channel::<OutMessage>(depth.saturating_add(1))?;
    let (says, ended) = oneshot::<RtError>()?;
    thread("hyper-rt-stdout", move || {
        // Locked per message, not for the thread's life, so other writers in the process (a log line)
        // interleave between chunks rather than wait forever.
        let output = std::io::stdout();
        loop {
            let Ok(OutMessage(message)) = out_rx.blocking_recv() else {
                // Every writer is gone: nothing more to write and no one to tell.
                return;
            };
            let why = match message {
                OutInner::Bytes(mut buffer) => match output.lock().write_all(&buffer) {
                    Err(e) => refused(WRITE, &e),
                    Ok(()) => {
                        buffer.clear();
                        match empty_tx.blocking_send(buffer) {
                            Ok(()) => continue,
                            // The writer's pool went with the writer.
                            Err(SyncError::Closed(_)) => return,
                            Err(SyncError::Full(_)) => RtError::Capacity {
                                what: "stdout's buffers handed back to its writer",
                                bound: depth,
                            },
                            Err(SyncError::NotOnShardThread(_)) => RtError::BadConfig {
                                what: "stdout's writer thread runs a shard",
                            },
                        }
                    }
                },
                OutInner::Flush(done) => match output.lock().flush() {
                    Ok(()) => {
                        // A flush its writer stopped waiting for has no one to answer.
                        let _ = done.send(());
                        continue;
                    }
                    Err(e) => {
                        // Said before the flush's answer goes unsent (`done`, dropped by the return), so
                        // the writer woken by that finds why.
                        let _ = says.send(refused(FLUSH, &e));
                        return;
                    }
                },
            };
            // Said before the channels go (the return drops them), so a writer that finds them closed
            // finds why; a writer gone has no one to hear it.
            let _ = says.send(why);
            return;
        }
    })?;
    Ok(Stdout {
        out: out_tx,
        empty: empty_rx,
        ended: Ended {
            said: ended,
            kept: None,
            places: depth.saturating_add(1),
        },
    })
}

/// The OS's refusal of `call` on standard output, with its error code. The one failure with no code is a
/// write the OS took none of (std's `WriteZero`), said as `uncoded`.
fn refused((call, uncoded): (&'static str, &'static str), e: &std::io::Error) -> RtError {
    match e.raw_os_error() {
        Some(code) => RtError::DriverRefused {
            call,
            code: Some(code),
        },
        None => RtError::DriverRefused {
            call: uncoded,
            code: None,
        },
    }
}

impl Stdout {
    /// Writes all of `buf`, a chunk at a time, awaiting a free buffer while the writer is behind.
    ///
    /// Each chunk awaits room on the writer's channel, then a buffer, then is handed over with no await
    /// between, so a write dropped at either await (a caller racing it against other work, as mantle's client
    /// races its sink against its transport) took no buffer and handed over nothing of that chunk. Each await
    /// may answer `Pending` with its operation undone, the task's turn spent (`crate::coop`); a buffer held
    /// across the hand-off's await was dropped with such a write, and once the pool's every buffer had gone so,
    /// the next write waited for good for one no thread would hand back (`tests/stdout_dropped_write.rs`).
    ///
    /// Refused with what ended the writer thread once it ended ([`Stdout`]): bytes handed over before a
    /// failure the thread had not yet met are refused by the next call, a flush at the latest.
    pub async fn write_all(&mut self, mut buf: &[u8]) -> Result<(), RtError> {
        while !buf.is_empty() {
            let room = match self.out.reserve().await {
                Ok(room) => room,
                Err(e) => return Err(self.ended.of(&e)),
            };
            let mut buffer = match self.empty.recv().await {
                Ok(buffer) => buffer,
                Err(e) => return Err(self.ended.of(&e)),
            };
            let n = buffer.capacity().min(buf.len());
            let (now, rest) = buf.split_at(n);
            buffer.extend_from_slice(now);
            buf = rest;
            if let Err(e) = room.send(OutMessage(OutInner::Bytes(buffer))) {
                return Err(self.ended.of(&e));
            }
        }
        Ok(())
    }

    /// Waits until everything written so far is flushed to the handle; refused with what ended the writer
    /// thread when a write or flush failed ([`Stdout`]).
    pub async fn flush(&mut self) -> Result<(), RtError> {
        let (done, flushed) = oneshot()?;
        if let Err(e) = self.out.send(OutMessage(OutInner::Flush(done))).await {
            return Err(self.ended.of(&e));
        }
        match flushed.await {
            Ok(()) => Ok(()),
            Err(e) => Err(self.ended.of(&e)),
        }
    }
}

/// What a writer hears from a writer thread that ended without saying why.
const ENDED: RtError = RtError::DriverRefused {
    call: "write(stdout): the writer thread ended",
    code: None,
};
