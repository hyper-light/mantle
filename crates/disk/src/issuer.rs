//! One issuer per physical device: every volume on the device hands it its writes and flushes,
//! and it keeps the device's depth in flight on a fixed set of workers (docs/design/node.md
//! §1.2; chunk-store.md §4).
//!
//! The issuer is one thread and a pool of blocking workers, all started when the device opens
//! and none after, whatever the number of volumes, batches or regions: a thread per region of a
//! batch, which the chunk writer started before, is a thread start on the write path and a count
//! of threads that grows with load (research/26 §4.7, recommendation 7). The pool is the portable
//! path of research/26 §2.5, the only one macOS offers: no interface there keeps more than 16
//! file I/Os in flight without a blocked thread per I/O, and none carries `F_FULLFSYNC` (§2.4).
//! Linux's io_uring and Windows's completion port, which node.md §1.2 takes where they exist, are
//! not yet built; the pool runs on every platform until they are.
//!
//! The pool holds `min(device queue, measured depth, thread budget)` workers ([`depth`]): the
//! device queues no more than its queue, throughput stops growing at the measured depth, so a
//! transfer past it only waits, and every pool draws from the process's budget before any thread
//! starts (research/26 §2.5, recommendation 4). Where the budget has fewer left the device runs
//! at the depth it leaves.
//!
//! Ownership is single and passed by message: the issuer thread owns every attached file, in
//! an arena its workers borrow inside the issuer's own thread scope, and the dispatch state;
//! each worker owns one slot its tasks arrive on, woken alone (research/26 §5.3). A volume's
//! writer holds an [`Attached`], through which it submits one batch at a time and waits for its
//! answer. A batch's writes all complete before its flush is issued, and the flush only if all
//! succeeded: a write that failed fails its batch, and its flush is never issued, because the
//! caller then fences and recovers rather than trust what reached the device (Rebello et al.,
//! ATC 2020). The answer comes once, after the flush.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread::{Builder, JoinHandle};

use crate::DiskError;
use crate::block::BlockFile;
use crate::buf::AlignedBuf;
use crate::calibrate::UNDESCRIBED_QUEUE_DEPTH;
use crate::threads::{self, Reservation};

/// The transfers a device's issuer keeps in flight: the smaller of the queue the OS reports
/// (`UNDESCRIBED_QUEUE_DEPTH` when it reports none) and the depth calibration measured
/// throughput to stop growing at. A device calibration has not measured gets one at a time,
/// as its reads do (docs/design/chunk-store.md §7).
pub fn depth(queue: Option<u32>, measured: Option<usize>) -> usize {
    let queue = queue
        .and_then(|q| usize::try_from(q).ok())
        .filter(|&q| q > 0)
        .unwrap_or(UNDESCRIBED_QUEUE_DEPTH);
    queue.min(measured.filter(|&d| d > 0).unwrap_or(1))
}

/// A device's issuer: its thread, its workers and the budget they hold. Dropping it lets the
/// transfers in flight end, refuses what is queued, and joins every thread.
pub struct Issuer {
    events: SyncSender<Event>,
    thread: Option<JoinHandle<()>>,
    workers: usize,
    path: PathBuf,
    /// Given back once the threads are joined.
    budget: Option<Reservation>,
}

/// What a batch's submitter is answered: the batch's buffers, in the order given, once every
/// write and the flush asked for have completed.
type Answer = Result<Vec<AlignedBuf>, DiskError>;

enum Event {
    Attach {
        file: Box<dyn BlockFile>,
        answers: SyncSender<Answer>,
        reply: SyncSender<Result<(usize, u64), DiskError>>,
    },
    Detach {
        slot: usize,
        generation: u64,
        done: SyncSender<()>,
    },
    Batch {
        slot: usize,
        generation: u64,
        writes: Vec<(AlignedBuf, u64)>,
        flush: bool,
    },
    Done {
        worker: usize,
        task: Finished,
    },
    Stop,
}

enum Op {
    /// The `index`th write of its batch.
    Write {
        index: usize,
        buf: AlignedBuf,
        at: u64,
    },
    Flush,
}

struct Task {
    slot: usize,
    generation: u64,
    op: Op,
}

/// A worker's report of one task: the write's index and buffer, `None` for a flush.
struct Finished {
    slot: usize,
    generation: u64,
    write: Option<(usize, Option<AlignedBuf>)>,
    result: Result<(), DiskError>,
}

/// An attached file, as the workers find it.
struct File {
    generation: u64,
    file: Box<dyn BlockFile>,
}

impl Issuer {
    /// Starts the issuer of the device at `path` with `depth` workers, or fewer when the
    /// process's thread budget has fewer left; [`DiskError::Threads`] when it has none for one
    /// worker and the issuer's own thread. Every thread starts here, before any is used.
    pub fn start(path: &Path, depth: usize) -> Result<Self, DiskError> {
        if depth == 0 {
            return Err(invalid(path, "an issuer needs a depth of one at least"));
        }
        let left = threads::left()?;
        let workers = depth.min(left.saturating_sub(1));
        let threads = workers.saturating_add(1);
        if workers == 0 {
            return Err(DiskError::Threads {
                path: path.to_path_buf(),
                asked: depth.saturating_add(1),
                left,
                ceiling: threads::ceiling()?,
            });
        }
        let budget = threads::reserve(threads, path)?;
        // Room for every worker's completion at once, so a worker never waits to report while
        // the issuer drains what arrives; a submitter waits for room behind them.
        let (events, inbox) = sync_channel(workers);
        let (ready, started) = sync_channel(1);
        let completions = events.clone();
        let device = path.to_path_buf();
        let thread = Builder::new()
            .name("mantle-issuer".into())
            .spawn(move || run(&device, &inbox, &completions, workers, &ready))
            .map_err(|source| DiskError::Io {
                op: "start a device issuer",
                path: path.to_path_buf(),
                source,
            })?;
        let outcome = started
            .recv()
            .unwrap_or_else(|_| Err(stopped(path, "the issuer ended before it started")));
        if let Err(e) = outcome {
            // The issuer has stopped what it started; its thread is ending.
            let _ = thread.join();
            return Err(e);
        }
        Ok(Self {
            events,
            thread: Some(thread),
            workers,
            path: path.to_path_buf(),
            budget: Some(budget),
        })
    }

    /// The workers the issuer runs: the transfers it keeps in flight at most.
    pub fn depth(&self) -> usize {
        self.workers
    }

    /// Hands the issuer a duplicate of `file`, for one submitter's batches.
    pub fn attach<F: BlockFile + 'static>(&self, file: &F) -> Result<Attached, DiskError> {
        let file: Box<dyn BlockFile> = Box::new(file.try_clone()?);
        // One batch is out at a time (`Attached::write` takes `&mut self`), so one answer.
        let (answers, answered) = sync_channel(1);
        let (reply, replied) = sync_channel(1);
        let gone = || stopped(&self.path, "the device's issuer has stopped");
        self.events
            .send(Event::Attach {
                file,
                answers,
                reply,
            })
            .map_err(|_| gone())?;
        let (slot, generation) = replied.recv().map_err(|_| gone())??;
        Ok(Attached {
            slot,
            generation,
            events: self.events.clone(),
            answers: answered,
            path: self.path.clone(),
        })
    }
}

impl Drop for Issuer {
    fn drop(&mut self) {
        // The issuer drains its inbox while it runs, so the stop finds room.
        let _ = self.events.send(Event::Stop);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.budget.take();
    }
}

/// One submitter's way to its device's issuer: a volume's writer holds one. Dropping it
/// detaches the file and returns once the issuer no longer holds it.
pub struct Attached {
    slot: usize,
    generation: u64,
    events: SyncSender<Event>,
    answers: Receiver<Answer>,
    path: PathBuf,
}

impl Attached {
    /// Issues every `(buffer, offset)` of `writes` at once, as deep as the device's workers go,
    /// and once all have completed, a flush when `flush` is set and every write succeeded.
    /// Returns the buffers, in order, after the last of them; the first failure otherwise, once
    /// none is in flight.
    pub fn write(
        &mut self,
        writes: Vec<(AlignedBuf, u64)>,
        flush: bool,
    ) -> Result<Vec<AlignedBuf>, DiskError> {
        if writes.is_empty() && !flush {
            return Ok(Vec::new());
        }
        let gone = || stopped(&self.path, "the device's issuer has stopped");
        self.events
            .send(Event::Batch {
                slot: self.slot,
                generation: self.generation,
                writes,
                flush,
            })
            .map_err(|_| gone())?;
        self.answers.recv().map_err(|_| gone())?
    }

    /// Makes every completed write durable: the platform's full flush, on a worker.
    pub fn flush(&mut self) -> Result<(), DiskError> {
        self.write(Vec::new(), true).map(|_| ())
    }
}

impl Drop for Attached {
    fn drop(&mut self) {
        let (done, detached) = sync_channel(1);
        let detach = Event::Detach {
            slot: self.slot,
            generation: self.generation,
            done,
        };
        if self.events.send(detach).is_ok() {
            // Answered once the file is out of the arena, or dropped if the issuer stops first.
            let _ = detached.recv();
        }
    }
}

/// The issuer's thread: starts the workers in its own scope, so they borrow the arena of files
/// it owns, reports whether all started, then dispatches until stopped.
fn run(
    path: &Path,
    inbox: &Receiver<Event>,
    completions: &SyncSender<Event>,
    workers: usize,
    ready: &SyncSender<Result<(), DiskError>>,
) {
    let files: RwLock<Vec<Option<File>>> = RwLock::new(Vec::new());
    std::thread::scope(|scope| {
        let mut slots = Vec::with_capacity(workers);
        let mut handles = Vec::with_capacity(workers);
        let mut failed = None;
        for id in 0..workers {
            let (tasks, assigned) = sync_channel::<Task>(1);
            let (files, done) = (&files, completions.clone());
            match Builder::new()
                .name("mantle-io".into())
                .spawn_scoped(scope, move || work(id, files, &assigned, &done))
            {
                Ok(handle) => {
                    slots.push(tasks);
                    handles.push(handle);
                }
                Err(source) => {
                    failed = Some(DiskError::Io {
                        op: "start a device worker",
                        path: path.to_path_buf(),
                        source,
                    });
                    break;
                }
            }
        }
        let started = match failed {
            Some(e) => Err(e),
            None => Ok(()),
        };
        if ready.send(started).is_ok() && slots.len() == workers {
            Dispatch {
                files: &files,
                idle: (0..slots.len()).rev().collect(),
                in_flight: 0,
                workers: slots,
                pending: VecDeque::new(),
                clients: Vec::new(),
                generation: 0,
                stopping: false,
                path,
            }
            .run(inbox);
        } else {
            drop(slots);
        }
        // Each worker ends once its slot's sender is gone; a worker that unwound is already
        // ended. Joined here, so the scope has nothing left to report.
        for handle in handles {
            let _ = handle.join();
        }
    });
}

/// A worker: takes each task from its own slot, issues it and reports. An unwind from the
/// file, which production code never raises, is caught here and reported as an error, so a
/// batch's submitter never waits on a report that cannot come.
fn work(
    id: usize,
    files: &RwLock<Vec<Option<File>>>,
    tasks: &Receiver<Task>,
    done: &SyncSender<Event>,
) {
    while let Ok(Task {
        slot,
        generation,
        op,
    }) = tasks.recv()
    {
        let (write, result) = match op {
            Op::Write { index, buf, at } => {
                let issued = std::panic::catch_unwind(AssertUnwindSafe(move || {
                    let result = with_file(files, slot, generation, |f| {
                        f.write_all_at(buf.as_slice(), at)
                    });
                    (buf, result)
                }));
                match issued {
                    Ok((buf, result)) => (Some((index, Some(buf))), result),
                    Err(_) => (Some((index, None)), Err(unwound())),
                }
            }
            Op::Flush => {
                let issued = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    with_file(files, slot, generation, |f| f.sync_data())
                }));
                (None, issued.unwrap_or_else(|_| Err(unwound())))
            }
        };
        let task = Finished {
            slot,
            generation,
            write,
            result,
        };
        if done.send(Event::Done { worker: id, task }).is_err() {
            return;
        }
    }
}

/// Runs `op` on the attached file, holding the arena to read only while it runs.
fn with_file(
    files: &RwLock<Vec<Option<File>>>,
    slot: usize,
    generation: u64,
    op: impl FnOnce(&dyn BlockFile) -> Result<(), DiskError>,
) -> Result<(), DiskError> {
    let files = files.read().map_err(|_| unwound())?;
    match files.get(slot) {
        Some(Some(f)) if f.generation == generation => op(&*f.file),
        _ => Err(stopped(
            Path::new(""),
            "a task for a file no longer attached",
        )),
    }
}

/// A submitter as the issuer keeps it: where its answers go, and its batch while one is out.
struct Client {
    generation: u64,
    answers: SyncSender<Answer>,
    batch: Option<Batch>,
}

struct Batch {
    /// Tasks issued or queued and not yet reported.
    outstanding: usize,
    buffers: Vec<Option<AlignedBuf>>,
    failed: Option<DiskError>,
    /// A flush is still to be issued once the writes have completed.
    flush: bool,
}

/// The issuer's state, owned by its thread.
struct Dispatch<'a> {
    files: &'a RwLock<Vec<Option<File>>>,
    /// Each worker's slot.
    workers: Vec<SyncSender<Task>>,
    /// Workers with no task.
    idle: Vec<usize>,
    /// Tasks handed to workers and not yet reported.
    in_flight: usize,
    /// Tasks waiting for a worker, in the order they arrived. Each attached submitter has at
    /// most one batch out, so these are at most the attached submitters' batches, whose
    /// buffers their submitters already hold.
    pending: VecDeque<Task>,
    /// Indexed as the arena is.
    clients: Vec<Option<Client>>,
    generation: u64,
    stopping: bool,
    path: &'a Path,
}

impl Dispatch<'_> {
    fn run(mut self, inbox: &Receiver<Event>) {
        while let Ok(event) = inbox.recv() {
            match event {
                Event::Attach {
                    file,
                    answers,
                    reply,
                } => {
                    let attached = self.attach(file, answers);
                    let _ = reply.try_send(attached);
                }
                Event::Detach {
                    slot,
                    generation,
                    done,
                } => {
                    self.detach(slot, generation);
                    let _ = done.try_send(());
                }
                Event::Batch {
                    slot,
                    generation,
                    writes,
                    flush,
                } => self.batch(slot, generation, writes, flush),
                Event::Done { worker, task } => {
                    self.idle.push(worker);
                    self.in_flight = self.in_flight.saturating_sub(1);
                    self.finished(task);
                }
                Event::Stop => {
                    self.stopping = true;
                    for task in std::mem::take(&mut self.pending) {
                        self.refused(task);
                    }
                }
            }
            self.dispatch();
            if self.stopping && self.in_flight == 0 {
                return;
            }
        }
    }

    fn attach(
        &mut self,
        file: Box<dyn BlockFile>,
        answers: SyncSender<Answer>,
    ) -> Result<(usize, u64), DiskError> {
        if self.stopping {
            return Err(stopped(self.path, "the device's issuer is stopping"));
        }
        self.generation = self.generation.saturating_add(1);
        let generation = self.generation;
        let mut files = self.files.write().map_err(|_| unwound())?;
        // A slot a detached file left is reused, so the arena holds the files attached now.
        let slot = files
            .iter()
            .position(Option::is_none)
            .unwrap_or(files.len());
        let file = Some(File { generation, file });
        let client = Some(Client {
            generation,
            answers,
            batch: None,
        });
        match files.get_mut(slot) {
            Some(free) => *free = file,
            None => files.push(file),
        }
        match self.clients.get_mut(slot) {
            Some(free) => *free = client,
            None => self.clients.push(client),
        }
        Ok((slot, generation))
    }

    fn detach(&mut self, slot: usize, generation: u64) {
        if !self
            .clients
            .get(slot)
            .is_some_and(|c| c.as_ref().is_some_and(|c| c.generation == generation))
        {
            return;
        }
        // The submitter waits for nothing once it detaches; its batch, if any, has ended.
        if let Some(client) = self.clients.get_mut(slot) {
            *client = None;
        }
        // A poisoned arena is still a list of files; this one must be let go whatever
        // unwound while the lock was held.
        let mut files = self
            .files
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(file) = files.get_mut(slot) {
            *file = None;
        }
    }

    fn batch(&mut self, slot: usize, generation: u64, writes: Vec<(AlignedBuf, u64)>, flush: bool) {
        let refused = self.stopping;
        let path = self.path;
        let Some(client) = self.client(slot, generation) else {
            // Not attached: the submitter's answers went with it.
            return;
        };
        if refused || client.batch.is_some() {
            let why = if refused {
                "the device's issuer is stopping"
            } else {
                "a second batch while one is out"
            };
            let _ = client.answers.try_send(Err(stopped(path, why)));
            return;
        }
        let count = writes.len();
        if count == 0 && !flush {
            let _ = client.answers.try_send(Ok(Vec::new()));
            return;
        }
        let outstanding = if count == 0 { 1 } else { count };
        client.batch = Some(Batch {
            outstanding,
            buffers: std::iter::repeat_with(|| None).take(count).collect(),
            failed: None,
            flush: flush && count > 0,
        });
        if count == 0 {
            self.pending.push_back(Task {
                slot,
                generation,
                op: Op::Flush,
            });
        }
        for (index, (buf, at)) in writes.into_iter().enumerate() {
            self.pending.push_back(Task {
                slot,
                generation,
                op: Op::Write { index, buf, at },
            });
        }
    }

    fn client(&mut self, slot: usize, generation: u64) -> Option<&mut Client> {
        self.clients
            .get_mut(slot)
            .and_then(Option::as_mut)
            .filter(|c| c.generation == generation)
    }

    /// Records a task's report, and answers its batch or issues its flush once every write
    /// has completed.
    fn finished(&mut self, task: Finished) {
        let Finished {
            slot,
            generation,
            write,
            result,
        } = task;
        let Some(client) = self.client(slot, generation) else {
            return;
        };
        let Some(batch) = client.batch.as_mut() else {
            return;
        };
        if let Some((index, buf)) = write
            && let Some(kept) = batch.buffers.get_mut(index)
        {
            *kept = buf;
        }
        if let Err(e) = result {
            batch.failed.get_or_insert(e);
        }
        batch.outstanding = batch.outstanding.saturating_sub(1);
        if batch.outstanding > 0 {
            return;
        }
        if batch.flush && batch.failed.is_none() {
            batch.flush = false;
            batch.outstanding = 1;
            self.pending.push_back(Task {
                slot,
                generation,
                op: Op::Flush,
            });
            return;
        }
        if let Some(batch) = client.batch.take() {
            let answer = match batch.failed {
                Some(e) => Err(e),
                None => Ok(batch.buffers.into_iter().flatten().collect()),
            };
            let _ = client.answers.try_send(answer);
        }
    }

    /// A task never issued: its batch fails.
    fn refused(&mut self, task: Task) {
        let write = match task.op {
            Op::Write { index, buf, .. } => Some((index, Some(buf))),
            Op::Flush => None,
        };
        let path = self.path;
        self.finished(Finished {
            slot: task.slot,
            generation: task.generation,
            write,
            result: Err(stopped(path, "the device's issuer is stopping")),
        });
    }

    /// Hands queued tasks to idle workers, in the order they arrived.
    fn dispatch(&mut self) {
        while !self.pending.is_empty() {
            let Some(worker) = self.idle.pop() else {
                return;
            };
            let Some(task) = self.pending.pop_front() else {
                self.idle.push(worker);
                return;
            };
            let sent = match self.workers.get(worker) {
                Some(slot) => slot.try_send(task),
                None => Err(TrySendError::Disconnected(task)),
            };
            // An idle worker's slot is empty; one that is not, or whose worker ended, keeps no
            // place among the idle, and its task fails its batch.
            match sent {
                Ok(()) => self.in_flight = self.in_flight.saturating_add(1),
                Err(TrySendError::Full(task) | TrySendError::Disconnected(task)) => {
                    let path = self.path;
                    let write = match task.op {
                        Op::Write { index, buf, .. } => Some((index, Some(buf))),
                        Op::Flush => None,
                    };
                    self.finished(Finished {
                        slot: task.slot,
                        generation: task.generation,
                        write,
                        result: Err(stopped(path, "a device worker has ended")),
                    });
                }
            }
        }
    }
}

fn stopped(path: &Path, why: &str) -> DiskError {
    DiskError::Io {
        op: "issue",
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::BrokenPipe, why.to_owned()),
    }
}

fn unwound() -> DiskError {
    stopped(Path::new(""), "a device worker unwound")
}

fn invalid(path: &Path, why: &str) -> DiskError {
    DiskError::Io {
        op: "issue",
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidInput, why.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Condvar, Mutex};

    use super::*;
    use crate::buf::Alignment;

    /// A file in memory that counts the transfers in flight, holds every write at a gate the
    /// test opens, and fails the write at one offset when asked.
    struct Probe {
        data: Mutex<Vec<u8>>,
        state: Mutex<State>,
        changed: Condvar,
    }

    #[derive(Default)]
    struct State {
        in_flight: usize,
        most: usize,
        entered: usize,
        open: bool,
        fail_at: Option<u64>,
        /// Writes completed when each flush was issued.
        flushes: Vec<usize>,
        completed: usize,
    }

    impl Probe {
        fn new(open: bool, fail_at: Option<u64>) -> Arc<Self> {
            Arc::new(Self {
                data: Mutex::new(vec![0; 1 << 20]),
                state: Mutex::new(State {
                    open,
                    fail_at,
                    ..State::default()
                }),
                changed: Condvar::new(),
            })
        }

        /// Waits until `n` writes have entered.
        fn entered(&self, n: usize) {
            let state = self.state.lock().unwrap();
            drop(self.changed.wait_while(state, |s| s.entered < n).unwrap());
        }

        fn open(&self) {
            self.state.lock().unwrap().open = true;
            self.changed.notify_all();
        }
    }

    impl BlockFile for Probe {
        fn alignment(&self) -> Alignment {
            Alignment::new(4096).unwrap()
        }

        fn len(&self) -> Result<u64, DiskError> {
            Ok(self.data.lock().unwrap().len() as u64)
        }

        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
            let at = usize::try_from(offset).unwrap();
            buf.copy_from_slice(&self.data.lock().unwrap()[at..at + buf.len()]);
            Ok(())
        }

        fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
            let mut state = self.state.lock().unwrap();
            state.in_flight += 1;
            state.most = state.most.max(state.in_flight);
            state.entered += 1;
            self.changed.notify_all();
            let state = self.changed.wait_while(state, |s| !s.open).unwrap();
            let fail = state.fail_at == Some(offset);
            drop(state);
            if !fail {
                let at = usize::try_from(offset).unwrap();
                self.data.lock().unwrap()[at..at + buf.len()].copy_from_slice(buf);
            }
            let mut state = self.state.lock().unwrap();
            state.in_flight -= 1;
            state.completed += 1;
            if fail {
                return Err(stopped(Path::new("probe"), "a write the test fails"));
            }
            Ok(())
        }

        fn sync_data(&self) -> Result<(), DiskError> {
            let mut state = self.state.lock().unwrap();
            let completed = state.completed;
            state.flushes.push(completed);
            Ok(())
        }
    }

    /// The byte the `i`th write of a batch is filled with.
    fn fill(i: usize) -> u8 {
        u8::try_from(i + 1).unwrap()
    }

    fn writes(n: usize) -> Vec<(AlignedBuf, u64)> {
        (0..n)
            .map(|i| {
                let mut buf = AlignedBuf::zeroed(4096, Alignment::new(4096).unwrap()).unwrap();
                buf.extend_from_slice(&[fill(i); 4096]).unwrap();
                (buf, (i * 4096) as u64)
            })
            .collect()
    }

    #[test]
    fn the_depth_is_the_smallest_of_queue_and_measurement() {
        assert_eq!(depth(Some(253), Some(64)), 64);
        assert_eq!(depth(Some(16), Some(64)), 16);
        // Unmeasured: one at a time, as unmeasured reads are.
        assert_eq!(depth(Some(253), None), 1);
        // Undescribed: NCQ's 32 bounds the measurement.
        assert_eq!(depth(None, Some(200)), UNDESCRIBED_QUEUE_DEPTH);
        assert_eq!(depth(Some(0), Some(0)), 1);
        assert!(Issuer::start(Path::new("dev"), 0).is_err());
    }

    /// A batch of three times the depth keeps exactly the depth in flight, its buffers come
    /// back in order, and its one flush is issued after every write has completed.
    #[test]
    fn a_batch_keeps_the_depth_and_flushes_after_its_writes() {
        let issuer = Issuer::start(Path::new("dev"), 4).unwrap();
        assert_eq!(issuer.depth(), 4);
        let probe = Probe::new(false, None);
        let mut attached = issuer.attach(&probe).unwrap();
        std::thread::scope(|s| {
            let batch = s.spawn(|| attached.write(writes(12), true));
            probe.entered(4);
            assert_eq!(probe.state.lock().unwrap().in_flight, 4);
            probe.open();
            let back = batch.join().unwrap().unwrap();
            assert_eq!(back.len(), 12);
            for (i, buf) in back.iter().enumerate() {
                assert_eq!(buf.as_slice()[0], fill(i));
            }
        });
        let state = probe.state.lock().unwrap();
        assert_eq!(state.most, 4);
        assert_eq!(state.flushes, vec![12]);
        drop(state);
        let data = probe.data.lock().unwrap();
        for i in 0..12 {
            assert!(data[i * 4096..(i + 1) * 4096].iter().all(|&b| b == fill(i)));
        }
    }

    /// A write that fails fails its batch once every write has ended, and the flush is never
    /// issued.
    #[test]
    fn a_failed_write_fails_its_batch_and_issues_no_flush() {
        let issuer = Issuer::start(Path::new("dev"), 2).unwrap();
        let probe = Probe::new(true, Some(2 * 4096));
        let mut attached = issuer.attach(&probe).unwrap();
        assert!(attached.write(writes(5), true).is_err());
        let state = probe.state.lock().unwrap();
        assert_eq!(state.completed, 5);
        assert!(state.flushes.is_empty());
        drop(state);
        // The next batch is the submitter's to decide on; the issuer serves it.
        attached.flush().unwrap();
        assert_eq!(probe.state.lock().unwrap().flushes, vec![5]);
    }

    /// Several submitters on one device share its workers: the device never has more than the
    /// depth in flight, whatever the number of files attached.
    #[test]
    fn submitters_share_the_depth() {
        let issuer = Issuer::start(Path::new("dev"), 2).unwrap();
        let probe = Probe::new(false, None);
        let mut attached: Vec<Attached> = (0..3).map(|_| issuer.attach(&probe).unwrap()).collect();
        std::thread::scope(|s| {
            let batches: Vec<_> = attached
                .iter_mut()
                .map(|a| s.spawn(move || a.write(writes(4), true)))
                .collect();
            probe.entered(2);
            probe.open();
            for batch in batches {
                assert_eq!(batch.join().unwrap().unwrap().len(), 4);
            }
        });
        let state = probe.state.lock().unwrap();
        assert_eq!(state.most, 2);
        assert_eq!(state.flushes.len(), 3);
    }

    /// Detaching gives the file back before it returns; once the issuer is dropped, a batch
    /// is refused rather than left waiting.
    #[test]
    fn detach_releases_the_file_and_a_stopped_issuer_refuses() {
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(true, None);
        let attached = issuer.attach(&probe).unwrap();
        assert_eq!(Arc::strong_count(&probe), 2);
        drop(attached);
        assert_eq!(Arc::strong_count(&probe), 1);
        let mut attached = issuer.attach(&probe).unwrap();
        drop(issuer);
        assert!(attached.write(writes(1), true).is_err());
        drop(attached);
        assert_eq!(Arc::strong_count(&probe), 1);
    }
}
