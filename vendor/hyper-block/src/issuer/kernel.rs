//! A device issuer's lanes on Linux ([`super::Issuer::start_device`]): the device's depth of
//! transfers held by the kernel, through one native AIO context of that depth, issued and reaped
//! on the issuer's own thread, whose completions ring the same bell its submitters ring, so one
//! wait takes both and no thread stands between the dispatcher and the device (hyper-raft
//! `docs/research/issuer-completions.md`; the owner's decision of 2026-10-01: native AIO on Linux,
//! not io_uring).
//!
//! Each lane stands where a pool's worker stands ([`super::work`]): it holds its own duplicate of
//! each file attached, is told a file's attach or detach before any transfer for it, and holds
//! one task at a time, which it reports once. So the dispatcher's rules are the pool's rules, one
//! lane a worker: a detached file's duplicates are each dropped only once no transfer of it is in
//! that lane, and the transfers in the kernel are at most the lanes, the context's size. A lane's
//! news is applied at once and reported at once; a transfer is handed to the kernel at the next
//! [`Kernel::pump`], up to the context's room, and reported when its completion is reaped.
//!
//! A transfer is the kernel's `pread`, `pwrite` or `fdatasync` of the lane's duplicate
//! ([`crate::block::BlockFile::kernel_file`]), with the bytes and errors the file's own
//! `read_exact_at`, `write_all_at` and `sync_data` give: the same alignment checks before
//! anything is issued, a read or write the kernel moves short of its length issued again for the
//! rest, a read that moves nothing past the file's end a short read, a write that moves nothing a
//! zero write. A flush the kernel refuses as an opcode (`IOCB_CMD_FDSYNC`, Linux 4.18) is made in
//! place from then on, on the issuer's thread. Over a buffered file the kernel makes a transfer
//! inside io_submit (Didona et al., SYSTOR '22, §2), so there the lanes run one at a time, on that
//! thread: correct, never in parallel; a volume opens its file for direct I/O wherever the file
//! system takes it.

use std::collections::VecDeque;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use super::{FileSlot, Finished, LifecycleFailure, Op, Report, Task, Transfer, place, retire_file};
use crate::DiskError;
use crate::aio::linux::{Bell, Context, Op as Record, Pushed};

/// One lane's transfer, from its hand-over to its report: what it moved so far.
struct Out {
    transfer: Transfer,
    /// Bytes the kernel has moved of it.
    done: usize,
}

/// The lanes: the context, its bell, each lane's files and transfer, and what is reported.
pub(super) struct Kernel {
    /// Declared first, so dropped first: destroying the context waits for every transfer the
    /// kernel holds (io_destroy(2)), before the lanes' buffers and files are dropped.
    ctx: Context,
    bell: Bell,
    /// Each lane's files, by attachment slot, as a pool's worker holds its own.
    files: Vec<Vec<FileSlot>>,
    /// Each lane's transfer, handed over and not yet reported: at most one a lane.
    out: Vec<Option<Out>>,
    /// Lanes whose transfer waits for the kernel, in the order handed over: at most the lanes.
    waiting: VecDeque<usize>,
    /// Records the kernel holds now: at most the context's size, the lanes.
    in_kernel: usize,
    /// Reports made on this thread, without a completion: a lane's news, a transfer refused
    /// before it was issued or that needed no system call, a flush made in place. At most one a
    /// lane, as a lane takes its next task only once its last is reported.
    ready: VecDeque<(usize, Report)>,
    path: PathBuf,
}

impl Kernel {
    /// `lanes` lanes for the device at `path`: a context of that size (io_setup(2), refused with
    /// `EAGAIN` past `/proc/sys/fs/aio-max-nr`) and the bell its completions ring (eventfd(2)).
    pub(super) fn new(path: &Path, lanes: usize) -> Result<Self, DiskError> {
        let io = |op: &'static str| {
            let path = path.to_path_buf();
            move |source| DiskError::Io { op, path, source }
        };
        let bell = Bell::new().map_err(io("make a device issuer's bell"))?;
        let ctx = Context::rung(lanes, &bell).map_err(io("io_setup"))?;
        let mut files = Vec::new();
        files
            .try_reserve_exact(lanes)
            .map_err(|e| io("reserve the issuer's lanes")(std::io::Error::other(e)))?;
        files.resize_with(lanes, Vec::new);
        let mut out = Vec::new();
        out.try_reserve_exact(lanes)
            .map_err(|e| io("reserve the issuer's lanes")(std::io::Error::other(e)))?;
        out.resize_with(lanes, || None);
        let mut waiting = VecDeque::new();
        waiting
            .try_reserve_exact(lanes)
            .map_err(|e| io("reserve the issuer's lanes")(std::io::Error::other(e)))?;
        let mut ready = VecDeque::new();
        ready
            .try_reserve_exact(lanes)
            .map_err(|e| io("reserve the issuer's lanes")(std::io::Error::other(e)))?;
        Ok(Self {
            ctx,
            bell,
            files,
            out,
            waiting,
            in_kernel: 0,
            ready,
            path: path.to_path_buf(),
        })
    }

    /// The lanes: the transfers the kernel holds at most.
    pub(super) fn lanes(&self) -> usize {
        self.files.len()
    }

    /// A descriptor of the bell for a submitter, which rings it once its batch is handed over.
    pub(super) fn bell(&self) -> std::io::Result<Bell> {
        self.bell.try_clone()
    }

    /// Hands `task` to `lane`, which holds none: news is applied and reported at once; a transfer
    /// goes to the kernel at the next [`Self::pump`]. The task comes back for a lane there is no
    /// such lane of, or one the lanes never take (a retired news slot).
    pub(super) fn send(&mut self, lane: usize, task: Task) -> Result<(), Task> {
        if lane >= self.lanes() || self.out.get(lane).is_some_and(Option::is_some) {
            return Err(task);
        }
        match task {
            Task::Attach {
                slot,
                generation,
                file,
            } => {
                let Some(files) = self.files.get_mut(lane) else {
                    return Err(Task::Attach {
                        slot,
                        generation,
                        file,
                    });
                };
                let failed = match place(files, slot, Some((generation, file))) {
                    Ok(old) => retire_file(old),
                    Err((unplaced, failed)) => {
                        let _ = retire_file(unplaced);
                        Some(failed)
                    }
                };
                self.report(
                    lane,
                    Report::News {
                        slot,
                        generation,
                        detached: false,
                        failed,
                    },
                );
            }
            Task::Detach { slot, generation } => {
                let old = self
                    .files
                    .get_mut(lane)
                    .and_then(|files| files.get_mut(slot))
                    .and_then(|held| {
                        if held.as_ref().is_some_and(|(g, _)| *g == generation) {
                            held.take()
                        } else {
                            None
                        }
                    });
                let failed = retire_file(old);
                self.report(
                    lane,
                    Report::News {
                        slot,
                        generation,
                        detached: true,
                        failed,
                    },
                );
            }
            Task::Transfer(transfer) => self.issue(lane, transfer),
            task @ (Task::Retired | Task::RetirementPanicked(_)) => return Err(task),
        }
        Ok(())
    }

    /// Checks `transfer` as the file's own call would before any system call, and queues it for
    /// the kernel; reports it at once where it is refused, or moves no bytes.
    fn issue(&mut self, lane: usize, transfer: Transfer) {
        let checked = match self.kernel_file(lane, &transfer) {
            Err(e) => Err(e),
            Ok((_, align, path)) => match &transfer.op {
                Op::Read { buf, at, .. } | Op::Write { buf, at, .. } => {
                    let len = buf.len();
                    let addr = buf.as_slice().as_ptr().addr();
                    if !align.is_aligned_u64(*at)
                        || !align.is_aligned(len)
                        || (len > 0 && !align.is_aligned(addr))
                    {
                        Err(DiskError::Misaligned {
                            offset: *at,
                            len,
                            align: align.get(),
                        })
                    } else if len == 0 {
                        // No bytes reach no system call, as the file's own call makes none.
                        Ok(false)
                    } else if u64::try_from(len)
                        .ok()
                        .and_then(|len| at.checked_add(len))
                        .is_none_or(|end| i64::try_from(end).is_err())
                    {
                        Err(DiskError::Io {
                            op: "offset",
                            path: path.to_path_buf(),
                            source: std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                format!("transfer at {at} passes the largest file offset"),
                            ),
                        })
                    } else {
                        Ok(true)
                    }
                }
                Op::Flush => Ok(true),
            },
        };
        match checked {
            Ok(true) => {
                if let Some(slot) = self.out.get_mut(lane) {
                    *slot = Some(Out { transfer, done: 0 });
                    self.waiting.push_back(lane);
                }
            }
            Ok(false) => self.finish(lane, transfer, Ok(())),
            Err(e) => self.finish(lane, transfer, Err(e)),
        }
    }

    /// The open file, alignment and path of `transfer`'s file in `lane`'s hold.
    fn kernel_file(
        &self,
        lane: usize,
        transfer: &Transfer,
    ) -> Result<(std::os::fd::RawFd, crate::buf::Alignment, &Path), DiskError> {
        let held = self
            .files
            .get(lane)
            .and_then(|files| files.get(transfer.slot))
            .and_then(Option::as_ref)
            .filter(|(g, _)| *g == transfer.generation)
            .and_then(|(_, handle)| handle.0.as_deref());
        let Some(file) = held else {
            return Err(super::stopped(
                Path::new(""),
                "a transfer for a file the worker does not hold",
            ));
        };
        let Some(kernel) = file.kernel_file() else {
            return Err(DiskError::Unsupported {
                path: self.path.clone(),
                reason: "a file the kernel does not hold whole, on an issuer whose transfers the kernel makes",
            });
        };
        Ok((kernel.file.as_raw_fd(), kernel.align, kernel.path))
    }

    /// Hands the waiting transfers to the kernel up to its room, in order; what it does not take
    /// waits at the front. A record io_submit refuses fails its own transfer and no other; a
    /// flush the kernel takes no AIO for is made in place, here.
    pub(super) fn pump(&mut self) {
        loop {
            let room = self.ctx.room().saturating_sub(self.in_kernel);
            if room == 0 || self.waiting.is_empty() {
                return;
            }
            self.ctx.clear();
            let mut pushed = 0usize;
            while pushed < room {
                let Some(lane) = self.waiting.pop_front() else {
                    break;
                };
                match self.record(lane) {
                    Some((fd, record)) => {
                        let tag = u64::try_from(lane).unwrap_or(u64::MAX);
                        match self.ctx.push_on(fd, tag, record) {
                            Pushed::Added => pushed = pushed.saturating_add(1),
                            Pushed::Full => {
                                self.waiting.push_front(lane);
                                break;
                            }
                            Pushed::Unrepresentable => self.fail(
                                lane,
                                DiskError::Unsupported {
                                    path: self.path.clone(),
                                    reason: "a transfer an iocb cannot carry",
                                },
                            ),
                        }
                    }
                    None => self.in_place(lane),
                }
            }
            let flushes = self.ctx.flushes();
            match self.ctx.submit_own() {
                Ok(taken) => {
                    self.in_kernel = self.in_kernel.saturating_add(taken);
                    self.requeue(taken, pushed);
                    // None taken because the first was a flush the kernel turned out to take no
                    // AIO for: the rest go again, that flush now made in place.
                    if taken == 0 && self.ctx.flushes() == flushes {
                        return;
                    }
                }
                Err(source) => {
                    // io_submit took none, and refused the first: it fails; the rest go again.
                    self.requeue(1, pushed);
                    if let Some(lane) = self.pushed_lane(0) {
                        let op = match self.out.get(lane).and_then(Option::as_ref) {
                            Some(Out {
                                transfer:
                                    Transfer {
                                        op: Op::Read { .. },
                                        ..
                                    },
                                ..
                            }) => "read",
                            Some(Out {
                                transfer:
                                    Transfer {
                                        op: Op::Write { .. },
                                        ..
                                    },
                                ..
                            }) => "write",
                            _ => "sync_data",
                        };
                        let path = self.path.clone();
                        self.fail(lane, DiskError::Io { op, path, source });
                    }
                }
            }
        }
    }

    /// The submission's records `from..to`, which the kernel did not take, back at the front of
    /// the queue, in their order.
    fn requeue(&mut self, from: usize, to: usize) {
        let mut at = to;
        while at > from {
            at = at.saturating_sub(1);
            if let Some(lane) = self.pushed_lane(at) {
                self.waiting.push_front(lane);
            }
        }
    }

    fn pushed_lane(&self, at: usize) -> Option<usize> {
        self.ctx
            .pushed(at)
            .and_then(|tag| usize::try_from(tag).ok())
    }

    /// The record `lane`'s transfer is issued as, from what it has moved so far, and its file;
    /// none for a flush the kernel takes no AIO for, which is made in place.
    fn record(&mut self, lane: usize) -> Option<(std::os::fd::RawFd, Record)> {
        let out = self.out.get_mut(lane)?.as_mut()?;
        let done = out.done;
        let record = match &mut out.transfer.op {
            Op::Read { buf, at, .. } => {
                let bytes = buf.as_mut_slice().get_mut(done..)?;
                Record::Read {
                    addr: bytes.as_mut_ptr().addr(),
                    len: bytes.len(),
                    offset: at.checked_add(u64::try_from(done).ok()?)?,
                }
            }
            Op::Write { buf, at, .. } => {
                let bytes = buf.as_slice().get(done..)?;
                Record::Write {
                    addr: bytes.as_ptr().addr(),
                    len: bytes.len(),
                    offset: at.checked_add(u64::try_from(done).ok()?)?,
                }
            }
            Op::Flush if self.ctx.flushes() => Record::Flush,
            Op::Flush => return None,
        };
        // The buffer's heap bytes stay where they are until the record completes: the lane's
        // transfer is reported only once the kernel no longer holds it, and the context is dropped
        // (and waits for every record) before the lanes' transfers are.
        let transfer = &self.out.get(lane)?.as_ref()?.transfer;
        let (fd, _, _) = self.kernel_file(lane, transfer).ok()?;
        Some((fd, record))
    }

    /// Makes `lane`'s flush in place, the kernel having refused AIO flushes, or fails a transfer
    /// whose file left its lane.
    fn in_place(&mut self, lane: usize) {
        let Some(out) = self.out.get_mut(lane).and_then(Option::take) else {
            return;
        };
        let result = match &out.transfer.op {
            Op::Flush => {
                let held = self
                    .files
                    .get(lane)
                    .and_then(|files| files.get(out.transfer.slot))
                    .and_then(Option::as_ref)
                    .filter(|(g, _)| *g == out.transfer.generation);
                match held {
                    Some((_, handle)) => crate::unwind::contained(|| handle.sync_data())
                        .unwrap_or_else(|| Err(super::unwound())),
                    None => Err(super::stopped(
                        Path::new(""),
                        "a transfer for a file the worker does not hold",
                    )),
                }
            }
            Op::Read { .. } | Op::Write { .. } => Err(super::stopped(
                Path::new(""),
                "a transfer for a file the worker does not hold",
            )),
        };
        self.finish(lane, out.transfer, result);
    }

    /// Fails `lane`'s transfer with `error`, its buffer given back.
    fn fail(&mut self, lane: usize, error: DiskError) {
        if let Some(out) = self.out.get_mut(lane).and_then(Option::take) {
            self.finish(lane, out.transfer, Err(error));
        }
    }

    /// Takes the completions the kernel has, without waiting for one, and settles each: a
    /// transfer moved whole, failed or past its file's end is reported; one moved short goes
    /// again for the rest.
    pub(super) fn reap(&mut self) -> Result<(), DiskError> {
        if self.in_kernel == 0 {
            return Ok(());
        }
        self.reap_with(false)
    }

    /// Waits for every record the kernel holds to complete and settles each, as the issuer's
    /// thread ends: their buffers are then the lanes' again, and their reports are taken.
    pub(super) fn drain(&mut self) -> Result<(), DiskError> {
        while self.in_kernel > 0 {
            self.reap_with(true)?;
        }
        // What went short and was queued again is not issued: it fails.
        while let Some(lane) = self.waiting.pop_front() {
            self.fail(
                lane,
                super::stopped(&self.path, "the device's issuer is stopping"),
            );
        }
        Ok(())
    }

    fn reap_with(&mut self, wait: bool) -> Result<(), DiskError> {
        let got = self
            .ctx
            .reap(wait && self.in_kernel > 0)
            .map_err(|source| DiskError::Io {
                op: "io_getevents",
                path: self.path.clone(),
                source,
            })?;
        for at in 0..got {
            let Some(event) = self.ctx.event(at) else {
                break;
            };
            self.in_kernel = self.in_kernel.saturating_sub(1);
            if let Ok(lane) = usize::try_from(event.data) {
                self.complete(lane, event.res);
            }
        }
        Ok(())
    }

    /// Settles `lane`'s record, which ended with `res`: bytes moved (none for a flush), or a
    /// negated errno.
    fn complete(&mut self, lane: usize, res: i64) {
        let Some(out) = self.out.get_mut(lane).and_then(Option::as_mut) else {
            return;
        };
        let errno = || {
            std::io::Error::from_raw_os_error(
                i32::try_from(res.saturating_neg()).unwrap_or(i32::MAX),
            )
        };
        let (len, at, op) = match &out.transfer.op {
            Op::Read { buf, at, .. } => (buf.len(), *at, "read"),
            Op::Write { buf, at, .. } => (buf.len(), *at, "write"),
            Op::Flush => {
                let result = if res == 0 {
                    Ok(())
                } else {
                    Err(self.io_error("sync_data", errno()))
                };
                self.settle(lane, result);
                return;
            }
        };
        let Ok(moved) = usize::try_from(res) else {
            let error = self.io_error(op, errno());
            self.settle(lane, Err(error));
            return;
        };
        out.done = out.done.saturating_add(moved);
        let done = out.done;
        if done >= len {
            self.settle(lane, Ok(()));
        } else if moved == 0 {
            let error = if op == "read" {
                DiskError::ShortRead {
                    path: self.path.clone(),
                    offset: at,
                    missing: len.saturating_sub(done),
                }
            } else {
                self.io_error("write", std::io::ErrorKind::WriteZero.into())
            };
            self.settle(lane, Err(error));
        } else {
            // Moved short: the rest goes again, as the file's own call loops.
            self.waiting.push_back(lane);
        }
    }

    fn settle(&mut self, lane: usize, result: Result<(), DiskError>) {
        if let Some(out) = self.out.get_mut(lane).and_then(Option::take) {
            self.finish(lane, out.transfer, result);
        }
    }

    fn io_error(&self, op: &'static str, source: std::io::Error) -> DiskError {
        DiskError::Io {
            op,
            path: self.path.clone(),
            source,
        }
    }

    /// Reports `transfer`'s end with its buffer, as a pool's worker reports one
    /// (`super::carry_out`).
    fn finish(&mut self, lane: usize, transfer: Transfer, result: Result<(), DiskError>) {
        let Transfer {
            slot,
            generation,
            number,
            op,
        } = transfer;
        let write = match op {
            Op::Write { index, buf, .. } | Op::Read { index, buf, .. } => Some((index, Some(buf))),
            Op::Flush => None,
        };
        self.report(
            lane,
            Report::Finished(Finished {
                slot,
                generation,
                number,
                write,
                result,
            }),
        );
    }

    fn report(&mut self, lane: usize, report: Report) {
        self.ready.push_back((lane, report));
    }

    /// The next report made, its lane's.
    pub(super) fn next_report(&mut self) -> Option<(usize, Report)> {
        self.ready.pop_front()
    }

    /// Whether a report waits to be taken.
    pub(super) fn reported(&self) -> bool {
        !self.ready.is_empty()
    }

    /// Waits until the bell rings: a completion, or a submitter's hand-over. A ring that came
    /// since the last wait ends it at once.
    pub(super) fn wait(&self) -> Result<(), DiskError> {
        self.bell
            .wait()
            .map(|_| ())
            .map_err(|source| DiskError::Io {
                op: "wait on a device issuer's bell",
                path: self.path.clone(),
                source,
            })
    }

    /// Retires every lane's duplicates, as a pool's worker does at its end: the first failure.
    pub(super) fn retire_files(&mut self) -> Option<LifecycleFailure> {
        let mut first = None;
        for files in &mut self.files {
            while let Some(file) = files.pop() {
                if let Some(failed) = retire_file(file) {
                    first.get_or_insert(failed);
                }
            }
        }
        first
    }
}
