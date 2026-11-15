//! A device issuer's lanes on Linux ([`super::Issuer::start_device`]): the device's depth of
//! transfers held by the kernel, through one native AIO context of that depth, issued and reaped
//! on the issuer's own thread, whose completions ring the same bell its submitters ring, so one
//! wait takes both and no thread stands between the dispatcher and the device (hyper-raft
//! `docs/research/issuer-completions.md`; the owner's decision of 2026-10-01: native AIO on Linux,
//! not io_uring).
//!
//! Each lane stands where a pool's worker stands ([`super::work`]) for its transfers: it holds one
//! task at a time, which it reports once, so the dispatcher's rules are the pool's, one lane a
//! worker, and the transfers in the kernel are at most the lanes, the context's size. A transfer is
//! handed to the kernel at the next [`Kernel::pump`], up to the context's room, and reported when its
//! completion is reaped. The files are the issuer's thread's alone, so the lanes share one
//! duplicate of each file attached, where a pool's workers, each a thread, hold one each: a deep
//! device's issuer holds a descriptor a file, not one a transfer in flight. A file detached is
//! retired once no transfer of it is out, and a transfer asked of it after its detach is refused,
//! as a pool's worker refuses one once it has dropped its duplicate.
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

use super::{
    FileSlot, Finished, Handle, LifecycleFailure, Op, Report, Task, Transfer, place, retire_file,
};
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
    /// The files attached, by attachment slot: one duplicate each, every lane's.
    files: Vec<FileSlot>,
    /// Attachments being detached, whose file is retired once no transfer of theirs is out: at
    /// most the attachments.
    detaching: Vec<(usize, u64)>,
    /// Files retired since the dispatcher last took them, each with its retirement's failure: what
    /// it answers their detaches with ([`Self::next_retired`]).
    retired: VecDeque<(usize, u64, Option<LifecycleFailure>)>,
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
            files: Vec::new(),
            detaching: Vec::new(),
            retired: VecDeque::new(),
            out,
            waiting,
            in_kernel: 0,
            ready,
            path: path.to_path_buf(),
        })
    }

    /// The lanes: the transfers the kernel holds at most.
    pub(super) fn lanes(&self) -> usize {
        self.out.len()
    }

    /// A descriptor of the bell for a submitter, which rings it once its batch is handed over.
    pub(super) fn bell(&self) -> std::io::Result<Bell> {
        self.bell.try_clone()
    }

    /// Takes the one duplicate of a file attached in `slot`, every lane's, its attachment's
    /// `generation`: the failure of placing it or of retiring what the slot held, if either fails.
    pub(super) fn attach(
        &mut self,
        slot: usize,
        generation: u64,
        file: Handle,
    ) -> Option<LifecycleFailure> {
        match place(&mut self.files, slot, Some((generation, file))) {
            Ok(old) => retire_file(old),
            Err((unplaced, failed)) => {
                let _ = retire_file(unplaced);
                Some(failed)
            }
        }
    }

    /// Detaches the file of `slot`'s attachment of `generation`: retired at once, its retirement's
    /// failure given (`Some`), where no transfer of it is out; else once its last is reported, then
    /// taken by [`Self::next_retired`] (`None` here). Its transfers asked after this are refused.
    pub(super) fn detach(
        &mut self,
        slot: usize,
        generation: u64,
    ) -> Option<Option<LifecycleFailure>> {
        if self.holds_out(slot, generation) {
            self.detaching.push((slot, generation));
            return None;
        }
        Some(self.retire(slot, generation))
    }

    /// Whether a lane holds a transfer of `slot`'s attachment of `generation`.
    fn holds_out(&self, slot: usize, generation: u64) -> bool {
        self.out
            .iter()
            .flatten()
            .any(|out| out.transfer.slot == slot && out.transfer.generation == generation)
    }

    /// Retires the file `slot` holds for `generation`, if it does: its retirement's failure.
    fn retire(&mut self, slot: usize, generation: u64) -> Option<LifecycleFailure> {
        let old = self.files.get_mut(slot).and_then(|held| {
            if held.as_ref().is_some_and(|(g, _)| *g == generation) {
                held.take()
            } else {
                None
            }
        });
        retire_file(old)
    }

    /// A file a deferred detach retired, and its retirement's failure.
    pub(super) fn next_retired(&mut self) -> Option<(usize, u64, Option<LifecycleFailure>)> {
        self.retired.pop_front()
    }

    /// Hands transfer `task` to `lane`, which holds none: it goes to the kernel at the next
    /// [`Self::pump`]. The task comes back for a lane there is no such lane of, one that holds a
    /// transfer, or a task the lanes never take: a file's news, which [`Self::attach`] and
    /// [`Self::detach`] take instead.
    pub(super) fn send(&mut self, lane: usize, task: Task) -> Result<(), Task> {
        if lane >= self.lanes() || self.out.get(lane).is_some_and(Option::is_some) {
            return Err(task);
        }
        match task {
            Task::Transfer(transfer) => {
                self.issue(lane, transfer);
                Ok(())
            }
            task @ (Task::Attach { .. }
            | Task::Detach { .. }
            | Task::Retired
            | Task::RetirementPanicked(_)) => Err(task),
        }
    }

    /// Checks `transfer` as the file's own call would before any system call, and queues it for
    /// the kernel; reports it at once where it is refused, or moves no bytes.
    fn issue(&mut self, lane: usize, transfer: Transfer) {
        let detached = self
            .detaching
            .contains(&(transfer.slot, transfer.generation));
        let checked = match self.kernel_file(&transfer).and_then(|held| {
            if detached {
                Err(super::stopped(
                    Path::new(""),
                    "a transfer for a file the worker does not hold",
                ))
            } else {
                Ok(held)
            }
        }) {
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

    /// The open file, alignment and path of `transfer`'s file. A file being detached is held until
    /// the transfers its lanes took before the detach are reported; [`Self::issue`] takes no new one.
    fn kernel_file(
        &self,
        transfer: &Transfer,
    ) -> Result<(std::os::fd::RawFd, crate::buf::Alignment, &Path), DiskError> {
        let held = self
            .files
            .get(transfer.slot)
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
        let (fd, _, _) = self.kernel_file(transfer).ok()?;
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
                    .get(out.transfer.slot)
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
        // The last transfer out of an attachment being detached: its file is retired now.
        if let Some(at) = self
            .detaching
            .iter()
            .position(|&held| held == (slot, generation))
            && !self.holds_out(slot, generation)
        {
            self.detaching.swap_remove(at);
            let failed = self.retire(slot, generation);
            self.retired.push_back((slot, generation, failed));
        }
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

    /// Retires every file the lanes hold, as a pool's worker does its duplicates at its end: the
    /// first failure.
    pub(super) fn retire_files(&mut self) -> Option<LifecycleFailure> {
        let mut first = None;
        while let Some(file) = self.files.pop() {
            if let Some(failed) = retire_file(file) {
                first.get_or_insert(failed);
            }
        }
        first
    }
}
