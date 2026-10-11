//! The kqueue driver (macOS, BSD): `EVFILT_USER` for kicks and a critical one-shot `EVFILT_TIMER` for a
//! wait's deadline [B: kqueue(2)], through rustix's wrappers and, for the wait (rustix cannot express
//! `NOTE_CRITICAL`), libc's. macOS lets a kevent timeout run later the longer it is, by its timer coalescing;
//! `NOTE_CRITICAL` asks it to "override default power-saving techniques to more strictly respect the leeway
//! value" [B: kqueue(2)], which held a shard's 1 ms deadline to its tick (mantle `docs/design/event-loop.md`
//! D10). macOS has no completion I/O, so later phases drive sockets by readiness through the same `wait`.
//!
//! The queue is created and its kick event registered by [`prepare`] on whatever thread builds
//! the shard's seed (the registry owns the descriptor and pins each foreign borrow);
//! the driver itself, which holds an event buffer of raw kernel records, is built on the shard's
//! own thread by [`KqueueDriver::from_prepared`]. rustix marks `kevent` unsafe because the
//! output buffer is filled by the kernel; the three calls here pass a change list of valid
//! events and a buffer with the capacity the call may fill.
#![allow(unsafe_code)]

use std::os::fd::OwnedFd;

use crate::driver::KickFd;

use rustix::event::kqueue::{
    Event, EventFilter, EventFlags, UserDefinedFlags, UserFlags, kevent, kqueue,
};

use crate::driver::{Completion, Driver, DriverKind, Kick, nanos_since, refused};
use crate::error::RtError;
use crate::interests::Readiness;

/// Format: the identifier of the kick event on the queue.
const KICK_IDENT: isize = 0;
/// Format: the identifier of the deadline timer on the queue.
const TIMER_IDENT: usize = 1;

/// Shape: events drained per wait; a wait that fills the buffer returns and the next wait drains
/// the rest (kqueue keeps them), so the size bounds latency, not correctness.
const EVENTS_PER_WAIT: usize = 64;

/// The driver.
pub struct KqueueDriver {
    kq: KickFd,
    /// The shard-clock reading the driver counts its clock from (`machine::clock::shard_clock_ns`).
    epoch: u64,
    events: Vec<libc::kevent>,
    nops: Vec<u64>,
    /// Whether the deadline timer is registered and has not been seen to fire.
    timer_armed: bool,
}

impl std::fmt::Debug for KqueueDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KqueueDriver")
            .field("events_capacity", &self.events.capacity())
            .finish()
    }
}

/// The deadline timer's change record: `flags` (add a one-shot, or delete) at `ns` nanoseconds from now,
/// critical (see the module doc).
fn timer_change(flags: u16, ns: isize) -> libc::kevent {
    libc::kevent {
        ident: TIMER_IDENT,
        filter: libc::EVFILT_TIMER,
        flags,
        fflags: libc::NOTE_NSECONDS | libc::NOTE_CRITICAL,
        data: ns,
        udata: std::ptr::null_mut(),
    }
}

fn user_event(flags: UserFlags, event_flags: EventFlags) -> Event {
    Event::new(
        EventFilter::User {
            ident: KICK_IDENT,
            flags,
            user_flags: UserDefinedFlags::new(0),
        },
        event_flags,
        std::ptr::null_mut(),
    )
}

/// Creates the queue and registers the kick event; the descriptor is owned by the registry slot
/// that the shard registers into (closed at unregistration, never leaked).
pub fn prepare() -> Result<OwnedFd, RtError> {
    let kq = kqueue().map_err(|e| refused("kqueue", e))?;
    let register = user_event(UserFlags::empty(), EventFlags::ADD | EventFlags::CLEAR);
    let mut none: Vec<Event> = Vec::new();
    // SAFETY: the change list is one valid event; the empty output vector receives nothing.
    unsafe { kevent(&kq, &[register], &mut none, None) }
        .map_err(|e| refused("kevent(EV_ADD EVFILT_USER)", e))?;
    Ok(kq)
}

/// Triggers the kick event on `kq`; safe from any thread. A closed slot (the shard unregistered)
/// makes it a no-op.
pub fn trigger(kq: &KickFd) {
    let _ = kq.with(|kq| {
        let trigger = user_event(UserFlags::TRIGGER, EventFlags::empty());
        let mut none: Vec<Event> = Vec::new();
        // SAFETY: one valid change record on an open queue; a closed queue returns an error we ignore.
        let _ = unsafe { kevent(kq, &[trigger], &mut none, None) };
    });
}

impl KqueueDriver {
    /// Builds the driver over the slot's prepared queue, on the shard's thread.
    pub fn from_prepared(kq: KickFd) -> KqueueDriver {
        KqueueDriver {
            kq,
            epoch: crate::machine::clock::shard_clock_ns(),
            events: Vec::with_capacity(EVENTS_PER_WAIT),
            nops: Vec::new(),
            timer_armed: false,
        }
    }
}

impl Driver for KqueueDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::Kqueue
    }

    fn kick_handle(&self) -> Kick {
        Kick::Kqueue(self.kq)
    }

    fn now_ns(&self) -> u64 {
        nanos_since(self.epoch)
    }

    fn clock(&self) -> crate::driver::Clock {
        crate::driver::Clock::Since(self.epoch)
    }

    fn wait(&mut self, timeout_ns: Option<u64>, out: &mut Vec<Completion>) -> Result<(), RtError> {
        let mut timeout_ns = timeout_ns;
        if !self.nops.is_empty() {
            out.extend(self.nops.drain(..).map(|user_data| Completion {
                user_data,
                result: 0,
            }));
            timeout_ns = Some(0);
        }
        // A zero timeout polls; a deadline (re)arms the critical timer and waits without a timeout; none takes a
        // timer still registered away, so it cannot end a later wait.
        let change = match timeout_ns {
            Some(0) => None,
            Some(ns) => Some(timer_change(
                libc::EV_ADD | libc::EV_ONESHOT,
                isize::try_from(ns).unwrap_or(isize::MAX),
            )),
            None => self.timer_armed.then(|| timer_change(libc::EV_DELETE, 0)),
        };
        let changes: &[libc::kevent] = change.as_slice();
        let poll = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let timeout: *const libc::timespec = if timeout_ns == Some(0) {
            &poll
        } else {
            std::ptr::null()
        };
        let capacity = i32::try_from(self.events.capacity()).unwrap_or(i32::MAX);
        self.events.clear();
        let events = self.events.as_mut_ptr();
        let returned = self
            .kq
            .with(|kq| {
                use std::os::fd::AsRawFd;
                // SAFETY: `changes` is a live slice of initialized records, read for the call's duration;
                // `events` points at this driver's own buffer with room for `capacity` records, which the
                // kernel fills from the front and counts in the return value; `timeout` is null or this
                // frame's own zero timespec, live for the call.
                unsafe {
                    libc::kevent(
                        kq.as_raw_fd(),
                        changes.as_ptr(),
                        i32::try_from(changes.len()).unwrap_or(0),
                        events,
                        capacity,
                        timeout,
                    )
                }
            })
            .ok_or(RtError::DriverLost)?;
        let Ok(filled) = usize::try_from(returned) else {
            let error = std::io::Error::last_os_error();
            return match error.raw_os_error() {
                Some(libc::EINTR) => Ok(()),
                Some(libc::EBADF) => Err(RtError::DriverLost),
                _ => Err(RtError::DriverRefused {
                    call: "kevent",
                    code: error.raw_os_error(),
                }),
            };
        };
        // SAFETY: the kernel initialized the first `filled` records (at most `capacity`, the buffer's room).
        unsafe { self.events.set_len(filled.min(self.events.capacity())) };
        if change.is_some() {
            self.timer_armed = timeout_ns.is_some();
        }
        // Kick events carry nothing, nor does the deadline's; other filters become completions keyed by udata.
        for ev in &self.events {
            if ev.flags & libc::EV_ERROR != 0 {
                // A change the kernel refused: the timer's delete after it fired unseen (ENOENT) is nothing.
                if ev.filter == libc::EVFILT_TIMER && ev.data == libc::ENOENT as isize {
                    self.timer_armed = false;
                    continue;
                }
                return Err(RtError::DriverRefused {
                    call: "kevent(change)",
                    code: i32::try_from(ev.data).ok(),
                });
            }
            let fired = match ev.filter {
                libc::EVFILT_USER => continue,
                libc::EVFILT_TIMER => {
                    self.timer_armed = false;
                    continue;
                }
                libc::EVFILT_WRITE => Readiness::WRITE,
                _ => Readiness::READ,
            };
            out.push(Completion {
                user_data: u64::try_from(ev.udata.addr()).unwrap_or(0),
                result: fired.bits(),
            });
        }
        Ok(())
    }

    fn submit_nop(&mut self, user_data: u64) -> Result<(), RtError> {
        self.nops.push(user_data);
        Ok(())
    }

    fn arm(&mut self, raw: i32, want: Readiness, tag: u64) -> Result<(), RtError> {
        // One filter per direction, each its own registration (ident and filter), so arming one never
        // touches the other; a filter armed earlier and no longer wanted fires once, to no waiter.
        let udata = core::ptr::without_provenance_mut(usize::try_from(tag).unwrap_or(usize::MAX));
        let read = want.contains(Readiness::READ).then(|| {
            Event::new(
                EventFilter::Read(raw),
                EventFlags::ADD | EventFlags::ONESHOT,
                udata,
            )
        });
        let write = want.contains(Readiness::WRITE).then(|| {
            Event::new(
                EventFilter::Write(raw),
                EventFlags::ADD | EventFlags::ONESHOT,
                udata,
            )
        });
        let mut none: Vec<Event> = Vec::new();
        for change in [read, write].into_iter().flatten() {
            self.kq
                .with(|kq| {
                    // SAFETY: one valid change record on the open queue; the output buffer receives nothing.
                    unsafe { kevent(kq, &[change], &mut none, None) }
                })
                .ok_or(RtError::DriverLost)?
                .map_err(|e| refused("kevent(EV_ADD)", e))?;
        }
        Ok(())
    }

    fn has_pending(&self) -> bool {
        !self.nops.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shape: a deadline that is not a whole millisecond, a millisecond and a half.
    const DEADLINE_NS: u64 = 1_500_000;

    /// A wait with a deadline registers the critical timer and is ended by it, never before the deadline; a
    /// kicked wait leaves the timer registered for the next; a wait with no deadline takes it away.
    #[test]
    #[cfg_attr(miri, ignore)] // a kqueue is an OS object Miri does not model
    fn a_deadline_waits_on_the_critical_timer_and_an_unbounded_wait_takes_it_away() {
        let kq = prepare().unwrap();
        let (registration, _control) =
            crate::registry::register(2, 1, crate::runtime::register_kick(Some(kq))).unwrap();
        let shard = registration.shard();
        let kick = crate::registry::with_entry(shard, |entry| entry.kick).unwrap();
        let Kick::Kqueue(fd) = kick else {
            panic!("a kqueue registration kicks through its queue: {kick:?}");
        };
        let mut driver = KqueueDriver::from_prepared(fd);
        let mut out = Vec::new();
        let start = driver.now_ns();
        while driver.now_ns() < start + DEADLINE_NS {
            driver
                .wait(
                    Some((start + DEADLINE_NS).saturating_sub(driver.now_ns())),
                    &mut out,
                )
                .unwrap();
        }
        assert!(!driver.timer_armed, "the timer fired and was seen");
        // A kick ends a wait with a deadline long before it: the timer stays registered.
        kick.kick();
        driver.wait(Some(1_000_000_000), &mut out).unwrap();
        assert!(driver.timer_armed);
        // A kick ends a wait with no deadline: the timer is taken away first.
        kick.kick();
        driver.wait(None, &mut out).unwrap();
        assert!(!driver.timer_armed);
        assert!(
            out.is_empty(),
            "kicks and timers are no completions: {out:?}"
        );
        drop(driver);
        drop(registration);
    }
}
