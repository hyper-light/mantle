//! The epoll driver (Linux's one driver; io_uring is not carried, docs/runtime.md §3.7): an eventfd for kicks
//! and a timerfd for deadlines registered on an epoll instance [B: epoll(7); B: eventfd(2); B:
//! timerfd_create(2)], through rustix's safe wrappers. A wait for a deadline sets the timerfd to it, on the
//! shard's own clock (`CLOCK_BOOTTIME`), and waits with no timeout: `epoll_wait` counts its timeout in whole
//! milliseconds, and rustix rounds a finer one up, which made a shard's 1 ms timer fire after 2. The timer is
//! set only when the deadline changes (`Driver::wait_until`), so a shard woken before its deadline waits again
//! with no system call for the timer (mantle `docs/design/event-loop.md` D10). The one unsafe idiom is
//! borrowing a caller-owned socket fd by number for a single `epoll_ctl` readiness registration (a
//! `UdpSocket` recv, a `TcpStream` read or write), each with a `// SAFETY:` note.
#![allow(unsafe_code)]

use std::os::fd::OwnedFd;

use rustix::event::epoll::{self, CreateFlags, EventData, EventFlags};

use crate::driver::{Completion, Driver, DriverKind, Kick, nanos_since, refused};
use crate::error::RtError;
use crate::interests::Readiness;

/// Creates the kick eventfd both Linux drivers wake on; the registry owns it until every shard driver has
/// retired.
pub fn prepare_eventfd() -> Result<OwnedFd, RtError> {
    rustix::event::eventfd(
        0,
        rustix::event::EventfdFlags::CLOEXEC | rustix::event::EventfdFlags::NONBLOCK,
    )
    .map_err(|e| refused("eventfd", e))
}

/// Format: the user word that marks the kick eventfd in epoll events.
const KICK_TAG: u64 = u64::MAX;
/// Format: the user word that marks the deadline timerfd in epoll events.
const TIMER_TAG: u64 = u64::MAX - 1;

/// Shape: events drained per wait (see the kqueue driver for the reasoning).
const EVENTS_PER_WAIT: usize = 64;

/// The driver.
pub struct EpollDriver {
    epfd: OwnedFd,
    efd: crate::driver::KickFd,
    /// The deadline timer, on `CLOCK_BOOTTIME`, the shard clock's own (`machine::clock::shard_clock_ns`).
    timer: OwnedFd,
    /// The deadline the timer is set to, on this driver's clock, until it is seen to fire or is cleared.
    timer_deadline: Option<u64>,
    /// The timer's settings, counted for the tests that hold it to one a deadline.
    #[cfg(test)]
    timer_sets: u64,
    /// The shard-clock reading the driver counts its clock from (`machine::clock::shard_clock_ns`).
    epoch: u64,
    events: Vec<epoll::Event>,
    nops: Vec<u64>,
}

impl std::fmt::Debug for EpollDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpollDriver")
            .field("events_capacity", &self.events.capacity())
            .finish()
    }
}

impl EpollDriver {
    /// Creates the instance over its registry-owned eventfd. The owning runtime retires the
    /// descriptor after this driver and all foreign kick borrows have ended.
    pub fn with_eventfd(efd: crate::driver::KickFd) -> Result<EpollDriver, RtError> {
        let epfd = epoll::create(CreateFlags::CLOEXEC).map_err(|e| refused("epoll_create1", e))?;
        efd.with(|fd| epoll::add(&epfd, fd, EventData::new_u64(KICK_TAG), EventFlags::IN))
            .ok_or(RtError::DriverLost)?
            .map_err(|e| refused("epoll_ctl(ADD eventfd)", e))?;
        let timer = rustix::time::timerfd_create(
            rustix::time::TimerfdClockId::Boottime,
            rustix::time::TimerfdFlags::CLOEXEC | rustix::time::TimerfdFlags::NONBLOCK,
        )
        .map_err(|e| refused("timerfd_create", e))?;
        // Edge-triggered and never read: an expiry is one event, and setting the timer again resets its count
        // (timerfd_create(2)), which readies it for the next.
        epoll::add(
            &epfd,
            &timer,
            EventData::new_u64(TIMER_TAG),
            EventFlags::IN | EventFlags::ET,
        )
        .map_err(|e| refused("epoll_ctl(ADD timerfd)", e))?;
        Ok(EpollDriver {
            epfd,
            efd,
            timer,
            timer_deadline: None,
            #[cfg(test)]
            timer_sets: 0,
            epoch: crate::machine::clock::shard_clock_ns(),
            events: Vec::with_capacity(EVENTS_PER_WAIT),
            nops: Vec::new(),
        })
    }

    fn drain_kick(&self) {
        let mut word = [0u8; size_of::<u64>()];
        let _ = self.efd.with(|efd| rustix::io::read(efd, &mut word));
    }

    /// Sets the timer to `deadline_ns` on this driver's clock, or clears it for `None`.
    fn set_timer(&mut self, deadline_ns: Option<u64>) -> Result<(), RtError> {
        // An absolute time of zero clears a timerfd; a deadline is the epoch, a boot-time reading, onwards.
        let at = deadline_ns.map_or(0, |deadline| self.epoch.saturating_add(deadline).max(1));
        let value = rustix::time::Itimerspec {
            it_interval: timespec(0),
            it_value: timespec(at),
        };
        rustix::time::timerfd_settime(
            &self.timer,
            rustix::time::TimerfdTimerFlags::ABSTIME,
            &value,
        )
        .map_err(|e| refused("timerfd_settime", e))?;
        self.timer_deadline = deadline_ns;
        #[cfg(test)]
        {
            self.timer_sets = self.timer_sets.saturating_add(1);
        }
        Ok(())
    }

    /// One `epoll_wait`: a poll when `poll` or when a nop is pending, else a wait for an event (the timer's
    /// among them) with no timeout.
    fn wait_events(&mut self, poll: bool, out: &mut Vec<Completion>) -> Result<(), RtError> {
        let mut timeout = poll.then(|| timespec(0));
        if !self.nops.is_empty() {
            out.extend(self.nops.drain(..).map(|user_data| Completion {
                user_data,
                result: 0,
            }));
            timeout = Some(timespec(0));
        }
        self.events.clear();
        let outcome = epoll::wait(
            &self.epfd,
            rustix::buffer::spare_capacity(&mut self.events),
            timeout.as_ref(),
        );
        if let Err(e) = outcome {
            return match e {
                rustix::io::Errno::INTR => Ok(()),
                rustix::io::Errno::BADF => Err(RtError::DriverLost),
                other => Err(refused("epoll_wait", other)),
            };
        }
        let mut kicked = false;
        for ev in &self.events {
            // The event record is packed: copy the fields out before touching them.
            let (data, flags) = (ev.data, ev.flags);
            if data.u64() == KICK_TAG {
                kicked = true;
            } else if data.u64() == TIMER_TAG {
                self.timer_deadline = None;
            } else {
                // An error or a hang-up is both directions' news: the waiter's call reports it.
                let broken = flags.intersects(EventFlags::ERR | EventFlags::HUP);
                let mut fired = Readiness::NONE;
                if broken || flags.intersects(EventFlags::IN | EventFlags::PRI | EventFlags::RDHUP)
                {
                    fired = fired.union(Readiness::READ);
                }
                if broken || flags.contains(EventFlags::OUT) {
                    fired = fired.union(Readiness::WRITE);
                }
                out.push(Completion {
                    user_data: data.u64(),
                    result: fired.bits(),
                });
            }
        }
        if kicked {
            self.drain_kick();
        }
        Ok(())
    }
}

impl Driver for EpollDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::Epoll
    }

    fn kick_handle(&self) -> Kick {
        Kick::Eventfd(self.efd)
    }

    fn now_ns(&self) -> u64 {
        nanos_since(self.epoch)
    }

    fn clock(&self) -> crate::driver::Clock {
        crate::driver::Clock::Since(self.epoch)
    }

    fn wait(&mut self, timeout_ns: Option<u64>, out: &mut Vec<Completion>) -> Result<(), RtError> {
        match timeout_ns {
            Some(0) => self.wait_events(true, out),
            timeout => {
                let deadline_ns = timeout.map(|ns| self.now_ns().saturating_add(ns));
                self.wait_until(deadline_ns, out)
            }
        }
    }

    fn wait_until(
        &mut self,
        deadline_ns: Option<u64>,
        out: &mut Vec<Completion>,
    ) -> Result<(), RtError> {
        match deadline_ns {
            Some(deadline) if deadline <= self.now_ns() => self.wait_events(true, out),
            Some(deadline) => {
                if self.timer_deadline != Some(deadline) {
                    self.set_timer(Some(deadline))?;
                }
                self.wait_events(false, out)
            }
            None => {
                if self.timer_deadline.is_some() {
                    self.set_timer(None)?;
                }
                self.wait_events(false, out)
            }
        }
    }

    fn submit_nop(&mut self, user_data: u64) -> Result<(), RtError> {
        self.nops.push(user_data);
        Ok(())
    }

    fn arm(&mut self, raw: i32, want: Readiness, tag: u64) -> Result<(), RtError> {
        let mut flags = EventFlags::ONESHOT;
        if want.contains(Readiness::READ) {
            flags |= EventFlags::IN;
        }
        if want.contains(Readiness::WRITE) {
            flags |= EventFlags::OUT;
        }
        self.arm_flags(raw, tag, flags)
    }

    fn has_pending(&self) -> bool {
        !self.nops.is_empty()
    }
}

impl EpollDriver {
    /// Arms one-shot `flags` interest on `raw` under the waker word `user_data`: `EPOLL_CTL_ADD` for a
    /// descriptor this epoll instance has not seen, and `EPOLL_CTL_MOD` for one it has — a one-shot
    /// registration is *disabled* after it fires, not removed, so the descriptor stays in the interest
    /// list and a second `ADD` is refused `EEXIST` (epoll(7): "EPOLLONESHOT … the user must call
    /// epoll_ctl with EPOLL_CTL_MOD to rearm"). Every await re-arms, so a receive loop's second await
    /// — the fleet's serve sockets after their first datagram — is the `MOD`
    /// (docs/bugs/2026-09-14-epoll-readiness-re-add-eexist.md).
    /// One registration of `raw` with `flags`, replacing any earlier one: `MOD` when it is registered, `ADD`
    /// when it is not (a fresh descriptor, or one closed and reused since).
    fn arm_flags(&self, raw: i32, user_data: u64, flags: EventFlags) -> Result<(), RtError> {
        // SAFETY: `raw` is a live socket the caller (a UdpSocket or TcpStream) owns for the registration;
        // the borrow is used only for these epoll_ctl calls and not retained.
        let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(raw) };
        let data = EventData::new_u64(user_data);
        match epoll::modify(&self.epfd, fd, data, flags) {
            Ok(()) => Ok(()),
            Err(rustix::io::Errno::NOENT) => {
                epoll::add(&self.epfd, fd, data, flags).map_err(|e| refused("epoll_ctl(ADD)", e))
            }
            Err(e) => Err(refused("epoll_ctl(MOD)", e)),
        }
    }
}

fn timespec(ns: u64) -> rustix::event::Timespec {
    /// Format: nanoseconds per second.
    const NANOS_PER_SECOND: u64 = 1_000_000_000;
    rustix::event::Timespec {
        tv_sec: i64::try_from(ns / NANOS_PER_SECOND).unwrap_or(i64::MAX),
        tv_nsec: i64::try_from(ns % NANOS_PER_SECOND).unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shape: a deadline that is not a whole millisecond, a millisecond and a half.
    const DEADLINE_NS: u64 = 1_500_000;
    /// Shape: a deadline far beyond any wait here, which a kick ends first.
    const FAR_NS: u64 = 1_000_000_000;

    /// The timer's time left, nanoseconds; 0 when it is clear.
    fn left_ns(driver: &EpollDriver) -> u64 {
        let value = rustix::time::timerfd_gettime(&driver.timer).unwrap();
        u64::try_from(value.it_value.tv_sec).unwrap() * 1_000_000_000
            + u64::try_from(value.it_value.tv_nsec).unwrap()
    }

    fn driver() -> (EpollDriver, Kick, crate::registry::Registration) {
        let efd = prepare_eventfd().unwrap();
        let (registration, _control) =
            crate::registry::register(2, 1, crate::runtime::register_kick(Some(efd))).unwrap();
        let kick = crate::registry::with_entry(registration.shard(), |entry| entry.kick).unwrap();
        let Kick::Eventfd(fd) = kick else {
            panic!("an epoll registration kicks through its eventfd: {kick:?}");
        };
        (EpollDriver::with_eventfd(fd).unwrap(), kick, registration)
    }

    /// A wait until a deadline sets the timerfd to it, once, however many waits a kick ends before it; the
    /// timer ends the wait at the deadline, never before; a new deadline sets it again, and a wait with none
    /// clears it.
    #[test]
    #[cfg_attr(miri, ignore)] // epoll and timerfd are OS objects Miri does not model
    fn a_deadline_sets_the_timerfd_once_and_the_timer_ends_the_wait() {
        let (mut driver, kick, registration) = driver();
        let mut out = Vec::new();
        let far = driver.now_ns() + FAR_NS;
        for _ in 0..3 {
            kick.kick();
            driver.wait_until(Some(far), &mut out).unwrap();
        }
        assert_eq!(
            driver.timer_sets, 1,
            "one setting for three waits on one deadline"
        );
        let left = left_ns(&driver);
        assert!(left > 0 && left <= FAR_NS, "{left}");
        let deadline = driver.now_ns() + DEADLINE_NS;
        while driver.now_ns() < deadline {
            driver.wait_until(Some(deadline), &mut out).unwrap();
        }
        assert_eq!(driver.timer_sets, 2, "a new deadline sets the timer again");
        assert_eq!(driver.timer_deadline, None, "the timer fired and was seen");
        assert_eq!(left_ns(&driver), 0);
        kick.kick();
        driver.wait_until(Some(far), &mut out).unwrap();
        kick.kick();
        driver.wait_until(None, &mut out).unwrap();
        assert_eq!(
            driver.timer_sets, 4,
            "set for the far deadline, then cleared"
        );
        assert_eq!(left_ns(&driver), 0);
        assert!(
            out.is_empty(),
            "kicks and timers are no completions: {out:?}"
        );
        drop(driver);
        drop(registration);
    }
}
