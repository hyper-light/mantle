//! Logical clients as records on few threads (docs/design/measurement.md §10; research/26 §3.4).
//!
//! A benchmark's client is its state, not a thread: a driver thread holds many, submits for
//! each that is ready, and learns of each answer through the client's waker, which pushes the
//! client's index onto the driver's ready queue. A completion so costs one push and one wake,
//! never a scan of every client, and the process's threads are the drivers whatever the number
//! of clients. Drivers are at most the cores the process is granted.
//!
//! Where clients are independent the load is open: each client's operations arrive at
//! intended start times drawn from a Poisson process of the stated rate, and an operation's
//! latency runs from its intended start, so a slow answer is charged to every operation it
//! delayed (wrk2's correction of coordinated omission, research/26 §3.2). Closed, an
//! operation's intended start is when its client's last answer came.

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::task::{Wake, Waker};
use std::thread::{Builder, Scope, ScopedJoinHandle};
use std::time::{Duration, Instant};

use mantle_disk::histogram::Histogram;
use mantle_disk::measure::SplitMix64;
use mantle_disk::threads::{self, Reservation};

use crate::bench::Error;

/// Driver threads for `clients` clients: the cores the process is granted, as
/// `available_parallelism` reports them with affinity and quotas (docs/design/node.md §1.2),
/// and no more than the clients.
pub fn drivers(clients: usize) -> usize {
    std::thread::available_parallelism()
        .map_or(1, NonZeroUsize::get)
        .min(clients)
        .max(1)
}

/// Threads started together, drawn from the process's thread budget until joined.
pub struct Started<'scope, T> {
    handles: Vec<ScopedJoinHandle<'scope, Option<T>>>,
    _budget: Reservation,
}

impl<T> Started<'_, T> {
    /// Each thread's answer, `None` for one that unwound or never worked.
    pub fn join(self) -> Vec<Option<T>> {
        self.handles
            .into_iter()
            .map(|h| h.join().ok().flatten())
            .collect()
    }
}

/// Starts `n` threads in `scope` for work on `path`, the `i`th running `make(i)`, all or none
/// of them working (audit S10). The threads are drawn from the process's thread budget before
/// any starts; each is handed its work through a channel of its own once all have started, so
/// none waits at a shared latch, and a thread that cannot be started leaves the others to end
/// without working.
pub fn start<'scope, T, F>(
    scope: &'scope Scope<'scope, '_>,
    path: &Path,
    n: usize,
    make: impl FnMut(usize) -> F,
) -> Result<Started<'scope, T>, Error>
where
    F: FnOnce() -> T + Send + 'scope,
    T: Send + 'scope,
{
    let budget = threads::reserve(n, path).map_err(Error::Disk)?;
    let work: Vec<F> = (0..n).map(make).collect();
    let mut handles = Vec::with_capacity(n);
    let mut senders = Vec::with_capacity(n);
    for _ in 0..n {
        let (sender, receiver) = sync_channel::<F>(1);
        let handle = Builder::new()
            .name("mantle-bench".into())
            .spawn_scoped(scope, move || receiver.recv().ok().map(|f| f()))
            // The senders dropped with the error end the threads started, unworked.
            .map_err(Error::Spawn)?;
        handles.push(handle);
        senders.push(sender);
    }
    for (sender, f) in senders.iter().zip(work) {
        if sender.send(f).is_err() {
            return Err(Error::Worker);
        }
    }
    Ok(Started {
        handles,
        _budget: budget,
    })
}

/// A client's waker: pushes the client's index onto its driver's ready queue, which holds one
/// entry per client, so the push never waits. `std` builds a safe `Waker` only from an
/// `Arc<impl Wake>`; the other constructor, a `RawWaker` vtable, is unsafe code outside the
/// OS-interface files CLAUDE.md §7 allows. Each client's is made once.
struct Ready {
    ready: SyncSender<usize>,
    index: usize,
}

impl Wake for Ready {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.ready.try_send(self.index);
    }
}

/// The waker of client `index`, whose driver reads `ready`.
pub fn waker(ready: &SyncSender<usize>, index: usize) -> Waker {
    Waker::from(Arc::new(Ready {
        ready: ready.clone(),
        index,
    }))
}

/// When a client's operations are meant to start: closed, when its last answer came; open,
/// at the arrivals of a Poisson process (research/26 §3.1–§3.2).
#[derive(Debug, Clone)]
pub enum Schedule {
    Closed,
    /// Mean nanoseconds between one client's arrivals, and the draws that space them.
    Open {
        mean_gap: f64,
        rng: SplitMix64,
    },
}

impl Schedule {
    /// Each of `clients` clients' share of `rate` operations a second in all, `None` for a
    /// closed loop; `seed` makes each client's arrivals its own.
    pub fn new(rate: Option<f64>, clients: usize, seed: u64) -> Self {
        match rate.filter(|r| r.is_finite() && *r > 0.0) {
            None => Self::Closed,
            Some(rate) => {
                let clients = u32::try_from(clients).map_or(f64::from(u32::MAX), f64::from);
                Self::Open {
                    mean_gap: 1e9 * clients / rate,
                    rng: SplitMix64::new(seed),
                }
            }
        }
    }

    /// The intended start of the operation after one meant to start at `last` whose answer
    /// came at `answered`.
    pub fn next(&mut self, last: Instant, answered: Instant) -> Instant {
        match self {
            Self::Closed => answered,
            Self::Open { mean_gap, rng } => {
                // An exponential gap by inversion of a uniform draw in [0, 1): 53 random bits
                // over 2^53, the significand's width.
                let u = (rng.next_u64() >> 11) as f64 * (f64::EPSILON / 2.0);
                let gap = -(1.0 - u).ln() * *mean_gap;
                let gap = Duration::try_from_secs_f64(gap / 1e9).unwrap_or(Duration::MAX);
                last.checked_add(gap).unwrap_or(last)
            }
        }
    }
}

/// What a driver says of itself: its CPU time, and in open loop how late it issued operations
/// past when they could start, so a run in which the generator was the bottleneck shows it
/// (docs/design/measurement.md §10).
#[derive(Debug, Clone, Default)]
pub struct Generator {
    pub cpu: Duration,
    /// Nanoseconds from when an operation could start to when it was issued.
    pub lateness: Histogram,
}

impl Generator {
    pub fn merge(&mut self, other: &Generator) {
        self.cpu = self.cpu.saturating_add(other.cpu);
        self.lateness.merge(&other.lateness);
    }
}

/// The calling thread's CPU time, zero where the OS will not say.
pub fn cpu() -> Duration {
    threads::thread_cpu().unwrap_or_default()
}

/// The threads the process runs, as the OS counts them; zero where it will not say.
pub fn thread_count() -> usize {
    threads::count().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Arrivals at a stated rate average the rate's gap; a closed loop starts at the answer.
    #[test]
    fn open_arrivals_keep_their_rate() {
        let t0 = Instant::now();
        assert_eq!(
            Schedule::Closed.next(t0, t0 + Duration::from_secs(1)),
            t0 + Duration::from_secs(1)
        );
        // 1,000 a second over 10 clients: a client's mean gap is 10 ms.
        let mut s = Schedule::new(Some(1000.0), 10, 5);
        let mut at = t0;
        for _ in 0..10_000 {
            at = s.next(at, at);
        }
        let mean = (at - t0).as_secs_f64() / 10_000.0;
        assert!((mean - 0.01).abs() < 0.0005, "mean gap {mean}");
    }

    /// Every thread works once all have started; the budget is given back when they are joined.
    #[test]
    fn started_threads_all_work() {
        let path = Path::new("/");
        let answers = std::thread::scope(|s| start(s, path, 4, |i| move || i * 3).unwrap().join());
        assert_eq!(answers, vec![Some(0), Some(3), Some(6), Some(9)]);
        let past = threads::ceiling().unwrap() + 1;
        let refused = std::thread::scope(|s| start(s, path, past, |_| || ()).map(|_| ()));
        assert!(matches!(
            refused,
            Err(Error::Disk(mantle_disk::DiskError::Threads { .. }))
        ));
    }
}
