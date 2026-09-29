//! Starting a group of scoped worker threads, all or none (audit S10).
//!
//! `std::thread::Scope::spawn` panics when the operating system cannot make a thread
//! (`std::thread::Scope::spawn`, "Panics"), and a measurement or benchmark asks for hundreds.
//! `Builder::spawn_scoped` returns the error instead, but workers already started would run
//! their full job while the caller could only give up. So every worker waits at a latch until
//! all have started; if one cannot be, the latch lets the others go without working, and the
//! error is returned once the scope joins them.

use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{Builder, Scope, ScopedJoinHandle};

/// Where the workers wait until all have started: `Some(true)` to work, `Some(false)` to
/// return without working.
#[derive(Default)]
struct Latch {
    open: Mutex<Option<bool>>,
    opened: Condvar,
}

impl Latch {
    fn set(&self, work: bool) {
        if let Ok(mut open) = self.open.lock() {
            *open = Some(work);
        }
        self.opened.notify_all();
    }

    /// Waits for the starter's word, which follows the last spawn; a poisoned lock is a no.
    fn wait(&self) -> bool {
        let Ok(mut open) = self.open.lock() else {
            return false;
        };
        loop {
            if let Some(work) = *open {
                return work;
            }
            open = match self.opened.wait(open) {
                Ok(open) => open,
                Err(_) => return false,
            };
        }
    }
}

/// Starts `n` workers in `scope`, the `i`th running what `make(i)` returns once all have
/// started. Each answers `None` if it did not work: only when another could not be started,
/// which is the error returned.
pub fn spawn_all<'scope, T, F>(
    scope: &'scope Scope<'scope, '_>,
    n: usize,
    make: impl FnMut(usize) -> F,
) -> Result<Vec<ScopedJoinHandle<'scope, Option<T>>>, io::Error>
where
    F: FnOnce() -> T + Send + 'scope,
    T: Send + 'scope,
{
    start(scope, n, make, usize::MAX)
}

/// `spawn_all`, refusing to make more than `may_start` threads as the operating system
/// refuses one it cannot make: the seam that tests the refusal.
fn start<'scope, T, F>(
    scope: &'scope Scope<'scope, '_>,
    n: usize,
    mut make: impl FnMut(usize) -> F,
    may_start: usize,
) -> Result<Vec<ScopedJoinHandle<'scope, Option<T>>>, io::Error>
where
    F: FnOnce() -> T + Send + 'scope,
    T: Send + 'scope,
{
    let latch = Arc::new(Latch::default());
    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        let work = make(i);
        let theirs = Arc::clone(&latch);
        let started = if i < may_start {
            Builder::new().spawn_scoped(scope, move || theirs.wait().then(work))
        } else {
            Err(io::Error::from(io::ErrorKind::OutOfMemory))
        };
        match started {
            Ok(handle) => handles.push(handle),
            Err(e) => {
                // The scope joins those started, once they return without working.
                latch.set(false);
                return Err(e);
            }
        }
    }
    latch.set(true);
    Ok(handles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn every_worker_runs_once_all_have_started() {
        let ran = AtomicUsize::new(0);
        let answers: Vec<Option<usize>> = std::thread::scope(|scope| {
            let handles = spawn_all(scope, 8, |i| {
                let ran = &ran;
                move || {
                    ran.fetch_add(1, Ordering::Relaxed);
                    i * 2
                }
            })
            .unwrap();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(ran.load(Ordering::Relaxed), 8);
        assert_eq!(answers, (0..8).map(|i| Some(i * 2)).collect::<Vec<_>>());
    }

    /// When a worker cannot be started, none works: those started return without working,
    /// the scope joins them, and the error is the caller's.
    #[test]
    fn a_worker_refused_stops_the_others_before_they_work() {
        let ran = AtomicUsize::new(0);
        let refused = std::thread::scope(|scope| {
            let started = start(
                scope,
                8,
                |_| {
                    let ran = &ran;
                    move || ran.fetch_add(1, Ordering::Relaxed)
                },
                3,
            );
            started.err().map(|e| e.kind())
        });
        assert_eq!(refused, Some(io::ErrorKind::OutOfMemory));
        // The scope has joined the three started; none worked.
        assert_eq!(ran.load(Ordering::Relaxed), 0);
    }

    /// A latch told no lets the workers go without working.
    #[test]
    fn a_latch_told_no_lets_its_workers_go() {
        let latch = Arc::new(Latch::default());
        let waiting: Vec<_> = (0..4)
            .map(|_| {
                let theirs = Arc::clone(&latch);
                std::thread::spawn(move || theirs.wait())
            })
            .collect();
        latch.set(false);
        for w in waiting {
            assert!(!w.join().unwrap());
        }
    }
}
