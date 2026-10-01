//! Room in the log's queue, and the submitters waiting for it (docs/design/raft-log.md §3;
//! docs/design/node.md §1.3).
//!
//! A submission holds its room until the writer answers it. A submitter that may not be refused
//! waits for room in a slot of its own, in arrival order: room the writer frees is handed to the
//! waiters it fits, in order, and only those are woken, each by its own thread's `unpark`. A
//! fence completes every waiter's slot once. Room was once a condition variable broadcast to
//! every waiter on every answer; on macOS that broadcast walks every waiter inside one kernel
//! spinlock, and thousands of waiters held it until the kernel panicked (research/26 §1.3–§1.4).
//!
//! A waiter whose own group already holds its [`GROUP_SUBMISSIONS`] waits for its group's room
//! and is passed over by room it could not use, so a hot group never takes the others' room.
//! A waiter that waits for the queue's room is never passed: room goes to it before any later
//! arrival, so none starves.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Mutex;
use std::thread::Thread;

use crate::LogError;

/// Submissions one group may have unanswered: the most its replica sends at once, the part of
/// its ready in flight and a compaction, each of which waits for its answer before the next
/// (docs/design/replica.md §3–§4); the writes a replica makes as it opens come before any
/// ready. A caller that sends more waits for its own room, and never takes another group's.
pub(crate) const GROUP_SUBMISSIONS: usize = 2;

/// What the queue admits.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub submissions: usize,
    pub bytes: u64,
    /// Waiters the list holds: a group waits with at most its own [`GROUP_SUBMISSIONS`], and
    /// the log holds at most `max_groups` (docs/design/raft-log.md §3). Past it, `Busy`.
    pub waiters: usize,
}

/// How a waiter's slot was completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    Admitted,
    Fenced,
}

struct Waiter {
    group: u128,
    bytes: u64,
    thread: Thread,
    answer: Option<Answer>,
}

#[derive(Default)]
struct Group {
    /// Submissions admitted and not yet answered.
    admitted: usize,
    /// The group's waiters, oldest first; at most [`GROUP_SUBMISSIONS`].
    waiting: VecDeque<u64>,
}

impl Group {
    /// Waiters of the group its own room lets wait for the queue's room: the oldest of them.
    fn eligible(&self) -> usize {
        self.waiting
            .len()
            .min(GROUP_SUBMISSIONS.saturating_sub(self.admitted))
    }
}

#[derive(Default)]
struct State {
    submissions: usize,
    bytes: u64,
    /// Groups with room held or a waiter: bounded by the queue's submissions and the waiters.
    groups: HashMap<u128, Group>,
    /// Every waiter's slot, by arrival number.
    waiters: HashMap<u64, Waiter>,
    /// Waiters whose group has room, in arrival order: the next room goes to the first.
    eligible: BTreeSet<u64>,
    next: u64,
    fenced: bool,
    /// Wakes given, counted for the tests that bound them.
    #[cfg(test)]
    wakes: u64,
}

impl State {
    fn fits(&self, limits: &Limits, bytes: u64) -> bool {
        self.submissions < limits.submissions
            && self
                .bytes
                .checked_add(bytes)
                .is_some_and(|total| total <= limits.bytes)
    }

    fn hold(&mut self, group: u128, bytes: u64) -> Result<(), LogError> {
        let submissions = self.submissions.checked_add(1).ok_or(LogError::Busy)?;
        let total = self.bytes.checked_add(bytes).ok_or(LogError::Busy)?;
        let g = self.groups.entry(group).or_default();
        g.admitted = g.admitted.checked_add(1).ok_or(LogError::Busy)?;
        self.submissions = submissions;
        self.bytes = total;
        Ok(())
    }

    fn wake(&mut self, thread: &Thread) {
        thread.unpark();
        #[cfg(test)]
        {
            self.wakes = self.wakes.saturating_add(1);
        }
    }

    /// Hands the room there is to the eligible waiters in arrival order, waking each admitted
    /// once. Stops at the first that does not fit, which keeps its turn.
    fn admit(&mut self, limits: &Limits) {
        while let Some(&seq) = self.eligible.first() {
            let Some((group, bytes)) = self.waiters.get(&seq).map(|w| (w.group, w.bytes)) else {
                self.eligible.remove(&seq);
                continue;
            };
            if !self.fits(limits, bytes) {
                break;
            }
            self.eligible.remove(&seq);
            let Some(waiter) = self.waiters.get_mut(&seq) else {
                continue;
            };
            waiter.answer = Some(Answer::Admitted);
            let thread = waiter.thread.clone();
            // The admitted waiter is its group's oldest: eligibility runs in arrival order
            // within a group, so the rest of the group's eligible set is unchanged.
            if let Some(g) = self.groups.get_mut(&group)
                && g.waiting.front() == Some(&seq)
            {
                g.waiting.pop_front();
            }
            // In range: `fits` checked the queue's bound, far below the counters' range.
            let _ = self.hold(group, bytes);
            self.wake(&thread);
        }
    }
}

pub(crate) struct Room {
    limits: Limits,
    state: Mutex<State>,
}

impl Room {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            state: Mutex::new(State::default()),
        }
    }

    /// Holds room for a submission whatever the bound: the restore at open, which one frame
    /// bounds.
    pub fn hold(&self, group: u128, bytes: u64) -> Result<(), LogError> {
        self.state
            .lock()
            .map_err(|_| LogError::Fenced)?
            .hold(group, bytes)
    }

    /// Takes room for a submission of `bytes` for `group`: at once when there is room and no
    /// one waits before it; otherwise `Busy`, or, when `wait`, in a slot of its own until room
    /// is handed to it or the log fences.
    pub fn take(&self, group: u128, bytes: u64, wait: bool) -> Result<(), LogError> {
        let seq = {
            let mut state = self.state.lock().map_err(|_| LogError::Fenced)?;
            if state.fenced {
                return Err(LogError::Fenced);
            }
            let (own, waiting) = state
                .groups
                .get(&group)
                .map_or((0, 0), |g| (g.admitted, g.waiting.len()));
            // Room goes to those who wait before anyone who comes after them.
            if waiting == 0
                && state.eligible.is_empty()
                && own < GROUP_SUBMISSIONS
                && state.fits(&self.limits, bytes)
            {
                return state.hold(group, bytes);
            }
            if !wait || state.waiters.len() >= self.limits.waiters || waiting >= GROUP_SUBMISSIONS {
                return Err(LogError::Busy);
            }
            let seq = state.next;
            state.next = seq.checked_add(1).ok_or(LogError::Busy)?;
            state.waiters.insert(
                seq,
                Waiter {
                    group,
                    bytes,
                    thread: std::thread::current(),
                    answer: None,
                },
            );
            let g = state.groups.entry(group).or_default();
            g.waiting.push_back(seq);
            if g.waiting.len() <= g.eligible() {
                state.eligible.insert(seq);
            }
            state.admit(&self.limits);
            seq
        };
        loop {
            {
                let mut state = self.state.lock().map_err(|_| LogError::Fenced)?;
                let answer = state.waiters.get(&seq).and_then(|w| w.answer);
                if let Some(answer) = answer {
                    state.waiters.remove(&seq);
                    return match answer {
                        Answer::Admitted => Ok(()),
                        Answer::Fenced => Err(LogError::Fenced),
                    };
                }
            }
            // A wake given before this park is kept by the thread's token, so none is lost;
            // a park that returns without one looks again.
            std::thread::park();
        }
    }

    /// Gives back the room of a submission of `group` answered or never sent, and hands it to
    /// the waiters it fits.
    pub fn release(&self, group: u128, bytes: u64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.submissions = state.submissions.saturating_sub(1);
        state.bytes = state.bytes.saturating_sub(bytes);
        let mut newly = None;
        let mut empty = false;
        if let Some(g) = state.groups.get_mut(&group) {
            let before = g.eligible();
            g.admitted = g.admitted.saturating_sub(1);
            if g.eligible() > before {
                newly = g.waiting.get(before).copied();
            }
            empty = g.admitted == 0 && g.waiting.is_empty();
        }
        if empty {
            state.groups.remove(&group);
        }
        if let Some(seq) = newly {
            state.eligible.insert(seq);
        }
        state.admit(&self.limits);
    }

    /// Fences the queue: nothing more is admitted, and every waiter's slot is completed
    /// `Fenced` and its thread woken once.
    pub fn fence(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.fenced = true;
        state.eligible.clear();
        for g in state.groups.values_mut() {
            g.waiting.clear();
        }
        let threads: Vec<Thread> = state
            .waiters
            .values_mut()
            .filter(|w| w.answer.is_none())
            .map(|w| {
                w.answer = Some(Answer::Fenced);
                w.thread.clone()
            })
            .collect();
        for thread in &threads {
            state.wake(thread);
        }
    }

    #[cfg(test)]
    fn waiting(&self) -> usize {
        self.state.lock().map_or(0, |s| {
            s.waiters.values().filter(|w| w.answer.is_none()).count()
        })
    }

    #[cfg(test)]
    fn wakes(&self) -> u64 {
        self.state.lock().map_or(0, |s| s.wakes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;

    fn room(submissions: usize, waiters: usize) -> Room {
        Room::new(Limits {
            submissions,
            bytes: u64::MAX,
            waiters,
        })
    }

    /// Waits until `n` submitters wait: the fact the test needs, not a guess at a time.
    fn until_waiting(room: &Room, n: usize) {
        while room.waiting() != n {
            std::thread::yield_now();
        }
    }

    /// `W` waiters and `K` answers: each answer wakes the one waiter it admits, in arrival
    /// order, so the wakes are exactly the waiters admitted, never `K × W`; a fence then wakes
    /// each remaining waiter exactly once.
    #[test]
    fn each_answer_wakes_only_the_waiter_it_admits_and_a_fence_wakes_each_once() {
        const W: usize = 48;
        const K: usize = 16;
        let room = room(1, W * GROUP_SUBMISSIONS);
        // Group `W` holds the only room.
        room.take(W as u128, 1, false).unwrap();
        let (sent, admitted) = channel();
        std::thread::scope(|s| {
            let mut handles = Vec::new();
            for i in 0..W {
                let (room, sent) = (&room, sent.clone());
                handles.push(s.spawn(move || {
                    let r = room.take(i as u128, 1, true);
                    sent.send((i, r.is_ok())).unwrap();
                    r
                }));
                // One at a time, so arrival order is `i`.
                until_waiting(room, i + 1);
            }
            assert_eq!(room.wakes(), 0, "waiting woke no one");
            let mut holder = W as u128;
            for k in 0..K {
                room.release(holder, 1);
                let (i, ok) = admitted.recv().unwrap();
                assert!(ok);
                assert_eq!(i, k, "room went out of arrival order");
                holder = i as u128;
                assert_eq!(
                    room.wakes(),
                    (k + 1) as u64,
                    "an answer woke more than it admitted"
                );
            }
            assert_eq!(room.waiting(), W - K);
            room.fence();
            let fenced: Vec<_> = (K..W).map(|_| admitted.recv().unwrap()).collect();
            assert!(fenced.iter().all(|&(i, ok)| !ok && i >= K));
            for (i, h) in handles.into_iter().enumerate() {
                let r = h.join().unwrap();
                assert_eq!(r.is_ok(), i < K, "waiter {i}");
            }
        });
        // K admissions, then W − K fenced: one wake each, every waiter woken exactly once.
        assert_eq!(room.wakes(), W as u64);
        assert!(matches!(room.take(0, 1, true), Err(LogError::Fenced)));
    }

    /// A group at its own bound waits for its own room: room freed by another group goes to
    /// the next waiter it fits, not to the hot group's.
    #[test]
    fn a_hot_group_waits_for_its_own_room() {
        let room = room(3, 8);
        room.take(1, 1, false).unwrap();
        room.take(1, 1, false).unwrap();
        room.take(2, 1, false).unwrap();
        let (sent, admitted) = channel();
        std::thread::scope(|s| {
            for group in [1u128, 3] {
                let (room, sent) = (&room, sent.clone());
                s.spawn(move || {
                    room.take(group, 1, true).unwrap();
                    sent.send(group).unwrap();
                });
                until_waiting(room, if group == 1 { 1 } else { 2 });
            }
            room.release(2, 1);
            assert_eq!(admitted.recv().unwrap(), 3);
            room.release(1, 1);
            assert_eq!(admitted.recv().unwrap(), 1);
        });
        assert_eq!(room.wakes(), 2);
    }

    /// Past its bound the waiting list refuses, and nothing that does not wait jumps it.
    #[test]
    fn the_waiting_list_is_bounded_and_not_jumped() {
        let room = room(1, 1);
        room.take(9, 1, false).unwrap();
        std::thread::scope(|s| {
            let waiter = s.spawn(|| room.take(1, 1, true));
            until_waiting(&room, 1);
            assert!(matches!(room.take(2, 1, true), Err(LogError::Busy)));
            room.release(9, 1);
            waiter.join().unwrap().unwrap();
        });
        // The room went to the waiter: one that does not wait finds none.
        assert!(matches!(room.take(3, 1, false), Err(LogError::Busy)));
    }
}
