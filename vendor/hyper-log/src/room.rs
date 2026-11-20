//! Room in the log's queue, and the submitters waiting for it (mantle docs/design/raft-log.md
//! §3; node.md §1.3). The owner holds it; nothing else reads it.
//!
//! A submission holds its room until the writer answers it. A submitter that may not be refused
//! waits for room in a slot of its own, in arrival order: room the writer frees is handed to the
//! waiters it fits, in order, and only those are told, each through its own ticket. A fence
//! completes every waiter's slot once. Room was once a condition variable broadcast to every
//! waiter on every answer; on macOS that broadcast walks every waiter inside one kernel spinlock,
//! and thousands of waiters held it until the kernel panicked (research/26 §1.3–§1.4).
//!
//! A waiter whose own group already holds its [`GROUP_SUBMISSIONS`], or its share of the queue's
//! bytes ([`Limits::group_bytes`]), waits for its group's room and is passed over by room it could
//! not use, so a hot group never takes the others' room.
//! A waiter that waits for the queue's room is never passed: room goes to it before any later
//! arrival, so none starves.

use std::collections::{BTreeSet, HashMap, VecDeque};

use crate::LogError;

/// Submissions one group may have unanswered: one in each of the log's pipeline frames
/// ([`crate::PIPELINE_FRAMES`]), as many as the readies its replica keeps ahead of their
/// persistence (`GroupLog::depth`; mantle docs/design/replica.md §3): the frame flushed and
/// awaiting the record that confirms it, the frame on the device, whose record does, and the
/// frame gathering while it flushes (mantle docs/design/raft-log.md §3.1). The third is what lets
/// the frame on the device be followed: admitted while that frame flushes, it tells the device
/// another frame follows, so the next frame's record confirms it and no confirmation of its own
/// is written (raft-log.md §3). With two, a busy group's third ready waited for its group's room
/// until the frame before was answered, which is at the flush's end, too late to say so
/// (`tests/follow.rs`). A compaction, which waits for its answer before the next, waits behind
/// them for its group's room. A caller that sends more waits for its own room, and never takes
/// another group's.
pub(crate) const GROUP_SUBMISSIONS: usize = crate::PIPELINE_FRAMES;

/// Submissions a group's own room admits whatever their bytes: its share of the queue's bytes
/// ([`Limits::group_bytes`]) is this many frames of the largest charge, and a submission holds at
/// most one frame's (mantle docs/design/raft-log.md §3.1). The rest of its [`GROUP_SUBMISSIONS`]
/// it admits while their bytes fit the share. A group's handle keeps out at most these and the
/// [`GROUP_SUBMISSIONS`] that may wait beyond them (`group.rs` `OUT`), so the room never refuses
/// a handle for want of its own group's room.
pub(crate) const GROUP_ADMITS: usize = crate::PIPELINE_FRAMES.saturating_sub(1);

/// What the queue admits.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub(crate) submissions: usize,
    pub(crate) bytes: u64,
    /// Charged bytes one group's submissions hold at most: the queue's less one frame's worth,
    /// [`GROUP_ADMITS`] frames of the largest charge, so one group never holds the last frame's
    /// worth the others need (mantle docs/design/raft-log.md §3.1).
    pub(crate) group_bytes: u64,
    /// Waiters the list holds: a group waits with at most its own [`GROUP_SUBMISSIONS`], and
    /// the log holds at most `max_groups` (mantle docs/design/raft-log.md §3). Past it, `Busy`.
    pub(crate) waiters: usize,
}

/// What a submission asking for room is told.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Take {
    /// It holds its room now.
    Admitted,
    /// It waits in the slot of this arrival number until room is handed to it.
    Waiting(u64),
}

struct Waiter {
    group: u128,
    bytes: u64,
}

#[derive(Default)]
struct Group {
    /// Submissions admitted and not yet answered.
    admitted: usize,
    /// Their charged bytes.
    bytes: u64,
    /// The group's waiters, oldest first; at most [`GROUP_SUBMISSIONS`].
    waiting: VecDeque<u64>,
}

impl Group {
    /// Whether the group's own room takes a submission of `bytes` more, its share of the queue's
    /// bytes being `share`.
    fn takes(&self, bytes: u64, share: u64) -> bool {
        self.admitted < GROUP_SUBMISSIONS
            && self
                .bytes
                .checked_add(bytes)
                .is_some_and(|total| total <= share)
    }
}

/// The queue's room, held by the log's owner.
pub(crate) struct Room {
    limits: Limits,
    submissions: usize,
    bytes: u64,
    /// Groups with room held or a waiter: bounded by the queue's submissions and the waiters.
    groups: HashMap<u128, Group>,
    /// Every waiter's slot, by arrival number.
    waiters: HashMap<u64, Waiter>,
    /// Waiters whose group has room, in arrival order: the next room goes to the first. A
    /// group's oldest waiter is here exactly while its group's own room takes it, and its later
    /// waiters are not: each comes once the one before it is admitted.
    eligible: BTreeSet<u64>,
    next: u64,
    fenced: bool,
}

impl Room {
    pub(crate) fn new(limits: Limits) -> Self {
        Self {
            limits,
            submissions: 0,
            bytes: 0,
            groups: HashMap::new(),
            waiters: HashMap::new(),
            eligible: BTreeSet::new(),
            next: 0,
            fenced: false,
        }
    }

    fn fits(&self, bytes: u64) -> bool {
        self.submissions < self.limits.submissions
            && self
                .bytes
                .checked_add(bytes)
                .is_some_and(|total| total <= self.limits.bytes)
    }

    /// Holds room for a submission whatever the bound: the restore at open, which one frame
    /// bounds.
    pub(crate) fn hold(&mut self, group: u128, bytes: u64) -> Result<(), LogError> {
        let submissions = self.submissions.checked_add(1).ok_or(LogError::Busy)?;
        let total = self.bytes.checked_add(bytes).ok_or(LogError::Busy)?;
        let g = self.groups.entry(group).or_default();
        let own = g.admitted.checked_add(1).ok_or(LogError::Busy)?;
        let own_bytes = g.bytes.checked_add(bytes).ok_or(LogError::Busy)?;
        g.admitted = own;
        g.bytes = own_bytes;
        self.submissions = submissions;
        self.bytes = total;
        Ok(())
    }

    /// Takes room for a submission of `bytes` for `group`: at once when there is room and no
    /// one waits before it; otherwise `Busy`, or, when `wait`, a slot of its own in which it
    /// waits until room is handed to it or the log fences. Any other waiter the room admits as
    /// the newcomer joins goes to `admitted`.
    pub(crate) fn take(
        &mut self,
        group: u128,
        bytes: u64,
        wait: bool,
        admitted: &mut Vec<u64>,
    ) -> Result<Take, LogError> {
        if self.fenced {
            return Err(LogError::Fenced);
        }
        let share = self.limits.group_bytes;
        let (takes, waiting) = self.groups.get(&group).map_or_else(
            || (Group::default().takes(bytes, share), 0),
            |g| (g.takes(bytes, share), g.waiting.len()),
        );
        // Room goes to those who wait before anyone who comes after them.
        if waiting == 0 && self.eligible.is_empty() && takes && self.fits(bytes) {
            return self.hold(group, bytes).map(|()| Take::Admitted);
        }
        if !wait || self.waiters.len() >= self.limits.waiters || waiting >= GROUP_SUBMISSIONS {
            return Err(LogError::Busy);
        }
        let seq = self.next;
        self.next = seq.checked_add(1).ok_or(LogError::Busy)?;
        self.waiters.insert(seq, Waiter { group, bytes });
        self.groups.entry(group).or_default().waiting.push_back(seq);
        // The group's oldest waiter, whose group's room takes it, waits for the queue's room.
        if waiting == 0 && takes {
            self.eligible.insert(seq);
        }
        self.admit(admitted);
        // The newcomer is admitted at once only when it fits behind no one.
        if let Some(at) = admitted.iter().position(|&a| a == seq) {
            admitted.swap_remove(at);
            return Ok(Take::Admitted);
        }
        Ok(Take::Waiting(seq))
    }

    /// Hands the room there is to the eligible waiters in arrival order, adding each admitted to
    /// `admitted`. Stops at the first that does not fit, which keeps its turn. Each turn admits a
    /// waiter or drops a stale entry, so it ends within the waiters there are.
    fn admit(&mut self, admitted: &mut Vec<u64>) {
        while let Some(&seq) = self.eligible.first() {
            let Some((group, bytes)) = self.waiters.get(&seq).map(|w| (w.group, w.bytes)) else {
                self.eligible.remove(&seq);
                continue;
            };
            if !self.fits(bytes) {
                break;
            }
            self.eligible.remove(&seq);
            self.waiters.remove(&seq);
            // The admitted waiter is its group's oldest: only the oldest is eligible.
            if let Some(g) = self.groups.get_mut(&group)
                && g.waiting.front() == Some(&seq)
            {
                g.waiting.pop_front();
            }
            // In range: `fits` checked the queue's bound, far below the counters' range.
            let _ = self.hold(group, bytes);
            admitted.push(seq);
            // The group's next waiter waits for the queue's room now, if its group's room takes it.
            self.ready(group);
        }
    }

    /// Makes `group`'s oldest waiter eligible if its group's own room takes it.
    fn ready(&mut self, group: u128) {
        let Some(g) = self.groups.get(&group) else {
            return;
        };
        let Some(&seq) = g.waiting.front() else {
            return;
        };
        if self
            .waiters
            .get(&seq)
            .is_some_and(|w| g.takes(w.bytes, self.limits.group_bytes))
        {
            self.eligible.insert(seq);
        }
    }

    /// Gives back the room of a submission of `group` answered, and hands it to the waiters it
    /// fits, adding each admitted to `admitted`.
    pub(crate) fn release(&mut self, group: u128, bytes: u64, admitted: &mut Vec<u64>) {
        self.submissions = self.submissions.saturating_sub(1);
        self.bytes = self.bytes.saturating_sub(bytes);
        let mut empty = false;
        if let Some(g) = self.groups.get_mut(&group) {
            g.admitted = g.admitted.saturating_sub(1);
            g.bytes = g.bytes.saturating_sub(bytes);
            empty = g.admitted == 0 && g.waiting.is_empty();
        }
        if empty {
            self.groups.remove(&group);
        }
        self.ready(group);
        self.admit(admitted);
    }

    /// Whether a submission of `group` holds room or waits for it: one the writer has yet to
    /// answer.
    pub(crate) fn holds(&self, group: u128) -> bool {
        self.groups
            .get(&group)
            .is_some_and(|g| g.admitted > 0 || !g.waiting.is_empty())
    }

    /// Fences the queue: nothing more is admitted, and every waiter's slot is completed: each
    /// arrival number goes to `fenced` once.
    pub(crate) fn fence(&mut self, fenced: &mut Vec<u64>) {
        self.fenced = true;
        self.eligible.clear();
        for g in self.groups.values_mut() {
            g.waiting.clear();
        }
        fenced.extend(self.waiters.drain().map(|(seq, _)| seq));
        fenced.sort_unstable();
    }

    /// Submitters waiting for room.
    #[cfg(test)]
    fn waiting(&self) -> usize {
        self.waiters.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn room(submissions: usize, waiters: usize) -> Room {
        Room::new(Limits {
            submissions,
            bytes: u64::MAX,
            group_bytes: u64::MAX,
            waiters,
        })
    }

    /// `W` waiters and `K` answers: each answer admits the one waiter it fits, in arrival
    /// order, so the waiters told are exactly the waiters admitted, never `K × W`; a fence then
    /// completes each remaining waiter exactly once.
    #[test]
    fn each_answer_wakes_only_the_waiter_it_admits_and_a_fence_wakes_each_once() {
        const W: usize = 48;
        const K: usize = 16;
        let mut room = room(1, W * GROUP_SUBMISSIONS);
        // Group `W` holds the only room.
        assert_eq!(
            room.take(W as u128, 1, false, &mut Vec::new()).unwrap(),
            Take::Admitted
        );
        let mut slots = Vec::new();
        for i in 0..W {
            match room.take(i as u128, 1, true, &mut Vec::new()).unwrap() {
                Take::Waiting(seq) => slots.push(seq),
                Take::Admitted => panic!("waiter {i} jumped the queue"),
            }
        }
        let mut told = 0usize;
        let mut holder = W as u128;
        for (k, &slot) in slots.iter().enumerate().take(K) {
            let mut admitted = Vec::new();
            room.release(holder, 1, &mut admitted);
            assert_eq!(admitted, vec![slot], "room went out of arrival order");
            told += admitted.len();
            holder = k as u128;
            assert_eq!(told, k + 1, "an answer woke more than it admitted");
        }
        assert_eq!(room.waiting(), W - K);
        let mut fenced = Vec::new();
        room.fence(&mut fenced);
        assert_eq!(fenced, slots[K..].to_vec());
        told += fenced.len();
        // K admissions, then W − K fenced: every waiter told exactly once.
        assert_eq!(told, W);
        assert!(matches!(
            room.take(0, 1, true, &mut Vec::new()),
            Err(LogError::Fenced)
        ));
    }

    /// A group at its own bound waits for its own room: room freed by another group goes to
    /// the next waiter it fits, not to the hot group's.
    #[test]
    fn a_hot_group_waits_for_its_own_room() {
        let mut room = room(GROUP_SUBMISSIONS + 1, 8);
        for _ in 0..GROUP_SUBMISSIONS {
            room.take(1, 1, false, &mut Vec::new()).unwrap();
        }
        room.take(2, 1, false, &mut Vec::new()).unwrap();
        let Take::Waiting(hot) = room.take(1, 1, true, &mut Vec::new()).unwrap() else {
            panic!("the hot group's one past its room was admitted");
        };
        let Take::Waiting(cold) = room.take(3, 1, true, &mut Vec::new()).unwrap() else {
            panic!("a waiter was passed");
        };
        let mut admitted = Vec::new();
        room.release(2, 1, &mut admitted);
        assert_eq!(admitted, vec![cold]);
        admitted.clear();
        room.release(1, 1, &mut admitted);
        assert_eq!(admitted, vec![hot]);
    }

    /// A group holds at most its share of the queue's bytes, however few its submissions: past
    /// it the group waits for its own room, and room another group frees goes to the next waiter
    /// it fits, so one group never holds the last frame's worth the others need.
    #[test]
    fn a_group_holds_at_most_its_share_of_the_bytes() {
        let mut room = Room::new(Limits {
            submissions: 8,
            bytes: 30,
            group_bytes: 20,
            waiters: 8,
        });
        room.take(1, 10, false, &mut Vec::new()).unwrap();
        room.take(1, 10, false, &mut Vec::new()).unwrap();
        assert!(matches!(
            room.take(1, 10, false, &mut Vec::new()),
            Err(LogError::Busy)
        ));
        let Take::Waiting(hot) = room.take(1, 10, true, &mut Vec::new()).unwrap() else {
            panic!("the group took past its share");
        };
        // The last frame's worth is the others'.
        assert_eq!(
            room.take(2, 10, false, &mut Vec::new()).unwrap(),
            Take::Admitted
        );
        let Take::Waiting(cold) = room.take(3, 10, true, &mut Vec::new()).unwrap() else {
            panic!("the queue held more than its bytes");
        };
        let mut admitted = Vec::new();
        room.release(2, 10, &mut admitted);
        assert_eq!(
            admitted,
            vec![cold],
            "the hot group took room it could not use"
        );
        admitted.clear();
        room.release(3, 10, &mut admitted);
        assert!(admitted.is_empty(), "the hot group passed its share");
        room.release(1, 10, &mut admitted);
        assert_eq!(admitted, vec![hot]);
    }

    /// A group's own room admits [`GROUP_ADMITS`] submissions of the largest charge whatever
    /// else it holds, its share being that many frames' worth as the log derives it, and
    /// [`GROUP_SUBMISSIONS`] more wait beyond them: a handle's bound of both (`group.rs` `OUT`)
    /// is never refused while its own group alone holds the queue, and one past it is.
    #[test]
    fn a_handles_bound_of_frame_sized_submissions_waits_never_refused() {
        let frame = 10;
        let frames = |n: usize| frame * u64::try_from(n).unwrap();
        let mut room = Room::new(Limits {
            submissions: 2 * GROUP_SUBMISSIONS,
            bytes: frames(crate::PIPELINE_FRAMES),
            group_bytes: frames(GROUP_ADMITS),
            waiters: GROUP_SUBMISSIONS,
        });
        for sent in 1..=GROUP_ADMITS + GROUP_SUBMISSIONS {
            let take = room.take(1, frame, true, &mut Vec::new());
            assert!(take.is_ok(), "submission {sent} refused: {take:?}");
        }
        assert!(matches!(
            room.take(1, frame, true, &mut Vec::new()),
            Err(LogError::Busy)
        ));
    }

    /// Past its bound the waiting list refuses, and nothing that does not wait jumps it.
    #[test]
    fn the_waiting_list_is_bounded_and_not_jumped() {
        let mut room = room(1, 1);
        room.take(9, 1, false, &mut Vec::new()).unwrap();
        let Take::Waiting(waiter) = room.take(1, 1, true, &mut Vec::new()).unwrap() else {
            panic!("a waiter jumped the holder");
        };
        assert!(matches!(
            room.take(2, 1, true, &mut Vec::new()),
            Err(LogError::Busy)
        ));
        let mut admitted = Vec::new();
        room.release(9, 1, &mut admitted);
        assert_eq!(admitted, vec![waiter]);
        // The room went to the waiter: one that does not wait finds none.
        assert!(matches!(
            room.take(3, 1, false, &mut Vec::new()),
            Err(LogError::Busy)
        ));
    }
}
