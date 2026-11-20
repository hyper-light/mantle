//! The trunk's maintenance tasks, filed by where each stands (CLAUDE.md §2), so a pass costs the
//! tasks it acts on, not every task queued: it steps the inline ones, hands out the ready ones,
//! finds a worker's result by its ticket and applies the done ones, and the debt is a running
//! total. A three-node cell taking 1-byte PUTs spent about 35% of its first shard thread's busy
//! samples walking the whole list for these (macOS `sample`, 2026-11-20).
//!
//! Bounds: each index holds a task at most once, so none holds more than the tasks, which are
//! bounded by the trunk's size: one compaction a pivot and one settle a node ([`Trunk`]'s
//! `tasks`).

use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::ops::{Deref, DerefMut};

/// Where a task stands, as the indices file it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    /// Stepped on the shard.
    Inline,
    /// Waiting for its inputs to land and a worker to be free.
    Ready,
    /// On a worker, its result named by the ticket its send returned.
    Out(Ticket),
    /// Finished inline or on a worker, its parts waiting to apply.
    Done,
    /// Dropped by an abandonment, or for a moment handed from ready to out.
    Dropped,
}

impl Class {
    fn of(run: &Running) -> Self {
        match run {
            Running::Inline(_) => Class::Inline,
            Running::Finished(_) => Class::Done,
            Running::Remote { state, .. } => match state {
                State::Ready(_) => Class::Ready,
                State::Out(ticket) => Class::Out(*ticket),
                State::Done(_) => Class::Done,
                State::Dropped => Class::Dropped,
            },
        }
    }
}

/// A task's share of the maintenance debt ([`Trunk::debt`]): what an inline compaction has left,
/// and the planned entries of one waiting for a worker or out on one.
fn owed(task: &NodeTask) -> u64 {
    match &task.run {
        Running::Inline(c) => c.remaining(),
        Running::Remote {
            state: State::Ready(_) | State::Out(_),
            ..
        } => task.units,
        _ => 0,
    }
}

/// A task and how it is filed.
#[derive(Debug)]
struct Filed {
    task: NodeTask,
    /// Whether its node is other than the root when it was planned. It stays so for the task's
    /// life: the root changes only as its split or settle applies, with no task left on it
    /// ([`Trunk::take_change`], [`Trunk::look_at`] while its settle is started), and the new root
    /// is a new node.
    below: bool,
    class: Class,
    owed: u64,
}

/// The indices, kept apart from the tasks so a task borrowed to change is refiled as it is let go
/// ([`TaskMut`]). Each holds a task at most once.
#[derive(Debug, Default, PartialEq, Eq)]
struct Index {
    /// The inline and the ready tasks by `(below, plan number)`: the root's first, each in plan
    /// order, the order a pass steps and hands them out in.
    inline: BTreeSet<(bool, u64)>,
    ready: BTreeSet<(bool, u64)>,
    /// The tasks out, by their job's ticket: the pool numbers every job it hands out, so a
    /// ticket names one task.
    out: BTreeMap<Ticket, u64>,
    /// The done tasks in plan order, the order they apply in.
    done: BTreeSet<u64>,
    /// Every task by `(node, plan number)`.
    nodes: BTreeSet<(usize, u64)>,
    /// The tasks' shares of the debt summed. Fewer than 2^64 tasks of at most 2^64 - 1 each sum
    /// below `u128::MAX`, so the sum never saturates and adding and taking a share are exact.
    owed: u128,
}

impl Index {
    fn file(&mut self, seq: u64, below: bool, class: Class) {
        match class {
            Class::Inline => {
                self.inline.insert((below, seq));
            }
            Class::Ready => {
                self.ready.insert((below, seq));
            }
            Class::Out(ticket) => {
                self.out.insert(ticket, seq);
            }
            Class::Done => {
                self.done.insert(seq);
            }
            Class::Dropped => {}
        }
    }

    fn unfile(&mut self, seq: u64, below: bool, class: Class) {
        match class {
            Class::Inline => {
                self.inline.remove(&(below, seq));
            }
            Class::Ready => {
                self.ready.remove(&(below, seq));
            }
            Class::Out(ticket) => {
                self.out.remove(&ticket);
            }
            Class::Done => {
                self.done.remove(&seq);
            }
            Class::Dropped => {}
        }
    }

    /// Files task `seq` again as it stands now: under its class, its share of the debt counted.
    fn refile(&mut self, seq: u64, filed: &mut Filed) {
        let class = Class::of(&filed.task.run);
        if class != filed.class {
            self.unfile(seq, filed.below, filed.class);
            self.file(seq, filed.below, class);
            filed.class = class;
        }
        let owed = owed(&filed.task);
        self.owed = self
            .owed
            .saturating_sub(u128::from(filed.owed))
            .saturating_add(u128::from(owed));
        filed.owed = owed;
    }
}

/// The trunk's tasks in plan order, each filed by where it stands.
#[derive(Debug, Default)]
pub(in crate::trunk) struct Tasks {
    /// Every task by its plan number, so in plan order.
    all: BTreeMap<u64, Filed>,
    /// The next task's plan number.
    next: u64,
    index: Index,
    /// The tasks looked at, which a test counts to show a pass costs the tasks it acts on.
    #[cfg(test)]
    visits: std::cell::Cell<u64>,
}

/// A task borrowed to change: refiled as it is let go, so the indices and the debt follow its
/// state on every path, an error's return included.
pub(super) struct TaskMut<'a> {
    filed: &'a mut Filed,
    seq: u64,
    index: &'a mut Index,
}

impl Deref for TaskMut<'_> {
    type Target = NodeTask;

    fn deref(&self) -> &NodeTask {
        &self.filed.task
    }
}

impl DerefMut for TaskMut<'_> {
    fn deref_mut(&mut self) -> &mut NodeTask {
        &mut self.filed.task
    }
}

impl Drop for TaskMut<'_> {
    fn drop(&mut self) {
        self.index.refile(self.seq, self.filed);
    }
}

/// The first of `set` after `from`, or its first.
fn after<K: Ord + Copy>(set: &BTreeSet<K>, from: Option<K>) -> Option<K> {
    match from {
        None => set.first().copied(),
        Some(k) => set.range((Excluded(k), Unbounded)).next().copied(),
    }
}

impl Tasks {
    pub(super) fn len(&self) -> usize {
        self.all.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.all.is_empty()
    }

    /// Files `task`, planned now, after every task planned before it; `root` the trunk's root.
    pub(super) fn push(&mut self, task: NodeTask, root: usize) -> Result<(), Error> {
        let seq = self.next;
        self.next = seq.checked_add(1).ok_or(corrupt())?;
        let filed = Filed {
            below: task.node != root,
            class: Class::of(&task.run),
            owed: owed(&task),
            task,
        };
        self.index.nodes.insert((filed.task.node, seq));
        self.index.file(seq, filed.below, filed.class);
        self.index.owed = self.index.owed.saturating_add(u128::from(filed.owed));
        self.all.insert(seq, filed);
        Ok(())
    }

    pub(super) fn get(&self, seq: u64) -> Option<&NodeTask> {
        self.visit();
        self.all.get(&seq).map(|f| &f.task)
    }

    pub(super) fn get_mut(&mut self, seq: u64) -> Option<TaskMut<'_>> {
        self.visit();
        let filed = self.all.get_mut(&seq)?;
        Some(TaskMut {
            filed,
            seq,
            index: &mut self.index,
        })
    }

    pub(super) fn remove(&mut self, seq: u64) -> Option<NodeTask> {
        self.visit();
        let filed = self.all.remove(&seq)?;
        self.index.unfile(seq, filed.below, filed.class);
        self.index.nodes.remove(&(filed.task.node, seq));
        self.index.owed = self.index.owed.saturating_sub(u128::from(filed.owed));
        Some(filed.task)
    }

    /// Every task, taken in plan order, the tasks left empty.
    pub(super) fn take_all(&mut self) -> impl Iterator<Item = NodeTask> + use<> {
        std::mem::take(self).all.into_values().map(|f| f.task)
    }

    /// Node `n`'s tasks, in plan order: one compaction a pivot and one settle at most.
    pub(super) fn on(&self, n: usize) -> impl Iterator<Item = &NodeTask> {
        self.index
            .nodes
            .range((n, 0)..=(n, u64::MAX))
            .filter_map(|&(_, seq)| self.get(seq))
    }

    /// Whether node `n` has a task.
    pub(super) fn busy(&self, n: usize) -> bool {
        self.index
            .nodes
            .range((n, 0)..=(n, u64::MAX))
            .next()
            .is_some()
    }

    /// The inline tasks, the root's first, each in plan order.
    pub(super) fn inline(&self) -> impl Iterator<Item = &NodeTask> {
        self.index
            .inline
            .iter()
            .filter_map(|&(_, seq)| self.get(seq))
    }

    /// The inline tasks of the root's pass (`below` false) or of the rest's, in plan order.
    pub(super) fn inline_of(&self, below: bool) -> impl Iterator<Item = &NodeTask> {
        self.index
            .inline
            .range((below, 0)..=(below, u64::MAX))
            .filter_map(|&(_, seq)| self.get(seq))
    }

    pub(super) fn inline_count(&self) -> usize {
        self.index.inline.len()
    }

    /// The inline task after `from` in the order a pass steps them, or the first.
    pub(super) fn next_inline(&self, from: Option<(bool, u64)>) -> Option<(bool, u64)> {
        after(&self.index.inline, from)
    }

    pub(super) fn ready_count(&self) -> usize {
        self.index.ready.len()
    }

    /// The ready task after `from` in the order a pass hands them out, or the first.
    pub(super) fn next_ready(&self, from: Option<(bool, u64)>) -> Option<(bool, u64)> {
        after(&self.index.ready, from)
    }

    pub(super) fn has_inline(&self) -> bool {
        !self.index.inline.is_empty()
    }

    pub(super) fn has_out(&self) -> bool {
        !self.index.out.is_empty()
    }

    /// Whether a job is out or done: its result gives back the grant it did not write as it is
    /// taken, and its task frees its inputs as it applies.
    pub(super) fn out_or_done(&self) -> bool {
        !self.index.out.is_empty() || !self.index.done.is_empty()
    }

    /// The task out whose job `ticket` names.
    pub(super) fn out(&self, ticket: Ticket) -> Option<u64> {
        self.index.out.get(&ticket).copied()
    }

    pub(super) fn done_count(&self) -> usize {
        self.index.done.len()
    }

    /// The first done task in plan order.
    pub(super) fn first_done(&self) -> Option<u64> {
        self.index.done.first().copied()
    }

    /// The tasks' shares of the debt summed, at most `u64::MAX`.
    pub(super) fn owed(&self) -> u64 {
        u64::try_from(self.index.owed).unwrap_or(u64::MAX)
    }

    /// Node `n`'s in-flight list lost its first `dead` bundles: its tasks' planned ranges move
    /// down with the list. A range is no part of where a task stands or of its debt, so none is
    /// refiled.
    pub(super) fn shift(&mut self, n: usize, dead: usize) {
        for &(_, seq) in self.index.nodes.range((n, 0)..=(n, u64::MAX)) {
            if let Some(f) = self.all.get_mut(&seq)
                && let What::Compact { start, covered, .. } | What::Settle { start, covered, .. } =
                    &mut f.task.what
            {
                *start = start.saturating_sub(dead);
                *covered = covered.saturating_sub(dead);
            }
        }
    }

    fn visit(&self) {
        #[cfg(test)]
        self.visits.set(self.visits.get().saturating_add(1));
    }

    /// The tasks looked at since the count was last taken.
    #[cfg(test)]
    pub(super) fn take_visits(&self) -> u64 {
        self.visits.take()
    }

    /// Every task, in plan order.
    #[cfg(test)]
    pub(super) fn iter(&self) -> impl Iterator<Item = &NodeTask> {
        self.all.values().map(|f| &f.task)
    }

    /// The indices and the debt, against a look at every task with `root` the trunk's root: a
    /// corruption if they differ.
    #[cfg(test)]
    pub(super) fn check(&self, root: usize) -> Result<(), Error> {
        let mut index = Index::default();
        for (&seq, f) in &self.all {
            if f.class != Class::of(&f.task.run)
                || f.owed != owed(&f.task)
                || f.below != (f.task.node != root)
            {
                return Err(mismatch());
            }
            index.nodes.insert((f.task.node, seq));
            index.file(seq, f.below, f.class);
            index.owed = index.owed.saturating_add(u128::from(f.owed));
        }
        let out = self
            .all
            .values()
            .filter(|f| matches!(f.class, Class::Out(_)))
            .count();
        if index == self.index && out == self.index.out.len() {
            Ok(())
        } else {
            Err(mismatch())
        }
    }
}

#[cfg(test)]
fn mismatch() -> Error {
    Error::Corruption {
        what: "the trunk's task indices and debt",
        why: Malformed::CountMismatch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready() -> Running {
        Running::Remote {
            grant: Vec::new(),
            state: State::Ready(Box::new(Task {
                inputs: Vec::new(),
                from: Vec::new(),
                end: None,
                drop_tombstones: false,
                per: 1,
                pages: 0,
            })),
        }
    }

    fn out(job: u64) -> Running {
        Running::Remote {
            grant: Vec::new(),
            state: State::Out(Ticket { worker: 0, job }),
        }
    }

    fn task(node: usize, run: Running, units: u64) -> NodeTask {
        NodeTask {
            node,
            what: What::Compact {
                key: vec![u8::try_from(node).unwrap()],
                start: 2,
                covered: 3,
            },
            run,
            units,
            reuse: Reuse::default(),
        }
    }

    /// The ready tasks in the order a pass hands them out.
    fn ready_order(tasks: &Tasks) -> Vec<(bool, u64)> {
        let mut order = Vec::new();
        let mut last = None;
        while let Some(key) = tasks.next_ready(last) {
            order.push(key);
            last = Some(key);
        }
        order
    }

    /// Do: plan tasks on the root (node 7) and below it, ready, out and finished, interleaved;
    /// hand one out, take one's result, finish another, drop one, apply the done.
    /// Expect: the ready ones come the root's first, each in plan order, as a pass hands them
    /// out; a result's task is found by its ticket; the done ones apply in plan order; a node's
    /// tasks and their ranges are found by the node; and the debt (the units of those ready or
    /// out) follows every change, the recount agreeing throughout.
    #[test]
    fn tasks_are_filed_in_the_order_a_pass_takes_them_and_the_debt_follows() {
        let root = 7;
        let mut tasks = Tasks::default();
        tasks.push(task(1, ready(), 1), root).unwrap();
        tasks.push(task(root, ready(), 2), root).unwrap();
        tasks.push(task(2, out(40), 4), root).unwrap();
        tasks
            .push(task(root, Running::Finished(Vec::new()), 8), root)
            .unwrap();
        tasks.push(task(root, ready(), 16), root).unwrap();
        tasks.push(task(1, ready(), 32), root).unwrap();
        tasks.check(root).unwrap();
        assert_eq!(
            ready_order(&tasks),
            [(false, 1), (false, 4), (true, 0), (true, 5)]
        );
        assert_eq!(tasks.owed(), 1 + 2 + 4 + 16 + 32);
        assert!(tasks.has_out() && tasks.out_or_done() && !tasks.has_inline());
        // The root's first ready task goes out on ticket 41.
        if let Running::Remote { state, .. } = &mut tasks.get_mut(1).unwrap().run {
            *state = State::Out(Ticket { worker: 0, job: 41 });
        }
        assert_eq!(ready_order(&tasks), [(false, 4), (true, 0), (true, 5)]);
        assert_eq!(tasks.out(Ticket { worker: 0, job: 41 }), Some(1));
        assert_eq!(tasks.owed(), 1 + 2 + 4 + 16 + 32);
        tasks.check(root).unwrap();
        // Ticket 40's result comes back: its task is done, its share of the debt paid.
        let seq = tasks.out(Ticket { worker: 0, job: 40 }).unwrap();
        assert_eq!(seq, 2);
        tasks.get_mut(seq).unwrap().run = Running::Finished(Vec::new());
        assert_eq!(tasks.owed(), 1 + 2 + 16 + 32);
        assert_eq!(tasks.out(Ticket { worker: 0, job: 40 }), None);
        // One below the root is dropped, as an abandonment drops it.
        if let Running::Remote { state, .. } = &mut tasks.get_mut(5).unwrap().run {
            *state = State::Dropped;
        }
        assert_eq!(tasks.owed(), 1 + 2 + 16);
        assert_eq!(ready_order(&tasks), [(false, 4), (true, 0)]);
        tasks.check(root).unwrap();
        // The done apply in plan order.
        assert_eq!(tasks.done_count(), 2);
        assert_eq!(tasks.first_done(), Some(2));
        tasks.remove(2).unwrap();
        assert_eq!(tasks.first_done(), Some(3));
        tasks.remove(3).unwrap();
        assert_eq!(tasks.first_done(), None);
        // A node's tasks, and their ranges moved down with its in-flight list.
        assert!(tasks.busy(root) && tasks.busy(1) && !tasks.busy(2));
        assert_eq!(tasks.on(1).count(), 2);
        tasks.shift(1, 2);
        for t in tasks.on(1) {
            assert!(matches!(
                t.what,
                What::Compact {
                    start: 0,
                    covered: 1,
                    ..
                }
            ));
        }
        for t in tasks.on(root) {
            assert!(matches!(
                t.what,
                What::Compact {
                    start: 2,
                    covered: 3,
                    ..
                }
            ));
        }
        assert_eq!(tasks.owed(), 1 + 2 + 16);
        tasks.check(root).unwrap();
        // Taken whole and filed again in plan order, as an abandonment keeps the rest.
        let kept: Vec<NodeTask> = tasks.take_all().collect();
        assert!(tasks.is_empty() && tasks.owed() == 0);
        let units: Vec<u64> = kept.iter().map(|t| t.units).collect();
        assert_eq!(units, [1, 2, 16, 32]);
        for t in kept {
            tasks.push(t, root).unwrap();
        }
        assert_eq!(tasks.owed(), 1 + 2 + 16);
        assert_eq!(ready_order(&tasks), [(false, 2), (true, 0)]);
        tasks.check(root).unwrap();
    }
}
