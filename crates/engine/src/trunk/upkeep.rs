//! The trunk's maintenance as flat per-node tasks (CLAUDE.md §10; docs/design/engine-structure.md
//! §6, "Flat maintenance"). A node's triggers plan its pivots' compactions, flush its full pivot
//! bundles by reference and take the change it wants (a leaf's settle, an index node's split); one
//! pass with a counted budget looks at the queued nodes, steps the inline tasks, hands out and
//! takes back the workers' jobs and applies what is done. No task waits on another node's: a node
//! held back by its parent's pivot is looked at again once that pivot's task is applied.

use super::*;

/// A change a node wants (CLAUDE.md §10's exclusivity): a leaf's settle into `parts` leaves, or
/// an index node's split into nodes of at most `fanout` pivots. While a node wants a change that
/// splits it, its parent's pivot for it plans no compaction and flushes its bundle down once, so
/// the parts can replace the pivot whole; once the change has `started`, the node takes no flush
/// and plans no compaction until it is applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Change {
    pub(super) kind: ChangeKind,
    pub(super) started: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ChangeKind {
    Settle { parts: u64 },
    Split,
}

impl Change {
    /// Whether the change makes more than one node of this one: then its parent's pivot must be
    /// empty for the parts to replace it.
    fn splits(self) -> bool {
        match self.kind {
            ChangeKind::Settle { parts } => parts > 1,
            ChangeKind::Split => true,
        }
    }
}

/// A maintenance task: node `node`'s compaction of one pivot's in-flight bundles, or a leaf's
/// settle, inline or out on a worker; `units` its inputs' entries when planned, its debt.
#[derive(Debug)]
pub(super) struct NodeTask {
    node: usize,
    what: What,
    run: Running,
    units: u64,
}

#[derive(Debug)]
enum What {
    /// The pivot keyed `key`, its in-flight bundles from `start` to `covered`.
    Compact {
        key: Vec<u8>,
        start: usize,
        covered: usize,
    },
    /// The leaf's whole compaction, its in-flight bundles from `start` to `covered` among its
    /// inputs; `extents` its bundle's branches hold, released once it replaces them (the
    /// in-flight bundles' go as the leaf reads past them), and what asked for it.
    Settle {
        extents: Vec<u64>,
        cause: Cause,
        start: usize,
        covered: usize,
    },
}

/// What leaf's settle would take now ([`Trunk::leaf_settle`]).
#[derive(Clone, Copy, Debug)]
struct LeafSettle {
    /// Entries over its bundle and its live in-flight bundles, an upper bound on its keys.
    count: u64,
    /// Its bundle's branches once an in-flight compaction would have added its one.
    after: usize,
    /// Seeks paid for it in a consolidation in progress, and whether its sources are several.
    was_paid: bool,
    paid: bool,
    /// Whether it settles now.
    due: bool,
}

/// What asked a leaf to settle: seeks' rent ([`Trunk::charge`]), more than `fanout` branches, or
/// more than `leaf_entries` entries (SplinterDB splits such a leaf by reference instead;
/// research/34 §1), counted apart so each cause's writes are measured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Cause {
    Seeks,
    Branches,
    Size,
}

#[derive(Debug)]
enum Running {
    /// On the shard, a budget at a time.
    Inline(Box<Compaction>),
    /// Finished inline, its parts waiting to apply.
    Finished(Vec<(Vec<u8>, Branch)>),
    /// On a worker: the extents granted it, top-ups included, and where it is.
    Remote { grant: Vec<u64>, state: State },
}

impl Running {
    fn done(&self) -> bool {
        matches!(
            self,
            Running::Finished(_)
                | Running::Remote {
                    state: State::Done(_),
                    ..
                }
        )
    }

    fn out(&self) -> bool {
        matches!(
            self,
            Running::Remote {
                state: State::Out(_),
                ..
            }
        )
    }
}

impl Trunk {
    /// Queues node `n` to be looked at, once until it is.
    pub(super) fn queue(&mut self, n: usize) {
        if let Some(node) = self.nodes.get_mut(n)
            && !node.queued
        {
            node.queued = true;
            self.looking.push_back(n);
        }
    }

    /// Whether the root has room for more branches: fewer than `fanout` sources a read probes
    /// above the root's pivot bundles (the pending branches and a pivot's live in-flight ones),
    /// the bound a pivot's bundle has before it flushes.
    pub fn has_room(&self) -> bool {
        let live = self.nodes.get(self.root).map_or(0, |node| {
            node.pivots
                .iter()
                .map(|p| node.inflight.len().saturating_sub(p.start))
                .max()
                .unwrap_or(0)
        });
        self.pending.len().saturating_add(live) < self.config.fanout
    }

    /// Whether maintenance is in progress: tasks out, nodes to look at, a change wanted, or a
    /// failure being abandoned; owed even when [`Self::debt`] counts nothing.
    pub fn maintaining(&self) -> bool {
        !self.tasks.is_empty()
            || !self.looking.is_empty()
            || self.abandoning.is_some()
            || self.nodes.iter().any(|n| n.wants.is_some())
    }

    /// Whether nothing is pending or in progress and every node's in-flight list is empty, which
    /// a checkpoint's image relies on.
    pub fn is_idle(&self) -> bool {
        self.pending.is_empty()
            && !self.maintaining()
            && self.nodes.iter().all(|n| n.inflight.is_empty())
    }

    /// The maintenance estimate, in work units: the pending branches' entries, what inline
    /// compactions have left, and the planned entries of those on the workers.
    pub fn debt(&self) -> u64 {
        let pending = self
            .pending
            .iter()
            .map(|b| b.count)
            .fold(0, u64::saturating_add);
        self.tasks
            .iter()
            .map(|t| match &t.run {
                Running::Inline(c) => c.remaining(),
                Running::Remote {
                    state: State::Ready(_) | State::Out(_),
                    ..
                } => t.units,
                _ => 0,
            })
            .fold(pending, u64::saturating_add)
    }

    /// Runs maintenance, waiting on the workers, until the root has room for more branches.
    /// Refused when a pass leaves it without room, changes nothing, and leaves no task out and
    /// no node to look at: no task could free it.
    pub fn make_room<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        self.wait = Wait::All;
        let mut mark = self.progress();
        while !self.has_room() {
            self.run(store, u64::MAX)?;
            let now = self.progress();
            if !self.has_room() && now == mark && self.tasks.is_empty() && self.looking.is_empty() {
                return Err(Error::InvalidArgument {
                    what: "a trunk whose root no task can make room in",
                });
            }
            mark = now;
        }
        Ok(())
    }

    /// Runs all maintenance to the end, each step waiting on the workers. Refused when a step
    /// leaves work, changes nothing, and leaves no task out and no node to look at: what is
    /// left, no task can do.
    pub fn drain<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        let mut mark = self.progress();
        while !self.is_idle() {
            self.step(store, u64::MAX)?;
            let now = self.progress();
            if !self.is_idle() && now == mark && self.tasks.is_empty() && self.looking.is_empty() {
                return Err(Error::InvalidArgument {
                    what: "a trunk whose maintenance no task can finish",
                });
            }
            mark = now;
        }
        Ok(())
    }

    /// One pass of [`Self::make_room`] without waiting on the workers, yielding on pages still
    /// being read; whether the root has room now.
    pub(crate) fn make_room_paced<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        budget: u64,
    ) -> Result<bool, Error> {
        self.wait = Wait::No;
        self.yield_io = true;
        let result = self.run(store, budget);
        self.yield_io = false;
        result.map(|_| self.has_room())
    }

    /// What a pass that progresses changes: the structure's counters, the tasks and pending
    /// branches held, the changes wanted, the in-flight bundles and the paid pivots.
    fn progress(&self) -> [u64; 9] {
        let s = &self.stats;
        let count = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
        [
            s.pivot_compactions,
            s.leaf_compactions,
            s.flushes,
            s.splits,
            count(self.tasks.len()),
            count(self.pending.len()),
            count(self.nodes.iter().filter(|n| n.wants.is_some()).count()),
            count(
                self.nodes
                    .iter()
                    .map(|n| n.inflight.len())
                    .fold(0usize, usize::saturating_add),
            ),
            count(self.paid),
        ]
    }

    /// One maintenance pass of up to `budget` work units: pending branches into the root, each
    /// queued node looked at, the inline tasks stepped (the root's first), the workers' jobs
    /// handed out and taken back as [`Wait`] says, every finished task applied, and what that
    /// set off looked at and handed out. Returns the units spent.
    pub(super) fn run<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        budget: u64,
    ) -> Result<u64, Error> {
        if self.saving() {
            return Err(Error::InvalidArgument {
                what: "a trunk changed while its checkpoint is being saved",
            });
        }
        self.io_waiting = false;
        self.workers_waiting = false;
        if self.abandoning.is_some() {
            return self.abandon_tasks(store).map(|_| 0);
        }
        self.incorporate_pending()?;
        self.start_consolidation()?;
        self.sweep();
        self.look(store)?;
        let used = self.step_inline(store, budget)?;
        if self.pool.is_some() {
            self.tend_tasks(store)?;
        }
        self.apply_done(store)?;
        self.incorporate_pending()?;
        self.look(store)?;
        if self.pool.is_some() && self.abandoning.is_none() {
            self.dispatch_tasks(store)?;
        }
        self.end_consolidation();
        Ok(used)
    }

    /// Moves the pending branches into the root's in-flight list, oldest first, unless the root
    /// is changing: they wait as pending, read before the tree, until the change is applied.
    fn incorporate_pending(&mut self) -> Result<(), Error> {
        if self.pending.is_empty() || self.node(self.root)?.wants.is_some() {
            return Ok(());
        }
        // A new branch ends a consolidation in progress and unsettles a consolidated layout.
        self.consolidating = false;
        self.consolidated = false;
        if let Some(p) = self.pool.as_mut() {
            p.period_started();
        }
        let root = self.root;
        for b in std::mem::take(&mut self.pending) {
            self.node_mut(root)?.inflight.push(vec![b]);
        }
        self.node_mut(root)?.received = true;
        self.queue(root);
        Ok(())
    }

    /// Idle time's consolidation, when seeks paid for one and nothing else is in progress: the
    /// paid pivots and those above them flush early and the leaves below settle to one branch.
    fn start_consolidation(&mut self) -> Result<(), Error> {
        if !self.consolidate
            || self.consolidating
            || !self.pending.is_empty()
            || self.maintaining()
            || !self.consolidation_owed()
        {
            return Ok(());
        }
        self.mark_paid_paths()?;
        self.consolidating = true;
        self.stats.consolidations_started = self.stats.consolidations_started.saturating_add(1);
        if let Some(p) = self.pool.as_mut() {
            p.period_started();
        }
        for n in 0..self.nodes.len() {
            self.queue(n);
        }
        Ok(())
    }

    /// A consolidation ends with nothing left in progress; a drain's leaves the layout
    /// consolidated until branches enter the root again.
    fn end_consolidation(&mut self) {
        if self.consolidating && !self.maintaining() {
            self.consolidating = false;
            if self.full {
                self.full = false;
                self.consolidated = true;
            }
        }
    }

    /// With nothing queued and nothing out, queues each node with work left: an in-flight
    /// list, a change wanted, or a bound passed. Every change queues the nodes it touches; this
    /// holds that true at a pass's cost of a look over the nodes, taken only when otherwise idle.
    fn sweep(&mut self) {
        if !self.looking.is_empty() || !self.tasks.is_empty() {
            return;
        }
        let fanout = self.config.fanout;
        let leaf_entries = self.leaf_limit();
        for at in 0..self.nodes.len() {
            let due = self.nodes.get(at).is_some_and(|n| {
                !n.inflight.is_empty()
                    || n.wants.is_some()
                    || n.pivots.len() > fanout
                    || n.pivots.iter().any(|p| p.bundle.branches().len() > fanout)
                    || (n.leaf
                        && n.pivots
                            .first()
                            .map(|p| entries(p.bundle.branches()))
                            .unwrap_or(0)
                            > leaf_entries)
            });
            if due {
                self.queue(at);
            }
        }
    }

    /// Looks at the nodes queued when it begins, each once: a node looked at may queue those it
    /// changed, and they wait for the next look ([`Self::run`] looks again once tasks apply).
    fn look<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        for _ in 0..self.looking.len() {
            let Some(n) = self.looking.pop_front() else {
                break;
            };
            if let Some(node) = self.nodes.get_mut(n) {
                node.queued = false;
            }
            self.look_at(store, n)?;
        }
        Ok(())
    }

    /// Node `n`'s triggers: its pivots' compactions, its full pivots' flushes, and the change it
    /// wants, each taken once nothing it would conflict with is in progress.
    fn look_at<F: BlockFile>(&mut self, store: &mut Store<F>, n: usize) -> Result<(), Error> {
        self.drop_dead(store, n)?;
        if self.node(n)?.wants.is_some_and(|c| c.started) {
            return Ok(());
        }
        let pivots = self.node(n)?.pivots.len();
        for i in 0..pivots {
            if self.compaction_due(n, i)? {
                self.plan_compact(n, i)?;
            }
        }
        for i in 0..pivots {
            if self.flush_due(n, i)? {
                self.flush_pivot(n, i)?;
            }
        }
        self.flush_fullest(n)?;
        self.want_change(n)?;
        self.take_change(store, n)
    }

    /// Whether pivot `i` of node `n` has live in-flight bundles and no compaction yet, and is
    /// not held for a child that will split.
    fn compaction_due(&self, n: usize, i: usize) -> Result<bool, Error> {
        let node = self.node(n)?;
        let pivot = node.pivots.get(i).ok_or(corrupt())?;
        Ok(node.inflight.len() > pivot.start
            && !self.compacting(n, &pivot.key)
            && !self.held(pivot)?
            && !(node.leaf && self.leaf_settle(n)?.due))
    }

    /// Leaf `n`'s sources as a settle takes them: its live in-flight bundles with its bundle.
    /// A settle due counting them takes them too, so what an in-flight compaction would write
    /// once before the settle rewrote it is written once (research/53 §5): the settle due once
    /// that compaction's branch had joined the bundle is due now.
    fn leaf_settle(&self, n: usize) -> Result<LeafSettle, Error> {
        let node = self.node(n)?;
        let pivot = node.pivots.first().ok_or(corrupt())?;
        let live = node.inflight.get(pivot.start..).unwrap_or(&[]);
        let live_branches = live
            .iter()
            .map(Vec::len)
            .fold(0usize, usize::saturating_add);
        let live_entries = live
            .iter()
            .map(|b| entries(b))
            .fold(0u64, u64::saturating_add);
        let held = pivot.bundle.branches().len();
        let count = entries(pivot.bundle.branches()).saturating_add(live_entries);
        let after = held.saturating_add(usize::from(live_branches > 0));
        let was_paid = self.consolidating && pivot.seek.paid;
        let paid = was_paid && held.saturating_add(live_branches) > 1;
        Ok(LeafSettle {
            count,
            after,
            was_paid,
            paid,
            due: paid || after > self.config.fanout || count > self.leaf_limit(),
        })
    }

    /// Whether a compaction of node `n`'s pivot keyed `key` is planned or out.
    fn compacting(&self, n: usize, key: &[u8]) -> bool {
        self.tasks.iter().any(|t| {
            t.node == n && matches!(&t.what, What::Compact { key: k, .. } if k.as_slice() == key)
        })
    }

    /// Whether `pivot`'s child wants a change that splits it: the pivot then plans no
    /// compaction, so its bundle stays empty for the parts to replace it.
    fn held(&self, pivot: &Pivot) -> Result<bool, Error> {
        match pivot.child {
            Some(c) => Ok(self.node(c)?.wants.is_some_and(Change::splits)),
            None => Ok(false),
        }
    }

    /// Whether pivot `i` of node `n` flushes its bundle to its child now: past the fanout, the
    /// last flush before its child splits, or a consolidation's through a paid or marked pivot.
    /// A consolidation carries the pivot's data down, so it waits for the pivot's live in-flight
    /// bundles to be compacted into the bundle, and flushes even an empty one, so the rent is
    /// spent and a paid pivot's payment goes down to the child's pivots. Never into a child whose
    /// change has started.
    fn flush_due(&self, n: usize, i: usize) -> Result<bool, Error> {
        let node = self.node(n)?;
        if node.leaf {
            return Ok(false);
        }
        let pivot = node.pivots.get(i).ok_or(corrupt())?;
        let Some(child) = pivot.child else {
            return Ok(false);
        };
        let wants = self.node(child)?.wants;
        if wants.is_some_and(|c| c.started) {
            return Ok(false);
        }
        let held = pivot.bundle.branches().len();
        // The last flush before the child splits comes first, a consolidation's too: the split
        // waits for this bundle to empty and this pivot's compaction waits for the split
        // ([`Self::held`]), so waiting for that compaction here would hold all three.
        if held > 0 && wants.is_some_and(Change::splits) {
            return Ok(true);
        }
        if self.consolidating && (pivot.seek.paid || (self.full && pivot.seek.below)) {
            return Ok(node.inflight.len() <= pivot.start && !self.compacting(n, &pivot.key));
        }
        Ok(held > self.config.fanout)
    }

    /// SplinterDB's pass through an index node that received data (`restore_balance_index`,
    /// trunk.c; research/50 §4): once what it received is compacted into its pivots, its fullest
    /// pivot flushes if it holds more than a memtable's entries, so index pivots hold about that
    /// much and the data lives in the leaves. Once a receipt; never into a child whose change has
    /// started.
    fn flush_fullest(&mut self, n: usize) -> Result<(), Error> {
        let node = self.node(n)?;
        if node.leaf || !node.received || node.pivots.iter().any(|p| node.inflight.len() > p.start)
        {
            return Ok(());
        }
        let mut fullest: Option<(usize, u64)> = None;
        for (i, p) in node.pivots.iter().enumerate() {
            let held = entries(p.bundle.branches());
            if held > self.config.leaf_entries && fullest.is_none_or(|(_, most)| held > most) {
                fullest = Some((i, held));
            }
        }
        self.node_mut(n)?.received = false;
        let Some((i, _)) = fullest else {
            return Ok(());
        };
        let child = self
            .node(n)?
            .pivots
            .get(i)
            .and_then(|p| p.child)
            .ok_or(corrupt())?;
        if self.node(child)?.wants.is_some_and(|c| c.started) {
            return Ok(());
        }
        self.stats.fullest_flushes = self.stats.fullest_flushes.saturating_add(1);
        self.flush_pivot(n, i)
    }

    /// Plans pivot `i` of node `n`'s compaction of its live in-flight bundles: inline without
    /// workers, else a job for them.
    fn plan_compact(&mut self, n: usize, i: usize) -> Result<(), Error> {
        let t = self.timed.then(std::time::Instant::now);
        let node = self.node(n)?;
        let pivot = node.pivots.get(i).ok_or(corrupt())?;
        let (start, covered) = (pivot.start, node.inflight.len());
        let live = node.inflight.get(start..covered).ok_or(corrupt())?;
        let (from, end) = (pivot.key.clone(), Self::pivot_end(node, i));
        let units = live
            .iter()
            .flatten()
            .map(|b| b.count)
            .fold(0u64, u64::saturating_add);
        // Newest first, the order a merge takes.
        let run = if self.pool.is_some() {
            Running::Remote {
                grant: Vec::new(),
                state: State::Ready(Box::new(Task {
                    inputs: live.iter().rev().flatten().map(Branch::for_merge).collect(),
                    from,
                    end,
                    drop_tombstones: false,
                    per: u64::MAX,
                })),
            }
        } else {
            Running::Inline(Box::new(Compaction::prepare(
                live.iter().rev().flatten(),
                from,
                end,
                false,
                u64::MAX,
            )))
        };
        let key = pivot.key.clone();
        self.tasks.push(NodeTask {
            node: n,
            what: What::Compact {
                key,
                start,
                covered,
            },
            run,
            units,
        });
        self.stats.plan_ns = self.stats.plan_ns.saturating_add(ns_since(t));
        Ok(())
    }

    /// Flushes pivot `i` of node `n`: its branches go to the child's in-flight list by
    /// reference, oldest first, and the child is looked at.
    fn flush_pivot(&mut self, n: usize, i: usize) -> Result<(), Error> {
        let cascades = self.consolidating && self.full;
        let pivot = self.node_mut(n)?.pivots.get_mut(i).ok_or(corrupt())?;
        let child = pivot.child.ok_or(corrupt())?;
        let paid = cascades && pivot.seek.paid;
        let bundle = pivot.bundle.take();
        self.spend(n, i)?;
        if !bundle.is_empty() {
            self.stats.flushes = self.stats.flushes.saturating_add(1);
        }
        // A paid flush pays for every pivot of the child it lands in: the bundle spans the
        // child's whole range, so each of its pivots takes part of it, and each must carry its
        // part down in turn until a leaf settles it to one branch.
        if paid {
            let mut newly = 0usize;
            for p in &mut self.node_mut(child)?.pivots {
                if !p.seek.paid {
                    p.seek.paid = true;
                    newly = newly.saturating_add(1);
                }
            }
            self.paid = self.paid.saturating_add(newly);
            self.stats.paid_below = self
                .stats
                .paid_below
                .saturating_add(u64::try_from(newly).unwrap_or(u64::MAX));
        }
        // Oldest first into the child's in-flight list: the bundle is newest first.
        let received = !bundle.is_empty();
        for b in bundle.into_iter().rev() {
            self.node_mut(child)?.inflight.push(vec![b]);
        }
        if received {
            self.node_mut(child)?.received = true;
        }
        self.queue(child);
        Ok(())
    }

    /// Notes the change node `n` wants, if any: a leaf past `fanout` branches or `leaf_entries`
    /// entries, or one seeks paid to consolidate, settles; an index node past `fanout` pivots
    /// splits. A change that splits queues the parent, whose pivot flushes down to it.
    fn want_change(&mut self, n: usize) -> Result<(), Error> {
        let fanout = self.config.fanout;
        let busy = self.tasks.iter().any(|t| t.node == n);
        let node = self.node(n)?;
        if node.wants.is_some() {
            return Ok(());
        }
        let live = node.pivots.iter().any(|p| node.inflight.len() > p.start);
        let kind = if node.leaf {
            let settle = self.leaf_settle(n)?;
            if !settle.due {
                // Paid with one source: nothing to rewrite once the leaf's own compactions are
                // in (a consolidation's flush may still be on its way down), then the rent is
                // spent; the apply of a compaction due or out looks at the leaf again.
                if settle.was_paid && !live && !busy {
                    self.spend(n, 0)?;
                }
                return Ok(());
            }
            ChangeKind::Settle {
                parts: self.leaf_parts(settle.count),
            }
        } else if node.pivots.len() > fanout {
            ChangeKind::Split
        } else {
            return Ok(());
        };
        let change = Change {
            kind,
            started: false,
        };
        self.node_mut(n)?.wants = Some(change);
        if change.splits() && n != self.root {
            let (parent, _) = self.parent_of(n)?;
            self.queue(parent);
        }
        Ok(())
    }

    /// Takes the change node `n` wants once it can: nothing in progress on the node, no live
    /// in-flight bundle, and, for a change that splits it, its parent's pivot empty with no
    /// compaction out. A settle becomes a task; a split, partitioning pivots only, applies now.
    fn take_change<F: BlockFile>(&mut self, store: &mut Store<F>, n: usize) -> Result<(), Error> {
        let Some(change) = self.node(n)?.wants else {
            return Ok(());
        };
        if change.started || self.tasks.iter().any(|t| t.node == n) {
            return Ok(());
        }
        let node = self.node(n)?;
        // A leaf's settle takes its live in-flight bundles; a split waits for them to empty.
        if !node.leaf && node.pivots.iter().any(|p| node.inflight.len() > p.start) {
            return Ok(());
        }
        if change.splits() && n != self.root {
            let (parent, i) = self.parent_of(n)?;
            let pivot = self.node(parent)?.pivots.get(i).ok_or(corrupt())?;
            if !pivot.bundle.branches().is_empty() || self.compacting(parent, &pivot.key) {
                return Ok(());
            }
        }
        match change.kind {
            ChangeKind::Settle { parts } => {
                // The parts as the bundle stands now: a settle that has come to split holds its
                // parent's pivot first, then waits for it to empty. A pivot already empty with no
                // compaction out has no flush left to come, and a flush is what would look at
                // this leaf again, so the split settle is planned now.
                let now = self.settle_parts(n)?;
                if now > 1 && parts <= 1 && n != self.root {
                    self.node_mut(n)?.wants = Some(Change {
                        kind: ChangeKind::Settle { parts: now },
                        started: false,
                    });
                    let (parent, i) = self.parent_of(n)?;
                    let pivot = self.node(parent)?.pivots.get(i).ok_or(corrupt())?;
                    if !pivot.bundle.branches().is_empty() || self.compacting(parent, &pivot.key) {
                        self.queue(parent);
                        return Ok(());
                    }
                }
                self.plan_settle(n)
            }
            ChangeKind::Split => {
                // The in-flight list is empty: every pivot is past it, so a split only
                // partitions the pivots.
                self.drop_dead(store, n)?;
                let parts = self.split_node(n)?;
                self.node_mut(n)?.wants = None;
                self.splice(n, parts)
            }
        }
    }

    /// The leaves leaf `n`'s settle would make, as its bundle and live in-flight bundles stand
    /// ([`Self::leaf_parts`]).
    fn settle_parts(&self, n: usize) -> Result<u64, Error> {
        Ok(self.leaf_parts(self.leaf_settle(n)?.count))
    }

    /// The entries a leaf holds before it splits: `fanout` memtables' (SplinterDB's
    /// `leaf_might_need_to_split`, `incorporation_size_kv_bytes * target_fanout`).
    pub(super) fn leaf_limit(&self) -> u64 {
        let fanout = u64::try_from(self.config.fanout).unwrap_or(u64::MAX);
        self.config.leaf_entries.saturating_mul(fanout)
    }

    /// The leaves a settle of `count` entries makes: one within [`Self::leaf_limit`], else
    /// `round(count / leaf_entries)` (SplinterDB's `leaf_split_target_num_leaves`).
    fn leaf_parts(&self, count: u64) -> u64 {
        if count <= self.leaf_limit() {
            return 1;
        }
        let target = self.config.leaf_entries.max(1);
        count
            .saturating_add(target / 2)
            .checked_div(target)
            .unwrap_or(1)
            .max(1)
    }

    /// Plans leaf `n`'s whole compaction, tombstones dropped, into `ceil(entries /
    /// leaf_entries)` leaves of even size, at least one (SplinterDB rewrites a leaf over `F`
    /// branches in place and splits one over its size into even parts; trunk.c, research/34).
    fn plan_settle(&mut self, n: usize) -> Result<(), Error> {
        let t = self.timed.then(std::time::Instant::now);
        // A leaf seeks paid to consolidate settles to one branch, within the ordinary bounds.
        let settle = self.leaf_settle(n)?;
        if !settle.due {
            if settle.was_paid {
                self.spend(n, 0)?;
            }
            self.node_mut(n)?.wants = None;
            return Ok(());
        }
        let node = self.node(n)?;
        let pivot = node.pivots.first().ok_or(corrupt())?;
        // Its live in-flight bundles with its bundle, newest first: they came down after the
        // bundle's branches were made. No flush reaches the leaf while its change is started
        // ([`Self::flush_due`]), so the range it takes is all there is until it applies.
        let (start, covered) = (pivot.start, node.inflight.len());
        let live = node.inflight.get(start..covered).ok_or(corrupt())?;
        let inputs = live.iter().rev().flatten().chain(pivot.bundle.branches());
        let (from, end) = (pivot.key.clone(), node.end.clone());
        let extents: Vec<u64> = pivot
            .bundle
            .branches()
            .iter()
            .flat_map(|b| b.extents.iter().copied())
            .collect();
        // Its entries counted over every input, an upper bound on its keys.
        let count = settle.count;
        let parts = self.leaf_parts(count);
        let per = crate::util::div_ceil(count, parts).unwrap_or(count).max(1);
        let run = if self.pool.is_some() {
            Running::Remote {
                grant: Vec::new(),
                state: State::Ready(Box::new(Task {
                    inputs: inputs.map(Branch::for_merge).collect(),
                    from,
                    end,
                    drop_tombstones: true,
                    per,
                })),
            }
        } else {
            Running::Inline(Box::new(Compaction::prepare(inputs, from, end, true, per)))
        };
        if settle.was_paid {
            self.spend(n, 0)?;
        }
        if settle.paid {
            self.stats.consolidations = self.stats.consolidations.saturating_add(1);
        }
        let cause = if settle.paid {
            Cause::Seeks
        } else if settle.after > self.config.fanout {
            Cause::Branches
        } else {
            Cause::Size
        };
        self.tasks.push(NodeTask {
            node: n,
            what: What::Settle {
                extents,
                cause,
                start,
                covered,
            },
            run,
            units: count,
        });
        if let Some(c) = self.node_mut(n)?.wants.as_mut() {
            c.started = true;
        }
        self.stats.plan_ns = self.stats.plan_ns.saturating_add(ns_since(t));
        Ok(())
    }

    /// Steps the inline tasks for up to `budget` units, the root's first: each finished one
    /// keeps its parts to apply. A step that stops for a page still being read ends the pass.
    fn step_inline<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        budget: u64,
    ) -> Result<u64, Error> {
        let mut used = 0u64;
        // The merging timed where it runs, a clock read a task's step, so a step with no inline
        // task reads none: a compacted key's measured cost ([`Trunk::note_merge`]).
        let mut merge_ns = 0u64;
        let root = self.root;
        // Two passes, the root's tasks first: they free room for pending branches.
        'passes: for roots in [true, false] {
            for at in 0..self.tasks.len() {
                if self.tasks.get(at).is_none_or(|t| (t.node == root) != roots) {
                    continue;
                }
                if used >= budget || self.io_waiting {
                    break 'passes;
                }
                let yield_io = self.yield_io;
                let Some(task) = self.tasks.get_mut(at) else {
                    break 'passes;
                };
                let Running::Inline(c) = &mut task.run else {
                    continue;
                };
                let node = self.nodes.get(task.node).ok_or(corrupt())?;
                // A task about to write takes its output's room first, as a worker's job takes
                // its grant (`dispatch_tasks`): the inputs' extents and one more a part, the
                // file grown in place where the shard may, else asked of the shard's growth and
                // the task stepped once it has grown.
                if c.opening() && store.segments().is_some() {
                    let inputs: Vec<&Branch> = match &task.what {
                        What::Compact { start, covered, .. } => node
                            .inflight
                            .get(*start..*covered)
                            .ok_or(corrupt())?
                            .iter()
                            .flatten()
                            .collect(),
                        What::Settle { start, covered, .. } => node
                            .inflight
                            .get(*start..*covered)
                            .ok_or(corrupt())?
                            .iter()
                            .rev()
                            .flatten()
                            .chain(node.pivots.first().ok_or(corrupt())?.bundle.branches())
                            .collect(),
                    };
                    let extents = inputs
                        .iter()
                        .map(|b| u64::try_from(b.extents.len()).unwrap_or(u64::MAX))
                        .fold(0u64, u64::saturating_add);
                    let parts = match &task.what {
                        What::Compact { .. } => 1,
                        What::Settle { .. } => self.leaf_parts(entries_of_refs(&inputs)),
                    };
                    let asked = extents.saturating_add(parts).saturating_add(1);
                    if store.capacity() < asked {
                        if store.grows_in_place() {
                            store.grow_now(asked)?;
                        }
                        if store.capacity() < asked {
                            self.capacity_wanted = self.capacity_wanted.max(asked);
                            continue;
                        }
                    }
                }
                let Some(task) = self.tasks.get_mut(at) else {
                    break 'passes;
                };
                let Running::Inline(c) = &mut task.run else {
                    continue;
                };
                let node = self.nodes.get(task.node).ok_or(corrupt())?;
                c.set_yield(yield_io);
                let left = budget.saturating_sub(used);
                let merging = std::time::Instant::now();
                let step = match &task.what {
                    What::Compact { start, covered, .. } => {
                        let live = node.inflight.get(*start..*covered).ok_or(corrupt())?;
                        c.step(store, live.iter().rev().flatten(), left)
                    }
                    What::Settle { start, covered, .. } => {
                        let live = node.inflight.get(*start..*covered).ok_or(corrupt())?;
                        let bundle = node.pivots.first().ok_or(corrupt())?.bundle.branches();
                        c.step(store, live.iter().rev().flatten().chain(bundle), left)
                    }
                };
                merge_ns = merge_ns.saturating_add(ns_since(Some(merging)));
                let spent = match step {
                    Ok(spent) => spent,
                    Err(error) => {
                        // Opening was canceled before any output was built: the task stays, so a
                        // caller can repair the read and resume. A later merge error may have
                        // consumed output and is not restartable: the task goes and is planned anew.
                        if !c.opening() {
                            let task = self.tasks.remove(at);
                            if let What::Settle { .. } = task.what
                                && let Some(c) = self.node_mut(task.node)?.wants.as_mut()
                            {
                                c.started = false;
                            }
                            self.queue(task.node);
                        }
                        return Err(error);
                    }
                };
                used = used.saturating_add(spent);
                self.io_waiting |= c.waiting();
                if c.is_done() {
                    let Running::Inline(c) =
                        std::mem::replace(&mut task.run, Running::Finished(Vec::new()))
                    else {
                        return Err(corrupt());
                    };
                    let finishing = std::time::Instant::now();
                    let parts = (*c).finish(store)?;
                    let ns = ns_since(Some(finishing));
                    merge_ns = merge_ns.saturating_add(ns);
                    if self.timed {
                        self.stats.finish_ns = self.stats.finish_ns.saturating_add(ns);
                    }
                    if let Some(task) = self.tasks.get_mut(at) {
                        task.run = Running::Finished(parts);
                    }
                }
            }
        }
        self.note_merge(merge_ns, used);
        Ok(used)
    }

    /// Applies every finished task, in plan order: each changes only its own node (and, for a
    /// settle that splits, its parent's pivot, held empty for it), and queues what it changed.
    fn apply_done<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        let mut at = 0usize;
        while let Some(task) = self.tasks.get(at) {
            if !task.run.done() {
                at = at.saturating_add(1);
                continue;
            }
            let task = self.tasks.remove(at);
            self.apply_task(store, task)?;
        }
        Ok(())
    }

    fn apply_task<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        task: NodeTask,
    ) -> Result<(), Error> {
        let t = self.timed.then(std::time::Instant::now);
        let NodeTask {
            node: n, what, run, ..
        } = task;
        let parts = match run {
            Running::Finished(parts) => parts,
            Running::Remote {
                state: State::Done(mut out),
                ..
            } => {
                // The pages the job wrote, cached where they were written, with its branches.
                if let Some(kept) = out.kept.take() {
                    store.adopt_pages(*kept);
                }
                store.grant_back(&out.unused)?;
                store.extend_end(out.end);
                // The worker timed its merge: a compacted key's cost, which consolidations are
                // priced at, measured wherever compactions run.
                self.note_merge(out.ns, out.units);
                out.parts
            }
            _ => return Err(corrupt()),
        };
        match what {
            What::Compact {
                key,
                start,
                covered,
            } => {
                let node = self.node(n)?;
                let i = node
                    .pivots
                    .iter()
                    .position(|p| p.key == key && p.start == start)
                    .ok_or(corrupt())?;
                let merged = parts.into_iter().next().map(|(_, b)| b);
                self.apply_pivot(n, i, merged, covered)?;
                self.drop_dead(store, n)?;
                self.queue(n);
                // A child waiting for this pivot to be empty may take its change now.
                if let Some(c) = self.node(n)?.pivots.get(i).and_then(|p| p.child) {
                    self.queue(c);
                }
            }
            What::Settle {
                extents,
                cause,
                start,
                covered,
            } => {
                let took = self
                    .node(n)?
                    .inflight
                    .get(start..covered)
                    .ok_or(corrupt())?
                    .iter()
                    .map(|b| entries(b))
                    .fold(0u64, u64::saturating_add);
                self.stats.settle_inflight_entries =
                    self.stats.settle_inflight_entries.saturating_add(took);
                let leaves = self.apply_settle(store, n, parts, &extents, cause)?;
                // The in-flight bundles it took are read by none now: the leaf reads past them.
                if let Some(p) = self.node_mut(n)?.pivots.first_mut() {
                    p.start = covered;
                }
                self.drop_dead(store, n)?;
                self.node_mut(n)?.wants = None;
                self.splice(n, leaves)?;
            }
        }
        self.stats.finish_ns = self.stats.finish_ns.saturating_add(ns_since(t));
        Ok(())
    }

    /// Node `n`'s change applied: the nodes now covering its range, each with its first key,
    /// replace its parent's pivot for it, which was held empty for them (reading the parent's
    /// in-flight bundles from the pivot's start, as it did), or become a new root's children.
    fn splice(&mut self, n: usize, parts: Vec<(Vec<u8>, usize)>) -> Result<(), Error> {
        for &(_, p) in &parts {
            self.queue(p);
        }
        if n == self.root {
            if parts.len() > 1 {
                self.grow_root(parts);
                self.queue(self.root);
            }
            return Ok(());
        }
        let (parent, i) = self.parent_of(n)?;
        if parts.len() > 1 {
            // The pivot the parts replace takes its rent with it.
            self.spend(parent, i)?;
            let node = self.node_mut(parent)?;
            let old = node.pivots.get(i).ok_or(corrupt())?;
            if !old.bundle.branches().is_empty() {
                return Err(corrupt());
            }
            let (first, start) = (old.key.clone(), old.start);
            let mut replacement: Vec<Pivot> = parts
                .into_iter()
                .map(|(key, child)| Pivot {
                    key,
                    child: Some(child),
                    bundle: Bundle::default(),
                    start,
                    seek: SeekRent::default(),
                })
                .collect();
            if let Some(p) = replacement.first_mut() {
                p.key = first;
            }
            node.pivots.splice(i..=i, replacement);
        }
        self.queue(parent);
        Ok(())
    }

    /// The node naming `n` as a pivot's child, and that pivot.
    fn parent_of(&self, n: usize) -> Result<(usize, usize), Error> {
        self.nodes
            .iter()
            .enumerate()
            .find_map(|(p, node)| {
                node.pivots
                    .iter()
                    .position(|q| q.child == Some(n))
                    .map(|i| (p, i))
            })
            .ok_or(corrupt())
    }

    /// Whether a job's tasks may take the cores: when a pending branch or a full root waits on
    /// them, a step waits for them anyway, or a consolidation seeks paid for is in progress
    /// (every seek pays rent until it is done, and it runs beside them on the cores the shard
    /// leaves); otherwise the pool's measured need.
    fn admit(&self) -> bool {
        self.wait != Wait::No || !self.pending.is_empty() || !self.has_room() || self.consolidating
    }

    /// Hands out each ready job whose inputs have landed (waited for when the step waits),
    /// the root's first, while a worker is free, each with a grant sized to its inputs: their
    /// extents and one more a part it may make. A job that needs more asks for it, and the pool
    /// answers from the store ([`Pool::take`]).
    fn dispatch_tasks<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        let wait = self.wait != Wait::No;
        let admit = self.admit();
        let root = self.root;
        // Two passes, the root's jobs first: they free room for pending branches.
        for roots in [true, false] {
            for at in 0..self.tasks.len() {
                if self.tasks.get(at).is_none_or(|t| (t.node == root) != roots) {
                    continue;
                }
                let Some(NodeTask {
                    run: Running::Remote { grant, state },
                    ..
                }) = self.tasks.get_mut(at)
                else {
                    continue;
                };
                let State::Ready(task) = &*state else {
                    continue;
                };
                let Some(pool) = self.pool.as_mut() else {
                    return Ok(());
                };
                if !pool.can_take(store, admit)? {
                    return Ok(());
                }
                let mut landed = true;
                for b in &task.inputs {
                    if wait {
                        store.settle_extents(&b.extents)?;
                    } else if !store.landed(&b.extents)? {
                        landed = false;
                        break;
                    }
                }
                if !landed {
                    continue;
                }
                let extents = task
                    .inputs
                    .iter()
                    .map(|b| b.extents.len())
                    .fold(0usize, usize::saturating_add);
                let entries = task
                    .inputs
                    .iter()
                    .map(|b| b.count)
                    .fold(0u64, u64::saturating_add);
                let parts = crate::util::div_ceil(entries, task.per.max(1))
                    .unwrap_or(entries)
                    .saturating_add(1);
                let parts = usize::try_from(parts).unwrap_or(usize::MAX);
                let needed = extents.saturating_add(parts);
                let asked = u64::try_from(needed).unwrap_or(u64::MAX);
                if store.segments().is_some() && store.capacity() < asked {
                    if store.grows_in_place() {
                        // Where the shard may wait, its file grows by the grant here.
                        store.grow_now(asked)?;
                    }
                    if store.capacity() < asked {
                        // The file's admitted extents cannot hold it yet: the shard grows the
                        // file by at least this much, and the job goes out once they can.
                        self.capacity_wanted = self.capacity_wanted.max(asked);
                        continue;
                    }
                }
                let given = store.grant(needed)?;
                let State::Ready(task) = std::mem::replace(state, State::Dropped) else {
                    continue;
                };
                grant.clone_from(&given);
                // The cache's room for what it writes: at most the pages its inputs hold in its
                // range, as a merge writes no more entries than it reads; a pivot's compaction
                // reads its slice of inputs that span the node.
                let pages = task
                    .inputs
                    .iter()
                    .map(|b| b.pages_between(&task.from, task.end.as_deref()))
                    .fold(0usize, usize::saturating_add);
                let job = Box::new(pool::Job {
                    work: Work::Compact(*task),
                    grant: given,
                    file_end: store.end(),
                    generation: store.generation(),
                    keep: store.lend_pages(pages),
                });
                let pool = self.pool.as_mut().ok_or(no_pool())?;
                let sent = match pool.send(job, Owner::Trunk, admit) {
                    Ok(sent) => sent,
                    Err((error, job)) => {
                        let job = *job;
                        if let Some(kept) = job.keep {
                            store.unlend_pages(*kept);
                        }
                        let Work::Compact(task) = job.work else {
                            return Err(corrupt());
                        };
                        *state = State::Ready(Box::new(task));
                        // Restoration precedes cleanup: a refused grant return keeps both the task
                        // and its owned extent list available to terminal handling.
                        store.grant_back(&job.grant)?;
                        grant.clear();
                        return Err(error);
                    }
                };
                match sent {
                    Ok(ticket) => *state = State::Out(ticket),
                    Err(job) => {
                        let job = *job;
                        if let Some(kept) = job.keep {
                            store.unlend_pages(*kept);
                        }
                        let Work::Compact(task) = job.work else {
                            return Err(corrupt());
                        };
                        *state = State::Ready(Box::new(task));
                        store.grant_back(&job.grant)?;
                        grant.clear();
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    /// Hands out ready jobs and takes the workers' results, for as long as [`Wait`] says: not
    /// at all, one, or every job out. A failed job starts the abandonment of every other.
    fn tend_tasks<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        let mut taken = 0usize;
        // Each loop takes a result or returns, and a task gives one result at most, since none is
        // planned or applied here: the tasks bound it, and a last loop finds none out.
        for _ in 0..=self.tasks.len() {
            self.dispatch_tasks(store)?;
            if !self.tasks.iter().any(|t| t.run.out()) {
                return Ok(());
            }
            let wait = match self.wait {
                Wait::No => false,
                Wait::One => taken == 0,
                Wait::All => true,
            };
            let pool = self.pool.as_mut().ok_or(no_pool())?;
            let Some(back) = pool.take(store, Owner::Trunk, wait)? else {
                if wait && !pool.out_for(Owner::Trunk) && pool.out_for(Owner::Pack) {
                    // Its input may still need feeding: return to the owning shard rather than
                    // wait here for a result that depends on that same shard.
                    pool.want_message();
                }
                self.workers_waiting = true;
                return Ok(());
            };
            taken = taken.saturating_add(1);
            let task = self.tasks.iter_mut().find(|t| {
                matches!(&t.run, Running::Remote { state: State::Out(k), .. } if *k == back.ticket)
            });
            match (task, back.result) {
                (
                    Some(NodeTask {
                        run: Running::Remote { grant, state },
                        ..
                    }),
                    Ok(mut out),
                ) => {
                    grant.extend_from_slice(&back.topped);
                    out.kept = back.kept;
                    *state = State::Done(out);
                }
                (task, result) => {
                    if let Some(kept) = back.kept {
                        store.unlend_pages(*kept);
                    }
                    if let Some(NodeTask {
                        run: Running::Remote { grant, state },
                        ..
                    }) = task
                    {
                        grant.extend_from_slice(&back.topped);
                        *state = State::Dropped;
                    }
                    let error = match result {
                        Err(error) => error,
                        Ok(_) => Error::InvalidArgument {
                            what: "a maintenance worker's result for no job of its",
                        },
                    };
                    self.abandoning = Some(error);
                    return self.abandon_tasks(store);
                }
            }
        }
        Ok(())
    }

    /// A job failed: every other one out is waited for (as far as [`Wait`] lets this step) and
    /// every grant released, outputs dropped unread (their extents are the grants'), and the
    /// failure reported once none is out; the nodes keep their state, so the tasks are planned
    /// anew.
    fn abandon_tasks<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        let wait = self.wait != Wait::No;
        // Each loop takes a job back or returns: the jobs out bound it.
        for _ in 0..=self.tasks.len() {
            if !self.tasks.iter().any(|t| t.run.out()) {
                break;
            }
            let pool = self.pool.as_mut().ok_or(no_pool())?;
            let Some(back) = pool.take(store, Owner::Trunk, wait)? else {
                self.workers_waiting = true;
                return Ok(());
            };
            if let Some(kept) = back.kept {
                store.unlend_pages(*kept);
            }
            for t in &mut self.tasks {
                if let Running::Remote { grant, state } = &mut t.run
                    && matches!(state, State::Out(k) if *k == back.ticket)
                {
                    grant.extend_from_slice(&back.topped);
                    *state = State::Dropped;
                }
            }
        }
        if self.tasks.iter().any(|t| t.run.out()) {
            self.workers_waiting = true;
            return Ok(());
        }
        let mut kept = Vec::with_capacity(self.tasks.len());
        for t in std::mem::take(&mut self.tasks) {
            match t.run {
                Running::Remote { grant, state } => {
                    if let State::Done(out) = state
                        && let Some(kept) = out.kept
                    {
                        store.unlend_pages(*kept);
                    }
                    for e in grant {
                        store.release(e)?;
                    }
                    if let What::Settle { .. } = t.what
                        && let Some(c) = self.node_mut(t.node)?.wants.as_mut()
                    {
                        c.started = false;
                    }
                    self.queue(t.node);
                }
                _ => kept.push(t),
            }
        }
        self.tasks = kept;
        match self.abandoning.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Node `n`'s in-flight list lost its first `dead` bundles: its tasks' planned ranges move
    /// down with the list.
    pub(super) fn shift_tasks(&mut self, n: usize, dead: usize) {
        for t in &mut self.tasks {
            if t.node == n
                && let What::Compact { start, covered, .. } | What::Settle { start, covered, .. } =
                    &mut t.what
            {
                *start = start.saturating_sub(dead);
                *covered = covered.saturating_sub(dead);
            }
        }
    }

    /// An inline compaction before the runtime's workers are prepared.
    pub(super) fn inline_out(&self) -> bool {
        self.tasks
            .iter()
            .any(|t| matches!(t.run, Running::Inline(_)))
    }
}

/// The entries of `branches`, an upper bound on their keys.
fn entries_of_refs(branches: &[&Branch]) -> u64 {
    branches
        .iter()
        .map(|b| b.count)
        .fold(0u64, u64::saturating_add)
}

fn entries(branches: &[Branch]) -> u64 {
    branches
        .iter()
        .map(|b| b.count)
        .fold(0u64, u64::saturating_add)
}
