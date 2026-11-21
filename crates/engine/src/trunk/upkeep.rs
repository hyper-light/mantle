//! The trunk's maintenance as flat per-node tasks (CLAUDE.md §10; docs/design/engine-structure.md
//! §6, "Flat maintenance"). A node's triggers plan its pivots' compactions, flush its full pivot
//! bundles by reference and take the change it wants (a leaf's settle, an index node's split); one
//! pass with a counted budget looks at the queued nodes, steps the inline tasks, hands out and
//! takes back the workers' jobs and applies what is done. No task waits on another node's: a node
//! held back by its parent's pivot is looked at again once that pivot's task is applied.

use super::*;

#[path = "tasks.rs"]
mod tasks;
pub(super) use tasks::Tasks;

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
/// settle, inline or out on a worker; `units` its inputs' entries when planned, its debt; and,
/// out on a worker, its reads counted against the kernel's page cache's window when planned,
/// which price its reads' waits when it comes back ([`Reuse`]).
#[derive(Debug)]
pub(super) struct NodeTask {
    node: usize,
    what: What,
    run: Running,
    units: u64,
    reuse: Reuse,
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
    /// A copy of the node's branch whose root page is `root`, which replaces it where it stands
    /// as it applies, its pages moved out of extents mostly dead (cleaning,
    /// docs/design/engine-structure.md §3).
    Relocate { root: u64, ask: u64 },
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
    /// Paid, with a flush still to come down its path in this consolidation
    /// ([`Trunk::flush_to_come`]): it settles once that flush is in, not before and again after.
    waits: bool,
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

/// What a pivot does with its bundle now ([`Trunk::flush_due`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Flush {
    /// Flushes it to its child.
    Now,
    /// Is due to, but its child, named, takes no flush until its live in-flight bundles fall
    /// below the fanout.
    Held(usize),
    /// Keeps it.
    No,
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
        let live = self.nodes.get(self.root).map_or(0, Node::live_most);
        self.pending.len().saturating_add(live) < self.config.fanout
    }

    /// Whether node `n` takes a flush: fewer than `fanout` live in-flight bundles for every pivot,
    /// the bound the root's [`Self::has_room`] keeps for the pending branches. A flush lands up to
    /// `fanout + 1` more (a bundle past the fanout), so a node holds at most `2 fanout`, and
    /// `3 fanout + 1` for the moment between the last flush before its split, which does not wait
    /// ([`Self::flush_due`]), and the split, which takes them.
    fn takes_flush(&self, n: usize) -> Result<bool, Error> {
        Ok(self.node(n)?.live_most() < self.config.fanout)
    }

    /// Whether maintenance is in progress: tasks out, nodes to look at, a change wanted, or a
    /// failure being abandoned; owed even when [`Self::debt`] counts nothing.
    pub fn maintaining(&self) -> bool {
        !self.tasks.is_empty()
            || !self.looking.is_empty()
            || self.abandoning.is_some()
            || self.wanting_nodes > 0
    }

    /// Whether nothing is pending or in progress and every node's in-flight list is empty, which
    /// a checkpoint's image relies on.
    pub fn is_idle(&self) -> bool {
        self.pending.is_empty() && !self.maintaining() && self.inflight_nodes == 0
    }

    /// The extents the next checkpoint takes besides the trunk's image, which maintenance leaves
    /// free: the memtables it packs and the allocator's map, as the shard counts them.
    pub(crate) fn set_checkpoint_room(&mut self, extents: u64) {
        if self.checkpoint_room != extents {
            self.checkpoint_room = extents;
            self.room_kept = None;
        }
    }

    /// The extents maintenance leaves free for the next checkpoint: its memtables' and map's
    /// ([`Self::set_checkpoint_room`]) and the trunk's image as it stands, counted once a pass.
    /// An extent freed while the durable checkpoint names it is not free until the next one is
    /// durable (`Allocator::release`), so without this room every write of the checkpoint that
    /// would free them could be refused, and the store would stay full.
    fn room_kept<F: BlockFile>(&mut self, store: &Store<F>) -> Result<u64, Error> {
        if let Some(kept) = self.room_kept {
            return Ok(kept);
        }
        let kept = self
            .checkpoint_room
            .saturating_add(self.image_extents_now(store)?)
            .saturating_add(self.clean_room);
        self.room_kept = Some(kept);
        Ok(kept)
    }

    /// The cleaner's reserve: the extents the largest relocation it makes may take, which every
    /// other task, grant and rotation leaves free beside the checkpoint's room, and a relocation
    /// alone may take ([`Self::step_inline`]), as Sprite LFS cleans before its clean segments run
    /// out (research/34 §6). A relocation's copy takes at most the extents its pages fill
    /// ([`Self::inline_asked`]), so one of a branch of at most `extents` extents' pages always
    /// has its room once the inline tasks writing now are done.
    pub(crate) fn set_clean_room(&mut self, extents: u64) {
        if self.clean_room != extents {
            self.clean_room = extents;
            self.room_kept = None;
        }
    }

    /// The cleaner's reserve ([`Self::set_clean_room`]).
    pub(crate) fn clean_room(&self) -> u64 {
        self.clean_room
    }

    /// While maintenance waits for room and no relocation is out, plans the relocation that frees
    /// the most extents net of its copy's for each page it copies, among the branches of nodes
    /// with no task and no change begun whose copy fits the store's free extents beside the room
    /// every other task leaves: Sprite LFS's cost-benefit, the free space a cleaning generates
    /// against the pages it reads and writes, with no age (research/34 §6). A branch holds an
    /// extent alone where the extent's live pages are its own pages there ([`Self::live`]), and
    /// such an extent is freed when it goes; the copy takes the extents its pages fill
    /// ([`Self::inline_asked`]). The cleaner's reserve is then that copy's room, which every other
    /// task, grant and rotation leaves free until it applies or is dropped: free when planned, it
    /// stays free for the copy, so the relocation never starts without its room and proceeds.
    fn plan_cleaning<F: BlockFile>(&mut self, store: &Store<F>) -> Result<(), Error> {
        if self.room_short.is_none() || self.clean_room > 0 {
            return Ok(());
        }
        let free = store
            .capacity()
            .saturating_sub(self.room_kept(store)?)
            .saturating_sub(self.inline_room(store)?);
        let per = u64::from(self.extent_pages).max(1);
        // (gain, pages, node, root) of the best so far.
        let mut best: Option<(u64, u64, usize, u64)> = None;
        for (n, node) in self.nodes.iter().enumerate() {
            if self.tasks.busy(n) || node.wants.is_some_and(|c| c.started) {
                continue;
            }
            let branches = node
                .inflight
                .iter()
                .flat_map(|f| f.branches.iter())
                .chain(node.pivots.iter().flat_map(|p| p.bundle.branches().iter()));
            for b in branches {
                let pages = u64::try_from(b.pages_between(b"", None)).unwrap_or(u64::MAX);
                let ask = pages.div_ceil(per);
                if ask == 0 || ask > free {
                    continue;
                }
                let alone = super::pages_by_extent(b, per)
                    .filter(|&(e, own)| u64::from(self.live_pages(e)) == own)
                    .count();
                let gain = u64::try_from(alone).unwrap_or(0).saturating_sub(ask);
                // More extents a page copied: gain / pages, compared crosswise.
                let better = best.is_none_or(|(g, p, ..)| {
                    u128::from(gain).saturating_mul(u128::from(p))
                        > u128::from(g).saturating_mul(u128::from(pages))
                });
                if gain > 0 && better {
                    best = Some((gain, pages, n, b.root));
                }
            }
        }
        if let Some((_, _, n, root)) = best {
            let ask = self.plan_relocation(n, root)?;
            self.set_clean_room(ask);
        }
        Ok(())
    }

    /// Plans the relocation of node `n`'s branch whose root page is `root`: a copy of it alone,
    /// its tombstones kept, written inline a budget at a time and waiting for room as a
    /// compaction does, which replaces it where it stands as it applies, the original's extents
    /// then released, free at the next checkpoint. Its node plans nothing else and takes no
    /// change while it is out ([`Self::look_at`]), so the branch stays where it is; one whose
    /// branch is no longer there as it applies is discarded, its copy released. The copy keeps
    /// the branch's life: a leaf's bundle's long, any other short. Refused for a node with a task
    /// or a change begun, and for a branch the node does not hold.
    pub(crate) fn plan_relocation(&mut self, n: usize, root: u64) -> Result<u64, Error> {
        let other = self.tasks.on(n).any(|t| match t.what {
            What::Relocate { root: r, .. } => r == root,
            _ => true,
        });
        if other || self.node(n)?.wants.is_some_and(|c| c.started) {
            return Err(Error::InvalidArgument {
                what: "a relocation beside another task of its node",
            });
        }
        let per = u64::from(self.extent_pages).max(1);
        let node = self.node(n)?;
        let (b, settled) = holding(node, root).ok_or(Error::InvalidArgument {
            what: "a relocation of a branch its node does not hold",
        })?;
        let units = b.count;
        let ask = u64::try_from(b.pages_between(b"", None))
            .unwrap_or(u64::MAX)
            .div_ceil(per);
        let mut copy = Compaction::prepare(std::iter::once(b), Vec::new(), None, false, u64::MAX);
        copy.set_life(if settled {
            crate::store::Life::Long
        } else {
            crate::store::Life::Short
        });
        let top = self.root;
        self.tasks.push(
            NodeTask {
                node: n,
                what: What::Relocate { root, ask },
                run: Running::Inline(Box::new(copy)),
                units,
                reuse: Reuse::default(),
            },
            top,
        )?;
        self.stats.relocations_planned = self.stats.relocations_planned.saturating_add(1);
        Ok(ask)
    }

    /// Whether maintenance waits for extents only a checkpoint frees: a task of the last pass
    /// found the store without its room beside the checkpoint's ([`Self::room_kept`]), no extent
    /// has been freed or admitted since, and no task holds inputs, is out on a worker or has
    /// output to apply. The trunk can then be saved as it
    /// stands ([`Self::drop_unstarted`]), and the checkpoint frees the extents the one before
    /// named.
    pub(crate) fn stalled_for_room<F: BlockFile>(&self, store: &Store<F>) -> bool {
        self.waits_for_room(store) && !self.tasks.is_empty() && self.none_begun()
    }

    /// Whether no task has begun: none holds output, is out on a worker or is being abandoned,
    /// so none gives extents back ([`Self::stalled_for_room`]).
    pub(crate) fn none_begun(&self) -> bool {
        self.abandoning.is_none()
            && self.tasks.inline_or_ready()
            && self
                .tasks
                .inline()
                .all(|t| matches!(&t.run, Running::Inline(c) if c.opening()))
    }

    /// Whether maintenance or a rotation waits for extents the store does not have free beside
    /// the checkpoint's room ([`Self::room_kept`]), and none has been freed or admitted since.
    pub(crate) fn waits_for_room<F: BlockFile>(&self, store: &Store<F>) -> bool {
        self.room_short.is_some_and(|need| store.capacity() < need)
    }

    /// The room a wait asks of the store, the checkpoint's included: kept until the store has it,
    /// so neither a checkpoint's own extents coming back nor tasks dropped for one end it.
    pub(crate) fn room_waited(&self) -> u64 {
        self.room_short.unwrap_or(0)
    }

    /// A task or a rotation waits for `need` extents free ([`Self::waits_for_room`]).
    pub(crate) fn note_room_short(&mut self, need: u64) {
        self.room_short = Some(self.room_short.map_or(need, |was| was.max(need)));
    }

    /// Drops every task that has not begun: an inline one still opening, no output built, and
    /// a worker's job not handed out, neither of which holds an extent. An opening's partial
    /// cursors give their read buffers and unclaimed reads back to the store first. Each is
    /// planned anew once its node is looked at again, as a node changed under a task replans it
    /// (CLAUDE.md §10). Refused while a task has begun.
    pub(crate) fn drop_unstarted<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
    ) -> Result<(), Error> {
        if !self.stalled_for_room(store) && !self.tasks.is_empty() {
            return Err(Error::InvalidArgument {
                what: "unstarted tasks dropped beside one begun",
            });
        }
        for mut t in self.tasks.take_all() {
            if let Running::Inline(c) = &mut t.run {
                c.cancel_opening(store);
            }
            if let What::Relocate { ask, .. } = t.what {
                self.set_clean_room(self.clean_room.saturating_sub(ask));
            }
            if let What::Settle { .. } = t.what
                && let Some(c) = self.node_mut(t.node)?.wants.as_mut()
            {
                c.started = false;
            }
            self.queue(t.node);
        }
        Ok(())
    }

    /// Appends `bundle` to node `n`'s in-flight list, counting the node among those holding one.
    fn push_inflight(&mut self, n: usize, bundle: super::InFlight) -> Result<(), Error> {
        let node = self.node_mut(n)?;
        let was_empty = node.inflight.is_empty();
        node.inflight.push(bundle);
        let live = u64::try_from(node.live_most()).unwrap_or(u64::MAX);
        self.stats.inflight_most = self.stats.inflight_most.max(live);
        if was_empty {
            self.inflight_nodes = self.inflight_nodes.checked_add(1).ok_or(corrupt())?;
        }
        Ok(())
    }

    /// Node `n` wants `change`, counted among the nodes wanting one if it wanted none.
    fn set_wants(&mut self, n: usize, change: Change) -> Result<(), Error> {
        if self.node_mut(n)?.wants.replace(change).is_none() {
            self.wanting_nodes = self.wanting_nodes.checked_add(1).ok_or(corrupt())?;
        }
        Ok(())
    }

    /// Node `n` wants no change, and leaves the count if it wanted one.
    fn clear_wants(&mut self, n: usize) -> Result<(), Error> {
        if self.node_mut(n)?.wants.take().is_some() {
            self.wanting_nodes = self.wanting_nodes.checked_sub(1).ok_or(corrupt())?;
        }
        Ok(())
    }

    /// The counts [`Self::is_idle`] reads, against a look at every node: a corruption if they
    /// differ.
    pub(crate) fn check_idle_counts(&self) -> Result<(), Error> {
        let inflight = self.nodes.iter().filter(|n| !n.inflight.is_empty()).count();
        let wanting = self.nodes.iter().filter(|n| n.wants.is_some()).count();
        if inflight == self.inflight_nodes && wanting == self.wanting_nodes {
            Ok(())
        } else {
            Err(Error::Corruption {
                what: "the trunk's counts of nodes with in-flight bundles and wanting a change",
                why: Malformed::CountMismatch,
            })
        }
    }

    /// The maintenance estimate, in work units: the pending branches' entries, what inline
    /// compactions have left, and the planned entries of those on the workers. Both are kept as
    /// running totals (`pending_entries`, [`Tasks`]): every put asks for it, and the sum over
    /// every task each ask took was 4.4% of a busy shard thread's samples (macOS `sample`,
    /// 2026-11-20).
    pub fn debt(&self) -> u64 {
        self.pending_entries.saturating_add(self.tasks.owed())
    }

    /// The pending branches' entries and the tasks' indices and debt, against a look at each: a
    /// corruption if they differ.
    #[cfg(test)]
    pub(crate) fn check_tasks(&self) -> Result<(), Error> {
        let pending = self
            .pending
            .iter()
            .map(super::Pending::entries)
            .fold(0, u64::saturating_add);
        if pending != self.pending_entries {
            return Err(Error::Corruption {
                what: "the trunk's count of pending entries",
                why: Malformed::CountMismatch,
            });
        }
        self.check_live()?;
        self.tasks.check(self.root)
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
            if !self.has_room() && now == mark && self.stalled_for_room(store) {
                // What would make room waits for extents only a checkpoint frees: refused,
                // typed, for the caller to checkpoint and ask again.
                return Err(Error::LimitExceeded {
                    what: "extents of a store free beside a checkpoint's room",
                    limit: store.capacity(),
                });
            }
            if !self.has_room() && now == mark && self.tasks.is_empty() && self.looking.is_empty() {
                return Err(Error::InvalidArgument {
                    what: "a trunk whose root no task can make room in",
                });
            }
            mark = now;
        }
        Ok(())
    }

    /// [`Self::make_room`] while its tasks find extents: true once the root has room, false
    /// once what would make it waits for extents only a checkpoint frees
    /// ([`Self::stalled_for_room`]).
    pub(crate) fn make_room_or_stall<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
    ) -> Result<bool, Error> {
        match self.make_room(store) {
            Ok(()) => Ok(true),
            Err(Error::LimitExceeded { .. }) if self.stalled_for_room(store) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Runs all maintenance to the end, each step waiting on the workers, or until what is left
    /// waits for extents only a checkpoint frees ([`Self::stalled_for_room`]). Refused when a
    /// step leaves work, changes nothing, and leaves no task out and no node to look at: what is
    /// left, no task can do.
    pub fn drain<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        let mut mark = self.progress();
        while !self.is_idle() {
            self.step(store, u64::MAX)?;
            let now = self.progress();
            if !self.is_idle() && now == mark && self.stalled_for_room(store) {
                // What is left waits for extents the durable checkpoint still names: the next
                // checkpoint frees them, and saves the trunk as it stands ([`Self::saveable`]).
                return Ok(());
            }
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
        self.know_extent_pages(store)?;
        // A wait for room lasts until the store has the room it waits for.
        if self.room_short.is_some_and(|need| store.capacity() >= need) {
            self.room_short = None;
        }
        self.room_kept = None;
        if self.abandoning.is_some() {
            return self.abandon_tasks(store).map(|_| 0);
        }
        self.incorporate_pending(store)?;
        self.start_consolidation()?;
        self.sweep();
        self.look(store)?;
        let used = self.step_inline(store, budget)?;
        if self.pool.is_some() {
            self.tend_tasks(store)?;
        }
        self.apply_done(store)?;
        self.incorporate_pending(store)?;
        self.look(store)?;
        if self.pool.is_some() && self.abandoning.is_none() {
            self.dispatch_tasks(store)?;
        }
        self.end_consolidation();
        #[cfg(test)]
        self.check_tasks()?;
        Ok(used)
    }

    /// Moves the pending branches into the root, oldest first, unless the root is changing: they
    /// wait as pending, read before the tree, until the change is applied. A memtable packed at
    /// the root's pivot keys goes into the pivots' bundles where [`Self::takes_parts`], its pages
    /// adopted into the cache; any other enters the in-flight list.
    fn incorporate_pending<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
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
        self.pending_entries = 0;
        for p in std::mem::take(&mut self.pending) {
            let entries = p.entries();
            self.entered = self.entered.saturating_add(entries);
            self.held = self.held.saturating_add(entries);
            if self.takes_parts(&p)? {
                self.stats.parted_entries = self.stats.parted_entries.saturating_add(entries);
                self.stats.packings_parted = self.stats.packings_parted.saturating_add(1);
                // The branches stay in the tree as written: their pages cached where they were
                // written, as a compaction's are.
                if let Some(kept) = p.kept {
                    store.adopt_pages(*kept);
                }
                for (_, b) in &p.parts {
                    self.count_live(b, true)?;
                }
                let node = self.node_mut(root)?;
                for (j, b) in p.parts {
                    node.pivots
                        .get_mut(j)
                        .ok_or(corrupt())?
                        .bundle
                        .push_newest(b);
                }
            } else {
                // Compacted next: the compaction's pages are the ones to cache.
                if let Some(kept) = p.kept {
                    store.unlend_pages(*kept);
                }
                for (_, b) in &p.parts {
                    self.count_live(b, true)?;
                }
                self.stats.packings_inflight = self.stats.packings_inflight.saturating_add(1);
                self.push_inflight(root, super::InFlight::packed(p.cuts, p.parts))?;
            }
        }
        self.node_mut(root)?.received = true;
        self.queue(root);
        Ok(())
    }

    /// Whether the root takes `p`'s branches straight into its pivots' bundles: its pivots are
    /// those `p`'s packing ended its branches at, and no pivot taking one has a live in-flight
    /// bundle, a compaction out or a child that will split. Each such pivot's compaction would
    /// then have had that branch alone for input and written its entries again, unchanged, as the
    /// pivot's newest bundle branch: 78.8M of the 374.0M entries a 100M fill wrote (research/57
    /// §7). Otherwise the branches enter as one in-flight bundle and are compacted.
    ///
    /// Nor does a pivot whose bundle waits for its child ([`Self::waits_for_child`]): it flushes
    /// nothing and plans no compaction until the child takes a flush, so parts taken into it
    /// would grow it by a branch, and an extent, every packing, past every bound, while the
    /// root's in-flight list stays empty and [`Self::has_room`] never holds a rotation. In an
    /// in-flight bundle they wait as an inline packing's branch does. A range's install on one
    /// worker grew a root bundle so to 194 branches and filled its store of 256 extents
    /// (mantle-node tests/engine_workers.rs).
    fn takes_parts(&self, p: &super::Pending) -> Result<bool, Error> {
        let node = self.node(self.root)?;
        if node.leaf
            || p.cuts.is_empty()
            || node.pivots.len() != p.cuts.len()
            || node.pivots.iter().zip(&p.cuts).any(|(q, c)| q.key != *c)
        {
            return Ok(false);
        }
        for (j, _) in &p.parts {
            let pivot = node.pivots.get(*j).ok_or(corrupt())?;
            if node.inflight.len() > pivot.start
                || self.compacting(self.root, &pivot.key)
                || self.held(pivot)?
                || self.waits_for_child(pivot)?
            {
                return Ok(false);
            }
        }
        Ok(true)
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
                self.consolidated_at = (self.stats.entries_written, self.entered);
                self.stats.whole_consolidations = self.stats.whole_consolidations.saturating_add(1);
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
        self.note_near_bound(store);
        self.plan_cleaning(store)?;
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
        // A relocation out holds the node's branches where they stand until it applies, which
        // looks at the node again.
        if self
            .tasks
            .on(n)
            .any(|t| matches!(t.what, What::Relocate { .. }))
        {
            return Ok(());
        }
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
            match self.flush_due(n, i)? {
                Flush::Now => {
                    if self.near_bound && self.held_within_fanout(n, i)? {
                        self.stats.near_bound_flushes =
                            self.stats.near_bound_flushes.saturating_add(1);
                    }
                    self.flush_pivot(n, i)?
                }
                // Looked at again once the child's live in-flight bundles fall below the fanout
                // ([`Self::wake_held`]).
                Flush::Held(child) => self.node_mut(child)?.held_by = Some(n),
                Flush::No => {}
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
            && !self.waits_for_child(pivot)?
            && !(node.leaf && {
                // A leaf whose settle is due takes its in-flight bundles, and one waiting for a
                // consolidation's flush will: either way they are merged once, by the settle.
                let settle = self.leaf_settle(n)?;
                settle.due || settle.waits
            }))
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
            .map(|b| b.len())
            .fold(0usize, usize::saturating_add);
        let live_entries = live
            .iter()
            .map(|b| entries(b))
            .fold(0u64, u64::saturating_add);
        let held = pivot.bundle.branches().len();
        let count = entries(pivot.bundle.branches()).saturating_add(live_entries);
        let after = held.saturating_add(usize::from(live_branches > 0));
        let was_paid = self.consolidating && pivot.seek.paid;
        let waits = was_paid && self.flush_to_come(n, &pivot.key)?;
        let paid = was_paid && !waits && held.saturating_add(live_branches) > 1;
        Ok(LeafSettle {
            count,
            after,
            was_paid,
            paid,
            waits,
            due: paid || after > self.config.fanout || count > self.leaf_limit(),
        })
    }

    /// Whether, in a whole consolidation, a pivot above node `n` on its path from the root, `key`
    /// one of `n`'s pivots' keys, will still flush data down to it: a paid one, or one a paid
    /// pivot lies below ([`Self::flush_due`]), that holds data in its bundle, in live in-flight
    /// bundles or in a compaction out. Its flush brings `n` that data and pays for `n`'s pivots
    /// again, so a pivot that went down before it goes down a second time once it lands, and a
    /// leaf settled before it settles a second time: at 100M every leaf of a four-level trunk
    /// did, the first settles 60.8M of the 127.2M entries the drain's consolidation wrote
    /// (research/57 §2). A pivot that holds nothing brings nothing, and waiting for it held every
    /// leaf of a 10M trunk behind the root's compactions (research/57 §5). Data comes down a path
    /// from a paid pivot, whose flush pays the next pivot on it, so the last flush above a waiting
    /// node lands in it and looks at it again. A consolidation seeks' rent bought pays no pivot
    /// it flushes into, and waits for none. Down from the root by the key: at most the height.
    fn flush_to_come(&self, n: usize, key: &[u8]) -> Result<bool, Error> {
        if !self.full {
            return Ok(false);
        }
        let mut at = self.root;
        for _ in 0..self.nodes.len() {
            if at == n {
                return Ok(false);
            }
            let node = self.node(at)?;
            let pivot = node
                .pivots
                .get(Self::pivot_of(node, key))
                .ok_or(corrupt())?;
            let pending = pivot.seek.paid || pivot.seek.below;
            if pending
                && (!pivot.bundle.branches().is_empty()
                    || node.inflight.len() > pivot.start
                    || self.compacting(at, &pivot.key))
            {
                return Ok(true);
            }
            at = pivot.child.ok_or(corrupt())?;
        }
        Err(corrupt())
    }

    /// Whether a compaction of node `n`'s pivot keyed `key` is planned or out.
    fn compacting(&self, n: usize, key: &[u8]) -> bool {
        self.tasks
            .on(n)
            .any(|t| matches!(&t.what, What::Compact { key: k, .. } if k.as_slice() == key))
    }

    /// Whether `pivot`'s child wants a change that splits it: the pivot then plans no
    /// compaction, so its bundle stays empty for the parts to replace it.
    fn held(&self, pivot: &Pivot) -> Result<bool, Error> {
        match pivot.child {
            Some(c) => Ok(self.node(c)?.wants.is_some_and(Change::splits)),
            None => Ok(false),
        }
    }

    /// Whether `pivot`'s bundle is past the fanout and its child takes no flush: the pivot then
    /// plans no compaction, which would grow its bundle a branch at a time while its flush waits,
    /// so its live in-flight bundles fill to the fanout and its own parent's flush waits in turn,
    /// up to the root, whose full in-flight list holds the next rotation ([`Self::has_room`]).
    fn waits_for_child(&self, pivot: &Pivot) -> Result<bool, Error> {
        match pivot.child {
            Some(c) if pivot.bundle.branches().len() > self.config.fanout => {
                Ok(!self.takes_flush(c)?)
            }
            _ => Ok(false),
        }
    }

    /// Looks again at the parent whose flush into node `n` waited, once `n`'s live in-flight
    /// bundles have fallen below the fanout.
    fn wake_held(&mut self, n: usize) -> Result<(), Error> {
        if self.node(n)?.held_by.is_some()
            && self.takes_flush(n)?
            && let Some(parent) = self.node_mut(n)?.held_by.take()
        {
            self.queue(parent);
        }
        Ok(())
    }

    /// What pivot `i` of node `n` does with its bundle now: flushes it to its child past the
    /// fanout, as the last flush before its child splits, or as a consolidation's through a paid or
    /// marked pivot. A consolidation carries the pivot's data down, so it waits for the pivot's live
    /// in-flight bundles to be compacted into the bundle, and flushes even an empty one, so the
    /// rent is spent and a paid pivot's payment goes down to the child's pivots. In a whole
    /// consolidation it also waits while a pivot above it will still flush
    /// ([`Self::flush_to_come`]): the consolidation is a pass down from the root, each pivot
    /// flushing once everything above it has, so no data lands in a node whose pivots have
    /// already gone down (research/57 §2). A flush that brings branches waits while its child
    /// takes none ([`Self::takes_flush`]), as the root's pending branches wait for room, but the
    /// last before the child splits, which the split waits for. Never into a child whose change
    /// has started.
    fn flush_due(&self, n: usize, i: usize) -> Result<Flush, Error> {
        let node = self.node(n)?;
        if node.leaf {
            return Ok(Flush::No);
        }
        let pivot = node.pivots.get(i).ok_or(corrupt())?;
        let Some(child) = pivot.child else {
            return Ok(Flush::No);
        };
        let wants = self.node(child)?.wants;
        if wants.is_some_and(|c| c.started) {
            return Ok(Flush::No);
        }
        let held = pivot.bundle.branches().len();
        // The last flush before the child splits comes first, a consolidation's too: the split
        // waits for this bundle to empty and this pivot's compaction waits for the split
        // ([`Self::held`]), so waiting for that compaction here would hold all three. It does not
        // wait for the child's room either: a leaf's split is its settle, which alone takes its
        // live in-flight bundles, so waiting for them would hold the flush, the split and the
        // room for good. The split takes what the flush lands.
        if held > 0 && wants.is_some_and(Change::splits) {
            return Ok(Flush::Now);
        }
        let due = if self.consolidating && (pivot.seek.paid || (self.full && pivot.seek.below)) {
            node.inflight.len() <= pivot.start
                && !self.compacting(n, &pivot.key)
                && !self.flush_to_come(n, &pivot.key)?
        } else {
            held > self.flush_past()
        };
        if !due {
            Ok(Flush::No)
        } else if held == 0 || self.takes_flush(child)? {
            Ok(Flush::Now)
        } else {
            Ok(Flush::Held(child))
        }
    }

    /// The branches a pivot's bundle holds before it flushes: past the fanout, SplinterDB's
    /// bound (a flush when the child's branches would pass `F`; research/34 §1), so a read's
    /// sources stay few; with the store near its bound, any branch ([`Self::note_near_bound`]).
    fn flush_past(&self) -> usize {
        if self.near_bound {
            0
        } else {
            self.config.fanout
        }
    }

    /// Whether pivot `i` of node `n` holds a bundle of at most `fanout` branches: flushed, it
    /// went down only because the store is near its bound.
    fn held_within_fanout(&self, n: usize, i: usize) -> Result<bool, Error> {
        let pivot = self.node(n)?.pivots.get(i).ok_or(corrupt())?;
        Ok((1..=self.config.fanout).contains(&pivot.bundle.branches().len()))
    }

    /// Notes whether the store is near its bound ([`Self::near_bound`]): the extents maintenance
    /// may write into ([`Self::room_free`]) are fewer than the extents the pages above the leaves
    /// fill ([`Self::above_leaves_pages`]). A key's versions above a leaf and in it are
    /// merged only by the leaf's settle, which alone drops the older; a pivot past the fanout
    /// flushes, so a store's index nodes may hold `fanout + 1` branches a pivot of versions the
    /// leaves will drop (SplinterDB's size tiering, whose space the paper leaves unbounded;
    /// research/34 §1). Below the mark the index pivots flush as soon as they hold a branch, so the
    /// data above the leaves is the data on its way down, as leveling bounds the levels above the
    /// last to what that level's own size sets (RocksDB's dynamic level sizing, DONG17 §3.1;
    /// research/12 §2.2) and trades the writes of more frequent settles for the space (the RUM
    /// conjecture, DONG17 §4; Dostoevsky's space amplification, O(T) tiered and O(1/T) leveled,
    /// research/12 §2.5). The mark is what pushing that data down takes: each compaction below
    /// rewrites what it merges and frees its inputs only at the next checkpoint, so a store with
    /// fewer free extents than those pages fill could not take them one level lower, and the
    /// versions would stay until the store stalls. Engaged, every index node holding a bundle
    /// is looked at, so its pivots flush now. The pages above the leaves are counted only where
    /// the store has fewer free pages than the tree's live ones ([`Self::live_total`]), which
    /// they are a part of: a store far from its bound is told so without a look at its nodes.
    fn note_near_bound<F: BlockFile>(&mut self, store: &Store<F>) {
        let per = u64::from(self.extent_pages).max(1);
        let free = self.room_free(store);
        let near = free.saturating_mul(per) < self.live_total
            && free < self.above_leaves_pages().div_ceil(per);
        if near && !self.near_bound {
            for n in 0..self.nodes.len() {
                let holds = self.nodes.get(n).is_some_and(|node| {
                    !node.leaf && node.pivots.iter().any(|p| !p.bundle.branches().is_empty())
                });
                if holds {
                    self.queue(n);
                }
            }
        }
        if near {
            self.stats.near_bound_looks = self.stats.near_bound_looks.saturating_add(1);
        }
        self.near_bound = near;
    }

    /// The extents maintenance may write into, as the mark reads them each look: those the store
    /// may still hand out once grown (`Store::room_most`) beside the room the next checkpoint's
    /// packings and map take ([`Self::set_checkpoint_room`]), the image's extents as last saved,
    /// which the next image replaces, and the cleaner's reserve. In constant time: the image as
    /// it stands, which [`Self::room_kept`] counts node by node for a task about to open, differs
    /// from the last only by the nodes changed since.
    pub(super) fn room_free<F: BlockFile>(&self, store: &Store<F>) -> u64 {
        let image = u64::try_from(self.saved.len()).unwrap_or(u64::MAX);
        store
            .room_most()
            .saturating_sub(self.checkpoint_room)
            .saturating_sub(image)
            .saturating_sub(self.clean_room)
    }

    /// SplinterDB's pass through an index node that received data (`restore_balance_index`,
    /// trunk.c; research/50 §4): once what it received is compacted into its pivots, its fullest
    /// pivot flushes if it holds more than a memtable's entries, so index pivots hold about that
    /// much and the data lives in the leaves. Once a receipt; never into a child whose change has
    /// started or that takes no flush ([`Self::takes_flush`]).
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
        if self.node(child)?.wants.is_some_and(|c| c.started) || !self.takes_flush(child)? {
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
        let (from, end) = (pivot.key.clone(), Self::pivot_end(node, i));
        // Its live in-flight bundles, oldest first, while the entries of their pages in its range
        // stay within a leaf's limit, one bundle at least; the newer wait for its next compaction,
        // above the branch this one makes. Each input was built under the same bound, or is a
        // memtable's packing, so every branch a compaction builds holds at most a leaf's limit or
        // a memtable's entries, whatever the keys, and so does its builder
        // (`shard_db::pooled_bytes`). The entries of those pages bound what the merge writes, so
        // its builder is sized by them (its `per`), not by its inputs' whole counts, each input
        // spanning the node: one branch, never split, since the merge writes no more.
        let pooled = self.pool.is_some();
        let limit = self.leaf_limit();
        let start = pivot.start;
        let range = (pivot.key.as_slice(), Self::pivot_end_of(node, i));
        let mut covered = start;
        let mut held = 0u64;
        let mut pages = 0usize;
        let mut reuse = Reuse::default();
        for bundle in node.inflight.get(start..).unwrap_or(&[]) {
            let (mut keys, mut read, mut more) = (0u64, 0usize, Reuse::default());
            for b in bundle.within(range.0, range.1) {
                let (p, k) = b.merge_size(range.0, range.1);
                keys = keys.saturating_add(k);
                read = read.saturating_add(p);
                if pooled {
                    more.add(reuse_of(b, p, self.taken, self.reuse_window));
                }
            }
            if covered > start && held.saturating_add(keys) > limit {
                break;
            }
            held = held.saturating_add(keys);
            pages = pages.saturating_add(read);
            reuse.add(more);
            covered = covered.saturating_add(1);
        }
        let live = node.inflight.get(start..covered).ok_or(corrupt())?;
        let inputs = merged_inputs(live, range.0, range.1, None);
        let units = inputs
            .clone()
            .map(|b| b.count)
            .fold(0u64, u64::saturating_add);
        // Newest first, the order a merge takes.
        let run = if pooled {
            Running::Remote {
                grant: Vec::new(),
                state: State::Ready(Box::new(Task {
                    inputs: inputs.map(Branch::for_merge).collect(),
                    from,
                    end,
                    drop_tombstones: false,
                    per: held.max(1),
                    pages,
                })),
            }
        } else {
            Running::Inline(Box::new(Compaction::prepare(
                inputs,
                from,
                end,
                false,
                held.max(1),
            )))
        };
        let key = pivot.key.clone();
        let root = self.root;
        self.tasks.push(
            NodeTask {
                node: n,
                what: What::Compact {
                    key,
                    start,
                    covered,
                },
                run,
                units,
                reuse,
            },
            root,
        )?;
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
            self.push_inflight(child, super::InFlight::flushed(b))?;
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
        let busy = self.tasks.busy(n);
        let node = self.node(n)?;
        if node.wants.is_some() {
            return Ok(());
        }
        let live = node.pivots.iter().any(|p| node.inflight.len() > p.start);
        let kind = if node.leaf {
            let settle = self.leaf_settle(n)?;
            if !settle.due {
                // Paid with one source: nothing to rewrite once the leaf's own compactions are
                // in and no flush is still to come down its path, then the rent is spent; the
                // apply of a compaction due or out, or the flush's arrival, looks at the leaf
                // again.
                if settle.was_paid && !settle.waits && !live && !busy {
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
        self.set_wants(n, change)?;
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
        if change.started || self.tasks.busy(n) {
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
                    self.set_wants(
                        n,
                        Change {
                            kind: ChangeKind::Settle { parts: now },
                            started: false,
                        },
                    )?;
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
                self.clear_wants(n)?;
                self.splice(n, parts)
            }
        }
    }

    /// The leaves leaf `n`'s settle would make, as its bundle and live in-flight bundles stand
    /// ([`Self::leaf_parts`]).
    fn settle_parts(&self, n: usize) -> Result<u64, Error> {
        Ok(self.leaf_parts(self.leaf_settle(n)?.count))
    }

    /// The entries a leaf holds before it splits ([`TrunkConfig::leaf_limit`]).
    pub(super) fn leaf_limit(&self) -> u64 {
        self.config.leaf_limit()
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
            // A leaf waiting for a consolidation's flush keeps its rent for the settle after it.
            if settle.was_paid && !settle.waits {
                self.spend(n, 0)?;
            }
            self.clear_wants(n)?;
            return Ok(());
        }
        let node = self.node(n)?;
        let pivot = node.pivots.first().ok_or(corrupt())?;
        // Its live in-flight bundles with its bundle, newest first: they came down after the
        // bundle's branches were made. No flush reaches the leaf while its change is started
        // ([`Self::flush_due`]), so the range it takes is all there is until it applies.
        let (start, covered) = (pivot.start, node.inflight.len());
        let live = node.inflight.get(start..covered).ok_or(corrupt())?;
        let inputs = merged_inputs(
            live,
            &pivot.key,
            node.end.as_deref(),
            Some(pivot.bundle.branches()),
        );
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
        let mut reuse = Reuse::default();
        let run = if self.pool.is_some() {
            let pages;
            (pages, reuse) = read_pages(
                inputs.clone(),
                &from,
                end.as_deref(),
                self.taken,
                self.reuse_window,
            );
            Running::Remote {
                grant: Vec::new(),
                state: State::Ready(Box::new(Task {
                    inputs: inputs.map(Branch::for_merge).collect(),
                    from,
                    end,
                    drop_tombstones: true,
                    per,
                    pages,
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
        let root = self.root;
        self.tasks.push(
            NodeTask {
                node: n,
                what: What::Settle {
                    extents,
                    cause,
                    start,
                    covered,
                },
                run,
                units: count,
                reuse,
            },
            root,
        )?;
        if let Some(c) = self.node_mut(n)?.wants.as_mut() {
            c.started = true;
        }
        self.stats.plan_ns = self.stats.plan_ns.saturating_add(ns_since(t));
        Ok(())
    }

    /// The extents an inline task writes at most, as a worker's job's grant is sized
    /// ([`Self::dispatch_tasks`]): the extents the pages its inputs hold fill, or its inputs'
    /// extents when that is less, and one more a part, and one for the last partial one. A merge
    /// writes no more pages than it reads; a branch of one page in an extent of four counts one.
    fn inline_asked<F: BlockFile>(&self, store: &Store<F>, task: &NodeTask) -> Result<u64, Error> {
        let node = self.nodes.get(task.node).ok_or(corrupt())?;
        let (from, end) = task_range(node, &task.what)?;
        let (extents, pages, entries) = task_inputs(node, &task.what)?.fold(
            (0u64, 0u64, 0u64),
            |(extents, pages, entries), b| {
                (
                    extents.saturating_add(u64::try_from(b.extents.len()).unwrap_or(u64::MAX)),
                    pages.saturating_add(
                        u64::try_from(b.pages_between(from, end)).unwrap_or(u64::MAX),
                    ),
                    entries.saturating_add(b.count),
                )
            },
        );
        let read = pages.div_ceil(u64::from(store.extent_pages()).max(1));
        // A part holds a page at least, so a merge makes no more parts than it reads pages,
        // however many versions its inputs' counts hold.
        let parts = match &task.what {
            What::Compact { .. } => 1,
            What::Settle { .. } => self.leaf_parts(entries).min(pages.max(1)),
            // A copy of one branch writes at most the pages a merge of it would, in the extents
            // they fill: begun part way into a tail it shares that extent, so the copy takes
            // `ceil((t + p) / e) - 1` fresh extents for `p` pages begun at page `t < e`, at most
            // `ceil(p / e)`, as begun in a fresh extent.
            What::Relocate { .. } => return Ok(read),
        };
        Ok(extents.min(read).saturating_add(parts).saturating_add(1))
    }

    /// The extents the inline tasks writing now may still take: each one's
    /// [`Self::inline_asked`], counted whole, so a grant never takes room one of them writes into.
    pub(crate) fn inline_room<F: BlockFile>(&self, store: &Store<F>) -> Result<u64, Error> {
        let mut room = 0u64;
        for t in self.tasks.inline() {
            if matches!(&t.run, Running::Inline(c) if c.holds_inputs() && !c.is_done()) {
                room = room.saturating_add(self.inline_asked(store, t)?);
            }
        }
        Ok(room)
    }

    /// The extents the trunk's owed maintenance needs to go on: what the inline tasks writing now
    /// may still take, and the largest ask of a task not begun, as its grant or its inline room
    /// is sized. A rotation leaves them free (`ShardDb::rotation_room`): the compactions they are
    /// for give extents back, and packings taking their room would fill the store with branches
    /// none could compact. The largest, not the sum: each task opens only beside its own room, and
    /// applied it gives its inputs back, free at once or at the checkpoint a range takes for room,
    /// so the owed tasks run one after another in the room of the largest. Kept for the sum, a
    /// trunk owing 102 extents of tasks held every entry with 92 free, where each task fit: the
    /// node suite's cell's fill on the shard alone met its bound at entry 8,925 of 24,000; kept
    /// for the largest, at 19,804 with small branches sharing extents and 13,528 without
    /// (mantle-node tests/engine_workers.rs, 2026-11-20).
    pub(crate) fn owed_room<F: BlockFile>(&self, store: &Store<F>) -> Result<u64, Error> {
        // A task not begun asks: an inline one still opening, or a worker's job not handed out.
        let mut most = 0u64;
        for t in self.tasks.inline() {
            if matches!(&t.run, Running::Inline(c) if c.opening()) {
                most = most.max(self.inline_asked(store, t)?);
            }
        }
        for t in self.tasks.ready() {
            if let Running::Remote {
                state: State::Ready(task),
                ..
            } = &t.run
            {
                most = most.max(u64::try_from(grant_size(store, task)).unwrap_or(u64::MAX));
            }
        }
        Ok(self.inline_room(store)?.saturating_add(most))
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
        // Two passes, the root's tasks first: they free room for pending branches. The inline
        // tasks are filed in that order ([`Tasks::next_inline`]), and a step plans none and makes
        // none inline, so those filed when it begins bound the loop.
        let mut pass = None;
        let mut holding = false;
        let mut last = None;
        for _ in 0..self.tasks.inline_count() {
            let Some((below, seq)) = self.tasks.next_inline(last) else {
                break;
            };
            last = Some((below, seq));
            if pass != Some(below) {
                pass = Some(below);
                // One task of a pass holds its inputs open at a time: one holding none opens only
                // while no other of its pass holds any, so the shard holds two compactions'
                // cursors and builders at most, the root's and one below
                // (`shard_db::pooled_bytes`). Tasks step in the order they were planned, so the
                // one holding them is the first of its pass, unless a lack of room for its output
                // passed it over.
                holding = self
                    .tasks
                    .inline_of(below)
                    .any(|t| matches!(&t.run, Running::Inline(c) if c.holds_inputs()));
            }
            if used >= budget || self.io_waiting {
                break;
            }
            let yield_io = self.yield_io;
            let Some(task) = self.tasks.get(seq) else {
                break;
            };
            let Running::Inline(c) = &task.run else {
                continue;
            };
            let opens = !c.holds_inputs();
            if opens && holding {
                continue;
            }
            let opening = c.opening();
            // A task about to write takes its output's room first, as a worker's job takes its
            // grant (`dispatch_tasks`): the inputs' extents and one more a part, beside what the
            // inline tasks writing now may still take and the room the next checkpoint takes
            // ([`Self::room_kept`]); the file grown in place where the shard may, else asked of
            // the shard's growth and the task stepped once it has grown. A store without that
            // room holds the task: it waits for a job out or done to give extents back, or for
            // the checkpoint that frees those the one before names. Begun without it, it took
            // the store's last extent part way and fenced the store: a sustained fill of the node
            // suite's 256-extent cell held 214 and had 42 waiting for the next checkpoint
            // (mantle-node tests/engine_workers.rs).
            if opening {
                let asked = self.inline_asked(store, task)?;
                // The cleaner's reserve is a relocation's to take and no other task's.
                let relocates = matches!(task.what, What::Relocate { .. });
                let kept = self.room_kept(store)?.saturating_sub(if relocates {
                    self.clean_room
                } else {
                    0
                });
                let need = asked
                    .saturating_add(kept)
                    .saturating_add(self.inline_room(store)?);
                if store.segments().is_some() && store.capacity() < need {
                    if store.grows_in_place() {
                        store.grow_now(need)?;
                    }
                    if store.capacity() < need {
                        self.capacity_wanted = self.capacity_wanted.max(need);
                        if store.room_most() < need {
                            self.note_room_short(need);
                        }
                        continue;
                    }
                }
                if store.capacity() < need {
                    self.note_room_short(need);
                    continue;
                }
            }
            let Some(mut task) = self.tasks.get_mut(seq) else {
                break;
            };
            let NodeTask {
                node, what, run, ..
            } = &mut *task;
            let Running::Inline(c) = &mut *run else {
                continue;
            };
            let node = self.nodes.get(*node).ok_or(corrupt())?;
            c.set_yield(yield_io);
            let left = budget.saturating_sub(used);
            let merging = std::time::Instant::now();
            let step = c.step(store, task_inputs(node, what)?, left);
            merge_ns = merge_ns.saturating_add(ns_since(Some(merging)));
            let spent = match step {
                Ok(spent) => spent,
                Err(error) => {
                    // Opening was canceled before any output was built: the task stays, so a
                    // caller can repair the read and resume. A later merge error may have
                    // consumed output and is not restartable: the task goes and is planned anew.
                    if !c.opening() {
                        drop(task);
                        let task = self.tasks.remove(seq).ok_or(corrupt())?;
                        if let What::Settle { .. } = task.what
                            && let Some(c) = self.node_mut(task.node)?.wants.as_mut()
                        {
                            c.started = false;
                        }
                        if let What::Relocate { ask, .. } = task.what {
                            self.set_clean_room(self.clean_room.saturating_sub(ask));
                        }
                        self.queue(task.node);
                    }
                    return Err(error);
                }
            };
            used = used.saturating_add(spent);
            self.io_waiting |= c.waiting();
            holding = c.holds_inputs() && !c.is_done();
            if c.is_done() {
                let Running::Inline(c) = std::mem::replace(run, Running::Finished(Vec::new()))
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
                *run = Running::Finished(parts);
            }
            // Let go, the task is filed as it now stands before the count below looks.
            drop(task);
            if opens && holding {
                self.note_opened();
            }
        }
        self.note_merge(merge_ns, used);
        Ok(used)
    }

    /// A task opened its inputs: the most holding theirs at once, counted.
    fn note_opened(&mut self) {
        let open = self
            .tasks
            .inline()
            .filter(|t| matches!(&t.run, Running::Inline(c) if c.holds_inputs() && !c.is_done()))
            .count();
        self.stats.open_most = self
            .stats
            .open_most
            .max(u64::try_from(open).unwrap_or(u64::MAX));
    }

    /// Applies every finished task, in plan order: each changes only its own node (and, for a
    /// settle that splits, its parent's pivot, held empty for it), and queues what it changed.
    /// An apply plans and finishes none, so the done tasks when it begins bound the loop.
    fn apply_done<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        for _ in 0..self.tasks.done_count() {
            let Some(seq) = self.tasks.first_done() else {
                break;
            };
            let task = self.tasks.remove(seq).ok_or(corrupt())?;
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
            node: n,
            what,
            run,
            reuse,
            ..
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
                super::hold_shared(store, out.parts.iter().map(|(_, b)| b))?;
                super::take_job_tail_back(store, &out)?;
                store.extend_end(out.end);
                // The worker timed its merge: a compacted key's cost, which consolidations are
                // priced at, measured wherever compactions run; and its reads' waits, what the
                // kernel's page cache cost it.
                self.note_merge(out.ns, out.units);
                self.note_reuse(reuse, out.read_wait_ns);
                out.parts
            }
            _ => return Err(corrupt()),
        };
        let built = parts.iter().map(|(_, b)| b.count).max().unwrap_or(0);
        self.stats.compacted_most = self.stats.compacted_most.max(built);
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
                // One branch at most: a pivot's compaction never splits (`plan_compact`), and one
                // that made more would lose the rest here.
                if parts.len() > 1 {
                    return Err(corrupt());
                }
                let merged = parts.into_iter().next().map(|(_, b)| b);
                self.apply_pivot(n, i, merged, covered)?;
                self.drop_dead(store, n)?;
                self.queue(n);
                self.wake_held(n)?;
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
                self.wake_held(n)?;
                self.clear_wants(n)?;
                self.splice(n, leaves)?;
            }
            What::Relocate { root, ask } => {
                // A copy of one branch is one part; the branch is where it stood, its node having
                // planned nothing meanwhile, or the copy is discarded.
                let mut parts = parts.into_iter();
                match (parts.next(), parts.next()) {
                    (Some((_, copy)), None) => {
                        if holding(self.node(n)?, root).is_some() {
                            self.count_live(&copy, true)?;
                            let slot = holding_mut(self.node_mut(n)?, root).ok_or(corrupt())?;
                            let old = std::mem::replace(slot, copy);
                            self.count_live(&old, false)?;
                            super::release(store, &old)?;
                            self.stats.relocations = self.stats.relocations.saturating_add(1);
                        } else {
                            super::release(store, &copy)?;
                            self.stats.relocations_discarded =
                                self.stats.relocations_discarded.saturating_add(1);
                        }
                    }
                    (first, second) => {
                        for (_, b) in first.into_iter().chain(second).chain(parts) {
                            super::release(store, &b)?;
                        }
                        self.stats.relocations_discarded =
                            self.stats.relocations_discarded.saturating_add(1);
                    }
                }
                self.set_clean_room(self.clean_room.saturating_sub(ask));
                self.queue(n);
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
    /// the root's first, while a worker is free, each with a grant sized to what it reads: the
    /// extents its range of its inputs fills, and one more a part it may make. A job that needs
    /// more asks for it, and the pool answers from the store ([`Pool::take`]).
    fn dispatch_tasks<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        let wait = self.wait != Wait::No;
        #[cfg(test)]
        if self.hold_dispatch && !wait {
            return Ok(());
        }
        let admit = self.admit();
        // Whether a job is out or done: its result gives back the grant it did not write as it is
        // taken, and its task frees its inputs as it applies.
        let mut others = self.tasks.out_or_done();
        // The inline tasks writing now take extents as they write: a grant leaves them room.
        let inline_room = self.inline_room(store)?;
        // Two passes, the root's jobs first: they free room for pending branches. The ready
        // tasks are filed in that order ([`Tasks::next_ready`]), and a hand-out plans none and
        // makes none ready, so those filed when it begins bound the loop: with every seat held,
        // the first ends it.
        let mut last = None;
        for _ in 0..self.tasks.ready_count() {
            let Some(key) = self.tasks.next_ready(last) else {
                break;
            };
            last = Some(key);
            let Some(NodeTask {
                run: Running::Remote { state, .. },
                ..
            }) = self.tasks.get(key.1)
            else {
                continue;
            };
            let State::Ready(task) = state else {
                continue;
            };
            let Some(pool) = self.pool.as_mut() else {
                return Ok(());
            };
            if !pool.can_take(store, admit)? {
                // Every seat held: only a worker's message frees one, so the shard waits for it
                // rather than pass again.
                pool.want_message();
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
            // A pivot's compaction reads its slice of inputs that span the node: its grant is the
            // extents the pages it writes at most fill, or all its inputs' extents when that is
            // less, and the cache lends room for as many pages.
            let pages = task.pages;
            let needed = grant_size(store, task);
            #[cfg(test)]
            let needed = self.compact_most.map_or(needed, |most| needed.min(most));
            let asked = u64::try_from(needed).unwrap_or(u64::MAX);
            // And the next checkpoint's room ([`Self::room_kept`]), counted once a pass.
            let need = asked.saturating_add(self.room_kept(store)?);
            if store.segments().is_some() && store.capacity() < need {
                if store.grows_in_place() {
                    // Where the shard may wait, its file grows by the grant here.
                    store.grow_now(need)?;
                }
                if store.capacity() < need {
                    // The file's admitted extents cannot hold it yet: the shard grows the file by
                    // at least this much, and the job goes out once they can.
                    self.capacity_wanted = self.capacity_wanted.max(need);
                    if store.room_most() < need {
                        self.note_room_short(need);
                    }
                    continue;
                }
            }
            // The store's extents hold no grant this size beside those held, the room the inline
            // tasks writing now still take and the next checkpoint's ([`Self::room_kept`]). It
            // waits while a job out or done may give extents back; with none, it is handed to the
            // shard, whose inline task takes the same room first, or waits for the checkpoint that
            // frees the extents the one before names (`step_inline`). A grant so never takes room
            // the store lacks or an inline task writes into (a 10,200-row install fenced so, a
            // grant of 4 out beside an inline compaction that then found none: mantle-node
            // tests/engine_workers.rs).
            let short = store.capacity() < need.saturating_add(inline_room);
            if short && others {
                self.note_room_short(need.saturating_add(inline_room));
                continue;
            }
            let Some(mut t) = self.tasks.get_mut(key.1) else {
                continue;
            };
            let NodeTask {
                node, what, run, ..
            } = &mut *t;
            let Running::Remote { grant, state } = &mut *run else {
                continue;
            };
            if short {
                let State::Ready(task) = std::mem::replace(state, State::Dropped) else {
                    continue;
                };
                let Task {
                    from,
                    end,
                    drop_tombstones,
                    per,
                    ..
                } = *task;
                let node = self.nodes.get(*node).ok_or(corrupt())?;
                let inline =
                    Compaction::prepare(task_inputs(node, what)?, from, end, drop_tombstones, per);
                *run = Running::Inline(Box::new(inline));
                continue;
            }
            let given = store.grant(needed)?;
            let State::Ready(task) = std::mem::replace(state, State::Dropped) else {
                continue;
            };
            // The job's first branch begins where the shard's last branch of its life ended, that
            // extent held once for the job and named in the task's grant, so a task dropped gives
            // the hold back with the rest (`abandon_tasks`).
            let tail = store.take_job_tail(Life::of_compaction(task.drop_tombstones))?;
            grant.clone_from(&given);
            grant.extend(tail.map(|t| t.extent));
            let job = Box::new(pool::Job {
                work: Work::Compact(*task),
                grant: given,
                file_end: store.end(),
                generation: store.generation(),
                keep: store.lend_pages(pages),
                tail,
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
                    super::give_tail_back(store, grant, job.tail)?;
                    return Err(error);
                }
            };
            match sent {
                Ok(ticket) => {
                    *state = State::Out(ticket);
                    others = true;
                }
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
                    super::give_tail_back(store, grant, job.tail)?;
                    pool.want_message();
                    return Ok(());
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
            if !self.tasks.has_out() {
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
            let mut task = self
                .tasks
                .out(back.ticket)
                .and_then(|seq| self.tasks.get_mut(seq));
            let remote = task.as_deref_mut().and_then(|t| match &mut t.run {
                Running::Remote { grant, state } => Some((grant, state)),
                _ => None,
            });
            match (remote, back.result) {
                (Some((grant, state)), Ok(mut out)) => {
                    grant.extend_from_slice(&back.topped);
                    // The grant it did not write goes back now, not when the task applies: a
                    // pass applies its tasks once every job of it is back, and a grant is sized
                    // to what a job reads, so ten such jobs held 130 extents in a store of 256
                    // for the few they wrote (mantle-node tests/engine_workers.rs). Done, a task
                    // holds what it wrote, as one finished on the shard does. A refused return
                    // changes no count, and the extents stay the task's to give back as it applies.
                    let given_back = store.grant_back(&out.unused);
                    if given_back.is_ok() {
                        grant.retain(|held| !out.unused.contains(held));
                        out.unused.clear();
                    }
                    // The next job of its life begins where this one ended, the task's grant
                    // holding that extent until its branches do (`Store::adopt_tail`).
                    if let Some(t) = out.tail.take() {
                        store.adopt_tail(t);
                    }
                    out.kept = back.kept;
                    *state = State::Done(out);
                    given_back?;
                }
                (remote, result) => {
                    if let Some(kept) = back.kept {
                        store.unlend_pages(*kept);
                    }
                    if let Some((grant, state)) = remote {
                        grant.extend_from_slice(&back.topped);
                        *state = State::Dropped;
                    }
                    let error = match result {
                        Err(error) => error,
                        Ok(_) => Error::InvalidArgument {
                            what: "a maintenance worker's result for no job of its",
                        },
                    };
                    drop(task);
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
            if !self.tasks.has_out() {
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
            if let Some(mut t) = self
                .tasks
                .out(back.ticket)
                .and_then(|seq| self.tasks.get_mut(seq))
                && let Running::Remote { grant, state } = &mut t.run
            {
                grant.extend_from_slice(&back.topped);
                *state = State::Dropped;
            }
        }
        if self.tasks.has_out() {
            self.workers_waiting = true;
            return Ok(());
        }
        let mut kept = Vec::with_capacity(self.tasks.len());
        for t in self.tasks.take_all() {
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
        // Filed again in plan order, under the same root.
        let root = self.root;
        for t in kept {
            self.tasks.push(t, root)?;
        }
        match self.abandoning.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Node `n`'s in-flight list lost its first `dead` bundles: its tasks' planned ranges move
    /// down with the list.
    pub(super) fn shift_tasks(&mut self, n: usize, dead: usize) {
        self.tasks.shift(n, dead);
    }

    /// An inline compaction before the runtime's workers are prepared.
    pub(super) fn inline_out(&self) -> bool {
        self.tasks.has_inline()
    }
}

/// A worker's job's grant ([`Trunk::dispatch_tasks`]): the extents the pages it reads at most
/// fill, or all its inputs' extents when that is less, and one more a part it may make.
fn grant_size<F: BlockFile>(store: &Store<F>, task: &Task) -> usize {
    let read = crate::util::div_ceil_usize(
        task.pages,
        usize::try_from(store.extent_pages()).unwrap_or(usize::MAX),
    );
    let extents = task
        .inputs
        .iter()
        .map(|b| b.extents.len())
        .fold(0usize, usize::saturating_add);
    let extents = read.map_or(extents, |read| read.min(extents));
    let entries = task
        .inputs
        .iter()
        .map(|b| b.count)
        .fold(0u64, u64::saturating_add);
    let parts = crate::util::div_ceil(entries, task.per.max(1))
        .unwrap_or(entries)
        .min(u64::try_from(task.pages.max(1)).unwrap_or(u64::MAX))
        .saturating_add(1);
    extents.saturating_add(usize::try_from(parts).unwrap_or(usize::MAX))
}

/// The entries of `branches`, an upper bound on their keys.
fn entries(branches: &[Branch]) -> u64 {
    branches
        .iter()
        .map(|b| b.count)
        .fold(0u64, u64::saturating_add)
}

/// Node `node`'s branch whose root page is `root`, in an in-flight bundle or a pivot's bundle,
/// and whether it is a leaf's bundle's (a settle's part, long-lived).
fn holding(node: &super::Node, root: u64) -> Option<(&Branch, bool)> {
    node.inflight
        .iter()
        .flat_map(|f| f.branches.iter())
        .find(|b| b.root == root)
        .map(|b| (b, false))
        .or_else(|| {
            node.pivots
                .iter()
                .flat_map(|p| p.bundle.branches().iter())
                .find(|b| b.root == root)
                .map(|b| (b, node.leaf))
        })
}

/// [`holding`]'s branch, to replace.
fn holding_mut(node: &mut super::Node, root: u64) -> Option<&mut Branch> {
    let super::Node {
        inflight, pivots, ..
    } = node;
    inflight
        .iter_mut()
        .flat_map(|f| f.branches.iter_mut())
        .chain(
            pivots
                .iter_mut()
                .flat_map(|p| p.bundle.branches_mut().iter_mut()),
        )
        .find(|b| b.root == root)
}

/// The keys a task merges: its pivot's range for a compaction, the leaf's for a settle.
fn task_range<'a>(
    node: &'a super::Node,
    what: &'a What,
) -> Result<(&'a [u8], Option<&'a [u8]>), Error> {
    match what {
        What::Compact { key, .. } => {
            let i = node
                .pivots
                .iter()
                .position(|p| p.key == *key)
                .ok_or(corrupt())?;
            Ok((key.as_slice(), Trunk::pivot_end_of(node, i)))
        }
        What::Settle { .. } => {
            let pivot = node.pivots.first().ok_or(corrupt())?;
            Ok((pivot.key.as_slice(), node.end.as_deref()))
        }
        // Every key the branch holds.
        What::Relocate { .. } => Ok((b"".as_slice(), None)),
    }
}

/// The inputs task `what` of `node` merges ([`merged_inputs`]): a pivot's compaction over the
/// pivot's range, a leaf's settle over the leaf's with its bundle.
fn task_inputs<'a>(
    node: &'a super::Node,
    what: &'a What,
) -> Result<impl Iterator<Item = &'a Branch> + Clone + 'a, Error> {
    let (start, covered, from, end, bundle) = match what {
        What::Compact {
            key,
            start,
            covered,
        } => {
            let i = node
                .pivots
                .iter()
                .position(|p| p.key == *key)
                .ok_or(corrupt())?;
            (
                *start,
                *covered,
                key.as_slice(),
                Trunk::pivot_end_of(node, i),
                None,
            )
        }
        What::Settle { start, covered, .. } => {
            let pivot = node.pivots.first().ok_or(corrupt())?;
            (
                *start,
                *covered,
                pivot.key.as_slice(),
                node.end.as_deref(),
                Some(pivot.bundle.branches()),
            )
        }
        What::Relocate { root, .. } => {
            let (b, _) = holding(node, *root).ok_or(corrupt())?;
            (0, 0, b"".as_slice(), None, Some(std::slice::from_ref(b)))
        }
    };
    let live = node.inflight.get(start..covered).ok_or(corrupt())?;
    Ok(merged_inputs(live, from, end, bundle))
}

/// The branches a compaction of `[from, end)` over in-flight bundles `live` merges, newest first,
/// and for a leaf's settle the leaf's `bundle` after them: of a packing's parts, only those whose
/// ranges meet it ([`super::InFlight::within`]). The same at a task's plan and at each of its
/// steps, which a compaction checks.
fn merged_inputs<'a>(
    live: &'a [super::InFlight],
    from: &'a [u8],
    end: Option<&'a [u8]>,
    bundle: Option<&'a [Branch]>,
) -> impl Iterator<Item = &'a Branch> + Clone + 'a {
    live.iter()
        .rev()
        .flat_map(move |f| f.within(from, end))
        .chain(bundle.into_iter().flatten())
}

/// The pages a merge of `[from, end)` reads from `inputs` at most, each one's
/// [`Branch::pages_between`] summed; and what they add to the kernel's page cache's counts
/// ([`Reuse`]), each input's pages by the extents taken since its branch, as of `taken`, against
/// `window`.
fn read_pages<'a>(
    inputs: impl Iterator<Item = &'a Branch>,
    from: &[u8],
    end: Option<&[u8]>,
    taken: u64,
    window: (u64, u64),
) -> (usize, Reuse) {
    let mut pages = 0usize;
    let mut reuse = Reuse::default();
    for b in inputs {
        let read = b.pages_between(from, end);
        pages = pages.saturating_add(read);
        reuse.add(reuse_of(b, read, taken, window));
    }
    (pages, reuse)
}

/// What branch `b`'s `read` pages add to the kernel's page cache's counts ([`Reuse`]): by the
/// extents taken since its branch, as of `taken`, against `window`.
fn reuse_of(b: &Branch, read: usize, taken: u64, window: (u64, u64)) -> Reuse {
    let mut reuse = Reuse::default();
    // A branch loaded at open has no stamp: its age is unknown, so no window holds it.
    let age = if b.taken == 0 {
        u64::MAX
    } else {
        taken.saturating_sub(b.taken)
    };
    if age >= window.0 {
        let read = u64::try_from(read).unwrap_or(u64::MAX);
        reuse.past = read;
        if age < window.1 {
            reuse.within = read;
        }
    }
    reuse
}

#[cfg(test)]
#[path = "seat_wait.rs"]
mod seat_wait;

#[cfg(test)]
#[path = "room.rs"]
mod room;

#[cfg(test)]
#[path = "shared_extents.rs"]
mod shared_extents;

#[cfg(test)]
#[path = "relocation.rs"]
mod relocation;

#[cfg(test)]
#[path = "near_bound.rs"]
mod near_bound;

#[cfg(test)]
mod reuse_tests {
    //! A job's reads counted for the kernel's page cache's ghost (docs/research/65 §8): each
    //! input's pages by the extents taken since its branch was, against the shard's window.
    use super::*;
    use crate::branch::filter::Keys;
    use crate::branch::{Builder, Op};
    use crate::store::Config;
    use hyper_block::buf::Alignment;
    use hyper_block::sim::SimFile;

    const CONFIG: Config = Config {
        page_size: 4096,
        extent_pages: 8,
        max_extents: 4096,
    };

    fn branch(store: &mut Store<SimFile>, first: u32, taken: u64) -> Branch {
        let mut b = Builder::new(store, Keys::Exactly(500)).unwrap();
        for k in first..first + 500 {
            b.add(store, format!("k{k:06}").as_bytes(), Op::Put, &[7u8; 100])
                .unwrap();
        }
        let mut b = b.finish(store).unwrap();
        b.taken = taken;
        b
    }

    #[test]
    fn the_trunk_stamps_each_branch_it_takes_with_the_extents_taken() {
        let align = Alignment::new(4096).unwrap();
        let file = SimFile::new(align, Alignment::new(512).unwrap(), 7).unwrap();
        let mut store = Store::create(file, CONFIG).unwrap();
        let mut trunk = Trunk::new(TrunkConfig {
            fanout: 4,
            leaf_entries: 128,
        })
        .unwrap();
        let a = branch(&mut store, 0, 0);
        let b = branch(&mut store, 300, 0);
        let c = branch(&mut store, 600, 0);
        let extents = |b: &Branch| u64::try_from(b.extents.len()).unwrap();
        let (ea, eb, ec) = (extents(&a), extents(&b), extents(&c));
        trunk.add(a);
        trunk.add_parts(Vec::new(), vec![(0, b), (0, c)], None);
        assert_eq!(trunk.taken, ea + eb + ec);
        let stamps: Vec<u64> = trunk
            .pending
            .iter()
            .flat_map(|p| p.parts.iter().map(|(_, b)| b.taken))
            .collect();
        assert_eq!(stamps, [ea, ea + eb, ea + eb + ec]);
    }

    #[test]
    fn each_jobs_waits_are_priced_by_its_own_reads_against_its_window() {
        let mut trunk = Trunk::new(TrunkConfig {
            fanout: 4,
            leaf_entries: 128,
        })
        .unwrap();
        let planned = |past, within| Reuse {
            past,
            within,
            ..Reuse::default()
        };
        // A quarter of one job's reads past the share a step more would hold, all of the other's:
        // a quarter of the first's waits and all of the second's.
        trunk.note_reuse(planned(100, 25), 1_000);
        trunk.note_reuse(planned(40, 40), 300);
        // A job that read nothing past the share, or waited on nothing: nothing spared.
        trunk.note_reuse(planned(0, 0), 5_000);
        trunk.note_reuse(planned(10, 10), 0);
        assert_eq!(
            trunk.take_reuse(),
            Reuse {
                past: 150,
                within: 75,
                waited_ns: 6_300,
                saved_ns: 250 + 300,
            }
        );
        // Taken, the counts start over; every wait at the extremes, in 128 bits, saturates.
        assert_eq!(trunk.take_reuse(), Reuse::default());
        trunk.note_reuse(planned(u64::MAX, u64::MAX), u64::MAX);
        trunk.note_reuse(planned(1, 1), 1);
        assert_eq!(trunk.take_reuse().saved_ns, u64::MAX);
    }

    #[test]
    fn a_jobs_reads_are_counted_by_their_branchs_age_against_the_window() {
        let align = Alignment::new(4096).unwrap();
        let file = SimFile::new(align, Alignment::new(512).unwrap(), 7).unwrap();
        let mut store = Store::create(file, CONFIG).unwrap();
        // At 100 extents taken: the old branch was taken 90 before, the young one 10, and the
        // loaded one has no stamp, its age unknown.
        let old = branch(&mut store, 0, 10);
        let young = branch(&mut store, 300, 90);
        let loaded = branch(&mut store, 600, 0);
        let pages = |b: &Branch| u64::try_from(b.pages_between(b"", None)).unwrap();
        let (o, y, l) = (pages(&old), pages(&young), pages(&loaded));
        assert!(o > 1 && y > 1 && l > 1, "one-page branches: {o} {y} {l}");
        let read = |window| read_pages([&old, &young, &loaded].into_iter(), b"", None, 100, window);
        let reuse = |past, within| Reuse {
            past,
            within,
            ..Reuse::default()
        };
        // Every read counted, the job's pages the sum; no window holds the loaded one's.
        let (all, r) = read((0, u64::MAX));
        assert_eq!(u64::try_from(all).unwrap(), o + y + l);
        assert_eq!(r, reuse(o + y + l, o + y));
        // A page cache of 20 extents holds the young one: the old and the loaded are past it,
        // and one of 91 would hold the old one too.
        assert_eq!(read((20, 91)).1, reuse(o + l, o));
        // One of 90 is past the old one as well; its end of 90 holds neither.
        assert_eq!(read((90, 90)).1, reuse(o + l, 0));
        // One of 91 holds both: only the loaded one is past it.
        assert_eq!(read((91, u64::MAX)).1, reuse(l, 0));
        // A job's reads within the trunk's range only: the young branch's keys past "k000400"
        // are not read.
        let (some, _) = read_pages(
            [&young].into_iter(),
            b"k000300",
            Some(b"k000400"),
            100,
            (0, u64::MAX),
        );
        assert!(u64::try_from(some).unwrap() < y, "{some} of {y}");
    }
}
