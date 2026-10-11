//! The trunk (docs/design/engine-structure.md §4, step E4): SplinterDB's size-tiered Bε-tree over
//! branches (research/34 §1), one shard's, so one worker runs it and no lock is taken.
//!
//! - A **node** has pivots, each a lower bound of the keys under it, with a child (none at a
//!   leaf) and a **pivot bundle** of branches, newest first; and **in-flight bundles** the node
//!   received, shared by its pivots, each pivot reading those from its own start on.
//! - A packed memtable waits as **pending**, read before the tree, until it enters the root's
//!   in-flight list (**incorporation**), which waits only while the root itself is changing.
//! - Maintenance is flat per-node tasks (CLAUDE.md §10; [`upkeep`]): each node's triggers plan
//!   its pivots' compactions, flush its full pivot bundles and take the change it wants, and one
//!   pass with a counted budget ([`Trunk::step`]) runs them, a compaction merging a budget of keys
//!   a step and changing its node only once it is done. Every change applies on the shard's
//!   thread between steps, so every read between steps sees the trunk whole.
//! - **Compaction**: a pivot with in-flight bundles merges them, clipped to its range, into one
//!   branch at the front of its pivot bundle; each entry is rewritten once a level.
//! - **Flush**: a pivot bundle of more than `fanout` branches moves to the child as in-flight
//!   bundles by reference, no entry rewritten (flush-then-compact). A node over `fanout` pivots
//!   splits.
//! - A **leaf** whose bundle passes `fanout` branches is compacted whole, tombstones dropped (no
//!   older entry lies below it), and split by entries once past `leaf_entries`.
//! - A **point read** goes root to leaf: each node's pivot for the key, its live in-flight
//!   bundles newest first, then its pivot bundle. Every branch is probed until maplets (step E5)
//!   name the branches a key may be in.
//!
//! References: a branch is named by one node at a time (a flush moves a bundle, it never copies
//! one) and holds the one reference its extents were allocated with; the node that drops it
//! releases them, so an extent is freed exactly when no node names it.

use std::collections::VecDeque;
use std::mem::size_of;

use crate::branch::merge::Compaction;
use crate::branch::{Branch, Op};
use crate::error::{Error, Malformed};
use crate::remix::{Build, Rebuild, View};
use crate::store::{Run, Store};
use hyper_block::block::BlockFile;
use pool::{Output, Owner, Pool, Task, Ticket, Work};

pub mod pool;
mod upkeep;
use upkeep::{Change, NodeTask};

/// The bytes a trunk's branches and views hold in memory ([`Trunk::memory`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Memory {
    /// Point filters.
    pub filters: usize,
    /// Leaf indexes.
    pub indexes: usize,
    /// Tree pages' entry counts.
    pub counts: usize,
    /// Range filters.
    pub ranges: usize,
    /// REMIX views.
    pub views: usize,
}

/// The trunk's shape: from measurement, given at creation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrunkConfig {
    /// Pivots a node holds, and branches a pivot bundle holds, before a split or a flush.
    pub fanout: usize,
    /// Entries a leaf holds before it splits.
    pub leaf_entries: u64,
}

#[derive(Clone, Debug)]
struct Pivot {
    key: Vec<u8>,
    child: Option<usize>,
    bundle: Bundle,
    /// The first of the node's in-flight bundles live for this pivot.
    start: usize,
    /// What seeks have paid for this pivot's sources being several ([`Trunk::charge`]).
    seek: SeekRent,
}

/// The rent seeks pay a pivot for opening more sources under it than one consolidated leaf
/// branch, and whether it covers the consolidation's price: then idle time flushes the pivot's
/// bundle down and settles the leaf below to one branch (the ski-rental rule; Karlin, Manasse,
/// Rudolph and Sleator, Algorithmica 1988). LevelDB compacts a file once seeks through it pass a
/// fixed allowance (`allowed_seeks`, a seek priced at 16 KiB of compaction; db/version_set.cc);
/// here both sides are measured on the running machine.
#[derive(Clone, Copy, Debug, Default)]
struct SeekRent {
    /// Nanoseconds seeks spent opening this pivot's extra sources, not yet spent back, since
    /// its sources last took the shape `shape` names: a change voids the rent, which priced a
    /// consolidation of sources no longer there.
    ns: u64,
    shape: (usize, u64),
    /// The rent covers the price: the next consolidation flushes or settles this pivot.
    paid: bool,
    /// A paid pivot lies below: a consolidation descends through this one to reach it.
    below: bool,
}

/// A pivot a seek's descent passed ([`Trunk::segment_at`]): where it is; the sources it gave the
/// seek, where they start in the segment's list, and the newest one's root page, which with
/// their number names the shape seeks paid rent for; and whether it is a leaf's.
#[derive(Clone, Copy, Debug)]
pub struct Passed {
    node: usize,
    pivot: usize,
    first: usize,
    sources: usize,
    newest: u64,
    leaf: bool,
}

impl Passed {
    /// The pivot's sources' place in the segment's list: from, and how many.
    pub fn sources(&self) -> (usize, usize) {
        (self.first, self.sources)
    }

    /// Whether the pivot is a leaf's.
    pub fn leaf(&self) -> bool {
        self.leaf
    }
}

/// What a consolidation's pages cost on this device, measured by the store: nanoseconds a page
/// read and a page written, and the pages an extent holds.
#[derive(Clone, Copy, Debug, Default)]
pub struct PageCosts {
    pub read_ns: u64,
    pub write_ns: u64,
    pub extent_pages: u64,
}

/// A source a scan of a leaf segment reads ([`Trunk::segment_at`]): a branch, or a pivot bundle
/// through its REMIX view with the branches it was built of.
#[derive(Clone, Copy, Debug)]
pub enum Source<'a> {
    Branch(&'a Branch),
    View(&'a View, &'a [Branch]),
}

/// Format: the most runs a REMIX view names (six bits of a selector, less the placeholder).
const MAX_VIEW_RUNS: usize = 63;

/// A pivot's bundle, newest first, and the REMIX view of it once one is built (two runs or more,
/// docs/design/engine-structure.md §5, E6). Every change to the branches drops the view, so a view
/// never describes branches other than those it was built of; a copy has none.
#[derive(Debug, Default)]
struct Bundle {
    branches: Vec<Branch>,
    view: Option<View>,
    /// The view of the branches but the `added` newest, kept as flushes added them, for the
    /// view's rebuild ([`Rebuild`]), a run merged in at a time.
    basis: Option<View>,
    added: usize,
    /// The extents the view is written in, once a saved image names it.
    stored: Option<Vec<u64>>,
    /// The maplet routing a key to the branches that may hold it, values their ages (the
    /// oldest 0), once built ([`Trunk::maplet_step`]; research/38).
    maplet: Option<crate::maplet::Maplet>,
    /// Whether a maplet built for these branches measured slower than their filters: none is
    /// built again until the branches change.
    declined: bool,
}

impl Clone for Bundle {
    fn clone(&self) -> Self {
        Self {
            branches: self.branches.clone(),
            view: None,
            basis: None,
            added: 0,
            stored: None,
            maplet: None,
            declined: false,
        }
    }
}

impl From<Vec<Branch>> for Bundle {
    fn from(branches: Vec<Branch>) -> Self {
        Self {
            branches,
            view: None,
            basis: None,
            added: 0,
            stored: None,
            maplet: None,
            declined: false,
        }
    }
}

impl Bundle {
    /// The branches, newest first.
    fn branches(&self) -> &[Branch] {
        &self.branches
    }

    /// The REMIX view of the branches, once built.
    fn view(&self) -> Option<&View> {
        self.view.as_ref()
    }

    /// The branches, to change: the view is dropped.
    fn branches_mut(&mut self) -> &mut Vec<Branch> {
        self.maplet = None;
        self.declined = false;
        self.view = None;
        self.basis = None;
        self.added = 0;
        self.stored = None;
        &mut self.branches
    }

    /// Adds `b` as the newest branch: the view, if built, becomes the basis of the next, and a
    /// basis kept counts the runs added since.
    fn push_newest(&mut self, b: Branch) {
        self.stored = None;
        self.maplet = None;
        self.declined = false;
        if let Some(v) = self.view.take() {
            self.basis = Some(v);
            self.added = 1;
        } else if self.basis.is_some() {
            self.added = self.added.saturating_add(1);
        }
        self.branches.insert(0, b);
    }

    /// The branches taken out, the bundle left empty, its view dropped.
    fn take(&mut self) -> Vec<Branch> {
        self.maplet = None;
        self.declined = false;
        self.view = None;
        self.basis = None;
        self.added = 0;
        self.stored = None;
        std::mem::take(&mut self.branches)
    }
}

#[derive(Clone, Debug)]
struct Node {
    leaf: bool,
    pivots: Vec<Pivot>,
    /// Oldest first.
    inflight: Vec<Vec<Branch>>,
    /// The keys below this, if any bound them: the next node's first pivot.
    end: Option<Vec<u8>>,
    /// The settle or split this node waits to take ([`upkeep::Change`]).
    wants: Option<Change>,
    /// Queued to be looked at ([`Trunk::queue`]).
    queued: bool,
}

/// What the trunk's maintenance has done since it was made or loaded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TrunkStats {
    /// Pivots whose in-flight bundles were compacted into one branch.
    pub pivot_compactions: u64,
    /// Leaves compacted whole.
    pub leaf_compactions: u64,
    /// Pivot bundles flushed to a child.
    pub flushes: u64,
    /// Nodes made by splits, leaves and index nodes.
    pub splits: u64,
    /// Entries written into branches by compactions.
    pub entries_written: u64,
    /// Nanoseconds planning compactions (their cursors' seeks), and finishing them (the last
    /// pages and filters, and the trunk's change).
    pub plan_ns: u64,
    pub finish_ns: u64,
    /// REMIX views of pivot bundles built, and builds dropped because their bundle changed.
    pub views_built: u64,
    pub views_dropped: u64,
    /// Of the views built, those rebuilt from an older view, and the runs those rebuilds merged
    /// in (more than the rebuilds when several runs were added before one).
    pub views_rebuilt: u64,
    pub views_merged: u64,
    /// Maplets of pivot bundles built, and builds dropped because their bundle changed.
    pub maplets_built: u64,
    pub maplets_dropped: u64,
    /// Maplets measured slower than their bundles' filters, and not kept.
    pub maplets_declined: u64,
    /// Leaves settled to one branch because seeks paid for it ([`Consolidation`]).
    pub consolidations: u64,
}

/// Nanoseconds since `t`, none when timing is off (`Trunk::set_timed`).
fn ns_since(t: Option<std::time::Instant>) -> u64 {
    t.map_or(0, |t| {
        u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX)
    })
}

/// One shard's trunk.
#[derive(Debug)]
pub struct Trunk {
    nodes: Vec<Node>,
    root: usize,
    config: TrunkConfig,
    /// The extents of the last saved image, released when the next one is saved.
    saved: Vec<u64>,
    saving: Saving,
    stats: TrunkStats,
    /// Packed memtables not yet in the tree, oldest first: read before it, and moved into the
    /// root's in-flight list once the root takes no change ([`upkeep`]).
    pending: Vec<Branch>,
    /// Maintenance in progress (CLAUDE.md §10): at most one compaction a pivot and one settle a
    /// node, so bounded by the trunk's size ([`upkeep`]).
    tasks: Vec<NodeTask>,
    /// Nodes to look at for work, each queued once until looked at.
    looking: VecDeque<usize>,
    /// A worker's failure, reported once every other job out is back and its grant released.
    abandoning: Option<Error>,
    /// Whether compactions' planning and finishing are timed ([`Trunk::set_timed`]).
    timed: bool,
    /// Whether compactions stop for pages still being read ([`Self::step_paced`]), and whether
    /// one did in the run in progress.
    yield_io: bool,
    io_waiting: bool,
    /// The REMIX view being built, for the pivot it names ([`Trunk::view_step`]).
    view_job: Option<ViewJob>,
    view_costs: ViewCosts,
    view_choice: ViewChoice,
    /// The extents the last saved image's views are written in, released by the next save as the
    /// views that left them are no longer named.
    view_extents: Vec<u64>,
    /// Nodes to check for a pivot bundle wanting a view since the trunk last changed, checked
    /// round from `view_at`: every node once after a change.
    views_unchecked: usize,
    view_at: usize,
    /// The maplet being built, and the nodes to check for a pivot bundle wanting one, as views'.
    maplet_job: Option<MapletJob>,
    maplets_unchecked: usize,
    maplet_at: usize,
    /// Gets' filter routes through bundles of several branches ([`Tally`]), and gets.
    routes: Tally,
    gets: u64,
    /// Nanoseconds maintenance steps took and the keys they merged: the measured price of a
    /// key's compaction, a consolidation's price per entry it rewrites.
    merge_ns: u64,
    merge_keys: u64,
    /// Pivots whose seeks' rent covers their consolidation ([`SeekRent::paid`]).
    paid: usize,
    /// Whether a drain asked for every pivot of several sources to be consolidated
    /// ([`Self::consolidate_all`]), and whether the layout is so consolidated: no branch has
    /// entered the root since the last such consolidation finished.
    full: bool,
    consolidated: bool,
    /// Whether the run in progress may start a consolidation: idle time's alone
    /// ([`Self::consolidate_step`]), never a put's share; and whether the maintenance in progress is
    /// one, the only kind that flushes paid pivots early.
    consolidate: bool,
    consolidating: bool,
    consolidation: Consolidation,
    /// The pivots above paid ones, found before a consolidation marks them; kept for the next.
    marks: Vec<(usize, usize)>,
    /// The maintenance workers compactions run on, when the shard has any ([`Self::set_pool`]).
    pool: Option<Pool>,
    /// How long this step waits on the workers ([`Wait`]), and whether it stopped for them.
    wait: Wait,
    workers_waiting: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum SavePhase {
    #[default]
    Idle,
    Views,
    ViewPages,
    ViewRun,
    Encoding,
    ImagePages,
    ImageRun,
    Drain,
    Publish,
    Abort,
}

#[derive(Debug)]
struct SavedView {
    node: usize,
    pivot: usize,
    extents: Vec<u64>,
}

/// The checkpoint owns one run and unpublished references until its owner writes retire.
/// Bytes and list capacities stay here for the next checkpoint; no view payload is copied
/// into a second framing buffer.
#[derive(Debug, Default)]
struct Saving {
    phase: SavePhase,
    node: usize,
    pivot: usize,
    page: usize,
    extent_count: usize,
    image_limit: usize,
    bytes: Vec<u8>,
    header: Vec<u8>,
    fresh: Vec<u64>,
    views: Vec<SavedView>,
    named: Vec<u64>,
    run: Option<Run>,
    head: Option<u64>,
    error: Option<Error>,
}

/// Image magic, serialized byte count and extent count, before the extent addresses.
fn image_header_bytes() -> Result<usize, Error> {
    size_of::<[u64; 2]>()
        .checked_add(size_of::<u32>())
        .ok_or(corrupt())
}

fn save_reserve<T>(out: &mut Vec<T>, additional: usize) -> Result<(), Error> {
    out.try_reserve(additional)
        .map_err(|_| Error::LimitExceeded {
            what: "a trunk checkpoint's serialized workspace",
            limit: u64::try_from(usize::MAX).unwrap_or(u64::MAX),
        })
}

fn save32(out: &mut Vec<u8>, n: usize) -> Result<(), Error> {
    out.extend_from_slice(&u32::try_from(n).map_err(|_| corrupt())?.to_le_bytes());
    Ok(())
}

impl Saving {
    fn address<F: BlockFile>(&self, store: &Store<F>, page: usize) -> Result<u64, Error> {
        let per = usize::try_from(store.extent_pages()).map_err(|_| corrupt())?;
        let extent = *self
            .fresh
            .get(page.checked_div(per).ok_or(corrupt())?)
            .ok_or(corrupt())?;
        store.address(
            extent,
            u32::try_from(page.checked_rem(per).ok_or(corrupt())?).map_err(|_| corrupt())?,
        )
    }
}

/// How long a step waits on the maintenance workers: not at all (a put's share, a runtime's
/// slice), for one message (an idle step, which has nothing else to do), or until every job out
/// is back (a stall, a drain).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Wait {
    No,
    One,
    All,
}

/// The filters' false positives a get meets now, against those Monkey's allocation of the same
/// bits gives (research/39 §2): rates in proportion to entries over visits. Both from the gets
/// seen and the branches live.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FilterPlan {
    pub gets: u64,
    pub branches: usize,
    pub bits: f64,
    pub now: f64,
    pub best: f64,
    /// The bits Monkey's allocation needs for today's false positives a get.
    pub bits_for_now: f64,
}

/// Gets' routes through bundles of several branches by their filters: the branches such bundles
/// held and the filters probed in them. Their ratio is the share of a bundle's filters a get
/// probes, which the share of keys absent and where present keys are found set; it prices a
/// bundle's filters against its maplet ([`Trunk::maplet_step`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Tally {
    pub branches: u64,
    pub probes: u64,
}

impl Tally {
    fn add(self, other: Self) -> Self {
        Self {
            branches: self.branches.saturating_add(other.branches),
            probes: self.probes.saturating_add(other.probes),
        }
    }
}

/// A maplet being built for pivot `pivot` of node `node` from its branches' hash lists, merged
/// in hash order: a cursor and its next hash for each branch, newest first, and the build.
#[derive(Debug)]
struct MapletJob {
    node: usize,
    pivot: usize,
    roots: Vec<u64>,
    heads: Vec<(crate::branch::HashCursor, Option<u32>)>,
    /// Initial heads opened, and the winner already added while its successor is read.
    opening: usize,
    advancing: Option<usize>,
    builder: crate::maplet::Builder,
    pages: Vec<Vec<u8>>,
    /// Once merged, the maplet being measured against the bundle's filters.
    trial: Option<Trial>,
}

impl MapletJob {
    fn give_back<F: BlockFile>(mut self, store: &mut Store<F>) {
        for (cursor, _) in self.heads.drain(..) {
            cursor.give_back(store);
        }
    }
}

/// A built maplet timed against its bundle's filters on as many random hashes as it holds keys,
/// a chunk a step: its routes' nanoseconds, the filters' probes' (every filter of the bundle
/// for each hash), and the hashes timed so far; the hashes drawn from `x`.
#[derive(Debug)]
struct Trial {
    maplet: crate::maplet::Maplet,
    done: u64,
    maplet_ns: u64,
    filter_ns: u64,
    x: u64,
    hashes: Vec<u64>,
}

/// A view being built for pivot `pivot` of node `node`: from its runs, or rebuilt from the view
/// of all but the newest.
#[derive(Debug)]
struct ViewJob {
    node: usize,
    pivot: usize,
    build: Job,
    /// The work the job was predicted at, in its kind's units, and the nanoseconds it has taken.
    units: u64,
    ns: u64,
}

/// How a bundle's view is made when it can be rebuilt: by the rates measured (the default), or
/// always one way, for a test that must know which ran.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ViewChoice {
    #[default]
    Measured,
    Build,
    Rebuild,
}

/// When seeks consolidate a pivot: once their measured rent covers its measured price (the
/// default), never, or at the first seek with a source to spare, for a test that must know which
/// ran whatever the timings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Consolidation {
    #[default]
    Measured,
    Never,
    Always,
}

/// What view builds and rebuilds have cost on this machine: nanoseconds and units of work, a
/// build's unit an entry read, a rebuild's a key probe. A job is chosen by the rates measured so
/// far (CLAUDE.md §4: measured on the running hardware); both start at a nanosecond a unit.
#[derive(Clone, Copy, Debug, Default)]
struct ViewCosts {
    build_ns: u128,
    build_units: u128,
    rebuild_ns: u128,
    rebuild_units: u128,
}

impl ViewCosts {
    /// Whether a rebuild of `rebuild` units is predicted cheaper than a build of `build`.
    fn rebuild_cheaper(&self, rebuild: u64, build: u64) -> bool {
        // Rates as fractions ns/units, a unit a nanosecond until measured; compared by
        // cross-multiplying, no division.
        let (bn, bu) = if self.build_units == 0 {
            (1, 1)
        } else {
            (self.build_ns, self.build_units)
        };
        let (rn, ru) = if self.rebuild_units == 0 {
            (1, 1)
        } else {
            (self.rebuild_ns, self.rebuild_units)
        };
        u128::from(rebuild).saturating_mul(rn).saturating_mul(bu)
            < u128::from(build).saturating_mul(bn).saturating_mul(ru)
    }
}

#[derive(Debug)]
enum Job {
    /// Boxed: a build holds a cursor a run and its view as it grows.
    Build(Box<Build>),
    /// A rebuild of the view of `runs[left..]` from that of `runs[left + 1..]`, and the bundle's
    /// roots: the runs still to merge in are `runs[..left]`, newest first.
    Rebuild {
        /// Boxed: a rebuild holds two views, the build a cursor a run.
        job: Option<Box<Rebuild>>,
        left: usize,
        roots: Vec<u64>,
        lo: Vec<u8>,
        hi: Option<Vec<u8>>,
    },
}

impl Job {
    fn reads(&self, runs: &[Branch]) -> bool {
        match self {
            Job::Build(b) => b.reads(runs),
            Job::Rebuild { roots, .. } => {
                roots.len() == runs.len() && roots.iter().zip(runs).all(|(&r, b)| r == b.root)
            }
        }
    }

    /// Up to `budget` entries: the entries taken, and whether the view of every run is whole.
    /// A rebuild whose run is merged starts the next, the next newer run merged into its view.
    fn step<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        runs: &[Branch],
        budget: u64,
        yield_io: bool,
    ) -> Result<(u64, bool), Error> {
        match self {
            Job::Build(b) => b.step_yielding(store, runs, budget, yield_io),
            Job::Rebuild {
                job, left, lo, hi, ..
            } => {
                let part = runs.get(*left..).ok_or(corrupt())?;
                let (taken, done) = job.as_mut().ok_or(corrupt())?.step(store, part, budget)?;
                if !done {
                    return Ok((taken, false));
                }
                let Some(next) = left.checked_sub(1) else {
                    return Ok((taken, true));
                };
                // The view of `runs[left..]` is whole: the next newer run is merged into it.
                let part = runs.get(next..).ok_or(corrupt())?;
                let finished = job.take().ok_or(corrupt())?.finish(store);
                *job = Some(Box::new(Rebuild::new(
                    store,
                    finished,
                    part,
                    lo,
                    hi.as_deref(),
                )?));
                *left = next;
                Ok((taken, false))
            }
        }
    }

    /// Whether the last step stopped for a page still being read.
    fn waiting(&self) -> bool {
        matches!(self, Job::Build(b) if b.waiting())
    }

    fn finish<F: BlockFile>(self, store: &mut Store<F>) -> View {
        match self {
            Job::Build(b) => b.finish(store),
            Job::Rebuild { job, .. } => job.map(|j| j.finish(store)).unwrap_or_default(),
        }
    }

    fn abandon<F: BlockFile>(self, store: &mut Store<F>) {
        match self {
            Job::Build(b) => b.abandon(store),
            Job::Rebuild { job, .. } => {
                if let Some(j) = job {
                    j.abandon(store);
                }
            }
        }
    }
}

#[derive(Debug)]
enum State {
    /// Waiting for its inputs to land and a worker to be free.
    Ready(Box<Task>),
    /// Out on a worker, as the ticket its send returned names it: a worker freed while its
    /// result waits for the trunk can take the trunk's next job too.
    Out(Ticket),
    Done(Output),
    /// Back from tasks abandoned for a job's failure, its output dropped; or, for a moment,
    /// handed from ready to out.
    Dropped,
}

/// The saved image's format version.
/// Format 2: each pivot names its view's extents after its branches. Format 3: each branch's
/// descriptor names its range filter's bytes after its leaf index's.
const IMAGE_FORMAT: u8 = 4;
/// The saved image header's magic: "mantleTK" in ASCII, little-endian.
const IMAGE_MAGIC: u64 = u64::from_le_bytes(*b"mantleTK");
/// None, in a child or an end key's place.
const ABSENT: u32 = u32::MAX;

fn no_pool() -> Error {
    Error::InvalidArgument {
        what: "a compaction out on workers the trunk no longer has",
    }
}

fn corrupt() -> Error {
    Error::Corruption {
        what: "a trunk",
        why: Malformed::OutOfRange,
    }
}

/// Writes `bytes` to fresh extents, its length first: the extents, in order.
fn write_blob<F: BlockFile>(store: &mut Store<F>, bytes: &[u8]) -> Result<Vec<u64>, Error> {
    let capacity = store.page_capacity();
    let per = usize::try_from(store.extent_pages()).map_err(|_| corrupt())?;
    let mut framed = Vec::with_capacity(bytes.len().saturating_add(8));
    framed.extend_from_slice(
        &u64::try_from(bytes.len())
            .map_err(|_| corrupt())?
            .to_le_bytes(),
    );
    framed.extend_from_slice(bytes);
    let pages = framed.len().div_ceil(capacity.max(1));
    let mut extents = Vec::with_capacity(pages.div_ceil(per.max(1)));
    for _ in 0..pages.div_ceil(per.max(1)) {
        extents.push(store.allocate_extent()?);
    }
    let mut run = store.run()?;
    for (i, chunk) in framed.chunks(capacity.max(1)).enumerate() {
        let extent = *extents
            .get(i.checked_div(per).ok_or(corrupt())?)
            .ok_or(corrupt())?;
        let page = u32::try_from(i.checked_rem(per).ok_or(corrupt())?).map_err(|_| corrupt())?;
        let address = store.address(extent, page)?;
        store.queue_page(&mut run, address, chunk)?;
    }
    store.write_run(&mut run)?;
    store.give_run(run);
    Ok(extents)
}

/// The bytes [`write_blob`] wrote to `extents`.
fn read_blob<F: BlockFile>(store: &mut Store<F>, extents: &[u64]) -> Result<Vec<u8>, Error> {
    let per = store.extent_pages();
    let mut framed = Vec::new();
    let mut want: Option<usize> = None;
    'extents: for &e in extents {
        for page in 0..per {
            if want.is_some_and(|w| framed.len() >= w) {
                break 'extents;
            }
            let address = store.address(e, page)?;
            store.read_page(address, &mut framed)?;
            if want.is_none() {
                let len = framed
                    .first_chunk::<8>()
                    .map(|b| u64::from_le_bytes(*b))
                    .ok_or(corrupt())?;
                want = Some(
                    usize::try_from(len)
                        .ok()
                        .and_then(|l| l.checked_add(8))
                        .ok_or(corrupt())?,
                );
            }
        }
    }
    let want = want.ok_or(corrupt())?;
    if framed.len() != want {
        return Err(corrupt());
    }
    Ok(framed.split_off(8))
}

/// The entries and pages `branches` hold, a page counted for every page of their extents.
fn size_of_branches<'b>(
    branches: impl Iterator<Item = &'b Branch>,
    extent_pages: u64,
) -> (u64, u64) {
    branches.fold((0u64, 0u64), |(e, p), b| {
        let extents = u64::try_from(b.extents.len()).unwrap_or(u64::MAX);
        (
            e.saturating_add(b.count),
            p.saturating_add(extents.saturating_mul(extent_pages)),
        )
    })
}

fn release<F: BlockFile>(store: &mut Store<F>, b: &Branch) -> Result<(), Error> {
    for &e in &b.extents {
        store.release(e)?;
    }
    Ok(())
}

impl Trunk {
    /// An empty trunk: one leaf over every key.
    pub fn new(config: TrunkConfig) -> Result<Self, Error> {
        if config.fanout < 2 || config.leaf_entries < 2 {
            return Err(Error::InvalidArgument {
                what: "a trunk of fanout below 2 or leaves below 2 entries",
            });
        }
        Ok(Self {
            nodes: vec![Node {
                leaf: true,
                pivots: vec![Pivot {
                    key: Vec::new(),
                    child: None,
                    bundle: Bundle::default(),
                    start: 0,
                    seek: SeekRent::default(),
                }],
                inflight: Vec::new(),
                end: None,
                wants: None,
                queued: false,
            }],
            root: 0,
            config,
            saved: Vec::new(),
            saving: Saving::default(),
            stats: TrunkStats::default(),
            pending: Vec::new(),
            tasks: Vec::new(),
            looking: VecDeque::new(),
            abandoning: None,
            timed: false,
            yield_io: false,
            io_waiting: false,
            view_job: None,
            view_costs: ViewCosts::default(),
            view_choice: ViewChoice::Measured,
            view_extents: Vec::new(),
            views_unchecked: 0,
            view_at: 0,
            maplet_job: None,
            maplets_unchecked: 0,
            maplet_at: 0,
            routes: Tally::default(),
            gets: 0,
            merge_ns: 0,
            merge_keys: 0,
            paid: 0,
            full: false,
            consolidated: false,
            consolidate: false,
            consolidating: false,
            consolidation: Consolidation::Measured,
            marks: Vec::new(),
            pool: None,
            wait: Wait::No,
            workers_waiting: false,
        })
    }

    fn node(&self, n: usize) -> Result<&Node, Error> {
        self.nodes.get(n).ok_or(corrupt())
    }

    fn node_mut(&mut self, n: usize) -> Result<&mut Node, Error> {
        self.nodes.get_mut(n).ok_or(corrupt())
    }

    /// The pivot of `node` whose range holds `key`.
    fn pivot_of(node: &Node, key: &[u8]) -> usize {
        node.pivots
            .partition_point(|p| p.key.as_slice() <= key)
            .saturating_sub(1)
    }

    /// The end of pivot `i`'s range: the next pivot's key, or the node's end.
    fn pivot_end(node: &Node, i: usize) -> Option<Vec<u8>> {
        node.pivots
            .get(i.saturating_add(1))
            .map(|p| p.key.clone())
            .or_else(|| node.end.clone())
    }

    /// The newest entry for `key` in the trunk: its operation and value into `value`.
    pub fn get<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        key: &[u8],
        value: &mut Vec<u8>,
    ) -> Result<Option<Op>, Error> {
        self.get_hashed(store, key, crate::branch::filter::hash(key), value)
    }

    /// [`Self::get`] of a key whose filter hash is `hash`.
    pub fn get_hashed<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        key: &[u8],
        hash: u64,
        value: &mut Vec<u8>,
    ) -> Result<Option<Op>, Error> {
        let mut tally = Tally::default();
        let found = self.find(store, key, hash, value, &mut tally);
        self.routes = self.routes.add(tally);
        found
    }

    /// The same newest-first search through an attached owner's demand reads. The exclusive
    /// borrow retains candidate references and freezes topology until the lookup ends.
    pub(crate) async fn get_hashed_async<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        key: &[u8],
        hash: u64,
        value: &mut Vec<u8>,
    ) -> Result<Option<Op>, Error> {
        self.gets = self.gets.saturating_add(1);
        for b in self.pending.iter_mut().rev() {
            b.probes = b.probes.saturating_add(1);
            if let Some(op) = b.get_hashed_async(store, key, hash, value).await? {
                return Ok(Some(op));
            }
        }
        let mut at = self.root;
        // Every level down is a node's child: at most the trunk's node count of steps.
        for _ in 0..self.nodes.len() {
            let node = self.nodes.get_mut(at).ok_or(corrupt())?;
            let p = Self::pivot_of(node, key);
            let Node {
                pivots, inflight, ..
            } = node;
            let pivot = pivots.get_mut(p).ok_or(corrupt())?;
            for bundle in inflight
                .get_mut(pivot.start..)
                .unwrap_or_default()
                .iter_mut()
                .rev()
            {
                for b in bundle.iter_mut() {
                    b.probes = b.probes.saturating_add(1);
                    if let Some(op) = b.get_hashed_async(store, key, hash, value).await? {
                        return Ok(Some(op));
                    }
                }
            }
            let Bundle {
                branches, maplet, ..
            } = &mut pivot.bundle;
            match maplet {
                Some(m) => {
                    let mask = m.route(hash)?;
                    let n = branches.len();
                    for (i, b) in branches.iter().enumerate() {
                        let age = n.saturating_sub(1).saturating_sub(i);
                        let named = u32::try_from(age)
                            .ok()
                            .and_then(|a| 1u64.checked_shl(a))
                            .is_some_and(|bit| mask & bit != 0);
                        if named && let Some(op) = b.get_routed_async(store, key, value).await? {
                            return Ok(Some(op));
                        }
                    }
                }
                None => {
                    let several = branches.len() >= 2;
                    if several {
                        self.routes.branches = self
                            .routes
                            .branches
                            .saturating_add(u64::try_from(branches.len()).unwrap_or(u64::MAX));
                    }
                    for b in branches.iter_mut() {
                        // Commit each started probe before its wait, so cancellation does not
                        // discard work already priced for maplet eligibility.
                        if several {
                            self.routes.probes = self.routes.probes.saturating_add(1);
                        }
                        b.probes = b.probes.saturating_add(1);
                        if let Some(op) = b.get_hashed_async(store, key, hash, value).await? {
                            return Ok(Some(op));
                        }
                    }
                }
            }
            match pivot.child {
                Some(child) => at = child,
                None => return Ok(None),
            }
        }
        Err(corrupt())
    }

    /// [`Self::get_hashed`], tallying the filters it probes in bundles of several branches and
    /// counting each filter's probes on its branch ([`Branch::probes`]).
    fn find<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        key: &[u8],
        hash: u64,
        value: &mut Vec<u8>,
        tally: &mut Tally,
    ) -> Result<Option<Op>, Error> {
        self.gets = self.gets.saturating_add(1);
        for b in self.pending.iter_mut().rev() {
            b.probes = b.probes.saturating_add(1);
            if let Some(op) = b.get_hashed(store, key, hash, value)? {
                return Ok(Some(op));
            }
        }
        let mut at = self.root;
        // Every level down is a node's child: at most the trunk's node count of steps.
        for _ in 0..self.nodes.len() {
            let node = self.nodes.get_mut(at).ok_or(corrupt())?;
            let p = Self::pivot_of(node, key);
            let Node {
                pivots, inflight, ..
            } = node;
            let pivot = pivots.get_mut(p).ok_or(corrupt())?;
            for bundle in inflight
                .get_mut(pivot.start..)
                .unwrap_or_default()
                .iter_mut()
                .rev()
            {
                for b in bundle.iter_mut() {
                    b.probes = b.probes.saturating_add(1);
                    if let Some(op) = b.get_hashed(store, key, hash, value)? {
                        return Ok(Some(op));
                    }
                }
            }
            let Bundle {
                branches, maplet, ..
            } = &mut pivot.bundle;
            match maplet {
                // One lookup names the branches that may hold the key, by age (the oldest 0):
                // those are read, newest first, without their filters.
                Some(m) => {
                    let mask = m.route(hash)?;
                    let n = branches.len();
                    for (i, b) in branches.iter().enumerate() {
                        let age = n.saturating_sub(1).saturating_sub(i);
                        let named = u32::try_from(age)
                            .ok()
                            .and_then(|a| 1u64.checked_shl(a))
                            .is_some_and(|bit| mask & bit != 0);
                        if named && let Some(op) = b.get_routed(store, key, value)? {
                            return Ok(Some(op));
                        }
                    }
                }
                None => {
                    let several = branches.len() >= 2;
                    if several {
                        tally.branches = tally
                            .branches
                            .saturating_add(u64::try_from(branches.len()).unwrap_or(u64::MAX));
                    }
                    for b in branches.iter_mut() {
                        if several {
                            tally.probes = tally.probes.saturating_add(1);
                        }
                        b.probes = b.probes.saturating_add(1);
                        if let Some(op) = b.get_hashed(store, key, hash, value)? {
                            return Ok(Some(op));
                        }
                    }
                }
            }
            match pivot.child {
                Some(child) => at = child,
                None => return Ok(None),
            }
        }
        Err(corrupt())
    }

    /// The filters as gets have used them since the trunk was made or loaded, against Monkey's
    /// allocation of the same bits ([`FilterPlan`]; research/39 §2).
    pub fn filter_plan(&self) -> FilterPlan {
        let gets = self.gets.max(1) as f64;
        let ln2sq = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        // Each live branch once: (entries, filter bits, visits a get).
        let mut live: Vec<(f64, f64, f64)> = Vec::new();
        let mut add = |b: &Branch| {
            let bits = u64::try_from(b.filter.bytes())
                .unwrap_or(u64::MAX)
                .saturating_mul(8);
            live.push((b.count.max(1) as f64, bits as f64, b.probes as f64 / gets));
        };
        self.pending.iter().for_each(&mut add);
        for node in &self.nodes {
            node.inflight.iter().flatten().for_each(&mut add);
            node.pivots
                .iter()
                .flat_map(|p| p.bundle.branches())
                .for_each(&mut add);
        }
        let bits: f64 = live.iter().map(|l| l.1).sum();
        let now: f64 = live
            .iter()
            .map(|&(n, b, v)| v * (-(b / n) * ln2sq).exp())
            .sum();
        // The rate each branch gets for multiplier lambda: lambda · n / v, at most 1 (no filter),
        // and the bits that takes.
        let spend = |lambda: f64| -> (f64, f64) {
            live.iter().fold((0.0, 0.0), |(fp, used), &(n, _, v)| {
                let p = if v > 0.0 {
                    (lambda * n / v).min(1.0)
                } else {
                    1.0
                };
                (fp + v * p, used + n * (-p.ln()).max(0.0) / ln2sq)
            })
        };
        // The bits spent fall as lambda rises: bisect lambda's bit pattern (positive floats order
        // as their bits do) for the largest lambda spending no more than today's bits: at most
        // 64 halvings.
        let (mut lo, mut hi) = (f64::MIN_POSITIVE.to_bits(), f64::MAX.to_bits());
        for _ in 0..u64::BITS {
            if lo >= hi {
                break;
            }
            let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
            if spend(f64::from_bits(mid)).1 > bits {
                lo = mid.saturating_add(1);
            } else {
                hi = mid;
            }
        }
        let best = spend(f64::from_bits(hi)).0;
        // The false positives also fall as lambda falls: the largest lambda whose rates meet
        // today's, and the bits it spends.
        let (mut lo, mut hi) = (f64::MIN_POSITIVE.to_bits(), f64::MAX.to_bits());
        for _ in 0..u64::BITS {
            if lo >= hi {
                break;
            }
            let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
            if spend(f64::from_bits(mid)).0 <= now {
                lo = mid.saturating_add(1);
            } else {
                hi = mid;
            }
        }
        let bits_for_now = spend(f64::from_bits(lo.saturating_sub(1))).1;
        FilterPlan {
            gets: self.gets,
            branches: live.len(),
            bits,
            now,
            best,
            bits_for_now,
        }
    }

    /// The leaf segment holding `key`: the branches a read of a key in it probes, newest first,
    /// into `branches`, in the order [`Self::get`] probes them, and the segment's end into
    /// `end`; false when the segment runs to the trunk's end. One descent, as `get`'s: a seek
    /// finds its leaf in the tree's height of steps, whatever the range's length, and the next
    /// segment is the one holding this one's end.
    pub fn segment_at<'a>(
        &'a self,
        key: &[u8],
        sources: &mut Vec<Source<'a>>,
        end: &mut Vec<u8>,
        path: &mut Vec<Passed>,
    ) -> Result<bool, Error> {
        sources.clear();
        end.clear();
        path.clear();
        sources.extend(self.pending.iter().rev().map(Source::Branch));
        let mut bound: Option<&[u8]> = None;
        let mut at = self.root;
        // Every level down is a node's child: at most the trunk's node count of steps.
        for _ in 0..self.nodes.len() {
            let node = self.node(at)?;
            let p = Self::pivot_of(node, key);
            let pivot = node.pivots.get(p).ok_or(corrupt())?;
            let pivot_end = node
                .pivots
                .get(p.saturating_add(1))
                .map(|q| q.key.as_slice())
                .or(node.end.as_deref());
            if let Some(e) = pivot_end {
                bound = Some(match bound {
                    Some(b) if b < e => b,
                    _ => e,
                });
            }
            let before = sources.len();
            for bundle in node.inflight.get(pivot.start..).unwrap_or(&[]).iter().rev() {
                sources.extend(bundle.iter().map(Source::Branch));
            }
            // The pivot's bundle: through its view once built, else a branch a source.
            match pivot.bundle.view() {
                Some(v) => sources.push(Source::View(v, pivot.bundle.branches())),
                None => sources.extend(pivot.bundle.branches().iter().map(Source::Branch)),
            }
            let newest = match sources.get(before) {
                Some(Source::Branch(b)) => b.root,
                Some(Source::View(_, runs)) => runs.first().map_or(0, |b| b.root),
                None => 0,
            };
            path.push(Passed {
                node: at,
                pivot: p,
                first: before,
                sources: sources.len().saturating_sub(before),
                newest,
                leaf: pivot.child.is_none(),
            });
            match pivot.child {
                Some(child) => at = child,
                None => {
                    return Ok(match bound {
                        Some(b) => {
                            end.extend_from_slice(b);
                            true
                        }
                        None => false,
                    });
                }
            }
        }
        Err(corrupt())
    }

    /// Charges the pivots seeks passed (`path`, from [`Self::segment_at`], each with the
    /// nanoseconds its extra sources took that seek to open, measured) as rent for those sources:
    /// all an index pivot gave, all but one of a leaf's. A pivot whose rent since its sources
    /// took their present shape covers its consolidation is paid. The price is what the
    /// consolidation rewrites, the bundles above it on the descent included: its entries at the
    /// measured cost of a compacted key, and its pages at `pages`' measured cost to read and
    /// write one. Idle time then runs the consolidations paid for, in bulk
    /// ([`Self::consolidate_step`]); a seek does none itself, so its latency carries none.
    pub fn charge(&mut self, path: &[(Passed, u64)], pages: PageCosts) {
        let key_ns = match self.consolidation {
            Consolidation::Never => return,
            Consolidation::Always => 0,
            Consolidation::Measured => match self.merge_ns.checked_div(self.merge_keys) {
                Some(ns) => ns.max(1),
                None => return,
            },
        };
        let page_ns = if self.consolidation == Consolidation::Always {
            0
        } else {
            pages.read_ns.saturating_add(pages.write_ns)
        };
        // The entries and pages of the bundles above, from the descent's root down: each
        // segment's path starts again at the root.
        let (mut above_entries, mut above_pages) = (0u64, 0u64);
        for &(ref step, rent) in path {
            if step.node == self.root {
                (above_entries, above_pages) = (0, 0);
            }
            let (entries, pages_own) = self.consolidation_size(step, pages.extent_pages);
            let price = above_entries
                .saturating_add(entries)
                .saturating_mul(key_ns)
                .saturating_add(
                    above_pages
                        .saturating_add(pages_own)
                        .saturating_mul(page_ns),
                );
            if !step.leaf {
                let (e, p) = self.bundle_size(step, pages.extent_pages);
                above_entries = above_entries.saturating_add(e);
                above_pages = above_pages.saturating_add(p);
            }
            let Some(pivot) = self
                .nodes
                .get_mut(step.node)
                .and_then(|n| n.pivots.get_mut(step.pivot))
            else {
                continue;
            };
            let shape = (step.sources, step.newest);
            if pivot.seek.shape != shape {
                // The sources changed since the rent was paid: it priced another consolidation.
                if pivot.seek.paid {
                    self.paid = self.paid.saturating_sub(1);
                }
                pivot.seek = SeekRent {
                    shape,
                    ..SeekRent::default()
                };
            }
            if rent == 0 {
                continue;
            }
            pivot.seek.ns = pivot.seek.ns.saturating_add(rent);
            if !pivot.seek.paid && pivot.seek.ns >= price {
                pivot.seek.paid = true;
                self.paid = self.paid.saturating_add(1);
            }
        }
    }

    /// The entries and pages a pivot's own bundle and live in-flight branches hold.
    fn bundle_size(&self, step: &Passed, extent_pages: u64) -> (u64, u64) {
        let Some(node) = self.nodes.get(step.node) else {
            return (u64::MAX, u64::MAX);
        };
        let Some(pivot) = node.pivots.get(step.pivot) else {
            return (u64::MAX, u64::MAX);
        };
        let inflight = node.inflight.get(pivot.start..).unwrap_or(&[]);
        size_of_branches(
            pivot
                .bundle
                .branches()
                .iter()
                .chain(inflight.iter().flatten()),
            extent_pages,
        )
    }

    /// The entries and pages consolidating the pivot `step` names rewrites, the bundles above
    /// it aside: a leaf's whole bundle; an index pivot's own branches, and the leaf below's too
    /// when its child is one, since the settle that follows rewrites it.
    fn consolidation_size(&self, step: &Passed, extent_pages: u64) -> (u64, u64) {
        let (entries, pages) = self.bundle_size(step, extent_pages);
        let below = self
            .nodes
            .get(step.node)
            .and_then(|n| n.pivots.get(step.pivot))
            .and_then(|p| p.child)
            .and_then(|c| self.nodes.get(c))
            .filter(|c| c.leaf)
            .and_then(|c| c.pivots.first())
            .map_or((0, 0), |p| {
                size_of_branches(p.bundle.branches().iter(), extent_pages)
            });
        (
            entries.saturating_add(below.0),
            pages.saturating_add(below.1),
        )
    }

    /// Counts an idle or drain slice of maintenance that took `ns` and merged `keys`: the measured
    /// cost of a compacted key a consolidation is priced at. Timed by the shard's idle paths, a
    /// clock read a slice, never by a put's share.
    pub fn note_merge(&mut self, ns: u64, keys: u64) {
        if keys > 0 {
            self.merge_ns = self.merge_ns.saturating_add(ns);
            self.merge_keys = self.merge_keys.saturating_add(keys);
        }
    }

    /// When seeks consolidate a pivot ([`Consolidation`]).
    pub fn set_consolidation(&mut self, choice: Consolidation) {
        self.consolidation = choice;
    }

    /// Whether a consolidation is owed: seeks paid for one, or a drain asked for one of a
    /// layout not consolidated since branches last entered the root.
    pub fn consolidation_owed(&self) -> bool {
        self.paid > 0 || (self.full && !self.consolidated)
    }

    /// A drain's request: the next consolidation rewrites every pivot of several sources, seeks'
    /// rent or not, so the layout it leaves serves every seek from one source. Nothing is owed
    /// when the layout is consolidated already, or consolidation is off.
    pub fn consolidate_all(&mut self) {
        self.full = self.consolidation != Consolidation::Never && !self.consolidated;
    }

    /// Marks every pivot above a paid one, so a consolidation's descent from the root reaches
    /// each: the marks are set again from the keys, since splits since the charge may have
    /// replaced the pivots above.
    fn mark_paid_paths(&mut self) -> Result<(), Error> {
        if self.full {
            // A drain's: every pivot whose sources are several, an index pivot holding
            // branches, a leaf more than one.
            for node in &mut self.nodes {
                let leaf = node.leaf;
                for p in &mut node.pivots {
                    let n = p.bundle.branches().len();
                    if !p.seek.paid && (if leaf { n > 1 } else { n > 0 }) {
                        p.seek.paid = true;
                        self.paid = self.paid.saturating_add(1);
                    }
                }
            }
        }
        let mut marks = std::mem::take(&mut self.marks);
        marks.clear();
        for node in &self.nodes {
            for p in node.pivots.iter().filter(|p| p.seek.paid) {
                // Down from the root by the paid pivot's key, to it: at most the height.
                let mut at = self.root;
                for _ in 0..self.nodes.len() {
                    let n = self.node(at)?;
                    let i = Self::pivot_of(n, &p.key);
                    let q = n.pivots.get(i).ok_or(corrupt())?;
                    if q.seek.paid && q.key == p.key {
                        break;
                    }
                    marks.push((at, i));
                    match q.child {
                        Some(c) => at = c,
                        None => break,
                    }
                }
            }
        }
        for node in &mut self.nodes {
            for p in &mut node.pivots {
                p.seek.below = false;
            }
        }
        for &(n, i) in &marks {
            if let Some(p) = self.node_mut(n)?.pivots.get_mut(i) {
                p.seek.below = true;
            }
        }
        self.marks = marks;
        // The count of paid pivots, from the pivots themselves: what a consolidation descends to.
        self.paid = self
            .nodes
            .iter()
            .map(|n| n.pivots.iter().filter(|p| p.seek.paid).count())
            .fold(0usize, usize::saturating_add);
        Ok(())
    }

    /// [`Self::step`] for idle time: with nothing else to do, a consolidation seeks paid for
    /// ([`Self::consolidation_owed`]) starts.
    pub fn consolidate_step<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        budget: u64,
        paced: bool,
    ) -> Result<u64, Error> {
        self.consolidate = true;
        let used = if paced {
            self.step_paced(store, budget)
        } else {
            self.step(store, budget)
        };
        self.consolidate = false;
        used
    }

    /// Clears pivot `i` of node `n`'s rent once its consolidation is done or planned.
    fn spend(&mut self, n: usize, i: usize) -> Result<(), Error> {
        let pivot = self.node_mut(n)?.pivots.get_mut(i).ok_or(corrupt())?;
        let was = pivot.seek.paid;
        pivot.seek = SeekRent::default();
        if was {
            self.paid = self.paid.saturating_sub(1);
        }
        Ok(())
    }

    /// Takes a packed memtable into the trunk and runs all the maintenance it sets off to the end.
    /// The branch's references are the trunk's from here.
    pub fn incorporate<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        branch: Branch,
    ) -> Result<(), Error> {
        self.add(branch);
        self.drain(store)
    }

    /// Takes a packed memtable as pending: read before the tree, taken into the root when the
    /// next step ([`Self::step`]). Its references are the trunk's from here.
    pub fn add(&mut self, branch: Branch) {
        self.pending.push(branch);
    }

    /// Packed memtables pending.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Runs maintenance for up to `budget` work units: one per initial leaf positioning move
    /// or merged key, and filter pages at their worth in keys. Structural planning, a flush by
    /// reference and a split cost no budget; each is taken once, so a step ends. Returns the
    /// units spent, on the tasks in progress and those every pending branch sets off.
    /// With maintenance workers ([`Self::set_pool`]), a step of the whole budget waits until
    /// the jobs out are back; any other step waits for none.
    pub fn step<F: BlockFile>(&mut self, store: &mut Store<F>, budget: u64) -> Result<u64, Error> {
        let wait = if budget == u64::MAX {
            Wait::All
        } else {
            Wait::No
        };
        self.step_with(store, budget, wait)
    }

    fn step_with<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        budget: u64,
        wait: Wait,
    ) -> Result<u64, Error> {
        if self.saving() {
            return Err(Error::InvalidArgument {
                what: "a trunk changed while its checkpoint is being saved",
            });
        }
        self.views_unchecked = self.nodes.len();
        self.maplets_unchecked = self.nodes.len();
        self.wait = wait;
        self.run(store, budget)
    }

    /// [`Self::step`] for an idle shard, which has nothing else to do: with compactions out on
    /// the workers, it waits for one of their messages rather than return at once to be called
    /// again.
    pub fn idle_step<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        budget: u64,
    ) -> Result<u64, Error> {
        let wait = if budget == u64::MAX {
            Wait::All
        } else {
            Wait::One
        };
        self.step_with(store, budget, wait)
    }

    /// Compactions the workers hold, which the shard's steps take
    /// back ([`Self::step`]).
    pub fn workers_out(&self) -> bool {
        self.pool.as_ref().is_some_and(|p| p.out_for(Owner::Trunk))
    }

    /// Ready compactions cannot take a seat until packing progresses. A packing can still
    /// need its owner to feed it, so the owner must not wait for its Done inside the trunk.
    pub(crate) fn packing_blocks_dispatch(&self) -> bool {
        self.pool.as_ref().is_some_and(|pool| {
            !pool.can_take(true) && !pool.out_for(Owner::Trunk) && pool.out_for(Owner::Pack)
        })
    }

    /// Hands the trunk maintenance workers: from the next compaction planned on, compactions
    /// run on them. Tasks already planned keep where they run.
    pub fn set_pool(&mut self, pool: Pool) {
        self.pool = Some(pool);
    }

    /// The maintenance workers, for the shard's packing.
    pub fn pool_mut(&mut self) -> Option<&mut Pool> {
        self.pool.as_mut()
    }

    /// [`Self::view_step`] for a step that may not wait for the device, as
    /// [`Self::step_paced`]: a build stops at a page still being read
    /// ([`Self::waiting_for_io`]), and a later step finds it read.
    pub(crate) fn view_step_paced<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        budget: u64,
    ) -> Result<u64, Error> {
        self.yield_io = true;
        let used = self.view_step(store, budget);
        self.yield_io = false;
        used
    }

    /// Every admitted worker has acknowledged its device setup.
    pub(crate) fn workers_prepared(&self) -> bool {
        self.pool.as_ref().is_some_and(Pool::prepared)
    }

    pub(crate) fn validate_worker_preparation(&self) -> Result<(), Error> {
        if self.inline_out() {
            return Err(Error::InvalidArgument {
                what: "an inline compaction before runtime worker preparation",
            });
        }
        self.pool.as_ref().ok_or(no_pool())?.can_prepare()
    }

    /// Whether the trunk has maintenance workers.
    pub fn has_pool(&self) -> bool {
        self.pool.is_some()
    }

    /// The worker capacity admitted by the shard's memory reservation.
    pub(crate) fn pool_capacity(&self) -> usize {
        self.pool.as_ref().map_or(0, Pool::capacity)
    }

    /// Stops the workers, each job still out run to its end and dropped.
    pub fn stop_workers(&mut self) {
        self.pool = None;
    }

    /// Warm worker seats held and active workers wanted, for diagnosis.
    pub fn pool_workers(&self) -> Option<(usize, usize)> {
        self.pool.as_ref().map(|p| (p.workers(), p.want()))
    }

    /// [`Self::step`] for a put's share: a compaction whose initial or next input page has not landed
    /// stops rather than wait for the device, and the step ends there, its budget unspent and
    /// still owed ([`Self::debt`]). The read is handed to the issuer once its input write has
    /// landed and a batch is available; a later step finds it read. A stall's
    /// [`Self::make_room`] and idle [`Self::step`]s wait.
    pub fn step_paced<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        budget: u64,
    ) -> Result<u64, Error> {
        self.yield_io = true;
        let used = self.step_with(store, budget, Wait::No);
        self.yield_io = false;
        used
    }

    /// The last paced step stopped for a required input page that had not landed.
    pub(crate) fn waiting_for_io(&self) -> bool {
        self.io_waiting || self.workers_waiting
    }

    /// A new idle slice reports only a wait encountered by that slice's trunk step.
    pub(crate) fn clear_io_wait(&mut self) {
        self.io_waiting = false;
        self.workers_waiting = false;
        if let Some(pool) = self.pool.as_mut() {
            pool.clear_wait();
        }
    }

    pub(crate) fn waiting_for_worker(&self) -> bool {
        self.pool.as_ref().is_some_and(Pool::waiting)
    }

    /// How views are made when they can be rebuilt ([`ViewChoice`]).
    pub fn set_view_choice(&mut self, choice: ViewChoice) {
        self.view_choice = choice;
    }

    /// Whether a pivot bundle may want a maplet: one being built, or nodes not checked since the
    /// trunk changed.
    pub fn maplets_owed(&self) -> bool {
        self.maplet_job.is_some() || self.maplets_unchecked > 0
    }

    /// Up to `budget` hashes of maplet building, for the shard's idle time, as [`Self::view_step`]
    /// builds views: the build in progress, if its pivot's bundle is still the one it reads; else
    /// a check of up to `budget` nodes for a pivot bundle of two branches or more with no maplet,
    /// whose build then starts. A bundle's maplet is merged from its branches' hash lists in hash
    /// order, its values their ages, without reading their entries (research/38 §5). Returns the
    /// work done, at least one while maplets are owed.
    pub fn maplet_step<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        budget: u64,
    ) -> Result<u64, Error> {
        if self.saving() {
            return Err(Error::InvalidArgument {
                what: "a trunk changed while its checkpoint is being saved",
            });
        }
        if let Some(mut job) = self.maplet_job.take() {
            let advanced = self.advance_maplet(store, &mut job, budget);
            match advanced {
                Ok((work, true)) => {
                    self.maplet_job = Some(job);
                    return Ok(work);
                }
                Ok((work, false)) => {
                    job.give_back(store);
                    return Ok(work);
                }
                Err(error) => {
                    job.give_back(store);
                    return Err(error);
                }
            }
        }
        let mut checked = 0u64;
        while self.maplets_unchecked > 0 && checked < budget.max(1) {
            self.maplets_unchecked = self.maplets_unchecked.saturating_sub(1);
            checked = checked.saturating_add(1);
            let n = self.maplet_at.checked_rem(self.nodes.len()).unwrap_or(0);
            self.maplet_at = n.saturating_add(1);
            let Some(node) = self.nodes.get(n) else {
                continue;
            };
            let wanting = node.pivots.iter().position(|p| {
                p.bundle.branches().len() >= 2 && p.bundle.maplet.is_none() && !p.bundle.declined
            });
            let Some(i) = wanting else { continue };
            let branches = self.bundle_of(n, i).ok_or(corrupt())?;
            let entries = branches
                .iter()
                .map(|b| b.count)
                .fold(0u64, u64::saturating_add);
            let value_bits =
                crate::maplet::ceil_log2(u64::try_from(branches.len()).unwrap_or(u64::MAX));
            let builder = crate::maplet::Builder::new(
                crate::maplet::bucket_bits(entries),
                value_bits,
                store.page_capacity(),
            )?;
            let mut heads = Vec::with_capacity(branches.len());
            for b in branches {
                // Each cursor is owned before its first read; opening is budgeted below.
                heads.push((b.hashes(store)?, None));
            }
            self.maplet_job = Some(MapletJob {
                node: n,
                pivot: i,
                roots: branches.iter().map(|b| b.root).collect(),
                heads,
                opening: 0,
                advancing: None,
                builder,
                pages: Vec::new(),
                trial: None,
            });
            // The node again, for its other pivots.
            self.maplet_at = n;
            self.maplets_unchecked = self.maplets_unchecked.saturating_add(1);
            break;
        }
        Ok(checked.max(1))
    }

    /// A paced idle maplet retains its hash cursor and unfinished opening/advance until
    /// the real device receipt; normal standalone maintenance keeps its synchronous API.
    pub(crate) fn maplet_step_paced<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        budget: u64,
    ) -> Result<u64, Error> {
        self.yield_io = true;
        let used = self.maplet_step(store, budget);
        self.yield_io = false;
        used
    }

    /// Drops optional hash work before checkpoint/terminal retirement or a structural
    /// turn can replace its branch identity. Any read remains owned by Store to retire.
    pub(crate) fn abandon_maplet<F: BlockFile>(&mut self, store: &mut Store<F>) {
        if let Some(job) = self.maplet_job.take() {
            job.give_back(store);
            self.stats.maplets_dropped = self.stats.maplets_dropped.saturating_add(1);
            self.maplets_unchecked = self.maplets_unchecked.max(self.nodes.len());
        }
    }

    /// Optional views retain cursors by root address. Return them before structural or
    /// durable turnover can reuse that identity; the same bundle remains eligible later.
    pub(crate) fn abandon_view<F: BlockFile>(&mut self, store: &mut Store<F>) {
        if let Some(job) = self.view_job.take() {
            job.build.abandon(store);
            self.stats.views_dropped = self.stats.views_dropped.saturating_add(1);
            self.views_unchecked = self.views_unchecked.max(self.nodes.len());
        }
    }

    fn advance_maplet<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        job: &mut MapletJob,
        budget: u64,
    ) -> Result<(u64, bool), Error> {
        let same = self
            .bundle_of(job.node, job.pivot)
            .is_some_and(|bs| bs.iter().map(|b| b.root).eq(job.roots.iter().copied()));
        if !same {
            self.stats.maplets_dropped = self.stats.maplets_dropped.saturating_add(1);
            return Ok((1, false));
        }
        let branches = self.bundle_of(job.node, job.pivot).ok_or(corrupt())?;
        if let Some(mut trial) = job.trial.take() {
            let entries = trial.maplet.shape.entries.max(1);
            let chunk = budget.max(1).min(entries.saturating_sub(trial.done));
            trial.hashes.clear();
            for _ in 0..chunk {
                trial.x ^= trial.x << 13;
                trial.x ^= trial.x >> 7;
                trial.x ^= trial.x << 17;
                trial.hashes.push(trial.x);
            }
            let mut sink = 0u64;
            let t = std::time::Instant::now();
            for &h in &trial.hashes {
                sink ^= trial.maplet.route(h)?;
            }
            let routed = t.elapsed();
            let t = std::time::Instant::now();
            for &h in &trial.hashes {
                for b in branches {
                    sink ^= u64::from(b.filter.may_contain(h));
                }
            }
            let probed = t.elapsed();
            std::hint::black_box(sink);
            let ns = |d: std::time::Duration| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
            trial.maplet_ns = trial.maplet_ns.saturating_add(ns(routed));
            trial.filter_ns = trial.filter_ns.saturating_add(ns(probed));
            trial.done = trial.done.saturating_add(chunk);
            if trial.done < entries {
                job.trial = Some(trial);
                return Ok((chunk.max(1), true));
            }
            // Kept when a route through it costs less than the filters' expected share: per
            // route, maplet_ns / n against (filter_ns / (n k)) · k · probes / branches, the
            // tally's share of a bundle's filters a get probes; with no tally yet, every
            // filter (an absent key's route).
            let routes = if self.routes.branches == 0 {
                Tally {
                    branches: 1,
                    probes: 1,
                }
            } else {
                self.routes
            };
            let keep = u128::from(trial.maplet_ns).saturating_mul(u128::from(routes.branches))
                < u128::from(trial.filter_ns).saturating_mul(u128::from(routes.probes));
            if let Some(p) = self
                .nodes
                .get_mut(job.node)
                .and_then(|nd| nd.pivots.get_mut(job.pivot))
            {
                if keep {
                    p.bundle.maplet = Some(trial.maplet);
                    self.stats.maplets_built = self.stats.maplets_built.saturating_add(1);
                } else {
                    p.bundle.declined = true;
                    self.stats.maplets_declined = self.stats.maplets_declined.saturating_add(1);
                }
            }
            return Ok((chunk.max(1), false));
        }
        let branches = self.bundle_of(job.node, job.pivot).ok_or(corrupt())?;
        let n = branches.len();
        let mut work = 0u64;
        // Opening and adding each hash cost one unit; successor completion is retained
        // continuation of its already-paid add, so a Waiting step may do zero work.
        while job.opening < n && work < budget.max(1) {
            let i = job.opening;
            let (cursor, head) = job.heads.get_mut(i).ok_or(corrupt())?;
            let branch = branches.get(i).ok_or(corrupt())?;
            let next = if self.yield_io {
                cursor.next_paced(store, branch)?
            } else {
                crate::branch::HashStep::Done(cursor.next(store, branch)?)
            };
            match next {
                crate::branch::HashStep::Waiting => {
                    self.io_waiting = true;
                    return Ok((work, true));
                }
                crate::branch::HashStep::Done(next) => {
                    *head = next;
                    job.opening = job.opening.checked_add(1).ok_or(corrupt())?;
                    work = work.saturating_add(1);
                }
            }
        }
        if job.opening < n {
            return Ok((work, true));
        }
        while work < budget.max(1) {
            if let Some(i) = job.advancing {
                let (cursor, head) = job.heads.get_mut(i).ok_or(corrupt())?;
                let branch = branches.get(i).ok_or(corrupt())?;
                let next = if self.yield_io {
                    cursor.next_paced(store, branch)?
                } else {
                    crate::branch::HashStep::Done(cursor.next(store, branch)?)
                };
                match next {
                    crate::branch::HashStep::Waiting => {
                        self.io_waiting = true;
                        return Ok((work, true));
                    }
                    crate::branch::HashStep::Done(next) => {
                        *head = next;
                        job.advancing = None;
                    }
                }
            }
            let next = job
                .heads
                .iter()
                .enumerate()
                .filter_map(|(i, (_, h))| h.map(|h| (h, i)))
                .min();
            let Some((hash, i)) = next else { break };
            let age = n.saturating_sub(1).saturating_sub(i);
            let pages = &mut job.pages;
            job.builder.add(
                hash,
                u8::try_from(age).map_err(|_| corrupt())?,
                &mut |page| {
                    pages.push(page.to_vec());
                    Ok(())
                },
            )?;
            *job.heads
                .get_mut(i)
                .map(|(_, head)| head)
                .ok_or(corrupt())? = None;
            job.advancing = Some(i);
            work = work.saturating_add(1);
        }
        if job.advancing.is_some() || job.heads.iter().any(|(_, head)| head.is_some()) {
            return Ok((work, true));
        }
        let builder = std::mem::replace(
            &mut job.builder,
            crate::maplet::Builder::new(crate::maplet::BUCKETS.trailing_zeros(), 0, 1)?,
        );
        let mut pages = std::mem::take(&mut job.pages);
        let shape = builder.close(&mut |p: &[u8]| {
            pages.push(p.to_vec());
            Ok(())
        })?;
        for (cursor, _) in job.heads.drain(..) {
            cursor.give_back(store);
        }
        job.trial = Some(Trial {
            maplet: crate::maplet::Maplet { shape, pages },
            done: 0,
            maplet_ns: 0,
            filter_ns: 0,
            x: job.roots.first().copied().unwrap_or(1) | 1,
            hashes: Vec::new(),
        });
        Ok((work.max(1), true))
    }

    /// Whether a pivot bundle may want a view: one being built, or nodes not checked since the
    /// trunk changed.
    pub fn views_owed(&self) -> bool {
        self.view_job.is_some() || self.views_unchecked > 0
    }

    /// Up to `budget` entries of view building, for the shard's idle time (SILK: deep work in the
    /// time requests leave; docs/design/engine-structure.md §5, E6): the build in progress, if its
    /// pivot's bundle is still the one it reads, else a check of up to `budget` nodes for a pivot
    /// bundle of two runs or more with no view, whose build then starts. Returns the work done,
    /// at least one while views are owed.
    pub fn view_step<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        budget: u64,
    ) -> Result<u64, Error> {
        if self.saving() {
            return Err(Error::InvalidArgument {
                what: "a trunk changed while its checkpoint is being saved",
            });
        }
        let yield_io = self.yield_io;
        if let Some(job) = self.view_job.take() {
            let done = {
                let runs = self.bundle_of(job.node, job.pivot);
                // A rebuild reads where its merge points fall, which a step that may not wait
                // cannot ready ahead: one begun by a step that could wait is dropped, and a build
                // that yields takes its place.
                let resumable = !yield_io || matches!(job.build, Job::Build(_));
                match runs {
                    Some(runs) if resumable && job.build.reads(runs) => {
                        let mut job = job;
                        let t = std::time::Instant::now();
                        let (taken, done) = match job.build.step(store, runs, budget, yield_io) {
                            Ok(done) => done,
                            Err(error) => {
                                job.build.abandon(store);
                                return Err(error);
                            }
                        };
                        self.io_waiting |= job.build.waiting();
                        job.ns = job.ns.saturating_add(
                            u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX),
                        );
                        Some((job, taken, done))
                    }
                    _ => {
                        job.build.abandon(store);
                        self.stats.views_dropped = self.stats.views_dropped.saturating_add(1);
                        None
                    }
                }
            };
            let mut work = 1u64;
            if let Some((job, taken, done)) = done {
                work = taken.max(1);
                if done {
                    let c = &mut self.view_costs;
                    let (ns, units) = (u128::from(job.ns), u128::from(job.units));
                    if matches!(job.build, Job::Rebuild { .. }) {
                        c.rebuild_ns = c.rebuild_ns.saturating_add(ns);
                        c.rebuild_units = c.rebuild_units.saturating_add(units);
                    } else {
                        c.build_ns = c.build_ns.saturating_add(ns);
                        c.build_units = c.build_units.saturating_add(units);
                    }
                    let view = job.build.finish(store);
                    if let Some(p) = self
                        .nodes
                        .get_mut(job.node)
                        .and_then(|n| n.pivots.get_mut(job.pivot))
                    {
                        p.bundle.view = Some(view);
                        p.bundle.stored = None;
                        self.stats.views_built = self.stats.views_built.saturating_add(1);
                    }
                } else {
                    self.view_job = Some(job);
                }
            }
            return Ok(work);
        }
        let mut checked = 0u64;
        while self.views_unchecked > 0 && checked < budget.max(1) {
            self.views_unchecked = self.views_unchecked.saturating_sub(1);
            checked = checked.saturating_add(1);
            let n = self.view_at.checked_rem(self.nodes.len()).unwrap_or(0);
            self.view_at = n.saturating_add(1);
            let Some(node) = self.nodes.get(n) else {
                continue;
            };
            let wanting = node.pivots.iter().position(|p| {
                let runs = p.bundle.branches().len();
                (2..=MAX_VIEW_RUNS).contains(&runs) && p.bundle.view().is_none()
            });
            if let Some(i) = wanting {
                let lo = node.pivots.get(i).map(|p| p.key.clone()).ok_or(corrupt())?;
                let hi = Self::pivot_end(node, i);
                // The view of all but the newest run, when it describes them: rebuilt by merging
                // the newest in; else built from the runs.
                let (basis, added) = self
                    .nodes
                    .get_mut(n)
                    .and_then(|node| node.pivots.get_mut(i))
                    .map(|p| (p.bundle.basis.take(), std::mem::take(&mut p.bundle.added)))
                    .unwrap_or((None, 0));
                let runs = self.bundle_of(n, i).ok_or(corrupt())?;
                let older = runs.get(added..).unwrap_or(&[]);
                // The predicted work of each: a build reads every entry; a rebuild gallops each
                // new key over the old entries, about twice the log of the old a new key passes.
                let entries =
                    |bs: &[Branch]| bs.iter().map(|b| b.count).fold(0u64, u64::saturating_add);
                let new = entries(runs.get(..added).unwrap_or(&[]));
                let old_n = entries(older);
                let gap = old_n.checked_div(new.max(1)).unwrap_or(0).saturating_add(1);
                let log = u64::from(u64::BITS.saturating_sub(gap.leading_zeros()));
                let rebuild_units = new.saturating_mul(log.saturating_mul(2)).max(1);
                let build_units = entries(runs).max(1);
                let cheaper = match self.view_choice {
                    ViewChoice::Measured => {
                        self.view_costs.rebuild_cheaper(rebuild_units, build_units)
                    }
                    ViewChoice::Build => false,
                    ViewChoice::Rebuild => true,
                };
                let build = match (basis, added.checked_sub(1)) {
                    // A step that may not wait builds, reading its runs in order, a page ready
                    // ahead of each move.
                    _ if yield_io => {
                        Job::Build(Box::new(Build::prepared(runs, &lo, hi.as_deref())?))
                    }
                    (Some(old), Some(left))
                        if cheaper
                            && old.roots().len() == older.len()
                            && old.roots().iter().zip(older).all(|(&r, b)| r == b.root) =>
                    {
                        // The oldest added run first: merged into the basis, then each newer.
                        let part = runs.get(left..).ok_or(corrupt())?;
                        Job::Rebuild {
                            job: Some(Box::new(Rebuild::new(
                                store,
                                old,
                                part,
                                &lo,
                                hi.as_deref(),
                            )?)),
                            left,
                            roots: runs.iter().map(|b| b.root).collect(),
                            lo: lo.clone(),
                            hi: hi.clone(),
                        }
                    }
                    _ => Job::Build(Box::new(Build::new(store, runs, &lo, hi.as_deref())?)),
                };
                if matches!(build, Job::Rebuild { .. }) {
                    self.stats.views_rebuilt = self.stats.views_rebuilt.saturating_add(1);
                    self.stats.views_merged = self
                        .stats
                        .views_merged
                        .saturating_add(u64::try_from(added).unwrap_or(u64::MAX));
                }
                let units = if matches!(build, Job::Rebuild { .. }) {
                    rebuild_units
                } else {
                    build_units
                };
                self.view_job = Some(ViewJob {
                    node: n,
                    pivot: i,
                    build,
                    units,
                    ns: 0,
                });
                // The node again, for its other pivots.
                self.view_at = n;
                self.views_unchecked = self.views_unchecked.saturating_add(1);
                break;
            }
        }
        Ok(checked.max(1))
    }

    /// The branches of pivot `pivot` of node `node`, newest first; none when there is no such
    /// pivot.
    fn bundle_of(&self, node: usize, pivot: usize) -> Option<&[Branch]> {
        Some(self.nodes.get(node)?.pivots.get(pivot)?.bundle.branches())
    }

    /// Times compactions' planning and finishing into [`TrunkStats`]: off by default.
    pub fn set_timed(&mut self, on: bool) {
        self.timed = on;
    }

    /// The trunk's fanout.
    pub fn fanout(&self) -> usize {
        self.config.fanout
    }

    /// The root's frame returned `parts`: more than one is a split, and a new root over them.
    fn grow_root(&mut self, parts: Vec<(Vec<u8>, usize)>) {
        if parts.len() <= 1 {
            return;
        }
        let pivots = parts
            .into_iter()
            .map(|(key, child)| Pivot {
                key,
                child: Some(child),
                bundle: Bundle::default(),
                start: 0,
                seek: SeekRent::default(),
            })
            .collect();
        self.nodes.push(Node {
            leaf: false,
            pivots,
            inflight: Vec::new(),
            end: None,
            wants: None,
            queued: false,
        });
        self.root = self.nodes.len().saturating_sub(1);
    }

    /// Writes the trunk's image for a checkpoint and returns its header page, which the
    /// superblock names as the root: the nodes, each pivot's key, child and branches, serialized
    /// into pages of fresh extents. The previous image's extents are released now and freed once
    /// the checkpoint naming this one is durable (the store's deferred free). Every node's
    /// in-flight list is empty between incorporations, which the image relies on.
    pub fn save<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<u64, Error> {
        if self.saving() {
            return Err(Error::InvalidArgument {
                what: "a synchronous save during a paced checkpoint",
            });
        }
        if !self.is_idle() {
            return Err(Error::InvalidArgument {
                what: "a trunk saved with maintenance pending",
            });
        }
        self.abandon_maplet(store);
        self.abandon_view(store);
        // Views not yet written go to fresh extents of their own, which the image then names.
        for node in &mut self.nodes {
            for p in &mut node.pivots {
                if let (Some(v), None) = (&p.bundle.view, &p.bundle.stored) {
                    let mut bytes = Vec::new();
                    v.encode(&mut bytes)?;
                    p.bundle.stored = Some(write_blob(store, &bytes)?);
                }
            }
        }
        let mut blob = Vec::new();
        blob.push(IMAGE_FORMAT);
        let put32 = |v: &mut Vec<u8>, n: usize| -> Result<(), Error> {
            v.extend_from_slice(&u32::try_from(n).map_err(|_| corrupt())?.to_le_bytes());
            Ok(())
        };
        put32(&mut blob, self.root)?;
        put32(&mut blob, self.config.fanout)?;
        blob.extend_from_slice(&self.config.leaf_entries.to_le_bytes());
        put32(&mut blob, self.nodes.len())?;
        for node in &self.nodes {
            if !node.inflight.is_empty() {
                return Err(corrupt());
            }
            blob.push(u8::from(node.leaf));
            match &node.end {
                Some(end) => {
                    put32(&mut blob, end.len())?;
                    blob.extend_from_slice(end);
                }
                None => blob.extend_from_slice(&ABSENT.to_le_bytes()),
            }
            put32(&mut blob, node.pivots.len())?;
            for p in &node.pivots {
                put32(&mut blob, p.key.len())?;
                blob.extend_from_slice(&p.key);
                match p.child {
                    Some(c) => put32(&mut blob, c)?,
                    None => blob.extend_from_slice(&ABSENT.to_le_bytes()),
                }
                put32(&mut blob, p.bundle.branches().len())?;
                for b in p.bundle.branches() {
                    b.encode(&mut blob)?;
                }
                // The view's extents, none when the bundle has no view.
                let extents = p.bundle.stored.as_deref().unwrap_or(&[]);
                put32(&mut blob, extents.len())?;
                for e in extents {
                    blob.extend_from_slice(&e.to_le_bytes());
                }
            }
        }
        // The header page, then the blob's pages, in fresh extents.
        let capacity = store.page_capacity();
        let per = usize::try_from(store.extent_pages()).map_err(|_| corrupt())?;
        let pages = blob.len().div_ceil(capacity).saturating_add(1);
        let extents = pages.div_ceil(per);
        let header_room = capacity.saturating_sub(20) / 8;
        if extents > header_room {
            return Err(Error::LimitExceeded {
                what: "extents a trunk image's header names",
                limit: u64::try_from(header_room).unwrap_or(u64::MAX),
            });
        }
        let mut fresh = Vec::with_capacity(extents);
        for _ in 0..extents {
            fresh.push(store.allocate_extent()?);
        }
        let address_of = |store: &Store<F>, i: usize| -> Result<u64, Error> {
            let extent = *fresh
                .get(i.checked_div(per).ok_or(corrupt())?)
                .ok_or(corrupt())?;
            store.address(
                extent,
                u32::try_from(i.checked_rem(per).ok_or(corrupt())?).map_err(|_| corrupt())?,
            )
        };
        let mut header = Vec::with_capacity(extents.saturating_mul(8).saturating_add(20));
        header.extend_from_slice(&IMAGE_MAGIC.to_le_bytes());
        header.extend_from_slice(
            &u64::try_from(blob.len())
                .map_err(|_| corrupt())?
                .to_le_bytes(),
        );
        put32(&mut header, extents)?;
        for e in &fresh {
            header.extend_from_slice(&e.to_le_bytes());
        }
        let head = address_of(store, 0)?;
        let mut run = store.run()?;
        store.queue_page(&mut run, head, &header)?;
        for (i, chunk) in blob.chunks(capacity).enumerate() {
            let address = address_of(store, i.saturating_add(1))?;
            store.queue_page(&mut run, address, chunk)?;
        }
        store.write_run(&mut run)?;
        for e in std::mem::replace(&mut self.saved, fresh) {
            store.release(e)?;
        }
        // The views' extents the new image names; those only the last named are released.
        let mut named: Vec<u64> = self
            .nodes
            .iter()
            .flat_map(|n| n.pivots.iter())
            .filter_map(|p| p.bundle.stored.as_deref())
            .flatten()
            .copied()
            .collect();
        named.sort_unstable();
        for &e in &self.view_extents {
            if named.binary_search(&e).is_err() {
                store.release(e)?;
            }
        }
        self.view_extents = named;
        Ok(head)
    }

    /// A checkpoint's immutable image is still owned by the trunk. Its caller must not
    /// incorporate new branches or change allocator structure until save or abort completes.
    pub(crate) fn saving(&self) -> bool {
        self.saving.phase != SavePhase::Idle
    }

    /// Saves one retained image in budgeted node/page steps. Unfinished pages remain owned;
    /// no view or old image reference changes before every owner write has retired.
    pub(crate) fn save_paced<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        budget: u64,
    ) -> Result<Option<u64>, Error> {
        if !self.saving() {
            if !self.is_idle() {
                return Err(Error::InvalidArgument {
                    what: "a trunk saved with maintenance pending",
                });
            }
            self.abandon_maplet(store);
            self.abandon_view(store);
            let capacity = store.page_capacity();
            let per = usize::try_from(store.extent_pages()).map_err(|_| corrupt())?;
            if capacity == 0 || per == 0 {
                return Err(corrupt());
            }
            let room = capacity
                .saturating_sub(image_header_bytes()?)
                .checked_div(size_of::<u64>())
                .ok_or(corrupt())?;
            // The image header names these extents; one page is the header itself.
            self.saving.image_limit = room
                .checked_mul(per)
                .and_then(|n| n.checked_sub(1))
                .and_then(|n| n.checked_mul(capacity))
                .ok_or(Error::LimitExceeded {
                    what: "extents a trunk image's header names",
                    limit: u64::try_from(room).unwrap_or(u64::MAX),
                })?;
            self.saving.run = Some(store.run()?);
            self.saving.phase = SavePhase::Views;
            self.saving.node = 0;
            self.saving.pivot = 0;
        }
        // Each pass encodes one node, allocates one extent, or accepts one page/run. A
        // completion wait leaves the cursor unchanged and consumes none of the next page.
        for _ in 0..budget {
            match self.save_step(store) {
                Ok((false, None)) => return Ok(None),
                Ok((_, Some(head))) => return Ok(Some(head)),
                Ok((true, None)) => {}
                Err(error) => {
                    if !self.saving() {
                        return Err(error);
                    }
                    self.saving.error.get_or_insert(error);
                    self.saving.phase = SavePhase::Abort;
                }
            }
        }
        Ok(None)
    }

    fn save_step<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
    ) -> Result<(bool, Option<u64>), Error> {
        let capacity = store.page_capacity();
        let per = usize::try_from(store.extent_pages()).map_err(|_| corrupt())?;
        if capacity == 0 || per == 0 {
            return Err(corrupt());
        }
        match self.saving.phase {
            SavePhase::Idle => return Err(corrupt()),
            SavePhase::Views => {
                let save = &mut self.saving;
                let Some(node) = self.nodes.get(save.node) else {
                    save.bytes.clear();
                    // Format, root, fanout, leaf entries and node count.
                    let bytes = size_of::<u8>()
                        .checked_add(size_of::<[u32; 3]>())
                        .and_then(|n| n.checked_add(size_of::<u64>()))
                        .ok_or(corrupt())?;
                    if bytes > save.image_limit {
                        return Err(Error::LimitExceeded {
                            what: "a trunk image's serialized bytes",
                            limit: u64::try_from(save.image_limit).unwrap_or(u64::MAX),
                        });
                    }
                    save_reserve(&mut save.bytes, bytes)?;
                    save.bytes.push(IMAGE_FORMAT);
                    save32(&mut save.bytes, self.root)?;
                    save32(&mut save.bytes, self.config.fanout)?;
                    save.bytes
                        .extend_from_slice(&self.config.leaf_entries.to_le_bytes());
                    save32(&mut save.bytes, self.nodes.len())?;
                    save.node = 0;
                    save.phase = SavePhase::Encoding;
                    return Ok((true, None));
                };
                let Some(pivot) = node.pivots.get(save.pivot) else {
                    save.node = save.node.checked_add(1).ok_or(corrupt())?;
                    save.pivot = 0;
                    return Ok((true, None));
                };
                let Some(view) = pivot
                    .bundle
                    .view
                    .as_ref()
                    .filter(|_| pivot.bundle.stored.is_none())
                else {
                    save.pivot = save.pivot.checked_add(1).ok_or(corrupt())?;
                    return Ok((true, None));
                };
                let runs = pivot.bundle.branches().len();
                if !view
                    .roots()
                    .iter()
                    .copied()
                    .eq(pivot.bundle.branches().iter().map(|b| b.root))
                {
                    return Err(corrupt());
                }
                // View::bytes counts 6 bytes/offset and 3 bytes/segment; its encoding uses
                // 10 and 8, plus six u32 lengths and one u64 root per run.
                let extra = view
                    .segments()
                    .checked_mul(
                        runs.checked_mul(4)
                            .and_then(|n| n.checked_add(5))
                            .ok_or(corrupt())?,
                    )
                    .and_then(|n| n.checked_add(runs.checked_mul(size_of::<u64>())?))
                    .and_then(|n| n.checked_add(size_of::<[u32; 6]>()))
                    .and_then(|n| n.checked_add(size_of::<u64>()))
                    .ok_or(corrupt())?;
                let bytes = view.bytes().checked_add(extra).ok_or(corrupt())?;
                save.bytes.clear();
                save_reserve(&mut save.bytes, bytes)?;
                save.bytes.extend_from_slice(&0u64.to_le_bytes());
                view.encode(&mut save.bytes)?;
                let len = save
                    .bytes
                    .len()
                    .checked_sub(size_of::<u64>())
                    .ok_or(corrupt())?;
                save.bytes
                    .get_mut(..size_of::<u64>())
                    .ok_or(corrupt())?
                    .copy_from_slice(&u64::try_from(len).map_err(|_| corrupt())?.to_le_bytes());
                save.extent_count = save.bytes.len().div_ceil(capacity).div_ceil(per);
                u32::try_from(save.extent_count).map_err(|_| corrupt())?;
                save_reserve(&mut save.fresh, save.extent_count)?;
                save.page = 0;
                save.phase = SavePhase::ViewPages;
            }
            SavePhase::ViewPages | SavePhase::ImagePages => {
                let save = &mut self.saving;
                if save.fresh.len() < save.extent_count {
                    save.fresh.push(store.allocate_extent()?);
                    return Ok((true, None));
                }
                let image = save.phase == SavePhase::ImagePages;
                if image && save.head.is_none() {
                    let header = image_header_bytes()?;
                    let len = save
                        .fresh
                        .len()
                        .checked_mul(size_of::<u64>())
                        .and_then(|n| n.checked_add(header))
                        .ok_or(corrupt())?;
                    save.header.clear();
                    save_reserve(&mut save.header, len)?;
                    save.header.extend_from_slice(&IMAGE_MAGIC.to_le_bytes());
                    save.header.extend_from_slice(
                        &u64::try_from(save.bytes.len())
                            .map_err(|_| corrupt())?
                            .to_le_bytes(),
                    );
                    save32(&mut save.header, save.fresh.len())?;
                    for extent in &save.fresh {
                        save.header.extend_from_slice(&extent.to_le_bytes());
                    }
                    save.head = Some(save.address(store, 0)?);
                }
                let pages = save
                    .bytes
                    .len()
                    .div_ceil(capacity)
                    .checked_add(usize::from(image))
                    .ok_or(corrupt())?;
                if save.page == pages {
                    save.phase = if image {
                        SavePhase::ImageRun
                    } else {
                        SavePhase::ViewRun
                    };
                    return Ok((true, None));
                }
                let address = save.address(store, save.page)?;
                let payload = if image && save.page == 0 {
                    save.header.as_slice()
                } else {
                    let page = save.page.checked_sub(usize::from(image)).ok_or(corrupt())?;
                    let start = page.checked_mul(capacity).ok_or(corrupt())?;
                    let end = start
                        .checked_add(capacity)
                        .ok_or(corrupt())?
                        .min(save.bytes.len());
                    save.bytes.get(start..end).ok_or(corrupt())?
                };
                let run = save.run.as_mut().ok_or(corrupt())?;
                if !store.queue_page_paced(run, address, payload)? {
                    return Ok((false, None));
                }
                save.page = save.page.checked_add(1).ok_or(corrupt())?;
            }
            SavePhase::ViewRun => {
                let save = &mut self.saving;
                if !store.write_run_paced(save.run.as_mut().ok_or(corrupt())?)? {
                    return Ok((false, None));
                }
                save_reserve(&mut save.views, 1)?;
                save.views.push(SavedView {
                    node: save.node,
                    pivot: save.pivot,
                    extents: std::mem::take(&mut save.fresh),
                });
                save.pivot = save.pivot.checked_add(1).ok_or(corrupt())?;
                save.phase = SavePhase::Views;
            }
            SavePhase::Encoding => {
                if self.saving.node == self.nodes.len() {
                    let save = &mut self.saving;
                    let pages = save
                        .bytes
                        .len()
                        .div_ceil(capacity)
                        .checked_add(1)
                        .ok_or(corrupt())?;
                    save.extent_count = pages.div_ceil(per);
                    let room = capacity
                        .saturating_sub(image_header_bytes()?)
                        .checked_div(size_of::<u64>())
                        .ok_or(corrupt())?;
                    if save.extent_count > room {
                        return Err(Error::LimitExceeded {
                            what: "extents a trunk image's header names",
                            limit: u64::try_from(room).unwrap_or(u64::MAX),
                        });
                    }
                    save_reserve(&mut save.fresh, save.extent_count)?;
                    save.named.sort_unstable();
                    save.page = 0;
                    save.phase = SavePhase::ImagePages;
                    return Ok((true, None));
                }
                self.encode_saved_node()?;
                self.saving.node = self.saving.node.checked_add(1).ok_or(corrupt())?;
            }
            SavePhase::ImageRun => {
                if !store.write_run_paced(self.saving.run.as_mut().ok_or(corrupt())?)? {
                    return Ok((false, None));
                }
                if let Some(run) = self.saving.run.take() {
                    store.give_run(run);
                }
                self.saving.phase = SavePhase::Drain;
            }
            SavePhase::Drain => {
                if !store.drain_paced()? {
                    return Ok((false, None));
                }
                self.saving.phase = SavePhase::Publish;
            }
            SavePhase::Publish => {
                // Validate all staged destinations before either old references or metadata
                // changes. Structural steps are refused throughout this retained save.
                for view in &self.saving.views {
                    let pivot = self
                        .nodes
                        .get(view.node)
                        .and_then(|n| n.pivots.get(view.pivot))
                        .ok_or(corrupt())?;
                    if pivot.bundle.stored.is_some() || pivot.bundle.view.is_none() {
                        return Err(corrupt());
                    }
                }
                for &extent in &self.saved {
                    store.release(extent)?;
                }
                for &extent in &self.view_extents {
                    if self.saving.named.binary_search(&extent).is_err() {
                        store.release(extent)?;
                    }
                }
                for view in self.saving.views.drain(..) {
                    let pivot = self
                        .nodes
                        .get_mut(view.node)
                        .and_then(|n| n.pivots.get_mut(view.pivot))
                        .ok_or(corrupt())?;
                    pivot.bundle.stored = Some(view.extents);
                }
                std::mem::swap(&mut self.saved, &mut self.saving.fresh);
                std::mem::swap(&mut self.view_extents, &mut self.saving.named);
                let head = self.saving.head.take().ok_or(corrupt())?;
                self.reset_save();
                return Ok((true, Some(head)));
            }
            SavePhase::Abort => {
                if matches!(store.drain_paced(), Ok(false)) {
                    return Ok((false, None));
                }
                let error = self.saving.error.take().ok_or(corrupt())?;
                // The original save error is retained even if reference cleanup also fails.
                let _ = self.discard_save(store);
                return Err(error);
            }
        }
        Ok((true, None))
    }

    fn encode_saved_node(&mut self) -> Result<(), Error> {
        let save = &mut self.saving;
        let node = self.nodes.get(save.node).ok_or(corrupt())?;
        if !node.inflight.is_empty() {
            return Err(corrupt());
        }
        // Reserve this node's exact descriptor shape before encoders append to the scratch.
        let mut bytes = size_of::<u8>()
            .checked_add(size_of::<[u32; 2]>())
            .ok_or(corrupt())?;
        bytes = bytes
            .checked_add(node.end.as_ref().map_or(0, Vec::len))
            .ok_or(corrupt())?;
        for (i, pivot) in node.pivots.iter().enumerate() {
            let extents = save
                .views
                .binary_search_by_key(&(save.node, i), |v| (v.node, v.pivot))
                .ok()
                .and_then(|n| save.views.get(n))
                .map_or(pivot.bundle.stored.as_deref().unwrap_or(&[]), |v| {
                    v.extents.as_slice()
                });
            bytes = bytes
                .checked_add(size_of::<[u32; 4]>())
                .and_then(|n| n.checked_add(pivot.key.len()))
                .ok_or(corrupt())?;
            for branch in pivot.bundle.branches() {
                // Branch::encode has seven u64 fields, two u32 fields and its u8 height.
                let fixed = size_of::<[u64; 7]>()
                    .checked_add(size_of::<[u32; 2]>())
                    .and_then(|n| n.checked_add(size_of::<u8>()))
                    .ok_or(corrupt())?;
                bytes = bytes
                    .checked_add(fixed)
                    .and_then(|n| {
                        n.checked_add(branch.extents.len().checked_mul(size_of::<u64>())?)
                    })
                    .ok_or(corrupt())?;
            }
            bytes = bytes
                .checked_add(
                    extents
                        .len()
                        .checked_mul(size_of::<u64>())
                        .ok_or(corrupt())?,
                )
                .ok_or(corrupt())?;
        }
        let total = save.bytes.len().checked_add(bytes).ok_or(corrupt())?;
        if total > save.image_limit {
            return Err(Error::LimitExceeded {
                what: "a trunk image's serialized bytes",
                limit: u64::try_from(save.image_limit).unwrap_or(u64::MAX),
            });
        }
        save_reserve(&mut save.bytes, bytes)?;
        save.bytes.push(u8::from(node.leaf));
        match &node.end {
            Some(end) => {
                save32(&mut save.bytes, end.len())?;
                save.bytes.extend_from_slice(end);
            }
            None => save.bytes.extend_from_slice(&ABSENT.to_le_bytes()),
        }
        save32(&mut save.bytes, node.pivots.len())?;
        for (i, pivot) in node.pivots.iter().enumerate() {
            save32(&mut save.bytes, pivot.key.len())?;
            save.bytes.extend_from_slice(&pivot.key);
            match pivot.child {
                Some(child) => save32(&mut save.bytes, child)?,
                None => save.bytes.extend_from_slice(&ABSENT.to_le_bytes()),
            }
            save32(&mut save.bytes, pivot.bundle.branches().len())?;
            for branch in pivot.bundle.branches() {
                branch.encode(&mut save.bytes)?;
            }
            let extents = save
                .views
                .binary_search_by_key(&(save.node, i), |v| (v.node, v.pivot))
                .ok()
                .and_then(|n| save.views.get(n))
                .map_or(pivot.bundle.stored.as_deref().unwrap_or(&[]), |v| {
                    v.extents.as_slice()
                });
            save_reserve(&mut save.named, extents.len())?;
            save.named.extend_from_slice(extents);
            save32(&mut save.bytes, extents.len())?;
            for extent in extents {
                save.bytes.extend_from_slice(&extent.to_le_bytes());
            }
        }
        Ok(())
    }

    fn reset_save(&mut self) {
        self.saving.phase = SavePhase::Idle;
        self.saving.bytes.clear();
        self.saving.header.clear();
        self.saving.fresh.clear();
        self.saving.views.clear();
        self.saving.named.clear();
        self.saving.head = None;
        self.saving.error = None;
    }

    fn discard_save<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        if let Some(run) = self.saving.run.take() {
            store.give_run(run);
        }
        let mut first = None;
        for extent in self.saving.fresh.drain(..).chain(
            self.saving
                .views
                .iter_mut()
                .flat_map(|v| v.extents.drain(..)),
        ) {
            if let Err(error) = store.release(extent) {
                first.get_or_insert(error);
            }
        }
        self.reset_save();
        first.map_or(Ok(()), Err)
    }

    /// Cancels an unfinished image only after its physical owner writes retired. No active
    /// memtable is flushed, and neither old image nor old view metadata is published/released.
    pub(crate) fn abort_save<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<(), Error> {
        if !self.saving() {
            return Ok(());
        }
        let mut first = self.saving.error.clone();
        match store.drain_paced() {
            Ok(false) => {
                return Err(Error::InvalidArgument {
                    what: "a paced save aborted before owner write retirement",
                });
            }
            Err(error) => {
                first.get_or_insert(error);
            }
            Ok(true) => {}
        }
        if let Err(error) = self.discard_save(store) {
            first.get_or_insert(error);
        }
        first.map_or(Ok(()), Err)
    }

    /// The trunk a checkpoint's image at `head` holds, its branches' filters read back.
    pub fn load<F: BlockFile>(store: &mut Store<F>, head: u64) -> Result<Self, Error> {
        let mut view_extents: Vec<u64> = Vec::new();
        let mut header = Vec::new();
        store.read_page(head, &mut header)?;
        let u64_at = |b: &[u8], at: usize| {
            b.get(at..)
                .and_then(<[u8]>::first_chunk::<8>)
                .map(|x| u64::from_le_bytes(*x))
                .ok_or(corrupt())
        };
        let u32_at = |b: &[u8], at: usize| {
            b.get(at..)
                .and_then(<[u8]>::first_chunk::<4>)
                .map(|x| u32::from_le_bytes(*x))
                .ok_or(corrupt())
        };
        if u64_at(&header, 0)? != IMAGE_MAGIC {
            return Err(Error::Corruption {
                what: "a trunk image",
                why: Malformed::BadMagic,
            });
        }
        let len = usize::try_from(u64_at(&header, 8)?).map_err(|_| corrupt())?;
        let extents = usize::try_from(u32_at(&header, 16)?).map_err(|_| corrupt())?;
        let mut saved = Vec::with_capacity(extents.min(header.len() / 8));
        for i in 0..extents {
            saved.push(u64_at(
                &header,
                20usize.saturating_add(i.saturating_mul(8)),
            )?);
        }
        let per = usize::try_from(store.extent_pages()).map_err(|_| corrupt())?;
        let capacity = store.page_capacity();
        let mut blob = Vec::with_capacity(len);
        let pages = len.div_ceil(capacity);
        for i in 1..=pages {
            let extent = *saved
                .get(i.checked_div(per).ok_or(corrupt())?)
                .ok_or(corrupt())?;
            let address = store.address(
                extent,
                u32::try_from(i.checked_rem(per).ok_or(corrupt())?).map_err(|_| corrupt())?,
            )?;
            store.read_page(address, &mut blob)?;
        }
        if blob.len() != len {
            return Err(corrupt());
        }
        let mut r = Reader {
            bytes: &blob,
            at: 0,
        };
        let format = *r.take(1)?.first().ok_or(corrupt())?;
        if format != IMAGE_FORMAT {
            return Err(Error::Corruption {
                what: "a trunk image",
                why: Malformed::UnknownVersion(u64::from(format)),
            });
        }
        let u32_of = |s: &[u8]| {
            s.first_chunk::<4>()
                .map(|b| u32::from_le_bytes(*b))
                .ok_or(corrupt())
        };
        let usize_of = |s: &[u8]| -> Result<usize, Error> {
            usize::try_from(u32_of(s)?).map_err(|_| corrupt())
        };
        let root = usize_of(r.take(4)?)?;
        let fanout = usize_of(r.take(4)?)?;
        let leaf_entries = r
            .take(8)?
            .first_chunk::<8>()
            .map(|b| u64::from_le_bytes(*b))
            .ok_or(corrupt())?;
        let count = usize_of(r.take(4)?)?;
        let mut nodes = Vec::with_capacity(count.min(len));
        for _ in 0..count {
            let leaf = *r.take(1)?.first().ok_or(corrupt())? == 1;
            let end_len = u32_of(r.take(4)?)?;
            let end = if end_len == ABSENT {
                None
            } else {
                Some(
                    r.take(usize::try_from(end_len).map_err(|_| corrupt())?)?
                        .to_vec(),
                )
            };
            let pivot_count = usize_of(r.take(4)?)?;
            let mut pivots = Vec::with_capacity(pivot_count.min(len));
            for _ in 0..pivot_count {
                let key_len = usize_of(r.take(4)?)?;
                let key = r.take(key_len)?.to_vec();
                let child = u32_of(r.take(4)?)?;
                let child = if child == ABSENT {
                    None
                } else {
                    Some(usize::try_from(child).map_err(|_| corrupt())?)
                };
                let bundle_len = usize_of(r.take(4)?)?;
                let mut bundle = Vec::with_capacity(bundle_len.min(len));
                for _ in 0..bundle_len {
                    let rest = r.bytes.get(r.at..).ok_or(corrupt())?;
                    let (branch, used) = Branch::decode(store, rest)?;
                    r.at = r.at.checked_add(used).ok_or(corrupt())?;
                    bundle.push(branch);
                }
                let view_len = usize_of(r.take(4)?)?;
                let mut extents = Vec::with_capacity(view_len.min(len));
                for _ in 0..view_len {
                    let b = r.take(8)?.first_chunk::<8>().copied().ok_or(corrupt())?;
                    extents.push(u64::from_le_bytes(b));
                }
                let mut bundle = Bundle::from(bundle);
                if !extents.is_empty() {
                    let bytes = read_blob(store, &extents)?;
                    let (view, _) = View::decode(&bytes)?;
                    // A view names the branches it describes: those of its bundle.
                    if view.roots().len() != bundle.branches().len()
                        || view
                            .roots()
                            .iter()
                            .zip(bundle.branches())
                            .any(|(&r, b)| r != b.root)
                    {
                        return Err(corrupt());
                    }
                    view_extents.extend_from_slice(&extents);
                    bundle.view = Some(view);
                    bundle.stored = Some(extents);
                }
                pivots.push(Pivot {
                    key,
                    child,
                    bundle,
                    start: 0,
                    seek: SeekRent::default(),
                });
            }
            nodes.push(Node {
                leaf,
                pivots,
                inflight: Vec::new(),
                end,
                wants: None,
                queued: false,
            });
        }
        if root >= nodes.len() {
            return Err(corrupt());
        }
        let unchecked = nodes.len();
        view_extents.sort_unstable();
        Ok(Self {
            nodes,
            root,
            config: TrunkConfig {
                fanout,
                leaf_entries,
            },
            saved,
            saving: Saving::default(),
            stats: TrunkStats::default(),
            pending: Vec::new(),
            tasks: Vec::new(),
            looking: VecDeque::new(),
            abandoning: None,
            timed: false,
            yield_io: false,
            io_waiting: false,
            view_job: None,
            view_costs: ViewCosts::default(),
            view_choice: ViewChoice::Measured,
            view_extents,
            // Views are not saved: every node is checked for a bundle wanting one after a load.
            views_unchecked: unchecked,
            view_at: 0,
            // Maplets are not saved either: rebuilt from the branches' hash lists after a load.
            maplet_job: None,
            maplets_unchecked: unchecked,
            maplet_at: 0,
            routes: Tally::default(),
            gets: 0,
            merge_ns: 0,
            merge_keys: 0,
            paid: 0,
            full: false,
            consolidated: false,
            consolidate: false,
            consolidating: false,
            consolidation: Consolidation::Measured,
            marks: Vec::new(),
            pool: None,
            wait: Wait::No,
            workers_waiting: false,
        })
    }

    /// Pivot `i`'s compaction is done: its branch goes to the front of the pivot's bundle, and
    /// the pivot reads in-flight bundles from `covered` on.
    fn apply_pivot(
        &mut self,
        n: usize,
        i: usize,
        merged: Option<Branch>,
        covered: usize,
    ) -> Result<(), Error> {
        self.stats.pivot_compactions = self.stats.pivot_compactions.saturating_add(1);
        if let Some(b) = &merged {
            self.stats.entries_written = self.stats.entries_written.saturating_add(b.count);
        }
        let pivot = self.node_mut(n)?.pivots.get_mut(i).ok_or(corrupt())?;
        pivot.start = covered;
        if let Some(b) = merged {
            pivot.bundle.push_newest(b);
        }
        Ok(())
    }

    /// In-flight bundles before every pivot's start are read by none: their references go, and
    /// every index into the list moves down with them, the tasks' planned ranges too. A task's
    /// range starts at or past its pivot's start, so none reads a bundle that goes.
    fn drop_dead<F: BlockFile>(&mut self, store: &mut Store<F>, n: usize) -> Result<(), Error> {
        let node = self.node_mut(n)?;
        let dead = node.pivots.iter().map(|p| p.start).min().unwrap_or(0);
        if dead == 0 {
            return Ok(());
        }
        let gone: Vec<Vec<Branch>> = node.inflight.drain(..dead).collect();
        for p in &mut node.pivots {
            p.start = p.start.saturating_sub(dead);
        }
        self.shift_tasks(n, dead);
        for bundle in gone {
            for b in &bundle {
                release(store, b)?;
            }
        }
        Ok(())
    }

    /// A leaf's compaction is done: `parts` replace the branches whose `extents` it releases,
    /// each part beyond the first a leaf of its own. Returns the leaves now covering the range, each with its first key.
    fn apply_settle<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        n: usize,
        parts: Vec<(Vec<u8>, Branch)>,
        extents: &[u64],
    ) -> Result<Vec<(Vec<u8>, usize)>, Error> {
        let node = self.node(n)?;
        let pivot = node.pivots.first().ok_or(corrupt())?;
        let (from, end) = (pivot.key.clone(), node.end.clone());
        self.stats.leaf_compactions = self.stats.leaf_compactions.saturating_add(1);
        self.stats.splits = self
            .stats
            .splits
            .saturating_add(u64::try_from(parts.len().saturating_sub(1)).unwrap_or(0));
        for (_, b) in &parts {
            self.stats.entries_written = self.stats.entries_written.saturating_add(b.count);
        }
        for &e in extents {
            store.release(e)?;
        }
        // Every compacted part's references are the leaf's that takes it.
        if parts.is_empty() {
            self.node_mut(n)?
                .pivots
                .first_mut()
                .ok_or(corrupt())?
                .bundle
                .branches_mut()
                .clear();
            return Ok(vec![(from, n)]);
        }
        let mut out = Vec::with_capacity(parts.len());
        let count = parts.len();
        for (j, (first, branch)) in parts.into_iter().enumerate() {
            let key = if j == 0 { from.clone() } else { first };
            if j == 0 {
                let node = self.node_mut(n)?;
                let p = node.pivots.first_mut().ok_or(corrupt())?;
                p.bundle = Bundle::from(vec![branch]);
                out.push((key, n));
            } else {
                self.nodes.push(Node {
                    leaf: true,
                    pivots: vec![Pivot {
                        key: key.clone(),
                        child: None,
                        bundle: Bundle::from(vec![branch]),
                        start: 0,
                        seek: SeekRent::default(),
                    }],
                    inflight: Vec::new(),
                    end: None,
                    wants: None,
                    queued: false,
                });
                out.push((key, self.nodes.len().saturating_sub(1)));
            }
        }
        // Each part ends where the next begins; the last where the leaf did.
        for j in 0..count {
            let end_key = out
                .get(j.saturating_add(1))
                .map(|(k, _)| k.clone())
                .or_else(|| end.clone());
            let (_, idx) = out.get(j).ok_or(corrupt())?;
            let idx = *idx;
            self.node_mut(idx)?.end = end_key;
        }
        Ok(out)
    }

    /// An index node over `fanout` pivots splits into nodes of at most `fanout` pivots. Its
    /// in-flight bundles are empty here: [`Self::compact_pivots`] ran first and left every pivot
    /// past them, so a split only partitions the pivots.
    fn split_node(&mut self, n: usize) -> Result<Vec<(Vec<u8>, usize)>, Error> {
        let fanout = self.config.fanout;
        let node = self.node_mut(n)?;
        let first = node
            .pivots
            .first()
            .map(|p| p.key.clone())
            .unwrap_or_default();
        let len = node.pivots.len();
        if len <= fanout {
            return Ok(vec![(first, n)]);
        }
        if !node.inflight.is_empty() {
            return Err(corrupt());
        }
        let parts = len.div_ceil(fanout);
        let size = len.div_ceil(parts);
        let end = node.end.clone();
        let mut rest = node.pivots.split_off(size);
        let mut out = vec![(first, n)];
        while !rest.is_empty() {
            let tail = rest.split_off(size.min(rest.len()));
            let key = rest.first().map(|p| p.key.clone()).unwrap_or_default();
            self.nodes.push(Node {
                leaf: false,
                pivots: rest,
                inflight: Vec::new(),
                end: None,
                wants: None,
                queued: false,
            });
            out.push((key, self.nodes.len().saturating_sub(1)));
            rest = tail;
        }
        for j in 0..out.len() {
            let end_key = out
                .get(j.saturating_add(1))
                .map(|(k, _)| k.clone())
                .or_else(|| end.clone());
            let idx = out.get(j).ok_or(corrupt())?.1;
            self.node_mut(idx)?.end = end_key;
        }
        self.stats.splits = self
            .stats
            .splits
            .saturating_add(u64::try_from(out.len().saturating_sub(1)).unwrap_or(0));
        Ok(out)
    }

    /// The pivot bundles that have a view now.
    pub fn views(&self) -> usize {
        self.nodes
            .iter()
            .flat_map(|n| n.pivots.iter())
            .filter(|p| p.bundle.view().is_some())
            .count()
    }

    /// The extents the last saved image's views are written in.
    pub fn view_extents(&self) -> &[u64] {
        &self.view_extents
    }

    /// The bytes its branches and views hold in memory.
    pub fn memory(&self) -> Memory {
        let mut m = Memory::default();
        let mut add = |b: &Branch| {
            m.filters = m.filters.saturating_add(b.filter.bytes());
            m.indexes = m.indexes.saturating_add(b.index.bytes());
            m.counts = m.counts.saturating_add(b.counts.len().saturating_mul(2));
            m.ranges = m.ranges.saturating_add(b.range.bytes());
        };
        self.pending.iter().for_each(&mut add);
        for n in &self.nodes {
            n.inflight.iter().flatten().for_each(&mut add);
            for p in &n.pivots {
                p.bundle.branches().iter().for_each(&mut add);
                if let Some(v) = p.bundle.view() {
                    m.views = m.views.saturating_add(v.bytes());
                }
            }
        }
        m
    }

    /// The trunk's shape: its height (1 for a lone leaf), its nodes, and its leaves.
    pub fn shape(&self) -> Result<(usize, usize, usize), Error> {
        let mut height = 1usize;
        let mut at = self.root;
        while let Some(child) = self.node(at)?.pivots.first().and_then(|p| p.child) {
            height = height.saturating_add(1);
            at = child;
            if height > self.nodes.len() {
                return Err(corrupt());
            }
        }
        let leaves = self.nodes.iter().filter(|n| n.leaf).count();
        Ok((height, self.nodes.len(), leaves))
    }

    /// What the trunk's maintenance has done.
    pub fn stats(&self) -> TrunkStats {
        self.stats
    }

    /// The extents of the trunk's saved image.
    pub fn image_extents(&self) -> &[u64] {
        &self.saved
    }

    /// The trunk's branches, for checks: every branch a node names, each naming once.
    pub fn branches(&self) -> Vec<Branch> {
        let mut out = self.pending.clone();
        for node in &self.nodes {
            for bundle in &node.inflight {
                out.extend(bundle.iter().cloned());
            }
            for p in &node.pivots {
                out.extend(p.bundle.branches().iter().cloned());
            }
        }
        out
    }
}

/// A position in an image's bytes.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    /// The next `n` bytes.
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self.at.checked_add(n).ok_or(corrupt())?;
        let s = self.bytes.get(self.at..end).ok_or(corrupt())?;
        self.at = end;
        Ok(s)
    }
}

#[cfg(test)]
mod optional_jobs_native;

#[cfg(test)]
mod view_cost_tests {
    use super::ViewCosts;

    #[test]
    fn the_cheaper_job_follows_the_measured_rates() {
        // Unmeasured, a unit of either costs the same: fewer units wins.
        let c = ViewCosts::default();
        assert!(c.rebuild_cheaper(99, 100));
        assert!(!c.rebuild_cheaper(100, 100));
        // A probe measured at three times an entry: 30 probes cost more than 80 entries, 26
        // fewer.
        let c = ViewCosts {
            build_ns: 1_000,
            build_units: 1_000,
            rebuild_ns: 3_000,
            rebuild_units: 1_000,
        };
        assert!(!c.rebuild_cheaper(30, 80));
        assert!(c.rebuild_cheaper(26, 80));
    }
}
