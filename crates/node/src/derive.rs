//! What `mantle serve` runs with that its operator does not state (docs/design/node.md §1.4),
//! derived from what it states, what it measures of its host and devices, and the code's own
//! bounds, each class of work at its floor: a share past a class's floor waits for its knee,
//! measured at the scale step (node.md §9.3). Each value carries its basis, which the node says
//! at start.

use std::mem::size_of;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use hyper_block::buf::Alignment;
use hyper_log::{Config as LogConfig, Facts, LogError, Unfit};
use hyper_raft::progress::Progress;
use hyper_raft::proto::{Entry, Message};
use hyper_raft::read::PendingRead;
use hyper_rt::machine::bench::MIN_SAMPLES;
use hyper_rt::machine::facts::{MemoryFacts, effective_capacity};
use mantle_chunk::layout::{self as volume_layout, Geometry};
use mantle_chunk::{ChunkError, Config as VolumeConfig, Limits, Reads, index};
use mantle_disk::profile::Profile;
use mantle_ec::durability::{self, Burst, Choice, DurabilityError, Rates, Scheme};
use mantle_engine::store::{Config as StoreConfig, Shape};
use mantle_engine::trunk::TrunkConfig;
use mantle_gateway::layout::Layout;
use mantle_gateway::{get, put};
use mantle_meta::apply::Layer;
use mantle_meta::record::{RecordError, Session};
use mantle_meta::session::Rules;
use mantle_meta::{key, wire};
use mantle_range::{ConfState, Range, Settings};

use crate::ask::ENTRIES_OUT;
use crate::node::Hosted;
use crate::process::{
    Enlisting, GatewayConfig, NativeShards, PeerLine, ProcessConfig, RangeShards, S3Stated,
};
use crate::storage::VolumeSettings;
use mantle_protocol::bounds::CONNECTIONS_PER_PEER;
use mantle_protocol::classes::Role;

/// focal's election timeout and heartbeat interval in ticks (docs/design/node.md §2.4), which
/// ranges keep though they elect by suspicion, which takes no ticks.
const ELECTION_TICK: usize = 10;
const HEARTBEAT_TICK: usize = 2;

/// Pivots a trunk node holds before it splits: SplinterDB's 8, at which the engine's
/// measurements ran (docs/design/engine-structure.md §5–§6), until the fanout is measured
/// against mantle's key counts as its §4 asks.
const FANOUT: usize = 8;

/// Pages of an engine's extent: the least the store takes, the superblocks' two in extent 0.
const EXTENT_PAGES: u32 = 2;

/// A block's annual chance of loss at most, unless the operator states one: S3's eleven nines,
/// applied to each block (docs/design/durability.md §1).
const TARGET: f64 = 1e-11;

/// Hours to repair a lost chunk unless the operator states the cell's repair rate: "a day—or more"
/// after a power-on restart (Chansler 2012), the longest the sources give (docs/research/15 §8.4).
/// The cell's repair holds itself to it: a chunk waits for an away node until a day less the
/// rebuild's measured time has passed (docs/design/repair.md §2).
const REPAIR_HOURS: f64 = 24.0;

/// Buckets an owner may have: S3's 10,000 (docs/research/05 §10.4).
const BUCKET_QUOTA: u32 = 10_000;

/// How long a block or file is held for the write above it: HDFS's 60-second soft limit
/// (docs/research/22 §3.6), one of the bounds metadata.md §2 names for the same window, until
/// the gateway measures research 22 §10.2's `D_b`; it bounds how long a leftover waits, not
/// correctness.
const HANDOVER: Duration = Duration::from_secs(60);

/// The longest a runnable thread waits behind one other: Linux's base slice of 0.75 ms scaled by
/// one and the log of up to 8 CPUs (`sysctl_sched_base_slice`, kernel/sched/fair.c v6.12), 3 ms
/// at most (docs/research/64).
#[cfg(target_os = "linux")]
const QUANTUM: Duration = Duration::from_millis(3);
/// macOS's: XNU's default preemption rate of 100 a second (osfmk/kern/sched_prim.c), 10 ms.
#[cfg(target_os = "macos")]
const QUANTUM: Duration = Duration::from_millis(10);
/// Windows': a multiple of its clock interval, 15.625 ms, a foreground thread's six of them,
/// 93.75 ms, as Microsoft measured them; a server's quantum Microsoft gives only as longer, and
/// on a starved server the probe keeps fewer wakes and says so (docs/research/64).
#[cfg(target_os = "windows")]
const QUANTUM: Duration = Duration::from_micros(93_750);
/// Elsewhere, the longest of those.
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
const QUANTUM: Duration = Duration::from_micros(93_750);

/// The largest checksum block a volume's records take, as a shift: 16 KiB, the length up to which
/// CRC-32C's guaranteed Hamming distances are verified (docs/research/03 §15.8 C2, research 11
/// §8.2).
const CHECKSUM_SHIFT_MOST: u8 = 14;

/// The stack a thread the node starts takes: Rust's default, 2 MiB on every tier-1 platform
/// (std `thread` module, "Stack size"), which no thread of mantle's or its crates' sets.
const STACK: u64 = 2 << 20;

/// How long one scrub of a whole volume takes: research 03 §15.9's target of seven days, of the
/// fourteen it allows at most.
const SCRUB: Duration = Duration::from_secs(7 * 24 * 3600);

/// Measured: the most of its store a range's engine took for each object: the Block range's, a
/// block's header and a chunk row and a reverse row for each of its three copies. The transfer
/// harness's small sets on an Apple M5 Max (binary 4858a0d7, 2026-10-31), each object a block
/// through one of the two doors, every node's store files at the case's end (a store grows its file
/// an extent past its end when none is free, so a file's length is the most it held at once,
/// compaction's room included): 14,696,448 bytes at 10,000 objects and 51,511,296 at 30,000, past
/// the empty store's 73,728 1,462 and 1,715 bytes an object, and 1,841 an object between them, the
/// most of the three (measurements/2026-10-28-transfer-scale.md). Measured to 30,000 objects: the
/// slope grew with them, and a range at millions waits on a measurement there.
pub const STORE_BYTES_AN_OBJECT: u64 = 1_841;

/// The rates a block's scheme is chosen by until mantle measures its own (docs/design/durability.md
/// §5): each chunk lost at the worst device class's annual rate, since a block's chunks may land
/// on any device of the cell; a power-on restart a year, destroying a hundredth of the nodes;
/// and repair in a day. No permanent loss of a rack, zone or site has a rate the sources give.
pub fn rates() -> Rates {
    rates_repaired(None)
}

/// [`rates`] with the cell's repair rate, per hour, where its operator states one.
pub fn rates_repaired(repair: Option<f64>) -> Rates {
    use durability::field::{DISK_FAILURES, POWER_LOSS_FRACTION, POWER_LOSSES};
    Rates {
        chunk: DISK_FAILURES / durability::YEAR,
        domains: None,
        bursts: vec![Burst {
            rate: POWER_LOSSES / durability::YEAR,
            fraction: POWER_LOSS_FRACTION,
        }],
        repair: repair.unwrap_or_else(|| REPAIR_HOURS.recip()),
    }
}

/// A block's annual chance of loss at most: what the operator states, or S3's eleven nines.
pub fn target(stated: Option<f64>) -> f64 {
    stated.unwrap_or(TARGET)
}

/// The orderly stop's bound where its operator states none (node.md §1.7): docker stop's default
/// grace period for a Linux container, 10 s, the shortest of the orchestrators' (Kubernetes 30 s,
/// systemd 90 s; research 64), so a stop ends before their SIGKILL comes.
pub const DEFAULT_STOP: Duration = Duration::from_secs(10);

/// Each probe of the machine's calibration runs within this: the samples its probes stop at,
/// Wilks' 59 (hyper-rt `machine::bench::MIN_SAMPLES`), each waiting at most the longest the
/// platform's scheduler keeps a runnable thread behind another. The budget binds only a starved
/// host, whose probe reports the wakes it measured (hyper-rt `machine::wake`).
pub fn probe_budget() -> Result<Duration, DeriveError> {
    u32::try_from(MIN_SAMPLES)
        .ok()
        .and_then(|samples| QUANTUM.checked_mul(samples))
        .ok_or(DeriveError::Overflow("the probe budget"))
}

/// What a node that hosts objects runs its drivers with: a window of one block, the floor; its
/// operations' bytes what its memory holds past every class's floor and the indexes' and engines'
/// wants ([`derive`], research/87 §4 rule D), and at most as many operations as those bytes hold at
/// what one costs, as many as it learns its knee is within them;
/// as many clients of the native protocol admitted at once, each with an operation it may run; S3's
/// quota of buckets; the rows each step of a bucket's attempt passes or removes that the largest
/// entry's bytes hold ([`scan_rows`]); HDFS's handover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gateway {
    pub window: usize,
    pub objects: usize,
    pub objects_bytes: u64,
    pub clients: usize,
    pub bucket_quota: u32,
    pub scan_rows: u32,
    pub handover_ns: u64,
}

/// How many operations `share` bytes hold, and the bytes left for what they hold at their own
/// sizes, where `n` operations cost their charges and `beside(n)` bytes of the node's other classes
/// (their exchanges, their clients' connections and trains, their reads' state), counted at `n`:
/// the bytes for what the node serves first ([`served`]), and as many operations more at the least
/// one holds as the share then holds; never fewer than the operations served where the share holds
/// them at the least, the bytes past them their parts', and at least one, its bytes at least the
/// most one operation is charged ([`most_charged`]), where the share holds less: every operation is
/// admitted alone. Never more than `most`, the operations the process's descriptors hold
/// ([`descriptor_operations`]), at least one, which [`derive`] refuses to start without.
/// `beside` grows with the operations and never shrinks; it may grow by less with each past a
/// bound the count reaches, as a receive's batch stops at `MAX_BATCH` trains (`bounds::io`), and
/// the operations past it hold only what they hold.
pub fn operations(
    share: u64,
    beside: &dyn Fn(u64) -> Option<u64>,
    charges: &Charges,
    serving: Served,
    most: u64,
) -> (usize, u64) {
    let floor = most_charged(charges);
    let held = serving.bytes.max(floor);
    let least = charges.least();
    let by_least = most_within(share, share.checked_div(least.max(1)).unwrap_or(0), &|n| {
        n.checked_mul(least)?.checked_add(beside(n)?)
    });
    let past = |bytes: u64| {
        share
            .checked_sub(bytes)
            .map_or(0, |rest| most_within(rest, by_least, beside))
    };
    let (by_held, by_floor) = (past(held), past(floor));
    let count = by_held
        .max(serving.operations.min(by_floor))
        .max(1)
        .min(most.max(1));
    let bytes = share
        .saturating_sub(beside(count).unwrap_or(u64::MAX))
        .max(floor);
    (usize::try_from(count).unwrap_or(usize::MAX), bytes)
}

/// The most operations, at most `upper`, whose `cost` is within `bytes`, none where one's is past
/// it; a cost of `None` is past any bytes. `cost` never shrinks as the operations grow, so the
/// count lies from `low`, whose cost fits, to `high`, past which every cost is past the bytes, and
/// halving that span finds it: a u64's span halves to nothing within 64 steps, the loop's bound.
fn most_within(bytes: u64, upper: u64, cost: &dyn Fn(u64) -> Option<u64>) -> u64 {
    let (mut low, mut high) = (0u64, upper);
    for _ in 0..=u64::BITS {
        let Some(span) = high.checked_sub(low).filter(|span| *span > 0) else {
            break;
        };
        // The upper middle, past `low`, so the span shrinks whichever side it falls.
        let middle = low.saturating_add(span.div_ceil(2));
        if cost(middle).is_some_and(|cost| cost <= bytes) {
            low = middle;
        } else {
            high = middle.saturating_sub(1);
        }
    }
    low
}

/// The drivers a copy runs at once once taken, its get of the source and its put of the copy
/// (research/89 §3.7): a copy is charged both at once ([`Charges::copy`], `host::charged`), each
/// at most the most an operation holds.
pub(crate) const COPY_WEIGHT: usize = 2;

/// The most bytes one operation is charged as it is admitted: a copy's, its get's and its put's
/// at once at the floor's window, each at most the most a get or a put holds ([`COPY_WEIGHT`];
/// [`Charges::copy`] of the largest body a copy writes, `host::charged`). The operations' bytes
/// are never less, so every operation is admitted alone, and the door, which takes its requests
/// in the order they came, never holds one that can never be taken. A put, a get or a copy holds
/// a window past the floor's only where those bytes hold it beside the others' (`host::fitting`).
pub fn most_charged(charges: &Charges) -> u64 {
    u64::try_from(COPY_WEIGHT)
        .ok()
        .and_then(|weight| charges.most().checked_mul(weight))
        .unwrap_or(u64::MAX)
}

/// The most one native PUT or GET holds above its node's idle at the floor's window, the bound a
/// body crossing the client, the host and the transport is held to (node.md §1.4): its charge,
/// the most an operation holds ([`Charges::most`]), which is its driver's window of blocks
/// (`gateway.window`) at the largest block (`put::most_bytes`, `get::most_bytes`), a put's chunks
/// copied to send for each block going down and one piece of its body carried, the transport's
/// receive chunk (`hyper_transport::RECEIVE_CHUNK`), and a get's block taken to be written out;
/// `beside`, what one more operation adds past its charge to the transport, the ranges, the
/// listener and the runtime, [`derive`]'s difference of the node at two operations and at one:
/// its client's connection and exchange at QUIC's windows, a range's read, its task; and the
/// host's records of it, its job and its boxed put or get (`node::record_bytes`). A put or a get
/// admitted at a window it learned holds that window's blocks instead ([`Charges::write`],
/// [`Charges::read`]), at most the operations' bytes. A put's charge holds its record of each
/// block it wrote until its file names them, which grows with the body by design, at the largest
/// body a PUT carries (`put::most_records`), or its own (`put::records_for`).
pub fn native(charges: &Charges, beside: u64) -> Option<u64> {
    let records = u64::try_from(crate::node::record_bytes()?).ok()?;
    charges.most().checked_add(beside)?.checked_add(records)
}

/// What one stock client's part holds going down (`listener::STOCK_PART`), charged as the host
/// charges a write of it (`host::sized`): every block of it at once, the most any window the node
/// learns gives a body of that length (`crate::window::Window::blocks`).
pub fn stock_part(charges: &Charges) -> u64 {
    let part = crate::listener::STOCK_PART;
    let blocks = part.div_ceil(charges.largest_block().max(1)).max(1);
    charges.write(usize::try_from(blocks).unwrap_or(usize::MAX), part)
}

/// What a node's operations serve at least: their count and the bytes they hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Served {
    pub operations: u64,
    pub bytes: u64,
}

/// What a node's operations serve at least ([`Served`]): one operation at the most one is charged
/// ([`most_charged`]); where it serves S3, as many as one stock client asks of its endpoint at
/// once (`listener::STOCK_CLIENT`), each a part of its own at the most a part is charged
/// ([`stock_part`]), so a default AWS CLI's or boto3's connections are each served and their parts
/// admitted at once, none waiting at the door past its client's patience.
pub fn served(charges: &Charges, s3: bool) -> Served {
    let floor = most_charged(charges);
    if !s3 {
        return Served {
            operations: 1,
            bytes: floor,
        };
    }
    let operations = crate::listener::STOCK_CLIENT;
    Served {
        operations,
        // A product past a u64 is past any memory: the floor that holds it refuses every
        // allowance.
        bytes: operations.saturating_mul(stock_part(charges)).max(floor),
    }
}

/// The gateway of a node that runs `objects` operations at once at most, their bytes at their own
/// sizes `objects_bytes` ([`operations`]): a window of one block going down or being read, the
/// floor (node.md §1.4); as many native clients as operations, each with one it may run.
pub fn gateway(objects: usize, objects_bytes: u64) -> Result<Gateway, DeriveError> {
    Ok(Gateway {
        window: 1,
        objects: 0,
        objects_bytes: 0,
        clients: 0,
        bucket_quota: BUCKET_QUOTA,
        scan_rows: scan_rows()?,
        handover_ns: u64::try_from(HANDOVER.as_nanos())
            .map_err(|_| DeriveError::Overflow("the handover"))?,
    }
    .running(objects, objects_bytes))
}

impl Gateway {
    /// This gateway running `objects` operations at once at most, their bytes `objects_bytes`,
    /// as [`gateway`] has it, without reckoning its scan's rows again: [`derive`] counts the
    /// node at many counts of operations, and the rows encode the largest entry to measure it.
    fn running(self, objects: usize, objects_bytes: u64) -> Self {
        Self {
            objects,
            objects_bytes,
            clients: objects,
            ..self
        }
    }
}

/// The rows one step of a bucket's attempt passes or removes (`coordinator::Bounds`): as many as the
/// largest entry a range takes (`wire::largest_entry_bytes`, `range.max_entry_bytes`) holds of what
/// one row a collection removes writes (`wire::largest_collected_bytes`), so a collection's writes
/// are within the bytes of the largest entry a range applies, and a probe passes as many a read. At
/// least one, the least that moves an attempt.
pub fn scan_rows() -> Result<u32, DeriveError> {
    let entry = wire::largest_entry_bytes()?;
    let row = wire::largest_collected_bytes()?;
    let rows = entry.checked_div(row).unwrap_or(0).max(1);
    u32::try_from(rows).map_err(|_| DeriveError::Overflow("a scan's rows"))
}

/// The schemes a node's blocks may take, its operations' charges are reckoned under: those
/// [`schemes`] gives at the most failure domains its cell's volume table names (`enlist`'s
/// `max_nodes`), or the widest scheme's width where it names none.
pub fn charges(
    window: usize,
    enlist: Option<&Enlisting>,
    rates: &Rates,
    target: f64,
) -> Result<Charges, DeriveError> {
    let widest = durability::candidates()
        .iter()
        .map(|s| s.width())
        .max()
        .unwrap_or(0);
    let domains = enlist.map_or(widest, |e| {
        usize::try_from(e.max_nodes).unwrap_or(usize::MAX)
    });
    Charges::new(window, &schemes(domains, rates, target)?)
}

/// The bytes an object operation holds while it runs, at its own size, under each scheme a block
/// may take ([`charges`]), each the driver's own (`put::bytes_for`, `put::most_bytes`,
/// `get::most_bytes`) and the host's: a put's chunks copied to send, every chunk of every block
/// going down, a piece of its body carried, and its records of the blocks it wrote until its file
/// names them (`put::records_for`); a get's block taken and written out. A write is
/// charged at its body's length, known from its head ([`Charges::write`]); a get for its window
/// ([`Charges::read`]); a copy for its get's and its put's at once ([`Charges::copy`]); every
/// other operation at the most any holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Charges {
    window: usize,
    layouts: Vec<Layout>,
    piece: u64,
    most: u64,
    least: u64,
}

impl Charges {
    /// The charges of operations holding `window` blocks going down or being read at once, their
    /// blocks under `schemes`.
    pub fn new(window: usize, schemes: &[Scheme]) -> Result<Self, DeriveError> {
        let overflow = || DeriveError::Overflow("an operation's bytes");
        let piece = u64::try_from(hyper_transport::RECEIVE_CHUNK).map_err(|_| overflow())?;
        let window_n = u64::try_from(window).map_err(|_| overflow())?;
        let mut layouts = Vec::new();
        let mut most = 0u64;
        for &scheme in schemes {
            let layout = Layout::new(scheme).map_err(|_| overflow())?;
            let block = layout.largest_block().ok_or_else(overflow)?;
            let put = put::most_bytes(window, &layout)
                .and_then(|bytes| bytes.checked_add(copies(&layout, block, window_n)?))
                .and_then(|bytes| bytes.checked_add(piece))
                .and_then(|bytes| bytes.checked_add(put::most_records(&layout)?))
                .ok_or_else(overflow)?;
            let get = get::most_bytes(window, &layout)
                .and_then(|bytes| bytes.checked_add(block))
                .ok_or_else(overflow)?;
            most = most.max(put).max(get);
            layouts.push(layout);
        }
        let mut charges = Self {
            window,
            layouts,
            piece,
            most,
            least: most,
        };
        charges.least = charges.of(Some(0));
        Ok(charges)
    }

    /// What an operation that writes `written` bytes holds, or one that writes none: a write at
    /// its body's length under the scheme that holds the most of it, every other operation the
    /// most. A length no layout lays out is charged the most.
    pub fn of(&self, written: Option<u64>) -> u64 {
        match written {
            Some(length) => self.write(self.window, length).min(self.most),
            None => self.most,
        }
    }

    /// What a write of `length` bytes holds with `window` blocks going down at once, under the
    /// scheme that holds the most of it: a length no layout lays out, the most a write of `window`
    /// blocks may hold.
    pub fn write(&self, window: usize, length: u64) -> u64 {
        let blocks = u64::try_from(window).unwrap_or(u64::MAX);
        let mut held = 0u64;
        for layout in &self.layouts {
            let bytes = layout.block_len(length, 0).ok().and_then(|block| {
                put::bytes_for(window, layout, length)?
                    .checked_add(copies(layout, block, blocks)?)?
                    .checked_add(self.piece)?
                    .checked_add(put::records_for(layout, length)?)
            });
            let most = || {
                layout.largest_block().and_then(|block| {
                    put::most_bytes(window, layout)?
                        .checked_add(copies(layout, block, blocks)?)?
                        .checked_add(self.piece)?
                        .checked_add(put::most_records(layout)?)
                })
            };
            held = held.max(bytes.or_else(most).unwrap_or(u64::MAX));
        }
        held
    }

    /// What a get holds with `window` blocks being read at once, under the scheme that holds the
    /// most of it: its driver's window at the largest block (`get::most_bytes`) and the block
    /// taken to be written out. A window no layout lays out is charged past any memory.
    pub fn read(&self, window: usize) -> u64 {
        let mut held = 0u64;
        for layout in &self.layouts {
            let bytes = layout
                .largest_block()
                .and_then(|block| get::most_bytes(window, layout)?.checked_add(block));
            held = held.max(bytes.unwrap_or(u64::MAX));
        }
        held
    }

    /// What a copy holds with `get` blocks of its source being read and `put` blocks of its copy
    /// of `length` bytes going down at once: its get's ([`Charges::read`]) beside its put's
    /// ([`Charges::write`]), as both run at once. A sum past a u64 saturates: past any memory.
    pub fn copy(&self, get: usize, put: usize, length: u64) -> u64 {
        self.read(get).saturating_add(self.write(put, length))
    }

    /// The least of the schemes' whole blocks: the most blocks a body is cut into.
    pub fn smallest_block(&self) -> u64 {
        self.layouts
            .iter()
            .filter_map(Layout::largest_block)
            .min()
            .unwrap_or(0)
    }

    /// The largest block any scheme's layout makes: the fewest blocks a body is cut into.
    pub fn largest_block(&self) -> u64 {
        self.layouts
            .iter()
            .filter_map(Layout::largest_block)
            .max()
            .unwrap_or(0)
    }

    /// The most any operation holds.
    pub fn most(&self) -> u64 {
        self.most
    }

    /// The least an operation holds: a write of no bytes.
    pub fn least(&self) -> u64 {
        self.least
    }
}

/// A put's chunks of a block of `block` stored bytes under `layout`, copied to send, for each of
/// `window` blocks going down.
fn copies(layout: &Layout, block: u64, window: u64) -> Option<u64> {
    let chunk = layout.chunk_len(block).ok()?;
    u64::try_from(layout.scheme().width())
        .ok()?
        .checked_mul(chunk)?
        .checked_mul(window)
}

/// What a node holds of its own asks at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Asks {
    pub chunks: usize,
    pub commands: usize,
    pub reads: usize,
}

/// The schemes a new block may take in a cell whose volume table names at most `domains` failure
/// domains: the durability model's choice at each count of them to the widest scheme's width,
/// as the host chooses a block's scheme over its volume table (`durability::choose`), the scheme
/// that meets `target` with the least overhead, or the closest where none does.
pub fn schemes(domains: usize, rates: &Rates, target: f64) -> Result<Vec<Scheme>, DeriveError> {
    let candidates = durability::candidates();
    let widest = candidates.iter().map(|s| s.width()).max().unwrap_or(0);
    let mut schemes: Vec<Scheme> = Vec::new();
    for count in 1..=domains.min(widest) {
        if let Some(Choice::Meets { scheme, .. } | Choice::Short { scheme, .. }) =
            durability::choose(&candidates, count, rates, target)?
            && !schemes.contains(&scheme)
        {
            schemes.push(scheme);
        }
    }
    Ok(schemes)
}

/// The most blocks a cell of `spread`'s nodes holds when each of its nodes' volumes holds
/// `fragments` chunks, as this node's do: each block a chunk on each of its scheme's width of
/// nodes, so the cell's chunks make a block for each width of them.
pub fn cell_blocks(fragments: u64, spread: Spread) -> Result<u64, DeriveError> {
    fragments
        .checked_mul(spread.nodes)
        .and_then(|chunks| chunks.checked_div(spread.width))
        .ok_or(DeriveError::Overflow("the cell's blocks"))
}

/// Each volume's bytes (docs/design/membership.md §3.1): one stated `volume = DIR SIZE` takes
/// `SIZE`; one stated to take what its file system spares keeps its layout's once laid out
/// (`Geometry::end`), and before takes the bytes the OS states this process may still write there,
/// less what its operator states the machine's other work keeps, less what the node writes there
/// besides: the volumes stated on that file system and not laid out yet, and where it is the node
/// directory's, the log's file of `log_file` bytes and the stores of each of its `ranges` ranges,
/// each at the most of its store a range's engine took an object ([`STORE_BYTES_AN_OBJECT`]) for
/// each block the cell's volumes make with this one's ([`cell_blocks`] at `spread`). That last is
/// the most `f` blocks of `b` bytes for which `f·b + R·c·n·(F + f)/w` stays within the space, `F`
/// the other volumes' fragments, `R` the ranges, `c` an object's store bytes and `n` and `w` the
/// spread, so the cell's objects' rows always fit beside it. No share is chosen: a file system two
/// such volumes may share would need one, and is refused.
pub fn volume_sizes(
    stated: &Stated,
    measured: &Measured,
    log_file: u64,
    ranges: usize,
    spread: Spread,
) -> Result<Vec<u64>, DeriveError> {
    let overflow = DeriveError::Overflow;
    let volumes: Vec<_> = stated.volumes.iter().zip(&measured.volumes).collect();
    let spares = |facts: &VolumeFacts, bytes: &VolumeBytes| {
        facts.formatted.is_none() && matches!(bytes, VolumeBytes::Spare { .. })
    };
    // The bytes settled without what a file system spares: stated, or laid out.
    let mut sizes = Vec::with_capacity(volumes.len());
    for ((_, bytes), facts) in &volumes {
        sizes.push(match (&facts.formatted, bytes) {
            (_, VolumeBytes::Stated(size)) => Some(*size),
            (Some(formatted), VolumeBytes::Spare { .. }) => Some(
                formatted
                    .geometry
                    .end()
                    .ok_or(overflow("a laid-out volume's bytes"))?,
            ),
            (None, VolumeBytes::Spare { .. }) => None,
        });
    }
    // Those that take what they spare, the one that may share the node directory's file system
    // last, since the stores there hold the rows of every other volume's objects.
    let mut order: Vec<usize> = (0..volumes.len()).collect();
    order.sort_by_key(|&i| volumes.get(i).map(|(_, f)| f.device.may_be(&measured.meta)));
    for i in order {
        let Some(&((dir, bytes), facts)) = volumes.get(i) else {
            continue;
        };
        let VolumeBytes::Spare { keep } = *bytes else {
            continue;
        };
        if facts.formatted.is_some() {
            continue;
        }
        let available = facts
            .device
            .available
            .ok_or_else(|| DeriveError::Unspared(dir.clone()))?;
        let shares_dir = facts.device.may_be(&measured.meta);
        let mut held = if shares_dir { log_file } else { 0 };
        let mut others = 0u64;
        for (j, (((other, theirs_stated), theirs), size)) in volumes.iter().zip(&sizes).enumerate()
        {
            if j == i {
                continue;
            }
            let near = theirs.device.may_be(&facts.device);
            if near && spares(theirs, theirs_stated) {
                return Err(DeriveError::Spares {
                    first: dir.clone(),
                    second: other.clone(),
                });
            }
            if near && theirs.formatted.is_none() {
                held = size
                    .and_then(|size| held.checked_add(size))
                    .ok_or(overflow("a volume's file system"))?;
            }
            if shares_dir {
                // Every other volume is sized by now, but one that may share the node directory's
                // file system too: the stores there would hold both's rows.
                let Some(size) = *size else {
                    return Err(DeriveError::Spares {
                        first: dir.clone(),
                        second: other.clone(),
                    });
                };
                let block = match &theirs.formatted {
                    Some(formatted) => Some(formatted.geometry.block),
                    None => volume_layout::block(theirs.device.align),
                };
                others = block
                    .and_then(|block| size.checked_div(block))
                    .and_then(|fragments| others.checked_add(fragments))
                    .ok_or(overflow("the volumes' fragments"))?;
            }
        }
        let short = || DeriveError::Spare {
            dir: dir.clone(),
            available,
            keep,
            held,
        };
        let room = available
            .checked_sub(keep)
            .and_then(|room| room.checked_sub(held))
            .ok_or_else(short)?;
        let size = if shares_dir {
            let block =
                volume_layout::block(facts.device.align).ok_or(overflow("a volume's block"))?;
            let rows = u128::try_from(ranges)
                .ok()
                .and_then(|ranges| ranges.checked_mul(u128::from(STORE_BYTES_AN_OBJECT)))
                .and_then(|rows| rows.checked_mul(u128::from(spread.nodes)))
                .ok_or(overflow("the stores' rows"))?;
            let width = u128::from(spread.width);
            let per_fragment = u128::from(block)
                .checked_mul(width)
                .and_then(|bytes| bytes.checked_add(rows))
                .ok_or(overflow("a fragment's bytes"))?;
            let fragments = u128::from(room)
                .checked_mul(width)
                .ok_or(overflow("a volume's file system"))?
                .checked_sub(rows.checked_mul(u128::from(others)).ok_or_else(short)?)
                .ok_or_else(short)?
                .checked_div(per_fragment)
                .ok_or(overflow("a volume's fragments"))?;
            fragments
                .checked_mul(u128::from(block))
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or(overflow("a volume's bytes"))?
        } else {
            room
        };
        if let Some(slot) = sizes.get_mut(i) {
            *slot = Some(size);
        }
    }
    sizes
        .into_iter()
        .map(|size| size.ok_or(overflow("a volume's bytes")))
        .collect()
}

/// A cell's nodes and the width of the scheme its blocks take ([`Spread::of`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Spread {
    pub nodes: u64,
    pub width: u64,
}

impl Spread {
    /// A cell of `domains` failure domains, a node each, its blocks at the scheme the durability
    /// model chooses at `domains` (`durability::choose`).
    pub fn of(domains: usize, rates: &Rates, target: f64) -> Result<Self, DeriveError> {
        let width = match durability::choose(&durability::candidates(), domains, rates, target)? {
            Some(Choice::Meets { scheme, .. } | Choice::Short { scheme, .. }) => scheme.width(),
            None => return Err(DeriveError::Schemes { domains }),
        };
        let overflow = || DeriveError::Overflow("the cell's blocks");
        Ok(Self {
            nodes: u64::try_from(domains).map_err(|_| overflow())?,
            width: u64::try_from(width).map_err(|_| overflow())?,
        })
    }
}

/// The asks a node of `gateway` holds at most, each operation it hosts the most of the schemes
/// its blocks may take (`schemes`): a put's window of chunks at their width, or a get's spans and
/// the whole chunks a failed span adds (`put::most_chunks`, `get::most_chunks`); a put's command
/// for each block of the largest body S3 takes and its file's write (`put::most_commands`); one
/// read an operation; and one command besides, the node's own enlistment.
pub fn asks(gateway: &Gateway, schemes: &[Scheme]) -> Result<Asks, DeriveError> {
    let overflow = |what| DeriveError::Overflow(what);
    let (mut chunks, mut commands) = (0usize, 0u64);
    for &scheme in schemes {
        let most = put::most_chunks(gateway.window, scheme)
            .zip(get::most_chunks(gateway.window, scheme))
            .map(|(put, get)| put.max(get))
            .ok_or(overflow("a scheme's chunk asks"))?;
        chunks = chunks.max(most);
        let layout = Layout::new(scheme).map_err(|_| overflow("a scheme's layout"))?;
        commands = commands.max(put::most_commands(&layout));
    }
    let per = |n: usize| n.checked_mul(gateway.objects);
    let commands = usize::try_from(commands)
        .ok()
        .and_then(per)
        .and_then(|n| n.checked_add(1))
        .ok_or(overflow("the command asks"))?;
    Ok(Asks {
        chunks: per(chunks).ok_or(overflow("the chunk asks"))?,
        commands,
        reads: per(1).ok_or(overflow("the read asks"))?,
    })
}

/// The objects' class at the floor: the bytes its operations may hold, and, where a scheme its
/// blocks may take codes, the coding library's tables, once a process (`mantle_ec::TABLE_BYTES`).
pub fn objects(gateway: &Gateway, schemes: &[Scheme]) -> Result<u64, DeriveError> {
    let codes = schemes.iter().any(|s| matches!(s, Scheme::Rs(_)));
    let tables = if codes { mantle_ec::TABLE_BYTES } else { 0 };
    gateway
        .objects_bytes
        .checked_add(tables)
        .ok_or(DeriveError::Overflow("an operation's bytes"))
}

/// What a node's operations hold at a count of them, beside their charges: their asks, the
/// transport's bounds at its floor, its bytes beside its budget, and the ranges' members.
struct Held {
    asked: Asks,
    bounds: mantle_protocol::bounds::Stated,
    beside: u64,
    ranges_bytes: u64,
    /// The S3 listener's connections, one an operation, each at what it holds
    /// (`listener::CONNECTION_BYTES`) and the slots of the TLS sessions it leaves
    /// (`listener::SESSION_BYTES`); none for a node with no S3 endpoint.
    listener: u64,
    /// The runtime's shards, sized for those connections, and the listener's lanes
    /// ([`runtime_bytes`]).
    runtime: u64,
}

/// Why the settings cannot be derived: a fact that admits no unit of some class of work.
#[derive(Debug, thiserror::Error)]
pub enum DeriveError {
    #[error("the metadata layer's bounds: {0}")]
    Record(#[from] RecordError),
    #[error("{0} past what this machine addresses")]
    Overflow(&'static str),
    #[error("no engine page holds a row of a {key}-byte key and a {value}-byte value")]
    Page { key: usize, value: usize },
    #[error("a range's share of the metadata device, {share} bytes, holds no extent of {extent}")]
    Extents { share: u64, extent: u64 },
    #[error("the log: {0}")]
    Log(#[from] LogError),
    #[error("no segment one I/O buffer reads holds a record of a {bytes}-byte chunk")]
    Chunk { bytes: u64 },
    #[error("a volume's index of {bytes} bytes holds no fragment")]
    Index { bytes: u64 },
    #[error("a volume: {0}")]
    Volume(#[from] ChunkError),
    #[error("the durability model: {0}")]
    Durability(#[from] DurabilityError),
    #[error("no scheme the durability model knows fits a cell of {domains} failure domains")]
    Schemes { domains: usize },
    #[error("the node's floor of {floor} bytes passes its allowance of {allowance} at its {class}")]
    Memory {
        class: Class,
        floor: u64,
        allowance: u64,
    },
    #[error(
        "a stated memory ceiling of {stated} bytes past the {capacity} the OS lets the process hold"
    )]
    Ceiling { stated: u64, capacity: u64 },
    #[error(
        "a range's share of the memory left, {bytes} bytes, holds the metadata of no extent of {per_extent}"
    )]
    Metadata { bytes: u64, per_extent: u64 },
    #[error(
        "the node directory's file system holds {bytes} bytes, short of the {held} its log's file and the volumes on its device take"
    )]
    Space { bytes: u64, held: u64 },
    #[error("the ranges' stores were made at pages of {first} and {second} bytes; a node runs one")]
    StorePages { first: usize, second: usize },
    #[error(
        "a range's store was made at pages of {page} bytes, which hold no row at the range's bounds or are off its device's {align}-byte unit"
    )]
    StorePage { page: usize, align: usize },
    #[error("the node's bounds: {0}")]
    Bounds(String),
    #[error("the node directory's file system does not say its size")]
    Unsized,
    #[error("the file system of the volume in {0} does not say what a process may write there")]
    Unspared(PathBuf),
    #[error(
        "the file system of the volume in {dir} spares {available} bytes, short of the {keep} its other work keeps and the {held} the node writes there besides"
    )]
    Spare {
        dir: PathBuf,
        available: u64,
        keep: u64,
        held: u64,
    },
    #[error(
        "the volumes in {first} and {second} may share a file system, and each is to take what it spares"
    )]
    Spares { first: PathBuf, second: PathBuf },
    #[error("{measured} devices measured for the {stated} volumes stated")]
    Measurements { stated: usize, measured: usize },
    #[error("range {group:x}'s store: {why}")]
    Store { group: u128, why: String },
    #[error(
        "the process may hold {limit} descriptors, short of the {fixed} the node holds whatever its operations and the {each} each of its {operations} operations holds beside them"
    )]
    Descriptors {
        limit: u64,
        fixed: u64,
        each: u64,
        operations: u64,
    },
}

/// The range's settings and rules, which every member of the cell derives alike, from the
/// identities the cell's configuration names and the metadata layer's own bounds alone:
///
/// - an entry of the range's largest (`wire::largest_entry_bytes`) is what each append carries and
///   what one drive applies; a leader holds as many of them uncommitted as a range can have, with
///   as many appends out to each follower ([`in_flight`]), so no append waits for another's
///   answer while an entry the cell asked is unsent;
/// - a session of each identity, for good: a registration ends its owner's last
///   (`mantle_meta::session::register`), so no session waits out a lifetime;
/// - a session's answers kept until the gateway acknowledges them, as many as its row holds
///   within the largest row S3 admits, so the store's page does not grow for them, and one at the
///   largest a node's command gets (`wire::largest_answer_bytes`) where it keeps no other; a
///   command whose answer its row would not hold is refused before it takes effect and asked
///   again (node.md §2.8), and an entry carries every command waiting that fits its bytes, no
///   count besides (research/87);
/// - a row's key and value at most the longest key S3's names give a row
///   (`key::largest_bytes`) and the largest value a request S3 admits, or a session at its
///   bounds, gives one (`wire::largest_row_bytes`).
pub fn cell(identities: u64) -> Result<(Settings, Rules), DeriveError> {
    let entry = u64::try_from(wire::largest_entry_bytes()?)
        .map_err(|_| DeriveError::Overflow("the largest entry"))?;
    let in_flight = in_flight(identities)?;
    let settings = Settings {
        election_tick: ELECTION_TICK,
        heartbeat_tick: HEARTBEAT_TICK,
        max_size_per_msg: entry,
        max_inflight_msgs: usize::try_from(in_flight)
            .map_err(|_| DeriveError::Overflow("the appends out"))?,
        max_uncommitted_size: entry
            .checked_mul(in_flight)
            .ok_or(DeriveError::Overflow("the uncommitted entries"))?,
        max_committed_size_per_ready: entry,
        max_entry_bytes: entry,
    };
    let row = wire::largest_row_bytes()?;
    // A session that keeps no other answer keeps one at the largest a node's command gets, so
    // every command applies in a session whose earlier answers are acknowledged.
    let one = wire::largest_answer_bytes()?;
    if Session::row_bytes(1, one).is_none_or(|session| session > row) {
        return Err(DeriveError::Overflow("a session's answer within a row"));
    }
    let rules = Rules {
        lifetime_ns: u64::MAX,
        max_sessions: identities,
        // Nothing expires by time: one is the least work an entry's expiry may do.
        expiries_per_entry: 1,
        max_key_bytes: key::largest_bytes(),
        max_value_bytes: row,
    };
    Ok((settings, rules))
}

/// The entries a range of a cell of `identities` holds uncommitted at most, and so the appends its
/// leader may keep out to each follower: [`ENTRIES_OUT`] an identity, as each asks a range through
/// at most as many entries out at once (`ask::Asking::another_entry`), and one the leader appends
/// of its own, its term's first entry or a change of its configuration (Ongaro 2014, §3.6.2 and
/// §4). More would never be sent; fewer leaves an asked entry unsent, refused for the uncommitted
/// bound, while a follower's round trip and flush answer the last, where two had held every frame
/// to a confirmation of its own (research/71, step 9). Bytes stay bounded at as many of the range's
/// largest entries (`max_uncommitted_size`). A proposal past them, a copy asked again while its
/// first is uncommitted, is refused by the core and asked again.
pub fn in_flight(identities: u64) -> Result<u64, DeriveError> {
    let overflow = || DeriveError::Overflow("the appends out");
    let out = u64::try_from(ENTRIES_OUT).map_err(|_| overflow())?;
    identities
        .checked_mul(out)
        .and_then(|entries| entries.checked_add(1))
        .ok_or_else(overflow)
}

/// The message bounds every node of a cell states, which a client of its ranges states the same:
/// a range's largest message from the cell's settings, which every range shares, and a chunk at
/// the largest a gateway writes (`mantle_gateway::layout::CHUNK`).
pub fn messages() -> Result<mantle_protocol::classes::Bounds, DeriveError> {
    let (settings, rules) = cell(1)?;
    crate::node::message_bounds(
        &[Hosted {
            group: 0,
            range: Range {
                layer: Layer::Name,
                rules,
                boot: ConfState::default(),
                settings,
                members: 0,
            },
            engine: (),
        }],
        mantle_gateway::layout::CHUNK,
    )
    .map_err(|e| DeriveError::Bounds(e.to_string()))
}

/// A range's engine on a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Engine {
    pub store: StoreConfig,
    pub memtable_bytes: usize,
    pub trunk: TrunkConfig,
}

/// The engine of each range a node hosts, on a metadata device whose write unit is `block` bytes
/// and of which each range's engine may take `share` bytes:
///
/// - a page of the device's write unit, at least the least whose branch pages hold a row at the
///   range's bounds (`mantle_engine::branch::least_page`), and extents of the least pages the
///   store takes, as many as the share holds;
/// - the least write buffer, one row at the bounds whole
///   (`mantle_engine::memtable::hashed::entry_bytes`);
/// - SplinterDB's fanout, and a leaf holding every row one memtable gives it, the most a
///   memtable holds being rows of the shortest key and no value (`key::least_bytes`).
pub fn engine(block: usize, share: u64, rules: &Rules) -> Result<Engine, DeriveError> {
    let (key, value) = (rules.max_key_bytes, rules.max_value_bytes);
    let page_size = mantle_engine::branch::least_page(block, key, value)
        .ok_or(DeriveError::Page { key, value })?;
    let memtable_bytes = mantle_engine::memtable::hashed::entry_bytes(key, value)
        .ok_or(DeriveError::Overflow("a memtable"))?;
    let extent = u64::try_from(page_size)
        .ok()
        .and_then(|page| page.checked_mul(u64::from(EXTENT_PAGES)))
        .ok_or(DeriveError::Overflow("an extent"))?;
    // The superblocks' extent and one more at least.
    let max_extents = share.checked_div(extent).unwrap_or(0);
    if max_extents < 2 {
        return Err(DeriveError::Extents { share, extent });
    }
    let least = mantle_engine::memtable::hashed::entry_bytes(key::least_bytes(), 0)
        .ok_or(DeriveError::Overflow("a memtable's entry"))?;
    let leaf_entries = memtable_bytes
        .checked_div(least)
        .and_then(|rows| u64::try_from(rows).ok())
        .ok_or(DeriveError::Overflow("a leaf's entries"))?;
    Ok(Engine {
        store: StoreConfig {
            page_size,
            extent_pages: EXTENT_PAGES,
            max_extents,
        },
        memtable_bytes,
        trunk: TrunkConfig {
            fanout: FANOUT,
            leaf_entries,
        },
    })
}

/// What `engine`'s resident metadata takes at its device's full share: each extent its share
/// holds at the metadata measured at the densest rows and its allocator's own
/// (`mantle_engine::shard_db::measured_per_extent`), the most its share of memory may be.
pub fn engine_wants(engine: &Engine) -> Option<u64> {
    engine
        .store
        .max_extents
        .checked_mul(mantle_engine::shard_db::measured_per_extent(&engine.store)?)
}

/// The bytes of an extent of `engine`'s store: its pages'.
fn extent_bytes(engine: &Engine) -> Option<u64> {
    u64::try_from(engine.store.page_size)
        .ok()?
        .checked_mul(u64::from(engine.store.extent_pages))
}

/// The extents `engine`'s rows take for `objects` objects: their store's bytes at the most of its
/// store any range's engine took an object ([`STORE_BYTES_AN_OBJECT`]), at most its device's share
/// holds.
fn extents_for(engine: &Engine, objects: u64) -> Option<u64> {
    Some(
        objects
            .checked_mul(STORE_BYTES_AN_OBJECT)?
            .div_ceil(extent_bytes(engine)?)
            .min(engine.store.max_extents),
    )
}

/// What `engine`'s resident metadata takes for `objects` objects, its want in [`shares`]: the
/// extents their rows take at the most of its store any range's engine took an object
/// ([`STORE_BYTES_AN_OBJECT`]), at most its device's share holds, each at the metadata measured at
/// the densest rows and its allocator's own (`mantle_engine::shard_db::measured_per_extent`).
pub fn engine_wants_for(engine: &Engine, objects: u64) -> Option<u64> {
    extents_for(engine, objects)?
        .checked_mul(mantle_engine::shard_db::measured_per_extent(&engine.store)?)
}

/// What `engine`'s page cache takes for `objects` objects, a performance want past the capacity
/// wants and the operations the node serves ([`derive`], research/87 §4 rule D): every page of the extents their rows take, the estimate its metadata's
/// want counts ([`engine_wants_for`]), at its store's page size, a cached page holding a page's
/// verified payload (`mantle_engine::store::cache`). The store's bytes an object are its file's
/// length at the most it held ([`STORE_BYTES_AN_OBJECT`]), so they count its pages as filled and
/// compaction's room besides; the cache keeps live pages alone, an extent released dropping its
/// own, so it never holds more. With it whole, a get or a one-row lookup of any row reads each
/// page from memory once read, where without it each read a page from the device (a `pread`, a
/// copy and a checksum on the node's busy shard) every time.
pub fn engine_cache_wants_for(engine: &Engine, objects: u64) -> Option<u64> {
    extents_for(engine, objects)?.checked_mul(extent_bytes(engine)?)
}

/// The extents an engine opens with: its superblocks' and one more for its rows. A share holding
/// fewer is refused ([`engine_within`]).
const OPENS_WITH: u64 = 2;

/// `engine` with as many extents as `metadata` bytes of the node's memory hold at the metadata
/// measured at the densest rows and their allocator's own
/// (`mantle_engine::shard_db::measured_per_extent`), at most its device's share holds, and the
/// extents it opens with at least ([`OPENS_WITH`]).
pub fn engine_within(engine: Engine, metadata: u64) -> Result<Engine, DeriveError> {
    let per_extent = mantle_engine::shard_db::measured_per_extent(&engine.store)
        .ok_or(DeriveError::Overflow("an extent's metadata"))?;
    let held = metadata.checked_div(per_extent).unwrap_or(0);
    if held < OPENS_WITH {
        return Err(DeriveError::Metadata {
            bytes: metadata,
            per_extent,
        });
    }
    Ok(Engine {
        store: StoreConfig {
            max_extents: engine.store.max_extents.min(held),
            ..engine.store
        },
        ..engine
    })
}

/// The maintenance workers each range's engine runs its packings and compactions on
/// (`ShardDb::set_workers_within`), where the allowance holds the node's need with them
/// ([`derive`]): one, the least a pool holds. Measured: at 4,000 to 6,000 small PUTs a second the
/// leader's shard spent 31% of its busy time packing and compacting its five ranges inline
/// (`Trunk::run` 26%, the memtable's packing 5%; macOS `sample` of `mantle bench doors --sizes 1
/// --kinds PUT,GET --load open:12000`, 2026-11-19), a need of one worker for all five by Little's
/// law, as an engine's pool measures its need (`trunk::pool`); each worker more costs a
/// compaction's statement, about 90 MB at the range's longest keys ([`workers_bytes`]). A pool
/// serves one engine's file (`trunk::pool::serve`), so the node's engines cannot share one, and its
/// five ranges hold five workers, where node.md §1.2 states the engines' pools shared.
pub const ENGINE_WORKERS: usize = 1;

/// The entries a sorter's two lists, a kept key's hash and offset, and a radix sort's counts take
/// a memtable's packing (`mantle_engine::memtable::hashed::Sorter`, `mantle_engine::kept::KeyList`):
/// 16 bytes an entry each list, 12 a key, and eight passes of 256 four-byte counts.
const SORT_ENTRY_BYTES: u64 = 2 * 16 + 12;
const SORT_COUNTS_BYTES: u64 = 8 * 256 * 4;

/// What `workers` maintenance workers of `engine`'s hold beside their threads' stacks, at keys of
/// at most `key` bytes: each worker's store keeps its pools at a compaction's most, a cursor over
/// each of its inputs (`TrunkConfig::inputs_most`) and two builders of at most a leaf's limit
/// (`TrunkConfig::built_most`), its reads one at a time with no issuer and the store's range
/// filter (`mantle_engine::shard_db::pooled_bytes`, a packing's one builder within them); and a
/// packing's buffers for each worker and the active memtable, which keeps the copy of its arena a
/// worker sorts (`SortPack`): an arena of the memtable's bytes, and the sorter's lists, the kept
/// keys and a cut at each of the root's pivots for its entries, at most a leaf's
/// (`TrunkConfig::leaf_entries`, every row a memtable holds). None for none: without workers a
/// memtable keeps no copy.
pub fn workers_bytes(engine: &Engine, key: usize, workers: usize) -> Option<u64> {
    if workers == 0 {
        return Some(0);
    }
    let trunk = &engine.trunk;
    let pooled = mantle_engine::shard_db::pooled_bytes(
        &engine.store,
        trunk.inputs_most(),
        2,
        key,
        trunk.built_most(trunk.leaf_entries),
        1,
        mantle_engine::store::RANGE_FILTER_BITS,
    )?;
    let cuts = u64::try_from(trunk.fanout.checked_add(1)?)
        .ok()?
        .checked_mul(u64::try_from(key.checked_add(size_of::<Vec<u8>>())?).ok()?)?;
    let packing = u64::try_from(engine.memtable_bytes)
        .ok()?
        .checked_add(trunk.leaf_entries.checked_mul(SORT_ENTRY_BYTES)?)?
        .checked_add(SORT_COUNTS_BYTES)?
        .checked_add(cuts)?;
    let workers = u64::try_from(workers).ok()?;
    workers
        .checked_mul(pooled)?
        .checked_add(workers.checked_add(1)?.checked_mul(packing)?)
}

/// The node's shared log on a device whose write unit is `align`, for `ranges` ranges and the
/// liveness stream's group (`hyper_log::Config::derive`):
///
/// - a frame of the range's largest entry with its durable shell's bytes;
/// - a checkpoint's cadence, and what a group holds uncommitted past it, of one `Ready` of the
///   range's settings each (`mantle_range::ready_held`): its entries, as many as its bytes make of
///   the least entry a range proposes and the term's empty first one, and its bytes with each
///   entry's shell, so a group's retention holds the two cadences and a `Ready`, as the range
///   checks (`mantle_range`'s `check_settings`);
/// - a cache of one largest entry a group;
/// - a file holding every group's retention at once, beside the least file the log runs in: a
///   `Ready`'s parts, each its entries behind their records' headers, as many as greedy parts of
///   a frame's room make of them, each a segment at most, since a frame never spans segments. A
///   write past what the file holds is refused `Full` until a checkpoint frees room, which a
///   group writing toward its retention would otherwise wait on.
pub fn log(align: Alignment, settings: &Settings, ranges: usize) -> Result<LogConfig, DeriveError> {
    let overflow = |what| DeriveError::Overflow(what);
    let shell = u64::try_from(hyper_durable::ENTRY_OVERHEAD).map_err(|_| overflow("a shell"))?;
    let largest_entry = settings
        .max_entry_bytes
        .checked_add(shell)
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or(overflow("the largest entry"))?;
    let groups = ranges.checked_add(1).ok_or(overflow("the log's groups"))?;
    let (entries, payload) = mantle_range::ready_held(settings).ok_or(overflow("a Ready"))?;
    let cache_bytes = u64::try_from(largest_entry)
        .ok()
        .and_then(|entry| entry.checked_mul(u64::try_from(groups).ok()?))
        .ok_or(overflow("the log's cache"))?;
    let facts = |disk_bytes| Facts {
        align,
        sealed: false,
        largest_entry,
        disk_bytes,
        max_groups: groups,
        cadence_entries: entries,
        cadence_bytes: payload,
        uncommitted_entries: entries,
        uncommitted_bytes: payload,
        cache_bytes,
    };
    // The least file, its persist area and three segments, is what a budget of none is refused
    // for; its segment is the one every file of these facts takes.
    let least_file = match LogConfig::derive(&facts(0)) {
        Err(LogError::Unfit(Unfit::Disk { needed, .. })) => needed,
        Err(e) => return Err(e.into()),
        Ok(config) => return Ok(config),
    };
    let segment = least_file.checked_div(4).ok_or(overflow("a segment"))?;
    let block = u64::try_from(align.get()).map_err(|_| overflow("the block"))?;
    let frame = hyper_log::format::FRAME_HEADER_BYTES;
    let on_disk = entries
        .checked_mul(hyper_log::format::ENTRY_HEADER_BYTES)
        .and_then(|headers| headers.checked_add(payload))
        .ok_or(overflow("a Ready on disk"))?;
    let room = segment
        .checked_sub(block)
        .and_then(|room| room.checked_sub(frame))
        .filter(|room| *room > 0)
        .ok_or(overflow("a frame's room"))?;
    // A Ready goes in parts, each filled until its next entry would pass a frame's room, so any
    // two in a row hold more than one room; and a part takes a frame of its own, which never
    // spans segments.
    let parts = on_disk
        .div_ceil(room)
        .checked_mul(2)
        .and_then(|parts| parts.checked_add(1))
        .ok_or(overflow("a Ready's parts"))?;
    // Each group's retention, two cadences and a Ready uncommitted, every group at once.
    let retained = parts
        .checked_mul(3)
        .and_then(|group| group.checked_mul(u64::try_from(groups).ok()?))
        .ok_or(overflow("the groups' retention"))?;
    let disk = retained
        .checked_mul(segment)
        .and_then(|bytes| bytes.checked_add(least_file))
        .ok_or(overflow("the log's file"))?;
    Ok(LogConfig::derive(&facts(disk))?)
}

/// What hyper-log keeps for each entry a group retains: its term, its place, its length and its
/// cached bytes' handle (`state::Slot`, 56 bytes on a 64-bit target).
const LOG_SLOT_BYTES: u64 = 56;

/// The segments' worth of buffers hyper-log keeps beside its groups: its device pool's two free,
/// its three frames in flight (`PIPELINE_FRAMES`) and the one it keeps, its queue's three
/// (`3·(frame room + PERSIST_GROUP_LEN)`), and recovery's window of one.
const LOG_SEGMENTS_HELD: u64 = 10;

/// The most bytes the node's log of `config` holds: for each group, its slots as a deque doubled
/// past its retained entries holds them, its handle's unapplied entries up to its retained bytes,
/// and the log's cache of it; one group's slots again while its deque grows, old beside new; and
/// the log's own buffers. Pinned by `tests/log_memory.rs`, every group filled to its retention.
pub fn log_bytes(config: &LogConfig) -> Option<u64> {
    let groups = u64::try_from(config.max_groups).ok()?;
    let slots = config.group_entries.checked_mul(LOG_SLOT_BYTES)?;
    let group = slots
        .checked_mul(2)?
        .checked_add(config.group_bytes)?
        .checked_add(config.group_cache)?;
    group
        .checked_mul(groups)?
        .checked_add(slots)?
        .checked_add(config.segment_bytes.checked_mul(LOG_SEGMENTS_HELD)?)
}

/// The writes a range's member keeps out at once on the node's log: one in each of hyper-log's
/// pipeline frames (`GroupLog::depth`), which the shell takes `Ready`s ahead of persistence to.
/// hyper-log keeps the count to itself; `tests/log_memory.rs` pins it to a live group's depth.
pub const LOG_DEPTH: usize = 3;

/// The heap a range's member takes as it opens, beside what it keeps for each member of its
/// configuration: measured, 1,672 bytes under elections by suspicion, the node's, at one to ten
/// members (`tests/range_memory.rs`); 1,584 before hyper-raft's d804110 gave the core's watch the
/// grants each candidacy stood for (`Watch::grant_wait`); 1,104 before hyper-raft's core step R-8
/// (5626e0b) gave a member its held-commit state, which b5ea01b keeps behind pointers, and 1,080
/// before d97153c gave the core's watch its hand-over order and the voter it left.
pub const MEMBER_BYTES: u64 = 1_672;

/// The bytes of the context a node names a read by: the read's number (`Node`'s reads).
const READ_CONTEXT_BYTES: u64 = 8;

/// The most bytes a member of `range` holds beside its log and its engine, asked `reads` reads at
/// once, its node's own and those its peers' exchanges bring: hyper-durable's holds (hyper-raft
/// docs/durable.md §6), each at the bound hyper-raft's limits put on it (`mantle_range::limits`),
/// a vector at twice what it holds as doubling grows it, four at least:
/// - what it keeps of each member: its progress and its window of appends, its vote and its
///   place in a quorum's count, as the core counts them (`ProgressTracker::resident_bytes`);
/// - the entries not yet durable, at most `unstable_entries`; their bytes a leader's uncommitted
///   proposals or a follower's window, and one entry past either (`mantle_range::ready_bytes`),
///   at twice those bytes, the room the encoder leaves a proposal;
/// - the entries kept ahead of a hole in its log, at most `unstable_entries`; their bytes the
///   leader's window to it and one entry past, the most a leader sends ahead of what it heard:
///   the core bounds their count and not their bytes;
/// - the messages that wait to be taken, past `pending_messages` refused and one operation adding
///   at most two a member and one a member for each place in its window: the queue, and one
///   given with each write out and kept spare after, `LOG_DEPTH` of them;
/// - the appends its window holds to each other member, of at most `entries_per_message` entries
///   and the larger of a message's bytes and one entry's;
/// - the reads, `reads` or `pending_reads` if fewer, in the leader's queue, its confirmed reads
///   and the shell's, each with its context, its askers and its confirmations, a member each;
/// - its store's (`hyper_durable::GroupStore`): each entry of its writes out copied in the log's
///   form, its bytes and the shell's and a slot of hyper-log's entry, the slots thrice as an
///   update is cut into a frame's parts, every entry in one write, so as many as are not yet
///   durable; and the page it reads the log by, of the larger of a message's bytes and a drive's
///   apply and one entry past, each entry counted at its fixed bytes in a message at least.
///
/// Pinned by `tests/range_memory.rs`, three members counted through the shapes that fill each
/// over stores that hold nothing of their own, and `tests/range_log_memory.rs`, the same over the
/// node's logs. What the core and the shell may hold is wider than what a range reaches: each
/// queue's bound is what one `Ready` of the range's settings holds (`mantle_range::limits`).
pub fn range_bytes(range: &Range, reads: usize) -> Option<u64> {
    let s = &range.settings;
    let limits = mantle_range::limits(range).ok()?;
    let count = |n: usize| u64::try_from(n).ok();
    let grown = |n: u64| n.checked_mul(2).map(|slots| slots.max(4));
    let members = count(limits.members)?;
    let inflight = count(s.max_inflight_msgs)?;
    let id = count(size_of::<u64>())?;
    let entry = count(size_of::<Entry>())?;
    let each = count(
        size_of::<(u64, Progress)>()
            .checked_add(size_of::<(u64, bool)>())?
            .checked_add(size_of::<u64>())?
            .checked_add(size_of::<u64>().checked_mul(4)?)?,
    )?
    .checked_add(grown(inflight)?.checked_mul(count(size_of::<(u64, u64)>())?)?)?;
    let tracker = members.checked_mul(each)?;
    let ready = mantle_range::ready_bytes(s)?;
    let window = inflight
        .checked_mul(s.max_size_per_msg)?
        .checked_add(s.max_entry_bytes)?;
    let slots = grown(count(limits.unstable_entries)?)?.checked_mul(entry)?;
    let unstable = slots.checked_add(ready.checked_mul(2)?)?;
    let early = slots.checked_add(window)?;
    let per_operation = members.checked_mul(inflight.checked_add(2)?)?;
    let queue = grown(count(limits.pending_messages)?.checked_add(per_operation)?)?
        .checked_mul(count(size_of::<Message>())?)?;
    let queues = queue.checked_mul(count(LOG_DEPTH)?.checked_add(1)?)?;
    let append = grown(count(limits.entries_per_message)?)?
        .checked_mul(entry)?
        .checked_add(s.max_size_per_msg.max(s.max_entry_bytes))?;
    let appends = members
        .checked_sub(1)?
        .checked_mul(inflight)?
        .checked_mul(append)?;
    let asked = count(reads.min(limits.pending_reads))?;
    let read_slots = count(
        size_of::<PendingRead>()
            .checked_add(size_of::<hyper_raft::ReadState>())?
            .checked_add(size_of::<(u64, Vec<u8>)>())?,
    )?;
    let per_read = READ_CONTEXT_BYTES
        .checked_mul(3)?
        .checked_add(grown(members)?.checked_mul(id)?.checked_mul(2)?)?;
    let read_bytes = grown(asked)?
        .checked_mul(read_slots)?
        .checked_add(asked.checked_mul(per_read)?)?;
    let shell = count(hyper_durable::ENTRY_OVERHEAD)?;
    let log_entry = count(size_of::<hyper_log::Entry>())?;
    let writes = count(limits.unstable_entries)?
        .checked_mul(shell.checked_add(log_entry.checked_mul(3)?)?)?
        .checked_add(ready)?;
    let page = s.max_size_per_msg.max(s.max_committed_size_per_ready);
    let fetched = grown(page.checked_add(s.max_entry_bytes)?.checked_add(shell)?)?.checked_add(
        grown(
            page.checked_div(count(hyper_raft::wire::ENTRY_FIXED_BYTES)?)?
                .checked_add(1)?,
        )?
        .checked_mul(count(size_of::<(u64, usize, usize)>())?)?,
    )?;
    MEMBER_BYTES
        .checked_add(writes)?
        .checked_add(fetched)?
        .checked_add(tracker)?
        .checked_add(unstable)?
        .checked_add(early)?
        .checked_add(queues)?
        .checked_add(appends)?
        .checked_add(read_bytes)
}

/// A peer's state on the plane beside its epochs' replay windows and its datagram's room:
/// measured, 182 bytes at the most.
const PLANE_PEER_BYTES: u64 = 182;

/// The most bytes the node's transport holds beside its budget (`transport_bytes`), for a node of
/// `stated`: what the budget does not charge. The transport's own, an endpoint that serves
/// handshakes (`mantle_protocol::bounds::beside`); the plane ([`plane_bytes`]); and the liveness
/// stream ([`liveness_bytes`]).
pub fn transport_bytes_beside(stated: &mantle_protocol::bounds::Stated) -> Option<u64> {
    mantle_protocol::bounds::beside(stated, true)?
        .checked_add(plane_bytes(&crate::node::plane(stated).ok()?)?)?
        .checked_add(liveness_bytes(stated.peers)?)
}

/// The most bytes the plane of `limits` holds: the datagram it seals into, and for each peer its
/// state, each epoch's replay window a bit a counter, and a datagram's room of messages queued.
pub fn plane_bytes(limits: &hyper_datagram::PlaneLimits) -> Option<u64> {
    let count = |n: usize| u64::try_from(n).ok();
    let datagram = count(hyper_datagram::MAX_DATAGRAM_BYTES)?;
    let windows =
        count(limits.window_limit.div_ceil(8))?.checked_mul(count(limits.epochs_per_peer)?)?;
    let peer = datagram
        .checked_add(PLANE_PEER_BYTES)?
        .checked_add(windows)?;
    count(limits.max_peers)?
        .checked_mul(peer)?
        .checked_add(datagram)
}

/// The most bytes the liveness stream of a node of `peers` holds: a link estimator for each peer
/// and two of the node's own, its pool's and its own flushes', each ring at its longest window,
/// `WINDOW_LIMIT + 1` prefix sums of eight bytes (hyper-timing's derivation from RFC 5905's
/// frequency tolerance).
pub fn liveness_bytes(peers: usize) -> Option<u64> {
    let ring = hyper_timing::WINDOW_LIMIT
        .checked_add(1)?
        .checked_mul(u64::try_from(size_of::<i64>()).ok()?)?;
    u64::try_from(peers.max(1))
        .ok()?
        .checked_add(2)?
        .checked_mul(ring)
}

/// Standard input, output and error, which every process holds open (POSIX's `STDIN_FILENO`,
/// `STDOUT_FILENO` and `STDERR_FILENO`; Windows' three standard handles).
pub const STDIO_DESCRIPTORS: u64 = 3;

/// What one of the runtime's shards holds on Linux: its epoll instance, its kick's eventfd, and the
/// timerfd it waits on where the kernel has no `epoll_pwait2` (hyper-rt `epoll.rs`, `driver.rs`).
pub const EPOLL_SHARD_DESCRIPTORS: u64 = 3;

/// What one of the runtime's shards holds on macOS and the BSDs: its kqueue, which its kick is an
/// `EVFILT_USER` event of (hyper-rt `kqueue.rs`).
pub const KQUEUE_SHARD_DESCRIPTORS: u64 = 1;

/// What one of the runtime's shards holds on Windows: its completion port and the AFD device its
/// sockets' readiness comes through (hyper-rt `iocp.rs`, `afd.rs`).
pub const IOCP_SHARD_DESCRIPTORS: u64 = 2;

/// What one of the runtime's shards holds on this platform ([`EPOLL_SHARD_DESCRIPTORS`],
/// [`KQUEUE_SHARD_DESCRIPTORS`], [`IOCP_SHARD_DESCRIPTORS`]).
pub fn shard_descriptors() -> u64 {
    if cfg!(target_os = "linux") {
        EPOLL_SHARD_DESCRIPTORS
    } else if cfg!(windows) {
        IOCP_SHARD_DESCRIPTORS
    } else {
        KQUEUE_SHARD_DESCRIPTORS
    }
}

/// What a device's issuer holds on Linux: the eventfd its kernel AIO context's completions ring
/// (hyper-block `aio/linux.rs`); the context itself is no descriptor (io_setup(2)). Elsewhere a pool
/// of threads carries its transfers and it holds none.
pub const AIO_ISSUER_DESCRIPTORS: u64 = 1;

/// What the runtime's signal thread reads the signals it is handed from on Unix, a pipe's two ends
/// (hyper-rt `signal.rs`).
pub const SIGNAL_PIPE_DESCRIPTORS: u64 = 2;

/// What a volume holds: its file and its directory's lock, which it keeps (`storage::Locks`,
/// `Storage::open_locked`).
pub const VOLUME_DESCRIPTORS: u64 = 2;

/// What the node directory holds: its lock (`Locks::node`), its readiness lock (`Layout::take_live`)
/// and the log's file (`node::open_log`).
pub const DIRECTORY_DESCRIPTORS: u64 = 3;

/// What a file the node keeps by replacing it whole holds at once (`files::write`): the temporary,
/// written, flushed and closed before the rename, then the directory, flushed after it; one
/// descriptor at a time, one file at a time from the node's task.
pub const KEEPING_DESCRIPTORS: u64 = 1;

/// What a name lookup holds at once, one at a time (`crate::names`): a socket for each name server
/// the resolver asks, at most `MAXNS`, 3 (resolv.conf(5)), and a file it reads (`/etc/hosts`,
/// nsswitch.conf(5)).
pub const LOOKUP_DESCRIPTORS: u64 = 4;

/// The transport's own sockets: its endpoint's listener (hyper-rt-transport `Driver::bind`) and the
/// datagram plane's (hyper-rt-transport `plane.rs`).
pub const TRANSPORT_DESCRIPTORS: u64 = 2;

/// The S3 listener's own beside the connections it serves and answers refused: its listening
/// socket, and the one connection past both budgets it accepts and closes as it comes
/// (`listener::Listener::start`).
pub const LISTENER_DESCRIPTORS: u64 = 2;

/// The descriptors a node holds whatever its operations (node.md §1.4): standard input, output and
/// error; each of its `shards` shards' poller ([`EPOLL_SHARD_DESCRIPTORS`],
/// [`KQUEUE_SHARD_DESCRIPTORS`], [`IOCP_SHARD_DESCRIPTORS`]); the signal thread's pipe on Unix; each
/// of its `volumes` volumes' file and lock, and an issuer of its own on Linux, a volume counted as
/// a device ([`AIO_ISSUER_DESCRIPTORS`]); the node directory's locks and log; a file kept and a
/// name looked up at once; each of its `ranges` ranges' store, one file; the transport's endpoint
/// and plane, and each of its `peers` peers' connections, [`CONNECTIONS_PER_PEER`] of them, each a
/// socket of its own where the platform lets a connected socket share the endpoint's port
/// (hyper-rt-transport's driver, "a socket for each peer"); and, where it serves S3 (`listening`),
/// the listener's own socket, the connections it answers refused ([`crate::listener::REFUSING`])
/// and the one it closes as it comes. `None` past a `u64`.
pub fn fixed_descriptors(
    shards: usize,
    volumes: usize,
    ranges: usize,
    peers: usize,
    listening: bool,
) -> Option<u64> {
    let shard = shard_descriptors();
    let issuer = if cfg!(target_os = "linux") {
        AIO_ISSUER_DESCRIPTORS
    } else {
        0
    };
    let signals = if cfg!(unix) {
        SIGNAL_PIPE_DESCRIPTORS
    } else {
        0
    };
    let listener = if listening {
        LISTENER_DESCRIPTORS.checked_add(crate::listener::REFUSING)?
    } else {
        0
    };
    let peer_sockets = u64::try_from(peers.max(1))
        .ok()?
        .checked_mul(u64::try_from(CONNECTIONS_PER_PEER).ok()?)?;
    STDIO_DESCRIPTORS
        .checked_add(u64::try_from(shards).ok()?.checked_mul(shard)?)?
        .checked_add(signals)?
        .checked_add(
            u64::try_from(volumes)
                .ok()?
                .checked_mul(VOLUME_DESCRIPTORS.checked_add(issuer)?)?,
        )?
        .checked_add(DIRECTORY_DESCRIPTORS)?
        .checked_add(KEEPING_DESCRIPTORS)?
        .checked_add(LOOKUP_DESCRIPTORS)?
        .checked_add(u64::try_from(ranges).ok()?)?
        .checked_add(TRANSPORT_DESCRIPTORS)?
        .checked_add(peer_sockets)?
        .checked_add(listener)
}

/// The descriptors one operation holds at most: its native client's connections, as many as a
/// peer's ([`CONNECTIONS_PER_PEER`]), each a socket of its own ([`fixed_descriptors`]), the node
/// admitting a client for each operation ([`gateway`]); and where the node serves S3
/// (`listening`), the listener's connection, one for each operation (`listener::Settings`).
pub fn operation_descriptors(listening: bool) -> Option<u64> {
    let native = u64::try_from(CONNECTIONS_PER_PEER).ok()?;
    if listening {
        native.checked_add(1)
    } else {
        Some(native)
    }
}

/// The cell's nodes and the gateways a node pins, from its `peers` lines, its `seeds` and its
/// cell's bound on nodes where it states one: the nodes its lines name and itself, or the bound
/// where more; the transport's peers are the nodes but itself and the gateways. `None` past a
/// `usize`.
pub fn cell_lines(
    peers: &[PeerLine],
    seeds: usize,
    enlist: Option<&Enlisting>,
) -> Option<(usize, usize)> {
    let node_lines = peers
        .iter()
        .filter(|p| p.role == Role::Node)
        .count()
        .checked_add(seeds)?
        .checked_add(1)?;
    let cell_nodes = enlist
        .and_then(|e| usize::try_from(e.max_nodes).ok())
        .unwrap_or(0)
        .max(node_lines);
    let gateway_lines = peers.iter().filter(|p| p.role != Role::Node).count();
    Some((cell_nodes, gateway_lines))
}

/// Whether a node of `shards` shards, `volumes` volumes, `ranges` ranges and the peers its `peers`
/// lines, `seeds` and `enlist` make ([`cell_lines`]), serving S3 or not (`listening`), holds its
/// `operations` within `limit` descriptors: the descriptors it holds at its most, or the refusal
/// naming them. `mantle node` runs the numbers it is told, so its start checks them here, as
/// `mantle serve` derives none past them ([`derive`]).
pub fn within_descriptors(
    limit: u64,
    (shards, volumes, ranges): (usize, usize, usize),
    (peers, seeds, enlist): (&[PeerLine], usize, Option<&Enlisting>),
    listening: bool,
    operations: usize,
) -> Result<u64, DeriveError> {
    let overflow = DeriveError::Overflow("the node's descriptors");
    let (cell_nodes, gateway_lines) = cell_lines(peers, seeds, enlist).ok_or(overflow)?;
    let transport_peers = cell_nodes
        .saturating_sub(1)
        .checked_add(gateway_lines)
        .ok_or(DeriveError::Overflow("the transport's peers"))?;
    let fixed = fixed_descriptors(shards, volumes, ranges, transport_peers, listening)
        .ok_or(DeriveError::Overflow("the node's descriptors"))?;
    let each = operation_descriptors(listening)
        .ok_or(DeriveError::Overflow("an operation's descriptors"))?;
    let operations =
        u64::try_from(operations).map_err(|_| DeriveError::Overflow("the operations"))?;
    let held = each
        .checked_mul(operations)
        .and_then(|held| held.checked_add(fixed))
        .ok_or(DeriveError::Overflow("the node's descriptors"))?;
    if held > limit {
        return Err(DeriveError::Descriptors {
            limit,
            fixed,
            each,
            operations,
        });
    }
    Ok(held)
}

/// The operations a process that may hold `limit` descriptors holds past the `fixed` its node
/// holds whatever its operations, each holding `each`.
pub fn descriptor_operations(limit: u64, fixed: u64, each: u64) -> u64 {
    limit.saturating_sub(fixed).checked_div(each).unwrap_or(0)
}

/// The threads a node starts beside the process's own, which waits for it to end: its own, which
/// the process's own leaves at once on a stop that ends it so (`process::start`), and which starts
/// its shards and waits for the node's end on the first (`process::run_on_first_shard`); its
/// standard input's and output's, its log's and its log's device's; its runtime's watchdog, one a
/// runtime (hyper-rt `Runtime::start`, `watch::Watchdog`); its `shards` shards', one for each CPU
/// the process runs at once (`process::runtime`); for each device holding volumes, its
/// issuer (`hyper_block::issuer::Issuer::start_device`), on Linux one thread whatever the device's
/// depth, the kernel holding its transfers, and elsewhere that thread and a worker for each read
/// its profile measured at the device at once (`Issuer::device_threads`); and for each volume its
/// writer and its cleaner, and its scrubber where it scrubs; and its ranges' engines' `workers`
/// maintenance workers, all of them ([`ENGINE_WORKERS`] each). `devices` are those devices'
/// depths, `volumes` whether each scrubs.
pub fn threads(shards: usize, devices: &[usize], volumes: &[bool], workers: usize) -> Option<u64> {
    // The node's own, its standard input's and output's, its log's and its log's device's, and
    // its runtime's watchdog.
    let mut threads = 6u64;
    threads = threads.checked_add(u64::try_from(shards).ok()?)?;
    threads = threads.checked_add(u64::try_from(workers).ok()?)?;
    for &depth in devices {
        let issuer = hyper_block::issuer::Issuer::device_threads(depth)?;
        threads = threads.checked_add(u64::try_from(issuer).ok()?)?;
    }
    for &scrubs in volumes {
        threads = threads.checked_add(if scrubs { 3 } else { 2 })?;
    }
    Some(threads)
}

/// The stacks of `threads` threads, each reserved whole at its start.
pub fn stacks(threads: u64) -> Option<u64> {
    threads.checked_mul(STACK)
}

/// What the node's runtime holds of its own beside its shards: its watchdog's thread and its lists
/// of shards. Measured, as each figure below, by hyper-measure's counting allocator across the
/// process's threads as hyper-rt's `Runtime::start` returns (`tests/shard_memory.rs`, macOS arm64,
/// 2026-11-17, the test profile): 16,058 bytes at one shard of four tasks, four timers, eight
/// waiters and four entries, 15,191 of them the shard's, each shard past it adding as many.
pub const RUNTIME_BYTES: u64 = 867;

/// What each shard holds at no task, timer, waiter or control entry: its registry entry, with the
/// watchdog's board and its table of slow polls (hyper-rt `slow::ENTRIES`, a 4 KiB page), its desk,
/// its driver's buffer of 64 events, its loop and its thread's handle. Measured: 11,859 on macOS
/// arm64, the 15,191 of the shard above less its tasks, timers, waiters and entries at the figures
/// below, the driver's 64 events 32-byte kevents there; Linux's 64 epoll events are 12 or 16 bytes
/// each, so it holds less. And the 16 octets of a group of control bytes Windows' table of
/// outstanding polls takes ([`SHARD_INTEREST_BYTES`]), its 64 events 32-byte `OVERLAPPED_ENTRY`s
/// as macOS's: derived from those structures' sizes, not measured there.
pub const SHARD_BYTES: u64 = 11_875;

/// What each task slot of a shard holds: its slot on the loop, its cell on the desk and its entry
/// in the registry. Measured on macOS arm64: 205 a slot from 5 slots to 257, a few bytes more past
/// (209,116 at 1,024 against 209,100), 205.1 a slot at 4,096; 206 bounds every count measured. No
/// part of it is the driver's, so no platform holds more.
pub const SHARD_TASK_BYTES: u64 = 206;

/// What each timer of a shard holds: its place on the wheel and in the list fired. Measured on
/// macOS arm64: 84 a timer from 5 to 4,096, the same at each. No part of it is the driver's.
pub const SHARD_TIMER_BYTES: u64 = 84;

/// What each readiness waiter of a shard holds beside its table's slots
/// ([`SHARD_INTEREST_SLOT_BYTES`]): its node and free place in the waiters' table (hyper-rt
/// `Interests::new`) and its part of the registry's. Measured on macOS arm64: 208 a waiter, and a
/// bit or so more past 64 (240.12 a waiter with the slots at 4,096, the slots' 32 of it), 209
/// bounding it; kqueue and epoll reserve nothing for a waiter (`Driver::reserve_handles`). Windows'
/// IOCP driver reserves a hashbrown table for two outstanding polls a waiter
/// (`IocpDriver::reserve_handles`): at most 32/7 buckets a waiter, the power of two at or past 8/7
/// of them, each a 40-byte `(i32, Watch)` and a control byte, 187.4 bytes, 188 counted: derived from
/// its sizes, not measured there. 397 in all, the most of any platform.
pub const SHARD_INTEREST_BYTES: u64 = 397;

/// What each slot of a shard's table of waiters' handles holds: 16 bytes, the table the power of two
/// at or past twice the waiters (hyper-rt `Interests::new`), measured on macOS arm64 as the step at
/// each power of two (+32,984 from 1,024 waiters to 1,025, 1,024 more slots and a waiter's 208).
pub const SHARD_INTEREST_SLOT_BYTES: u64 = 16;

/// The slots of a shard's table of waiters' handles the floor charges each waiter: the table is the
/// power of two at or past twice its waiters (hyper-rt `Interests::new`), so it holds fewer than
/// four slots a waiter at every count, and four bounds it. Charged at its power of two instead, the
/// runtime grew by nothing from one S3 connection to two and by a doubled table at each power of
/// two past them, so the operations' count, which charged each operation that difference then
/// (`derive`), passed the floor it was counted for: a node of three shards at 7 GiB counted a floor
/// 190,464 bytes past its allowance, three tables grown from 128 slots to 4,096 at 16 bytes each
/// (CI run 44286448798, macOS x86_64).
pub const SHARD_INTEREST_SLOTS: usize = 4;

/// What each entry of a shard's control queue holds, and the poll of its batch the loop reserves
/// for it, a batch at most as many polls as entries (hyper-rt `RuntimeConfig::calibrate_batch`):
/// measured on macOS arm64, 40 an entry and 24 a poll of the batch, from 5 entries to 1,024.
pub const SHARD_RING_BYTES: u64 = 64;

/// What a shard's queue makes as a task of the node takes the Critical class there: the first's as
/// the node's task does (`process::run_on_first_shard`), and each that runs ranges as their task
/// does (`node::shard::Lanes::start`); hyper-rt `queue::Kept`, made once a shard's first task takes
/// a class other than Normal, and kept: the classes' own state and Critical's lists' besides their
/// slots. Counted on the shard's thread around the spawn, a Critical spawn's count less a Normal
/// one's of the same future (`tests/shard_memory.rs`, macOS arm64, 2026-11-17): 228 at four task
/// slots, 192 less their 36. No other task of the node takes a class, and none of it is the
/// driver's, so no platform holds more.
pub const CRITICAL_BYTES: u64 = 192;

/// What a shard's queue makes for each of its task slots as a task of the node takes the Critical
/// class there: the slot's class, an octet, and its places in Critical's two lists, four octets
/// each. Counted on macOS arm64: 9,216 for 1,024 slots more, 9 a slot.
pub const CRITICAL_SLOT_BYTES: u64 = 9;

/// What the S3 listener's lane to a shard past the first holds besides its connections: the lane,
/// its channel's and count's own state, and its taker's task as it is handed to the shard.
/// Measured on the listener's thread around `Listener::spread` (`tests/shard_memory.rs`, macOS
/// arm64): 2,456 a lane at one connection served, 1,576 less its 11 connections' places. The
/// taker's request is freed on the shard that takes it, so it is counted though it is not held.
pub const LANE_BYTES: u64 = 1_576;

/// What a lane holds for each connection it may hold (`listener::tasks`): a slot of its channel
/// holding a connection's socket and its places in the listener's and the lane's counts, and a
/// place for a sender waiting. Measured on macOS arm64: 80 a connection from 1 to 1,001. Windows'
/// socket is 8 octets where a Unix descriptor is 4, so its slot is at most 8 octets more: 88,
/// derived from the sizes.
pub const LANE_CONNECTION_BYTES: u64 = 88;

/// What each of the node's shards is sized for, hyper-rt sizing every shard alike
/// (`process::runtime`): the largest role, the first's or that of a shard past it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardShape {
    /// The node's task and the launcher that spawns it, and with an S3 listener a task for each
    /// connection it holds at once, served or answered refused (`listener::tasks`); past the first,
    /// where the node spreads its ranges, their task and its launcher, the listener's taker and
    /// those connections.
    pub tasks: usize,
    /// The timers the node's task holds at once (`process::TIMERS`), and one for each of those
    /// connections.
    pub timers: usize,
    /// Two readiness waiters for each task (hyper-rt `interests_for`), or for each socket where
    /// more: the node's endpoint and plane, each peer's connection, and the listener and each of
    /// its connections.
    pub interests: usize,
}

/// The shape of each of the node's shards with `peers` peers, with an S3 listener serving
/// `listening` connections where it has one, its ranges placed and its native requests driven as
/// `ranges` and `native` state ([`ShardShape`]).
pub fn shard_shape(
    listening: Option<u64>,
    peers: usize,
    (ranges, native): (RangeShards, NativeShards),
) -> Option<ShardShape> {
    let connections = match listening {
        Some(served) => usize::try_from(crate::listener::tasks(served)).ok()?,
        None => 0,
    };
    // Whether the shards past the first run ranges (node.md §2.9), and whether they drive native
    // requests (step 6f): each a task, the launcher that spawns it holding its own slot while it
    // spawns (`node::shard::Lanes::start`, `node::driving::Lanes::start`), and the task's wait.
    let ranging = usize::from(ranges == RangeShards::Spread)
        .checked_add(usize::from(native == NativeShards::Spread))?;
    // The first shard: the node's task, the launcher that spawns it, and each connection, which
    // falls back to it where the shard it was handed to refuses it. Each past it: the listener's
    // taker of the connections it is handed, and each of them; and where it runs ranges, their
    // task and the launcher that spawns it Critical (`node::shard::Lanes::start`), holding its own
    // slot while it spawns.
    let first = connections.checked_add(2)?;
    let past = connections
        .checked_add(usize::from(listening.is_some()))?
        .checked_add(ranging.checked_mul(2)?)?;
    let tasks = first.max(past);
    let sockets = peers.checked_add(2)?.checked_add(match listening {
        Some(_) => connections.checked_add(1)?,
        None => 0,
    })?;
    // The first's: those the node's task holds at once; each past it, its ranges' task's wait where
    // it runs ranges, and each connection's.
    let timers = crate::process::TIMERS
        .checked_add(connections)?
        .max(connections.checked_add(ranging)?);
    Some(ShardShape {
        tasks,
        timers,
        interests: hyper_rt::runtime::interests_for(tasks).max(sockets.checked_mul(2)?),
    })
}

/// What the node's runtime holds of its `shards` shards, each of `shape` with `ring` control
/// entries, beside their threads' stacks ([`stacks`]): its own ([`RUNTIME_BYTES`]); each shard's
/// fixed bytes, its tasks', timers', waiters', waiters' table's at its bound
/// ([`SHARD_INTEREST_SLOTS`], so the runtime grows by the same bytes with each S3 connection) and
/// control entries'; the classes a shard's queue makes as a task takes Critical there
/// ([`CRITICAL_BYTES`]), on the first for the node's task and on each of the `ranging` shards past
/// it that run ranges for their task; and where an S3 listener serves `listening` connections, its
/// lane to each shard past the first, each holding as many as the listener holds at once
/// (`listener::tasks`).
pub fn runtime_bytes(
    shards: usize,
    shape: &ShardShape,
    ring: usize,
    listening: Option<u64>,
    ranging: usize,
) -> Option<u64> {
    let count = |n: usize| u64::try_from(n).ok();
    let slots = shape.interests.max(1).checked_mul(SHARD_INTEREST_SLOTS)?;
    let shard = SHARD_BYTES
        .checked_add(SHARD_TASK_BYTES.checked_mul(count(shape.tasks)?)?)?
        .checked_add(SHARD_TIMER_BYTES.checked_mul(count(shape.timers)?)?)?
        .checked_add(SHARD_INTEREST_BYTES.checked_mul(count(shape.interests)?)?)?
        .checked_add(SHARD_INTEREST_SLOT_BYTES.checked_mul(count(slots)?)?)?
        .checked_add(SHARD_RING_BYTES.checked_mul(count(ring)?)?)?;
    let lane = match listening {
        Some(served) => LANE_CONNECTION_BYTES
            .checked_mul(crate::listener::tasks(served))?
            .checked_add(LANE_BYTES)?,
        None => 0,
    };
    // On the first shard and on each past it that runs ranges.
    let critical = CRITICAL_SLOT_BYTES
        .checked_mul(count(shape.tasks)?)?
        .checked_add(CRITICAL_BYTES)?
        .checked_mul(count(ranging)?.checked_add(1)?)?;
    let shards = count(shards)?;
    // The shards past the first, each with a lane: none past a lone shard.
    let lanes = shards.saturating_sub(1).checked_mul(lane)?;
    shards
        .checked_mul(shard)?
        .checked_add(RUNTIME_BYTES)?
        .checked_add(critical)?
        .checked_add(lanes)
}

/// A class of what a node holds at its floor (node.md §1.4), in the order the floor counts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// The objects' put and get, a window of one block ([`objects`]).
    Objects,
    /// Each volume's buffers (`mantle_chunk::layout::most_bytes`).
    Volumes,
    /// Each range's engine's configured buffers (`mantle_engine::shard_db::bounded_bytes`), its
    /// extents' allocator apart: each extent's is charged with its metadata from what remains
    /// (`mantle_engine::shard_db::measured_per_extent`).
    Engines,
    /// Each range's member ([`range_bytes`]).
    Ranges,
    /// The node's shared log ([`log_bytes`]).
    Log,
    /// The transport beside its budget, its plane and its liveness stream
    /// ([`transport_bytes_beside`], [`plane_bytes`], [`liveness_bytes`]), and the piece the node's
    /// task reads native bodies into outside the budget (`node::PIECE`).
    Transport,
    /// The runtime's shards, each sized for the first's role, and the S3 listener's lanes to the
    /// shards past the first ([`runtime_bytes`]).
    Runtime,
    /// The threads the node starts, their stacks reserved whole ([`threads`], [`stacks`]).
    Threads,
}

impl std::fmt::Display for Class {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Objects => "objects",
            Self::Volumes => "volumes",
            Self::Engines => "engines",
            Self::Ranges => "ranges",
            Self::Log => "log",
            Self::Transport => "transport",
            Self::Runtime => "runtime",
            Self::Threads => "threads",
        })
    }
}

/// The memory a node may hold (research 64 §5): the ceiling its operator states; else the limit
/// the OS sets for the process (cgroup v2's `memory.max`, a job's limit, `RLIMIT_AS`), which in an
/// orchestrator is the operator's statement; else the machine's physical memory
/// (`hyper_rt::machine::facts::effective_capacity`). A ceiling past what the OS lets the process
/// hold is refused: the process would be killed, or swapped, before it reached it.
pub fn allowance(stated: Option<u64>, memory: &MemoryFacts) -> Result<u64, DeriveError> {
    let capacity = effective_capacity(memory.total, memory.limit);
    match stated {
        Some(stated) if stated > capacity => Err(DeriveError::Ceiling { stated, capacity }),
        Some(stated) => Ok(stated),
        None => Ok(capacity),
    }
}

/// What a node may hold past its floor within `allowance`: the floor, `classes` each at the most
/// its configuration lets it hold, counted in their order; refused at the class whose bytes take
/// the count past the allowance, the whole floor named beside it ([`DeriveError::Memory`]). What
/// remains is each volume's index's and each engine's resident metadata's and page cache's
/// ([`shares`], [`derive`]'s order).
pub fn floor(classes: &[(Class, u64)], allowance: u64) -> Result<u64, DeriveError> {
    let total = classes
        .iter()
        .try_fold(0u64, |sum, &(_, bytes)| sum.checked_add(bytes))
        .ok_or(DeriveError::Overflow("the floor"))?;
    let mut counted = 0u64;
    for &(class, bytes) in classes {
        counted = counted
            .checked_add(bytes)
            .ok_or(DeriveError::Overflow("the floor"))?;
        if counted > allowance {
            return Err(DeriveError::Memory {
                class,
                floor: total,
                allowance,
            });
        }
    }
    allowance
        .checked_sub(total)
        .ok_or(DeriveError::Overflow("the floor"))
}

/// What `remaining` bytes give `wants`: each its want while they hold them all, and when less
/// remains than all of them want, each the same part of its want, so they fill together: the
/// capacity wants, each volume's index and each engine's resident metadata, from what remains past
/// the floor; the performance wants, each engine's page cache and the transport's growth, from
/// what remains past those and the operations the node serves ([`derive`]).
pub fn shares(remaining: u64, wants: &[u64]) -> Result<Vec<u64>, DeriveError> {
    let overflow = || DeriveError::Overflow("the floor's shares");
    let total = wants
        .iter()
        .try_fold(0u64, |sum, &want| sum.checked_add(want))
        .ok_or_else(overflow)?;
    if total <= remaining {
        return Ok(wants.to_vec());
    }
    wants
        .iter()
        .map(|&want| {
            u128::from(want)
                .checked_mul(u128::from(remaining))
                .and_then(|part| part.checked_div(u128::from(total)))
                .and_then(|share| u64::try_from(share).ok())
                .ok_or_else(overflow)
        })
        .collect()
}

/// What a volume laid out already fixed at format, as its superblock records it: where
/// everything is, and its records' checksum blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Formatted {
    pub geometry: Geometry,
    pub checksum_shift: u8,
}

/// The block, checksum shift and segment of a volume laid out on a device whose transfers align
/// to `align`, holding chunks of `chunk_bytes` at most: the volume's block
/// (`layout::block`); checksum blocks of that block, at most CRC-32C's verified 16 KiB, so a read
/// at the device's alignment verifies no byte it did not ask for (research 11 §8.2); and the least
/// segment holding one record of the largest chunk whole (`layout::least_segment`), since smaller
/// segments clean better (research 11 §7.3–§7.4).
pub fn blocks(align: Alignment, chunk_bytes: u64) -> Result<(u64, u8, u64), DeriveError> {
    let block = volume_layout::block(align).ok_or(DeriveError::Overflow("a volume's block"))?;
    let shift = u8::try_from(block.trailing_zeros())
        .map_err(|_| DeriveError::Overflow("a volume's block"))?
        .min(CHECKSUM_SHIFT_MOST);
    let segment = volume_layout::least_segment(block, shift, chunk_bytes)
        .ok_or(DeriveError::Chunk { bytes: chunk_bytes })?;
    Ok((block, shift, segment))
}

/// A chunk volume of `size` bytes on a device whose transfers align to `align`, measured as
/// `profile`, holding chunks of `chunk_bytes` at most, whose index may ask `index_bytes` of the
/// allocator; `formatted` when it was laid out already, whose choices made at format stand:
///
/// - the block, checksum blocks and segment [`blocks`] gives;
/// - the standard layout, its superblocks 16 MiB apart beyond the reach of clustered latent
///   sector errors, or the compact one where the standard leaves fewer than two segments
///   (chunk-store.md §2);
/// - batches of every chunk waiting that a segment's room holds, each at least a block, within
///   the records a batch's frame holds (`layout::most_batch_requests`), so all of them share one
///   flush (Helland et al., Tandem TR 88.1, §2; research/71, step 9: batches of one record flushed
///   each chunk alone; research/87 §1), and one fragment a chunk, as the node puts chunks whole;
/// - as many fragments as the index's bytes hold (`index::most_fragments`) and the volume holds
///   at a block each, the least a fragment takes standing for the chunk sizes research 11 §6.2
///   learns from, within what the log of a volume laid out already holds checkpoints of;
/// - research 03 §15.9's week a scrub, and the profile's reads and first writes.
pub fn volume(
    align: Alignment,
    size: u64,
    chunk_bytes: u64,
    profile: &Profile,
    index_bytes: u64,
    formatted: Option<&Formatted>,
) -> Result<VolumeConfig, DeriveError> {
    let (block, checksum_shift, segment_size) = match formatted {
        Some(formatted) => (
            formatted.geometry.block,
            formatted.checksum_shift,
            formatted.geometry.segment_size,
        ),
        None => blocks(align, chunk_bytes)?,
    };
    let batch_bytes = segment_size
        .checked_sub(block)
        .and_then(|room| usize::try_from(room).ok())
        .ok_or(DeriveError::Overflow("a batch"))?;
    // A batch carries every chunk waiting that its bytes hold, each at least a block, within what
    // its frame records: no count besides (group commit's rule, research/87 §1).
    let batch_requests = u64::try_from(batch_bytes)
        .ok()
        .and_then(|bytes| bytes.checked_div(block))
        .and_then(|n| usize::try_from(n).ok())
        .zip(volume_layout::most_batch_requests())
        .map(|(bytes, frame)| bytes.min(frame))
        .ok_or(DeriveError::Overflow("a batch"))?;
    let at_a_block = size.checked_div(block).unwrap_or(0);
    let mut config = VolumeConfig {
        segment_size,
        checksum_shift,
        max_fragments: index::most_fragments(index_bytes).min(at_a_block),
        compact: false,
        scrub_period: Some(SCRUB),
        limits: Limits {
            batch_requests: batch_requests.max(1),
            batch_bytes,
            fragments_per_chunk: 1,
        },
        prewrite: profile.prewrite,
        reads: Reads::measured(profile.depth, profile.read_bytes, profile.read_gap),
    };
    if config.max_fragments == 0 {
        return Err(DeriveError::Index { bytes: index_bytes });
    }
    match formatted {
        Some(formatted) => config.max_fragments = held(&formatted.geometry, &config),
        None => {
            if Geometry::plan(size, align, &config).is_err() {
                config.compact = true;
                Geometry::plan(size, align, &config)?;
            }
        }
    }
    config.check()?;
    Ok(config)
}

/// What a volume of `size` bytes on a device whose transfers align to `align` wants of the
/// node's memory for its index at its full size: a fragment at each of its blocks
/// (`index::most_bytes`), its want in [`shares`].
pub fn index_wants(align: Alignment, size: u64) -> Option<u64> {
    let block = volume_layout::block(align)?;
    index::most_bytes(size.checked_div(block)?)
}

/// The most fragments, at most `config`'s, whose checkpoints and `config`'s batches the log of
/// the volume laid out in `geometry` holds: what it was sized for at format bounds them since
/// (`Geometry::holds`). The log needs more as fragments grow, so halving the span between the
/// most that fits and the least that does not finds it, in a step a bit of the span.
fn held(geometry: &Geometry, config: &VolumeConfig) -> u64 {
    let fits = |max_fragments| {
        geometry
            .holds(&VolumeConfig {
                max_fragments,
                ..*config
            })
            .is_ok()
    };
    if fits(config.max_fragments) {
        return config.max_fragments;
    }
    let (mut fit, mut over) = (0u64, config.max_fragments);
    while over.saturating_sub(fit) > 1 {
        let mid = fit.saturating_add(over.saturating_sub(fit) >> 1);
        if fits(mid) {
            fit = mid;
        } else {
            over = mid;
        }
    }
    fit
}

/// A device as the OS names it and its file system holds (`mantle_disk::probe::identify`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
    /// The OS's name for it, by which volumes on one device share its issuer; none where the OS
    /// gives none.
    pub name: Option<String>,
    /// The unit its transfers align to (`mantle_disk::identity::Identity::transfer_alignment`).
    pub align: Alignment,
    /// The commands the OS queues to it, where it says.
    pub queue: Option<u32>,
    /// Bytes its file system holds, where the OS says.
    pub bytes: Option<u64>,
    /// Bytes the OS states this process may still write on its file system, where it says
    /// (`statvfs`'s `f_bavail`, `GetDiskFreeSpaceExW`'s bytes available to the caller).
    pub available: Option<u64>,
}

impl Device {
    /// Whether `other` may be this device: the same name, or a name either lacks, since a device
    /// the OS does not name may be any.
    fn may_be(&self, other: &Device) -> bool {
        match (&self.name, &other.name) {
            (Some(a), Some(b)) => a == b,
            _ => true,
        }
    }
}

/// A data device's facts: the device, its profile, whether that was measured now or kept from a
/// measurement the device still matches, and its volume's layout once laid out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeFacts {
    pub device: Device,
    pub profile: Profile,
    pub measured_now: bool,
    pub formatted: Option<Formatted>,
}

/// What `mantle serve` measures of its host and its devices (node.md §1.4's second column).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Measured {
    /// The machine's memory and the OS's limit for the process (hyper-rt `MemoryFacts`).
    pub memory: MemoryFacts,
    /// The CPUs the process runs at once (hyper-rt `Facts::cpus_at_once`): its shards.
    pub cores: usize,
    /// The entries of each shard's control queue, from the host's calibration (hyper-rt
    /// `RuntimeConfig::from_calibration`: Little's law at its wake's p99 over a syscall's median).
    pub ring: usize,
    /// The OS's base page.
    pub page: u64,
    /// The node directory's device, which holds its log and its ranges' stores.
    pub meta: Device,
    /// Each volume's device, in the volumes' order.
    pub volumes: Vec<VolumeFacts>,
    /// The most descriptors the process may hold open at once, raised at its start and read back
    /// (`crate::descriptors`).
    pub descriptors: u64,
}

/// A volume's bytes as its `volume` line states them (docs/design/membership.md §3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeBytes {
    /// `volume = DIR SIZE`: these.
    Stated(u64),
    /// `volume = DIR spare KEEP`: what its file system spares ([`volume_sizes`]), less `keep`, the
    /// bytes its operator or supervisor states the machine's other work keeps there.
    Spare { keep: u64 },
}

/// What `mantle serve`'s configuration states (node.md §1.4's first column).
#[derive(Debug, Clone, PartialEq)]
pub struct Stated {
    pub id: u64,
    pub dir: PathBuf,
    pub quic: SocketAddr,
    pub plane: SocketAddr,
    pub authority: PathBuf,
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub peers: Vec<PeerLine>,
    /// The members a granted node dials first (docs/design/membership.md §3 step 2).
    pub seeds: Vec<crate::process::SeedLine>,
    /// The host its peers dial it at, where it differs from the address it binds.
    pub reach: Option<String>,
    pub ranges: Vec<(u128, Layer)>,
    pub voters: Vec<u64>,
    /// Each volume's device directory and bytes.
    pub volumes: Vec<(PathBuf, VolumeBytes)>,
    /// For a node with no volume of its own, the bytes each member's volumes hold in its cell, as
    /// its founders state theirs (`cell.volume`, which `mantle init grant` writes from the cell
    /// file): what its engines estimate the cell's rows by.
    pub cell_volume: Option<u64>,
    pub enlist: Option<Enlisting>,
    /// The root key's generations, for a node that hosts objects.
    pub keys: Vec<(u32, PathBuf)>,
    /// The most memory the node may hold, where its operator states it.
    pub ceiling: Option<u64>,
    /// A block's annual chance of loss at most, where its operator states it.
    pub target: Option<f64>,
    /// The cell's repair rate, per hour, where its operator states it: what the durability model
    /// takes, and what the cell's repair holds itself to (docs/design/repair.md §2).
    pub repair: Option<f64>,
    /// The orderly stop's bound, where its operator states it.
    pub stop: Option<Duration>,
    /// The S3 endpoint, where its operator states it (node.md §4).
    pub s3: Option<S3Stated>,
    /// The shards the node places its ranges over, where its operator states them (node.md §2.9).
    pub range_shards: Option<RangeShards>,
    /// The shards the node drives its native requests on, where its operator states them (node.md
    /// §2.9 step 6f).
    pub native_shards: Option<NativeShards>,
}

/// Where a value comes from (node.md §1.4's table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// The operator's.
    Stated,
    /// The host's or a device's, measured.
    Measured,
    /// A model's, from stated and measured values and the code's own bounds.
    Derived,
    /// A source's: a figure the literature, a standard or a measured system gives.
    Cited,
    /// A bound on a measurement.
    Bound,
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Stated => "stated",
            Self::Measured => "measured",
            Self::Derived => "derived",
            Self::Cited => "cited",
            Self::Bound => "bound",
        })
    }
}

/// A value the node runs with and its basis: the key it goes by, `mantle node`'s where it has
/// one, a volume's `volume.N.` with N its place among the volumes from 1, and `memory.` for the
/// floor; what it is; its kind; and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Basis {
    pub key: String,
    pub value: String,
    pub kind: Kind,
    pub why: String,
}

impl std::fmt::Display for Basis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} = {} ({}: {})",
            self.key, self.value, self.kind, self.why
        )
    }
}

/// The configuration `mantle node` runs with, each value with its basis in the order the node
/// says them.
pub struct Derived {
    pub config: ProcessConfig,
    pub bases: Vec<Basis>,
}

#[derive(Default)]
struct Said(Vec<Basis>);

impl Said {
    fn add(
        &mut self,
        key: impl Into<String>,
        value: impl std::fmt::Display,
        kind: Kind,
        why: &str,
    ) {
        self.0.push(Basis {
            key: key.into(),
            value: value.to_string(),
            kind,
            why: why.to_owned(),
        });
    }
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::Node => "node",
        Role::Gateway => "gateway",
        Role::Client => "client",
        Role::Enrolling => "enrolling",
    }
}

fn layer_name(layer: Layer) -> &'static str {
    match layer {
        Layer::Root => "root",
        Layer::Bucket => "bucket",
        Layer::Name => "name",
        Layer::File => "file",
        Layer::Block => "block",
    }
}

/// Every value `mantle node`'s configuration names, from what `stated` states and what
/// `measured` measured, by node.md §1.4's rules, each with its basis:
///
/// - the cell's settings and rules from its identities ([`cell`]), the log from the node
///   directory's device and the ranges ([`log`]);
/// - the objects' floor, a window of one block and the bytes its operations may hold, as many
///   operations as those bytes hold at the least one holds, and their asks under the schemes a
///   block may take in a cell of the volume table's nodes ([`gateway`], [`Charges`], [`schemes`],
///   [`asks`], [`objects`]), for a node that names root keys;
/// - each volume at its device's measured profile ([`volume`]), its index at first a fragment at
///   each of its blocks, and a volume laid out already keeping its layout;
/// - each range's engine at the least page that holds a row at the range's bounds, on its share of
///   the node directory's file system past the volumes on that device and the log's file
///   ([`engine`]), a range's store made already keeping its page and extent sizes;
/// - the transport at its floor (`bounds::least_transport`), for the peers, ranges, transfers and
///   messages the node opens with as `Node::open_on` states them;
/// - the floor, class by class, within the allowance ([`allowance`], [`floor`]), and what remains
///   in rule D's order (research/87 §4): (1) the capacity wants, each volume's index and each
///   engine's resident metadata, whole while what remains holds them, each the same part of its
///   want when less does ([`shares`], [`index_wants`], [`cell_blocks`], [`engine_wants_for`]);
///   (2) the operations the node serves at least ([`served`]); (3) the transport's growth past its
///   floor to a receive chunk each share for the connections those operations open
///   (`bounds::chunked_transport`), whole while what remains past (1) and (2) holds it; (4) every
///   operation more that what remains past those holds, each with its client's windows at the
///   transport's, what they hold beside their charges counted at their count, so a bound the
///   count reaches (a receive's batch at `MAX_BATCH` trains) charges none past it
///   ([`operations`]). What no want takes goes to the engines' metadata, up to their
///   devices' shares: a volume's fragments and an engine's extents as many as their shares hold
///   ([`volume`], [`engine_within`]), an engine never fewer than its store holds, which it would
///   not open below (`Store::open`). An engine's page cache takes no share: its want, every page
///   of its rows at most its store's ([`engine_cache_wants_for`]), is the most it may hold, and
///   the node gives it none until it lends the cache bytes no operation holds.
/// - each engine's maintenance workers ([`ENGINE_WORKERS`]), their stacks among the floor's
///   threads and their cursors, builders and packings among its engines ([`workers_bytes`]),
///   where the allowance holds the node's need with them, said as `memory.need_workers`; else
///   none, each engine packing and compacting on its shard, as at `memory.need`, which every want
///   whole needs: a compaction's statement at the range's longest keys is about 90 MB, and CI's
///   7 GiB macOS runner holds no five of them past a founder's need.
///
/// `stores` reads range `group`'s store as it stands, its superblocks read at the page a new store
/// would take or the one they name (`Store::shape`): none before its first start.
pub fn derive(
    stated: Stated,
    measured: &Measured,
    stores: &mut dyn FnMut(u128, usize) -> Result<Option<Shape>, String>,
) -> Result<Derived, DeriveError> {
    let allowed = allowance(stated.ceiling, &measured.memory)?;
    let (bare, need) = derive_with(stated.clone(), measured, stores, (0, None))?;
    match derive_with(stated, measured, stores, (ENGINE_WORKERS, Some(need))) {
        Ok((derived, needed)) if needed <= allowed => Ok(derived),
        _ => Ok(bare),
    }
}

/// [`derive`] with `workers` maintenance workers each engine, and the node's need with them;
/// `bare` the need without, said as `memory.need` where given.
fn derive_with(
    stated: Stated,
    measured: &Measured,
    stores: &mut dyn FnMut(u128, usize) -> Result<Option<Shape>, String>,
    (workers, bare): (usize, Option<u64>),
) -> Result<(Derived, u64), DeriveError> {
    let overflow = DeriveError::Overflow;
    let mut said = Said::default();
    say_stated(&mut said, &stated);
    said.add(
        "memory.total",
        measured.memory.total,
        Kind::Measured,
        "the machine's physical memory (hyper-rt MemoryFacts::total)",
    );
    said.add(
        "memory.limit",
        measured
            .memory
            .limit
            .map_or_else(|| "none".to_owned(), |limit| limit.to_string()),
        Kind::Measured,
        "the tightest limit the OS sets on the process: cgroup v2's memory.max, a job's limit, \
         RLIMIT_AS (hyper-rt MemoryFacts::limit)",
    );
    said.add(
        "cores",
        measured.cores,
        Kind::Measured,
        "the CPUs the process runs at once, its cores capped by the whole CPUs its CPU budget buys \
         (hyper-rt Facts::cpus_at_once): a shard each; the node runs its ranges on the first and \
         its S3 connections on the rest (node.md §1.2)",
    );
    said.add(
        "ring",
        measured.ring,
        Kind::Measured,
        "the entries of each shard's control queue, from the host's calibration: Little's law at \
         its wake's p99 over a syscall's median (hyper-rt RuntimeConfig::from_calibration); the \
         floor counts each shard's (derive::runtime_bytes)",
    );
    said.add(
        "page",
        measured.page,
        Kind::Measured,
        "the OS's base page (hyper-rt PageFacts::base): the node's talk takes a buffer of one \
         each way",
    );
    said.add(
        "dir.unit",
        measured.meta.align.get(),
        Kind::Measured,
        "the unit the node directory's device transfers in (mantle_disk::probe::identify), the \
         log's block and the engine pages' multiple",
    );
    let dir_bytes = measured.meta.bytes.ok_or(DeriveError::Unsized)?;
    said.add(
        "dir.bytes",
        dir_bytes,
        Kind::Measured,
        "the bytes the node directory's file system holds, shared by its log, the ranges' \
         stores and any volume on its device",
    );

    // The cell's identities: its nodes, as many as it states it holds where it states it, else this
    // one and the nodes its lines name, and the gateways it pins (docs/design/membership.md §9).
    let (cell_nodes, gateway_lines) =
        cell_lines(&stated.peers, stated.seeds.len(), stated.enlist.as_ref())
            .ok_or(overflow("the cell's nodes"))?;
    let identities = cell_nodes
        .checked_add(gateway_lines)
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(overflow("the cell's identities"))?;
    let (settings, rules) = cell(identities)?;
    say_cell(&mut said, &settings, &rules);
    let allowed = allowance(stated.ceiling, &measured.memory)?;

    // The objects a node with root keys hosts, at the floor.
    let hosts = !stated.keys.is_empty();
    let rates = rates_repaired(stated.repair);
    let target = target(stated.target);
    let widest = durability::candidates()
        .iter()
        .map(|s| s.width())
        .max()
        .unwrap_or(0);
    let domains = stated.enlist.as_ref().map_or(widest, |e| {
        usize::try_from(e.max_nodes).unwrap_or(usize::MAX)
    });
    let schemes = if hosts {
        schemes(domains, &rates, target)?
    } else {
        Vec::new()
    };
    // The gateway at no operations, its scan's rows reckoned once: the node is counted at many.
    let idle = gateway(0, 0)?;
    let charges = Charges::new(idle.window, &schemes)?;

    // Each volume as its index's whole want would have it: its buffers, its transfers, its want.
    if stated.volumes.len() != measured.volumes.len() {
        return Err(DeriveError::Measurements {
            stated: stated.volumes.len(),
            measured: measured.volumes.len(),
        });
    }
    // The log's file, which the node directory's file system holds beside the ranges' stores and
    // any volume there, and each volume's bytes, what it spares where stated so.
    let ranges = stated.ranges.len();
    let log_config = log(measured.meta.align, &settings, ranges)?;
    let log_file = log_config
        .segment_bytes
        .checked_mul(u64::from(log_config.max_segments))
        .ok_or(overflow("the log's file"))?;
    let spread = Spread::of(domains, &rates, target)?;
    let sizes = volume_sizes(&stated, measured, log_file, ranges, spread)?;
    say_spared(&mut said, &stated, &sizes);
    let chunk = mantle_gateway::layout::CHUNK;
    let Wholes {
        bytes: volume_bytes,
        transfers,
        fragments,
        wants,
    } = wholes(&sizes, measured, |facts| facts.profile)?;

    // Each range's engine on its share of what the log and the volumes on the node directory's
    // device leave of its file system.
    let mut held = log_file;
    for (size, facts) in sizes.iter().zip(&measured.volumes) {
        if facts.device.may_be(&measured.meta) {
            held = held
                .checked_add(*size)
                .ok_or(overflow("the node directory's device"))?;
        }
    }
    let space = dir_bytes.checked_sub(held).ok_or(DeriveError::Space {
        bytes: dir_bytes,
        held,
    })?;
    let share = u64::try_from(ranges)
        .ok()
        .and_then(|ranges| space.checked_div(ranges))
        .ok_or(overflow("a range's share of the node directory's device"))?;
    let made = engine(measured.meta.align.get(), share, &rules)?;
    let mut shapes = Vec::with_capacity(ranges);
    for &(group, _) in &stated.ranges {
        shapes.push(
            stores(group, made.store.page_size).map_err(|why| DeriveError::Store { group, why })?,
        );
    }
    let (made, held_extents) = founded(made, &shapes, share, &rules, measured.meta.align)?;

    // The transport at its floor, for what the node opens with (`Node::open_on`): each share of its
    // windows at QUIC's initial window (`bounds::least_transport`); at its want past it, a receive
    // chunk (`bounds::chunked_transport`).
    let initial = hyper_transport::initial_window(hyper_transport::MIN_DATAGRAM);
    let chunk_window =
        u64::try_from(hyper_transport::RECEIVE_CHUNK).map_err(|_| overflow("a receive chunk"))?;
    let hosted: Vec<Hosted<()>> = stated
        .ranges
        .iter()
        .map(|&(group, layer)| Hosted {
            group,
            range: Range {
                layer,
                rules,
                boot: ConfState {
                    voters: stated.voters.clone(),
                    ..ConfState::default()
                },
                settings,
                members: stated
                    .enlist
                    .as_ref()
                    .and_then(|e| usize::try_from(e.max_nodes).ok())
                    .unwrap_or(0),
            },
            engine: (),
        })
        .collect();
    let messages = messages()?;
    // The transport's peers: the cell's other nodes and the gateways it pins.
    let transport_peers = cell_nodes
        .saturating_sub(1)
        .checked_add(gateway_lines)
        .ok_or(overflow("the transport's peers"))?;
    // The descriptors: what the node holds whatever its operations, and what each holds beside,
    // within the process's limit (node.md §1.4).
    let listening = hosts && stated.s3.is_some();
    let fixed_held = fixed_descriptors(
        measured.cores,
        stated.volumes.len(),
        ranges,
        transport_peers,
        listening,
    )
    .ok_or(overflow("the node's descriptors"))?;
    let each_held =
        operation_descriptors(listening).ok_or(overflow("an operation's descriptors"))?;
    let by_descriptors = descriptor_operations(measured.descriptors, fixed_held, each_held);
    if measured.descriptors < fixed_held || (hosts && by_descriptors == 0) {
        return Err(DeriveError::Descriptors {
            limit: measured.descriptors,
            fixed: fixed_held,
            each: each_held,
            operations: u64::from(hosts),
        });
    }
    // The shards the node places its ranges over, which its runtime's shards are sized for.
    let (range_shards, range_shards_why) = match stated.range_shards {
        Some(shards) => (shards, Kind::Stated),
        None => (RangeShards::First, Kind::Cited),
    };
    // The shards it drives its native requests on, which its runtime's shards are sized for too.
    let (native_shards, native_shards_why) = match stated.native_shards {
        Some(shards) => (shards, Kind::Stated),
        None => (NativeShards::First, Kind::Cited),
    };
    // The shards it places its ranges over: its first alone, or each of its runtime's, one a CPU
    // (`process::shards`), one where it measured none. A peer opens it a lane for each of them that
    // runs ranges, and each past the first that runs ranges has its two lanes to the node's
    // (`node::lanes`, node.md §2.9): with every range on its first shard, one lane a peer and none
    // between its shards, as `Node::spread` moves no range, whatever its runtime's shards.
    let placed = match range_shards {
        RangeShards::First => NonZeroUsize::MIN,
        RangeShards::Spread => NonZeroUsize::new(measured.cores).unwrap_or(NonZeroUsize::MIN),
    };
    // What `objects` operations at once hold beside their charges, the volumes moving `transfers`
    // at once: their asks, the transport's bounds with a native client each beside the pinned
    // peers (node.md §3.6) drawing its exchanges from a pool of as many, each share of its windows
    // at `window` (`bounds::transport_at`), and each range's reads.
    let held_at = |objects: usize, transfers: usize, window: u64| -> Result<Held, DeriveError> {
        let gateway = idle.running(objects, 0);
        let asked = if hosts {
            asks(&gateway, &schemes)?
        } else {
            Asks {
                chunks: 0,
                commands: usize::from(stated.enlist.is_some()),
                reads: 0,
            }
        };
        let clients = if hosts { gateway.clients } else { 0 };
        let mut bounds = mantle_protocol::bounds::Stated {
            peers: transport_peers,
            // A range's messages go to the cell's other nodes alone (`Node::send`).
            replicas: cell_nodes.saturating_sub(1),
            clients,
            pool: if hosts { objects } else { 0 },
            ranges,
            inflight: settings.max_inflight_msgs,
            // A lane for each shard that runs ranges, of those the node places them over.
            lanes: crate::node::lanes(placed, ranges),
            transfers,
            asked: asked.chunks,
            transport_bytes: 0,
            messages,
            max_head: crate::node::MAX_HEAD,
        };
        let transport_bytes = mantle_protocol::bounds::transport_at(&bounds, window)
            .map_err(|e| DeriveError::Bounds(e.to_string()))?;
        bounds.transport_bytes = transport_bytes;
        let limits = mantle_protocol::bounds::limits(&bounds)
            .map_err(|e| DeriveError::Bounds(e.to_string()))?;
        let beside = transport_bytes_beside(&bounds).ok_or(overflow("the transport"))?;
        let reads = asked
            .reads
            .checked_add(limits.exchanges)
            .ok_or(overflow("a range's reads"))?;
        let mut ranges_bytes = 0u64;
        for hosted in &hosted {
            ranges_bytes = range_bytes(&hosted.range, reads)
                .and_then(|bytes| ranges_bytes.checked_add(bytes))
                .ok_or(overflow("the ranges' members"))?;
        }
        let listener = if hosts && stated.s3.is_some() {
            u64::try_from(objects)
                .ok()
                .zip(crate::listener::CONNECTION_BYTES.checked_add(crate::listener::SESSION_BYTES))
                .and_then(|(n, each)| n.checked_mul(each))
                .ok_or(overflow("the listener's connections"))?
        } else {
            0
        };
        // The S3 listener serves as many connections as the node runs operations, none where it
        // hosts no objects (`process::run_node`).
        let listening = match stated.s3 {
            Some(_) if hosts => Some(u64::try_from(objects).map_err(|_| overflow("the listener"))?),
            Some(_) => Some(0),
            None => None,
        };
        // The shards past the first that run ranges, each with its two lanes to the node's
        // (node.md §2.9), each lane holding the exchanges the transport admits at once, and each
        // running their task Critical (`node::shard::Lanes::start`): none where the node places
        // its ranges on its first shard.
        let far = if placed.get() > 1 {
            crate::node::lanes(placed, ranges)
        } else {
            0
        };
        let lanes = crate::node::shard_lanes_bytes(limits.exchanges)
            .and_then(|pair| u64::try_from(far).ok()?.checked_mul(pair))
            .ok_or(overflow("the ranges' shards' lanes"))?;
        // Where the node drives its native requests past the first (node.md §2.9 step 6f), each
        // shard past it runs a driving task with its three lanes, each holding the exchanges the
        // transport admits at once, the answers' the asks the node holds at once.
        let driving = match native_shards {
            NativeShards::Spread if hosts => measured.cores.saturating_sub(1),
            NativeShards::Spread | NativeShards::First => 0,
        };
        let slots = asked
            .chunks
            .checked_add(asked.commands)
            .and_then(|asks| asks.checked_add(asked.reads))
            .ok_or(overflow("the driving shards' asks"))?;
        let lanes = crate::node::driving_lanes_bytes(limits.exchanges, slots)
            .and_then(|each| u64::try_from(driving).ok()?.checked_mul(each))
            .and_then(|driving| driving.checked_add(lanes))
            .ok_or(overflow("the driving shards' lanes"))?;
        let runtime = shard_shape(listening, stated.peers.len(), (range_shards, native_shards))
            .and_then(|shape| runtime_bytes(measured.cores, &shape, measured.ring, listening, far))
            .and_then(|bytes| bytes.checked_add(lanes))
            .ok_or(overflow("the node's runtime"))?;
        Ok(Held {
            asked,
            bounds,
            beside,
            ranges_bytes,
            listener,
            runtime,
        })
    };
    let held_by = |objects: usize, window: u64| held_at(objects, transfers, window);
    // The floor, class by class. Each engine's configured buffers, and what its maintenance
    // workers hold beside their stacks.
    let all_workers = workers
        .checked_mul(ranges)
        .ok_or(overflow("the engines' workers"))?;
    let engines_bytes = mantle_engine::shard_db::bounded_bytes(
        &StoreConfig {
            max_extents: 0,
            ..made.store
        },
        made.memtable_bytes,
    )
    .zip(workers_bytes(&made, rules.max_key_bytes, workers))
    .and_then(|(each, held)| each.checked_add(held))
    .and_then(|each| each.checked_mul(u64::try_from(ranges).ok()?))
    .ok_or(overflow("the engines' buffers"))?;
    // A device's issuer, one a device the OS names, started by its first volume.
    let depths: Vec<usize> = devices_of(measured, issuer_depth)
        .into_iter()
        .map(|(_, depth)| depth)
        .collect();
    let scrubs = vec![true; stated.volumes.len()];
    let started = threads(measured.cores, &depths, &scrubs, all_workers)
        .ok_or(overflow("the node's threads"))?;
    let log_held = log_bytes(&log_config).ok_or(overflow("the log's memory"))?;
    let stacks_held = stacks(started).ok_or(overflow("the threads' stacks"))?;
    // Every class at its floor: the operations' at `objects_bytes` and what they hold beside in
    // `held`, the volumes' buffers at `volume_bytes` and the threads' stacks at `stacks_held`.
    let classes_of = |held: &Held,
                      objects_bytes: u64,
                      volume_bytes: u64,
                      stacks_held: u64|
     -> Result<[(Class, u64); 8], DeriveError> {
        Ok([
            (Class::Objects, objects_bytes),
            (Class::Volumes, volume_bytes),
            (Class::Engines, engines_bytes),
            (Class::Ranges, held.ranges_bytes),
            (Class::Log, log_held),
            (
                Class::Transport,
                held.bounds
                    .transport_bytes
                    .checked_add(held.beside)
                    .and_then(|bytes| bytes.checked_add(held.listener))
                    // The node's task's one piece it reads every native body into, outside the
                    // transport's budget (`Node::piece`, `node::PIECE`), made once at most a piece.
                    .and_then(|bytes| bytes.checked_add(crate::node::PIECE))
                    .ok_or(overflow("the transport's memory"))?,
            ),
            (Class::Runtime, held.runtime),
            (Class::Threads, stacks_held),
        ])
    };
    let classes_at = |held: &Held, objects_bytes: u64| {
        classes_of(held, objects_bytes, volume_bytes, stacks_held)
    };
    // The operations' first, at the most one is charged ([`operations`]); none where the node
    // hosts no objects.
    let first = if hosts {
        crate::derive::objects(&idle.running(1, most_charged(&charges)), &schemes)?
    } else {
        0
    };
    // What its operations serve at least: a stock client's connections and their parts where the
    // node serves S3 ([`served`]).
    let serving = served(&charges, hosts && stated.s3.is_some());
    // What an orchestrator gives the node: its floor at the most its devices can be measured to
    // take, with as many operations as one stock client asks of an S3 endpoint at once
    // (`listener::STOCK_CLIENT`), the least that never leaves a default client's transfers
    // waiting on a connection the listener does not serve, or the first alone where the node
    // serves no S3 (research/87 §4, rule D: past those the operations hold what the data's wants
    // leave), and every want whole (`need`). With the first alone, a pod given its need and
    // measuring its device's queue ran one operation, its listener served one connection, and
    // boto3's other parts timed out writing their bodies (kind-cell, CI run 43760462414).
    let deepest_floor = {
        let deep = wholes(&sizes, measured, deepest)?;
        let deep_depths: Vec<usize> = devices_of(measured, |facts| {
            hyper_block::issuer::depth(facts.device.queue, Some(deepest(facts).depth))
        })
        .into_iter()
        .map(|(_, depth)| depth)
        .collect();
        let deep_threads = threads(measured.cores, &deep_depths, &scrubs, all_workers)
            .ok_or(overflow("the node's threads"))?;
        let served = if hosts {
            usize::try_from(serving.operations).map_err(|_| overflow("a stock client"))?
        } else {
            0
        };
        // The bytes for what the node serves ([`served`]): with one operation of the largest
        // blocks and the rest at the least one holds, a pod given its need admitted one of
        // boto3's ten 8 MiB parts at a time, and the last waited at the door past its client's
        // patience (kind-cell, CI run 43790965629).
        let served_bytes = if hosts {
            crate::derive::objects(&idle.running(served, serving.bytes), &schemes)?
        } else {
            0
        };
        classes_of(
            &held_at(served, deep.transfers, initial)?,
            served_bytes,
            deep.bytes,
            stacks(deep_threads).ok_or(overflow("the threads' stacks"))?,
        )?
    };
    // What remains, shared: each index wants a fragment at each of its volume's blocks, and each
    // engine the rows of the objects those fragments make across the cell, so both fill together.
    // A node with no volume of its own counts no fragment, yet its ranges hold the cell's rows as
    // every member's do: it counts the fragments of a volume of the bytes its cell's founders state
    // theirs at (`cell.volume`), at its own device's block, each node's volumes holding what a
    // founder's do as a founder estimates them. Wanting nothing, its engines took what the
    // operations' share left, rounding, which on CI's 7 GiB macOS runner was nothing: the node
    // refused to start (`DeriveError::Metadata`) and, given only the two extents an engine opens
    // with, its first writes were refused and its engine fenced (2026-11-19).
    let counted = match (stated.volumes.is_empty(), stated.cell_volume) {
        (true, Some(bytes)) => volume_layout::block(measured.meta.align)
            .and_then(|block| bytes.checked_div(block))
            .ok_or(overflow("the cell's volume"))?,
        _ => fragments,
    };
    let held_objects = cell_blocks(counted, spread)?;
    let engine_want =
        engine_wants_for(&made, held_objects).ok_or(overflow("an engine's metadata"))?;
    // Each engine's page cache wants the pages of those rows: with no cache, every page a get or a
    // lookup needs was a read from the device on the node's busy shard, 100 pages for 100 repeated
    // gets of 1,100 rows and none with a cache. The most it borrows, not a share.
    let cache_want =
        engine_cache_wants_for(&made, held_objects).ok_or(overflow("an engine's cache"))?;
    // The capacity wants (research/87 §4, rule D, the data first): each volume's index and each
    // engine's resident metadata, what bounds the objects the node holds.
    let mut capacity = wants;
    capacity.resize(
        stated
            .volumes
            .len()
            .checked_add(ranges)
            .ok_or(overflow("the floor's shares"))?,
        engine_want,
    );
    let wanted = capacity
        .iter()
        .try_fold(0u64, |sum, &want| sum.checked_add(want))
        .ok_or(overflow("the wants"))?;
    // The need holds the capacity wants and the operations the node serves at least: a node given
    // it starts with every capacity want whole, and the transport's growth and more operations
    // take what a larger allowance leaves.
    let needed = need(&deepest_floor, wanted)?;
    say_need(&mut said, bare.unwrap_or(needed));
    if bare.is_some() {
        said.add(
            "memory.need_workers",
            needed,
            Kind::Derived,
            "memory.need with each engine's maintenance workers, their stacks and their cursors, \
             builders and packings (derive::ENGINE_WORKERS, derive::workers_bytes): a node \
             allowed less runs none, its engines packing and compacting on its shard",
        );
    }
    // The bytes of every class at `held` with `objects_bytes` for the operations.
    let floor_at = |held: &Held, objects_bytes: u64| -> Result<u64, DeriveError> {
        classes_at(held, objects_bytes)?
            .iter()
            .try_fold(0u64, |sum, &(_, bytes)| sum.checked_add(bytes))
            .ok_or(overflow("the floor"))
    };
    // What the transport, the ranges, the listener and the runtime hold at `held`.
    let total = |held: &Held| {
        held.bounds
            .transport_bytes
            .checked_add(held.beside)?
            .checked_add(held.ranges_bytes)?
            .checked_add(held.listener)?
            .checked_add(held.runtime)
    };
    // What one more operation adds to the transport and the ranges with each share of the
    // transport's windows at `window`, the model's own difference, the most one adds; the floor
    // with one operation at the most one is charged; and what that one's transport, ranges,
    // listener and runtime hold.
    let one_at = |window: u64| -> Result<(u64, u64, u64), DeriveError> {
        let one = held_by(1, window)?;
        let held = total(&one).ok_or(overflow("an operation's state"))?;
        let each = total(&held_by(2, window)?)
            .and_then(|two| two.checked_sub(held))
            .ok_or(overflow("an operation's state"))?;
        Ok((each, floor_at(&one, first)?, held))
    };
    // What `n` operations hold beside their charges with each share at `window`, where one holds
    // `one` and `each` is what one more adds: `each` for the first, as the operations' share
    // counts it, and what `n` hold past one, counted at `n` (`held_by`), so a bound the count
    // reaches stops what grows with it. A receive's batch takes a train from each peer and client
    // at once within `MAX_BATCH` trains (`bounds::io`), and a range holds its reads within its
    // `pending_reads` (`range_bytes`). Charged the difference from one operation to two, every
    // operation past the batch's 1,024 trains was charged a train no batch holds, the plane
    // socket's outbound slot (65,592 bytes; `Io::held_bound`), and on Linux the transport's
    // ring's and the plane's 64 KiB buffers and their headers too (197,752;
    // `bounds::ring_growth`), and every one past a range's reads their state: a ladder founder on
    // 96 GiB had 4.42 GB on macOS and 11.10 GB on Linux charged to operations and never held, 3.83
    // GB and 10.59 GB of it trains, which went to its engines' metadata (`derive::tests`).
    let beside_at = |n: u64, window: u64, (one, each): (u64, u64)| -> Option<u64> {
        if n == 0 {
            return Some(0);
        }
        let held = held_by(usize::try_from(n).ok()?, window).ok()?;
        total(&held)?.checked_sub(one)?.checked_add(each)
    };
    // Rule D's order (research/87 §4): (1) the capacity wants, whole while the memory past the
    // floor holds them; (2) the operations the node serves at least ([`served`]), the one the
    // floor holds and the rest at what each adds; (3) the transport's growth past its floor to a
    // receive chunk each share for the connections those operations open, whole while what
    // remains holds it; (4) every operation more that what remains then holds, each with its
    // client's windows at a receive chunk. No page cache takes a share: each engine's borrows the
    // operations' idle bytes (`admit::cache_part`, `node::host::lend`), at most its want,
    // `engine.cache_bytes`.
    let (each, floor_one, one) = if hosts { one_at(initial)? } else { (0, 0, 0) };
    let base = if hosts {
        floor_one
    } else {
        floor_at(&held_by(0, initial)?, 0)?
    };
    let serves = if hosts {
        usize::try_from(serving.operations).map_err(|_| overflow("a stock client"))?
    } else {
        0
    };
    // What the operations served hold past the one the floor holds: their parts' bytes past its
    // charge, and their state past its.
    let served_past = if hosts {
        let most = most_charged(&charges);
        serving
            .bytes
            .max(most)
            .checked_sub(most)
            .zip(
                beside_at(serving.operations, initial, (one, each))
                    .and_then(|state| state.checked_sub(each)),
            )
            .and_then(|(bytes, state)| bytes.checked_add(state))
            .ok_or(overflow("the operations served"))?
    } else {
        0
    };
    let transport_room = allowed
        .saturating_sub(base)
        .saturating_sub(wanted)
        .saturating_sub(served_past);
    // The transport's growth for the connections the served operations open, each share from
    // QUIC's initial window to a receive chunk: its windows and the pool's chunks in its budget,
    // and what QUIC keeps to send beside it, at most a peer's window.
    let growth = total(&held_at(serves, transfers, chunk_window)?)
        .zip(total(&held_at(serves, transfers, initial)?))
        .and_then(|(chunks, floor)| chunks.checked_sub(floor))
        .ok_or(overflow("the transport's growth"))?;
    // (3): the transport's growth, a byte amount whole while what remains holds it. Short of it,
    // the operations are those served, each share's window past QUIC's initial one by the part
    // over the shares (`bounds::limits`); whole, every operation's windows hold a chunk.
    let transport_part = growth.min(transport_room);
    let window = if transport_part == growth {
        chunk_window
    } else {
        initial
    };
    let (objects, objects_bytes, native) = if hosts {
        // (4): what the memory holds past the floor, the capacity wants and the transport's part
        // is the operations', with the one, its state and its charge, each operation adding its
        // client's windows at `window`: a chunk each where (3) is whole, so a datagram each
        // connection receives is cut from a chunk, not copied (10,414 operations where 13,764 at
        // QUIC's initial window on a 16 GiB host, research/87 §4). What they hold beside their
        // charges is counted at their count (`beside_at`).
        let (each, floor_one, one) = if window == initial {
            (each, floor_one, one)
        } else {
            one_at(window)?
        };
        let part = if window == initial { transport_part } else { 0 };
        let slack = allowed
            .saturating_sub(floor_one)
            .saturating_sub(wanted)
            .saturating_sub(part);
        let share = most_charged(&charges)
            .checked_add(each)
            .and_then(|share| share.checked_add(slack))
            .ok_or(overflow("the operations' share"))?;
        let beside = |n: u64| beside_at(n, window, (one, each));
        let (by_memory, _) = operations(share, &beside, &charges, serving, u64::MAX);
        let (objects, bytes) = operations(share, &beside, &charges, serving, by_descriptors);
        let native = native(&charges, each).ok_or(overflow("a native operation's bound"))?;
        say_descriptors(
            &mut said,
            measured.descriptors,
            (fixed_held, each_held),
            by_descriptors,
            objects < by_memory,
        );
        (objects, bytes, Some(native))
    } else {
        say_descriptors(
            &mut said,
            measured.descriptors,
            (fixed_held, each_held),
            0,
            false,
        );
        (0, 0, None)
    };
    let gateway = idle.running(objects, objects_bytes);
    let Held {
        asked,
        mut bounds,
        beside,
        ranges_bytes,
        listener,
        runtime,
    } = held_by(objects, window)?;
    let transport_floor = mantle_protocol::bounds::least_transport(&bounds)
        .map_err(|e| DeriveError::Bounds(e.to_string()))?;
    // At a chunk's windows the budget is the transport's want whole; short of it, its floor and
    // the part (3) gave.
    let transport_bytes = if window == initial {
        transport_floor
            .checked_add(transport_part)
            .ok_or(overflow("the transport's budget"))?
    } else {
        bounds.transport_bytes
    };
    bounds.transport_bytes = transport_bytes;
    let transport_want = mantle_protocol::bounds::chunked_transport(&bounds)
        .ok()
        .and_then(|chunks| chunks.checked_sub(transport_floor))
        .ok_or(overflow("the transport's want"))?;
    let objects_bytes = if hosts {
        crate::derive::objects(&gateway, &schemes)?
    } else {
        0
    };
    say_objects(
        &mut said,
        hosts,
        stated.enlist.is_some(),
        &asked,
        &gateway,
        &rates,
        (target, stated.target.is_some(), stated.repair.is_some()),
    );
    if let Some(native) = native {
        say_native(&mut said, native);
    }

    let classes = classes_at(
        &Held {
            asked,
            bounds,
            beside,
            ranges_bytes,
            listener,
            runtime,
        },
        objects_bytes,
    )?;
    let remaining = floor(&classes, allowed)?;
    say_objects_held(&mut said, fragments, held_objects);
    say_floor(
        &mut said,
        stated.ceiling,
        measured,
        allowed,
        &classes,
        remaining,
    );

    // (1): the capacity wants, each whole while what remains holds them all, else each the same
    // part of its want.
    let parts = shares(remaining, &capacity)?;
    let (indexes, metadata) = parts.split_at(stated.volumes.len());
    let taken = parts
        .iter()
        .try_fold(0u64, |sum, &part| sum.checked_add(part))
        .ok_or(overflow("the floor's shares"))?;
    // What no want takes goes to the engines' metadata, each up to what its device's share holds:
    // the rows no volume bounds, an empty object's, a delete marker's, an upload's in progress.
    let spare = u64::try_from(ranges)
        .ok()
        .and_then(|ranges| remaining.checked_sub(taken)?.checked_div(ranges))
        .unwrap_or(0);
    let most = engine_wants(&made).ok_or(overflow("an engine's metadata"))?;
    let metadata = metadata
        .iter()
        .copied()
        .min()
        .unwrap_or(0)
        .checked_add(spare)
        .ok_or(overflow("an engine's metadata"))?
        .min(most);
    let mut volumes = Vec::with_capacity(stated.volumes.len());
    for (n, ((((dir, _), size), facts), &index_bytes)) in stated
        .volumes
        .iter()
        .zip(&sizes)
        .zip(&measured.volumes)
        .zip(indexes)
        .enumerate()
    {
        let config = volume(
            facts.device.align,
            *size,
            chunk,
            &facts.profile,
            index_bytes,
            facts.formatted.as_ref(),
        )?;
        say_volume(&mut said, n.saturating_add(1), facts, &config, index_bytes);
        volumes.push(VolumeSettings {
            dir: dir.clone(),
            size: *size,
            config,
            depth: issuer_depth(facts),
        });
    }
    let within = engine_within(made, metadata)?;
    let engine = Engine {
        store: StoreConfig {
            max_extents: within.store.max_extents.max(held_extents),
            ..within.store
        },
        ..within
    };
    // The most a cache borrows, its want: no more pages than its store holds, where a want rounded
    // past the extents its metadata's share holds would be a limit no page fills.
    let cache = extent_bytes(&engine)
        .and_then(|extent| extent.checked_mul(engine.store.max_extents))
        .ok_or(overflow("an engine's cache"))?
        .min(cache_want);
    say_engine(
        &mut said,
        &engine,
        (metadata, cache),
        held_extents,
        shapes.iter().any(Option::is_some),
    );
    said.add(
        "engine.workers",
        workers,
        Kind::Derived,
        "the most maintenance workers each range's engine packs and compacts on, so an apply on \
         the node's shard writes none of their pages: one, the least a pool holds, the measured \
         need of all five ranges' maintenance under small PUTs (derive::ENGINE_WORKERS); their \
         stacks among the floor's threads, their cursors, \
         builders and packings among its engines (derive::workers_bytes); none where the \
         allowance does not hold the node's need with them, each engine then packing and \
         compacting on its shard",
    );
    let config = log_config;
    say_log(&mut said, &config);
    say_transport(&mut said, transport_bytes, transport_floor, transport_want);
    // In whole milliseconds, as the key states it, rounded up: a bound on a starved host only.
    let budget_ms = u64::try_from(probe_budget()?.as_nanos().div_ceil(1_000_000))
        .map_err(|_| overflow("the probe budget"))?;
    let probe_budget = Duration::from_millis(budget_ms);
    said.add(
        "runtime.probe_budget_ms",
        budget_ms,
        Kind::Bound,
        "Wilks' 59 samples a probe at the platform's longest wait behind another runnable \
         thread: 3 ms on Linux, 10 ms on macOS, 93.75 ms on Windows (research 64; \
         derive::probe_budget)",
    );
    let (stop, stop_why) = match stated.stop {
        Some(stop) => (stop, Kind::Stated),
        None => (DEFAULT_STOP, Kind::Cited),
    };
    let stop_ms = u64::try_from(stop.as_millis()).map_err(|_| overflow("the stop's bound"))?;
    said.add(
        "stop_ms",
        stop_ms,
        stop_why,
        "the orderly stop's bound, from its first signal: as stated, or docker stop's default grace \
         period for a Linux container, 10 s, the shortest of the orchestrators' (Kubernetes 30 s, \
         systemd 90 s), so a stop ends before their SIGKILL (research 64; node.md §1.7)",
    );
    said.add(
        "range_shards",
        range_shards,
        range_shards_why,
        "the shards the node places its ranges over: as stated, or its first alone, as ladder r40 \
         (lean-doors ec7519e1) measured placement over its shards slower than r28 (54fda898) with \
         every range on the first (node.md §2.9)",
    );
    said.add(
        "native_shards",
        native_shards,
        native_shards_why,
        "the shards the node drives its native requests on: as stated, or its first alone, as every \
         ladder before step 6f measured it, until a ladder measures them spread (node.md §2.9 step \
         6f)",
    );
    let gateway_config = hosts.then(|| GatewayConfig {
        keys: stated.keys.clone(),
        handover_ns: gateway.handover_ns,
        window: gateway.window,
        objects: gateway.objects,
        objects_bytes: gateway.objects_bytes,
        clients: gateway.clients,
        bucket_quota: gateway.bucket_quota,
        scan_rows: gateway.scan_rows,
        chunk_rate: rates.chunk,
        repair_rate: rates.repair,
        bursts: rates.bursts.clone(),
        target,
    });
    Ok((
        Derived {
            config: ProcessConfig {
                id: stated.id,
                dir: stated.dir,
                quic: stated.quic,
                plane: stated.plane,
                authority: stated.authority,
                certificate: stated.certificate,
                key: stated.key,
                peers: stated.peers,
                seeds: stated.seeds,
                reach: stated.reach,
                ranges: stated.ranges,
                voters: stated.voters,
                settings,
                rules,
                log: config,
                store: engine.store,
                memtable_bytes: engine.memtable_bytes,
                cache_bytes: cache,
                trunk: engine.trunk,
                engine_workers: workers,
                transport_bytes,
                probe_budget,
                stop,
                volumes,
                chunk_bytes: chunk,
                chunk_asks: asked.chunks,
                command_asks: asked.commands,
                read_asks: asked.reads,
                enlist: stated.enlist,
                gateway: gateway_config,
                range_shards,
                native_shards,
            },
            bases: said.0,
        },
        needed,
    ))
}

/// The transfers a volume's device's issuer keeps in flight: the smaller of the queue the OS
/// reports and the depth its profile measured (`hyper_block::issuer::depth`).
fn issuer_depth(facts: &VolumeFacts) -> usize {
    hyper_block::issuer::depth(facts.device.queue, Some(facts.profile.depth))
}

/// Each device holding volumes, one a device the OS names and one for each it does not (each
/// such volume's issuer is its own, `Storage::open`), with `depth` of its first volume's facts.
fn devices_of(
    measured: &Measured,
    depth: impl Fn(&VolumeFacts) -> usize,
) -> Vec<(&Option<String>, usize)> {
    let mut devices: Vec<(&Option<String>, usize)> = Vec::new();
    for facts in &measured.volumes {
        let name = &facts.device.name;
        if name.is_none() || !devices.iter().any(|(held, _)| *held == name) {
            devices.push((name, depth(facts)));
        }
    }
    devices
}

/// The volumes, each of its derived bytes (`volume_sizes`) as its index's whole want would have it
/// at the profile `profile` gives its device: their buffers, their transfers, their fragments and
/// each index's want.
struct Wholes {
    bytes: u64,
    transfers: usize,
    fragments: u64,
    wants: Vec<u64>,
}

fn wholes(
    sizes: &[u64],
    measured: &Measured,
    profile: impl Fn(&VolumeFacts) -> Profile,
) -> Result<Wholes, DeriveError> {
    let overflow = DeriveError::Overflow;
    let chunk = mantle_gateway::layout::CHUNK;
    let mut found = Wholes {
        bytes: 0,
        transfers: 0,
        fragments: 0,
        wants: Vec::with_capacity(sizes.len()),
    };
    for (size, facts) in sizes.iter().zip(&measured.volumes) {
        let block = match &facts.formatted {
            Some(formatted) => formatted.geometry.block,
            None => volume_layout::block(facts.device.align).ok_or(overflow("a volume's block"))?,
        };
        let at_a_block = size
            .checked_div(block)
            .ok_or(overflow("a volume's index"))?;
        found.fragments = found
            .fragments
            .checked_add(at_a_block)
            .ok_or(overflow("the volumes' fragments"))?;
        let want = index::most_bytes(at_a_block).ok_or(overflow("a volume's index"))?;
        let whole = volume(
            facts.device.align,
            *size,
            chunk,
            &profile(facts),
            want,
            facts.formatted.as_ref(),
        )?;
        let geometry = match &facts.formatted {
            Some(formatted) => formatted.geometry,
            None => Geometry::plan(*size, facts.device.align, &whole)?,
        };
        found.bytes = volume_layout::most_bytes(&whole, &geometry)
            .and_then(|bytes| found.bytes.checked_add(bytes))
            .ok_or(overflow("the volumes' buffers"))?;
        found.transfers = found
            .transfers
            .checked_add(whole.transfers())
            .ok_or(overflow("the volumes' transfers"))?;
        found.wants.push(want);
    }
    Ok(found)
}

/// The most calibration can measure of `facts`' device, as its profile: its reads at once where the
/// random-read ladder stops at the deepest, the device's reported queue
/// (`issuer::UNDESCRIBED_QUEUE_DEPTH` where it reports none) within the plan's bound, and its
/// reads' bytes at the sequential ladder's deepest point of the plan's large transfers
/// (`mantle_disk::calibrate::Plan::standard`), or what was measured where a profile kept from
/// another plan holds more. A floor taken at it holds whatever a calibration of the device measures.
fn deepest(facts: &VolumeFacts) -> Profile {
    let plan = mantle_disk::calibrate::Plan::standard(facts.device.align, facts.device.queue);
    let queue = facts
        .device
        .queue
        .and_then(|q| usize::try_from(q).ok())
        .filter(|&q| q > 0)
        .unwrap_or(hyper_block::issuer::UNDESCRIBED_QUEUE_DEPTH);
    let sequential = plan.sequential_depths.iter().copied().max().unwrap_or(1);
    let read_bytes = u64::try_from(sequential)
        .ok()
        .and_then(|depth| depth.checked_mul(u64::try_from(plan.large).ok()?))
        .unwrap_or(u64::MAX);
    Profile {
        depth: plan.max_read_depth.min(queue).max(facts.profile.depth),
        read_bytes: read_bytes.max(facts.profile.read_bytes),
        ..facts.profile
    }
}

/// What a node needs to be given to hold its floor and every capacity want whole:
/// `floor_at_deepest`, its floor class by class at the most its devices can be measured to take
/// ([`deepest`]) with the operations it serves ([`served`]), and `wanted`, every volume's index's
/// and engine's metadata's want together ([`shares`]). The performance wants, the engines' caches
/// and the transport's growth, take what a larger allowance leaves past it ([`derive`]).
pub fn need(floor_at_deepest: &[(Class, u64)], wanted: u64) -> Result<u64, DeriveError> {
    floor_at_deepest
        .iter()
        .try_fold(wanted, |sum, &(_, bytes)| sum.checked_add(bytes))
        .ok_or(DeriveError::Overflow("the node's need"))
}

/// `made` on the stores the ranges hold already, each made at one page and extent size it opens
/// only at (`Store::open`): those sizes, which must hold a row at the range's bounds and be a
/// multiple of the device's unit, and as many extents as `share` holds of them; and the most
/// extents a store holds, which the engine never admits fewer than.
fn founded(
    made: Engine,
    stores: &[Option<Shape>],
    share: u64,
    rules: &Rules,
    align: Alignment,
) -> Result<(Engine, u64), DeriveError> {
    let mut found: Option<Shape> = None;
    let mut held = 0u64;
    for shape in stores.iter().flatten() {
        if let Some(first) = found
            && (first.page_size, first.extent_pages) != (shape.page_size, shape.extent_pages)
        {
            return Err(DeriveError::StorePages {
                first: first.page_size,
                second: shape.page_size,
            });
        }
        found = Some(*shape);
        held = held.max(shape.extents);
    }
    let Some(shape) = found else {
        return Ok((made, held));
    };
    if !mantle_range::holds_rows(shape.page_size, made.memtable_bytes, rules)
        || !align.is_aligned(shape.page_size)
        || shape.extent_pages < 2
    {
        return Err(DeriveError::StorePage {
            page: shape.page_size,
            align: align.get(),
        });
    }
    let extent = u64::try_from(shape.page_size)
        .ok()
        .and_then(|page| page.checked_mul(u64::from(shape.extent_pages)))
        .ok_or(DeriveError::Overflow("an extent"))?;
    let max_extents = share.checked_div(extent).unwrap_or(0);
    if max_extents < 2 {
        return Err(DeriveError::Extents { share, extent });
    }
    Ok((
        Engine {
            store: StoreConfig {
                page_size: shape.page_size,
                extent_pages: shape.extent_pages,
                max_extents,
            },
            ..made
        },
        held,
    ))
}

fn say_stated(said: &mut Said, stated: &Stated) {
    said.add(
        "id",
        stated.id,
        Kind::Stated,
        "the node's identity (node.md §1.5)",
    );
    said.add(
        "dir",
        stated.dir.display(),
        Kind::Stated,
        "the node's directory: its log, its run record and its ranges' stores (node.md §1.6)",
    );
    said.add(
        "quic",
        stated.quic,
        Kind::Stated,
        "where the node's QUIC endpoint listens",
    );
    said.add(
        "plane",
        stated.plane,
        Kind::Stated,
        "where the node's datagram plane listens",
    );
    said.add(
        "authority",
        stated.authority.display(),
        Kind::Stated,
        "the cell authority's certificate, which signs every member's (node.md §1.5)",
    );
    said.add(
        "certificate",
        stated.certificate.display(),
        Kind::Stated,
        "the node's certificate, which the cell's authority signed",
    );
    said.add(
        "key",
        stated.key.display(),
        Kind::Stated,
        "the node's private key, PKCS #8 DER",
    );
    for peer in &stated.peers {
        let at = peer
            .at
            .as_ref()
            .map_or_else(String::new, |(quic, plane)| format!(" {quic} {plane}"));
        said.add(
            "peer",
            format!(
                "{} {} {}{at}",
                peer.id,
                role_name(peer.role),
                peer.certificate.display()
            ),
            Kind::Stated,
            "a peer, by the certificate the cell's authority issued it (node.md §3.6)",
        );
    }
    for seed in &stated.seeds {
        let at = seed
            .at
            .as_ref()
            .map_or_else(String::new, |(quic, plane)| format!(" {quic} {plane}"));
        said.add(
            "seed",
            format!("{}{at}", seed.id),
            Kind::Stated,
            "a member a granted node dials first, known by its name under the cell's authority \
             (docs/design/membership.md §3)",
        );
    }
    if let Some(reach) = &stated.reach {
        said.add(
            "reach",
            reach,
            Kind::Stated,
            "the host its peers dial it at on its QUIC and plane ports, which its join states \
             (docs/design/membership.md §3)",
        );
    }
    let voters: Vec<String> = stated.voters.iter().map(u64::to_string).collect();
    said.add(
        "voters",
        voters.join(","),
        Kind::Stated,
        "the founding ranges' voters, drawn with the cell's authority (node.md §1.5)",
    );
    for (group, layer) in &stated.ranges {
        said.add(
            "range",
            format!("{group:x} {}", layer_name(*layer)),
            Kind::Stated,
            "a founding range: its group and the layer whose rows it holds (node.md §1.5)",
        );
    }
    for (generation, file) in &stated.keys {
        said.add(
            "root_key",
            format!("{generation} {}", file.display()),
            Kind::Stated,
            "a generation of the root key, 32 bytes only its owner may read (encryption.md §2)",
        );
    }
    if let Some(enlist) = &stated.enlist {
        said.add(
            "domain",
            &enlist.domain,
            Kind::Stated,
            "the node's failure domain",
        );
        said.add(
            "root.max_nodes",
            enlist.max_nodes,
            Kind::Stated,
            "the cell's bound on its volume table's nodes (architecture §11)",
        );
        said.add(
            "root.max_volumes",
            enlist.max_volumes,
            Kind::Stated,
            "the cell's bound on its volume table's volumes (architecture §11)",
        );
    }
    for (n, (dir, bytes)) in stated.volumes.iter().enumerate() {
        let n = n.saturating_add(1);
        said.add(
            format!("volume.{n}.dir"),
            dir.display(),
            Kind::Stated,
            "a device directory that holds data: the volume is mantle/volume in it",
        );
        match bytes {
            VolumeBytes::Stated(size) => said.add(
                format!("volume.{n}.size"),
                size,
                Kind::Stated,
                "the volume's bytes, laid out at its first start",
            ),
            VolumeBytes::Spare { keep } => said.add(
                format!("volume.{n}.keep"),
                keep,
                Kind::Stated,
                "the bytes the machine's other work keeps on the volume's file system",
            ),
        }
    }
}

/// The bytes of each volume stated to take what its file system spares ([`volume_sizes`]).
fn say_spared(said: &mut Said, stated: &Stated, sizes: &[u64]) {
    for (n, ((_, bytes), size)) in stated.volumes.iter().zip(sizes).enumerate() {
        if matches!(bytes, VolumeBytes::Spare { .. }) {
            said.add(
                format!("volume.{}.size", n.saturating_add(1)),
                size,
                Kind::Derived,
                "what the volume's file system spares: the bytes the OS states the process may \
                 write there, less those its other work keeps and those the node writes there \
                 besides, its laid-out bytes once laid out (membership.md §3.1)",
            );
        }
    }
}

fn say_cell(said: &mut Said, settings: &Settings, rules: &Rules) {
    let ready = "the floor's Ready: an entry of the range's largest (derive::cell)";
    said.add(
        "range.election_tick",
        settings.election_tick,
        Kind::Cited,
        "focal's 10 ticks (node.md §2.4), checked though ranges elect by suspicion, which takes \
         no ticks",
    );
    said.add(
        "range.heartbeat_tick",
        settings.heartbeat_tick,
        Kind::Cited,
        "focal's 2 ticks (node.md §2.4)",
    );
    said.add(
        "range.max_entry_bytes",
        settings.max_entry_bytes,
        Kind::Derived,
        "the range's largest entry (mantle_meta::wire::largest_entry_bytes, replica.md §7)",
    );
    said.add(
        "range.max_size_per_msg",
        settings.max_size_per_msg,
        Kind::Derived,
        ready,
    );
    said.add(
        "range.max_inflight_msgs",
        settings.max_inflight_msgs,
        Kind::Derived,
        "an append out to a follower for each entry a range can hold uncommitted: ask::ENTRIES_OUT \
         an identity of the cell, each asking through as many entries out, and one of the \
         leader's own (derive::in_flight)",
    );
    said.add(
        "range.max_uncommitted_size",
        settings.max_uncommitted_size,
        Kind::Derived,
        "an entry of the range's largest for each append out (derive::cell, derive::in_flight)",
    );
    said.add(
        "range.max_committed_size_per_ready",
        settings.max_committed_size_per_ready,
        Kind::Derived,
        ready,
    );
    said.add(
        "rules.lifetime_ns",
        rules.lifetime_ns,
        Kind::Derived,
        "no session expires by time: a registration ends its owner's last \
         (mantle_meta::session::register, node.md §1.4)",
    );
    said.add(
        "rules.max_sessions",
        rules.max_sessions,
        Kind::Derived,
        "a session for each identity the configuration names, the node's and each peer's, for \
         good",
    );
    said.add(
        "rules.expiries_per_entry",
        rules.expiries_per_entry,
        Kind::Derived,
        "nothing expires by time: the least work an entry's expiry may do",
    );
    said.add(
        "rules.max_key_bytes",
        rules.max_key_bytes,
        Kind::Derived,
        "the longest key S3's names give a row (mantle_meta::key::largest_bytes)",
    );
    said.add(
        "rules.max_value_bytes",
        rules.max_value_bytes,
        Kind::Derived,
        "the largest value a request S3 admits, or a session at its bounds, gives a row \
         (mantle_meta::wire::largest_row_bytes)",
    );
}

fn say_objects(
    said: &mut Said,
    hosts: bool,
    enlists: bool,
    asked: &Asks,
    gateway: &Gateway,
    rates: &Rates,
    (target, stated, repair_stated): (f64, bool, bool),
) {
    said.add(
        "chunk_bytes",
        mantle_gateway::layout::CHUNK,
        Kind::Cited,
        "the largest chunk a gateway writes, Tectonic's 8 MiB (mantle_gateway::layout::CHUNK)",
    );
    let (chunks, commands, reads) = if hosts {
        (
            "the gateway.objects operations, each at its most chunks under the schemes a block \
             may take in a cell of the volume table's nodes (derive::asks)",
            "a put of the largest body's commands for each of the gateway.objects operations, and \
             the node's enlistment (derive::asks)",
            "one read for each of the gateway.objects operations (derive::asks)",
        )
    } else if enlists {
        (
            "none: the node hosts no objects",
            "the node's enlistment of its volumes (node.md §6.6)",
            "none: the node hosts no objects",
        )
    } else {
        (
            "none: the node hosts no objects",
            "none: the node asks no range itself",
            "none: the node hosts no objects",
        )
    };
    said.add("chunk_asks", asked.chunks, Kind::Derived, chunks);
    said.add("command_asks", asked.commands, Kind::Derived, commands);
    said.add("read_asks", asked.reads, Kind::Derived, reads);
    if !hosts {
        return;
    }
    said.add(
        "gateway.window",
        gateway.window,
        Kind::Derived,
        "the floor: a window of one block (node.md §1.4); a put and a get each hold the window the \
         node learns of its own puts and gets by Little's law, a block's time times its clients' \
         pace over a block's bytes, where the operations' bytes hold it (crate::window, node.md \
         §2.8)",
    );
    said.add(
        "gateway.objects_bytes",
        gateway.objects_bytes,
        Kind::Derived,
        "the bytes the operations hold at once, each charged what it holds at its own size: what \
         the memory holds past every class's floor and the indexes' and engines' wants, the data \
         first, and at least one operation of the largest blocks (research/87 §4 rule D; \
         derive::operations)",
    );
    said.add(
        "gateway.objects",
        gateway.objects,
        Kind::Derived,
        "the most operations at once, those bytes at the least an operation holds, a write of no \
         bytes (derive::Charges), with what they hold beside it counted at their count, a \
         receive's batch at most MAX_BATCH trains (derive::operations); within it the node runs \
         as many as it learns its knee is (crate::admit, node.md §2.8)",
    );
    said.add(
        "gateway.clients",
        gateway.clients,
        Kind::Derived,
        "as many clients of the native protocol at once as operations, each with one it may run, \
         admitted by the certificates the cell's authority issued for the client role (node.md \
         §3.6)",
    );
    said.add(
        "gateway.bucket_quota",
        gateway.bucket_quota,
        Kind::Cited,
        "S3's 10,000 buckets an owner (research 05 §10.4)",
    );
    said.add(
        "gateway.scan_rows",
        gateway.scan_rows,
        Kind::Derived,
        "the rows each step of a bucket's attempt passes or removes: as many as the largest entry a \
         range takes holds of what one row a collection removes writes, its key deleted and a \
         part's file released (derive::scan_rows, wire::largest_collected_bytes)",
    );
    said.add(
        "gateway.handover_ns",
        gateway.handover_ns,
        Kind::Cited,
        "HDFS's 60 s soft limit (research 22 §3.6), until the gateway measures research 22 \
         §10.2's D_b; it bounds how long a leftover waits, not correctness",
    );
    said.add(
        "durability.chunk_rate",
        rates.chunk,
        Kind::Cited,
        "the worst device class's annual failure rate, an hour, since a block's chunks may land \
         on any device of the cell (mantle_ec::durability's disk rate; durability.md §5)",
    );
    if repair_stated {
        said.add(
            "durability.repair_rate",
            rates.repair,
            Kind::Stated,
            "the operator's repairs an hour: an away node's chunks wait one over it, less the \
             rebuild's measured time (docs/design/repair.md §2)",
        );
    } else {
        said.add(
            "durability.repair_rate",
            rates.repair,
            Kind::Cited,
            "repair in a day, the longest research 15 §8.4 gives: an away node's chunks wait a day, \
             less the rebuild's measured time (docs/design/repair.md §2)",
        );
    }
    for burst in &rates.bursts {
        said.add(
            "durability.burst",
            format!("{} {}", burst.rate, burst.fraction),
            Kind::Cited,
            "a power-on restart a year, taking a hundredth of the nodes (durability.md §5)",
        );
    }
    if stated {
        said.add(
            "durability.target",
            target,
            Kind::Stated,
            "the operator's annual chance of losing a block at most",
        );
    } else {
        said.add(
            "durability.target",
            target,
            Kind::Cited,
            "S3's eleven nines a year, applied to each block (durability.md §1)",
        );
    }
}

/// The process's descriptor limit, what the node holds whatever its operations and what each holds
/// beside, and the operations the rest holds: `bound` where they bind `gateway.objects`, fewer than
/// the memory holds.
fn say_descriptors(said: &mut Said, limit: u64, (fixed, each): (u64, u64), most: u64, binds: bool) {
    said.add(
        "descriptors.limit",
        limit,
        Kind::Measured,
        "the most descriptors the process may hold open, its soft RLIMIT_NOFILE raised to the hard \
         limit at its start and read back, on macOS at most kern.maxfilesperproc; Windows' 2^24 \
         handles a process (crate::descriptors)",
    );
    said.add(
        "descriptors.fixed",
        fixed,
        Kind::Derived,
        "what the node holds whatever its operations: stdio, each shard's poller, the signal pipe, \
         each volume's file, lock and issuer, the directory's locks and log, a file kept and a \
         name looked up at once, each range's store, the transport's endpoint and plane and two \
         sockets a peer, and the S3 listener's own and its refusals' (derive::fixed_descriptors)",
    );
    said.add(
        "descriptors.each",
        each,
        Kind::Derived,
        "what one operation holds beside: its native client's two connections, a socket each, and \
         its S3 connection where the node serves S3 (derive::operation_descriptors)",
    );
    said.add(
        "descriptors.operations",
        most,
        if binds { Kind::Bound } else { Kind::Derived },
        if binds {
            "the operations the descriptors past the fixed hold, fewer than the memory holds: they \
             bound gateway.objects, the S3 listener's connections and the native clients"
        } else {
            "the operations the descriptors past the fixed hold, none fewer than the memory holds: \
             gateway.objects is the memory's"
        },
    );
}

fn say_native(said: &mut Said, native: u64) {
    said.add(
        "memory.native",
        native,
        Kind::Derived,
        "the most one native PUT or GET holds above the node's idle at gateway.window: the most \
         an operation is charged, its window of blocks at the largest block, a put's chunks copied \
         to send, a piece of its body and its records of the blocks of the largest body a PUT \
         carries (derive::Charges); what one more operation adds beside \
         it to the transport, the ranges, the listener and the runtime; and the host's records of \
         it (derive::native). A put or a get admitted at a window it learned holds that window's \
         blocks instead (Charges::write, Charges::read), at most gateway.objects_bytes",
    );
}

fn say_need(said: &mut Said, need: u64) {
    said.add(
        "memory.need",
        need,
        Kind::Derived,
        "what to give the node, an orchestrator's limit for its pod: its floor at the most its \
         devices can be measured to take, each one's queue (derive::deepest), with as many \
         operations as one stock client asks of its S3 endpoint at once (listener::STOCK_CLIENT; \
         the first alone with no endpoint), and every volume's index and engine's resident \
         metadata whole for the objects the cell's volumes hold, the capacity wants; the engines' \
         page caches and the transport's growth take what a larger allowance leaves \
         (derive::need; research/87 §4 rule D)",
    );
}

fn say_floor(
    said: &mut Said,
    ceiling: Option<u64>,
    measured: &Measured,
    allowed: u64,
    classes: &[(Class, u64)],
    remaining: u64,
) {
    let why = match (ceiling, measured.memory.limit) {
        (Some(_), _) => "the operator's ceiling, within what the OS lets the process hold",
        (None, Some(limit)) if limit < measured.memory.total => {
            "the OS's limit for the process, the operator's statement in an orchestrator \
             (research 64 §5)"
        }
        _ => "the machine's physical memory, with no ceiling stated and no tighter limit set",
    };
    let kind = if ceiling.is_some() {
        Kind::Stated
    } else {
        Kind::Measured
    };
    said.add("memory.allowance", allowed, kind, why);
    for &(class, bytes) in classes {
        said.add(
            format!("memory.floor.{class}"),
            bytes,
            Kind::Derived,
            match class {
                Class::Objects => {
                    "the bytes the operations hold at once, gateway.objects_bytes, and a coding \
                     scheme's tables (derive::objects)"
                }
                Class::Volumes => "each volume's buffers (mantle_chunk::layout::most_bytes)",
                Class::Engines => {
                    "each engine's memtables and store buffers \
                     (mantle_engine::shard_db::bounded_bytes) and its maintenance workers' \
                     cursors, builders and packings (derive::workers_bytes); its extents' from \
                     what remains"
                }
                Class::Ranges => {
                    "each range's member at hyper-raft's limits, its reads the node's own and \
                     its exchanges' (derive::range_bytes)"
                }
                Class::Log => "the node's shared log (derive::log_bytes)",
                Class::Transport => {
                    "the transport's budget and what it holds beside it, its plane and its \
                     liveness stream (derive::transport_bytes_beside), the node's task's piece \
                     it reads native bodies into (node::PIECE), and the S3 listener's \
                     connections, as many as the operations, each at its measured bytes and \
                     its sessions' slots (listener::CONNECTION_BYTES, listener::SESSION_BYTES)"
                }
                Class::Runtime => {
                    "the runtime's shards, one a CPU, each sized for the first's tasks, timers, \
                     waiters and control entries, and the S3 listener's lane to each shard past \
                     the first, each at its measured bytes (derive::runtime_bytes)"
                }
                Class::Threads => {
                    "the threads the node starts, its engines' maintenance workers among them, \
                     each a 2 MiB stack reserved whole (derive::threads)"
                }
            },
        );
    }
    said.add(
        "memory.remaining",
        remaining,
        Kind::Derived,
        "the allowance past the floor, in rule D's order (research/87 §4): each volume's index and \
         each engine's resident metadata, whole while it holds them, each the same part of its \
         want when it does not; the operations the node serves; each engine's page cache and the \
         transport's growth, in proportion to their wants; the operations past those; what no \
         want takes the engines' metadata's (derive::derive, derive::shares)",
    );
}

fn say_volume(
    said: &mut Said,
    n: usize,
    facts: &VolumeFacts,
    config: &VolumeConfig,
    index_bytes: u64,
) {
    let profile = if facts.measured_now {
        "the device's calibration, measured now and kept in its profile (chunk-store.md §7)"
    } else {
        "the device's calibration, kept in its profile, which the device still matches \
         (chunk-store.md §7, node.md §1.6)"
    };
    let p = &facts.profile;
    said.add(
        format!("volume.{n}.depth"),
        p.depth,
        Kind::Measured,
        profile,
    );
    said.add(
        format!("volume.{n}.issuer_depth"),
        issuer_depth(facts),
        Kind::Derived,
        "the transfers the device's issuer keeps in flight: the smaller of the queue the OS \
         reports and the depth measured (hyper_block::issuer::depth)",
    );
    said.add(
        format!("volume.{n}.read_bytes"),
        p.read_bytes,
        Kind::Measured,
        profile,
    );
    said.add(
        format!("volume.{n}.read_gap"),
        p.read_gap,
        Kind::Measured,
        profile,
    );
    said.add(
        format!("volume.{n}.prewrite"),
        u8::from(p.prewrite),
        Kind::Measured,
        profile,
    );
    let laid = facts.formatted.is_some();
    said.add(
        format!("volume.{n}.segment_bytes"),
        config.segment_size,
        Kind::Derived,
        if laid {
            "as laid out: a volume keeps its segments (mantle_chunk::formatted)"
        } else {
            "the least segment holding a record of the largest chunk whole, smaller segments \
             cleaning better (research 11 §7.3–§7.4)"
        },
    );
    said.add(
        format!("volume.{n}.checksum_shift"),
        config.checksum_shift,
        Kind::Derived,
        if laid {
            "as laid out: a volume keeps its checksum blocks (mantle_chunk::formatted)"
        } else {
            "checksum blocks of the volume's block, at most CRC-32C's verified 16 KiB \
             (research 11 §8.2)"
        },
    );
    said.add(
        format!("volume.{n}.index_bytes"),
        index_bytes,
        Kind::Derived,
        "the volume's share of what remains of memory past the floor (derive::shares)",
    );
    said.add(
        format!("volume.{n}.max_fragments"),
        config.max_fragments,
        Kind::Derived,
        "as many fragments as the index's share holds at its measured peak \
         (index::most_fragments) and the volume holds at a block each, within what a laid-out \
         volume's log holds checkpoints of",
    );
    said.add(
        format!("volume.{n}.compact"),
        u8::from(config.compact),
        Kind::Derived,
        "the standard layout, or the compact one where the standard leaves fewer than two \
         segments (chunk-store.md §2)",
    );
    said.add(
        format!("volume.{n}.scrub_period_s"),
        config.scrub_period.map_or(0, |period| period.as_secs()),
        Kind::Cited,
        "a week a scrub, research 03 §15.9's target of the fourteen days it allows",
    );
    said.add(
        format!("volume.{n}.batch_requests"),
        config.limits.batch_requests,
        Kind::Derived,
        "every chunk waiting a segment's room holds at a block each, within the records a \
         batch's frame holds (layout::most_batch_requests; research/87 §1)",
    );
    said.add(
        format!("volume.{n}.batch_bytes"),
        config.limits.batch_bytes,
        Kind::Derived,
        "a segment's room, a record of the largest chunk",
    );
    said.add(
        format!("volume.{n}.fragments_per_chunk"),
        config.limits.fragments_per_chunk,
        Kind::Derived,
        "one fragment a chunk, as the node puts chunks whole",
    );
}

fn say_objects_held(said: &mut Said, fragments: u64, objects: u64) {
    said.add(
        "volumes.fragments",
        fragments,
        Kind::Derived,
        "the chunks the node's volumes hold at most, a fragment at each of their blocks, which \
         their indexes want (derive::index_wants)",
    );
    said.add(
        "engine.objects",
        objects,
        Kind::Derived,
        "the most objects whose rows each range's engine wants room for: the cell's blocks when \
         each node's volumes hold what this node's do, an object a block, each block's chunks on \
         as many nodes as the scheme chosen at the cell's domains, root.max_nodes \
         (derive::cell_blocks)",
    );
}

fn say_engine(
    said: &mut Said,
    engine: &Engine,
    (metadata, cache): (u64, u64),
    held_extents: u64,
    founded: bool,
) {
    said.add(
        "engine.page_size",
        engine.store.page_size,
        Kind::Derived,
        if founded {
            "as the ranges' stores were made: a store opens only at its page (Store::shape)"
        } else {
            "the least page whose branch pages hold a row at the range's bounds, a multiple of \
             the device's unit (mantle_engine::branch::least_page, replica.md §2)"
        },
    );
    said.add(
        "engine.extent_pages",
        engine.store.extent_pages,
        Kind::Derived,
        "the least pages the store takes an extent, the superblocks' two in extent 0",
    );
    said.add(
        "engine.metadata_bytes",
        metadata,
        Kind::Derived,
        "each range's share of what remains of memory past the floor, for its engine's resident \
         metadata: the extents its rows of engine.objects take, a capacity want, whole while what \
         remains holds it, in proportion with each index's want when less remains \
         (derive::engine_wants_for, derive::shares), and what no want takes, up to what its share \
         of the node directory's device holds",
    );
    said.add(
        "engine.max_extents",
        engine.store.max_extents,
        Kind::Derived,
        if held_extents >= engine.store.max_extents && held_extents > 0 {
            "as many extents as a range's store holds already, past what its share holds: a \
             store opens only admitting its extents (Store::open)"
        } else {
            "as many extents as the range's share of memory holds at the metadata measured at the \
             densest rows and its share of the node directory's device holds \
             (derive::engine_within, shard_db::MEASURED_METADATA_PPM)"
        },
    );
    said.add(
        "engine.cache_bytes",
        cache,
        Kind::Derived,
        "the most each range's engine's page cache of verified pages holds, its want: every page \
         of the extents its rows of engine.objects take, with which a get or a lookup reads each \
         page from the device once, at most its store's pages (derive::engine_cache_wants_for). \
         No share of memory: the node opens each engine with no cache (process::engine), and \
         each cache on the node's shard borrows its part of the operations' bytes no running \
         operation holds, at most this, giving it back at once before admission charges an \
         operation it and growing back once a turn (admit::cache_part, node::host::lend; \
         research/87 §4 rule B for caches); none on another shard, none on a node hosting no \
         objects",
    );
    said.add(
        "engine.memtable_bytes",
        engine.memtable_bytes,
        Kind::Derived,
        "the least write buffer, one row at the range's bounds whole \
         (mantle_engine::memtable::hashed::entry_bytes)",
    );
    said.add(
        "engine.fanout",
        engine.trunk.fanout,
        Kind::Cited,
        "SplinterDB's 8, at which the engine's measurements ran (engine-structure.md §5–§6)",
    );
    said.add(
        "engine.leaf_entries",
        engine.trunk.leaf_entries,
        Kind::Derived,
        "every row one memtable gives a leaf, the most a memtable holds being rows of the \
         shortest key and no value (mantle_meta::key::least_bytes)",
    );
}

/// The transport's budget, `bytes`: its `floor` and the part of its `want` past it that what
/// remains past the capacity wants and the operations served gives it ([`derive`]).
fn say_transport(said: &mut Said, bytes: u64, floor: u64, want: u64) {
    said.add(
        "transport.floor",
        floor,
        Kind::Derived,
        "what the transport holds besides QUIC's windows, the connections' state at the most \
         connections it holds (bounds::connections_state) and the receive ring's growth at its \
         most (bounds::ring_growth) among it, and each share at QUIC's initial window, 12,000 \
         bytes, below a receive chunk, so each datagram it receives is copied into a buffer of \
         its own, for the node's peers, ranges, transfers and messages (bounds::least_transport)",
    );
    said.add(
        "transport.want",
        want,
        Kind::Derived,
        "past its floor, each share at a receive chunk, 65,527 bytes: each connection's window \
         grows to a chunk as hyper-transport's auto-tuning finds its path wants it (RFC 9000 §4; \
         Chromium QUIC's QuicFlowController) and each peer's pool holds a chunk for it, so a \
         datagram is cut from a chunk, not copied (bounds::chunked_transport); a performance \
         want, past the capacity wants and the operations the node serves, whole while what \
         remains holds it, and then each operation more costs its client's windows at a chunk \
         (research/87 §4 rule D)",
    );
    said.add(
        "transport_bytes",
        bytes,
        Kind::Derived,
        "the transport's budget: where what remains past the capacity wants and the operations \
         the node serves at least holds those operations' windows at a receive chunk, its want \
         whole, every operation's client's windows at a chunk, each operation more costing them, \
         so each datagram is cut from a chunk, not copied (fewer operations for it: 10,414 where \
         13,764 at QUIC's initial window on a 16 GiB host); short of it, transport.floor at the \
         operations served and what remains, which raises each window past QUIC's initial one by \
         it over the shares (derive::derive, bounds::limits)",
    );
}

fn say_log(said: &mut Said, config: &LogConfig) {
    let why = "hyper_log::Config::derive from the log's block, its largest entry with the shell's \
               bytes, its groups, a group's retention of two cadences and a Ready, and a file \
               holding every group's retention at once (derive::log)";
    said.add(
        "log.segment_bytes",
        config.segment_bytes,
        Kind::Derived,
        why,
    );
    said.add("log.max_segments", config.max_segments, Kind::Derived, why);
    said.add(
        "log.max_groups",
        config.max_groups,
        Kind::Derived,
        "a group for each range and the liveness stream's",
    );
    said.add(
        "log.group_entries",
        config.group_entries,
        Kind::Derived,
        why,
    );
    said.add("log.group_bytes", config.group_bytes, Kind::Derived, why);
    said.add("log.group_cache", config.group_cache, Kind::Derived, why);
    said.add(
        "log.queue_submissions",
        config.queue_submissions,
        Kind::Derived,
        why,
    );
    said.add(
        "log.waits",
        match config.waits {
            hyper_log::Waits::Measured => "measured",
            hyper_log::Waits::Never => "never",
            hyper_log::Waits::Told => "told",
        },
        Kind::Cited,
        "the writer waits for the replicas it just answered while that is expected to lower \
         total latency (raft-log.md §3, hyper-log's Waits::Measured)",
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper_block::sim::SimFile;
    use hyper_log::Log;

    /// Every member derives the same settings and rules from the same identities. The settings
    /// take one `Ready` of the largest entry; the rules keep a session of each identity for
    /// good, admit the rows the session layer writes of itself, and bound a row at what a
    /// session at its bounds and a request S3 admits write.
    #[test]
    fn the_cell_derives_alike_and_its_rules_hold_its_own_rows() {
        let (settings, rules) = cell(7).unwrap();
        assert_eq!(cell(7).unwrap(), (settings, rules));
        let entry = u64::try_from(wire::largest_entry_bytes().unwrap()).unwrap();
        assert_eq!(settings.max_entry_bytes, entry);
        assert_eq!(settings.max_size_per_msg, entry);
        // Two entries out for each of the seven identities, and one of the leader's own.
        assert_eq!(settings.max_inflight_msgs, 15);
        assert_eq!(settings.max_uncommitted_size, entry * 15);
        // A session's row is a row the range already admits, so no page grows, and holds one
        // answer at the largest a node's command gets.
        assert_eq!(rules.max_value_bytes, wire::largest_row_bytes().unwrap());
        let one = wire::largest_answer_bytes().unwrap();
        assert!(Session::row_bytes(1, one).unwrap() <= rules.max_value_bytes);
        assert_eq!(rules.max_sessions, 7);
        assert!(rules.admit_sessions());
        assert!(wire::largest_row_bytes().unwrap() <= rules.max_value_bytes);
        assert_eq!(rules.max_key_bytes, key::largest_bytes());
    }

    /// The engine derived holds a row at the range's bounds, and a page one write unit smaller
    /// or a memtable a byte smaller does not; its extents are the share's, and a share of fewer
    /// than two is refused.
    #[test]
    fn the_engine_holds_a_row_at_the_bounds_and_no_smaller_page_or_memtable_does() {
        let (_, rules) = cell(3).unwrap();
        let block = 4096;
        let e = engine(block, 1 << 30, &rules).unwrap();
        let page = e.store.page_size;
        assert!(mantle_range::holds_rows(page, e.memtable_bytes, &rules));
        assert!(!mantle_range::holds_rows(
            page - block,
            e.memtable_bytes,
            &rules
        ));
        assert!(!mantle_range::holds_rows(
            page,
            e.memtable_bytes - 1,
            &rules
        ));
        let extent = u64::try_from(page).unwrap() * u64::from(EXTENT_PAGES);
        assert_eq!(e.store.max_extents, (1 << 30) / extent);
        assert!(e.trunk.leaf_entries >= 2);
        assert!(matches!(
            engine(block, 2 * extent - 1, &rules),
            Err(DeriveError::Extents { .. })
        ));
        // A device of a larger write unit takes a page of it.
        let wide = engine(64 << 10, 1 << 30, &rules).unwrap();
        assert_eq!(wide.store.page_size % (64 << 10), 0);
    }

    /// The rates are the design's defaults until mantle measures its own: the worst device
    /// class's loss a chunk, a yearly power-on restart taking a hundredth of the nodes, repair in
    /// a day; and the target is S3's unless stated.
    #[test]
    fn the_rates_and_target_are_the_designs_defaults() {
        let r = rates();
        assert_eq!(r.chunk, durability::field::DISK_FAILURES / durability::YEAR);
        assert!(r.chunk > durability::field::FLASH_FAILURES / durability::YEAR);
        assert_eq!(r.repair, 1.0 / 24.0);
        assert_eq!(r.bursts.len(), 1);
        assert_eq!(r.bursts[0].fraction, durability::field::POWER_LOSS_FRACTION);
        assert_eq!(target(None), 1e-11);
        assert_eq!(target(Some(1e-9)), 1e-9);
    }

    /// The probe budget is Wilks' samples each at the platform's longest wait behind another
    /// thread.
    #[test]
    fn the_probe_budget_is_wilks_samples_at_a_quantum_each() {
        let budget = probe_budget().unwrap();
        assert_eq!(budget, QUANTUM * u32::try_from(MIN_SAMPLES).unwrap());
        assert_eq!(MIN_SAMPLES, 59);
    }

    /// A node's asks hold its operations at their most under every scheme a block may take: the
    /// widest put's chunks or the get that adds the most whole chunks to its spans, the put of the
    /// largest body's commands with its file's write beside them, one read each, and its
    /// enlistment.
    #[test]
    fn the_asks_hold_each_operation_at_its_most_under_any_scheme() {
        let g = gateway(8, 0).unwrap();
        let schemes = durability::candidates();
        let a = asks(&g, &schemes).unwrap();
        let chunks = schemes
            .iter()
            .map(|&s| match s {
                durability::Scheme::Copies(n) => n,
                durability::Scheme::Rs(code) => code.width().max(2 * code.data() - 1),
            })
            .max()
            .unwrap();
        assert_eq!(a.chunks, g.objects * chunks);
        // RS(10,4): a get's 19 chunks past the widest put's 15.
        assert_eq!(chunks, 19);
        let blocks = schemes
            .iter()
            .map(|&s| Layout::new(s).unwrap().blocks(mantle_s3::body::MAX_UPLOAD))
            .max()
            .unwrap();
        assert_eq!(
            a.commands,
            g.objects * (usize::try_from(blocks).unwrap() + 1) + 1
        );
        assert_eq!(a.reads, g.objects);
    }

    /// The node's own six threads, its runtime's watchdog among them, and a thread a shard, each
    /// device's issuer and workers, each volume's writer, cleaner and scrubber where it scrubs, and
    /// its engines' maintenance workers, each a 2 MiB stack.
    #[test]
    fn the_threads_are_the_nodes_own_its_shards_and_its_storages() {
        // A device's issuer: one thread on Linux, the kernel holding its depth; elsewhere a worker
        // for each transfer at once besides.
        let issuer = |depth: u64| {
            if cfg!(target_os = "linux") {
                1
            } else {
                depth + 1
            }
        };
        assert_eq!(threads(1, &[], &[], 0), Some(7));
        assert_eq!(threads(8, &[], &[], 0), Some(6 + 8));
        assert_eq!(threads(1, &[2], &[true], 0), Some(7 + issuer(2) + 3));
        assert_eq!(
            threads(4, &[1, 4], &[true, false], 0),
            Some(6 + 4 + issuer(1) + issuer(4) + 3 + 2)
        );
        assert_eq!(threads(18, &[], &[], 15), Some(6 + 18 + 15));
        assert_eq!(stacks(10), Some(20 << 20));
    }

    /// Each shard is sized for the first's role: the node's task and launcher, the timers its task
    /// holds, and with an S3 listener a task, a timer and a socket for each connection it holds at
    /// once, two waiters a socket, their table at four slots a waiter; the runtime holds each shard
    /// at those, the classes the node's Critical task makes on the first, and a lane to each shard
    /// past the first holding as many connections.
    #[test]
    fn the_runtime_holds_each_shard_at_the_firsts_shape_and_a_lane_past_the_first() {
        assert_eq!(
            shard_shape(None, 4, (RangeShards::First, NativeShards::First)),
            Some(ShardShape {
                tasks: 2,
                timers: 3,
                interests: 2 * 6,
            })
        );
        // 256 served and 10 answered refused.
        let shape = shard_shape(Some(256), 4, (RangeShards::First, NativeShards::First)).unwrap();
        assert_eq!(
            shape,
            ShardShape {
                tasks: 268,
                timers: 269,
                interests: 2 * (4 + 2 + 267),
            }
        );
        let shard = SHARD_BYTES
            + 268 * SHARD_TASK_BYTES
            + 269 * SHARD_TIMER_BYTES
            + 546 * SHARD_INTEREST_BYTES
            + 4 * 546 * SHARD_INTEREST_SLOT_BYTES
            + 16 * SHARD_RING_BYTES;
        let lane = LANE_BYTES + 266 * LANE_CONNECTION_BYTES;
        // The node's task Critical on the first shard alone.
        let critical = CRITICAL_BYTES + 268 * CRITICAL_SLOT_BYTES;
        assert_eq!(
            runtime_bytes(1, &shape, 16, Some(256), 0),
            Some(RUNTIME_BYTES + shard + critical)
        );
        assert_eq!(
            runtime_bytes(10, &shape, 16, Some(256), 0),
            Some(RUNTIME_BYTES + 10 * shard + critical + 9 * lane)
        );
        // No listener, no lane.
        let alone = shard_shape(None, 4, (RangeShards::First, NativeShards::First)).unwrap();
        assert_eq!(
            runtime_bytes(10, &alone, 16, None, 0),
            Some(
                RUNTIME_BYTES
                    + CRITICAL_BYTES
                    + 2 * CRITICAL_SLOT_BYTES
                    + 10 * (SHARD_BYTES
                        + 2 * SHARD_TASK_BYTES
                        + 3 * SHARD_TIMER_BYTES
                        + 12 * SHARD_INTEREST_BYTES
                        + 4 * 12 * SHARD_INTEREST_SLOT_BYTES
                        + 16 * SHARD_RING_BYTES)
            )
        );
        assert_eq!(runtime_bytes(usize::MAX, &shape, 16, Some(256), 0), None);
    }

    /// Where the node spreads its ranges, a shard past the first that runs them holds their task
    /// and the launcher that spawns it Critical beside the listener's taker and each connection
    /// handed to it, one task more than the first holds; and the runtime holds the classes their
    /// Critical task makes on each such shard, as the node's makes on the first.
    #[test]
    fn a_shard_that_runs_ranges_holds_their_launcher_and_the_classes_their_critical_task_makes() {
        // 256 served and 10 answered refused, the taker, the ranges' task and its launcher.
        let shape = shard_shape(Some(256), 4, (RangeShards::Spread, NativeShards::First)).unwrap();
        assert_eq!(
            shape,
            ShardShape {
                tasks: 269,
                timers: 269,
                interests: 2 * (4 + 2 + 267),
            }
        );
        // No listener: the first's two, the node's task and its launcher, as the ranges' two.
        assert_eq!(
            shard_shape(None, 4, (RangeShards::Spread, NativeShards::First)),
            shard_shape(None, 4, (RangeShards::First, NativeShards::First))
        );
        let shard = SHARD_BYTES
            + 269 * SHARD_TASK_BYTES
            + 269 * SHARD_TIMER_BYTES
            + 546 * SHARD_INTEREST_BYTES
            + 4 * 546 * SHARD_INTEREST_SLOT_BYTES
            + 16 * SHARD_RING_BYTES;
        let lane = LANE_BYTES + 266 * LANE_CONNECTION_BYTES;
        let critical = CRITICAL_BYTES + 269 * CRITICAL_SLOT_BYTES;
        // The first shard's and each of the nine past it that run ranges.
        assert_eq!(
            runtime_bytes(10, &shape, 16, Some(256), 9),
            Some(RUNTIME_BYTES + 10 * shard + 10 * critical + 9 * lane)
        );
        // Three ranges over ten shards run on three past the first.
        assert_eq!(
            runtime_bytes(10, &shape, 16, Some(256), 3),
            Some(RUNTIME_BYTES + 10 * shard + 4 * critical + 9 * lane)
        );
        assert_eq!(runtime_bytes(10, &shape, 16, Some(256), usize::MAX), None);
    }

    /// A shard past the first that drives native requests (node.md §2.9 step 6f) holds its driving
    /// task and the launcher that spawns it, holding its own slot while it spawns, and the task's
    /// timer, beside its ranges' where they are spread too; its lanes hold the exchanges the
    /// transport admits at once each way and the node's asks back.
    #[test]
    fn a_shard_that_drives_native_requests_holds_their_task_and_its_launcher() {
        // 256 served and 10 answered refused, the taker, and the driving task and its launcher.
        let driving = shard_shape(Some(256), 4, (RangeShards::First, NativeShards::Spread));
        let ranging = shard_shape(Some(256), 4, (RangeShards::Spread, NativeShards::First));
        assert_eq!(driving, ranging);
        let both = shard_shape(Some(256), 4, (RangeShards::Spread, NativeShards::Spread)).unwrap();
        assert_eq!((both.tasks, both.timers), (271, 269));
        // No listener: past the first, two tasks and their launchers, the first's two less.
        let alone = shard_shape(None, 4, (RangeShards::Spread, NativeShards::Spread)).unwrap();
        assert_eq!((alone.tasks, alone.timers), (4, crate::process::TIMERS));
        // Each lane's room, two lanes of exchanges and one of answers, at least a value each.
        let lanes = crate::node::driving_lanes_bytes(8, 3).unwrap();
        assert!(lanes > crate::node::driving_lanes_bytes(4, 3).unwrap());
        assert!(lanes > crate::node::driving_lanes_bytes(8, 2).unwrap());
        assert_eq!(crate::node::driving_lanes_bytes(usize::MAX, 3), None);
    }

    /// The runtime grows by the same bytes with each S3 connection, at every count, shard and
    /// placement of the ranges: the operations' count charges each operation the difference from
    /// one to two (`derive`), so a step past it is a floor the count passes. Charged at its power
    /// of two, each shard's table of waiters' handles grew by nothing from one connection to two,
    /// then doubled; a joiner of three shards at 7 GiB counted a floor 190,464 bytes past its
    /// allowance, three tables grown from 128 slots to 4,096 (CI run 44286448798, macOS x86_64).
    #[test]
    fn the_runtime_grows_by_the_same_bytes_with_each_connection() {
        for shards in [1usize, 3, 4, 10] {
            for peers in [2usize, 8] {
                for ranges in [RangeShards::First, RangeShards::Spread] {
                    let ranging = match ranges {
                        RangeShards::First => 0,
                        RangeShards::Spread => shards - 1,
                    };
                    let at = |served: u64| {
                        let shape = shard_shape(Some(served), peers, (ranges, NativeShards::First))
                            .unwrap();
                        runtime_bytes(shards, &shape, 16, Some(served), ranging).unwrap()
                    };
                    let each = at(2) - at(1);
                    // Either side of each power of two the old table doubled at, to past what a
                    // node of 16 GiB serves.
                    for served in [3u64, 53, 54, 117, 118, 501, 502, 1013, 1014, 3004, 16_374] {
                        assert_eq!(
                            at(served) - at(1),
                            (served - 1) * each,
                            "{shards} shards, {peers} peers, {ranges:?}, {served} served"
                        );
                    }
                }
            }
        }
    }

    /// A node on the runners CI's membership suite runs on, S3 served and no volume of its own as
    /// that suite's joiner has, at their cores and memory: three on the macOS runner's 7 GiB, four on
    /// Windows' 16 GiB. Its listener serves thousands of connections, past the two at once that
    /// suite's load and reads open, so its door answers none of them `503 SlowDown` for want of a
    /// slot, and its floor, counted at the operations it derived, stays within its allowance. It is
    /// granted its cell's volume, as `mantle init grant` writes the suite's (`cell.volume`), so
    /// its engines want the cell's rows. Stating none, they want nothing and hold what the
    /// operations leave: nothing, now that each operation is charged what it holds, and the node
    /// refuses (`DeriveError::Metadata`), as it did on the macOS runner before `cell.volume`
    /// (node.md §1.4); it had started here only on the trains past the receive batch's cap each
    /// operation was charged and no batch holds, which its engines took.
    #[test]
    fn a_joiner_on_a_ci_runner_serves_thousands_of_connections_within_its_allowance() {
        let joiner = || Stated {
            s3: Some(crate::process::S3Stated {
                listen: "127.0.0.1:7480".parse().unwrap(),
                region: "us-east-1".into(),
                names: Vec::new(),
                certificate: at("/node/s3/cert.der"),
                key: at("/node/s3/key.der"),
                access: Vec::new(),
            }),
            keys: vec![(1, at("/node/keys/1"))],
            enlist: Some(Enlisting {
                root: 0x10,
                domain: "light".into(),
                max_nodes: 5,
                max_volumes: 5,
            }),
            cell_volume: Some(256 << 20),
            ..stated(0)
        };
        for (cores, memory) in [(3usize, 7u64 << 30), (4, 16 << 30)] {
            let mut measured = facts(0);
            measured.cores = cores;
            measured.memory = machine(memory, None);
            let derived = derive(joiner(), &measured, &mut fresh).unwrap();
            let floor: u64 = [
                "objects",
                "volumes",
                "engines",
                "ranges",
                "log",
                "transport",
                "runtime",
                "threads",
            ]
            .iter()
            .map(|class| said_bytes(&derived, &format!("memory.floor.{class}")))
            .sum();
            let connections = said_bytes(&derived, "gateway.objects");
            eprintln!(
                "{cores} cores, {memory} bytes: {connections} connections, floor {floor}, {} past it",
                memory - floor
            );
            assert!(connections >= 1_000, "{cores} cores: {connections}");
            assert!(floor <= memory, "{cores} cores: {floor}");
        }
    }

    /// A range's member holds more for each read it is asked, up to the core's bound on reads,
    /// and for each member its configuration names; a range whose settings this machine cannot
    /// address has no bound.
    #[test]
    fn a_ranges_member_holds_its_queues_at_their_bounds() {
        let (settings, rules) = cell(3).unwrap();
        let range = |voters: u64| Range {
            layer: mantle_meta::apply::Layer::Name,
            rules,
            boot: mantle_range::ConfState {
                voters: (1..=voters).collect(),
                ..mantle_range::ConfState::default()
            },
            settings,
            members: 0,
        };
        let three = range(3);
        let reads = mantle_range::limits(&three).unwrap().pending_reads;
        let none = range_bytes(&three, 0).unwrap();
        assert!(range_bytes(&three, 1).unwrap() > none);
        assert_eq!(range_bytes(&three, usize::MAX), range_bytes(&three, reads));
        assert!(range_bytes(&range(5), 0).unwrap() > none);
        let unfit = Range {
            settings: Settings {
                max_entry_bytes: u64::MAX,
                ..settings
            },
            ..three
        };
        assert_eq!(range_bytes(&unfit, 0), None);
    }

    /// The liveness stream keeps a ring a peer and two of its own, at the longest window; the
    /// plane its datagram, and each peer's room, state and windows; the transport beside its
    /// budget grows with the peers a node keeps and the windows its budget funds.
    #[test]
    fn the_transport_beside_its_budget_counts_its_peers_and_windows() {
        let ring = (hyper_timing::WINDOW_LIMIT + 1) * 8;
        assert_eq!(liveness_bytes(0), Some(3 * ring));
        assert_eq!(liveness_bytes(4), Some(6 * ring));
        let limits = hyper_datagram::PlaneLimits {
            max_peers: 2,
            epochs_per_peer: 2,
            window_limit: 64,
        };
        let datagram = hyper_datagram::MAX_DATAGRAM_BYTES as u64;
        assert_eq!(
            plane_bytes(&limits),
            Some(datagram + 2 * (datagram + PLANE_PEER_BYTES + 2 * 8))
        );
        let (settings, _) = cell(3).unwrap();
        let stated = |peers: usize, transport_bytes: u64| mantle_protocol::bounds::Stated {
            peers,
            replicas: peers,
            clients: 0,
            pool: 0,
            ranges: 5,
            inflight: settings.max_inflight_msgs,
            lanes: 1,
            transfers: 38,
            asked: 0,
            transport_bytes,
            messages: mantle_protocol::classes::Bounds {
                replication: settings.max_entry_bytes + 1_024,
                request: settings.max_entry_bytes + 1_024,
                transfer: u64::from(u32::MAX),
                bulk: u64::from(u32::MAX),
            },
            max_head: crate::node::MAX_HEAD,
        };
        let least = |peers| mantle_protocol::bounds::least_transport(&stated(peers, 0)).unwrap();
        let one = transport_bytes_beside(&stated(1, least(1))).unwrap();
        assert!(one > mantle_protocol::bounds::CERTIFICATE_COMPRESSION_BYTES);
        assert!(transport_bytes_beside(&stated(2, least(2))).unwrap() > one);
        assert!(transport_bytes_beside(&stated(1, 8 * least(1))).unwrap() > one);
        assert_eq!(transport_bytes_beside(&stated(1, least(1) - 1)), None);
        // The transport's floor counts the driver's receive ring at its most past its first
        // buffer, which the driver charges to the budget as the ring grows (`bounds::ring_growth`):
        // on Linux a 64 KiB buffer for each peer past the first, a train from each a receive call
        // takes (`bounds::io`), none elsewhere; a budget that leaves the ring less is refused.
        for peers in [1usize, 2, 9] {
            let ring = mantle_protocol::bounds::ring_growth(&stated(peers, 0)).unwrap();
            let buffers = if cfg!(target_os = "linux") { peers } else { 1 };
            assert_eq!(
                ring,
                (buffers as u64 - 1) * hyper_rt_transport::RECEIVE_BYTES as u64
            );
            assert!(mantle_protocol::bounds::limits(&stated(peers, least(peers))).is_ok());
            if ring > 0 {
                let short = stated(peers, least(peers) - ring);
                assert!(
                    mantle_protocol::bounds::limits(&short).is_err(),
                    "{peers} peers"
                );
            }
            // And the connections' state, which the endpoint charges to the budget as each
            // connection is made: hyper-transport's `state_charge` for each connection admission
            // holds and each handshake it lets wait beside them, three a peer; a budget that
            // leaves it less is refused.
            let floor = mantle_protocol::bounds::limits(&stated(peers, least(peers))).unwrap();
            let state = mantle_protocol::bounds::connections_state(&stated(peers, 0)).unwrap();
            assert_eq!(
                state,
                3 * peers as u64
                    * hyper_transport::state_charge::<mantle_protocol::classes::NodeClasses>(
                        &floor
                    )
            );
            let short = stated(peers, least(peers) - state);
            assert!(
                mantle_protocol::bounds::limits(&short).is_err(),
                "{peers} peers"
            );
        }
    }

    /// A receive's batch takes a datagram train from each peer and client at once, within
    /// `MAX_BATCH` trains (`bounds::io`, `UIO_MAXIOV`): the plane's socket holds an outbound slot
    /// for each, a 64 KiB datagram and its address (hyper-rt `Io::held_bound`), and on Linux, where
    /// a receive takes a batch (recvmmsg(2) with `UDP_GRO`), the transport's receive ring and the
    /// plane's socket a 64 KiB buffer for each besides, with their messages' headers
    /// (`bounds::ring_growth`, `Ring::bytes_for`). A client adds a train only while the batch has
    /// room for it; past its 1,024 trains it adds its connections, windows and exchanges alone, a
    /// train less: 65,592 bytes on macOS and Windows, 197,752 on Linux. Charged what a client adds
    /// below the cap, as derive charged every operation the difference from one to two
    /// (c1589e30), a node of three peers and 60,000 clients is charged a train for each of the
    /// 58,979 past the cap, which no batch holds: 3.87 GB on macOS, 11.66 GB on Linux.
    #[test]
    fn a_receive_batch_holds_a_train_a_client_to_its_cap_and_no_more() {
        use hyper_rt::udp::{Io, MAX_BATCH, RECEIVE_BYTES, Ring, Slot};
        use mantle_protocol::bounds;
        let (settings, _) = cell(3).unwrap();
        let messages = messages().unwrap();
        // The trains a node of three peers and `clients` clients batches, and what its transport
        // holds in its budget and beside it, each share at a receive chunk.
        let held = |clients: usize| {
            let mut stated = bounds::Stated {
                peers: 3,
                replicas: 2,
                clients,
                pool: clients,
                ranges: 5,
                inflight: settings.max_inflight_msgs,
                lanes: 1,
                transfers: 38,
                asked: 0,
                transport_bytes: 0,
                messages,
                max_head: crate::node::MAX_HEAD,
            };
            stated.transport_bytes = bounds::chunked_transport(&stated).unwrap();
            let bytes = stated.transport_bytes + transport_bytes_beside(&stated).unwrap();
            (bounds::io(&stated).batch, bytes)
        };
        // A train, as hyper-rt states what the ring and the socket that hold it hold.
        let bytes = |n: usize| u64::try_from(n).unwrap();
        let ring = |batch: usize| bytes(Ring::bytes_for(Io { batch }, batch));
        let socket = |batch: usize| bytes(Io { batch }.held_bound());
        let train =
            ring(MAX_BATCH) - ring(MAX_BATCH - 1) + socket(MAX_BATCH) - socket(MAX_BATCH - 1);
        let slot = bytes(RECEIVE_BYTES + size_of::<Slot>());
        assert_eq!(slot, 65_592);
        // On Linux, besides the slot: the ring's buffer and the socket's, and their headers, the
        // ring's place for a buffer (24 bytes), a receive's header and its account (384) and the
        // socket's send's and receive's headers and the account (680; glibc and musl on aarch64
        // and x86_64).
        let linux = slot + 2 * bytes(RECEIVE_BYTES) + 24 + 384 + 680;
        assert_eq!(linux, 197_752);
        assert_eq!(
            train,
            if cfg!(target_os = "linux") {
                linux
            } else {
                slot
            }
        );
        // The clients whose trains fill the batch beside the three peers'.
        let full = MAX_BATCH - 3;
        let (short, below) = held(full - 1);
        let (filled, at) = held(full);
        let (past, beyond) = held(full + 1);
        assert_eq!((short, filled, past), (MAX_BATCH - 1, MAX_BATCH, MAX_BATCH));
        // What one client more adds below the cap and past it: a train apart.
        let (below, beyond) = (at - below, beyond - at);
        assert_eq!(below - beyond, train);
        // Past the cap the batch stays, and each client more adds what one adds past it.
        let many = 60_000;
        let (batch, most) = held(many);
        assert_eq!(batch, MAX_BATCH);
        let more = bytes(many - full);
        assert_eq!(most - at, more * beyond);
        // Charged at what one adds below the cap, they are charged a train each that no batch
        // holds.
        assert_eq!(more * below - (most - at), more * train);
    }

    /// The S3 endpoint a test node states.
    fn s3() -> Option<crate::process::S3Stated> {
        Some(crate::process::S3Stated {
            listen: "127.0.0.1:7480".parse().unwrap(),
            region: "us-east-1".into(),
            names: Vec::new(),
            certificate: at("/node/s3/cert.der"),
            key: at("/node/s3/key.der"),
            access: Vec::new(),
        })
    }

    /// The transport's budget `derived` says: its floor, its want past it, and what it is given.
    fn transport(derived: &Derived) -> (u64, u64, u64) {
        let bytes = said_bytes(derived, "transport_bytes");
        assert_eq!(bytes, derived.config.transport_bytes);
        (
            said_bytes(derived, "transport.floor"),
            said_bytes(derived, "transport.want"),
            bytes,
        )
    }

    /// The operations a node derived on this platform runs: `linux` on Linux, `elsewhere` on macOS
    /// and Windows. Each operation holds 133,520 bytes more on Linux while the receive's batch
    /// holds its client's train: a receive there takes a datagram train from each peer and client
    /// at once (recvmmsg(2) with `UDP_GRO`, `bounds::io`), so the transport's receive ring and the
    /// plane's socket each hold a buffer of 64 KiB for it (hyper-rt `RECEIVE_BYTES`;
    /// `bounds::ring_growth`, `Io::held_bound`), 131,072 bytes, and the headers of its messages,
    /// 2,448; a receive elsewhere takes one datagram into one buffer. Read as `memory.native`, an
    /// operation's charge, what one more adds beside it and its records: 133,520 bytes apart on
    /// Linux and macOS in every node these tests derive (c1589e30). Past the batch's 1,024 trains
    /// an operation holds no train on either, and 1,360 bytes more on Linux, the headers of its
    /// client's two connections' sockets
    /// ([`a_receive_batch_holds_a_train_a_client_to_its_cap_and_no_more`]).
    fn runs(linux: std::ops::Range<u64>, elsewhere: std::ops::Range<u64>) -> std::ops::Range<u64> {
        if cfg!(target_os = "linux") {
            linux
        } else {
            elsewhere
        }
    }

    /// A founder of the ladder's cell (three founders, each a 32 GiB volume and its S3 endpoint)
    /// on the Mac Studio's 96 GiB and 32 cores gives its transport its want whole: every share,
    /// each operation's client's among them, at a receive chunk, so each datagram is cut from a
    /// chunk, not copied. Each operation so costs its client's windows at a chunk, 10.04 GB past
    /// the transport's floor on macOS; the need is 36ad012b's, which holds the capacity wants and
    /// the operations served, and the piece the node's task reads every native body into, which
    /// step 6 counts in the transport's floor (`node::PIECE`, node.md §2.9 step 6c). What its
    /// operations hold beside their charges is counted at their count: it runs 62,502 on macOS and
    /// 62,354 on Linux ([`runs`]), and every byte past its capacity wants is its operations', so
    /// each engine's metadata is its want. Charged the difference from one operation to two, it
    /// ran 59,412 and 54,591 (c1589e30): each of the 58,391 and 53,570 past the receive batch's
    /// 1,024 trains was charged a train no batch holds, 65,592 bytes on macOS and 197,752 on Linux
    /// ([`a_receive_batch_holds_a_train_a_client_to_its_cap_and_no_more`]), and each past the
    /// reads a range holds (`pending_reads`) their state, 4.42 GB and 11.10 GB in all, which went
    /// to the engines' metadata: 2.27 GB each on macOS and 3.60 GB on Linux, where it wants
    /// 1.38 GB.
    #[test]
    fn a_node_with_memory_to_spare_gives_every_operations_windows_a_receive_chunk() {
        let mut founder = Stated {
            s3: s3(),
            ..stated(1)
        };
        founder.volumes[0].1 = VolumeBytes::Stated(32 << 30);
        let mut measured = facts(1);
        measured.memory = machine(96 << 30, None);
        measured.cores = 32;
        let derived = derive(founder, &measured, &mut fresh).unwrap();
        let (floor, want, bytes) = transport(&derived);
        let clients = said_bytes(&derived, "gateway.clients");
        let chunk = u64::try_from(hyper_transport::RECEIVE_CHUNK).unwrap();
        let initial = hyper_transport::initial_window(hyper_transport::MIN_DATAGRAM);
        eprintln!(
            "96 GiB, 32 cores: transport.floor {floor}, transport.want {want}, transport_bytes \
             {bytes}, gateway.clients {clients}, gateway.objects {}, gateway.objects_bytes {}, \
             memory.remaining {}",
            said_bytes(&derived, "gateway.objects"),
            said_bytes(&derived, "gateway.objects_bytes"),
            said_bytes(&derived, "memory.remaining"),
        );
        // Every client's shares at a chunk is its want, and the transport holds it whole.
        assert_eq!(want, (3 + clients) * 3 * (chunk - initial));
        assert_eq!(bytes - floor, want);
        let operations = said_bytes(&derived, "gateway.objects");
        assert!(
            runs(61_350..63_350, 61_500..63_500).contains(&operations),
            "{operations}"
        );
        assert_eq!(
            said_bytes(&derived, "memory.need"),
            NEED_96_GIB + crate::node::PIECE
        );
        // Its five engines each take a maintenance worker: its need with them is its need and
        // five workers' stacks, cursors, builders and packings, about 90 MB each at the range's
        // longest keys, which its operations' bytes give up.
        assert_eq!(
            said_bytes(&derived, "engine.workers"),
            ENGINE_WORKERS as u64
        );
        let c = &derived.config;
        let engine = Engine {
            store: c.store,
            memtable_bytes: c.memtable_bytes,
            trunk: c.trunk,
        };
        // What remains past the floor is the capacity wants whole and nothing more: the
        // operations took the rest, so no engine's metadata takes what no want does.
        let objects = said_bytes(&derived, "engine.objects");
        let metadata = engine_wants_for(&engine, objects).unwrap();
        assert_eq!(said_bytes(&derived, "engine.metadata_bytes"), metadata);
        assert_eq!(
            said_bytes(&derived, "memory.remaining"),
            said_bytes(&derived, "volume.1.index_bytes") + 5 * metadata
        );
        let each = workers_bytes(&engine, c.rules.max_key_bytes, ENGINE_WORKERS).unwrap();
        assert!((80_000_000..100_000_000).contains(&each), "{each}");
        assert_eq!(
            said_bytes(&derived, "memory.need_workers"),
            NEED_96_GIB + crate::node::PIECE + 5 * (each + STACK)
        );
    }

    /// The need of a ladder founder on 96 GiB and 32 cores at 36ad012b, rule D's order before
    /// each operation's windows held a chunk: the floor at the operations served and the
    /// capacity wants, which the transport's growth and the operations past those never raise.
    /// Its floor counts the lanes of the shards its ranges are placed over, its first alone
    /// (5f3839f2): 291,834,348 bytes less than at every one of its 32 shards. A connection's state
    /// holds its lanes' unsent bytes by rank since hyper-transport kept them as sums
    /// (`lane::Unsent`, vendor/UPSTREAM.md): 1,248 bytes more over the connections it counts.
    /// Linux's differs in two classes. Its threads: the kernel holds the device's 32 transfers on
    /// the issuer's one thread, where a pool's worker carries each elsewhere
    /// (`Issuer::device_threads`), 32 stacks fewer. Its transport: a receive takes a train from
    /// each of the node's three peers and the ten operations' clients it serves at once
    /// ([`runs`]), 1,605,344 bytes more: the buffers of the 12 trains past the first, 131,072
    /// bytes each, and every train's messages' headers.
    const NEED_96_GIB: u64 = if cfg!(target_os = "linux") {
        13_714_100_270 - 32 * STACK + 1_605_344
    } else {
        13_714_100_270
    };

    /// On CI's macOS arm64 runner, three cores and 7 GiB, rule D's order keeps the capacity wants
    /// as they were before the caches were wants: a founder of the membership suite's cell holds
    /// its index's 23,068,852 bytes and each engine's metadata's 18,029,352 at every depth its
    /// device may measure, and a node of it granted with no volume of its own its engines'
    /// metadata, each with at least the ten operations a stock client asks at once, and more
    /// where what remains holds them, each with its client's windows at a receive chunk: a
    /// founder measuring four deep runs 235, where 27 while its floor counted a lane at each of
    /// its three shards (5f3839f2) and 31 at QUIC's initial window (36ad012b). On Linux it runs
    /// 219: each operation holds 133,520 bytes more there ([`runs`]), though the kernel holds the
    /// device's four transfers on the issuer's one thread, four stacks fewer. Counting its
    /// operations at their count moves neither: with its eight peers they make 243 trains and 227,
    /// within the receive batch's 1,024, and every train charged is one it holds. Each cache's
    /// line is its want, its store's pages at most.
    #[test]
    fn on_cis_macos_runner_the_capacity_wants_stand_and_the_operations_take_the_rest() {
        let member = |volume: bool| {
            let mut s = Stated {
                s3: s3(),
                enlist: Some(Enlisting {
                    root: 0x10,
                    domain: "d1".into(),
                    max_nodes: 5,
                    max_volumes: 5,
                }),
                ..stated(1)
            };
            if volume {
                s.volumes[0].1 = VolumeBytes::Stated(256 << 20);
            } else {
                s.volumes.clear();
                s.cell_volume = Some(256 << 20);
            }
            s.peers.retain(|p| p.role == Role::Node);
            for id in 6..=9 {
                s.peers
                    .push(peer(id, Role::Gateway, "/node/identity/c.der", None));
            }
            s
        };
        for depth in [4, 64, 256] {
            for volume in [true, false] {
                let mut measured = deep(256, depth);
                measured.cores = 3;
                measured.memory = machine(7 << 30, None);
                if !volume {
                    measured.volumes.clear();
                }
                let derived = derive(member(volume), &measured, &mut fresh)
                    .unwrap_or_else(|e| panic!("depth {depth}, volume {volume}: {e}"));
                let (floor, want, bytes) = transport(&derived);
                let cache = said_bytes(&derived, "engine.cache_bytes");
                let cache_want = cache_wanted(&derived, said_bytes(&derived, "engine.objects"));
                let operations = said_bytes(&derived, "gateway.objects");
                eprintln!(
                    "7 GiB, depth {depth}, volume {volume}: transport_bytes {bytes}, {} of its \
                     want {want}, engine.cache_bytes {cache} of {cache_want}, gateway.objects \
                     {operations}, gateway.objects_bytes {}",
                    bytes - floor,
                    said_bytes(&derived, "gateway.objects_bytes"),
                );
                if volume {
                    assert_eq!(said_bytes(&derived, "volume.1.index_bytes"), 23_068_852);
                }
                assert_eq!(said_bytes(&derived, "engine.metadata_bytes"), 18_029_352);
                assert!(
                    operations >= crate::listener::STOCK_CLIENT,
                    "depth {depth}, volume {volume}"
                );
                if operations > crate::listener::STOCK_CLIENT {
                    // Past those served, every operation's windows at a chunk: the want whole.
                    assert_eq!(bytes - floor, want, "depth {depth}, volume {volume}");
                }
                if depth == 4 && volume {
                    assert!(
                        runs(215..225, 230..240).contains(&operations),
                        "{operations}"
                    );
                }
                assert!(cache <= cache_want && cache > 0);
            }
        }
    }

    /// The schemes a block may take grow with the domains a cell holds, each no wider than them:
    /// one copy where the cell has one domain, and past the widest candidate's width no more.
    #[test]
    fn the_schemes_are_the_models_choices_at_each_count_of_domains() {
        let (r, t) = (rates(), target(None));
        assert_eq!(
            schemes(1, &r, t).unwrap(),
            vec![durability::Scheme::Copies(1)]
        );
        let widest = durability::candidates()
            .iter()
            .map(|s| s.width())
            .max()
            .unwrap();
        let mut before = Vec::new();
        for domains in 1..=widest {
            let now = schemes(domains, &r, t).unwrap();
            assert!(now.iter().all(|s| s.width() <= domains));
            assert!(before.iter().all(|s| now.contains(s)));
            before = now;
        }
        assert_eq!(schemes(widest + 10, &r, t).unwrap(), before);
    }

    /// An operation of the largest blocks holds the larger of a put and a get: for one copy, a
    /// get's spans and the other copy, the copy cut, the bytes opened and the host's block taken,
    /// past a put's segment, block filling, block going down, its copy sent and a piece of body. A
    /// write of a few bytes holds a few bytes past its piece of body under each scheme, and one past
    /// a block what the largest put holds; every other operation the most.
    #[test]
    fn an_operation_is_charged_what_it_holds_at_its_own_size() {
        let copies = durability::Scheme::Copies(1);
        let three = durability::Scheme::Copies(3);
        let charges = Charges::new(1, &[copies, three]).unwrap();
        let sealed = mantle_gateway::layout::SEALED;
        let piece = u64::try_from(hyper_transport::RECEIVE_CHUNK).unwrap();
        let block = Layout::new(copies).unwrap().largest_block().unwrap();
        let records = |scheme, length| put::records_for(&Layout::new(scheme).unwrap(), length);
        // Under one copy a get holds the most; a put of the largest body, its records with it,
        // holds no more.
        let one = Layout::new(copies).unwrap();
        let largest = put::most_bytes(1, &one).unwrap() + block + piece;
        let most = Charges::new(1, &[copies]).unwrap().most();
        assert_eq!(most, 5 * block + sealed);
        assert!(largest + put::most_records(&one).unwrap() <= most);
        assert_eq!(charges.of(None), charges.most());
        assert_eq!(charges.of(Some(u64::MAX)), charges.most());
        let tag = sealed - u64::try_from(mantle_s3::seal::SEGMENT).unwrap();
        // Under three copies: the segment it seals in, the block filling and the block going
        // down, and the three chunks copied to send, each the body and its tag; a piece of body;
        // and its record of its one block.
        for length in [0u64, 1, 4096] {
            assert_eq!(
                charges.of(Some(length)),
                piece + 6 * (length + tag) + records(three, length).unwrap(),
                "{length}"
            );
        }
        assert_eq!(
            charges.least(),
            piece + 6 * tag + records(three, 0).unwrap()
        );
        // A write of the largest body is charged the most, its records of its blocks with it.
        let largest = mantle_s3::body::MAX_UPLOAD;
        assert_eq!(charges.of(Some(largest)), charges.most());
        assert!(charges.of(Some(1 << 30)) < charges.most());
        // A write holding more blocks going down holds more, a block's copies each; the window
        // of one is what `of` charges.
        assert_eq!(charges.write(1, 4096), charges.of(Some(4096)));
        let long = 64u64 << 20;
        assert!(charges.write(3, long) > charges.write(1, long));
        assert!(charges.write(3, long) - charges.write(2, long) >= 4 * block);
        assert_eq!(charges.largest_block(), block);
        let coded = durability::Scheme::Rs(mantle_ec::Code::new(2, 1).unwrap());
        let mixed = Charges::new(1, &[three, coded]).unwrap();
        assert!(mixed.of(Some(1)) >= charges.of(Some(1)));
        assert!(mixed.of(Some(1)) < mixed.most());
    }

    /// A step of a bucket's attempt passes or removes as many rows as the largest entry a range
    /// takes holds of what one row a collection removes writes: more than one, and their writes
    /// within that entry's bytes.
    #[test]
    fn a_buckets_step_passes_the_rows_the_largest_entry_holds() {
        let rows = usize::try_from(scan_rows().unwrap()).unwrap();
        let entry = wire::largest_entry_bytes().unwrap();
        let row = wire::largest_collected_bytes().unwrap();
        assert!(row > key::largest_bytes());
        assert!(rows > 1, "{rows}");
        assert!(rows * row <= entry);
        assert!((rows + 1) * row > entry);
        assert_eq!(gateway(1, 0).unwrap().scan_rows, scan_rows().unwrap());
    }

    /// A get is charged for its window under the scheme that holds the most of it: its driver's
    /// blocks being read at the largest block (`get::most_bytes`) and the block taken to be
    /// written out, so what it holds at any window it learns is within its charge; at the floor's
    /// window no more than the most an operation holds, and each block more a block's bytes more
    /// at least. The smallest block is the most blocks a body is cut into.
    #[test]
    fn a_get_is_charged_for_the_blocks_its_window_holds() {
        let copies = durability::Scheme::Copies(3);
        let coded = durability::Scheme::Rs(mantle_ec::Code::new(2, 1).unwrap());
        let charges = Charges::new(1, &[copies, coded]).unwrap();
        for window in 1..=8 {
            for scheme in [copies, coded] {
                let layout = Layout::new(scheme).unwrap();
                let block = layout.largest_block().unwrap();
                let held = get::most_bytes(window, &layout).unwrap() + block;
                assert!(charges.read(window) >= held, "{window} {scheme:?}");
            }
            assert!(charges.read(window + 1) - charges.read(window) >= charges.smallest_block());
        }
        assert!(charges.read(1) <= charges.most());
        assert_eq!(charges.read(usize::MAX), u64::MAX);
        let block = |scheme| Layout::new(scheme).unwrap().largest_block().unwrap();
        assert_eq!(charges.smallest_block(), block(copies));
        assert_eq!(charges.largest_block(), block(coded));
        assert!(block(copies) < block(coded));
    }

    /// A copy is charged its get and its put at once, each for its own window: its get's blocks
    /// being read beside its put's going down of its copy's length, each block more of either
    /// charged more. At the floor's windows and the largest body a copy writes it is within the
    /// most one operation is charged, which the operations' bytes always hold, and a shorter copy
    /// holds less.
    #[test]
    fn a_copy_is_charged_its_get_and_its_put_at_once() {
        let copies = durability::Scheme::Copies(3);
        let coded = durability::Scheme::Rs(mantle_ec::Code::new(2, 1).unwrap());
        let charges = Charges::new(1, &[copies, coded]).unwrap();
        let largest = mantle_s3::body::MAX_UPLOAD;
        for (get, put, length) in [(1, 1, largest), (4, 2, largest), (2, 3, 1 << 20)] {
            assert_eq!(
                charges.copy(get, put, length),
                charges.read(get) + charges.write(put, length)
            );
            assert!(charges.copy(get + 1, put, length) > charges.copy(get, put, length));
            assert!(charges.copy(get, put + 1, largest) > charges.copy(get, put, largest));
        }
        let floor = charges.copy(1, 1, largest);
        assert!(floor > charges.most());
        assert!(floor <= most_charged(&charges));
        assert!(charges.copy(1, 1, 1 << 20) < floor);
        assert_eq!(charges.copy(usize::MAX, 1, largest), u64::MAX);
    }

    /// A share holds an operation of the largest blocks and as many more at the least an operation
    /// holds and what it adds beside as fit; at least one at the most where it holds less. What
    /// the operations hold beside is counted at their count: past a bound that stops a part of it,
    /// each operation more costs only what still grows, and the bytes the share holds for their
    /// charges are what their count leaves.
    #[test]
    fn the_operations_hold_their_share_of_the_memory() {
        let charges = Charges::new(1, &[durability::Scheme::Copies(3)]).unwrap();
        let share = 1u64 << 30;
        let beside = 300_000;
        let each = |n: u64| n.checked_mul(beside);
        let most = most_charged(&charges);
        let one = Served {
            operations: 1,
            bytes: most,
        };
        let (count, bytes) = operations(share, &each, &charges, one, u64::MAX);
        let count = u64::try_from(count).unwrap();
        assert_eq!(count, share / (charges.least() + beside));
        assert_eq!(bytes, share - count * beside);
        assert!(bytes >= most && bytes >= count * charges.least());
        assert_eq!(operations(1, &each, &charges, one, u64::MAX), (1, most));
        // Bytes held for what the node serves come first, the operations past them each at the
        // least.
        let held = Served {
            operations: 10,
            bytes: share / 2,
        };
        let (count, bytes) = operations(share, &each, &charges, held, u64::MAX);
        let count = u64::try_from(count).unwrap();
        assert!(bytes >= held.bytes, "{bytes} of {}", held.bytes);
        assert_eq!(count, (share - held.bytes) / beside);
        // A share short of those bytes still serves the operations it holds at the least, the
        // bytes past them theirs.
        let short = Served {
            operations: 10,
            bytes: share * 2,
        };
        let (count, bytes) = operations(share, &each, &charges, short, u64::MAX);
        assert_eq!(count, 10);
        assert_eq!(bytes, share - 10 * beside);
        // A part of what each operation holds beside, a train, stops growing at the hundredth:
        // the operations past it cost the rest alone, and the share holds as many more as that
        // frees, every byte of it charged to an operation held. Charged the train each, they
        // were fewer, and the share's bytes for their charges kept what no train held.
        let (train, trains) = (100_000u64, 100u64);
        let capped = |n: u64| {
            n.checked_mul(beside - train)?
                .checked_add(n.min(trains) * train)
        };
        let (count, bytes) = operations(share, &capped, &charges, one, u64::MAX);
        let count = u64::try_from(count).unwrap();
        let least = charges.least();
        let held = |n: u64| n * least + capped(n).unwrap();
        assert!(held(count) <= share && held(count + 1) > share, "{count}");
        assert_eq!(bytes, share - capped(count).unwrap());
        assert_eq!(
            count,
            trains + (share - trains * (least + beside)) / (least + beside - train)
        );
        let (charged, _) = operations(share, &each, &charges, one, u64::MAX);
        let charged = u64::try_from(charged).unwrap();
        assert!(count > charged, "{count} {charged}");
        assert!(share - held(count) < least + beside - train);
        assert!(share - (charged * least + capped(charged).unwrap()) >= least + beside - train);
        // The count never passes the descriptors' bound, nor the bytes a cost the closure cannot
        // count.
        assert_eq!(operations(share, &capped, &charges, one, 7).0, 7);
        assert_eq!(
            operations(share, &|_| None, &charges, one, u64::MAX),
            (1, most)
        );
        let g = gateway(7, 9).unwrap();
        assert_eq!(
            (g.objects, g.objects_bytes, g.clients, g.window),
            (7, 9, 7, 1)
        );
        let coded = durability::Scheme::Rs(mantle_ec::Code::new(2, 1).unwrap());
        let copies = durability::Scheme::Copies(1);
        assert_eq!(objects(&g, &[copies]).unwrap(), 9);
        assert_eq!(
            objects(&g, &[copies, coded]).unwrap(),
            9 + mantle_ec::TABLE_BYTES
        );
    }

    /// The most an operation is charged holds a repair of the largest block of every scheme a cell
    /// may take reading a chunk from as many places as the block is wide: a repair reserves that
    /// charge for each width of its block's places (node::repair).
    #[test]
    fn an_operations_most_charge_holds_a_repair_of_a_blocks_width_of_places() {
        let window = gateway(0, 0).unwrap().window;
        for scheme in durability::candidates() {
            let layout = Layout::new(scheme).unwrap();
            let block = layout.largest_block().unwrap();
            let (data, parity) = match scheme {
                Scheme::Copies(n) => (1, n - 1),
                Scheme::Rs(code) => (code.data(), code.parity()),
            };
            let header = mantle_meta::record::BlockHeader {
                length: block,
                data: u8::try_from(data).unwrap(),
                parity: u8::try_from(parity).unwrap(),
                chunk_len: layout.chunk_len(block).unwrap(),
                crc32c: 0,
            };
            let repair = mantle_gateway::repair::most_bytes(&header, scheme.width()).unwrap();
            let most = Charges::new(window, &[scheme]).unwrap().most();
            assert!(
                repair <= most,
                "{scheme:?}: a repair's {repair} bytes past an operation's most {most}"
            );
        }
    }

    /// The log derived takes a frame of the range's largest entry with its shell's bytes; a group
    /// retains two cadences and a `Ready` of the settings, in its entries of the least entry a range
    /// proposes and its bytes with their shells, as the range checks; and its file holds every
    /// group's retention at once on disk, beside the least three segments the log runs in.
    #[test]
    fn the_log_takes_the_largest_entry_and_holds_every_groups_retention() {
        let (settings, _) = cell(3).unwrap();
        let align = Alignment::new(4096).unwrap();
        let config = log(align, &settings, 5).unwrap();
        assert_eq!(config.max_groups, 6);
        let (entries, bytes) = mantle_range::ready_held(&settings).unwrap();
        let least = u64::try_from(wire::least_entry_bytes().unwrap()).unwrap();
        // The appends out and one entry past them (`mantle_range::ready_bytes`).
        let held = in_flight(3).unwrap() + 1;
        assert_eq!(entries, settings.max_entry_bytes * held / least + 1);
        assert_eq!(config.group_entries, 3 * entries);
        assert_eq!(config.group_bytes, 3 * bytes);
        let retained = 6 * config.group_bytes
            + 6 * config.group_entries * hyper_log::format::ENTRY_HEADER_BYTES;
        assert!(u64::from(config.max_segments) * config.segment_bytes >= retained);
        assert!(config.max_segments >= 3);
        let file = SimFile::new(align, Alignment::new(512).unwrap(), 1).unwrap();
        let log = Log::create(file, config, 1).unwrap();
        let entry = usize::try_from(settings.max_entry_bytes).unwrap();
        assert!(log.entry_room().unwrap() >= entry + hyper_durable::ENTRY_OVERHEAD);
    }

    const PROFILE: Profile = Profile {
        depth: 4,
        read_bytes: 1 << 20,
        read_gap: 64 << 10,
        prewrite: true,
    };

    /// A volume laid out on a device of 4 KiB blocks: checksum blocks of the block, the least
    /// segment a record of the largest chunk fits whole, batches of one record, the profile's
    /// reads, and as many fragments as both the volume at a block each and the index's bytes
    /// hold; the compact layout on a volume too small for the standard one, and a refusal for
    /// one too small for either, or an index that holds no fragment.
    #[test]
    fn a_volume_holds_a_chunk_a_segment_and_the_fragments_its_index_holds() {
        let align = Alignment::new(4096).unwrap();
        let chunk = mantle_gateway::layout::CHUNK;
        let v = volume(align, 1 << 30, chunk, &PROFILE, 1 << 30, None).unwrap();
        assert_eq!(v.checksum_shift, 12);
        assert!(volume_layout::record_room(v.segment_size, 4096, 12) >= chunk);
        assert!(volume_layout::record_room(v.segment_size - 4096, 4096, 12) < chunk);
        assert!(!v.compact);
        // A batch holds every chunk waiting its bytes hold at a block each, within its frame.
        assert_eq!(
            (v.limits.batch_requests, v.limits.fragments_per_chunk),
            (
                v.limits
                    .batch_bytes
                    .div_euclid(4096)
                    .min(volume_layout::most_batch_requests().unwrap()),
                1
            )
        );
        assert!(v.limits.batch_requests > 1);
        assert_eq!(v.max_fragments, (1 << 30) / 4096);
        assert_eq!(v.reads, Reads::measured(4, 1 << 20, 64 << 10));
        assert!(v.prewrite);
        Geometry::plan(1 << 30, align, &v).unwrap();
        let bytes = index::most_bytes(1000).unwrap();
        let few = volume(align, 1 << 30, chunk, &PROFILE, bytes, None).unwrap();
        assert_eq!(few.max_fragments, 1000);
        assert!(matches!(
            volume(align, 1 << 30, chunk, &PROFILE, index::INDEX_BYTES, None),
            Err(DeriveError::Index { .. })
        ));
        let small = volume(align, 64 << 20, chunk, &PROFILE, 1 << 30, None).unwrap();
        assert!(small.compact);
        Geometry::plan(64 << 20, align, &small).unwrap();
        assert!(matches!(
            volume(align, 16 << 20, chunk, &PROFILE, 1 << 30, None),
            Err(DeriveError::Volume(_))
        ));
        // Checksum blocks stop at CRC-32C's verified 16 KiB on a device of a larger unit.
        let wide = Alignment::new(64 << 10).unwrap();
        let w = volume(wide, 1 << 30, chunk, &PROFILE, 1 << 30, None).unwrap();
        assert_eq!(w.checksum_shift, 14);
        Geometry::plan(1 << 30, wide, &w).unwrap();
    }

    /// A volume laid out already keeps its block, segment and checksum blocks, and holds no
    /// more fragments than its log was laid out for, however many its index's bytes would.
    #[test]
    fn a_formatted_volume_keeps_its_layout_and_the_fragments_its_log_holds() {
        let align = Alignment::new(4096).unwrap();
        let chunk = mantle_gateway::layout::CHUNK;
        let bytes = index::most_bytes(1000).unwrap();
        let first = volume(align, 8 << 30, chunk, &PROFILE, bytes, None).unwrap();
        let formatted = Formatted {
            geometry: Geometry::plan(8 << 30, align, &first).unwrap(),
            checksum_shift: first.checksum_shift,
        };
        let wide = Alignment::new(64 << 10).unwrap();
        let again = volume(wide, 8 << 30, chunk, &PROFILE, 1 << 30, Some(&formatted)).unwrap();
        assert_eq!(
            (again.segment_size, again.checksum_shift),
            (first.segment_size, first.checksum_shift)
        );
        assert_eq!(again.limits.batch_bytes, first.limits.batch_bytes);
        assert!(again.max_fragments >= 1000);
        assert!(again.max_fragments < (8 << 30) / 4096);
        formatted.geometry.holds(&again).unwrap();
        let one_more = VolumeConfig {
            max_fragments: again.max_fragments + 1,
            ..again
        };
        assert!(formatted.geometry.holds(&one_more).is_err());
        let fewer = volume(align, 8 << 30, chunk, &PROFILE, bytes / 2, Some(&formatted)).unwrap();
        assert_eq!(fewer.max_fragments, index::most_fragments(bytes / 2));
    }

    fn machine(total: u64, limit: Option<u64>) -> MemoryFacts {
        MemoryFacts {
            total,
            available: total / 2,
            address_bits: 48,
            limit,
        }
    }

    #[test]
    fn the_allowance_is_the_ceiling_else_the_os_limit_else_physical_memory() {
        let gib = 1u64 << 30;
        let ok = |stated, total, limit| allowance(stated, &machine(total, limit)).ok();
        // Stated, within what the OS lets the process hold.
        assert_eq!(ok(Some(4 * gib), 16 * gib, Some(8 * gib)), Some(4 * gib));
        assert_eq!(ok(Some(8 * gib), 16 * gib, Some(8 * gib)), Some(8 * gib));
        // Stated past it: refused, naming both.
        assert!(matches!(
            allowance(Some(8 * gib + 1), &machine(16 * gib, Some(8 * gib))),
            Err(DeriveError::Ceiling { stated, capacity })
                if stated == 8 * gib + 1 && capacity == 8 * gib
        ));
        assert!(allowance(Some(16 * gib + 1), &machine(16 * gib, None)).is_err());
        // None stated: the OS's limit, physical memory where that is past it or there is none.
        assert_eq!(ok(None, 16 * gib, Some(8 * gib)), Some(8 * gib));
        assert_eq!(ok(None, 16 * gib, Some(32 * gib)), Some(16 * gib));
        assert_eq!(ok(None, 16 * gib, None), Some(16 * gib));
    }

    #[test]
    fn the_floor_admits_up_to_its_allowance_and_names_the_class_past_it() {
        let classes = [
            (Class::Objects, 10),
            (Class::Volumes, 20),
            (Class::Engines, 30),
            (Class::Threads, 40),
        ];
        let crossed = |allowance| match floor(&classes, allowance) {
            Err(DeriveError::Memory {
                class,
                floor,
                allowance: named,
            }) if named == allowance => Some((class, floor)),
            _ => None,
        };
        // At the allowance exactly nothing remains; a byte more remains a byte.
        assert_eq!(floor(&classes, 100).ok(), Some(0));
        assert_eq!(floor(&classes, 101).ok(), Some(1));
        // A byte short: the last class crosses, the whole floor named.
        assert_eq!(crossed(99), Some((Class::Threads, 100)));
        // Crossed early: the class whose bytes took the count past the allowance.
        assert_eq!(crossed(59), Some((Class::Engines, 100)));
        assert_eq!(crossed(29), Some((Class::Volumes, 100)));
        assert_eq!(crossed(9), Some((Class::Objects, 100)));
        assert!(matches!(
            floor(&[(Class::Log, u64::MAX), (Class::Ranges, 1)], u64::MAX),
            Err(DeriveError::Overflow("the floor"))
        ));
        // The refusal says what the node needs.
        let refused = floor(&classes, 99).unwrap_err().to_string();
        assert!(
            refused.contains("threads") && refused.contains("100"),
            "{refused}"
        );
    }

    #[test]
    fn what_remains_is_shared_in_proportion_to_what_each_device_wants() {
        // Enough for every want: each takes its own.
        assert_eq!(shares(100, &[30, 70]).ok(), Some(vec![30, 70]));
        assert_eq!(shares(1_000, &[30, 70]).ok(), Some(vec![30, 70]));
        // Less: each the same part of its want, rounded down, never past what remains.
        let parts = shares(50, &[30, 70]).unwrap();
        assert_eq!(parts, vec![15, 35]);
        let parts = shares(7, &[1, 2, 4]).unwrap();
        assert_eq!(parts, vec![1, 2, 4]);
        let parts = shares(6, &[1, 2, 4]).unwrap();
        assert!(parts.iter().sum::<u64>() <= 6, "{parts:?}");
        assert_eq!(parts, vec![0, 1, 3]);
        let parts = shares(u64::MAX / 2, &[u64::MAX / 2, u64::MAX / 2]).unwrap();
        assert!(parts.iter().sum::<u64>() <= u64::MAX / 2, "{parts:?}");
        assert_eq!(shares(0, &[5, 5]).ok(), Some(vec![0, 0]));
        assert_eq!(shares(10, &[]).ok(), Some(vec![]));
    }

    #[test]
    fn an_engine_takes_the_extents_its_share_of_memory_holds_at_the_measured_metadata() {
        let (_, rules) = cell(3).unwrap();
        let full = engine(4096, 1 << 30, &rules).unwrap();
        let per = mantle_engine::shard_db::measured_per_extent(&full.store).unwrap();
        let extent = u64::try_from(full.store.page_size).unwrap() * u64::from(EXTENT_PAGES);
        // An extent's memory is its metadata measured at the densest rows, a tenth of its bytes
        // or so, and its allocator's few bytes.
        assert!(per > extent / 20 && per < extent / 5, "{per} of {extent}");
        assert_eq!(engine_wants(&full), Some(full.store.max_extents * per));
        let within = |bytes| engine_within(full, bytes).map(|e| e.store.max_extents);
        assert_eq!(within(10 * per).ok(), Some(10));
        assert_eq!(within(10 * per + per - 1).ok(), Some(10));
        assert_eq!(within(2 * per).ok(), Some(2));
        assert!(matches!(
            within(2 * per - 1),
            Err(DeriveError::Metadata { bytes, per_extent }) if bytes == 2 * per - 1 && per_extent == per
        ));
        // Never past what its device's share holds.
        assert_eq!(within(u64::MAX).ok(), Some(full.store.max_extents));
        let page = engine_within(full, 10 * per).unwrap().store.page_size;
        assert_eq!(page, full.store.page_size);
    }

    #[test]
    fn what_remains_sizes_each_volumes_index_and_each_engines_extents() {
        let align = Alignment::new(4096).unwrap();
        let chunk = mantle_gateway::layout::CHUNK;
        let size = 1u64 << 30;
        let index = index_wants(align, size).unwrap();
        // At its want, a volume's index holds a fragment at each of its blocks.
        let v = volume(align, size, chunk, &PROFILE, index, None).unwrap();
        assert_eq!(v.max_fragments, size / 4096);
        let (_, rules) = cell(3).unwrap();
        let full = engine(4096, 1 << 30, &rules).unwrap();
        let wants = [index, engine_wants_for(&full, size / 4096).unwrap()];
        // A third of what they want remains: each takes a third of its want.
        let remaining = (wants[0] + wants[1]) / 3;
        let parts = shares(remaining, &wants).unwrap();
        assert!(parts[0] + parts[1] <= remaining);
        let v = volume(align, size, chunk, &PROFILE, parts[0], None).unwrap();
        assert_eq!(v.max_fragments, index::most_fragments(parts[0]));
        assert!(v.max_fragments < size / 4096);
        let e = engine_within(full, parts[1]).unwrap();
        assert!(e.store.max_extents < full.store.max_extents);
        let per = mantle_engine::shard_db::measured_per_extent(&e.store).unwrap();
        assert!(e.store.max_extents * per <= parts[1]);
        assert!((e.store.max_extents + 1) * per > parts[1]);
    }

    /// An engine wants the extents its rows take for the objects, at the most store a range took an
    /// object, never past what its device's share holds.
    #[test]
    fn an_engine_wants_the_extents_its_rows_take_for_the_objects() {
        let (_, rules) = cell(3).unwrap();
        let full = engine(4096, 1 << 30, &rules).unwrap();
        let per = mantle_engine::shard_db::measured_per_extent(&full.store).unwrap();
        let extent = u64::try_from(full.store.page_size).unwrap() * u64::from(EXTENT_PAGES);
        let objects = 100_000;
        assert_eq!(
            engine_wants_for(&full, objects),
            Some((objects * STORE_BYTES_AN_OBJECT).div_ceil(extent) * per)
        );
        assert_eq!(engine_wants_for(&full, 0), Some(0));
        assert_eq!(
            engine_wants_for(&full, u64::MAX / STORE_BYTES_AN_OBJECT),
            engine_wants(&full)
        );
    }

    /// An engine's cache wants every page of the extents its rows take for the objects, the
    /// estimate its metadata's want counts, never past what its device's share holds.
    #[test]
    fn an_engines_cache_wants_the_pages_its_rows_take_for_the_objects() {
        let (_, rules) = cell(3).unwrap();
        let full = engine(4096, 1 << 30, &rules).unwrap();
        let per = mantle_engine::shard_db::measured_per_extent(&full.store).unwrap();
        let extent = u64::try_from(full.store.page_size).unwrap() * u64::from(EXTENT_PAGES);
        // Within the device's share of a GiB.
        for objects in [1, 1_000, 100_000, 500_000] {
            let extents = (objects * STORE_BYTES_AN_OBJECT).div_ceil(extent);
            let cache = engine_cache_wants_for(&full, objects).unwrap();
            assert_eq!(cache, extents * extent, "{objects}");
            // The metadata's extents, each page of them.
            assert_eq!(
                cache / extent * per,
                engine_wants_for(&full, objects).unwrap()
            );
            // Its rows' store bytes, to the next extent.
            let rows = objects * STORE_BYTES_AN_OBJECT;
            assert!(cache >= rows && cache - extent < rows, "{objects}");
        }
        assert_eq!(engine_cache_wants_for(&full, 0), Some(0));
        assert_eq!(
            engine_cache_wants_for(&full, u64::MAX / STORE_BYTES_AN_OBJECT),
            Some(full.store.max_extents * extent)
        );
    }

    /// A cell's blocks are its nodes' chunks over the width of the scheme its domains take: a cell
    /// of three puts a chunk of every block on each node, and a cell of one each block on its node.
    #[test]
    fn a_cells_blocks_are_its_nodes_chunks_over_its_schemes_width() {
        let (rates, target) = (rates(), target(None));
        let blocks = |domains| cell_blocks(1_000, Spread::of(domains, &rates, target)?);
        assert_eq!(blocks(3).ok(), Some(1_000));
        assert_eq!(blocks(1).ok(), Some(1_000));
        for domains in [2usize, 5, 9, 20] {
            let width = match durability::choose(&durability::candidates(), domains, &rates, target)
            {
                Ok(Some(Choice::Meets { scheme, .. } | Choice::Short { scheme, .. })) => {
                    scheme.width()
                }
                other => panic!("{domains} domains: {other:?}"),
            };
            assert_eq!(
                blocks(domains).ok(),
                Some(1_000 * domains as u64 / width as u64)
            );
        }
        assert!(matches!(
            blocks(0),
            Err(DeriveError::Schemes { domains: 0 })
        ));
    }

    /// A volume stated to take what its file system spares takes what the OS states the process
    /// may write there less what its other work keeps and what the node writes there besides: on
    /// a file system of its own, every byte of that; on the node directory's, the most blocks
    /// beside which the log's file and the stores' rows of the cell's objects still fit, the
    /// objects those blocks and every other volume's make. Laid out, it keeps its layout's bytes;
    /// short of its keep, or beside a second such volume, it is refused.
    #[test]
    fn a_volume_of_what_its_file_system_spares_leaves_the_node_and_the_machine_their_room() {
        let (rates, target) = (rates(), target(None));
        let spread = Spread::of(3, &rates, target).unwrap();
        let log_file = 1 << 30;
        let mut stated = stated(2);
        let mut measured = facts(2);
        stated.volumes[0].1 = VolumeBytes::Spare { keep: 3 << 30 };
        let ranges = stated.ranges.len();
        let sizes = volume_sizes(&stated, &measured, log_file, ranges, spread).unwrap();
        assert_eq!(sizes, vec![(1 << 39) - (3 << 30), 8 << 30]);

        // On the node directory's file system: the blocks, and beside them the log and the rows.
        measured.volumes[0].device.name = measured.meta.name.clone();
        let sizes = volume_sizes(&stated, &measured, log_file, ranges, spread).unwrap();
        let block = volume_layout::block(measured.volumes[0].device.align).unwrap();
        let fragments = sizes[0] / block;
        assert_eq!(sizes[0] % block, 0);
        let rows = |fragments: u64| {
            let objects = cell_blocks(fragments + (8 << 30) / block, spread).unwrap();
            ranges as u64 * objects * STORE_BYTES_AN_OBJECT
        };
        let room = (1u64 << 39) - (3 << 30) - log_file;
        assert!(sizes[0] + rows(fragments) <= room);
        // One more block's bytes and rows would not fit, the cell's objects counted whole.
        let (more, width) = (u128::from(fragments + 1), u128::from(spread.width));
        let per_object = (ranges as u64 * STORE_BYTES_AN_OBJECT * spread.nodes) as u128;
        assert!(
            more * u128::from(block) * width
                + per_object * (more + u128::from((8u64 << 30) / block))
                > u128::from(room) * width
        );

        // Laid out, its layout's bytes, whatever the file system spares now.
        let geometry = Geometry::plan(
            4 << 30,
            measured.volumes[0].device.align,
            &VolumeConfig::default(),
        )
        .unwrap();
        measured.volumes[0].formatted = Some(Formatted {
            geometry,
            checksum_shift: 12,
        });
        measured.volumes[0].device.available = Some(0);
        let sizes = volume_sizes(&stated, &measured, log_file, ranges, spread).unwrap();
        assert_eq!(sizes[0], geometry.end().unwrap());

        // Short of what it keeps and the node writes there: refused, as is a file system that
        // says nothing, and two such volumes that may share one.
        measured.volumes[0].formatted = None;
        measured.volumes[0].device.available = Some(3 << 30);
        assert!(matches!(
            volume_sizes(&stated, &measured, log_file, ranges, spread),
            Err(DeriveError::Spare { .. })
        ));
        measured.volumes[0].device.available = None;
        assert!(matches!(
            volume_sizes(&stated, &measured, log_file, ranges, spread),
            Err(DeriveError::Unspared(_))
        ));
        measured.volumes[0].device.available = Some(1 << 39);
        measured.volumes[1].device.name = None;
        stated.volumes[1].1 = VolumeBytes::Spare { keep: 0 };
        assert!(matches!(
            volume_sizes(&stated, &measured, log_file, ranges, spread),
            Err(DeriveError::Spares { .. })
        ));
    }

    /// A host of 16 GiB with no OS limit, a node directory on a 4 KiB-unit device of a terabyte, and
    /// each volume on a device of its own, measured as `PROFILE`.
    fn facts(volumes: usize) -> Measured {
        let device = |name: String| Device {
            name: Some(name),
            align: Alignment::new(4096).unwrap(),
            queue: Some(32),
            bytes: Some(1 << 40),
            available: Some(1 << 39),
        };
        Measured {
            memory: machine(16 << 30, None),
            cores: 8,
            ring: 16,
            page: 16_384,
            meta: device("meta".into()),
            volumes: (0..volumes)
                .map(|n| VolumeFacts {
                    device: device(format!("data{n}")),
                    profile: PROFILE,
                    measured_now: true,
                    formatted: None,
                })
                .collect(),
            descriptors: 1 << 20,
        }
    }

    /// `path`, rooted, as an absolute path on this platform: on Windows one with no drive is the
    /// config's drive's, which a configuration read back resolves it against, so the test names one.
    fn at(path: &str) -> PathBuf {
        let root = if cfg!(windows) { "C:" } else { "" };
        PathBuf::from(format!("{root}{path}"))
    }

    fn peer(id: u64, role: Role, certificate: &str, at_: Option<(&str, &str)>) -> PeerLine {
        PeerLine {
            id,
            role,
            certificate: at(certificate),
            at: at_.map(|(quic, plane)| {
                (
                    crate::process::Endpoint::parse(quic).unwrap(),
                    crate::process::Endpoint::parse(plane).unwrap(),
                )
            }),
        }
    }

    /// A node of a three-node cell hosting its five founding ranges, `volumes` volumes of 8 GiB and,
    /// with them, objects.
    fn stated(volumes: usize) -> Stated {
        Stated {
            repair: None,
            cell_volume: None,
            id: 1,
            dir: at("/node"),
            quic: "127.0.0.1:7000".parse().unwrap(),
            plane: "127.0.0.1:7001".parse().unwrap(),
            authority: at("/node/identity/authority.der"),
            certificate: at("/node/identity/node.der"),
            key: at("/node/identity/node.key"),
            peers: vec![
                peer(
                    2,
                    Role::Node,
                    "/node/identity/2.der",
                    Some(("node-2.example:7000", "node-2.example:7001")),
                ),
                peer(
                    3,
                    Role::Node,
                    "/node/identity/3.der",
                    Some(("[::1]:7000", "[::1]:7001")),
                ),
                peer(9, Role::Gateway, "/node/identity/9.der", None),
            ],
            seeds: Vec::new(),
            reach: None,
            ranges: vec![
                (0x10, Layer::Root),
                (0x11, Layer::Bucket),
                (0x12, Layer::Name),
                (0x13, Layer::File),
                (0x14, Layer::Block),
            ],
            voters: vec![1, 2, 3],
            volumes: (0..volumes)
                .map(|n| (at(&format!("/data{n}")), VolumeBytes::Stated(8 << 30)))
                .collect(),
            enlist: (volumes > 0).then(|| Enlisting {
                root: 0x10,
                domain: "rack-1".into(),
                max_nodes: 3,
                max_volumes: 3,
            }),
            keys: if volumes > 0 {
                vec![(1, at("/node/keys/1"))]
            } else {
                Vec::new()
            },
            ceiling: None,
            stop: None,
            target: None,
            s3: None,
            range_shards: None,
            native_shards: None,
        }
    }

    fn fresh(_: u128, _: usize) -> Result<Option<Shape>, String> {
        Ok(None)
    }

    /// The bases as `mantle node`'s file: each key `mantle node` names and each line it repeats as
    /// said, the volume's `volume.1.` keys its `volume.` ones and its line.
    fn node_file(bases: &[Basis]) -> String {
        use crate::process::{KEYS, REPEATED};
        let mut text = String::new();
        let mut line = Vec::new();
        for basis in bases {
            if let Some(field) = basis.key.strip_prefix("volume.1.") {
                match field {
                    "dir" | "size" | "depth" | "read_bytes" | "read_gap" | "prewrite" => {
                        line.push(basis.value.clone());
                    }
                    "index_bytes" | "issuer_depth" => {}
                    _ => text.push_str(&format!("volume.{field} = {}\n", basis.value)),
                }
            } else if KEYS.contains(&basis.key.as_str()) || REPEATED.contains(&basis.key.as_str()) {
                text.push_str(&format!("{} = {}\n", basis.key, basis.value));
            }
        }
        if !line.is_empty() {
            text.push_str(&format!("volume = {}\n", line.join(" ")));
        }
        text
    }

    /// Every value `mantle node`'s configuration names is derived with its basis, each key once and
    /// each repeated line as often as it repeats; written as `mantle node`'s file, they read back as
    /// exactly what the node runs with, and pass the node's own checks.
    #[test]
    fn every_value_mantle_node_names_is_derived_with_its_basis() {
        let derived = derive(stated(1), &facts(1), &mut fresh).unwrap();
        let said = |key: &str| derived.bases.iter().filter(|b| b.key == key).count();
        for key in crate::process::KEYS {
            let key = key
                .strip_prefix("volume.")
                .map_or_else(|| (*key).to_owned(), |field| format!("volume.1.{field}"));
            assert_eq!(said(&key), 1, "{key}");
        }
        for (key, lines) in [
            ("peer", 3),
            ("range", 5),
            ("root_key", 1),
            ("durability.burst", 1),
        ] {
            assert_eq!(said(key), lines, "{key}");
        }
        for key in ["dir", "size", "depth", "read_bytes", "read_gap", "prewrite"] {
            assert_eq!(said(&format!("volume.1.{key}")), 1, "{key}");
        }
        // Each says where it comes from, and why.
        for basis in &derived.bases {
            assert!(!basis.value.is_empty() && !basis.why.is_empty(), "{basis}");
        }
        let kind = |key: &str| derived.bases.iter().find(|b| b.key == key).unwrap().kind;
        assert_eq!(kind("id"), Kind::Stated);
        assert_eq!(kind("volume.1.depth"), Kind::Measured);
        assert_eq!(kind("engine.page_size"), Kind::Derived);
        assert_eq!(kind("engine.fanout"), Kind::Cited);
        assert_eq!(kind("runtime.probe_budget_ms"), Kind::Bound);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.conf");
        std::fs::write(&path, node_file(&derived.bases)).unwrap();
        let read = ProcessConfig::read(&path).unwrap();
        let config = &derived.config;
        assert_eq!(
            (read.id, &read.dir, read.quic, read.plane),
            (config.id, &config.dir, config.quic, config.plane)
        );
        assert_eq!(
            (&read.authority, &read.certificate, &read.key),
            (&config.authority, &config.certificate, &config.key)
        );
        assert_eq!(read.peers, config.peers);
        assert_eq!(read.ranges, config.ranges);
        assert_eq!(read.voters, config.voters);
        assert_eq!(read.settings, config.settings);
        assert_eq!(read.rules, config.rules);
        assert_eq!(read.log, config.log);
        assert_eq!(read.store, config.store);
        assert_eq!(read.memtable_bytes, config.memtable_bytes);
        assert_eq!(read.trunk, config.trunk);
        assert_eq!(read.transport_bytes, config.transport_bytes);
        assert_eq!(read.probe_budget, config.probe_budget);
        assert_eq!(
            format!("{:?}", read.volumes),
            format!("{:?}", config.volumes)
        );
        assert_eq!(read.chunk_bytes, config.chunk_bytes);
        assert_eq!(
            (read.chunk_asks, read.command_asks, read.read_asks),
            (config.chunk_asks, config.command_asks, config.read_asks)
        );
        assert_eq!(read.enlist, config.enlist);
        assert_eq!(read.range_shards, config.range_shards);
        assert_eq!(read.native_shards, config.native_shards);
        let (read, config) = (read.gateway.unwrap(), config.gateway.as_ref().unwrap());
        assert_eq!(read.keys, config.keys);
        assert_eq!(
            (
                read.handover_ns,
                read.window,
                read.objects,
                read.bucket_quota,
                read.scan_rows
            ),
            (
                config.handover_ns,
                config.window,
                config.objects,
                config.bucket_quota,
                config.scan_rows
            )
        );
        assert_eq!(
            (read.chunk_rate, read.repair_rate, read.target),
            (config.chunk_rate, config.repair_rate, config.target)
        );
        assert_eq!(read.bursts, config.bursts);
    }

    /// The bytes basis `key` says, of `derived`.
    fn said_bytes(derived: &Derived, key: &str) -> u64 {
        let basis = derived.bases.iter().find(|b| b.key == key).unwrap();
        basis.value.parse().unwrap()
    }

    /// `measured` on a host of 64 GiB, whose memory past `stated(1)`'s floor holds every want of
    /// its node: each of its five engines' caches wants the pages of the cell's rows at its 8 GiB
    /// volume, 3.9 GB, more together than a 16 GiB host holds.
    fn roomy(mut measured: Measured) -> Measured {
        measured.memory = machine(64 << 30, None);
        measured
    }

    /// What each engine of `derived` wants for its cache at `objects` objects, at the store it runs
    /// with ([`engine_cache_wants_for`]).
    fn cache_wanted(derived: &Derived, objects: u64) -> u64 {
        let config = &derived.config;
        let engine = Engine {
            store: config.store,
            memtable_bytes: config.memtable_bytes,
            trunk: config.trunk,
        };
        engine_cache_wants_for(&engine, objects).unwrap()
    }

    /// The bound a native PUT or GET is held to (`memory.native`) is the most an operation is
    /// charged at the floor's window, what one more operation adds beside its charge, and the
    /// host's records of it; a node that hosts no objects says none.
    #[test]
    fn a_native_operations_bound_is_its_charge_its_state_and_its_records() {
        let records = u64::try_from(crate::node::record_bytes().unwrap()).unwrap();
        let three = Charges::new(1, &[durability::Scheme::Copies(3)]).unwrap();
        assert_eq!(
            native(&three, 300_000),
            Some(three.most() + 300_000 + records)
        );
        // The charge holds a put of the largest body, its window of blocks and its record of each
        // block it wrote until its file names them.
        let layout = Layout::new(durability::Scheme::Copies(3)).unwrap();
        let put = put::most_bytes(1, &layout).unwrap() + put::most_records(&layout).unwrap();
        assert!(three.most() >= put, "{} {put}", three.most());
        let hosting = stated(1);
        let charges = charges(1, hosting.enlist.as_ref(), &rates(), target(None)).unwrap();
        let derived = derive(hosting, &facts(1), &mut fresh).unwrap();
        let said = said_bytes(&derived, "memory.native");
        let beside = said - charges.most() - records;
        eprintln!(
            "memory.native {said}: charge {}, beside {beside}, records {records}",
            charges.most()
        );
        assert!(beside > 0, "{said}");
        assert_eq!(native(&charges, beside), Some(said));
        let alone = derive(stated(0), &facts(0), &mut fresh).unwrap();
        assert!(alone.bases.iter().all(|b| b.key != "memory.native"));
    }

    /// `facts(1)` with its volume's device reporting a queue of `queue` and calibrated at `depth`.
    fn deep(queue: u32, depth: usize) -> Measured {
        let mut measured = facts(1);
        let volume = measured.volumes.first_mut().unwrap();
        volume.device.queue = Some(queue);
        volume.profile.depth = depth;
        measured
    }

    /// A device's depth costs a node no thread's stack where the kernel holds its transfers
    /// (Linux): the KIND cell's pod whose device measured 316 needed its 2 MiB stacks past its
    /// 2 GiB (kind-cell run 131575146397), and here the threads class is the same at a depth of 316
    /// as at one. Elsewhere a pool's worker a transfer still counts its stack.
    #[test]
    fn a_devices_depth_costs_no_threads_stack_where_the_kernel_holds_it() {
        let threads_at = |depth| {
            let derived = derive(stated(1), &deep(316, depth), &mut fresh).unwrap();
            said_bytes(&derived, "memory.floor.threads")
        };
        let (shallow, deepest) = (threads_at(1), threads_at(316));
        if cfg!(target_os = "linux") {
            assert_eq!(shallow, deepest);
        } else {
            assert_eq!(deepest - shallow, 315 * STACK);
        }
    }

    /// A node of the membership suite granted with no volume of its own (`mantle init grant`, the
    /// founders its seeds, the four test clients pinned as gateways, the founders' 256 MiB volumes
    /// its cell's, `cell.volume`) starts on CI's three-core 7 GiB macOS runner at every depth its
    /// device may measure, its engines wanting what a founder's do there and its door serving
    /// operations past them. Counting no fragment, its engines' wants were none and they took what
    /// the operations' share left, nothing there (`DeriveError::Metadata`, CI run 44420627795),
    /// and given only the extents an engine opens with its engine was fenced at its first writes
    /// (run 44442435486). Without the cell's volume its engines still want nothing and hold only
    /// what the operations' count leaves: there, 1,679,155 bytes an engine, a tenth of the cell's
    /// rows, at every depth. It refused there while its floor counted lanes its ranges on its first
    /// shard never open, which left less than the extents an engine opens with.
    #[test]
    fn a_node_with_no_volume_on_cis_macos_runner_wants_its_cells_rows() {
        let granted = || {
            let mut s = Stated {
                s3: Some(crate::process::S3Stated {
                    listen: "127.0.0.1:7480".parse().unwrap(),
                    region: "us-east-1".into(),
                    names: Vec::new(),
                    certificate: at("/node/s3/cert.der"),
                    key: at("/node/s3/key.der"),
                    access: Vec::new(),
                }),
                enlist: Some(Enlisting {
                    root: 0x10,
                    domain: "doomed".into(),
                    max_nodes: 5,
                    max_volumes: 5,
                }),
                ..stated(1)
            };
            s.volumes.clear();
            s.cell_volume = Some(256 << 20);
            s.peers.retain(|p| p.role == Role::Node);
            for id in 6..=9 {
                s.peers
                    .push(peer(id, Role::Gateway, "/node/identity/c.der", None));
            }
            s
        };
        for depth in [4, 64, 256] {
            let mut measured = deep(256, depth);
            measured.cores = 3;
            measured.memory = machine(7 << 30, None);
            measured.volumes.clear();
            let derived = derive(granted(), &measured, &mut fresh)
                .unwrap_or_else(|e| panic!("depth {depth}: {e}"));
            // Its engines want what a founder's do on the same runner, the rows of the cell's
            // blocks at the founders' volumes (18,029,352 bytes each), and hold it, far past the
            // extents an engine opens with; its operations hold what is left.
            let per_extent =
                mantle_engine::shard_db::measured_per_extent(&derived.config.store).unwrap();
            let metadata = said_bytes(&derived, "engine.metadata_bytes");
            assert!(metadata >= per_extent * OPENS_WITH, "depth {depth}");
            assert!(metadata >= 18_029_352, "depth {depth}: {metadata}");
            assert!(
                said_bytes(&derived, "gateway.objects") >= 2,
                "depth {depth}"
            );
            // Without its cell's volume it has nothing to estimate the rows by: its engines want
            // nothing and hold what the operations' count leaves, short of the cell's rows, or it
            // refuses where that is less than the extents an engine opens with.
            let mut unstated = granted();
            unstated.cell_volume = None;
            match derive(unstated, &measured, &mut fresh) {
                Ok(derived) => {
                    let left = said_bytes(&derived, "engine.metadata_bytes");
                    assert!(left < 18_029_352, "depth {depth}: {left}");
                }
                Err(e) => assert!(
                    matches!(e, DeriveError::Metadata { .. }),
                    "depth {depth}: {e}"
                ),
            }
        }
    }

    /// A founder of the membership suite's cell (`three_of_five` made by `Cell::made`: five nodes
    /// at most and the four test clients pinned as gateways, nine identities; its five founding
    /// ranges; a volume of 256 MiB; S3 served) on CI's macOS arm64 runner, three cores and 7 GiB,
    /// its device's queue 256 and measured 256 deep, starts on every platform. CI's founder refused
    /// to start at its threads, its floor 7,718,104,000 bytes (run 44349745026), its transport
    /// charged a lane's frames to each of the four gateways, which no range's message goes to
    /// (`Node::send`); charged to the four other nodes alone, the floor holds the issuer's 257
    /// threads' stacks where a pool's worker carries each transfer (macOS, Windows). Its ranges
    /// and log, a range's entries counted at the least an entry takes since nothing bounds their
    /// count but their bytes (node.md §1.4), hold more than 5 GiB at any depth, the ranges alone
    /// more than a third of the machine: the suite's three founders, which are alike, each start,
    /// but their floors together pass the machine. Its need, its floor at the deepest its queue lets
    /// calibration measure with every want whole, holds the issuer's threads at 256 deep, and their
    /// stacks decide where it stands (`Issuer::device_threads`): where a pool's worker carries each
    /// transfer (macOS, Windows) its 256 workers' stacks take it past the machine, and no memory it
    /// states starts it there; where the kernel holds the transfers on the issuer's one thread
    /// (Linux) it is within the machine, 7,227,826,162 bytes (c1589e30), and a node given it starts.
    #[test]
    fn a_membership_founder_on_cis_macos_runner_starts_at_its_issuers_stacks() {
        let founder = || {
            let mut s = Stated {
                s3: Some(crate::process::S3Stated {
                    listen: "127.0.0.1:7480".parse().unwrap(),
                    region: "us-east-1".into(),
                    names: Vec::new(),
                    certificate: at("/node/s3/cert.der"),
                    key: at("/node/s3/key.der"),
                    access: Vec::new(),
                }),
                enlist: Some(Enlisting {
                    root: 0x10,
                    domain: "d1".into(),
                    max_nodes: 5,
                    max_volumes: 5,
                }),
                ..stated(1)
            };
            s.volumes[0].1 = VolumeBytes::Stated(256 << 20);
            s.peers.retain(|p| p.role == Role::Node);
            for id in 6..=9 {
                s.peers
                    .push(peer(id, Role::Gateway, "/node/identity/c.der", None));
            }
            s
        };
        let memory = 7u64 << 30;
        let runner = |depth| {
            let mut measured = deep(256, depth);
            measured.cores = 3;
            measured.memory = machine(memory, None);
            measured
        };
        let shallow = derive(founder(), &runner(4), &mut fresh).unwrap();
        let need = said_bytes(&shallow, "memory.need");
        // The issuer's workers at 256 deep beside its own thread: 256 where a pool carries the
        // transfers, none where the kernel holds them. Without their stacks the need is within
        // the machine on every platform; with them, past it.
        let workers =
            u64::try_from(hyper_block::issuer::Issuer::device_threads(256).unwrap() - 1).unwrap();
        assert!(need - workers * STACK <= memory, "{need}");
        let given = allowance(Some(need), &machine(memory, None));
        if workers > 0 {
            assert!(need > memory, "{need}");
            assert!(matches!(given, Err(DeriveError::Ceiling { .. })));
        } else {
            assert_eq!(given.ok(), Some(need));
        }
        let floor = |derived: &Derived, classes: &[&str]| -> u64 {
            classes
                .iter()
                .map(|class| said_bytes(derived, &format!("memory.floor.{class}")))
                .sum()
        };
        let large = floor(&shallow, &["ranges", "log"]);
        assert!(large > 5 << 30, "{large}");
        assert!(floor(&shallow, &["ranges"]) > memory / 3);
        let deep = derive(founder(), &runner(256), &mut fresh).unwrap();
        // What remains past its floor holds less than its wants: each engine's cache takes its
        // part, never more than its store's pages.
        let cache = said_bytes(&deep, "engine.cache_bytes");
        let store = &deep.config.store;
        let pages = store.max_extents * u64::from(store.extent_pages);
        eprintln!(
            "founder at depth 256: engine.cache_bytes {cache}, the store's {pages} pages of {}, \
             engine.metadata_bytes {}, volume.1.index_bytes {}, memory.remaining {}",
            store.page_size,
            said_bytes(&deep, "engine.metadata_bytes"),
            said_bytes(&deep, "volume.1.index_bytes"),
            said_bytes(&deep, "memory.remaining"),
        );
        assert!(cache <= pages * store.page_size as u64, "{cache}");
        assert_eq!(said_bytes(&deep, "memory.need"), need);
        assert_eq!(said_bytes(&deep, "memory.allowance"), memory);
        let all = floor(
            &deep,
            &[
                "objects",
                "volumes",
                "engines",
                "ranges",
                "log",
                "transport",
                "runtime",
                "threads",
            ],
        );
        assert!(all <= memory, "{all}");
        // Its engines take no maintenance workers: 7 GiB holds no five compactions' statements
        // past its need (`derive`, `ENGINE_WORKERS`). Its threads are the node's six, a shard's on
        // each of three cores, its volume's three, the issuer's own and its workers: 269 with a
        // worker for each of 256 transfers, 13 where the kernel holds them.
        let stacks = threads(3, &[256], &[true], 0).unwrap() * STACK;
        assert_eq!(stacks, (13 + workers) * STACK);
        assert_eq!(said_bytes(&deep, "engine.workers"), 0);
        assert_eq!(said_bytes(&deep, "memory.floor.threads"), stacks);
    }

    /// A founder of the repair suite's cells (crates/mantle/tests/repair.rs `founders`, made by
    /// `Cell::made`: five founders, each in a domain of its own with one or two volumes of 256 MiB,
    /// the table's bounds five nodes and four volumes a volume, the four test clients pinned as
    /// gateways, nine identities; its five founding ranges, on its first shard as unstated; S3 served
    /// at its QUIC address) on CI's macOS arm64 runner, three cores, 7 GiB and 128 control entries a
    /// shard, its directory and volumes on one device of queue 256. CI's founders refused to start:
    /// one volume measured 256 deep at its threads, 7,641,718,260 bytes, and two volumes measured
    /// shallow at their runtime, 7,610,350,228 bytes at a depth of 4. The floor counted a lane from
    /// each peer for each of the runtime's shards past the first, and two lanes to the node's
    /// shard from each of them, which run no range where the node keeps its ranges on its first: it
    /// opens its transport at one lane a peer (`Node::open_on`) and moves no range past its first
    /// (`Node::spread`). Counted at the lanes it opens, the transport's budget is the least the
    /// node's own transport takes, which its bounds admit, and the runtime holds no lane between
    /// shards: one operation's floor is 7,341,380,028 bytes with one volume at 256 and 7,254,623,836
    /// with two at 4 (macOS), within the machine. Two volumes measured past 112 deep still pass it
    /// where a pool's worker carries each transfer (macOS, Windows): 7,518,023,644 bytes at 113,
    /// 7,863,584,860 at 256, refused at their threads, as their ranges, log and transport grow with
    /// their transfers and the issuer's workers' stacks with the depth.
    #[test]
    fn a_repair_founder_on_cis_macos_runner_starts_at_the_lanes_its_ranges_open() {
        let founder = |volumes: usize| {
            let mut s = Stated {
                quic: "127.0.0.1:0".parse().unwrap(),
                plane: "127.0.0.1:0".parse().unwrap(),
                s3: Some(crate::process::S3Stated {
                    listen: "127.0.0.1:0".parse().unwrap(),
                    region: "us-east-1".into(),
                    names: Vec::new(),
                    certificate: at("/node/identity/s3.der"),
                    key: at("/node/identity/s3.key"),
                    access: Vec::new(),
                }),
                voters: vec![1, 2, 3, 4, 5],
                enlist: Some(Enlisting {
                    root: 0x10,
                    domain: "rack-0".into(),
                    max_nodes: 5,
                    max_volumes: u32::try_from(5 * volumes * 4).unwrap(),
                }),
                ..stated(volumes)
            };
            for (_, bytes) in &mut s.volumes {
                *bytes = VolumeBytes::Stated(256 << 20);
            }
            s.peers = (2..=5)
                .map(|id| peer(id, Role::Node, "/node/identity/peers/p.der", None))
                .chain((6..=9).map(|id| peer(id, Role::Gateway, "/node/identity/c.der", None)))
                .collect();
            s
        };
        let memory = 7u64 << 30;
        let runner = |volumes: usize, depth: usize| {
            let mut measured = facts(volumes);
            measured.cores = 3;
            measured.ring = 128;
            measured.memory = machine(memory, None);
            for v in &mut measured.volumes {
                v.device.name = measured.meta.name.clone();
                v.device.queue = Some(256);
                v.profile.depth = depth;
            }
            measured
        };
        let classes = [
            "objects",
            "volumes",
            "engines",
            "ranges",
            "log",
            "transport",
            "runtime",
            "threads",
        ];
        for (volumes, depth) in [(1, 4), (1, 256), (2, 4), (2, 16)] {
            let case = format!("{volumes} volumes, {depth} deep");
            // The floor at one operation, what the node is refused at where it passes the
            // allowance: a ceiling of a byte leaves the operations nothing past their first.
            let one = match derive(
                Stated {
                    ceiling: Some(1),
                    ..founder(volumes)
                },
                &runner(volumes, depth),
                &mut fresh,
            ) {
                Err(DeriveError::Memory { floor, .. }) => floor,
                other => panic!("{case}: {:?}", other.err()),
            };
            assert!(one <= memory, "{case}: {one}");
            let derived = derive(founder(volumes), &runner(volumes, depth), &mut fresh)
                .unwrap_or_else(|e| panic!("{case}: {e}"));
            let held: Vec<(&str, u64)> = classes
                .iter()
                .map(|class| {
                    (
                        *class,
                        said_bytes(&derived, &format!("memory.floor.{class}")),
                    )
                })
                .collect();
            eprintln!("{case}: one operation's floor {one}; at the machine {held:?}");
            // The node's own bounds as `Node::open_on` states them from its configuration, its
            // ranges on its first shard: a lane from each peer.
            let config = &derived.config;
            let gateway = config.gateway.as_ref().unwrap();
            let opened = mantle_protocol::bounds::Stated {
                peers: 8,
                replicas: 4,
                clients: gateway.clients,
                pool: gateway.objects,
                ranges: 5,
                inflight: config.settings.max_inflight_msgs,
                lanes: crate::node::lanes(NonZeroUsize::MIN, 5),
                transfers: config.volumes.iter().map(|v| v.config.transfers()).sum(),
                asked: config.chunk_asks,
                transport_bytes: config.transport_bytes,
                messages: messages().unwrap(),
                max_head: crate::node::MAX_HEAD,
            };
            assert_eq!(opened.lanes, 1);
            assert_eq!(
                config.transport_bytes,
                mantle_protocol::bounds::least_transport(&opened).unwrap(),
                "{case}"
            );
            assert!(mantle_protocol::bounds::limits(&opened).is_ok(), "{case}");
            // The runtime: its shards and the listener's lanes to those past the first, no lane
            // between shards and no range's Critical task past the first.
            let listening = u64::try_from(gateway.objects).unwrap();
            let shape = shard_shape(
                Some(listening),
                8,
                (RangeShards::First, NativeShards::First),
            )
            .unwrap();
            assert_eq!(
                said_bytes(&derived, "memory.floor.runtime"),
                runtime_bytes(3, &shape, 128, Some(listening), 0).unwrap(),
                "{case}"
            );
        }
        // Two volumes measured at the device's queue: the issuer's 257 threads' stacks where a pool
        // carries the transfers pass the machine; Linux's kernel holds them, and it starts.
        let deepest = derive(founder(2), &runner(2, 256), &mut fresh);
        if cfg!(target_os = "linux") {
            assert!(deepest.is_ok(), "{:?}", deepest.err());
        } else {
            assert!(
                matches!(
                    deepest,
                    Err(DeriveError::Memory {
                        class: Class::Threads,
                        ..
                    })
                ),
                "{:?}",
                deepest.err()
            );
        }
        // Spread over the shards, the same founder's ranges take a lane from each peer for each
        // shard past the first, and two lanes to the node's shard from each.
        let spread = derive(
            Stated {
                range_shards: Some(RangeShards::Spread),
                ceiling: Some(1),
                ..founder(1)
            },
            &runner(1, 4),
            &mut fresh,
        );
        let first = derive(
            Stated {
                ceiling: Some(1),
                ..founder(1)
            },
            &runner(1, 4),
            &mut fresh,
        );
        match (spread, first) {
            (
                Err(DeriveError::Memory { floor: spread, .. }),
                Err(DeriveError::Memory { floor: first, .. }),
            ) => assert!(spread > first, "{spread} {first}"),
            (spread, first) => panic!("{:?} {:?}", spread.err(), first.err()),
        }
    }

    /// What a node needs (`memory.need`) is its floor at the most its device can be measured to
    /// take, its queue, and its wants whole: the same whatever depth calibration measured; given
    /// it, a node measured at any depth up to the queue holds its index's whole want, and its
    /// engines at least what they hold at the deepest; given a byte less at the deepest, a share
    /// falls short of its want.
    #[test]
    fn a_node_given_its_need_holds_every_want_whatever_its_device_measures() {
        let need = said_bytes(
            &derive(stated(1), &deep(316, 1), &mut fresh).unwrap(),
            "memory.need",
        );
        let given = |ceiling, depth| {
            let stated = Stated {
                ceiling: Some(ceiling),
                ..stated(1)
            };
            derive(stated, &deep(316, depth), &mut fresh).unwrap()
        };
        // The machine's 16 GiB past every floor: each index takes its want whole.
        let index_want = said_bytes(
            &derive(stated(1), &deep(316, 316), &mut fresh).unwrap(),
            "volume.1.index_bytes",
        );
        let at_deepest = given(need, 316);
        for depth in [1, 4, 64, 316] {
            let derived = given(need, depth);
            assert_eq!(said_bytes(&derived, "memory.need"), need, "depth {depth}");
            assert_eq!(
                said_bytes(&derived, "volume.1.index_bytes"),
                index_want,
                "the index's want whole at depth {depth}"
            );
            assert!(
                said_bytes(&derived, "engine.metadata_bytes")
                    >= said_bytes(&at_deepest, "engine.metadata_bytes"),
                "depth {depth}"
            );
        }
        let short = given(need - 1, 316);
        assert!(
            said_bytes(&short, "volume.1.index_bytes") < index_want,
            "a byte short of the need, the index's share falls short of its want"
        );
    }

    /// A node with an S3 endpoint needs as many operations as one stock client asks at once
    /// (`listener::STOCK_CLIENT`), each a part of its own (`listener::STOCK_PART`): given its need,
    /// at its device's queue it runs at least that many, so its listener serves every connection a
    /// default AWS CLI or boto3 opens, its admission takes all of their parts at once at the most
    /// a part is charged, and its index holds its whole want; a byte short, the index stays whole,
    /// the data first (research/87 §4 rule D). Given the first operation alone, a pod at its
    /// device's queue served one connection and boto3's parts timed out (kind-cell); given one
    /// part's bytes and nine operations' least, it admitted one part at a time and the last timed
    /// out (kind-cell, CI run 43790965629).
    #[test]
    fn a_node_given_its_need_serves_one_stock_clients_connections() {
        let s3 = |ceiling| Stated {
            ceiling,
            s3: Some(crate::process::S3Stated {
                listen: "127.0.0.1:7480".parse().unwrap(),
                region: "us-east-1".into(),
                names: Vec::new(),
                certificate: at("/node/s3/cert.der"),
                key: at("/node/s3/key.der"),
                access: Vec::new(),
            }),
            ..stated(1)
        };
        let need = said_bytes(
            &derive(s3(None), &deep(316, 1), &mut fresh).unwrap(),
            "memory.need",
        );
        let index_want = said_bytes(
            &derive(s3(None), &deep(316, 316), &mut fresh).unwrap(),
            "volume.1.index_bytes",
        );
        let stock = crate::listener::STOCK_CLIENT;
        let given = derive(s3(Some(need)), &deep(316, 316), &mut fresh).unwrap();
        assert!(said_bytes(&given, "gateway.objects") >= stock);
        assert_eq!(said_bytes(&given, "volume.1.index_bytes"), index_want);
        // The charges the node's process reckons from its file (`process::gateway_settings`).
        let gateway = given.config.gateway.as_ref().unwrap();
        let rates = Rates {
            chunk: gateway.chunk_rate,
            domains: None,
            bursts: gateway.bursts.clone(),
            repair: gateway.repair_rate,
        };
        let charges = charges(
            gateway.window,
            given.config.enlist.as_ref(),
            &rates,
            gateway.target,
        )
        .unwrap();
        let part = stock_part(&charges);
        assert!(part > charges.least());
        let mut admission = crate::admit::Admission::new(gateway.objects, gateway.objects_bytes, 0);
        for n in 0..stock {
            assert_eq!(admission.admits(part), Ok(()), "part {n} of {stock}");
            admission.admitted(part);
        }
        let short = derive(s3(Some(need - 1)), &deep(316, 316), &mut fresh).unwrap();
        assert_eq!(said_bytes(&short, "volume.1.index_bytes"), index_want);
    }

    /// Where the process's descriptors past those the node holds whatever its operations hold fewer
    /// operations than its memory does, they bind, and the node says so: it runs as many as they
    /// hold, its S3 listener serves as many connections and it admits as many native clients, the
    /// descriptors it holds at its most within the limit. Where its memory holds fewer, the
    /// descriptors change nothing. A limit short of the fixed descriptors and one operation's
    /// refuses the start.
    #[test]
    fn the_descriptor_limit_bounds_the_operations_where_it_is_the_smaller() {
        let s3 = Stated {
            s3: Some(crate::process::S3Stated {
                listen: "127.0.0.1:7480".parse().unwrap(),
                region: "us-east-1".into(),
                names: Vec::new(),
                certificate: at("/node/s3/cert.der"),
                key: at("/node/s3/key.der"),
                access: Vec::new(),
            }),
            ..stated(1)
        };
        let kind = |derived: &Derived| {
            derived
                .bases
                .iter()
                .find(|b| b.key == "descriptors.operations")
                .unwrap()
                .kind
        };
        let roomy = derive(s3.clone(), &facts(1), &mut fresh).unwrap();
        let by_memory = said_bytes(&roomy, "gateway.objects");
        let fixed = said_bytes(&roomy, "descriptors.fixed");
        let each = said_bytes(&roomy, "descriptors.each");
        assert_eq!(
            each, 3,
            "a native client's two sockets and an S3 connection"
        );
        assert!(by_memory >= 2, "{by_memory} operations");
        assert_eq!(kind(&roomy), Kind::Derived);
        assert!(said_bytes(&roomy, "descriptors.operations") >= by_memory);

        let most = by_memory / 2;
        let limit = fixed + each * most + (each - 1);
        let bound = derive(
            s3.clone(),
            &Measured {
                descriptors: limit,
                ..facts(1)
            },
            &mut fresh,
        )
        .unwrap();
        assert_eq!(said_bytes(&bound, "descriptors.limit"), limit);
        assert_eq!(said_bytes(&bound, "descriptors.operations"), most);
        assert_eq!(said_bytes(&bound, "gateway.objects"), most);
        assert_eq!(kind(&bound), Kind::Bound);
        let gateway = bound.config.gateway.as_ref().unwrap();
        assert_eq!(u64::try_from(gateway.clients).unwrap(), most);
        assert!(fixed + each * u64::try_from(gateway.objects).unwrap() <= limit);

        let short = Measured {
            descriptors: fixed + each - 1,
            ..facts(1)
        };
        assert!(matches!(
            derive(s3, &short, &mut fresh),
            Err(DeriveError::Descriptors { .. })
        ));
    }

    /// `mantle node` runs the operations it is told: within the limit its start says the
    /// descriptors they take, past it the start is refused, naming them; `derive`'s own bound is
    /// within it at its limit.
    #[test]
    fn a_told_node_past_its_descriptors_is_refused() {
        let s3 = Stated {
            s3: Some(crate::process::S3Stated {
                listen: "127.0.0.1:7480".parse().unwrap(),
                region: "us-east-1".into(),
                names: Vec::new(),
                certificate: at("/node/s3/cert.der"),
                key: at("/node/s3/key.der"),
                access: Vec::new(),
            }),
            ..stated(1)
        };
        let measured = Measured {
            descriptors: 1_024,
            ..facts(1)
        };
        let derived = derive(s3.clone(), &measured, &mut fresh).unwrap();
        let objects = derived.config.gateway.as_ref().unwrap().objects;
        let check = |operations| {
            within_descriptors(
                1_024,
                (measured.cores, s3.volumes.len(), s3.ranges.len()),
                (&s3.peers, s3.seeds.len(), s3.enlist.as_ref()),
                true,
                operations,
            )
        };
        let held = check(objects).unwrap();
        assert!(held <= 1_024, "{held}");
        assert_eq!(
            held,
            said_bytes(&derived, "descriptors.fixed")
                + said_bytes(&derived, "descriptors.each") * u64::try_from(objects).unwrap()
        );
        let past = usize::try_from(said_bytes(&derived, "descriptors.operations")).unwrap() + 1;
        match check(past) {
            Err(DeriveError::Descriptors {
                limit, operations, ..
            }) => assert_eq!((limit, operations), (1_024, u64::try_from(past).unwrap())),
            other => panic!("{past} operations within 1,024 descriptors: {other:?}"),
        }
    }

    /// A node holds a fixed descriptor for each shard's poller, volume and range more, and its
    /// peers' sockets, two a peer; its S3 listener adds its own socket, its refusals' and the one
    /// it closes as it comes, and each operation one connection.
    #[test]
    fn the_fixed_descriptors_grow_with_what_the_node_holds() {
        let base = fixed_descriptors(1, 1, 1, 1, false).unwrap();
        let shard = fixed_descriptors(2, 1, 1, 1, false).unwrap() - base;
        assert!((1..=3).contains(&shard), "{shard} a shard");
        assert!(fixed_descriptors(1, 2, 1, 1, false).unwrap() - base >= VOLUME_DESCRIPTORS);
        assert_eq!(fixed_descriptors(1, 1, 2, 1, false).unwrap() - base, 1);
        assert_eq!(fixed_descriptors(1, 1, 1, 2, false).unwrap() - base, 2);
        assert_eq!(
            fixed_descriptors(1, 1, 1, 1, true).unwrap() - base,
            LISTENER_DESCRIPTORS + crate::listener::REFUSING
        );
        assert_eq!(operation_descriptors(false), Some(2));
        assert_eq!(operation_descriptors(true), Some(3));
        assert_eq!(descriptor_operations(1_024, 100, 3), 308);
        assert_eq!(descriptor_operations(99, 100, 3), 0);
    }

    /// The floor's allowance is a stated ceiling where one is: the node refuses one past what the OS
    /// lets it hold, and one below its floor, naming the class that crosses it. The data comes
    /// first (research/87 §4 rule D): what remains past the floor is the indexes' and engines'
    /// wants, each taken whole, and the operations hold what the memory holds past both, so a
    /// lower ceiling runs fewer operations before any index holds fewer fragments.
    #[test]
    fn a_stated_ceiling_bounds_the_floor_and_what_remains() {
        let host = roomy(facts(1));
        let full = derive(stated(1), &host, &mut fresh).unwrap();
        let bytes = |derived: &Derived, key: &str| -> u64 {
            let basis = derived.bases.iter().find(|b| b.key == key).unwrap();
            basis.value.parse().unwrap()
        };
        let floor = |derived: &Derived| -> u64 {
            [
                "objects",
                "volumes",
                "engines",
                "ranges",
                "log",
                "transport",
                "runtime",
                "threads",
            ]
            .iter()
            .map(|class| bytes(derived, &format!("memory.floor.{class}")))
            .sum()
        };
        let at_full = floor(&full);
        assert_eq!(bytes(&full, "memory.allowance"), 64 << 30);
        assert_eq!(bytes(&full, "memory.remaining"), (64 << 30) - at_full);
        // What remains is the data's wants, each whole: the index's and each engine's metadata's.
        let wanted =
            bytes(&full, "volume.1.index_bytes") + 5 * bytes(&full, "engine.metadata_bytes");
        assert!(wanted <= bytes(&full, "memory.remaining"));
        let ceiling = |bytes| Stated {
            ceiling: Some(bytes),
            ..stated(1)
        };
        assert!(matches!(
            derive(ceiling((64 << 30) + 1), &host, &mut fresh),
            Err(DeriveError::Ceiling { .. })
        ));
        assert!(matches!(
            derive(ceiling(1), &host, &mut fresh),
            Err(DeriveError::Memory { floor, .. }) if floor > 1
        ));
        // A ceiling at the full allowance's floor: the operations give way, the data keeps its
        // wants.
        let tight = derive(ceiling(at_full), &host, &mut fresh).unwrap();
        let at_tight = floor(&tight);
        assert!(at_tight < at_full);
        assert_eq!(bytes(&tight, "memory.remaining"), at_full - at_tight);
        assert!(bytes(&tight, "gateway.objects") < bytes(&full, "gateway.objects"));
        assert_eq!(
            tight
                .bases
                .iter()
                .find(|b| b.key == "memory.allowance")
                .unwrap()
                .kind,
            Kind::Stated
        );
        assert_eq!(
            tight.config.volumes[0].config.max_fragments,
            full.config.volumes[0].config.max_fragments
        );
        assert_eq!(tight.config.cache_bytes, full.config.cache_bytes);
    }

    /// However large the node directory's file system, each index takes its whole want while what
    /// remains holds every want: an engine wants the rows of the objects the cell's volumes hold, not
    /// its device's share, and takes what no want does (the box's 16 GB pass gave each index 33.6 MB
    /// beside an 8 TB file system, 2026-10-30).
    #[test]
    fn each_index_takes_its_whole_want_beside_a_file_system_of_any_size() {
        let mut measured = facts(1);
        measured.meta.bytes = Some(8 << 40);
        let derived = derive(stated(1), &measured, &mut fresh).unwrap();
        let bytes = |key: &str| -> u64 {
            let basis = derived.bases.iter().find(|b| b.key == key).unwrap();
            basis.value.parse().unwrap()
        };
        let fragments = (8u64 << 30) / 4096;
        assert_eq!(bytes("volumes.fragments"), fragments);
        // A cell of three: a chunk of every block on each node, so a block for each fragment.
        assert_eq!(bytes("engine.objects"), fragments);
        let align = Alignment::new(4096).unwrap();
        assert_eq!(
            bytes("volume.1.index_bytes"),
            index_wants(align, 8 << 30).unwrap()
        );
        assert_eq!(derived.config.volumes[0].config.max_fragments, fragments);
        // The engines take the rest, each its metadata's want and an equal part of what no want
        // took; the caches take no share.
        let given = bytes("volume.1.index_bytes") + 5 * bytes("engine.metadata_bytes");
        let remaining = bytes("memory.remaining");
        assert!(given <= remaining && remaining - given < 5);
    }

    /// Rule D's order (research/87 §4): on a host whose memory holds every want and on a 16 GiB
    /// host alike, the index and the engines' metadata take their whole wants, the transport its
    /// want whole, every operation's client's windows at a receive chunk, and the operations
    /// everything past those, no more on the smaller host: on 16 GiB 10,999 on macOS and 10,860 on
    /// Linux ([`runs`]), 1.77 GB past the transport's floor, and each engine's metadata its want.
    /// Charged the difference from one operation to two, it ran 10,396 and 9,264 (c1589e30):
    /// each of the 9,375 and 8,243 past the receive batch's 1,024 trains was charged a train no
    /// batch holds, 65,592 bytes on macOS and 197,752 on Linux
    /// ([`a_receive_batch_holds_a_train_a_client_to_its_cap_and_no_more`]), 0.61 GB and 1.63 GB
    /// that went to the engines' metadata. No cache takes a share: `engine.cache_bytes` is each
    /// cache's want, the most it may borrow of the operations' idle bytes, the same on either host.
    /// The classes the operations and the transport do not size are the same on either host.
    #[test]
    fn the_caches_take_no_share_and_the_operations_take_what_the_wants_leave() {
        let roomy = derive(stated(1), &roomy(facts(1)), &mut fresh).unwrap();
        let small = derive(stated(1), &facts(1), &mut fresh).unwrap();
        let objects = said_bytes(&roomy, "engine.objects");
        let want = cache_wanted(&roomy, objects);
        for derived in [&roomy, &small] {
            assert_eq!(said_bytes(derived, "engine.cache_bytes"), want);
            assert_eq!(derived.config.cache_bytes, want);
        }
        assert_eq!(
            said_bytes(&small, "volume.1.index_bytes"),
            said_bytes(&roomy, "volume.1.index_bytes"),
            "the index's want whole"
        );
        let metadata = {
            let config = &small.config;
            let engine = Engine {
                store: config.store,
                memtable_bytes: config.memtable_bytes,
                trunk: config.trunk,
            };
            engine_wants_for(&engine, objects).unwrap()
        };
        assert_eq!(
            said_bytes(&small, "engine.metadata_bytes"),
            metadata,
            "each engine's metadata its want whole, and what no want takes the operations'"
        );
        let (floor, want_past, bytes) = transport(&small);
        let (roomy_floor, _, roomy_bytes) = transport(&roomy);
        eprintln!(
            "16 GiB: index {}, each metadata {}, each cache's want {want}, the transport's growth \
             {} of {want_past}, operations {} of {}, gateway.objects_bytes {}",
            said_bytes(&small, "volume.1.index_bytes"),
            said_bytes(&small, "engine.metadata_bytes"),
            bytes - floor,
            said_bytes(&small, "gateway.objects"),
            said_bytes(&roomy, "gateway.objects"),
            said_bytes(&small, "gateway.objects_bytes"),
        );
        // Every share at a receive chunk on either host: the transport's want whole.
        assert_eq!(bytes - floor, want_past);
        let (_, roomy_want, _) = transport(&roomy);
        assert_eq!(roomy_bytes - roomy_floor, roomy_want);
        let operations = said_bytes(&small, "gateway.objects");
        assert!(
            runs(10_350..11_350, 10_500..11_500).contains(&operations),
            "{operations}"
        );
        assert!(operations <= said_bytes(&roomy, "gateway.objects"));
        for class in ["volumes", "engines", "log", "threads"] {
            let key = format!("memory.floor.{class}");
            assert_eq!(
                said_bytes(&small, &key),
                said_bytes(&roomy, &key),
                "{class}"
            );
        }
    }

    /// A founder of the ladder's cell (`mantle bench doors`, `transfer::cell::Cell::make`: three
    /// founders, each a 32 GiB volume and its S3 endpoint) on a host of the Mac Studio's 96 GiB and
    /// 32 cores holds every want whole, each engine's cache every page of the cell's rows, and runs
    /// at least the operations one stock client asks at once past them.
    #[test]
    fn a_ladder_founder_on_a_96_gib_host_caches_every_page_of_its_rows() {
        let mut founder = Stated {
            s3: Some(crate::process::S3Stated {
                listen: "127.0.0.1:7480".parse().unwrap(),
                region: "us-east-1".into(),
                names: Vec::new(),
                certificate: at("/node/s3/cert.der"),
                key: at("/node/s3/key.der"),
                access: Vec::new(),
            }),
            ..stated(1)
        };
        founder.volumes[0].1 = VolumeBytes::Stated(32 << 30);
        let mut measured = facts(1);
        measured.memory = machine(96 << 30, None);
        measured.cores = 32;
        let derived = derive(founder, &measured, &mut fresh).unwrap();
        let objects = said_bytes(&derived, "engine.objects");
        let cache = said_bytes(&derived, "engine.cache_bytes");
        eprintln!(
            "96 GiB, 32 cores: engine.objects {objects}, engine.cache_bytes {cache} ({} pages of \
             {}), engine.metadata_bytes {}, volume.1.index_bytes {}, gateway.objects {}, \
             gateway.objects_bytes {}, memory.remaining {}, memory.need {}",
            cache / derived.config.store.page_size as u64,
            derived.config.store.page_size,
            said_bytes(&derived, "engine.metadata_bytes"),
            said_bytes(&derived, "volume.1.index_bytes"),
            said_bytes(&derived, "gateway.objects"),
            said_bytes(&derived, "gateway.objects_bytes"),
            said_bytes(&derived, "memory.remaining"),
            said_bytes(&derived, "memory.need"),
        );
        assert_eq!(objects, (32 << 30) / 4096);
        assert_eq!(cache, cache_wanted(&derived, objects));
        assert!(said_bytes(&derived, "gateway.objects") >= crate::listener::STOCK_CLIENT);
    }

    /// A range's engine opened as the node opens it (`process::engine`), from the configuration
    /// `mantle serve` derives, with no cache, and given its cache's want, `engine.cache_bytes` over
    /// its page: over
    /// 1,100 rows of a Name range's size, a get and a one-row lookup repeated read no page from the
    /// device once read, created or opened again, where with no cache each reads its pages from
    /// the device every time (`IoStats::pages_read`, the store's reads of the device).
    #[test]
    fn a_range_opened_as_the_node_opens_it_reads_its_pages_from_its_derived_cache() {
        use hyper_block::file::DeviceFile;
        use mantle_meta::engine::{Engine as _, Rows as _, Write};
        use mantle_range::Stored;
        const ROWS: u64 = 1_100;
        const ASKED: u64 = 100;
        let dir = tempfile::tempdir().unwrap();
        let mut config = derive(stated(1), &facts(1), &mut fresh).unwrap().config;
        config.dir = dir.path().to_path_buf();
        let page = config.store.page_size;
        let pages = usize::try_from(config.cache_bytes).unwrap() / page;
        assert!(pages > 0, "{}", config.cache_bytes);
        let row = |i: u64| {
            (
                format!("v/b/k{i:06}/{}", "0".repeat(24)).into_bytes(),
                vec![u8::try_from(i % 251).unwrap(); 120],
            )
        };
        let group = 0x12;
        let mut e = crate::process::engine(&config, group, Layer::Name).unwrap();
        assert_eq!(e.io_stats().cache_limit_pages, 0);
        e.set_cache(pages);
        assert_eq!(
            e.io_stats().cache_limit_pages,
            u64::try_from(pages).unwrap()
        );
        for i in 0..ROWS {
            let (k, v) = row(i);
            e.apply(i + 1, vec![Write::Put(k, v)]).unwrap();
        }
        e.persist().unwrap();
        let (key, value) = row(ROWS / 2);
        let (from, to) = (key.clone(), row(ROWS / 2 + 1).0);
        // The device reads `ask` makes, `ASKED` times, past its first.
        let reads = |e: &mut Stored<DeviceFile>, ask: &dyn Fn(&mut Stored<DeviceFile>)| {
            ask(e);
            let before = e.io_stats().pages_read;
            for _ in 0..ASKED {
                ask(e);
            }
            e.io_stats().pages_read - before
        };
        let get = |e: &mut Stored<DeviceFile>| {
            assert_eq!(e.get(&key).unwrap().as_deref(), Some(&value[..]));
        };
        let next = |e: &mut Stored<DeviceFile>| {
            assert_eq!(
                e.next(&from, &to).unwrap(),
                Some((key.clone(), value.clone()))
            );
        };
        assert_eq!(reads(&mut e, &get), 0, "gets of a range created");
        assert_eq!(reads(&mut e, &next), 0, "lookups of a range created");
        // With no cache, each reads its pages from the device every time.
        e.set_cache(0);
        let (gets, lookups) = (reads(&mut e, &get), reads(&mut e, &next));
        eprintln!(
            "{ASKED} gets read {gets} pages and {ASKED} lookups {lookups} with no cache, none \
             with {pages} pages of {page} bytes"
        );
        assert!(gets >= ASKED && lookups >= ASKED, "{gets} {lookups}");
        drop(e);
        let mut e = crate::process::engine(&config, group, Layer::Name).unwrap();
        assert_eq!(e.io_stats().cache_limit_pages, 0);
        e.set_cache(pages);
        assert_eq!(
            e.io_stats().cache_limit_pages,
            u64::try_from(pages).unwrap()
        );
        assert_eq!(reads(&mut e, &get), 0, "gets of a range opened again");
        assert_eq!(reads(&mut e, &next), 0, "lookups of a range opened again");
    }

    /// The ranges' caches borrow the operations' idle bytes (node.md §1.4; `admit::cache_part`,
    /// `admit::lend_to`, as `node::host::lend` lends them): over random admissions, ends, reads
    /// and turns, three engines opened as the node opens them, the operations' charges and the
    /// caches' bytes are within `gateway.objects_bytes` at every step, each cache shrinking at once
    /// before an operation is charged what it holds, its memory within its part, and at each turn
    /// back at its part of what is idle: its want where the idle bytes hold every want, else the
    /// same part of them. Once every operation ended each holds its want; a cache whose bytes an
    /// operation took reads its pages from the device, and grown back once the operation ended,
    /// reads its hot pages from memory.
    #[test]
    fn the_caches_borrow_the_operations_idle_bytes_and_give_them_back_before_they_are_charged() {
        use crate::admit::{Admission, cache_part, lend_to};
        use hyper_block::file::DeviceFile;
        use mantle_meta::engine::{Engine as _, Rows as _, Write};
        use mantle_range::Stored;
        const ROWS: u64 = 1_100;
        const ASKED: u64 = 100;
        let dir = tempfile::tempdir().unwrap();
        let mut config = derive(stated(1), &facts(1), &mut fresh).unwrap().config;
        config.dir = dir.path().to_path_buf();
        let page = u64::try_from(config.store.page_size).unwrap();
        // Each cache wants 16 pages here, and the operations' memory is 64: three caches' wants
        // whole only while at most 16 pages are charged.
        let want = 16 * page;
        let budget = 64 * page;
        let row = |i: u64| {
            (
                format!("v/b/k{i:06}/{}", "0".repeat(24)).into_bytes(),
                vec![u8::try_from(i % 251).unwrap(); 120],
            )
        };
        let mut caches: Vec<(u64, Stored<DeviceFile>)> = (0..3u128)
            .map(|n| {
                let mut e = crate::process::engine(&config, 0x20 + n, Layer::Name).unwrap();
                for i in 0..ROWS {
                    let (k, v) = row(i);
                    e.apply(i + 1, vec![Write::Put(k, v)]).unwrap();
                }
                e.persist().unwrap();
                (0, e)
            })
            .collect();
        let lend = |admission: &mut Admission,
                    caches: &mut [(u64, Stored<DeviceFile>)],
                    more: u64,
                    grow: bool| {
            let part = cache_part(admission.idle_past(more), want, caches.len());
            let mut lent = 0;
            for (held, e) in caches.iter_mut() {
                *held = lend_to(e, *held, part, grow);
                lent += *held;
            }
            admission.lend(lent);
        };
        let mut admission = Admission::new(1_000, budget, 0);
        let mut running: Vec<u64> = Vec::new();
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        for step in 0..3_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            match x % 4 {
                0 => {
                    let charge = (1 + (x >> 8) % 24) * page;
                    if admission.admits(charge).is_ok() {
                        lend(&mut admission, &mut caches, charge, false);
                        admission.admitted(charge);
                        running.push(charge);
                    }
                }
                1 if !running.is_empty() => {
                    let at = usize::try_from((x >> 8) % running.len() as u64).unwrap();
                    let charge = running.swap_remove(at);
                    admission.completed(1, 1, charge);
                }
                2 => {
                    // A turn: each cache back to its part of what is idle, its want at most.
                    lend(&mut admission, &mut caches, 0, true);
                    let part = cache_part(budget - admission.lent().1, want, caches.len());
                    assert!(caches.iter().all(|(held, _)| *held == part / page * page));
                }
                _ => {
                    for (_, e) in caches.iter_mut() {
                        let (k, v) = row((x >> 8) % ROWS);
                        assert_eq!(e.get(&k).unwrap(), Some(v));
                    }
                }
            }
            let (lent, held) = admission.lent();
            assert!(held + lent <= budget, "step {step}: {held} + {lent}");
            assert_eq!(held, running.iter().sum::<u64>(), "step {step}");
            for (part, e) in &caches {
                let stats = e.io_stats();
                assert_eq!(stats.cache_limit_pages * page, *part, "step {step}");
                assert!(stats.cache_bytes <= *part, "step {step}");
            }
        }
        for charge in running.drain(..) {
            admission.completed(1, 1, charge);
        }
        lend(&mut admission, &mut caches, 0, true);
        // Every operation ended: 64 pages idle over three caches hold each its want, 16 pages.
        let idle = cache_part(budget, want, caches.len()) / page * page;
        assert_eq!(idle, want);
        assert!(caches.iter().all(|(part, _)| *part == want));
        assert_eq!(admission.lent(), (3 * want, 0));
        // The device reads 100 gets of one row make past the first.
        let (key, value) = row(ROWS / 2);
        let reads = |e: &mut Stored<DeviceFile>| {
            assert_eq!(e.get(&key).unwrap().as_deref(), Some(&value[..]));
            let before = e.io_stats().pages_read;
            for _ in 0..ASKED {
                assert_eq!(e.get(&key).unwrap().as_deref(), Some(&value[..]));
            }
            e.io_stats().pages_read - before
        };
        assert_eq!(reads(&mut caches[0].1), 0, "hot pages, cached");
        // An operation charged every byte: the caches give back theirs before it is.
        lend(&mut admission, &mut caches, budget, false);
        admission.admitted(budget);
        assert_eq!(admission.lent(), (0, budget));
        assert!(caches.iter().all(|(part, _)| *part == 0));
        let cold = reads(&mut caches[0].1);
        assert!(cold >= ASKED, "{cold}");
        // It ends, and the turn lends the idle bytes again: the hot pages are read from memory.
        admission.completed(1, 1, budget);
        lend(&mut admission, &mut caches, 0, true);
        assert_eq!(admission.lent(), (3 * want, 0));
        assert_eq!(reads(&mut caches[0].1), 0, "hot pages, cached again");
        eprintln!(
            "100 gets read {cold} pages while an operation held every byte, none once its cache \
             grew back to {} pages of {page}",
            idle / page
        );
    }

    /// A node restarted on stores it made keeps their page and extent sizes, and admits the extents
    /// they hold however few its share now holds; stores of two shapes, or of a page that holds no
    /// row at the bounds, are refused.
    #[test]
    fn stores_made_already_keep_their_shape_and_their_extents() {
        let derived = derive(stated(1), &facts(1), &mut fresh).unwrap();
        let page = derived.config.store.page_size;
        let wider = page + 4096;
        let held = derived.config.store.max_extents + 10;
        let mut made = |group: u128, asked: usize| -> Result<Option<Shape>, String> {
            assert_eq!(asked, page);
            let shape = Shape {
                page_size: wider,
                extent_pages: 2,
                extents: held,
            };
            Ok(if group == 0x14 { None } else { Some(shape) })
        };
        let again = derive(stated(1), &facts(1), &mut made).unwrap();
        assert_eq!(again.config.store.page_size, wider);
        assert_eq!(again.config.store.max_extents, held);
        let mut two = |group: u128, _: usize| -> Result<Option<Shape>, String> {
            Ok(Some(Shape {
                page_size: if group == 0x10 { page } else { wider },
                extent_pages: 2,
                extents: 3,
            }))
        };
        assert!(matches!(
            derive(stated(1), &facts(1), &mut two),
            Err(DeriveError::StorePages { .. })
        ));
        let mut small = |_: u128, _: usize| -> Result<Option<Shape>, String> {
            Ok(Some(Shape {
                page_size: 4096,
                extent_pages: 2,
                extents: 3,
            }))
        };
        assert!(matches!(
            derive(stated(1), &facts(1), &mut small),
            Err(DeriveError::StorePage { page: 4096, .. })
        ));
        let mut unread = |group: u128, _: usize| -> Result<Option<Shape>, String> {
            Err(format!("store {group:x} does not read"))
        };
        assert!(matches!(
            derive(stated(1), &facts(1), &mut unread),
            Err(DeriveError::Store { group: 0x10, .. })
        ));
    }

    /// A volume laid out already keeps its layout; a node of no volumes and no root keys hosts no
    /// objects and asks nothing itself; and a node directory too small for its log is refused.
    #[test]
    fn a_laid_out_volume_keeps_its_layout_and_a_bare_node_asks_nothing() {
        let align = Alignment::new(4096).unwrap();
        let first = volume(
            align,
            8 << 30,
            mantle_gateway::layout::CHUNK,
            &PROFILE,
            1 << 20,
            None,
        )
        .unwrap();
        let wider = VolumeConfig {
            segment_size: first.segment_size * 2,
            ..first
        };
        let mut laid = facts(1);
        laid.volumes[0].formatted = Some(Formatted {
            geometry: Geometry::plan(8 << 30, align, &wider).unwrap(),
            checksum_shift: 12,
        });
        laid.volumes[0].measured_now = false;
        let derived = derive(stated(1), &laid, &mut fresh).unwrap();
        assert_eq!(
            derived.config.volumes[0].config.segment_size,
            wider.segment_size
        );
        let depth = derived
            .bases
            .iter()
            .find(|b| b.key == "volume.1.depth")
            .unwrap();
        assert!(depth.why.contains("kept"), "{depth}");

        let bare = derive(stated(0), &facts(0), &mut fresh).unwrap();
        let config = &bare.config;
        assert!(config.gateway.is_none() && config.enlist.is_none() && config.volumes.is_empty());
        assert_eq!(
            (config.chunk_asks, config.command_asks, config.read_asks),
            (0, 0, 0)
        );
        assert!(!bare.bases.iter().any(|b| b.key.starts_with("gateway.")));

        let mut small = facts(0);
        small.meta.bytes = Some(1 << 20);
        assert!(matches!(
            derive(stated(0), &small, &mut fresh),
            Err(DeriveError::Space { .. })
        ));
        small.meta.bytes = None;
        assert!(matches!(
            derive(stated(0), &small, &mut fresh),
            Err(DeriveError::Unsized)
        ));
    }
}
