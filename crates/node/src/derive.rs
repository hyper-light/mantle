//! What `mantle serve` runs with that its operator does not state (docs/design/node.md §1.4),
//! derived from what it states, what it measures of its host and devices, and the code's own
//! bounds, each class of work at its floor: a share past a class's floor waits for its knee,
//! measured at the scale step (node.md §9.3). Each value carries its basis, which the node says
//! at start.

use std::mem::size_of;
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
use mantle_ec::durability::{self, Burst, Choice, DurabilityError, Rates, Scheme};
use mantle_engine::store::Config as StoreConfig;
use mantle_engine::trunk::TrunkConfig;
use mantle_gateway::layout::Layout;
use mantle_gateway::{get, put};
use mantle_meta::record::{RecordError, Session};
use mantle_meta::session::Rules;
use mantle_meta::{key, wire};
use mantle_range::{Range, Settings};

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

/// Hours to repair a lost chunk until repair is built and measured: "a day—or more" after a
/// power-on restart (Chansler 2012), the longest the sources give (docs/research/15 §8.4).
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

/// The rates a block's scheme is chosen by until mantle measures its own (docs/design/durability.md
/// §5): each chunk lost at the worst device class's annual rate, since a block's chunks may land
/// on any device of the cell; a power-on restart a year, destroying a hundredth of the nodes;
/// and repair in a day. No permanent loss of a rack, zone or site has a rate the sources give.
pub fn rates() -> Rates {
    use durability::field::{DISK_FAILURES, POWER_LOSS_FRACTION, POWER_LOSSES};
    Rates {
        chunk: DISK_FAILURES / durability::YEAR,
        domains: None,
        bursts: vec![Burst {
            rate: POWER_LOSSES / durability::YEAR,
            fraction: POWER_LOSS_FRACTION,
        }],
        repair: REPAIR_HOURS.recip(),
    }
}

/// A block's annual chance of loss at most: what the operator states, or S3's eleven nines.
pub fn target(stated: Option<f64>) -> f64 {
    stated.unwrap_or(TARGET)
}

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

/// What a node that hosts objects runs its drivers with, at the floor: a window of one block;
/// two operations at once, of any kind, so a put and a get; S3's quota of buckets; one row each
/// step of a bucket's attempt passes or removes, the least that moves it; HDFS's handover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gateway {
    pub window: usize,
    pub objects: usize,
    pub bucket_quota: u32,
    pub scan_rows: u32,
    pub handover_ns: u64,
}

pub fn gateway() -> Result<Gateway, DeriveError> {
    Ok(Gateway {
        window: 1,
        objects: 2,
        bucket_quota: BUCKET_QUOTA,
        scan_rows: 1,
        handover_ns: u64::try_from(HANDOVER.as_nanos())
            .map_err(|_| DeriveError::Overflow("the handover"))?,
    })
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

/// The asks a node of `gateway` holds at most, each operation it hosts the most of the schemes
/// its blocks may take (`schemes`): a put's window of chunks at their width, or a get's spans and
/// the whole chunks a failed span adds (`put::most_chunks`, `get::most_chunks`); a put's command
/// for each block of the largest body S3 takes (`put::most_commands`); one read an operation; and
/// one command besides, the node's own enlistment.
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

/// The most bytes the objects a node hosts hold at once: `gateway.objects` operations of any
/// kind, each the larger of a put and a get of the largest blocks the schemes its blocks may take
/// make. Each is the driver's own (`put::most_bytes`, `get::most_bytes`) and the host's: a put's
/// chunks copied to send, every chunk of every block going down, and a piece of its body carried;
/// a get's block taken and written out. A scheme that codes adds the coding library's tables,
/// once a process (`mantle_ec::TABLE_BYTES`).
pub fn objects(gateway: &Gateway, schemes: &[Scheme]) -> Result<u64, DeriveError> {
    let overflow = || DeriveError::Overflow("an operation's bytes");
    let piece = u64::try_from(hyper_transport::RECEIVE_CHUNK).map_err(|_| overflow())?;
    let window = u64::try_from(gateway.window).map_err(|_| overflow())?;
    let (mut most, mut codes) = (0u64, false);
    for &scheme in schemes {
        let layout = Layout::new(scheme).map_err(|_| overflow())?;
        let block = layout.largest_block().ok_or_else(overflow)?;
        let chunk = layout.chunk_len(block).map_err(|_| overflow())?;
        let copies = u64::try_from(scheme.width())
            .ok()
            .and_then(|width| width.checked_mul(chunk)?.checked_mul(window))
            .ok_or_else(overflow)?;
        let put = put::most_bytes(gateway.window, &layout)
            .and_then(|bytes| bytes.checked_add(copies)?.checked_add(piece))
            .ok_or_else(overflow)?;
        let get = get::most_bytes(gateway.window, &layout)
            .and_then(|bytes| bytes.checked_add(block))
            .ok_or_else(overflow)?;
        most = most.max(put).max(get);
        codes |= matches!(scheme, Scheme::Rs(_));
    }
    let tables = if codes { mantle_ec::TABLE_BYTES } else { 0 };
    u64::try_from(gateway.objects)
        .ok()
        .and_then(|objects| objects.checked_mul(most)?.checked_add(tables))
        .ok_or_else(overflow)
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
}

/// The range's settings and rules, which every member of the cell derives alike, from the
/// identities the cell's configuration names and the metadata layer's own bounds alone:
///
/// - an entry of the range's largest (`wire::largest_entry_bytes`) is what each append carries,
///   what a leader holds uncommitted and what one drive applies, with one append out to each
///   follower: the floor's `Ready`;
/// - a session of each identity, for good: a registration ends its owner's last
///   (`mantle_meta::session::register`), so no session waits out a lifetime;
/// - one command an entry, a session keeping its answer at the largest a node's command gets
///   (`wire::largest_answer_bytes`) until the next entry acknowledges it;
/// - a row's key and value at most the longest key S3's names give a row
///   (`key::largest_bytes`) and the largest value a request S3 admits, or a session at its
///   bounds, gives one (`wire::largest_row_bytes`).
pub fn cell(identities: u64) -> Result<(Settings, Rules), DeriveError> {
    let entry = u64::try_from(wire::largest_entry_bytes()?)
        .map_err(|_| DeriveError::Overflow("the largest entry"))?;
    let settings = Settings {
        election_tick: ELECTION_TICK,
        heartbeat_tick: HEARTBEAT_TICK,
        max_size_per_msg: entry,
        max_inflight_msgs: 1,
        max_uncommitted_size: entry,
        max_committed_size_per_ready: entry,
        max_entry_bytes: entry,
    };
    let answer = wire::largest_answer_bytes()?;
    let session = Session::row_bytes(1, answer).ok_or(DeriveError::Overflow("a session's row"))?;
    let rules = Rules {
        lifetime_ns: u64::MAX,
        max_sessions: identities,
        max_answers: 1,
        max_answer_bytes: answer,
        // Nothing expires by time: one is the least work an entry's expiry may do.
        expiries_per_entry: 1,
        max_key_bytes: key::largest_bytes(),
        max_value_bytes: wire::largest_row_bytes()?.max(session),
    };
    Ok((settings, rules))
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
/// configuration: measured, 1,080 bytes under elections by suspicion, the node's, at one to ten
/// members (`tests/range_memory.rs`).
pub const MEMBER_BYTES: u64 = 1_080;

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

/// A transport endpoint as it opens, its TLS configurations with the node's certificate, key and
/// authority included, beside the tables its limits size: measured, 59,325 bytes, and 713 for
/// each peer its directory names, at one, two and four peers (`tests/transport_memory.rs`).
const ENDPOINT_BYTES: u64 = 59_325;
const ENDPOINT_PEER_BYTES: u64 = 713;

/// A slot of an endpoint's table of receive chunks, beside the chunk its budget charges:
/// measured, 72 bytes.
const CHUNK_SLOT_BYTES: u64 = 72;

/// A QUIC connection and its TLS session once established, a side's: measured, 122,012 bytes at
/// the most any window took, and 95 for each stream it may hold.
const CONNECTION_BYTES: u64 = 122_012;
const STREAM_BYTES: u64 = 95;

/// An exchange in use, a side's, beside what its budget charges: its stream's state and the
/// transport's record of it, measured, 2,151 bytes at the most.
const EXCHANGE_BYTES: u64 = 2_151;

/// What a server's handshake takes, once a certificate, to compress the certificate (RFC 8879):
/// hyper-tls's brotli at quality 11 and a 4 MiB window, measured, 34,267,200 bytes, given back as
/// the handshake goes on.
const CERTIFICATE_COMPRESSION_BYTES: u64 = 34_267_200;

/// A peer's state on the plane beside its epochs' replay windows and its datagram's room:
/// measured, 182 bytes at the most.
const PLANE_PEER_BYTES: u64 = 182;

/// The most bytes the node's transport holds beside its budget (`transport_bytes`), for a node of
/// `stated`: what the budget does not charge.
/// - The endpoint: as it opens, a slot of each exchange in use, a slot of each receive chunk, and
///   the compression of its certificate as a server's first handshake makes it.
/// - Each connection its admission takes: QUIC's and TLS's state with a stream's for each it may
///   hold, and what QUIC keeps to send, at most what its peer's window takes, `window_ceiling`
///   (every node of a cell states the same).
/// - The sockets: the listener's, one for each connection where the platform gives a connection a
///   socket of its own, and the plane's, each at what hyper-rt states one holds (`Io::held_bound`).
/// - The plane ([`plane_bytes`]) and the liveness stream ([`liveness_bytes`]).
///
/// QUIC's datagrams are none: no node sends them, though hyper-quic's default takes up to
/// 1,250,000 bytes of them a connection. What aws-lc keeps of keys is in its own heap, which a
/// counting allocator does not see.
pub fn transport_bytes_beside(stated: &crate::bounds::Stated) -> Option<u64> {
    let limits = crate::bounds::limits(stated).ok()?;
    let count = |n: usize| u64::try_from(n).ok();
    let peers = count(stated.peers.max(1))?;
    let connections = count(limits.admission.connections)?;
    let endpoint = peers
        .checked_mul(ENDPOINT_PEER_BYTES)?
        .checked_add(ENDPOINT_BYTES)?
        .checked_add(CERTIFICATE_COMPRESSION_BYTES)?
        .checked_add(count(limits.exchanges)?.checked_mul(EXCHANGE_BYTES)?)?
        .checked_add(count(limits.receive_chunks)?.checked_mul(CHUNK_SLOT_BYTES)?)?;
    let connection = u64::from(limits.streams_per_connection)
        .checked_mul(STREAM_BYTES)?
        .checked_add(CONNECTION_BYTES)?
        .checked_add(limits.window_ceiling)?;
    let sockets = connections
        .checked_add(2)?
        .checked_mul(count(crate::bounds::io(stated).held_bound())?)?;
    endpoint
        .checked_add(connections.checked_mul(connection)?)?
        .checked_add(sockets)?
        .checked_add(plane_bytes(&crate::bounds::plane(stated).ok()?)?)?
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

/// The threads a node starts beside its shard's, which runs on the process's own: its standard
/// input's and output's, its log's and its log's device's; for each device holding volumes, its
/// issuer and a worker for each read its profile measured at the device at once
/// (`hyper_block::issuer`); and for each volume its writer and its cleaner, and its scrubber
/// where it scrubs. `devices` are those devices' depths, `volumes` whether each scrubs.
pub fn threads(devices: &[usize], volumes: &[bool]) -> Option<u64> {
    let mut threads = 4u64;
    for &depth in devices {
        threads = threads.checked_add(u64::try_from(depth).ok()?.checked_add(1)?)?;
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

/// A class of what a node holds at its floor (node.md §1.4), in the order the floor counts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// The objects' put and get, a window of one block ([`objects`]).
    Objects,
    /// Each volume's buffers (`mantle_chunk::layout::most_bytes`).
    Volumes,
    /// Each range's engine's configured buffers (`mantle_engine::shard_db::bounded_bytes`).
    Engines,
    /// Each range's member ([`range_bytes`]).
    Ranges,
    /// The node's shared log ([`log_bytes`]).
    Log,
    /// The transport beside its budget, its plane and its liveness stream
    /// ([`transport_bytes_beside`], [`plane_bytes`], [`liveness_bytes`]).
    Transport,
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
/// remains is each volume's index's and each engine's resident metadata's ([`shares`]).
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

/// What remains past the floor, shared by each volume's index and each engine's resident metadata:
/// each takes what its device at its full size wants (`wants`), and when less remains than all of
/// them want, each the same part of its want, so no device fills before the rest.
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

/// What a data device measured (docs/design/chunk-store.md §7; `mantle_disk::calibrate`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Profile {
    /// Reads at the device at once where throughput stops growing.
    pub depth: usize,
    /// Bytes the reads at the device buffer at once where large reads stop gaining.
    pub read_bytes: u64,
    /// Bytes a read goes past rather than ask the device a second time.
    pub read_gap: u64,
    /// Whether a durable write into never-written space is slower than one over written space.
    pub prewrite: bool,
}

/// What a volume laid out already fixed at format, as its superblock records it: where
/// everything is, and its records' checksum blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Formatted {
    pub geometry: Geometry,
    pub checksum_shift: u8,
}

/// A chunk volume of `size` bytes on a device whose transfers align to `align`, measured as
/// `profile`, holding chunks of `chunk_bytes` at most, whose index may ask `index_bytes` of the
/// allocator; `formatted` when it was laid out already, whose choices made at format stand:
///
/// - checksum blocks of the volume's block, at most CRC-32C's verified 16 KiB: a read at the
///   device's alignment then verifies no byte it did not ask for (research 11 §8.2);
/// - the least segment holding one record of the largest chunk whole (`layout::least_segment`),
///   since smaller segments clean better (research 11 §7.3–§7.4);
/// - the standard layout, its superblocks 16 MiB apart beyond the reach of clustered latent
///   sector errors, or the compact one where the standard leaves fewer than two segments
///   (chunk-store.md §2);
/// - batches of one record, a segment's room in bytes, and one fragment a chunk, as the node
///   puts chunks whole;
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
        None => {
            let block =
                volume_layout::block(align).ok_or(DeriveError::Overflow("a volume's block"))?;
            let shift = u8::try_from(block.trailing_zeros())
                .map_err(|_| DeriveError::Overflow("a volume's block"))?
                .min(CHECKSUM_SHIFT_MOST);
            let segment = volume_layout::least_segment(block, shift, chunk_bytes)
                .ok_or(DeriveError::Chunk { bytes: chunk_bytes })?;
            (block, shift, segment)
        }
    };
    let batch_bytes = segment_size
        .checked_sub(block)
        .and_then(|room| usize::try_from(room).ok())
        .ok_or(DeriveError::Overflow("a batch"))?;
    let at_a_block = size.checked_div(block).unwrap_or(0);
    let mut config = VolumeConfig {
        segment_size,
        checksum_shift,
        max_fragments: index::most_fragments(index_bytes).min(at_a_block),
        compact: false,
        scrub_period: Some(SCRUB),
        limits: Limits {
            batch_requests: 1,
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
        assert_eq!(settings.max_inflight_msgs, 1);
        assert_eq!(rules.max_sessions, 7);
        assert!(rules.admit_sessions());
        assert!(rules.largest_session_row() <= rules.max_value_bytes);
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
    /// largest body's commands, one read each, and its enlistment.
    #[test]
    fn the_asks_hold_each_operation_at_its_most_under_any_scheme() {
        let g = gateway().unwrap();
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
        assert_eq!(a.commands, g.objects * usize::try_from(blocks).unwrap() + 1);
        assert_eq!(a.reads, g.objects);
    }

    /// The node's own four threads, and each device's issuer and workers and each volume's
    /// writer, cleaner and scrubber where it scrubs, each a 2 MiB stack.
    #[test]
    fn the_threads_are_the_nodes_own_and_its_storages() {
        assert_eq!(threads(&[], &[]), Some(4));
        assert_eq!(threads(&[2], &[true]), Some(4 + 3 + 3));
        assert_eq!(threads(&[1, 4], &[true, false]), Some(4 + 2 + 5 + 3 + 2));
        assert_eq!(stacks(10), Some(20 << 20));
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
        let stated = |peers: usize, transport_bytes: u64| crate::bounds::Stated {
            peers,
            ranges: 5,
            inflight: settings.max_inflight_msgs,
            transfers: 38,
            transport_bytes,
            messages: crate::classes::Bounds {
                replication: settings.max_entry_bytes + 1_024,
                request: settings.max_entry_bytes + 1_024,
                transfer: u64::from(u32::MAX),
                bulk: u64::from(u32::MAX),
            },
        };
        let least = |peers| crate::bounds::least_transport(&stated(peers, 0)).unwrap();
        let one = transport_bytes_beside(&stated(1, least(1))).unwrap();
        assert!(one > CERTIFICATE_COMPRESSION_BYTES);
        assert!(transport_bytes_beside(&stated(2, least(2))).unwrap() > one);
        assert!(transport_bytes_beside(&stated(1, 8 * least(1))).unwrap() > one);
        assert_eq!(transport_bytes_beside(&stated(1, least(1) - 1)), None);
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

    /// Two operations of the larger of a put and a get each: for one copy, a get's spans and the
    /// other copy, the copy cut, the bytes opened and the host's block taken, past a put's
    /// segment, block filling, block going down, its copy sent and a piece of body; a code adds
    /// the coding library's tables once.
    #[test]
    fn the_objects_hold_two_operations_of_their_largest_blocks() {
        let g = gateway().unwrap();
        let sealed = mantle_gateway::layout::SEALED;
        let copies = durability::Scheme::Copies(1);
        let block = Layout::new(copies).unwrap().largest_block().unwrap();
        assert_eq!(objects(&g, &[copies]).unwrap(), 2 * (5 * block + sealed));
        let coded = durability::Scheme::Rs(mantle_ec::Code::new(2, 1).unwrap());
        let both = objects(&g, &[copies, coded]).unwrap();
        let alone = objects(&g, &[coded]).unwrap();
        assert_eq!(both, alone);
        assert!(alone > 2 * (5 * block + sealed) + mantle_ec::TABLE_BYTES);
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
        assert_eq!(entries, settings.max_entry_bytes * 2 / least + 1);
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
        assert_eq!(
            (v.limits.batch_requests, v.limits.fragments_per_chunk),
            (1, 1)
        );
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
}
