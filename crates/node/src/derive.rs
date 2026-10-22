//! What `mantle serve` runs with that its operator does not state (docs/design/node.md §1.4),
//! derived from what it states, what it measures of its host and devices, and the code's own
//! bounds, each class of work at its floor: a share past a class's floor waits for its knee,
//! measured at the scale step (node.md §9.3). Each value carries its basis, which the node says
//! at start.

use std::time::Duration;

use hyper_block::buf::Alignment;
use hyper_log::{Config as LogConfig, Facts, LogError, Unfit};
use hyper_rt::machine::bench::MIN_SAMPLES;
use mantle_chunk::layout::{self as volume_layout, Geometry};
use mantle_chunk::{ChunkError, Config as VolumeConfig, Limits, Reads, index};
use mantle_ec::durability::{self, Burst, Rates};
use mantle_engine::store::Config as StoreConfig;
use mantle_engine::trunk::TrunkConfig;
use mantle_gateway::layout::Layout;
use mantle_gateway::{get, put};
use mantle_meta::record::{RecordError, Session};
use mantle_meta::session::Rules;
use mantle_meta::{key, wire};
use mantle_range::Settings;

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
/// one put and one get; S3's quota of buckets; one row each step of a bucket's attempt passes or
/// removes, the least that moves it; HDFS's handover.
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

/// The asks a node of `gateway` holds at most, each operation it hosts the most of any scheme a
/// block may take (`mantle_ec::durability::candidates`): a put's window of chunks at their width,
/// or a get's spans and the whole chunks a failed span adds (`put::most_chunks`,
/// `get::most_chunks`); a put's command for each block of the largest body S3 takes
/// (`put::most_commands`); one read an operation; and one command besides, the node's own
/// enlistment.
pub fn asks(gateway: &Gateway) -> Result<Asks, DeriveError> {
    let overflow = |what| DeriveError::Overflow(what);
    let (mut chunks, mut commands) = (0usize, 0u64);
    for scheme in durability::candidates() {
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
/// liveness stream's group (`hyper_log::Config::derive`): a frame of the range's largest entry
/// with its durable shell's bytes; a checkpoint's cadence, and what a group holds uncommitted
/// past it, of one `Ready` of the range's settings each, in bytes and in entries of the fewest
/// bytes, so the group's retention holds the two cadences and a `Ready` the range checks it
/// against (`mantle_range`'s `check_settings`); a cache of one largest entry a group; and the
/// least file the log runs in, its persist area and three segments (raft-log.md §5 sizes the
/// file by measured rates later).
pub fn log(align: Alignment, settings: &Settings, ranges: usize) -> Result<LogConfig, DeriveError> {
    let overhead = hyper_durable::ENTRY_OVERHEAD;
    let entry = usize::try_from(settings.max_entry_bytes)
        .map_err(|_| DeriveError::Overflow("the largest entry"))?;
    let largest_entry = entry
        .checked_add(overhead)
        .ok_or(DeriveError::Overflow("the largest entry"))?;
    let groups = ranges
        .checked_add(1)
        .ok_or(DeriveError::Overflow("the log's groups"))?;
    let ready = settings
        .max_entry_bytes
        .checked_mul(2)
        .ok_or(DeriveError::Overflow("a Ready"))?;
    let entries = ready
        .checked_div(u64::try_from(overhead).map_err(|_| DeriveError::Overflow("an entry"))?)
        .ok_or(DeriveError::Overflow("a Ready's entries"))?;
    let cache_bytes = u64::try_from(largest_entry)
        .ok()
        .and_then(|entry| entry.checked_mul(u64::try_from(groups).ok()?))
        .ok_or(DeriveError::Overflow("the log's cache"))?;
    let facts = |disk_bytes| Facts {
        align,
        sealed: false,
        largest_entry,
        disk_bytes,
        max_groups: groups,
        cadence_entries: entries,
        cadence_bytes: ready,
        uncommitted_entries: entries,
        uncommitted_bytes: ready,
        cache_bytes,
    };
    // The least file is what a budget of none is refused for.
    let needed = match LogConfig::derive(&facts(0)) {
        Err(LogError::Unfit(Unfit::Disk { needed, .. })) => needed,
        Err(e) => return Err(e.into()),
        Ok(config) => return Ok(config),
    };
    Ok(LogConfig::derive(&facts(needed))?)
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
        let a = asks(&g).unwrap();
        let schemes = durability::candidates();
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

    /// The log derived takes a frame of the range's largest entry with its shell's bytes, and a
    /// group holds a `Ready` of the settings in bytes and in entries of the fewest bytes, as the
    /// range checks; its file is the least, its persist area and three segments.
    #[test]
    fn the_log_takes_the_largest_entry_and_a_ready_in_the_least_file() {
        let (settings, _) = cell(3).unwrap();
        let align = Alignment::new(4096).unwrap();
        let config = log(align, &settings, 5).unwrap();
        assert_eq!(config.max_groups, 6);
        assert_eq!(config.max_segments, 3);
        let ready = settings.max_entry_bytes * 2;
        let overhead = u64::try_from(hyper_durable::ENTRY_OVERHEAD).unwrap();
        assert!(config.group_bytes >= ready);
        assert!(config.group_entries >= ready / overhead);
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
}
