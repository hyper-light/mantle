//! What held a shard: each long poll (`crate::attribution`: one that held its shard past twice the step quantum
//! since it was last let go on, docs/runtime.md §3.11) counted against its task's spawn site and its label, with
//! how long those polls held the shard and whose time it was. The long-poll counters say how many polls broke
//! the bounded-work contract and whether the task or the host held them, not whose code it was; this names it.
//!
//! - **The site** is where the task was spawned, taken by `#[track_caller]` at every spawn
//!   (`std::panic::Location`), so it costs nothing until a poll runs long.
//! - **The label** is what the task says it is doing ([`crate::futures::label`]: an S3 operation, say), kept
//!   on its slot until it sets another; the poll is counted under the label it ended with.
//! - **The threshold** is the long-poll threshold, derived from the shard's quantum, not a picked duration: a
//!   task that asks `wanted` once a quantum and yields when told to never passes twice the quantum, so every
//!   poll past it broke the contract (Seastar's reactor counts a task past twice its task quota as a
//!   violation, `task_queue_group::account_runtime`). A poll past one quantum but under two kept it, and the
//!   attribution judges none of those, so they are not counted here.
//! - **Whose time**: each counted poll's verdict from the attribution, the task's (on the CPU or blocked in a
//!   call) or the host's (preempted), counted on the same entry, at once or at its step's end where the
//!   shard judges a step's long polls together; a poll neither's is counted only in the entry's polls.
//!
//! **Bounded, one writer, any reader, no lock.** Each shard's table is a fixed array of [`ENTRIES`] entries on
//! its registry entry, so nothing grows per task: the key space is the program's spawn sites and labels, both
//! `'static`, and an entry once keyed keeps its key. The eviction rule is that there is none: a pair that finds
//! every entry taken is counted, with its hold and verdict, in one more entry with no key, so no hold is lost
//! and a reader sees how much went unnamed. Only the shard writes its table, at a long poll; any thread reads
//! it ([`slow_polls`]). A key is set once ([`OnceLock`]: its one writer never waits on it) before the entry's
//! first count, which is published with release ordering, so a reader that sees a count sees its key; the
//! counts are relaxed beyond that, so a reader may see a count and a total from moments apart, never a torn key.

use std::panic::Location;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::attribution::Attribution;
use crate::registry::{self, SlotHolder};

/// Format: the smallest base page any supported target maps, bytes: x86-64's 4-KByte page (Intel SDM Vol. 3A
/// ch. 4, "Paging"; Windows' and Linux's base page there), which Linux and Windows on arm64 use as their
/// granule too; macOS on arm64 maps 16 KiB pages (`vm_page_size`).
const SMALLEST_PAGE: usize = 4096;

/// Derived: entries in a shard's table: as many as one page of the smallest a supported target maps holds
/// ([`SMALLEST_PAGE`] over an entry's size). A shard writes its table alone and any reader copies it whole,
/// so a page bounds the memory each shard keeps for it and a reader's copy; a pair past them is counted in the
/// table's unkeyed entry (the module's notes).
pub const ENTRIES: usize = SMALLEST_PAGE / std::mem::size_of::<Entry>();

/// What a task says it is doing ([`crate::futures::label`]): a `static`, so a table keeps it by reference, as
/// it keeps a spawn site's `Location`, and tells two labels apart by their addresses.
#[derive(Debug)]
pub struct Label {
    /// The name the table's readers show.
    pub name: &'static str,
}

impl Label {
    /// A label named `name`, for a `static`.
    pub const fn new(name: &'static str) -> Label {
        Label { name }
    }
}

/// An entry's key: the task's spawn site and its label.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Key {
    pub(crate) site: Option<&'static Location<'static>>,
    pub(crate) label: Option<&'static Label>,
}

impl Key {
    /// The same site (by its file, line and column: one call site's `Location` may be two statics, one per
    /// instance of a generic caller) and the same label (by its address).
    fn is(&self, other: &Key) -> bool {
        self.site == other.site
            && match (self.label, other.label) {
                (Some(a), Some(b)) => std::ptr::eq(a, b),
                (None, None) => true,
                _ => false,
            }
    }

    /// The entry its probe starts at, of `n`: the site's line and column and the label's address mixed by
    /// Knuth's multiplicative hash (TAOCP Vol. 3 §6.4: the multiplier is 2^64 over the golden ratio).
    fn start(&self, n: usize) -> usize {
        /// Cited: 2^64 / φ, rounded to odd (Knuth, TAOCP Vol. 3 §6.4, multiplicative hashing).
        const GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;
        /// Format: the bits of a `u64` above its low half, where the line goes beside the column.
        const HALF: u32 = u32::BITS;
        let site = self.site.map_or(0, |s| {
            u64::from(s.line()).wrapping_shl(HALF) | u64::from(s.column())
        });
        let label = self.label.map_or(0, |l| {
            u64::try_from(std::ptr::from_ref(l).addr()).unwrap_or(0)
        });
        let mixed = (site ^ label).wrapping_mul(GOLDEN).wrapping_shr(HALF);
        usize::try_from(mixed)
            .ok()
            .and_then(|m| m.checked_rem(n))
            .unwrap_or(0)
    }
}

/// One entry: a key set once, then its long polls, how long they held the shard in all and at most, and of
/// them those the attribution found the task's and the host's.
#[derive(Debug, Default)]
struct Entry {
    key: OnceLock<Key>,
    polls: AtomicU64,
    held_ns: AtomicU64,
    longest_ns: AtomicU64,
    task_polls: AtomicU64,
    host_polls: AtomicU64,
}

impl Entry {
    /// Counts one long poll that held the shard `held_ns` (the writer's alone): the count last, with release
    /// ordering, so a reader that sees it sees the key and the hold.
    fn count(&self, held_ns: u64) {
        self.held_ns.fetch_add(held_ns, Ordering::Relaxed);
        self.longest_ns.fetch_max(held_ns, Ordering::Relaxed);
        self.polls.fetch_add(1, Ordering::Release);
    }

    /// Counts a counted poll's verdict.
    fn attribute(&self, attributed: Attribution) {
        if attributed.is_tasks() {
            self.task_polls.fetch_add(1, Ordering::Relaxed);
        } else if attributed == Attribution::Preempted {
            self.host_polls.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The entry as a reader sees it: none while it has counted nothing.
    fn read(&self) -> Option<SlowPoll> {
        let polls = self.polls.load(Ordering::Acquire);
        if polls == 0 {
            return None;
        }
        let key = self.key.get().copied().unwrap_or(Key {
            site: None,
            label: None,
        });
        Some(SlowPoll {
            site: key.site,
            label: key.label,
            polls,
            held_ns: self.held_ns.load(Ordering::Relaxed),
            longest_ns: self.longest_ns.load(Ordering::Relaxed),
            task_polls: self.task_polls.load(Ordering::Relaxed),
            host_polls: self.host_polls.load(Ordering::Relaxed),
        })
    }
}

/// Where a long poll was counted: an entry of the table, or [`ENTRIES`] for the unkeyed one; what a poll
/// judged at its step's end carries until its verdict.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Place(usize);

/// A shard's table of long polls (see the module's notes).
#[derive(Debug)]
pub(crate) struct SlowTable {
    entries: [Entry; ENTRIES],
    /// What a full table had no entry for.
    rest: Entry,
}

impl Default for SlowTable {
    fn default() -> Self {
        Self {
            entries: std::array::from_fn(|_| Entry::default()),
            rest: Entry::default(),
        }
    }
}

impl SlowTable {
    /// Counts one long poll that held the shard `held_ns`, by the task spawned at `site` with `label` (the
    /// shard's own call): in the entry keyed so, else the first free one its probe meets, else the unkeyed
    /// one. Where it went, for its verdict.
    pub(crate) fn note(&self, key: Key, held_ns: u64) -> Place {
        let start = key.start(ENTRIES);
        let found = (0..ENTRIES)
            .filter_map(|i| start.wrapping_add(i).checked_rem(ENTRIES))
            .find(|&i| {
                self.entries
                    .get(i)
                    .is_some_and(|entry| entry.key.get().is_none_or(|k| k.is(&key)))
            });
        let Some((place, entry)) = found.and_then(|i| Some((i, self.entries.get(i)?))) else {
            self.rest.count(held_ns);
            return Place(ENTRIES);
        };
        // The shard is the table's only writer: a free entry it found is still free as it sets the key.
        let _ = entry.key.set(key);
        entry.count(held_ns);
        Place(place)
    }

    /// Counts the verdict of the poll counted at `place`.
    pub(crate) fn attribute(&self, place: Place, attributed: Attribution) {
        self.entry(place.0).attribute(attributed);
    }

    /// The entry at `place`, the unkeyed one past the table.
    fn entry(&self, place: usize) -> &Entry {
        self.entries.get(place).unwrap_or(&self.rest)
    }

    /// Every entry with a poll counted, then what a full table had no entry for (no site, no label), as they
    /// stand: any thread's.
    fn read(&self) -> Vec<SlowPoll> {
        self.entries
            .iter()
            .chain(std::iter::once(&self.rest))
            .filter_map(Entry::read)
            .collect()
    }
}

/// What held a shard: a spawn site and a label, with the long polls they held it past twice its quantum
/// (`crate::attribution`) and for how long. Counts are since the shard started.
#[derive(Clone, Copy, Debug)]
pub struct SlowPoll {
    /// Where the task was spawned; `None` for what a full table had no entry for.
    pub site: Option<&'static Location<'static>>,
    /// The task's label as the polls ended, if it had one.
    pub label: Option<&'static Label>,
    /// Long polls.
    pub polls: u64,
    /// How long they held the shard, added up, nanoseconds: each from its start, or from the last time it
    /// asked `wanted` and was let go on, to its end (docs/runtime.md §3.11).
    pub held_ns: u64,
    /// The longest of them, nanoseconds.
    pub longest_ns: u64,
    /// Of them, those the attribution found the task's own: past the quantum on the CPU, or blocked inside a
    /// call.
    pub task_polls: u64,
    /// Of them, those the attribution found the host's: the thread runnable and off the CPU, preempted. The
    /// rest of [`SlowPoll::polls`] the readings could not attribute (`crate::attribution`).
    pub host_polls: u64,
}

/// The long polls of the shard `holder` names, by spawn site and label, as they stand: any thread's, touching
/// nothing of the shard's own. Empty for a holder no longer registered.
pub fn slow_polls(holder: SlotHolder) -> Vec<SlowPoll> {
    registry::with_holder(holder, |entry| entry.slow.read()).unwrap_or_default()
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    static ROUTE: Label = Label::new("GetObject");
    static OTHER: Label = Label::new("PutObject");

    fn key(site: &'static Location<'static>, label: Option<&'static Label>) -> Key {
        Key {
            site: Some(site),
            label,
        }
    }

    /// Polls are counted by site and label, each pair its own entry with its count, total and longest; the
    /// same site under another label, or none, is another entry.
    #[test]
    fn long_polls_count_by_site_and_label() {
        let table = SlowTable::default();
        let here = Location::caller();
        table.note(key(here, Some(&ROUTE)), 150_000);
        table.note(key(here, Some(&ROUTE)), 450_000);
        table.note(key(here, Some(&OTHER)), 200_000);
        table.note(key(here, None), 120_000);
        let mut read = table.read();
        read.sort_by_key(|p| std::cmp::Reverse(p.held_ns));
        assert_eq!(read.len(), 3, "{read:?}");
        let first = read[0];
        assert_eq!(
            (first.polls, first.held_ns, first.longest_ns),
            (2, 600_000, 450_000)
        );
        assert_eq!(first.label.map(|l| l.name), Some(ROUTE.name));
        assert_eq!(first.site, Some(here));
        assert!(read.iter().any(|p| p.label.is_none() && p.polls == 1));
    }

    /// Each verdict is counted on the entry its poll was counted in: the task's, the host's, and one neither's
    /// in the polls alone.
    #[test]
    fn verdicts_count_on_their_polls_entry() {
        let table = SlowTable::default();
        let here = Location::caller();
        let a = table.note(key(here, Some(&ROUTE)), 600_000);
        table.attribute(a, Attribution::Preempted);
        let b = table.note(key(here, Some(&ROUTE)), 150_000);
        table.attribute(b, Attribution::Long);
        let c = table.note(key(here, Some(&ROUTE)), 900_000);
        table.attribute(c, Attribution::Blocked);
        let d = table.note(key(here, Some(&ROUTE)), 300_000);
        table.attribute(d, Attribution::OffCpu);
        let read = table.read();
        assert_eq!(read.len(), 1, "{read:?}");
        let p = read[0];
        assert_eq!((p.polls, p.held_ns, p.longest_ns), (4, 1_950_000, 900_000));
        assert_eq!((p.task_polls, p.host_polls), (2, 1));
    }

    /// A table with every entry taken counts the rest in its unkeyed entry, verdicts too, and loses no poll.
    #[test]
    fn a_full_table_keeps_the_rest_unkeyed() {
        let table = SlowTable::default();
        let extra: u64 = 5;
        let labels: Vec<&'static Label> = (0..ENTRIES + extra as usize)
            .map(|_| &*Box::leak(Box::new(Label::new("an operation"))))
            .collect();
        for label in &labels {
            let place = table.note(
                Key {
                    site: None,
                    label: Some(label),
                },
                1,
            );
            table.attribute(place, Attribution::Long);
        }
        let read = table.read();
        assert_eq!(read.len(), ENTRIES + 1, "every entry and the unkeyed one");
        assert_eq!(
            read.iter().map(|p| p.polls).sum::<u64>(),
            labels.len() as u64
        );
        let rest: Vec<&SlowPoll> = read
            .iter()
            .filter(|p| p.label.is_none() && p.site.is_none())
            .collect();
        assert_eq!(rest.len(), 1);
        assert_eq!((rest[0].polls, rest[0].task_polls), (extra, extra));
    }
}
