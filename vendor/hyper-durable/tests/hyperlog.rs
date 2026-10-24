//! The shell on hyper-log's group handle (`GroupStore`), the first store: a write larger than a
//! frame goes in parts and is durable whole; a write the log refuses for the group's retained
//! bound stalls the replica, the writes sent behind it are refused too (`LogError::Behind`), and
//! a compaction frees the room the refused writes are made again in, in order.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity
)]

mod support;

use std::sync::mpsc::{Receiver, sync_channel};
use std::task::Waker;

use hyper_block::sim::SimFile;
use hyper_durable::{GroupStore, LogStore, Output, Replica, Unbounded};
use hyper_log::{Config, Log, Waits};
use hyper_raft::proto::ConfState;
use support::Kv;
use support::cluster::settings;
use support::device::sim_file;

type Member = Replica<GroupStore<SimFile>, Kv, Unbounded>;

fn config(segment_blocks: u64, group_entries: u64) -> Config {
    Config {
        segment_bytes: segment_blocks * 4096,
        max_segments: 64,
        max_groups: 4,
        group_entries,
        group_bytes: 1 << 24,
        group_cache: 1 << 20,
        queue_submissions: 64,
        waits: Waits::Never,
    }
}

fn sole(log: &Log<SimFile>) -> (Member, Waker, Receiver<usize>) {
    let store = GroupStore::claim(log, 1).unwrap();
    let kv = Kv::new(
        ConfState {
            voters: vec![1],
            ..ConfState::default()
        },
        false,
    );
    let mut r = Replica::open(&settings(1, 3), store, kv, Unbounded).unwrap();
    let (tell, woken) = sync_channel(1024);
    let (waker, _) = hyper_measure::wake::waker(0, tell);
    r.campaign().unwrap();
    (r, waker, woken)
}

/// Drives until nothing is out and nothing more to do, waiting on the log's answers.
fn settle(r: &mut Member, waker: &Waker, woken: &Receiver<usize>) {
    settle_sending(r, waker, woken, &mut Vec::new());
}

/// As [`settle`], keeping the messages the member sends.
fn settle_sending(
    r: &mut Member,
    waker: &Waker,
    woken: &Receiver<usize>,
    sent: &mut Vec<hyper_raft::proto::Message>,
) {
    let mut out = Output::default();
    loop {
        out.clear();
        let driven = r.drive(now(), waker, &mut out).unwrap();
        sent.append(&mut out.messages);
        if driven.more {
            continue;
        }
        if r.in_flight() == 0 {
            return;
        }
        woken.recv().unwrap();
    }
}

#[test]
fn a_write_larger_than_a_frame_goes_in_parts_and_is_durable_whole() {
    let log = Log::create(sim_file(21), config(16, 1 << 12), 1).unwrap();
    let (mut r, waker, woken) = sole(&log);
    settle(&mut r, &waker, &woken);
    assert!(r.is_leader());
    let room = log.frame_room().unwrap();
    let entry = 4096;
    let count = 3 * room / entry + 1;
    for i in 0..count {
        r.propose(Vec::new(), vec![i as u8; entry]).unwrap();
    }
    let before = log.flushed().0;
    settle(&mut r, &waker, &woken);
    assert_eq!(r.machine().now.entries.len(), count + 1);
    assert!(
        log.flushed().0 - before >= 3,
        "one frame took more than a frame holds"
    );
    let view = r.core().store().log().view().unwrap();
    assert_eq!(view.last, count as u64 + 1);
    assert_eq!(view.hard_state.commit, count as u64 + 1);
}

#[test]
fn a_refusal_for_the_groups_bound_stalls_until_a_compaction_frees_it() {
    let bound = 16;
    let log = Log::create(sim_file(22), config(16, bound), 1).unwrap();
    let (mut r, waker, woken) = sole(&log);
    settle(&mut r, &waker, &woken);
    let mut stalled = 0;
    let mut out = Output::default();
    for i in 0..(3 * bound) {
        match r.propose(Vec::new(), i.to_le_bytes().to_vec()) {
            Ok(()) => {}
            Err(hyper_durable::ReplicaError::Stalled) => {}
            Err(e) => panic!("{e}"),
        }
        out.clear();
        r.drive(now(), &waker, &mut out).unwrap();
        settle(&mut r, &waker, &woken);
        if r.is_stalled() {
            stalled += 1;
            assert!(r.compact(2, now(), &waker).unwrap());
            settle(&mut r, &waker, &woken);
            assert!(!r.is_stalled(), "a compaction did not free the room");
        }
    }
    assert!(stalled > 0, "the group's bound never refused a write");
    let entries = &r.machine().now.entries;
    assert!(
        entries.windows(2).all(|w| w[1].0 == w[0].0 + 1),
        "entries applied out of order"
    );
    let view = r.core().store().log().view().unwrap();
    assert!(view.hard_state.commit <= view.last);
    assert_eq!(view.last, r.applied().index);
}

/// An owner with nothing else to do for its group waits for the log's answer to the oldest write
/// out, whole, however many parts it went in (`GroupStore::wait`), and its next drive takes it,
/// though the write's waker wakes no one; with nothing out there is nothing to wait for.
#[test]
fn an_owner_waits_for_its_oldest_write_whole_and_the_next_drive_takes_it() {
    let log = Log::create(sim_file(23), config(16, 1 << 12), 1).unwrap();
    let (mut r, waker, woken) = sole(&log);
    settle(&mut r, &waker, &woken);
    assert!(!r.log_mut().wait(), "nothing out");
    let room = log.frame_room().unwrap();
    let entry = 4096;
    let count = 2 * room / entry + 1;
    for i in 0..count {
        r.propose(Vec::new(), vec![i as u8; entry]).unwrap();
    }
    // The frames flushed are counted from before the drive hands the write to the log, whose own
    // thread may flush its parts before the drive's caller reads the count (CI run 40090099990,
    // windows-2025: fewer than two were left to count, and the parts read as one).
    let before = log.flushed().0;
    let mut out = Output::default();
    r.drive(now(), Waker::noop(), &mut out).unwrap();
    assert_eq!(r.in_flight(), 1);
    assert!(r.log_mut().wait());
    assert!(log.flushed().0 - before >= 2, "the write went in parts");
    out.clear();
    r.drive(now(), Waker::noop(), &mut out).unwrap();
    let last = count as u64 + 1;
    assert_eq!(r.logged_commit(), last, "the drive took the write's answer");
    // What it committed is applied a page a drive, each drive one entry at least.
    for _ in 0..count {
        if r.applied().index == last {
            break;
        }
        out.clear();
        r.drive(now(), Waker::noop(), &mut out).unwrap();
    }
    assert_eq!(r.machine().now.entries.len(), count + 1);
    let view = r.core().store().log().view().unwrap();
    assert_eq!(view.hard_state.commit, last);
}

/// The owner's clock in nanoseconds, simulated: each reading a nanosecond after the one before, so
/// time only moves forward, as an owner's monotonic clock does, and every run reads the same times.
/// No test here waits on elapsed time; those that judge time state it outright.
fn now() -> u64 {
    thread_local! {
        static CLOCK: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }
    CLOCK.with(|clock| {
        let now = clock.get() + 1;
        clock.set(now);
        now
    })
}

/// Group commit above the log (focal's finding, 2026-10-05): N proposals made before one drive
/// take one write, so one frame and one flush. The shell never waits on a flush per proposal: what
/// the core holds not yet durable goes out as one write when the replica is driven.
#[test]
fn proposals_made_before_one_drive_take_one_frame() {
    let log = Log::create(sim_file(24), config(16, 1 << 12), 1).unwrap();
    let (mut r, waker, woken) = sole(&log);
    settle(&mut r, &waker, &woken);
    let n = 64;
    for i in 0..n {
        r.propose(Vec::new(), vec![i as u8; 64]).unwrap();
    }
    let (frames, updates) = log.flushed();
    settle(&mut r, &waker, &woken);
    let (frames_after, updates_after) = log.flushed();
    assert_eq!(
        frames_after - frames,
        1,
        "the proposals took more than one frame"
    );
    assert_eq!(
        updates_after - updates,
        1,
        "the proposals took more than one write"
    );
    assert_eq!(r.machine().now.entries.len(), n + 1);
}

/// Two groups on one log, driven one after the other: neither drive waits on the log, so both
/// writes are out before either is answered, for the log to take together. That they then share
/// one frame is the log's own grouping (hyper-log's `many_submitters_share_flushes`); here the bound
/// is exact: two groups' writes take at most two frames.
#[test]
fn two_groups_driven_in_turn_both_have_their_write_out_before_either_waits() {
    let log = Log::create(sim_file(25), config(16, 1 << 12), 1).unwrap();
    let mut members = Vec::new();
    for group in [1u128, 2] {
        let store = GroupStore::claim(&log, group).unwrap();
        let kv = Kv::new(
            ConfState {
                voters: vec![1],
                ..ConfState::default()
            },
            false,
        );
        let mut r = Replica::open(&settings(1, 3), store, kv, Unbounded).unwrap();
        let (tell, woken) = sync_channel(1024);
        let (waker, _) = hyper_measure::wake::waker(0, tell);
        r.campaign().unwrap();
        settle(&mut r, &waker, &woken);
        members.push((r, waker, woken));
    }
    for (r, _, _) in &mut members {
        for i in 0..16u8 {
            r.propose(Vec::new(), vec![i; 64]).unwrap();
        }
    }
    let (frames, _) = log.flushed();
    let mut out = Output::default();
    for (r, waker, _) in &mut members {
        out.clear();
        r.drive(now(), waker, &mut out).unwrap();
    }
    for (r, _, _) in &members {
        assert_eq!(r.in_flight(), 1, "a drive waited on the log");
    }
    for (r, waker, woken) in &mut members {
        settle(r, waker, woken);
    }
    let (frames_after, _) = log.flushed();
    assert!(
        frames_after - frames <= 2,
        "two groups' writes took {} frames",
        frames_after - frames
    );
}

/// A voter holds a fast proposal above its log, and the leader's append reaches that index
/// before the write goes out: the one write carries the entry and the holding at its index. The
/// holding is the voter's vote, kept until a release (`docs/raft.md` §3.5), so the log takes it
/// with the entry that reached it rather than refusing the write.
#[test]
fn a_holding_the_same_write_reaches_by_an_append_is_kept() {
    use hyper_raft::proto::{Entry, Message, MessageType};
    let log = Log::create(sim_file(23), config(16, 1 << 12), 1).unwrap();
    let store = GroupStore::claim(&log, 1).unwrap();
    let kv = Kv::new(
        ConfState {
            voters: vec![1, 2, 3],
            ..ConfState::default()
        },
        false,
    );
    let mut s = settings(1, 3);
    s.core.fast = true;
    let mut r: Member = Replica::open(&s, store, kv, Unbounded).unwrap();
    let (tell, woken) = sync_channel(1024);
    let (waker, _) = hyper_measure::wake::waker(0, tell);
    r.step(Message {
        msg_type: MessageType::MsgHeartbeat,
        from: 2,
        to: 1,
        term: 1,
        ..Message::default()
    })
    .unwrap();
    settle(&mut r, &waker, &woken);
    let last = r.core().raft.log().last_index().unwrap();
    let at = last + 1;
    r.step(Message {
        msg_type: hyper_raft::fast::FAST_PROPOSE,
        from: 3,
        to: 1,
        term: 1,
        entries: vec![Entry {
            index: at,
            term: 1,
            data: b"held".to_vec(),
            ..Entry::default()
        }],
        ..Message::default()
    })
    .unwrap();
    let log_term = r.core().raft.log().term(last).unwrap();
    r.step(Message {
        msg_type: MessageType::MsgAppend,
        from: 2,
        to: 1,
        term: 1,
        index: last,
        log_term,
        entries: vec![Entry {
            index: at,
            term: 1,
            data: b"appended".to_vec(),
            ..Entry::default()
        }],
        ..Message::default()
    })
    .unwrap();
    settle(&mut r, &waker, &woken);
    let view = r.core().store().log().view().unwrap();
    assert_eq!(view.last, at);
    let initial = hyper_raft::Storage::initial_state(r.core().store()).unwrap();
    assert!(
        initial.proposals.iter().any(|p| p.index == at),
        "the holding is kept beside the entry that reached it"
    );
}

fn store_waker() -> (Waker, Receiver<usize>) {
    let (tell, woken) = sync_channel(1024);
    let (waker, _) = hyper_measure::wake::waker(0, tell);
    (waker, woken)
}

fn entries_of(first: u64, last: u64, term: u64) -> Vec<hyper_raft::proto::Entry> {
    (first..=last)
        .map(|index| hyper_raft::proto::Entry {
            index,
            term,
            data: vec![index as u8; 96],
            ..hyper_raft::proto::Entry::default()
        })
        .collect()
}

fn write_of(first: u64, entries: &[hyper_raft::proto::Entry]) -> hyper_durable::Write<'_> {
    hyper_durable::Write {
        start: None,
        entries: Some(hyper_durable::Entries { first, entries }),
        hard_state: None,
        proposals: &[],
        released: None,
    }
}

/// The oldest write's answer, waited for.
fn answer_of(store: &mut GroupStore<SimFile>, woken: &Receiver<usize>) {
    loop {
        if let Some(answer) = store.poll() {
            answer.unwrap();
            return;
        }
        woken.recv().unwrap();
    }
}

/// What a replica has yet to apply is read back from its store's handle, never through the log: the
/// log closed, and the group's share of its cache too small for an entry, the entries after the
/// replica's mark still read, and those it applied no longer do. What is kept is counted exactly from
/// the write's answer, and let go as the replica applies it.
#[test]
fn entries_the_replica_has_yet_to_apply_are_read_back_from_its_store_until_applied() {
    let log = Log::create(
        sim_file(31),
        Config {
            group_cache: 1,
            ..config(16, 1 << 12)
        },
        1,
    )
    .unwrap();
    let mut store = GroupStore::claim(&log, 1).unwrap();
    let (waker, woken) = store_waker();
    // As a replica opens with nothing applied.
    store.keep_after(0);
    let written = entries_of(1, 8, 1);
    let bytes = |entries: &[hyper_raft::proto::Entry]| -> u64 {
        entries
            .iter()
            .map(|entry| hyper_durable::encode_entry(entry).unwrap().len() as u64)
            .sum()
    };
    store.submit(&write_of(1, &written), &waker).unwrap();
    assert_eq!(
        store.kept_bytes(),
        0,
        "nothing is kept before the log has the write"
    );
    answer_of(&mut store, &woken);
    assert_eq!(
        store.kept_bytes(),
        bytes(&written),
        "each entry kept from the answer"
    );
    let mut read = Vec::new();
    store.entries(3, 7, u64::MAX, &mut read).unwrap();
    assert_eq!(read, written[2..6]);
    store.keep_after(4);
    assert_eq!(
        store.kept_bytes(),
        bytes(&written[4..]),
        "what was applied is let go"
    );
    drop(log);
    let mut kept = Vec::new();
    store.entries(5, 9, u64::MAX, &mut kept).unwrap();
    assert_eq!(
        kept,
        written[4..8],
        "read from what was kept, the log closed"
    );
    let mut visited = Vec::new();
    store
        .visit(5, 9, 1, &mut |entry| {
            visited.push(entry.index);
            false
        })
        .unwrap();
    assert_eq!(visited, vec![5, 6, 7, 8]);
    let mut gone = Vec::new();
    assert!(
        store.entries(2, 4, u64::MAX, &mut gone).is_err(),
        "a range applied is the log's, and the log is closed"
    );
    // A lower mark changes nothing.
    store.keep_after(2);
    assert_eq!(store.kept_bytes(), bytes(&written[4..]));
    store.keep_after(8);
    assert_eq!(store.kept_bytes(), 0);
}

/// A write that replaces a suffix shows nothing of it before it is answered: until then the
/// indices it carries read the entries the answered writes left, after it the new ones.
#[test]
fn a_replaced_suffix_is_read_as_the_answered_writes_left_it() {
    let log = Log::create(sim_file(32), config(16, 1 << 12), 1).unwrap();
    let mut store = GroupStore::claim(&log, 1).unwrap();
    let (waker, woken) = store_waker();
    let first = entries_of(1, 6, 1);
    store.submit(&write_of(1, &first), &waker).unwrap();
    answer_of(&mut store, &woken);
    let replacing = entries_of(4, 6, 2);
    store.submit(&write_of(4, &replacing), &waker).unwrap();
    let mut before = Vec::new();
    store.entries(1, 7, u64::MAX, &mut before).unwrap();
    assert_eq!(before, first, "the replacing write is not answered yet");
    answer_of(&mut store, &woken);
    let mut after = Vec::new();
    store.entries(4, 7, u64::MAX, &mut after).unwrap();
    assert_eq!(after, replacing);
    let mut whole = Vec::new();
    store.entries(1, 7, u64::MAX, &mut whole).unwrap();
    assert_eq!(&whole[..3], &first[..3]);
    assert_eq!(&whole[3..], &replacing[..]);
}

/// A read asks for as many entries as its bytes admit and one at least, from what was kept as
/// from the log: the bytes of their bodies in a message, as the core counts them.
#[test]
fn a_read_from_what_was_kept_takes_what_its_bytes_admit_and_one_at_least() {
    let log = Log::create(sim_file(33), config(16, 1 << 12), 1).unwrap();
    let mut store = GroupStore::claim(&log, 1).unwrap();
    let (waker, woken) = store_waker();
    let written = entries_of(1, 6, 1);
    store.submit(&write_of(1, &written), &waker).unwrap();
    answer_of(&mut store, &woken);
    let one = hyper_raft::proto::encoded_bytes(&written[0]);
    let mut two = Vec::new();
    store.entries(1, 7, one * 2, &mut two).unwrap();
    assert_eq!(two, written[..2]);
    let mut least = Vec::new();
    store.entries(1, 7, 1, &mut least).unwrap();
    assert_eq!(least, written[..1]);
}

/// Entries of a few tens of bytes, of lengths that differ, every fourth with a context: a range's
/// commands as mantle writes them, about 50 bytes of data each (2026-10-19).
fn small_entries(count: u64) -> Vec<hyper_raft::proto::Entry> {
    (1..=count)
        .map(|index| hyper_raft::proto::Entry {
            index,
            term: 1,
            data: vec![index as u8; 20 + (index as usize * 7) % 61],
            context: if index % 4 == 0 {
                vec![0xc0; 6]
            } else {
                Vec::new()
            },
            ..hyper_raft::proto::Entry::default()
        })
        .collect()
}

/// How many of `entries` the core's page takes within `max_bytes`: the rule of
/// `hyper_raft::log::page_and_bytes`, which every page the core builds holds to, the longest prefix
/// whose running total of the entries' bodies in a message (`hyper_raft::proto::encoded_bytes`)
/// fits, and the first whatever its bytes.
fn core_page(entries: &[hyper_raft::proto::Entry], max_bytes: u64) -> usize {
    let mut total = 0u64;
    let mut taken = 0;
    for entry in entries {
        total = total.saturating_add(hyper_raft::proto::encoded_bytes(entry));
        if taken > 0 && total > max_bytes {
            break;
        }
        taken += 1;
    }
    taken
}

/// The bounds a page of `entries` turns at: each prefix's bytes in a message, one fewer and one
/// more, and none, one byte and no bound.
fn bounds_of(entries: &[hyper_raft::proto::Entry]) -> Vec<u64> {
    let mut bounds = vec![0, 1, u64::MAX];
    let mut total = 0u64;
    for entry in entries {
        total += hyper_raft::proto::encoded_bytes(entry);
        bounds.extend([total - 1, total, total + 1]);
    }
    bounds
}

/// A store of group 1 holding `written` from index 1, answered, on a log whose groups each keep
/// `group_cache` bytes of their entries in memory.
fn holding(
    seed: u64,
    group_cache: u64,
    written: &[hyper_raft::proto::Entry],
) -> (Log<SimFile>, GroupStore<SimFile>) {
    let log = Log::create(
        sim_file(seed),
        Config {
            group_cache,
            ..config(16, 1 << 12)
        },
        1,
    )
    .unwrap();
    let mut store = GroupStore::claim(&log, 1).unwrap();
    let (waker, woken) = store_waker();
    store.submit(&write_of(1, written), &waker).unwrap();
    answer_of(&mut store, &woken);
    (log, store)
}

/// Every page `store` reads of `written`, from several starts and at every bound a page turns at,
/// is the core's: what its entries take in a message fits the bound unless it holds one, and it is
/// the longest prefix that does. Each read asks the log `asks` times: none when the handle holds the
/// entries, once when they are older than its cache.
fn every_page_is_the_cores(
    store: &GroupStore<SimFile>,
    written: &[hyper_raft::proto::Entry],
    asks: u64,
) {
    let high = written.len() as u64 + 1;
    for low in [1, 2, 9, high / 2, high - 1] {
        let from = &written[(low - 1) as usize..];
        for max_bytes in bounds_of(from) {
            let asked = store.handle().asked();
            let mut page = Vec::new();
            store.entries(low, high, max_bytes, &mut page).unwrap();
            assert_eq!(
                store.handle().asked() - asked,
                asks,
                "[{low}, {high}) by {max_bytes}: read where the entries are"
            );
            let bytes: u64 = page.iter().map(hyper_raft::proto::encoded_bytes).sum();
            assert!(
                page.len() == 1 || bytes <= max_bytes,
                "[{low}, {high}) by {max_bytes}: {} entries take {bytes} bytes in a message",
                page.len()
            );
            let taken = core_page(from, max_bytes);
            assert_eq!(
                page.len(),
                taken,
                "[{low}, {high}) by {max_bytes}: the longest prefix that fits"
            );
            assert_eq!(page, from[..taken]);
        }
    }
}

/// A page the store reads is the page the core would take: as many entries as `max_bytes` of
/// their bodies in a message admit, and one at least, counted as the core counts an entry
/// (`hyper_raft::proto::encoded_bytes`), never by the shorter encoding the log stores
/// (`encode_entry` keeps no term or index). The core takes a page the store cut as whole, so a page
/// cut by the stored bytes ran past the core's bound by 20 bytes an entry: in mantle a leader's
/// append read from the store was 1,553 bytes where the core promises 1,024 and an append's fixed
/// part, its transport refused it on every retry, and the follower never caught up (2026-10-19).
/// Held from the handle's memory and from the log's file alike.
#[test]
fn a_page_read_from_the_store_is_the_cores_by_what_its_entries_take_in_a_message() {
    let written = small_entries(64);
    // The handle holds every entry.
    let (_log, cached) = holding(34, 1 << 20, &written);
    every_page_is_the_cores(&cached, &written, 0);
    // The group's share of the cache holds none: each read goes to the log's file.
    let (_log, old) = holding(35, 1, &written);
    every_page_is_the_cores(&old, &written, 1);
}

/// A walk of the store reads its pages by the same rule: at most `page` bytes of the entries'
/// bodies in a message at a time, the bytes its callers state a page in (the replica's walks, a
/// page of `max_committed_size_per_ready`), and one entry at least. Seen where each page is one read
/// of the log: entries older than the handle's cache.
#[test]
fn a_walk_of_the_store_reads_pages_of_the_cores_rule() {
    let written = small_entries(64);
    let (_log, store) = holding(36, 1, &written);
    let high = written.len() as u64 + 1;
    for low in [1, 9] {
        let from = &written[(low - 1) as usize..];
        let mut pages = bounds_of(&from[..8]);
        pages.retain(|&page| page != u64::MAX);
        for page in pages {
            let asked = store.handle().asked();
            let mut visited = Vec::new();
            store
                .visit(low, high, page, &mut |entry| {
                    visited.push(entry.index);
                    false
                })
                .unwrap();
            assert_eq!(visited, (low..high).collect::<Vec<_>>());
            let mut reads = 0;
            let mut at = 0;
            while at < from.len() {
                at += core_page(&from[at..], page);
                reads += 1;
            }
            assert_eq!(
                store.handle().asked() - asked,
                reads,
                "[{low}, {high}) by pages of {page}: one read a page of the core's rule"
            );
        }
    }
}

/// A learner caught up from its leader's log is sent appends within the core's bound on one
/// (`max_size_per_msg`, counted by what the entries take in the message), though every page is
/// read from the store, whose page the core takes whole: from the log's file and from the handle's
/// memory alike. In mantle such an append ran past the bound and was refused by its transport on
/// every retry (2026-10-19).
#[test]
fn a_learner_caught_up_from_the_log_is_sent_appends_within_the_bound() {
    use hyper_raft::proto::{
        ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, Message, MessageType,
    };
    let bound = settings(1, 3).core.max_size_per_msg;
    for (seed, group_cache) in [(37, 1), (38, 1 << 20)] {
        let log = Log::create(
            sim_file(seed),
            Config {
                group_cache,
                ..config(16, 1 << 12)
            },
            1,
        )
        .unwrap();
        let (mut r, waker, woken) = sole(&log);
        settle(&mut r, &waker, &woken);
        assert!(r.is_leader());
        for i in 0..48u8 {
            r.propose(Vec::new(), vec![i; 50]).unwrap();
        }
        settle(&mut r, &waker, &woken);
        let learner = ConfChangeV2 {
            transition: ConfChangeTransition::Auto,
            changes: vec![ConfChangeSingle {
                change_type: ConfChangeType::AddLearnerNode,
                node_id: 2,
            }],
            context: Vec::new(),
        };
        r.change(Vec::new(), &learner).unwrap();
        let mut sent = Vec::new();
        settle_sending(&mut r, &waker, &woken, &mut sent);
        let probe = sent
            .iter()
            .find(|m| m.to == 2 && m.msg_type == MessageType::MsgAppend)
            .cloned()
            .expect("a probe of the learner");
        // The learner holds nothing: its refusal takes the leader back to the log's first entry.
        r.step(Message {
            msg_type: MessageType::MsgAppendResponse,
            from: 2,
            to: 1,
            term: probe.term,
            index: probe.index,
            reject: true,
            reject_hint: 0,
            ..Message::default()
        })
        .unwrap();
        let last = r.core().raft.log().last_index().unwrap();
        let mut matched = 0;
        let mut most = 0;
        // Each round sends at least one entry the learner lacks, so it is caught up within `last`.
        for _ in 0..last {
            sent.clear();
            settle_sending(&mut r, &waker, &woken, &mut sent);
            for m in sent
                .iter()
                .filter(|m| m.to == 2 && m.msg_type == MessageType::MsgAppend)
            {
                let bytes: u64 = m.entries.iter().map(hyper_raft::proto::encoded_bytes).sum();
                assert!(
                    m.entries.len() <= 1 || bytes <= bound,
                    "cache {group_cache}: an append of {} entries takes {bytes} bytes, past {bound}",
                    m.entries.len()
                );
                most = most.max(m.entries.len());
                matched = m.entries.last().map_or(matched, |e| matched.max(e.index));
            }
            if matched >= last {
                break;
            }
            // The learner takes what it was sent.
            r.step(Message {
                msg_type: MessageType::MsgAppendResponse,
                from: 2,
                to: 1,
                term: probe.term,
                index: matched,
                ..Message::default()
            })
            .unwrap();
        }
        assert_eq!(
            matched, last,
            "cache {group_cache}: the learner is caught up"
        );
        assert!(
            most > 1,
            "cache {group_cache}: the bound admits several entries an append"
        );
    }
}

/// A group stopped with a write out and opened again at once in the same process opens on the
/// group as the log holds it, every write the log took included (focal, 2026-10-12: a shell group
/// reopened in-process failed `Compacted` at `Replica::open`). The member compacts and is dropped
/// before the compaction's answer comes; its store's handle goes with it, and a claim made then was
/// given the group without the compaction, so the open read entries the compaction had let go.
/// Each seed's log takes the compaction at its own point in the drop, as a device answers when it
/// does.
#[test]
fn a_group_stopped_with_its_compaction_out_opens_again_at_once() {
    for seed in 0..16 {
        let log = Log::create(sim_file(40 + seed), config(16, 1 << 12), 1).unwrap();
        let (mut r, waker, woken) = sole(&log);
        settle(&mut r, &waker, &woken);
        for i in 0..8u8 {
            r.propose(Vec::new(), vec![i; 64]).unwrap();
        }
        settle(&mut r, &waker, &woken);
        let applied = r.applied().index;
        assert!(
            r.compact(1, now(), &waker).unwrap(),
            "seed {seed}: a compaction is due"
        );
        assert!(r.in_flight() > 0, "seed {seed}: the compaction is out");
        // The machine persisted what the compaction keeps behind (`Replica::compact`): it opens
        // again on that.
        let kv = r.machine().crashed();
        drop(r);
        let store = GroupStore::claim(&log, 1).unwrap();
        let again = Replica::open(&settings(1, 3), store, kv, Unbounded)
            .unwrap_or_else(|e| panic!("seed {seed}: the group opens again: {e:?}"));
        let ours = again.core().store().log().handle().view().unwrap().unwrap();
        let theirs = log.view(1).unwrap().unwrap();
        assert_eq!(
            ours, theirs,
            "seed {seed}: the store holds the group as the log does"
        );
        assert_eq!(
            theirs.start.index,
            applied - 1,
            "seed {seed}: the compaction is the log's"
        );
    }
}
