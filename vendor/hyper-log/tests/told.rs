//! `Waits::Told`: the writer writes nothing until its caller cuts (`Log::cut`), so what one frame
//! holds and when it is answered are the caller's. A simulation decides with it when each write
//! reaches its device (Quiver's `docs/design/persisted-sim.md`).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity
)]

use std::time::Duration;

use hyper_block::buf::Alignment;
use hyper_block::sim::{Crash, Fault, SimFile};
use hyper_log::{Config, Entries, Entry, Fetched, Log, LogError, Page, Pending, Update, Waits};

const ID: u128 = 0x746f_6c64;
const BLOCK: usize = 4096;

fn config(waits: Waits) -> Config {
    Config {
        segment_bytes: 16 * BLOCK as u64,
        max_segments: 16,
        max_groups: 8,
        group_entries: 1 << 10,
        group_bytes: 1 << 20,
        group_cache: 1 << 16,
        queue_submissions: 64,
        waits,
    }
}

fn sim(seed: u64) -> SimFile {
    SimFile::new(
        Alignment::new(BLOCK).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

/// Entry `index` of a group, of `len` bytes tagged with it.
fn append(index: u64, len: usize) -> Update {
    Update {
        entries: Some(Entries {
            first: index,
            entries: vec![Entry {
                term: 1,
                bytes: (0..len).map(|b| (index as usize + b) as u8).collect(),
            }],
        }),
        ..Update::default()
    }
}

/// The device's writes and flushes so far.
fn ops(log: &Log<SimFile>) -> (u64, u64) {
    let stats = log.with_file(|f| f.stats().unwrap()).unwrap();
    (stats.writes, stats.syncs)
}

/// Long enough for the log's threads to have written anything they were going to.
const SETTLE: Duration = Duration::from_millis(50);

#[test]
fn under_told_nothing_is_written_until_the_cut_then_one_frame_holds_it_all() {
    let log = Log::create(sim(1), config(Waits::Told), ID).unwrap();
    let before = ops(&log);
    let pending: Vec<Pending> = (1..=4)
        .map(|group| log.submit(group, append(1, 100)).unwrap())
        .collect();
    std::thread::sleep(SETTLE);
    assert_eq!(
        ops(&log),
        before,
        "nothing reaches the device before the cut"
    );
    assert!(
        pending.iter().all(|p| p.poll().is_none()),
        "nothing is answered before the cut"
    );
    let frames = log.stats(None).unwrap().frames;

    log.cut().unwrap().wait().unwrap();
    // The cut is answered once every update it took is: each is answered already.
    for p in &pending {
        assert!(matches!(p.poll(), Some(Ok(()))));
    }
    let stats = log.stats(None).unwrap();
    assert_eq!(stats.frames, frames + 1, "one frame holds the four updates");
    assert_eq!(stats.updates, 4);
    for group in 1..=4 {
        assert_eq!(log.view(group).unwrap().unwrap().last, 1);
    }
    // Written and confirmed on its own: nothing more follows until the next cut.
    let after = ops(&log);
    std::thread::sleep(SETTLE);
    assert_eq!(ops(&log), after);
}

#[test]
fn a_cut_that_finds_nothing_held_is_answered_at_once() {
    let log = Log::create(sim(2), config(Waits::Told), ID).unwrap();
    let before = ops(&log);
    log.cut().unwrap().wait().unwrap();
    assert_eq!(ops(&log), before);
}

#[test]
fn each_cut_takes_only_what_came_before_it() {
    let log = Log::create(sim(3), config(Waits::Told), ID).unwrap();
    let first = log.submit(1, append(1, 10)).unwrap();
    log.cut().unwrap().wait().unwrap();
    assert!(matches!(first.poll(), Some(Ok(()))));
    let second = log.submit(1, append(2, 10)).unwrap();
    std::thread::sleep(SETTLE);
    assert!(
        second.poll().is_none(),
        "the next update waits for the next cut"
    );
    assert_eq!(log.view(1).unwrap().unwrap().last, 1);
    log.cut().unwrap().wait().unwrap();
    assert!(matches!(second.poll(), Some(Ok(()))));
    assert_eq!(log.view(1).unwrap().unwrap().last, 2);
}

#[test]
fn under_the_other_waits_a_cut_is_answered_at_once() {
    for waits in [Waits::Never, Waits::Measured] {
        let log = Log::create(sim(4), config(waits), ID).unwrap();
        log.submit(1, append(1, 10)).unwrap().wait().unwrap();
        let before = ops(&log);
        log.cut().unwrap().wait().unwrap();
        // Under `Measured` the log may write zeros ahead while it idles; under `Never` nothing.
        if waits == Waits::Never {
            assert_eq!(ops(&log), before);
        }
    }
}

/// The same updates and cuts from the same seed, with power cut at the same operation, give the
/// same answers, the same operations and, after the crash, the same log: what a frame holds is
/// fixed by the cuts, not by when the log's threads ran. (The bytes differ: each segment's nonce
/// comes from the operating system, `docs/raft-log.md` §2.)
#[test]
fn the_same_updates_and_cuts_leave_the_same_log_after_a_crash() {
    let run = |cut_after: u64| {
        let log = Log::create(sim(5), config(Waits::Told), ID).unwrap();
        let mut answers = Vec::new();
        for round in 0..20_u64 {
            if round == 12 {
                log.with_file(move |f| f.inject(Fault::PowerCut { ops: cut_after }).unwrap())
                    .unwrap();
            }
            // Refused at once once the log has fenced.
            let groups = 1..=1 + u128::from(round % 4);
            let pending: Vec<Option<Pending>> = groups
                .map(|group| {
                    log.submit(group, append(round / 4 + 1, 64 * round as usize))
                        .ok()
                })
                .collect();
            if round % 3 == 2 {
                // Submitted and not cut: the next cut takes these too.
                continue;
            }
            answers.push(log.cut().unwrap().wait().is_ok());
            let durable =
                |p: &Option<Pending>| p.as_ref().is_some_and(|p| matches!(p.poll(), Some(Ok(()))));
            answers.extend(pending.iter().map(durable));
        }
        let ops = ops(&log);
        let file = log.close().unwrap();
        file.crash(Crash::Random).unwrap();
        file.clear_faults().unwrap();
        let (log, recovery) = Log::open(file, config(Waits::Told), ID).unwrap();
        let views: Vec<Option<(u64, u64)>> = (1..=4)
            .map(|group| log.view(group).unwrap().map(|v| (v.start.index, v.last)))
            .collect();
        (
            answers,
            ops,
            recovery.frames,
            recovery.damaged,
            recovery.restored,
            views,
        )
    };
    for cut_after in [0, 1, 2, 3, 5, 8, 13] {
        assert_eq!(
            run(cut_after),
            run(cut_after),
            "power cut after {cut_after} operations"
        );
    }
}

#[test]
fn a_power_cut_in_the_cut_s_frame_fences_the_cut_and_its_updates() {
    let log = Log::create(sim(6), config(Waits::Told), ID).unwrap();
    let pending = log.submit(1, append(1, 10)).unwrap();
    log.with_file(|f| f.inject(Fault::PowerCut { ops: 0 }).unwrap())
        .unwrap();
    assert!(matches!(log.cut().unwrap().wait(), Err(LogError::Fenced)));
    assert!(matches!(pending.poll(), Some(Err(LogError::Fenced))));
    // A fenced log answers a later cut at once.
    assert!(matches!(log.cut().unwrap().wait(), Err(LogError::Fenced)));
}

#[test]
fn a_close_writes_what_was_held_as_a_cut_would() {
    let log = Log::create(sim(7), config(Waits::Told), ID).unwrap();
    let pending = log.submit(1, append(1, 10)).unwrap();
    let file = log.close().unwrap();
    assert!(matches!(pending.poll(), Some(Ok(()))));
    let (log, _) = Log::open(file, config(Waits::Told), ID).unwrap();
    assert_eq!(log.view(1).unwrap().unwrap().last, 1);
}

/// Under `Waits::Told` a fetch of entries the log no longer holds in memory waits for a serve
/// (`Log::serve`), which is answered once the fetch is; a serve with nothing held, and any serve
/// under the other waits, is answered at once.
#[test]
fn under_told_a_fetch_from_the_device_waits_for_a_serve() {
    let log = Log::create(sim(8), config(Waits::Told), ID).unwrap();
    // Past the group's cache (64 KiB): the first entries are read back from the device.
    for index in 1..=24 {
        log.submit(1, append(index, 8 << 10)).unwrap();
        log.cut().unwrap().wait().unwrap();
    }
    let page = Page::payload(u64::MAX);
    let fetching = log
        .fetch_waking(1, 1, 2, page, Fetched::default(), None)
        .unwrap();
    std::thread::sleep(SETTLE);
    assert!(
        fetching.poll().is_none(),
        "a read from the device waits for the serve"
    );
    log.serve().unwrap().wait().unwrap();
    let fetched = fetching.poll().unwrap().unwrap();
    assert_eq!(fetched.iter().count(), 1);
    log.serve().unwrap().wait().unwrap();
    // A caller that waits on its fetch as it asks could serve nothing until answered: read as asked.
    let fetched = log.fetch(1, 1, 2, page, Fetched::default()).unwrap();
    assert_eq!(fetched.iter().count(), 1);
    let group = log.group(1).unwrap();
    assert_eq!(group.entries(1, 2, u64::MAX).unwrap().len(), 1);

    let log = Log::create(sim(9), config(Waits::Never), ID).unwrap();
    log.serve().unwrap().wait().unwrap();
}

/// Under `Waits::Told` a caller that waits on its write from the moment it sends it (`Log::write`,
/// a group's `write`, as a replica's open makes) is answered without a cut: it could make none
/// until answered, so its waiting tells the writer. What was queued before it goes in its frame.
#[test]
fn under_told_a_blocking_write_tells_the_writer_by_waiting() {
    let log = Log::create(sim(11), config(Waits::Told), ID).unwrap();
    let queued = log.submit(2, append(1, 10)).unwrap();
    log.write(1, append(1, 10)).unwrap();
    assert!(matches!(queued.poll(), Some(Ok(()))));
    assert_eq!(log.view(1).unwrap().unwrap().last, 1);
    let mut group = log.group(3).unwrap();
    group.write(append(1, 10)).unwrap();
    assert_eq!(log.view(3).unwrap().unwrap().last, 1);
}
