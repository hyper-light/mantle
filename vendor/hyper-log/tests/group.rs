//! A group's handle (`GroupLog`): it answers every read as the log does, from the group's state
//! as its answered writes left it; it asks the log only for what it does not hold; it holds its
//! group against everyone else's writes; and each of its writes wakes its own waker alone.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cognitive_complexity,
    clippy::unwrap_in_result
)]

use hyper_block::buf::Alignment;
use hyper_block::sim::{Fault, SimFile};
use hyper_log::{
    Config, Entries, Entry, Fetched, GroupLog, HardState, Log, LogError, Page, Proposal, Start,
    Update, Waits,
};
use proptest::prelude::*;

const ID: u128 = 0x0067_726f_7570;
const BLOCK: usize = 4096;

fn config(group_cache: u64) -> Config {
    Config {
        segment_bytes: 16 * BLOCK as u64,
        max_segments: 16,
        max_groups: 64,
        group_entries: 1 << 10,
        group_bytes: 1 << 20,
        group_cache,
        queue_submissions: 64,
        waits: Waits::Never,
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

/// `count` entries from `first` of `term`, each of `len` bytes tagged with its index.
fn entries(first: u64, count: u64, term: u64, len: usize) -> Entries {
    Entries {
        first,
        entries: (first..first + count)
            .map(|i| Entry {
                term,
                bytes: (0..len).map(|b| (i as usize + b) as u8).collect(),
            })
            .collect(),
    }
}

/// The kind of a generated write, drawn as a number by weight (`prop_oneof!` would box its
/// arms in `Arc`, which the lint wall refuses in tests too).
#[derive(Debug, Clone, Copy)]
struct Step {
    kind: u8,
    at: u64,
    count: u64,
    len: usize,
    hard: bool,
}

fn step() -> impl Strategy<Value = Step> {
    (0u8..100, 0u64..64, 0u64..5, 0usize..700, any::<bool>()).prop_map(
        |(kind, at, count, len, hard)| Step {
            kind,
            at,
            count,
            len,
            hard,
        },
    )
}

/// The update a step makes of the group as its handle says it stands: mostly appends, then
/// conflicting suffixes, compactions within and past the log, removals, proposals, invalid
/// gaps, and empty entries that only say where the log ends.
fn update(s: Step, start: Start, last: u64, term: &mut u64) -> Update {
    let held = last - start.index;
    let pick = |n: u64| start.index + 1 + s.at % n.max(1);
    let hard = s.hard.then_some(HardState {
        term: *term,
        vote: s.at % 3,
        commit: last.min(s.at),
        held: None,
    });
    let mut u = Update {
        hard_state: hard,
        ..Update::default()
    };
    match s.kind {
        0..=49 => u.entries = Some(entries(last + 1, s.count, *term, s.len)),
        50..=64 if held > 0 => {
            *term += 1;
            u.entries = Some(entries(pick(held), s.count + 1, *term, s.len));
        }
        65..=79 if held > 0 => {
            u.start = Some(Start {
                index: pick(held),
                term: *term,
            });
        }
        80..=84 => {
            u.start = Some(Start {
                index: last + 1 + s.at % 3,
                term: *term,
            });
        }
        85..=87 => {
            return Update {
                remove: true,
                ..Update::default()
            };
        }
        88..=91 => {
            u.proposals.push(Proposal {
                index: last + 2 + s.at % 3,
                term: *term,
                bytes: vec![7; s.len.min(64)],
            });
        }
        92..=95 => u.entries = Some(entries(last + 2, s.count + 1, *term, s.len)),
        _ => u.entries = Some(entries(last + 1, 0, *term, 0)),
    }
    u
}

/// Every read the handle answers equals the log's own answer to it: the view, the bounds, the
/// term of every index around the group's, and fetches of every range, by every byte bound, with
/// each entry counted at its payload alone and at its payload and more.
fn same_reads(log: &Log<SimFile>, h: &GroupLog<SimFile>, g: u128) -> Result<(), TestCaseError> {
    let view = log.view(g).unwrap();
    prop_assert_eq!(&h.view().unwrap(), &view);
    let (start, last) = view
        .as_ref()
        .map_or((Start::default(), 0), |v| (v.start, v.last));
    prop_assert_eq!(h.bounds().unwrap(), (start, last));
    let low = start.index.saturating_sub(1);
    for i in low..=last + 1 {
        let ours = h.term(i).map_err(|e| e.to_string());
        let theirs = log.term(g, i).map_err(|e| e.to_string());
        prop_assert_eq!(ours, theirs, "term of {}", i);
    }
    for lo in low..=last + 1 {
        for hi in [lo, lo + 1, last + 1, last + 2] {
            for page in [
                Page::payload(0),
                Page::payload(600),
                Page {
                    max_bytes: 600,
                    per_entry: 25,
                },
                Page::payload(u64::MAX),
            ] {
                let ours = h.fetch(lo, hi, page, Fetched::new());
                let theirs = log.fetch(g, lo, hi, page, Fetched::new());
                prop_assert_eq!(
                    ours.map_err(|e| e.to_string()),
                    theirs.map_err(|e| e.to_string()),
                    "fetch [{}, {}) by {:?}",
                    lo,
                    hi,
                    page
                );
            }
        }
    }
    Ok(())
}

fn handle_reads_as_the_log_does(
    steps: &[Step],
    cache: u64,
    seed: u64,
    marks: bool,
) -> Result<(), TestCaseError> {
    let log = Log::create(sim(seed), config(cache), ID).unwrap();
    let g = 5u128;
    let mut h = log.group(g).unwrap();
    let mut term = 1u64;
    if marks {
        // As a replica marks what it applied when it opens.
        h.keep_after(0);
    }
    for &s in steps {
        if marks {
            // Raised as a replica applies, by any amount; a lower mark changes nothing.
            let (start, _) = h.bounds().unwrap();
            h.keep_after(start.index + s.at % 7);
        } else {
            prop_assert_eq!(h.kept_bytes(), 0);
        }
        let (start, last) = h.bounds().unwrap();
        let u = update(s, start, last, &mut term);
        match h.write(u) {
            Ok(())
            | Err(
                LogError::Invalid { .. }
                | LogError::Backlog(_)
                | LogError::Full
                | LogError::TooLarge(_),
            ) => {}
            Err(e) => return Err(TestCaseError::fail(format!("{e}"))),
        }
        same_reads(&log, &h, g)?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Whatever its group's writes, a handle answers every read as the log does, with a cache
    /// that holds nothing, a few entries, or all of them, and whether or not a replica marks what
    /// it applied (`GroupLog::keep_after`): what the handle keeps changes where a read is answered,
    /// never what it says.
    #[test]
    fn a_handle_reads_as_the_log_does(
        steps in prop::collection::vec(step(), 1..40),
        cache in prop::sample::select(vec![0u64, 1_500, 1 << 20]),
        seed in any::<u64>(),
        marks in any::<bool>(),
    ) {
        handle_reads_as_the_log_does(&steps, cache, seed, marks)?;
    }

    /// A handle splits an update into the parts the log would, whole or in several frames or
    /// refused as too large, without asking the log's owner: a replica needs nothing but its
    /// handle to write a ready larger than a frame.
    #[test]
    fn a_handle_splits_an_update_as_the_log_does(
        start in prop::option::of(0u64..8),
        count in 0u64..40,
        len in 0usize..3_000,
        hard in any::<bool>(),
        proposals in prop::collection::vec(0usize..70_000, 0..3),
    ) {
        let log = Log::create(sim(9), config(1 << 20), ID).unwrap();
        let h = log.group(1).unwrap();
        let last = start.unwrap_or(0) + count;
        let update = Update {
            start: start.map(|index| Start { index, term: 1 }),
            entries: (count > 0).then(|| entries(start.unwrap_or(0) + 1, count, 1, len)),
            hard_state: hard.then_some(HardState { term: 1, vote: 0, commit: 0, held: None, }),
            proposals: (last + 1..)
                .zip(&proposals)
                .map(|(index, &len)| Proposal { index, term: 1, bytes: vec![7; len] })
                .collect(),
            ..Update::default()
        };
        let theirs = format!("{:?}", log.parts(1, update.clone()));
        let ours = format!("{:?}", h.parts(update));
        prop_assert_eq!(ours, theirs);
        prop_assert_eq!(h.asked(), 0);
    }
}

/// The log holds a group for its handle: its other writers are refused, a second handle is
/// refused, and once the handle is dropped the group is anyone's again.
#[test]
fn a_handle_holds_its_group() {
    let log = Log::create(sim(1), config(1 << 20), ID).unwrap();
    let h = log.group(1).unwrap();
    let u = || Update {
        entries: Some(entries(1, 1, 1, 8)),
        ..Update::default()
    };
    assert!(matches!(log.write(1, u()), Err(LogError::Claimed(1))));
    assert!(matches!(log.submit(1, u()), Err(LogError::Claimed(1))));
    assert!(matches!(log.group(1), Err(LogError::Claimed(1))));
    // Another group is not held.
    log.write(2, u()).unwrap();
    drop(h);
    log.write(1, u()).unwrap();
    let h = log.group(1).unwrap();
    assert_eq!(h.bounds().unwrap(), (Start::default(), 1));
}

/// A handle answers reads of what it holds itself, and asks the log only for entries older
/// than its cache; it takes over the entries the log held in memory when it was made.
#[test]
fn a_handle_asks_the_log_only_for_what_it_lacks() {
    let log = Log::create(sim(2), config(3 * 100), ID).unwrap();
    // Written before the handle: the log keeps the last three in memory, which the handle takes.
    log.write(
        1,
        Update {
            entries: Some(entries(1, 5, 1, 100)),
            ..Update::default()
        },
    )
    .unwrap();
    let mut h = log.group(1).unwrap();
    for i in 0..5 {
        h.term(i).unwrap();
    }
    h.bounds().unwrap();
    h.view().unwrap();
    let recent = h
        .fetch(3, 6, Page::payload(u64::MAX), Fetched::new())
        .unwrap();
    assert_eq!(recent.len(), 3);
    assert_eq!(h.asked(), 0, "the handle asked for what it held");
    // Older than the cache: from the file, through the log.
    let old = h
        .fetch(1, 6, Page::payload(u64::MAX), Fetched::new())
        .unwrap();
    assert_eq!(old.len(), 5);
    assert_eq!(h.asked(), 1);
    // Its own writes' entries come back to it: the newest three are its own.
    h.write(Update {
        entries: Some(entries(6, 3, 1, 100)),
        ..Update::default()
    })
    .unwrap();
    let mine = h
        .fetch(6, 9, Page::payload(u64::MAX), Fetched::new())
        .unwrap();
    assert_eq!(
        mine.get(2).unwrap().1,
        entries(8, 1, 1, 100).entries[0].bytes
    );
    assert_eq!(h.asked(), 1);
}

/// How many entries of payloads `lens` a page takes: those whose running total fits its bytes,
/// each counted at its payload and the page's `per_entry`, and the first whatever its count.
fn page_of(lens: &[u64], page: Page) -> usize {
    let mut total = 0u64;
    let mut taken = 0;
    for &len in lens {
        total += len + page.per_entry;
        if taken > 0 && total > page.max_bytes {
            break;
        }
        taken += 1;
    }
    taken
}

/// A fetch counts each entry at its payload and the page's `per_entry` more, so a caller that
/// bounds its pages by its own encoding of an entry, larger than the payload by a fixed part, is
/// given the page that bound admits, chosen before anything is copied (hyper-durable's store: what
/// an entry takes in a Raft message, mantle 2026-10-19). Held at every bound a page turns at, from
/// the handle's memory and from the log's file alike.
#[test]
fn a_fetch_counts_each_entry_at_its_payload_and_the_pages_part_for_each() {
    let lens = [30u64, 70, 5, 120, 0, 64, 64, 9, 1, 200];
    let high = lens.len() as u64 + 1;
    // The handle holds every entry; the group's share of the cache holds none, and each fetch is
    // read from the log's file.
    for (cache, asks) in [(1 << 20, 0), (1, 1)] {
        let log = Log::create(sim(12), config(cache), ID).unwrap();
        let mut h = log.group(1).unwrap();
        h.write(Update {
            entries: Some(Entries {
                first: 1,
                entries: lens
                    .iter()
                    .map(|&len| Entry {
                        term: 1,
                        bytes: vec![7; len as usize],
                    })
                    .collect(),
            }),
            ..Update::default()
        })
        .unwrap();
        for per_entry in [0, 25, 1_000] {
            let mut bounds = vec![0, 1, u64::MAX];
            let mut total = 0;
            for &len in &lens {
                total += len + per_entry;
                bounds.extend([total - 1, total, total + 1]);
            }
            for max_bytes in bounds {
                let page = Page {
                    max_bytes,
                    per_entry,
                };
                let asked = h.asked();
                let fetched = h.fetch(1, high, page, Fetched::new()).unwrap();
                assert_eq!(h.asked() - asked, asks, "cache {cache}: read by {page:?}");
                let taken = page_of(&lens, page);
                assert_eq!(fetched.len(), taken, "cache {cache}: by {page:?}");
                for (at, (term, bytes)) in fetched.iter().enumerate() {
                    assert_eq!((term, bytes.len() as u64), (1, lens[at]));
                }
            }
        }
    }
}

/// A handle keeps the entries after its replica's mark whatever its group's share of the log's
/// cache, and asks the log for none of them; a raised mark lets the entries it passes go by the
/// share again. What it keeps past the share is counted exactly, through a suffix replaced too, and
/// a lower mark changes nothing.
#[test]
fn a_handle_keeps_what_its_replica_has_yet_to_apply() {
    let log = Log::create(sim(5), config(1), ID).unwrap();
    let mut h = log.group(1).unwrap();
    assert_eq!(h.kept_bytes(), 0, "nothing is kept before a mark");
    h.keep_after(0);
    h.write(Update {
        entries: Some(entries(1, 6, 1, 100)),
        ..Update::default()
    })
    .unwrap();
    assert_eq!(h.kept_bytes(), 600);
    assert_eq!(
        h.fetch(1, 7, Page::payload(u64::MAX), Fetched::new())
            .unwrap()
            .len(),
        6
    );
    assert_eq!(h.asked(), 0, "every kept entry answered by the handle");
    // The share is a byte: what the mark passes goes, and is the log's to answer.
    h.keep_after(4);
    assert_eq!(h.kept_bytes(), 200);
    assert_eq!(
        h.fetch(5, 7, Page::payload(u64::MAX), Fetched::new())
            .unwrap()
            .len(),
        2
    );
    assert_eq!(h.asked(), 0);
    assert_eq!(
        h.fetch(3, 5, Page::payload(u64::MAX), Fetched::new())
            .unwrap()
            .len(),
        2
    );
    assert_eq!(h.asked(), 1, "what the mark passed is the log's");
    // A suffix past the mark replaced: its old entry uncounted, the new ones counted.
    h.write(Update {
        entries: Some(entries(6, 3, 2, 50)),
        ..Update::default()
    })
    .unwrap();
    assert_eq!(h.kept_bytes(), 100 + 3 * 50);
    let replaced = h
        .fetch(5, 9, Page::payload(u64::MAX), Fetched::new())
        .unwrap();
    assert_eq!(
        replaced.get(1).unwrap(),
        (2, &entries(6, 1, 2, 50).entries[0].bytes[..])
    );
    assert_eq!(h.asked(), 1);
    h.keep_after(2);
    assert_eq!(h.kept_bytes(), 250, "a lower mark changes nothing");
    h.keep_after(8);
    assert_eq!(h.kept_bytes(), 0);
}

/// A write that fails leaves the handle reading from the log: it cannot know what the failed
/// frame left.
#[test]
fn a_failed_write_leaves_reads_to_the_log() {
    let log = Log::create(sim(3), config(1 << 20), ID).unwrap();
    let mut h = log.group(1).unwrap();
    h.write(Update {
        entries: Some(entries(1, 2, 1, 16)),
        ..Update::default()
    })
    .unwrap();
    log.with_file(|f| f.inject(Fault::WriteError).unwrap())
        .unwrap();
    let failed = h.write(Update {
        entries: Some(entries(3, 1, 1, 16)),
        ..Update::default()
    });
    assert!(matches!(failed, Err(LogError::Fenced)), "{failed:?}");
    let asked = h.asked();
    assert_eq!(h.view().unwrap(), log.view(1).unwrap());
    assert_eq!(h.asked(), asked + 1);
}

/// Each handle's write wakes its own waker alone, once, after its answer: 48 groups, each
/// writing through its handle with a counting waker, 12 rounds each.
#[test]
fn a_handles_write_wakes_only_its_writer() {
    const GROUPS: usize = 48;
    const ROUNDS: usize = 12;
    let log = Log::create(sim(4), config(1 << 20), ID).unwrap();
    let (ready, woken) = std::sync::mpsc::sync_channel(GROUPS);
    let wakers: Vec<_> = (0..GROUPS)
        .map(|g| hyper_measure::wake::waker(g, ready.clone()))
        .collect();
    drop(ready);
    let mut handles: Vec<GroupLog<SimFile>> =
        (0..GROUPS).map(|g| log.group(g as u128).unwrap()).collect();
    let submit = |h: &mut GroupLog<SimFile>, g: usize, round: usize| {
        let u = Update {
            entries: Some(entries(round as u64 + 1, 1, 1, 16)),
            ..Update::default()
        };
        h.submit_waking(u, wakers[g].0.clone()).unwrap();
    };
    for (g, h) in handles.iter_mut().enumerate() {
        submit(h, g, 0);
    }
    let mut answered = vec![0usize; GROUPS];
    let mut total = 0usize;
    while total < GROUPS * ROUNDS {
        let g = woken.recv().unwrap();
        handles[g].poll().expect("woken before its answer").unwrap();
        answered[g] += 1;
        total += 1;
        // A waker woken for another's answer would have been woken before its own came.
        assert_eq!(wakers[g].1.wakes() as usize, answered[g], "waker {g}");
        if answered[g] < ROUNDS {
            submit(&mut handles[g], g, answered[g]);
        }
    }
    for (g, h) in handles.iter().enumerate() {
        assert_eq!(h.bounds().unwrap().1, ROUNDS as u64, "group {g}");
    }
    drop(handles);
    drop(log);
    for (g, (_, slot)) in wakers.iter().enumerate() {
        assert_eq!(slot.wakes() as usize, ROUNDS, "waker {g}");
    }
    assert!(woken.try_recv().is_err(), "a wake with no answer");
}

/// An opener claims as the log does (focal 27 §15.8): from another thread its handle writes and
/// reads the group; two openers, or an opener and the log, cannot both hold one group, and once
/// the handle is dropped the group is anyone's again.
#[test]
fn an_opener_claims_as_the_log_does_from_any_thread() {
    fn shareable<T: Clone + Send + Sync + 'static>() {}
    shareable::<hyper_log::LogOpener<SimFile>>();
    let log = Log::create(sim(3), config(1 << 20), ID).unwrap();
    let (first, second) = (log.opener(), log.opener());
    assert_eq!(first.entry_room().unwrap(), log.entry_room().unwrap());
    assert_eq!(first.frame_room().unwrap(), log.frame_room().unwrap());
    let h = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let mut h = first.group(1).unwrap();
                h.write(Update {
                    entries: Some(entries(1, 2, 1, 8)),
                    ..Update::default()
                })
                .unwrap();
                h
            })
            .join()
            .unwrap()
    });
    assert_eq!(h.bounds().unwrap(), (Start::default(), 2));
    assert!(matches!(second.group(1), Err(LogError::Claimed(1))));
    assert!(matches!(log.group(1), Err(LogError::Claimed(1))));
    assert_eq!(second.groups().unwrap(), vec![1]);
    // The log's counts, read through an opener as through the log.
    let (seen, own) = (second.stats(None).unwrap(), log.stats(None).unwrap());
    assert_eq!((seen.frames, seen.updates), (own.frames, own.updates));
    assert!(seen.frames >= 1);
    drop(h);
    let h = second.group(1).unwrap();
    assert_eq!(h.bounds().unwrap(), (Start::default(), 2));
    assert!(matches!(first.group(1), Err(LogError::Claimed(1))));
}

/// An opener outliving its log claims nothing: every call after the log closed answers `Closed`.
#[test]
fn an_opener_after_its_log_closed_answers_closed() {
    let log = Log::create(sim(4), config(1 << 20), ID).unwrap();
    let opener = log.opener();
    // Both name the log they were made with, before it closes and after.
    assert_eq!((log.id(), opener.id()), (ID, ID));
    drop(log.close().unwrap());
    assert_eq!(opener.id(), ID);
    assert!(matches!(opener.group(1), Err(LogError::Closed)));
    assert!(matches!(opener.groups(), Err(LogError::Closed)));
    assert!(matches!(opener.stats(None), Err(LogError::Closed)));
}

/// The writes a dropped handle may leave out, one of each kind: a compaction's start, entries, a
/// release of proposals, a hard state.
fn left_out() -> [(&'static str, Update); 4] {
    [
        (
            "start",
            Update {
                start: Some(Start { index: 3, term: 1 }),
                ..Update::default()
            },
        ),
        (
            "entries",
            Update {
                entries: Some(entries(5, 2, 1, 64)),
                ..Update::default()
            },
        ),
        (
            "released",
            Update {
                released: Some(3),
                ..Update::default()
            },
        ),
        (
            "hard state",
            Update {
                hard_state: Some(HardState {
                    term: 1,
                    vote: 0,
                    commit: 4,
                    held: None,
                }),
                ..Update::default()
            },
        ),
    ]
}

/// A group claimed again at once after its handle was dropped with a write out is given the group
/// with that write, and reads as the log does: the claim waits for the write's answer, which went
/// to the dropped handle (focal, 2026-10-12: a shell group stopped and opened again at once in the
/// same process read entries a compaction left out had let go, `Compacted`, and missed entries and
/// marks). Each kind of write, left out at whatever point each seed's device answers it.
#[test]
fn a_group_claimed_again_at_once_holds_every_write_its_dropped_handle_left_out() {
    for seed in 0..32u64 {
        for (kind, update) in left_out() {
            let log = Log::create(sim(seed), config(1 << 20), ID).unwrap();
            let mut h = log.group(1).unwrap();
            h.write(Update {
                entries: Some(entries(1, 4, 1, 64)),
                ..Update::default()
            })
            .unwrap();
            h.submit(update).unwrap();
            drop(h);
            let again = log.group(1).unwrap();
            assert_eq!(
                again.view().unwrap(),
                log.view(1).unwrap(),
                "seed {seed}, {kind}"
            );
            same_reads(&log, &again, 1).unwrap_or_else(|e| panic!("seed {seed}, {kind}: {e}"));
        }
    }
}

/// Holds the log's device in a look at the file until `go` is told, applying the fault it is
/// told: the frames behind the look wait for it, so what is submitted meanwhile stays out. The
/// look holds the device once this returns.
fn hold_device<'scope>(
    scope: &'scope std::thread::Scope<'scope, '_>,
    log: &'scope Log<SimFile>,
) -> (
    std::sync::mpsc::SyncSender<Option<Fault>>,
    std::thread::ScopedJoinHandle<'scope, ()>,
) {
    let (holding, held) = std::sync::mpsc::sync_channel(1);
    let (go, told) = std::sync::mpsc::sync_channel::<Option<Fault>>(1);
    let looking = scope.spawn(move || {
        log.with_file(move |file| {
            holding.send(()).unwrap();
            if let Some(fault) = told.recv().unwrap() {
                file.inject(fault).unwrap();
            }
        })
        .unwrap();
    });
    held.recv().unwrap();
    (go, looking)
}

/// Claims `group` twice at once, each on a thread of its own: the answers, in the order they come.
fn claim_twice<'scope>(
    scope: &'scope std::thread::Scope<'scope, '_>,
    opener: &hyper_log::LogOpener<SimFile>,
    group: u128,
) -> std::sync::mpsc::Receiver<Result<GroupLog<SimFile>, LogError>> {
    let (told, answers) = std::sync::mpsc::sync_channel(2);
    for _ in 0..2 {
        let (told, opener) = (told.clone(), opener.clone());
        scope.spawn(move || told.send(opener.group(group)).unwrap());
    }
    answers
}

/// While a dropped handle's write is out, one claim of its group waits for the write's answer and
/// a second is refused `Claimed`, as is anyone else's write to the group, the waiting claim being
/// first; once the write is answered, the waiting claim is given the group with it.
#[test]
fn a_claim_waits_for_its_groups_write_out_and_holds_the_group_meanwhile() {
    let log = Log::create(sim(11), config(1 << 20), ID).unwrap();
    let opener = log.opener();
    let mut h = log.group(1).unwrap();
    h.write(Update {
        entries: Some(entries(1, 4, 1, 64)),
        ..Update::default()
    })
    .unwrap();
    std::thread::scope(|scope| {
        let (go, looking) = hold_device(scope, &log);
        h.submit(Update {
            start: Some(Start { index: 3, term: 1 }),
            ..Update::default()
        })
        .unwrap();
        drop(h);
        let answers = claim_twice(scope, &opener, 1);
        let first = answers.recv().unwrap();
        assert!(matches!(first, Err(LogError::Claimed(1))), "{first:?}");
        let theirs = log.submit(
            1,
            Update {
                hard_state: Some(HardState {
                    term: 1,
                    vote: 0,
                    commit: 4,
                    held: None,
                }),
                ..Update::default()
            },
        );
        assert!(matches!(theirs, Err(LogError::Claimed(1))), "{theirs:?}");
        go.send(None).unwrap();
        looking.join().unwrap();
        let again = answers.recv().unwrap().unwrap();
        assert_eq!(again.bounds().unwrap(), (Start { index: 3, term: 1 }, 4));
        assert_eq!(again.view().unwrap(), log.view(1).unwrap());
    });
}

/// Anyone's write to a group whose dropped handle's write is out, with no claim waiting, is taken
/// behind that write, as once the group is released; a claim then waits for both.
#[test]
fn a_write_to_a_group_whose_dropped_handles_write_is_out_is_taken_behind_it() {
    let log = Log::create(sim(12), config(1 << 20), ID).unwrap();
    let mut h = log.group(1).unwrap();
    h.write(Update {
        entries: Some(entries(1, 4, 1, 64)),
        ..Update::default()
    })
    .unwrap();
    let theirs = std::thread::scope(|scope| {
        let (go, looking) = hold_device(scope, &log);
        h.submit(Update {
            entries: Some(entries(5, 2, 1, 64)),
            ..Update::default()
        })
        .unwrap();
        drop(h);
        let theirs = log
            .submit(
                1,
                Update {
                    entries: Some(entries(7, 1, 1, 64)),
                    ..Update::default()
                },
            )
            .unwrap();
        go.send(None).unwrap();
        looking.join().unwrap();
        theirs
    });
    let again = log.group(1).unwrap();
    theirs.wait().unwrap();
    assert_eq!(again.bounds().unwrap(), (Start::default(), 7));
    same_reads(&log, &again, 1).unwrap();
}

/// A claim waiting for a dropped handle's write is answered when the log fences on that write's
/// frame: the write is answered `Fenced`, and the claim is given the group without it.
#[test]
fn a_claim_waiting_for_a_write_the_log_fences_on_is_given_the_group_without_it() {
    let log = Log::create(sim(13), config(1 << 20), ID).unwrap();
    let opener = log.opener();
    let mut h = log.group(1).unwrap();
    h.write(Update {
        entries: Some(entries(1, 4, 1, 64)),
        ..Update::default()
    })
    .unwrap();
    std::thread::scope(|scope| {
        let (go, looking) = hold_device(scope, &log);
        h.submit(Update {
            start: Some(Start { index: 3, term: 1 }),
            ..Update::default()
        })
        .unwrap();
        drop(h);
        let answers = claim_twice(scope, &opener, 1);
        let first = answers.recv().unwrap();
        assert!(matches!(first, Err(LogError::Claimed(1))), "{first:?}");
        go.send(Some(Fault::WriteError)).unwrap();
        looking.join().unwrap();
        let again = answers.recv().unwrap().unwrap();
        assert!(log.is_fenced());
        assert_eq!(again.bounds().unwrap(), (Start::default(), 4));
        assert_eq!(again.view().unwrap(), log.view(1).unwrap());
    });
}
