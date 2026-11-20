//! A leader that catches a member up from its log's file sends it, in the drive that takes a run's
//! read, as many pages as the member's credit admits (`Config::max_inflight_msgs`), rather than one
//! with the rest waiting for the member's acknowledgement (`Replica::stream_catch_up`,
//! `docs/durable.md` §8). The suite stands alone: `tests/support` takes hyper-check, which is not
//! vendored with this crate, so it runs through this crate's own manifest wherever it is.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cast_possible_truncation
)]

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::task::Waker;
use std::time::Duration;

use hyper_block::DiskError;
use hyper_block::block::{BlockFile, Durable};
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_durable::{
    Disk, EntryRef, Fatal, GroupStore, Output, Point, Replica, Settings, StateMachine, Unbounded,
};
use hyper_log::{Config, Log, Waits};
use hyper_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Message,
    MessageType,
};

type Member = Replica<GroupStore<Gated>, Noted, Unbounded>;

/// How long the test waits at most for a read of the file or a wake: far past either on a working
/// machine, however busy; it fails only a wait that would never end.
const PATIENCE: Duration = Duration::from_secs(60);

/// A state machine that keeps only where it stands, durable as it applies.
struct Noted {
    at: Point,
    configuration: ConfState,
}

impl StateMachine for Noted {
    type Answer = ();

    fn apply(&mut self, entry: &EntryRef<'_>, _: &mut Vec<()>) -> Result<bool, Fatal> {
        self.at = Point {
            index: entry.index,
            term: entry.term,
        };
        Ok(true)
    }

    fn apply_change(
        &mut self,
        at: Point,
        _: &ConfChangeV2,
        configuration: &ConfState,
    ) -> Result<(), Fatal> {
        self.at = at;
        self.configuration = configuration.clone();
        Ok(())
    }

    fn durable(&self) -> Point {
        self.at
    }

    fn configuration(&self) -> &ConfState {
        &self.configuration
    }

    fn acts_at_start(&self, _: &EntryRef<'_>) -> bool {
        false
    }

    fn image(&mut self, _: &mut Vec<u8>) -> Result<(Point, ConfState), Fatal> {
        Ok((self.at, self.configuration.clone()))
    }

    fn image_bytes(&self) -> Option<u64> {
        Some(0)
    }

    fn install(&mut self, _: &[u8], at: Point, configuration: &ConfState) -> Result<(), Fatal> {
        self.at = at;
        self.configuration = configuration.clone();
        Ok(())
    }

    fn persist(&mut self) -> Result<(), Fatal> {
        Ok(())
    }
}

/// A file whose reads wait while the gate is shut ([`SHUT`]): the reads are hyper-log's fetcher's
/// (`BlockFile::try_clone` moves the gate's opening to it), as in `tests/catch_up.rs`.
struct Gated {
    file: DeviceFile,
    /// A token for each opening of the gate ([`Gate::open`]).
    opened: Cell<Option<Receiver<()>>>,
}

/// Whether the fetcher's reads of the file wait: the test decides it between its calls into the
/// replica, so which read waits follows from what the replica has done, however many reads of the
/// file the log makes for one fetch, a run's entries lying in as many reads as frames apart. One
/// test in this binary holds it.
static SHUT: AtomicBool = AtomicBool::new(false);

/// The test's side of the gate. Dropped as the test ends, before the log, it opens: a read waiting
/// at it when the test fails goes on, and the log's threads end.
struct Gate {
    opened: SyncSender<()>,
}

impl Gate {
    fn shut(&self) {
        SHUT.store(true, Ordering::Release);
    }

    /// Opens the gate and wakes a read waiting at it. A token already waiting wakes it as well, so
    /// none is sent past the channel's one.
    fn open(&self) {
        SHUT.store(false, Ordering::Release);
        let _ = self.opened.try_send(());
    }
}

impl Drop for Gate {
    fn drop(&mut self) {
        self.open();
    }
}

impl BlockFile for Gated {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn layout_block(&self) -> Alignment {
        self.file.layout_block()
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        let opened = self.opened.take();
        if let Some(o) = &opened {
            // A token left by an earlier opening only looks again; a gate dropped lets it go.
            while SHUT.load(Ordering::Acquire) {
                if o.recv().is_err() {
                    break;
                }
            }
        }
        self.opened.set(opened);
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }

    fn write_durable_at(&self, buf: &[u8], offset: u64) -> Result<Durable, DiskError> {
        self.file.write_durable_at(buf, offset)
    }

    fn fills_new_space(&self) -> bool {
        self.file.fills_new_space()
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            opened: Cell::new(self.opened.take()),
        })
    }
}

/// A learner the test plays: it holds the leader's entries through `matched`, takes an append that
/// follows what it holds and answers it at once, and refuses one past it, saying where it ends. It
/// answers what a drive sent it before the next drive, so a drive's pages are all it has unanswered,
/// and it holds the leader to its credit there.
struct Learner {
    id: u64,
    matched: u64,
    credit: usize,
}

impl Learner {
    fn answer(&mut self, r: &mut Member, sent: &[Message]) {
        let pages = pages_to(sent, self.id);
        assert!(
            pages <= self.credit,
            "a drive sent the learner {pages} pages, past its credit of {}",
            self.credit
        );
        for m in sent
            .iter()
            .filter(|m| m.to == self.id && m.msg_type == MessageType::MsgAppend)
        {
            let mut answer = Message {
                msg_type: MessageType::MsgAppendResponse,
                from: self.id,
                to: m.from,
                term: m.term,
                ..Message::default()
            };
            if m.index <= self.matched {
                let end = m.index + m.entries.len() as u64;
                self.matched = self.matched.max(end);
                answer.index = end;
            } else {
                answer.index = m.index;
                answer.reject = true;
                answer.reject_hint = self.matched;
            }
            r.step(answer).unwrap();
        }
    }
}

/// The replica's clock: it only moves forward, and no step here waits on it.
fn now() -> u64 {
    thread_local! {
        static CLOCK: Cell<u64> = const { Cell::new(0) };
    }
    CLOCK.with(|clock| {
        clock.set(clock.get() + 1);
        clock.get()
    })
}

/// Drives `r` until it has nothing more to do and no write out, its learner answering what it
/// sends; with `pages`, until no page of a catch-up is being read either.
fn settle(
    r: &mut Member,
    waker: &Waker,
    woken: &Receiver<usize>,
    learner: &mut Learner,
    pages: bool,
) {
    let mut out = Output::default();
    loop {
        out.clear();
        let driven = r.drive(now(), waker, &mut out).unwrap();
        learner.answer(r, &out.messages);
        if driven.more || !out.messages.is_empty() {
            continue;
        }
        if r.in_flight() == 0 && !(pages && r.catching_up()) {
            return;
        }
        woken.recv_timeout(PATIENCE).expect("the log answers");
    }
}

/// One turn of the test's: drives `r`, its learner answering what it sends, until a drive sends
/// nothing and has no more to do, waiting on nothing.
fn turn(r: &mut Member, waker: &Waker, learner: &mut Learner) {
    let mut out = Output::default();
    loop {
        out.clear();
        let driven = r.drive(now(), waker, &mut out).unwrap();
        learner.answer(r, &out.messages);
        if !driven.more && out.messages.is_empty() {
            return;
        }
    }
}

/// The pages of entries `sent` carries to member `to`.
fn pages_to(sent: &[Message], to: u64) -> usize {
    sent.iter()
        .filter(|m| m.to == to && m.msg_type == MessageType::MsgAppend && !m.entries.is_empty())
        .count()
}

/// Do: a sole voter writes 512 entries of 2 KiB on a log of 1 MiB segments, its store reading a run
/// of 64 KiB at a time and its core sending pages of 8 KiB, a member's credit 4 of them. A learner
/// joins holding nothing and answers each append at once; the read of its second run is held until
/// it has taken every page of its first, then let go. Expect: the drive that takes the run's read
/// sends the learner 4 pages, its credit, at once. Before, it sent one, and the rest of the credit
/// went out only with the learner's acknowledgement of it, a round trip for every run read.
///
/// The second run's read is held by a gate the test shuts before each of its turns of the replica
/// and opens after one that left the learner holding nothing: a run's read is asked for only within
/// a turn, the second's within the turn that took the first's, the first after which the learner
/// holds entries, so the first run's reads all pass, however many reads of the file it takes, and
/// the second's all wait.
#[test]
fn the_drive_that_takes_a_runs_read_sends_a_member_pages_up_to_its_credit() {
    const ENTRY: usize = 2 << 10;
    const BEHIND: usize = 512;
    const CREDIT: usize = 4;
    const RUN: u64 = 64 << 10;
    const LEARNER: u64 = 3;
    let dir = tempfile::tempdir().unwrap();
    let (opens, opened) = sync_channel(1);
    let file = Gated {
        file: DeviceFile::open(
            &dir.path().join("log"),
            true,
            CachingRequest::PreferDirect,
            Alignment::new(4096).unwrap(),
        )
        .unwrap(),
        opened: Cell::new(Some(opened)),
    };
    let config = Config {
        segment_bytes: 1 << 20,
        max_segments: 64,
        max_groups: 4,
        group_entries: 1 << 16,
        group_bytes: 1 << 26,
        group_cache: 1 << 16,
        queue_submissions: 64,
        waits: Waits::Never,
    };
    let log = Log::create(file, config, 0x0063_7265_6469_7473).unwrap();
    let gate = Gate { opened: opens };
    let limits = hyper_raft::Limits::derive(hyper_raft::Stated {
        message: 64 << 10,
        members: 3,
        memory: 16 << 20,
        depth: 1,
    })
    .unwrap();
    let settings = Settings {
        core: hyper_raft::Config {
            max_size_per_msg: 8 << 10,
            max_inflight_msgs: CREDIT,
            max_committed_size_per_ready: 1 << 20,
            check_quorum: true,
            pre_vote: true,
            seed: 3,
            ..hyper_raft::Config::new(1, limits)
        },
        quiet: Duration::from_millis(50),
        elections: hyper_raft::Elections::Suspicion,
        disk: Disk::Flushed,
    };
    let machine = Noted {
        at: Point::default(),
        configuration: ConfState {
            voters: vec![1],
            ..ConfState::default()
        },
    };
    let store = GroupStore::new(log.group(1).unwrap(), RUN);
    let mut r = Replica::open(&settings, store, machine, Unbounded).unwrap();
    let (tell, woken) = sync_channel(1 << 12);
    let (waker, _) = hyper_measure::wake::waker(0, tell);
    let mut learner = Learner {
        id: LEARNER,
        matched: 0,
        credit: CREDIT,
    };
    r.campaign().unwrap();
    settle(&mut r, &waker, &woken, &mut learner, true);
    assert!(r.is_leader());
    for batch in 0..BEHIND / 32 {
        for _ in 0..32 {
            r.propose(Vec::new(), vec![batch as u8; ENTRY]).unwrap();
        }
        settle(&mut r, &waker, &woken, &mut learner, true);
    }

    // The learner joins holding nothing, and an append it refuses leaves it so. The gate is open
    // between turns until it holds entries: the turn that takes its first run's read sends it pages
    // of it and asks for its second run's read, which waits at the gate, shut from then on, while
    // the learner takes every page of the first.
    gate.shut();
    let change = ConfChangeV2 {
        transition: ConfChangeTransition::Auto,
        changes: vec![ConfChangeSingle {
            change_type: ConfChangeType::AddLearnerNode,
            node_id: LEARNER,
        }],
        context: Vec::new(),
    };
    r.change(Vec::new(), &change).unwrap();
    turn(&mut r, &waker, &mut learner);
    while learner.matched == 0 {
        gate.open();
        woken
            .recv_timeout(PATIENCE)
            .expect("the learner's first run is read from the file");
        gate.shut();
        turn(&mut r, &waker, &mut learner);
    }
    while r.in_flight() > 0 {
        woken
            .recv_timeout(PATIENCE)
            .expect("the log answers the leader's writes");
        turn(&mut r, &waker, &mut learner);
    }
    assert!(
        r.catching_up(),
        "the learner's second run is asked of the file"
    );
    let last = r.core().raft.log().last_index().unwrap();
    let first = learner.matched;
    assert!(
        first > 0 && first < last,
        "the learner took its first run, {first} of {last} entries, and waits at its second"
    );

    // The second run's read let go, the drive that takes it sends the learner its credit.
    gate.open();
    let mut out = Output::default();
    let pages = loop {
        woken
            .recv_timeout(PATIENCE)
            .expect("the second run's read is answered");
        out.clear();
        r.drive(now(), &waker, &mut out).unwrap();
        let pages = pages_to(&out.messages, LEARNER);
        if pages > 0 {
            break pages;
        }
    };
    eprintln!("the drive that took the second run's read sent the learner {pages} pages");
    assert_eq!(
        pages, CREDIT,
        "the drive that took the run's read sent the learner pages up to its credit"
    );
    learner.answer(&mut r, &out.messages);
    while learner.matched < last {
        settle(&mut r, &waker, &woken, &mut learner, true);
    }
}
