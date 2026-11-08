//! A leader catches up a member from its log's file without waiting on the read
//! (`LogStore::catch_up`, `docs/durable.md` §8): the page is read off the log's device by
//! hyper-log's fetcher and off the replica's thread, so the leader's writes and its appends to its
//! other members go on while the page is read, and the member is sent each page as it is read.
//! Before, the read held the log's one-job device and the replica's thread, and the leader's writes
//! waited for it: a reconnecting runner's catch-up behind every write of its leader's (kiln,
//! 2026-11-06).
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

mod support;

use std::cell::Cell;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::task::Waker;
use std::time::{Duration, Instant};

use hyper_block::DiskError;
use hyper_block::block::{BlockFile, Durable};
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_durable::{GroupStore, Output, Replica, Unbounded};
use hyper_log::{Config, Log, Waits};
use hyper_raft::proto::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Message,
    MessageType,
};
use support::Kv;
use support::cluster::settings;

type Member = Replica<GroupStore<Trapped>, Kv, Unbounded>;

/// How long the test holds the catch-up's read at most before it lets it go: far past any write's
/// frame and flush on a working machine, however busy. On the old code the leader's writes wait for
/// the read whatever this is, and the test fails once it passes.
const HELD_FOR: Duration = Duration::from_secs(60);

/// A file whose next read, once armed, holds until let go: the read is the handle's that reads
/// entries back, hyper-log's fetcher's (`BlockFile::try_clone` moves the trap to it), or the
/// device's on a log with none.
struct Trapped {
    file: DeviceFile,
    trap: Cell<Option<Trap>>,
}

struct Trap {
    armed: Receiver<()>,
    held: SyncSender<()>,
    go: Receiver<()>,
}

impl BlockFile for Trapped {
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
        let trap = self.trap.take();
        if let Some(t) = &trap
            && t.armed.try_recv().is_ok()
        {
            let _ = t.held.send(());
            let _ = t.go.recv();
        }
        self.trap.set(trap);
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
            trap: Cell::new(self.trap.take()),
        })
    }
}

/// A learner the test plays: it holds the leader's entries through `matched`, takes an append that
/// follows what it holds, and refuses one past it, saying where it ends.
struct Learner {
    id: u64,
    matched: u64,
    /// Appends with entries it took.
    pages: u64,
    /// Answers held a round, for a learner whose answers come late; none for one that answers at once.
    late: Option<Vec<Message>>,
}

impl Learner {
    fn answer(&mut self, r: &mut Member, sent: &[Message]) {
        if let Some(held) = self.late.as_mut() {
            for answer in std::mem::take(held) {
                r.step(answer).unwrap();
            }
        }
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
                self.pages += u64::from(!m.entries.is_empty());
                answer.index = end;
            } else {
                answer.index = m.index;
                answer.reject = true;
                answer.reject_hint = self.matched;
            }
            match self.late.as_mut() {
                Some(held) => held.push(answer),
                None => r.step(answer).unwrap(),
            }
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

/// Drives `r` until it has nothing more to do and no write out, its learners answering what it
/// sends them; with `pages`, until no page of a catch-up is being read either.
fn settle(
    r: &mut Member,
    waker: &Waker,
    woken: &Receiver<usize>,
    learners: &mut [Learner],
    pages: bool,
) {
    let mut out = Output::default();
    loop {
        out.clear();
        let driven = r.drive(now(), waker, &mut out).unwrap();
        for learner in learners.iter_mut() {
            learner.answer(r, &out.messages);
        }
        if driven.more || !out.messages.is_empty() {
            continue;
        }
        if r.in_flight() == 0 && !(pages && r.catching_up()) {
            return;
        }
        woken.recv().unwrap();
    }
}

fn add_learner(r: &mut Member, id: u64) {
    let change = ConfChangeV2 {
        transition: ConfChangeTransition::Auto,
        changes: vec![ConfChangeSingle {
            change_type: ConfChangeType::AddLearnerNode,
            node_id: id,
        }],
        context: Vec::new(),
    };
    r.change(Vec::new(), &change).unwrap();
}

/// Do: a sole voter leads learners 2 and 3 on a log on a file; it writes 8 MiB in 16 KiB entries
/// with learner 2 taking each append, past what the log keeps in memory, then learner 3, holding
/// nothing, asks for all of it. Hold the read of its first page, and while it is held, write 32
/// entries more. Expect: every write durable, applied and taken by learner 2 while the read is held;
/// once let go, learner 3 caught up page by page.
#[test]
fn a_leader_writes_and_appends_while_a_members_catch_up_is_read_from_its_file() {
    const ENTRY: usize = 16 << 10;
    const BEHIND: usize = 512;
    const BATCH: usize = 32;
    let dir = tempfile::tempdir().unwrap();
    let (arm, armed) = sync_channel(1);
    let (held, holding) = sync_channel(1);
    let (go, let_go) = sync_channel(1);
    let file = Trapped {
        file: DeviceFile::open(
            &dir.path().join("log"),
            true,
            CachingRequest::PreferDirect,
            Alignment::new(4096).unwrap(),
        )
        .unwrap(),
        trap: Cell::new(Some(Trap {
            armed,
            held,
            go: let_go,
        })),
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
    let log = Log::create(file, config, 0x0063_6174_6368_7570).unwrap();
    let mut settings = settings(1, 3);
    settings.core.max_size_per_msg = 64 << 10;
    settings.core.max_committed_size_per_ready = 1 << 20;
    let kv = Kv::new(
        ConfState {
            voters: vec![1],
            ..ConfState::default()
        },
        false,
    );
    let mut r = Replica::open(
        &settings,
        GroupStore::claim(&log, 1).unwrap(),
        kv,
        Unbounded,
    )
    .unwrap();
    let (tell, woken) = sync_channel(1 << 12);
    let (waker, _) = hyper_measure::wake::waker(0, tell);
    let mut learners = [
        Learner {
            id: 2,
            matched: 0,
            pages: 0,
            late: None,
        },
        Learner {
            id: 3,
            matched: 0,
            pages: 0,
            late: None,
        },
    ];
    r.campaign().unwrap();
    settle(&mut r, &waker, &woken, &mut learners, true);
    assert!(r.is_leader());
    add_learner(&mut r, 2);
    settle(&mut r, &waker, &woken, &mut learners, true);
    let mut written = Vec::new();
    for batch in 0..BEHIND / BATCH {
        let began = Instant::now();
        for _ in 0..BATCH {
            r.propose(Vec::new(), vec![batch as u8; ENTRY]).unwrap();
        }
        settle(&mut r, &waker, &woken, &mut learners, true);
        written.push(began.elapsed());
    }
    let behind = r.core().raft.log().last_index().unwrap();
    assert_eq!(learners[0].matched, behind, "learner 2 took every append");

    // Learner 3 joins holding nothing; the read of its first page from the file is held.
    arm.send(()).unwrap();
    let (done, finished) = sync_channel::<()>(1);
    let (fired, watchdog) = sync_channel::<()>(1);
    let releaser = std::thread::spawn(move || {
        if finished.recv_timeout(HELD_FOR) == Err(RecvTimeoutError::Timeout) {
            let _ = fired.send(());
        }
        let _ = go.send(());
    });
    add_learner(&mut r, 3);
    settle(&mut r, &waker, &woken, &mut learners, false);
    holding
        .recv_timeout(HELD_FOR)
        .expect("learner 3's catch-up reaches the file");

    // While it is held, the leader writes, applies and sends learner 2 what it writes.
    let mut writing = Vec::new();
    for _ in 0..BATCH {
        let began = Instant::now();
        r.propose(Vec::new(), vec![0xee; ENTRY]).unwrap();
        settle(&mut r, &waker, &woken, &mut learners, false);
        writing.push(began.elapsed());
    }
    let last = r.core().raft.log().last_index().unwrap();
    let held_throughout = watchdog.try_recv().is_err();
    assert!(
        held_throughout,
        "the leader's writes waited {HELD_FOR:?} on learner 3's catch-up"
    );
    assert_eq!(r.applied().index, last, "the leader applied what it wrote");
    assert_eq!(
        learners[0].matched, last,
        "learner 2 took what the leader wrote"
    );
    assert_eq!(
        learners[1].matched, 0,
        "learner 3 was sent nothing while its page was read"
    );
    assert!(r.catching_up(), "learner 3's page is still being read");

    // Let go, learner 3 is caught up page by page.
    done.send(()).unwrap();
    releaser.join().unwrap();
    let began = Instant::now();
    while learners[1].matched < last {
        settle(&mut r, &waker, &woken, &mut learners, true);
    }
    let caught_up = began.elapsed();
    let page = settings.core.max_size_per_msg;
    assert!(
        learners[1].pages * page >= (BEHIND * ENTRY) as u64,
        "learner 3 was sent its catch-up in pages of {page} bytes at most: {} pages",
        learners[1].pages
    );
    written.sort();
    writing.sort();
    eprintln!(
        "a batch of {BATCH} writes before: median {:?}; each write while learner 3's read was held: \
         median {:?}, most {:?}; learner 3 caught up {} MiB in {} pages in {caught_up:?}",
        written[written.len() / 2],
        writing[writing.len() / 2],
        writing[writing.len() - 1],
        (BEHIND * ENTRY) >> 20,
        learners[1].pages,
    );
}

/// Do: a sole voter writes 8 MiB in 16 KiB entries on a log of 1 MiB segments, then a learner joins holding nothing
/// and answers each append a round late, so the window the leader sends it ahead of its answers (8 appends, half a run)
/// fills while each run of the log is sent and the next is read. Expect: every run read from the file once. Before, a
/// run read while the learner was still in the one before it held none of its next index and was let go, and was read
/// again once the learner reached it, a time for each window it took to cross the run before (kiln's store::group,
/// 2026-11-07: 47 reads for 48 MiB of 8 MiB runs).
#[test]
fn a_learner_that_answers_late_is_sent_each_run_read_once() {
    const ENTRY: usize = 16 << 10;
    const BEHIND: usize = 512;
    const SEGMENT: u64 = 1 << 20;
    let dir = tempfile::tempdir().unwrap();
    let file = Trapped {
        file: DeviceFile::open(
            &dir.path().join("log"),
            true,
            CachingRequest::PreferDirect,
            Alignment::new(4096).unwrap(),
        )
        .unwrap(),
        trap: Cell::new(None),
    };
    let config = Config {
        segment_bytes: SEGMENT,
        max_segments: 64,
        max_groups: 4,
        group_entries: 1 << 16,
        group_bytes: 1 << 26,
        group_cache: 1 << 16,
        queue_submissions: 64,
        waits: Waits::Never,
    };
    let log = Log::create(file, config, 0x006c_6174_6531).unwrap();
    let mut settings = settings(1, 3);
    settings.core.max_size_per_msg = 64 << 10;
    settings.core.max_committed_size_per_ready = 1 << 20;
    let kv = Kv::new(
        ConfState {
            voters: vec![1],
            ..ConfState::default()
        },
        false,
    );
    let mut r = Replica::open(
        &settings,
        GroupStore::claim(&log, 1).unwrap(),
        kv,
        Unbounded,
    )
    .unwrap();
    let (tell, woken) = sync_channel(1 << 12);
    let (waker, _) = hyper_measure::wake::waker(0, tell);
    let mut learners = [Learner {
        id: 3,
        matched: 0,
        pages: 0,
        late: Some(Vec::new()),
    }];
    r.campaign().unwrap();
    settle(&mut r, &waker, &woken, &mut learners, true);
    for batch in 0..BEHIND / 32 {
        for _ in 0..32 {
            r.propose(Vec::new(), vec![batch as u8; ENTRY]).unwrap();
        }
        settle(&mut r, &waker, &woken, &mut learners, true);
    }
    let asked = r.log().handle().asked();
    add_learner(&mut r, 3);
    let last = r.core().raft.log().last_index().unwrap();
    while learners[0].matched < last {
        settle(&mut r, &waker, &woken, &mut learners, true);
    }
    let reads = r.log().handle().asked() - asked;
    let runs = (BEHIND * ENTRY) as u64 / SEGMENT + 1;
    assert!(
        reads <= runs,
        "{reads} reads of the log's file for {} MiB in runs of a {SEGMENT}-byte segment ({runs} at most)",
        (BEHIND * ENTRY) >> 20
    );
}
