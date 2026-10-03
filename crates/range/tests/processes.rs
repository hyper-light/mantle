//! A range's group as real processes on real disks (docs/design/replica.md §5): each member an OS
//! process of its own, this test binary launched again with `MANTLE_RANGE_MEMBER` set, its log on a
//! real file through the direct-I/O file layer and the platform's full flush, speaking to the
//! supervisor over TCP on the loopback interface. The supervisor is the network. It runs the group
//! in rounds, as the simulation's directed runs do (`tests/sim.rs`): each round every member takes
//! what was sent to it the round before, ticks once and works until it has nothing left to do,
//! its writes durable; and it kills members with `SIGKILL`.
//!
//! D-1's first test (hyper-raft docs/durable.md §11) on real processes: a replacement of a lost
//! member (`membership::Replacement`), one member killed inside the window between a change's
//! commit and the commit its log states, which the member names as it reaches it and waits there
//! to be killed; then every member killed at once the moment the leader says every voter knows the
//! final configuration, and each restarted from its device must reopen in that configuration; then
//! one member of it lost for good, and the other two must elect a leader and commit an entry.
//!
//! The engine is the model engine, which keeps nothing across a process's death: a member that
//! restarts opens it empty and applies again what its log states committed, as one whose engine
//! never persisted. No compaction runs, so the log holds every entry.
//!
//! Frames on the wire, both ways: a length and a CRC-32C of the payload, little-endian, then the
//! payload. A Raft message inside a frame is the core's own record, with its own CRC-32C
//! (hyper-raft `wire`).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::disallowed_methods
)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use focal_raft::wire::Record;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_log::{Config as LogConfig, Log, Waits};
use mantle_codec::{Reader, Writer};
use mantle_meta::apply::Layer;
use mantle_meta::engine::{Engine, Model};
use mantle_meta::name;
use mantle_meta::session::Rules;
use mantle_meta::wire::{Answer, Command as Op, Entry, Sessioned};
use mantle_range::membership::{Next, Replacement};
use mantle_range::{ConfState, Message, Range, Replica, ReplicaError, Settings};

const GROUP: u128 = 0x0072_616e_6765;

/// The simulation's log, on a real file: a ready of the range's settings fits a group's bounds.
fn log_config() -> LogConfig {
    LogConfig {
        segment_bytes: 16 * 4096,
        max_segments: 64,
        max_groups: 4,
        group_entries: 1 << 10,
        group_bytes: 4 << 10,
        group_cache: 1 << 12,
        queue_submissions: 16,
        // Each member has one update out at a time, so none returns within a wait.
        waits: Waits::Never,
    }
}

/// The simulation's settings (`tests/sim.rs`), so the two run the same group.
const SETTINGS: Settings = Settings {
    election_tick: 10,
    heartbeat_tick: 2,
    max_size_per_msg: 1 << 10,
    max_inflight_msgs: 2,
    max_uncommitted_size: 2 << 10,
    max_committed_size_per_ready: 1 << 20,
    max_entry_bytes: 1 << 10,
};

const RULES: Rules = Rules {
    lifetime_ns: u64::MAX / 2,
    max_sessions: 64,
    max_answers: 16,
    max_answer_bytes: usize::MAX,
    expiries_per_entry: 8,
};

/// Voters the group starts with.
const FOUNDERS: u64 = 3;

/// Rounds a run waits for what a group with a quorum and no faults does within a few election
/// timeouts (elect a leader, commit an entry, make a change): twenty of the longest timeout the
/// core draws, `2 · election_tick`, as the simulation's directed runs wait. Reaching it is the
/// failure the run looks for.
const PATIENT_ROUNDS: u64 = 40 * SETTINGS.election_tick as u64;

/// Rounds a leader waits before it proposes again a change or an entry that has not taken
/// effect: the simulation's patience.
const PATIENCE: u64 = 40;

fn range() -> Range {
    Range {
        layer: Layer::Name,
        rules: RULES,
        boot: ConfState {
            voters: (1..=FOUNDERS).collect(),
            ..ConfState::default()
        },
        settings: SETTINGS,
    }
}

/// The engine of a cell's first Name range, before any entry.
fn first_range() -> Model {
    let mut m = Model::default();
    m.install(0, name::first(1).unwrap()).unwrap();
    m.persist().unwrap();
    m
}

fn registration(nonce: u64) -> Entry {
    Entry {
        at_ns: 0,
        commands: vec![Sessioned {
            session: 0,
            serial: nonce,
            unanswered: 0,
            command: Op::Register,
        }],
    }
}

/// What the supervisor tells a member to do, besides taking messages and a tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Order {
    Campaign,
    /// Run this replacement while it leads, until it is done.
    Replace {
        failed: u64,
        joining: u64,
    },
    /// Propose a registration of this nonce while it leads, until it is applied.
    Register(u64),
}

/// What a member says at the end of each round.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Status {
    id: u64,
    term: u64,
    leader: u64,
    leads: bool,
    applied: u64,
    committed: u64,
    logged: u64,
    /// The replacement it ran as leader is done: every voter knows the final configuration.
    done: bool,
    voters: Vec<u64>,
    outgoing: Vec<u64>,
    learners: Vec<u64>,
    /// The registrations it applied.
    registered: Vec<u64>,
}

impl Status {
    fn configuration(&self) -> ConfState {
        ConfState {
            voters: self.voters.clone(),
            learners: self.learners.clone(),
            voters_outgoing: self.outgoing.clone(),
            ..ConfState::default()
        }
    }

    fn put(&self, w: &mut Writer) {
        for v in [
            self.id,
            self.term,
            self.leader,
            u64::from(self.leads),
            self.applied,
            self.committed,
            self.logged,
            u64::from(self.done),
        ] {
            w.u64(v);
        }
        for list in [
            &self.voters,
            &self.outgoing,
            &self.learners,
            &self.registered,
        ] {
            w.u32(list.len() as u32);
            for v in list {
                w.u64(*v);
            }
        }
    }

    fn take(r: &mut Reader<'_>) -> Self {
        let mut v = [0u64; 8];
        for x in &mut v {
            *x = r.u64().unwrap();
        }
        let mut lists: [Vec<u64>; 4] = Default::default();
        for list in &mut lists {
            let n = r.u32().unwrap();
            for _ in 0..n {
                list.push(r.u64().unwrap());
            }
        }
        let [voters, outgoing, learners, registered] = lists;
        Status {
            id: v[0],
            term: v[1],
            leader: v[2],
            leads: v[3] == 1,
            applied: v[4],
            committed: v[5],
            logged: v[6],
            done: v[7] == 1,
            voters,
            outgoing,
            learners,
            registered,
        }
    }
}

/// A frame: its payload's length and CRC-32C, then the payload.
fn send(stream: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    let mut frame = Vec::with_capacity(payload.len() + 8);
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&mantle_crc::crc32c(payload).to_le_bytes());
    frame.extend_from_slice(payload);
    stream.write_all(&frame)
}

/// The next frame's payload, its CRC-32C checked; `None` once the other side has gone.
fn receive(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut head = [0u8; 8];
    stream.read_exact(&mut head).ok()?;
    let len = u32::from_le_bytes(head[..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(head[4..].try_into().unwrap());
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).ok()?;
    assert_eq!(mantle_crc::crc32c(&payload), crc, "a frame's checksum");
    Some(payload)
}

const ROUND: u8 = 1;
const HELLO: u8 = 2;
const REPORT: u8 = 3;
const WINDOW: u8 = 4;

fn put_messages(w: &mut Writer, messages: &[Message]) {
    w.u32(messages.len() as u32);
    for m in messages {
        let bytes = m.encode_to_vec();
        w.u32(bytes.len() as u32);
        w.bytes(&bytes);
    }
}

fn take_messages(r: &mut Reader<'_>) -> Vec<Message> {
    let n = r.u32().unwrap();
    (0..n)
        .map(|_| {
            let len = r.u32().unwrap() as usize;
            Message::decode(r.take(len).unwrap()).expect("a message's record")
        })
        .collect()
}

// The member.

/// The member's side of one process: its log, its replica, and what the supervisor asked of it.
struct Member {
    replica: Replica<DeviceFile, Model>,
    /// The log, which outlives the replica that holds its group's handle.
    log: Log<DeviceFile>,
    replacing: Option<Replacement>,
    proposed_change_at: Option<u64>,
    done: bool,
    register: Option<u64>,
    proposed_entry_at: Option<u64>,
    registered: BTreeSet<u64>,
    round: u64,
    /// Name the window of this ordinal and wait there to be killed (`Replica::change_unlogged`):
    /// the member counts each time it enters one.
    stop_at_window: Option<u64>,
    windows: u64,
    in_window: bool,
}

/// The member's file: created empty for a new log, which sizes it as it writes.
fn open_file(path: &Path) -> DeviceFile {
    DeviceFile::open(
        path,
        !path.exists(),
        CachingRequest::PreferDirect,
        Alignment::new(4096).unwrap(),
    )
    .unwrap()
}

impl Member {
    fn open(id: u64, dir: &Path, stop_at_window: Option<u64>) -> Self {
        let path = dir.join(format!("log-{id}"));
        let fresh = !path.exists();
        let file = open_file(&path);
        let log = if fresh {
            Log::create(file, log_config(), u128::from(id)).unwrap()
        } else {
            Log::open(file, log_config(), u128::from(id)).unwrap().0
        };
        let replica = Replica::open(id, GROUP, &log, first_range(), &range(), id).unwrap();
        Self {
            replica,
            log,
            replacing: None,
            proposed_change_at: None,
            done: false,
            register: None,
            proposed_entry_at: None,
            registered: BTreeSet::new(),
            round: 0,
            stop_at_window,
            windows: 0,
            in_window: false,
        }
    }

    fn status(&self) -> Status {
        let r = &self.replica;
        let c = r.configuration();
        Status {
            id: r.id(),
            term: r.term(),
            leader: r.leader(),
            leads: r.is_leader(),
            applied: r.applied(),
            committed: r.committed(),
            logged: r.logged_commit(),
            done: self.done,
            voters: c.voters.clone(),
            outgoing: c.voters_outgoing.clone(),
            learners: c.learners.clone(),
            registered: self.registered.iter().copied().collect(),
        }
    }

    /// Works until the replica has nothing left to do, its writes durable, or until it reaches
    /// the window it was asked to stop at. A unit of work is one `Ready` taken or finished, and
    /// every input gives at most two (one taken, one that its notice gives), so a round's work
    /// is bounded by its inputs.
    fn work(&mut self, inputs: usize, sent: &mut Vec<Message>, stream: &mut TcpStream) {
        let budget = 2 * (inputs + 1) + 2;
        for _ in 0..budget {
            let out = self.replica.begin().unwrap();
            let idle = out.messages.is_empty() && out.applied.is_empty() && !out.persisting;
            sent.extend(out.messages);
            for a in out.applied {
                for (session, serial, answer) in a.answers {
                    if session == 0 && matches!(answer, Answer::Registered { .. }) {
                        self.registered.insert(serial);
                    }
                }
            }
            let in_window = self.replica.change_unlogged().unwrap();
            if in_window && !self.in_window {
                self.windows += 1;
            }
            self.in_window = in_window;
            if in_window && self.stop_at_window == Some(self.windows) {
                let mut w = Writer::default();
                w.u8(WINDOW);
                self.status().put(&mut w);
                send(stream, w.as_slice()).unwrap();
                // Killed here: nothing more is done, and nothing more is read.
                loop {
                    std::thread::park();
                }
            }
            if out.persisting {
                self.replica.wait_persisted();
                continue;
            }
            if idle {
                return;
            }
        }
        panic!(
            "member {} still had work after {budget} units",
            self.replica.id()
        );
    }

    /// One round: the messages, the orders, a tick, the work, and what the leader runs.
    fn round(&mut self, deliveries: Vec<Message>, orders: Vec<Order>, stream: &mut TcpStream) {
        self.round += 1;
        let inputs = deliveries.len() + orders.len() + 1;
        for m in deliveries {
            match self.replica.step(m) {
                Ok(())
                | Err(
                    ReplicaError::Refused(_)
                    | ReplicaError::Stalled
                    | ReplicaError::MessagesHeld { .. },
                ) => {}
                Err(e) => panic!("step on {}: {e}", self.replica.id()),
            }
        }
        for order in orders {
            match order {
                Order::Campaign => match self.replica.campaign() {
                    Ok(()) | Err(ReplicaError::Refused(_)) => {}
                    Err(e) => panic!("campaign: {e}"),
                },
                Order::Replace { failed, joining } => {
                    self.replacing = Replacement::new(failed, joining);
                }
                Order::Register(nonce) => self.register = Some(nonce),
            }
        }
        self.replica.tick().unwrap();
        let mut sent = Vec::new();
        self.work(inputs, &mut sent, stream);
        self.lead();
        self.work(1, &mut sent, stream);
        let mut w = Writer::default();
        w.u8(REPORT);
        self.status().put(&mut w);
        put_messages(&mut w, &sent);
        send(stream, w.as_slice()).unwrap();
    }

    /// While it leads: the replacement's next change, and the registration asked for, each
    /// proposed again after a patience without effect.
    fn lead(&mut self) {
        if !self.replica.is_leader() {
            return;
        }
        let round = self.round;
        if let Some(replacement) = self.replacing {
            match replacement.next(
                self.replica.configuration(),
                self.replica.caught_up(replacement.joining()),
                self.replica.configuration_known(),
            ) {
                Next::Done => self.done = true,
                Next::Wait => {}
                Next::Propose(change) => {
                    if self
                        .proposed_change_at
                        .is_none_or(|at| round - at >= PATIENCE)
                    {
                        match self.replica.propose_change(&change) {
                            Ok(()) | Err(ReplicaError::Refused(_) | ReplicaError::Stalled) => {}
                            Err(e) => panic!("propose a change: {e}"),
                        }
                        self.proposed_change_at = Some(round);
                    }
                }
            }
        }
        if let Some(nonce) = self.register
            && !self.registered.contains(&nonce)
            && self
                .proposed_entry_at
                .is_none_or(|at| round - at >= PATIENCE)
        {
            match self.replica.propose(&registration(nonce)) {
                Ok(()) | Err(ReplicaError::Refused(_) | ReplicaError::Stalled) => {}
                Err(e) => panic!("propose: {e}"),
            }
            self.proposed_entry_at = Some(round);
        }
    }
}

/// The member process: opens its log and replica, applies alone what its durable state says is
/// committed, says hello with what it reopened at, and serves rounds until the supervisor goes.
fn member_main(id: u64) {
    let dir = PathBuf::from(std::env::var("MANTLE_RANGE_DIR").unwrap());
    let supervisor = std::env::var("MANTLE_RANGE_SUPERVISOR").unwrap();
    let stop_at_window = std::env::var("MANTLE_RANGE_WINDOW")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&k| k > 0);
    // Connected before anything else, so a member that dies opening closes its stream and the
    // supervisor hears it.
    let mut stream = TcpStream::connect(supervisor).unwrap();
    stream.set_nodelay(true).unwrap();
    let mut member = Member::open(id, &dir, None);
    // Alone, it reaches what its own durable state says is committed.
    let mut unused = Vec::new();
    member.work(PATIENT_ROUNDS as usize, &mut unused, &mut stream);
    member.stop_at_window = stop_at_window;
    member.windows = 0;
    member.in_window = false;
    let mut w = Writer::default();
    w.u8(HELLO);
    member.status().put(&mut w);
    send(&mut stream, w.as_slice()).unwrap();
    while let Some(payload) = receive(&mut stream) {
        let mut r = Reader::new(&payload);
        assert_eq!(r.u8(), Some(ROUND));
        let deliveries = take_messages(&mut r);
        let n = r.u32().unwrap();
        let orders = (0..n)
            .map(|_| match r.u8().unwrap() {
                1 => Order::Campaign,
                2 => Order::Replace {
                    failed: r.u64().unwrap(),
                    joining: r.u64().unwrap(),
                },
                3 => Order::Register(r.u64().unwrap()),
                kind => panic!("order {kind}"),
            })
            .collect();
        member.round(deliveries, orders, &mut stream);
    }
    // The supervisor went: the member's replica goes before its log.
    let Member { replica, log, .. } = member;
    drop(replica);
    drop(log);
}

// The supervisor.

/// A member process the supervisor runs, and kills when it goes.
struct Process {
    child: Child,
    stream: TcpStream,
    status: Status,
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Supervisor {
    listener: TcpListener,
    dir: PathBuf,
    members: BTreeMap<u64, Process>,
    /// Messages sent in the last round, by their destination.
    wire: BTreeMap<u64, Vec<Message>>,
    orders: BTreeMap<u64, Vec<Order>>,
    rounds: u64,
}

impl Supervisor {
    fn new(dir: &Path) -> Self {
        Self {
            listener: TcpListener::bind("127.0.0.1:0").unwrap(),
            dir: dir.to_path_buf(),
            members: BTreeMap::new(),
            wire: BTreeMap::new(),
            orders: BTreeMap::new(),
            rounds: 0,
        }
    }

    /// Starts member `id` from what its device holds, and waits for its hello. It stops at its
    /// `stop_at_window`-th window, if one is given.
    fn start(&mut self, id: u64, stop_at_window: Option<u64>) -> &Status {
        let exe = std::env::current_exe().unwrap();
        let child = Command::new(exe)
            .args([
                "member_entry",
                "--exact",
                "--nocapture",
                "--test-threads",
                "1",
            ])
            .env("MANTLE_RANGE_MEMBER", id.to_string())
            .env("MANTLE_RANGE_DIR", &self.dir)
            .env(
                "MANTLE_RANGE_SUPERVISOR",
                self.listener.local_addr().unwrap().to_string(),
            )
            .env(
                "MANTLE_RANGE_WINDOW",
                stop_at_window.unwrap_or(0).to_string(),
            )
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let (mut stream, _) = self.listener.accept().unwrap();
        stream.set_nodelay(true).unwrap();
        let hello =
            receive(&mut stream).unwrap_or_else(|| panic!("member {id} went before it said hello"));
        let mut r = Reader::new(&hello);
        assert_eq!(r.u8(), Some(HELLO));
        let status = Status::take(&mut r);
        assert_eq!(status.id, id);
        self.members.insert(
            id,
            Process {
                child,
                stream,
                status,
            },
        );
        &self.members[&id].status
    }

    /// Kills member `id` with SIGKILL.
    fn kill(&mut self, id: u64) {
        let mut process = self.members.remove(&id).unwrap();
        process.child.kill().unwrap();
        process.child.wait().unwrap();
        self.wire.remove(&id);
        self.orders.remove(&id);
    }

    fn order(&mut self, id: u64, order: Order) {
        self.orders.entry(id).or_default().push(order);
    }

    /// One round. A member that names the window is killed there and started again; the
    /// members it would have heard from are reported in `windows`.
    fn round(&mut self, windows: &mut Vec<u64>) {
        self.rounds += 1;
        let ids: Vec<u64> = self.members.keys().copied().collect();
        let mut sent: BTreeMap<u64, Vec<Message>> = BTreeMap::new();
        for id in ids {
            let deliveries = self.wire.remove(&id).unwrap_or_default();
            let orders = self.orders.remove(&id).unwrap_or_default();
            let mut w = Writer::default();
            w.u8(ROUND);
            put_messages(&mut w, &deliveries);
            w.u32(orders.len() as u32);
            for order in orders {
                match order {
                    Order::Campaign => w.u8(1),
                    Order::Replace { failed, joining } => {
                        w.u8(2);
                        w.u64(failed);
                        w.u64(joining);
                    }
                    Order::Register(nonce) => {
                        w.u8(3);
                        w.u64(nonce);
                    }
                }
            }
            let process = self.members.get_mut(&id).unwrap();
            send(&mut process.stream, w.as_slice()).unwrap();
            let payload = receive(&mut process.stream)
                .unwrap_or_else(|| panic!("member {id} went in round {}", self.rounds));
            let mut r = Reader::new(&payload);
            match r.u8() {
                Some(REPORT) => {
                    process.status = Status::take(&mut r);
                    for m in take_messages(&mut r) {
                        sent.entry(m.to).or_default().push(m);
                    }
                }
                Some(WINDOW) => {
                    let at = Status::take(&mut r);
                    assert!(at.committed > at.logged, "{at:?}");
                    windows.push(id);
                    // Killed inside the window, and started again from its device.
                    self.kill(id);
                    self.start(id, None);
                }
                other => panic!("member {id} said {other:?}"),
            }
        }
        self.wire = sent;
    }

    fn leader(&self) -> Option<&Status> {
        self.members.values().map(|p| &p.status).find(|s| s.leads)
    }

    fn states(&self) -> Vec<Status> {
        self.members.values().map(|p| p.status.clone()).collect()
    }
}

fn same_configuration(a: &ConfState, b: &ConfState) -> bool {
    let sorted = |ids: &[u64]| {
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids
    };
    sorted(&a.voters) == sorted(&b.voters)
        && sorted(&a.learners) == sorted(&b.learners)
        && sorted(&a.voters_outgoing) == sorted(&b.voters_outgoing)
}

/// A replacement of member 3 by member 4 under member 1's lead, as real processes: `target`
/// stops at its `window`-th window between a change's commit and the commit its log states, is
/// killed there and started again; then every member is killed the moment the leader says the
/// replacement is done, each must reopen from its device in the final configuration, and with
/// `lost` stopped for good the other two must elect a leader and commit an entry. Returns
/// whether the target reached that window.
fn replacement_killed_inside_a_window(target: u64, window: u64, lost: u64) -> bool {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Supervisor::new(dir.path());
    for id in 1..=FOUNDERS {
        s.start(id, None);
    }
    s.order(1, Order::Campaign);
    let mut windows = Vec::new();
    for _ in 0..PATIENT_ROUNDS {
        s.round(&mut windows);
        if s.members[&1].status.leads {
            break;
        }
    }
    assert!(s.members[&1].status.leads, "member 1 is never elected");
    // Member 3 is lost for good, its device and all; member 4 joins on a new one. The target is
    // started again to be told where to stop: its writes are durable at a round's end.
    s.kill(3);
    std::fs::remove_file(dir.path().join("log-3")).unwrap();
    s.start(4, (target == 4).then_some(window));
    if target != 4 {
        s.kill(target);
        s.start(target, Some(window));
    }
    for id in [1, 2, 4] {
        s.order(
            id,
            Order::Replace {
                failed: 3,
                joining: 4,
            },
        );
    }
    let mut done = false;
    for _ in 0..4 * PATIENT_ROUNDS {
        s.round(&mut windows);
        if s.leader().is_some_and(|l| l.done) {
            done = true;
            break;
        }
    }
    assert!(
        done,
        "target {target}, window {window}: the replacement never finished: {:#?}",
        s.states()
    );
    assert!(
        windows.is_empty() || windows == [target],
        "target {target}, window {window}: killed {windows:?}"
    );
    // Every member killed at once, the moment the leader says every voter knows; each reopens
    // from its device in the final configuration.
    let made = ConfState {
        voters: vec![1, 2, 4],
        ..ConfState::default()
    };
    for id in [1, 2, 4] {
        s.kill(id);
    }
    s.wire.clear();
    for id in [1, 2, 4] {
        let reopened = s.start(id, None).configuration();
        assert!(
            same_configuration(&reopened, &made),
            "target {target}, window {window}: member {id} reopens in {reopened:?} after the \
             leader said every voter knew {made:?}"
        );
    }
    // One member of the final configuration lost for good; the other two go on.
    s.kill(lost);
    let nonce = 0x4e55;
    for id in [1, 2, 4] {
        if id != lost {
            s.order(id, Order::Register(nonce));
        }
    }
    let mut live = false;
    for _ in 0..PATIENT_ROUNDS {
        s.round(&mut windows);
        if s.members
            .values()
            .all(|p| p.status.registered.contains(&nonce))
        {
            live = true;
            break;
        }
    }
    assert!(
        live,
        "target {target}, window {window}, member {lost} lost: the other two never elected a \
         leader that committed an entry on both: {:#?}",
        s.states()
    );
    !windows.is_empty()
}

/// Kills `target` at each of its windows in turn, a run each, until a run in which it reaches
/// none; the member lost for good after turns with the window over the final configuration.
fn every_window(target: u64) {
    // A replacement is three changes, each a window at a member that applies it.
    const MAX_WINDOWS: u64 = 1 << 4;
    let lost_after = [1, 2, 4];
    let mut window = 1;
    while replacement_killed_inside_a_window(
        target,
        window,
        lost_after[((window + target) % 3) as usize],
    ) {
        window += 1;
        assert!(
            window < MAX_WINDOWS,
            "target {target} never ran out of windows"
        );
    }
    assert!(window > 1, "target {target} reached no window");
}

/// The member process's entry: does nothing unless the supervisor launched it.
#[test]
fn member_entry() {
    let Ok(id) = std::env::var("MANTLE_RANGE_MEMBER") else {
        return;
    };
    member_main(id.parse().unwrap());
}

/// The leader of a replacement killed inside each window: its commit of a change known, its log
/// not yet stating it.
#[test]
fn a_replacements_leader_killed_inside_a_window_leaves_a_group_that_elects() {
    if std::env::var("MANTLE_RANGE_MEMBER").is_ok() {
        return;
    }
    every_window(1);
}

/// A follower that stays a voter, killed inside each window, its write stating the commit out.
#[test]
fn a_replacements_follower_killed_inside_a_window_leaves_a_group_that_elects() {
    if std::env::var("MANTLE_RANGE_MEMBER").is_ok() {
        return;
    }
    every_window(2);
}

/// The joining member killed inside each window of the changes that make it a voter.
#[test]
fn a_replacements_joining_member_killed_inside_a_window_leaves_a_group_that_elects() {
    if std::env::var("MANTLE_RANGE_MEMBER").is_ok() {
        return;
    }
    every_window(4);
}
